//! HimoStore — 紐1本分のストレージ。
//!
//! Column（ソースオブトゥルース）+ LockFreeCylinder（検索キャッシュ、lock-free）。
//!
//! ぶら下げる → Column に書く + Cylinder に append
//! 引く       → Cylinder の raw を Column verify で filter（append-only なら skip）
//!
//! #95: 旧 `RwLock<BucketCylinder>` は read↔write が相互排他で、 長い read が write を
//! stall させた。 `LockFreeCylinder` に置換し read を完全 lock-free 化。 削除/更新は
//! Cylinder を触らず（append-only）、 stale は read 側の Column verify で落とす
//! （lazy / conditional verify）。

use parking_lot::Mutex;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::column::Column;
use crate::lockfree_cylinder::LockFreeCylinder;
use crate::region::Region;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ValueType {
    /// 共有タグ — Vocabulary を引く (dedupe あり)。複数 entity が同じ tag を共有する hub。
    Tag = 0,
    /// タグなし — u32 をそのまま値として扱う。inline 数値・eid 等。
    Number = 1,
    /// 他 entity への参照 — u32 を eid として扱う。engine は素通しするだけ、FK 制約は schema 層。
    Ref = 2,
    /// 終端タグ — FreeStore を引く (dedupe なし)。1 entity しか繋がらない葉ノード。
    Leaf = 3,
}

impl ValueType {
    pub fn from_byte(b: u8) -> Self {
        match b {
            0 => Self::Tag,
            2 => Self::Ref,
            3 => Self::Leaf,
            _ => Self::Number,
        }
    }
}

// #95 並行性:
//   LockFreeCylinder 自体は「同時に 1 writer」を要求する。 write の呼び出し元は
//   1 本ではない — consumer thread (tie_async) に加えて、 **同期 tie API
//   (`tie_to_by_id` 系、 schema `RowBuilder::commit` の経路) は任意の user thread が
//   &self で直接呼ぶ**。 master ではこれを RwLock の write lock が直列化していた。
//   よって:
//   - set/remove/restore は per-himo `write_lock` で直列化（多 thread 呼び出し可、
//     master の write 直列度と同一。 himo が違えば並列）
//   - pull/slice_len は lock-free read（epoch）。 read は write を一切待たない
//     （write_lock は writer 同士のみ、 reader は一切触らない = #95 の目的は維持）
//   Column の write も write_lock 下に入る（get_value→set の read-modify-write が
//   atomic になる）。 pull の verify は Column を直読みするが、 これは #95 とは
//   別の既知の無同期読みで別 issue（#100 と同時に設計）。

/// incremental compaction (request12 P2) の trigger: これ未満の bucket は
/// 組み直さない (小 bucket は verify の方が安い)。stale 率 (len-live)/len >= 1/2
/// と併用。閾値は churn_read ベンチで調整。
const COMPACT_MIN_LEN: usize = 64;

pub struct HimoStore {
    col: UnsafeCell<Column>,
    cyl: LockFreeCylinder,
    pub value_type: ValueType,
    /// 初期 bucket サイズのヒント。0 は「ヒントなし、必要時に拡張」。値の上限ではない。
    pub max_values: u32,
    /// cylinder が column から populate 済みか。lazy rebuild 用。
    /// `init`（新規 DB）では即 true（column 空）、`load`（既存 DB）では false で開始。
    cyl_built: AtomicBool,
    /// writer 直列化 lock。 lazy build と set/remove/restore が取る。
    /// reader (pull 系) は一切取らない。 同期 tie / schema commit の多 thread
    /// 呼び出しを master (RwLock write) と同じ直列度で安全にする（#96 レビュー発覚分）。
    write_lock: Mutex<()>,
}

// SAFETY: writer は write_lock で直列、 reader は lock-free（Cylinder は epoch +
// Mutex(sparse)）。 Column の read は無同期（既知、 別 issue）。
unsafe impl Sync for HimoStore {}
unsafe impl Send for HimoStore {}

impl HimoStore {
    pub fn init(col_region: Region, ht: ValueType, max_values: u32, max_entities: u32) -> Self {
        let col = Column::init(col_region, 4, max_entities);
        Self {
            col: UnsafeCell::new(col),
            cyl: LockFreeCylinder::new(max_values),
            value_type: ht,
            max_values,
            // 新規 column は空なので rebuild 不要、即 built 状態。
            cyl_built: AtomicBool::new(true),
            write_lock: Mutex::new(()),
        }
    }

    /// open 時の load。 cylinder は空のまま返し、最初の cyl 触りで
    /// `ensure_cylinder_built` 経由で rebuild する（reopen latency を膨らませないため）。
    pub fn load(col_region: Region, ht: ValueType, max_values: u32) -> Self {
        let col = Column::load(col_region);
        Self {
            col: UnsafeCell::new(col),
            cyl: LockFreeCylinder::new(max_values),
            value_type: ht,
            max_values,
            cyl_built: AtomicBool::new(false),
            write_lock: Mutex::new(()),
        }
    }

    fn col(&self) -> &Column {
        unsafe { &*self.col.get() }
    }

    /// cylinder が未 build なら column から rebuild。lazy build の入口。
    /// fast path は AtomicBool::load で即 return。
    ///
    /// #95: build は多数 insert（単一 writer 必須）なので `write_lock` で排他する。
    /// build 中の他 thread は write_lock を待つ（one-time、load 直後の初回のみ）。
    #[inline]
    fn ensure_cylinder_built(&self) {
        if self.cyl_built.load(Ordering::Acquire) {
            return;
        }
        let _g = self.write_lock.lock();
        // double-check（build 中に競合した別 thread が先に置いた可能性）
        if self.cyl_built.load(Ordering::Acquire) {
            return;
        }
        let col = self.col();
        let count = col.count();
        for eid in 0..count {
            let stored = u32::from_le_bytes(col.get(eid).try_into().unwrap());
            if stored != 0 {
                self.cyl.insert(eid, stored - 1);
            }
        }
        self.cyl_built.store(true, Ordering::Release);
    }

    // ──── ぶら下げる / 外す ────

    pub fn set(&self, eid: u32, value: u32) {
        self.ensure_cylinder_built();
        let _w = self.write_lock.lock();
        self.col().ensure_count(eid);
        let old = self.get_value(eid);
        if old == Some(value) {
            return; // 冗長な re-tie = no-op（bucket に dup を作らない）
        }
        let mut stale = None;
        if let Some(o) = old {
            // 値更新: 旧 value の bucket に stale が残る → その bucket の read は verify する。
            // Column を書き換える **前** に flag を立てる (request12、順序契約は note_stale 参照)
            stale = self.cyl.note_stale(o).map(|s| (o, s));
        }
        // #106: Release store。 leaf offset を publish する前に書いた LeafStore slot
        // (payload/gen) を、 offset を Acquire で読む reader が必ず観測できるようにする。
        self.col().store_u32_release(eid, value + 1);
        self.cyl.insert(eid, value);
        // compaction は Column 更新の **後** (keep = value_eq が新状態を見るため)
        if let Some((o, (len, live))) = stale {
            self.maybe_compact(o, len, live);
        }
    }

    pub fn remove(&self, eid: u32) {
        if eid < self.col().count() {
            self.ensure_cylinder_built();
            let _w = self.write_lock.lock();
            if let Some(o) = self.get_value(eid) {
                // 削除: 旧 bucket に stale が残る（Cylinder は触らない、verify で落とす）。
                // flag → Column の順 (set と同じ)
                let stale = self.cyl.note_stale(o);
                self.col().clear(eid);
                if let Some((len, live)) = stale {
                    self.maybe_compact(o, len, live);
                }
            }
        }
    }

    /// stale 率が閾値を超えた bucket を Column 基準で組み直す (request12 P2)。
    /// write_lock 下・Column 更新後に呼ぶこと。trigger は stale 率 50% なので
    /// amortized O(1)/write (Vec doubling と同じ理屈 — 組み直し後の stale は 0、
    /// 次の trigger までに live 相当数の churn が必要)。
    fn maybe_compact(&self, value: u32, len: usize, live: u32) {
        if len >= COMPACT_MIN_LEN && (len - live as usize) * 2 >= len {
            self.cyl.compact_bucket(value, |eid| self.value_eq(eid, value));
        }
    }

    /// 全 bucket を Column 基準で即時 compaction する明示 API (運用/テスト用)。
    /// reader は停止しない (bucket ごとの epoch swap)。
    pub fn compact_now(&self) {
        self.ensure_cylinder_built();
        let _w = self.write_lock.lock();
        for v in self.cyl.unique_values() {
            // clean bucket (churn 痕なし) は組み直し不要 — 無条件 swap は巨大 himo で
            // write_lock の長期保持 + 旧 backing の epoch 滞留 (一時 ~2x RSS) を招く
            // (PR #103 レビュー)。write_lock 下なので flag 判定は正確。
            if self.cyl.bucket_needs_verify(v) {
                self.cyl.compact_bucket(v, |eid| self.value_eq(eid, v));
            }
        }
    }

    /// 現在の unique 値数 (live 基準、churn があっても正確 — request12)。
    pub fn unique_count(&self) -> u32 {
        self.ensure_cylinder_built();
        self.cyl.unique_live()
    }

    // ──── 読む（Column 直読み、Cylinder 非依存）────

    pub fn get_value(&self, eid: u32) -> Option<u32> {
        if eid >= self.col().count() {
            return None;
        }
        // #106: Acquire load。 writer の `store_u32_release` と対で、 leaf offset を
        // 掴んだら対応する LeafStore slot の payload/gen も必ず観測できるようにする。
        let stored = self.col().load_u32_acquire(eid);
        if stored == 0 {
            None
        } else {
            Some(stored - 1)
        }
    }

    /// SIMD 集計向け raw stored values への view（stored 形式: 0 = 未設定、N = 値 N-1）。
    #[inline]
    pub fn stored_slice(&self) -> &[u32] {
        self.col().values_u32()
    }

    /// bulk get-value（stored 形式、buffer reuse）。
    #[inline]
    pub fn get_stored_into(&self, eids: &[enchudb_oplog::EntityId], out: &mut Vec<u32>) {
        let col = self.col();
        let count = col.count();
        out.clear();
        out.reserve(eids.len());
        for &eid in eids {
            let lid = enchudb_oplog::eid_local(eid);
            if lid >= count {
                out.push(0);
                continue;
            }
            out.push(u32::from_le_bytes(col.get(lid).try_into().unwrap()));
        }
    }

    /// eid の現在値が value か（= lazy verify の primitive、Column 直読み）。
    #[inline(always)]
    pub fn value_eq(&self, eid: u32, value: u32) -> bool {
        u32::from_le_bytes(self.col().get(eid).try_into().unwrap()) == value + 1
    }

    pub fn get_raw_bytes(&self, eid: u32) -> [u8; 4] {
        if eid >= self.col().count() {
            return [0u8; 4];
        }
        self.col().get(eid).try_into().unwrap()
    }

    pub fn restore(&self, eid: u32, old_bytes: &[u8; 4]) {
        self.ensure_cylinder_built();
        let _w = self.write_lock.lock();
        self.col().ensure_count(eid);
        let stored = u32::from_le_bytes(*old_bytes);
        let old = self.get_value(eid);
        let new = if stored == 0 { None } else { Some(stored - 1) };
        if old == new {
            return; // 同値 restore = no-op (bytes も同一)
        }
        let mut stale = None;
        if let Some(o) = old {
            // flag → Column の順 (set と同じ、request12)
            stale = self.cyl.note_stale(o).map(|s| (o, s));
        }
        self.col().set(eid, old_bytes);
        if let Some(n) = new {
            self.cyl.insert(eid, n);
        }
        if let Some((o, (len, live))) = stale {
            self.maybe_compact(o, len, live);
        }
    }

    // ──── 引く ────

    /// 値に合致する entity。#95: Cylinder の raw を、削除/更新があった **bucket** でのみ
    /// Column verify + dedup で filter（churn していない bucket は raw 直返し =
    /// fast path。request12 で himo 単位 → bucket 単位に局所化）。
    pub fn pull(&self, value: u32) -> Vec<u32> {
        self.ensure_cylinder_built();
        let (raw, needs_verify) = self.cyl.read_to_vec_verify(value);
        if !needs_verify {
            // この bucket は append-only: 全 live、dup なし
            raw
        } else {
            // lazy verify: Column で現在値を確認、churn 由来の dup を除去
            let mut out: Vec<u32> = raw
                .into_iter()
                .filter(|&eid| self.value_eq(eid, value))
                .collect();
            out.sort_unstable();
            out.dedup();
            out
        }
    }

    /// 値が tie された全 entity（= column 非ゼロ走査）。O(next_eid) で重い。
    pub fn entities_with_value(&self) -> Vec<u32> {
        let count = self.col().count();
        let mut result = Vec::new();
        for eid in 0..count {
            let stored = u32::from_le_bytes(self.col().get(eid).try_into().unwrap());
            if stored != 0 {
                result.push(eid);
            }
        }
        result
    }

    /// value の live 件数 (= pull 結果の件数、正確)。planner の pivot 選択用。
    /// request12 で raw (stale 込み over-count) から live 基準に変更 — churn 後も
    /// 最小スライスを正しく選べる。
    pub fn slice_len(&self, value: u32) -> usize {
        self.ensure_cylinder_built();
        self.cyl.slice_len_live(value)
    }

    /// 入っている値を列挙（順序保証なし、churn 時は stale 含む近似）。
    pub fn unique_values(&self) -> Vec<u32> {
        self.ensure_cylinder_built();
        self.cyl.unique_values()
    }

    /// 総件数 (live 基準、churn があっても正確 — request12)。
    pub fn total(&self) -> usize {
        self.ensure_cylinder_built();
        self.cyl.total_live()
    }

    /// Cylinder の eid backing 総 bytes（メモリ観測用、#95）。
    pub fn cyl_backing_bytes(&self) -> usize {
        self.ensure_cylinder_built();
        self.cyl.backing_bytes()
    }

    pub fn delta_eids(&self) -> &[u32] {
        &[]
    }
    pub fn delta_is_empty(&self) -> bool {
        true
    }
    pub fn delta_needs_rebuild(&self) -> bool {
        false
    }

    pub fn rebuild_cylinder(&self) {}

    pub fn scan(&self, value: u32) -> Vec<u32> {
        let count = self.col().count();
        let target = value + 1;
        let mut result = Vec::new();
        for eid in 0..count {
            let stored = u32::from_le_bytes(self.col().get(eid).try_into().unwrap());
            if stored == target {
                result.push(eid);
            }
        }
        result
    }

    pub fn sync(&self) {}
}
