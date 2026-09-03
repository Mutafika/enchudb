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
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::column::Column;
use crate::lockfree_cylinder::LockFreeCylinder;
use crate::region::Region;

/// column region の取得元 (request23 D2)。
///
/// v10 の DB では himo 1 本 = file 1 本なので、 open 時に全 himo の column を作ると
/// 「そのコマンドが触らない himo」 の分まで `open(2)` + `mmap` を払う。 kenning の
/// 実測では 1 コマンドが触る himo は 48 本中 2〜13 本だった。 そこで column を
/// **最初に触ったときに** 組み立てる。
///
/// segment の存在と長さは `SegmentSet::open` が stat で確かめているので、 遅らせても
/// 「欠けた DB を黙って開く」 ことにはならない。
#[cfg(not(target_arch = "wasm32"))]
struct LazyCol {
    set: std::sync::Arc<crate::segments::SegmentSet>,
    kind: crate::segments::SegmentKind,
}

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
    /// `load_lazy` で作った store では最初の `col()` まで空 (request23 D2)。
    /// `init` / `load` は構築時に埋まる。
    col: OnceLock<Column>,
    #[cfg(not(target_arch = "wasm32"))]
    lazy: Option<LazyCol>,
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

/// `col.get(eid)` の 4 byte を stored 形式の u32 で。
///
/// `col()` が (遅延解決のため) atomic load になったので、 **要素ごとに `self.col()` を
/// 呼ぶとループ外に巻き上げられない**。 hot loop は `let col = self.col();` を 1 回だけ
/// 取って、 この free 関数に渡すこと (request23 D2 の計測で sunsu2 phase2_chaos が
/// 82.6s → 91.7s になった原因がこれだった)。
#[inline(always)]
fn stored_at(col: &Column, eid: u32) -> u32 {
    u32::from_le_bytes(col.get(eid).try_into().unwrap())
}

/// `get_value` の col 受け取り版。
#[inline(always)]
fn value_at(col: &Column, eid: u32) -> Option<u32> {
    if eid >= col.count() {
        return None;
    }
    // #106: Acquire load。 writer の `store_u32_release` と対。
    let stored = col.load_u32_acquire(eid);
    if stored == 0 { None } else { Some(stored - 1) }
}

fn ready(col: Column) -> OnceLock<Column> {
    let cell = OnceLock::new();
    let _ = cell.set(col);
    cell
}

// SAFETY: writer は write_lock で直列、 reader は lock-free（Cylinder は epoch +
// Mutex(sparse)）。 Column の read は無同期（既知、 別 issue）。
unsafe impl Sync for HimoStore {}
unsafe impl Send for HimoStore {}

impl HimoStore {
    pub fn init(col_region: Region, ht: ValueType, max_values: u32, max_entities: u32) -> Self {
        let col = Column::init(col_region, 4, max_entities);
        Self {
            col: ready(col),
            #[cfg(not(target_arch = "wasm32"))]
            lazy: None,
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
            col: ready(col),
            #[cfg(not(target_arch = "wasm32"))]
            lazy: None,
            cyl: LockFreeCylinder::new(max_values),
            value_type: ht,
            max_values,
            cyl_built: AtomicBool::new(false),
            write_lock: Mutex::new(()),
        }
    }

    /// `load` の遅延版 (request23 D2) — column region の mmap を最初の read/write
    /// まで遅らせる。 触られなければ segment file は open すらされない。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load_lazy(
        set: std::sync::Arc<crate::segments::SegmentSet>,
        kind: crate::segments::SegmentKind,
        ht: ValueType,
        max_values: u32,
    ) -> Self {
        Self {
            col: OnceLock::new(),
            lazy: Some(LazyCol { set, kind }),
            cyl: LockFreeCylinder::new(max_values),
            value_type: ht,
            max_values,
            cyl_built: AtomicBool::new(false),
            write_lock: Mutex::new(()),
        }
    }

    /// column への参照。 遅延 store では初回だけ segment を mmap する。
    #[inline]
    fn col(&self) -> &Column {
        match self.col.get() {
            Some(c) => c,
            #[cfg(not(target_arch = "wasm32"))]
            None => self.col_slow(),
            #[cfg(target_arch = "wasm32")]
            None => unreachable!("HimoStore column is not initialized"),
        }
    }

    /// 遅延 column の初回組み立て。 競合しても `OnceLock` が 1 本に絞る
    /// (負けた側の `Column::load` は header を読むだけで副作用が無い)。
    #[cold]
    #[cfg(not(target_arch = "wasm32"))]
    fn col_slow(&self) -> &Column {
        let lazy = self
            .lazy
            .as_ref()
            .expect("HimoStore column is neither loaded nor lazy");
        self.col
            .get_or_init(|| Column::load(lazy.set.region(lazy.kind)))
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
            let stored = stored_at(col, eid);
            if stored != 0 {
                self.cyl.insert(eid, stored - 1);
            }
        }
        self.cyl_built.store(true, Ordering::Release);
    }

    // ──── ぶら下げる / 外す ────

    /// cell に値を書く。 **書けなかったら `false`** (#167: growable backing で
    /// commit を伸ばせない = ディスク満杯。 未 commit page に書くと SIGBUS になるので
    /// 書かずに諦める)。 戻り値を無視しても従来どおり動く。
    pub fn set(&self, eid: u32, value: u32) -> bool {
        self.ensure_cylinder_built();
        let _w = self.write_lock.lock();
        let col = self.col();
        if col.ensure_committed_for(eid).is_err() {
            return false;
        }
        col.ensure_count(eid);
        let old = value_at(col, eid);
        if old == Some(value) {
            return true; // 冗長な re-tie = no-op（bucket に dup を作らない）
        }
        let mut stale = None;
        if let Some(o) = old {
            // 値更新: 旧 value の bucket に stale が残る → その bucket の read は verify する。
            // Column を書き換える **前** に flag を立てる (request12、順序契約は note_stale 参照)
            stale = self.cyl.note_stale(o).map(|s| (o, s));
        }
        // #106: Release store。 leaf offset を publish する前に書いた LeafStore slot
        // (payload/gen) を、 offset を Acquire で読む reader が必ず観測できるようにする。
        col.store_u32_release(eid, value + 1);
        self.cyl.insert(eid, value);
        // compaction は Column 更新の **後** (keep = value_eq が新状態を見るため)
        if let Some((o, (len, live))) = stale {
            self.maybe_compact(o, len, live);
        }
        true
    }

    pub fn remove(&self, eid: u32) {
        let col = self.col();
        if eid < col.count() {
            self.ensure_cylinder_built();
            let _w = self.write_lock.lock();
            if let Some(o) = value_at(col, eid) {
                // 削除: 旧 bucket に stale が残る（Cylinder は触らない、verify で落とす）。
                // flag → Column の順 (set と同じ)
                let stale = self.cyl.note_stale(o);
                col.clear(eid);
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
            let col = self.col();
            self.cyl.compact_bucket(value, |eid| stored_at(col, eid) == value + 1);
        }
    }

    /// 全 bucket を Column 基準で即時 compaction する明示 API (運用/テスト用)。
    /// reader は停止しない (bucket ごとの epoch swap)。
    pub fn compact_now(&self) {
        self.ensure_cylinder_built();
        let _w = self.write_lock.lock();
        let col = self.col();
        for v in self.cyl.unique_values() {
            // clean bucket (churn 痕なし) は組み直し不要 — 無条件 swap は巨大 himo で
            // write_lock の長期保持 + 旧 backing の epoch 滞留 (一時 ~2x RSS) を招く
            // (PR #103 レビュー)。write_lock 下なので flag 判定は正確。
            if self.cyl.bucket_needs_verify(v) {
                self.cyl.compact_bucket(v, |eid| stored_at(col, eid) == v + 1);
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
        // #106: Acquire load。 writer の `store_u32_release` と対で、 leaf offset を
        // 掴んだら対応する LeafStore slot の payload/gen も必ず観測できるようにする。
        value_at(self.col(), eid)
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
            out.push(stored_at(col, lid));
        }
    }

    /// eid の現在値が value か（= lazy verify の primitive、Column 直読み）。
    #[inline(always)]
    pub fn value_eq(&self, eid: u32, value: u32) -> bool {
        stored_at(self.col(), eid) == value + 1
    }

    pub fn get_raw_bytes(&self, eid: u32) -> [u8; 4] {
        let col = self.col();
        if eid >= col.count() {
            return [0u8; 4];
        }
        col.get(eid).try_into().unwrap()
    }

    pub fn restore(&self, eid: u32, old_bytes: &[u8; 4]) {
        self.ensure_cylinder_built();
        let _w = self.write_lock.lock();
        // v10: `set` と同じく、 書く cell まで segment の commit を伸ばす (#167: 伸ばせなければ書かない)。
        let col = self.col();
        if col.ensure_committed_for(eid).is_err() {
            return;
        }
        col.ensure_count(eid);
        let stored = u32::from_le_bytes(*old_bytes);
        let old = value_at(col, eid);
        let new = if stored == 0 { None } else { Some(stored - 1) };
        if old == new {
            return; // 同値 restore = no-op (bytes も同一)
        }
        let mut stale = None;
        if let Some(o) = old {
            // flag → Column の順 (set と同じ、request12)
            stale = self.cyl.note_stale(o).map(|s| (o, s));
        }
        col.set(eid, old_bytes);
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
            let col = self.col();
            let mut out: Vec<u32> = raw
                .into_iter()
                .filter(|&eid| stored_at(col, eid) == value + 1)
                .collect();
            out.sort_unstable();
            out.dedup();
            out
        }
    }

    /// 値が tie された全 entity（= column 非ゼロ走査）。O(next_eid) で重い。
    pub fn entities_with_value(&self) -> Vec<u32> {
        let col = self.col();
        let count = col.count();
        let mut result = Vec::new();
        for eid in 0..count {
            let stored = stored_at(col, eid);
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

    /// #255: cylinder を**組まずに**、 column の設定済み cell の値を `f` に渡す (eid 順、
    /// 重複あり)。 `ensure_cylinder_built` と同じ走査を cylinder insert 抜きで行うので、
    /// 集めた集合は `unique_values()` と一致する。 writer open 時の LeafStore free-list
    /// 再構成用 — 以前は `unique_values()` 経由で leaf を持つ全 himo の index を eager に
    /// 組んでいた (117 himo / 9211 entity の DB で open +150 ms / drop +100 ms)。
    ///
    /// 走査は非 atomic な raw view (`values_u32`) なので、 **書き込みと並走しない場面**
    /// (open 直後) でだけ使うこと。
    pub fn for_each_set_value(&self, mut f: impl FnMut(u32)) {
        let col = self.col();
        for &stored in col.values_u32() {
            if stored != 0 {
                f(stored - 1);
            }
        }
    }

    /// cylinder (in-memory index) が組まれているか。 観測用 (#255 の gate)。
    pub fn cylinder_built(&self) -> bool {
        self.cyl_built.load(Ordering::Acquire)
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
        let col = self.col();
        let count = col.count();
        let target = value + 1;
        let mut result = Vec::new();
        for eid in 0..count {
            let stored = stored_at(col, eid);
            if stored == target {
                result.push(eid);
            }
        }
        result
    }

    pub fn sync(&self) {}
}
