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

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

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
//   LockFreeCylinder は単一 writer（consumer）/ 多 reader。
//   - set/remove/restore は consumer thread（or 同期 tie の &mut self）が呼ぶ = 単一 writer
//   - pull/slice_len は lock-free read（epoch）。 read は write を一切待たない
//   Column は UnsafeCell のまま（Column::set/clear は単一 writer 前提、write path は
//   consumer 1 本 + 同期 tie が &mut self なので競合しない。 pull の verify は Column を
//   直読みするが、これは #95 とは別の既知の無同期読みで別 issue 候補）。

pub struct HimoStore {
    col: UnsafeCell<Column>,
    cyl: LockFreeCylinder,
    pub value_type: ValueType,
    /// 初期 bucket サイズのヒント。0 は「ヒントなし、必要時に拡張」。値の上限ではない。
    pub max_values: u32,
    /// cylinder が column から populate 済みか。lazy rebuild 用。
    /// `init`（新規 DB）では即 true（column 空）、`load`（既存 DB）では false で開始。
    cyl_built: AtomicBool,
    /// lazy build を単一 thread に絞る guard（build は多数 insert = 単一 writer 必須）。
    /// build 完了後は cyl_built の fast path で触られない。
    build_lock: Mutex<()>,
}

// SAFETY: 単一 writer / 多 reader。Cylinder は epoch + Mutex(sparse)、Column は上記の
// 単一 writer 契約で保護。
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
            build_lock: Mutex::new(()),
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
            build_lock: Mutex::new(()),
        }
    }

    fn col(&self) -> &Column {
        unsafe { &*self.col.get() }
    }

    /// cylinder が未 build なら column から rebuild。lazy build の入口。
    /// fast path は AtomicBool::load で即 return。
    ///
    /// #95: build は多数 insert（単一 writer 必須）なので `build_lock` で排他する。
    /// build 中の他 thread は build_lock を待つ（one-time、load 直後の初回のみ）。
    #[inline]
    fn ensure_cylinder_built(&self) {
        if self.cyl_built.load(Ordering::Acquire) {
            return;
        }
        let _g = self.build_lock.lock().unwrap();
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
        self.col().ensure_count(eid);
        let old = self.get_value(eid);
        if old == Some(value) {
            return; // 冗長な re-tie = no-op（bucket に dup を作らない）
        }
        if old.is_some() {
            // 値更新: 旧 value の bucket に stale が残る → 以降 read は verify する
            self.cyl.mark_removed();
        }
        self.col().set(eid, &(value + 1).to_le_bytes());
        self.cyl.insert(eid, value);
    }

    pub fn remove(&self, eid: u32) {
        if eid < self.col().count() {
            self.ensure_cylinder_built();
            let had = self.get_value(eid).is_some();
            self.col().clear(eid);
            if had {
                // 削除: 旧 bucket に stale が残る（Cylinder は触らない、verify で落とす）
                self.cyl.mark_removed();
            }
        }
    }

    /// 現在の unique 値数。append-only では正確、churn 時は over-count（#95、compaction 未実装）。
    pub fn unique_count(&self) -> u32 {
        self.ensure_cylinder_built();
        self.cyl.unique_count()
    }

    // ──── 読む（Column 直読み、Cylinder 非依存）────

    pub fn get_value(&self, eid: u32) -> Option<u32> {
        if eid >= self.col().count() {
            return None;
        }
        let stored = u32::from_le_bytes(self.col().get(eid).try_into().unwrap());
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
        self.col().ensure_count(eid);
        let stored = u32::from_le_bytes(*old_bytes);
        let old = self.get_value(eid);
        self.col().set(eid, old_bytes);
        if stored == 0 {
            if old.is_some() {
                self.cyl.mark_removed();
            }
        } else {
            let new_val = stored - 1;
            if old != Some(new_val) {
                if old.is_some() {
                    self.cyl.mark_removed();
                }
                self.cyl.insert(eid, new_val);
            }
        }
    }

    // ──── 引く ────

    /// 値に合致する entity。#95: Cylinder の raw を、削除/更新があった himo でのみ
    /// Column verify + dedup で filter（append-only himo なら raw 直返し = fast path）。
    pub fn pull(&self, value: u32) -> Vec<u32> {
        self.ensure_cylinder_built();
        let raw = self.cyl.read_to_vec(value);
        if !self.cyl.any_removed() {
            // append-only: 全 live、dup なし
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

    /// bucket 長（raw、stale 込み）。planner の pivot 選択用 heuristic（over-count 可）。
    pub fn slice_len(&self, value: u32) -> usize {
        self.ensure_cylinder_built();
        self.cyl.slice_len(value)
    }

    /// 入っている値を列挙（順序保証なし、churn 時は stale 含む近似）。
    pub fn unique_values(&self) -> Vec<u32> {
        self.ensure_cylinder_built();
        self.cyl.unique_values()
    }

    /// 総件数（append-only では正確、churn 時は over-count）。
    pub fn total(&self) -> usize {
        self.ensure_cylinder_built();
        self.cyl.total()
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
