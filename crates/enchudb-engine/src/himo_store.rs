//! HimoStore — 紐1本分のストレージ。
//!
//! Column（ソースオブトゥルース）+ Cylinder（検索キャッシュ）+ Delta（差分）。
//!
//! ぶら下げる → Column に書く + delta に eid 追記
//! 引く       → Cylinder 結果 + delta 分を Column 直読みで補正
//! rebuild    → delta を Cylinder にマージ（delta が大きくなった時だけ）

use std::sync::atomic::{AtomicU32, Ordering};
use std::cell::UnsafeCell;

use crate::column::Column;
use crate::cylinder_v27::BucketCylinder;
use crate::region::Region;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum HimoType {
    Symbol = 0,
    Value = 1,
    Ref = 2,
}

impl HimoType {
    pub fn from_byte(b: u8) -> Self {
        match b { 0 => Self::Symbol, 2 => Self::Ref, _ => Self::Value }
    }
}

// BucketCylinder を直接積む。
//
// 並行性:
//   BucketCylinder は RwLock で保護。
//   - set/remove/restore は write lock を取る
//   - pull/slice_one は read lock を取り、Vec<u32> を clone して返す(slice 返しは不可)
//   Engine を Arc<Engine> で複数 thread から共有可能(tie_async + 並行 reader)。
//   Column は UnsafeCell のまま(atomic-size = 4byte、Column::set/clear は単一 writer
//   を仮定しているが、write path が consumer thread 1 本 + 同期 tie が &mut self
//   なので競合しない)。

use std::sync::RwLock;

pub struct HimoStore {
    col: UnsafeCell<Column>,
    cyl: RwLock<BucketCylinder>,
    pub himo_type: HimoType,
    /// 初期 bucket サイズのヒント。0 は「ヒントなし、必要時に拡張」。
    /// 実際の値制限ではない。value > max_values でも tie 可能(BucketCylinder が動的拡張)。
    pub max_values: u32,
    /// 現在の unique 値数(非空バケット数)。`himo_cardinality` API が参照する。
    unique_count: AtomicU32,
}

unsafe impl Sync for HimoStore {}
unsafe impl Send for HimoStore {}

impl HimoStore {
    pub fn init(col_region: Region, ht: HimoType, max_values: u32, max_entities: u32) -> Self {
        let col = Column::init(col_region, 4, max_entities);
        // max_values == 0 は「ヒントなし」。bucket を空で作って必要時に拡張。
        let cyl = BucketCylinder::new(max_values, max_entities);
        Self {
            col: UnsafeCell::new(col),
            cyl: RwLock::new(cyl),
            himo_type: ht,
            max_values,
            unique_count: AtomicU32::new(0),
        }
    }

    pub fn load(col_region: Region, ht: HimoType, max_values: u32) -> Self {
        let col = Column::load(col_region);
        let max_entities = col.max_entities;
        let mut cyl = BucketCylinder::new(max_values, max_entities);
        cyl.rebuild_from_column(&col);
        let uniq = cyl.unique_count();
        Self {
            col: UnsafeCell::new(col),
            cyl: RwLock::new(cyl),
            himo_type: ht,
            max_values,
            unique_count: AtomicU32::new(uniq),
        }
    }

    fn col(&self) -> &Column { unsafe { &*self.col.get() } }

    // ──── ぶら下げる / 外す ────

    pub fn set(&self, eid: u32, value: u32) {
        self.col().ensure_count(eid);
        self.col().set(eid, &(value + 1).to_le_bytes());
        let delta = self.cyl.write().unwrap().insert(eid, value);
        self.apply_unique_delta(delta);
    }

    pub fn remove(&self, eid: u32) {
        if eid < self.col().count() {
            self.col().clear(eid);
            let delta = self.cyl.write().unwrap().remove(eid);
            self.apply_unique_delta(delta);
        }
    }

    #[inline]
    fn apply_unique_delta(&self, delta: crate::cylinder_v27::UniqueDelta) {
        if delta.added > 0 {
            self.unique_count.fetch_add(delta.added, Ordering::Relaxed);
        }
        if delta.removed > 0 {
            self.unique_count.fetch_sub(delta.removed, Ordering::Relaxed);
        }
    }

    /// 現在の unique 値数(非空バケット数)。O(1)。
    pub fn unique_count(&self) -> u32 {
        self.unique_count.load(Ordering::Relaxed)
    }

    // ──── 読む ────

    pub fn get_value(&self, eid: u32) -> Option<u32> {
        if eid >= self.col().count() { return None; }
        let stored = u32::from_le_bytes(self.col().get(eid).try_into().unwrap());
        if stored == 0 { None } else { Some(stored - 1) }
    }

    #[inline(always)]
    pub fn value_eq(&self, eid: u32, value: u32) -> bool {
        u32::from_le_bytes(self.col().get(eid).try_into().unwrap()) == value + 1
    }

    pub fn get_raw_bytes(&self, eid: u32) -> [u8; 4] {
        if eid >= self.col().count() { return [0u8; 4]; }
        self.col().get(eid).try_into().unwrap()
    }

    pub fn restore(&self, eid: u32, old_bytes: &[u8; 4]) {
        self.col().ensure_count(eid);
        let stored = u32::from_le_bytes(*old_bytes);
        self.col().set(eid, old_bytes);
        let delta = {
            let mut cyl = self.cyl.write().unwrap();
            if stored == 0 {
                cyl.remove(eid)
            } else {
                cyl.insert(eid, stored - 1)
            }
        };
        self.apply_unique_delta(delta);
    }

    // ──── 引く ────

    /// 値に合致する entity を Vec<u32> で返す(read lock 下でクローン)。
    pub fn pull(&self, value: u32) -> Vec<u32> {
        self.cyl.read().unwrap().slice_one(value).to_vec()
    }

    /// 同上だが長さだけ。プランナー用。
    pub fn slice_len(&self, value: u32) -> usize {
        self.cyl.read().unwrap().slice_one(value).len()
    }

    /// 入っている値を列挙(順序は保証しない)。
    pub fn unique_values(&self) -> Vec<u32> {
        self.cyl.read().unwrap().unique_values()
    }

    /// 総件数。
    pub fn total(&self) -> usize {
        self.cyl.read().unwrap().total()
    }

    pub fn delta_eids(&self) -> &[u32] { &[] }
    pub fn delta_is_empty(&self) -> bool { true }
    pub fn delta_needs_rebuild(&self) -> bool { false }

    pub fn has_bitmaps(&self) -> bool { false }
    pub fn bitmap(&self, _value: u32) -> Option<&[u64]> { None }
    pub fn bitmap_words(&self) -> usize { 0 }

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
