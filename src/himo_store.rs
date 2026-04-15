//! HimoStore — 紐1本分のストレージ。
//!
//! Column（ソースオブトゥルース）+ Cylinder（検索キャッシュ）+ Delta（差分）。
//!
//! ぶら下げる → Column に書く + delta に eid 追記
//! 引く       → Cylinder 結果 + delta 分を Column 直読みで補正
//! rebuild    → delta を Cylinder にマージ（delta が大きくなった時だけ）

#[cfg(not(feature = "v27"))]
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::cell::UnsafeCell;

use crate::column::Column;
#[cfg(not(feature = "v27"))]
use crate::cylinder::Cylinder;
#[cfg(feature = "v27")]
use crate::cylinder_v27::BucketCylinder;
use crate::region::Region;

#[derive(Clone, Copy, PartialEq)]
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

#[cfg(not(feature = "v27"))]
const BITMAP_BUDGET: usize = 32 * 1024 * 1024;
#[cfg(not(feature = "v27"))]
const DELTA_CAP: usize = 4096;

#[cfg(not(feature = "v27"))]
pub struct HimoStore {
    col: UnsafeCell<Column>,
    cyl_a: Cylinder,
    cyl_b: Cylinder,
    use_b: AtomicBool,
    rebuilding: AtomicBool,
    pub himo_type: HimoType,
    pub max_values: u32,
    pub dirty: AtomicBool,
    bitmaps: UnsafeCell<Option<Vec<Vec<u64>>>>,
    bitmap_words: usize,
    // delta: Cylinder に反映されていない変更の eid 一覧
    delta_eids: UnsafeCell<Vec<u32>>,
    delta_len: AtomicU32,
}

#[cfg(not(feature = "v27"))]
unsafe impl Sync for HimoStore {}
#[cfg(not(feature = "v27"))]
unsafe impl Send for HimoStore {}

#[cfg(not(feature = "v27"))]
impl HimoStore {
    fn compute_bitmap_words(max_entities: u32) -> usize {
        (max_entities as usize + 63) / 64
    }

    fn should_build_bitmaps(max_values: u32, bitmap_words: usize) -> bool {
        if max_values == 0 { return false; }
        let total_bytes = (max_values as usize + 1) * bitmap_words * 8;
        total_bytes <= BITMAP_BUDGET
    }

    fn new_delta() -> Vec<u32> {
        let mut v = Vec::with_capacity(DELTA_CAP);
        v.resize(DELTA_CAP, 0);
        v
    }

    pub fn init(col_region: Region, cyl_a_region: Region, cyl_b_region: Region,
                ht: HimoType, max_values: u32, max_entities: u32) -> Self {
        let col = Column::init(col_region, 4, max_entities);
        let cyl_a = Cylinder::init(cyl_a_region, max_entities, max_values);
        let cyl_b = Cylinder::init(cyl_b_region, max_entities, max_values);
        let bitmap_words = Self::compute_bitmap_words(max_entities);
        Self {
            col: UnsafeCell::new(col), cyl_a, cyl_b,
            use_b: AtomicBool::new(false), rebuilding: AtomicBool::new(false),
            himo_type: ht, max_values, dirty: AtomicBool::new(false),
            bitmaps: UnsafeCell::new(None), bitmap_words,
            delta_eids: UnsafeCell::new(Self::new_delta()),
            delta_len: AtomicU32::new(0),
        }
    }

    pub fn load(col_region: Region, cyl_a_region: Region, cyl_b_region: Region,
                ht: HimoType, max_values: u32) -> Self {
        let col = Column::load(col_region);
        let max_entities = col.max_entities;
        let cyl_a = Cylinder::load(cyl_a_region);
        let cyl_b = Cylinder::load(cyl_b_region);
        let bitmap_words = Self::compute_bitmap_words(max_entities);
        Self {
            col: UnsafeCell::new(col), cyl_a, cyl_b,
            use_b: AtomicBool::new(false), rebuilding: AtomicBool::new(false),
            himo_type: ht, max_values, dirty: AtomicBool::new(true),
            bitmaps: UnsafeCell::new(None), bitmap_words,
            delta_eids: UnsafeCell::new(Self::new_delta()),
            delta_len: AtomicU32::new(0),
        }
    }

    fn col(&self) -> &Column { unsafe { &*self.col.get() } }

    pub fn cylinder(&self) -> &Cylinder {
        if self.use_b.load(Ordering::Acquire) { &self.cyl_b } else { &self.cyl_a }
    }

    // ──── ぶら下げる / 外す ────

    pub fn set(&self, eid: u32, value: u32) {
        self.col().ensure_count(eid);
        self.col().set(eid, &(value + 1).to_le_bytes());
        self.delta_push(eid);
        self.dirty.store(true, Ordering::Release);
    }

    pub fn remove(&self, eid: u32) {
        if eid < self.col().count() {
            self.col().clear(eid);
            self.delta_push(eid);
            self.dirty.store(true, Ordering::Release);
        }
    }

    /// delta に eid を追記。CASでスロット確保→データ書き込み。
    fn delta_push(&self, eid: u32) {
        let delta = unsafe { &mut *self.delta_eids.get() };
        loop {
            let idx = self.delta_len.load(Ordering::Relaxed) as usize;
            if idx >= DELTA_CAP {
                // delta 溢れ → rebuild で吸収（次の引く時に実行される）
                return;
            }
            // CASでスロット確保
            if self.delta_len.compare_exchange(
                idx as u32, (idx + 1) as u32,
                Ordering::AcqRel, Ordering::Relaxed
            ).is_ok() {
                // 確保成功 → データ書き込み
                delta[idx] = eid;
                std::sync::atomic::fence(Ordering::Release);
                return;
            }
            // 失敗 → 別スレッドが先にスロット取った → リトライ
        }
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
        self.col().set(eid, old_bytes);
        self.delta_push(eid);
        self.dirty.store(true, Ordering::Release);
    }

    // ──── 引く（Cylinder + delta 補正）────

    /// 引く。Cylinder のスナップショット + delta 差分で補正。
    pub fn pull(&self, value: u32) -> &[u32] {
        let dlen = self.delta_len.load(Ordering::Acquire) as usize;
        if dlen == 0 {
            // delta 空 → Cylinder そのまま（ゼロコピー）
            return self.cylinder().slice_one(value);
        }
        if dlen > DELTA_CAP {
            // delta 溢れ → rebuild 必要（呼び出し元の Engine が判断）
            // ここでは Cylinder をそのまま返す（古い可能性あり）
            return self.cylinder().slice_one(value);
        }
        // delta が小さい場合も、slice_one は参照を返すだけなので
        // 補正は Engine 側で行う（HimoStore は &[u32] しか返せない）
        self.cylinder().slice_one(value)
    }

    /// delta の eid 一覧を返す（Engine が補正に使う）。
    pub fn delta_eids(&self) -> &[u32] {
        let dlen = (self.delta_len.load(Ordering::Acquire) as usize).min(DELTA_CAP);
        std::sync::atomic::fence(Ordering::Acquire); // delta_push の Release と対
        let delta = unsafe { &*self.delta_eids.get() };
        &delta[..dlen]
    }

    /// delta が空か。
    pub fn delta_is_empty(&self) -> bool {
        self.delta_len.load(Ordering::Acquire) == 0
    }

    /// delta が満杯で rebuild が必要か。
    pub fn delta_needs_rebuild(&self) -> bool {
        self.delta_len.load(Ordering::Acquire) as usize >= DELTA_CAP
    }

    // ──── bitmap ────

    pub fn has_bitmaps(&self) -> bool {
        unsafe { (*self.bitmaps.get()).is_some() }
    }

    pub fn bitmap(&self, value: u32) -> Option<&[u64]> {
        unsafe {
            let bms = &*self.bitmaps.get();
            bms.as_ref().and_then(|v| v.get(value as usize).map(|b| b.as_slice()))
        }
    }

    pub fn bitmap_words(&self) -> usize { self.bitmap_words }

    // ──── rebuild ────

    pub fn rebuild_cylinder(&self) {
        if !self.dirty.load(Ordering::Acquire) { return; }
        if self.rebuilding.compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed).is_err() {
            return;
        }
        if !self.dirty.load(Ordering::Acquire) {
            self.rebuilding.store(false, Ordering::Release);
            return;
        }

        let count = self.col().count();
        let mut pairs = Vec::new();
        for eid in 0..count {
            let stored = u32::from_le_bytes(self.col().get(eid).try_into().unwrap());
            if stored != 0 {
                pairs.push((stored - 1, eid));
            }
        }

        if Self::should_build_bitmaps(self.max_values, self.bitmap_words) {
            let slots = self.max_values as usize + 1;
            let mut bms = vec![vec![0u64; self.bitmap_words]; slots];
            for &(val, eid) in &pairs {
                if (val as usize) < slots {
                    bms[val as usize][eid as usize / 64] |= 1u64 << (eid % 64);
                }
            }
            unsafe { *self.bitmaps.get() = Some(bms); }
        }

        let standby = if self.use_b.load(Ordering::Acquire) { &self.cyl_a } else { &self.cyl_b };
        standby.rebuild(pairs);
        self.use_b.fetch_xor(true, Ordering::Release);

        // delta クリア
        self.delta_len.store(0, Ordering::Release);
        self.dirty.store(false, Ordering::Release);
        self.rebuilding.store(false, Ordering::Release);
    }

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

    pub fn sync(&self) {
        self.rebuild_cylinder();
    }
}

// ════════════════ v27 実装 ════════════════
//
// BucketCylinder を直接積む。ダブルバッファ/delta/bitmap は全廃。
// 注意: BucketCylinder::insert/remove は &mut self。単一スレッド前提。
// Engine を Arc 共有する並行書き込みは v27 v1 では未サポート。

#[cfg(feature = "v27")]
pub struct HimoStore {
    col: UnsafeCell<Column>,
    cyl: UnsafeCell<BucketCylinder>,
    pub himo_type: HimoType,
    pub max_values: u32,
}

#[cfg(feature = "v27")]
unsafe impl Sync for HimoStore {}
#[cfg(feature = "v27")]
unsafe impl Send for HimoStore {}

#[cfg(feature = "v27")]
impl HimoStore {
    pub fn init(col_region: Region, ht: HimoType, max_values: u32, max_entities: u32) -> Self {
        let col = Column::init(col_region, 4, max_entities);
        let effective_max = if max_values == 0 { 65_536 } else { max_values };
        let cyl = BucketCylinder::new(effective_max, max_entities);
        Self {
            col: UnsafeCell::new(col),
            cyl: UnsafeCell::new(cyl),
            himo_type: ht,
            max_values,
        }
    }

    pub fn load(col_region: Region, ht: HimoType, max_values: u32) -> Self {
        let col = Column::load(col_region);
        let max_entities = col.max_entities;
        let effective_max = if max_values == 0 { 65_536 } else { max_values };
        let mut cyl = BucketCylinder::new(effective_max, max_entities);
        cyl.rebuild_from_column(&col);
        Self {
            col: UnsafeCell::new(col),
            cyl: UnsafeCell::new(cyl),
            himo_type: ht,
            max_values,
        }
    }

    fn col(&self) -> &Column { unsafe { &*self.col.get() } }

    pub fn cylinder(&self) -> &BucketCylinder { unsafe { &*self.cyl.get() } }

    fn cyl_mut(&self) -> &mut BucketCylinder { unsafe { &mut *self.cyl.get() } }

    // ──── ぶら下げる / 外す ────

    pub fn set(&self, eid: u32, value: u32) {
        self.col().ensure_count(eid);
        self.col().set(eid, &(value + 1).to_le_bytes());
        self.cyl_mut().insert(eid, value);
    }

    pub fn remove(&self, eid: u32) {
        if eid < self.col().count() {
            self.col().clear(eid);
            self.cyl_mut().remove(eid);
        }
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
        if stored == 0 {
            self.cyl_mut().remove(eid);
        } else {
            self.cyl_mut().insert(eid, stored - 1);
        }
    }

    // ──── 引く ────

    pub fn pull(&self, value: u32) -> &[u32] {
        self.cylinder().slice_one(value)
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
