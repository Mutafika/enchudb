//! HimoStore — 紐1本分のストレージ。
//!
//! Column（紐）+ Cylinder（キャッシュ、ダブルバッファ）。
//! Column がソースオブトゥルース。tie/untie は Column だけ触る。
//! Cylinder は rebuild で Column スキャンから構築。
//! max_values が指定されていれば prefix sum O(1)。

use std::sync::atomic::{AtomicBool, Ordering};
use std::cell::UnsafeCell;

use crate::column::Column;
use crate::cylinder::Cylinder;
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

pub struct HimoStore {
    col: UnsafeCell<Column>,
    cyl_a: Cylinder,
    cyl_b: Cylinder,
    use_b: AtomicBool,
    pub himo_type: HimoType,
    pub max_values: u32,
    pub dirty: AtomicBool,
}

unsafe impl Sync for HimoStore {}
unsafe impl Send for HimoStore {}

impl HimoStore {
    /// 新規Region群から初期化。
    pub fn init(col_region: Region, cyl_a_region: Region, cyl_b_region: Region,
                ht: HimoType, max_values: u32, max_entities: u32) -> Self {
        let col = Column::init(col_region, 4, max_entities);
        let cyl_a = Cylinder::init(cyl_a_region, max_entities, max_values);
        let cyl_b = Cylinder::init(cyl_b_region, max_entities, max_values);
        Self {
            col: UnsafeCell::new(col), cyl_a, cyl_b,
            use_b: AtomicBool::new(false),
            himo_type: ht, max_values, dirty: AtomicBool::new(false),
        }
    }

    /// 既存Region群からロード。
    pub fn load(col_region: Region, cyl_a_region: Region, cyl_b_region: Region,
                ht: HimoType, max_values: u32) -> Self {
        let col = Column::load(col_region);
        let cyl_a = Cylinder::load(cyl_a_region);
        let cyl_b = Cylinder::load(cyl_b_region);
        Self {
            col: UnsafeCell::new(col), cyl_a, cyl_b,
            use_b: AtomicBool::new(false),
            himo_type: ht, max_values, dirty: AtomicBool::new(true),
        }
    }

    fn col(&self) -> &Column { unsafe { &*self.col.get() } }
    fn col_mut(&self) -> &mut Column { unsafe { &mut *self.col.get() } }

    pub fn cylinder(&self) -> &Cylinder {
        if self.use_b.load(Ordering::Acquire) { &self.cyl_b } else { &self.cyl_a }
    }

    pub fn set(&self, eid: u32, value: u32) {
        if self.col().count() <= eid {
            self.col_mut().write_count(eid + 1);
        }
        self.col().set(eid, &(value + 1).to_le_bytes());
        self.dirty.store(true, Ordering::Release);
    }

    pub fn remove(&self, eid: u32) {
        if eid < self.col().count() {
            self.col().clear(eid);
            self.dirty.store(true, Ordering::Release);
        }
    }

    pub fn get_value(&self, eid: u32) -> Option<u32> {
        if eid >= self.col().count() { return None; }
        let stored = u32::from_le_bytes(self.col().get(eid).try_into().unwrap());
        if stored == 0 { None } else { Some(stored - 1) }
    }

    /// Column直読み。Cylinderから来たeidに対して使う（bounds checkなし）。
    #[inline(always)]
    pub fn value_eq(&self, eid: u32, value: u32) -> bool {
        u32::from_le_bytes(self.col().get(eid).try_into().unwrap()) == value + 1
    }

    pub fn get_raw_bytes(&self, eid: u32) -> [u8; 4] {
        if eid >= self.col().count() { return [0u8; 4]; }
        self.col().get(eid).try_into().unwrap()
    }

    pub fn restore(&self, eid: u32, old_bytes: &[u8; 4]) {
        if self.col().count() <= eid {
            self.col_mut().write_count(eid + 1);
        }
        self.col().set(eid, old_bytes);
        self.dirty.store(true, Ordering::Release);
    }

    pub fn rebuild_cylinder(&self) {
        if !self.dirty.load(Ordering::Acquire) { return; }
        let count = self.col().count();
        let mut pairs = Vec::new();
        for eid in 0..count {
            let stored = u32::from_le_bytes(self.col().get(eid).try_into().unwrap());
            if stored != 0 {
                pairs.push((stored - 1, eid));
            }
        }
        let standby = if self.use_b.load(Ordering::Acquire) { &self.cyl_a } else { &self.cyl_b };
        standby.rebuild(pairs);
        self.use_b.fetch_xor(true, Ordering::Release);
        self.dirty.store(false, Ordering::Release);
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

    /// Cylinder をリビルド（Engine::flush から呼ばれる）。
    pub fn sync(&self) {
        self.rebuild_cylinder();
    }
}
