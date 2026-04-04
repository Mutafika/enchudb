//! HimoStore — 紐1本分のストレージ。
//!
//! Column（紐）+ Cylinder（キャッシュ、ダブルバッファ）。
//! Column がソースオブトゥルース。tie/untie は Column だけ触る。
//! Cylinder は rebuild で Column スキャンから構築。
//! max_values が指定されていれば prefix sum O(1)。

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::cell::UnsafeCell;

use crate::column::Column;
use crate::cylinder::Cylinder;

#[derive(Clone, Copy, PartialEq)]
pub enum HimoType {
    Symbol = 0,
    Value = 1,
}

impl HimoType {
    pub fn from_byte(b: u8) -> Self {
        match b { 0 => Self::Symbol, _ => Self::Value }
    }
}

pub struct HimoStore {
    col: UnsafeCell<Column>,
    cyl_a: Cylinder,
    cyl_b: Cylinder,
    use_b: AtomicBool,
    pub himo_type: HimoType,
    pub max_values: u32, // 0 = 無制限
    pub dirty: AtomicBool,
}

unsafe impl Sync for HimoStore {}
unsafe impl Send for HimoStore {}

impl HimoStore {
    const DEFAULT_MAX_ENTITIES: u32 = 16_777_216; // 16M

    pub fn create(dir: &str, himo_type: HimoType, max_values: u32) -> io::Result<Self> {
        Self::create_with(dir, himo_type, max_values, Self::DEFAULT_MAX_ENTITIES)
    }

    pub fn create_with(dir: &str, himo_type: HimoType, max_values: u32, max_entities: u32) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let col = Column::create_with_max(&format!("{dir}/col.dat"), 4, max_entities)?;
        let cyl_a = Cylinder::create(&format!("{dir}/cyl_a.dat"), max_entities, max_values)?;
        let cyl_b = Cylinder::create(&format!("{dir}/cyl_b.dat"), max_entities, max_values)?;
        Ok(Self {
            col: UnsafeCell::new(col), cyl_a, cyl_b,
            use_b: AtomicBool::new(false),
            himo_type, max_values, dirty: AtomicBool::new(false),
        })
    }

    pub fn open(dir: &str, himo_type: HimoType) -> io::Result<Self> {
        let col = Column::open(&format!("{dir}/col.dat"))?;
        let cyl_a = Cylinder::open(&format!("{dir}/cyl_a.dat"))?.unwrap_or_else(||
            Cylinder::create(&format!("{dir}/cyl_a.dat"), 4_194_304, 0).unwrap());
        let cyl_b = Cylinder::open(&format!("{dir}/cyl_b.dat"))?.unwrap_or_else(||
            Cylinder::create(&format!("{dir}/cyl_b.dat"), 4_194_304, 0).unwrap());
        Ok(Self {
            col: UnsafeCell::new(col), cyl_a, cyl_b,
            use_b: AtomicBool::new(false),
            himo_type, max_values: 0, dirty: AtomicBool::new(true),
        })
    }

    fn col(&self) -> &Column { unsafe { &*self.col.get() } }
    fn col_mut(&self) -> &mut Column { unsafe { &mut *self.col.get() } }

    /// active cylinder。読み側が使う。
    pub fn cylinder(&self) -> &Cylinder {
        if self.use_b.load(Ordering::Acquire) { &self.cyl_b } else { &self.cyl_a }
    }

    /// 紐を張る。Column に書くだけ。O(1)。
    pub fn set(&self, eid: u32, value: u32) {
        if self.col().count() <= eid {
            self.col_mut().write_count(eid + 1);
        }
        self.col().set(eid, &(value + 1).to_le_bytes());
        self.dirty.store(true, Ordering::Release);
    }

    /// 紐を外す。Column をクリアするだけ。O(1)。
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

    /// Column の生バイトを取得（undo 用）。
    pub fn get_raw_bytes(&self, eid: u32) -> [u8; 4] {
        if eid >= self.col().count() { return [0u8; 4]; }
        self.col().get(eid).try_into().unwrap()
    }

    /// Column の生バイトを復元（undo 復旧用）。
    pub fn restore(&self, eid: u32, old_bytes: &[u8; 4]) {
        if self.col().count() <= eid {
            self.col_mut().write_count(eid + 1);
        }
        self.col().set(eid, old_bytes);
        self.dirty.store(true, Ordering::Release);
    }

    /// Column スキャンで Cylinder を再構築。
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

    /// Column スキャンで value に一致する entity を返す。ソート済み。
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

    pub fn flush(&self) -> io::Result<()> {
        self.rebuild_cylinder();
        self.col().flush()?;
        self.cyl_a.flush()?;
        self.cyl_b.flush()?;
        Ok(())
    }
}
