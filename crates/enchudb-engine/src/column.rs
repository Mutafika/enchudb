//! Column — 固定長カラム。Region経由で単一mmapの一部を使う。
//!
//! remap なし。ロックなし。ensure_capacity なし。
//! 仮想アドレス空間だけ確保、物理メモリは書いたページ分だけ。

use std::sync::atomic::{AtomicU32, Ordering};
use crate::region::Region;

const HEADER: usize = 16;

pub struct Column {
    region: Region,
    count: AtomicU32,
    pub(crate) value_size: u32,
    pub(crate) max_entities: u32,
}

unsafe impl Sync for Column {}
unsafe impl Send for Column {}

impl Column {
    pub fn init(region: Region, value_size: u32, max_entities: u32) -> Self {
        let mm = region.slice_mut();
        mm[0..4].copy_from_slice(&0u32.to_le_bytes());
        mm[4..8].copy_from_slice(&value_size.to_le_bytes());
        mm[8..12].copy_from_slice(&max_entities.to_le_bytes());
        Self { region, count: AtomicU32::new(0), value_size, max_entities }
    }

    pub fn load(region: Region) -> Self {
        let mm = region.slice();
        let count = u32::from_le_bytes(mm[0..4].try_into().unwrap());
        let value_size = u32::from_le_bytes(mm[4..8].try_into().unwrap());
        let max_entities = u32::from_le_bytes(mm[8..12].try_into().unwrap());
        Self { region, count: AtomicU32::new(count), value_size, max_entities }
    }

    pub fn region_size(max_entities: u32, value_size: u32) -> usize {
        HEADER + (max_entities as usize) * (value_size as usize)
    }

    #[inline]
    pub fn set(&self, entity_id: u32, value: &[u8]) {
        let vs = self.value_size as usize;
        let off = HEADER + (entity_id as usize) * vs;
        let len = value.len().min(vs);
        let mm = self.region.slice_mut();
        mm[off..off + len].copy_from_slice(&value[..len]);
        self.region.mark_dirty(off, len);
    }

    #[inline]
    pub fn get(&self, entity_id: u32) -> &[u8] {
        let vs = self.value_size as usize;
        let off = HEADER + (entity_id as usize) * vs;
        let mm = self.region.slice();
        &mm[off..off + vs]
    }

    #[inline]
    pub fn clear(&self, entity_id: u32) {
        let vs = self.value_size as usize;
        let off = HEADER + (entity_id as usize) * vs;
        let mm = self.region.slice_mut();
        for b in &mut mm[off..off + vs] { *b = 0; }
        self.region.mark_dirty(off, vs);
    }

    pub fn count(&self) -> u32 { self.count.load(Ordering::Relaxed) }

    /// count を eid+1 まで引き上げる（atomic、並列安全）。
    pub fn ensure_count(&self, eid: u32) {
        let needed = eid + 1;
        self.count.fetch_max(needed, Ordering::Relaxed);
        // mmapヘッダにも書く
        let mm = self.region.slice_mut();
        // ヘッダの count は rebuild/flush 時に確定するので、ここでは最大値を書く
        let current = u32::from_le_bytes(mm[0..4].try_into().unwrap());
        if needed > current {
            mm[0..4].copy_from_slice(&needed.to_le_bytes());
        }
    }

    /// flush用: 現在のcountをmmapヘッダに書き出す。
    pub fn sync_count(&self) {
        let c = self.count.load(Ordering::Relaxed);
        let mm = self.region.slice_mut();
        mm[0..4].copy_from_slice(&c.to_le_bytes());
    }
}
