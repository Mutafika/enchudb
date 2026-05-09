//! Undo ログ — 書き込み前の値を記録。COMMIT でクリア、クラッシュ時に自動復旧。
//!
//! Layout (Region内):
//!   Header (16B): [magic:4][count:u32][reserved:8]
//!   Entries (10B each): [eid:u32][dim_id:u16][old_value:4B]

use std::sync::atomic::{AtomicU32, Ordering};
use crate::region::Region;

const MAGIC: [u8; 4] = [b'U', b'N', b'D', b'1'];
const HEADER: usize = 16;
const ENTRY_SIZE: usize = 10;
pub(crate) const DEFAULT_MAX_ENTRIES: u32 = 16_777_216;

pub struct UndoLog {
    region: Region,
    count: AtomicU32,
}

unsafe impl Sync for UndoLog {}
unsafe impl Send for UndoLog {}

impl UndoLog {
    pub fn region_size() -> usize {
        Self::region_size_with(DEFAULT_MAX_ENTRIES)
    }

    /// Tunable variant — small-footprint engines (state-log presets,
    /// embedded apps) pass a much smaller `max_entries` so the undo
    /// region doesn't dominate the layout. The default 16 M entries
    /// × 10 bytes = 160 MB which is the bulk of `create_compact`'s
    /// 305 MB apparent size.
    pub fn region_size_with(max_entries: u32) -> usize {
        HEADER + max_entries as usize * ENTRY_SIZE
    }

    /// 新規領域を初期化。
    pub fn init(region: Region) -> Self {
        let mm = region.slice_mut();
        mm[0..4].copy_from_slice(&MAGIC);
        mm[4..8].copy_from_slice(&0u32.to_le_bytes());
        Self { region, count: AtomicU32::new(0) }
    }

    /// 既存領域をロード。
    pub fn load(region: Region) -> Self {
        let mm = region.slice();
        let count = if mm.len() >= HEADER && mm[0..4] == MAGIC {
            u32::from_le_bytes(mm[4..8].try_into().unwrap())
        } else {
            // 初期化されていない場合はクリア状態で返す
            let mm = region.slice_mut();
            mm[0..4].copy_from_slice(&MAGIC);
            mm[4..8].copy_from_slice(&0u32.to_le_bytes());
            0
        };
        Self { region, count: AtomicU32::new(count) }
    }

    #[inline]
    pub fn record(&self, eid: u32, dim_id: u16, old_value: &[u8; 4]) {
        let idx = self.count.fetch_add(1, Ordering::Relaxed);
        let mm = self.region.slice_mut();
        let off = HEADER + idx as usize * ENTRY_SIZE;
        mm[off..off + 4].copy_from_slice(&eid.to_le_bytes());
        mm[off + 4..off + 6].copy_from_slice(&dim_id.to_le_bytes());
        mm[off + 6..off + 10].copy_from_slice(old_value);
        mm[4..8].copy_from_slice(&(idx + 1).to_le_bytes());
    }

    #[inline]
    pub fn commit(&self) {
        self.count.store(0, Ordering::Release);
        let mm = self.region.slice_mut();
        mm[4..8].copy_from_slice(&0u32.to_le_bytes());
    }

    pub fn pending_count(&self) -> u32 {
        self.count.load(Ordering::Acquire)
    }

    pub fn entries_reverse(&self) -> Vec<(u32, u16, [u8; 4])> {
        let count = self.count.load(Ordering::Acquire) as usize;
        let mm = self.region.slice();
        let mut result = Vec::with_capacity(count);
        for i in (0..count).rev() {
            let off = HEADER + i * ENTRY_SIZE;
            let eid = u32::from_le_bytes(mm[off..off + 4].try_into().unwrap());
            let dim_id = u16::from_le_bytes(mm[off + 4..off + 6].try_into().unwrap());
            let mut old_value = [0u8; 4];
            old_value.copy_from_slice(&mm[off + 6..off + 10]);
            result.push((eid, dim_id, old_value));
        }
        result
    }
}
