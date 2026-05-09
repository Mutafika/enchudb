//! EntitySet — ビットセット + 空きID管理。Region経由。
//!
//! entity の生存管理。allocate / free / is_live / iter。
//! AtomicU32 で next_eid を管理。ロック不要。
//!
//! Layout:
//!   Header (16B): [magic:4][next_eid:AtomicU32][live_count:AtomicU32][reserved:4]
//!   Bitset: ceil(max_entities / 8) bytes — bit=1 で live
//!   Free stack: [count:4][eid0:4][eid1:4]...

use std::sync::atomic::{AtomicU32, Ordering};
use crate::region::Region;

const MAGIC: [u8; 4] = [b'E', b'N', b'T', b'1'];
const HEADER: usize = 16;
const FREE_STACK_MAX: u32 = 1_048_576;

pub struct EntitySet {
    region: Region,
    max_entities: u32,
    bitset_offset: usize,
    free_offset: usize,
}

unsafe impl Sync for EntitySet {}
unsafe impl Send for EntitySet {}

impl EntitySet {
    pub fn region_size(max_entities: u32) -> usize {
        let bitset_size = ((max_entities + 7) / 8) as usize;
        // free_stack に積めるのは「これまで allocate された eid」 だけなので、
        // 論理上限は max_entities。 FREE_STACK_MAX (= 1 M) は default preset
        // (max_entities = 16 M) 想定の上限で、 tiny preset (max_entities =
        // 1024) では 4 MB の無駄。 min を取ることで tiny で 4 KB まで縮む。
        let free_cap = (FREE_STACK_MAX as usize).min(max_entities as usize);
        let free_size = 4 + free_cap * 4;
        HEADER + bitset_size + free_size
    }

    /// 新規領域を初期化。
    ///
    /// growable backing で initial_commit が region の手前で打ち切られて
    /// いる可能性があるため、 region 全体 (header + bitset + free stack)
    /// を init 時点で commit する。 EntitySet 全体は max_entities が
    /// 制限されてれば十分小さい (tiny preset で ~4 KB)。
    pub fn init(region: Region, max_entities: u32) -> Self {
        let bitset_offset = HEADER;
        let bitset_size = ((max_entities + 7) / 8) as usize;
        let free_offset = (bitset_offset + bitset_size + 3) & !3;
        let region_total = Self::region_size(max_entities);

        let _ = region.ensure_committed(region_total);
        let mm = region.slice_mut();
        mm[0..4].copy_from_slice(&MAGIC);
        // next_eid = 0, live_count = 0 (already zero from fresh region)

        Self { region, max_entities, bitset_offset, free_offset }
    }

    /// 既存領域をロード。
    pub fn load(region: Region, max_entities: u32) -> Self {
        let mm = region.slice();
        if mm.len() < HEADER || mm[0..4] != MAGIC {
            panic!("bad entity set magic");
        }
        let bitset_offset = HEADER;
        let bitset_size = ((max_entities + 7) / 8) as usize;
        let free_offset = (bitset_offset + bitset_size + 3) & !3; // AtomicU32 alignment
        Self { region, max_entities, bitset_offset, free_offset }
    }

    fn next_eid_atomic(&self) -> &AtomicU32 {
        let mm = self.region.slice();
        unsafe { &*(mm.as_ptr().add(4) as *const AtomicU32) }
    }

    fn live_count_atomic(&self) -> &AtomicU32 {
        let mm = self.region.slice();
        unsafe { &*(mm.as_ptr().add(8) as *const AtomicU32) }
    }

    pub fn allocate(&self) -> u32 {
        // 高速パス: monotonic increment（欠番方式、ロックフリー）
        let eid = self.next_eid_atomic().fetch_add(1, Ordering::Relaxed);
        if eid < self.max_entities {
            self.set_bit(eid, true);
            self.live_count_atomic().fetch_add(1, Ordering::Relaxed);
            return eid;
        }
        // 上限到達 → fetch_addを巻き戻してfree stackから再利用
        self.next_eid_atomic().fetch_sub(1, Ordering::Relaxed);
        self.allocate_from_free_stack()
    }

    /// free stack から CAS で安全に pop。上限到達時のみ呼ばれる。
    fn allocate_from_free_stack(&self) -> u32 {
        let mm = self.region.slice_mut();
        let fc_ptr = unsafe {
            &*(mm.as_ptr().add(self.free_offset) as *const AtomicU32)
        };
        loop {
            let fc = fc_ptr.load(Ordering::Acquire);
            assert!(fc > 0, "entity limit reached: max_entities={}, no free slots", self.max_entities);
            if fc_ptr.compare_exchange(fc, fc - 1, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                let eid_off = self.free_offset + 4 + ((fc - 1) as usize) * 4;
                let eid = u32::from_le_bytes(mm[eid_off..eid_off + 4].try_into().unwrap());
                self.set_bit(eid, true);
                self.live_count_atomic().fetch_add(1, Ordering::Relaxed);
                return eid;
            }
        }
    }

    pub fn free(&self, eid: u32) {
        if !self.is_live(eid) { return; }
        self.set_bit(eid, false);
        self.live_count_atomic().fetch_sub(1, Ordering::Relaxed);

        let mm = self.region.slice_mut();
        let free_count_off = self.free_offset;
        let fc = u32::from_le_bytes(mm[free_count_off..free_count_off + 4].try_into().unwrap());
        // free_cap は region_size() と同じ式 — 「論理上限 = 確保された
        // エントリ数 = max_entities」 で region サイズを縮めているので、
        // ここも同じ cap で打ち切る (超えたら静かに drop する旧仕様を踏襲)。
        let free_cap = (FREE_STACK_MAX as usize).min(self.max_entities as usize) as u32;
        if fc < free_cap {
            let eid_off = self.free_offset + 4 + (fc as usize) * 4;
            mm[eid_off..eid_off + 4].copy_from_slice(&eid.to_le_bytes());
            mm[free_count_off..free_count_off + 4].copy_from_slice(&(fc + 1).to_le_bytes());
        }
    }

    /// rollback用: 削除されたentityを復活させる。
    pub fn revive(&self, eid: u32) {
        if eid >= self.max_entities || self.is_live(eid) { return; }
        self.set_bit(eid, true);
        self.live_count_atomic().fetch_add(1, Ordering::Relaxed);
    }

    /// v32: リモート peer から届いた eid を「存在する」ことにする。
    /// 既に live なら no-op。next_eid / live_count / live bitmap を整合的に更新する。
    pub fn ensure_live(&self, eid: u32) {
        if eid >= self.max_entities { return; }
        if self.is_live(eid) { return; }
        self.set_bit(eid, true);
        self.live_count_atomic().fetch_add(1, Ordering::Relaxed);
        // next_eid は「これまで allocate した最大 +1」の概念。local を超えていたら進める。
        let cur = self.next_eid_atomic().load(Ordering::Acquire);
        if eid >= cur {
            self.next_eid_atomic().store(eid + 1, Ordering::Release);
        }
    }

    #[inline]
    pub fn is_live(&self, eid: u32) -> bool {
        if eid >= self.max_entities { return false; }
        let mm = self.region.slice();
        let byte_off = self.bitset_offset + (eid / 8) as usize;
        let bit = 1u8 << (eid % 8);
        (mm[byte_off] & bit) != 0
    }

    fn set_bit(&self, eid: u32, live: bool) {
        if eid >= self.max_entities { return; }
        let mm = self.region.slice_mut();
        let byte_off = self.bitset_offset + (eid / 8) as usize;
        let bit = 1u8 << (eid % 8);
        if live {
            mm[byte_off] |= bit;
        } else {
            mm[byte_off] &= !bit;
        }
    }

    pub fn count(&self) -> u32 {
        self.live_count_atomic().load(Ordering::Relaxed)
    }

    pub fn next_eid(&self) -> u32 {
        self.next_eid_atomic().load(Ordering::Relaxed)
    }

    pub fn iter(&self) -> Vec<u32> {
        let mm = self.region.slice();
        let next = self.next_eid();
        let mut result = Vec::with_capacity(self.count() as usize);
        for eid in 0..next {
            let byte_off = self.bitset_offset + (eid / 8) as usize;
            let bit = 1u8 << (eid % 8);
            if (mm[byte_off] & bit) != 0 {
                result.push(eid);
            }
        }
        result
    }
}
