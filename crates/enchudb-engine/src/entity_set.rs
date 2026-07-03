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
    /// #77-H7: free stack の push/pop 直列化。 push (count 読み → eid 書き →
    /// count 書き) は非 atomic な複合操作のため、 並行 free / saturation 時の
    /// allocate と混ざると slot 破壊・二重払い出しが起きる。 writer は
    /// .db.lock で 1 プロセスに限定されるため in-process Mutex で足りる。
    free_lock: std::sync::Mutex<()>,
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

        Self { region, max_entities, bitset_offset, free_offset,
               free_lock: std::sync::Mutex::new(()) }
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
        Self { region, max_entities, bitset_offset, free_offset,
               free_lock: std::sync::Mutex::new(()) }
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
            // issue6 (perf 退化対策): writer hot path から mark_dirty を撤廃。
            // EntitySet 全領域は body_msync 内で常時 msync される (固定サイズ
            // で cheap な小領域)。
            return eid;
        }
        // 上限到達 → fetch_addを巻き戻してfree stackから再利用
        self.next_eid_atomic().fetch_sub(1, Ordering::Relaxed);
        self.allocate_from_free_stack()
    }

    /// free stack から pop。上限到達時のみ呼ばれる。
    /// #77-H7: `free_lock` で push と直列化 (旧 CAS pop は push 側の
    /// 「slot 書き → count 書き」非 atomic 複合と混ざると、 eid 未書き込みの
    /// slot を読み得た)。
    fn allocate_from_free_stack(&self) -> u32 {
        let _g = self.free_lock.lock().unwrap_or_else(|p| p.into_inner());
        let mm = self.region.slice_mut();
        let free_count_off = self.free_offset;
        let fc = u32::from_le_bytes(mm[free_count_off..free_count_off + 4].try_into().unwrap());
        assert!(fc > 0, "entity limit reached: max_entities={}, no free slots", self.max_entities);
        let eid_off = self.free_offset + 4 + ((fc - 1) as usize) * 4;
        let eid = u32::from_le_bytes(mm[eid_off..eid_off + 4].try_into().unwrap());
        mm[free_count_off..free_count_off + 4].copy_from_slice(&(fc - 1).to_le_bytes());
        self.set_bit(eid, true);
        self.live_count_atomic().fetch_add(1, Ordering::Relaxed);
        // EntitySet 全領域は body_msync 内で常時 msync (issue6 perf 対策)。
        eid
    }

    pub fn free(&self, eid: u32) {
        // #77-H7: was-live 判定を bit の atomic 遷移そのもので行う。
        // 旧実装の is_live → set_bit の 2 段は並行 free が両方通過し、
        // free stack への二重 push → 同一 eid の二重払い出しに繋がった。
        let was_live = self.set_bit(eid, false);
        if !was_live { return; }
        self.live_count_atomic().fetch_sub(1, Ordering::Relaxed);

        let _g = self.free_lock.lock().unwrap_or_else(|p| p.into_inner());
        let mm = self.region.slice_mut();
        let free_count_off = self.free_offset;
        let fc = u32::from_le_bytes(mm[free_count_off..free_count_off + 4].try_into().unwrap());
        let free_cap = (FREE_STACK_MAX as usize).min(self.max_entities as usize) as u32;
        if fc < free_cap {
            let eid_off = self.free_offset + 4 + (fc as usize) * 4;
            mm[eid_off..eid_off + 4].copy_from_slice(&eid.to_le_bytes());
            mm[free_count_off..free_count_off + 4].copy_from_slice(&(fc + 1).to_le_bytes());
        }
        // EntitySet 全領域は body_msync 内で常時 msync (issue6 perf 対策)。
    }

    /// rollback用: 削除されたentityを復活させる。
    pub fn revive(&self, eid: u32) {
        if eid >= self.max_entities { return; }
        // bit 遷移で was-live を判定 (並行 revive の二重 count 防止)
        if !self.set_bit(eid, true) {
            self.live_count_atomic().fetch_add(1, Ordering::Relaxed);
        }
    }

    /// v32: リモート peer から届いた eid を「存在する」ことにする。
    /// 既に live なら no-op。next_eid / live_count / live bitmap を整合的に更新する。
    pub fn ensure_live(&self, eid: u32) {
        if eid >= self.max_entities { return; }
        if self.set_bit(eid, true) { return; } // 既に live
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

    /// #77-H7: bit を atomic に立てる/落とす。 変更前にその bit が立っていたか
    /// を返す。 旧実装の `mm[byte_off] |= bit` は非 atomic RMW で、 隣接 eid を
    /// 払い出された並行スレッドと同一バイトを書き合うと片方の bit が消えた。
    fn set_bit(&self, eid: u32, live: bool) -> bool {
        if eid >= self.max_entities { return false; }
        let mm = self.region.slice();
        let byte_off = self.bitset_offset + (eid / 8) as usize;
        let byte = unsafe {
            &*(mm.as_ptr().add(byte_off) as *const std::sync::atomic::AtomicU8)
        };
        let bit = 1u8 << (eid % 8);
        let prev = if live {
            byte.fetch_or(bit, Ordering::AcqRel)
        } else {
            byte.fetch_and(!bit, Ordering::AcqRel)
        };
        (prev & bit) != 0
        // EntitySet 全領域は body_msync 内で常時 msync (issue6 perf 対策)。
    }

    pub fn count(&self) -> u32 {
        self.live_count_atomic().load(Ordering::Relaxed)
    }

    pub fn next_eid(&self) -> u32 {
        self.next_eid_atomic().load(Ordering::Relaxed)
    }

    /// 指定 eid を live としてマークし、 必要なら global next_eid を `eid + 1`
    /// まで進める。 β-light の table-aware allocation 用 (`entity_in`)。
    ///
    /// 並行性: CAS で next_eid を進めるため、 同時に走る `allocate()` とは
    /// 安全に共存できる (重複 eid を返さない)。 ただし呼び出し側で eid 範囲を
    /// 分離する責務を持つ — 例えば table A の range が [0, 1M), B の range が
    /// [1M, 2M) のように互いに disjoint であれば、 並行 allocate_at は安全。
    pub fn allocate_at(&self, eid: u32) {
        assert!(
            eid < self.max_entities,
            "allocate_at: eid {} exceeds max_entities {}",
            eid, self.max_entities,
        );
        if !self.set_bit(eid, true) {
            self.live_count_atomic().fetch_add(1, Ordering::Relaxed);
        }
        // next_eid を max(current, eid + 1) まで進める (CAS で torn write 防止)
        let mut cur = self.next_eid_atomic().load(Ordering::Relaxed);
        while cur <= eid {
            match self.next_eid_atomic().compare_exchange_weak(
                cur,
                eid + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_set(max_entities: u32) -> Arc<EntitySet> {
        let size = EntitySet::region_size(max_entities);
        let buf: Box<[u8]> = vec![0u8; size].into_boxed_slice();
        let ptr = Box::leak(buf).as_mut_ptr();
        let region = unsafe { Region::new(ptr, size) };
        Arc::new(EntitySet::init(region, max_entities))
    }

    /// #77-H7 regression: 並行 allocate で隣接 eid の live bit が消えないこと。
    /// 旧実装は `mm[byte] |= bit` の非 atomic RMW だったため、同一バイトを
    /// 共有する eid (7/8 の確率) の bit が並行書きで消失した。
    #[test]
    fn concurrent_allocate_no_lost_bits() {
        let set = make_set(100_000);
        const THREADS: usize = 8;
        const PER: usize = 500;
        let handles: Vec<_> = (0..THREADS).map(|_| {
            let s = set.clone();
            std::thread::spawn(move || {
                (0..PER).map(|_| s.allocate()).collect::<Vec<u32>>()
            })
        }).collect();
        let mut all: Vec<u32> = handles.into_iter()
            .flat_map(|h| h.join().unwrap()).collect();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), THREADS * PER, "eid が重複払い出しされた");
        for &eid in &all {
            assert!(set.is_live(eid), "eid {eid} の live bit が消失");
        }
        assert_eq!(set.count(), (THREADS * PER) as u32);
        assert_eq!(set.iter().len(), THREADS * PER);
    }

    /// #77-H7 regression: 同一 eid の並行 free が free stack に二重 push
    /// しないこと。旧実装は is_live → set_bit の 2 段で両者が通過し、
    /// 飽和後の allocate が同一 eid を二重払い出しした。
    #[test]
    fn concurrent_double_free_no_double_push() {
        let set = make_set(4);
        for _ in 0..4 { set.allocate(); } // 0..3 で飽和
        let handles: Vec<_> = (0..8).map(|_| {
            let s = set.clone();
            std::thread::spawn(move || s.free(2))
        }).collect();
        for h in handles { h.join().unwrap(); }
        assert_eq!(set.count(), 3, "live_count が二重減算された");

        // free stack には 2 が 1 回だけ積まれているはず
        assert_eq!(set.allocate(), 2, "stack から 2 が出るはず");
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            set.allocate() // stack 空 + 飽和 → panic が正しい
        }));
        assert!(result.is_err(), "二重 push された 2 が再払い出しされた");
    }
}
