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
        region.write_at(0, &0u32.to_le_bytes());
        region.write_at(4, &value_size.to_le_bytes());
        region.write_at(8, &max_entities.to_le_bytes());
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
        self.region.write_at(off, &value[..len]);
        self.region.mark_dirty(off, len);
    }

    #[inline]
    pub fn get(&self, entity_id: u32) -> &[u8] {
        let vs = self.value_size as usize;
        let off = HEADER + (entity_id as usize) * vs;
        let mm = self.region.slice();
        &mm[off..off + vs]
    }

    /// #106: 4B 値を Release で publish する (leaf offset 用)。 `load_u32_acquire` と対で、
    /// この offset を書く *前* の書込 (= LeafStore slot の payload/gen) の可視性を保証する。
    /// これが無いと reader が「新 offset は見えるが leaf slot はまだ stale」を掴み、
    /// seqlock (gen) を stale 値で誤通過して torn read になる。 value_size==4 前提。
    #[inline]
    pub fn store_u32_release(&self, entity_id: u32, v: u32) {
        let off = HEADER + (entity_id as usize) * (self.value_size as usize);
        self.region.as_atomic_u32(off).store(v, Ordering::Release);
        self.region.mark_dirty(off, 4);
    }

    /// #106: 4B 値を Acquire で読む (`store_u32_release` と対)。 value_size==4 前提。
    #[inline]
    pub fn load_u32_acquire(&self, entity_id: u32) -> u32 {
        let off = HEADER + (entity_id as usize) * (self.value_size as usize);
        self.region.as_atomic_u32(off).load(Ordering::Acquire)
    }

    /// 0.8.6: u32 packed value slice。 SIMD 集計 / 全件 scan 用の fast path。
    /// `value_size == 4` (= Number / Tag / Leaf 等の通常 himo) でのみ意味あり。
    /// 戻り値の長さは `count()`、 stored 形式 (= 0 = 未設定、 N = 値 N-1)。
    ///
    /// HEADER (= 16 bytes) は u32 alignment、 mmap region は page-aligned なので
    /// アライン安全。 LE 前提 (aarch64 / x86_64 等の supported target で OK)。
    #[inline]
    pub fn values_u32(&self) -> &[u32] {
        debug_assert_eq!(self.value_size, 4, "values_u32 requires value_size == 4");
        let n = self.count() as usize;
        let mm = self.region.slice();
        // SAFETY: HEADER (16) は u32 アラインで、 mmap region は page-aligned。
        // n * 4 <= max_entities * 4 (= region 内 packed 領域の上限)。
        // u32 LE は aarch64 / x86_64 で native u32 と一致。
        let base = unsafe { mm.as_ptr().add(HEADER) as *const u32 };
        unsafe { std::slice::from_raw_parts(base, n) }
    }

    #[inline]
    pub fn clear(&self, entity_id: u32) {
        let vs = self.value_size as usize;
        let off = HEADER + (entity_id as usize) * vs;
        self.region.fill_at(off, vs, 0);
        self.region.mark_dirty(off, vs);
    }

    pub fn count(&self) -> u32 { self.count.load(Ordering::Relaxed) }

    /// count を eid+1 まで引き上げる（atomic、並列安全）。
    pub fn ensure_count(&self, eid: u32) {
        let needed = eid + 1;
        self.count.fetch_max(needed, Ordering::Relaxed);
        // mmapヘッダにも書く。 count は rebuild/flush 時に確定するので、ここでは最大値。
        let current = u32::from_le_bytes(self.region.slice()[0..4].try_into().unwrap());
        if needed > current {
            self.region.write_at(0, &needed.to_le_bytes());
        }
    }

    /// flush用: 現在のcountをmmapヘッダに書き出す。
    pub fn sync_count(&self) {
        let c = self.count.load(Ordering::Relaxed);
        self.region.write_at(0, &c.to_le_bytes());
    }
}
