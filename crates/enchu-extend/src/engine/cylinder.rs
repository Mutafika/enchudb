//! Cylinder（円柱の実体）— prefix sum O(1) + 二分探索 O(log n) 自動切替。
//!
//! max_values が指定されていれば prefix sum で O(1)。
//! 未指定なら二分探索 O(log n)。紐の値域で自動判定。
//!
//! Layout (Region内):
//!   Header (16B): [magic:4][total:u32][max_entities:u32][max_values:u32]
//!   Values    [H .. H + max_ent*4]:              value per entry (sorted)
//!   Entities  [H + max_ent*4 .. H + max_ent*8]:  entity_id per entry (parallel)
//!   PrefixSum [H + max_ent*8 .. H + max_ent*8 + (max_values+2)*4]:  (max_values>0 のみ)

use std::sync::atomic::{AtomicU32, Ordering};
use super::region::Region;

const MAGIC: [u8; 4] = [b'C', b'Y', b'3', b'1'];
const HEADER: usize = 16;

pub struct Cylinder {
    region: Region,
    max_entities: u32,
    max_values: u32,
    total: AtomicU32,
    values_offset: usize,
    entities_offset: usize,
    prefix_offset: usize,
}

unsafe impl Sync for Cylinder {}
unsafe impl Send for Cylinder {}

impl Cylinder {
    pub fn region_size(max_entities: u32, max_values: u32) -> usize {
        let values_size = (max_entities as usize) * 4;
        let entities_size = (max_entities as usize) * 4;
        let prefix_size = if max_values > 0 { (max_values as usize + 2) * 4 } else { 0 };
        HEADER + values_size + entities_size + prefix_size
    }

    /// 新規領域を初期化。
    pub fn init(region: Region, max_entities: u32, max_values: u32) -> Self {
        let values_offset = HEADER;
        let entities_offset = values_offset + (max_entities as usize) * 4;
        let prefix_offset = entities_offset + (max_entities as usize) * 4;

        let mm = region.slice_mut();
        mm[0..4].copy_from_slice(&MAGIC);
        mm[4..8].copy_from_slice(&0u32.to_le_bytes());
        mm[8..12].copy_from_slice(&max_entities.to_le_bytes());
        mm[12..16].copy_from_slice(&max_values.to_le_bytes());

        Self {
            region, max_entities, max_values,
            total: AtomicU32::new(0),
            values_offset, entities_offset, prefix_offset,
        }
    }

    /// 既存領域をロード。
    pub fn load(region: Region) -> Self {
        let mm = region.slice();
        let total = u32::from_le_bytes(mm[4..8].try_into().unwrap());
        let max_entities = u32::from_le_bytes(mm[8..12].try_into().unwrap());
        let max_values = u32::from_le_bytes(mm[12..16].try_into().unwrap());
        let values_offset = HEADER;
        let entities_offset = values_offset + (max_entities as usize) * 4;
        let prefix_offset = entities_offset + (max_entities as usize) * 4;

        Self {
            region, max_entities, max_values,
            total: AtomicU32::new(total),
            values_offset, entities_offset, prefix_offset,
        }
    }

    /// 円柱をリビルド。ソート済み並列配列 + prefix sum（max_values > 0 なら）。
    pub fn rebuild(&self, mut pairs: Vec<(u32, u32)>) {
        pairs.sort_unstable_by_key(|&(v, e)| (v, e));

        let mm = self.region.slice_mut();

        for (i, &(val, eid)) in pairs.iter().enumerate() {
            let vo = self.values_offset + i * 4;
            let eo = self.entities_offset + i * 4;
            mm[vo..vo + 4].copy_from_slice(&val.to_le_bytes());
            mm[eo..eo + 4].copy_from_slice(&eid.to_le_bytes());
        }

        if self.max_values > 0 {
            let slots = self.max_values as usize + 2;
            let pbase = self.prefix_offset;
            let mut counts = vec![0u32; slots];
            for &(val, _) in &pairs {
                let idx = (val as usize).min(slots - 2);
                counts[idx] += 1;
            }
            let mut acc = 0u32;
            for s in 0..slots {
                let off = pbase + s * 4;
                mm[off..off + 4].copy_from_slice(&acc.to_le_bytes());
                acc += counts[s];
            }
        }

        let t = pairs.len() as u32;
        mm[4..8].copy_from_slice(&t.to_le_bytes());
        self.total.store(t, Ordering::Release);
    }

    /// 完全一致。max_values > 0 なら O(1)、それ以外 O(log n)。
    #[inline]
    pub fn slice_one(&self, value: u32) -> &[u32] {
        let n = self.total.load(Ordering::Acquire) as usize;
        if n == 0 { return &[]; }

        if self.max_values > 0 && value <= self.max_values {
            return self.slice_one_prefix(value);
        }
        self.slice_one_bsearch(value, n)
    }

    #[inline]
    fn slice_one_prefix(&self, value: u32) -> &[u32] {
        let mm = self.region.slice();
        let pbase = self.prefix_offset;
        let off_start = pbase + (value as usize) * 4;
        let off_end = pbase + (value as usize + 1) * 4;
        let start = u32::from_le_bytes(mm[off_start..off_start + 4].try_into().unwrap()) as usize;
        let end = u32::from_le_bytes(mm[off_end..off_end + 4].try_into().unwrap()) as usize;
        if start >= end { return &[]; }
        self.entities_sub(start, end)
    }

    #[inline]
    fn slice_one_bsearch(&self, value: u32, n: usize) -> &[u32] {
        let vals = self.values_slice(n);
        let start = vals.partition_point(|&v| v < value);
        let end = vals[start..].partition_point(|&v| v <= value) + start;
        if start >= end { return &[]; }
        self.entities_sub(start, end)
    }

    /// 範囲。O(log n)。
    #[inline]
    pub fn slice_range(&self, low: u32, high: u32) -> &[u32] {
        let n = self.total.load(Ordering::Acquire) as usize;
        if n == 0 { return &[]; }
        let vals = self.values_slice(n);
        let start = vals.partition_point(|&v| v < low);
        let end = vals.partition_point(|&v| v <= high);
        if start >= end { return &[]; }
        self.entities_sub(start, end)
    }

    pub fn total(&self) -> usize {
        self.total.load(Ordering::Acquire) as usize
    }

    /// ソート済みvalues配列からユニーク値を返す。
    pub fn unique_values(&self, n: usize) -> Vec<u32> {
        if n == 0 { return vec![]; }
        let vals = self.values_slice(n);
        let mut result = Vec::new();
        let mut prev = u32::MAX;
        for &v in vals {
            if v != prev {
                result.push(v);
                prev = v;
            }
        }
        result
    }

    #[inline(always)]
    fn values_slice(&self, n: usize) -> &[u32] {
        let mm = self.region.slice();
        let ptr = mm[self.values_offset..].as_ptr() as *const u32;
        unsafe { std::slice::from_raw_parts(ptr, n) }
    }

    #[inline(always)]
    fn entities_sub(&self, start: usize, end: usize) -> &[u32] {
        let mm = self.region.slice();
        let base = self.entities_offset + start * 4;
        let ptr = mm[base..].as_ptr() as *const u32;
        unsafe { std::slice::from_raw_parts(ptr, end - start) }
    }
}
