//! Cylinder（円柱の実体）— prefix sum O(1) + 二分探索 O(log n) 自動切替。
//!
//! max_values が指定されていれば prefix sum で O(1)。
//! 未指定なら二分探索 O(log n)。紐の値域で自動判定。
//!
//! Layout (single mmap file):
//!   Header (16B): [magic:4][total:u32][max_entities:u32][max_values:u32]
//!   Values    [H .. H + max_ent*4]:              value per entry (sorted)
//!   Entities  [H + max_ent*4 .. H + max_ent*8]:  entity_id per entry (parallel)
//!   PrefixSum [H + max_ent*8 .. H + max_ent*8 + (max_values+2)*4]:  (max_values>0 のみ)

use std::cell::UnsafeCell;
use std::fs::OpenOptions;
use std::io;
use std::sync::atomic::{AtomicU32, Ordering};
use memmap2::MmapMut;

const MAGIC: [u8; 4] = [b'C', b'Y', b'3', b'1']; // 3.1 — prefix sum 対応
const HEADER: usize = 16;

pub struct Cylinder {
    mmap: UnsafeCell<MmapMut>,
    max_entities: u32,
    max_values: u32, // 0 = 無制限（二分探索のみ）
    total: AtomicU32,
    values_offset: usize,
    entities_offset: usize,
    prefix_offset: usize, // max_values > 0 のときだけ有効
}

unsafe impl Sync for Cylinder {}
unsafe impl Send for Cylinder {}

impl Cylinder {
    pub fn create(path: &str, max_entities: u32, max_values: u32) -> io::Result<Self> {
        let values_offset = HEADER;
        let entities_offset = values_offset + (max_entities as usize) * 4;
        let prefix_offset = entities_offset + (max_entities as usize) * 4;
        let prefix_size = if max_values > 0 { (max_values as usize + 2) * 4 } else { 0 };
        let total_size = prefix_offset + prefix_size;

        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path)?;
        file.set_len(total_size as u64)?;

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        mmap[0..4].copy_from_slice(&MAGIC);
        mmap[4..8].copy_from_slice(&0u32.to_le_bytes());
        mmap[8..12].copy_from_slice(&max_entities.to_le_bytes());
        mmap[12..16].copy_from_slice(&max_values.to_le_bytes());

        Ok(Self {
            mmap: UnsafeCell::new(mmap),
            max_entities, max_values,
            total: AtomicU32::new(0),
            values_offset, entities_offset, prefix_offset,
        })
    }

    pub fn open(path: &str) -> io::Result<Option<Self>> {
        let file = match OpenOptions::new().read(true).write(true).open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        if mmap.len() < HEADER || mmap[0..4] != MAGIC { return Ok(None); }

        let total = u32::from_le_bytes(mmap[4..8].try_into().unwrap());
        let max_entities = u32::from_le_bytes(mmap[8..12].try_into().unwrap());
        let max_values = u32::from_le_bytes(mmap[12..16].try_into().unwrap());
        let values_offset = HEADER;
        let entities_offset = values_offset + (max_entities as usize) * 4;
        let prefix_offset = entities_offset + (max_entities as usize) * 4;

        Ok(Some(Self {
            mmap: UnsafeCell::new(mmap),
            max_entities, max_values,
            total: AtomicU32::new(total),
            values_offset, entities_offset, prefix_offset,
        }))
    }

    /// 円柱をリビルド。ソート済み並列配列 + prefix sum（max_values > 0 なら）。
    pub fn rebuild(&self, mut pairs: Vec<(u32, u32)>) {
        pairs.sort_unstable_by_key(|&(v, e)| (v, e));

        let mm = unsafe { &mut *self.mmap.get() };

        for (i, &(val, eid)) in pairs.iter().enumerate() {
            let vo = self.values_offset + i * 4;
            let eo = self.entities_offset + i * 4;
            mm[vo..vo + 4].copy_from_slice(&val.to_le_bytes());
            mm[eo..eo + 4].copy_from_slice(&eid.to_le_bytes());
        }

        // prefix sum 構築
        // prefix[v] = value < v のエントリ数。prefix[v+1] - prefix[v] = value==v の件数。
        if self.max_values > 0 {
            let slots = self.max_values as usize + 2;
            let pbase = self.prefix_offset;
            // カウント: counts[v] = value==v の件数
            let mut counts = vec![0u32; slots];
            for &(val, _) in &pairs {
                let idx = (val as usize).min(slots - 2);
                counts[idx] += 1;
            }
            // 累積和 → prefix[0]=0, prefix[1]=counts[0], prefix[2]=counts[0]+counts[1], ...
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

    /// prefix sum O(1)。
    #[inline]
    fn slice_one_prefix(&self, value: u32) -> &[u32] {
        let mm = unsafe { &*self.mmap.get() };
        let pbase = self.prefix_offset;
        let off_start = pbase + (value as usize) * 4;
        let off_end = pbase + (value as usize + 1) * 4;
        let start = u32::from_le_bytes(mm[off_start..off_start + 4].try_into().unwrap()) as usize;
        let end = u32::from_le_bytes(mm[off_end..off_end + 4].try_into().unwrap()) as usize;
        if start >= end { return &[]; }
        self.entities_sub(start, end)
    }

    /// 二分探索 O(log n)。
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

    #[inline(always)]
    fn values_slice(&self, n: usize) -> &[u32] {
        let mm = unsafe { &*self.mmap.get() };
        let ptr = mm[self.values_offset..].as_ptr() as *const u32;
        unsafe { std::slice::from_raw_parts(ptr, n) }
    }

    #[inline(always)]
    fn entities_sub(&self, start: usize, end: usize) -> &[u32] {
        let mm = unsafe { &*self.mmap.get() };
        let base = self.entities_offset + start * 4;
        let ptr = mm[base..].as_ptr() as *const u32;
        unsafe { std::slice::from_raw_parts(ptr, end - start) }
    }

    pub fn flush(&self) -> io::Result<()> {
        let mm = unsafe { &*self.mmap.get() };
        mm.flush()
    }
}
