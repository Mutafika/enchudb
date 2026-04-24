//! Vocabulary — ユニーク値辞書。Symbol の値を value_id (u32) に変換。
//! 3つのRegion（data, offsets, index）で構成。

use std::sync::atomic::{AtomicU32, Ordering};
use crate::region::Region;

const MAGIC: [u8; 4] = [b'V', b'O', b'C', b'1'];
const HEADER: usize = 16;
const INDEX_MAGIC: [u8; 4] = [b'V', b'I', b'X', b'2'];
const INDEX_HEADER: usize = 16;
const INDEX_SLOT_SIZE: usize = 13;

pub struct Vocabulary {
    data: Region,
    offsets: Region,
    index: Region,
    count: AtomicU32,
    data_end: AtomicU32,
    max_entries: u32,
    index_cap: u32,
}

unsafe impl Sync for Vocabulary {}
unsafe impl Send for Vocabulary {}

impl Vocabulary {
    pub fn data_region_size(data_size: usize) -> usize { data_size.max(HEADER) }
    pub fn offsets_region_size(max_entries: u32) -> usize { (max_entries as usize) * 8 }
    pub fn index_region_size(index_cap: u32) -> usize { INDEX_HEADER + (index_cap as usize) * INDEX_SLOT_SIZE }

    /// 新規領域を初期化。
    pub fn init(data: Region, offsets: Region, index: Region, max_entries: u32, index_cap: u32) -> Self {
        let index_cap = index_cap.next_power_of_two();

        // data header
        let dm = data.slice_mut();
        dm[0..4].copy_from_slice(&MAGIC);
        dm[4..8].copy_from_slice(&0u32.to_le_bytes());
        dm[8..12].copy_from_slice(&(HEADER as u32).to_le_bytes());

        // index header
        let xm = index.slice_mut();
        xm[0..4].copy_from_slice(&INDEX_MAGIC);
        xm[4..8].copy_from_slice(&index_cap.to_le_bytes());

        Self {
            data, offsets, index,
            count: AtomicU32::new(0),
            data_end: AtomicU32::new(HEADER as u32),
            max_entries, index_cap,
        }
    }

    /// 既存領域をロード。ハッシュインデックスを再構築する。
    pub fn load(data: Region, offsets: Region, index: Region) -> Self {
        let dm = data.slice();
        let count = u32::from_le_bytes(dm[4..8].try_into().unwrap());
        let data_end = u32::from_le_bytes(dm[8..12].try_into().unwrap());

        let om = offsets.slice();
        let max_entries = (om.len() / 8) as u32;

        let xm = index.slice();
        let index_cap = u32::from_le_bytes(xm[4..8].try_into().unwrap());

        let v = Self {
            data, offsets, index,
            count: AtomicU32::new(count),
            data_end: AtomicU32::new(data_end),
            max_entries, index_cap,
        };
        v.rebuild_index();
        v
    }

    /// ハッシュインデックスを data/offsets から再構築する。
    fn rebuild_index(&self) {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 { return; }

        // インデックス領域をゼロクリア（ヘッダは保持）
        let xm = self.index.slice_mut();
        for b in &mut xm[INDEX_HEADER..] { *b = 0; }

        // 全エントリを再挿入
        for id in 0..count {
            let value = self.get(id);
            self.index_insert(value, id);
        }
    }

    pub fn get_or_insert(&self, value: &[u8]) -> u32 {
        if let Some(id) = self.lookup(value) { return id; }
        let id = self.insert(value);
        // 並列挿入の競合チェック: 別スレッドが先に同じ値を挿入した場合、先着のidを使う
        if let Some(winner) = self.lookup(value) {
            if winner != id { return winner; }
        }
        id
    }

    #[inline]
    pub fn get(&self, id: u32) -> &[u8] {
        let om = self.offsets.slice();
        let off_pos = (id as usize) * 8;
        let offset = u32::from_le_bytes(om[off_pos..off_pos + 4].try_into().unwrap()) as usize;
        let len = u32::from_le_bytes(om[off_pos + 4..off_pos + 8].try_into().unwrap()) as usize;
        let dm = self.data.slice();
        &dm[offset..offset + len]
    }

    #[inline]
    pub fn lookup(&self, value: &[u8]) -> Option<u32> {
        let mask = (self.index_cap - 1) as u64;
        let xm = self.index.slice();
        let h = fxhash(value);
        let mut idx = (h & mask) as usize;
        loop {
            let off = INDEX_HEADER + idx * INDEX_SLOT_SIZE;
            if xm[off] == 0 { return None; }
            let slot_hash = u64::from_le_bytes(xm[off + 1..off + 9].try_into().unwrap());
            if slot_hash == h {
                let vid = u32::from_le_bytes(xm[off + 9..off + 13].try_into().unwrap());
                let stored = self.get(vid);
                if stored == value { return Some(vid); }
            }
            idx = ((idx as u64 + 1) & mask) as usize;
        }
    }

    fn insert(&self, value: &[u8]) -> u32 {
        let id = self.count.fetch_add(1, Ordering::Relaxed);
        assert!(id < self.max_entries, "vocabulary full");
        let len = value.len() as u32;
        let offset = self.data_end.fetch_add(len, Ordering::Relaxed);
        let dm = self.data.slice_mut();
        dm[offset as usize..offset as usize + len as usize].copy_from_slice(value);
        // count/data_end を mmap header に即書き戻し（flush なしで drop されても復元可能に）
        let new_count = id + 1;
        let new_end = offset + len;
        dm[4..8].copy_from_slice(&new_count.to_le_bytes());
        dm[8..12].copy_from_slice(&new_end.to_le_bytes());
        let om = self.offsets.slice_mut();
        let off_pos = (id as usize) * 8;
        om[off_pos..off_pos + 4].copy_from_slice(&offset.to_le_bytes());
        om[off_pos + 4..off_pos + 8].copy_from_slice(&len.to_le_bytes());
        self.index_insert(value, id);
        id
    }

    fn index_insert(&self, value: &[u8], id: u32) {
        let mask = (self.index_cap - 1) as u64;
        let xm = self.index.slice_mut();
        let h = fxhash(value);
        let mut idx = (h & mask) as usize;
        loop {
            let off = INDEX_HEADER + idx * INDEX_SLOT_SIZE;
            let flag = unsafe {
                &*(xm.as_ptr().add(off) as *const std::sync::atomic::AtomicU8)
            };
            let f = flag.load(Ordering::Acquire);
            if f == 0 {
                match flag.compare_exchange(0, 2, Ordering::AcqRel, Ordering::Relaxed) {
                    Ok(_) => {
                        xm[off + 1..off + 9].copy_from_slice(&h.to_le_bytes());
                        xm[off + 9..off + 13].copy_from_slice(&id.to_le_bytes());
                        flag.store(1, Ordering::Release);
                        return;
                    }
                    Err(_) => continue,
                }
            }
            if f == 2 {
                while flag.load(Ordering::Acquire) == 2 { std::hint::spin_loop(); }
                continue;
            }
            let slot_hash = u64::from_le_bytes(xm[off + 1..off + 9].try_into().unwrap());
            if slot_hash == h {
                // ハッシュ一致 → 実際の値を比較して本当に重複か確認
                let vid = u32::from_le_bytes(xm[off + 9..off + 13].try_into().unwrap());
                if self.get(vid) == value { return; } // 本当の重複
                // ハッシュ衝突 → linear probe 続行
            }
            idx = ((idx as u64 + 1) & mask) as usize;
        }
    }

    pub fn count(&self) -> u32 { self.count.load(Ordering::Relaxed) }

    /// 内部状態をRegionヘッダに書き戻す（flushの前に呼ぶ）。
    pub fn sync(&self) {
        let dm = self.data.slice_mut();
        dm[4..8].copy_from_slice(&self.count.load(Ordering::Relaxed).to_le_bytes());
        dm[8..12].copy_from_slice(&self.data_end.load(Ordering::Relaxed).to_le_bytes());
    }
}

#[inline(always)]
fn fxhash(data: &[u8]) -> u64 {
    const SEED: u64 = 0x517cc1b727220a95;
    let mut h: u64 = 0;
    let mut i = 0;
    while i + 8 <= data.len() {
        let word = u64::from_le_bytes([
            data[i], data[i+1], data[i+2], data[i+3],
            data[i+4], data[i+5], data[i+6], data[i+7],
        ]);
        h = (h.rotate_left(5) ^ word).wrapping_mul(SEED);
        i += 8;
    }
    while i < data.len() {
        h = (h.rotate_left(5) ^ data[i] as u64).wrapping_mul(SEED);
        i += 1;
    }
    h
}
