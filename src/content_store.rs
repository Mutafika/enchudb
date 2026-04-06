//! ContentStore — 非索引コンテンツ格納。Region経由。
//!
//! entity の自由テキスト（備考、メモ等）を格納。紐を張らない。検索対象外。
//!
//! Layout:
//!   index Region: (eid * MAX_KEYS + key_hash) × 8B → [offset:u32, len:u32]
//!   data Region:  append-only blob store

use std::sync::atomic::{AtomicU32, Ordering};
use crate::region::Region;

const MAGIC: [u8; 4] = [b'C', b'N', b'T', b'1'];
const INDEX_HEADER: usize = 16;
const MAX_KEYS: u32 = 16;
const MAX_ENTITIES: u32 = 16_777_216;
const DATA_HEADER: usize = 12;

pub struct ContentStore {
    index: Region,
    data: Region,
    data_end: AtomicU32,
}

unsafe impl Sync for ContentStore {}
unsafe impl Send for ContentStore {}

impl ContentStore {
    pub fn index_region_size() -> usize {
        INDEX_HEADER + (MAX_ENTITIES as usize) * (MAX_KEYS as usize) * 8
    }

    pub fn data_region_size() -> usize {
        64 * 1024 * 1024 // 64MB
    }

    /// 新規領域を初期化。
    pub fn init(index: Region, data: Region) -> Self {
        let im = index.slice_mut();
        im[0..4].copy_from_slice(&MAGIC);

        let dm = data.slice_mut();
        dm[0..4].copy_from_slice(&MAGIC);
        dm[4..8].copy_from_slice(&(DATA_HEADER as u32).to_le_bytes());

        Self {
            index, data,
            data_end: AtomicU32::new(DATA_HEADER as u32),
        }
    }

    /// 既存領域をロード。
    pub fn load(index: Region, data: Region) -> Self {
        let dm = data.slice();
        let data_end = u32::from_le_bytes(dm[4..8].try_into().unwrap());
        Self {
            index, data,
            data_end: AtomicU32::new(data_end),
        }
    }

    fn key_hash(key: &str) -> u32 {
        let mut h = 0x811c9dc5u32;
        for b in key.bytes() { h ^= b as u32; h = h.wrapping_mul(0x01000193); }
        h % MAX_KEYS
    }

    fn index_offset(eid: u32, key_hash: u32) -> usize {
        INDEX_HEADER + ((eid as usize) * (MAX_KEYS as usize) + key_hash as usize) * 8
    }

    pub fn set(&self, eid: u32, key: &str, content: &[u8]) {
        let kh = Self::key_hash(key);
        let off = Self::index_offset(eid, kh);

        let len = content.len() as u32;
        let data_off = self.data_end.fetch_add(len, Ordering::Relaxed);
        let dm = self.data.slice_mut();
        if (data_off + len) as usize <= dm.len() {
            dm[data_off as usize..(data_off + len) as usize].copy_from_slice(content);
        }

        let im = self.index.slice_mut();
        if off + 8 <= im.len() {
            im[off..off + 4].copy_from_slice(&data_off.to_le_bytes());
            im[off + 4..off + 8].copy_from_slice(&len.to_le_bytes());
        }
    }

    pub fn get(&self, eid: u32, key: &str) -> Option<&[u8]> {
        let kh = Self::key_hash(key);
        let off = Self::index_offset(eid, kh);

        let im = self.index.slice();
        if off + 8 > im.len() { return None; }

        let data_off = u32::from_le_bytes(im[off..off + 4].try_into().unwrap()) as usize;
        let len = u32::from_le_bytes(im[off + 4..off + 8].try_into().unwrap()) as usize;
        if data_off == 0 || len == 0 { return None; }

        let dm = self.data.slice();
        if data_off + len > dm.len() { return None; }
        Some(&dm[data_off..data_off + len])
    }

    pub fn remove(&self, eid: u32, key: &str) {
        let kh = Self::key_hash(key);
        let off = Self::index_offset(eid, kh);
        let im = self.index.slice_mut();
        if off + 8 <= im.len() {
            im[off..off + 8].fill(0);
        }
    }

    /// 内部状態をRegionヘッダに書き戻す。
    pub fn sync(&self) {
        let end = self.data_end.load(Ordering::Relaxed);
        let dm = self.data.slice_mut();
        dm[4..8].copy_from_slice(&end.to_le_bytes());
    }
}
