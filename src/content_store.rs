//! ContentStore — 非索引コンテンツ格納。mmap ベース。
//!
//! entity の自由テキスト（備考、メモ等）を格納。紐を張らない。検索対象外。
//!
//! Layout:
//!   index.dat: (eid * MAX_KEYS + key_hash) × 8B → [offset:u32, len:u32]
//!   data.dat:  append-only blob store

use std::cell::UnsafeCell;
use std::fs::OpenOptions;
use std::io;
use std::sync::atomic::{AtomicU32, Ordering};
use memmap2::MmapMut;

const MAGIC: [u8; 4] = [b'C', b'N', b'T', b'1'];
const INDEX_HEADER: usize = 16;
const MAX_KEYS: u32 = 16;      // 1 entity あたり最大 16 content keys
const MAX_ENTITIES: u32 = 16_777_216;
const DATA_HEADER: usize = 12;
const DEFAULT_DATA_SIZE: usize = 64 * 1024 * 1024; // 64MB

pub struct ContentStore {
    index_mmap: UnsafeCell<MmapMut>,
    data_mmap: UnsafeCell<MmapMut>,
    data_end: AtomicU32,
}

unsafe impl Sync for ContentStore {}
unsafe impl Send for ContentStore {}

impl ContentStore {
    pub fn create(dir: &str) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;

        // index: MAX_ENTITIES * MAX_KEYS * 8B
        let index_size = INDEX_HEADER + (MAX_ENTITIES as usize) * (MAX_KEYS as usize) * 8;
        let idx_file = OpenOptions::new().read(true).write(true).create(true).truncate(true)
            .open(format!("{dir}/index.dat"))?;
        idx_file.set_len(index_size as u64)?;
        let mut idx_mmap = unsafe { MmapMut::map_mut(&idx_file)? };
        idx_mmap[0..4].copy_from_slice(&MAGIC);

        // data: append-only
        let dat_file = OpenOptions::new().read(true).write(true).create(true).truncate(true)
            .open(format!("{dir}/data.dat"))?;
        dat_file.set_len(DEFAULT_DATA_SIZE as u64)?;
        let mut dat_mmap = unsafe { MmapMut::map_mut(&dat_file)? };
        dat_mmap[0..4].copy_from_slice(&MAGIC);
        // data_end at offset 4
        dat_mmap[4..8].copy_from_slice(&(DATA_HEADER as u32).to_le_bytes());

        Ok(Self {
            index_mmap: UnsafeCell::new(idx_mmap),
            data_mmap: UnsafeCell::new(dat_mmap),
            data_end: AtomicU32::new(DATA_HEADER as u32),
        })
    }

    pub fn open(dir: &str) -> io::Result<Self> {
        let idx_file = OpenOptions::new().read(true).write(true).open(format!("{dir}/index.dat"))?;
        let idx_mmap = unsafe { MmapMut::map_mut(&idx_file)? };

        let dat_file = OpenOptions::new().read(true).write(true).open(format!("{dir}/data.dat"))?;
        let dat_mmap = unsafe { MmapMut::map_mut(&dat_file)? };
        let data_end = u32::from_le_bytes(dat_mmap[4..8].try_into().unwrap());

        Ok(Self {
            index_mmap: UnsafeCell::new(idx_mmap),
            data_mmap: UnsafeCell::new(dat_mmap),
            data_end: AtomicU32::new(data_end),
        })
    }

    fn key_hash(key: &str) -> u32 {
        let mut h = 0x811c9dc5u32;
        for b in key.bytes() { h ^= b as u32; h = h.wrapping_mul(0x01000193); }
        h % MAX_KEYS
    }

    fn index_offset(eid: u32, key_hash: u32) -> usize {
        INDEX_HEADER + ((eid as usize) * (MAX_KEYS as usize) + key_hash as usize) * 8
    }

    /// コンテンツを格納。
    pub fn set(&self, eid: u32, key: &str, content: &[u8]) {
        let kh = Self::key_hash(key);
        let off = Self::index_offset(eid, kh);

        // data に append
        let len = content.len() as u32;
        let data_off = self.data_end.fetch_add(len, Ordering::Relaxed);
        let dm = unsafe { &mut *self.data_mmap.get() };
        if (data_off + len) as usize <= dm.len() {
            dm[data_off as usize..(data_off + len) as usize].copy_from_slice(content);
        }

        // index に (offset, len) を書く
        let im = unsafe { &mut *self.index_mmap.get() };
        if off + 8 <= im.len() {
            im[off..off + 4].copy_from_slice(&data_off.to_le_bytes());
            im[off + 4..off + 8].copy_from_slice(&len.to_le_bytes());
        }
    }

    /// コンテンツを取得。
    pub fn get(&self, eid: u32, key: &str) -> Option<&[u8]> {
        let kh = Self::key_hash(key);
        let off = Self::index_offset(eid, kh);

        let im = unsafe { &*self.index_mmap.get() };
        if off + 8 > im.len() { return None; }

        let data_off = u32::from_le_bytes(im[off..off + 4].try_into().unwrap()) as usize;
        let len = u32::from_le_bytes(im[off + 4..off + 8].try_into().unwrap()) as usize;
        if data_off == 0 || len == 0 { return None; }

        let dm = unsafe { &*self.data_mmap.get() };
        if data_off + len > dm.len() { return None; }
        Some(&dm[data_off..data_off + len])
    }

    /// コンテンツを削除。
    pub fn remove(&self, eid: u32, key: &str) {
        let kh = Self::key_hash(key);
        let off = Self::index_offset(eid, kh);
        let im = unsafe { &mut *self.index_mmap.get() };
        if off + 8 <= im.len() {
            im[off..off + 8].fill(0);
        }
    }

    pub fn flush(&self) -> io::Result<()> {
        // data_end を data_mmap のヘッダに書き戻す
        let end = self.data_end.load(Ordering::Relaxed);
        let dm = unsafe { &mut *self.data_mmap.get() };
        dm[4..8].copy_from_slice(&end.to_le_bytes());

        let im = unsafe { &*self.index_mmap.get() };
        im.flush()?;
        dm.flush()?;
        Ok(())
    }
}
