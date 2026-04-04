//! Column — mmap 固定長カラム。LMDB方式で最初から大きく確保。
//!
//! remap なし。ロックなし。ensure_capacity なし。
//! 仮想アドレス空間だけ確保、物理メモリは書いたページ分だけ。

use std::cell::UnsafeCell;
use std::fs::OpenOptions;
use std::io;
use memmap2::MmapMut;

const HEADER: usize = 16;
const DEFAULT_MAX_ENTITIES: u32 = 16_777_216; // 16M

pub struct Column {
    mmap: UnsafeCell<MmapMut>,
    count: u32,
    value_size: u32,
    max_entities: u32,
}

unsafe impl Sync for Column {}
unsafe impl Send for Column {}

impl Column {
    pub fn create(path: &str, value_size: u32) -> io::Result<Self> {
        Self::create_with_max(path, value_size, DEFAULT_MAX_ENTITIES)
    }

    pub fn create_with_max(path: &str, value_size: u32, max_entities: u32) -> io::Result<Self> {
        let file_size = HEADER + (max_entities as usize) * (value_size as usize);
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path)?;
        file.set_len(file_size as u64)?;

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        mmap[0..4].copy_from_slice(&0u32.to_le_bytes());
        mmap[4..8].copy_from_slice(&value_size.to_le_bytes());
        mmap[8..12].copy_from_slice(&max_entities.to_le_bytes());

        Ok(Self { mmap: UnsafeCell::new(mmap), count: 0, value_size, max_entities })
    }

    pub fn open(path: &str) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        let count = u32::from_le_bytes(mmap[0..4].try_into().unwrap());
        let value_size = u32::from_le_bytes(mmap[4..8].try_into().unwrap());
        let max_entities = u32::from_le_bytes(mmap[8..12].try_into().unwrap());
        Ok(Self { mmap: UnsafeCell::new(mmap), count, value_size, max_entities })
    }

    #[inline]
    pub fn set(&self, entity_id: u32, value: &[u8]) {
        let vs = self.value_size as usize;
        let off = HEADER + (entity_id as usize) * vs;
        let len = value.len().min(vs);
        unsafe {
            let mmap = &mut *self.mmap.get();
            mmap[off..off + len].copy_from_slice(&value[..len]);
        }
    }

    #[inline]
    pub fn get(&self, entity_id: u32) -> &[u8] {
        let vs = self.value_size as usize;
        let off = HEADER + (entity_id as usize) * vs;
        unsafe {
            let mmap = &*self.mmap.get();
            &mmap[off..off + vs]
        }
    }

    #[inline]
    pub fn clear(&self, entity_id: u32) {
        let vs = self.value_size as usize;
        let off = HEADER + (entity_id as usize) * vs;
        unsafe {
            let mmap = &mut *self.mmap.get();
            for b in &mut mmap[off..off + vs] { *b = 0; }
        }
    }

    pub fn count(&self) -> u32 { self.count }

    pub fn write_count(&mut self, count: u32) {
        self.count = count;
        let mmap = self.mmap.get_mut();
        mmap[0..4].copy_from_slice(&count.to_le_bytes());
    }

    pub fn flush(&self) -> io::Result<()> {
        unsafe { (*self.mmap.get()).flush() }
    }
}
