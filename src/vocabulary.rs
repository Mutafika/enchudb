//! Vocabulary — ユニーク値辞書。Symbol の値を value_id (u32) に変換。

use std::cell::UnsafeCell;
use std::fs::OpenOptions;
use std::io;
use std::sync::atomic::{AtomicU32, Ordering};
use memmap2::MmapMut;

const MAGIC: [u8; 4] = [b'V', b'O', b'C', b'1'];
const HEADER: usize = 16;
const DEFAULT_DATA_SIZE: usize = 64 * 1024 * 1024;
const INDEX_MAGIC: [u8; 4] = [b'V', b'I', b'X', b'2'];
const INDEX_HEADER: usize = 16;
const DEFAULT_INDEX_CAP: u32 = 131072;
const INDEX_SLOT_SIZE: usize = 13;

pub struct Vocabulary {
    data_mmap: UnsafeCell<MmapMut>,
    count: AtomicU32,
    data_end: AtomicU32,
    offsets_mmap: UnsafeCell<MmapMut>,
    max_entries: u32,
    index_mmap: UnsafeCell<MmapMut>,
    index_cap: u32,
}

unsafe impl Sync for Vocabulary {}
unsafe impl Send for Vocabulary {}

impl Vocabulary {
    pub fn create(dir: &str) -> io::Result<Self> {
        Self::create_with_params(dir, 16_777_216, 256, DEFAULT_INDEX_CAP)
    }

    pub fn create_with_params(dir: &str, max_entries: u32, _max_value_len: u32, index_cap: u32) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;

        let offsets_path = format!("{dir}/offsets.dat");
        let offsets_size = (max_entries as usize) * 8;
        let of = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&offsets_path)?;
        of.set_len(offsets_size as u64)?;
        let offsets_mmap = unsafe { MmapMut::map_mut(&of)? };

        let data_path = format!("{dir}/data.dat");
        let df = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&data_path)?;
        df.set_len(DEFAULT_DATA_SIZE as u64)?;
        let mut data_mmap = unsafe { MmapMut::map_mut(&df)? };
        data_mmap[0..4].copy_from_slice(&MAGIC);
        data_mmap[4..8].copy_from_slice(&0u32.to_le_bytes());
        data_mmap[8..12].copy_from_slice(&(HEADER as u32).to_le_bytes());
        data_mmap.flush()?;

        let index_cap = index_cap.next_power_of_two();
        let index_path = format!("{dir}/index.dat");
        let index_size = INDEX_HEADER + (index_cap as usize) * INDEX_SLOT_SIZE;
        let xf = OpenOptions::new().read(true).write(true).create(true).truncate(true).open(&index_path)?;
        xf.set_len(index_size as u64)?;
        let mut index_mmap = unsafe { MmapMut::map_mut(&xf)? };
        index_mmap[0..4].copy_from_slice(&INDEX_MAGIC);
        index_mmap[4..8].copy_from_slice(&index_cap.to_le_bytes());
        index_mmap.flush()?;

        Ok(Self {
            data_mmap: UnsafeCell::new(data_mmap),
            count: AtomicU32::new(0),
            data_end: AtomicU32::new(HEADER as u32),
            offsets_mmap: UnsafeCell::new(offsets_mmap),
            max_entries,
            index_mmap: UnsafeCell::new(index_mmap),
            index_cap,
        })
    }

    pub fn open(dir: &str) -> io::Result<Self> {
        let data_path = format!("{dir}/data.dat");
        let df = OpenOptions::new().read(true).write(true).open(&data_path)?;
        let data_mmap = unsafe { MmapMut::map_mut(&df)? };
        let count = u32::from_le_bytes(data_mmap[4..8].try_into().unwrap());
        let data_end = u32::from_le_bytes(data_mmap[8..12].try_into().unwrap());

        let offsets_path = format!("{dir}/offsets.dat");
        let of = OpenOptions::new().read(true).write(true).open(&offsets_path)?;
        let offsets_mmap = unsafe { MmapMut::map_mut(&of)? };
        let max_entries = (offsets_mmap.len() / 8) as u32;

        let index_path = format!("{dir}/index.dat");
        let xf = OpenOptions::new().read(true).write(true).open(&index_path)?;
        let index_mmap = unsafe { MmapMut::map_mut(&xf)? };
        let index_cap = u32::from_le_bytes(index_mmap[4..8].try_into().unwrap());

        Ok(Self {
            data_mmap: UnsafeCell::new(data_mmap),
            count: AtomicU32::new(count),
            data_end: AtomicU32::new(data_end),
            offsets_mmap: UnsafeCell::new(offsets_mmap),
            max_entries,
            index_mmap: UnsafeCell::new(index_mmap),
            index_cap,
        })
    }

    pub fn get_or_insert(&self, value: &[u8]) -> u32 {
        if let Some(id) = self.lookup(value) { return id; }
        self.insert(value)
    }

    #[inline]
    pub fn get(&self, id: u32) -> &[u8] {
        let om = unsafe { &*self.offsets_mmap.get() };
        let off_pos = (id as usize) * 8;
        let offset = u32::from_le_bytes(om[off_pos..off_pos + 4].try_into().unwrap()) as usize;
        let len = u32::from_le_bytes(om[off_pos + 4..off_pos + 8].try_into().unwrap()) as usize;
        let dm = unsafe { &*self.data_mmap.get() };
        &dm[offset..offset + len]
    }

    #[inline]
    pub fn lookup(&self, value: &[u8]) -> Option<u32> {
        let mask = (self.index_cap - 1) as u64;
        let xm = unsafe { &*self.index_mmap.get() };
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
        let dm = unsafe { &mut *self.data_mmap.get() };
        dm[offset as usize..offset as usize + len as usize].copy_from_slice(value);
        let om = unsafe { &mut *self.offsets_mmap.get() };
        let off_pos = (id as usize) * 8;
        om[off_pos..off_pos + 4].copy_from_slice(&offset.to_le_bytes());
        om[off_pos + 4..off_pos + 8].copy_from_slice(&len.to_le_bytes());
        self.index_insert(value, id);
        id
    }

    fn index_insert(&self, value: &[u8], id: u32) {
        let mask = (self.index_cap - 1) as u64;
        let xm = unsafe { &mut *self.index_mmap.get() };
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
            if slot_hash == h { return; }
            idx = ((idx as u64 + 1) & mask) as usize;
        }
    }

    pub fn count(&self) -> u32 { self.count.load(Ordering::Relaxed) }

    pub fn flush(&self) -> io::Result<()> {
        let dm = unsafe { &mut *self.data_mmap.get() };
        dm[4..8].copy_from_slice(&self.count.load(Ordering::Relaxed).to_le_bytes());
        dm[8..12].copy_from_slice(&self.data_end.load(Ordering::Relaxed).to_le_bytes());
        unsafe {
            (*self.data_mmap.get()).flush()?;
            (*self.offsets_mmap.get()).flush()?;
            (*self.index_mmap.get()).flush()?;
        }
        Ok(())
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
