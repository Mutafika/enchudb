//! EntitySet — mmap ビットセット + 空きID管理。
//!
//! entity の生存管理。allocate / free / is_live / iter。
//! AtomicU32 で next_eid を管理。ロック不要。
//!
//! Layout:
//!   Header (16B): [magic:4][next_eid:AtomicU32][live_count:AtomicU32][reserved:4]
//!   Bitset: ceil(max_entities / 8) bytes — bit=1 で live
//!   Free stack: [count:4][eid0:4][eid1:4]...

use std::cell::UnsafeCell;
use std::fs::OpenOptions;
use std::io;
use std::sync::atomic::{AtomicU32, Ordering};
use memmap2::MmapMut;

const MAGIC: [u8; 4] = [b'E', b'N', b'T', b'1'];
const HEADER: usize = 16;
const DEFAULT_MAX: u32 = 16_777_216; // 16M entities
const FREE_STACK_MAX: u32 = 1_048_576; // 1M free slots

pub struct EntitySet {
    mmap: UnsafeCell<MmapMut>,
    max_entities: u32,
    bitset_offset: usize,
    free_offset: usize,
}

unsafe impl Sync for EntitySet {}
unsafe impl Send for EntitySet {}

impl EntitySet {
    pub fn create(path: &str) -> io::Result<Self> {
        Self::create_with(path, DEFAULT_MAX)
    }

    pub fn create_with(path: &str, max_entities: u32) -> io::Result<Self> {
        let bitset_offset = HEADER;
        let bitset_size = ((max_entities + 7) / 8) as usize;
        let free_offset = bitset_offset + bitset_size;
        let free_size = 4 + (FREE_STACK_MAX as usize) * 4;
        let total = free_offset + free_size;

        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path)?;
        file.set_len(total as u64)?;

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        mmap[0..4].copy_from_slice(&MAGIC);
        // next_eid = 0, live_count = 0
        mmap.flush()?;

        Ok(Self { mmap: UnsafeCell::new(mmap), max_entities, bitset_offset, free_offset })
    }

    pub fn open(path: &str) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        if mmap.len() < HEADER || mmap[0..4] != MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "bad entity set magic"));
        }
        let max_entities = DEFAULT_MAX; // TODO: store in header
        let bitset_offset = HEADER;
        let bitset_size = ((max_entities + 7) / 8) as usize;
        let free_offset = bitset_offset + bitset_size;
        Ok(Self { mmap: UnsafeCell::new(mmap), max_entities, bitset_offset, free_offset })
    }

    fn next_eid_atomic(&self) -> &AtomicU32 {
        let mm = unsafe { &*self.mmap.get() };
        unsafe { &*(mm.as_ptr().add(4) as *const AtomicU32) }
    }

    fn live_count_atomic(&self) -> &AtomicU32 {
        let mm = unsafe { &*self.mmap.get() };
        unsafe { &*(mm.as_ptr().add(8) as *const AtomicU32) }
    }

    /// entity を確保。空きがあれば再利用、なければ新規 ID。
    pub fn allocate(&self) -> u32 {
        let mm = unsafe { &mut *self.mmap.get() };

        // free stack から pop
        let free_count_off = self.free_offset;
        let fc = u32::from_le_bytes(mm[free_count_off..free_count_off + 4].try_into().unwrap());
        if fc > 0 {
            let new_fc = fc - 1;
            let eid_off = self.free_offset + 4 + (new_fc as usize) * 4;
            let eid = u32::from_le_bytes(mm[eid_off..eid_off + 4].try_into().unwrap());
            mm[free_count_off..free_count_off + 4].copy_from_slice(&new_fc.to_le_bytes());
            self.set_bit(eid, true);
            self.live_count_atomic().fetch_add(1, Ordering::Relaxed);
            return eid;
        }

        // 新規 ID
        let eid = self.next_eid_atomic().fetch_add(1, Ordering::Relaxed);
        self.set_bit(eid, true);
        self.live_count_atomic().fetch_add(1, Ordering::Relaxed);
        eid
    }

    /// entity を解放。
    pub fn free(&self, eid: u32) {
        if !self.is_live(eid) { return; }
        self.set_bit(eid, false);
        self.live_count_atomic().fetch_sub(1, Ordering::Relaxed);

        // free stack に push
        let mm = unsafe { &mut *self.mmap.get() };
        let free_count_off = self.free_offset;
        let fc = u32::from_le_bytes(mm[free_count_off..free_count_off + 4].try_into().unwrap());
        if fc < FREE_STACK_MAX {
            let eid_off = self.free_offset + 4 + (fc as usize) * 4;
            mm[eid_off..eid_off + 4].copy_from_slice(&eid.to_le_bytes());
            mm[free_count_off..free_count_off + 4].copy_from_slice(&(fc + 1).to_le_bytes());
        }
    }

    /// entity が生存しているか。
    #[inline]
    pub fn is_live(&self, eid: u32) -> bool {
        if eid >= self.max_entities { return false; }
        let mm = unsafe { &*self.mmap.get() };
        let byte_off = self.bitset_offset + (eid / 8) as usize;
        let bit = 1u8 << (eid % 8);
        (mm[byte_off] & bit) != 0
    }

    fn set_bit(&self, eid: u32, live: bool) {
        if eid >= self.max_entities { return; }
        let mm = unsafe { &mut *self.mmap.get() };
        let byte_off = self.bitset_offset + (eid / 8) as usize;
        let bit = 1u8 << (eid % 8);
        if live {
            mm[byte_off] |= bit;
        } else {
            mm[byte_off] &= !bit;
        }
    }

    /// live entity 数。
    pub fn count(&self) -> u32 {
        self.live_count_atomic().load(Ordering::Relaxed)
    }

    /// next_eid（最大 ID + 1）。
    pub fn next_eid(&self) -> u32 {
        self.next_eid_atomic().load(Ordering::Relaxed)
    }

    /// 全 live entity を返す。
    pub fn iter(&self) -> Vec<u32> {
        let mm = unsafe { &*self.mmap.get() };
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

    pub fn flush(&self) -> io::Result<()> {
        let mm = unsafe { &*self.mmap.get() };
        mm.flush()
    }
}
