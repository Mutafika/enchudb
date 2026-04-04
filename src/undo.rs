//! Undo ログ — 書き込み前の値を記録。COMMIT でクリア、クラッシュ時に自動復旧。
//!
//! Layout (mmap):
//!   Header (16B): [magic:4][count:u32][reserved:8]
//!   Entries (10B each): [eid:u32][dim_id:u16][old_value:4B]

use std::cell::UnsafeCell;
use std::fs::OpenOptions;
use std::io;
use std::sync::atomic::{AtomicU32, Ordering};
use memmap2::MmapMut;

const MAGIC: [u8; 4] = [b'U', b'N', b'D', b'1'];
const HEADER: usize = 16;
const ENTRY_SIZE: usize = 10; // eid:4 + dim_id:2 + old_value:4
const DEFAULT_MAX_ENTRIES: u32 = 16_777_216; // 16M entries ≈ 160MB

pub struct UndoLog {
    mmap: UnsafeCell<MmapMut>,
    count: AtomicU32,
}

unsafe impl Sync for UndoLog {}
unsafe impl Send for UndoLog {}

impl UndoLog {
    pub fn create(path: &str) -> io::Result<Self> {
        let total = HEADER + DEFAULT_MAX_ENTRIES as usize * ENTRY_SIZE;
        let file = OpenOptions::new()
            .read(true).write(true).create(true).truncate(true)
            .open(path)?;
        file.set_len(total as u64)?;

        let mut mmap = unsafe { MmapMut::map_mut(&file)? };
        mmap[0..4].copy_from_slice(&MAGIC);
        mmap[4..8].copy_from_slice(&0u32.to_le_bytes()); // count = 0

        Ok(Self {
            mmap: UnsafeCell::new(mmap),
            count: AtomicU32::new(0),
        })
    }

    pub fn open(path: &str) -> io::Result<Self> {
        let file = match OpenOptions::new().read(true).write(true).open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Self::create(path),
            Err(e) => return Err(e),
        };
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        if mmap.len() < HEADER || mmap[0..4] != MAGIC {
            drop(mmap);
            return Self::create(path);
        }

        let count = u32::from_le_bytes(mmap[4..8].try_into().unwrap());

        Ok(Self {
            mmap: UnsafeCell::new(mmap),
            count: AtomicU32::new(count),
        })
    }

    /// 元の値を記録。tie の前に呼ぶ。
    #[inline]
    pub fn record(&self, eid: u32, dim_id: u16, old_value: &[u8; 4]) {
        let idx = self.count.fetch_add(1, Ordering::Relaxed);
        let mm = unsafe { &mut *self.mmap.get() };
        let off = HEADER + idx as usize * ENTRY_SIZE;
        mm[off..off + 4].copy_from_slice(&eid.to_le_bytes());
        mm[off + 4..off + 6].copy_from_slice(&dim_id.to_le_bytes());
        mm[off + 6..off + 10].copy_from_slice(old_value);
        mm[4..8].copy_from_slice(&(idx + 1).to_le_bytes());
    }

    /// COMMIT — undo をクリア。
    #[inline]
    pub fn commit(&self) {
        self.count.store(0, Ordering::Release);
        let mm = unsafe { &mut *self.mmap.get() };
        mm[4..8].copy_from_slice(&0u32.to_le_bytes());
    }

    /// エントリ数。0 なら復旧不要。
    pub fn pending_count(&self) -> u32 {
        self.count.load(Ordering::Acquire)
    }

    /// 逆順にエントリを返す（復旧用）。
    pub fn entries_reverse(&self) -> Vec<(u32, u16, [u8; 4])> {
        let count = self.count.load(Ordering::Acquire) as usize;
        let mm = unsafe { &*self.mmap.get() };
        let mut result = Vec::with_capacity(count);
        for i in (0..count).rev() {
            let off = HEADER + i * ENTRY_SIZE;
            let eid = u32::from_le_bytes(mm[off..off + 4].try_into().unwrap());
            let dim_id = u16::from_le_bytes(mm[off + 4..off + 6].try_into().unwrap());
            let mut old_value = [0u8; 4];
            old_value.copy_from_slice(&mm[off + 6..off + 10]);
            result.push((eid, dim_id, old_value));
        }
        result
    }

    pub fn flush(&self) -> io::Result<()> {
        let mm = unsafe { &*self.mmap.get() };
        mm.flush()
    }
}
