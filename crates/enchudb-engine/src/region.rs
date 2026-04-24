//! Region — 単一mmapファイル内のスライス。ロックなし。

use std::sync::atomic::AtomicU32;

pub struct Region {
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for Region {}
unsafe impl Sync for Region {}

impl Region {
    /// # Safety
    /// ptr must point to a valid mutable memory region of at least `len` bytes
    /// that outlives this Region.
    pub unsafe fn new(ptr: *mut u8, len: usize) -> Self {
        Self { ptr, len }
    }

    #[inline(always)]
    pub fn slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    #[inline(always)]
    pub fn slice_mut(&self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize { self.len }

    /// mmap 上の 4 byte を `AtomicU32` として参照する。
    ///
    /// `MAP_SHARED` で map された region は複数プロセスで **同じ物理ページ** を共有するので、
    /// ここに atomic u32 を置くと `fetch_add` 等の操作がプロセス跨ぎで整合する。
    /// cross-process counter (ContentStore::data_end 等) 用。
    ///
    /// # Panics
    /// - `offset + 4 > len` なら panic
    /// - `offset % 4 != 0` なら panic(AtomicU32 は 4 byte aligned 要求)
    #[inline(always)]
    pub fn as_atomic_u32(&self, offset: usize) -> &AtomicU32 {
        assert!(offset + 4 <= self.len, "atomic_u32 out of range: {} + 4 > {}", offset, self.len);
        assert!(offset % 4 == 0, "atomic_u32 offset {} not 4-byte aligned", offset);
        unsafe { &*(self.ptr.add(offset) as *const AtomicU32) }
    }
}
