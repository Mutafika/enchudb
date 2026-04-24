//! Region — 単一mmapファイル内のスライス。ロックなし。

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
}
