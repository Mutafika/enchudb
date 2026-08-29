//! `SegmentMap` の Windows 版 ([[request21]] / v10 Phase 0)。 unix 版 (`segment_map.rs`) と
//! 同じ API を **placeholder API** (Win10 1803+ / Server 2019+) で実装する:
//!
//! - `VirtualAlloc2(MEM_RESERVE | MEM_RESERVE_PLACEHOLDER)` で予約 (= unix の `PROT_NONE`)
//! - 伸長は **末尾に extent を足す**: placeholder を `VirtualFree(MEM_PRESERVE_PLACEHOLDER)`
//!   で分割し、 `MapViewOfFile3(MEM_REPLACE_PLACEHOLDER)` で file view を差し込む。
//!   既存 view には触らないので、 unix 版が全域 remap するのと違って **他 thread の
//!   lock-free read と競合する窓が無い** (unix は kernel が in-place で差し替えるので不要)
//! - view の file offset は allocation granularity (64 KB) 単位。 commit もその単位
//! - `refresh` は reader 側が writer の伸長を拾う経路 (unix と同じ契約: 縮めない)
//!
//! 旧 `growable_map_stub.rs` が Windows を非対応にしていた理由 (`MapViewOfFileEx` が
//! 予約領域の中に置けない) は、 この API で解消する。
//!
//! **本 file は macOS 上で `--target x86_64-pc-windows-msvc` の compile check のみ**。
//! 実機 (Windows) での runtime 検証は Phase 1 の完了条件に含める。

use std::fs::{File, OpenOptions};
use std::io;
use std::os::windows::io::AsRawHandle;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;
use windows_sys::Win32::System::Memory::{
    CreateFileMappingW, FlushViewOfFile, MapViewOfFile3, UnmapViewOfFile2, VirtualAlloc2,
    VirtualFree, MEMORY_MAPPED_VIEW_ADDRESS, MEM_PRESERVE_PLACEHOLDER, MEM_RELEASE,
    MEM_REPLACE_PLACEHOLDER, MEM_RESERVE, MEM_RESERVE_PLACEHOLDER, PAGE_NOACCESS, PAGE_READONLY,
    PAGE_READWRITE,
};
use windows_sys::Win32::System::SystemServices::MEM_COALESCE_PLACEHOLDERS;
use windows_sys::Win32::System::SystemInformation::{GetSystemInfo, SYSTEM_INFO};
use windows_sys::Win32::System::Threading::GetCurrentProcess;

const SPACE_MARGIN: u64 = 32 * 1024 * 1024;

/// allocation granularity (通常 64 KB)。 view の file offset / 長さはこの単位。
fn granularity() -> usize {
    use std::sync::atomic::AtomicUsize as A;
    static CACHED: A = A::new(0);
    let cur = CACHED.load(Ordering::Relaxed);
    if cur != 0 {
        return cur;
    }
    let mut si: SYSTEM_INFO = unsafe { std::mem::zeroed() };
    unsafe { GetSystemInfo(&mut si) };
    let g = (si.dwAllocationGranularity as usize).max(4096);
    CACHED.store(g, Ordering::Relaxed);
    g
}

fn align_up(v: usize, a: usize) -> usize {
    (v + a - 1) & !(a - 1)
}

fn wide(p: &Path) -> Vec<u16> {
    use std::os::windows::ffi::OsStrExt;
    p.as_os_str().encode_wide().chain(std::iter::once(0)).collect()
}

fn free_bytes_for_path(p: &Path) -> io::Result<u64> {
    let dir = p.parent().filter(|d| !d.as_os_str().is_empty()).unwrap_or(Path::new("."));
    let w = wide(dir);
    let mut avail: u64 = 0;
    let ok = unsafe { GetDiskFreeSpaceExW(w.as_ptr(), &mut avail, ptr::null_mut(), ptr::null_mut()) };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(avail)
}

pub struct SegmentMap {
    path: PathBuf,
    base: *mut u8,
    reserved: usize,
    committed: AtomicUsize,
    readonly: bool,
    /// 伸長の直列化 + map 済み extent (start, len) の台帳 (Drop で unmap する)。
    grow_lock: Mutex<Vec<(usize, usize)>>,
    dirty_lo: AtomicUsize,
    dirty_hi: AtomicUsize,
    space_margin: AtomicU64,
    space_denials: AtomicU64,
}

unsafe impl Send for SegmentMap {}
unsafe impl Sync for SegmentMap {}

impl std::fmt::Debug for SegmentMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentMap")
            .field("path", &self.path)
            .field("reserved", &self.reserved)
            .field("committed", &self.committed())
            .field("readonly", &self.readonly)
            .finish()
    }
}

impl SegmentMap {
    pub fn create(path: &Path, reserve: usize, initial: usize) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(true).create_new(true).open(path)?;
        let g = granularity();
        let reserve = align_up(reserve.max(g), g);
        let initial = align_up(initial.max(g), g);
        if initial > reserve {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "initial > reserve"));
        }
        file.set_len(initial as u64)?;
        Self::map_new(path.to_path_buf(), file, reserve, initial, false)
    }

    pub fn open(path: &Path, reserve: usize, readonly: bool) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(!readonly).open(path)?;
        let g = granularity();
        let reserve = align_up(reserve.max(g), g);
        let len = file.metadata()?.len() as usize;
        // writer は常に granularity 単位で伸ばす。 readonly で端数があれば切り捨て
        // (view を EOF より先に張れないため)、 writer なら切り上げて set_len する。
        let committed = if readonly { (len / g) * g } else { align_up(len.max(g), g) };
        if committed > reserve {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("segment {} is {len} bytes, larger than reservation {reserve}", path.display()),
            ));
        }
        if !readonly && (committed as u64) > len as u64 {
            file.set_len(committed as u64)?;
        }
        Self::map_new(path.to_path_buf(), file, reserve, committed, readonly)
    }

    fn map_new(path: PathBuf, file: File, reserve: usize, committed: usize, readonly: bool) -> io::Result<Self> {
        let base = unsafe {
            VirtualAlloc2(
                GetCurrentProcess(),
                ptr::null(),
                reserve,
                MEM_RESERVE | MEM_RESERVE_PLACEHOLDER,
                PAGE_NOACCESS,
                ptr::null_mut(),
                0,
            )
        };
        if base.is_null() {
            return Err(io::Error::last_os_error());
        }
        let me = Self {
            path,
            base: base as *mut u8,
            reserved: reserve,
            committed: AtomicUsize::new(0),
            readonly,
            grow_lock: Mutex::new(Vec::new()),
            dirty_lo: AtomicUsize::new(usize::MAX),
            dirty_hi: AtomicUsize::new(0),
            space_margin: AtomicU64::new(SPACE_MARGIN),
            space_denials: AtomicU64::new(0),
        };
        if committed > 0 {
            let mut extents = me.grow_lock.lock().unwrap_or_else(|p| p.into_inner());
            me.map_extent(&file, 0, committed, &mut extents)?;
            me.committed.store(committed, Ordering::Release);
        }
        drop(file);
        Ok(me)
    }

    fn reopen(&self) -> io::Result<File> {
        OpenOptions::new().read(true).write(!self.readonly).open(&self.path)
    }

    /// [start, start+len) の placeholder を file view に差し替える (lock 下)。
    fn map_extent(&self, file: &File, start: usize, len: usize, extents: &mut Vec<(usize, usize)>) -> io::Result<()> {
        if len == 0 {
            return Ok(());
        }
        let g = granularity();
        debug_assert!(start % g == 0 && len % g == 0 && start + len <= self.reserved);
        let addr = unsafe { self.base.add(start) } as *mut core::ffi::c_void;
        // placeholder [start, reserved) を [start, start+len) と残りに分割する。
        // ぴったり残り全部なら分割不要。
        if start + len < self.reserved {
            let ok = unsafe { VirtualFree(addr, len, MEM_RELEASE | MEM_PRESERVE_PLACEHOLDER) };
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
        }
        let file_len = file.metadata()?.len();
        let protect = if self.readonly { PAGE_READONLY } else { PAGE_READWRITE };
        let mapping: HANDLE = unsafe {
            CreateFileMappingW(
                file.as_raw_handle() as HANDLE,
                ptr::null(),
                protect,
                (file_len >> 32) as u32,
                (file_len & 0xFFFF_FFFF) as u32,
                ptr::null(),
            )
        };
        if mapping.is_null() {
            return Err(io::Error::last_os_error());
        }
        let view: MEMORY_MAPPED_VIEW_ADDRESS = unsafe {
            MapViewOfFile3(
                mapping,
                GetCurrentProcess(),
                addr,
                start as u64,
                len,
                MEM_REPLACE_PLACEHOLDER,
                protect,
                ptr::null_mut(),
                0,
            )
        };
        let err = io::Error::last_os_error();
        unsafe { CloseHandle(mapping) };
        if view.Value.is_null() {
            return Err(err);
        }
        extents.push((start, len));
        Ok(())
    }

    pub fn grow_to(&self, new_size: usize) -> io::Result<()> {
        let g = granularity();
        let aligned = align_up(new_size, g);
        if aligned <= self.committed.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.readonly {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "readonly segment cannot grow (use refresh to follow the writer)",
            ));
        }
        let mut extents = self.grow_lock.lock().unwrap_or_else(|p| p.into_inner());
        let cur = self.committed.load(Ordering::Acquire);
        if aligned <= cur {
            return Ok(());
        }
        if aligned > self.reserved {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("grow {aligned} exceeds reservation {} ({})", self.reserved, self.path.display()),
            ));
        }
        let file = self.reopen()?;
        let file_len = file.metadata()?.len();
        if file_len < aligned as u64 {
            let delta = aligned as u64 - file_len;
            if let Ok(free) = free_bytes_for_path(&self.path) {
                let margin = self.space_margin.load(Ordering::Relaxed);
                if free < delta.saturating_add(margin) {
                    self.space_denials.fetch_add(1, Ordering::Relaxed);
                    return Err(io::Error::new(
                        io::ErrorKind::StorageFull,
                        format!(
                            "refusing to grow segment {}: {free} bytes free, need {delta} + {margin} margin (#167)",
                            self.path.display()
                        ),
                    ));
                }
            }
            file.set_len(aligned as u64)?;
        }
        self.map_extent(&file, cur, aligned - cur, &mut extents)?;
        self.committed.store(aligned, Ordering::Release);
        Ok(())
    }

    pub fn grow_amortized(&self, needed: usize) -> io::Result<()> {
        let g = granularity();
        let cur = self.committed.load(Ordering::Acquire);
        let needed_aligned = align_up(needed, g);
        if needed_aligned <= cur {
            return Ok(());
        }
        const SMALL_THRESHOLD: usize = 1024 * 1024;
        const LINEAR_CHUNK: usize = 1024 * 1024;
        let target = if cur < SMALL_THRESHOLD {
            let doubled = cur.saturating_mul(2).max(cur + 64 * 1024);
            doubled.max(needed_aligned)
        } else {
            (cur + LINEAR_CHUNK).max(needed_aligned)
        }
        .min(self.reserved);
        if target < needed_aligned {
            return Err(io::Error::new(io::ErrorKind::OutOfMemory, "needed exceeds reservation"));
        }
        self.grow_to(target)
    }

    pub fn refresh(&self) -> io::Result<usize> {
        let g = granularity();
        let mut extents = self.grow_lock.lock().unwrap_or_else(|p| p.into_inner());
        let file = self.reopen()?;
        let len = file.metadata()?.len() as usize;
        let target = ((len / g) * g).min(self.reserved);
        let cur = self.committed.load(Ordering::Acquire);
        if target <= cur {
            return Ok(cur);
        }
        self.map_extent(&file, cur, target - cur, &mut extents)?;
        self.committed.store(target, Ordering::Release);
        Ok(target)
    }

    pub fn base(&self) -> *mut u8 {
        self.base
    }
    pub fn committed(&self) -> usize {
        self.committed.load(Ordering::Acquire)
    }
    pub fn reserved(&self) -> usize {
        self.reserved
    }
    pub fn is_readonly(&self) -> bool {
        self.readonly
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn file_len(&self) -> io::Result<u64> {
        Ok(std::fs::metadata(&self.path)?.len())
    }
    pub fn set_space_margin(&self, bytes: u64) {
        self.space_margin.store(bytes, Ordering::Relaxed);
    }
    pub fn space_denials(&self) -> u64 {
        self.space_denials.load(Ordering::Relaxed)
    }
    pub fn free_bytes(&self) -> io::Result<u64> {
        free_bytes_for_path(&self.path)
    }

    /// [offset, offset+len) を disk へ。 view 境界を跨ぐ範囲は view ごとに
    /// `FlushViewOfFile` し、 最後に `FlushFileBuffers` (= unix の MS_SYNC 相当)。
    pub fn flush(&self, offset: usize, len: usize) -> io::Result<()> {
        let end = offset + len;
        if end > self.committed() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "flush past committed"));
        }
        if len == 0 {
            return Ok(());
        }
        let extents = self.grow_lock.lock().unwrap_or_else(|p| p.into_inner());
        for &(s, l) in extents.iter() {
            let lo = offset.max(s);
            let hi = end.min(s + l);
            if hi > lo {
                let ok = unsafe { FlushViewOfFile(self.base.add(lo) as *const _, hi - lo) };
                if ok == 0 {
                    return Err(io::Error::last_os_error());
                }
            }
        }
        drop(extents);
        if !self.readonly {
            self.reopen()?.sync_data()?;
        }
        Ok(())
    }

    pub fn flush_aligned(&self, offset: usize, len: usize) -> io::Result<()> {
        if len == 0 {
            return Ok(());
        }
        let ps = 4096;
        let lo = offset & !(ps - 1);
        let hi = align_up(offset + len, ps).min(self.committed());
        if hi <= lo {
            return Ok(());
        }
        self.flush(lo, hi - lo)
    }

    pub fn flush_all(&self) -> io::Result<()> {
        self.flush(0, self.committed())
    }

    #[inline]
    pub fn mark_dirty(&self, offset: usize, len: usize) {
        if len == 0 {
            return;
        }
        let end = offset + len;
        if self.dirty_lo.load(Ordering::Relaxed) <= offset && self.dirty_hi.load(Ordering::Relaxed) >= end {
            return;
        }
        self.dirty_lo.fetch_min(offset, Ordering::Release);
        self.dirty_hi.fetch_max(end, Ordering::Release);
    }

    pub fn flush_dirty(&self) -> io::Result<()> {
        let lo = self.dirty_lo.swap(usize::MAX, Ordering::AcqRel);
        let hi = self.dirty_hi.swap(0, Ordering::AcqRel);
        if hi <= lo {
            return Ok(());
        }
        self.flush_aligned(lo, hi - lo)
    }
}

impl Drop for SegmentMap {
    fn drop(&mut self) {
        let extents = std::mem::take(self.grow_lock.get_mut().unwrap_or_else(|p| p.into_inner()));
        unsafe {
            // view を placeholder に戻す → 隣接 placeholder を 1 つに併合 → 解放
            for (s, _) in extents {
                let v = MEMORY_MAPPED_VIEW_ADDRESS { Value: self.base.add(s) as *mut _ };
                UnmapViewOfFile2(GetCurrentProcess(), v, MEM_PRESERVE_PLACEHOLDER);
            }
            VirtualFree(self.base as *mut _, self.reserved, MEM_RELEASE | MEM_COALESCE_PLACEHOLDERS);
            VirtualFree(self.base as *mut _, 0, MEM_RELEASE);
        }
    }
}
