//! `SegmentMap` の Windows 版 ([[request21]] / v10 Phase 1)。 unix 版 (`segment_map.rs`) と
//! 同じ API を **sparse file の全域 map** で実装する:
//!
//! - create 時に `FSCTL_SET_SPARSE` を立てて `set_len(reserve)` → 全域を map する。
//!   NTFS の sparse file は書いた cluster しか実消費しないので、 **物理は書いた分**、
//!   見かけ (apparent) は予約サイズになる (unix 版は見かけも書いた分)
//! - 全域 map なので base 不動 / read は 0 / write は常に可 (`grow_to` は簿記だけ)。
//!   `refresh` も no-op
//! - `growable_map_stub.rs` が Windows を非対応にしていた問題 (#245 の背景) はこれで解消
//!
//! placeholder API (`VirtualAlloc2` + `MapViewOfFile3`、 Win10 1803+) で見かけも書いた分に
//! する版は後続 (request21 open question 3)。 まず動く形を優先。
//!
//! **本 file は macOS 上で `--target x86_64-pc-windows-msvc` の compile check のみ**。
//! 実機 runtime 検証は Phase 1 の完了条件に含める。
//!
//! 既知の差: readonly open で file 長 < reserve のとき (unix writer が作った segment を
//! Windows reader が開く = network share 前提で unsupported) は file 長までしか map
//! できず、 その先の read は fault する。

use std::fs::{File, OpenOptions};
use std::io;
use std::os::windows::io::AsRawHandle;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use memmap2::{MmapMut, MmapOptions};
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW;
use windows_sys::Win32::System::Ioctl::FSCTL_SET_SPARSE;
use windows_sys::Win32::System::IO::DeviceIoControl;

const SPACE_MARGIN: u64 = 32 * 1024 * 1024;
const GRANULARITY: usize = 64 * 1024;

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

fn set_sparse(file: &File) -> io::Result<()> {
    let mut returned: u32 = 0;
    let ok = unsafe {
        DeviceIoControl(
            file.as_raw_handle() as HANDLE,
            FSCTL_SET_SPARSE,
            ptr::null(),
            0,
            ptr::null_mut(),
            0,
            &mut returned,
            ptr::null_mut(),
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

pub struct SegmentMap {
    path: PathBuf,
    map: MmapMut,
    reserved: usize,
    committed: AtomicUsize,
    readonly: bool,
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
        let reserve = align_up(reserve.max(GRANULARITY), GRANULARITY);
        if align_up(initial, GRANULARITY) > reserve {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "initial > reserve"));
        }
        // sparse にしてから伸ばす (逆だと伸ばした分が実体化する fs がある)
        set_sparse(&file)?;
        file.set_len(reserve as u64)?;
        Self::map_whole(path.to_path_buf(), file, reserve, false)
    }

    pub fn open(path: &Path, reserve: usize, readonly: bool) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(!readonly).open(path)?;
        let reserve = align_up(reserve.max(GRANULARITY), GRANULARITY);
        let len = file.metadata()?.len() as usize;
        if len > reserve {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("segment {} is {len} bytes, larger than reservation {reserve}", path.display()),
            ));
        }
        if !readonly && len < reserve {
            let _ = set_sparse(&file);
            file.set_len(reserve as u64)?;
        }
        Self::map_whole(path.to_path_buf(), file, reserve, readonly)
    }

    fn map_whole(path: PathBuf, file: File, reserve: usize, readonly: bool) -> io::Result<Self> {
        let len = (file.metadata()?.len() as usize).min(reserve);
        let map = unsafe {
            if readonly {
                // readonly でも型を揃えるため map_copy (private COW) ではなく map_mut を
                // 読み取り専用 handle で開けないので、 書き込み handle 無しの環境では
                // MmapOptions::map_mut が失敗する。 その場合は copy-on-write で読む。
                match MmapOptions::new().len(len).map_mut(&file) {
                    Ok(m) => m,
                    Err(_) => MmapOptions::new().len(len).map_copy(&file)?,
                }
            } else {
                MmapOptions::new().len(len).map_mut(&file)?
            }
        };
        drop(file);
        Ok(Self {
            path,
            map,
            reserved: reserve,
            committed: AtomicUsize::new(len),
            readonly,
            dirty_lo: AtomicUsize::new(usize::MAX),
            dirty_hi: AtomicUsize::new(0),
            space_margin: AtomicU64::new(SPACE_MARGIN),
            space_denials: AtomicU64::new(0),
        })
    }

    /// 全域 map 済みなので簿記だけ。 予約超過は Err、 readonly で map 外は Err。
    pub fn grow_to(&self, new_size: usize) -> io::Result<()> {
        let aligned = align_up(new_size, GRANULARITY);
        if aligned <= self.committed.load(Ordering::Acquire) {
            return Ok(());
        }
        if aligned > self.reserved {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("grow {aligned} exceeds reservation {} ({})", self.reserved, self.path.display()),
            ));
        }
        if self.readonly {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "readonly segment cannot grow (use refresh to follow the writer)",
            ));
        }
        // #167 相当: sparse の穴を書く前に空きを見る (best-effort)。
        if let Ok(free) = free_bytes_for_path(&self.path) {
            let delta = (aligned - self.committed()) as u64;
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
        self.committed.store(aligned, Ordering::Release);
        Ok(())
    }

    pub fn grow_amortized(&self, needed: usize) -> io::Result<()> {
        self.grow_to(needed)
    }

    pub fn refresh(&self) -> io::Result<usize> {
        Ok(self.committed())
    }

    pub fn base(&self) -> *mut u8 {
        self.map.as_ptr() as *mut u8
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

    pub fn flush(&self, offset: usize, len: usize) -> io::Result<()> {
        if offset + len > self.map.len() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "flush past committed"));
        }
        if len == 0 || self.readonly {
            return Ok(());
        }
        self.map.flush_range(offset, len)
    }

    pub fn flush_aligned(&self, offset: usize, len: usize) -> io::Result<()> {
        if len == 0 {
            return Ok(());
        }
        let ps = 4096;
        let lo = offset & !(ps - 1);
        let hi = align_up(offset + len, ps).min(self.map.len());
        if hi <= lo {
            return Ok(());
        }
        self.flush(lo, hi - lo)
    }

    pub fn flush_all(&self) -> io::Result<()> {
        self.flush(0, self.map.len())
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
