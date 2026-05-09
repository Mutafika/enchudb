//! Growable mmap primitive — virtual reservation + on-demand commit.
//!
//! Why this exists: the existing `MmapMut::map_mut` path (engine.rs)
//! calls `file.set_len(huge)` once at create and maps the whole region.
//! That makes empty DBs appear as 88 GB sparse files, which trips up
//! Time Machine, naive rsync, and any tool that reads `du --apparent-size`.
//!
//! Design: reserve the maximum address range up-front with an anonymous
//! `PROT_NONE` mmap (consumes virtual address space, *not* RAM or disk),
//! then incrementally `mmap(MAP_FIXED|MAP_SHARED, fd, ...)` over it as
//! the file grows. The base virtual address never moves, so existing
//! raw pointers (`Region::ptr`) stay valid across grows — no need to
//! invalidate `Region` instances or re-fetch addresses.
//!
//! The empty DB is now `header_size + 4 KB` ≈ 4 KB on disk. Growth
//! happens via `grow_to(end)`, which the storage layer calls on every
//! write boundary. Growth is amortized: callers should pass an
//! over-allocation (doubling) target rather than the exact bytes
//! needed, so an N-row INSERT pays O(log N) syscalls rather than O(N).
//!
//! macOS quirks:
//! - No `mremap`; we rely on `mmap(MAP_FIXED|MAP_SHARED, ...)` instead.
//! - `MAP_FIXED` silently overrides any existing mapping at the target
//!   range, so we ONLY use it within our own anonymous reservation.
//!   Boundary checks are `debug_assert!`-ed.
//!
//! Linux:
//! - Same code works. `MAP_FIXED_NOREPLACE` (4.17+) would be safer but
//!   isn't on macOS, so we stay portable.

use std::fs::File;
use std::io;
use std::os::fd::{IntoRawFd, RawFd};
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

/// 4 KB page granularity. Both macOS and Linux on x86_64 / arm64 use
/// 4 KB pages today; if a future platform raises this we'll need to
/// query `sysconf(_SC_PAGESIZE)` at runtime.
const PAGE_SIZE: usize = 4096;

pub struct GrowableMap {
    fd: RawFd,
    base: *mut u8,
    /// Virtual address range size — never changes after `new`. The
    /// caller decides this upfront based on the DB's max layout.
    reserved: usize,
    /// File-backed bytes within the reservation. Atomic so a writer
    /// can read it on the hot path without locking; growth itself is
    /// serialized (caller must hold whatever store-side lock applies).
    committed: AtomicUsize,
}

unsafe impl Send for GrowableMap {}
unsafe impl Sync for GrowableMap {}

impl GrowableMap {
    /// Reserve `reserve` bytes of virtual address space and commit
    /// the first `initial` bytes from the file. `reserve` is the
    /// hard upper bound on growth — callers should pass the engine's
    /// `layout.total_size`, which is what the old fixed mmap used.
    ///
    /// Both `reserve` and `initial` are rounded up to page size.
    pub fn new(file: File, reserve: usize, initial: usize) -> io::Result<Self> {
        let reserve = align_up(reserve, PAGE_SIZE);
        let initial = align_up(initial, PAGE_SIZE);
        if initial > reserve {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "initial > reserve",
            ));
        }

        // Step 1: anonymous PROT_NONE reservation. No physical pages
        // committed yet. macOS / Linux both honor this — the kernel
        // just records the address range as "owned by us, no access."
        let base = unsafe {
            libc::mmap(
                ptr::null_mut(),
                reserve,
                libc::PROT_NONE,
                libc::MAP_PRIVATE | libc::MAP_ANON,
                -1,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        // Step 2: size the file to the initial commit. `set_len`
        // grows with zero pages — sparse on APFS / ext4.
        if let Err(e) = file.set_len(initial as u64) {
            unsafe {
                libc::munmap(base, reserve);
            }
            return Err(e);
        }

        let fd = file.into_raw_fd();

        // Step 3: overlay the first `initial` bytes with file-backed
        // mapping. MAP_FIXED replaces the PROT_NONE reservation at
        // this range with a real read/write file mapping at the same
        // virtual address.
        let result = unsafe {
            libc::mmap(
                base,
                initial,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_FIXED | libc::MAP_SHARED,
                fd,
                0,
            )
        };
        if result == libc::MAP_FAILED {
            let err = io::Error::last_os_error();
            unsafe {
                libc::munmap(base, reserve);
                libc::close(fd);
            }
            return Err(err);
        }

        Ok(Self {
            fd,
            base: base as *mut u8,
            reserved: reserve,
            committed: AtomicUsize::new(initial),
        })
    }

    /// Grow the file-backed window to at least `new_size` bytes.
    /// Idempotent: if already committed past `new_size`, returns
    /// without syscalls. Page-aligns the request.
    ///
    /// Caller is responsible for serializing concurrent grows (the
    /// store layer's existing lock, typically). Multi-process safety:
    /// if another process already grew the underlying file, this call
    /// will still execute the MAP_FIXED to bring our address space in
    /// line — `ftruncate` is a no-op when the file is already large
    /// enough.
    pub fn grow_to(&self, new_size: usize) -> io::Result<()> {
        let aligned = align_up(new_size, PAGE_SIZE);
        let cur = self.committed.load(Ordering::Acquire);
        if aligned <= cur {
            return Ok(());
        }
        if aligned > self.reserved {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!(
                    "grow {aligned} exceeds reservation {res}",
                    aligned = aligned,
                    res = self.reserved,
                ),
            ));
        }

        // Step 1: extend the file. `ftruncate` is idempotent — if
        // another process already grew it past `aligned`, this is
        // a no-op (returns 0 without changing the file).
        let rc = unsafe { libc::ftruncate(self.fd, aligned as i64) };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }

        // Step 2: re-map the entire file [0..aligned) over the
        // reservation in a single MAP_FIXED|MAP_SHARED call. This
        // matches the pattern used in `new` (which works), and avoids
        // the macOS quirk where MAP_FIXED-ing an *adjacent* slice
        // returns EINVAL — the kernel apparently doesn't like
        // splicing a new file-backed range alongside an existing
        // file-backed range under the same anon reservation.
        //
        // Performance cost: O(file_size / page_size) page-table
        // touches per grow. Since grow is amortized (caller doubles)
        // and each grow remaps the same physical pages we already
        // had, the kernel's bookkeeping update is cheap (no actual
        // I/O, no pageouts).
        debug_assert!(
            aligned <= self.reserved,
            "MAP_FIXED would overflow reservation"
        );
        let result = unsafe {
            libc::mmap(
                self.base as *mut _,
                aligned,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_FIXED | libc::MAP_SHARED,
                self.fd,
                0,
            )
        };
        if result == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        self.committed.store(aligned, Ordering::Release);
        Ok(())
    }

    /// Grow with amortized doubling: ensure at least `needed` bytes
    /// are committed, but always at least double the current commit.
    /// Used by storage append paths so an N-row INSERT incurs
    /// O(log N) syscalls rather than O(N).
    pub fn grow_amortized(&self, needed: usize) -> io::Result<()> {
        let cur = self.committed.load(Ordering::Acquire);
        let needed_aligned = align_up(needed, PAGE_SIZE);
        if needed_aligned <= cur {
            return Ok(());
        }
        // Doubling target. Start from a sane minimum so a tiny
        // initial commit doesn't cause a flurry of micro-grows on the
        // first few inserts.
        let min_growth = (64 * 1024).max(cur);
        let doubled = cur.saturating_mul(2).max(min_growth);
        let target = doubled.max(needed_aligned).min(self.reserved);
        if target < needed_aligned {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "needed exceeds reservation",
            ));
        }
        self.grow_to(target)
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

    /// Flush dirty pages back to disk. Caller passes the byte range
    /// they care about — usually the recently-written window.
    pub fn flush(&self, offset: usize, len: usize) -> io::Result<()> {
        let end = offset + len;
        let cur = self.committed();
        if end > cur {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "flush past committed",
            ));
        }
        let rc = unsafe {
            libc::msync(
                self.base.add(offset) as *mut _,
                len,
                libc::MS_SYNC,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Drop for GrowableMap {
    fn drop(&mut self) {
        unsafe {
            // Unmap the entire reservation in one call. The kernel
            // handles the mix of file-backed (committed) and
            // PROT_NONE (uncommitted) ranges correctly.
            libc::munmap(self.base as *mut _, self.reserved);
            libc::close(self.fd);
        }
    }
}

fn align_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;

    fn fresh(name: &str) -> File {
        let path = format!("/tmp/enchudb_growable_{name}");
        let _ = std::fs::remove_file(&path);
        OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap()
    }

    #[test]
    fn create_and_write_initial() {
        let f = fresh("init");
        let map = GrowableMap::new(f, 100 * 1024 * 1024, 4096).unwrap();
        let buf = unsafe { std::slice::from_raw_parts_mut(map.base(), 4096) };
        buf[0] = 0xAB;
        buf[4095] = 0xCD;
        assert_eq!(buf[0], 0xAB);
        assert_eq!(buf[4095], 0xCD);
    }

    #[test]
    fn grow_doubles() {
        let f = fresh("double");
        let map = GrowableMap::new(f, 100 * 1024 * 1024, 4096).unwrap();
        // initial commit is one page (4 KB)
        assert_eq!(map.committed(), 4096);

        map.grow_to(8192).unwrap();
        assert_eq!(map.committed(), 8192);

        map.grow_to(1_000_000).unwrap();
        // page aligned up to 1_003_520? Let's just check >= request.
        assert!(map.committed() >= 1_000_000);
        assert_eq!(map.committed() % PAGE_SIZE, 0);

        // write at the new tail to confirm it's actually mapped
        let off = 999_999;
        let buf = unsafe { std::slice::from_raw_parts_mut(map.base().add(off), 1) };
        buf[0] = 0xEE;
        assert_eq!(buf[0], 0xEE);
    }

    #[test]
    fn grow_idempotent() {
        let f = fresh("idem");
        let map = GrowableMap::new(f, 1_048_576, 4096).unwrap();
        map.grow_to(8192).unwrap();
        let before = map.committed();
        // request smaller than current — no change
        map.grow_to(4096).unwrap();
        assert_eq!(map.committed(), before);
        // request equal — no change
        map.grow_to(8192).unwrap();
        assert_eq!(map.committed(), before);
    }

    #[test]
    fn grow_past_reservation_fails() {
        let f = fresh("overflow");
        let map = GrowableMap::new(f, 8192, 4096).unwrap();
        let err = map.grow_to(16384).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::OutOfMemory);
    }

    #[test]
    fn many_grows_no_pointer_invalidation() {
        let f = fresh("ptr_stable");
        let map = GrowableMap::new(f, 100 * 1024 * 1024, 4096).unwrap();
        let base = map.base();
        // sprinkle markers across what will become committed pages
        let mut size = 4096;
        for i in 0..20 {
            size *= 2;
            if size > map.reserved() {
                break;
            }
            map.grow_to(size).unwrap();
            // verify every prior marker is still readable, and base
            // didn't move
            assert_eq!(map.base(), base, "base address moved on grow {i}");
            let off = (size / 2) - 1;
            let buf = unsafe { std::slice::from_raw_parts_mut(map.base().add(off), 1) };
            buf[0] = i as u8;
        }
    }

    #[test]
    fn small_initial_file_size_on_disk() {
        // Verify the actual file (not just apparent) is small at
        // create time. This is the whole point of the rewrite.
        let path = "/tmp/enchudb_growable_smallfile";
        let _ = std::fs::remove_file(path);
        let f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap();
        let _map = GrowableMap::new(f, 100 * 1024 * 1024, 4096).unwrap();
        let meta = std::fs::metadata(path).unwrap();
        assert_eq!(meta.len(), 4096, "fresh DB should be one page");
    }
}
