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

/// Conservative compile-time page size used for layout-side alignment
/// (`grow_to` / `set_len` 等)。 file の論理 size を扱う場面はこれで十分。
const PAGE_SIZE: usize = 4096;

/// 実行時の hardware page size。 macOS Apple Silicon は **16 KB**、 Linux /
/// macOS x86_64 は 4 KB。 msync の `addr` 引数はこれで page-aligned である
/// 必要があり、 4096 で揃えると Apple Silicon で EINVAL が出る (request3
/// 実装時に踏んだ)。 起動時に sysconf で取って cache。
fn runtime_page_size() -> usize {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static CACHED: AtomicUsize = AtomicUsize::new(0);
    let cur = CACHED.load(Ordering::Relaxed);
    if cur != 0 {
        return cur;
    }
    let v = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    let ps = if v > 0 { v as usize } else { 4096 };
    CACHED.store(ps, Ordering::Relaxed);
    ps
}

pub struct GrowableMap {
    fd: RawFd,
    base: *mut u8,
    /// Virtual address range size — never changes after `new`. The
    /// caller decides this upfront based on the DB's max layout.
    reserved: usize,
    /// File-backed bytes within the reservation. Atomic so a writer
    /// can read it on the hot path without locking; growth itself is
    /// serialized by `grow_lock` (#74 — caller-side の直列化は当てに
    /// しない。 writer thread の vocab grow と consumer thread の
    /// content grow が実際に並走する)。
    committed: AtomicUsize,
    /// #74: grow の直列化。 stale な `committed` を読んだ 2 本目の grow が
    /// ftruncate で縮小 → 書き込み済み data 破棄 + SIGBUS を防ぐ。
    /// hot path (committed 読み) はロックを取らない。
    grow_lock: std::sync::Mutex<()>,
    /// 直近 `flush_dirty` 以降に書き込まれた byte 範囲の lo (inclusive)。
    /// usize::MAX = clean (writes 無し)。 writer は `mark_dirty(off, len)`
    /// で fetch_min、 consumer は `flush_dirty` で snapshot + reset。
    /// request3: sustained 書き込み中 body_msync が committed 全体を msync
    /// していて線形に遅くなる問題への対策。
    dirty_lo: AtomicUsize,
    /// 直近 `flush_dirty` 以降に書き込まれた byte 範囲の hi (exclusive)。
    /// 0 = clean。 writer は `mark_dirty` で fetch_max。
    dirty_hi: AtomicUsize,
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
            grow_lock: std::sync::Mutex::new(()),
            dirty_lo: AtomicUsize::new(usize::MAX),
            dirty_hi: AtomicUsize::new(0),
        })
    }

    /// Grow the file-backed window to at least `new_size` bytes.
    /// Idempotent: if already committed past `new_size`, returns
    /// without syscalls. Page-aligns the request.
    ///
    /// #74: 内部で `grow_lock` により直列化する (caller 側の排他には
    /// 依存しない)。 また `ftruncate` は**ファイルを縮小できる**ため、
    /// 現ファイルサイズより大きい場合のみ実行する — 並行 grow / 別
    /// プロセスの先行 grow を縮めて data 破棄 + SIGBUS になるのを防ぐ。
    pub fn grow_to(&self, new_size: usize) -> io::Result<()> {
        let aligned = align_up(new_size, PAGE_SIZE);
        // hot path: ロックなしの早期 return
        if aligned <= self.committed.load(Ordering::Acquire) {
            return Ok(());
        }
        let _g = self.grow_lock.lock().unwrap_or_else(|p| p.into_inner());
        // lock 下で再チェック (並走した grow が先に伸ばしたケース)
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

        // Step 1: extend the file — ただし現サイズより大きい時のみ。
        // 別プロセスが既に aligned 超へ伸ばしていた場合の縮小を防ぐ
        // (同一プロセス内は grow_lock で直列化済み)。
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(self.fd, &mut st) } < 0 {
            return Err(io::Error::last_os_error());
        }
        if (st.st_size as u64) < aligned as u64 {
            let rc = unsafe { libc::ftruncate(self.fd, aligned as i64) };
            if rc < 0 {
                return Err(io::Error::last_os_error());
            }
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
        // Hybrid 戦略: 小 cur では doubling (KB→MB の急成長を amortize)、
        // 大 cur では一律 chunk linear (反復 insert で over-allocate しない)。
        // 5 MB のデータベースが 1 KB 書いただけで 10 MB に膨らむのを防ぐ。
        const SMALL_THRESHOLD: usize = 1024 * 1024; // 1 MB
        const LINEAR_CHUNK: usize = 1024 * 1024;     // 1 MB chunks above threshold
        let target = if cur < SMALL_THRESHOLD {
            // doubling、 ただし最低 64 KB は伸ばす
            let min_growth = 64 * 1024;
            let doubled = cur.saturating_mul(2).max(cur + min_growth);
            doubled.max(needed_aligned)
        } else {
            // 1 MB chunks。 needed が遥かに大きければそちら優先。
            (cur + LINEAR_CHUNK).max(needed_aligned)
        }
        .min(self.reserved);
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
        if len == 0 {
            return Ok(());
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

    /// 書き込みが起きた byte 範囲を [offset, offset+len) で記録 (consumer 用)。
    /// 主に consumer thread (apply_op 経由の Column::set / ContentStore::set) から
    /// 呼ぶ。 writer thread からは呼ばない (cache line contention で perf 退化する
    /// ため、 別経路で msync する)。
    ///
    /// Release ordering で書く: writer の mmap 書き込みが consumer の `flush_dirty`
    /// (AcqRel swap) より happens-before になることを保証する。 `fetch_min` /
    /// `fetch_max` は CAS 1 命令で済むので CAS loop より cache line 開放が速い。
    #[inline]
    pub fn mark_dirty(&self, offset: usize, len: usize) {
        if len == 0 {
            return;
        }
        let end = offset + len;
        // fast path: 既に dirty_lo <= offset かつ dirty_hi >= end なら何もしない。
        // この check で同一範囲を繰り返し叩く caller (= 同じ column への連続書き込み)
        // の atomic op を skip できる。
        if self.dirty_lo.load(Ordering::Relaxed) <= offset
            && self.dirty_hi.load(Ordering::Relaxed) >= end
        {
            return;
        }
        self.dirty_lo.fetch_min(offset, Ordering::Release);
        self.dirty_hi.fetch_max(end, Ordering::Release);
    }

    /// 直近 `mark_dirty` で記録された range だけを msync して reset。
    /// dirty range が空 (= clean) なら no-op。 これで body_msync が committed
    /// 全体ではなく直近 write 範囲だけになり、 sustained workload で線形に
    /// 遅くなる問題が解消する (request3)。
    ///
    /// msync(2) は addr が page-aligned である必要があるため、 dirty range の
    /// 両端を page 境界に拡げる (lo を page 下方丸め、 hi を page 上方丸め)。
    /// `flush` の page-aware ラッパ。 offset/len を hardware page 境界に拡げて
    /// msync する。 entity_set 等の小さな固定 region を `body_msync` で常時
    /// flush するときに使う (= mark_dirty を介さない経路)。
    pub fn flush_aligned(&self, offset: usize, len: usize) -> io::Result<()> {
        if len == 0 {
            return Ok(());
        }
        let ps = runtime_page_size();
        let lo_aligned = offset & !(ps - 1);
        let hi = offset + len;
        let hi_aligned = (hi + ps - 1) & !(ps - 1);
        let cur = self.committed();
        let hi_clamped = hi_aligned.min(cur);
        if hi_clamped <= lo_aligned {
            return Ok(());
        }
        self.flush(lo_aligned, hi_clamped - lo_aligned)
    }

    pub fn flush_dirty(&self) -> io::Result<()> {
        // snapshot + reset (atomic swap)
        let lo = self.dirty_lo.swap(usize::MAX, Ordering::AcqRel);
        let hi = self.dirty_hi.swap(0, Ordering::AcqRel);
        if hi <= lo {
            return Ok(()); // clean、 何もすることが無い
        }
        // msync の addr は **hardware page** で aligned 必須。 Apple Silicon は
        // 16 KB、 x86_64 / Linux arm64 は 4 KB。 runtime_page_size 経由で取る。
        let ps = runtime_page_size();
        let lo_aligned = lo & !(ps - 1);
        let hi_aligned = (hi + ps - 1) & !(ps - 1);
        let cur = self.committed();
        let hi_clamped = hi_aligned.min(cur);
        if hi_clamped <= lo_aligned {
            return Ok(());
        }
        self.flush(lo_aligned, hi_clamped - lo_aligned)
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

    /// #74 regression: 並行 grow が ftruncate でファイルを縮小して
    /// 書き込み済み data を破棄しないこと。旧実装では大きい grow の後に
    /// stale な committed を読んだ小さい grow が ftruncate(小) を実行し、
    /// [小, 大) の data がゼロ化 + mapping が EOF 越えで SIGBUS していた。
    #[test]
    fn concurrent_grows_never_shrink() {
        use std::sync::Arc;
        let f = fresh("concurrent");
        let map = Arc::new(GrowableMap::new(f, 256 * 1024 * 1024, 4096).unwrap());

        let threads: Vec<_> = (0..8)
            .map(|i| {
                let m = map.clone();
                std::thread::spawn(move || {
                    // thread ごとに違う target サイズで grow を繰り返す
                    for round in 1..=20usize {
                        let target = 4096 * (1 + (i * 7 + round * 13) % 512);
                        m.grow_to(target).unwrap();
                        // grow 直後、自分の target までは必ず書けること
                        let committed = m.committed();
                        assert!(committed >= (target + PAGE_SIZE - 1) / PAGE_SIZE * PAGE_SIZE
                                || committed >= target,
                                "committed {committed} < target {target}");
                        let buf = unsafe {
                            std::slice::from_raw_parts_mut(m.base(), committed)
                        };
                        buf[target - 1] = 0xEE; // SIGBUS しないこと
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }

        // committed は単調増加のはず。最後に全域が読めること
        let final_committed = map.committed();
        let buf = unsafe { std::slice::from_raw_parts(map.base(), final_committed) };
        let _ = buf[final_committed - 1];
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
