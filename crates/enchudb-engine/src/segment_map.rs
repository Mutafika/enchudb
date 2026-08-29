//! `SegmentMap` — **1 ファイル 1 予約** の mmap primitive ([[request21]] / v10 Phase 0)。
//!
//! # 位置づけ
//!
//! v10 は DB 本体を region ごとの独立ファイル (segment) に分ける。 各 segment は
//! この型 1 個で map され、 **他の segment とファイル長も address も共有しない**。
//! 旧 `GrowableMap` (1 ファイル固定 layout の commit 高水位、 v10 で撤去) が抱えていた
//! 「末尾 region を触ると手前が全部 commit される」 (#172) はファイル長が segment
//! ごとになるので構造的に消える。
//!
//! # 設計 (= 旧 `GrowableMap` の一般化)
//!
//! - open 時に `reserve` byte の仮想アドレスを **読み取り専用の anonymous zero page**
//!   (`PROT_READ`, `MAP_ANON|MAP_PRIVATE|MAP_NORESERVE`) で予約する。 RAM / disk は 0
//!   (実測: 1 TB 予約で RSS 増 0、 64 箇所 read で +1 MB = zero-fill page 分)。
//!   **base は以後不動** — store が握る `Region` の生ポインタを無効化しない
//! - **未 commit 領域の read は 0 を返す** (= 今の sparse mmap と同じ意味論)。 したがって
//!   store の read path は無改修でよく、 **write path だけが `ensure_committed` を要る**
//!   (未 commit page への write は SIGSEGV)。 これが `GrowableMap` の `PROT_NONE` 予約
//!   (read も落ちる) との違いで、 request21 の 「store は無改修」 を成立させる要
//! - ファイルの現在長までを `MAP_FIXED | MAP_SHARED` で予約の上に重ねる (= commit)
//! - 伸長は writer だけ: `ftruncate` → 全 [0..new) を MAP_FIXED で貼り直す
//!   (macOS が隣接 slice の MAP_FIXED を EINVAL にする quirk を避ける、 `GrowableMap` と同じ)
//! - **別 process の reader** は `refresh()` でファイル長を見て自分の commit を伸ばす
//!   (縮めない = SIGBUS しない、 oboro / opyula の readonly 直読み契約)
//! - fd は **map 後に閉じる**。 macOS GUI app の既定 `ulimit -n 256` に数百 segment を
//!   乗せるため。 伸長 / refresh / 空き容量確認は path から開き直す
//! - #167: 伸長時に 「これから commit する分 + margin」 の空きを statvfs で確認し、
//!   足りなければ **`StorageFull` を Result で返す** (sparse の穴を書いて SIGBUS する経路が無い)
//!
//! 仮想空間の上限は実測で問題にならない (macOS 25.2 / RAM 96 GB で 64 TB の単一予約、
//! 600 × 4 GB の同時予約とも ok — request21 open question 2)。

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

/// 実行時の hardware page size。 macOS Apple Silicon は **16 KB**、 Linux / macOS x86_64 は
/// 4 KB。 msync の `addr` はこれで page-aligned である必要がある (4096 で揃えると
/// Apple Silicon で EINVAL)。 起動時に sysconf で取って cache。
pub(crate) fn runtime_page_size() -> usize {
    use std::sync::atomic::AtomicUsize as A;
    static CACHED: A = A::new(0);
    let cur = CACHED.load(Ordering::Relaxed);
    if cur != 0 {
        return cur;
    }
    let v = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    let ps = if v > 0 { v as usize } else { 4096 };
    CACHED.store(ps, Ordering::Relaxed);
    ps
}

pub(crate) fn align_up(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

/// fd の載っている filesystem で **非 root user が使える空き byte 数** (#167)。
pub(crate) fn free_bytes_for_fd(fd: libc::c_int) -> io::Result<u64> {
    let mut vfs: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::fstatvfs(fd, &mut vfs) } < 0 {
        return Err(io::Error::last_os_error());
    }
    // f_frsize が 0 を返す filesystem があるので f_bsize に落ちる
    let unit = if vfs.f_frsize != 0 { vfs.f_frsize as u64 } else { vfs.f_bsize as u64 };
    Ok((vfs.f_bavail as u64).saturating_mul(unit))
}

/// #167: 伸長時に残す空き容量 margin (`GrowableMap` と同じ既定)。
const SPACE_MARGIN: u64 = 32 * 1024 * 1024;

pub struct SegmentMap {
    path: PathBuf,
    base: *mut u8,
    /// 予約した仮想アドレス幅。 open 後は不変。 伸長の上限。
    reserved: usize,
    /// file-backed に貼ってある byte 数 (page 単位)。 hot path は lock なしで読む。
    committed: AtomicUsize,
    readonly: bool,
    /// 伸長 / refresh の直列化 (#74 と同じ理由: stale な committed を読んだ 2 本目が
    /// ftruncate で縮めない)。
    grow_lock: Mutex<()>,
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
    /// 新規 segment file を作る。 既に存在すれば `AlreadyExists`。
    /// `initial` byte まで commit した状態で返す (page 切り上げ)。
    pub fn create(path: &Path, reserve: usize, initial: usize) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        let ps = runtime_page_size();
        let reserve = align_up(reserve.max(ps), ps);
        let initial = align_up(initial.max(ps), ps);
        if initial > reserve {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "initial > reserve"));
        }
        file.set_len(initial as u64)?;
        Self::map_new(path.to_path_buf(), file, reserve, initial, false)
    }

    /// 既存 segment file を開く。 commit はファイルの現在長 (page 切り上げ)。
    /// `readonly` なら `PROT_READ` で貼り、 伸長 API は `PermissionDenied`。
    pub fn open(path: &Path, reserve: usize, readonly: bool) -> io::Result<Self> {
        let file = OpenOptions::new().read(true).write(!readonly).open(path)?;
        let ps = runtime_page_size();
        let reserve = align_up(reserve.max(ps), ps);
        let len = file.metadata()?.len() as usize;
        let committed = align_up(len.max(ps), ps);
        if committed > reserve {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("segment {} is {len} bytes, larger than reservation {reserve}", path.display()),
            ));
        }
        // file 長が page 境界に無い (unpack が data の末尾で切った等) と、 最終 page の EOF より
        // 先への write が書き戻されない。 writer は page 境界まで伸ばしてから map する。
        if !readonly && (len as u64) < committed as u64 {
            file.set_len(committed as u64)?;
        }
        Self::map_new(path.to_path_buf(), file, reserve, committed, readonly)
    }

    fn prot(readonly: bool) -> libc::c_int {
        if readonly {
            libc::PROT_READ
        } else {
            libc::PROT_READ | libc::PROT_WRITE
        }
    }

    fn map_new(
        path: PathBuf,
        file: File,
        reserve: usize,
        committed: usize,
        readonly: bool,
    ) -> io::Result<Self> {
        use std::os::unix::io::AsRawFd;
        // zero page 予約: PROT_READ の anonymous mapping。 read は 0、 write は fault。
        let base = unsafe {
            libc::mmap(
                ptr::null_mut(),
                reserve,
                libc::PROT_READ,
                libc::MAP_PRIVATE | libc::MAP_ANON | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        let r = unsafe {
            libc::mmap(
                base,
                committed,
                Self::prot(readonly),
                libc::MAP_FIXED | libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if r == libc::MAP_FAILED {
            let e = io::Error::last_os_error();
            unsafe { libc::munmap(base, reserve) };
            return Err(e);
        }
        // fd はここで閉じる (mapping は生き続ける)。
        drop(file);
        Ok(Self {
            path,
            base: base as *mut u8,
            reserved: reserve,
            committed: AtomicUsize::new(committed),
            readonly,
            grow_lock: Mutex::new(()),
            dirty_lo: AtomicUsize::new(usize::MAX),
            dirty_hi: AtomicUsize::new(0),
            space_margin: AtomicU64::new(SPACE_MARGIN),
            space_denials: AtomicU64::new(0),
        })
    }

    fn reopen(&self) -> io::Result<File> {
        OpenOptions::new().read(true).write(!self.readonly).open(&self.path)
    }

    /// [0..end) を file-backed で貼り直す (lock 下で呼ぶこと)。
    fn remap_to(&self, file: &File, end: usize) -> io::Result<()> {
        use std::os::unix::io::AsRawFd;
        debug_assert!(end <= self.reserved);
        let r = unsafe {
            libc::mmap(
                self.base as *mut _,
                end,
                Self::prot(self.readonly),
                libc::MAP_FIXED | libc::MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if r == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        self.committed.store(end, Ordering::Release);
        Ok(())
    }

    /// commit を `new_size` 以上に伸ばす (page 切り上げ、 冪等)。 writer 専用。
    ///
    /// ファイルは **現在長より大きい時だけ** `ftruncate` する (別 process が先に
    /// 伸ばしていた場合に縮めない)。 空き容量が足りなければ `StorageFull` (#167)。
    pub fn grow_to(&self, new_size: usize) -> io::Result<()> {
        let aligned = align_up(new_size, runtime_page_size());
        if aligned <= self.committed.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.readonly {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "readonly segment cannot grow (use refresh to follow the writer)",
            ));
        }
        let _g = self.grow_lock.lock().unwrap_or_else(|p| p.into_inner());
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
            use std::os::unix::io::AsRawFd;
            let delta = aligned as u64 - file_len;
            if let Ok(free) = free_bytes_for_fd(file.as_raw_fd()) {
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
        self.remap_to(&file, aligned)
    }

    /// amortized 伸長 (`GrowableMap::grow_amortized` と同じ hybrid 戦略)。
    pub fn grow_amortized(&self, needed: usize) -> io::Result<()> {
        let ps = runtime_page_size();
        let cur = self.committed.load(Ordering::Acquire);
        let needed_aligned = align_up(needed, ps);
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

    /// ファイルの現在長まで commit を追従させる (reader が writer の伸長を拾う経路。
    /// writer 自身が別 process の先行 grow を拾うのにも使える)。 縮めることはしない。
    /// 戻り値は追従後の commit。
    pub fn refresh(&self) -> io::Result<usize> {
        let _g = self.grow_lock.lock().unwrap_or_else(|p| p.into_inner());
        let file = self.reopen()?;
        let len = file.metadata()?.len() as usize;
        let ps = runtime_page_size();
        let target = align_up(len.max(ps), ps).min(self.reserved);
        let cur = self.committed.load(Ordering::Acquire);
        if target <= cur {
            return Ok(cur);
        }
        self.remap_to(&file, target)?;
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

    /// ファイルの現在長 (= 物理消費の上限、 apparent)。 observability 用。
    pub fn file_len(&self) -> io::Result<u64> {
        Ok(std::fs::metadata(&self.path)?.len())
    }

    /// #167: テスト用。 巨大な値で 「空きが足りない」 を決定的に作る。
    pub fn set_space_margin(&self, bytes: u64) {
        self.space_margin.store(bytes, Ordering::Relaxed);
    }

    pub fn space_denials(&self) -> u64 {
        self.space_denials.load(Ordering::Relaxed)
    }

    pub fn free_bytes(&self) -> io::Result<u64> {
        use std::os::unix::io::AsRawFd;
        let file = OpenOptions::new().read(true).open(&self.path)?;
        free_bytes_for_fd(file.as_raw_fd())
    }

    /// [offset, offset+len) を msync (MS_SYNC)。 offset は page aligned であること。
    pub fn flush(&self, offset: usize, len: usize) -> io::Result<()> {
        let end = offset + len;
        if end > self.committed() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "flush past committed"));
        }
        if len == 0 {
            return Ok(());
        }
        let rc = unsafe { libc::msync(self.base.add(offset) as *mut _, len, libc::MS_SYNC) };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// page 境界に拡げて flush。
    pub fn flush_aligned(&self, offset: usize, len: usize) -> io::Result<()> {
        if len == 0 {
            return Ok(());
        }
        let ps = runtime_page_size();
        let lo = offset & !(ps - 1);
        let hi = align_up(offset + len, ps).min(self.committed());
        if hi <= lo {
            return Ok(());
        }
        self.flush(lo, hi - lo)
    }

    /// commit 済み全域を flush。
    pub fn flush_all(&self) -> io::Result<()> {
        self.flush(0, self.committed())
    }

    #[inline]
    pub fn mark_dirty(&self, offset: usize, len: usize) {
        if len == 0 {
            return;
        }
        let end = offset + len;
        if self.dirty_lo.load(Ordering::Relaxed) <= offset
            && self.dirty_hi.load(Ordering::Relaxed) >= end
        {
            return;
        }
        self.dirty_lo.fetch_min(offset, Ordering::Release);
        self.dirty_hi.fetch_max(end, Ordering::Release);
    }

    /// 直近 `mark_dirty` の範囲だけ msync して reset。
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
        unsafe {
            libc::munmap(self.base as *mut _, self.reserved);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("enchu_segmap_{}_{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    const MB: usize = 1024 * 1024;

    #[test]
    fn create_write_reopen_roundtrip() {
        let d = dir("roundtrip");
        let p = d.join("a.seg");
        let base;
        {
            let m = SegmentMap::create(&p, 64 * MB, 4096).unwrap();
            base = m.base() as usize;
            unsafe { *m.base().add(100) = 0xAB };
            m.grow_to(3 * MB).unwrap();
            assert_eq!(m.base() as usize, base, "grow で base が動いた");
            unsafe { *m.base().add(2 * MB + 7) = 0xCD };
            m.flush_all().unwrap();
        }
        let m = SegmentMap::open(&p, 64 * MB, false).unwrap();
        assert!(m.committed() >= 3 * MB);
        assert_eq!(unsafe { *m.base().add(100) }, 0xAB);
        assert_eq!(unsafe { *m.base().add(2 * MB + 7) }, 0xCD);
        // ファイル長は commit 分だけ (予約 64 MB ではない)
        assert!(m.file_len().unwrap() <= (3 * MB + 64 * 1024) as u64);
        let _ = std::fs::remove_dir_all(&d);
    }

    /// 別 process 相当: 同じ file を 2 つの map で開き、 片方の伸長をもう片方が
    /// `refresh` で拾う。 base は動かず、 伸びた領域の内容が読める。
    #[test]
    fn reader_follows_writer_growth_via_refresh() {
        let d = dir("refresh");
        let p = d.join("a.seg");
        let w = SegmentMap::create(&p, 64 * MB, 4096).unwrap();
        let r = SegmentMap::open(&p, 64 * MB, true).unwrap();
        let rbase = r.base() as usize;
        assert!(r.is_readonly());
        assert!(matches!(r.grow_to(MB).unwrap_err().kind(), io::ErrorKind::PermissionDenied));

        w.grow_to(2 * MB).unwrap();
        unsafe { *w.base().add(MB + 5) = 0x77 };
        assert!(r.committed() < 2 * MB, "refresh 前に勝手に伸びている");
        let c = r.refresh().unwrap();
        assert!(c >= 2 * MB);
        assert_eq!(r.base() as usize, rbase, "refresh で base が動いた");
        assert_eq!(unsafe { *r.base().add(MB + 5) }, 0x77);
        // 二度目は no-op
        assert_eq!(r.refresh().unwrap(), c);
        let _ = std::fs::remove_dir_all(&d);
    }

    /// request21 の要: 多数 segment を同時に開き、 ばらばらに伸ばしても
    /// (a) どの base も動かない (b) 他 segment のファイル長が動かない。
    #[test]
    fn many_segments_grow_independently() {
        let d = dir("many");
        let n = 120;
        let maps: Vec<SegmentMap> = (0..n)
            .map(|i| SegmentMap::create(&d.join(format!("{i:04}.seg")), 4 * MB, 4096).unwrap())
            .collect();
        let bases: Vec<usize> = maps.iter().map(|m| m.base() as usize).collect();
        let before: Vec<u64> = maps.iter().map(|m| m.file_len().unwrap()).collect();
        // 奇数番だけ伸ばす
        for (i, m) in maps.iter().enumerate() {
            if i % 2 == 1 {
                m.grow_to(MB + i * 4096).unwrap();
                unsafe { *m.base().add(MB) = i as u8 };
            }
        }
        for (i, m) in maps.iter().enumerate() {
            assert_eq!(m.base() as usize, bases[i], "segment {i} の base が動いた");
            let len = m.file_len().unwrap();
            if i % 2 == 1 {
                assert!(len >= MB as u64, "segment {i} が伸びていない");
                assert_eq!(unsafe { *m.base().add(MB) }, i as u8);
            } else {
                assert_eq!(len, before[i], "触っていない segment {i} のファイル長が動いた");
            }
        }
        let _ = std::fs::remove_dir_all(&d);
    }

    /// request21 の要 2: 未 commit 領域の read は fault せず 0 を返す (sparse mmap と同じ)。
    /// store の read path を無改修で済ませる前提。
    #[test]
    fn reads_beyond_committed_are_zero_without_fault() {
        let d = dir("zeroread");
        let m = SegmentMap::create(&d.join("a.seg"), 64 * MB, 4096).unwrap();
        let far = 50 * MB;
        assert!(m.committed() < far);
        assert_eq!(unsafe { *m.base().add(far) }, 0, "未 commit 領域が 0 でない");
        // slice として読んでも同じ (Column::values_u32 相当)
        let s = unsafe { std::slice::from_raw_parts(m.base(), 64 * MB) };
        assert_eq!(s[far + 1], 0);
        // その後 commit を伸ばして書けば見える
        m.grow_to(far + 4096).unwrap();
        unsafe { *m.base().add(far) = 9 };
        assert_eq!(s[far], 9);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn grow_past_reservation_fails_and_is_idempotent() {
        let d = dir("reserve");
        let m = SegmentMap::create(&d.join("a.seg"), 2 * MB, 4096).unwrap();
        assert!(matches!(m.grow_to(3 * MB).unwrap_err().kind(), io::ErrorKind::OutOfMemory));
        m.grow_to(MB).unwrap();
        let c = m.committed();
        m.grow_to(MB / 2).unwrap();
        assert_eq!(m.committed(), c, "縮む方向の grow が効いた");
        let _ = std::fs::remove_dir_all(&d);
    }

    /// #167: 空き不足は SIGBUS ではなく StorageFull で返る。
    #[test]
    fn enospc_is_reported_as_error() {
        let d = dir("enospc");
        let m = SegmentMap::create(&d.join("a.seg"), 64 * MB, 4096).unwrap();
        m.set_space_margin(u64::MAX / 2);
        let e = m.grow_to(MB).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::StorageFull);
        assert_eq!(m.space_denials(), 1);
        assert_eq!(m.file_len().unwrap(), runtime_page_size() as u64, "拒否したのにファイルが伸びた");
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn open_rejects_file_larger_than_reservation() {
        let d = dir("toolarge");
        let p = d.join("a.seg");
        {
            let m = SegmentMap::create(&p, 8 * MB, 4096).unwrap();
            m.grow_to(4 * MB).unwrap();
        }
        assert!(matches!(
            SegmentMap::open(&p, MB, false).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        ));
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn dirty_range_flush_only_touches_committed() {
        let d = dir("dirty");
        let m = SegmentMap::create(&d.join("a.seg"), 8 * MB, 4096).unwrap();
        m.grow_to(MB).unwrap();
        m.mark_dirty(10, 100);
        m.mark_dirty(MB - 8, 8);
        m.flush_dirty().unwrap();
        m.flush_dirty().unwrap(); // clean なら no-op
        assert!(m.flush(0, 2 * MB).is_err(), "committed を越えた flush が通った");
        let _ = std::fs::remove_dir_all(&d);
    }
}
