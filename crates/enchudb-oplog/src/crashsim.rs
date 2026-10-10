//! 電源断の模擬 (feature `crashsim`、 テスト専用)。
//!
//! SIGKILL で書き手を殺すテスト (`v10_crash_consistency`) は **process の死** しか見ない: mmap の
//! dirty page は page cache に残り、 process が死んでも file に届く。 msync / fsync を呼び忘れても、
//! 呼ぶ順番を間違えても通ってしまう。 電源断は page cache ごと失う。
//!
//! ここは 「どの byte がディスクに届いたと言えるか」 を書き手の横で記録する:
//!
//! - **書き出しの記録** — engine が msync (segment / oplog) や `sync_all` (sidecar) に成功した所で、
//!   書き出した page の中身を控える ([`copy_for_sync`] → [`data_synced`] / [`file_synced`])。 控えは file の
//!   (device, inode) ごと (rename で名前が変わっても同じ file を指す)。 Linux (ext4 / overlayfs) は空いた
//!   inode の番号をすぐ次に作った file に渡すので、 控えのある file を手放す所 (rename での置き換え /
//!   削除) は [`releasing`] を通して控えを捨てる (捨てないと、 番号を引き継いだ別の file に古い控えが写る)。
//! - **電源断の像** — [`capture`] が directory を丸ごと別の場所に写す。 各 page の中身は
//!   - [`Mode::Lost`]: 控え (書き出しの済んだ中身)。 控えの無い page はゼロ
//!   - [`Mode::Mixed`]: page ごとに控えか今の中身 (OS が勝手に書き出していた分) を seed で選ぶ
//!
//! **模型の前提** (journaling FS の振る舞い):
//!
//! - file の作成・長さ (ftruncate)・rename・削除は、 起きた時点で durable (ext4 data=ordered /
//!   APFS の metadata journal)。 POSIX はこれを約束しない (directory の fsync が要る) が、 ここでは
//!   見ない — 見るのは **中身** の書き出しの漏れと順序
//! - msync / fsync は page 単位で書き出す。 page の途中で破れる (torn sector) は見ない
//! - 書き出しの記録は msync の **前に** 中身を写し、 成功した後に控える。 書き出しの最中に書き手が
//!   page を書き換えても、 控えは 「確かに届いた」 側 (古い方) に倒れる
//! - 書き出しは重なりうる (oplog の fsync は consumer の周期と `oplog_sync` の呼び手が同時に呼ぶ)。 後に写した方の
//!   msync が返れば写した時点の中身は届いていて、 先に写した方が後で終わってもディスクは古い方へ戻らない。 控えは
//!   page ごとに後に写した中身を残す (写すのは 1 本ずつ、 写した順 = 中身の新しさ、 #446)

use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static ACTIVE: AtomicBool = AtomicBool::new(false);
static EVENTS: AtomicU64 = AtomicU64::new(0);
static STATE: Mutex<Option<State>> = Mutex::new(None);

/// page 番号 → 書き出しの済んだ中身
type Pages = BTreeMap<u64, Page>;

struct Page {
    /// 中身を写した順番 ([`copy_for_sync`] / [`file_synced`])。 大きいほど新しい中身
    order: u64,
    /// page 長 (末尾の page も page 長)
    bytes: Box<[u8]>,
}

/// 写す係を 1 本に並べる lock と、 最後に写した順番。 写している間は他が写さないので、 順番が後の写しは
/// どの page も前の写しと同じか新しい中身
static COPY: Mutex<u64> = Mutex::new(0);

type Hook = std::sync::Arc<dyn Fn(Copied) + Send + Sync>;
static AFTER_COPY: Mutex<Option<Hook>> = Mutex::new(None);

/// 写したものの種類 ([`copy_for_sync`]、 hook にも渡す)。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Copied {
    /// oplog の fsync
    Oplog,
    /// segment の msync
    Segment,
}

struct State {
    page: usize,
    /// (device, inode) ごとの控え
    files: HashMap<(u64, u64), Pages>,
}

/// 像の作り方。
#[derive(Clone, Copy, Debug)]
pub enum Mode {
    /// 書き出しの済んだ中身だけ。 書き出していない page は消える (ゼロ / 前に書き出した中身)
    Lost,
    /// page ごとに 「書き出しの済んだ中身」 か 「今の中身」 を seed で選ぶ
    Mixed { seed: u64 },
}

/// 像 1 枚の内訳。
#[derive(Clone, Debug, Default)]
pub struct CaptureStats {
    pub files: usize,
    /// 書き出しの済んだ中身を置いた page
    pub durable_pages: usize,
    /// 今の中身を置いた page (Mixed のみ)
    pub current_pages: usize,
    /// 今の中身と書き出しの済んだ中身が違った page (= 電源断で失われうる page)
    pub divergent_pages: usize,
    /// 失われうる page の内訳 (root からの相対 path, page 番号)。 失敗した像の手がかり
    pub divergent: Vec<(String, u64)>,
}

/// 記録を始める (控えを空にする)。 page は OS の page 長。
pub fn start() {
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
    *STATE.lock().unwrap() = Some(State { page, files: HashMap::new() });
    EVENTS.store(0, Ordering::Relaxed);
    ACTIVE.store(true, Ordering::Release);
}

/// 記録をやめる。
pub fn stop() {
    ACTIVE.store(false, Ordering::Release);
    *STATE.lock().unwrap() = None;
}

/// 記録中か。 hook はこれが false なら何もしない (写しも取らない)。
#[inline]
pub fn active() -> bool {
    ACTIVE.load(Ordering::Acquire)
}

/// これまでに控えた書き出しの回数。
pub fn events() -> u64 {
    EVENTS.load(Ordering::Relaxed)
}

fn identity(file: &File) -> io::Result<(u64, u64)> {
    let md = file.metadata()?;
    Ok((md.dev(), md.ino()))
}

/// msync / fsync の前に写した中身 ([`copy_for_sync`])。 書き出しが成功したら [`data_synced`] に渡す。
pub struct SyncCopy {
    order: u64,
    offset: u64,
    bytes: Vec<u8>,
}

/// msync / fsync の **前に**、 書き出す範囲の今の中身を写す。 `bytes` は file の `[offset, offset + len)` の mmap。
/// offset は page 境界であること (msync は page 単位で書くので、 呼ぶ側は末尾も page まで広げて写す — 写した範囲の
/// 外は控えに入らない)。
///
/// 写すのは 1 本ずつで、 写した順番を付ける。 書き出しが重なって先に写した方が後で終わっても、 [`data_synced`] は
/// 後に写した中身を古い写しで戻さない (#446: 戻していた頃は、 後の書き出しが届けた oplog の record が控えから消え、
/// 開いた像が古い checkpoint から再生して、 書き出しの返った untie の前の値を当て直した)。
pub fn copy_for_sync(what: Copied, offset: u64, bytes: &[u8]) -> SyncCopy {
    let copy = {
        let mut last = COPY.lock().unwrap_or_else(|p| p.into_inner());
        *last += 1;
        SyncCopy { order: *last, offset, bytes: bytes.to_vec() }
    };
    // 写す係の lock を離してから (止めた書き出しの横で、 他の書き出しが写せるように)
    let hook = AFTER_COPY.lock().unwrap_or_else(|p| p.into_inner()).clone();
    if let Some(h) = hook {
        h(what);
    }
    copy
}

/// 試験用: [`copy_for_sync`] が写した直後 (書き出しの前) に、 写したものの種類を渡して呼ぶ hook。 書き出しを重ねる試験が、
/// 写した後で書き出しを止めるのに使う。 None で外す。
pub fn set_after_copy_hook(hook: Option<Hook>) {
    *AFTER_COPY.lock().unwrap_or_else(|p| p.into_inner()) = hook;
}

/// [`copy_for_sync`] で写した範囲の書き出しが済んだ (msync 成功)。 page ごとに、 控えより後に写した中身だけ置く。
pub fn data_synced(file: &File, copy: SyncCopy) {
    if !active() {
        return;
    }
    let id = identity(file).expect("crashsim::data_synced: metadata");
    let mut guard = STATE.lock().unwrap();
    let Some(st) = guard.as_mut() else { return };
    let ps = st.page as u64;
    debug_assert_eq!(copy.offset % ps, 0, "crashsim::data_synced: offset が page 境界に無い");
    let pages = st.files.entry(id).or_default();
    for (i, chunk) in copy.bytes.chunks(st.page).enumerate() {
        let p = copy.offset / ps + i as u64;
        let page = pages.entry(p).or_insert_with(|| Page { order: 0, bytes: vec![0u8; st.page].into_boxed_slice() });
        // 後に写した中身が先に届いている (重なった書き出しの、 先に写した方が後で終わった)
        if page.order > copy.order {
            continue;
        }
        page.order = copy.order;
        page.bytes[..chunk.len()].copy_from_slice(chunk);
    }
    EVENTS.fetch_add(1, Ordering::Relaxed);
}

/// file 全体の書き出しが済んだ (`sync_all` 成功)。 今の中身を全部控える (write で書く小さな
/// file 用 — mmap した file はその書き出しを [`data_synced`] で控えること)。 書き手の fd は
/// 書き込み専用のことがあるので、 中身は `path` を開き直して読む。
pub fn file_synced(path: &Path) {
    if !active() {
        return;
    }
    // 読めないなら控えない、 では 「控えが無い = 全部失う」 像になって検証が嘘をつく。 落とす
    let file = File::open(path).unwrap_or_else(|e| panic!("crashsim::file_synced: {} を開けない: {e}", path.display()));
    let id = identity(&file).expect("crashsim::file_synced: metadata");
    // 書き出しの後に読んだ中身は、 それまでに写したどの中身より新しい (写す係と同じ順番に並べる)
    let (order, buf) = {
        let mut last = COPY.lock().unwrap_or_else(|p| p.into_inner());
        *last += 1;
        let buf = std::fs::read(path).unwrap_or_else(|e| panic!("crashsim::file_synced: {} を読めない: {e}", path.display()));
        (*last, buf)
    };
    let mut guard = STATE.lock().unwrap();
    let Some(st) = guard.as_mut() else { return };
    let pages = st.files.entry(id).or_default();
    pages.clear();
    for (i, chunk) in buf.chunks(st.page).enumerate() {
        let mut bytes = vec![0u8; st.page].into_boxed_slice();
        bytes[..chunk.len()].copy_from_slice(chunk);
        pages.insert(i as u64, Page { order, bytes });
    }
    EVENTS.fetch_add(1, Ordering::Relaxed);
}

/// `path` の file を手放す操作 (rename で置き換える / 消す) を `op` で行い、 その file の控えを捨てる。
///
/// Linux (ext4 / overlayfs) は空いた inode の番号をすぐ次に作った file に渡す。 捨てないと、 番号を
/// 引き継いだ file の page に古い控えが写る (table の定義の sidecar を rename で置き換えた直後に作った
/// 列の segment の page 0 が、 古い sidecar の中身 `TBL1` になった)。 控えの lock を持ったまま行うので、
/// 像は手放す前 (古い file と控え) か後 (古い控えは無い) のどちらか。 `path` が無い / `op` が失敗した
/// 時は何も捨てない。 記録していない時は `op` を呼ぶだけ。
pub fn releasing<R>(path: &Path, op: impl FnOnce() -> io::Result<R>) -> io::Result<R> {
    if !active() {
        return op();
    }
    let mut guard = STATE.lock().unwrap();
    let old = std::fs::metadata(path).ok().map(|md| (md.dev(), md.ino()));
    let r = op()?;
    if let (Some(id), Some(st)) = (old, guard.as_mut()) {
        st.files.remove(&id);
    }
    Ok(r)
}

/// `root` の下を丸ごと、 電源断が今起きた時の姿で `out` に写す。 控えの lock を持ったまま写すので、
/// 写している間の書き出しは像に入らない (像は 「この瞬間に落ちた」 1 枚)。
pub fn capture(root: &Path, out: &Path, mode: Mode) -> io::Result<CaptureStats> {
    let guard = STATE.lock().unwrap();
    let st = guard.as_ref().ok_or_else(|| io::Error::other("crashsim: start() されていない"))?;
    let mut stats = CaptureStats::default();
    std::fs::create_dir_all(out)?;
    copy_tree(st, root, root, out, mode, &mut stats)?;
    Ok(stats)
}

fn copy_tree(
    st: &State,
    root: &Path,
    dir: &Path,
    out: &Path,
    mode: Mode,
    stats: &mut CaptureStats,
) -> io::Result<()> {
    for ent in std::fs::read_dir(dir)? {
        let ent = ent?;
        let path = ent.path();
        let rel = path.strip_prefix(root).expect("root の下");
        let dst = out.join(rel);
        let ft = ent.file_type()?;
        if ft.is_dir() {
            std::fs::create_dir_all(&dst)?;
            copy_tree(st, root, &path, out, mode, stats)?;
        } else if ft.is_file() {
            // 書いている最中の tmp が消えることがある (rename / remove の競争)。 その file は像に無いだけ
            let Ok(src) = File::open(&path) else { continue };
            copy_file(st, &src, &path, rel, &dst, mode, stats)?;
            stats.files += 1;
        }
    }
    Ok(())
}

fn copy_file(
    st: &State,
    src: &File,
    path: &Path,
    rel: &Path,
    dst: &Path,
    mode: Mode,
    stats: &mut CaptureStats,
) -> io::Result<()> {
    let id = identity(src)?;
    let len = src.metadata()?.len();
    let ps = st.page as u64;
    let durable = st.files.get(&id);
    let data = data_pages(src, len, ps);
    let out = File::create(dst)?;
    out.set_len(len)?;
    let n_pages = len.div_ceil(ps);
    let mut cur = vec![0u8; st.page];
    let salt = fnv(path.as_os_str().as_encoded_bytes());
    for p in 0..n_pages {
        let off = p * ps;
        let take = ((len - off) as usize).min(st.page);
        let saved: Option<&[u8]> = durable.and_then(|m| m.get(&p)).map(|pg| &pg.bytes[..take]);
        let has_data = data.as_ref().is_none_or(|d| d.contains(p));
        let current: Option<&[u8]> = if has_data {
            src.read_exact_at(&mut cur[..take], off)?;
            Some(&cur[..take])
        } else {
            None
        };
        let differs = match (saved, current) {
            (Some(s), Some(c)) => s != c,
            (None, Some(c)) => c.iter().any(|&b| b != 0),
            (Some(s), None) => s.iter().any(|&b| b != 0),
            (None, None) => false,
        };
        if differs {
            stats.divergent_pages += 1;
            stats.divergent.push((rel.display().to_string(), p));
        }
        let use_current = match mode {
            Mode::Lost => false,
            Mode::Mixed { seed } => differs && splitmix(seed ^ salt ^ p.wrapping_mul(0x9E37_79B9)) & 1 == 1,
        };
        let chosen = if use_current {
            stats.current_pages += 1;
            current
        } else {
            if saved.is_some() {
                stats.durable_pages += 1;
            }
            saved
        };
        if let Some(bytes) = chosen
            && bytes.iter().any(|&b| b != 0)
        {
            out.write_all_at(bytes, off)?;
        }
    }
    Ok(())
}

/// 実体のある page の集合 (SEEK_DATA / SEEK_HOLE)。 引けない FS では None (= 全 page を読む)。
fn data_pages(src: &File, len: u64, ps: u64) -> Option<PageSet> {
    use std::os::unix::io::AsRawFd;
    let fd = src.as_raw_fd();
    let mut ranges = Vec::new();
    let mut pos: i64 = 0;
    while (pos as u64) < len {
        let d = unsafe { libc::lseek(fd, pos, libc::SEEK_DATA) };
        if d < 0 {
            let e = io::Error::last_os_error();
            // ENXIO = この先に data が無い
            if e.raw_os_error() == Some(libc::ENXIO) {
                break;
            }
            return None;
        }
        let h = unsafe { libc::lseek(fd, d, libc::SEEK_HOLE) };
        if h < 0 {
            return None;
        }
        ranges.push(((d as u64) / ps, (h as u64).div_ceil(ps)));
        pos = h;
    }
    Some(PageSet(ranges))
}

struct PageSet(Vec<(u64, u64)>);

impl PageSet {
    fn contains(&self, p: u64) -> bool {
        self.0.iter().any(|&(lo, hi)| lo <= p && p < hi)
    }
}

fn fnv(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}
