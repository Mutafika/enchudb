//! 履歴 (スピン 案 W の試作、 branch `proto/spin-w`): 宣言した列 (`Engine::declare_history`) の書き込みを、 カード (identity =
//! eid の世代 | 場所) ごとの集まりとして持つ。
//!
//! - **本体 = identity ごとの書き込みの集まり** (時刻, 値)。 列 (cell) と円柱は集まりから作る今の値のキャッシュで、 今まで通り
//!   書き手が直に書く (今の読みの速さは変えない)。 ここは cell を書いた後に 1 件足すだけ
//! - 置き場は列ごとの segment 2 本: `hist/NNNN.seg` (header 1 page + entry 32 B の並び、 足すだけ) と `hist/NNNN.head.seg`
//!   (場所ごとの最新 entry の位置、 4 B / 場所)
//! - entry は同じ identity の 1 つ前の entry への糸 (`prev`) と、 もっと前への飛び糸 (`jump`、 skew binary、 Myers 1983) を持つ。
//!   identity の最初の entry (`FIRST`) の `jump` は同じ場所の前の住人の最後の entry (場所が使い回されても前の住人の履歴は古い
//!   eid で読める)
//! - 時刻 = WAL の record の HLC を詰めた数 ([`pack`]) + 更新の回数 (`depth` = その identity のその列の何回目の entry か、
//!   宣言の時の値 (`BASE`) / 最初の entry が 0)
//! - 同じ書き込みは (identity, 列, 時刻) で 1 つ: 足す前に同じ時刻の entry を探して、 あれば足さない (WAL の再生・当て直し)
//! - デッキ単位の読み (時点 t に 「列 = v」 だったカードの束) は、 値ごとの差分の並び (時刻, eid, 入った / 出た) を **最初に
//!   見た時に** entry を全部なめて作り、 後は書き手が足す ([`HistCol::pull_at`])。 見ていない値は並べない
//!
//! ## 時刻の分からない entry (`OBSERVED`)
//! 時刻を運べない道 (開く時の突き合わせ・gen.seg の死の完了・WAL の無い内部の書き込み) の entry は 「遅くともこの時刻には
//! こうなっていた」 の印を付け、 時刻は今の時計 (それまでに見た最大 + 1)。 本当の変更はそれ以前のどこか。 読みはこの entry の
//! 前の区間の答えに `uncertain` を立てる。 WAL の無い session のユーザの書き込みは engine の時計で、 印は付けない
//! (`Engine::declare_history` の説明)。
//!
//! ## 順序
//! 書き手は行の lock の下で [`HistCol::note`] を呼ぶ (同じ場所の entry は 1 本ずつ並ぶ = 同じ identity の鎖は足した順)。
//! entry の場所は `len` の fetch_add で取り、 中身を書いてから flags を Release で書き、 その後で場所の最新 (`heads`) を
//! Release で置く。 読み手は heads を Acquire で読んでから entry を読む。 entry は書いた後は変えない (開く時の繋ぎ直し
//! [`HistCol::relink`] だけが、 書き手の居ない所で糸を書き換える)。
//!
//! ## 電源断
//! 書き出し (`Engine::write_out_body` の最後) は header に `len` を書いてから entry の segment と heads を msync する。 きれいに
//! 閉じなかった後に開くと、 WAL の再生の前に entry をなめて鎖と heads を作り直し ([`HistCol::relink`]、 header の `len` より後でも
//! flags の立った entry は拾う)、 再生が entry を足し直し (重複は時刻で落ちる)、 再生の後に cell と突き合わせる
//! (`Engine::settle_history_at_open`)。 entry は flags を最初の 4 B に置き、 最後に書く (書き出しの写しは低い番地から読むので、
//! flags が見えた entry は他の欄も書き終わっている)。

use crate::segment_map::SegmentMap;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

/// entry 1 件の大きさ (page をまたがない)。
pub const ENTRY: usize = 32;
/// entry の segment の header (1 page。 entry は次の page から)。
const HEADER: usize = 4096;
const MAGIC: &[u8; 4] = b"HIS1";
const VERSION: u32 = 1;
/// 予約する仮想アドレス (byte): header + entry 2^32 件 (= 128 GiB + 1 page)。 位置は u32 (糸は位置 + 1 なので、 最後の 1 件は使わない)
const RESERVE_ENTRIES: usize = HEADER + (1usize << 32) * ENTRY;
/// 糸の終わり (糸は entry の位置 + 1)。
const NONE: u32 = 0;
/// 値の無い entry (untie / 削除) の値。
pub const ABSENT: u64 = u64::MAX;
const DEPTH_MASK: u32 = 0x00FF_FFFF;

/// entry の印 (flags の上位 8 bit)。
pub mod flag {
    /// 書き終わった entry (0 の page / 書きかけと見分ける)
    pub const VALID: u32 = 0x80;
    /// 時刻の分からない道の entry: 「遅くともこの時刻には」 (本当の変更はそれ以前)
    pub const OBSERVED: u32 = 0x40;
    /// identity の最初の entry (`jump` = 同じ場所の前の住人の最後の entry)
    pub const FIRST: u32 = 0x20;
    /// 行の削除で外れた
    pub const DELETE: u32 = 0x10;
    /// 宣言の時に在った値 (時刻 = 宣言の時刻 = 地平)
    pub const BASE: u32 = 0x08;
    /// 開く時の突き合わせで捨てた entry (cell に届かなかった書き込み)。 鎖から外す
    pub const VOID: u32 = 0x04;
    /// 案 A (溜める): 書いたが糸をまだ繋いでいない entry (`HistCol::drain` が繋ぐ)
    pub const PEND: u32 = 0x02;
    /// 案 C (見られるまで繋がない): 列が一度も読まれていない間に足した、 糸も heads も無い entry (最初の読みが列全体を繋ぐ)
    pub const LAZY: u32 = 0x01;
}

/// HLC を時刻に詰める: wall ms << 20 | logical (logical は 2^20 未満に切る。 1 ms に 100 万回は書かない)。
pub fn pack(h: enchudb_oplog::Hlc) -> u64 {
    (h.wall << 20) | u64::from(h.logical.min((1 << 20) - 1))
}

/// [`pack`] の逆 (peer は 0)。
pub fn unpack(t: u64) -> enchudb_oplog::Hlc {
    enchudb_oplog::Hlc { wall: t >> 20, logical: (t & ((1 << 20) - 1)) as u32, peer: 0 }
}

/// 履歴の読みが答えられない理由。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistError {
    /// 紐が無い / 履歴を宣言していない
    NotDeclared,
    /// 時点が地平 (宣言の時刻) より前
    TooOld { horizon: u64 },
    /// readonly で開いた process で、 場所ごとの最新を戻せなかった (書き手が開いている / きれいに閉じていない)。 繋ぎ直せない (entry を
    /// 書く) ので答えない (前は 「値なし」 を Ok で返した)
    Unavailable,
}

impl From<TooOld> for HistError {
    fn from(t: TooOld) -> Self {
        HistError::TooOld { horizon: t.horizon }
    }
}

/// 時点が地平 (宣言の時刻) より前: その時点の値は持っていない。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TooOld {
    pub horizon: u64,
}

/// 時点 t の 1 行の答え。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct At {
    /// その時点の値 (None = 値が無い / まだ居ない / 消えた)
    pub value: Option<u64>,
    /// 答えた entry の更新の回数 (identity の最初の entry = 0)。 entry が無ければ None
    pub seq: Option<u32>,
    /// 次の entry が時刻の分からない entry (`OBSERVED`): 本当の変更が t より前だったかもしれない
    pub uncertain: bool,
}

/// 1 回の変更 ([`HistCol::history`])。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Change {
    pub time: u64,
    pub value: Option<u64>,
    pub seq: u32,
    pub observed: bool,
    pub deleted: bool,
    pub base: bool,
}

#[derive(Clone, Copy, Debug)]
struct Entry {
    flags: u32,
    depth: u32,
    eid: u32,
    prev: u32,
    jump: u32,
    time: u64,
    value: u64,
}

/// 値ごとの差分 1 件: (時刻, 入った/出た の bit | entry の位置 << 1, eid)。 並びは (時刻, 位置)。
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Diff {
    time: u64,
    idx_in: u32,
    eid: u32,
}

/// heads の 1 場所 (32 B): 最新の entry の糸と写し (書き手が entry を読まずに済むように)。 読み手は糸だけを読む。
const HEAD: usize = 32;
const HEADS_MAGIC: &[u8; 4] = b"HIH1";
/// heads の file の header。 中身は header の後ろから private に貼る (mmap の offset は page の倍数: page は 4 KiB / 16 KiB (Apple
/// Silicon) / 64 KiB (一部の arm64 Linux) なので、 その全部の倍数の 64 KiB。 前は 4096 で、 16 KiB の page の機械で貼れずに毎回繋ぎ直していた)
const HEADS_HEADER: usize = 64 * 1024;

/// 場所ごとの最新 (memory の上)。 msync しない (書き出しのたびに page が write-protect に戻り、 書き手が場所ごとに fault を
/// 払うのを避ける)。 きれいに閉じる時だけ file に置き (`persist`)、 開く時は file を private に貼る (読まずに開く)。 file が
/// 無い / 合わない (きれいに閉じなかった) 時は entry から作り直す (`HistCol::relink`)。
struct Heads {
    base: *mut u8,
    len: usize,
    /// 触った場所の上限 + 1
    hi: AtomicU32,
}

unsafe impl Send for Heads {}
unsafe impl Sync for Heads {}

impl Heads {
    fn anon(slots: u32) -> io::Result<Heads> {
        let ps = crate::segment_map::runtime_page_size();
        let len = ((slots.max(1) as usize * HEAD).div_ceil(ps)) * ps;
        let p = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        if p == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Heads { base: p as *mut u8, len, hi: AtomicU32::new(0) })
    }

    /// file `path` の中身を private に貼る。 header の印 `stamp` (entry の数) が合う時だけ。 貼れたら true。
    fn load(&self, path: &Path, stamp: u64) -> bool {
        use std::os::unix::io::AsRawFd;
        let Ok(f) = std::fs::File::open(path) else { return false };
        let mut h = [0u8; 24];
        if std::os::unix::fs::FileExt::read_exact_at(&f, &mut h, 0).is_err() || &h[0..4] != HEADS_MAGIC {
            return false;
        }
        let got = u64::from_le_bytes(h[8..16].try_into().unwrap());
        let used = u64::from_le_bytes(h[16..24].try_into().unwrap()) as usize;
        let flen = f.metadata().map(|m| m.len() as usize).unwrap_or(0);
        if got != stamp || used * HEAD > self.len || HEADS_HEADER + used * HEAD > flen {
            return false;
        }
        if used == 0 {
            return true;
        }
        let ps = crate::segment_map::runtime_page_size();
        let map = ((used * HEAD).div_ceil(ps) * ps).min(flen - HEADS_HEADER);
        let p = unsafe {
            libc::mmap(
                self.base as *mut libc::c_void,
                map,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_FIXED,
                f.as_raw_fd(),
                HEADS_HEADER as libc::off_t,
            )
        };
        if p == libc::MAP_FAILED {
            return false;
        }
        self.hi.store(used as u32, Ordering::Release);
        true
    }

    /// file に置く (きれいに閉じる時、 書き手の居ない所で)。 header に印 `stamp`。 page の倍数に詰める。
    fn persist(&self, path: &Path, stamp: u64) -> io::Result<()> {
        let used = self.hi.load(Ordering::Acquire) as usize;
        let ps = crate::segment_map::runtime_page_size();
        let mut head = vec![0u8; HEADS_HEADER];
        head[0..4].copy_from_slice(HEADS_MAGIC);
        head[8..16].copy_from_slice(&stamp.to_le_bytes());
        head[16..24].copy_from_slice(&(used as u64).to_le_bytes());
        let body = unsafe { std::slice::from_raw_parts(self.base, used * HEAD) };
        let pad = vec![0u8; (used * HEAD).div_ceil(ps) * ps - used * HEAD];
        crate::db_files::write_atomic_parts(path, &[&head, body, &pad])
    }

    #[inline]
    fn slot_ptr(&self, slot: u32) -> Option<*mut u8> {
        let off = slot as usize * HEAD;
        (off + HEAD <= self.len).then(|| unsafe { self.base.add(off) })
    }
}

impl Drop for Heads {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.base as *mut libc::c_void, self.len);
        }
    }
}

/// 差分の並びの表の hash (値は u64 そのもの。 SipHash は書き手の道に重い)。
#[derive(Default)]
struct U64Hash(u64);
impl std::hash::Hasher for U64Hash {
    fn finish(&self) -> u64 {
        self.0.wrapping_mul(0x9E37_79B9_7F4A_7C15)
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 << 8) | u64::from(b);
        }
    }
    fn write_u64(&mut self, v: u64) {
        self.0 = v;
    }
}

/// [`HistCol::plan`] の答え: 足す entry の糸と印。
struct Plan {
    prev: u32,
    depth: u32,
    jump: u32,
    flags: u32,
    disordered: bool,
    /// 足した entry を場所の最新にする (今の住人の entry)
    to_head: bool,
    /// 鎖の上の前の値
    old: u64,
}

/// 宣言した列 1 本の履歴。
pub struct HistCol {
    pub hid: u16,
    entries: SegmentMap,
    heads: Heads,
    /// heads を file から戻せなかった (開いた後に entry から作り直す)
    heads_stale: AtomicBool,
    /// 開く時に heads を file から貼れた
    heads_loaded: AtomicBool,
    dir: PathBuf,
    /// 場所を取った entry の数 (書きかけを含む)
    len: AtomicU32,
    /// 地平 (宣言の時刻)
    horizon: u64,
    /// 見た最大の時刻 (時刻の分からない entry に付ける今)
    clock: AtomicU64,
    /// 見た値の差分の並び (最初に見た時に作る)
    decks: RwLock<HashMap<u64, Vec<Diff>, std::hash::BuildHasherDefault<U64Hash>>>,
    observed: AtomicBool,
    /// 同じ identity の鎖に時刻の戻る entry を足した (開く時の再生・突き合わせ)。 開く道の終わりに繋ぎ直す
    disordered: AtomicBool,
    /// entry を `VOID` にした (繋ぎ直しで鎖から外す)
    voided: AtomicBool,
    readonly: bool,
    /// 案 A: 書き手は entry を糸なしで足して位置を溜め、 まとめて heads を直す (`note_batched` / `drain`)
    batch: AtomicBool,
    /// 案 B: デッキは値ごとの差分の並びでなく、 entry の区間ごとの控え (読みの時に値で並べる、 `pull_at_blocks`)
    blocks_mode: AtomicBool,
    /// 案 C: 見られるまで繋がない。 `unlinked` の間、 書き手は entry を糸なし (`LAZY`) で足すだけ (`note_lazy`)
    lazy: AtomicBool,
    unlinked: AtomicBool,
    /// 案 A の溜め: (場所, entry の位置)。 足した順 (同じ場所は位置の順)
    pending: Mutex<Vec<(u32, u32)>>,
    pend_any: AtomicBool,
    /// 案 A: 溜めを当てるのは 1 本ずつ (同じ場所の entry を順に繋ぐ)
    apply_lock: Mutex<()>,
    /// 案 B の控え
    blocks: RwLock<Blocks>,
    /// 書きかけ (entry を足してから cell を書き終えるまで) の書き手の数
    inflight: AtomicU32,
    /// 確定の線: 書きかけが 0 に戻った時の時計。 この時刻以下の時点の読みは後から変わらない
    settled: AtomicU64,
}

/// 案 A の溜めの上限 (これだけ溜まったら書き手が当てる)。
const BATCH: usize = 1024;
/// 案 B: 小さい区間 (entry の数) と、 それを束ねた大きい区間。
const SMALL: u32 = 4096;
const BIG: u32 = 65536;

/// 案 B の区間 1 つの控え: 差分を (値, 時刻, 位置) の順に並べた物と、 区間の最大の時刻。
struct Block {
    diffs: Vec<BlockDiff>,
    max_time: u64,
}

#[derive(Clone, Copy)]
struct BlockDiff {
    value: u64,
    time: u64,
    eid: u32,
    /// 位置 << 1 | 入った
    idx_in: u32,
}

#[derive(Default)]
struct Blocks {
    /// 大きい区間 b = entry [b*BIG, (b+1)*BIG)
    big: Vec<Arc<Block>>,
    /// 大きい区間の後ろの小さい区間 (big.len()*BIG から)
    small: Vec<Arc<Block>>,
}

/// 試作の既定の形 (環境変数、 列を作る / 開く時に読む): ENCHU_HIST_WRITE=batch (案 A)、 ENCHU_HIST_DECK=blocks (案 B)。
fn default_modes() -> (bool, bool) {
    let w = std::env::var("ENCHU_HIST_WRITE").is_ok_and(|v| v == "batch");
    let d = std::env::var("ENCHU_HIST_DECK").is_ok_and(|v| v == "blocks");
    (w, d)
}

fn entry_path(dir: &Path, hid: u16) -> PathBuf {
    dir.join("hist").join(format!("{hid:04}.seg"))
}

fn head_path(dir: &Path, hid: u16) -> PathBuf {
    dir.join("hist").join(format!("{hid:04}.head.seg"))
}

/// 深さ `d` (≥ 1) の entry の飛び幅: 飛び先の深さは d − 幅 (下の `skew_jump` の規則と同じ。 幅 = d を 2^k − 1 の和に大きい方から
/// 分けた時のいちばん小さい項)。 書き手が飛び先の深さを entry を読まずに出すのに使う。
#[inline]
fn skip_gap(mut d: u32) -> u32 {
    let mut w = 0;
    while d > 0 {
        w = ((1u64 << (63 - (u64::from(d) + 1).leading_zeros())) - 1) as u32;
        d -= w;
    }
    w
}

/// skew binary の飛び糸: 親 `p` (深さ d) の子 (深さ d + 1) の飛び先。 `p` の飛び先 `j` (深さ dj) と `j` の飛び先 `jj` (深さ djj) の
/// 幅が揃っていれば `jj`、 そうでなければ `p`。 深さ 0 の entry の飛び糸は前の住人を指すので、 ここでは自分 (深さ 0) と見る。
#[inline]
fn skew_jump(p_idx: u32, p_depth: u32, j: Option<(u32, u32)>, jj: Option<(u32, u32)>) -> u32 {
    match (j, jj) {
        (Some((_, dj)), Some((jj_idx, djj))) if p_depth > 0 && dj > 0 && p_depth - dj == dj - djj => jj_idx,
        _ => p_idx,
    }
}

impl HistCol {
    /// 新しく作る (宣言)。 既にあれば消して作り直す (前の宣言を外した残り)。
    pub fn create(dir: &Path, hid: u16, horizon: u64, slots: u32) -> io::Result<Self> {
        std::fs::create_dir_all(dir.join("hist"))?;
        let (ep, hp) = (entry_path(dir, hid), head_path(dir, hid));
        let _ = std::fs::remove_file(&ep);
        let _ = std::fs::remove_file(&hp);
        let _ = hp;
        let entries = SegmentMap::create(&ep, RESERVE_ENTRIES, HEADER + 64 * ENTRY)?;
        // heads は場所の全部 (u32) を予約する (NORESERVE、 触った所だけ memory)。 宣言の後に engine の枠が伸びても (`grow_entity_cap`) 足りる
        let _ = slots;
        let heads = Heads::anon(u32::MAX)?;
        let c = HistCol::new(dir, hid, entries, heads, horizon, false);
        c.write_header(0);
        c.entries.flush(0, HEADER)?;
        Ok(c)
    }

    /// 開く。 header が読めなければ Err (呼び手は宣言を捨てる)。
    pub fn open(dir: &Path, hid: u16, slots: u32, readonly: bool) -> io::Result<Self> {
        let entries = SegmentMap::open(&entry_path(dir, hid), RESERVE_ENTRIES, readonly)?;
        let _ = slots;
        let heads = Heads::anon(u32::MAX)?;
        if entries.committed() < HEADER {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "hist: short header"));
        }
        let h = unsafe { std::slice::from_raw_parts(entries.base(), 32) };
        if &h[0..4] != MAGIC || u32::from_le_bytes(h[4..8].try_into().unwrap()) != VERSION {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "hist: bad magic"));
        }
        let stored_hid = u32::from_le_bytes(h[8..12].try_into().unwrap());
        if stored_hid != u32::from(hid) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "hist: himo mismatch"));
        }
        let len = u64::from_le_bytes(h[16..24].try_into().unwrap());
        let horizon = u64::from_le_bytes(h[24..32].try_into().unwrap());
        let c = HistCol::new(dir, hid, entries, heads, horizon, readonly);
        let cap = ((c.entries.committed() - HEADER) / ENTRY) as u64;
        c.len.store(len.min(cap) as u32, Ordering::Relaxed);
        // heads: きれいに閉じた時に置いた file (印 = entry の数) を貼る。 合わなければ開いた後に作り直す
        if c.heads.load(&head_path(dir, hid), len) {
            c.heads_loaded.store(true, Ordering::Release);
        } else {
            c.heads_stale.store(true, Ordering::Release);
        }
        // 時計は最後の entry まで (readonly でも読む)
        let n = c.len.load(Ordering::Relaxed);
        let mut top = horizon;
        for i in n.saturating_sub(4096)..n {
            if let Some(e) = c.valid(i) {
                top = top.max(e.time);
            }
        }
        c.clock.store(top, Ordering::Relaxed);
        Ok(c)
    }

    fn new(dir: &Path, hid: u16, entries: SegmentMap, heads: Heads, horizon: u64, readonly: bool) -> Self {
        HistCol {
            hid,
            entries,
            heads,
            heads_stale: AtomicBool::new(false),
            heads_loaded: AtomicBool::new(false),
            dir: dir.to_path_buf(),
            len: AtomicU32::new(0),
            horizon,
            clock: AtomicU64::new(horizon),
            decks: RwLock::new(HashMap::default()),
            observed: AtomicBool::new(false),
            disordered: AtomicBool::new(false),
            voided: AtomicBool::new(false),
            readonly,
            batch: AtomicBool::new(default_modes().0),
            lazy: AtomicBool::new(std::env::var("ENCHU_HIST_WRITE").is_ok_and(|v| v == "lazy")),
            unlinked: AtomicBool::new(std::env::var("ENCHU_HIST_WRITE").is_ok_and(|v| v == "lazy")),
            blocks_mode: AtomicBool::new(default_modes().1),
            pending: Mutex::new(Vec::new()),
            pend_any: AtomicBool::new(false),
            apply_lock: Mutex::new(()),
            blocks: RwLock::new(Blocks::default()),
            inflight: AtomicU32::new(0),
            settled: AtomicU64::new(horizon),
        }
    }

    /// 列 `hid` の file 2 本 (宣言を外す時に engine が消す。 map は engine が閉じるまで残る)。
    pub fn paths(dir: &Path, hid: u16) -> [PathBuf; 2] {
        [entry_path(dir, hid), head_path(dir, hid)]
    }

    pub fn horizon(&self) -> u64 {
        self.horizon
    }

    /// 見た最大の時刻。 書き手は時計を触らない (書き込みの道の RMW を 1 つ減らす): entry は大体時刻の順に足すので、 末尾の entry の
    /// 時刻の最大と覚えた値の大きい方。
    pub fn clock(&self) -> u64 {
        let n = self.len();
        let mut top = self.clock.load(Ordering::Acquire);
        for i in n.saturating_sub(256)..n {
            if let Some(e) = self.valid(i) {
                top = top.max(e.time);
            }
        }
        self.clock.fetch_max(top, Ordering::AcqRel);
        top
    }

    /// WAL の無い session の書き込みの時刻: 壁時計の ms と、 列の時計 + 1 の大きい方 (同じ列の時刻は戻らない)。 列の時計を進める。
    pub fn now(&self) -> u64 {
        let ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
        let wall = pack(enchudb_oplog::Hlc { wall: ms, logical: 0, peer: 0 });
        let t = wall.max(self.clock.load(Ordering::Acquire) + 1);
        self.clock.fetch_max(t, Ordering::AcqRel);
        t
    }

    /// 書きかけを始める (entry を足す前、 行の lock の下)。
    pub fn begin_write(&self) {
        self.inflight.fetch_add(1, Ordering::AcqRel);
    }

    /// 書きかけを終える (cell を書いた後)。 0 に戻ったら確定の線を今の時計まで進める。
    pub fn end_write(&self) {
        if self.inflight.fetch_sub(1, Ordering::AcqRel) == 1 {
            let c = self.clock();
            // 0 に戻った後に別の書き手が始めていても、 その entry の時刻は今の時計より後 (時刻は増える) なので線は越えない
            self.settled.fetch_max(c, Ordering::AcqRel);
        }
    }

    /// 確定の線 (この時刻以下の時点の読みは後から変わらない)。 書きかけが無ければ今の時計。
    pub fn settled(&self) -> u64 {
        if self.inflight.load(Ordering::Acquire) == 0 {
            let c = self.clock();
            self.settled.fetch_max(c, Ordering::AcqRel);
        }
        self.settled.load(Ordering::Acquire)
    }

    /// 書き手で開いた時: heads の file を外す (貼った中身は private の写しに残る)。 閉じる時に置き直す。 置いたまま書き始めると、 途中の
    /// flush が 「きれいに閉じた」 の印を立てた後に落ちた時、 次に開く process が古い heads を印の合う物として貼る
    pub fn unlink_heads_file(&self) -> PathBuf {
        head_path(&self.dir, self.hid)
    }

    /// 時刻の分からない entry に付ける今 (見た最大 + 1)。
    pub fn tick(&self) -> u64 {
        let t = self.clock() + 1;
        self.clock.fetch_max(t, Ordering::AcqRel);
        t
    }

    /// 場所を取った entry の数。
    pub fn len(&self) -> u32 {
        self.len.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// disk の大きさ (entry の segment の使った分 + heads の commit)。
    pub fn disk_bytes(&self) -> (u64, u64) {
        let hb = std::fs::metadata(head_path(&self.dir, self.hid)).map(|m| m.len()).unwrap_or(0);
        ((HEADER + self.len() as usize * ENTRY) as u64, hb.max((self.heads.hi.load(Ordering::Acquire) as usize * HEAD) as u64))
    }

    fn write_header(&self, len: u32) {
        let h = unsafe { std::slice::from_raw_parts_mut(self.entries.base(), 32) };
        h[0..4].copy_from_slice(MAGIC);
        h[4..8].copy_from_slice(&VERSION.to_le_bytes());
        h[8..12].copy_from_slice(&u32::from(self.hid).to_le_bytes());
        h[12..16].copy_from_slice(&0u32.to_le_bytes());
        h[16..24].copy_from_slice(&u64::from(len).to_le_bytes());
        h[24..32].copy_from_slice(&self.horizon.to_le_bytes());
        self.entries.mark_dirty(0, 32);
    }

    #[inline]
    fn ptr(&self, idx: u32) -> *mut u8 {
        unsafe { self.entries.base().add(HEADER + idx as usize * ENTRY) }
    }

    #[inline]
    fn flags_cell(&self, idx: u32) -> &AtomicU32 {
        unsafe { AtomicU32::from_ptr(self.ptr(idx) as *mut u32) }
    }

    /// entry `idx` (flags の立った物だけ。 `VOID` も返す)。
    #[inline]
    fn valid(&self, idx: u32) -> Option<Entry> {
        if HEADER + (idx as usize + 1) * ENTRY > self.entries.committed() {
            return None;
        }
        let fd = self.flags_cell(idx).load(Ordering::Acquire);
        let flags = fd >> 24;
        if flags & flag::VALID == 0 {
            return None;
        }
        let p = self.ptr(idx);
        unsafe {
            Some(Entry {
                flags,
                depth: fd & DEPTH_MASK,
                eid: std::ptr::read(p.add(4) as *const u32),
                prev: std::ptr::read(p.add(8) as *const u32),
                jump: std::ptr::read(p.add(12) as *const u32),
                time: std::ptr::read(p.add(16) as *const u64),
                value: std::ptr::read(p.add(24) as *const u64),
            })
        }
    }

    /// 糸 (位置 + 1) の先の entry。
    #[inline]
    fn at(&self, link: u32) -> Option<(u32, Entry)> {
        if link == NONE {
            return None;
        }
        self.valid(link - 1).map(|e| (link - 1, e))
    }

    fn write(&self, idx: u32, e: &Entry) {
        let p = self.ptr(idx);
        unsafe {
            std::ptr::write(p.add(4) as *mut u32, e.eid);
            std::ptr::write(p.add(8) as *mut u32, e.prev);
            std::ptr::write(p.add(12) as *mut u32, e.jump);
            std::ptr::write(p.add(16) as *mut u64, e.time);
            std::ptr::write(p.add(24) as *mut u64, e.value);
        }
        self.flags_cell(idx).store((e.flags << 24) | (e.depth & DEPTH_MASK), Ordering::Release);
        self.entries.mark_dirty(HEADER + idx as usize * ENTRY, ENTRY);
    }

    fn alloc(&self) -> io::Result<u32> {
        // SeqCst: 差分の並びを作る側の 「見た」 を立てる → len を読む、 との約束 (`build_deck`。 x86 では lock xadd のまま)
        let idx = self.len.fetch_add(1, Ordering::SeqCst);
        if idx >= u32::MAX - 1 {
            // 位置が u32 を一周すると糸 0 (= 糸なし) と重なる。 足さない
            self.len.store(u32::MAX - 1, Ordering::SeqCst);
            return Err(io::Error::new(io::ErrorKind::StorageFull, "hist: 2^32 entries"));
        }
        let end = HEADER + (idx as usize + 1) * ENTRY;
        if end > self.entries.committed() {
            if let Err(e) = self.entries.grow_amortized(end) {
                // 取った場所は書かない (flags の無い穴。 読みも繋ぎ直しも飛ばす)
                return Err(e);
            }
        }
        Ok(idx)
    }

    #[inline]
    fn head(&self, slot: u32) -> u32 {
        match self.heads.slot_ptr(slot) {
            Some(p) => unsafe { AtomicU32::from_ptr(p as *mut u32) }.load(Ordering::Acquire),
            None => NONE,
        }
    }

    /// heads の写し (書き手、 行の lock の下): (糸, 最新の entry (prev は 0 = 読まない))。
    #[inline]
    fn head_copy(&self, slot: u32) -> Option<(u32, Entry)> {
        let p = self.heads.slot_ptr(slot)?;
        let link = unsafe { AtomicU32::from_ptr(p as *mut u32) }.load(Ordering::Acquire);
        if link == NONE {
            return None;
        }
        unsafe {
            let fd = std::ptr::read(p.add(8) as *const u32);
            Some((
                link - 1,
                Entry {
                    flags: fd >> 24,
                    depth: fd & DEPTH_MASK,
                    eid: std::ptr::read(p.add(4) as *const u32),
                    prev: 0,
                    jump: std::ptr::read(p.add(12) as *const u32),
                    time: std::ptr::read(p.add(16) as *const u64),
                    value: std::ptr::read(p.add(24) as *const u64),
                },
            ))
        }
    }

    /// 場所 `slot` の最新を entry `link` (`e` はその中身、 None なら空にする) にする。
    fn set_head(&self, slot: u32, link: u32, e: Option<&Entry>) -> io::Result<()> {
        let Some(p) = self.heads.slot_ptr(slot) else {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "hist: slot beyond the heads"));
        };
        unsafe {
            if let Some(e) = e {
                std::ptr::write(p.add(4) as *mut u32, e.eid);
                std::ptr::write(p.add(8) as *mut u32, (e.flags << 24) | (e.depth & DEPTH_MASK));
                std::ptr::write(p.add(12) as *mut u32, e.jump);
                std::ptr::write(p.add(16) as *mut u64, e.time);
                std::ptr::write(p.add(24) as *mut u64, e.value);
            }
            AtomicU32::from_ptr(p as *mut u32).store(if e.is_some() { link } else { NONE }, Ordering::Release);
        }
        self.heads.hi.fetch_max(slot + 1, Ordering::AcqRel);
        Ok(())
    }

    /// heads を file から戻せなかったか (呼び手は開いた後に [`Self::relink`] する)。
    pub fn heads_loaded(&self) -> bool {
        self.heads_loaded.load(Ordering::Acquire)
    }

    pub fn heads_stale(&self) -> bool {
        self.heads_stale.load(Ordering::Acquire)
    }

    /// きれいに閉じる時 (書き手の居ない所): heads を file に置く (印 = 書き出した entry の数)。
    pub fn persist_heads(&self) -> io::Result<()> {
        self.drain();
        if self.readonly {
            return Ok(());
        }
        self.write_out(false)?;
        self.heads.persist(&head_path(&self.dir, self.hid), u64::from(self.len()))
    }

    /// 場所 `slot` の鎖で、 identity `eid` の最新の entry (前の住人へは最初の entry の飛び糸でたどる)。
    fn latest_of(&self, slot: u32, eid: u32) -> Option<(u32, Entry)> {
        let mut cur = self.at(self.head(slot));
        while let Some((idx, e)) = cur {
            if e.eid == eid {
                return Some((idx, e));
            }
            // この identity の最初の entry まで飛んで、 前の住人へ
            let (_, first) = self.first_of(idx, e);
            cur = self.at(first.jump);
        }
        None
    }

    /// identity の最初の entry (深さ 0) まで飛び糸でたどる。
    fn first_of(&self, mut idx: u32, mut e: Entry) -> (u32, Entry) {
        while e.depth > 0 {
            let next = if e.jump != NONE && e.jump != e.prev { e.jump } else { e.prev };
            match self.at(next) {
                Some((i, n)) => {
                    idx = i;
                    e = n;
                }
                None => break,
            }
        }
        (idx, e)
    }

    /// 鎖 (時刻の順) で、 時刻 `t` 以下の最新の entry と、 その次の entry (無ければ None)。 `from` は identity の最新。
    fn floor(&self, from: (u32, Entry), t: u64) -> (Option<(u32, Entry)>, Option<Entry>) {
        let (mut idx, mut e) = from;
        let mut next: Option<Entry> = None;
        loop {
            if e.time <= t {
                return (Some((idx, e)), next);
            }
            if e.depth == 0 {
                return (None, Some(e));
            }
            // 飛び先も t より後なら飛ぶ (飛ばした entry のうち一番古い物が 「次」 の候補 = 飛び先の 1 つ後。 飛び先の後は
            // 飛ばした区間の中なので、 次は prev で 1 つずつ戻った時に決める)
            if e.jump != e.prev
                && let Some((ji, je)) = self.at(e.jump)
                && je.time > t
            {
                idx = ji;
                e = je;
                next = None;
                continue;
            }
            match self.at(e.prev) {
                Some((pi, pe)) => {
                    next = Some(e);
                    idx = pi;
                    e = pe;
                }
                None => return (None, Some(e)),
            }
        }
    }

    /// 書き込みを 1 件足す (行の lock の下で呼ぶ)。 足したら true、 同じ時刻の entry がある / 値が変わらないなら false。
    ///
    /// `value` None = 値が無くなった (untie / 削除)。 `extra` は `OBSERVED` / `DELETE` / `BASE`。 時刻の分からない (`OBSERVED`)
    /// entry の時刻は呼び手が [`Self::tick`] で取る。
    pub fn note(&self, slot: u32, eid: u32, value: Option<u64>, time: u64, extra: u32) -> io::Result<bool> {
        self.drain();
        let v = value.unwrap_or(ABSENT);
        let Some(pl) = self.plan(slot, eid, v, time, extra, value.is_none()) else { return Ok(false) };
        let idx = self.alloc()?;
        let ne = Entry { flags: pl.flags, depth: pl.depth, eid, prev: pl.prev, jump: pl.jump, time, value: v };
        self.write(idx, &ne);
        if pl.disordered {
            // 時刻の戻る entry / 前の住人の entry: 鎖を時刻の順に繋ぎ直す (差分の並びも捨てる。 次に見た時に作り直す)
            self.relink_slot(slot, idx)?;
            return Ok(true);
        }
        if pl.to_head {
            self.set_head(slot, idx + 1, Some(&ne))?;
        }
        // 見た値の差分の並びに足す。 場所を取った (len の SeqCst の RMW) 後に読む: 作る側が 「見た」 を立てた後に len を読んだなら、
        // その len より後ろの場所を取った書き手はここで 「見た」 を見る。 前の場所の entry は作る側が待って読む (`build_deck`)
        if self.observed.load(Ordering::SeqCst) {
            self.push_diffs(idx, eid, time, pl.old, v);
        }
        Ok(true)
    }

    /// 足す entry の糸と印を決める (場所 `slot` の鎖を読む)。 足さない (同じ書き込み / 値が変わらない) なら None。
    fn plan(&self, slot: u32, eid: u32, v: u64, time: u64, extra: u32, absent: bool) -> Option<Plan> {
        let hc = self.head_copy(slot);
        let head = hc.map_or(NONE, |(i, _)| i + 1);
        let mut disordered = false;
        // 速い道: 場所の最新がこの identity (heads の写しだけを読む)。 でなければ鎖をたどる (前の住人 / 新しい住人)
        let latest = match hc {
            Some((i, e)) if e.eid == eid => Some((i, e)),
            _ => self.latest_of(slot, eid),
        };
        let (prev, depth, jump, flags) = match latest {
            Some((li, le)) => {
                if extra & flag::OBSERVED == 0 && time <= le.time {
                    // 同じ時刻・同じ値の entry があれば足さない (再生・当て直し)。 時刻だけ同じで値が違うのは別の書き込み: 時刻を詰める
                    // 所で logical を切った (壁時計が戻っていた間、 HLC は wall を止めて logical を増やす) / 時計の entry (`OBSERVED`)
                    let (fl, _) = self.floor(self.at(li + 1).unwrap_or((li, le)), time);
                    if fl.is_some_and(|(_, f)| f.time == time && f.value == v && f.flags & flag::OBSERVED == 0) {
                        return None;
                    }
                    // 時刻の戻る entry (再生が、 後の entry まで届いた像に前の書き込みを当て直す): その時刻の前の値と同じなら変更
                    // ではない (書いた時も足さなかった)
                    let before = fl.map_or(ABSENT, |(_, f)| f.value);
                    if before == v && extra & flag::BASE == 0 {
                        return None;
                    }
                    // 最新と同じ時刻で値の違う書き込みは、 順に足す (鎖の上で後の entry が勝つ = 書いた順)
                    disordered = time < le.time;
                }
                if le.value == v && extra & flag::BASE == 0 && time >= le.time {
                    return None; // 値が変わらない (書き直し) は変更ではない
                }
                // 飛び先の深さは深さだけで決まる (d − skip_gap(d))。 幅が揃う時だけ飛び先の entry を読んで、 その飛び先へ
                let jump = if le.depth == 0 {
                    li + 1
                } else {
                    let dj = le.depth - skip_gap(le.depth);
                    let djj = if dj > 0 { dj - skip_gap(dj) } else { 0 };
                    match self.at(le.jump) {
                        Some((_, je)) if dj > 0 && le.depth - dj == dj - djj => je.jump,
                        _ => li + 1,
                    }
                };
                if head != li + 1 {
                    // 前の住人の entry (今の住人の後ろ): 鎖の頭にはできない。 足した後にその場所を繋ぎ直す
                    disordered = true;
                }
                (li + 1, le.depth + 1, jump, 0)
            }
            None => {
                if absent {
                    return None; // 値の無い所を外す = 変更ではない
                }
                (NONE, 0, head, flag::FIRST)
            }
        };
        Some(Plan {
            prev,
            depth,
            jump,
            flags: flags | flag::VALID | extra,
            disordered,
            to_head: latest.is_none() || latest.is_some_and(|(li, _)| head == li + 1),
            old: latest.map(|(_, e)| e.value).unwrap_or(ABSENT),
        })
    }

    /// 案 A が有効か。
    pub fn batched(&self) -> bool {
        self.batch.load(Ordering::Relaxed)
    }

    /// 案 B が有効か。
    pub fn blocks_mode(&self) -> bool {
        self.blocks_mode.load(Ordering::Relaxed)
    }

    /// 試作の形を切り替える (測りの交互の回し用)。 案 A を外す時は溜めを当ててから。 デッキの形を変えたら前の形の控えは捨てる。
    pub fn set_modes(&self, batch: bool, blocks: bool) {
        if !batch {
            self.batch.store(false, Ordering::Release);
            self.drain();
        } else {
            self.batch.store(true, Ordering::Release);
        }
        if blocks != self.blocks_mode() {
            self.blocks_mode.store(blocks, Ordering::Release);
            self.forget_decks();
            *self.blocks.write().unwrap_or_else(|p| p.into_inner()) = Blocks::default();
        }
    }

    /// 案 C: 見られるまで繋がない形にする / やめる (やめる時は呼び手が先に [`Self::link_all`] で繋ぐ)。 する時は次の書き込みから
    /// 糸なしで足す (今ある鎖はそのまま、 次の読みが全体を繋ぐ)。
    pub fn set_lazy(&self, on: bool) {
        self.lazy.store(on, Ordering::Release);
        if on {
            self.unlinked.store(true, Ordering::Release);
        }
    }

    /// 案 C: 繋いでいない entry があるか (読む前に [`Self::link_all`] が要る)。
    #[inline]
    pub fn unlinked(&self) -> bool {
        self.unlinked.load(Ordering::Acquire)
    }

    /// 案 C の書き込み (行の lock の下): entry を糸なし (`LAZY`) で足すだけ。 heads も cell の前の値も読まない。 値の変わらない書き込みも
    /// 足す (繋ぐ時に落とす)。 足した後に 「繋いだ」 を見たら (読み手が繋いだ後に足した)、 この場所を繋ぎ直す。
    pub fn note_lazy(&self, slot: u32, eid: u32, value: Option<u64>, time: u64, extra: u32) -> io::Result<bool> {
        let v = value.unwrap_or(ABSENT);
        let idx = self.alloc()?;
        self.write(idx, &Entry { flags: flag::VALID | flag::LAZY | extra, depth: 0, eid, prev: NONE, jump: NONE, time, value: v });
        if !self.unlinked.load(Ordering::SeqCst) {
            self.relink_slot(slot, idx)?;
        }
        Ok(true)
    }

    /// 案 C: 列全体を繋ぐ (呼び手は列の書き手を止めている)。 繋いだ entry の数と、 かかった時間。
    pub fn link_all(&self, slot_of: impl Fn(u32) -> u32) -> io::Result<(usize, std::time::Duration)> {
        let t0 = std::time::Instant::now();
        if !self.unlinked.load(Ordering::Acquire) {
            return Ok((0, t0.elapsed()));
        }
        let n = self.relink(slot_of)?;
        // 繋いだ後の書き込みは今の形 (か案 A) で
        self.lazy.store(false, Ordering::Release);
        self.unlinked.store(false, Ordering::SeqCst);
        Ok((n, t0.elapsed()))
    }

    /// 案 C が有効で、 まだ繋いでいないか (書き手が `note_lazy` を使う)。
    #[inline]
    pub fn lazy_unlinked(&self) -> bool {
        self.lazy.load(Ordering::Relaxed) && self.unlinked.load(Ordering::Acquire)
    }

    /// 案 A の書き込み (行の lock の下): 値が cell の前の値 `cell_old` と同じなら足さない。 entry を糸なし (`PEND`) で足して位置を溜める。
    /// 糸・heads は [`Self::drain`] がまとめて繋ぐ (溜めが [`BATCH`] に届いた書き手か、 読み手)。 heads を読まないので、 乱れた行への
    /// 書き換えが heads の cache の外れを払わない (まとめて繋ぐ時に先読みで重ねる)。
    pub fn note_batched(&self, slot: u32, eid: u32, value: Option<u64>, time: u64, extra: u32, cell_old: Option<u64>) -> io::Result<bool> {
        let v = value.unwrap_or(ABSENT);
        if value == cell_old && extra & flag::BASE == 0 {
            return Ok(false);
        }
        let idx = self.alloc()?;
        self.write(idx, &Entry { flags: flag::VALID | flag::PEND | extra, depth: 0, eid, prev: NONE, jump: NONE, time, value: v });
        let full = {
            let mut p = self.pending.lock().unwrap_or_else(|p| p.into_inner());
            p.push((slot, idx));
            self.pend_any.store(true, Ordering::Release);
            p.len() >= BATCH
        };
        if self.observed.load(Ordering::SeqCst) {
            self.push_diffs(idx, eid, time, cell_old.unwrap_or(ABSENT), v);
        }
        if full {
            self.drain();
        }
        Ok(true)
    }

    /// 案 A の溜めを繋ぐ (溜めが無ければ atomic 1 回)。 読み手・開く道・書き出し (閉じる時) の前に呼ぶ。
    pub fn drain(&self) {
        if !self.pend_any.load(Ordering::Acquire) {
            return;
        }
        let _one = self.apply_lock.lock().unwrap_or_else(|p| p.into_inner());
        let list = {
            let mut p = self.pending.lock().unwrap_or_else(|p| p.into_inner());
            self.pend_any.store(false, Ordering::Release);
            std::mem::take(&mut *p)
        };
        for (i, &(slot, idx)) in list.iter().enumerate() {
            // heads の場所を先に読み始める (外れを並べて重ねる)
            #[cfg(target_arch = "x86_64")]
            if let Some(&(s2, _)) = list.get(i + 8)
                && let Some(p) = self.heads.slot_ptr(s2)
            {
                unsafe { std::arch::x86_64::_mm_prefetch(p as *const i8, std::arch::x86_64::_MM_HINT_T0) };
            }
            let _ = i;
            if self.apply_one(slot, idx).is_err() {
                // heads を伸ばせない (場所が範囲の外): 鎖に入らない entry。 開く時の繋ぎ直しが拾う
                self.heads_stale.store(true, Ordering::Release);
            }
        }
    }

    /// 溜めた entry `idx` を繋ぐ。
    fn apply_one(&self, slot: u32, idx: u32) -> io::Result<()> {
        let Some(mut e) = self.valid(idx) else { return Ok(()) };
        let extra = e.flags & (flag::OBSERVED | flag::DELETE | flag::BASE);
        let Some(pl) = self.plan(slot, e.eid, e.value, e.time, extra, e.value == ABSENT) else {
            // 繋がない entry (鎖の最新と同じ値・同じ時刻): 外す (鎖から届かない。 繋ぎ直しも拾わない)
            let c = self.flags_cell(idx);
            c.store((c.load(Ordering::Acquire) | (flag::VOID << 24)) & !(flag::PEND << 24), Ordering::Release);
            self.entries.mark_dirty(HEADER + idx as usize * ENTRY, ENTRY);
            return Ok(());
        };
        e.flags = pl.flags;
        e.depth = pl.depth;
        e.prev = pl.prev;
        e.jump = pl.jump;
        self.write(idx, &e);
        if pl.disordered {
            return self.relink_slot(slot, idx);
        }
        if pl.to_head {
            self.set_head(slot, idx + 1, Some(&e))?;
        }
        Ok(())
    }

    fn push_diffs(&self, idx: u32, eid: u32, time: u64, old: u64, new: u64) {
        let mut decks = self.decks.write().unwrap_or_else(|p| p.into_inner());
        for (val, inn) in [(old, false), (new, true)] {
            if val == ABSENT || old == new {
                continue;
            }
            if let Some(d) = decks.get_mut(&val) {
                let x = Diff { time, idx_in: (idx << 1) | u32::from(inn), eid };
                // 普通は末尾 (時刻の順に足す)。 末尾だけ見る (並び全体の二分探索は cold な memory を 10 回ほど読む)
                if d.last().is_none_or(|y| (y.time, y.idx_in >> 1) < (time, idx)) {
                    d.push(x);
                    continue;
                }
                let pos = d.partition_point(|y| (y.time, y.idx_in >> 1) < (time, idx));
                if d.get(pos).is_some_and(|y| y.time == time && y.idx_in >> 1 == idx) {
                    continue; // 作る時になめた entry
                }
                d.insert(pos, x);
            }
        }
    }

    /// identity `eid` (場所 `slot`) の時点 `t` の値。 `t` が地平より前なら [`TooOld`]。
    pub fn value_at(&self, slot: u32, eid: u32, t: u64) -> Result<At, TooOld> {
        self.drain();
        if t < self.horizon {
            return Err(TooOld { horizon: self.horizon });
        }
        let Some(latest) = self.latest_of(slot, eid) else {
            return Ok(At { value: None, seq: None, uncertain: false });
        };
        // 飛んだ後は必ず prev で 1 歩以上戻ってから止まるので、 floor の返す 「次」 は答えた entry の 1 つ後
        let (fl, next) = self.floor(latest, t);
        let uncertain = next.is_some_and(|n| n.flags & flag::OBSERVED != 0);
        Ok(match fl {
            Some((_, e)) => At { value: (e.value != ABSENT).then_some(e.value), seq: Some(e.depth), uncertain },
            None => At { value: None, seq: None, uncertain },
        })
    }

    /// identity `eid` の `n` 回目の entry (最初 = 0)。
    pub fn at_seq(&self, slot: u32, eid: u32, n: u32) -> Option<Change> {
        self.drain();
        let (mut idx, mut e) = self.latest_of(slot, eid)?;
        if n > e.depth {
            return None;
        }
        while e.depth > n {
            let next = match self.at(e.jump) {
                Some((_, je)) if e.jump != e.prev && je.depth >= n && je.depth < e.depth => e.jump,
                _ => e.prev,
            };
            let (i, ne) = self.at(next)?;
            idx = i;
            e = ne;
        }
        let _ = idx;
        Some(change(&e))
    }

    /// identity `eid` の変更の一覧 (古い順)。
    pub fn history(&self, slot: u32, eid: u32) -> Vec<Change> {
        self.drain();
        let mut out = Vec::new();
        let mut cur = self.latest_of(slot, eid);
        while let Some((_, e)) = cur {
            out.push(change(&e));
            if e.depth == 0 {
                break;
            }
            cur = self.at(e.prev);
        }
        out.reverse();
        out
    }

    /// 時点 `t` に値 `v` だったカード (eid) の束 (順不同)。 その値を初めて見た時に差分の並びを作る。
    pub fn pull_at(&self, v: u64, t: u64) -> Result<Vec<u32>, TooOld> {
        self.drain();
        if t < self.horizon {
            return Err(TooOld { horizon: self.horizon });
        }
        {
            let decks = self.decks.read().unwrap_or_else(|p| p.into_inner());
            if let Some(d) = decks.get(&v) {
                return Ok(members(d, t));
            }
        }
        let d = self.build_deck(v);
        Ok(members(&d, t))
    }

    /// 値 `v` の差分の並びを作る (entry を全部なめる)。 作った物の写しを返す。
    fn build_deck(&self, v: u64) -> Vec<Diff> {
        // entry をなめる間は lock を握らない (握ると、 デッキを見た列の書き手全部が `push_diffs` で行の lock を持ったまま待つ)。 「見た」 を
        // 立ててから n を読み、 [0, n) を lock の外で、 [n, 今) を lock の中でなめてから入れる。 その間に足して push した書き手の差分は
        // まだ表に無い値には入らない (入れる前) か、 入れた後なら (時刻, 位置) で重複を落とす
        {
            let decks = self.decks.read().unwrap_or_else(|p| p.into_inner());
            if let Some(d) = decks.get(&v) {
                return d.clone();
            }
        }
        self.observed.store(true, Ordering::SeqCst);
        let n = self.len.load(Ordering::SeqCst);
        let mut d = Vec::new();
        self.scan_deck(v, 0, n, &mut d, true);
        let mut decks = self.decks.write().unwrap_or_else(|p| p.into_inner());
        if let Some(have) = decks.get(&v) {
            return have.clone();
        }
        let n2 = self.len.load(Ordering::SeqCst);
        // [n, 今) の溜めた (`PEND`) entry は、 書き手が 「見た」 を見て自分で差分を足す (場所を取った後に読むので)
        self.scan_deck(v, n, n2, &mut d, false);
        d.sort_unstable_by_key(|x| (x.time, x.idx_in >> 1));
        d.dedup_by_key(|x| (x.time, x.idx_in));
        decks.insert(v, d.clone());
        d
    }

    /// entry [lo, hi) の、 値 `v` の差分を `d` に足す。
    ///
    /// `drain`: 溜めた (`PEND`) entry は溜めを繋いでから読む (lock の外の走査だけ。 案 A の書き手は 「見た」 を見なかったかもしれない)。
    fn scan_deck(&self, v: u64, lo: u32, hi: u32, d: &mut Vec<Diff>, drain: bool) {
        for idx in lo..hi {
            // 場所を取って書き終わっていない entry は書き終わるのを待って読む (場所を取って書けなかった穴 (伸ばせなかった) は 10 ms で諦める)
            let mut e = self.valid(idx);
            if e.is_none() && HEADER + (idx as usize + 1) * ENTRY <= self.entries.committed() {
                let t0 = std::time::Instant::now();
                while e.is_none() && t0.elapsed() < std::time::Duration::from_millis(10) {
                    std::hint::spin_loop();
                    e = self.valid(idx);
                }
            }
            let Some(mut e) = e else { continue };
            if e.flags & flag::VOID != 0 {
                continue;
            }
            if drain && e.flags & flag::PEND != 0 {
                // 書き手が溜めに入れるのは entry を書いた後: 入るのを待って繋ぐ
                let t0 = std::time::Instant::now();
                while e.flags & flag::PEND != 0 && t0.elapsed() < std::time::Duration::from_millis(10) {
                    self.drain();
                    std::hint::spin_loop();
                    match self.valid(idx) {
                        Some(x) => e = x,
                        None => break,
                    }
                }
            }
            if e.flags & (flag::VOID | flag::PEND) != 0 {
                continue;
            }
            let old = if e.depth == 0 { ABSENT } else { self.at(e.prev).map_or(ABSENT, |(_, p)| p.value) };
            if old == e.value {
                continue;
            }
            if old == v {
                d.push(Diff { time: e.time, idx_in: idx << 1, eid: e.eid });
            }
            if e.value == v {
                d.push(Diff { time: e.time, idx_in: (idx << 1) | 1, eid: e.eid });
            }
        }
    }

    /// 案 B: 時点 `t` に値 `v` だった eid の束。 `current` = 今その値の eid (円柱)。 t より後の差分を、 閉じた区間ごとの控え (読みの時に
    /// 作って値で並べる) と開いた末尾 (なめる) から集め、 eid ごとに t より後の最初の差分で t の時を決める (入った → t には居ない、
    /// 出た → 居た)。 書き手は何もしない (値ごとの並びを保たない)。
    pub fn pull_at_blocks(&self, v: u64, t: u64, current: &[u32]) -> Result<Vec<u32>, TooOld> {
        if t < self.horizon {
            return Err(TooOld { horizon: self.horizon });
        }
        self.drain();
        let n = self.len.load(Ordering::SeqCst);
        let (big, small) = self.closed_blocks(n);
        let covered = (big.len() as u32) * BIG + (small.len() as u32) * SMALL;
        // eid → t より後の最初の差分 ((時刻, 位置), 入ったか)。 最初は時刻で決める: 位置の順は時刻の順とは限らない (電源断の後の再生は前の
        // 時刻の entry を末尾に足す)
        let mut first: HashMap<u32, ((u64, u32), bool), std::hash::BuildHasherDefault<U64Hash>> = HashMap::default();
        let mut take = |d: &BlockDiff| {
            let k = (d.time, d.idx_in >> 1);
            let inn = d.idx_in & 1 == 1;
            first.entry(d.eid).and_modify(|x| if k < x.0 { *x = (k, inn) }).or_insert((k, inn));
        };
        for b in big.iter().chain(small.iter()) {
            if b.max_time <= t {
                continue;
            }
            let lo = b.diffs.partition_point(|d| (d.value, d.time) <= (v, t));
            for d in b.diffs[lo..].iter().take_while(|d| d.value == v) {
                take(d);
            }
        }
        for idx in covered..n {
            for d in self.diffs_of(idx) {
                if d.value == v && d.time > t {
                    take(&d);
                }
            }
        }
        // 今の束に居て t より後の最初が 「出た」 (出て戻った) 行は、 今の束の側で数える (二重にしない)
        let mut out: Vec<u32> = current.iter().copied().filter(|e| first.get(e).is_none_or(|x| !x.1)).collect();
        let now: std::collections::HashSet<u32, std::hash::BuildHasherDefault<U64Hash>> = current.iter().copied().collect();
        out.extend(first.iter().filter(|&(e, x)| !x.1 && !now.contains(e)).map(|(&e, _)| e));
        Ok(out)
    }

    /// entry `idx` の差分 (古い値から出た / 新しい値に入った)。 繋いでいない / 外した entry は無し。
    fn diffs_of(&self, idx: u32) -> impl Iterator<Item = BlockDiff> {
        let mut out = [None, None];
        if let Some(e) = self.valid(idx)
            && e.flags & (flag::VOID | flag::PEND) == 0
        {
            let old = if e.depth == 0 { ABSENT } else { self.at(e.prev).map_or(ABSENT, |(_, p)| p.value) };
            if old != e.value {
                if old != ABSENT {
                    out[0] = Some(BlockDiff { value: old, time: e.time, eid: e.eid, idx_in: idx << 1 });
                }
                if e.value != ABSENT {
                    out[1] = Some(BlockDiff { value: e.value, time: e.time, eid: e.eid, idx_in: (idx << 1) | 1 });
                }
            }
        }
        out.into_iter().flatten()
    }

    /// entry [lo, hi) の控え。
    fn make_block(&self, lo: u32, hi: u32) -> Arc<Block> {
        let mut diffs: Vec<BlockDiff> = (lo..hi).flat_map(|i| self.diffs_of(i)).collect();
        diffs.sort_unstable_by_key(|d| (d.value, d.time, d.idx_in >> 1));
        let max_time = diffs.iter().map(|d| d.time).max().unwrap_or(0);
        Arc::new(Block { diffs, max_time })
    }

    /// `n` 個の entry のうち閉じた区間の控え (無ければ作る。 小さい区間が大きい区間 1 つ分溜まったら大きい区間に作り直す)。
    fn closed_blocks(&self, n: u32) -> (Vec<Arc<Block>>, Vec<Arc<Block>>) {
        {
            let b = self.blocks.read().unwrap_or_else(|p| p.into_inner());
            let covered = (b.big.len() as u32) * BIG + (b.small.len() as u32) * SMALL;
            if n - covered.min(n) < SMALL {
                return (b.big.clone(), b.small.clone());
            }
        }
        let mut b = self.blocks.write().unwrap_or_else(|p| p.into_inner());
        while (b.big.len() as u32 + 1) * BIG <= n {
            let lo = b.big.len() as u32 * BIG;
            let blk = self.make_block(lo, lo + BIG);
            b.big.push(blk);
            let skip = (BIG / SMALL) as usize;
            let k = b.small.len().min(skip);
            b.small.drain(..k);
        }
        loop {
            let covered = (b.big.len() as u32) * BIG + (b.small.len() as u32) * SMALL;
            if covered + SMALL > n {
                break;
            }
            let blk = self.make_block(covered, covered + SMALL);
            b.small.push(blk);
        }
        (b.big.clone(), b.small.clone())
    }

    /// 書き出し: header に `len` を書いて entry と heads を msync (`all` = commit 済み全域)。
    pub fn write_out(&self, all: bool) -> io::Result<()> {
        if self.readonly {
            return Ok(());
        }
        self.write_header(self.len());
        if all {
            self.entries.flush_all()?;
        } else {
            self.entries.flush_dirty()?;
        }
        Ok(())
    }

    pub fn has_dirty(&self) -> bool {
        self.entries.flush_pending()
    }

    /// 開く時の突き合わせが使う: 場所 `slot` の住人 (新しい順) の (eid, 最新の値, 最新の時刻, 最初の時刻)。
    pub fn residents(&self, slot: u32) -> Vec<(u32, Option<u64>, u64, u64)> {
        self.drain();
        let mut out = Vec::new();
        let mut cur = self.at(self.head(slot));
        while let Some((idx, e)) = cur {
            let (_, first) = self.first_of(idx, e);
            out.push((e.eid, (e.value != ABSENT).then_some(e.value), e.time, first.time));
            cur = self.at(first.jump);
            if out.len() > 1 << 20 {
                break;
            }
        }
        out
    }

    /// 開く時の突き合わせが使う: 場所 `slot` の identity `eid` の最新の entry の (値, 時刻)。
    pub fn latest_value(&self, slot: u32, eid: u32) -> Option<(Option<u64>, u64)> {
        self.drain();
        self.latest_of(slot, eid).map(|(_, e)| ((e.value != ABSENT).then_some(e.value), e.time))
    }

    /// 開く時の突き合わせ: identity `eid` の entry を新しい方から見て、 `keep` が true の entry に当たるまでの entry を `VOID` に
    /// する。 当たった entry の (値, 時刻) を返す。 当たらなければ、 `void_all` なら全部を `VOID` にし、 でなければ何もしない
    /// (どちらも None)。 外した entry は繋ぎ直し ([`Self::relink`]) が鎖から外す。
    pub fn void_newer(
        &self,
        slot: u32,
        eid: u32,
        void_all: bool,
        keep: impl Fn(Option<u64>, u64) -> bool,
    ) -> Option<(Option<u64>, u64)> {
        let mut cur = self.latest_of(slot, eid);
        let mut list = Vec::new();
        while let Some((idx, e)) = cur {
            let v = (e.value != ABSENT).then_some(e.value);
            if keep(v, e.time) {
                for i in list {
                    self.mark_void(i);
                }
                return Some((v, e.time));
            }
            list.push(idx);
            if e.depth == 0 {
                break;
            }
            cur = self.at(e.prev);
        }
        if void_all {
            for i in list {
                self.mark_void(i);
            }
        }
        None
    }

    fn mark_void(&self, idx: u32) {
        let c = self.flags_cell(idx);
        let fd = c.load(Ordering::Acquire);
        c.store(fd | (flag::VOID << 24), Ordering::Release);
        self.entries.mark_dirty(HEADER + idx as usize * ENTRY, ENTRY);
        self.voided.store(true, Ordering::Release);
    }

    /// 繋ぎ直しが要るか (時刻の戻る entry を足した / `VOID` にした)。
    pub fn needs_relink(&self) -> bool {
        self.disordered.load(Ordering::Acquire) || self.voided.load(Ordering::Acquire)
    }

    /// 鎖と heads を entry から作り直す (書き手の居ない時、 開く道だけ)。 header の `len` より後でも flags の立った entry は拾う
    /// (書き出した len の後に OS が書いた entry)。 同じ identity の同じ時刻の entry は 1 つにする。 `slot_of` は eid → 場所。
    /// 作り直した entry の数を返す。
    pub fn relink(&self, slot_of: impl Fn(u32) -> u32) -> io::Result<usize> {
        self.drain();
        // 拾う範囲: commit 済みの全域 (flags の無い所は飛ばす)
        let cap = ((self.entries.committed().saturating_sub(HEADER)) / ENTRY) as u32;
        let mut all: Vec<(u32, u32, u64, u32)> = Vec::new(); // (場所, eid, 時刻, 位置)
        let mut top = 0u32;
        for idx in 0..cap {
            let Some(e) = self.valid(idx) else { continue };
            top = idx + 1;
            if e.flags & flag::VOID != 0 {
                continue;
            }
            all.push((slot_of(e.eid), e.eid, e.time, idx));
        }
        all.sort_unstable_by_key(|&(s, _, _, idx)| (s, idx));
        // heads を空にしてから置く
        for s in 0..self.heads.hi.load(Ordering::Acquire) {
            if self.head(s) != NONE {
                self.set_head(s, NONE, None)?;
            }
        }
        let mut rewritten = 0;
        let mut clock = self.horizon;
        for group in all.chunk_by(|a, b| a.0 == b.0) {
            let mut items: Vec<(u32, u64, u32)> = group.iter().map(|&(_, eid, t, idx)| (eid, t, idx)).collect();
            clock = items.iter().fold(clock, |c, x| c.max(x.1));
            rewritten += self.link_slot(group[0].0, &mut items)?;
        }
        self.len.store(top.max(self.len()), Ordering::Release);
        self.clock.fetch_max(clock, Ordering::AcqRel);
        self.disordered.store(false, Ordering::Release);
        self.voided.store(false, Ordering::Release);
        self.heads_stale.store(false, Ordering::Release);
        self.forget_decks();
        Ok(rewritten)
    }

    fn forget_decks(&self) {
        self.decks.write().unwrap_or_else(|p| p.into_inner()).clear();
        self.observed.store(false, Ordering::SeqCst);
        // 案 B の控えも (糸を書き換えると、 控えの 「前の値」 が変わりうる)
        *self.blocks.write().unwrap_or_else(|p| p.into_inner()) = Blocks::default();
    }

    /// 場所 `slot` の entry (`items`: (eid, 時刻, 位置)、 並びは問わない) で、 その場所の鎖と heads を作り直す: identity を最初の
    /// 時刻の順に、 identity の中は時刻の順に並べ、 同じ時刻の entry は 1 つにする (残りは `VOID`)。 繋いだ entry の数。
    fn link_slot(&self, slot: u32, items: &mut [(u32, u64, u32)]) -> io::Result<usize> {
        let mut first_time: HashMap<u32, u64> = HashMap::new();
        for &(eid, t, _) in items.iter() {
            let f = first_time.entry(eid).or_insert(t);
            *f = (*f).min(t);
        }
        items.sort_unstable_by_key(|&(eid, t, idx)| (first_time[&eid], eid, t, idx));
        let mut rewritten = 0;
        let mut resident_last = NONE;
        let mut last_entry: Option<Entry> = None;
        let mut i = 0;
        while i < items.len() {
            let eid = items[i].0;
            // この identity の鎖: 深さ = 並びの位置。 糸 (位置 + 1) → (深さ, 飛び糸) を引く表
            let mut chain: Vec<u32> = Vec::new(); // 深さ → 位置
            let mut info: HashMap<u32, (u32, u32)> = HashMap::new(); // 糸 → (深さ, 飛び糸)
            let mut last_time: Option<u64> = None;
            let mut last_value: Option<u64> = None;
            while i < items.len() && items[i].0 == eid {
                let (_, t, idx) = items[i];
                i += 1;
                if last_time == Some(t) && self.valid(idx).map(|x| x.value) == last_value {
                    self.mark_void(idx); // 同じ時刻・同じ値 = 同じ書き込み
                    continue;
                }
                let mut e = self.valid(idx).expect("拾った entry");
                // 値の変わらない entry (案 C は書き直しも足す) と、 identity の最初の 「値なし」 は変更ではない
                if e.flags & flag::BASE == 0 && (Some(e.value) == last_value || (last_value.is_none() && e.value == ABSENT)) {
                    self.mark_void(idx);
                    continue;
                }
                last_time = Some(t);
                last_value = Some(e.value);
                let depth = chain.len() as u32;
                let (prev, jump) = match chain.last() {
                    None => (NONE, resident_last),
                    Some(&pi) => {
                        let (pd, pj) = info[&(pi + 1)];
                        let j = (pd > 0).then(|| (pj, info[&pj].0));
                        let jj = j.and_then(|(ji, jd)| (jd > 0).then(|| info[&ji].1)).map(|jj| (jj, info[&jj].0));
                        (pi + 1, if pd == 0 { pi + 1 } else { skew_jump(pi + 1, pd, j, jj) })
                    }
                };
                e.flags = (e.flags & !(flag::FIRST | flag::PEND | flag::LAZY)) | if depth == 0 { flag::FIRST } else { 0 };
                e.depth = depth;
                e.prev = prev;
                e.jump = jump;
                self.write(idx, &e);
                last_entry = Some(e);
                chain.push(idx);
                info.insert(idx + 1, (depth, jump));
                rewritten += 1;
            }
            if let Some(&li) = chain.last() {
                resident_last = li + 1;
            }
        }
        if resident_last != NONE || self.head(slot) != NONE {
            self.set_head(slot, resident_last, last_entry.as_ref().filter(|_| resident_last != NONE))?;
        }
        Ok(rewritten)
    }

    /// 場所 `slot` の鎖を、 鎖でたどれる entry と `also` (鎖から届かない、 今足した entry) から繋ぎ直す (時刻の戻る entry を足した
    /// 時、 行の lock の下)。 その間の読み手は途中の糸を見うる (開く道の再生・突き合わせだけが通る。 試作の限り)。
    fn relink_slot(&self, slot: u32, also: u32) -> io::Result<()> {
        let mut seen = std::collections::HashSet::new();
        let mut items: Vec<(u32, u64, u32)> = Vec::new();
        fn take(seen: &mut std::collections::HashSet<u32>, idx: u32, e: &Entry, items: &mut Vec<(u32, u64, u32)>) {
            if seen.insert(idx) && e.flags & flag::VOID == 0 {
                items.push((e.eid, e.time, idx));
            }
        }
        if let Some(e) = self.valid(also) {
            take(&mut seen, also, &e, &mut items);
        }
        let mut res = self.at(self.head(slot));
        while let Some((idx, e)) = res {
            // この住人の entry を prev でたどる (最初の entry の飛び糸 = 前の住人)
            let (mut ci, mut ce) = (idx, e);
            loop {
                take(&mut seen, ci, &ce, &mut items);
                match self.at(ce.prev) {
                    Some((pi, pe)) if !seen.contains(&pi) => {
                        ci = pi;
                        ce = pe;
                    }
                    _ => break,
                }
            }
            res = self.at(ce.jump).filter(|(ji, _)| !seen.contains(ji));
        }
        // 足した entry から届く前の entry も (前の住人へ足した entry の prev)
        if let Some(mut e) = self.valid(also) {
            while let Some((pi, pe)) = self.at(e.prev) {
                if !seen.insert(pi) {
                    break;
                }
                if pe.flags & flag::VOID == 0 {
                    items.push((pe.eid, pe.time, pi));
                }
                e = pe;
            }
        }
        self.link_slot(slot, &mut items)?;
        self.forget_decks();
        Ok(())
    }

    /// 全部の valid な entry の数 (VOID を除く) と、 時刻の分からない entry の数 (試験・測り用)。
    pub fn counts(&self) -> (u64, u64) {
        self.drain();
        let (mut n, mut obs) = (0, 0);
        for idx in 0..self.len() {
            if let Some(e) = self.valid(idx)
                && e.flags & flag::VOID == 0
            {
                n += 1;
                if e.flags & flag::OBSERVED != 0 {
                    obs += 1;
                }
            }
        }
        (n, obs)
    }
}

fn change(e: &Entry) -> Change {
    Change {
        time: e.time,
        value: (e.value != ABSENT).then_some(e.value),
        seq: e.depth,
        observed: e.flags & flag::OBSERVED != 0,
        deleted: e.flags & flag::DELETE != 0,
        base: e.flags & flag::BASE != 0,
    }
}

/// 差分の並び `d` から時点 `t` の束: 時刻 ≤ t の差分のうち、 eid ごとに最後の差分が 「入った」 の eid。 後ろから読み、 初めて見た eid
/// だけを決める (開番地の小さな表。 並べ替えない)。
fn members(d: &[Diff], t: u64) -> Vec<u32> {
    let k = d.partition_point(|x| x.time <= t);
    let cap = (k * 2).next_power_of_two().max(16);
    let mask = cap - 1;
    let mut seen = vec![0u32; cap]; // eid + 1 (0 = 空き)
    let mut out = Vec::new();
    for x in d[..k].iter().rev() {
        let key = x.eid.wrapping_add(1);
        let mut i = (u64::from(x.eid).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 32) as usize & mask;
        loop {
            match seen[i] {
                0 => {
                    seen[i] = key;
                    if x.idx_in & 1 == 1 {
                        out.push(x.eid);
                    }
                    break;
                }
                k2 if k2 == key => break,
                _ => i = (i + 1) & mask,
            }
        }
    }
    out
}

/// engine の持つ履歴の列の集まり。 宣言の無い engine は `any` の読み 1 回で抜ける (表も作らない)。
pub struct Histories {
    any: AtomicBool,
    /// 紐の番号 → 列 (宣言の無い engine では作らない)
    table: OnceLock<Box<[AtomicPtr<HistCol>]>>,
    /// 持ち主 (engine が閉じるまで落とさない: 書き手が掴んだ参照を生かす)
    owned: Mutex<Vec<Arc<HistCol>>>,
    /// きれいに閉じなかった後に開いた: WAL の再生の後に cell と突き合わせる (`Engine::settle_history_at_open`)
    pub repair: AtomicBool,
}

impl Default for Histories {
    fn default() -> Self {
        Histories { any: AtomicBool::new(false), table: OnceLock::new(), owned: Mutex::new(Vec::new()), repair: AtomicBool::new(false) }
    }
}

impl Histories {
    #[inline(always)]
    pub fn get(&self, hid: usize) -> Option<&HistCol> {
        if !self.any.load(Ordering::Acquire) {
            return None;
        }
        self.get_slow(hid)
    }

    #[cold]
    fn get_slow(&self, hid: usize) -> Option<&HistCol> {
        let p = self.table.get()?.get(hid)?.load(Ordering::Acquire);
        // SAFETY: `owned` が engine の寿命の間 Arc を持つ
        (!p.is_null()).then(|| unsafe { &*p })
    }

    pub fn any(&self) -> bool {
        self.any.load(Ordering::Acquire)
    }

    pub fn insert(&self, col: HistCol) -> &HistCol {
        let hid = col.hid as usize;
        let a = Arc::new(col);
        let p = Arc::as_ptr(&a) as *mut HistCol;
        self.owned.lock().unwrap_or_else(|p| p.into_inner()).push(a);
        let t = self.table.get_or_init(|| (0..=u16::MAX as usize).map(|_| AtomicPtr::new(std::ptr::null_mut())).collect());
        t[hid].store(p, Ordering::Release);
        self.any.store(true, Ordering::Release);
        unsafe { &*p }
    }

    pub fn remove(&self, hid: usize) -> bool {
        let Some(t) = self.table.get() else { return false };
        let was = t[hid].swap(std::ptr::null_mut(), Ordering::AcqRel);
        if !was.is_null() && self.declared().is_empty() {
            self.any.store(false, Ordering::Release);
        }
        !was.is_null()
    }

    /// 宣言した紐の番号 (番号の順)。
    pub fn declared(&self) -> Vec<u16> {
        let Some(t) = self.table.get() else { return Vec::new() };
        self.owned
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|c| c.hid)
            .filter(|&h| !t[h as usize].load(Ordering::Acquire).is_null())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// 宣言した列を全部。
    pub fn cols(&self) -> Vec<&HistCol> {
        self.declared().into_iter().filter_map(|h| self.get(h as usize)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(tag: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("enchu_hist_unit_{}_{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    /// 案 A (溜める) / 案 B (区間ごとの控え) の列が、 今の形の列と同じ答えを返す。 同じ書き込みを 15 万回 (案 B の大きい区間 2 つ +
    /// 小さい区間 + 末尾、 案 A の溜めの上限を何度も越える)、 場所の使い回しを混ぜる。
    #[test]
    fn variants_answer_like_the_base() {
        for (batch, blocks, lazy) in [(true, false, false), (false, true, false), (true, true, false), (false, false, true)] {
            let d = tmp(&format!("var_{batch}_{blocks}_{lazy}"));
            let base = HistCol::create(&d.join("a"), 1, 10, 4096).unwrap();
            let var = HistCol::create(&d.join("b"), 1, 10, 4096).unwrap();
            base.set_modes(false, false);
            var.set_modes(batch, blocks);
            var.set_lazy(lazy);
            let mut cell: Vec<Option<u64>> = vec![None; 2000];
            let mut gens = vec![0u32; 2000];
            let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ u64::from(batch) ^ (u64::from(blocks) << 1);
            let mut t = 100u64;
            let mut times = Vec::new();
            let step = |x: &mut u64| {
                *x ^= *x << 13;
                *x ^= *x >> 7;
                *x ^= *x << 17;
                *x
            };
            for i in 0..150_000u32 {
                let slot = (step(&mut x) % 2000) as u32;
                let r = step(&mut x) % 20;
                t += 1;
                let eid = (gens[slot as usize] << 20) | slot;
                let new = if r == 0 { None } else { Some(step(&mut x) % 8) };
                for c in [&base, &var] {
                    if c.lazy_unlinked() {
                        c.note_lazy(slot, eid, new, t, 0).unwrap();
                    } else if c.batched() {
                        c.note_batched(slot, eid, new, t, 0, cell[slot as usize]).unwrap();
                    } else {
                        c.note(slot, eid, new, t, 0).unwrap();
                    }
                }
                cell[slot as usize] = new;
                if r == 0 && step(&mut x) % 2 == 0 {
                    gens[slot as usize] += 1; // 消えて使い回す
                }
                if i % 4999 == 0 {
                    times.push(t);
                }
            }
            // 案 C: 最初の読みの前に全体を繋ぐ (engine は列の書き手を止めて呼ぶ)
            if var.unlinked() {
                let (n, _) = var.link_all(|e| e & 0xFFFFF).unwrap();
                assert!(n > 0);
            }
            for &tt in &times {
                for v in 0..8u64 {
                    let mut a = base.pull_at(v, tt).unwrap();
                    // 案 B は今の円柱が要る: 今の値 v の identity
                    let now: Vec<u32> =
                        (0..2000u32).filter(|&s| cell[s as usize] == Some(v)).map(|s| (gens[s as usize] << 20) | s).collect();
                    let mut b = if var.blocks_mode() { var.pull_at_blocks(v, tt, &now).unwrap() } else { var.pull_at(v, tt).unwrap() };
                    a.sort_unstable();
                    b.sort_unstable();
                    assert_eq!(a, b, "batch={batch} blocks={blocks} lazy={lazy} t={tt} v={v}");
                }
                for s in (0..2000u32).step_by(37) {
                    for g in 0..=gens[s as usize] {
                        let e = (g << 20) | s;
                        assert_eq!(base.value_at(s, e, tt).unwrap(), var.value_at(s, e, tt).unwrap(), "value_at s={s} g={g} t={tt}");
                    }
                }
            }
            assert_eq!(base.counts(), var.counts());
            let _ = std::fs::remove_dir_all(&d);
        }
    }

    /// レビュー 1 回目の 3: 時刻を詰める所で logical が切れて同じ時刻になった、 値の違う書き込みは両方残り、 後の方が勝つ。
    #[test]
    fn same_time_different_values() {
        let d = tmp("sametime");
        let c = HistCol::create(&d, 2, 10, 64).unwrap();
        let t = pack(enchudb_oplog::Hlc { wall: 5, logical: u32::MAX, peer: 0 });
        assert_eq!(t, pack(enchudb_oplog::Hlc { wall: 5, logical: (1 << 20) + 7, peer: 0 }), "前提: 切れて同じ時刻");
        assert!(c.note(3, 3, Some(1), t, 0).unwrap());
        assert!(c.note(3, 3, Some(2), t, 0).unwrap(), "同じ時刻で値の違う書き込みを重複として捨てた");
        assert!(!c.note(3, 3, Some(2), t, 0).unwrap(), "同じ時刻・同じ値は重複");
        assert_eq!(c.value_at(3, 3, t).unwrap().value, Some(2));
        assert_eq!(c.history(3, 3).iter().map(|x| x.value).collect::<Vec<_>>(), vec![Some(1), Some(2)]);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn chain_floor_and_jumps() {
        let d = tmp("chain");
        let c = HistCol::create(&d, 3, 10, 1024).unwrap();
        // 行 5 に 200 回書く (時刻 100, 110, ...)
        for i in 0..200u64 {
            assert!(c.note(5, 5, Some(i), 100 + i * 10, 0).unwrap());
        }
        assert!(!c.note(5, 5, Some(77), 100 + 77 * 10, 0).unwrap(), "同じ時刻は足さない");
        for t in [100u64, 105, 109, 110, 1000, 1005, 2090, 99_999] {
            let want = if t < 100 { None } else { Some(((t - 100) / 10).min(199)) };
            assert_eq!(c.value_at(5, 5, t).unwrap().value, want, "t={t}");
        }
        assert_eq!(c.value_at(5, 5, 50).unwrap().value, None);
        assert!(c.value_at(5, 5, 5).is_err());
        assert_eq!(c.history(5, 5).len(), 200);
        for n in [0u32, 1, 2, 63, 64, 100, 199] {
            assert_eq!(c.at_seq(5, 5, n).unwrap().value, Some(n as u64));
        }
        // 場所 5 を世代 1 (eid = 1 << 20 | 5) が使い回す: 前の住人は古い eid で読める
        let y = (1 << 20) | 5;
        assert!(c.note(5, y, Some(1000), 5000, 0).unwrap());
        assert_eq!(c.value_at(5, 5, 4000).unwrap().value, Some(199));
        assert_eq!(c.value_at(5, y, 4000).unwrap().value, None);
        assert_eq!(c.value_at(5, y, 5000).unwrap().value, Some(1000));
        // デッキ
        assert_eq!(c.pull_at(199, 3000).unwrap(), vec![5]);
        assert!(c.note(5, y, Some(199), 6000, 0).unwrap());
        let mut deck = c.pull_at(199, 6000).unwrap();
        deck.sort_unstable();
        assert_eq!(deck, vec![5, y]);
        // 繋ぎ直しても同じ
        c.relink(|e| e & 0xFFFFF).unwrap();
        assert_eq!(c.value_at(5, 5, 1005).unwrap().value, Some(90));
        assert_eq!(c.value_at(5, y, 5500).unwrap().value, Some(1000));
        let _ = std::fs::remove_dir_all(&d);
    }
}
