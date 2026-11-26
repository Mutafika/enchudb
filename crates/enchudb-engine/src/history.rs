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
//! 前の区間の答えに `uncertain` を立てる。 ユーザの書き込みで時刻が無い (WAL の無い session) ものは engine が断る
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
/// 予約する仮想アドレス (entry は 2^32 件まで = 128 GiB)。
const RESERVE_ENTRIES: usize = 1 << 40;
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

/// 宣言した列 1 本の履歴。
pub struct HistCol {
    pub hid: u16,
    entries: SegmentMap,
    heads: SegmentMap,
    /// 場所を取った entry の数 (書きかけを含む)
    len: AtomicU32,
    /// 地平 (宣言の時刻)
    horizon: u64,
    /// 見た最大の時刻 (時刻の分からない entry に付ける今)
    clock: AtomicU64,
    /// 見た値の差分の並び (最初に見た時に作る)
    decks: RwLock<HashMap<u64, Vec<Diff>>>,
    observed: AtomicBool,
    /// 同じ identity の鎖に時刻の戻る entry を足した (開く時の再生・突き合わせ)。 開く道の終わりに繋ぎ直す
    disordered: AtomicBool,
    /// entry を `VOID` にした (繋ぎ直しで鎖から外す)
    voided: AtomicBool,
    readonly: bool,
}

fn entry_path(dir: &Path, hid: u16) -> PathBuf {
    dir.join("hist").join(format!("{hid:04}.seg"))
}

fn head_path(dir: &Path, hid: u16) -> PathBuf {
    dir.join("hist").join(format!("{hid:04}.head.seg"))
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
        let entries = SegmentMap::create(&ep, RESERVE_ENTRIES, HEADER + 64 * ENTRY)?;
        let heads = SegmentMap::create(&hp, (slots as usize).max(1) * 4 + HEADER, 4096)?;
        let c = HistCol::new(hid, entries, heads, horizon, false);
        c.write_header(0);
        c.entries.flush(0, HEADER)?;
        Ok(c)
    }

    /// 開く。 header が読めなければ Err (呼び手は宣言を捨てる)。
    pub fn open(dir: &Path, hid: u16, slots: u32, readonly: bool) -> io::Result<Self> {
        let entries = SegmentMap::open(&entry_path(dir, hid), RESERVE_ENTRIES, readonly)?;
        let heads = SegmentMap::open(&head_path(dir, hid), (slots as usize).max(1) * 4 + HEADER, readonly)?;
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
        let c = HistCol::new(hid, entries, heads, horizon, readonly);
        let cap = ((c.entries.committed() - HEADER) / ENTRY) as u64;
        c.len.store(len.min(cap) as u32, Ordering::Relaxed);
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

    fn new(hid: u16, entries: SegmentMap, heads: SegmentMap, horizon: u64, readonly: bool) -> Self {
        HistCol {
            hid,
            entries,
            heads,
            len: AtomicU32::new(0),
            horizon,
            clock: AtomicU64::new(horizon),
            decks: RwLock::new(HashMap::new()),
            observed: AtomicBool::new(false),
            disordered: AtomicBool::new(false),
            voided: AtomicBool::new(false),
            readonly,
        }
    }

    /// 列 `hid` の file 2 本 (宣言を外す時に engine が消す。 map は engine が閉じるまで残る)。
    pub fn paths(dir: &Path, hid: u16) -> [PathBuf; 2] {
        [entry_path(dir, hid), head_path(dir, hid)]
    }

    pub fn horizon(&self) -> u64 {
        self.horizon
    }

    /// 見た最大の時刻。
    pub fn clock(&self) -> u64 {
        self.clock.load(Ordering::Acquire)
    }

    /// 時刻の分からない entry に付ける今 (見た最大 + 1)。
    pub fn tick(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::AcqRel) + 1
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
        ((HEADER + self.len() as usize * ENTRY) as u64, self.heads.committed() as u64)
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
        let idx = self.len.fetch_add(1, Ordering::AcqRel);
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
        let off = HEADER + slot as usize * 4;
        if off + 4 > self.heads.committed() {
            return NONE;
        }
        unsafe { AtomicU32::from_ptr(self.heads.base().add(off) as *mut u32) }.load(Ordering::Acquire)
    }

    fn set_head(&self, slot: u32, link: u32) -> io::Result<()> {
        let off = HEADER + slot as usize * 4;
        if off + 4 > self.heads.committed() {
            self.heads.grow_amortized(off + 4)?;
        }
        unsafe { AtomicU32::from_ptr(self.heads.base().add(off) as *mut u32) }.store(link, Ordering::Release);
        self.heads.mark_dirty(off, 4);
        Ok(())
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
        let v = value.unwrap_or(ABSENT);
        let head = self.head(slot);
        let mut disordered = false;
        let latest = self.latest_of(slot, eid);
        let (prev, depth, jump, mut flags) = match latest {
            Some((li, le)) => {
                if extra & flag::OBSERVED == 0 && time <= le.time {
                    // 同じ時刻の entry があれば足さない (再生・当て直し)
                    // 時刻の分からない entry (`OBSERVED`) は時計から取った時刻なので、 同じ時刻でも別の書き込み
                    let (fl, _) = self.floor((li, le), time);
                    if fl.is_some_and(|(_, f)| f.time == time && f.flags & flag::OBSERVED == 0) {
                        return Ok(false);
                    }
                    // 時刻の戻る entry (再生が、 後の entry まで届いた像に前の書き込みを当て直す): その時刻の前の値と同じなら変更
                    // ではない (書いた時も足さなかった)
                    let before = fl.map_or(ABSENT, |(_, f)| f.value);
                    if before == v && extra & flag::BASE == 0 {
                        return Ok(false);
                    }
                    disordered = true;
                }
                if le.value == v && extra & flag::BASE == 0 && time >= le.time {
                    return Ok(false); // 値が変わらない (書き直し) は変更ではない
                }
                let j = self.at(le.jump).map(|(i, e)| (i + 1, e.depth));
                let jj = j.and_then(|(ji, _)| self.at(ji)).and_then(|(_, je)| self.at(je.jump)).map(|(i, e)| (i + 1, e.depth));
                let jump = if le.depth == 0 { li + 1 } else { skew_jump(li + 1, le.depth, j, jj) };
                if head != li + 1 {
                    // 前の住人の entry (今の住人の後ろ): 鎖の頭にはできない。 足した後にその場所を繋ぎ直す
                    disordered = true;
                }
                (li + 1, le.depth + 1, jump, 0)
            }
            None => {
                if value.is_none() {
                    return Ok(false); // 値の無い所を外す = 変更ではない
                }
                (NONE, 0, head, flag::FIRST)
            }
        };
        flags |= flag::VALID | extra;
        let idx = self.alloc()?;
        self.write(idx, &Entry { flags, depth, eid, prev, jump, time, value: v });
        // SeqCst の RMW: 下の 「見た」 の読みと、 差分の並びを作る側の 「見た」 を立てる → entry をなめる、 の間の約束 (x86 では
        // lock 付きの命令 1 つ。 前は SeqCst の fence を別に置いていた)
        self.clock.fetch_max(time, Ordering::SeqCst);
        if disordered {
            // 時刻の戻る entry / 前の住人の entry: 鎖を時刻の順に繋ぎ直す (差分の並びも捨てる。 次に見た時に作り直す)
            self.relink_slot(slot, idx)?;
            return Ok(true);
        }
        if latest.is_none() || latest.is_some_and(|(li, _)| head == li + 1) {
            self.set_head(slot, idx + 1)?;
        }
        // 見た値の差分の並びに足す (書いた entry が見えた後で見る。 作る側は 「見た」 を立ててから entry をなめる)
        if self.observed.load(Ordering::SeqCst) {
            let old = latest.map(|(_, e)| e.value).unwrap_or(ABSENT);
            self.push_diffs(idx, eid, time, old, v);
        }
        Ok(true)
    }

    fn push_diffs(&self, idx: u32, eid: u32, time: u64, old: u64, new: u64) {
        let mut decks = self.decks.write().unwrap_or_else(|p| p.into_inner());
        for (val, inn) in [(old, false), (new, true)] {
            if val == ABSENT || old == new {
                continue;
            }
            if let Some(d) = decks.get_mut(&val) {
                let x = Diff { time, idx_in: (idx << 1) | u32::from(inn), eid };
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
        let mut decks = self.decks.write().unwrap_or_else(|p| p.into_inner());
        if let Some(d) = decks.get(&v) {
            return d.clone();
        }
        self.observed.store(true, Ordering::SeqCst);
        std::sync::atomic::fence(Ordering::SeqCst);
        let n = self.len();
        let mut d = Vec::new();
        for idx in 0..n {
            let Some(e) = self.valid(idx) else { continue };
            if e.flags & flag::VOID != 0 {
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
        d.sort_unstable_by_key(|x| (x.time, x.idx_in >> 1));
        decks.insert(v, d.clone());
        d
    }

    /// 書き出し: header に `len` を書いて entry と heads を msync (`all` = commit 済み全域)。
    pub fn write_out(&self, all: bool) -> io::Result<()> {
        if self.readonly {
            return Ok(());
        }
        self.write_header(self.len());
        if all {
            self.entries.flush_all()?;
            self.heads.flush_all()?;
        } else {
            self.entries.flush_dirty()?;
            self.heads.flush_dirty()?;
        }
        Ok(())
    }

    pub fn has_dirty(&self) -> bool {
        self.entries.flush_pending() || self.heads.flush_pending()
    }

    /// 開く時の突き合わせが使う: 場所 `slot` の住人 (新しい順) の (eid, 最新の値, 最新の時刻, 最初の時刻)。
    pub fn residents(&self, slot: u32) -> Vec<(u32, Option<u64>, u64, u64)> {
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
        let head_slots = (self.heads.committed().saturating_sub(HEADER)) / 4;
        for s in 0..head_slots as u32 {
            if self.head(s) != NONE {
                self.set_head(s, NONE)?;
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
        self.forget_decks();
        Ok(rewritten)
    }

    fn forget_decks(&self) {
        self.decks.write().unwrap_or_else(|p| p.into_inner()).clear();
        self.observed.store(false, Ordering::SeqCst);
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
        let mut i = 0;
        while i < items.len() {
            let eid = items[i].0;
            // この identity の鎖: 深さ = 並びの位置。 糸 (位置 + 1) → (深さ, 飛び糸) を引く表
            let mut chain: Vec<u32> = Vec::new(); // 深さ → 位置
            let mut info: HashMap<u32, (u32, u32)> = HashMap::new(); // 糸 → (深さ, 飛び糸)
            let mut last_time: Option<u64> = None;
            while i < items.len() && items[i].0 == eid {
                let (_, t, idx) = items[i];
                i += 1;
                if last_time == Some(t) {
                    self.mark_void(idx); // 同じ時刻 = 同じ書き込み
                    continue;
                }
                last_time = Some(t);
                let mut e = self.valid(idx).expect("拾った entry");
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
                e.flags = (e.flags & !flag::FIRST) | if depth == 0 { flag::FIRST } else { 0 };
                e.depth = depth;
                e.prev = prev;
                e.jump = jump;
                self.write(idx, &e);
                chain.push(idx);
                info.insert(idx + 1, (depth, jump));
                rewritten += 1;
            }
            if let Some(&li) = chain.last() {
                resident_last = li + 1;
            }
        }
        if resident_last != NONE || self.head(slot) != NONE {
            self.set_head(slot, resident_last)?;
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

/// 差分の並び `d` から時点 `t` の束: 時刻 ≤ t の差分のうち、 eid ごとに最後の差分が 「入った」 の eid。
fn members(d: &[Diff], t: u64) -> Vec<u32> {
    let k = d.partition_point(|x| x.time <= t);
    let mut v: Vec<u64> = d[..k].iter().enumerate().map(|(i, x)| (u64::from(x.eid) << 32) | i as u64).collect();
    v.sort_unstable();
    let mut out = Vec::new();
    let mut i = 0;
    while i < v.len() {
        let eid = (v[i] >> 32) as u32;
        let mut j = i;
        while j + 1 < v.len() && (v[j + 1] >> 32) as u32 == eid {
            j += 1;
        }
        let last = (v[j] & 0xFFFF_FFFF) as usize;
        if d[last].idx_in & 1 == 1 {
            out.push(eid);
        }
        i = j + 1;
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
        assert_eq!(c.pull_at(199, 6000).unwrap(), vec![5, y]);
        // 繋ぎ直しても同じ
        c.relink(|e| e & 0xFFFFF).unwrap();
        assert_eq!(c.value_at(5, 5, 1005).unwrap().value, Some(90));
        assert_eq!(c.value_at(5, y, 5500).unwrap().value, Some(1000));
        let _ = std::fs::remove_dir_all(&d);
    }
}
