//! changefeed の記録 (`changefeed.seg`)。 listener に渡す record を番号付きで残し、 開き直した後も番号から続きを読めるようにする。
//!
//! # なぜ要るか
//!
//! listener の位置はメモリにしか無い (WAL の offset)。 開き直すと head から始まり、 畳んだ (fold) record は `audit` にも出ない
//! ので、 落ちた consumer が処理済みの次から読み直す手が無かった。 渡していない分の溜め (#560) もメモリだけ。
//!
//! # 形
//!
//! ```text
//! [header 128 B: magic "CFL1" | cap | tail | first | head | next | 最後の record の印 | crc]  [data: cap B の循環領域]
//! entry (8 B 整列): [no u64][len u32][crc u32][hlc: wall u64 | logical u32 | peer u32][印 u64][bytes len B][pad]
//! ```
//!
//! - 番号 (`no`) は 1 つずつ増える。 生きている entry は `first..next`、 data の `[tail, head)` (循環) に番号の順に並ぶ。
//! - entry は data の終わりをまたがない。 入らない時は終わりに折り返しの印 (`no == WRAP`) を置いて頭へ (印も入らない
//!   隙間は印なしで頭へ)。
//! - 満杯なら古い方から捨てる (`first` を進める)。 捨てた番号を読もうとした読み手は [`Truncated`]。
//! - 印 (`wal_id`) は呼び手 (engine) が WAL の record ごとに決める値 (signed_bytes の hash)。 開き直した時に、 最後に記録した
//!   record を WAL の中で探して続きの位置を決めるのに使う ([`ChangeLog::last_written`])。 置けない大きさで中身を残さなかった
//!   record の印も header に残る。
//!
//! # 落ちた時
//!
//! 足す 1 回 ([`ChangeLog::append`]) は、 (1) 捨てる分を header に書いて書き出す → (2) entry を書いて書き出す → (3) 新しい
//! head / next を header に書いて書き出す、 の順。 header は書き出した entry しか指さない。 (2) の途中で落ちても、 (1) で捨てた
//! 場所にしか書いていない。 開く時は header の head から、 番号が続き crc の合う entry を拾い足す ((2) の後・(3) の前に落ちた分)。
//! header の crc が合わない時は空の記録として開く (番号を 0 から振り直し、 古い番号の読み手は全員 [`Truncated`])。
//!
//! 書き手 (呼び手の pump) と読み手 (通知係) は同じ lock を取る。

use std::collections::VecDeque;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

use enchudb_oplog::Hlc;

use crate::region::Region;
use crate::segment_map::SegmentMap;

/// DB の directory の中の file 名。 `*.seg` なので複製 (`db_files::is_segment_entry`) にそのまま載る。
pub const FILE: &str = "changefeed.seg";
const MAGIC: [u8; 4] = *b"CFL1";
const HEADER: usize = 128;
/// header の中で crc が覆う範囲と crc の位置。
const HEADER_FIELDS: std::ops::Range<usize> = 16..76;
const HEADER_CRC: usize = 76;
const ENTRY_HEADER: usize = 40;
const ALIGN: usize = 8;
/// 折り返しの印の番号。
const WRAP: u64 = u64::MAX;
/// 読み手が位置を探すための目印を置く間隔 (entry の数)。
const MARK_EVERY: u64 = 64;
/// 小さすぎる記録は作らない。
pub const MIN_CAP: usize = 1 << 20;

fn entry_size(len: usize) -> usize {
    (ENTRY_HEADER + len).next_multiple_of(ALIGN)
}

fn fnv1a(parts: &[&[u8]]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for p in parts {
        for &b in *p {
            h ^= b as u32;
            h = h.wrapping_mul(0x0100_0193);
        }
    }
    h
}

/// 64 bit の FNV-1a (呼び手が record の印を作るのに使う)。
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// 読もうとした位置が記録に無い。 `from` は今残っている最初の番号 (その前は捨てた)。 位置が記録の先 (`next` より後 = 別の記録の
/// 位置) の時も、 今の `next` を `from` にして返す。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Truncated {
    pub from: u64,
}

impl std::fmt::Display for Truncated {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "position is no longer kept (oldest kept: {})", self.from)
    }
}

impl std::error::Error for Truncated {}

/// 足す 1 件。
#[derive(Debug, Clone)]
pub struct NewEntry {
    pub hlc: Hlc,
    /// 呼び手の record の印 (開き直した時に探す)。
    pub wal_id: u64,
    pub bytes: Vec<u8>,
}

/// 読んだ 1 件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub no: u64,
    pub hlc: Hlc,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Copy, Default)]
struct Cursor {
    /// 最古の entry の data 内の位置 (空なら head と同じ)。
    tail: usize,
    /// 次に置く位置。
    head: usize,
    first: u64,
    next: u64,
    /// 最後に記録した record (中身を残さなかったものも) の (HLC, 印)。
    last: Option<(Hlc, u64)>,
}

struct State {
    c: Cursor,
    /// (番号, 位置) の目印。 `MARK_EVERY` ごと。 開いた時は空で、 最初に探す時に tail から作る (`marked`)。
    marks: VecDeque<(u64, usize)>,
    marked: bool,
}

pub struct ChangeLog {
    map: Arc<SegmentMap>,
    region: Region,
    cap: usize,
    st: Mutex<State>,
    /// 試験: entry を書き出した後・新しい header の前で止める (落ちた形)。
    #[cfg(test)]
    stop_before_header: std::sync::atomic::AtomicBool,
}

impl ChangeLog {
    /// `{dir}/changefeed.seg` を開く。 無ければ `cap` で作る (`create` が false なら `NotFound`)。 既に在る file は作った時の大きさで開く。
    pub fn open(dir: &Path, cap: usize, create: bool, readonly: bool) -> io::Result<Self> {
        let path = dir.join(FILE);
        if !path.exists() {
            if !create || readonly {
                return Err(io::Error::new(io::ErrorKind::NotFound, "changefeed.seg が無い"));
            }
            Self::create(dir, &path, cap.max(MIN_CAP).next_multiple_of(ALIGN))?;
        }
        let mut h = [0u8; HEADER];
        std::io::Read::read_exact(&mut std::fs::File::open(&path)?, &mut h)?;
        if h[..4] != MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "changefeed.seg: bad magic"));
        }
        let cap = u64::from_le_bytes(h[8..16].try_into().unwrap()) as usize;
        let map = Arc::new(SegmentMap::open(&path, HEADER + cap, readonly)?);
        // SAFETY: map の予約は HEADER + cap 以上 (open / create がその大きさで予約する)
        let region = unsafe { Region::from_segment(map.clone(), HEADER + cap) };
        let log = Self {
            map,
            region,
            cap,
            st: Mutex::new(State { c: Cursor::default(), marks: VecDeque::new(), marked: true }),
            #[cfg(test)]
            stop_before_header: std::sync::atomic::AtomicBool::new(false),
        };
        log.restore(&h);
        Ok(log)
    }

    /// 空の記録を `path` に置く。 #441 と同じく、 別の名前で作って header を書き出してから rename で置く。
    fn create(dir: &Path, path: &Path, cap: usize) -> io::Result<()> {
        let tmp = dir.join(format!("{FILE}.tmp"));
        if tmp.exists() {
            #[cfg(all(feature = "crashsim", unix))]
            enchudb_oplog::crashsim::releasing(&tmp, || std::fs::remove_file(&tmp))?;
            #[cfg(not(all(feature = "crashsim", unix)))]
            std::fs::remove_file(&tmp)?;
        }
        let map = Arc::new(SegmentMap::create(&tmp, HEADER + cap, 4096)?);
        // SAFETY: 上で HEADER + cap を予約した
        let region = unsafe { Region::from_segment(map.clone(), HEADER + cap) };
        region.write_at(0, &MAGIC);
        region.write_at(8, &(cap as u64).to_le_bytes());
        write_header(&region, &Cursor::default());
        region.mark_dirty(0, HEADER);
        map.flush_dirty()?;
        drop(region);
        drop(map);
        std::fs::rename(&tmp, path)?;
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
        Ok(())
    }

    fn restore(&self, h: &[u8; HEADER]) {
        let mut st = self.st.lock().unwrap();
        let u64_at = |a: usize| u64::from_le_bytes(h[a..a + 8].try_into().unwrap());
        let u32_at = |a: usize| u32::from_le_bytes(h[a..a + 4].try_into().unwrap());
        let crc_ok = u32_at(HEADER_CRC) == fnv1a(&[&h[HEADER_FIELDS]]);
        let mut c = Cursor {
            tail: u64_at(16) as usize,
            first: u64_at(24),
            head: u64_at(32) as usize,
            next: u64_at(40),
            last: (u32_at(72) != 0)
                .then(|| (Hlc { wall: u64_at(56), logical: u32_at(64), peer: u32_at(68) }, u64_at(48))),
        };
        if !crc_ok || c.tail >= self.cap || c.head >= self.cap || c.first > c.next {
            eprintln!(
                "[enchudb] warning: changefeed.seg header is damaged — starting an empty change log (readers resume as truncated)"
            );
            *st = State { c: Cursor::default(), marks: VecDeque::new(), marked: true };
            return;
        }
        // header を書いた後に書き出した entry (足す途中で落ちた分) を拾う
        loop {
            let at = self.wrap_at(c.head);
            match self.entry_at(at) {
                Some(e) if e.no == c.next && (c.first == c.next || self.free_at(&c, at, entry_size(e.len))) => {
                    c.head = (at + entry_size(e.len)) % self.cap;
                    c.next += 1;
                    c.last = Some((e.hlc, e.wal_id));
                }
                _ => break,
            }
        }
        if c.first == c.next {
            c.tail = c.head;
        }
        *st = State { c, marks: VecDeque::new(), marked: c.first == c.next };
    }

    /// 生きている番号の範囲 `(first, next)`。
    pub fn range(&self) -> (u64, u64) {
        let st = self.st.lock().unwrap();
        (st.c.first, st.c.next)
    }

    /// 最後に記録した record の (HLC, 印)。 開き直した時に WAL の中で続きの位置を探す手がかり。
    pub fn last_written(&self) -> Option<(Hlc, u64)> {
        self.st.lock().unwrap().c.last
    }

    /// 1 件として置ける大きさの上限 (record の bytes)。 越える record は番号だけ使い、 中身は残さない ([`Self::append`])。
    pub fn max_entry(&self) -> usize {
        self.cap / 4 - ENTRY_HEADER
    }

    /// record を番号の順に足す。 置く場所が無ければ古い entry から捨てる。 `watch` の番号のうち捨てたものは、 その entry の HLC を
    /// `lost` に積む (呼び手は、 その位置で読んでいた読み手を外して `lost_from` に渡す)。 [`Self::max_entry`] を越える record は
    /// 番号を使うだけで残さず、 その前の entry も全部捨てる (読み手はその番号で [`Truncated`] になる)。 途中で書き出しに失敗したら
    /// Err (それまでに足した分は残る、 `range` の `next` で数が判る)。
    pub fn append(&self, recs: &[NewEntry], watch: &[u64], lost: &mut Vec<(u64, Hlc)>) -> io::Result<()> {
        let mut st = self.st.lock().unwrap();
        let mut i = 0;
        while i < recs.len() {
            if recs[i].bytes.len() > self.max_entry() {
                self.skip_one(&mut st, &recs[i], watch, lost)?;
                i += 1;
                continue;
            }
            // 1 回の書き出しで置く分 (data の 1/4 まで)。 折り返しの隙間を足しても、 同じ回の entry が互いを捨てない大きさ
            let mut chunk = 0usize;
            let mut j = i;
            while j < recs.len() && recs[j].bytes.len() <= self.max_entry() {
                let n = entry_size(recs[j].bytes.len());
                if j > i && chunk + n > self.cap / 4 {
                    break;
                }
                chunk += n;
                j += 1;
            }
            self.append_chunk(&mut st, &recs[i..j], watch, lost)?;
            i = j;
        }
        Ok(())
    }

    /// 置けない大きさの record: 生きている entry を全部捨て、 番号を 1 つ使う。
    fn skip_one(&self, st: &mut State, rec: &NewEntry, watch: &[u64], lost: &mut Vec<(u64, Hlc)>) -> io::Result<()> {
        let mut sim = st.c;
        let limit = sim.next;
        while sim.first < limit {
            self.drop_oldest(&mut sim, limit, watch, lost);
        }
        st.c = Cursor { tail: st.c.head, head: st.c.head, first: limit + 1, next: limit + 1, last: Some((rec.hlc, rec.wal_id)) };
        st.marks.clear();
        st.marked = true;
        write_header(&self.region, &st.c);
        self.region.mark_dirty(0, HEADER);
        self.map.flush_dirty()
    }

    fn append_chunk(&self, st: &mut State, recs: &[NewEntry], watch: &[u64], lost: &mut Vec<(u64, Hlc)>) -> io::Result<()> {
        // (1) 置き場所を決め、 捨てる分を数える (まだ data は触らない)
        let old_next = st.c.next;
        let mut sim = st.c;
        let mut plan: Vec<(usize, Option<usize>)> = Vec::with_capacity(recs.len()); // (位置, 折り返しの印の位置)
        for r in recs {
            let n = entry_size(r.bytes.len());
            let at = loop {
                let at = if self.cap - sim.head >= n { sim.head } else { 0 };
                if sim.first == sim.next {
                    sim.tail = at;
                    break at;
                }
                if self.free_at(&sim, at, n) {
                    break at;
                }
                if sim.first >= old_next {
                    // 古い entry を全部捨てても入らない = 1 回分が大きすぎる (append の切り方の誤り)
                    return Err(io::Error::other("changefeed.seg: chunk larger than the log"));
                }
                self.drop_oldest(&mut sim, old_next, watch, lost);
            };
            let wrap = (at != sim.head && self.cap - sim.head >= ENTRY_HEADER).then_some(sim.head);
            plan.push((at, wrap));
            sim.head = (at + n) % self.cap;
            sim.next += 1;
        }
        if sim.first != st.c.first {
            // header が捨てた場所を指さないようにしてから上書きする。 古い entry が全部消えたら空の記録
            let mut mid = st.c;
            if sim.first >= old_next {
                mid.first = old_next;
                mid.tail = mid.head;
            } else {
                mid.first = sim.first;
                mid.tail = sim.tail;
            }
            write_header(&self.region, &mid);
            self.region.mark_dirty(0, HEADER);
            self.map.flush_dirty()?;
        }
        // (2) entry を書いて書き出す
        let crcs = entry_crcs(old_next, recs);
        for (k, (r, (at, wrap))) in recs.iter().zip(plan.iter()).enumerate() {
            let no = old_next + k as u64;
            if let Some(w) = *wrap {
                self.write_entry_header(HEADER + w, WRAP, 0, 0, Hlc::ZERO, 0)?;
            }
            let base = HEADER + at;
            let n = entry_size(r.bytes.len());
            self.region.ensure_committed(base + n)?;
            self.region.write_at(base + ENTRY_HEADER, &r.bytes);
            let crc = crcs[k];
            self.write_entry_header(base, no, r.bytes.len() as u32, crc, r.hlc, r.wal_id)?;
            self.region.mark_dirty(base, n);
        }
        self.map.flush_dirty()?;
        #[cfg(test)]
        if self.stop_before_header.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(io::Error::other("test: stopped before the header"));
        }
        // (3) 新しい head / next
        sim.last = recs.last().map(|r| (r.hlc, r.wal_id)).or(sim.last);
        st.c = sim;
        write_header(&self.region, &st.c);
        self.region.mark_dirty(0, HEADER);
        self.map.flush_dirty()?;
        // 目印: 捨てた分を外し、 足した分を置く
        while st.marks.front().is_some_and(|&(m, _)| m < st.c.first) {
            st.marks.pop_front();
        }
        if st.marked {
            for (k, (at, _)) in plan.iter().enumerate() {
                let no = old_next + k as u64;
                if no.is_multiple_of(MARK_EVERY) {
                    st.marks.push_back((no, *at));
                }
            }
        }
        Ok(())
    }

    /// `pos` から最大 `max_bytes` (少なくとも 1 件) 読む。 `pos == next` は空。 `pos` が記録に無ければ [`Truncated`]。
    pub fn read(&self, pos: u64, max_bytes: usize) -> Result<Vec<Entry>, Truncated> {
        let mut st = self.st.lock().unwrap();
        let (first, next) = (st.c.first, st.c.next);
        if pos < first || pos > next {
            return Err(Truncated { from: if pos < first { first } else { next } });
        }
        let mut out = Vec::new();
        if pos == next {
            return Ok(out);
        }
        let Some(mut at) = self.locate(&mut st, pos) else {
            return Err(Truncated { from: first });
        };
        let mut bytes = 0;
        let mut no = pos;
        while no < next {
            at = self.wrap_at(at);
            match self.entry_at(at) {
                Some(e) if e.no == no => {
                    let base = HEADER + at + ENTRY_HEADER;
                    let b = self.region.slice()[base..base + e.len].to_vec();
                    bytes += b.len();
                    out.push(Entry { no, hlc: e.hlc, bytes: b });
                    at = (at + entry_size(e.len)) % self.cap;
                }
                _ => return Err(Truncated { from: first }),
            }
            no += 1;
            if bytes >= max_bytes {
                break;
            }
        }
        Ok(out)
    }

    /// 書いた範囲を disk へ (足すたびに書き出しているので、 普段は何もしない)。
    pub fn flush(&self) -> io::Result<()> {
        self.map.flush_dirty()
    }

    /// `no` の entry の位置。 目印から番号の順にたどる。 目印がまだ無ければ tail から作る。
    fn locate(&self, st: &mut State, no: u64) -> Option<usize> {
        let c = st.c;
        if no < c.first || no >= c.next {
            return None;
        }
        if !st.marked {
            let (mut at, mut k) = (c.tail, c.first);
            st.marks.clear();
            while k < c.next {
                at = self.wrap_at(at);
                let e = self.entry_at(at).filter(|e| e.no == k)?;
                if k.is_multiple_of(MARK_EVERY) {
                    st.marks.push_back((k, at));
                }
                at = (at + entry_size(e.len)) % self.cap;
                k += 1;
            }
            st.marked = true;
        }
        let i = st.marks.partition_point(|&(m, _)| m <= no);
        let (mut k, mut at) = if i == 0 { (c.first, c.tail) } else { st.marks[i - 1] };
        while k < no {
            at = self.wrap_at(at);
            let e = self.entry_at(at).filter(|e| e.no == k)?;
            at = (at + entry_size(e.len)) % self.cap;
            k += 1;
        }
        Some(self.wrap_at(at))
    }

    /// 折り返しの印 / 印も入らない隙間なら頭 (0) を返す。
    fn wrap_at(&self, at: usize) -> usize {
        if self.cap - at < ENTRY_HEADER {
            return 0;
        }
        if self.region.is_committed(HEADER + at + ENTRY_HEADER) && self.u64_at(HEADER + at) == WRAP {
            return 0;
        }
        at
    }

    /// `at` の entry の header。 crc が合わない / 範囲外 / 書かれていなければ None。
    fn entry_at(&self, at: usize) -> Option<EntryHeader> {
        let e = self.header_at(at)?;
        let base = HEADER + at;
        let s = self.region.slice();
        let crc = u32::from_le_bytes(s[base + 12..base + 16].try_into().unwrap());
        let bytes = &s[base + ENTRY_HEADER..base + ENTRY_HEADER + e.len];
        (entry_crc(e.no, e.len as u32, &e.hlc, e.wal_id, bytes) == crc).then_some(e)
    }

    /// `at` の entry の header (crc は見ない)。 範囲外 / 書かれていなければ None。
    fn header_at(&self, at: usize) -> Option<EntryHeader> {
        let base = HEADER + at;
        if self.cap - at < ENTRY_HEADER || !self.region.is_committed(base + ENTRY_HEADER) {
            return None;
        }
        let s = self.region.slice();
        let u32_at = |a: usize| u32::from_le_bytes(s[a..a + 4].try_into().unwrap());
        let no = self.u64_at(base);
        let len = u32_at(base + 8) as usize;
        if no == WRAP || at + entry_size(len) > self.cap || !self.region.is_committed(base + ENTRY_HEADER + len) {
            return None;
        }
        let hlc = Hlc { wall: self.u64_at(base + 16), logical: u32_at(base + 24), peer: u32_at(base + 28) };
        Some(EntryHeader { no, len, hlc, wal_id: self.u64_at(base + 32) })
    }

    /// 捨てる entry の header。 中身は読まない (#667: 捨てる 1 件ごとに中身の crc を取ると、 記録に写す時間の 3 割ほどだった)。
    /// 長さが壊れていないかは、 次の entry の番号が 1 つ先かで見る。 次が今回置く entry (`limit` の番号、 まだ書いていない) の
    /// 最後の 1 件だけは crc で見る。
    fn dropped_at(&self, at: usize, first: u64, limit: u64) -> Option<EntryHeader> {
        let e = self.header_at(at).filter(|e| e.no == first)?;
        if e.no + 1 >= limit {
            return self.entry_at(at);
        }
        let next = self.wrap_at((at + entry_size(e.len)) % self.cap);
        self.header_at(next).filter(|n| n.no == e.no + 1).map(|_| e)
    }

    /// 最古の entry を捨てる (`sim` の上で)。 `watch` に入っている番号なら HLC を `lost` に積む。 `limit` 以上の番号 (今回置く
    /// entry、 まだ書いていない) は捨てない。
    fn drop_oldest(&self, sim: &mut Cursor, limit: u64, watch: &[u64], lost: &mut Vec<(u64, Hlc)>) {
        if sim.first >= limit {
            return;
        }
        let at = self.wrap_at(sim.tail);
        match self.dropped_at(at, sim.first, limit) {
            Some(e) => {
                if watch.contains(&e.no) {
                    lost.push((e.no, e.hlc));
                }
                sim.tail = (at + entry_size(e.len)) % self.cap;
                sim.first += 1;
            }
            _ => {
                // 読めない (壊れた) entry から先の古い entry は捨てる
                for no in sim.first..limit {
                    if watch.contains(&no) {
                        lost.push((no, Hlc::ZERO));
                    }
                }
                sim.first = limit;
                sim.tail = sim.head;
            }
        }
        if sim.first == sim.next {
            sim.tail = sim.head;
        }
    }

    /// `at` から `n` byte が生きている entry (`[tail, head)`、 循環) に重ならないか。 `at` は head か 0 (折り返し)。
    fn free_at(&self, c: &Cursor, at: usize, n: usize) -> bool {
        let (t, h) = (c.tail, c.head);
        if at + n > self.cap {
            return false;
        }
        if t < h {
            // 生きている = [t, h)
            at == h || at + n <= t
        } else if t > h {
            // 生きている = [t, cap) ∪ [0, h)
            at == h && h + n <= t
        } else {
            false // 満杯
        }
    }

    fn write_entry_header(&self, base: usize, no: u64, len: u32, crc: u32, hlc: Hlc, wal_id: u64) -> io::Result<()> {
        self.region.ensure_committed(base + ENTRY_HEADER)?;
        let mut h = [0u8; ENTRY_HEADER];
        h[0..8].copy_from_slice(&no.to_le_bytes());
        h[8..12].copy_from_slice(&len.to_le_bytes());
        h[12..16].copy_from_slice(&crc.to_le_bytes());
        h[16..24].copy_from_slice(&hlc.wall.to_le_bytes());
        h[24..28].copy_from_slice(&hlc.logical.to_le_bytes());
        h[28..32].copy_from_slice(&hlc.peer.to_le_bytes());
        h[32..40].copy_from_slice(&wal_id.to_le_bytes());
        self.region.write_at(base, &h);
        self.region.mark_dirty(base, ENTRY_HEADER);
        Ok(())
    }

    fn u64_at(&self, off: usize) -> u64 {
        u64::from_le_bytes(self.region.slice()[off..off + 8].try_into().unwrap())
    }
}

struct EntryHeader {
    no: u64,
    len: usize,
    hlc: Hlc,
    wal_id: u64,
}

/// 並べて crc を取る回の大きさの下限 (entry の数)。
const PAR_CRC_MIN: usize = 4096;

/// `first` 番から並ぶ entry の crc。 大きな回は rayon で並べる (#667: FNV は 1 byte ずつの掛け算の連鎖で、 1 件 (約 200 B) に約
/// 0.25 µs かかり、 記録に写す時間の 4 割ほどだった。 1 件ごとに独立)。
fn entry_crcs(first: u64, recs: &[NewEntry]) -> Vec<u32> {
    let one = |(k, r): (usize, &NewEntry)| entry_crc(first + k as u64, r.bytes.len() as u32, &r.hlc, r.wal_id, &r.bytes);
    #[cfg(not(target_arch = "wasm32"))]
    if recs.len() >= PAR_CRC_MIN {
        use rayon::prelude::*;
        return recs.par_iter().enumerate().map(one).collect();
    }
    recs.iter().enumerate().map(one).collect()
}

fn entry_crc(no: u64, len: u32, hlc: &Hlc, wal_id: u64, bytes: &[u8]) -> u32 {
    fnv1a(&[
        &no.to_le_bytes(),
        &len.to_le_bytes(),
        &hlc.wall.to_le_bytes(),
        &hlc.logical.to_le_bytes(),
        &hlc.peer.to_le_bytes(),
        &wal_id.to_le_bytes(),
        bytes,
    ])
}

fn write_header(region: &Region, c: &Cursor) {
    let mut h = [0u8; HEADER];
    h[16..24].copy_from_slice(&(c.tail as u64).to_le_bytes());
    h[24..32].copy_from_slice(&c.first.to_le_bytes());
    h[32..40].copy_from_slice(&(c.head as u64).to_le_bytes());
    h[40..48].copy_from_slice(&c.next.to_le_bytes());
    if let Some((hlc, id)) = c.last {
        h[48..56].copy_from_slice(&id.to_le_bytes());
        h[56..64].copy_from_slice(&hlc.wall.to_le_bytes());
        h[64..68].copy_from_slice(&hlc.logical.to_le_bytes());
        h[68..72].copy_from_slice(&hlc.peer.to_le_bytes());
        h[72..76].copy_from_slice(&1u32.to_le_bytes());
    }
    let crc = fnv1a(&[&h[HEADER_FIELDS]]);
    h[HEADER_CRC..HEADER_CRC + 4].copy_from_slice(&crc.to_le_bytes());
    region.write_at(16, &h[16..HEADER_CRC + 4]);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tempdir(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("cfl-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn rec(i: u64, len: usize) -> NewEntry {
        let mut b = vec![(i % 251) as u8; len.max(8)];
        b[..8].copy_from_slice(&i.to_le_bytes());
        NewEntry { hlc: Hlc { wall: 1000 + i, logical: 0, peer: 1 }, wal_id: 7000 + i, bytes: b }
    }

    fn add(log: &ChangeLog, recs: &[NewEntry], watch: &[u64]) -> Vec<(u64, Hlc)> {
        let mut lost = Vec::new();
        log.append(recs, watch, &mut lost).unwrap();
        lost
    }

    fn id_of(e: &Entry) -> u64 {
        u64::from_le_bytes(e.bytes[..8].try_into().unwrap())
    }

    fn read_all(log: &ChangeLog, pos: u64) -> Result<Vec<Entry>, Truncated> {
        let mut out = Vec::new();
        let mut p = pos;
        loop {
            let got = log.read(p, 64 << 10)?;
            if got.is_empty() {
                return Ok(out);
            }
            p = got.last().unwrap().no + 1;
            out.extend(got);
        }
    }

    #[test]
    fn entries_read_back_in_order_from_any_position() {
        let dir = tempdir("order");
        let log = ChangeLog::open(&dir, MIN_CAP, true, false).unwrap();
        let recs: Vec<_> = (0..500).map(|i| rec(i, 100 + (i as usize % 7) * 13)).collect();
        assert!(add(&log, &recs, &[]).is_empty());
        assert_eq!(log.range(), (0, 500));
        assert_eq!(log.last_written(), Some((recs[499].hlc, 7499)));
        for pos in [0u64, 1, 63, 64, 65, 300, 499] {
            let got = read_all(&log, pos).unwrap();
            assert_eq!(got.len() as u64, 500 - pos);
            for (k, e) in got.iter().enumerate() {
                assert_eq!(e.no, pos + k as u64);
                assert_eq!(id_of(e), e.no);
                assert_eq!(e.hlc.wall, 1000 + e.no);
                assert_eq!(e.bytes, recs[e.no as usize].bytes);
            }
        }
        assert_eq!(log.read(500, 1).unwrap(), vec![]);
        assert_eq!(log.read(501, 1), Err(Truncated { from: 500 }));
    }

    /// 一周を越えて足すと古い方から捨て、 捨てた位置の読み手は Truncated、 見張った位置は HLC が返る。
    #[test]
    fn wrapping_drops_the_oldest_and_reports_watched_positions() {
        let dir = tempdir("wrap");
        let log = ChangeLog::open(&dir, MIN_CAP, true, false).unwrap();
        let mut lost = Vec::new();
        let mut i = 0u64;
        // 1 回 20 件 × 約 10 KiB を 1 MiB の記録に 30 回 (= 約 6 MiB、 6 周)
        for _ in 0..30 {
            let batch: Vec<_> = (i..i + 20).map(|k| rec(k, 10_000 + (k as usize % 5) * 777)).collect();
            lost.extend(add(&log, &batch, &[3, 150]));
            i += 20;
        }
        let (first, next) = log.range();
        assert_eq!(next, 600);
        assert!(first > 150, "捨てていない — 試験の前提 (first = {first})");
        assert_eq!(lost.iter().map(|&(no, _)| no).collect::<Vec<_>>(), vec![3, 150]);
        assert_eq!(lost[0].1.wall, 1003);
        assert_eq!(lost[1].1.wall, 1150);
        assert_eq!(log.read(first - 1, 1), Err(Truncated { from: first }));
        let got = read_all(&log, first).unwrap();
        assert_eq!(got.len() as u64, next - first);
        for e in &got {
            assert_eq!(id_of(e), e.no);
        }
        // 残っている量は記録の大きさの 3/4 近く (捨てすぎない)
        let kept: usize = got.iter().map(|e| entry_size(e.bytes.len())).sum();
        assert!(kept >= MIN_CAP * 3 / 4 - 32 * 1024, "残りが少なすぎる: {kept}");
    }

    #[test]
    fn reopen_keeps_numbers_and_entries() {
        let dir = tempdir("reopen");
        {
            let log = ChangeLog::open(&dir, MIN_CAP, true, false).unwrap();
            for b in 0..10u64 {
                let batch: Vec<_> = (b * 50..b * 50 + 50).map(|k| rec(k, 3000)).collect();
                add(&log, &batch, &[]);
            }
        }
        // 作る時の大きさは file のものが勝つ
        let log = ChangeLog::open(&dir, 64 << 20, true, false).unwrap();
        assert_eq!(log.cap, MIN_CAP);
        let (first, next) = log.range();
        assert_eq!(next, 500);
        assert!(first > 0, "一周していない — 試験の前提");
        assert_eq!(log.last_written().unwrap().1, 7499);
        // 目印は開いた後の最初の読みで作る。 途中からでも読める
        let mid = (first + next) / 2;
        let got = read_all(&log, mid).unwrap();
        assert_eq!(got.first().unwrap().no, mid);
        assert_eq!(got.len() as u64, next - mid);
        // 続きの番号で足せる
        add(&log, &[rec(500, 3000)], &[]);
        assert_eq!(log.range().1, 501);
        assert_eq!(id_of(&log.read(500, 1).unwrap()[0]), 500);
    }

    /// entry を書き出した後・header を書く前に落ちた形: 開いた時に header の先の entry を拾い、 最後の印もそれになる。
    #[test]
    fn entries_written_after_the_last_header_are_picked_up() {
        let dir = tempdir("pickup");
        {
            let log = ChangeLog::open(&dir, MIN_CAP, true, false).unwrap();
            add(&log, &(0..40).map(|k| rec(k, 2000)).collect::<Vec<_>>(), &[]);
            let stale = log.region.slice()[16..HEADER].to_vec();
            add(&log, &(40..60).map(|k| rec(k, 2000)).collect::<Vec<_>>(), &[]);
            // header だけ 40 件の時に戻す
            log.region.write_at(16, &stale);
            log.region.mark_dirty(0, HEADER);
            log.flush().unwrap();
        }
        let log = ChangeLog::open(&dir, MIN_CAP, false, false).unwrap();
        assert_eq!(log.range(), (0, 60));
        assert_eq!(log.last_written().unwrap().1, 7059);
        assert_eq!(read_all(&log, 0).unwrap().len(), 60);
    }

    /// 一周して古い entry の場所に書いた後、 新しい header を書く前に落ちた形: 開いた時に、 残っている番号は全部読めて中身も正しい
    /// (header が上書きした場所を指したままだと、 最古の番号が別の entry になって読めない)。
    #[test]
    fn a_crash_after_overwriting_old_entries_keeps_the_log_readable() {
        let dir = tempdir("overwrite");
        let recs: Vec<_> = (0..400).map(|k| rec(k, 6000)).collect();
        {
            let log = ChangeLog::open(&dir, MIN_CAP, true, false).unwrap();
            for c in recs[..200].chunks(20) {
                add(&log, c, &[]);
            }
            assert!(log.range().0 > 0, "前提: 一周して捨てている");
            log.stop_before_header.store(true, std::sync::atomic::Ordering::Relaxed);
            let mut lost = Vec::new();
            assert!(log.append(&recs[200..240], &[], &mut lost).is_err());
        }
        let log = ChangeLog::open(&dir, MIN_CAP, false, false).unwrap();
        let (first, next) = log.range();
        let got = read_all(&log, first).unwrap_or_else(|t| panic!("最古の番号 {first} から読めない: {t:?}"));
        assert_eq!(got.len() as u64, next - first);
        for e in &got {
            assert_eq!(e.bytes, recs[e.no as usize].bytes, "{} 番の中身が違う", e.no);
        }
        assert!(next >= 200, "書き出した header の分まで残っていない (next {next})");
    }

    #[test]
    fn a_damaged_header_opens_as_an_empty_log() {
        let dir = tempdir("damaged");
        {
            let log = ChangeLog::open(&dir, MIN_CAP, true, false).unwrap();
            add(&log, &(0..10).map(|k| rec(k, 100)).collect::<Vec<_>>(), &[]);
            log.region.write_at(24, &[0xff; 4]);
            log.region.mark_dirty(0, HEADER);
            log.flush().unwrap();
        }
        let log = ChangeLog::open(&dir, MIN_CAP, false, false).unwrap();
        assert_eq!(log.range(), (0, 0));
        assert_eq!(log.last_written(), None);
        // 前の番号で読む読み手は Truncated (黙って別の entry を返さない)
        assert_eq!(log.read(5, 1), Err(Truncated { from: 0 }));
    }

    /// 置けない大きさの record は番号を使い、 その位置と前の位置の読み手は Truncated。 その後の record は読める。 印は残る。
    #[test]
    fn a_record_too_large_for_the_log_uses_its_number_and_truncates() {
        let dir = tempdir("toobig");
        let log = ChangeLog::open(&dir, MIN_CAP, true, false).unwrap();
        let big = rec(2, log.max_entry() + 1);
        let lost = add(&log, &[rec(0, 100), rec(1, 100), big.clone()], &[1]);
        assert_eq!(lost.iter().map(|&(no, _)| no).collect::<Vec<_>>(), vec![1]);
        assert_eq!(log.range(), (3, 3));
        assert_eq!(log.last_written(), Some((big.hlc, big.wal_id)));
        add(&log, &[rec(3, 100)], &[]);
        assert_eq!(log.read(2, 1), Err(Truncated { from: 3 }));
        assert_eq!(id_of(&log.read(3, 1).unwrap()[0]), 3);
        drop(log);
        let log = ChangeLog::open(&dir, MIN_CAP, false, false).unwrap();
        assert_eq!(log.range(), (3, 4));
    }

    /// 捨てる entry は中身を読まない (#667)。 中身が壊れていても、 要る分だけ捨てる。 長さが壊れていれば (次の entry の番号が
    /// 合わない) それより古い entry を全部捨て、 header が壊れた所を指さない (捨てる最後の 1 件が長さの壊れた entry の時に、
    /// 長さを確かめないと tail が中身の途中を指して読めなくなる)。
    #[test]
    fn dropping_skips_the_body_but_catches_a_damaged_length() {
        // 6000 B を 173 件で一杯の手前 (捨てない)、 次の 9000 B は頭に折り返して 0 番と 1 番を捨てる
        let fill: Vec<_> = (0..173).map(|k| rec(k, 6000)).collect();
        let big = rec(173, 9000);
        let control = {
            let log = ChangeLog::open(&tempdir("drop-control"), MIN_CAP, true, false).unwrap();
            add(&log, &fill, &[]);
            assert_eq!(log.range(), (0, 173), "試験の前提: 一杯の手前で捨てていない");
            add(&log, std::slice::from_ref(&big), &[]);
            log.range()
        };
        assert_eq!(control, (2, 174), "試験の前提: 捨てる最後の 1 件が 1 番");
        for (tag, offset, value, want) in [
            ("drop-body", ENTRY_HEADER + 100, vec![0xEE], control),        // 中身の 1 byte
            ("drop-len", 8, 3000u32.to_le_bytes().to_vec(), (173, 174)), // 長さ (範囲の中の別の値)
        ] {
            let log = ChangeLog::open(&tempdir(tag), MIN_CAP, true, false).unwrap();
            add(&log, &fill, &[]);
            let one = HEADER + entry_size(6000);
            log.region.write_at(one + offset, &value);
            add(&log, std::slice::from_ref(&big), &[]);
            assert_eq!(log.range(), want, "{tag}");
            let got = read_all(&log, want.0).unwrap_or_else(|t| panic!("{tag}: 最古の番号から読めない: {t:?}"));
            assert_eq!(got.len() as u64, want.1 - want.0, "{tag}");
            for e in &got {
                let r = if e.no == 173 { &big } else { &fill[e.no as usize] };
                assert_eq!(e.bytes, r.bytes, "{tag}: {} 番の中身が違う", e.no);
            }
        }
    }

    /// 1 回に置く entry が多い (`PAR_CRC_MIN` 件以上) と crc を並べて取る (#667)。 その entry も、 開き直した後も、 どの位置からも
    /// 読める (crc の番号がずれると読めない)。
    #[test]
    fn entries_of_a_large_append_read_back_with_parallel_crcs() {
        let dir = tempdir("parcrc");
        let recs: Vec<_> = (0..6000).map(|i| rec(i, 8)).collect();
        assert!(MIN_CAP / 4 / entry_size(8) >= PAR_CRC_MIN, "試験の前提: 1 回 (記録の 1/4) に PAR_CRC_MIN 件以上置く");
        {
            let log = ChangeLog::open(&dir, MIN_CAP, true, false).unwrap();
            add(&log, &recs, &[]);
            assert_eq!(log.range(), (0, 6000));
        }
        let log = ChangeLog::open(&dir, MIN_CAP, false, false).unwrap();
        assert_eq!(log.range(), (0, 6000));
        for pos in [0u64, 1, 4095, 4096, 5999] {
            let got = read_all(&log, pos).unwrap_or_else(|t| panic!("{pos} から読めない: {t:?}"));
            assert_eq!(got.len() as u64, 6000 - pos);
            for e in &got {
                assert_eq!(e.bytes, recs[e.no as usize].bytes);
            }
        }
    }

    /// 1 回で記録の大きさを越える量を足しても (畳む時の写し)、 最後の分が残る。
    #[test]
    fn one_append_larger_than_the_log_keeps_the_newest() {
        let dir = tempdir("huge");
        let log = ChangeLog::open(&dir, MIN_CAP, true, false).unwrap();
        let recs: Vec<_> = (0..1000).map(|k| rec(k, 5000)).collect();
        add(&log, &recs, &[]);
        let (first, next) = log.range();
        assert_eq!(next, 1000);
        let got = read_all(&log, first).unwrap();
        assert_eq!(got.len() as u64, next - first);
        assert!(got.len() > 100);
        for e in &got {
            assert_eq!(e.bytes, recs[e.no as usize].bytes);
        }
    }
}
