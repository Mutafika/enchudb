//! 球の土台 (試作、 branch `proto/ball-history`): 球にした紐ごとに、 changefeed の記録 (`changefeed.seg`) の始まりの時点の
//! 列を disk に置く。 開き直した後の球は、 土台 (時点 = 地平) に記録の残りを前へ当てて組み直す ([`crate::engine::Engine::declare_ball`])。
//!
//! # なぜ要るか
//!
//! 記録の record は新しい値しか持たない。 記録に残っている範囲で entity が最初に書き換わる前の値は、 捨てた record の側にある。
//! 土台は、 記録が捨てる record を捨てる前に当てた列 = 「記録の始まりの時点の列」。
//!
//! # 形
//!
//! ```text
//! ball/NNNN.seg (NNNN = 紐の番号): [header 4096 B: magic "BAL1" | 紐の番号 u16 | done u64 | end u64 | 地平 (HLC) | crc]  [cell: 値 + 1 (u64、 0 = 値なし) × local eid]
//! ```
//!
//! - `done`: cell が当て終えている record の番号の終わり (この番号より前は全部当たっている)
//! - `end`: cell を当てている番号の終わり (`done < end` は当てている途中、 またはその後に記録が捨てる前に落ちた)
//! - 地平: 番号 `end − 1` の record の HLC (作った時は作った時刻)。 cell は地平の時点の列
//!
//! # 捨てる時 ([`OnDiscard::discard`]、 記録の捨てる header の前)
//!
//! header に `end` = 捨てる終わり を書いて書き出す → `done` 以上の番号の record を cell に当てて書き出す → 返る (記録が捨てる
//! header を書く)。 header の `done` は次に捨てる時に書く (メモリの `done` は cell を書き出した後に進める)。
//!
//! # 落ちた時 ([`BallBases::attach`]、 開く時)
//!
//! `done < end` なら、 記録に残っている `[done, end)` を当て直す (番号の順に上書きするので、 途中まで当たっていても同じ列になる)。
//! 記録が `done` より先を捨てていれば、 捨てる header より先に cell を書き出したので、 その分は当たっている。 記録の始まりが `end`
//! より先 (当てずに捨てた) / `end` が記録の終わりより先 (記録を作り直した) / header の crc が合わない土台は使えない (消す)。
//!
//! # 試作の範囲
//!
//! - 自分の書き込みだけ (他の peer の record は番号の写像が要る。 sync の DB の球は土台を持たない)
//! - local-only の表 (`_` で始まる表) の書き込みは記録に載らない
//! - 置けない大きさで中身を残さなかった record (`hole`) は球の紐の書き込みでない (Tie は小さい) と見なす

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use enchudb_oplog::Hlc;
use enchudb_oplog::oplog::DecodedOp;

use crate::change_log::{ChangeLog, Entry, OnDiscard};
use crate::region::Region;
use crate::segment_map::SegmentMap;

/// DB の directory の中の置き場。
pub const DIR: &str = "ball";
const MAGIC: [u8; 4] = *b"BAL1";
const HEADER: usize = 4096;
const HEADER_CRC: usize = 40;
/// cell の予約 (local eid は u32、 予約は仮想だけ)。
const RESERVE: usize = HEADER + 8 * (1usize << 32);
/// 記録から当て直す時に 1 回に読む量。
const READ_CHUNK: usize = 4 << 20;

fn fnv1a(bytes: &[u8]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &b in bytes {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

/// 紐 `hid` の土台の file。
pub fn path_of(dir: &Path, hid: u16) -> PathBuf {
    dir.join(DIR).join(format!("{hid:04}.seg"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Header {
    hid: u16,
    done: u64,
    end: u64,
    horizon: Hlc,
}

/// 記録の record 1 件を、 土台に当てる形にしたもの。
#[derive(Clone, Debug)]
pub(crate) enum BaseOp {
    /// (紐, local eid, 値 (None = 外す))。
    Set(u16, u32, Option<u64>),
    /// 名前で紐を指す書き込み (`TieNamed`)。
    SetNamed(String, u32, u64),
    /// entity を消す (全部の紐を外す)。
    Delete(u32),
}

/// 記録の entry を読む: 球の土台に関わる書き込みなら Some。 他の peer の record は扱わない (試作の範囲)。
pub(crate) fn decode(e: &Entry, self_peer: u32) -> Option<BaseOp> {
    let (r, _) = crate::transport::WireRecord::decode(&e.bytes).ok()?;
    if self_peer != 0 && r.author_peer != 0 && r.author_peer != self_peer {
        return None;
    }
    let local = enchudb_oplog::eid_local;
    match r.op {
        DecodedOp::Tie { eid, himo_id, value } => Some(BaseOp::Set(himo_id, local(eid), Some(value))),
        DecodedOp::Untie { eid, himo_id } => Some(BaseOp::Set(himo_id, local(eid), None)),
        DecodedOp::Delete { eid } => Some(BaseOp::Delete(local(eid))),
        DecodedOp::TieNamed { eid, himo_name, value, .. } => Some(BaseOp::SetNamed(himo_name, local(eid), value)),
        _ => None,
    }
}

/// 土台 1 本 (紐 1 本)。
pub(crate) struct Base {
    hid: u16,
    name: String,
    map: Arc<SegmentMap>,
    region: Region,
    /// cell が当て終えている番号の終わり (メモリ。 disk の header は次に捨てる時に追いつく)。
    done: u64,
    /// cell の時点 (番号 `done − 1` の record の HLC、 作った時は作った時刻)。
    horizon: Hlc,
}

impl Base {
    /// 今の列 `cells` で作る (`done = end = end`、 地平 `horizon`)。 在れば置き換える。 #441 と同じく別の名前で作って書き出してから rename。
    pub(crate) fn create(dir: &Path, hid: u16, name: &str, cells: &[(u32, u64)], end: u64, horizon: Hlc) -> io::Result<Self> {
        let path = path_of(dir, hid);
        let sub = dir.join(DIR);
        std::fs::create_dir_all(&sub)?;
        let tmp = sub.join(format!("{hid:04}.seg.tmp"));
        if tmp.exists() {
            std::fs::remove_file(&tmp)?;
        }
        {
            let map = Arc::new(SegmentMap::create(&tmp, RESERVE, HEADER)?);
            // SAFETY: 上で RESERVE を予約した
            let region = unsafe { Region::from_segment(map.clone(), RESERVE) };
            let b = Self { hid, name: name.to_string(), map, region, done: end, horizon };
            for &(e, v) in cells {
                b.put(e, Some(v))?;
            }
            b.map.flush_dirty()?;
            b.write_header(&Header { hid, done: end, end, horizon })?;
        }
        std::fs::rename(&tmp, &path)?;
        if let Ok(d) = std::fs::File::open(&sub) {
            let _ = d.sync_all();
        }
        Self::open(dir, hid, name).map(|(b, _)| b)
    }

    /// 開く。 返り値の 2 つ目は disk の header (`done` / `end` の照合は呼び手)。
    fn open(dir: &Path, hid: u16, name: &str) -> io::Result<(Self, Header)> {
        let path = path_of(dir, hid);
        let map = Arc::new(SegmentMap::open(&path, RESERVE, false)?);
        // SAFETY: SegmentMap::open は RESERVE を予約した
        let region = unsafe { Region::from_segment(map.clone(), RESERVE) };
        if region.committed_len() < HEADER {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "ball base: too short"));
        }
        let s = &region.slice()[..HEADER];
        if s[..4] != MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "ball base: bad magic"));
        }
        let u64_at = |a: usize| u64::from_le_bytes(s[a..a + 8].try_into().unwrap());
        let u32_at = |a: usize| u32::from_le_bytes(s[a..a + 4].try_into().unwrap());
        if u32_at(HEADER_CRC) != fnv1a(&s[..HEADER_CRC]) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "ball base: header crc"));
        }
        let h = Header {
            hid: u16::from_le_bytes(s[4..6].try_into().unwrap()),
            done: u64_at(8),
            end: u64_at(16),
            horizon: Hlc { wall: u64_at(24), logical: u32_at(32), peer: u32_at(36) },
        };
        if h.hid != hid || h.done > h.end {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "ball base: header fields"));
        }
        Ok((Self { hid, name: name.to_string(), map, region, done: h.done, horizon: h.horizon }, h))
    }

    fn write_header(&self, h: &Header) -> io::Result<()> {
        let mut b = [0u8; HEADER_CRC + 4];
        b[..4].copy_from_slice(&MAGIC);
        b[4..6].copy_from_slice(&h.hid.to_le_bytes());
        b[8..16].copy_from_slice(&h.done.to_le_bytes());
        b[16..24].copy_from_slice(&h.end.to_le_bytes());
        b[24..32].copy_from_slice(&h.horizon.wall.to_le_bytes());
        b[32..36].copy_from_slice(&h.horizon.logical.to_le_bytes());
        b[36..40].copy_from_slice(&h.horizon.peer.to_le_bytes());
        let crc = fnv1a(&b[..HEADER_CRC]);
        b[HEADER_CRC..].copy_from_slice(&crc.to_le_bytes());
        self.region.write_at(0, &b);
        self.region.mark_dirty(0, b.len());
        self.map.flush_dirty()
    }

    /// cell `e` に値を置く (書き出しは呼び手)。
    fn put(&self, e: u32, v: Option<u64>) -> io::Result<()> {
        let at = HEADER + 8 * e as usize;
        let stored = v.map_or(0, |v| v + 1);
        if !self.region.is_committed(at + 8) {
            if stored == 0 {
                return Ok(()); // 伸ばしていない所は値なし
            }
            self.region.ensure_committed(at + 8)?;
        }
        self.region.write_at(at, &stored.to_le_bytes());
        self.region.mark_dirty(at, 8);
        Ok(())
    }

    fn apply(&self, op: &BaseOp) -> io::Result<()> {
        match op {
            BaseOp::Set(h, e, v) if *h == self.hid => self.put(*e, *v),
            BaseOp::SetNamed(n, e, v) if *n == self.name => self.put(*e, Some(*v)),
            BaseOp::Delete(e) => self.put(*e, None),
            _ => Ok(()),
        }
    }

    /// cell の時点 (地平)。
    pub(crate) fn horizon(&self) -> Hlc {
        self.horizon
    }

    /// cell が当て終えている番号の終わり (この番号から記録を前へ当てる)。
    pub(crate) fn done(&self) -> u64 {
        self.done
    }

    /// 値のある cell を (local eid, 値) で、 eid の順に。
    pub(crate) fn for_each(&self, mut f: impl FnMut(u32, u64)) {
        let s = &self.region.slice()[..self.region.committed_len()];
        let n = (s.len().saturating_sub(HEADER)) / 8;
        for e in 0..n {
            let at = HEADER + 8 * e;
            let v = u64::from_le_bytes(s[at..at + 8].try_into().unwrap());
            if v != 0 {
                f(e as u32, v - 1);
            }
        }
    }
}

/// engine 1 つの土台の全部。 記録の捨てる前の口 ([`OnDiscard`]) として記録に付ける。
pub(crate) struct BallBases {
    self_peer: std::sync::atomic::AtomicU32,
    bases: Mutex<Vec<Base>>,
    /// 試験: 捨てる時に止める所 (1 = header を書き出した後・cell の前、 2 = cell を半分当てて書き出した後)。
    #[cfg(test)]
    pub(crate) stop: std::sync::atomic::AtomicU8,
}

impl BallBases {
    pub(crate) fn new() -> Self {
        Self {
            self_peer: std::sync::atomic::AtomicU32::new(0),
            bases: Mutex::new(Vec::new()),
            #[cfg(test)]
            stop: std::sync::atomic::AtomicU8::new(0),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Base>> {
        self.bases.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub(crate) fn set_self_peer(&self, p: u32) {
        self.self_peer.store(p, std::sync::atomic::Ordering::Relaxed);
    }

    /// 開く時 (記録を開いた後、 捨て始める前): `{dir}/ball/` の土台を開き、 記録と照らして使えるものを持つ。 落ちて当てている途中の
    /// 土台は記録から当て直す。 使えない土台は消す。 `name_of` は紐の番号 → 名前 (無い紐の土台も消す)。
    pub(crate) fn attach(&self, dir: &Path, log: &ChangeLog, name_of: impl Fn(u16) -> Option<String>) {
        let sub = dir.join(DIR);
        let Ok(rd) = std::fs::read_dir(&sub) else { return };
        let mut found: Vec<u16> =
            rd.flatten().filter_map(|e| e.file_name().to_str()?.strip_suffix(".seg")?.parse::<u16>().ok()).collect();
        found.sort_unstable();
        let peer = self.self_peer.load(std::sync::atomic::Ordering::Relaxed);
        for hid in found {
            let res = name_of(hid)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such himo"))
                .and_then(|name| Base::open(dir, hid, &name))
                .and_then(|(mut b, h)| Self::repair(&mut b, h, log, peer).map(|()| b));
            match res {
                Ok(b) => self.lock().push(b),
                Err(e) => {
                    eprintln!(
                        "[enchudb] warning: ball base {} cannot be used ({e}) — its history is lost; the ball starts over when declared",
                        path_of(dir, hid).display()
                    );
                    let _ = std::fs::remove_file(path_of(dir, hid));
                }
            }
        }
    }

    /// 記録 `(first, next)` と照らす。 `done < end` なら記録の `[done, end)` を当て直して header を `done = end` に。
    fn repair(b: &mut Base, h: Header, log: &ChangeLog, peer: u32) -> io::Result<()> {
        let (first, next) = log.range();
        if h.end > next {
            return Err(io::Error::new(io::ErrorKind::InvalidData, format!("base end {} is past the change log ({next})", h.end)));
        }
        if first > h.end {
            return Err(io::Error::new(io::ErrorKind::InvalidData, format!("the change log dropped {}..{first} without the base", h.end)));
        }
        if h.done < h.end {
            // [done, first) は記録が捨てた = 捨てる header より先に cell を書き出してある
            let mut pos = h.done.max(first);
            while pos < h.end {
                let es = log.read(pos, READ_CHUNK).map_err(|t| io::Error::new(io::ErrorKind::InvalidData, t.to_string()))?;
                let Some(last) = es.last() else { break };
                pos = last.no + 1;
                for e in es.iter().filter(|e| e.no < h.end) {
                    if let Some(op) = decode(e, peer) {
                        b.apply(&op)?;
                    }
                }
            }
            b.map.flush_dirty()?;
            b.write_header(&Header { done: h.end, ..h })?;
        }
        b.done = h.end;
        b.horizon = h.horizon;
        Ok(())
    }

    /// 紐 `hid` の土台を持つか。
    pub(crate) fn has(&self, hid: u16) -> bool {
        self.lock().iter().any(|b| b.hid == hid)
    }

    /// 土台を足す (同じ紐の土台は置き換える)。
    pub(crate) fn insert(&self, b: Base) {
        let mut g = self.lock();
        g.retain(|x| x.hid != b.hid);
        g.push(b);
    }

    /// 紐 `hid` の土台を読む (`f` は土台を持ったまま呼ぶ = その間は捨てない)。
    pub(crate) fn with<R>(&self, hid: u16, f: impl FnOnce(&Base) -> R) -> Option<R> {
        self.lock().iter().find(|b| b.hid == hid).map(f)
    }
}

impl OnDiscard for BallBases {
    fn wants(&self) -> bool {
        !self.lock().is_empty()
    }

    fn discard(&self, first: u64, end: u64, end_hlc: Hlc, hole: Option<u64>, entries: &[Entry]) -> io::Result<()> {
        let peer = self.self_peer.load(std::sync::atomic::Ordering::Relaxed);
        let ops: Vec<(u64, Option<BaseOp>)> = entries.iter().map(|e| (e.no, decode(e, peer))).collect();
        let mut g = self.lock();
        let mut broken = Vec::new();
        for (k, b) in g.iter_mut().enumerate() {
            if end <= b.done {
                continue; // 作った時より前の番号 (cell に入っている)
            }
            // [done, end) が全部読めたか (hole は中身の無い番号)
            let from = b.done.max(first);
            let have = |no: u64| hole == Some(no) || ops.binary_search_by_key(&no, |(n, _)| *n).is_ok();
            if b.done < first || !(from..end).all(have) || end_hlc == Hlc::ZERO {
                broken.push(k);
                continue;
            }
            b.write_header(&Header { hid: b.hid, done: b.done, end, horizon: end_hlc })?;
            #[cfg(test)]
            let stop = self.stop.load(std::sync::atomic::Ordering::Relaxed);
            #[cfg(test)]
            if stop == 1 {
                return Err(io::Error::other("test: stopped after the base header"));
            }
            let todo: Vec<&BaseOp> = ops.iter().filter(|(no, _)| *no >= b.done && *no < end).filter_map(|(_, op)| op.as_ref()).collect();
            #[cfg(test)]
            if stop == 2 {
                for op in &todo[..todo.len() / 2] {
                    b.apply(op)?;
                }
                b.map.flush_dirty()?;
                return Err(io::Error::other("test: stopped after half of the cells"));
            }
            for op in todo {
                b.apply(op)?;
            }
            b.map.flush_dirty()?;
            b.done = end;
            b.horizon = end_hlc;
        }
        for k in broken.into_iter().rev() {
            let b = g.remove(k);
            eprintln!(
                "[enchudb] warning: the change log dropped records the ball base of himo {} could not read — its history is lost",
                b.hid
            );
            let _ = std::fs::remove_file(b.map.path());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change_log::{MIN_CAP, NewEntry};
    use crate::transport::WireRecord;
    use std::collections::BTreeMap;

    const HID: u16 = 3;
    const NAME: &str = "t.x";

    fn tempdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("ballbase-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// 記録に載る書き込み 1 つ。 `Other` は別の紐、 `Named` は名前で指す同じ紐。
    #[derive(Clone, Copy, Debug)]
    enum W {
        Set(u32, u64),
        Unset(u32),
        Del(u32),
        Other(u32, u64),
        Named(u32, u64),
    }

    fn writes(n: usize, seed: u64) -> Vec<W> {
        let mut x = seed;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        (0..n)
            .map(|_| {
                let (k, e, v) = (next() % 20, (next() % 300) as u32, next() % 40);
                match k {
                    0..=11 => W::Set(e, v),
                    12..=13 => W::Unset(e),
                    14 => W::Del(e),
                    15..=17 => W::Other(e, v),
                    _ => W::Named(e, v + (1 << 33)), // 大きい値 (Tie64 の形) も
                }
            })
            .collect()
    }

    fn hlc(i: u64) -> Hlc {
        Hlc { wall: 10_000 + i, logical: 0, peer: 0 }
    }

    fn entry(i: u64, w: W) -> NewEntry {
        let op = match w {
            W::Set(e, v) => DecodedOp::Tie { eid: e as u64, himo_id: HID, value: v },
            W::Unset(e) => DecodedOp::Untie { eid: e as u64, himo_id: HID },
            W::Del(e) => DecodedOp::Delete { eid: e as u64 },
            W::Other(e, v) => DecodedOp::Tie { eid: e as u64, himo_id: HID + 1, value: v },
            W::Named(e, v) => DecodedOp::TieNamed { eid: e as u64, himo_name: NAME.into(), himo_kind: 0, value: v },
        };
        // 署名の欄を埋めた大きさ (record 1 件 ≈ 250 B、 1 MiB の記録に約 4000 件)
        let w = WireRecord { hlc: hlc(i), author_peer: 0, op, signature: [0; 64], pubkey_fp: [0; 8], signed_bytes: vec![0; 100] };
        NewEntry { hlc: w.hlc, wal_id: i, bytes: w.encode() }
    }

    /// 書き込み `[0, n)` を当てた列。
    fn state(ws: &[W], n: u64) -> BTreeMap<u32, u64> {
        let mut m = BTreeMap::new();
        for w in &ws[..n as usize] {
            match *w {
                W::Set(e, v) | W::Named(e, v) => {
                    m.insert(e, v);
                }
                W::Unset(e) | W::Del(e) => {
                    m.remove(&e);
                }
                W::Other(..) => {}
            }
        }
        m
    }

    fn cells(bases: &BallBases) -> (u64, Hlc, BTreeMap<u32, u64>) {
        bases
            .with(HID, |b| {
                let mut m = BTreeMap::new();
                b.for_each(|e, v| {
                    m.insert(e, v);
                });
                (b.done(), b.horizon(), m)
            })
            .expect("土台がある")
    }

    fn setup(dir: &Path) -> (ChangeLog, Arc<BallBases>) {
        let log = ChangeLog::open(dir, MIN_CAP, true, false).unwrap();
        let bases = Arc::new(BallBases::new());
        bases.insert(Base::create(dir, HID, NAME, &[], 0, hlc(0)).unwrap());
        log.set_on_discard(bases.clone());
        (log, bases)
    }

    fn reopen(dir: &Path) -> (ChangeLog, Arc<BallBases>) {
        let log = ChangeLog::open(dir, MIN_CAP, false, false).unwrap();
        let bases = Arc::new(BallBases::new());
        bases.attach(dir, &log, |h| (h == HID).then(|| NAME.to_string()));
        log.set_on_discard(bases.clone());
        (log, bases)
    }

    /// 足す (100 件ずつ)。 `at` から `upto` まで。
    fn add(log: &ChangeLog, ws: &[W], at: &mut u64, upto: u64) -> io::Result<()> {
        while *at < upto {
            let n = (upto - *at).min(100);
            let batch: Vec<_> = (*at..*at + n).map(|i| entry(i, ws[i as usize])).collect();
            log.append(&batch, &[], &mut Vec::new())?;
            *at += n;
        }
        Ok(())
    }

    /// 土台は記録の始まりの時点の列: done = 記録の最古の番号、 cell = その前の書き込みを全部当てた列、 地平 = 番号 done − 1 の HLC。
    fn assert_at_log_start(log: &ChangeLog, bases: &BallBases, ws: &[W], what: &str) {
        let (first, _) = log.range();
        let (done, hz, m) = cells(bases);
        assert_eq!(done, first, "{what}: 土台は記録の始まりまで");
        assert_eq!(m, state(ws, done), "{what}: 土台の列 (done = {done})");
        if done > 0 {
            assert_eq!(hz, hlc(done - 1), "{what}: 地平");
        }
    }

    #[test]
    fn discards_keep_the_base_at_the_start_of_the_log() {
        let dir = tempdir("keep");
        let ws = writes(30_000, 0x2545_F491_4F6C_DD1D);
        let (log, bases) = setup(&dir);
        let mut at = 0;
        for k in 1..=30u64 {
            add(&log, &ws, &mut at, k * 1000).unwrap();
            assert_at_log_start(&log, &bases, &ws, &format!("{k} 千件"));
        }
        assert!(log.range().0 > 20_000, "記録が何周も捨てていない — 試験の前提 ({:?})", log.range());
        // 開き直しても同じ (header の done は次に捨てる時に追いつくが、 開く時に記録から当て直す)
        drop(log);
        drop(bases);
        let (log, bases) = reopen(&dir);
        assert_at_log_start(&log, &bases, &ws, "開き直した後");
    }

    /// 捨てる途中で落ちた形 (土台の header の後 / cell を半分当てた後 / 土台を書き出して記録の header の前) → 開く時に記録から
    /// 当て直して、 当てていた終わりの時点の列になる。 続けて足すと、 また記録の始まりの時点の列。
    #[test]
    fn crash_points_recover_on_open() {
        for stop in 1..=3u8 {
            let dir = tempdir(&format!("crash{stop}"));
            let ws = writes(20_000, 0x9E37_79B9_7F4A_7C15 ^ stop as u64);
            let (log, bases) = setup(&dir);
            let mut at = 0;
            add(&log, &ws, &mut at, 8_000).unwrap();
            let (first0, _) = log.range();
            assert!(first0 > 0, "まだ捨てていない — 試験の前提");
            match stop {
                3 => log.stop_after_discard_hook.store(true, std::sync::atomic::Ordering::Relaxed),
                s => bases.stop.store(s, std::sync::atomic::Ordering::Relaxed),
            }
            // 捨てるまで足す (止まった 1 回は Err)
            let mut failed = false;
            for _ in 0..20 {
                let batch: Vec<_> = (at..at + 100).map(|i| entry(i, ws[i as usize])).collect();
                if log.append(&batch, &[], &mut Vec::new()).is_err() {
                    failed = true;
                    break;
                }
                at += 100;
            }
            assert!(failed, "stop {stop}: 止まっていない");
            let first_before = log.range().0;
            drop(log);
            drop(bases);
            let (log, bases) = reopen(&dir);
            let (first, _) = log.range();
            assert_eq!(first, first_before, "stop {stop}: 記録は捨てる前のまま");
            let (done, hz, m) = cells(&bases);
            assert!(done > first, "stop {stop}: 当てていた終わり ({done}) が記録の始まり ({first}) の先でない");
            assert_eq!(m, state(&ws, done), "stop {stop}: 当て直した列 (done = {done})");
            assert_eq!(hz, hlc(done - 1), "stop {stop}: 地平");
            // 続けて足す: 次に捨てる時に記録の始まりに追いつく
            add(&log, &ws, &mut at, 20_000).unwrap();
            assert_at_log_start(&log, &bases, &ws, &format!("stop {stop} の後"));
            drop(log);
            drop(bases);
            let (log, bases) = reopen(&dir);
            assert_at_log_start(&log, &bases, &ws, &format!("stop {stop} の後に開き直した"));
        }
    }

    /// 置けない大きさの record (中身を残さない番号) で全部捨てても、 土台は番号を数えて記録の始まりに居る。
    #[test]
    fn a_record_too_large_to_keep_is_a_hole() {
        let dir = tempdir("hole");
        let ws = writes(3_000, 77);
        let (log, bases) = setup(&dir);
        let mut at = 0;
        add(&log, &ws, &mut at, 2_000).unwrap();
        let big = NewEntry { hlc: hlc(2_000), wal_id: 2_000, bytes: vec![0; MIN_CAP / 2] };
        log.append(&[big], &[], &mut Vec::new()).unwrap();
        assert_eq!(log.range(), (2_001, 2_001));
        let (done, hz, m) = cells(&bases);
        assert_eq!((done, hz), (2_001, hlc(2_000)));
        assert_eq!(m, state(&ws, 2_000));
    }

    /// 記録が土台に当てずに捨てた (土台を知らない binary で開いた等): 開く時に土台を消す。
    #[test]
    fn a_base_behind_the_log_is_dropped() {
        let dir = tempdir("behind");
        let ws = writes(10_000, 5);
        {
            let (log, bases) = setup(&dir);
            let mut at = 0;
            add(&log, &ws, &mut at, 100).unwrap();
            drop(bases);
            drop(log);
        }
        // 口を付けずに足して捨てさせる
        let log = ChangeLog::open(&dir, MIN_CAP, false, false).unwrap();
        let mut at = 100;
        add(&log, &ws, &mut at, 10_000).unwrap();
        assert!(log.range().0 > 0);
        let bases = BallBases::new();
        bases.attach(&dir, &log, |h| (h == HID).then(|| NAME.to_string()));
        assert!(!bases.has(HID));
        assert!(!path_of(&dir, HID).exists(), "使えない土台は消す");
    }
}
