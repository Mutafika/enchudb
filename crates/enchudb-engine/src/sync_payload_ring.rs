//! `_sync_ops` の payload 専用の循環バッファ (`sync.payload.seg`)。
//!
//! # なぜ要るか
//!
//! `_sync_ops.payload` は engine 内部 table の Leaf なので LeafStore に載らず (`leaf_for`)、 辞書 (vocab) に
//! 置いていた。 辞書は値の byte を回収しない (#381 の回収も番号だけ) ので、 **bridge した record の分だけ辞書が
//! 一生ぶん伸び続けた** — 書き込みのたびに数十〜数百 byte、 ring から row を reclaim しても戻らない。 書き込みの
//! 多い DB は、 生きている行が少なくても `vocab_data_size` に着いて全 table の Tag 書き込みが止まる。
//!
//! ring の payload は「lsn 順に足して、 古い方から捨てる」 待ち行列なので、 汎用の置き場 (辞書 / LeafStore) では
//! なく FIFO の byte ring に置く: head に足し、 生きている row のうち最小 lsn の entry を tail とする。 個別の解放も
//! 空き一覧も持たない。 dead row の purge (#217) で途中に穴が空いても、 tail がそこを越えた時に使い回される。
//!
//! # 形
//!
//! ```text
//! [header 64 B: magic "SPR1" | cap u64]  [data: cap B の循環領域]
//! entry (8 B 整列): [len u32][lsn u32][bytes len B][pad]
//! ```
//!
//! row の `_sync_ops.payload_at` には `handle = entry の位置 / 8 + 1` を置く (0 は「無い」)。 読む時は entry の
//! `lsn` が row の lsn と一致することを確かめる — 一致しなければ、 その場所は後の record に使い回された後 (= その
//! row はもう reclaim / purge 済み) なので `None`。
//!
//! 書き手は bridge だけ (`transfer_lock` の中で 1 本ずつ)。 読み手は並行しうるので、 entry の `lsn` を
//! 本体より後に書き (Release)、 読み手は本体を写す前後で `lsn` を確かめる。

use std::io;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use crate::region::Region;
use crate::segment_map::SegmentMap;
use enchudb_oplog::Hlc;

/// DB の directory の中の file 名。 `*.seg` なので複製 (`db_files::is_segment_entry`) にそのまま載る。
pub const FILE: &str = "sync.payload.seg";
const MAGIC: [u8; 4] = *b"SPR1";
const HEADER: usize = 64;
const ENTRY_HEADER: usize = 8;
const ALIGN: usize = 8;
/// 1 entry がこれを越える payload は ring に置かない (呼び手は辞書に置く従来経路へ戻す)。 ring の 1/4 を
/// 1 件で占めると、 その 1 件が生きている間は他がほとんど置けないため。
const MAX_ENTRY_FRACTION: usize = 4;

/// 循環領域の大きさの下限と上限。 `_sync_ops` の行数の上限 × 1 行あたりの目安から決める ([`capacity_for`])。
const MIN_CAP: usize = 16 << 20;
const MAX_CAP: usize = 1 << 30;
/// 1 行あたりの payload の目安 (署名 72 B + header + 値)。 これより大きい record が続くと行の枠より先に ring が
/// 埋まるが、 その時は bridge が待つだけ (行の枠が埋まった時と同じ backpressure)。
const BYTES_PER_ROW: usize = 256;

/// #451 (段階 1): 配り済みの印の置き場 = header の 16..56 (`[tag "DMK1"][wall u64][logical u32][peer u32][lsn u32][wal lap u32]
/// [wal offset u64][check u32]`、 LE、 check は tag から wal offset までの FNV-1a 32)。 0.35 以前の binary は header の 12 B より後ろを読まず、 ring を作る時は 0 で
/// 作る = 印が無い (開く時は全部の照合、 安全側)。 file の version は上げない
const MARK_AT: usize = 16;
const MARK_TAG: [u8; 4] = *b"DMK1";
const MARK_LEN: usize = 40;

/// #451 (段階 1、 OPEN_TIME_PLAN の段階 1): 配り済みの印。 印が disk にある時、 **自分 (`hlc.peer`) の write で HLC ≤ `hlc` の
/// ものは全部、 record が disk にある** (oplog、 畳んだ後なら `_sync_ops` の行、 または floor)。 lsn ≤ `lsn` の `_sync_ops` の行は
/// HLC ≤ `hlc` の record から作った行。 開く時 (`Engine::relog_unlogged_writes`) は HLC > `hlc` の cell と lsn > `lsn` の行だけを
/// 見る。 置くのは consumer / `oplog_sync` が、 Commit の前に切った切れ目の候補を、 fsync と本体の書き出しが返った後に
/// (`Engine::take_durable_cut` / `publish_durable_mark`)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurableMark {
    pub hlc: Hlc,
    pub lsn: u32,
    /// oplog の周と、 切れ目の時の終わり (`OpLog::durable_cut_for`): この周でこれより前の record は印より前なので、 開く時の header の
    /// 走査はここから (周が違えば先頭から)
    pub wal_lap: u32,
    pub wal_off: u64,
}

fn fnv1a32(bytes: &[u8]) -> u32 {
    let mut h = 0x811c_9dc5u32;
    for &b in bytes {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

fn encode_mark(m: DurableMark) -> [u8; MARK_LEN] {
    let mut b = [0u8; MARK_LEN];
    b[..4].copy_from_slice(&MARK_TAG);
    b[4..12].copy_from_slice(&m.hlc.wall.to_le_bytes());
    b[12..16].copy_from_slice(&m.hlc.logical.to_le_bytes());
    b[16..20].copy_from_slice(&m.hlc.peer.to_le_bytes());
    b[20..24].copy_from_slice(&m.lsn.to_le_bytes());
    b[24..28].copy_from_slice(&m.wal_lap.to_le_bytes());
    b[28..36].copy_from_slice(&m.wal_off.to_le_bytes());
    let check = fnv1a32(&b[..36]);
    b[36..40].copy_from_slice(&check.to_le_bytes());
    b
}

fn decode_mark(b: &[u8]) -> Option<DurableMark> {
    if b.len() != MARK_LEN || b[..4] != MARK_TAG || fnv1a32(&b[..36]) != u32::from_le_bytes(b[36..40].try_into().ok()?) {
        return None;
    }
    Some(DurableMark {
        hlc: Hlc {
            wall: u64::from_le_bytes(b[4..12].try_into().ok()?),
            logical: u32::from_le_bytes(b[12..16].try_into().ok()?),
            peer: u32::from_le_bytes(b[16..20].try_into().ok()?),
        },
        lsn: u32::from_le_bytes(b[20..24].try_into().ok()?),
        wal_lap: u32::from_le_bytes(b[24..28].try_into().ok()?),
        wal_off: u64::from_le_bytes(b[28..36].try_into().ok()?),
    })
}

/// `_sync_ops` の行数の上限から ring の大きさを決める。
pub fn capacity_for(rows: u32) -> usize {
    (rows as usize).saturating_mul(BYTES_PER_ROW).clamp(MIN_CAP, MAX_CAP)
}

fn entry_size(len: usize) -> usize {
    (ENTRY_HEADER + len).next_multiple_of(ALIGN)
}

/// handle (cell の値) と data 内の位置の相互変換。 0 は「無い」。
pub fn handle_of(off: usize) -> u32 {
    (off / ALIGN) as u32 + 1
}
fn off_of(handle: u32) -> Option<usize> {
    (handle != 0).then(|| (handle as usize - 1) * ALIGN)
}

/// `head` から `n` byte 置ける位置。 生きている entry (`tail` から `head` まで、 循環) に重なるなら `None`。
/// `tail == None` は生きている entry が無い (= どこに置いてもよい)。
fn place(head: usize, tail: Option<usize>, n: usize, cap: usize) -> Option<usize> {
    if n > cap {
        return None;
    }
    let Some(t) = tail else {
        return Some(if head + n <= cap { head } else { 0 });
    };
    if t < head {
        // 生きている範囲 = [t, head)。 末尾に入らなければ先頭に戻って t の手前まで。
        if head + n <= cap {
            Some(head)
        } else if n <= t {
            Some(0)
        } else {
            None
        }
    } else {
        // 一周している: 生きている範囲 = [t, cap) ∪ [0, head)。 t == head は満杯。
        (head + n <= t).then_some(head)
    }
}

struct Cursor {
    /// 次に置く位置 (data 内)。
    head: usize,
    /// 生きている entry の先頭 (data 内)。 古い (= 実際より手前) 値を持っていても安全側 (空きを少なく見る) なので、
    /// 置けない時だけ呼び手に数え直してもらう。
    tail: Option<usize>,
}

pub struct PayloadRing {
    _map: Arc<SegmentMap>,
    region: Region,
    cap: usize,
    cursor: Mutex<Cursor>,
}

impl PayloadRing {
    /// `{dir}/sync.payload.seg` を開く (無ければ `cap` で作る)。 既に在る file は作った時の大きさで開く。
    pub fn open_or_create(dir: &Path, cap: usize, readonly: bool) -> io::Result<Self> {
        let path = dir.join(FILE);
        if !path.exists() {
            if readonly {
                return Err(io::Error::new(io::ErrorKind::NotFound, "sync.payload.seg が無い (readonly)"));
            }
            Self::create(dir, &path, cap.next_multiple_of(ALIGN))?;
        }
        // header は普通に読む (大きさが判る前に map すると、 予約より伸びた file を `SegmentMap::open` が断る)
        let mut h = [0u8; 12];
        std::io::Read::read_exact(&mut std::fs::File::open(&path)?, &mut h)?;
        if h[..4] != MAGIC {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "sync.payload.seg: bad magic"));
        }
        let cap = u64::from_le_bytes(h[4..12].try_into().unwrap()) as usize;
        let map = Arc::new(SegmentMap::open(&path, HEADER + cap, readonly)?);
        Ok(Self::with_map(map, cap))
    }

    /// 空の ring を `path` に置く。 #441: 別の名前で作って header を書き出してから rename で置き、 directory を書き出す。
    /// 置く名前で作ると、 header が届く前に電源が落ちた時に 「header の無い file」 が残り (file を作ったことは中身より
    /// 先に durable になる)、 以後ずっと bad magic で開けず、 payload を辞書に置き続けた。
    fn create(dir: &Path, path: &Path, cap: usize) -> io::Result<()> {
        let tmp = dir.join(format!("{FILE}.tmp"));
        if tmp.exists() {
            // 前に作りかけて落ちた分
            #[cfg(all(feature = "crashsim", unix))]
            enchudb_oplog::crashsim::releasing(&tmp, || std::fs::remove_file(&tmp))?;
            #[cfg(not(all(feature = "crashsim", unix)))]
            std::fs::remove_file(&tmp)?;
        }
        let ring = Self::with_map(Arc::new(SegmentMap::create(&tmp, HEADER + cap, 4096)?), cap);
        let mut h = [0u8; 12];
        h[..4].copy_from_slice(&MAGIC);
        h[4..12].copy_from_slice(&(cap as u64).to_le_bytes());
        ring.region.write_at(0, &h);
        ring.region.mark_dirty(0, HEADER);
        ring.flush()?;
        drop(ring);
        std::fs::rename(&tmp, path)?;
        // 置いたことを書き出す (書き出す前に落ちると ring が無いことになり、 それを指す row が読めない)
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
        Ok(())
    }

    fn with_map(map: Arc<SegmentMap>, cap: usize) -> Self {
        // SAFETY: map の予約は HEADER + cap 以上 (open / create がその大きさで予約する)
        let region = unsafe { Region::from_segment(map.clone(), HEADER + cap) };
        Self { _map: map, region, cap, cursor: Mutex::new(Cursor { head: 0, tail: None }) }
    }

    /// 開いた時の位置合わせ。 `newest` = 生きている row (entry が読める row、 #442) のうち最大 lsn の handle、 `oldest` =
    /// 最小 lsn の handle。 どちらも無ければ空 (head = 0)。
    pub fn restore(&self, newest: Option<(u32, u32)>, oldest: Option<u32>) {
        let mut c = self.cursor.lock().unwrap();
        c.head = newest
            .and_then(|(h, lsn)| {
                let off = off_of(h)?;
                let len = self.entry_len_if(off, lsn)?;
                Some((off + entry_size(len)) % self.cap)
            })
            .unwrap_or(0);
        c.tail = oldest.and_then(off_of);
    }

    /// この大きさの payload を ring に置けるか (大きすぎるものは辞書の従来経路へ)。
    pub fn accepts(&self, len: usize) -> bool {
        entry_size(len) <= self.cap / MAX_ENTRY_FRACTION
    }

    /// `bytes` を置く場所を取る。 置けなければ `oldest` (生きている row のうち最小 lsn の handle を数え直す) を
    /// 1 回だけ呼んで tail を進め、 それでも置けなければ `None` (= 満杯、 bridge は待つ)。
    pub fn reserve(&self, len: usize, oldest: impl FnOnce() -> Option<u32>) -> Option<usize> {
        let n = entry_size(len);
        let mut c = self.cursor.lock().unwrap();
        if let Some(off) = place(c.head, c.tail, n, self.cap) {
            return Some(off);
        }
        c.tail = oldest().and_then(off_of);
        place(c.head, c.tail, n, self.cap)
    }

    /// `reserve` で取った `off` に entry を書いて head を進め、 cell に置く handle を返す。 `lsn` は最後に書く
    /// (読み手がそれを見て完成を判断する)。
    pub fn write(&self, off: usize, lsn: u32, bytes: &[u8]) -> io::Result<u32> {
        let n = entry_size(bytes.len());
        let at = HEADER + off;
        self.region.ensure_committed(at + n)?;
        self.lsn_at(at).store(0, Ordering::Release);
        self.region.write_at(at, &(bytes.len() as u32).to_le_bytes());
        self.region.write_at(at + ENTRY_HEADER, bytes);
        self.lsn_at(at).store(lsn, Ordering::Release);
        self.region.mark_dirty(at, n);
        let mut c = self.cursor.lock().unwrap();
        c.head = (off + n) % self.cap;
        if c.tail.is_none() {
            c.tail = Some(off);
        }
        Ok(handle_of(off))
    }

    /// handle の payload。 その場所が `lsn` の entry でなければ (使い回された / 書きかけ / 壊れている) `None`。
    pub fn read(&self, handle: u32, lsn: u32) -> Option<Vec<u8>> {
        let off = off_of(handle)?;
        let len = self.entry_len_if(off, lsn)?;
        let at = HEADER + off;
        let bytes = self.region.slice()[at + ENTRY_HEADER..at + ENTRY_HEADER + len].to_vec();
        std::sync::atomic::fence(Ordering::Acquire);
        (self.lsn_at(at).load(Ordering::Acquire) == lsn).then_some(bytes)
    }

    /// handle の場所に `lsn` の entry が在るか (読めるか)。 #442: 行は届いたのに entry が届かなかった (電源断) 行を、
    /// 開いた時の位置合わせと満杯の数え直しで生きている entry と数えないため。
    pub fn holds(&self, handle: u32, lsn: u32) -> bool {
        off_of(handle).and_then(|off| self.entry_len_if(off, lsn)).is_some()
    }

    /// `off` の entry が `lsn` のものなら、 その長さ。
    fn entry_len_if(&self, off: usize, lsn: u32) -> Option<usize> {
        let at = HEADER + off;
        if lsn == 0 || off % ALIGN != 0 || at + ENTRY_HEADER > HEADER + self.cap {
            return None;
        }
        if !self.region.is_committed(at + ENTRY_HEADER) || self.lsn_at(at).load(Ordering::Acquire) != lsn {
            return None;
        }
        let len = u32::from_le_bytes(self.region.slice()[at..at + 4].try_into().unwrap()) as usize;
        (at + ENTRY_HEADER + len <= HEADER + self.cap && self.region.is_committed(at + ENTRY_HEADER + len))
            .then_some(len)
    }

    fn lsn_at(&self, at: usize) -> &std::sync::atomic::AtomicU32 {
        self.region.as_atomic_u32(at + 4)
    }

    /// 書いた範囲を disk へ (本体の msync と同じ契機で呼ぶ)。
    pub fn flush(&self) -> io::Result<()> {
        self._map.flush_dirty()
    }

    /// 配り済みの印 (#451 段階 1)。 無い (0.35 以前の binary が作った ring / 一度も置いていない) か壊れていれば `None`。
    pub fn durable_mark(&self) -> Option<DurableMark> {
        let _c = self.cursor.lock().unwrap();
        decode_mark(&self.region.slice()[MARK_AT..MARK_AT + MARK_LEN])
    }

    /// 印を置く。 進める時だけ (今の印より前の候補は置かない = 印は一度正しければずっと正しい)。 置いたら true。 disk に届くのは
    /// 次の [`PayloadRing::flush`] (本体の書き出しと同じ契機)。
    pub fn set_durable_mark(&self, m: DurableMark) -> bool {
        let _c = self.cursor.lock().unwrap();
        if decode_mark(&self.region.slice()[MARK_AT..MARK_AT + MARK_LEN]).is_some_and(|cur| (m.hlc, m.lsn) <= (cur.hlc, cur.lsn)) {
            return false;
        }
        self.region.write_at(MARK_AT, &encode_mark(m));
        self.region.mark_dirty(MARK_AT, MARK_LEN);
        true
    }

    /// ring を開かずに印だけ読む (開く時に使う: ring を開くと位置合わせで `_sync_ops` の行を全部なめる)。 file が無い /
    /// 読めない / 印が無いなら `None`。
    pub fn read_durable_mark(dir: &Path) -> Option<DurableMark> {
        let mut h = [0u8; HEADER];
        std::io::Read::read_exact(&mut std::fs::File::open(dir.join(FILE)).ok()?, &mut h).ok()?;
        if h[..4] != MAGIC {
            return None;
        }
        decode_mark(&h[MARK_AT..MARK_AT + MARK_LEN])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tempdir(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("spr-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn place_wraps_and_respects_the_live_range() {
        let cap = 100;
        // 空: どこでもよい (末尾に入らなければ先頭へ)
        assert_eq!(place(0, None, 40, cap), Some(0));
        assert_eq!(place(80, None, 40, cap), Some(0));
        // 生きている [10, 60): 末尾 [60, 100) に 40 入る
        assert_eq!(place(60, Some(10), 40, cap), Some(60));
        // 末尾に入らない → 先頭 [0, 10) に 8 は入る、 16 は入らない
        assert_eq!(place(96, Some(10), 8, cap), Some(0));
        assert_eq!(place(96, Some(10), 16, cap), None);
        // 一周後: 生きている [70, 100) ∪ [0, 20)。 [20, 70) に 50 まで
        assert_eq!(place(20, Some(70), 50, cap), Some(20));
        assert_eq!(place(20, Some(70), 51, cap), None);
        // 満杯 (tail == head、 生きている entry あり)
        assert_eq!(place(30, Some(30), 8, cap), None);
    }

    #[test]
    fn entries_round_trip_and_reused_slots_read_as_gone() {
        let dir = tempdir("reuse");
        let ring = PayloadRing::open_or_create(&dir, 4096, false).unwrap();
        let a = ring.reserve(100, || None).unwrap();
        let ha = ring.write(a, 1, &[7u8; 100]).unwrap();
        assert_eq!(ring.read(ha, 1).unwrap(), vec![7u8; 100]);
        // lsn が違えば別の entry = 読めない
        assert!(ring.read(ha, 2).is_none());
        // 一周させて a の場所を使い回すと、 lsn 1 では読めなくなる
        let mut lsn = 2;
        loop {
            let Some(off) = ring.reserve(100, || None) else { break };
            ring.write(off, lsn, &[lsn as u8; 100]).unwrap();
            lsn += 1;
            if off == a {
                break;
            }
        }
        assert!(ring.read(ha, 1).is_none(), "使い回した場所を古い lsn で読めてしまう");
    }

    /// #441: 前に作りかけて落ちた一時 file が残っていても作れる。 置いた ring は header を持つ。
    #[test]
    fn create_replaces_a_leftover_temporary_file() {
        let dir = tempdir("leftover");
        std::fs::write(dir.join(format!("{FILE}.tmp")), b"half made").unwrap();
        let ring = PayloadRing::open_or_create(&dir, 1 << 20, false).unwrap();
        let off = ring.reserve(10, || None).unwrap();
        let h = ring.write(off, 1, b"0123456789").unwrap();
        ring.flush().unwrap();
        drop(ring);
        assert!(!dir.join(format!("{FILE}.tmp")).exists(), "一時 file が残った");
        let ring = PayloadRing::open_or_create(&dir, 1 << 20, true).unwrap();
        assert_eq!(ring.read(h, 1).unwrap(), b"0123456789");
    }

    /// #451 (段階 1): 印の往復。 作ったばかりの ring (header の残りは 0 = 0.35 以前の binary が作った形) に印は無い、 前の印は
    /// 置かない、 開き直しても残る (ring を開かずに読む道も同じ)、 check の合わない印は無いのと同じ
    #[test]
    fn durable_mark_round_trips_and_rejects_older_marks_and_garbage() {
        let d = tempdir("mark");
        let ring = PayloadRing::open_or_create(&d, 64 * 1024, false).unwrap();
        assert_eq!(ring.durable_mark(), None, "作ったばかりの ring に印は無い");
        assert_eq!(PayloadRing::read_durable_mark(&d), None);
        let m1 = DurableMark { hlc: Hlc { wall: 10, logical: 2, peer: 1 }, lsn: 5, wal_lap: 3, wal_off: 4096 };
        assert!(ring.set_durable_mark(m1));
        assert_eq!(ring.durable_mark(), Some(m1));
        let older = DurableMark { hlc: Hlc { wall: 10, logical: 1, peer: 1 }, lsn: 9, wal_lap: 3, wal_off: 8192 };
        assert!(!ring.set_durable_mark(older), "前の印は置かない");
        assert!(!ring.set_durable_mark(m1), "同じ印は置き直さない");
        assert_eq!(ring.durable_mark(), Some(m1));
        let more_rows = DurableMark { lsn: 6, wal_off: 8192, ..m1 };
        assert!(ring.set_durable_mark(more_rows), "同じ HLC でも行が進めば置く");
        ring.flush().unwrap();
        drop(ring);
        assert_eq!(PayloadRing::read_durable_mark(&d), Some(more_rows), "ring を開かずに読める");
        let ring = PayloadRing::open_or_create(&d, 64 * 1024, false).unwrap();
        assert_eq!(ring.durable_mark(), Some(more_rows), "開き直しても残る");
        ring.region.write_at(MARK_AT + 8, &[0xFF]);
        assert_eq!(ring.durable_mark(), None, "check の合わない印は無いのと同じ");
        assert!(ring.set_durable_mark(older), "壊れた印の上には置ける");
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn reopen_restores_header_and_entries() {
        let dir = tempdir("reopen");
        let h = {
            // file が 1 page / 予約の切り上げを越えて伸びた状態で開き直す (header を map で読むと断られた)
            let ring = PayloadRing::open_or_create(&dir, 1 << 20, false).unwrap();
            let mut last = None;
            for lsn in 1..=200u32 {
                let off = ring.reserve(1000, || None).unwrap();
                last = Some((ring.write(off, lsn, &[lsn as u8; 1000]).unwrap(), lsn));
            }
            ring.flush().unwrap();
            last.unwrap()
        };
        let (h, lsn) = h;
        assert!(std::fs::metadata(dir.join(FILE)).unwrap().len() > 128 * 1024, "file が伸びていない — テスト前提");
        let ring = PayloadRing::open_or_create(&dir, 4096, false).unwrap();
        assert_eq!(ring.cap, 1 << 20, "作った時の大きさで開く");
        ring.restore(Some((h, lsn)), Some(handle_of(0)));
        assert_eq!(ring.read(h, lsn).unwrap(), vec![lsn as u8; 1000]);
        // head は最後の entry の直後から
        let next = ring.reserve(5, || Some(handle_of(0))).unwrap();
        assert_eq!(next, off_of(h).unwrap() + entry_size(1000));
    }
}
