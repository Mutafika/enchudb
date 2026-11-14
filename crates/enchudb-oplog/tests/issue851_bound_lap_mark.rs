//! #851: oplog file v5 (周の印を record の中身に結ぶ) と、 旧い版が書いた file からの変わり目。
//!
//! `fixtures/` の file (どれも Tie は eid 1・himo 0、 値が 5 の倍数の Tie の後に Commit):
//!
//! - `oplog-v4-0.37.4.bin` — 0.37.4 (tag v0.37.4) の binary が書いた 32 KiB の WAL (2 区画、 1 区画 16 KiB)。
//!   `create(32768)` → 値 1..=20 → checkpoint を head へ → 畳む (A が周 2) → 値 100..=122 (121・122 は閉じていない末尾) →
//!   fsync。 checkpoint は頭のまま = 落ちた後の file (再生は 100..=122)。 **A が今の区画、 B は空**
//! - `oplog-v4-0.37.4-b-active.bin` — 同じ binary。 `create(32768)` → A を埋めて B へ移る (eid 9 の詰め物) → checkpoint を
//!   head へ → 畳む (A が空になる、 周 3) → B に値 100..=122 → fsync。 **B が今の区画 (周 2)、 A は空**
//! - `oplog-v3-0.36.2.bin` — 0.36.2 (tag v0.36.2 の oplog の crate) が書いた 64 KiB の WAL (1 区画の v3)。 値 1..=20 → 畳む →
//!   値 100..=356 (区画の半分を越える。 最後の 356 は閉じていない末尾) → fsync
//! - `oplog-v5.bin` — この版 (PR #860) が書いた 32 KiB の v5。 中身の手順は `oplog-v4-0.37.4.bin` と同じ。 両方の区画の header に
//!   「結んだ印だけ」 の印 (版 5)。 後の版が読める事を留める
//!
//! cargo test -p enchudb-oplog --test issue851_bound_lap_mark
//! cargo test -p enchudb-oplog --features crashsim --test issue851_bound_lap_mark -- --test-threads=1   (電源断の像も)

#![cfg(unix)]

use enchudb_oplog::oplog::{
    DecodedOp, OPLOG_FILE_VERSION, OPLOG_FILE_VERSION_V3, OPLOG_FILE_VERSION_V4, Op, OpLog, Record, wal_pos_offset,
};
use std::path::PathBuf;

const HALF: usize = 16 * 1024;
const HEADER: usize = 32;
const REC_HEADER: usize = 112;
const OFF_CRC: usize = 36;
/// `oplog.rs` の `LAP_SALT` (file の形の一部)。
const LAP_SALT: u32 = 0xC3A5_5A3C;
/// Tie (payload 16 B) の record の大きさ。
const TIE: usize = REC_HEADER + 16 + 8;
/// Commit の record の大きさ。
const COMMIT: usize = REC_HEADER + 8;

fn fixture_path(name: &str) -> String {
    format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"))
}

struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchu-issue851-oplog-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        Scratch(p)
    }

    /// fixture の写し。
    fn fixture(&self, name: &str) -> PathBuf {
        let p = self.0.join("oplog");
        std::fs::copy(fixture_path(name), &p).unwrap();
        p
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn values(recs: &[Record]) -> Vec<u64> {
    recs.iter()
        .filter_map(|r| match r.op {
            DecodedOp::Tie { eid: 1, value, .. } => Some(value),
            _ => None,
        })
        .collect()
}

fn tie(wal: &OpLog, v: u64) {
    wal.append(Op::Tie { eid: 1, himo_id: 0, value: v }).unwrap();
}

/// 値 `range` を fixture と同じ並びで書く (値が 5 の倍数の Tie の後に Commit)。
fn write_values(wal: &OpLog, range: std::ops::RangeInclusive<u64>) {
    for v in range {
        tie(wal, v);
        if v % 5 == 0 {
            wal.append(Op::Commit).unwrap();
        }
    }
}

fn r32(file: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(file[at..at + 4].try_into().unwrap())
}

fn r64(file: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(file[at..at + 8].try_into().unwrap())
}

/// 区画 A / B の header の版。
fn half_versions(file: &[u8]) -> (u32, u32) {
    (r32(file, 4), r32(file, HALF + 4))
}

/// 周の印の形。
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Mark {
    /// v3 / v4: `lap ^ LAP_SALT`
    Plain,
    /// v5: `lap ^ LAP_SALT ^ crc`
    Bound,
}

/// file の中の位置 `end` で終わる Tie (payload 16 B) の record の印の形。
fn mark_of_tie_ending_at(file: &[u8], end: usize) -> Mark {
    let at = end - TIE;
    assert_eq!(&file[at..at + 2], b"WL", "record の頭");
    let (crc, lap, check) = (r32(file, at + OFF_CRC), r32(file, end - 8), r32(file, end - 4));
    assert_ne!(crc, 0);
    if check == lap ^ LAP_SALT {
        Mark::Plain
    } else {
        assert_eq!(check, lap ^ LAP_SALT ^ crc, "印がどちらの形でもない");
        Mark::Bound
    }
}

/// Tie を 1 件積んで、 その印の形 (file から読む)。 `half` = 積んだ区画の番号。
fn mark_of_next_tie(wal: &OpLog, path: &std::path::Path, half: usize, v: u64) -> Mark {
    tie(wal, v);
    let end = half * HALF + wal_pos_offset(wal.head()) as usize;
    mark_of_tie_ending_at(&std::fs::read(path).unwrap(), end)
}

/// 0.37.x の読み (v4 の file の再生) を写した小さな読み手: 印は結ばない形 `lap ^ LAP_SALT` だけを通す。 古い区画に本体へ
/// 当てていない record があればその checkpoint から読んで今の区画を頭から、 無ければ今の区画を checkpoint から、 header の head
/// まで読む。 古い区画の途中で止まったら新しい区画は読まない。 戻り値は再生する Tie (eid 1) の値。 v4 でない file は None
/// (0.37.x は `WAL version N unsupported` で開かない)。
fn replay_as_0_37(file: &[u8]) -> Option<Vec<u64>> {
    if r32(file, 4) != 4 {
        return None;
    }
    let fnv = |h: u32, data: &[u8]| data.iter().fold(h, |h, &b| (h ^ b as u32).wrapping_mul(0x0100_0193));
    let crc_of = |header: &[u8], payload: &[u8]| {
        // record v3: payload の FNV-1a に、 header の op 〜 author (3..36) を 4 byte ずつ混ぜる
        header[3..36].chunks(4).fold(fnv(0x811c_9dc5, payload), |h, c| {
            let mut w = [0u8; 4];
            w[..c.len()].copy_from_slice(c);
            (h ^ u32::from_le_bytes(w)).wrapping_mul(0x0100_0193)
        })
    };
    // (base, head, checkpoint, 周)。 B の header が v4 として読めなければ、 0.37.x は B を空の区画として書き直す
    let half = |base: usize| (base, r64(file, base + 8) as usize, r64(file, base + 16) as usize, r32(file, base + 28));
    let a = half(0);
    let b_ok = &file[HALF..HALF + 4] == b"EWAL" && r32(file, HALF + 4) == 4 && r32(file, HALF + 24) as usize == HALF;
    let b = if b_ok { half(HALF) } else { (HALF, HEADER, HEADER, 0) };
    let newer = |x: u32, y: u32| x != 0 && (y == 0 || (x.wrapping_sub(y) as i32) > 0);
    let active = match (a.1 > HEADER, b.1 > HEADER) {
        (true, false) => 0,
        (false, true) => 1,
        _ => usize::from(newer(b.3, a.3)),
    };
    let hs = [a, b];
    let (act, old) = (hs[active], hs[active ^ 1]);
    let mut plan = Vec::new();
    if old.1 > HEADER && old.2 < old.1 {
        plan.push((old, old.2));
        plan.push((act, HEADER));
    } else {
        plan.push((act, act.2));
    }
    let mut out = Vec::new();
    for ((base, head, _, lap), start) in plan {
        let mut off = start;
        while off < head {
            let at = base + off;
            let header = &file[at..at + REC_HEADER];
            if &header[0..2] != b"WL" {
                return Some(out);
            }
            let len = r32(file, at + 4) as usize;
            let payload = &file[at + REC_HEADER..at + REC_HEADER + len];
            let t = at + REC_HEADER + len;
            if r32(file, at + OFF_CRC) != crc_of(header, payload) || r32(file, t + 4) != r32(file, t) ^ LAP_SALT || r32(file, t) != lap {
                return Some(out);
            }
            // Tie (op 0) の eid 1
            if header[3] == 0 && r64(payload, 0) == 1 {
                out.push(r32(payload, 12) as u64);
            }
            off += REC_HEADER + len + 8;
        }
    }
    Some(out)
}

/// 0.37.4 が書いた v4 の file (A が今の区画、 B は空) は読めて、 そのまま v4 の形で書き足せる (0.37.4 が読める印)。 A をその場で
/// 畳むと v5 になり、 以後の record は印を中身に結ぶ。 畳んだ A と空の B の header の版は 5 (= その周は結んだ印だけ)。
#[test]
fn a_v4_file_written_by_0_37_4_is_read_and_becomes_v5_at_the_first_fold() {
    let root = Scratch::new("upgrade");
    let p = root.fixture("oplog-v4-0.37.4.bin");
    let wal = OpLog::open(&p).unwrap();
    assert_eq!((wal.file_version(), wal.two_halves(), wal.lap()), (OPLOG_FILE_VERSION_V4, true, 2));
    assert_eq!(values(&wal.recover_with_tail()), (100..=122).collect::<Vec<_>>(), "落ちた v4 の file の末尾まで再生する");

    // 畳む前: v4 のまま、 印は結ばない形 (ここで落ちて 0.37.4 に戻しても読める)
    assert_eq!(mark_of_next_tie(&wal, &p, 0, 123), Mark::Plain);
    wal.append(Op::Commit).unwrap();
    wal.fsync().unwrap();
    let file = std::fs::read(&p).unwrap();
    assert_eq!(half_versions(&file), (4, 4));
    assert_eq!(replay_as_0_37(&file), Some((100..=123).collect()), "0.37.x の読みで全部読める");

    // 最初に畳む時に v5
    wal.advance_checkpoint(wal.head());
    assert!(wal.try_reset());
    assert_eq!(wal.file_version(), OPLOG_FILE_VERSION);
    assert_eq!(half_versions(&std::fs::read(&p).unwrap()), (5, 5), "畳んだ A と、 空の B に 「結んだ印だけ」 の印");
    assert_eq!(mark_of_next_tie(&wal, &p, 0, 200), Mark::Bound, "v5 になった後の record は印を中身に結ぶ");
    tie(&wal, 201);
    wal.append(Op::Commit).unwrap();
    assert_eq!(values(&wal.recover_with_tail()), vec![200, 201]);
    assert_eq!(replay_as_0_37(&std::fs::read(&p).unwrap()), None, "0.37.x は v5 を開かない");
    wal.fsync().unwrap();
    drop(wal);
    let wal = OpLog::open(&p).unwrap();
    assert_eq!((wal.file_version(), wal.two_halves()), (OPLOG_FILE_VERSION, true));
    assert_eq!(values(&wal.recover_with_tail()), vec![200, 201]);
    // 開き直した後も結んで書く
    assert_eq!(mark_of_next_tie(&wal, &p, 0, 202), Mark::Bound);
}

/// engine の開く道と同じ順 (再生 → `drop_records_after_a_hole` → checkpoint) で v4 の file を開いて書く。 最初の書き込みは
/// もう一方の区画 (B) へ移り (#706)、 古い区画 (A) を畳んだ所で v5 になる。 B は書き足し中 = 結ばない形の record が居るので、
/// B の header の版は 4 のまま (B の読みは両方の形を通す)。 B を畳むと 5。
#[test]
fn one_half_holds_marks_of_both_forms_across_the_change() {
    let root = Scratch::new("mixed");
    let p = root.fixture("oplog-v4-0.37.4.bin");
    let wal = OpLog::open(&p).unwrap();
    assert_eq!(values(&wal.recover_with_tail()).len(), 23);
    wal.drop_records_after_a_hole();
    let a_end = wal.head();
    wal.advance_checkpoint(a_end);
    wal.fsync().unwrap();

    for v in 200..=204 {
        tie(&wal, v);
    }
    let plain_end = HALF + wal_pos_offset(wal.head()) as usize;
    wal.append(Op::Commit).unwrap();
    assert_eq!(wal.switches(), 1, "前提: 最初の書き込みはもう一方の区画へ移る");
    assert_eq!(wal.file_version(), OPLOG_FILE_VERSION_V4);
    assert!(wal.try_reset(), "前提: 古い区画 (A) を畳める");
    assert_eq!(wal.file_version(), OPLOG_FILE_VERSION);
    for v in 205..=209 {
        tie(&wal, v);
    }
    let bound_end = HALF + wal_pos_offset(wal.head()) as usize;
    wal.append(Op::Commit).unwrap();
    wal.fsync().unwrap();

    let file = std::fs::read(&p).unwrap();
    assert_eq!(
        (mark_of_tie_ending_at(&file, plain_end), mark_of_tie_ending_at(&file, bound_end)),
        (Mark::Plain, Mark::Bound),
        "前提: 同じ区画・同じ周に 2 つの形"
    );
    assert_eq!(half_versions(&file), (5, 4), "結ばない形の record が残る B は、 畳むまで版 4");
    let want: Vec<u64> = (200..=209).collect();
    assert_eq!(values(&wal.recover_with_tail()), want);
    drop(wal);
    let wal = OpLog::open(&p).unwrap();
    assert_eq!(wal.file_version(), OPLOG_FILE_VERSION);
    assert_eq!(values(&wal.recover_with_tail()), want);
    // B を畳むと B も 「結んだ印だけ」
    wal.advance_checkpoint(wal.head());
    assert!(wal.try_reset());
    assert_eq!(half_versions(&std::fs::read(&p).unwrap()), (5, 5));
}

/// B が今の区画の v4 の file (A は空) で、 B をその場で畳む: A は空 = 旧い形の record が無いので、 ここで v5 になる
/// (A の header の版を書き出してから結び始める)。
#[test]
fn folding_b_in_place_binds_when_a_is_empty() {
    let root = Scratch::new("b-in-place");
    let p = root.fixture("oplog-v4-0.37.4-b-active.bin");
    let wal = OpLog::open(&p).unwrap();
    assert_eq!((wal.file_version(), wal.lap()), (OPLOG_FILE_VERSION_V4, 2));
    assert_eq!(values(&wal.recover_with_tail()), (100..=122).collect::<Vec<_>>());
    assert_eq!(mark_of_next_tie(&wal, &p, 1, 123), Mark::Plain, "B に書き足す (v4 のまま)");
    wal.append(Op::Commit).unwrap();
    wal.advance_checkpoint(wal.head());
    assert!(wal.try_reset(), "前提: B をその場で畳める");
    assert_eq!(wal.switches(), 0);
    assert_eq!(wal.file_version(), OPLOG_FILE_VERSION);
    assert_eq!(half_versions(&std::fs::read(&p).unwrap()), (5, 5));
    assert_eq!(mark_of_next_tie(&wal, &p, 1, 200), Mark::Bound);
    wal.fsync().unwrap();
    drop(wal);
    let wal = OpLog::open(&p).unwrap();
    assert_eq!(wal.file_version(), OPLOG_FILE_VERSION);
    assert_eq!(values(&wal.recover_with_tail()), vec![200]);
}

/// B を畳む時に A が書き足し中 (A に結ばない形の record が居る) なら、 まだ v5 にしない: A の header の版 5 は 「A の今の周は
/// 結んだ印だけ」 も言うので。 次に A を畳んだ所で v5 になる (その時書き足し中の B は版 4 のまま、 B を畳むと 5)。
#[test]
fn folding_b_while_a_is_being_written_waits_for_the_fold_of_a() {
    let root = Scratch::new("b-then-a");
    let p = root.fixture("oplog-v4-0.37.4-b-active.bin");
    let wal = OpLog::open(&p).unwrap();
    assert_eq!(values(&wal.recover_with_tail()).len(), 23);
    wal.drop_records_after_a_hole();
    wal.advance_checkpoint(wal.head());
    wal.fsync().unwrap();
    // 最初の書き込みは A へ移る (#706)
    assert_eq!(mark_of_next_tie(&wal, &p, 0, 200), Mark::Plain);
    assert_eq!(wal.switches(), 1);
    assert!(wal.try_reset(), "前提: 古い区画 (B) を畳める");
    assert_eq!(wal.file_version(), OPLOG_FILE_VERSION_V4, "A が書き足し中の間は v4 のまま");
    assert_eq!(half_versions(&std::fs::read(&p).unwrap()), (4, 4));
    assert_eq!(mark_of_next_tie(&wal, &p, 0, 201), Mark::Plain);
    // A を埋めて B へ移り、 A を畳む
    let mut v = 202;
    while wal.switches() == 1 {
        tie(&wal, v);
        v += 1;
    }
    wal.append(Op::Commit).unwrap();
    // 今の区画 (B) の head まで checkpoint を進める = 古い区画 (A) は終わりまで
    wal.advance_checkpoint(wal.head());
    assert!(wal.try_reset(), "前提: 古い区画 (A) を畳める");
    assert_eq!(wal.file_version(), OPLOG_FILE_VERSION);
    assert_eq!(half_versions(&std::fs::read(&p).unwrap()), (5, 4));
    assert_eq!(mark_of_next_tie(&wal, &p, 1, v), Mark::Bound);
    wal.append(Op::Commit).unwrap();
    wal.advance_checkpoint(wal.head());
    assert!(wal.try_reset(), "前提: B をその場で畳める");
    assert_eq!(half_versions(&std::fs::read(&p).unwrap()), (5, 5));
}

/// #851 の像、 v4 から上がった file の最初の周: 前の周の record は結ばない形 (0.37.4 が書いた)、 今の周の record は結んだ形。
/// 今の周の record の中身 (header + payload) だけ前の周の bytes に戻すと、 印は今の record の CRC に結んであるので合わない。
#[test]
fn a_v4_record_under_a_bound_mark_of_the_next_lap_stops_the_replay() {
    let root = Scratch::new("torn-v4");
    let p = root.fixture("oplog-v4-0.37.4.bin");
    let before = std::fs::read(&p).unwrap();
    // fixture の並び: Tie 100、 Commit、 Tie 101 … = 3 つ目の record が Tie 101
    let at = HEADER + TIE + COMMIT;
    assert_eq!(mark_of_tie_ending_at(&before, at + TIE), Mark::Plain, "前提: 前の周の record は結ばない形");
    let wal = OpLog::open(&p).unwrap();
    wal.advance_checkpoint(wal.head());
    assert!(wal.try_reset());
    write_values(&wal, 200..=222); // 同じ並び = 同じ位置・同じ長さ
    wal.fsync().unwrap();
    drop(wal);
    let mut image = std::fs::read(&p).unwrap();
    assert_eq!(mark_of_tie_ending_at(&image, at + TIE), Mark::Bound, "前提: 今の周の record は結んだ形");
    assert_ne!(image[at..at + REC_HEADER + 16], before[at..at + REC_HEADER + 16]);
    image[at..at + REC_HEADER + 16].copy_from_slice(&before[at..at + REC_HEADER + 16]);
    std::fs::write(&p, &image).unwrap();
    let wal = OpLog::open(&p).unwrap();
    assert_eq!(values(&wal.recover_with_tail()), vec![200], "前の周の値 101 を今の周の record として読んだ");
}

/// A の版が 4・B の版が 5 の file (規約の中では出ない。 動いている DB の file を写した像で出うる): v4 の file として開き、 B の
/// record を黙って捨てない。 B に残る結んだ形の record も読める。 書き足す record は結ばない形。
#[test]
fn a_file_whose_b_header_says_5_under_an_a_header_of_4_keeps_the_records_of_b() {
    let root = Scratch::new("a4-b5");
    let p = root.fixture("oplog-v4-0.37.4-b-active.bin");
    {
        // B をその場で畳んで v5 にし、 B に結んだ形の record を書く
        let wal = OpLog::open(&p).unwrap();
        wal.advance_checkpoint(wal.head());
        assert!(wal.try_reset());
        write_values(&wal, 200..=207);
        wal.fsync().unwrap();
    }
    let mut image = std::fs::read(&p).unwrap();
    assert_eq!(half_versions(&image), (5, 5));
    image[4..8].copy_from_slice(&4u32.to_le_bytes());
    std::fs::write(&p, &image).unwrap();
    let wal = OpLog::open(&p).unwrap();
    assert_eq!(wal.file_version(), OPLOG_FILE_VERSION_V4);
    assert_eq!(values(&wal.recover_with_tail()), (200..=207).collect::<Vec<_>>(), "B の record を捨てた");
    assert_eq!(mark_of_next_tie(&wal, &p, 1, 208), Mark::Plain, "v4 の file には結ばない形で書く");
    assert_eq!(half_versions(&std::fs::read(&p).unwrap()), (4, 4), "B の版は 4 に戻す (結ばない形の record が入る)");
    assert_eq!(values(&wal.recover_with_tail()), (200..=208).collect::<Vec<_>>());
}

/// 0.36.2 が書いた v3 の file (1 区画、 区画の半分を越えて record がある) を disk から開く。 読めて、 最初に畳む時に 2 区画の
/// v5 になる。
#[test]
fn a_v3_file_written_by_0_36_2_is_read_and_becomes_v5_with_two_halves() {
    let root = Scratch::new("v3");
    let p = root.fixture("oplog-v3-0.36.2.bin");
    let wal = OpLog::open(&p).unwrap();
    assert_eq!((wal.file_version(), wal.two_halves(), wal.capacity()), (OPLOG_FILE_VERSION_V3, false, 64 * 1024));
    assert_eq!(values(&wal.recover_with_tail()), (100..=356).collect::<Vec<_>>());
    tie(&wal, 357);
    let end = wal_pos_offset(wal.head()) as usize;
    assert_eq!(mark_of_tie_ending_at(&std::fs::read(&p).unwrap(), end), Mark::Plain, "v3 のうちは結ばない形");
    wal.append(Op::Commit).unwrap();
    wal.advance_checkpoint(wal.head());
    assert!(wal.try_reset());
    assert_eq!((wal.file_version(), wal.two_halves(), wal.half_capacity()), (OPLOG_FILE_VERSION, true, 32 * 1024));
    let file = std::fs::read(&p).unwrap();
    assert_eq!((r32(&file, 4), r32(&file, 32 * 1024 + 4)), (5, 5));
    tie(&wal, 400);
    let end = wal_pos_offset(wal.head()) as usize;
    assert_eq!(mark_of_tie_ending_at(&std::fs::read(&p).unwrap(), end), Mark::Bound);
    wal.fsync().unwrap();
    drop(wal);
    let wal = OpLog::open(&p).unwrap();
    assert_eq!((wal.file_version(), wal.two_halves()), (OPLOG_FILE_VERSION, true));
    assert_eq!(values(&wal.recover_with_tail()), vec![400]);
}

/// この版が書いた v5 の file を開いて読む (形を留める: 後の版がこの file を読めなくなったら落ちる)。 両方の区画の header の版は
/// 5 = 「結んだ印だけ」 で、 record の印は `lap ^ LAP_SALT ^ crc`。
#[test]
fn the_v5_fixture_is_read() {
    let file = std::fs::read(fixture_path("oplog-v5.bin")).unwrap();
    assert_eq!(half_versions(&file), (5, 5));
    assert_eq!(mark_of_tie_ending_at(&file, HEADER + TIE), Mark::Bound);
    assert_eq!(replay_as_0_37(&file), None);
    let root = Scratch::new("v5");
    let p = root.fixture("oplog-v5.bin");
    let wal = OpLog::open(&p).unwrap();
    assert_eq!((wal.file_version(), wal.two_halves(), wal.lap()), (OPLOG_FILE_VERSION, true, 2));
    assert_eq!(values(&wal.recover_with_tail()), (100..=122).collect::<Vec<_>>());
    // 結ばない形の印は通さない: 3 つ目の record (Tie 101) の印を旧い形に書き換えると、 そこで止まる
    drop(wal);
    let mut image = file.clone();
    let t = HEADER + TIE + COMMIT + REC_HEADER + 16;
    let lap = r32(&image, t);
    image[t + 4..t + 8].copy_from_slice(&(lap ^ LAP_SALT).to_le_bytes());
    std::fs::write(&p, &image).unwrap();
    let wal = OpLog::open(&p).unwrap();
    assert_eq!(values(&wal.recover_with_tail()), vec![100], "印のある区画が、 結ばない形の印を通した");
}

/// 開く時に古い区画の穴を見つけて新しい区画を畳む道 (#663) も畳みなので、 v5 にする: 穴のある v4 の file は、 穴の後の区画が A
/// なら開いただけで v5 になる (A を畳む = A に旧い形の record が残らない)。
#[test]
fn a_v4_file_with_a_hole_in_the_old_half_becomes_v5_when_opened() {
    let root = Scratch::new("hole");
    let p = root.fixture("oplog-v4-0.37.4-b-active.bin");
    {
        // B (古くなる区画) に record を残したまま、 A へ移って書く (v4 のまま)
        let wal = OpLog::open(&p).unwrap();
        assert_eq!(values(&wal.recover_with_tail()).len(), 23);
        wal.drop_records_after_a_hole();
        write_values(&wal, 200..=204);
        assert_eq!((wal.switches(), wal.file_version()), (1, OPLOG_FILE_VERSION_V4));
        wal.fsync().unwrap();
    }
    // B の 3 つ目の record (Tie 101) の payload を壊す = 届かなかった page
    let mut image = std::fs::read(&p).unwrap();
    assert_eq!(half_versions(&image), (4, 4));
    let b_cp = r64(&image, HALF + 16) as usize;
    let at = HALF + b_cp + TIE + COMMIT;
    assert_eq!(&image[at..at + 2], b"WL");
    image[at + REC_HEADER + 12] ^= 0xff;
    std::fs::write(&p, &image).unwrap();
    assert_eq!(replay_as_0_37(&image), Some(vec![100]), "前提: 古い区画の途中で止まる");

    let wal = OpLog::open(&p).unwrap();
    assert_eq!(values(&wal.recover_with_tail()), vec![100], "穴の前まで再生する");
    assert_eq!(wal.file_version(), OPLOG_FILE_VERSION_V4);
    assert!(wal.drop_records_after_a_hole(), "前提: 穴の後を捨てる");
    assert_eq!(wal.file_version(), OPLOG_FILE_VERSION, "穴の後の区画 (A) を畳んだ所で v5");
    assert_eq!(half_versions(&std::fs::read(&p).unwrap()), (5, 4));
}

/// v4 → v5 の変わり目の電源断の像: どの像も v4 か v5 として開けて、 書き出しの返った値まで再生する (それより古い値で終わらない・
/// 値が飛ばない)。 版が 4 の像は、 0.37.x の読み (結ばない形の印だけを通す) でも書き出しの返った値まで届く。
#[cfg(feature = "crashsim")]
mod power_loss {
    use super::*;
    use enchudb_oplog::crashsim::{self, Mode};
    use std::path::Path;

    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// 再生した値は `first` から 1 つずつ続き、 書き出しの返った `acked` まで届く (`acked` が None なら、 何も無くてもよい)。
    fn assert_replays_through(tag: &str, vals: &[u64], first: u64, acked: Option<u64>) {
        let want: Vec<u64> = (first..first + vals.len() as u64).collect();
        assert_eq!(vals, want, "{tag}: 値が続いていない");
        if let Some(acked) = acked {
            assert!(vals.last().is_some_and(|&v| v >= acked), "{tag}: 書き出しの返った {acked} まで届かない ({:?})", vals.last());
        }
    }

    /// 像を撮って開く (`Lost` と `Mixed` の seed 32 個)。 `versions` = その段でありうる版。 版が 4 の像は 0.37.x の読みでも確かめる。
    fn check_images(live: &Path, root: &Path, tag: &str, first: u64, acked: Option<u64>, versions: &[u32]) {
        let modes = std::iter::once(Mode::Lost).chain((0..32).map(|seed| Mode::Mixed { seed }));
        for (i, mode) in modes.enumerate() {
            let dir = root.join(format!("{tag}-{i}"));
            crashsim::capture(live, &dir, mode).unwrap();
            let img = dir.join("oplog");
            let tag = format!("{tag} {mode:?}");
            // 0.37.x の読み (開く前の像で)
            let image = std::fs::read(&img).unwrap();
            match replay_as_0_37(&image) {
                Some(vals) => assert_replays_through(&format!("{tag} (0.37.x の読み)"), &vals, first, acked),
                None => assert_eq!(r32(&image, 4), 5, "{tag}: 0.37.x が開かないのは v5 だけ"),
            }
            let w = OpLog::open(&img).unwrap_or_else(|e| panic!("{tag}: 像を開けない: {e}"));
            let ver = w.file_version();
            assert!(versions.contains(&ver), "{tag}: 版 {ver}");
            assert_eq!(ver, r32(&image, 4), "{tag}: 開いた版は A の header の版");
            assert_replays_through(&tag, &values(&w.recover_with_tail()), first, acked);
            std::fs::remove_dir_all(&dir).unwrap();
        }
    }

    /// fixture を live に置いて開く (0.37.4 が書いて disk にある file)。
    fn open_live(root: &Scratch, name: &str) -> (PathBuf, OpLog) {
        let live = root.0.join("live");
        std::fs::create_dir_all(&live).unwrap();
        let p = live.join("oplog");
        std::fs::copy(fixture_path(name), &p).unwrap();
        crashsim::start();
        crashsim::file_synced(&p);
        let wal = OpLog::open(&p).unwrap();
        assert_eq!(values(&wal.recover_with_tail()).len(), 23);
        (live, wal)
    }

    /// A が今の区画の file を engine の順で開く: 書き込みは B へ移り、 古い区画 A を畳んだ所で v5。
    #[test]
    fn images_around_the_fold_of_a() {
        let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let root = Scratch::new("images-a");
        let (live, wal) = open_live(&root, "oplog-v4-0.37.4.bin");
        wal.drop_records_after_a_hole();
        // 再生した分 (古い区画 A) は本体に当てて書き出した。 以後の書き込み (B) は checkpoint を進めない = 再生に残る
        wal.advance_checkpoint(wal.head());
        wal.fsync().unwrap();

        // 1) v4 のまま書いて書き出す
        write_values(&wal, 200..=210);
        wal.fsync().unwrap();
        assert_eq!(wal.file_version(), OPLOG_FILE_VERSION_V4);
        check_images(&live, &root.0, "v4", 200, Some(210), &[OPLOG_FILE_VERSION_V4]);
        let v4_image = root.0.join("v4-kept");
        crashsim::capture(&live, &v4_image, Mode::Lost).unwrap();

        // 2) 古い区画を畳む = v5 になる。 A の header は、 結び始める前に書き出してある = どの像も版 5
        assert!(wal.try_reset(), "前提: 古い区画 (A) を畳める");
        assert_eq!(wal.file_version(), OPLOG_FILE_VERSION);
        check_images(&live, &root.0, "folded", 200, Some(210), &[OPLOG_FILE_VERSION]);

        // 3) 結んだ形の record を書く (書き出さない): B の page は届いても届かなくてもよい
        write_values(&wal, 211..=220);
        check_images(&live, &root.0, "bound-unsynced", 200, Some(210), &[OPLOG_FILE_VERSION]);

        // 4) 書き出す
        wal.fsync().unwrap();
        check_images(&live, &root.0, "bound-synced", 200, Some(220), &[OPLOG_FILE_VERSION]);

        // 5) 規約の中では出ない像 (版を書き出してから結ぶので): A の header の page だけ v4 の時のまま、 B の page は今の中身。
        //    この版は v4 の file として開いて、 結ばない形と結んだ形の両方を再生する
        let img = root.0.join("stitched");
        crashsim::capture(&live, &img, Mode::Lost).unwrap();
        crashsim::stop();
        let old = std::fs::read(v4_image.join("oplog")).unwrap();
        let mut image = std::fs::read(img.join("oplog")).unwrap();
        assert_eq!((r32(&image, 4), r32(&old, 4)), (5, 4));
        image[..HALF].copy_from_slice(&old[..HALF]);
        std::fs::write(img.join("oplog"), &image).unwrap();
        let w = OpLog::open(&img.join("oplog")).unwrap();
        assert_eq!(w.file_version(), OPLOG_FILE_VERSION_V4);
        assert_replays_through("stitched", &values(&w.recover_with_tail()), 200, Some(220));
        drop(wal);
    }

    /// B が今の区画・A が空の file で、 B をその場で畳む: A の header の版 5 を書き出してから結ぶ。 結んだ record の page が
    /// 版より先に届いた像 (版 4 の file に結んだ印の record = 0.37.x が書きかけと読んで止まる) は出ない: v5 にした後の像は全部版 5。
    #[test]
    fn images_around_the_fold_of_b_in_place() {
        let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let root = Scratch::new("images-b");
        let (live, wal) = open_live(&root, "oplog-v4-0.37.4-b-active.bin");
        // 1) v4 のまま B に書き足して書き出す
        write_values(&wal, 123..=130);
        wal.fsync().unwrap();
        check_images(&live, &root.0, "v4", 100, Some(130), &[OPLOG_FILE_VERSION_V4]);
        // 2) B をその場で畳む (全部を本体に当てて書き出した後) = v5
        wal.advance_checkpoint(wal.head());
        wal.fsync().unwrap();
        assert!(wal.try_reset(), "前提: B をその場で畳める");
        assert_eq!(wal.file_version(), OPLOG_FILE_VERSION);
        check_images(&live, &root.0, "folded", 200, None, &[OPLOG_FILE_VERSION]);
        // 3) 結んだ形の record を書く (書き出さない)
        write_values(&wal, 200..=210);
        check_images(&live, &root.0, "bound-unsynced", 200, None, &[OPLOG_FILE_VERSION]);
        // 4) 書き出す
        wal.fsync().unwrap();
        check_images(&live, &root.0, "bound-synced", 200, Some(210), &[OPLOG_FILE_VERSION]);
        crashsim::stop();
        drop(wal);
    }

    /// B を畳む時に A が書き足し中なら v4 のまま (どの像も 0.37.x が読める)。 A を埋めて B へ移り、 A を畳んだ所で v5。
    #[test]
    fn images_around_the_fold_of_b_while_a_is_being_written() {
        let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let root = Scratch::new("images-ba");
        let (live, wal) = open_live(&root, "oplog-v4-0.37.4-b-active.bin");
        wal.drop_records_after_a_hole();
        wal.advance_checkpoint(wal.head());
        wal.fsync().unwrap();
        // 1) A へ移って書く (v4)
        write_values(&wal, 200..=210);
        wal.fsync().unwrap();
        assert_eq!(wal.switches(), 1);
        check_images(&live, &root.0, "v4-a", 200, Some(210), &[OPLOG_FILE_VERSION_V4]);
        // 2) 古い区画 B を畳む: A が書き足し中なので v4 のまま
        assert!(wal.try_reset(), "前提: 古い区画 (B) を畳める");
        assert_eq!(wal.file_version(), OPLOG_FILE_VERSION_V4);
        write_values(&wal, 211..=220);
        check_images(&live, &root.0, "v4-b-folded", 200, Some(210), &[OPLOG_FILE_VERSION_V4]);
        wal.fsync().unwrap();
        check_images(&live, &root.0, "v4-b-folded-synced", 200, Some(220), &[OPLOG_FILE_VERSION_V4]);
        // 3) A を埋めて B へ移る (B の record も結ばない形)。 書き出す
        let mut v = 221;
        while wal.switches() == 1 {
            write_values(&wal, v..=v);
            v += 1;
        }
        wal.fsync().unwrap();
        check_images(&live, &root.0, "v4-switched", 200, Some(v - 1), &[OPLOG_FILE_VERSION_V4]);
        // 4) 全部を本体に当てて書き出し、 A を畳む = v5。 B は書き足し中 (結ばない形の record が居る) なので版 4 のまま
        wal.append(Op::Commit).unwrap();
        wal.advance_checkpoint(wal.head());
        wal.fsync().unwrap();
        assert!(wal.try_reset(), "前提: 古い区画 (A) を畳める");
        assert_eq!(wal.file_version(), OPLOG_FILE_VERSION);
        check_images(&live, &root.0, "v5-folded", v, None, &[OPLOG_FILE_VERSION]);
        write_values(&wal, v..=v + 9);
        check_images(&live, &root.0, "v5-unsynced", v, None, &[OPLOG_FILE_VERSION]);
        wal.fsync().unwrap();
        check_images(&live, &root.0, "v5-synced", v, Some(v + 9), &[OPLOG_FILE_VERSION]);
        crashsim::stop();
        drop(wal);
    }
}
