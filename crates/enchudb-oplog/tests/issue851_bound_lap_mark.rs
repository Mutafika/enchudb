//! #851: oplog file v5 (周の印を record の中身に結ぶ) と、 0.37.4 が書いた v4 の file からの変わり目。
//!
//! `fixtures/oplog-v4-0.37.4.bin` は 0.37.4 (tag v0.37.4) の binary が書いた 32 KiB の WAL (2 区画、 1 区画 16 KiB):
//! `OpLog::create(32768)` → Tie (eid 1) の値 1..=20 を 5 件ごとに Commit → checkpoint を head へ → 畳む (周 2) → 値 100..=122 を
//! 5 件ごとに Commit (121・122 は閉じていない末尾) → fsync。 checkpoint は頭のまま = 落ちた後の file (再生は 100..=122)。
//! 周 1 の record の残りが head の先にある。
//!
//! cargo test -p enchudb-oplog --test issue851_bound_lap_mark
//! cargo test -p enchudb-oplog --features crashsim --test issue851_bound_lap_mark -- --test-threads=1   (電源断の像も)

#![cfg(unix)]

use enchudb_oplog::oplog::{
    DecodedOp, OPLOG_FILE_VERSION, OPLOG_FILE_VERSION_V4, Op, OpLog, Record, wal_pos_offset,
};
use std::path::{Path, PathBuf};

const FIXTURE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/oplog-v4-0.37.4.bin");
const HALF: usize = 16 * 1024;
const REC_HEADER: usize = 112;
const OFF_CRC: usize = 36;
/// `oplog.rs` の `LAP_SALT` (file の形の一部)。
const LAP_SALT: u32 = 0xC3A5_5A3C;

struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchu-issue851-oplog-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        Scratch(p)
    }

    /// 0.37.4 が書いた file の写し。
    fn fixture(&self) -> PathBuf {
        let p = self.0.join("oplog");
        std::fs::copy(FIXTURE, &p).unwrap();
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
            DecodedOp::Tie { value, .. } => Some(value),
            _ => None,
        })
        .collect()
}

fn tie(wal: &OpLog, v: u64) {
    wal.append(Op::Tie { eid: 1, himo_id: 0, value: v }).unwrap();
}

fn r32(file: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(file[at..at + 4].try_into().unwrap())
}

/// 区画 (header は `base`) の header の版。
fn half_version(file: &[u8], base: usize) -> u32 {
    r32(file, base + 4)
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
    let at = end - (REC_HEADER + 16 + 8);
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

/// 0.37.4 が書いた v4 の file は読めて、 そのまま v4 の形で書き足せる (0.37.4 が読める印)。 最初に畳む時に v5 になり、 以後の
/// record は印を中身に結ぶ。 B の header の版は、 A の版が disk に届いた後の畳みで 5 になる。
#[test]
fn a_v4_file_written_by_0_37_4_is_read_and_becomes_v5_at_the_first_fold() {
    let root = Scratch::new("upgrade");
    let p = root.fixture();
    let wal = OpLog::open(&p).unwrap();
    assert_eq!((wal.file_version(), wal.two_halves(), wal.lap()), (OPLOG_FILE_VERSION_V4, true, 2));
    assert_eq!(values(&wal.recover_with_tail()), (100..=122).collect::<Vec<_>>(), "落ちた v4 の file の末尾まで再生する");

    // 畳む前: v4 のまま、 印は結ばない形 (ここで落ちて 0.37.4 に戻しても読める)
    tie(&wal, 123);
    let end = wal_pos_offset(wal.head()) as usize;
    wal.append(Op::Commit).unwrap();
    let file = std::fs::read(&p).unwrap();
    assert_eq!(mark_of_tie_ending_at(&file, end), Mark::Plain);
    assert_eq!((half_version(&file, 0), half_version(&file, HALF)), (4, 4));
    assert_eq!(values(&wal.recover_with_tail()), (100..=123).collect::<Vec<_>>());
    wal.fsync().unwrap(); // v4 のうちの書き出しは 「v5 の版が届いた」 に数えない

    // 最初に畳む時に v5
    wal.advance_checkpoint(wal.head());
    assert!(wal.try_reset());
    assert_eq!(wal.file_version(), OPLOG_FILE_VERSION);
    tie(&wal, 200);
    let end = wal_pos_offset(wal.head()) as usize;
    tie(&wal, 201);
    wal.append(Op::Commit).unwrap();
    let file = std::fs::read(&p).unwrap();
    assert_eq!(mark_of_tie_ending_at(&file, end), Mark::Bound, "v5 になった後の record は印を中身に結ぶ");
    assert_eq!((half_version(&file, 0), half_version(&file, HALF)), (5, 4), "A の版が届くまで B の版は 4 のまま");
    assert_eq!(values(&wal.recover_with_tail()), vec![200, 201]);
    // 全体を書き出さずにもう 1 回畳んでも、 B の版は 4 のまま (B の 5 だけ先に届くと、 0.37.x は B を空に書き直す)
    wal.advance_checkpoint(wal.head());
    assert!(wal.try_reset());
    tie(&wal, 250);
    wal.append(Op::Commit).unwrap();
    assert_eq!(half_version(&std::fs::read(&p).unwrap(), HALF), 4);

    // A の版が届いた後 (fsync) の畳みで B の版も 5
    wal.fsync().unwrap();
    wal.advance_checkpoint(wal.head());
    assert!(wal.try_reset());
    tie(&wal, 300);
    wal.append(Op::Commit).unwrap();
    let file = std::fs::read(&p).unwrap();
    assert_eq!((half_version(&file, 0), half_version(&file, HALF)), (5, 5));
    drop(wal);
    let wal = OpLog::open(&p).unwrap();
    assert_eq!((wal.file_version(), wal.two_halves()), (OPLOG_FILE_VERSION, true));
    assert_eq!(values(&wal.recover_with_tail()), vec![300]);
    // 開き直した後も結んで書く
    tie(&wal, 301);
    let end = wal_pos_offset(wal.head()) as usize;
    assert_eq!(mark_of_tie_ending_at(&std::fs::read(&p).unwrap(), end), Mark::Bound);
}

/// engine の開く道と同じ順 (再生 → `drop_records_after_a_hole` → checkpoint) で v4 の file を開いて書く。 最初の書き込みは
/// もう一方の区画へ移り (#706)、 古い区画を畳んだ所で v5 になる = 今の区画には、 結ばない形の record の後ろに結んだ形の record が
/// 続く。 読みはどちらも通す (開き直しても)。
#[test]
fn one_half_holds_marks_of_both_forms_across_the_change() {
    let root = Scratch::new("mixed");
    let p = root.fixture();
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
    let want: Vec<u64> = (200..=209).collect();
    assert_eq!(values(&wal.recover_with_tail()), want);
    drop(wal);
    let wal = OpLog::open(&p).unwrap();
    assert_eq!(wal.file_version(), OPLOG_FILE_VERSION);
    assert_eq!(values(&wal.recover_with_tail()), want);
}

/// v4 → v5 の変わり目の電源断の像: どの像も v4 か v5 として開けて、 書き出しの返った値まで再生する (それより古い値で終わらない・
/// 値が飛ばない)。
#[cfg(feature = "crashsim")]
mod power_loss {
    use super::*;
    use enchudb_oplog::crashsim::{self, Mode};

    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn page() -> usize {
        unsafe extern "C" {
            fn getpagesize() -> i32;
        }
        unsafe { getpagesize() as usize }
    }

    /// 像を開いて (版, 再生した値)。
    fn open_image(img: &Path) -> (u32, Vec<u64>) {
        let w = OpLog::open(img).unwrap_or_else(|e| panic!("像 {} を開けない: {e}", img.display()));
        (w.file_version(), values(&w.recover_with_tail()))
    }

    /// 再生した値は 200 から 1 つずつ続き、 書き出しの返った `acked` まで届く。
    fn assert_replays_through(tag: &str, vals: &[u64], acked: u64) {
        let want: Vec<u64> = (200..200 + vals.len() as u64).collect();
        assert_eq!(vals, want, "{tag}: 値が続いていない");
        assert!(vals.last().is_some_and(|&v| v >= acked), "{tag}: 書き出しの返った {acked} まで届かない ({:?})", vals.last());
    }

    /// 段ごとに像を撮る (`Lost` と `Mixed` の seed 32 個)。 `versions` = その段でありうる版。
    fn check_images(live: &Path, root: &Path, tag: &str, acked: u64, versions: &[u32]) {
        let modes = std::iter::once(Mode::Lost).chain((0..32).map(|seed| Mode::Mixed { seed }));
        for (i, mode) in modes.enumerate() {
            let dir = root.join(format!("{tag}-{i}"));
            crashsim::capture(live, &dir, mode).unwrap();
            let (ver, vals) = open_image(&dir.join("oplog"));
            let tag = format!("{tag} {mode:?}");
            assert!(versions.contains(&ver), "{tag}: 版 {ver}");
            assert_replays_through(&tag, &vals, acked);
            std::fs::remove_dir_all(&dir).unwrap();
        }
    }

    #[test]
    fn every_image_around_the_change_opens_and_replays_the_acked_values() {
        let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let root = Scratch::new("images");
        let live = root.0.join("live");
        std::fs::create_dir_all(&live).unwrap();
        let p = live.join("oplog");
        std::fs::copy(FIXTURE, &p).unwrap();
        crashsim::start();
        crashsim::file_synced(&p); // 0.37.4 が書いて disk にある file
        let wal = OpLog::open(&p).unwrap();
        assert_eq!(values(&wal.recover_with_tail()).len(), 23);
        wal.drop_records_after_a_hole();
        let a_end = wal.head();
        // 再生した分 (古い区画 A) は本体に当てて書き出した。 以後の書き込み (B) は checkpoint を進めない = 再生に残る
        wal.advance_checkpoint(a_end);
        wal.fsync().unwrap();

        // 1) v4 のまま書いて書き出す
        for v in 200..=209 {
            tie(&wal, v);
        }
        wal.append(Op::Commit).unwrap();
        wal.fsync().unwrap();
        assert_eq!(wal.file_version(), OPLOG_FILE_VERSION_V4);
        check_images(&live, &root.0, "v4", 209, &[OPLOG_FILE_VERSION_V4]);
        let v4_image = root.0.join("v4-kept");
        crashsim::capture(&live, &v4_image, Mode::Lost).unwrap();

        // 2) 古い区画を畳む = v5 になる (A の header の page は畳んだ側が書き出す)
        assert!(wal.try_reset(), "前提: 古い区画 (A) を畳める");
        assert_eq!(wal.file_version(), OPLOG_FILE_VERSION);
        check_images(&live, &root.0, "folded", 209, &[OPLOG_FILE_VERSION]);

        // 3) v5 の形の record を書く (書き出さない): B の page は届いても届かなくてもよい
        for v in 210..=219 {
            tie(&wal, v);
        }
        wal.append(Op::Commit).unwrap();
        check_images(&live, &root.0, "bound-unsynced", 209, &[OPLOG_FILE_VERSION]);

        // 4) 書き出す
        wal.fsync().unwrap();
        check_images(&live, &root.0, "bound-synced", 219, &[OPLOG_FILE_VERSION]);

        // 5) A の header の page だけ v4 の時のまま (版 4・前の周)、 B の page は今の中身: v5 の版が届く前に、 結んだ形の record の
        //    page を OS が先に書いた像。 v4 の file として開けて、 結ばない形と結んだ形の両方を再生する
        let img = root.0.join("stitched");
        crashsim::capture(&live, &img, Mode::Lost).unwrap();
        crashsim::stop();
        let ps = page();
        let head_pages = HALF.div_ceil(ps) * ps;
        assert_eq!(head_pages, HALF, "前提: 区画の境は page の境");
        let old = std::fs::read(v4_image.join("oplog")).unwrap();
        let mut image = std::fs::read(img.join("oplog")).unwrap();
        assert_eq!((half_version(&image, 0), half_version(&old, 0)), (5, 4));
        image[..HALF].copy_from_slice(&old[..HALF]);
        std::fs::write(img.join("oplog"), &image).unwrap();
        let (ver, vals) = open_image(&img.join("oplog"));
        assert_eq!(ver, OPLOG_FILE_VERSION_V4);
        assert_replays_through("stitched", &vals, 219);
        drop(wal);
    }
}
