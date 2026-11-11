//! #707: 畳んだ後 (メモリの header だけ新しい周・頭)、 header が disk に届く前に新しい周の record を書くと、 「header は前の周の
//! まま、 後ろの page だけ新しい周」 の像で、 再生が前の周の record を checkpoint (disk の値は 1 回分遅れている) から上書き
//! された所までだけ当て直した。 本体は畳む前の終わりまで当てて書き出してあるので、 同じ cell の値が途中の値に戻る。
//!
//! 直し: 畳んだ区画には、 畳んだ header が届くまで書かない (`OpLog::settle_fold`)。 像は crashsim の控え (`Mode::Lost` =
//! 書き出しの済んだ page だけ) を土台に、 「OS が先に書いていた page」 を手で重ねる (どの page も 「最後に書き出した中身」 か
//! 「今の中身」)。 同じ cell を書き換え続ける (各 cell を 1 回しか書かない試験では、 途中までの当て直しが見えない)。
//!
//! cargo test -p enchudb-oplog --features crashsim --test issue707_unsynced_fold -- --test-threads=1

#![cfg(all(feature = "crashsim", unix))]

use enchudb_oplog::crashsim::{self, Mode};
use enchudb_oplog::oplog::{DecodedOp, Op, OpLog, Record, wal_pos_offset};
use std::path::{Path, PathBuf};

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

const CAP: usize = 128 * 1024; // 2 区画、 1 区画 64 KiB
const HALF: usize = CAP / 2;

/// 一時の置き場 (落ちても消す)。
struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchu-issue707-oplog-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        Scratch(p)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn page() -> usize {
    unsafe extern "C" {
        fn getpagesize() -> i32;
    }
    unsafe { getpagesize() as usize }
}

/// 1 回の書き込み = 同じ cell への Tie + Commit (どの書き込みも同じ大きさ)。
fn write(wal: &OpLog, v: u64) {
    wal.append(Op::Tie { eid: 1, himo_id: 0, value: v }).unwrap();
    wal.append(Op::Commit).unwrap();
}

fn values(recs: &[Record]) -> Vec<u64> {
    recs.iter()
        .filter_map(|r| match r.op {
            DecodedOp::Tie { value, .. } => Some(value),
            _ => None,
        })
        .collect()
}

/// engine の `oplog_sync` と同じ順: 終わりを取る → fsync (+ 本体の msync) → checkpoint を進める。
fn sync(wal: &OpLog) {
    let head = wal.written_head();
    wal.fsync().unwrap();
    wal.advance_checkpoint(head);
}

/// `img` の byte `range` (page の境) を、 live の今の中身で置き換える (OS が先に書き出していた page)。
fn overlay(live: &Path, img: &Path, range: std::ops::Range<usize>) {
    assert!(range.start.is_multiple_of(page()) && range.end.is_multiple_of(page()));
    let cur = std::fs::read(live).unwrap();
    let mut out = std::fs::read(img).unwrap();
    out[range.clone()].copy_from_slice(&cur[range]);
    std::fs::write(img, out).unwrap();
}

/// 今電源が落ちたら disk に残る oplog (書き出しの済んだ page だけ)。
fn on_disk(live: &Path, root: &Path, tag: &str) -> Vec<u8> {
    let dir = root.join(tag);
    crashsim::capture(live, &dir, Mode::Lost).unwrap();
    std::fs::read(dir.join("oplog")).unwrap()
}

/// 区画の header (base から 32 byte) の (周, head, checkpoint)。
fn header(file: &[u8], base: usize) -> (u32, u64, u64) {
    let r64 = |o: usize| u64::from_le_bytes(file[base + o..base + o + 8].try_into().unwrap());
    (u32::from_le_bytes(file[base + 28..base + 32].try_into().unwrap()), r64(8), r64(16))
}

/// 再生した値は 「何も無い」 か 「書き出しの返った値まで届く」 こと。 手前で終わると、 本体 (書き出しの返った値まで当てて
/// 書き出し済み) の値が、 それより古い値に戻る。
fn assert_replay_does_not_go_back(vals: &[u64], acked: u64) {
    assert!(
        vals.is_empty() || *vals.last().unwrap() >= acked,
        "再生が書き出しの返った値 {acked} より古い値 {:?} で終わる ({} 件、 先頭 {:?}) = 本体の値が前に戻る",
        vals.last(),
        vals.len(),
        vals.first()
    );
}

/// 暇な WAL をその場で畳んだ後に書き始め、 新しい周の record の page (page 1) だけが先に届いた像。
#[test]
fn a_fold_in_place_is_not_overwritten_before_its_header_is_durable() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = Scratch::new("inplace");
    let live = root.0.join("live");
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let path = live.join("oplog");
    let wal = OpLog::create(&path, CAP).unwrap();
    assert!(wal.two_halves());
    let ps = page();

    for v in 1..=5 {
        write(&wal, v);
    }
    sync(&wal);
    for v in 6..=150 {
        write(&wal, v);
    }
    sync(&wal); // ← 150 まで書き出しが返った (本体も 150 まで書き出し済み)
    let acked = 150u64;
    let before = on_disk(&live, &root.0, "before");
    let (lap_before, head_before, cp_before) = header(&before, 0);
    assert!(head_before as usize > 2 * ps, "前提: 畳む前の record は page 1 の先まである (head {head_before})");
    assert!((cp_before as usize) < ps, "前提: disk の checkpoint は 1 回前の書き出しの位置 = page 0 の中 ({cp_before})");

    // consumer の tick: head == checkpoint なので畳む
    assert!(wal.try_reset(), "前提: その場で畳める");
    let lap_after = wal.lap();
    assert_ne!(lap_after, lap_before, "前提: 畳むと新しい周");
    // 新しい周に書く (書き出さない)。 page 1 の先まで
    for v in 151..=300 {
        write(&wal, v);
    }
    assert!(wal_pos_offset(wal.head()) as usize > 2 * ps);
    let img_dir = root.0.join("img");
    crashsim::capture(&live, &img_dir, Mode::Lost).unwrap();
    crashsim::stop();
    let img = img_dir.join("oplog");
    overlay(&path, &img, ps..2 * ps); // OS が page 1 だけ先に書いていた
    let now = std::fs::read(&path).unwrap();
    drop(wal);

    // 像の形: page 1 は新しい周の record (畳む前と違う中身)、 header の page は 「畳む前のまま」 か 「畳んだ後」
    let image = std::fs::read(&img).unwrap();
    assert_eq!(image[ps..2 * ps], now[ps..2 * ps]);
    assert_ne!(image[ps..2 * ps], before[ps..2 * ps], "前提: page 1 は新しい周の record で上書きされている");
    let (img_lap, img_head, img_cp) = header(&image, 0);
    eprintln!("[in place] 像の header: 周 {img_lap} head {img_head} checkpoint {img_cp} (畳む前: 周 {lap_before} head {head_before} checkpoint {cp_before}、 畳んだ後の周 {lap_after})");
    assert!(
        (img_lap, img_head, img_cp) == (lap_before, head_before, cp_before) || img_lap == lap_after,
        "前提: header の page は畳む前のままか、 畳んだ後"
    );

    let w = OpLog::open(&img).unwrap();
    let vals = values(&w.recover_with_tail());
    eprintln!("[in place] 再生した値 {} 件: 先頭 {:?} 最後 {:?} (書き出しの返った値 {acked})", vals.len(), vals.first(), vals.last());
    assert_replay_does_not_go_back(&vals, acked);
    // 仕組み: 新しい周の record を書いた時には、 畳んだ header が届いている
    assert_eq!(img_lap, lap_after, "新しい周の record を書く前に、 畳んだ header が disk に届いていること");
}

/// 2 区画の普段の回り方: 古い区画 A を畳み、 書き手が今の区画 B を埋めて A へ移り、 A の page 1 から先だけが先に届いた像。
/// A の header が畳む前のままだと、 再生は A の前の周を checkpoint から page 0 の終わりまで当て直して止まり、 その後に積んだ
/// B の record (書き出しの返った分も) を穴の後ろとして読まない。
#[test]
fn a_folded_old_half_is_not_entered_before_its_header_is_durable() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = Scratch::new("oldhalf");
    let live = root.0.join("live");
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let path = live.join("oplog");
    let wal = OpLog::create(&path, CAP).unwrap();
    assert!(wal.two_halves());
    let ps = page();

    let mut v = 1u64;
    for _ in 0..5 {
        write(&wal, v);
        v += 1;
    }
    sync(&wal); // A の disk の checkpoint は次の書き出しまでここ (page 0 の中)
    // A を埋めて B へ移り、 B に少し書く
    while wal.switches() == 0 {
        write(&wal, v);
        v += 1;
    }
    for _ in 0..20 {
        write(&wal, v);
        v += 1;
    }
    sync(&wal); // ← ここまで書き出しが返った (A の record は全部、 B の頭の分も)
    let acked = v - 1;
    let before = on_disk(&live, &root.0, "before");
    let (a_lap, a_head, a_cp) = header(&before, 0);
    assert!((a_cp as usize) < ps && a_head as usize > 2 * ps, "前提: A の disk の checkpoint は page 0 の中、 record は page 1 の先まで");
    // consumer: 古い区画 A (全部書き出し済み) を畳む
    assert!(wal.try_reset(), "前提: 古い区画を畳める");
    // 書き手: B を埋めて A へ移り (移る時に書き出すのは B の header の page だけ)、 A の page 1 の先まで書く
    while wal.switches() == 1 {
        write(&wal, v);
        v += 1;
    }
    let a_lap_new = wal.lap();
    assert_ne!(a_lap_new, a_lap);
    while (wal_pos_offset(wal.head()) as usize) < 2 * ps + 1024 {
        write(&wal, v);
        v += 1;
    }
    let img_dir = root.0.join("img");
    crashsim::capture(&live, &img_dir, Mode::Lost).unwrap();
    crashsim::stop();
    let img = img_dir.join("oplog");
    overlay(&path, &img, ps..2 * ps); // OS が A の page 1 だけ先に書いていた
    let now = std::fs::read(&path).unwrap();
    drop(wal);

    let image = std::fs::read(&img).unwrap();
    assert_eq!(image[ps..2 * ps], now[ps..2 * ps]);
    assert_ne!(image[ps..2 * ps], before[ps..2 * ps], "前提: A の page 1 は新しい周の record で上書きされている");
    let (img_lap, img_head, img_cp) = header(&image, 0);
    let (b_lap, b_head, _) = header(&image, HALF);
    eprintln!("[old half] 像の A の header: 周 {img_lap} head {img_head} checkpoint {img_cp} (畳む前: 周 {a_lap} head {a_head} checkpoint {a_cp}、 畳んだ後の周 {a_lap_new})、 B: 周 {b_lap} head {b_head}");
    assert!((img_lap, img_head, img_cp) == (a_lap, a_head, a_cp) || img_lap == a_lap_new, "前提: A の header の page は畳む前のままか、 畳んだ後");
    assert!(b_head as usize > HALF - 1024, "前提: B の header は移る時に書き出した最後の head");

    let w = OpLog::open(&img).unwrap();
    let vals = values(&w.recover_with_tail());
    eprintln!("[old half] 再生した値 {} 件: 先頭 {:?} 最後 {:?} (書き出しの返った値 {acked})", vals.len(), vals.first(), vals.last());
    assert_replay_does_not_go_back(&vals, acked);
    assert_eq!(img_lap, a_lap_new, "畳んだ区画へ移る前に、 畳んだ header が disk に届いていること");
}
