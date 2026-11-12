//! #706: 開いた区画を同じ周のまま head から書き足すと、 前の session が書いて header に届かなかった record (head の先に残る。
//! 周の印は同じ、 CRC も合う) を、 新しい session の record の後ろに続きとして再生した。 区画を移る時は header の page だけを
//! 書き出す (#663) ので、 「header の head は区画の終わり、 途中の page から先は前の session のまま」 の像が、 書き出しの済んだ
//! page だけ (`Mode::Lost`) でできる。 record の境目がそろう (同じ大きさの record の繰り返し) と、 そのまま読める。
//!
//! 直し: 開いた session は、 この session が置いた周の区画にしか書かない (`OpLog::drop_records_after_a_hole` の後半)。
//! 同じ cell を書き換え続ける (各 cell を 1 回しか書かない試験では、 古い値での上書きが見えない)。
//!
//! cargo test -p enchudb-oplog --features crashsim --test issue706_previous_session_records -- --test-threads=1

#![cfg(all(feature = "crashsim", unix))]

use enchudb_oplog::crashsim::{self, Mode};
use enchudb_oplog::oplog::{DecodedOp, Op, OpLog, Record, wal_pos_offset};
use std::path::{Path, PathBuf};

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

const CAP: usize = 128 * 1024; // 2 区画、 1 区画 64 KiB
const HALF: usize = CAP / 2;
/// session ごとの値の土台 (u32 に入らない値 = payload に 8 byte でそのまま載る。 像の中を byte で探せる)。
const S1: u64 = 0x5E55_0001_0000_0000;
const S2: u64 = 0x5E55_0002_0000_0000;
const S3: u64 = 0x5E55_0003_0000_0000;

/// 一時の置き場 (落ちても消す)。
struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchu-issue706-oplog-{}-{tag}", std::process::id()));
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

/// 1 回の書き込み = 同じ cell への Tie + Commit (どの書き込みも同じ大きさ)。 載らなければ (満杯) false。
fn try_write(wal: &OpLog, v: u64) -> bool {
    wal.append(Op::Tie { eid: 1, himo_id: 0, value: v }).is_ok() && wal.append(Op::Commit).is_ok()
}

fn write(wal: &OpLog, v: u64) {
    assert!(try_write(wal, v), "満杯");
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

/// file の中で値 `v` (8 byte) が載っている位置。
fn find_value(file: &[u8], v: u64) -> Option<usize> {
    let pat = v.to_le_bytes();
    file.windows(8).position(|w| w == pat)
}

/// engine の開く道と同じ: 再生 → 穴の後ろを捨てる (+ この session の書く区画を決める) → 書き出して checkpoint を head へ。
fn open_like_the_engine(path: &Path) -> (OpLog, Vec<u64>) {
    let wal = OpLog::open(path).unwrap();
    let vals = values(&wal.recover_with_tail());
    wal.drop_records_after_a_hole();
    wal.fsync().unwrap();
    wal.advance_checkpoint(wal.head());
    (wal, vals)
}

/// session 1: 1 件を書き出した後、 書き出さずに `n` 件書いて電源断。 header の page は前の書き出しのまま、 区画 A の page 1 から
/// 先は届いた像 (いちばん普通の途中の像) を `to` に作る。 戻り値は (header に届いた head = h0、 A の周)。
fn session_1_dies_with_unsynced_records(root: &Path, to: &Path, n: u64) -> (u64, u32) {
    let live = root.join("live1");
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let path = live.join("oplog");
    let wal = OpLog::create(&path, CAP).unwrap();
    assert!(wal.two_halves());
    write(&wal, 1);
    wal.fsync().unwrap(); // header の head = h0 が届く
    let h0 = wal.head();
    for k in 0..n {
        write(&wal, S1 + k);
    }
    assert_eq!(wal.switches(), 0, "前提: session 1 は区画 A の中");
    crashsim::capture(&live, to, Mode::Lost).unwrap();
    crashsim::stop();
    overlay(&path, &to.join("oplog"), page()..HALF);
    let lap = wal.lap();
    drop(wal);
    // 前提: session 1 の record は head の先に (header に届かないまま) 残っている
    let image = std::fs::read(to.join("oplog")).unwrap();
    let at = find_value(&image, S1 + n - 1).expect("前提: session 1 の最後の record が像にある");
    assert!(at > wal_pos_offset(h0) as usize && at < HALF);
    (h0, lap)
}

/// 開いてすぐ (畳む前に) 同じ大きさの record を書いて書き出し、 さらに区画を移るまで書いた像。
#[test]
fn records_left_past_the_head_by_a_dead_session_are_never_replayed() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = Scratch::new("session");
    let live2 = root.0.join("live2");
    let (h0, lap1) = session_1_dies_with_unsynced_records(&root.0, &live2, 200);

    // ── session 2: 像を開く ──
    crashsim::start();
    let p2 = live2.join("oplog");
    let (wal, replayed) = open_like_the_engine(&p2);
    assert_eq!(replayed, vec![1], "前提: session 1 の再生は header の head まで");
    assert_eq!(wal.head(), h0, "前提: 開いた時の続きは h0 (session 1 の周 {lap1}) — 開く時に何も積まない");
    for k in 0..100 {
        write(&wal, S2 + k);
    }
    sync(&wal); // ← S2 + 99 まで書き出しが返った
    let acked = S2 + 99;
    // 区画を移るまで書く (移る時に、 埋めた区画の header の page だけを書き出す = head は区画の終わり)。 その後に少し。
    // もう移れない形 (もう一方に畳めていない record がある) なら満杯で止まる
    let switches = wal.switches();
    let mut k = 100;
    while wal.switches() == switches && try_write(&wal, S2 + k) {
        k += 1;
    }
    for _ in 0..5 {
        if !try_write(&wal, S2 + k) {
            break;
        }
        k += 1;
    }
    let img_dir = root.0.join("img");
    let st = crashsim::capture(&live2, &img_dir, Mode::Lost).unwrap(); // 書き出しの済んだ page だけ
    crashsim::stop();
    drop(wal);
    let img = img_dir.join("oplog");
    eprintln!("[session] 像: 書き出し済みの page {}、 失われうる page {:?}", st.durable_pages, st.divergent);
    // 前提: 像には session 1 の record がまだ残っている (消えたから読まれない、 ではない)
    let image = std::fs::read(&img).unwrap();
    let at = find_value(&image, S1 + 199).expect("前提: session 1 の record が像に残っている");
    assert!(at > wal_pos_offset(h0) as usize && at < HALF);

    // ── session 3 ──
    let w = OpLog::open(&img).unwrap();
    let vals = values(&w.recover_with_tail());
    let stale: Vec<u64> = vals.iter().copied().filter(|v| (S1..S2).contains(v)).collect();
    eprintln!(
        "[session] 再生した値 {} 件: 先頭 {:x?} … 最後 {:x?}。 session 1 の値が {} 件 ({:x?} 〜 {:x?})、 書き出しの返った値 {acked:x}",
        vals.len(),
        vals.first(),
        vals.last(),
        stale.len(),
        stale.first(),
        stale.last()
    );
    assert!(
        stale.is_empty(),
        "前の session の record (開いた時に捨てたはずの {} 件) を、 書き出しの返った {acked:x} の後ろに再生した",
        stale.len()
    );
    assert!(vals.is_empty() || *vals.last().unwrap() >= acked, "再生が書き出しの返った値より古い値 {:x?} で終わる", vals.last());
}

/// 開いた session が置いた周も、 その session が header を届ける前に落ちたら次の session が使い直さないこと。
/// session 2 は開いてすぐ書いて (書き出さずに) 落ち、 record の page だけが届く。 session 3 が同じ数の周で同じ区画に書くと、
/// session 2 の record が同じ形で session 3 の record の後ろに残る。
#[test]
fn a_lap_placed_by_a_session_that_died_before_any_sync_is_not_reused() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = Scratch::new("twice");
    let live2 = root.0.join("live2");
    let (h0, _) = session_1_dies_with_unsynced_records(&root.0, &live2, 200);

    // ── session 2: 開いて、 書き出さずに 200 件書いて落ちる。 header の page (各区画の頭) は届かず、 他の page は届く ──
    // (開いた後の fsync を呼ばない開き方: 置いた周は最初の append が届ける)
    crashsim::start();
    let p2 = live2.join("oplog");
    let wal = OpLog::open(&p2).unwrap();
    assert_eq!(values(&wal.recover_with_tail()), vec![1]);
    wal.drop_records_after_a_hole();
    assert_eq!(wal.head(), h0);
    for k in 0..200 {
        write(&wal, S2 + k);
    }
    let live3 = root.0.join("live3");
    crashsim::capture(&live2, &live3, Mode::Lost).unwrap();
    crashsim::stop();
    let ps = page();
    overlay(&p2, &live3.join("oplog"), ps..HALF);
    overlay(&p2, &live3.join("oplog"), HALF + ps..CAP);
    let lap2 = wal.lap();
    drop(wal);
    let image = std::fs::read(live3.join("oplog")).unwrap();
    let s2_at = find_value(&image, S2 + 199).expect("前提: session 2 の record が像にある");

    // ── session 3: 開いて書き、 書き出しが返り、 さらに区画を移るまで書いた像 ──
    crashsim::start();
    let p3 = live3.join("oplog");
    let (wal, replayed) = open_like_the_engine(&p3);
    assert!(replayed.iter().all(|v| !(S2..S3).contains(v)), "前提: session 2 の record は header に届いていない (再生しない)");
    for k in 0..50 {
        write(&wal, S3 + k);
    }
    let lap3 = wal.lap();
    sync(&wal); // ← S3 + 49 まで書き出しが返った
    let acked = S3 + 49;
    // consumer: 書き出し済みの古い区画を畳む (畳めれば)。 書き手: 今の区画を埋めて移る
    wal.try_reset();
    let switches = wal.switches();
    let mut k = 50;
    while wal.switches() == switches && try_write(&wal, S3 + k) {
        k += 1;
    }
    for _ in 0..5 {
        if !try_write(&wal, S3 + k) {
            break;
        }
        k += 1;
    }
    let img_dir = root.0.join("img");
    crashsim::capture(&live3, &img_dir, Mode::Lost).unwrap();
    crashsim::stop();
    drop(wal);
    let image = std::fs::read(img_dir.join("oplog")).unwrap();
    assert_eq!(find_value(&image, S2 + 199), Some(s2_at), "前提: session 2 の record が像に残っている");

    let w = OpLog::open(&img_dir.join("oplog")).unwrap();
    let vals = values(&w.recover_with_tail());
    let stale: Vec<u64> = vals.iter().copied().filter(|v| (S1..S3).contains(v)).collect();
    eprintln!(
        "[twice] session 2 の周 {lap2}、 session 3 の周 {lap3}。 再生した値 {} 件: 最後 {:x?}。 前の session の値が {} 件 ({:x?} 〜 {:x?})、 書き出しの返った値 {acked:x}",
        vals.len(),
        vals.last(),
        stale.len(),
        stale.first(),
        stale.last()
    );
    assert!(stale.is_empty(), "前の session の record {} 件を、 書き出しの返った {acked:x} の後ろに再生した", stale.len());
    assert!(vals.is_empty() || *vals.last().unwrap() >= acked, "再生が書き出しの返った値より古い値 {:x?} で終わる", vals.last());
    assert_ne!(lap3, lap2, "落ちた session が置いた周を使い直さないこと");
}
