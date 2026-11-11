//! review (0.37.4): 電源断の像で、 再生 (`recover_with_tail`) が 「書き出しの返った record より古い record」 を後ろに当てる 2 つの道。
//!
//! 像は crashsim の控え (`Mode::Lost` = 書き出しの済んだ page だけ) を土台にし、 `Mixed` が選びうる 1 つの組 (oplog の特定の page
//! だけ 「今の中身」) を手で作る (seed を探す代わり)。 どの page も 「最後に書き出した中身」 か 「今の中身」 のどちらか。
//!
//! cargo test -p enchudb-oplog --features crashsim --test review_wal_stale_replay -- --nocapture --test-threads=1

#![cfg(all(feature = "crashsim", unix))]

use enchudb_oplog::crashsim::{self, Mode};
use enchudb_oplog::oplog::{DecodedOp, Op, OpLog, Record};
use std::path::{Path, PathBuf};

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

const CAP: usize = 128 * 1024; // 2 区画、 1 区画 64 KiB

fn scratch(tag: &str) -> PathBuf {
    let base = std::env::var("REVIEW_SCRATCH").unwrap_or_else(|_| {
        "/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/eedfc333-d87a-4a47-a3b0-daf4b6d7337a/scratchpad".into()
    });
    let p = PathBuf::from(base).join(format!("review_wal_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn page() -> usize {
    unsafe { libc_page() }
}
unsafe fn libc_page() -> usize {
    unsafe extern "C" {
        fn getpagesize() -> i32;
    }
    unsafe { getpagesize() as usize }
}

/// 1 回の書き込み = Tie + Commit (どの書き込みも同じ大きさ)。
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

/// `img` の oplog の page `pages` を、 live の今の中身で置き換える (OS が先に書き出していた page)。
fn overlay(live: &Path, img: &Path, pages: std::ops::Range<usize>) {
    let ps = page();
    let cur = std::fs::read(live).unwrap();
    let mut out = std::fs::read(img).unwrap();
    for p in pages {
        out[p * ps..(p + 1) * ps].copy_from_slice(&cur[p * ps..(p + 1) * ps]);
    }
    std::fs::write(img, out).unwrap();
}

/// (1) その場で畳んだ後 (header は書き出さない = 既定)、 新しい周の record の page だけが先に届いた像。
/// header は前の周 (head = H1、 checkpoint = 1 つ前の書き出しの位置) のまま、 page 1 は新しい周。
/// 再生は前の周の record を checkpoint から page 0 の終わりまでだけ当てる = 本体 (H1 まで当てて書き出し済み) の値を、
/// それより古い値で上書きする。
#[test]
fn stale_prefix_is_replayed_after_an_unsynced_fold() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("fold");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    crashsim::start();
    let path = live.join("oplog");
    let wal = OpLog::create(&path, CAP).unwrap();
    assert!(wal.two_halves());

    // engine の oplog_sync と同じ順: 終わりを取る → fsync (+ 本体の msync) → checkpoint を進める
    let sync = |wal: &OpLog| {
        let head = wal.written_head();
        wal.fsync().unwrap();
        wal.advance_checkpoint(head);
    };
    for v in 1..=5 {
        write(&wal, v);
    }
    sync(&wal);
    for v in 6..=150 {
        write(&wal, v);
    }
    sync(&wal); // ← 150 まで書き出しが返った (本体も 150 まで書き出し済み)
    let acked = 150u64;
    let lap_before = wal.lap();
    // consumer の tick: head == checkpoint なので畳む (その場で、 header は書き出さない)
    assert!(wal.try_reset(), "前提: その場で畳める");
    assert_ne!(wal.lap(), lap_before);
    // 新しい周に書く (書き出さない)
    for v in 151..=300 {
        write(&wal, v);
    }
    let img = root.join("img");
    crashsim::capture(&live, &img, Mode::Lost).unwrap();
    crashsim::stop();
    // OS が page 1 だけ先に書いていた
    overlay(&path, &img.join("oplog"), 1..2);
    drop(wal);

    let w = OpLog::open(&img.join("oplog")).unwrap();
    let recs = w.recover_with_tail();
    let vals = values(&recs);
    let _ = std::fs::remove_dir_all(&root);
    eprintln!("[fold] 再生した値: {} 件、 先頭 {:?} 最後 {:?} (書き出しの返った値 = {acked})", vals.len(), vals.first(), vals.last());
    // 正しい再生は 「何も当てない」 (畳んだ後) か 「150 まで全部」 か 「150 より後まで」。 150 より前で止まると、 本体の 150 が古い値に戻る
    assert!(
        vals.is_empty() || *vals.last().unwrap() >= acked,
        "再生が書き出しの返った値 {acked} より古い値 {:?} で終わる (本体の値が前に戻る)",
        vals.last()
    );
}

/// (2) 同じ周のまま開き直した区画に、 前の session の record が head の先に残る。 次の session がその上に同じ大きさの record を
/// 書き、 区画を移る時に header の page だけを書き出す (#663) と、 「header の head は区画の終わり、 途中の page は前の session の
/// record (同じ周・CRC も合う・境目もそろう)」 という像が、 書き出しの済んだ page だけ (`Lost`) でできる。
///
/// session 1 の像だけ `Mixed` (header の page は前の書き出しのまま、 record の page は届いた = いちばん普通の途中の像)。
#[test]
fn records_of_a_previous_session_are_replayed_after_synced_ones() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("session");
    let live1 = root.join("live1");
    std::fs::create_dir_all(&live1).unwrap();

    // ── session 1 ──
    crashsim::start();
    let p1 = live1.join("oplog");
    let wal = OpLog::create(&p1, CAP).unwrap();
    write(&wal, 1);
    wal.fsync().unwrap(); // header の head = h0 が届く
    let h0 = wal.head();
    for v in 1000..1200 {
        write(&wal, v); // 書き出さない (page 0 の残り 〜 page 3)
    }
    let live2 = root.join("live2");
    crashsim::capture(&live1, &live2, Mode::Lost).unwrap();
    crashsim::stop();
    let np = CAP / 2 / page();
    overlay(&p1, &live2.join("oplog"), 1..np); // 区画 A の page 1 から先は届いていた (header の page 0 は前のまま)
    drop(wal);

    // ── session 2: 像を開く ──
    crashsim::start();
    let p2 = live2.join("oplog");
    let wal = OpLog::open(&p2).unwrap();
    let recs = wal.recover_with_tail();
    assert_eq!(values(&recs), vec![1], "前提: session 1 の再生は header の head まで");
    assert!(!wal.drop_records_after_a_hole());
    assert_eq!(wal.head(), h0, "前提: 続きは h0 から (同じ周)");
    // engine の開く時と同じ: 書き出してから checkpoint を head へ
    wal.fsync().unwrap();
    wal.advance_checkpoint(wal.head());
    for v in 2000..2100 {
        write(&wal, v);
    }
    let head = wal.written_head();
    wal.fsync().unwrap(); // ← 2099 まで書き出しが返った
    wal.advance_checkpoint(head);
    let acked = 2099u64;
    // 区画を移るまで書く (移る時に A の header の page だけ書き出す)。 その後 B に少し
    let mut v = 2100;
    while wal.switches() == 0 {
        write(&wal, v);
        v += 1;
    }
    for _ in 0..5 {
        write(&wal, v);
        v += 1;
    }
    let img = root.join("img");
    let st = crashsim::capture(&live2, &img, Mode::Lost).unwrap(); // 書き出しの済んだ page だけ
    crashsim::stop();
    drop(wal);
    eprintln!("[session] 像: 書き出し済みの page {}、 失われうる page {:?}", st.durable_pages, st.divergent);

    // ── session 3 ──
    let w = OpLog::open(&img.join("oplog")).unwrap();
    let recs = w.recover_with_tail();
    let vals = values(&recs);
    let _ = std::fs::remove_dir_all(&root);
    let stale: Vec<u64> = vals.iter().copied().filter(|v| (1000..1200).contains(v)).collect();
    eprintln!(
        "[session] 再生した値 {} 件: 先頭 {:?} … 最後 {:?}。 session 1 の値 (1000..1200) が {} 件 ({:?} 〜 {:?})、 書き出しの返った値 = {acked}",
        vals.len(),
        vals.first(),
        vals.last(),
        stale.len(),
        stale.first(),
        stale.last()
    );
    assert!(stale.is_empty(), "前の session の record (開いた時に捨てたはずの {} 件) を、 書き出しの返った {acked} の後ろに再生した", stale.len());
}
