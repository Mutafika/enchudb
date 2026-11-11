//! review (0.37.4): `crates/enchudb-oplog/tests/review_wal_stale_replay.rs` の 2 つの道を engine で (版数の列の無い、 既定の DB)。
//! 書き出し (`oplog_sync`) の返った値が、 電源断の像を開くと **それより古い値** に戻る。
//!
//! cargo test -p enchudb-engine --features crashsim --test review_wal_power_loss_regress -- --nocapture --test-threads=1

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::crashsim::{self, Copied, Mode};
use enchudb_oplog::oplog::wal_pos_offset;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
const CAP: usize = 128 * 1024;

fn scratch(tag: &str) -> PathBuf {
    let base = std::env::var("REVIEW_SCRATCH").unwrap_or_else(|_| {
        "/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/eedfc333-d87a-4a47-a3b0-daf4b6d7337a/scratchpad".into()
    });
    let p = PathBuf::from(base).join(format!("review_wal_eng_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn page() -> usize {
    unsafe extern "C" {
        fn getpagesize() -> i32;
    }
    unsafe { getpagesize() as usize }
}

fn overlay(live: &Path, img: &Path, pages: std::ops::Range<usize>) {
    let ps = page();
    let cur = std::fs::read(live).unwrap();
    let mut out = std::fs::read(img).unwrap();
    for p in pages {
        out[p * ps..(p + 1) * ps].copy_from_slice(&cur[p * ps..(p + 1) * ps]);
    }
    std::fs::write(img, out).unwrap();
}

fn cp_r(from: &Path, to: &Path) {
    let st = std::process::Command::new("cp").arg("-R").arg(from).arg(to).status().unwrap();
    assert!(st.success());
}

/// consumer を、 次に oplog を写した所 (fsync の前、 lock を持たない) で止める。 返した Sender を drop すると放す。
/// 2 つ目は 「止まった」 の知らせ。
fn park_consumer_at_next_oplog_sync() -> (mpsc::Sender<()>, mpsc::Receiver<()>) {
    let (parked_tx, parked_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let hold = std::sync::Mutex::new(Some((parked_tx, release_rx)));
    crashsim::set_after_copy_hook(Some(Arc::new(move |what| {
        if what == Copied::Oplog && std::thread::current().name() == Some("enchudb-consumer") {
            let taken = hold.lock().unwrap().take();
            if let Some((parked, release)) = taken {
                let _ = parked.send(());
                let _ = release.recv();
            }
        }
    })));
    (release_tx, parked_rx)
}

fn new_db(db: &Path) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 65_536).unwrap();
    eng.define_table("t", 1_000).unwrap();
    eng.define_himo_in("t", "x", ValueType::Number, 0).unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    Engine::concurrentize_with_oplog(eng, CAP).unwrap()
}

/// (1) 暇な時にその場で畳んだ後 (header は書き出さない)、 書き始めた新しい周の page 1 だけが先に届いた像。
#[test]
fn acked_value_goes_back_after_an_unsynced_fold() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("fold");
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    crashsim::start();
    let eng = new_db(&db);
    let wal = eng.oplog().unwrap().clone();
    assert!(wal.two_halves());
    let e = eng.entity_in("t").unwrap();
    for v in 1..=5u32 {
        eng.tie_to(e, "t.x", v);
    }
    eng.oplog_sync().unwrap();
    for v in 6..=150u32 {
        eng.tie_to(e, "t.x", v);
    }
    eng.oplog_sync().unwrap(); // ← 150 は電源断の後も残る
    let lap0 = wal.lap();
    // consumer の周期 (100 ms) が暇な WAL をその場で畳むのを待つ (状態で待つ)
    let mut waited = 0;
    while !(wal.is_empty() && wal.lap() != lap0) {
        std::thread::sleep(std::time::Duration::from_millis(5));
        waited += 1;
        assert!(waited < 4000, "前提: consumer が暇な WAL を畳む");
    }
    // 以後 consumer の書き出しは届かない (fsync の前で止める = その瞬間に電源が落ちる)
    let (release, _parked) = park_consumer_at_next_oplog_sync();
    for v in 151..=400u32 {
        eng.tie_to(e, "t.x", v);
    }
    let img = root.join("img");
    crashsim::capture(&live, &img, Mode::Lost).unwrap();
    overlay(&db.join("oplog"), &img.join("db").join("oplog"), 1..2); // OS が oplog の page 1 だけ先に書いていた
    crashsim::stop();
    // 前提: 像の oplog の header は畳む前の周のまま
    let hdr = std::fs::read(img.join("db").join("oplog")).unwrap();
    let img_lap = u32::from_le_bytes(hdr[28..32].try_into().unwrap());
    let img_head = u64::from_le_bytes(hdr[8..16].try_into().unwrap());
    let img_cp = u64::from_le_bytes(hdr[16..24].try_into().unwrap());
    eprintln!("[fold] 像の oplog header: lap {img_lap} (畳む前 {lap0}) head {img_head} checkpoint {img_cp}");
    drop(release);
    crashsim::set_after_copy_hook(None);
    drop(eng);
    assert_eq!(img_lap, lap0, "前提: header の page は畳む前のまま");

    let got = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), CAP).map(|eng| eng.get(e, "t.x"));
    let _ = std::fs::remove_dir_all(&root);
    let got = got.unwrap();
    eprintln!("[fold] 開いた後の値 {got:?} (書き出しの返った値 150)");
    assert!(got.is_some_and(|v| v >= 150), "書き出しの返った 150 が {got:?} に戻った");
}

/// (2) 前の session の record が head の先に残った区画に同じ周のまま書き足し、 区画を移った (A の header の page だけ書き出す) 直後の像。
#[test]
fn acked_value_is_overwritten_by_a_previous_session_record() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch("session");
    let live1 = root.join("live1");
    std::fs::create_dir_all(&live1).unwrap();
    let db1 = live1.join("db");

    // ── session 1 ──
    crashsim::start();
    let (release, parked) = park_consumer_at_next_oplog_sync();
    let eng = new_db(&db1);
    let wal = eng.oplog().unwrap().clone();
    let e = eng.entity_in("t").unwrap();
    eng.tie_to(e, "t.x", 1u32);
    // consumer が Commit を打って書き出しに入り、 止まるのを待つ (以後 consumer は Commit も畳みもしない)
    parked.recv_timeout(std::time::Duration::from_secs(60)).expect("前提: consumer が書き出しで止まる");
    eng.oplog_sync().unwrap(); // header の head = h0 が届く
    let h0 = wal.head();
    for v in 1000..1200u32 {
        eng.tie_to(e, "t.x", v); // [Tie][Commit] の組 (書き出さない)
        eng.oplog_commit();
    }
    assert_eq!(wal.switches(), 0);
    let live2 = root.join("live2");
    crashsim::capture(&live1, &live2, Mode::Lost).unwrap();
    overlay(&db1.join("oplog"), &live2.join("db").join("oplog"), 1..CAP / 2 / page()); // 区画 A の page 1 から先は届いていた
    drop(release);
    crashsim::set_after_copy_hook(None);
    drop(eng);
    crashsim::stop();

    // ── session 2: 像を開いてすぐ書き続ける ──
    crashsim::start();
    let (release, parked) = park_consumer_at_next_oplog_sync();
    let db2 = live2.join("db");
    let eng = Engine::open_concurrent_with_oplog(db2.to_str().unwrap(), CAP).unwrap();
    let wal = eng.oplog().unwrap().clone();
    assert_eq!(eng.get(e, "t.x"), Some(1), "前提: session 1 の再生は書き出しの返った 1 まで");
    assert_eq!(wal.head(), h0, "前提: 開いた後の続きは h0 (同じ周) — 開く時に何も積まない");
    eng.tie_to(e, "t.x", 2000u32);
    parked.recv_timeout(std::time::Duration::from_secs(60)).expect("前提: consumer が書き出しで止まる"); // consumer の Commit で組になる
    for v in 2001..2099u32 {
        eng.tie_to(e, "t.x", v);
        eng.oplog_commit();
    }
    eng.tie_to(e, "t.x", 2099u32);
    eng.oplog_sync().unwrap(); // ← 2099 は電源断の後も残る
    eprintln!("[session] 書き出しの返った位置 {} (h0 から {} byte)", wal_pos_offset(wal.head()), wal_pos_offset(wal.head()) - wal_pos_offset(h0));
    let mut v = 2100u32;
    while wal.switches() == 0 {
        eng.tie_to(e, "t.x", v);
        eng.oplog_commit();
        v += 1;
    }
    for _ in 0..5 {
        eng.tie_to(e, "t.x", v);
        eng.oplog_commit();
        v += 1;
    }
    // 像: oplog は書き出しの済んだ page だけ。 本体は今の中身 (本体の page は全部 OS が書いていたとする。 開き直した file の
    // 本体は crashsim が全部は控えていないので Lost にできない)
    let lost = root.join("lost");
    crashsim::capture(&live2, &lost, Mode::Lost).unwrap();
    let img = root.join("img");
    std::fs::create_dir_all(&img).unwrap();
    cp_r(&db2, &img.join("db"));
    std::fs::copy(lost.join("db").join("oplog"), img.join("db").join("oplog")).unwrap();
    let live_now = eng.get(e, "t.x");
    drop(release);
    crashsim::set_after_copy_hook(None);
    drop(eng);
    crashsim::stop();

    let got = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), CAP).map(|eng| eng.get(e, "t.x"));
    let _ = std::fs::remove_dir_all(&root);
    let got = got.unwrap();
    eprintln!("[session] 開いた後の値 {got:?} (書き出しの返った値 2099、 落ちる直前の値 {live_now:?}、 session 1 が書いて届かなかった値は 1000..1200)");
    assert!(got.is_some_and(|v| v >= 2099), "書き出しの返った 2099 が、 前の session の record の値 {got:?} に戻った");
}
