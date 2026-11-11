//! #706 (engine、 版数の列の無い既定の DB): 前の session が書いて header に届かなかった record が head の先に残る区画に、
//! 開いた session が同じ周のまま書き足すと、 区画を移った (埋めた区画の header の page だけを書き出す) 直後の像で、 再生が
//! `oplog_sync` の返った record の後ろに前の session の record を当てた (値が前の session の値に戻る)。
//! oplog 単体は `crates/enchudb-oplog/tests/issue706_previous_session_records.rs`。
//!
//! cargo test -p enchudb-engine --features crashsim --test issue706_previous_session_records -- --test-threads=1

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::crashsim::{self, Copied, Mode};
use enchudb_oplog::oplog::wal_pos_offset;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
const CAP: usize = 128 * 1024;
const HALF: usize = CAP / 2;
/// session ごとの値の土台 (像の中を 4 byte で探せる値)。
const S1: u32 = 0x5E55_1000;
const S2: u32 = 0x5E55_2000;

/// 一時の置き場 (落ちても消す)。
struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchu-issue706-engine-{}-{tag}", std::process::id()));
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

fn cp_r(from: &Path, to: &Path) {
    let st = std::process::Command::new("cp").arg("-R").arg(from).arg(to).status().unwrap();
    assert!(st.success());
}

/// file の中で値 `v` (4 byte) が載っている位置。
fn find_value(file: &[u8], v: u32) -> Option<usize> {
    let pat = v.to_le_bytes();
    file.windows(4).position(|w| w == pat)
}

/// consumer を、 次に oplog を写した所 (書き出しの前、 lock を持たない) で止める。 返した Sender を drop すると放す。
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

#[test]
fn an_acked_value_is_not_overwritten_by_a_record_of_a_previous_session() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = Scratch::new("session");
    let live1 = root.0.join("live1");
    std::fs::create_dir_all(&live1).unwrap();
    let db1 = live1.join("db");
    let ps = page();

    // ── session 1: 1 を書き出した後、 同じ cell を書き出さずに 200 回書き換えて電源断 ──
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
    for k in 0..200u32 {
        eng.tie_to(e, "t.x", S1 + k); // [Tie][Commit] の組 (書き出さない)
        eng.oplog_commit();
    }
    assert_eq!(wal.switches(), 0);
    let live2 = root.0.join("live2");
    crashsim::capture(&live1, &live2, Mode::Lost).unwrap();
    // 区画 A の page 1 から先は届いていた (header の page は前の書き出しのまま)
    let now = std::fs::read(db1.join("oplog")).unwrap();
    let img2 = live2.join("db").join("oplog");
    let mut image = std::fs::read(&img2).unwrap();
    image[ps..HALF].copy_from_slice(&now[ps..HALF]);
    std::fs::write(&img2, &image).unwrap();
    drop(release);
    crashsim::set_after_copy_hook(None);
    drop(eng);
    crashsim::stop();
    // 前提: session 1 の record は head の先に (header に届かないまま) 残っている
    let at = find_value(&image[..HALF], S1 + 199).expect("前提: session 1 の最後の record が像にある");
    assert!(at > wal_pos_offset(h0) as usize);

    // ── session 2: 像を開いてすぐ (consumer が畳む前に) 書き続ける ──
    crashsim::start();
    let (release, parked) = park_consumer_at_next_oplog_sync();
    let db2 = live2.join("db");
    let eng = Engine::open_concurrent_with_oplog(db2.to_str().unwrap(), CAP).unwrap();
    let wal = eng.oplog().unwrap().clone();
    assert_eq!(eng.get(e, "t.x"), Some(1), "前提: session 1 の再生は書き出しの返った 1 まで");
    assert_eq!(wal.head(), h0, "前提: 開いた時の続きは h0 — 開く時に何も積まない");
    eng.tie_to(e, "t.x", S2);
    parked.recv_timeout(std::time::Duration::from_secs(60)).expect("前提: consumer が書き出しで止まる"); // consumer の Commit で組になる
    for k in 1..99u32 {
        eng.tie_to(e, "t.x", S2 + k);
        eng.oplog_commit();
    }
    eng.tie_to(e, "t.x", S2 + 99);
    eng.oplog_sync().unwrap(); // ← S2 + 99 は電源断の後も残る
    let acked = S2 + 99;
    // 区画を移るまで書く (移る時に、 埋めた区画の header の page だけを書き出す)。 その後に少し。 もう移れない形 (もう一方に
    // 畳めていない record がある。 consumer は止めてある) なら、 今の区画が埋まる手前で止める
    let switches = wal.switches();
    let mut k = 100u32;
    while wal.switches() == switches && wal.free_bytes() > 4096 {
        eng.tie_to(e, "t.x", S2 + k);
        eng.oplog_commit();
        k += 1;
    }
    for _ in 0..5 {
        if wal.free_bytes() <= 4096 {
            break;
        }
        eng.tie_to(e, "t.x", S2 + k);
        eng.oplog_commit();
        k += 1;
    }
    // 像: oplog は書き出しの済んだ page だけ。 本体は今の中身 (本体の page は全部 OS が書いていたとする。 開き直した file の
    // 本体は crashsim が全部は控えていないので Lost にできない)
    let lost = root.0.join("lost");
    crashsim::capture(&live2, &lost, Mode::Lost).unwrap();
    let img = root.0.join("img");
    std::fs::create_dir_all(&img).unwrap();
    cp_r(&db2, &img.join("db"));
    std::fs::copy(lost.join("db").join("oplog"), img.join("db").join("oplog")).unwrap();
    let live_now = eng.get(e, "t.x");
    drop(release);
    crashsim::set_after_copy_hook(None);
    drop(eng);
    crashsim::stop();
    // 前提: 像の oplog には session 1 の record がまだ残っている (消えたから読まれない、 ではない)
    let image = std::fs::read(img.join("db").join("oplog")).unwrap();
    let at = find_value(&image[..HALF], S1 + 199).expect("前提: session 1 の record が像に残っている");
    assert!(at > wal_pos_offset(h0) as usize);

    let got = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), CAP).map(|eng| eng.get(e, "t.x"));
    let got = got.unwrap();
    eprintln!(
        "[session] 開いた後の値 {:x?} (書き出しの返った値 {acked:x}、 落ちる直前の値 {:x?}、 session 1 が書いて届かなかった値は {S1:x}..{:x})",
        got,
        live_now,
        S1 + 200
    );
    assert!(got.is_some_and(|v| v >= acked as u64), "書き出しの返った {acked:x} が、 前の session の record の値 {got:x?} に戻った");
}
