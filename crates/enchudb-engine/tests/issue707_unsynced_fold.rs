//! #707 (engine、 版数の列の無い既定の DB): 暇な WAL を consumer がその場で畳んだ後に書き始め、 新しい周の record の page だけが
//! 先に届いて電源断。 header が畳む前のままだと、 開く時の再生が前の周の record を途中まで当て直し、 `oplog_sync` の返った値が
//! それより古い値に戻った。 oplog 単体は `crates/enchudb-oplog/tests/issue707_unsynced_fold.rs`。
//!
//! cargo test -p enchudb-engine --features crashsim --test issue707_unsynced_fold -- --test-threads=1

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::crashsim::{self, Copied, Mode};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
const CAP: usize = 128 * 1024;

/// 一時の置き場 (落ちても消す)。
struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchu-issue707-engine-{}-{tag}", std::process::id()));
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

/// 区画 A の header の (周, head, checkpoint)。
fn header(file: &[u8]) -> (u32, u64, u64) {
    let r64 = |o: usize| u64::from_le_bytes(file[o..o + 8].try_into().unwrap());
    (u32::from_le_bytes(file[28..32].try_into().unwrap()), r64(8), r64(16))
}

/// 今電源が落ちたら disk に残る oplog (書き出しの済んだ page だけ)。
fn oplog_on_disk(live: &Path, root: &Path, tag: &str) -> Vec<u8> {
    let dir = root.join(tag);
    crashsim::capture(live, &dir, Mode::Lost).unwrap();
    std::fs::read(dir.join("db").join("oplog")).unwrap()
}

/// consumer を、 次に oplog を写した所 (書き出しの前、 lock を持たない) で止める。 返した Sender を drop すると放す。
fn park_consumer_at_next_oplog_sync() -> mpsc::Sender<()> {
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let hold = std::sync::Mutex::new(Some(release_rx));
    crashsim::set_after_copy_hook(Some(Arc::new(move |what| {
        if what == Copied::Oplog && std::thread::current().name() == Some("enchudb-consumer") {
            let taken = hold.lock().unwrap().take();
            if let Some(release) = taken {
                let _ = release.recv();
            }
        }
    })));
    release_tx
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
fn an_acked_value_does_not_go_back_after_a_fold_whose_header_was_not_written() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = Scratch::new("fold");
    let live = root.0.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    crashsim::start();
    let eng = new_db(&db);
    let wal = eng.oplog().unwrap().clone();
    assert!(wal.two_halves());
    let ps = page();
    // 同じ cell を書き換え続ける
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
    let mut before = oplog_on_disk(&live, &root.0, "before");
    let mut waited = 0;
    while !(wal.is_empty() && wal.lap() != lap0) {
        before = oplog_on_disk(&live, &root.0, "before");
        std::thread::sleep(std::time::Duration::from_millis(2));
        waited += 1;
        assert!(waited < 10_000, "前提: consumer が暇な WAL を畳む");
    }
    let lap1 = wal.lap();
    // 畳む前に disk にあった header (畳みを見る前に撮った最後の像): 周は前のまま、 checkpoint は head より手前でありうる
    let (b_lap, b_head, b_cp) = header(&before);
    assert_eq!(b_lap, lap0, "前提: 畳む前の像の header は前の周");
    assert!(b_head as usize > ps + 1024, "前提: 畳む前の record は page 1 の中まである (head {b_head})");
    assert!((b_cp as usize) < ps, "前提: disk の checkpoint は page 0 の中 = 像の header が前の周なら、 再生は page 0 の record を当て直す ({b_cp})");
    // 以後 consumer の書き出しは届かない (書き出しの前で止める = その瞬間に電源が落ちる)
    let release = park_consumer_at_next_oplog_sync();
    for v in 151..=400u32 {
        eng.tie_to(e, "t.x", v);
    }
    let img = root.0.join("img");
    crashsim::capture(&live, &img, Mode::Lost).unwrap();
    crashsim::stop();
    // OS が oplog の page 1 だけ先に書いていた
    let now = std::fs::read(db.join("oplog")).unwrap();
    let img_oplog = img.join("db").join("oplog");
    let mut image = std::fs::read(&img_oplog).unwrap();
    image[ps..2 * ps].copy_from_slice(&now[ps..2 * ps]);
    std::fs::write(&img_oplog, &image).unwrap();
    drop(release);
    crashsim::set_after_copy_hook(None);
    drop(eng);

    // 像の形: page 1 は新しい周の record (畳む前と違う)、 header は 「畳む前の周」 か 「畳んだ後の周」
    assert_ne!(image[ps..2 * ps], before[ps..2 * ps], "前提: page 1 は新しい周の record で上書きされている");
    let (img_lap, img_head, img_cp) = header(&image);
    eprintln!("[fold] 像の oplog header: 周 {img_lap} head {img_head} checkpoint {img_cp} (畳む前: 周 {lap0} head {b_head} checkpoint {b_cp}、 畳んだ後の周 {lap1})");
    assert!(img_lap == lap0 || img_lap == lap1, "前提: header の page は畳む前の周か、 畳んだ後の周");

    let got = Engine::open_concurrent_with_oplog(img.join("db").to_str().unwrap(), CAP).map(|eng| eng.get(e, "t.x"));
    let got = got.unwrap();
    eprintln!("[fold] 開いた後の値 {got:?} (書き出しの返った値 150)");
    assert!(got.is_some_and(|v| v >= 150), "書き出しの返った 150 が {got:?} に戻った");
    assert_eq!(img_lap, lap1, "新しい周の record を書く前に、 畳んだ header が disk に届いていること");
}
