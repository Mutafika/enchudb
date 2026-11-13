//! #796: きれいに閉じた DB を開く道は WAL を fsync しない (届ける record が無い)。 その時に WAL で変わっているのは、 この
//! session の周を置いた header だけ (#706)。 その header は、 最初の record を書く**前に** disk に届く (#707 の約束:
//! 周の header が届く前にその区画へ書かない。 最初の append が自分で header の page を書き出す)。
//!
//! 見る物: 書き出しの済んだ page だけの像 (`Mode::Lost`) の oplog の header の周。
//! - 開いた直後: この session の周ではない (= 開く道は WAL を書き出していない。 試験の前提)
//! - 最初の record を書いた後 (consumer の書き出しは止めてある): この session の周で、 record はまだ無い
//!
//! cargo test -p enchudb-engine --features crashsim --test issue796_open_without_wal_fsync -- --test-threads=1
#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::crashsim::{self, Copied, Mode};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
const CAP: usize = 128 * 1024;
const HALF: usize = CAP / 2;
/// 区画の header の中の周の位置 (`oplog.rs` の `OFF_LAP`)。
const OFF_LAP: usize = 28;
const V: u32 = 0x5E55_7960;

struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchu-issue796-{}-{tag}", std::process::id()));
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

fn find_value(file: &[u8], v: u32) -> Option<usize> {
    let pat = v.to_le_bytes();
    file.windows(4).position(|w| w == pat)
}

/// 像の oplog の 2 つの区画の header の周。
fn laps(image: &[u8]) -> [u32; 2] {
    [0, HALF].map(|b| u32::from_le_bytes(image[b + OFF_LAP..b + OFF_LAP + 4].try_into().unwrap()))
}

fn lost_oplog(live: &Path, out: &Path) -> Vec<u8> {
    let _ = std::fs::remove_dir_all(out);
    crashsim::capture(live, out, Mode::Lost).unwrap();
    std::fs::read(out.join("db").join("oplog")).unwrap()
}

/// consumer を、 次に oplog を写した所 (書き出しの前) で止める。 返した Sender を drop すると放す。
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

#[test]
fn the_session_header_reaches_the_disk_before_the_first_record_of_a_cleanly_closed_db() {
    let _s = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = Scratch::new("clean");
    let live = root.0.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    // session 1: 書いて、 きれいに閉じる
    let e = {
        let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 65_536).unwrap();
        eng.define_table("t", 1_000).unwrap();
        eng.define_himo_in("t", "x", ValueType::Number, 0).unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, CAP).unwrap();
        let e = eng.entity_in("t").unwrap();
        eng.tie_to(e, "t.x", 1u32);
        eng.oplog_sync().unwrap();
        e
    };

    // session 2: 開く
    crashsim::start();
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), CAP).unwrap();
    let wal = eng.oplog().unwrap().clone();
    assert_eq!(eng.get(e, "t.x"), Some(1));
    assert!(wal.two_halves(), "前提: 2 区画の形 (周の header を持つ)");
    assert!(wal.recover_with_tail().is_empty(), "前提: きれいに閉じた DB の WAL に、 当てる record は無い");
    let head0 = wal.head();
    // 開く道が header に置いた周 (file の今の中身にはある) は、 まだ disk に無い
    let now = std::fs::read(db.join("oplog")).unwrap();
    let img0 = lost_oplog(&live, &root.0.join("img0"));
    let placed = *laps(&now).iter().max().unwrap();
    assert!(
        !laps(&img0).contains(&placed),
        "前提: 開く道は WAL を書き出していない (書き出しの済んだ header の周 {:?}、 開く道が置いた周 {placed})",
        laps(&img0)
    );

    // 最初の record。 consumer の書き出し (WAL 全体の fsync) は止めておく = header を届けるのは書き手だけ
    let release = park_consumer_at_next_oplog_sync();
    eng.tie_to(e, "t.x", V);
    assert_ne!(wal.head(), head0);
    // record の入った区画の周 (開く道が置いた周か、 移る時に置き直した周)
    let lap = wal.lap();
    assert!(!laps(&img0).contains(&lap), "前提: record の入った区画の周 {lap} は、 開いた時には disk に無い");
    let now = std::fs::read(db.join("oplog")).unwrap();
    assert!(find_value(&now, V).is_some(), "前提: record は WAL に書いてある");
    let img1 = lost_oplog(&live, &root.0.join("img1"));
    assert!(
        laps(&img1).contains(&lap),
        "record を書いたのに、 その区画の周 {lap} の header が disk に届いていない (届いている周 {:?})",
        laps(&img1)
    );
    assert_eq!(find_value(&img1, V), None, "前提: record の page はまだ書き出していない (届いたのは header だけ)");
    drop(release);
    crashsim::set_after_copy_hook(None);
    drop(eng);
    drop(wal);
    crashsim::stop();

    // きれいに閉じた後、 もう一度開いても値はある (開く道が fsync しなくても、 閉じる時の書き出しが届けている)
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), CAP).unwrap();
    assert_eq!(eng.get(e, "t.x"), Some(V as u64));
}
