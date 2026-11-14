//! #880: WAL 付きで書いて落ちた DB を単独 (`Engine::open_standalone` = schema の `Database::open`) で開いても、 WAL 付きで開いた時と
//! 同じ中身になる (先に WAL 付きの open で再生して閉じてから開き直す)。
//!
//! 前は単独の open が WAL を見ず:
//! - A: WAL の fsync の後・本体の列を書き出す前に落ちた像で、 WAL にだけある値を黙って失った
//! - B: #857 の止め (消した行が sidecar より先、 checkpoint は止まったまま) の像で、 消した行の eid をもう一度払い出した

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::engine::write_out_hook::{self, Phase};
use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::crashsim::{self, Mode};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// crashsim の控えと書き出しの hook は process に 1 つなので、 試験を 1 本ずつ回す。
static SERIAL: Mutex<()> = Mutex::new(());

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn scratch(tag: &str) -> Scratch {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let p = std::env::temp_dir().join(format!("enchudb-issue880-{tag}-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(p.join("live")).unwrap();
    Scratch(p)
}

fn copy(from: &Path, to: &Path) {
    assert!(std::process::Command::new("cp").arg("-R").arg(from).arg(to).status().unwrap().success());
}

/// sync しない DB、 表 `t` (列 a)。
fn engine(db: &Path) -> (Arc<Engine>, u16) {
    let mut eng = Engine::create_growable_with_capacity(db.to_str().unwrap(), 1 << 16).unwrap();
    eng.define_table("t", 100).unwrap();
    eng.define_himo_in("t", "a", ValueType::Number, 0).unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, 4 << 20).unwrap();
    let a = eng.himo_id("t.a").unwrap() as u16;
    (eng, a)
}

/// 像を写して 2 通りで開き、 (行 x の値, 次に払い出す eid) を返す。
fn open_both(image: &Path, x: u64, a: u16) -> ((Option<u64>, u64), (Option<u64>, u64)) {
    let other = image.with_extension("concurrent");
    copy(image, &other);
    let mut s = Engine::open_standalone(image.join("db").to_str().unwrap()).unwrap();
    let standalone = (s.get_by_id(x, a), s.entity_in("t").unwrap());
    s.flush().unwrap();
    drop(s);
    let c = Engine::open_concurrent_with_oplog(other.join("db").to_str().unwrap(), 4 << 20).unwrap();
    let concurrent = (c.get_by_id(x, a), c.entity_in("t").unwrap());
    (standalone, concurrent)
}

/// A + C: WAL にだけある値を、 単独の open も見る。 その後に単独で書いた値は、 次の WAL 付きの open で古い record に戻らない。
#[test]
fn a_value_only_in_the_wal_survives_a_standalone_open() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let s = scratch("walonly");
    let live = s.0.join("live");
    crashsim::start();
    let (eng, a) = engine(&live.join("db"));
    let hold = eng.hold_consumer();
    let x = eng.entity_in("t").unwrap();
    eng.tie_to_by_id(x, a, 1u32);
    eng.oplog_sync().unwrap();
    eng.tie_to_by_id(x, a, 2u32);
    // 次の書き出しの、 WAL を fsync した後・本体の列を書き出す前の像
    let image = s.0.join("img");
    let shot = Arc::new(AtomicBool::new(false));
    {
        let (live, image, shot) = (live.clone(), image.clone(), shot.clone());
        write_out_hook::set(Some(Arc::new(move |p: Phase| {
            if p == Phase::BeforeGate && !shot.swap(true, Ordering::AcqRel) {
                crashsim::capture(&live, &image, Mode::Lost).unwrap();
            }
        })));
    }
    eng.oplog_sync().unwrap();
    write_out_hook::set(None);
    drop(hold);
    drop(eng);
    crashsim::stop();
    assert!(shot.load(Ordering::Acquire), "前提: 像を撮った");
    let c_image = s.0.join("img_c");
    copy(&image, &c_image);
    let (standalone, concurrent) = open_both(&image, x, a);
    assert_eq!(concurrent.0, Some(2), "前提: WAL 付きの open は WAL にだけある 2 を再生する");
    assert_eq!(standalone.0, Some(2), "単独の open が WAL にだけある値を失った");
    // C: 単独で開いて 5 を書いて閉じ、 その後 WAL 付きで開く
    {
        let mut st = Engine::open_standalone(c_image.join("db").to_str().unwrap()).unwrap();
        assert_eq!(st.get_by_id(x, a), Some(2));
        st.tie(x, "t.a", 5u32);
        st.flush().unwrap();
    }
    let c = Engine::open_concurrent_with_oplog(c_image.join("db").to_str().unwrap(), 4 << 20).unwrap();
    assert_eq!(c.get_by_id(x, a), Some(5), "単独で書いた値が、 次の WAL 付きの open で戻った");
}

/// B: #857 の止めで残した Delete を、 単独の open も再生する (消した行の eid をもう一度払い出さない)。
#[test]
fn a_held_delete_is_replayed_by_a_standalone_open() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let s = scratch("held");
    let live = s.0.join("live");
    let (eng, a) = engine(&live.join("db"));
    let hold = eng.hold_consumer();
    let x = eng.entity_in("t").unwrap();
    eng.tie_to_by_id(x, a, 1u32);
    eng.delete(x);
    eng.oplog_sync().unwrap();
    let image = s.0.join("img");
    copy(&live, &image);
    drop(hold);
    drop(eng);
    let (standalone, concurrent) = open_both(&image, x, a);
    assert_ne!(concurrent.1, x, "前提: WAL 付きの open は消した行の eid を払い出さない");
    assert_eq!(standalone, concurrent, "単独の open が WAL 付きの open と違う (消した行 {x:#x})");
}

/// きれいに閉じた DB は、 単独の open が WAL 付きの open を通らない (WAL が空)。 中身も今までどおり。
#[test]
fn a_cleanly_closed_db_opens_standalone_as_before() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let s = scratch("clean");
    let db = s.0.join("live").join("db");
    let x = {
        let (eng, a) = engine(&db);
        let x = eng.entity_in("t").unwrap();
        eng.tie_to_by_id(x, a, 3u32);
        x
    };
    let oplog = db.join("oplog");
    let before = std::fs::metadata(&oplog).unwrap().modified().unwrap();
    let st = Engine::open_standalone(db.to_str().unwrap()).unwrap();
    assert_eq!(st.get_by_id(x, st.himo_id("t.a").unwrap() as u16), Some(3));
    drop(st);
    assert_eq!(std::fs::metadata(&oplog).unwrap().modified().unwrap(), before, "きれいに閉じた DB で WAL 付きの open を通った (WAL が書かれた)");
}

/// 再生して閉じても WAL に record が残る時 (`tables` を書けず #870 の止めで checkpoint を進められない) は、 単独の open は断る
/// (黙って WAL を見ずに開かない)。 書けるように戻せば開ける。
#[test]
fn a_standalone_open_refuses_when_the_wal_cannot_be_written_out() {
    let _g = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let s = scratch("refuse");
    let live = s.0.join("live");
    let (eng, a) = engine(&live.join("db"));
    let hold = eng.hold_consumer();
    let x = eng.entity_in("t").unwrap();
    eng.tie_to_by_id(x, a, 1u32);
    eng.delete(x);
    eng.oplog_sync().unwrap();
    let image = s.0.join("img");
    copy(&live, &image);
    drop(hold);
    drop(eng);
    // `tables` を書けない (tmp の置き場に directory)
    let db = image.join("db");
    std::fs::create_dir(db.join("tables.tmp")).unwrap();
    match Engine::open_standalone(db.to_str().unwrap()) {
        Err(e) => assert!(e.to_string().contains("#880"), "断った理由: {e}"),
        Ok(_) => panic!("WAL を書き出せないのに単独で開いた"),
    }
    std::fs::remove_dir(db.join("tables.tmp")).unwrap();
    let mut st = Engine::open_standalone(db.to_str().unwrap()).expect("書けるように戻したのに開けない");
    assert_ne!(st.entity_in("t").unwrap(), x);
}
