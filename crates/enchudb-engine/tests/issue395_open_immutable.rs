//! #395: 書き手の居ない DB を複数の読み手で開く `open_immutable`。 lock を取らず (読み手同士も書き手も待たせない)、
//! Leaf の借用の読みを #107 の番人で止めない。 宣言が嘘 (書き手が居る) なら、 開く時は error、 開いた後に来た書き手は
//! debug build の借用の読みが panic で知らせる。
//!
//! flock は open file description ごとなので、 同じ process の 2 つの Engine は別 process の 2 つと同じに振る舞う。

use enchudb_engine::{Engine, EntityValue, ValueType};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Duration;

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue395-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

/// Leaf 列 `body` に本文を張った DB を作って閉じる (= 作り終えて公開した DB)。
fn published(tag: &str) -> (String, u64) {
    let path = tmp(tag);
    let mut eng = Engine::create_growable_with_capacity(&path, 1_000).unwrap();
    eng.define_himo("body", ValueType::Leaf, 0);
    let e = eng.entity().unwrap();
    eng.tie_text(e, "body", "published text");
    eng.content(e, "memo", b"published memo");
    eng.flush().unwrap();
    drop(eng);
    (path, e)
}

/// 読み手が 2 つ同時に開けて、 どちらも Leaf を借用で読める (debug build で #107 の番人が止めない)。
#[test]
fn two_readers_borrow_leaf_values() {
    let (path, e) = published("two");
    let a = Engine::open_immutable(&path).unwrap();
    let b = Engine::open_immutable(&path).unwrap();
    assert!(a.is_immutable() && a.is_readonly());
    for eng in [&a, &b] {
        assert_eq!(eng.get_text(e, "body"), Some(&b"published text"[..]));
        assert_eq!(eng.get_content(e, "memo"), Some(&b"published memo"[..]));
        let fields = eng.get_entity(e);
        assert!(fields.iter().any(|(n, v)| *n == "body" && matches!(v, EntityValue::Text(t) if *t == b"published text")));
    }
    drop((a, b));
    let _ = std::fs::remove_dir_all(&path);
}

/// 読み手は lock を持たない: 開いている間も書き手は待たずに開ける。
#[test]
fn readers_do_not_block_a_writer() {
    let (path, _) = published("noblock");
    let r = Engine::open_immutable(&path).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let p = path.clone();
    let t = std::thread::spawn(move || {
        let w = Engine::open_standalone(&p).unwrap();
        tx.send(()).unwrap();
        drop(w);
    });
    assert!(rx.recv_timeout(Duration::from_secs(5)).is_ok(), "書き手が読み手の lock で待たされた");
    t.join().unwrap();
    drop(r);
    let _ = std::fs::remove_dir_all(&path);
}

/// 書き手が開いている DB は open_immutable で開けない (書き手と並べて読むのは open_readonly)。 書き手が閉じれば開ける。
#[test]
fn open_fails_while_a_writer_has_the_db() {
    let (path, _) = published("writer");
    let w = Engine::open_standalone(&path).unwrap();
    let err = Engine::open_immutable(&path).err().expect("書き手が居るのに開けた");
    assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
    assert!(Engine::open_readonly(&path).is_ok(), "open_readonly は書き手と並べて開ける");
    drop(w);
    assert!(Engine::open_immutable(&path).is_ok());
    let _ = std::fs::remove_dir_all(&path);
}

/// 書き込み API は open_readonly と同じく拒む。
#[test]
fn writes_are_rejected() {
    let (path, e) = published("write");
    let r = Engine::open_immutable(&path).unwrap();
    let panicked = catch_unwind(AssertUnwindSafe(|| r.tie_text_to(e, "body", "changed"))).is_err();
    assert!(panicked, "書き込みが通った");
    assert_eq!(r.get_text_owned(e, "body").as_deref(), Some(&b"published text"[..]));
    drop(r);
    let _ = std::fs::remove_dir_all(&path);
}

/// 宣言が嘘: 開いた後に書き手が来たら、 debug build の借用の読みが panic で知らせる。 書き手が閉じれば止めない。
#[cfg(debug_assertions)]
#[test]
fn a_writer_arriving_later_stops_borrowed_reads_in_debug() {
    let (path, e) = published("late");
    let r = Engine::open_immutable(&path).unwrap();
    assert!(r.get_text(e, "body").is_some());
    let w = Engine::open_standalone(&path).unwrap();
    let msg = match catch_unwind(AssertUnwindSafe(|| r.get_text(e, "body").map(<[u8]>::to_vec))) {
        Ok(_) => panic!("書き手が居るのに借用の読みが通った"),
        Err(p) => p.downcast_ref::<String>().cloned().unwrap_or_default(),
    };
    assert!(msg.contains("#395"), "番人でない panic: {msg}");
    // copy 版は止めない
    assert!(r.get_text_owned(e, "body").is_some());
    drop(w);
    assert!(r.get_text(e, "body").is_some());
    drop(r);
    let _ = std::fs::remove_dir_all(&path);
}
