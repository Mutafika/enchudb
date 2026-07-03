//! #80: 同一プロセスで writer を二重 open したときの挙動。
//!
//! 修正後の仕様:
//! 1. 同一プロセスの 2 回目の writer open は flock で block せず、
//!    `ErrorKind::WouldBlock` + 説明的メッセージで **即エラー**
//! 2. 1 本目を drop すれば同じ path を再 open できる
//! 3. readonly open は writer lock を取らないので併存できる
//!
//! 別プロセス writer との排他 (blocking flock、 sqlite 互換) は変更しない。

use enchudb::Engine;
use std::time::{Duration, Instant};

fn tmp(name: &str) -> String {
    let p = format!("/tmp/enchudb-issue80-{}-{}", name, std::process::id());
    for suffix in ["", ".oplog", ".lock", ".crc", ".tables", ".eidmap"] {
        let _ = std::fs::remove_file(format!("{}{}", p, suffix));
    }
    p
}

fn assert_fast_fail(res: std::io::Result<impl Sized>, started: Instant) {
    let err = match res {
        Ok(_) => panic!("2nd writer open は成功してはいけない"),
        Err(e) => e,
    };
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::WouldBlock,
        "kind は WouldBlock: {err}"
    );
    assert!(
        err.to_string().contains("already open for writing in this process"),
        "説明的メッセージを含む: {err}"
    );
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "block せず即返る (elapsed = {:?})",
        started.elapsed()
    );
}

#[test]
fn second_writer_open_fails_fast_in_same_process() {
    let path = tmp("standalone");
    let eng1 = Engine::create_standalone(&path).expect("create");

    // (1) 2 回目は block ではなく即エラー
    let t = Instant::now();
    assert_fast_fail(Engine::open_standalone(&path), t);

    // (2) 1 本目を drop すれば再 open できる
    drop(eng1);
    let eng2 = Engine::open_standalone(&path).expect("drop 後の open は成功");
    drop(eng2);
}

#[test]
fn second_concurrent_oplog_open_fails_fast_in_same_process() {
    let path = tmp("oplog");
    drop(Engine::create_standalone(&path).expect("create"));

    let eng1 = Engine::open_concurrent_with_oplog(&path, 1 << 20).expect("1st open");

    let t = Instant::now();
    assert_fast_fail(Engine::open_concurrent_with_oplog(&path, 1 << 20), t);

    drop(eng1);
    let eng2 = Engine::open_concurrent_with_oplog(&path, 1 << 20).expect("drop 後の open は成功");
    drop(eng2);
}

#[test]
fn readonly_open_coexists_with_writer() {
    let path = tmp("readonly");
    let mut eng1 = Engine::create_standalone(&path).expect("create");
    // 反映のため一度 flush (create 直後でも open_readonly できる状態にする)
    let _ = eng1.flush();

    // readonly は writer lock を取らないので即成功する
    let t = Instant::now();
    let ro = Engine::open_readonly(&path);
    assert!(ro.is_ok(), "readonly 併存は許可: {:?}", ro.err());
    assert!(t.elapsed() < Duration::from_millis(500), "readonly は block しない");
    drop(ro);
    drop(eng1);
}

#[test]
fn different_paths_do_not_interfere() {
    let p1 = tmp("multi-a");
    let p2 = tmp("multi-b");
    let a = Engine::create_standalone(&p1).expect("create a");
    let b = Engine::create_standalone(&p2).expect("create b (別 path は干渉しない)");
    drop(a);
    drop(b);
}
