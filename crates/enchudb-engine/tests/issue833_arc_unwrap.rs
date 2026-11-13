//! #833: 裏の thread (consumer / WAL の空きの待ち手 / 通知係) を持つ engine を `Arc` から取り出す (`Arc::try_unwrap` /
//! `Arc::into_inner`) と、 裏の thread は `Arc` の中の住所を生のポインタで指したままなので、 放された memory を読む
//! (直す前は SIGSEGV)。
//!
//! 根の直し (裏の thread が engine の住所によらない形) はまだ無い。 ここで見るのは 「黙って落ちず、 決まった知らせで止まる」:
//! - debug build: 取り出した engine の API を呼ぶと panic
//! - どちらの build も: 取り出した engine を落とす / `concurrentize_with_oplog` に渡し直すと、 知らせを出して止まる
//!
//! 止まり方は panic / abort なので、 子 process で回して終わり方と stderr を見る。
#![cfg(all(unix, not(target_arch = "wasm32")))]

use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Stdio};
use std::sync::Arc;

use enchudb_engine::{Engine, ValueType};

const CHILD_ENV: &str = "ENCHU_ISSUE833_CHILD";
const MODE_ENV: &str = "ENCHU_ISSUE833_MODE";
/// engine が出す知らせの一部
const NOTICE: &str = "moved out of its Arc";
const SIGSEGV: i32 = 11;
const SIGBUS: i32 = 10;

fn tmp(tag: &str) -> String {
    let p = format!("{}/enchudb-issue833-{}-{}", std::env::temp_dir().display(), tag, std::process::id());
    let _ = std::fs::remove_dir_all(&p);
    p
}

/// 放された `Arc` の置き場を、 同じ大きさの確保で踏む (中身は 0x01 の並び = ポインタとして読むと飛ぶ)。 直しが無い時に、
/// consumer の次の周期で必ず落ちるようにする。
fn stomp_freed_arc() -> Vec<Vec<u8>> {
    let size = std::mem::size_of::<Engine>() + 16;
    (0..64).map(|_| vec![0x01u8; size]).collect()
}

#[test]
fn issue833_child() {
    let Ok(path) = std::env::var(CHILD_ENV) else { return };
    let mode = std::env::var(MODE_ENV).unwrap_or_default();
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("age", ValueType::Number, 100);
    assert!(!eng.has_background_threads());
    let eng = Engine::concurrentize_with_oplog(eng, 1 << 20).unwrap();
    assert!(eng.has_background_threads());
    let e = eng.entity().unwrap();
    eng.tie_to(e, "age", 30);
    // consumer の最初の周期が終わって眠るのを待つ (取り出した直後に consumer が前の住所を読む窓を避ける)
    std::thread::sleep(std::time::Duration::from_millis(30));
    let take = |eng: Arc<Engine>| -> Engine {
        if mode.starts_with("into_inner") { Arc::into_inner(eng).unwrap() } else { Arc::try_unwrap(eng).ok().unwrap() }
    };
    match mode.as_str() {
        // 対照: 取り出さない。 `Arc::get_mut` は住所を動かさないので知らせは出ない
        "keep" => {
            let mut eng = eng;
            assert!(Arc::get_mut(&mut eng).is_some());
            std::thread::sleep(std::time::Duration::from_millis(350));
            eng.tie_to(e, "age", 31);
            eng.commit();
            eng.oplog_sync().unwrap();
            assert_eq!(eng.get(e, "age"), Some(31));
            assert_eq!(eng.pull_raw("age", 31u32).len(), 1);
            drop(eng);
            let back = Engine::open_standalone(&path).unwrap();
            assert_eq!(back.get(e, "age"), Some(31));
            eprintln!("ISSUE833_OK");
        }
        // 取り出して別の置き場へ動かす → 書く → consumer の周期を待つ
        "unwrap_use" | "into_inner_use" => {
            let moved = Box::new(take(eng));
            let _stomp = stomp_freed_arc();
            for i in 0..40u32 {
                moved.tie_to(e, "age", 32 + i);
                moved.commit();
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            eprintln!("ISSUE833_SURVIVED");
        }
        // 取り出して読むだけ
        "unwrap_read" => {
            let moved = Box::new(take(eng));
            let _stomp = stomp_freed_arc();
            let _ = moved.get(e, "age");
            std::thread::sleep(std::time::Duration::from_millis(400));
            eprintln!("ISSUE833_SURVIVED");
        }
        // 取り出してすぐ落とす
        "unwrap_drop" | "into_inner_drop" => {
            let moved = Box::new(take(eng));
            let _stomp = stomp_freed_arc();
            drop(moved);
            std::thread::sleep(std::time::Duration::from_millis(400));
            eprintln!("ISSUE833_SURVIVED");
        }
        // 取り出して `concurrentize_with_oplog` に渡し直す (前の consumer は前の住所を指したまま、 2 本目が起きる)
        "unwrap_again" => {
            let moved = take(eng);
            let _stomp = stomp_freed_arc();
            let again = Engine::concurrentize_with_oplog(moved, 1 << 20).unwrap();
            std::thread::sleep(std::time::Duration::from_millis(400));
            drop(again);
            eprintln!("ISSUE833_SURVIVED");
        }
        other => panic!("mode {other}"),
    }
}

fn run(mode: &str) -> (std::process::ExitStatus, String) {
    let db = tmp(mode);
    let out = Command::new(std::env::current_exe().unwrap())
        .args(["issue833_child", "--exact", "--test-threads=1", "--nocapture"])
        .env(CHILD_ENV, &db)
        .env(MODE_ENV, mode)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("spawn child");
    let _ = std::fs::remove_dir_all(&db);
    (out.status, String::from_utf8_lossy(&out.stderr).into_owned())
}

fn tail(s: &str) -> String {
    let v: Vec<&str> = s.lines().rev().take(8).collect();
    v.into_iter().rev().collect::<Vec<_>>().join(" | ")
}

/// 取り出した engine は、 黙って落ちず (SIGSEGV / SIGBUS でなく)、 生き延びもせず、 知らせを出して止まる。
fn stops_with_the_notice(mode: &str) {
    let (st, err) = run(mode);
    assert!(
        !matches!(st.signal(), Some(SIGSEGV) | Some(SIGBUS)),
        "{mode}: 子が signal {:?} で落ちた (知らせなし): {}",
        st.signal(),
        tail(&err)
    );
    assert!(!st.success() && !err.contains("ISSUE833_SURVIVED"), "{mode}: 取り出した engine がそのまま動いた ({st:?}): {}", tail(&err));
    assert!(err.contains(NOTICE), "{mode}: 知らせが出ていない ({st:?}): {}", tail(&err));
}

#[test]
fn control_keeping_the_arc_is_fine() {
    let (st, err) = run("keep");
    assert!(st.success() && err.contains("ISSUE833_OK"), "対照が落ちた ({st:?}): {}", tail(&err));
    assert!(!err.contains(NOTICE), "取り出していないのに知らせが出た: {}", tail(&err));
}

/// debug build だけ: 取り出した engine の API (書き込み / 読み) を呼ぶと panic。 release build は書き込みの道で見ない。
#[cfg(debug_assertions)]
#[test]
fn using_an_unwrapped_engine_panics_in_debug() {
    for mode in ["unwrap_use", "into_inner_use", "unwrap_read"] {
        stops_with_the_notice(mode);
    }
}

#[test]
fn dropping_an_unwrapped_engine_stops_with_the_notice() {
    for mode in ["unwrap_drop", "into_inner_drop"] {
        stops_with_the_notice(mode);
    }
}

#[test]
fn concurrentizing_an_unwrapped_engine_stops_with_the_notice() {
    stops_with_the_notice("unwrap_again");
}
