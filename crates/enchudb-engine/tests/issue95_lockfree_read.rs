//! #95 regression: LockFreeCylinder による lock-free read の統合検証。
//!
//! (1) consumer が tie_async を apply し続ける最中に pull_raw を並行しても
//!     壊れない・crash しない（旧 RwLock 相当の相互排他を撤去した後の安全性）。
//! (2) 値更新で生じた stale entry が、 read 側の Column verify で正しく落ちる
//!     （append-only + lazy verify）。

use enchudb_engine::{Engine, ValueType};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

fn fresh(path: &str) {
    for suf in ["", ".oplog", ".lock"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

fn define(eng: &Arc<Engine>, name: &str) -> u16 {
    // build phase 相当: Arc 単一所有のうちに define（queue_backpressure と同 idiom）
    let eng_mut = unsafe { &mut *(Arc::as_ptr(eng) as *mut Engine) };
    eng_mut.define_himo(name, ValueType::Number, 0);
    eng.himo_id(name).unwrap() as u16
}

/// (1) 並行 read が write と同時に走っても crash / 破損しない。
#[test]
fn concurrent_pull_during_writes() {
    let path = "/tmp/test_issue95_concurrent.db";
    fresh(path);
    let eng: Arc<Engine> =
        Engine::create_concurrent_with_oplog(path, 16 * 1024 * 1024).expect("create");
    let vhid = define(&eng, "v");

    let stop = Arc::new(AtomicBool::new(false));
    let bad = Arc::new(AtomicU64::new(0));
    let n = 50_000u32;

    // reader x3: value 7 を引き続け、 返った eid が全部 valid（< n の範囲）か軽く検査
    let readers: Vec<_> = (0..3)
        .map(|_| {
            let eng = eng.clone();
            let stop = stop.clone();
            let bad = bad.clone();
            std::thread::spawn(move || {
                let mut prev = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    let hits = eng.pull_raw("v", 7);
                    // append-only なので value 7 の件数は単調非減少
                    if hits.len() + 100 < prev {
                        bad.fetch_add(1, Ordering::Relaxed);
                    }
                    prev = hits.len();
                    // eid は 0..n の範囲（local id）
                    for e in &hits {
                        if enchudb_oplog::eid_local(*e) >= n {
                            bad.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();

    // writer: value 7 を大量 tie_async
    for _ in 0..n {
        let e = eng.entity();
        eng.tie_async_by_id(e, vhid, 7);
    }
    eng.flush_writes();
    stop.store(true, Ordering::Relaxed);
    for r in readers {
        r.join().unwrap();
    }

    assert_eq!(bad.load(Ordering::Relaxed), 0, "並行 read で破損を観測");
    assert_eq!(
        eng.pull_raw("v", 7).len(),
        n as usize,
        "全 apply 後の件数が合わない"
    );
    fresh(path);
}

/// (2) 値更新の stale が Column verify で落ちる。
#[test]
fn update_stale_filtered_by_verify() {
    let path = "/tmp/test_issue95_verify.db";
    fresh(path);
    let eng: Arc<Engine> =
        Engine::create_concurrent_with_oplog(path, 8 * 1024 * 1024).expect("create");
    let vhid = define(&eng, "v");

    let e = eng.entity();
    eng.tie_async_by_id(e, vhid, 7);
    eng.flush_writes();
    assert_eq!(eng.pull_raw("v", 7).len(), 1);

    // e を value 8 に更新 → bucket 7 に stale が残る
    eng.tie_async_by_id(e, vhid, 8);
    eng.flush_writes();

    // bucket 7 の stale は verify で落ちる、 bucket 8 に現れる
    assert_eq!(eng.pull_raw("v", 7).len(), 0, "stale が verify で落ちてない");
    assert_eq!(eng.pull_raw("v", 8).len(), 1, "更新後の値が引けない");

    // 別 entity を 7 に足す → 7 が復活（stale と混ざらない）
    let e2 = eng.entity();
    eng.tie_async_by_id(e2, vhid, 7);
    eng.flush_writes();
    let got = eng.pull_raw("v", 7);
    assert_eq!(got.len(), 1);
    assert_eq!(enchudb_oplog::eid_local(got[0]), enchudb_oplog::eid_local(e2));
    fresh(path);
}
