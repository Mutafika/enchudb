//! β-heavy phase 1: positions sidecar の lazy migration / 永続化テスト。
//!
//! 確認したいこと:
//!   1. 新規 DB 作成で sidecar file が作られる
//!   2. sidecar 削除 → 再 open で sidecar が再生成され、 lazy rebuild で復元
//!   3. flush 後の sidecar には valid magic が打たれている (slot_initialized)

#![cfg(not(target_arch = "wasm32"))]

use enchudb_engine::{Engine, HimoType};

fn tmp(tag: &str) -> String {
    let p = format!(
        "/tmp/enchudb-pos-sidecar-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    for suf in ["", ".wal", ".crc", ".db.lock", ".tables", ".positions"] {
        let _ = std::fs::remove_file(format!("{}{}", p, suf));
    }
    p
}

fn cleanup(p: &str) {
    for suf in ["", ".wal", ".crc", ".db.lock", ".tables", ".positions"] {
        let _ = std::fs::remove_file(format!("{}{}", p, suf));
    }
}

#[test]
fn create_makes_sidecar_file() {
    let p = tmp("create_sidecar");
    {
        let mut eng = Engine::create_standalone(&p).unwrap();
        eng.define_himo("age", HimoType::Number, 100);
        let e = eng.entity();
        eng.tie(e, "age", 30);
        eng.flush().unwrap();
    }
    let side = format!("{}.positions", p);
    assert!(
        std::fs::metadata(&side).is_ok(),
        "sidecar file should exist after create + flush: {}",
        side
    );
    cleanup(&p);
}

#[test]
fn missing_sidecar_lazy_recreates() {
    let p = tmp("missing_sidecar");

    // 1. DB 作って data 入れる
    {
        let mut eng = Engine::create_standalone(&p).unwrap();
        eng.define_himo("age", HimoType::Number, 100);
        for i in 0..50u32 {
            let e = eng.entity();
            eng.tie(e, "age", i % 10);
        }
        eng.flush().unwrap();
    }

    // 2. sidecar だけ削除 (v5 DB を v0.5.5 engine で open する経路の simulation)
    std::fs::remove_file(format!("{}.positions", p)).unwrap();

    // 3. open → sidecar 再生成
    {
        let eng = Engine::open_standalone(&p).unwrap();
        // 全部読めるはず (lazy rebuild が column scan で復元)
        for v in 0..10u32 {
            let r = eng.pull_raw("age", v);
            assert_eq!(r.len(), 5, "value {}: expected 5 entities, got {}", v, r.len());
        }
    }

    cleanup(&p);
}

#[test]
fn sidecar_persists_across_reopen() {
    let p = tmp("sidecar_persist");

    // 1. DB 作って data 入れる
    {
        let mut eng = Engine::create_standalone(&p).unwrap();
        eng.define_himo("v", HimoType::Number, 50);
        let e1 = eng.entity();
        let e2 = eng.entity();
        eng.tie(e1, "v", 7);
        eng.tie(e2, "v", 7);
        // 一度 pull_raw して lazy rebuild を走らせる (= sidecar に書き込まれる)
        let _ = eng.pull_raw("v", 7);
        eng.flush().unwrap();
    }

    // 2. open: sidecar に valid magic があれば load される
    {
        let eng = Engine::open_standalone(&p).unwrap();
        let r = eng.pull_raw("v", 7);
        assert_eq!(r.len(), 2);
    }

    cleanup(&p);
}

#[test]
fn late_himo_with_sparse_eids() {
    // PositionsRegion::set の prepend-not-supported 制約に対応するため、
    // ensure_himo 時に init_with_offset(0) を使う方式の regression test。
    let p = tmp("late_sparse");

    {
        let mut eng = Engine::create_standalone(&p).unwrap();
        for _ in 0..1000u32 {
            let _ = eng.entity();
        }
        // 後付けで himo を追加し、 sparse な eid に tie
        eng.define_himo("flag", HimoType::Number, 5);
        eng.tie(500, "flag", 1);
        eng.tie(999, "flag", 1);
        eng.tie(0, "flag", 1); // ← 旧 init() だと panic していた経路
        let r = eng.pull_raw("flag", 1);
        assert_eq!(r.len(), 3);
        eng.flush().unwrap();
    }

    // reopen して値が読めることを確認
    {
        let eng = Engine::open_standalone(&p).unwrap();
        let r = eng.pull_raw("flag", 1);
        assert_eq!(r.len(), 3, "reopen 後も flag=1 で 3 entity 引けるはず");
    }
    cleanup(&p);
}
