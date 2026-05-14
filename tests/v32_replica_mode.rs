//! v32: Read-only replica モードのテスト。
//!
//! replica として開いた Engine は書き込み API が panic、sync 経由 (remote_*_apply) のみ通る。
//! エッジ node の想定シナリオ: origin で書き、レプリカは pull で追従、ローカル書き込み不可。

#![cfg(feature = "v32")]

use std::sync::Arc;
use enchudb::{Engine, HimoType};
use enchudb_wal::Hlc;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};

fn tmp(tag: &str) -> String {
    let p = format!("/tmp/enchudb-v32-replica-{}-{}", tag, std::process::id());
    for suffix in ["", ".wal", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", p, suffix));
    }
    p
}

fn cleanup(path: &str) {
    for suffix in ["", ".wal", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

#[test]
fn replica_rejects_tie() {
    let path = tmp("rejects_tie");
    // origin で schema 定義 + flush
    {
        let mut eng = Engine::create_standalone(&path).unwrap();
        eng.define_himo("val", HimoType::Number, 100);
        eng.flush().unwrap();
    }
    // replica として open
    let eng = Engine::open_replica(&path).unwrap();
    assert!(eng.is_replica());

    let eng = Arc::new(eng);
    eng.set_peer_id(9); // replica 側の peer id
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        eng.tie_to(enchudb_wal::make_eid(9, 1), "val", 42);
    }));
    assert!(result.is_err(), "tie_to on replica must panic");

    cleanup(&path);
}

#[test]
fn replica_rejects_entity_and_delete() {
    let path = tmp("rejects_entity");
    {
        let mut eng = Engine::create_standalone(&path).unwrap();
        eng.define_himo("val", HimoType::Number, 100);
        eng.flush().unwrap();
    }
    let eng = Arc::new(Engine::open_replica(&path).unwrap());

    let entity_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        eng.entity();
    }));
    assert!(entity_panic.is_err(), "entity() must panic on replica");

    let delete_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        eng.delete(enchudb_wal::make_eid(1, 0));
    }));
    assert!(delete_panic.is_err(), "delete() must panic on replica");

    let untie_panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        eng.untie(enchudb_wal::make_eid(1, 0), "val");
    }));
    assert!(untie_panic.is_err(), "untie() must panic on replica");

    cleanup(&path);
}

#[test]
fn replica_rejects_content() {
    let path = tmp("rejects_content");
    {
        let mut eng = Engine::create_standalone(&path).unwrap();
        eng.flush().unwrap();
    }
    let eng = Arc::new(Engine::open_replica(&path).unwrap());

    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        eng.content(enchudb_wal::make_eid(1, 0), "memo", b"hello");
    }));
    assert!(r.is_err(), "content() must panic on replica");

    cleanup(&path);
}

#[test]
fn replica_allows_remote_apply_and_read() {
    // origin 側に書いた値を replica が remote_tie_apply で受け取って get で読める
    let path = tmp("remote_apply");
    {
        let mut eng = Engine::create_standalone(&path).unwrap();
        eng.define_himo("val", HimoType::Number, 100);
        eng.flush().unwrap();
    }
    let eng = Arc::new(Engine::open_replica(&path).unwrap());

    // replica として「リモートから Tie が届いた」シミュレーション
    let eid = enchudb_wal::make_eid(1, 7);
    let himo_id = eng.himo_id("val").unwrap();
    eng.remote_tie_apply(eid, himo_id as u16, 42);

    // 読めるはず
    let v = eng.get(eid, "val");
    assert_eq!(v, Some(42), "remote_tie_apply should land, replica should read it");

    cleanup(&path);
}

#[test]
fn replica_syncs_from_origin_via_syncer() {
    // E2E: origin peer が publish、replica peer が pull して反映を確認
    let path_origin = tmp("e2e_origin");
    let path_replica = tmp("e2e_replica");

    // origin 準備 (通常の書き込み DB)
    {
        let mut eng = Engine::create_standalone(&path_origin).unwrap();
        eng.define_himo("val", HimoType::Number, 100);
        eng.flush().unwrap();
    }
    let origin = Engine::open_concurrent_with_wal(&path_origin, 16 * 1024 * 1024).unwrap();
    origin.set_peer_id(1);

    // replica 準備 (同じ schema で作った後、replica として open)
    {
        let mut eng = Engine::create_standalone(&path_replica).unwrap();
        eng.define_himo("val", HimoType::Number, 100);
        eng.flush().unwrap();
    }
    let replica = Engine::open_concurrent_replica(&path_replica, 16 * 1024 * 1024).unwrap();
    replica.set_peer_id(9);

    // transport
    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());

    // origin 側 Syncer は publish 用
    let origin_syncer = Syncer::new(origin.clone(), transport.clone());

    // replica 側 Syncer は pull 用
    let replica_syncer = Syncer::new(replica.clone(), transport.clone());

    // origin が直接 transport に push して publish 代用 (WAL 無しルート)
    let eid = enchudb_wal::make_eid(1, 3);
    transport.publish(1, vec![
        enchudb::transport::WireRecord::unsigned(
            Hlc { wall: 100, logical: 0, peer: 1 },
            1,
            enchudb_wal::wal::DecodedOp::Tie { eid, himo_id: origin.himo_id("val").unwrap() as u16, value: 77 },
        ),
    ]);

    // replica が pull
    let out = replica_syncer.pull_once(1);
    assert_eq!(out.applied, 1);
    assert_eq!(out.skipped, 0);

    // replica が値を読める
    let v = replica.get(eid, "val");
    assert_eq!(v, Some(77));

    // 念のため replica では直接書き込めない
    let panic_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        replica.tie_to(eid, "val", 999);
    }));
    assert!(panic_result.is_err());

    cleanup(&path_origin);
    cleanup(&path_replica);
    let _ = origin_syncer;
}

#[test]
fn set_replica_mode_toggle() {
    // runtime で replica on/off できる
    let path = tmp("toggle");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("val", HimoType::Number, 100);
    let eng = Arc::new(eng);
    eng.set_peer_id(1);

    // 初期: 書ける
    let e1 = eng.entity();
    eng.tie_to(e1, "val", 10);
    assert_eq!(eng.get(e1, "val"), Some(10));

    // replica on → 書き込み panic
    eng.set_replica_mode(true);
    assert!(eng.is_replica());

    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        eng.tie_to(e1, "val", 20);
    }));
    assert!(r.is_err());

    // replica off に戻す → 再び書ける
    eng.set_replica_mode(false);
    assert!(!eng.is_replica());
    eng.tie_to(e1, "val", 30);
    assert_eq!(eng.get(e1, "val"), Some(30));

    cleanup(&path);
}
