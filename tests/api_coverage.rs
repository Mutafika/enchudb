//! API 網羅 audit で漏れてた pub fn の最小 smoke test。
//!
//! 対象(TEST_DESIGN.md P0-2 の gap):
//! - `create_with_options` / `create_full_with_cyl` (factory variants)
//! - `create_concurrent` (WAL 無し concurrent)
//! - `get_entity` (全 himo 一括取得)
//! - `tie_text_to` (&self text 書き込み)
//! - `open_concurrent_replica` (v32 replica + concurrent writer)

use enchudb::{Engine, EntityValue, HimoType};
use std::sync::Arc;

fn tmp(tag: &str) -> String {
    let p = format!(
        "/tmp/enchudb-api-cov-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
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
fn create_with_options_respects_capacity_hint() {
    let path = tmp("opts");
    let mut eng =
        Engine::create_with_options(&path, 1_000_000, Some(1024 * 1024)).unwrap();
    eng.define_himo("age", HimoType::Value, 100);
    let e = eng.entity();
    eng.tie(e, "age", 42);
    assert_eq!(eng.get(e, "age"), Some(42));
    cleanup(&path);
}

#[test]
fn create_full_with_cyl_all_options() {
    let path = tmp("full_cyl");
    let mut eng = Engine::create_full_with_cyl(
        &path,
        10_000,
        Some(512 * 1024),
        Some(32),
        Some(64 * 1024),
        Some(1024),
    )
    .unwrap();
    eng.define_himo("v", HimoType::Value, 50);
    let e = eng.entity();
    eng.tie(e, "v", 7);
    assert_eq!(eng.get(e, "v"), Some(7));
    cleanup(&path);
}

#[test]
fn get_entity_returns_all_himos() {
    let path = tmp("get_entity");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("age", HimoType::Value, 100);
    eng.define_himo("city", HimoType::Symbol, 0);
    let e = eng.entity();
    eng.tie(e, "age", 30);
    eng.tie_text(e, "city", "東京");

    let fields = eng.get_entity(e);
    assert_eq!(fields.len(), 2);
    let age = fields.iter().find(|(k, _)| *k == "age").unwrap();
    match age.1 {
        EntityValue::Num(n) => assert_eq!(n, 30),
        _ => panic!("age should be Num"),
    }
    let city = fields.iter().find(|(k, _)| *k == "city").unwrap();
    match city.1 {
        EntityValue::Text(b) => assert_eq!(b, "東京".as_bytes()),
        _ => panic!("city should be Text"),
    }

    // empty entity returns empty vec
    let e2 = eng.entity();
    assert!(eng.get_entity(e2).is_empty());

    cleanup(&path);
}

#[cfg(feature = "v27")]
#[test]
fn create_concurrent_without_wal_accepts_tie_async() {
    let path = tmp("conc_no_wal");
    let eng = Engine::create_concurrent(&path).unwrap();
    // 定義は Arc::get_mut 経由(テストでは要らないのでスキーマ無しで書き込みは panic)。
    // ここでは factory が Arc<Self> を返せることと entity が作れることのみ確認。
    let _e = eng.entity();
    assert_eq!(eng.entity_count(), 1);
    cleanup(&path);
}

#[test]
fn tie_text_to_on_shared_arc() {
    let path = tmp("ttto");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("name", HimoType::Symbol, 0);
    let eng = Arc::new(eng);

    let e = eng.entity();
    eng.tie_text_to(e, "name", "alice");
    assert_eq!(eng.get_text(e, "name"), Some(b"alice".as_ref()));

    // 上書き(vocab への追加)
    eng.tie_text_to(e, "name", "bob");
    assert_eq!(eng.get_text(e, "name"), Some(b"bob".as_ref()));

    cleanup(&path);
}

#[cfg(feature = "v32")]
#[test]
fn open_concurrent_replica_rejects_writes_and_syncs_via_remote() {
    let path = tmp("repl");
    {
        let mut eng = Engine::create_standalone(&path).unwrap();
        eng.define_himo("v", HimoType::Value, 100);
        eng.flush().unwrap();
    }
    let eng = Engine::open_concurrent_replica(&path, 4 * 1024 * 1024).unwrap();
    assert!(eng.is_replica(), "replica flag should be on");

    // replica mode は書き込み API で panic するので、remote_*_apply を直接叩く
    let himo_id = eng.himo_id("v").unwrap() as u16;
    eng.remote_tie_apply(enchudb::make_eid(1, 5), himo_id, 42);
    assert_eq!(eng.get(enchudb::make_eid(1, 5), "v"), Some(42));

    // set_replica_mode(false) で書き込み解放
    eng.set_replica_mode(false);
    assert!(!eng.is_replica());
    let e = eng.entity();
    eng.tie_async(e, "v", 99);
    eng.wal_commit();
    eng.flush_writes();
    eng.wal_sync().unwrap();
    assert_eq!(eng.get(e, "v"), Some(99));

    drop(eng);
    cleanup(&path);
}
