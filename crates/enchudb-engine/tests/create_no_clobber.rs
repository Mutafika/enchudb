//! 0.9.0 (H11) regression: `Engine::create_*` は既存ファイルを silent truncate
//! で破壊してはならない。 typo った path への create が既存 DB を全損させる
//! 事故を `AlreadyExists` で防ぐ (FFI 契約: 「明示 create は既存で Engine 側エラー」)。

use enchudb_engine::Engine;
use std::io::ErrorKind;

fn tmp(name: &str) -> String {
    let p = std::env::temp_dir().join(format!(
        "enchu-no-clobber-{}-{}.db",
        name,
        std::process::id()
    ));
    let s = p.to_str().unwrap().to_string();
    // 前回残骸を掃除 (DB 本体 + sidecar 群)
    for suffix in ["", ".oplog", ".crc", ".tables", ".eidmap", ".lock"] {
        let _ = std::fs::remove_file(format!("{s}{suffix}"));
    }
    s
}

/// Engine は Debug ではないので unwrap_err が使えない — Err だけ取り出す helper。
fn expect_err<T>(r: std::io::Result<T>, ctx: &str) -> std::io::Error {
    match r {
        Err(e) => e,
        Ok(_) => panic!("{ctx}: expected error, got Ok"),
    }
}

#[test]
fn create_standalone_over_existing_is_already_exists() {
    let path = tmp("standalone");
    {
        let mut eng = Engine::create_standalone(&path).unwrap();
        let e = eng.entity();
        eng.tie(e, "age", 42);
        eng.flush().unwrap();
    } // drop → writer lock 解放

    let err = expect_err(Engine::create_standalone(&path), "create over existing");
    assert_eq!(err.kind(), ErrorKind::AlreadyExists, "got: {err}");

    // 既存データが無傷であること (旧実装は truncate で 0 byte 化していた)
    let eng = Engine::open_standalone(&path).unwrap();
    assert_eq!(eng.entity_count(), 1);
    drop(eng);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn create_wal_variant_over_existing_is_already_exists() {
    let path = tmp("wal");
    {
        let eng = Engine::create(&path).unwrap();
        let _e = eng.entity();
        eng.flush_writes();
    }

    let err = expect_err(Engine::create(&path), "create (WAL) over existing");
    assert_eq!(err.kind(), ErrorKind::AlreadyExists, "got: {err}");

    // open 経路は引き続き成功する
    let eng = Engine::open(&path).unwrap();
    assert_eq!(eng.entity_count(), 1);
    drop(eng);
    for suffix in ["", ".oplog", ".crc", ".tables", ".eidmap"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
    }
}

#[test]
fn create_growable_over_existing_is_already_exists() {
    let path = tmp("growable");
    {
        let _eng = Engine::create_growable_tiny(&path).unwrap();
    }
    let err = expect_err(Engine::create_growable_tiny(&path), "growable_tiny over existing");
    assert_eq!(err.kind(), ErrorKind::AlreadyExists, "got: {err}");
    let err = expect_err(Engine::create_growable(&path), "growable over existing");
    assert_eq!(err.kind(), ErrorKind::AlreadyExists, "got: {err}");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn create_over_non_db_file_is_already_exists() {
    // DB ですらない既存ファイル (ユーザの他ファイルを typo で潰すケース) も保護
    let path = tmp("nondb");
    std::fs::write(&path, b"precious user data, definitely not a DB").unwrap();
    let err = expect_err(Engine::create_standalone(&path), "create over non-DB file");
    assert_eq!(err.kind(), ErrorKind::AlreadyExists, "got: {err}");
    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(&bytes[..], b"precious user data, definitely not a DB");
    let _ = std::fs::remove_file(&path);
}
