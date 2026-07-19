//! #109: schema `Database::create_growable_with_leaf(path, max_entities, leaf_data_size)`
//! の検証。 vocab 版 `create_growable_with_options` に対する Leaf 領域サイズ指定版。
//!
//! - default 512 MiB を **超える** leaf 領域を指定して作成でき、 長文 Leaf 値が
//!   往復 + reopen 永続する (機能面)。
//! - `leaf_data_size` が engine の scale cap 検証まで届いている (= 単なる無視では
//!   ない) ことを、 Gb16 cap (16 GiB) 超過が Err になることで担保 (plumbing)。

use enchudb_schema::{Database, Value};

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue109-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suf in ["", ".tables", ".oplog", ".wal", ".crc", ".db.lock", ".eidmap", ".positions"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

/// default 512 MiB より大きい leaf 領域で作成 → 長文 Leaf が往復 + reopen 永続。
#[test]
fn create_growable_with_leaf_round_trips_above_default() {
    let path = tmp("rt");
    cleanup(&path);

    // default (512 MiB) を超える leaf 領域。 sparse 予約なので物理は触ったページのみ。
    let leaf_size = 640 * 1024 * 1024;
    let long = "x".repeat(50_000); // default 512B vocab bucket には収まらない長文 Leaf

    let (e1, e2) = {
        let mut db = Database::create_growable_with_leaf(&path, 10_000, leaf_size)
            .expect("create_growable_with_leaf");
        let notes = db
            .table("notes")
            .number("id")
            .leaf("body")
            .with_capacity(1_000)
            .build()
            .unwrap();
        let e1 = notes.insert().set("id", 1i64).set("body", "hello leaf").commit().unwrap();
        let e2 = notes.insert().set("id", 2i64).set("body", long.as_str()).commit().unwrap();

        // 同 session read
        assert_eq!(
            notes.entity(e1).get("body"),
            Some(Value::Text("hello leaf".into())),
            "短い Leaf が往復しない",
        );
        assert_eq!(
            notes.entity(e2).get("body"),
            Some(Value::Text(long.clone())),
            "長文 Leaf が往復しない",
        );
        (e1, e2)
    }; // db drop で flush

    // reopen 永続
    {
        let db = Database::open(&path).unwrap();
        let notes = db.get_table("notes").unwrap();
        assert_eq!(notes.entity(e1).get("body"), Some(Value::Text("hello leaf".into())));
        assert_eq!(notes.entity(e2).get("body"), Some(Value::Text(long.clone())));
    }

    cleanup(&path);
}

/// `leaf_data_size` が engine の scale cap まで届いていることを、 Gb16 (16 GiB) cap を
/// 超える指定が Err になることで担保する (validate は allocation 前なので disk 消費なし)。
#[test]
fn create_growable_with_leaf_rejects_over_scale_cap() {
    let path = tmp("cap");
    cleanup(&path);

    let over_16gb = 20usize * 1024 * 1024 * 1024; // Gb16 cap 超え
    let r = Database::create_growable_with_leaf(&path, 1_000, over_16gb);
    assert!(
        r.is_err(),
        "Gb16 cap (16 GiB) を超える leaf_data_size が弾かれていない = 未 plumbing",
    );

    cleanup(&path);
}
