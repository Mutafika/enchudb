//! #116: write/oplog-record queue の capacity が (1) default で max_entities に
//! 連動し、(2) `finish_*_with_queue` で明示指定できること。
//!
//! 1M slot 固定は per-DB ~128MiB の固定 RSS (queue 2 本) になり、per-user /
//! per-tenant に DB を分ける構成の host 密度を縛っていた。RSS の実測は
//! sunsu2 `examples/memory_probe.rs` 側で行う (ここは capacity の契約のみ)。

use enchudb_schema::Database;

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue116-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    for suf in ["", ".oplog", ".tables", ".schema", ".crc", ".db.lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

fn build(path: &str, cap: u32) -> Database {
    let mut db = Database::create_with_capacity(path, cap).unwrap();
    db.table("t").tag("k").number("v").primary_key("k").build().unwrap();
    db
}

/// 小 DB の default は max_entities に連動して縮む。
#[test]
fn default_queue_cap_scales_with_max_entities() {
    let path = tmp("scaled");
    cleanup(&path);
    let db = build(&path, 10_000).finish_with_oplog(4 * 1024 * 1024).unwrap();
    assert_eq!(db.engine().write_queue_capacity(), Some(10_000));
    drop(db);
    cleanup(&path);
}

/// floor: 極小 DB でも burst 吸収の下限 4096 は確保する。
#[test]
fn default_queue_cap_has_a_floor() {
    let path = tmp("floor");
    cleanup(&path);
    let db = build(&path, 1_000).finish_with_oplog(4 * 1024 * 1024).unwrap();
    assert_eq!(db.engine().write_queue_capacity(), Some(4_096));
    drop(db);
    cleanup(&path);
}

/// ceiling: 大 DB は従来どおり 1M slot で頭打ち (挙動不変)。
#[test]
fn default_queue_cap_is_capped_at_one_million() {
    let path = tmp("cap");
    cleanup(&path);
    let mut db = Database::create(&path).unwrap(); // max_entities = 16M default
    db.table("t").tag("k").number("v").primary_key("k").build().unwrap();
    let db = db.finish_with_oplog(4 * 1024 * 1024).unwrap();
    assert_eq!(db.engine().write_queue_capacity(), Some(1_048_576));
    drop(db);
    cleanup(&path);
}

/// 明示指定 (finish_with_oplog_with_queue) が default より優先される。
#[test]
fn explicit_queue_cap_wins() {
    let path = tmp("explicit");
    cleanup(&path);
    let db = build(&path, 100_000)
        .finish_with_oplog_with_queue(4 * 1024 * 1024, 4_096)
        .unwrap();
    assert_eq!(db.engine().write_queue_capacity(), Some(4_096));
    // 通常の書き込みが問題なく通る
    db.get_table("t").unwrap().insert().set("k", "a").set("v", 1i64).commit().unwrap();
    assert_eq!(db.get_table("t").unwrap().all().count().unwrap(), 1);
    drop(db);
    cleanup(&path);
}

/// oplog なし版 (finish_concurrent_with_queue)。
#[test]
fn explicit_queue_cap_without_oplog() {
    let path = tmp("no-oplog");
    cleanup(&path);
    let db = build(&path, 100_000).finish_concurrent_with_queue(8_192).unwrap();
    assert_eq!(db.engine().write_queue_capacity(), Some(8_192));
    drop(db);
    cleanup(&path);
}

/// reopen 側 (open_with_oplog_with_queue) — LRU pool 運用の open hot path。
/// 省略版 open は header の max_entities から scaled default。
#[test]
fn open_side_queue_cap() {
    let path = tmp("open");
    cleanup(&path);
    {
        let db = build(&path, 50_000).finish_with_oplog(4 * 1024 * 1024).unwrap();
        db.get_table("t").unwrap().insert().set("k", "a").set("v", 1i64).commit().unwrap();
        db.engine().oplog_sync().unwrap();
    }
    {
        let db = Database::open_with_oplog(&path, 4 * 1024 * 1024).unwrap();
        assert_eq!(db.engine().write_queue_capacity(), Some(50_000), "scaled default on open");
    }
    {
        let db = Database::open_with_oplog_with_queue(&path, 4 * 1024 * 1024, 4_096).unwrap();
        assert_eq!(db.engine().write_queue_capacity(), Some(4_096), "explicit knob on open");
        assert_eq!(db.get_table("t").unwrap().all().count().unwrap(), 1, "data intact");
    }
    cleanup(&path);
}
