//! #116: 0.33 までは engine ごとに write/oplog-record queue を 2 本 eager に確保していた (default は max_entities に
//! 連動、 上限 1M slot = per-DB 最大 ~128MiB の固定 RSS)。 `finish_*_with_queue` / `open_with_oplog_with_queue` で cap を
//! 指定できた。
//!
//! 0.34 で queue を外した (書き込みは呼んだ thread がその場で書く) ので、 queue の分の固定のメモリは無い。 ここでは、
//! どの開き方でも queue が無いこと、 cap の knob は互換のために受け付けて使わず、 DB は普通に使えることを見る。

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

/// どの開き方でも write queue は無い (0.33 の default: 小 DB は max_entities に連動・下限 4096、 大 DB は 1M slot)。
#[test]
fn no_write_queue_in_any_mode() {
    for (tag, cap) in [("small", 1_000u32), ("mid", 10_000)] {
        let path = tmp(tag);
        cleanup(&path);
        let db = build(&path, cap).finish_with_oplog(4 * 1024 * 1024).unwrap();
        assert_eq!(db.engine().write_queue_capacity(), None, "{tag}: finish_with_oplog");
        drop(db);
        cleanup(&path);
    }
    let path = tmp("big");
    cleanup(&path);
    let mut db = Database::create(&path).unwrap(); // max_entities = 16M default
    db.table("t").tag("k").number("v").primary_key("k").build().unwrap();
    let db = db.finish_with_oplog(4 * 1024 * 1024).unwrap();
    assert_eq!(db.engine().write_queue_capacity(), None, "big: finish_with_oplog");
    drop(db);
    cleanup(&path);
    let path = tmp("no-oplog-default");
    cleanup(&path);
    let db = build(&path, 100_000).finish_concurrent().unwrap();
    assert_eq!(db.engine().write_queue_capacity(), None, "finish_concurrent");
    drop(db);
    cleanup(&path);
}

/// cap の knob (finish_with_oplog_with_queue) は受け付けて使わない。 書き込みは普通に通る。
#[test]
fn queue_cap_knob_is_accepted_and_ignored() {
    let path = tmp("explicit");
    cleanup(&path);
    let db = build(&path, 100_000)
        .finish_with_oplog_with_queue(4 * 1024 * 1024, 4_096)
        .unwrap();
    assert_eq!(db.engine().write_queue_capacity(), None);
    db.get_table("t").unwrap().insert().set("k", "a").set("v", 1i64).commit().unwrap();
    assert_eq!(db.get_table("t").unwrap().all().count().unwrap(), 1);
    drop(db);
    cleanup(&path);
}

/// oplog なし版 (finish_concurrent_with_queue)。
#[test]
fn queue_cap_knob_without_oplog() {
    let path = tmp("no-oplog");
    cleanup(&path);
    let db = build(&path, 100_000).finish_concurrent_with_queue(8_192).unwrap();
    assert_eq!(db.engine().write_queue_capacity(), None);
    db.get_table("t").unwrap().insert().set("k", "a").set("v", 1i64).commit().unwrap();
    assert_eq!(db.get_table("t").unwrap().all().count().unwrap(), 1);
    drop(db);
    cleanup(&path);
}

/// reopen 側 (open_with_oplog / open_with_oplog_with_queue) — LRU pool 運用の open hot path。 中身はそのまま。
#[test]
fn open_side_queue_cap_knob() {
    let path = tmp("open");
    cleanup(&path);
    {
        let db = build(&path, 50_000).finish_with_oplog(4 * 1024 * 1024).unwrap();
        db.get_table("t").unwrap().insert().set("k", "a").set("v", 1i64).commit().unwrap();
        db.engine().oplog_sync().unwrap();
    }
    {
        let db = Database::open_with_oplog(&path, 4 * 1024 * 1024).unwrap();
        assert_eq!(db.engine().write_queue_capacity(), None, "open");
    }
    {
        let db = Database::open_with_oplog_with_queue(&path, 4 * 1024 * 1024, 4_096).unwrap();
        assert_eq!(db.engine().write_queue_capacity(), None, "open with the knob");
        assert_eq!(db.get_table("t").unwrap().all().count().unwrap(), 1, "data intact");
    }
    cleanup(&path);
}
