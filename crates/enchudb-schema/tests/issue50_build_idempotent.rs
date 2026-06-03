//! issue #50: `TableBuilder::build()` が reopen 時に
//! engine sidecar 側の "already exists" で fail する bug の regression test。
//!
//! 状態の作り方:
//! 1. `Database::create` で table A を declare、 finalize
//! 2. close (= engine sidecar に A の TableDef が persist 済)
//! 3. 再 open (`Database::open`) — schema blob にも A はあるので
//!    db.tables にも A は復元される → `db.table("A")` は早期 return path
//!    (= find_table_inner で hit) で問題なし
//!
//! ↑ これは bug を踏まない。 bug を踏ますためには
//! **schema blob には無いが engine sidecar には有る** 状態が必要。
//! `Engine::open_standalone` で直接 `define_table` を呼ぶことで作る。

use enchudb_schema::{Database, ColumnType};
use enchudb_engine::Engine;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue50-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
    let _ = std::fs::remove_file(format!("{}.tables", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.schema", path));
    let _ = std::fs::remove_file(format!("{}.db.lock", path));
}

/// 状態を作る:
/// engine sidecar (.tables) には "snapshot_tags" がある、
/// schema blob (.schema) には無い。
/// この上で `TableBuilder::build("snapshot_tags")` が ok を返すこと。
#[test]
fn build_recovers_from_already_exists_on_reopen() {
    let path = tmp_path("recover");
    cleanup(&path);

    // 1) 初期 setup: Database で table A を declare、 sidecar に persist させる
    {
        let mut db = Database::create(&path).unwrap();
        db.table("articles")
            .column("url", ColumnType::Tag)
            .build()
            .unwrap();
        // drop → flush で .tables + .schema 両方 persist
    }

    // 2) Engine 直で "snapshot_tags" を後から define
    //    (= schema 経由しないので .schema blob には入らない、 .tables sidecar
    //    には persist される)
    {
        let mut eng = Engine::open_standalone(&path).unwrap();
        eng.define_table("snapshot_tags", 1000).unwrap();
        eng.define_himo_in("snapshot_tags", "tag_id", enchudb_engine::HimoType::Tag, 0).unwrap();
        eng.persist_tables().unwrap();
        // drop
    }

    // 3) Database で再 open、 同じ "snapshot_tags" を declare
    //    schema blob には無いので find_table_inner は None → define_table 経路 →
    //    "already exists" → fix なしだと Err 返す、 fix ありだと Ok
    {
        let mut db = Database::open(&path).unwrap();
        let result = db.table("snapshot_tags")
            .column("tag_id", ColumnType::Tag)
            .build();
        assert!(
            result.is_ok(),
            "TableBuilder::build() should be idempotent on reopen, got: {:?}",
            result.err(),
        );
        // 後で使えること
        let handle = db.get_table("snapshot_tags").unwrap();
        let _ = handle; // 取れれば OK
    }

    cleanup(&path);
}

/// 同じ shape を 2 回 declare してもエラーにならない (= 同一 process 内の idempotent)。
/// これは元から OK だが regression 防止のため明示。
#[test]
fn build_twice_in_same_process_is_idempotent() {
    let path = tmp_path("twice");
    cleanup(&path);

    let mut db = Database::create(&path).unwrap();
    db.table("foo")
        .column("name", ColumnType::Tag)
        .primary_key("name")
        .build()
        .unwrap();
    // 2 回目: 同じ shape を declare — エラーにならない
    let r = db.table("foo")
        .column("name", ColumnType::Tag)
        .primary_key("name")
        .build();
    assert!(r.is_ok(), "second build() of same table should be idempotent, got: {:?}", r.err());

    cleanup(&path);
}
