//! #833: `Arc::try_unwrap(Arc<Database>)` で戻した、 consumer の居る `Database` に `finish_*` を呼ぶと、 `into_parts` が
//! consumer 付きの engine を `Arc` から取り出して動かしていた (consumer は前の住所を読む = SIGSEGV)。 取り出さずに `Err`。
#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;

use enchudb_schema::Database;

fn tmp(tag: &str) -> String {
    let p = format!("{}/enchudb-issue833-schema-{}-{}", std::env::temp_dir().display(), tag, std::process::id());
    let _ = std::fs::remove_dir_all(&p);
    p
}

#[test]
fn finish_on_a_database_with_a_consumer_is_refused() {
    let path = tmp("refuse");
    let mut db = Database::create_growable_tiny(&path).unwrap();
    db.table("kv").number("id").number("v").primary_key("id").build().unwrap();
    let db = db.finish_with_oplog(1 << 20).unwrap();
    db.get_table("kv").unwrap().insert().set("id", 1i64).set("v", 10i64).commit().unwrap();
    let Ok(db) = Arc::try_unwrap(db) else { panic!("唯一の持ち主") };
    assert!(db.engine().has_background_threads(), "前提: consumer が居る");
    let again = db.finish_with_oplog(1 << 20);
    assert!(again.is_err(), "consumer の居る Database の finish_with_oplog が通った");
    // 断られた Database は落ちている (中の engine はふつうに閉じた)。 開き直すと書いた行がある
    let db = Database::open_with_oplog(&path, 1 << 20).unwrap();
    let kv = db.get_table("kv").unwrap();
    assert_eq!(kv.all().count().unwrap(), 1);
    let Ok(db) = Arc::try_unwrap(db) else { panic!("唯一の持ち主") };
    assert!(db.finish_concurrent().is_err(), "consumer の居る Database の finish_concurrent が通った");
    let _ = std::fs::remove_dir_all(&path);
}

/// WAL なしの並行の形 (`finish_concurrent`、 裏の thread は無い) を戻して WAL を付けるのは今まで通りできる。
#[test]
fn finish_with_oplog_after_finish_concurrent_still_works() {
    let path = tmp("upgrade");
    let mut db = Database::create_growable_tiny(&path).unwrap();
    db.table("kv").number("id").number("v").primary_key("id").build().unwrap();
    let db = db.finish_concurrent().unwrap();
    db.get_table("kv").unwrap().insert().set("id", 1i64).set("v", 10i64).commit().unwrap();
    let Ok(db) = Arc::try_unwrap(db) else { panic!("唯一の持ち主") };
    let db = db.finish_with_oplog(1 << 20).unwrap();
    let kv = db.get_table("kv").unwrap();
    kv.insert().set("id", 2i64).set("v", 20i64).commit().unwrap();
    assert_eq!(kv.all().count().unwrap(), 2);
    drop(db);
    let _ = std::fs::remove_dir_all(&path);
}
