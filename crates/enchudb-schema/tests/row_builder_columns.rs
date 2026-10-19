//! 書き込みの builder (`insert` / `upsert` / `entity().update()` / `entity().set()`) の列の扱い。 列は `set` の時に引く
//! (前は列の名前を写して commit で引き直していた)。 振る舞いは前と同じ:
//!
//! - 列の名前は大文字小文字を区別しない
//! - 知らない列: `insert` / `upsert` は何も書かずに `UnknownColumn` (行も作らない)。 `update` は書く順に書いていき、 知らない列の
//!   所で `UnknownColumn` (その前の列は書いてある)
//! - 同じ列を 2 回 `set` したら後の値

use enchudb_schema::{Database, SchemaError, Value};

fn tmp_path(name: &str) -> String {
    format!("/tmp/enchudb-row-builder-{}-{}", name, std::process::id())
}

fn cleanup(path: &str) {
    let _ = enchudb_engine::db_files::remove_db(path);
}

fn open(name: &str) -> (String, Database) {
    let path = tmp_path(name);
    cleanup(&path);
    let mut db = Database::create(&path).unwrap();
    db.table("users").number("id").tag("name").number("age").bigint("at").primary_key("id").build().unwrap();
    (path, db)
}

#[test]
fn column_names_are_case_insensitive() {
    let (path, db) = open("case");
    let t = db.get_table("users").unwrap();
    let e = t.insert().set("ID", 1i64).set("Name", "Alice").set("AGE", 30i64).set("At", -5i64).commit().unwrap();
    assert_eq!(t.entity(e).get("id"), Some(Value::Number(1)));
    assert_eq!(t.entity(e).get("name"), Some(Value::Text("Alice".into())));
    assert_eq!(t.entity(e).get("age"), Some(Value::Number(30)));
    assert_eq!(t.entity(e).get("at"), Some(Value::Number(-5)));
    t.entity(e).update().set("aGe", 31i64).commit().unwrap();
    t.entity(e).set("NAME", "Bob").commit().unwrap();
    assert_eq!(t.entity(e).get("age"), Some(Value::Number(31)));
    assert_eq!(t.entity(e).get("name"), Some(Value::Text("Bob".into())));
    drop(db);
    cleanup(&path);
}

#[test]
fn unknown_column_in_insert_writes_nothing() {
    let (path, db) = open("insert");
    let t = db.get_table("users").unwrap();
    let before = t.all().count().unwrap();
    // 知らない列が最後でも途中でも、 何も書かない (行も作らない)
    for row in [
        t.insert().set("id", 1i64).set("age", 30i64).set("height", 170i64),
        t.insert().set("id", 1i64).set("height", 170i64).set("age", 30i64),
    ] {
        match row.commit() {
            Err(SchemaError::UnknownColumn(c)) => assert_eq!(c, "height"),
            other => panic!("UnknownColumn のはず: {other:?}"),
        }
    }
    assert_eq!(t.all().count().unwrap(), before, "行を作らない");
    assert!(t.where_eq("id", 1i64).find().unwrap().is_empty(), "id を書かない");
    assert!(t.where_eq("age", 30i64).find().unwrap().is_empty(), "age を書かない");
    // upsert も同じ (既にある行を書き換えない)
    let e = t.insert().set("id", 2i64).set("age", 40i64).commit().unwrap();
    match t.upsert().set("id", 2i64).set("age", 41i64).set("height", 1i64).commit() {
        Err(SchemaError::UnknownColumn(c)) => assert_eq!(c, "height"),
        other => panic!("UnknownColumn のはず: {other:?}"),
    }
    assert_eq!(t.entity(e).get("age"), Some(Value::Number(40)), "upsert は既にある行を書き換えない");
    assert_eq!(t.all().count().unwrap(), before + 1);
    drop(db);
    cleanup(&path);
}

#[test]
fn unknown_column_in_update_stops_there() {
    let (path, db) = open("update");
    let t = db.get_table("users").unwrap();
    let e = t.insert().set("id", 1i64).set("age", 30i64).set("name", "Alice").commit().unwrap();
    match t.entity(e).update().set("age", 31i64).set("height", 170i64).set("name", "Bob").commit() {
        Err(SchemaError::UnknownColumn(c)) => assert_eq!(c, "height"),
        other => panic!("UnknownColumn のはず: {other:?}"),
    }
    assert_eq!(t.entity(e).get("age"), Some(Value::Number(31)), "知らない列の前は書いてある");
    assert_eq!(t.entity(e).get("name"), Some(Value::Text("Alice".into())), "知らない列の後は書かない");
    match t.entity(e).set("weight", 60i64).commit() {
        Err(SchemaError::UnknownColumn(c)) => assert_eq!(c, "weight"),
        other => panic!("UnknownColumn のはず: {other:?}"),
    }
    drop(db);
    cleanup(&path);
}

#[test]
fn setting_a_column_twice_keeps_the_last_value() {
    let (path, db) = open("twice");
    let t = db.get_table("users").unwrap();
    let e = t.insert().set("id", 1i64).set("age", 30i64).set("age", 31i64).commit().unwrap();
    assert_eq!(t.entity(e).get("age"), Some(Value::Number(31)));
    t.entity(e).update().set("age", 40i64).set("AGE", 41i64).commit().unwrap();
    assert_eq!(t.entity(e).get("age"), Some(Value::Number(41)));
    assert_eq!(t.where_eq("age", 41i64).find().unwrap(), vec![e]);
    assert!(t.where_eq("age", 30i64).find().unwrap().is_empty());
    drop(db);
    cleanup(&path);
}
