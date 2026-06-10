//! issue #61: `.schema` sidecar の serialize は table/column 名を区切り文字
//! `|` `;` `:` 改行・ relation `->` で連結するため、 名前にこれらが入ると
//! round-trip で schema が壊れる。 build 時に弾くことを確認する。

use enchudb_schema::Database;

fn tmp(name: &str) -> String {
    let p = format!("/tmp/enchudb_issue61_{name}.db");
    let _ = std::fs::remove_file(&p);
    let _ = std::fs::remove_file(format!("{p}.schema"));
    let _ = std::fs::remove_file(format!("{p}.tables"));
    p
}

#[test]
fn table_name_with_pipe_rejected() {
    let path = tmp("tbl_pipe");
    let mut db = Database::create(&path).unwrap();
    let r = db.table("users|posts").number("id").primary_key("id").build();
    assert!(r.is_err(), "table name with '|' must be rejected");
    let msg = format!("{}", r.err().unwrap());
    assert!(msg.contains("reserved"), "got: {msg}");
}

#[test]
fn column_name_with_colon_rejected() {
    let path = tmp("col_colon");
    let mut db = Database::create(&path).unwrap();
    let r = db.table("t").number("a:b").primary_key("a:b").build();
    assert!(r.is_err(), "column name with ':' must be rejected");
}

#[test]
fn column_name_with_semicolon_rejected() {
    let path = tmp("col_semi");
    let mut db = Database::create(&path).unwrap();
    let r = db.table("t").tag("name;drop").build();
    assert!(r.is_err(), "column name with ';' must be rejected");
}

#[test]
fn name_with_arrow_rejected() {
    let path = tmp("arrow");
    let mut db = Database::create(&path).unwrap();
    let r = db.table("a->b").number("id").primary_key("id").build();
    assert!(r.is_err(), "name with '->' must be rejected");
}

#[test]
fn clean_names_still_build() {
    // 通常の名前は従来どおり通ること (regression を作らない)。
    let path = tmp("clean");
    let mut db = Database::create(&path).unwrap();
    let r = db.table("users").number("id").tag("name").primary_key("id").build();
    assert!(r.is_ok(), "ordinary names must still build: {:?}", r.err());
}
