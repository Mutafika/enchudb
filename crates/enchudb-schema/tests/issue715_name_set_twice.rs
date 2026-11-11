//! #715: insert / upsert で名前の列を 2 回以上 set した時、 最後の値がその行の名前。 重複の確かめも名前の lock も書き込みも
//! 最後の値でする (`entity(e).update()` と同じ)。
//!
//! 前は確かめが最初の値・書くのは最後の値で、 既にある名前の 2 つ目の行ができた (#601 の約束 = 名前が同じなら同じデッキ、 が破れる)。

use enchudb_schema::{Database, SchemaError, Value};

fn tmp_path(tag: &str) -> String {
    format!(
        "{}/enchudb-issue715-{}-{}-{}",
        std::env::temp_dir().display(),
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn an_insert_checks_the_last_name_it_was_given() {
    let path = tmp_path("number");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").number("a").primary_key("id").build().unwrap();
    let t = db.get_table("t").unwrap();
    let e2 = t.insert().set("id", 2i64).set("a", 20i64).commit().unwrap();
    let before = db.engine().entity_count();

    let r = t.insert().set("id", 1i64).set("id", 2i64).set("a", 22i64).commit();
    assert!(matches!(r, Err(SchemaError::DuplicatePk)), "最後の名前 2 は既にある: {r:?}");
    assert_eq!(db.engine().entity_count(), before, "断った行の書きかけが残った");
    assert_eq!(t.where_eq("id", 2i64).find().unwrap(), vec![e2]);
    assert_eq!(t.where_eq("id", 1i64).count().unwrap(), 0, "最初の名前の行ができた");
    assert_eq!(t.entity(e2).get("a"), Some(Value::Number(20)), "断った insert が前の行を書き換えた");

    // 最初の値が既にある名前でも、 最後の値が新しければ作れる (最後の値の行が 1 つ、 最初の名前は元のまま)
    let e3 = t.insert().set("id", 2i64).set("id", 3i64).set("a", 30i64).commit().unwrap();
    assert_ne!(e3, e2);
    assert_eq!(t.where_eq("id", 3i64).find().unwrap(), vec![e3]);
    assert_eq!(t.where_eq("id", 2i64).find().unwrap(), vec![e2]);
    assert_eq!(t.entity(e3).get("id"), Some(Value::Number(3)));
    assert_eq!(t.all().count().unwrap(), 2);
}

#[test]
fn an_insert_checks_the_last_tag_name_it_was_given() {
    let path = tmp_path("tag");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("t").tag("key").number("a").primary_key("key").build().unwrap();
    let t = db.get_table("t").unwrap();
    let x = t.insert().set("key", "x").set("a", 1i64).commit().unwrap();
    // 最初の値はまだ辞書に無い語 (前はここで 「その名前の行は無い」 と決めた)
    let r = t.insert().set("key", "fresh").set("a", 2i64).set("key", "x").commit();
    assert!(matches!(r, Err(SchemaError::DuplicatePk)), "最後の名前 x は既にある: {r:?}");
    assert_eq!(t.where_eq("key", "x").find().unwrap(), vec![x]);
    assert_eq!(t.where_eq("key", "fresh").count().unwrap(), 0);
    assert_eq!(t.entity(x).get("a"), Some(Value::Number(1)));
    assert_eq!(t.all().count().unwrap(), 1);
}

/// upsert は最後の名前の行に当たる (前は最初の名前で引いて当たらず、 最後の名前の 2 つ目の行を作った)。
#[test]
fn an_upsert_lands_on_the_row_of_the_last_name() {
    let path = tmp_path("upsert");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").number("a").primary_key("id").build().unwrap();
    let t = db.get_table("t").unwrap();
    let e1 = t.insert().set("id", 1i64).set("a", 10i64).commit().unwrap();
    let e2 = t.insert().set("id", 2i64).set("a", 20i64).commit().unwrap();

    // 当たる行の無い最初の値 → 最後の値 2 の行に当たる
    let got = t.upsert().set("id", 9i64).set("id", 2i64).set("a", 21i64).commit().unwrap();
    assert_eq!(got, e2);
    assert_eq!(t.where_eq("id", 2i64).find().unwrap(), vec![e2]);
    assert_eq!(t.where_eq("id", 9i64).count().unwrap(), 0);
    assert_eq!(t.entity(e2).get("a"), Some(Value::Number(21)));

    // 最初の値が別の行の名前でも、 書くのは最後の名前の行だけ (最初の名前の行は名前も値も元のまま)
    let got = t.upsert().set("id", 1i64).set("id", 2i64).set("a", 22i64).commit().unwrap();
    assert_eq!(got, e2);
    assert_eq!(t.entity(e1).get_many(&["id", "a"]), vec![Some(Value::Number(1)), Some(Value::Number(10))]);
    assert_eq!(t.entity(e2).get_many(&["id", "a"]), vec![Some(Value::Number(2)), Some(Value::Number(22))]);

    // どちらの値の行も無ければ、 最後の名前の行を 1 つ作る
    let e6 = t.upsert().set("id", 5i64).set("id", 6i64).set("a", 60i64).commit().unwrap();
    assert_eq!(t.where_eq("id", 6i64).find().unwrap(), vec![e6]);
    assert_eq!(t.where_eq("id", 5i64).count().unwrap(), 0);
    assert_eq!(t.all().count().unwrap(), 3);
}

/// 最後の値が Null なら名前の無い行なので作らない (#548)。 前の値は使わない。
#[test]
fn a_last_name_of_null_is_a_row_without_a_name() {
    let path = tmp_path("null");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").number("a").primary_key("id").build().unwrap();
    let t = db.get_table("t").unwrap();
    let before = db.engine().entity_count();
    let r = t.insert().set("id", 1i64).set("id", Value::Null).set("a", 1i64).commit();
    assert!(matches!(r, Err(SchemaError::BadValue(_))), "{r:?}");
    assert_eq!(db.engine().entity_count(), before);
    assert_eq!(t.where_eq("id", 1i64).count().unwrap(), 0);
    assert_eq!(t.all().where_ge("a", 0i64).count().unwrap(), 0);
}
