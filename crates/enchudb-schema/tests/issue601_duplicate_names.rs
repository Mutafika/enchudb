//! #601: 名前 (表で名前と決めた列 = 主キー) が同じ生きた行は 1 つの DB に 1 つだけ (名前が同じなら同じデッキ)。
//!
//! - insert: 既にその名前の行があれば `DuplicatePk` で断り、 何も書かない (前は 2 つ目の行を作った)
//! - 名前の書き換え: 別の行が既にその名前なら `DuplicatePk` (前は同じ名前の行が増えた)。 自分の名前を同じ値で書くのは今まで通り
//! - upsert は今まで通り (その名前の行に当たる)
//! - 確かめと書き込みは名前ごとの lock で並ぶ: 並んだ書き手が同じ名前を insert / upsert しても 1 行

use enchudb_schema::{Database, SchemaError, Value};
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue601-{}-{}-{}",
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
fn a_second_row_with_the_same_name_is_refused() {
    let path = tmp_path("insert");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("u").tag("key").tag("title").primary_key("key").build().unwrap();
    db.table("n").number("id").tag("title").primary_key("id").build().unwrap();
    let u = db.get_table("u").unwrap();
    let n = db.get_table("n").unwrap();

    let x = u.insert().set("key", "x").set("title", "a").commit().unwrap();
    let before = db.engine().entity_count();
    let r = u.insert().set("key", "x").set("title", "b").commit();
    assert!(matches!(r, Err(SchemaError::DuplicatePk)), "同じ名前の 2 つ目の行を作った: {r:?}");
    assert_eq!(db.engine().entity_count(), before, "断った行の書きかけが残った");
    db.engine().rebuild();
    assert_eq!(u.where_eq("key", "x").count().unwrap(), 1);
    assert_eq!(u.entity(x).get("title"), Some(Value::Text("a".into())), "断った insert が前の行を書き換えた");

    // 数の名前も
    n.insert().set("id", 7i64).commit().unwrap();
    assert!(matches!(n.insert().set("id", 7i64).commit(), Err(SchemaError::DuplicatePk)));

    // upsert は今まで通り当たる
    u.upsert().set("key", "x").set("title", "c").commit().unwrap();
    db.engine().rebuild();
    assert_eq!(u.where_eq("key", "x").count().unwrap(), 1);
    assert_eq!(u.entity(x).get("title"), Some(Value::Text("c".into())));

    // 消した後は同じ名前で作れる
    u.entity(x).delete().unwrap();
    u.insert().set("key", "x").set("title", "d").commit().unwrap();
    db.engine().rebuild();
    assert_eq!(u.where_eq("key", "x").count().unwrap(), 1);
}

#[test]
fn renaming_into_a_name_another_row_has_is_refused() {
    let path = tmp_path("rename");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("u").tag("key").tag("title").primary_key("key").build().unwrap();
    let u = db.get_table("u").unwrap();
    let x = u.insert().set("key", "x").commit().unwrap();
    let y = u.insert().set("key", "y").commit().unwrap();

    let r = u.entity(y).set("key", "x").commit();
    assert!(matches!(r, Err(SchemaError::DuplicatePk)), "別の行の名前に改名できた: {r:?}");
    let r = u.entity(y).update().set("title", "t").set("key", "x").commit();
    assert!(matches!(r, Err(SchemaError::DuplicatePk)));
    assert_eq!(u.entity(y).get("title"), None, "断った書き換えの他の列が書かれた");
    db.engine().rebuild();
    assert_eq!(u.where_eq("key", "x").find().unwrap(), vec![x]);
    assert_eq!(u.entity(y).get("key"), Some(Value::Text("y".into())));

    // 自分の名前を同じ値で書く・空いている名前への改名は今まで通り
    u.entity(y).set("key", "y").commit().unwrap();
    u.entity(y).set("key", "z").commit().unwrap();
    db.engine().rebuild();
    assert_eq!(u.where_eq("key", "z").find().unwrap(), vec![y]);
}

/// 並んだ書き手 8 本が同じ 64 個の名前を insert / upsert しても、 名前ごとに 1 行。 insert の 2 本目からは DuplicatePk。
#[test]
fn concurrent_writers_of_the_same_names_make_one_row_each() {
    let path = tmp_path("concurrent");
    let _c = Cleanup(path.clone());
    let mut b = Database::create(&path).unwrap();
    b.table("u").tag("key").number("n").primary_key("key").build().unwrap();
    let db = b.finish_with_oplog(4 << 20).unwrap();
    let ok = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    std::thread::scope(|s| {
        for w in 0..8 {
            let (db, ok) = (db.clone(), ok.clone());
            s.spawn(move || {
                let u = db.get_table("u").unwrap();
                for i in 0..64 {
                    let key = format!("k{i}");
                    if w % 2 == 0 {
                        match u.insert().set("key", key.as_str()).set("n", w as i64).commit() {
                            Ok(_) => {
                                ok.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                            Err(SchemaError::DuplicatePk) => {}
                            Err(e) => panic!("{e:?}"),
                        }
                    } else {
                        u.upsert().set("key", key.as_str()).set("n", w as i64).commit().unwrap();
                    }
                }
            });
        }
    });
    db.engine().rebuild();
    let u = db.get_table("u").unwrap();
    for i in 0..64 {
        assert_eq!(u.where_eq("key", format!("k{i}").as_str()).count().unwrap(), 1, "k{i} の行が 1 つでない");
    }
    assert_eq!(u.all().count().unwrap(), 64);
    assert!(ok.load(std::sync::atomic::Ordering::Relaxed) <= 64, "同じ名前の insert が 2 本以上通った");
}
