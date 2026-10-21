//! #548: 名前 (表で名前と決めた列 = 主キー) の無い行は表の行でない (デッキは名前でしかできない)。 前は schema で作れて
//! しまい、 表の全部を引く時は出ず、 他の列で引く・数える時は出た。
//!
//! - 名前の無い新しい行は作らない (`insert` / 当たる行の無い `upsert` は `BadValue`、 何も書かない)
//! - 名前を外す書き換えは前から無い (schema の書き換えは値を外せない、 Null は `BadValue`)
//! - 行を書く時は名前のカードを最初に書く (sync で書き込みが分かれて届いても、 受け手は名前より先に他のカードを受けない)

use enchudb_engine::AuditFilter;
use enchudb_oplog::oplog::DecodedOp;
use enchudb_schema::{Database, SchemaError, Value};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue548-{}-{}-{}",
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
fn a_row_without_its_name_is_not_made() {
    let path = tmp_path("make");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("users").tag("key").tag("city").number("age").primary_key("key").build().unwrap();
    db.table("logs").tag("msg").build().unwrap(); // 名前の列の無い表 (対照)
    let users = db.get_table("users").unwrap();
    let before = db.engine().entity_count();

    for (what, r) in [
        ("insert", users.insert().set("city", "Tokyo").set("age", 30i64).commit()),
        ("upsert", users.upsert().set("city", "Tokyo").commit()),
        ("Null の名前", users.insert().set("key", Value::Null).set("city", "Tokyo").commit()),
    ] {
        assert!(matches!(r, Err(SchemaError::BadValue(_))), "{what}: 名前の無い行を作った: {r:?}");
    }
    assert_eq!(db.engine().entity_count(), before, "断った行の書きかけが残った");
    db.engine().rebuild();
    assert_eq!(users.where_eq("city", "Tokyo").count().unwrap(), 0);

    // 名前のある行・名前の列の無い表は今まで通り
    let e = users.insert().set("city", "Osaka").set("key", "a").commit().unwrap();
    users.upsert().set("age", 40i64).set("key", "a").commit().unwrap();
    db.get_table("logs").unwrap().insert().set("msg", "hi").commit().unwrap();
    db.engine().rebuild();
    assert_eq!(users.all().count().unwrap(), 1);
    assert_eq!(users.entity(e).get("age"), Some(Value::Number(40)), "upsert が名前の行に当たらない");

    // 名前を外す書き換えは無い (名前は残る)
    let r = users.entity(e).update().set("key", Value::Null).commit();
    assert!(matches!(r, Err(SchemaError::BadValue(_))), "名前を外せた: {r:?}");
    assert_eq!(users.entity(e).get("key"), Some(Value::Text("a".into())));
}

/// 行を書く時は、 `.set()` の順によらず名前のカードを最初に oplog に書く。
#[test]
fn the_name_is_written_first() {
    let path = tmp_path("order");
    let _c = Cleanup(path.clone());
    let mut b = Database::create(&path).unwrap();
    b.table("users").tag("city").number("age").tag("key").primary_key("key").build().unwrap();
    let db = b.finish_with_oplog(1 << 20).unwrap();
    let users = db.get_table("users").unwrap();
    let e = users.insert().set("city", "Tokyo").set("age", 30i64).set("key", "a").commit().unwrap();
    users.upsert().set("age", 31i64).set("key", "b").commit().unwrap();
    let eng = db.engine();
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    let key = eng.himo_id("users.key").unwrap() as u16;
    let first_ties: Vec<(u64, u16)> = {
        let mut seen = std::collections::HashSet::new();
        eng.audit(&AuditFilter::default())
            .into_iter()
            .filter_map(|r| match r.op {
                DecodedOp::Tie { eid, himo_id, .. } if seen.insert(eid) => Some((eid, himo_id)),
                _ => None,
            })
            .collect()
    };
    assert_eq!(first_ties.len(), 2, "前提: 2 行の Tie が oplog にある: {first_ties:?}");
    for (eid, himo) in first_ties {
        assert_eq!(himo, key, "行 {eid} の最初の Tie が名前でない (set した順に書いた)");
    }
    let _ = e;
}
