//! #316: ディスクの空きが少ない時に、 commit が Ok のまま Tag 列の値が消えていた。
//!
//! 空き不足は `set_space_margin` (伸ばす時に残す空き) を大きくして作る。
//! - 空きが少なくても、 辞書の索引 (疎なファイル、 slot が全体に散る) は触るページの分だけ空きを見るので断らない
//!   (旧: 伸ばす見かけの長さ = 数 GB と空きを比べて断り、 その拒否が commit の Ok に化けていた)
//! - 伸ばせない時は commit が `WriteRejected(DiskSpace)` を返し、 書きかけの新しい row を残さない

use enchudb_schema::{Database, SchemaError, Table, Value};
use enchudb_engine::{FaultKind, TieRejected};

fn tmp(tag: &str) -> String {
    format!("/tmp/enchudb-issue316-{tag}-{}.db", std::process::id())
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    let _ = std::fs::remove_file(path);
}

fn name(i: i64) -> String {
    format!("name-{i:06}")
}

fn read(t: &Table, e: u64) -> (Option<Value>, Option<Value>) {
    (t.entity(e).get("name"), t.entity(e).get("age"))
}

/// 使える空きを 64 MB に絞っても、 Tag の値は全部書けて読める。
#[test]
fn tags_are_written_when_free_space_is_small() {
    let path = tmp("small");
    cleanup(&path);
    let mut db = Database::create(&path).unwrap();
    db.table("users").number("id").tag("name").number("age").primary_key("id").build().unwrap();
    let t = db.get_table("users").unwrap();
    let eng = db.engine();
    let free = eng.disk_free_bytes().expect("growable backing");
    eng.set_space_margin(free.saturating_sub(64 << 20));
    for i in 0..5000i64 {
        let e = t.insert().set("id", i).set("name", name(i).as_str()).set("age", i % 100).commit().unwrap();
        assert_eq!(read(&t, e), (Some(Value::Text(name(i))), Some(Value::Number(i % 100))), "row {i}");
    }
    assert_eq!(eng.fault_count(FaultKind::VocabSpace), 0);
    assert_eq!(eng.fault_count(FaultKind::DiskSpace), 0);
    drop(t);
    drop(db);
    cleanup(&path);
}

/// 伸ばすのを全部断らせると、 どこかで commit が `WriteRejected(DiskSpace)` を返す。 Ok の row は全部読め、
/// 拒否された row は残らない。 「辞書が一杯」 とは報告しない。
#[test]
fn refused_growth_is_an_error() {
    let path = tmp("refused");
    cleanup(&path);
    let mut db = Database::create(&path).unwrap();
    db.table("users").number("id").tag("name").number("age").primary_key("id").build().unwrap();
    let t = db.get_table("users").unwrap();
    let eng = db.engine();
    eng.set_space_margin(u64::MAX / 4);
    let mut ok = Vec::new();
    let mut rejected = None;
    for i in 0..200_000i64 {
        match t.insert().set("id", i).set("name", name(i).as_str()).set("age", i % 100).commit() {
            Ok(e) => ok.push((i, e)),
            Err(SchemaError::WriteRejected(r)) => {
                assert_eq!(r, TieRejected::Fault(FaultKind::DiskSpace), "row {i}");
                rejected = Some(i);
                break;
            }
            Err(e) => panic!("row {i}: {e}"),
        }
    }
    let i = rejected.expect("伸ばすのを断っても commit が一度も Err を返さない");
    for &(j, e) in &ok {
        assert_eq!(read(&t, e), (Some(Value::Text(name(j))), Some(Value::Number(j % 100))), "row {j}");
    }
    assert_eq!(t.where_eq("id", i).find_one().unwrap(), None, "拒否された row {i} が残っている");
    assert!(eng.fault_count(FaultKind::DiskSpace) > 0);
    assert_eq!(eng.fault_count(FaultKind::VocabSpace), 0, "空き不足を 「辞書が一杯」 と報告した");
    drop(t);
    drop(db);
    cleanup(&path);
}

/// 列の本体を伸ばせない時 (Tag の無い table): commit は Err、 Ok の row は全部読める。
/// (旧: 列を伸ばせない書き込みは捨てられ、 oplog に積んだ Tie だけが peer に届いていた)
#[test]
fn refused_column_growth_is_an_error() {
    let path = tmp("column");
    cleanup(&path);
    let mut db = Database::create(&path).unwrap();
    db.table("events").number("id").number("kind").bigint("at").primary_key("id").build().unwrap();
    let t = db.get_table("events").unwrap();
    let eng = db.engine();
    eng.set_space_margin(u64::MAX / 4);
    let mut ok = Vec::new();
    let mut rejected = None;
    for i in 0..2_000_000i64 {
        match t.insert().set("id", i).set("kind", i % 7).set("at", -i).commit() {
            Ok(e) => ok.push((i, e)),
            Err(SchemaError::WriteRejected(r)) => {
                assert_eq!(r, TieRejected::Fault(FaultKind::DiskSpace), "row {i}");
                rejected = Some(i);
                break;
            }
            Err(e) => panic!("row {i}: {e}"),
        }
    }
    let i = rejected.expect("列を伸ばすのを断っても commit が一度も Err を返さない");
    for &(j, e) in &ok {
        let got = (t.entity(e).get("id"), t.entity(e).get("kind"), t.entity(e).get("at"));
        assert_eq!(got, (Some(Value::Number(j)), Some(Value::Number(j % 7)), Some(Value::Number(-j))), "row {j}");
    }
    assert_eq!(t.where_eq("id", i).find_one().unwrap(), None, "拒否された row {i} が残っている");
    drop(t);
    drop(db);
    cleanup(&path);
}
