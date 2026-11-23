//! #1066 (#1046 の (1)): sync を有効にした DB は、 WAL 付きの session (`open_with_oplog` / `finish_with_oplog` の後) でしか、
//! ユーザの表の行を書けない。 build phase (`finish_with_oplog` の前) と `Database::open` (WAL なし) の insert / update / delete は
//! `WriteRejected(Fault(NoWal))` で、 何も変えない。 読みはできる。
//!
//! falsify: engine の `refuses_walless_sync_write` / `try_entity_in_row` の断りを外すと、 WAL なしの insert / update / delete が通る。
use enchudb_engine::{FaultKind, TieRejected};
use enchudb_schema::{Database, SchemaError, Value};

const WAL: usize = 4 * 1024 * 1024;

struct Rm(String);
impl Drop for Rm {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn tmp(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir()
        .join(format!("enchudb-schema-1066-{tag}-{}-{nanos}", std::process::id()))
        .to_str()
        .unwrap()
        .to_string()
}

fn refused<T: std::fmt::Debug>(r: Result<T, SchemaError>) -> bool {
    matches!(r, Err(SchemaError::WriteRejected(TieRejected::Fault(FaultKind::NoWal))))
}

#[test]
fn a_sync_db_writes_rows_only_with_the_wal() {
    let path = tmp("db");
    let _rm = Rm(path.clone());
    // build phase: sync を有効にした後は行を書けない
    let mut db = Database::create(&path).unwrap();
    db.table("notes").number("id").tag("body").primary_key("id").build().unwrap();
    db.enable_sync().unwrap();
    let in_build = refused(db.get_table("notes").unwrap().insert().set("id", 1i64).set("body", "a").commit());
    assert!(in_build, "build phase の sync する DB で insert が断られない");
    assert_eq!(db.get_table("notes").unwrap().all().count().unwrap(), 0, "断った insert が行を残した");
    // finish_with_oplog の後は書ける
    let x = {
        let db = db.finish_with_oplog(WAL).unwrap();
        let notes = db.get_table("notes").unwrap();
        notes.insert().set("id", 1i64).set("body", "a").commit().expect("WAL 付きで insert できない")
    };
    // WAL なしで開く: 読めるが書けない
    {
        let db = Database::open(&path).unwrap();
        let notes = db.get_table("notes").unwrap();
        assert_eq!(notes.entity(x).get("body"), Some(Value::Text("a".into())), "WAL なしで開いて読めない");
        let refusals = [
            ("insert", refused(notes.insert().set("id", 2i64).set("body", "b").commit())),
            ("update", refused(notes.entity(x).set("body", "c").commit())),
            ("delete", refused(notes.entity(x).delete())),
        ];
        for (what, ok) in &refusals {
            assert!(ok, "WAL なしで開いた sync する DB で {what} が断られない");
        }
        assert_eq!(notes.all().count().unwrap(), 1, "断った書き込みが行を増やした / 消した");
        assert_eq!(notes.entity(x).get("body"), Some(Value::Text("a".into())), "断った update が値を変えた");
    }
    // WAL 付きで開き直すと書ける
    let db = Database::open_with_oplog(&path, WAL).unwrap();
    let notes = db.get_table("notes").unwrap();
    notes.entity(x).set("body", "d").commit().expect("WAL 付きで開き直して update できない");
    notes.insert().set("id", 3i64).set("body", "e").commit().expect("WAL 付きで開き直して insert できない");
    assert_eq!(notes.entity(x).get("body"), Some(Value::Text("d".into())));
    assert_eq!(notes.all().count().unwrap(), 2);
}
