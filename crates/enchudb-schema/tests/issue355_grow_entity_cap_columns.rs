//! #355: `grow_entity_cap` で entity cap を広げた後も、 列が伸びずに
//! `WriteRejected(Fault(DiskSpace))` で断られる (空きは十分)。

use enchudb_schema::Database;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue355-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    for suf in ["", ".oplog", ".tables", ".schema", ".crc", ".db.lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

/// 作成時 cap 4096 から倍々で伸ばしながら 4 万行書ける。
#[test]
fn rows_past_the_grown_cap_are_written() {
    let path = tmp_path("double");
    cleanup(&path);
    let mut b = Database::create_growable_with_capacity(&path, 4096).unwrap();
    b.table("rows").tag("id").tag("v").primary_key("id").build().unwrap();
    let db = b.finish_with_oplog(64 << 20).unwrap();
    for i in 0..40_000u32 {
        let write = || db.get_table("rows").unwrap().upsert().set("id", format!("r{i}")).set("v", "x").commit();
        match write() {
            Ok(_) => {}
            Err(e) if e.to_string().contains("exhausted") => {
                let eng = db.engine();
                eng.grow_entity_cap(eng.max_entities() * 2).unwrap();
                write().unwrap_or_else(|e| panic!("row {i}: retry after grow: {e:?}"));
            }
            Err(e) => panic!("row {i}: {e:?} (max_entities {})", db.engine().max_entities()),
        }
    }
    drop(db);
    cleanup(&path);
}

/// 最初に大きく伸ばし、 sync 有効 (版数列 / tombstone 列あり) で cap の外まで書く。
/// 消した row も含め、 reopen 後に全部読める。 key は Number (Tag の辞書は作成時 cap × 16 で
/// 焼かれ、 grow では伸びない — 別の上限なのでここでは当てない)。
#[test]
fn grown_cap_covers_version_and_tombstone_columns_across_reopen() {
    use enchudb_schema::Value;
    const N: i64 = 40_000;
    let path = tmp_path("sync");
    cleanup(&path);
    {
        let mut b = Database::create_growable_with_capacity(&path, 4096).unwrap();
        b.table("rows").number("id").number("n").primary_key("id").build().unwrap();
        b.enable_sync().unwrap();
        let db = b.finish_with_oplog(64 << 20).unwrap();
        db.engine().grow_entity_cap(1_000_000).unwrap();
        let t = db.get_table("rows").unwrap();
        for i in 0..N {
            t.upsert().set("id", i).set("n", i).commit()
                .unwrap_or_else(|e| panic!("row {i}: {e:?}"));
        }
        let last = t.where_eq("id", N - 1).find_one().unwrap().unwrap();
        t.entity(last).delete().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let t = db.get_table("rows").unwrap();
    for i in [0, 4096, 16_379, 16_380, N - 2] {
        let e = t.where_eq("id", i).find_one().unwrap().unwrap_or_else(|| panic!("row {i} missing"));
        assert_eq!(t.entity(e).get("n"), Some(Value::Number(i)), "row {i}");
    }
    assert_eq!(t.where_eq("id", N - 1).find_one().unwrap(), None);
    drop(t);
    drop(db);
    cleanup(&path);
}
