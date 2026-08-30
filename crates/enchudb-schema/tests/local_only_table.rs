//! request19: schema builder の `.local_only()` — WAL の耐久性は使うが peer には
//! 配らない table を、 通常の table と同じ書き味で宣言できること。
//!
//! 「この端末で観測した事実」 (例: 「この path を、 まさに disk と突き合わせた」) を
//! 置く場所。 本体の行と同じ WAL / commit に載る必要がある一方、 相手に配ると嘘になる。

use enchudb_schema::{Database, Value};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-schema-local-only-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    for suf in ["", ".oplog", ".tables", ".schema", ".crc", ".db.lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

#[test]
fn local_only_table_round_trips_through_the_schema_layer() {
    let path = tmp_path("roundtrip");
    cleanup(&path);

    {
        let mut db = Database::create_with_capacity(&path, 4096).unwrap();
        db.table("notes").tag("body").with_capacity(64).build().unwrap();
        db.table("_seen")
            .local_only()
            .tag("path")
            .tag("hash")
            .with_capacity(64)
            .build()
            .unwrap();
        db.enable_sync().unwrap();
        let db = db.finish_with_oplog(8 * 1024 * 1024).unwrap();

        let notes = db.get_table("notes").expect("notes");
        notes.insert().set("body", "hello").commit().unwrap();
        let seen = db.get_table("_seen").expect("local-only table が handle から引けない");
        seen.insert().set("path", "a.txt").set("hash", "deadbeef").commit().unwrap();

        let rows = seen.all().find().unwrap();
        assert_eq!(rows.len(), 1, "local-only table に書いた行が読めない");
    }

    // reopen — schema blob の再 register が `_` 始まりで失敗しないこと
    // (`define_table` は reserved namespace を弾くので、 load_schema が
    //  `define_reserved_table` へ振り分けていないとここで落ちる)。
    {
        let db = Database::open(&path).expect("reopen");
        let seen = db.get_table("_seen").expect("reopen 後に local-only table が消えている");
        let rows = seen.all().find().unwrap();
        assert_eq!(rows.len(), 1, "local-only table の行が reopen で消えている");
        let e = seen.entity(rows[0]);
        assert_eq!(e.get("path"), Some(Value::Text("a.txt".into())));
    }

    cleanup(&path);
}

#[test]
fn local_only_table_is_not_bridged_to_peers() {
    let path = tmp_path("bridge");
    cleanup(&path);

    let mut db = Database::create_with_capacity(&path, 4096).unwrap();
    db.table("notes").number("n").with_capacity(64).build().unwrap();
    db.table("_seen").local_only().number("n").with_capacity(64).build().unwrap();
    db.enable_sync().unwrap();
    let db = db.finish_with_oplog(8 * 1024 * 1024).unwrap();

    const USER_MARKER: i64 = 0x0123_4567;
    const LOCAL_MARKER: i64 = 0x0BAD_F00D;

    db.get_table("notes").unwrap().insert().set("n", USER_MARKER).commit().unwrap();
    db.get_table("_seen").unwrap().insert().set("n", LOCAL_MARKER).commit().unwrap();

    let eng = db.engine();
    eng.flush_writes();
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
    std::thread::sleep(std::time::Duration::from_millis(250));
    eng.transfer_oplog_to_sync_ops();

    let payloads = eng.pending_sync_ops(0);
    let contains = |m: i64| {
        let pat = (m as u32).to_le_bytes();
        payloads.iter().any(|p| p.windows(4).any(|w| w == pat))
    };
    assert!(contains(USER_MARKER), "user table の write が bridge されていない — 前提が壊れている");
    assert!(
        !contains(LOCAL_MARKER),
        "local-only table の write が `_sync_ops` に流れている (peer に配られてしまう)"
    );

    cleanup(&path);
}
