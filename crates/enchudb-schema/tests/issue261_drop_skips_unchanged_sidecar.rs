//! #261: 書き込みゼロの rw session (= 開いて読んで閉じるだけ) が、 `impl Drop for Database`
//! の無条件 `persist_schema()` で `schema` fsync + engine flush を毎回払っていた。
//! 1 コマンド 1 process の消費側 (kenning の増分 update、 `sf` の条件付き更新) では
//! これが drop の 15〜21 ms として乗る。
//!
//! gate は 3 本:
//!   1. 何も変えていない rw session は sidecar を書き直さない (mtime 不変)
//!   2. schema を変えた session は `schema` を書き直す (= skip が効きすぎていない)
//!   3. 行を足した session は `tables` を書き直す (next_local が動くため)
//!
//! [#259] は同じ DB の **open 側**を見ている (`load_schema` → `define_ref_in`)。
//! こちらは **drop 側**。
//!
//! [#259]: https://github.com/Mutafika/enchudb/issues/259
use enchudb_schema::{Database, Value};

fn tmp_path(tag: &str) -> String {
    format!(
        "{}/enchudb-issue261-{}-{}",
        std::env::temp_dir().display(),
        tag,
        std::process::id()
    )
}

fn mtime(path: &str, sidecar: &str) -> std::time::SystemTime {
    std::fs::metadata(format!("{path}/{sidecar}"))
        .unwrap_or_else(|e| panic!("{sidecar} sidecar: {e}"))
        .modified()
        .unwrap()
}

/// kenning / sf の形: 数 table + relation + 実データ入り。
fn seed(p: &str) {
    let _ = enchudb_engine::db_files::remove_db(p);
    let mut db = Database::create_growable_with_capacity(p, 4096).unwrap();
    db.table("file").tag("path").primary_key("path").build().unwrap();
    db.table("sym").tag("name").leaf("body").ref_to("f", "file").build().unwrap();
    let file = db.get_table("file").unwrap();
    let f0 = file.insert().set("path", "a.rs").commit().unwrap();
    let sym = db.get_table("sym").unwrap();
    for i in 0..64 {
        sym.insert()
            .set("name", format!("s{i}"))
            .set("body", format!("fn s{i}() {{}}"))
            .set("f", Value::Ref(f0))
            .commit()
            .unwrap();
    }
}

/// sidecar の mtime が動いたかを見るには、 seed の drop から測定までに
/// timestamp 分解能 (APFS は 1 ns だが、 同一 ms 内の判定を確実にするため) を跨がせる。
fn settle() {
    std::thread::sleep(std::time::Duration::from_millis(30));
}

#[test]
fn write_zero_rw_session_does_not_rewrite_sidecars() {
    let p = tmp_path("writezero");
    seed(&p);
    let before = ["schema", "tables"].map(|s| mtime(&p, s));
    settle();

    {
        // 読むだけ。 query も通して 「読みが sidecar を dirty にしない」 ことまで見る。
        let db = Database::open(&p).unwrap();
        let sym = db.get_table("sym").unwrap();
        assert_eq!(sym.where_eq("name", "s7").count().unwrap(), 1);
    } // ← Drop: persist_schema → engine flush → Engine::drop

    let after = ["schema", "tables"].map(|s| mtime(&p, s));
    assert_eq!(after, before, "書き込みゼロの rw session が sidecar を書き直している (#261)");

    // skip しても内容は生きている
    let db = Database::open_readonly(&p).unwrap();
    assert!(db.get_table("sym").is_some() && db.get_table("file").is_some());
    drop(db);
    let _ = enchudb_engine::db_files::remove_db(&p);
}

#[test]
fn schema_change_still_rewrites_schema_sidecar() {
    let p = tmp_path("schemachange");
    seed(&p);
    let before = mtime(&p, "schema");
    settle();

    {
        let mut db = Database::open(&p).unwrap();
        db.table("sym").tag("name").leaf("body").ref_to("f", "file").leaf("doc").build().unwrap();
    }

    assert_ne!(mtime(&p, "schema"), before, "schema を変えたのに schema sidecar が更新されていない");
    let db = Database::open(&p).unwrap();
    assert!(
        db.get_table("sym").unwrap().columns().iter().any(|c| c.name == "doc"),
        "追加列が reopen で消えている"
    );
    drop(db);
    let _ = enchudb_engine::db_files::remove_db(&p);
}

#[test]
fn row_insert_still_rewrites_tables_sidecar() {
    let p = tmp_path("rowinsert");
    seed(&p);
    let before = mtime(&p, "tables");
    settle();

    {
        let db = Database::open(&p).unwrap();
        let sym = db.get_table("sym").unwrap();
        sym.insert().set("name", "added").set("body", "fn added() {}").commit().unwrap();
    }

    // next_local が進むので tables sidecar は必ず変わる。 ここが変わらないと
    // reopen 時に生きた eid を再払い出しする (#117 と同じ silent 破壊)。
    assert_ne!(mtime(&p, "tables"), before, "行を足したのに tables sidecar が更新されていない");
    let db = Database::open_readonly(&p).unwrap();
    assert_eq!(db.get_table("sym").unwrap().where_eq("name", "added").count().unwrap(), 1);
    drop(db);
    let _ = enchudb_engine::db_files::remove_db(&p);
}
