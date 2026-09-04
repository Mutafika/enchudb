//! #259: schema 層の `Database::open` (rw) が、 書かずに閉じるだけでも relation 数 × fsync を
//! 払っていた (`load_schema` → `define_ref_in` → 無条件 `try_persist_tables`)。
//!
//! gate: ref relation を持つ DB を rw で開き直しても、 **open の時点では** `tables` sidecar が
//! 書き直されない (mtime 不変)。 Drop 側の `persist_schema` は本 issue の範囲外なので、
//! 判定は drop の前に行う。
use enchudb_schema::Database;

fn tmp_path(tag: &str) -> String {
    format!(
        "{}/enchudb-issue259-schema-{}-{}",
        std::env::temp_dir().display(),
        tag,
        std::process::id()
    )
}

fn tables_mtime(path: &str) -> std::time::SystemTime {
    std::fs::metadata(format!("{path}/tables")).expect("tables sidecar").modified().unwrap()
}

#[test]
fn rw_open_with_relations_does_not_rewrite_tables_sidecar() {
    let p = tmp_path("rw");
    let _ = enchudb_engine::db_files::remove_db(&p);
    {
        // kenning と同じ形: 8 本の ref relation
        let mut db = Database::create_growable_with_capacity(&p, 4096).unwrap();
        db.table("file").tag("path").build().unwrap();
        db.table("sym")
            .tag("name")
            .ref_to("r0", "file")
            .ref_to("r1", "file")
            .ref_to("r2", "file")
            .ref_to("r3", "file")
            .ref_to("r4", "file")
            .ref_to("r5", "file")
            .ref_to("r6", "file")
            .ref_to("r7", "file")
            .build()
            .unwrap();
    } // Drop で schema / tables を persist
    let before = tables_mtime(&p);
    std::thread::sleep(std::time::Duration::from_millis(30));

    let db = Database::open(&p).unwrap(); // rw: load_schema が relation を再登録する
    assert_eq!(tables_mtime(&p), before, "rw open の時点で tables sidecar が書き直されている");
    assert!(db.get_table("sym").is_some() && db.get_table("file").is_some(), "schema が復元されていない");
    drop(db);
    let _ = enchudb_engine::db_files::remove_db(&p);
}
