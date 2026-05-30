//! 0.8.7 fix: engine 直で構築した DB (= schema crate を経由していない)
//! を `Database::open` した時 panic しないこと (= mlbpulse の 4.5M pitch DB の再現)。
//!
//! 旧 behavior: `Database::open` は `__enchu_schema_meta__` marker himo の存在を
//! 前提にしていたが、 engine 直構築 DB には marker が無いため、 query 実行時に
//! `ensure_schema_entity` が `eng.entity()` (= anonymous) を呼び panic:
//! ```
//! anonymous table is closed (define_table was called);
//! use entity_in('<table>') instead of entity()
//! ```
//!
//! 修正後: `.schema` sidecar (= 0.8.7 以降の正規 path) が無く、 legacy blob も
//! 無い engine 直 DB でも、 engine の `.tables` + himo_types から fallback 復元
//! して query 可能になる (PK は不明扱い、 他の column type は復元される)。

use enchudb_engine::{Engine, HimoType};
use enchudb_schema::Database;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-engine-built-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
    let _ = std::fs::remove_file(format!("{}.tables", path));
    let _ = std::fs::remove_file(format!("{}.schema", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.db.lock", path));
}

#[test]
fn database_open_works_on_engine_built_db_without_schema_marker() {
    let path = tmp_path("no_marker");
    cleanup(&path);

    // === phase 1: engine 直で table を構築 (= mlbpulse の build phase 相当) ===
    {
        let mut eng = Engine::create_standalone(&path).unwrap();
        eng.define_table("pitches", 100).unwrap();
        eng.define_himo_in("pitches", "pitch_type", HimoType::Tag, 0).unwrap();
        eng.define_himo_in("pitches", "velo", HimoType::Number, 0).unwrap();
        // tie
        let e1 = eng.entity_in("pitches").unwrap();
        eng.tie_text(e1, "pitches.pitch_type", "FF");
        eng.tie(e1, "pitches.velo", 95);
        let e2 = eng.entity_in("pitches").unwrap();
        eng.tie_text(e2, "pitches.pitch_type", "CH");
        eng.tie(e2, "pitches.velo", 82);
        eng.flush().unwrap();
        // marker himo は 一切定義していない
    }

    // === phase 2: Database::open → query しても panic しない ===
    let db = Database::open(&path).expect("Database::open should succeed without marker");
    let pitches = db
        .get_table("pitches")
        .expect("pitches table should be recovered from engine sidecar");

    // 複合 schema 情報の確認: column 復元
    let cols = pitches.columns();
    let col_names: Vec<String> = cols.iter().map(|c| c.name.clone()).collect();
    assert!(col_names.iter().any(|n| n == "pitch_type"));
    assert!(col_names.iter().any(|n| n == "velo"));

    // query が panic しないこと
    let ff_count = pitches.where_eq("pitch_type", "FF").count().unwrap();
    assert_eq!(ff_count, 1);

    let velo_95 = pitches.where_eq("velo", 95i64).find().unwrap();
    assert_eq!(velo_95.len(), 1);

    drop(db);
    cleanup(&path);
}

#[test]
fn database_create_and_open_writes_schema_sidecar() {
    let path = tmp_path("sidecar");
    cleanup(&path);

    // === Database 経由で構築 ===
    {
        let mut db = Database::create(&path).unwrap();
        db.table("users")
            .number("id")
            .tag("name")
            .primary_key("id")
            .build()
            .unwrap();
        // explicit drop → persist_schema → .schema sidecar 書き出し
    }

    // === .schema sidecar が存在 ===
    let schema_sidecar = format!("{}.schema", path);
    assert!(
        std::path::Path::new(&schema_sidecar).exists(),
        "schema sidecar should exist at {}",
        schema_sidecar,
    );

    // === reopen で sidecar 経由復元 ===
    let db2 = Database::open(&path).unwrap();
    let users = db2.get_table("users").unwrap();
    let cols = users.columns();
    let pk_col = cols.iter().find(|c| c.is_pk).unwrap();
    assert_eq!(pk_col.name, "id", "PK should be preserved via .schema sidecar");

    drop(db2);
    cleanup(&path);
}
