//! issue #52: schema 層の `.schema` sidecar fail-readable / self-heal の regression。
//!
//! 検証:
//! - `.schema` sidecar が破損していても `Database::open` が成功する
//! - 破損 `.schema` が `.schema.corrupt-<ts>` に rename される
//! - engine 側の `.tables` が無傷なら synthesize で table 復元される
//! - `.schema.tmp` 残骸が open 時に削除される

use enchudb_schema::{Database, ColumnType};
use std::fs;
use std::path::PathBuf;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue52-schema-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup_all(path: &str) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.oplog", path));
    let _ = fs::remove_file(format!("{}.tables", path));
    let _ = fs::remove_file(format!("{}.tables.tmp", path));
    let _ = fs::remove_file(format!("{}.schema", path));
    let _ = fs::remove_file(format!("{}.schema.tmp", path));
    let _ = fs::remove_file(format!("{}.crc", path));
    let _ = fs::remove_file(format!("{}.db.lock", path));
    let parent = std::path::Path::new(path).parent().unwrap_or(std::path::Path::new("/tmp"));
    let stem = std::path::Path::new(path).file_name().unwrap().to_string_lossy().to_string();
    if let Ok(entries) = fs::read_dir(parent) {
        for ent in entries.flatten() {
            let n = ent.file_name().to_string_lossy().to_string();
            if n.starts_with(&stem) && n.contains(".corrupt-") {
                let _ = fs::remove_file(ent.path());
            }
        }
    }
}

#[test]
fn open_recovers_from_corrupt_schema_sidecar() {
    let path = tmp_path("corrupt-schema");
    cleanup_all(&path);

    {
        let mut db = Database::create(&path).unwrap();
        db.table("articles")
            .column("url", ColumnType::Tag)
            .build()
            .unwrap();
        // drop で persist
    }

    // `.schema` を破損
    let schema_path = format!("{}.schema", path);
    assert!(std::path::Path::new(&schema_path).exists(), ".schema should exist after build/drop");
    fs::write(&schema_path, b"!!!corrupted-schema-blob!!!").unwrap();

    // open: fail-readable → 成功 + .schema.corrupt-<ts> に rename
    let result = Database::open(&path);
    assert!(
        result.is_ok(),
        "Database::open should succeed with corrupt .schema, got: {:?}",
        result.err()
    );
    let db = result.unwrap();

    // engine `.tables` から articles が synthesize で復元されている
    let articles = db.get_table("articles");
    assert!(
        articles.is_some(),
        "articles should be recovered via engine synthesize"
    );

    // .schema.corrupt-<ts> backup が存在
    let parent = std::path::Path::new(&path).parent().unwrap_or(std::path::Path::new("/tmp"));
    let stem = std::path::Path::new(&path).file_name().unwrap().to_string_lossy().to_string();
    let backup_found = fs::read_dir(parent)
        .unwrap()
        .flatten()
        .any(|ent| {
            let n = ent.file_name().to_string_lossy().to_string();
            n.starts_with(&stem) && n.contains(".schema.corrupt-")
        });
    assert!(backup_found, "expected .schema.corrupt-<ts> backup file");

    cleanup_all(&path);
}

#[test]
fn open_self_heals_stale_schema_tmp() {
    let path = tmp_path("self-heal-schema-tmp");
    cleanup_all(&path);

    {
        let mut db = Database::create(&path).unwrap();
        db.table("foo")
            .column("name", ColumnType::Tag)
            .build()
            .unwrap();
    }

    // `.schema.tmp` 残骸を意図的に置く
    let tmp: PathBuf = PathBuf::from(format!("{}.schema.tmp", path));
    fs::write(&tmp, b"crashed persist residue").unwrap();
    assert!(tmp.exists());

    // open
    let _db = Database::open(&path).unwrap();

    // .tmp 削除確認
    assert!(
        !tmp.exists(),
        "stale .schema.tmp should be removed at open"
    );

    cleanup_all(&path);
}
