//! SQL の文の並び (`enchudb_sql::Database::execute`)。 入力を `;` で切って、 小さな DB に順に流す (16 文まで)。 文法の
//! 誤り・型の合わない値・無い表は Err で返ってよい。 panic しない。 最後に開き直して、 表の定義が読めることも見る。
#![no_main]
use enchudb_sql::Database;
use libfuzzer_sys::fuzz_target;
use std::sync::atomic::{AtomicU64, Ordering};

static N: AtomicU64 = AtomicU64::new(0);

fuzz_target!(|data: &[u8]| {
    let sql = String::from_utf8_lossy(data);
    let dir = std::env::temp_dir().join(format!("enchudb-fuzz-sql-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join(N.fetch_add(1, Ordering::Relaxed).to_string());
    let path = path.to_str().unwrap();
    let Ok(mut db) = Database::create_growable_tiny(path) else { return };
    for stmt in sql.split(';').take(16) {
        let _ = db.execute(stmt);
    }
    let tables = db.list_tables().len();
    drop(db);
    if let Ok(db) = Database::open(path) {
        assert_eq!(db.list_tables().len(), tables, "開き直すと表の数が変わった");
    }
    let _ = std::fs::remove_dir_all(path);
});
