//! #533: 開く時に sidecar を読めない (中身が壊れているのではなく読み込みが失敗する) なら、 開くのを失敗させ、 file を
//! 書き換えない。 旧: 警告を 1 行出して無いものとして開き、 次の保存 (flush / Drop) が正しい file を空の中身で上書きした
//! (書き出しの返った table の定義が消えた)。
//!
//! 読み込みを失敗させるため、 sidecar を同じ名前の directory に置き換える (`read` が `EISDIR`。 権限を外すのと違い root
//! でも失敗する)。 一時的な `EIO` の形は `tests/fault_matrix.rs` (crashsim) が点ごとに見る。

use enchudb_engine::{Engine, ValueType};
use std::path::PathBuf;

fn tmp(tag: &str) -> String {
    let p = std::env::temp_dir().join(format!("enchu_issue533_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p.to_str().unwrap().to_string()
}

fn sidecar(db: &str, name: &str) -> PathBuf {
    enchudb_engine::db_files::path_for(db, name)
}

#[test]
fn unreadable_tables_sidecar_fails_the_open_and_keeps_the_file() {
    let db = tmp("tables");
    let e;
    {
        let mut eng = Engine::create_with_capacity(&db, 4096).unwrap();
        eng.define_table("t", 100).unwrap();
        eng.define_himo_in("t", "n", ValueType::Number, 0).unwrap();
        e = eng.entity_in("t").unwrap();
        eng.tie(e, "t.n", 42u32);
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
    }
    let path = sidecar(&db, enchudb_engine::db_files::TABLES);
    let bytes = std::fs::read(&path).expect("前提: tables の sidecar がある");
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();

    let r = Engine::open_standalone(&db);
    assert!(r.is_err(), "読めない tables の sidecar で開けた (table の定義を捨てて開いた)");
    drop(r);
    // 開くのをやめた engine は何も書かない: 置き換えた directory はそのまま、 書きかけの tmp も無い (Drop が保存しようと
    // すると tmp を書いてから directory への rename で失敗し、 tmp が残る)
    assert!(path.is_dir(), "開くのをやめた engine が tables を書き直した");
    assert!(!enchudb_engine::db_files::tmp_path_for(&path).exists(), "開くのをやめた engine が tables を書こうとした");

    std::fs::remove_dir(&path).unwrap();
    std::fs::write(&path, &bytes).unwrap();
    let eng = Engine::open_standalone(&db).unwrap();
    assert!(eng.list_tables().iter().any(|t| t.1 == "t"), "table t が無い");
    assert_eq!(eng.get(e, "t.n"), Some(42));
    drop(eng);
    let _ = std::fs::remove_dir_all(&db);
}
