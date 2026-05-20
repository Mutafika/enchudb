//! β-heavy phase 2: drop_table API テスト。

#![cfg(not(target_arch = "wasm32"))]

use enchudb_engine::{Engine, HimoType};

fn tmp(tag: &str) -> String {
    let p = format!(
        "/tmp/enchudb-drop-table-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    cleanup(&p);
    p
}

fn cleanup(p: &str) {
    for suf in ["", ".wal", ".crc", ".db.lock", ".tables", ".positions"] {
        let _ = std::fs::remove_file(format!("{}{}", p, suf));
    }
    // table column files (任意)
    for name in ["users", "posts", "drop_me"] {
        let _ = std::fs::remove_file(format!("{}.t.{}.col", p, name));
    }
}

#[test]
fn drop_table_unlinks_column_file() {
    let p = tmp("unlink_col");

    let mut eng = Engine::create_standalone(&p).unwrap();
    eng.define_table("drop_me", 100).unwrap();
    eng.define_himo_in("drop_me", "v", HimoType::Number, 10).unwrap();
    let e = eng.entity_in("drop_me").unwrap();
    eng.tie(e, "drop_me.v", 7);
    eng.flush().unwrap();

    let col_path = format!("{}.t.drop_me.col", p);
    assert!(
        std::fs::metadata(&col_path).is_ok(),
        "column file should exist before drop: {}",
        col_path
    );

    eng.drop_table("drop_me").unwrap();
    assert!(
        std::fs::metadata(&col_path).is_err(),
        "column file should be unlinked after drop"
    );
    assert!(
        !eng.list_tables().iter().any(|(_, n, _, _)| n == "drop_me"),
        "drop_me should be removed from list_tables"
    );

    cleanup(&p);
}

#[test]
fn drop_table_rejects_anonymous() {
    let p = tmp("reject_anon");
    let mut eng = Engine::create_standalone(&p).unwrap();
    let r = eng.drop_table("anonymous");
    assert!(r.is_err());
    cleanup(&p);
}

#[test]
fn drop_table_rejects_unknown() {
    let p = tmp("reject_unknown");
    let mut eng = Engine::create_standalone(&p).unwrap();
    let r = eng.drop_table("missing");
    assert!(r.is_err());
    cleanup(&p);
}

#[test]
fn drop_table_then_reopen_keeps_other_tables() {
    let p = tmp("reopen_other");

    {
        let mut eng = Engine::create_standalone(&p).unwrap();
        eng.define_table("users", 100).unwrap();
        eng.define_table("posts", 100).unwrap();
        eng.define_himo_in("users", "name", HimoType::Number, 10).unwrap();
        eng.define_himo_in("posts", "title", HimoType::Number, 10).unwrap();
        let u = eng.entity_in("users").unwrap();
        let pst = eng.entity_in("posts").unwrap();
        eng.tie(u, "users.name", 5);
        eng.tie(pst, "posts.title", 7);
        eng.drop_table("users").unwrap();
        eng.flush().unwrap();
    }

    {
        let eng = Engine::open_standalone(&p).unwrap();
        let tables: Vec<String> = eng
            .list_tables()
            .iter()
            .map(|(_, n, _, _)| n.clone())
            .collect();
        assert!(!tables.contains(&"users".to_string()));
        assert!(tables.contains(&"posts".to_string()));
        // posts data は残る
        assert_eq!(eng.pull_raw("posts.title", 7).len(), 1);
    }

    cleanup(&p);
}

#[test]
fn table_column_file_path_query() {
    let p = tmp("query_path");
    let mut eng = Engine::create_standalone(&p).unwrap();
    eng.define_table("users", 100).unwrap();
    assert_eq!(
        eng.table_column_file("users"),
        Some(format!("{}.t.users.col", p))
    );
    assert_eq!(eng.table_column_file("nonexistent"), None);
    cleanup(&p);
}
