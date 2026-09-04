//! #259: **`define_ref_in` が idempotent な再登録でも `tables` sidecar を fsync していた**。
//!
//! schema 層の `Database::open` (rw) は `load_schema` で sidecar から復元した各 relation を
//! `define_ref_in` で engine に再登録する。 fk entry が既に登録済みでも末尾で無条件に
//! `try_persist_tables()` を呼んでいたため、 relation 1 本につき fsync 1 回 (APFS ~6 ms) が
//! rw open の定数コストになっていた (kenning: relation 8 本で open 42〜69 ms、 修正後 1 ms)。
//! persist を「entry を新規に push した時だけ」に変えた。
//!
//! gate は 2 つ:
//! 1. reopen 後に既存 relation を再登録しても `tables` sidecar は書き直されない (mtime 不変)
//! 2. 新規 relation は今まで通り persist され、 reopen 後も残っている (退行防止)
use enchudb_engine::{db_files, Engine, ValueType};
use std::time::SystemTime;

fn tmp(tag: &str) -> String {
    let p = format!("{}/enchudb-issue259-{}-{}", std::env::temp_dir().display(), tag, std::process::id());
    let _ = db_files::remove_db(&p);
    p
}

fn tables_mtime(path: &str) -> SystemTime {
    std::fs::metadata(db_files::path_for(path, db_files::TABLES)).expect("tables sidecar").modified().unwrap()
}

const RELS: u32 = 8; // kenning と同じ本数

/// 2 table + `RELS` 本の ref relation を持つ DB を作って閉じる。
fn build(path: &str) {
    let mut eng = Engine::create_with_capacity(path, 4096).unwrap();
    eng.define_table("file", 1024).unwrap();
    eng.define_table("sym", 1024).unwrap();
    for i in 0..RELS {
        eng.define_ref_in("sym", &format!("r{i}"), "file").unwrap();
    }
    eng.flush().unwrap();
}

/// `load_schema` が rw open のたびに行う再登録を engine API で再現する。
fn reregister(eng: &mut Engine) {
    for i in 0..RELS {
        let _ = eng.define_himo_in("sym", &format!("r{i}"), ValueType::Ref, 0);
        eng.define_ref_in("sym", &format!("r{i}"), "file").unwrap();
    }
}

#[test]
fn reregistering_existing_relations_does_not_rewrite_tables_sidecar() {
    let p = tmp("idempotent");
    build(&p);
    let before = tables_mtime(&p);
    // mtime 粒度が粗い FS でも「書き直された」が確実に見えるよう間を空ける
    std::thread::sleep(std::time::Duration::from_millis(30));

    let mut eng = Engine::open_standalone(&p).unwrap();
    reregister(&mut eng);
    assert_eq!(tables_mtime(&p), before, "既存 relation の再登録で tables sidecar が書き直されている (fsync × {RELS})");
    // 再登録は idempotent: relation は増えていない
    assert_eq!(eng.fk_refs_for_table_named("sym").len(), RELS as usize);
    drop(eng);
    let _ = db_files::remove_db(&p);
}

#[test]
fn new_relation_is_still_persisted_and_survives_reopen() {
    let p = tmp("new");
    build(&p);
    let before = tables_mtime(&p);
    std::thread::sleep(std::time::Duration::from_millis(30));

    let mut eng = Engine::open_standalone(&p).unwrap();
    eng.define_ref_in("sym", "extra", "file").unwrap();
    assert_ne!(tables_mtime(&p), before, "新規 relation が persist されていない");
    drop(eng);

    let eng = Engine::open_standalone(&p).unwrap();
    let refs = eng.fk_refs_for_table_named("sym");
    assert_eq!(refs.len(), RELS as usize + 1, "reopen 後に新規 relation が消えている: {refs:?}");
    assert!(refs.iter().any(|(h, t)| h.ends_with("extra") && t == "file"), "extra → file が無い: {refs:?}");
    drop(eng);
    let _ = db_files::remove_db(&p);
}
