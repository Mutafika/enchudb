//! 0.8.1 fix: short-lived CLI で oplog 経由 recover した時の entity 状態整合。
//!
//! 旧 behavior: `apply_oplog_op` の Tie/Content path で `entities.ensure_live`
//! も `table.next_local` 推進も呼ばれず → 次 open で entity_in が重複 eid を
//! 払い出す defect。 sinfo の sf CLI (= open → write → drop, sidecar persist
//! 機会無し) で表面化。
//!
//! 期待:
//! - drop 後 reopen で 既存 tie の value が見える (= himo recover)
//! - 既存 eid と新規 entity_in が衝突しない (= next_local + entities 復元)
//! - 既存 eid の `is_live` 相当が true (= entities.live bitmap 復元)

use enchudb_engine::{Engine, ValueType};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-recover-short-{}-{}-{}",
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
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.db.lock", path));
}

#[test]
fn reopen_after_concurrent_tie_recovers_himo_value() {
    let path = tmp_path("himo");
    cleanup(&path);

    // === build phase ===
    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "val", ValueType::Number, 0).unwrap();
    eng.flush().unwrap();

    // === concurrentize + 1 tie ===
    let eng = Engine::concurrentize_with_oplog(eng, 4 * 1024 * 1024).unwrap();
    let e1 = eng.entity_in("notes").unwrap();
    eng.tie_to(e1, "notes.val", 42);
    eng.oplog_commit();
    eng.flush_writes();
    // intentionally drop without persist_tables() — short-lived CLI を再現
    drop(eng);

    // === reopen ===
    let eng2 = Engine::open_concurrent_with_oplog(&path, 4 * 1024 * 1024).unwrap();
    let v = eng2.get_by_id(e1, 0);
    assert_eq!(v, Some(42), "himo value should be recovered from oplog");

    drop(eng2);
    cleanup(&path);
}

#[test]
fn reopen_after_concurrent_tie_advances_next_local() {
    let path = tmp_path("next_local");
    cleanup(&path);

    // === build phase ===
    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "val", ValueType::Number, 0).unwrap();
    eng.flush().unwrap();

    // === concurrentize + 3 ties ===
    let eng = Engine::concurrentize_with_oplog(eng, 4 * 1024 * 1024).unwrap();
    let e1 = eng.entity_in("notes").unwrap();
    let e2 = eng.entity_in("notes").unwrap();
    let e3 = eng.entity_in("notes").unwrap();
    eng.tie_to(e1, "notes.val", 10);
    eng.tie_to(e2, "notes.val", 20);
    eng.tie_to(e3, "notes.val", 30);
    eng.oplog_commit();
    eng.flush_writes();
    drop(eng);

    // === reopen + new entity_in ===
    let eng2 = Engine::open_concurrent_with_oplog(&path, 4 * 1024 * 1024).unwrap();
    let e4 = eng2.entity_in("notes").unwrap();

    // 0.8.0 以前は next_local が 0 のまま → e4 = e1 で衝突
    assert_ne!(e4, e1);
    assert_ne!(e4, e2);
    assert_ne!(e4, e3);

    // 旧 value も生きてる、 新規 tie も独立して効く
    eng2.tie_to(e4, "notes.val", 99);
    eng2.oplog_commit();
    eng2.flush_writes();

    assert_eq!(eng2.get_by_id(e1, 0), Some(10));
    assert_eq!(eng2.get_by_id(e2, 0), Some(20));
    assert_eq!(eng2.get_by_id(e3, 0), Some(30));
    assert_eq!(eng2.get_by_id(e4, 0), Some(99));

    drop(eng2);
    cleanup(&path);
}

#[test]
fn persist_tables_api_works_under_arc() {
    let path = tmp_path("persist_api");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "val", ValueType::Number, 0).unwrap();
    eng.flush().unwrap();

    let eng = Engine::concurrentize_with_oplog(eng, 4 * 1024 * 1024).unwrap();
    let _e1 = eng.entity_in("notes").unwrap();
    let _e2 = eng.entity_in("notes").unwrap();

    // Arc<Engine> でも persist_tables(&self) で sidecar を更新できる
    eng.persist_tables().expect("persist_tables should succeed");

    // 副作用: oplog 走査無しでも tables sidecar から next_local が復元される
    // ことを示すため、 oplog を削除してから reopen して衝突しないことを assert
    drop(eng);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
    let _ = std::fs::remove_file(format!("{}.db.lock", path));

    let eng2 = Engine::open_concurrent_with_oplog(&path, 4 * 1024 * 1024).unwrap();
    let e3 = eng2.entity_in("notes").unwrap();
    // sidecar から next_local = 2 が復元されている → e3 の local は 2 以上
    let local = enchudb_oplog::eid_local(e3);
    assert!(local >= 2, "next_local should be persisted via sidecar, got local={}", local);

    drop(eng2);
    cleanup(&path);
}
