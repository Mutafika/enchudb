//! 0.8.0 Phase 1: consumer thread が fsync 後に `_sync_ops` へ自動転送する。
//!
//! 0.7.0 では user が `transfer_oplog_to_sync_ops()` を手動で呼ぶ必要があった。
//! 0.8.0 で consumer thread に自動化を組み込み、 user は何もしなくても
//! tie 後 fsync_interval (100ms) 経過すれば pending_sync_ops で取れる。

use enchudb_engine::{Engine, ValueType};
use std::sync::Arc;
use std::time::Duration;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-phase1-{}-{}-{}",
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
fn consumer_thread_auto_transfers_to_sync_ops() {
    let path = tmp_path("auto_transfer");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();

    // tie するだけ、 transfer_oplog_to_sync_ops() は user として呼ばない
    for i in 1u32..=5 {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();

    // consumer thread の fsync_interval = 100ms、 余裕を持って 300ms 待つ
    std::thread::sleep(Duration::from_millis(300));

    // pending_sync_ops は自動転送されてるはず
    let pending = eng.pending_sync_ops(0);
    assert!(
        !pending.is_empty(),
        "consumer thread should have auto-transferred records, got 0"
    );
    assert!(
        pending.len() >= 5,
        "should have at least 5 ties auto-transferred, got {}",
        pending.len()
    );

    let lsn = eng.current_sync_lsn();
    assert!(lsn >= 5, "lsn should advance, got {lsn}");

    cleanup(&path);
}

#[test]
fn manual_transfer_remains_idempotent_after_auto() {
    // 0.7.0 互換: user が手動で transfer_oplog_to_sync_ops() を呼んでも壊れない
    // (= 既に自動転送済みの記録は 2 回目以降 no-op になるはず)
    let path = tmp_path("manual_compat");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();

    for i in 1u32..=3 {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();
    std::thread::sleep(Duration::from_millis(300));

    // 自動転送済みのはず
    let auto_lsn = eng.current_sync_lsn();
    assert!(auto_lsn >= 3);

    // 手動 transfer は idempotent (= 既に転送済なので 0 件)
    let manual = eng.transfer_oplog_to_sync_ops();
    assert_eq!(manual, 0, "manual transfer after auto should be no-op, got {manual}");
    assert_eq!(eng.current_sync_lsn(), auto_lsn, "lsn should not double-advance");

    cleanup(&path);
}
