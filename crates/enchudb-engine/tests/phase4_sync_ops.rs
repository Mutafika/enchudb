//! 0.7.0 Phase 4: watermark 駆動 reclaim の動作確認。
//!
//! - enable_sync で `_sync_ops` / `_sync_peers` reserved table 定義
//! - 通常の tie_async で oplog に書く
//! - transfer_oplog_to_sync_ops で `_sync_ops` table へ転送
//! - pending_sync_ops で since_lsn 以降を取り出せる
//! - ack_sync(peer, lsn) で watermark 前進
//! - reclaim_sync_ops で古い row が消える

use enchudb_engine::{Engine, HimoType};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-phase4-{}-{}-{}",
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
fn enable_sync_creates_reserved_tables() {
    let path = tmp_path("enable_sync");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    assert!(!eng.sync_tables_enabled(), "sync tables should be disabled initially");

    eng.enable_sync_tables().unwrap();
    assert!(eng.sync_tables_enabled());

    // 2 度目は idempotent
    eng.enable_sync_tables().unwrap();
    assert!(eng.sync_tables_enabled());

    // engine 内部 list_tables には _sync_ops / _sync_peers が居る
    let tables: Vec<String> = eng.list_tables().into_iter().map(|(_, n, _, _)| n).collect();
    assert!(tables.iter().any(|n| n == "_sync_ops"));
    assert!(tables.iter().any(|n| n == "_sync_peers"));

    // list_user_tables からは除外される
    let user_tables: Vec<String> = eng.list_user_tables().into_iter().map(|(_, n, _, _)| n).collect();
    assert!(!user_tables.iter().any(|n| n.starts_with('_')));

    cleanup(&path);
}

#[test]
fn transfer_and_pending_roundtrip() {
    let path = tmp_path("transfer");
    cleanup(&path);

    use std::sync::Arc;
    // concurrent + WAL モードで起動。 user table を作ってから enable_sync。
    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", HimoType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();

    // 3 件 tie
    for i in 1u32..=3 {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();

    // _sync_ops に転送
    let count = eng.transfer_oplog_to_sync_ops();
    assert!(count > 0, "should transfer some records, got {count}");

    // pending_sync_ops(since=0) で全件取れる
    let pending = eng.pending_sync_ops(0);
    assert_eq!(pending.len(), count, "pending should equal transferred count");
    // 各 payload は non-empty (= wire bytes)
    for p in &pending {
        assert!(!p.is_empty());
    }

    // 2 度目の transfer は idempotent (= 既に転送済み、 0 件)
    let count2 = eng.transfer_oplog_to_sync_ops();
    assert_eq!(count2, 0, "second transfer should be idempotent");

    cleanup(&path);
}

#[test]
fn ack_and_watermark_drive_reclaim() {
    let path = tmp_path("reclaim");
    cleanup(&path);

    use std::sync::Arc;
    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", HimoType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();

    // 5 件 tie
    for i in 1u32..=5 {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();

    let transferred = eng.transfer_oplog_to_sync_ops();
    assert!(transferred >= 5, "should transfer at least the 5 ties");

    let initial_pending = eng.pending_sync_ops(0).len();

    // watermark = 0 (= 誰も ack してない、 reclaim できない)
    assert_eq!(eng.sync_watermark(), 0);
    let purged_before_ack = eng.reclaim_sync_ops();
    assert_eq!(purged_before_ack, 0, "reclaim before any ack should purge 0");

    // peer 1 が lsn=3 まで ack
    eng.ack_sync(1, 3).unwrap();
    assert_eq!(eng.sync_watermark(), 3);

    // reclaim → lsn < 3 (= lsn 1, 2) が消える
    let purged = eng.reclaim_sync_ops();
    assert!(purged >= 2, "should purge at least 2 rows (lsn 1, 2), got {purged}");

    // 残りは lsn >= 3
    let remaining = eng.pending_sync_ops(0).len();
    assert!(remaining < initial_pending, "remaining should be less than initial");

    // since=3 で取り出すと lsn=4..= が出る
    let after_3 = eng.pending_sync_ops(3);
    assert!(!after_3.is_empty());

    cleanup(&path);
}

#[test]
fn reserved_table_writes_do_not_reappear_in_oplog() {
    // 0.7.0 Phase 4: `_sync_ops` への internal write は oplog に再 append されない。
    // これがないと oplog → _sync_ops → oplog 再書込み → ... の無限ループになる。
    let path = tmp_path("no_oplog_loop");
    cleanup(&path);

    use std::sync::Arc;
    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", HimoType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();

    let e = eng.entity_in("notes").unwrap();
    eng.tie_to(e, "notes.note", 42);
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();

    let oplog_head_before = eng.oplog().unwrap().head();
    let _ = eng.transfer_oplog_to_sync_ops();
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    let oplog_head_after = eng.oplog().unwrap().head();

    // transfer 後の oplog head 増分は Commit 1 件分のみ (= reserved table の Tie
    // record は append されてない)。 厳密な byte 比較は record format 依存なので、
    // 1 KB 以内に収まることを確認 (Commit 1 record = ~112 byte)。
    let delta = oplog_head_after.saturating_sub(oplog_head_before);
    assert!(delta < 1024,
            "reserved table writes should not blow up oplog, delta={delta} bytes");

    cleanup(&path);
}
