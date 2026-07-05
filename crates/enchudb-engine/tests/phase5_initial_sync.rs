//! 0.7.0 Phase 5: initial sync (snapshot transfer + lsn mark)。
//!
//! 0.7.0 では既存の `snapshot_export` を使って snapshot を file copy で取って、
//! receiver 側で `current_sync_lsn` を取って `ack_sync` で watermark 進める
//! 流れを engine API レベルで成立させる。 transport wire は別 phase で拡張。

use enchudb_engine::{Engine, ValueType};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-phase5-{}-{}-{}",
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
    let _ = std::fs::remove_file(format!("{}.tables.tmp", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.db.lock", path));
}

#[test]
fn snapshot_export_carries_sync_state() {
    // peer A: data を書く → snapshot export → snapshot 時点の lsn を控える
    // peer B: snapshot から open → ack で watermark 進める → 以降の sync 続行
    use std::sync::Arc;

    let path_a = tmp_path("peer_a");
    let path_b = tmp_path("peer_b_snapshot");
    cleanup(&path_a);
    cleanup(&path_b);

    // === peer A: 初期 data 投入 ===
    let mut eng_a = Engine::create_with_capacity(&path_a, 65_536).unwrap();
    eng_a.define_table("notes", 1000).unwrap();
    eng_a.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng_a.enable_sync_tables().unwrap();
    let eng_a: Arc<Engine> = Engine::concurrentize_with_oplog(eng_a, 16 * 1024 * 1024).unwrap();

    for i in 1u32..=10 {
        let e = eng_a.entity_in("notes").unwrap();
        eng_a.tie_to(e, "notes.note", i);
    }
    eng_a.oplog_commit();
    eng_a.flush_writes();
    eng_a.oplog_sync().unwrap();

    // _sync_ops に転送 (= sync 経路の正規化)
    eng_a.transfer_oplog_to_sync_ops();
    let snapshot_lsn = eng_a.current_sync_lsn();
    assert!(snapshot_lsn >= 10, "expected at least 10 records, got lsn={snapshot_lsn}");

    // snapshot を export (= file copy)
    eng_a.snapshot_export(&path_b).unwrap();

    // === peer B: snapshot から open + initial sync 完了マーク ===
    // peer B の peer_id を 2 とする (= A は 1 と仮定)
    drop(eng_a); // writer lock 解放
    let mut eng_b = Engine::open_standalone(&path_b).unwrap();
    eng_b.set_peer_id(2);
    let eng_b: Arc<Engine> = Engine::concurrentize_with_oplog(eng_b, 16 * 1024 * 1024).unwrap();

    // snapshot 時点の lsn を「peer A から受け取った最後の record」 として ack
    eng_b.ack_sync(1, snapshot_lsn).unwrap();

    // watermark = peer 1 の consumed_lsn (= snapshot_lsn)
    assert_eq!(eng_b.sync_watermark(), snapshot_lsn);

    // reclaim で snapshot に含まれた lsn は purge できる
    let purged = eng_b.reclaim_sync_ops();
    assert!(purged >= 5, "should purge at least half of snapshot records, got {purged}");

    cleanup(&path_a);
    cleanup(&path_b);
}

#[test]
fn current_sync_lsn_advances_with_transfer() {
    let path = tmp_path("lsn_advance");
    cleanup(&path);

    use std::sync::Arc;
    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();

    assert_eq!(eng.current_sync_lsn(), 0, "initial lsn is 0");

    for i in 1u32..=3 {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();

    // 0.9.0: oplog_sync が bridge も済ませるため、 この時点で lsn は払出済み
    // (旧: 手動 transfer まで 0 のままだった)
    assert!(eng.current_sync_lsn() >= 3,
            "after oplog_sync lsn should be >= 3, got {}", eng.current_sync_lsn());

    // 手動 transfer は追いつき済みで lsn を進めない (idempotent)
    let lsn_before = eng.current_sync_lsn();
    eng.transfer_oplog_to_sync_ops();
    assert_eq!(eng.current_sync_lsn(), lsn_before,
               "manual transfer after oplog_sync should be a no-op");

    cleanup(&path);
}
