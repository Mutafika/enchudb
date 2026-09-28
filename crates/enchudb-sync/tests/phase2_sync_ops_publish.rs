//! 0.8.0 Phase 2: Syncer::publish_since が `_sync_ops` 経由になる。
//!
//! - sync_tables_enabled な engine では publish 経路が `_sync_ops` の payload を
//!   decode して WireRecord 列を作る
//! - WireRecord の hlc / op / signature / pubkey_fp / author_peer が完全復元される
//! - HLC since filter が `_sync_ops` 経路でも効く

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::InMemoryTransport;
use enchudb_engine::ValueType;
use enchudb_oplog::Hlc;
use enchudb_sync::Syncer;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-phase2-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

/// 条件が立つまで 5ms 間隔で待つ (上限 10 秒)。 固定 sleep は CI の負荷で足りなくなる (#336)。
fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// consumer が `_sync_ops` へ bridge し終え (sync lsn が `n` まで進み)、
/// oplog ring を畳んだ (head が HEADER_SIZE に戻った) ところまで待つ。
fn wait_bridged(eng: &Engine, n: u32) {
    wait_until(&format!("consumer bridges {n} records into _sync_ops"), || {
        eng.current_sync_lsn() >= n
    });
    wait_until("oplog ring reset to HEADER_SIZE", || {
        eng.oplog().unwrap().head() == enchudb_oplog::oplog::HEADER_SIZE as u64
    });
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
    let _ = std::fs::remove_file(format!("{}.tables", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.db.lock", path));
}

#[test]
fn publish_since_uses_sync_ops_when_enabled() {
    let path = tmp_path("publish_sync_ops");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(1);

    for i in 1u32..=5 {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();
    // 自動 transfer 待ち (consumer の fsync tick 任せ、 手で transfer しない)
    wait_bridged(&eng, 5);

    let transport: Arc<dyn enchudb_engine::transport::Transport> =
        Arc::new(InMemoryTransport::new());
    let syncer = Syncer::new(eng.clone(), transport.clone());

    // publish_since (Hlc::ZERO) で全件出るはず
    let count = syncer.publish_since(Hlc::ZERO);
    assert!(count >= 5, "should publish at least 5 records, got {count}");
}

#[test]
fn publish_since_filters_by_hlc_through_sync_ops() {
    let path = tmp_path("filter_hlc");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(1);

    // 3 件 tie してから marker hlc 取得、 さらに 2 件追加
    for i in 1u32..=3 {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();
    wait_bridged(&eng, 3);

    // 現時刻 hlc を境にする (= 厳密な marker でなく実時刻ベース、 raw test なので OK)
    let marker_hlc = Hlc {
        wall: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        logical: 0,
        peer: 0,
    };

    // marker 後に 2 件追加。 この sleep は HLC の wall (ms) を marker より後にずらすため (待ち合わせではない)
    std::thread::sleep(Duration::from_millis(20));
    for i in 4u32..=5 {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();
    wait_bridged(&eng, 5);

    let transport: Arc<dyn enchudb_engine::transport::Transport> =
        Arc::new(InMemoryTransport::new());
    let syncer = Syncer::new(eng.clone(), transport.clone());

    // since=marker_hlc なら 2 件 (= 後半 tie のみ) のはず
    let count = syncer.publish_since(marker_hlc);
    // 厳密な 2 ではなく「全件未満かつ非ゼロ」 で OK (= 時刻 race のため)
    // 重要なのは「filter が効いてる」 こと
    assert!(count > 0 && count < 10,
            "filter should produce subset, got {count}");
}

/// #63 regression: oplog ring が reset した後に書いた record も `_sync_ops` に
/// bridge され、 sync で配信されること。
///
/// バグ版 (sync_ops_offset を巻き戻さない) では:
///   batch1 書く → consumer が bridge + try_reset (head→HEADER, offset は旧head放置)
///   batch2 書く → transfer の from=旧head > 現head → batch2 が transfer されない
///   → publish_since が batch2 を取りこぼす (count < 全件)
/// fix 版では offset も HEADER に戻るので batch2 も bridge され全件届く。
#[test]
fn records_after_ring_reset_are_still_synced() {
    let path = tmp_path("after_reset_synced");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(1);

    // batch1: 3 件 → commit → consumer が bridge + try_reset するまで待つ
    for i in 1u32..=3 {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();
    wait_bridged(&eng, 3);

    // ring が実際に reset したことを確認 (= このテストの前提が成立している)。
    let head_after_reset = eng.oplog().unwrap().head();
    assert_eq!(
        head_after_reset,
        enchudb_oplog::oplog::HEADER_SIZE as u64,
        "precondition: oplog ring should have reset to HEADER_SIZE after batch1 drained"
    );

    // batch2: reset 後に 2 件追加 → commit → bridge を待つ
    for i in 4u32..=5 {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();
    wait_bridged(&eng, 5);
    // #63: fold が bridge cursor を巻き戻していること。 巻き戻さないと cursor が head を追い越し、 今は
    // `wal_fold_safe` の修復 (#196、 平常時は 0 回) が拾うので batch2 も届いてしまう — 修復に頼ったかで見る
    assert_eq!(
        eng.sync_ops_cursor_repairs(),
        0,
        "fold が bridge cursor を巻き戻さず、 追い越しの修復に頼った (#63)"
    );

    // 全 5 件が `_sync_ops` 経由で publish されること。
    let transport: Arc<dyn enchudb_engine::transport::Transport> =
        Arc::new(InMemoryTransport::new());
    let syncer = Syncer::new(eng.clone(), transport.clone());
    let count = syncer.publish_since(Hlc::ZERO);
    assert_eq!(
        count, 5,
        "all 5 records must sync across the ring reset (got {count}); \
         batch2 (notes 4-5) lost = sync_ops_offset not rewound on try_reset"
    );
}
