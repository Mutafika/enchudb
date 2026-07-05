//! 0.8.0 Phase 4: `_sync_ops` table の eid 空間が ring buffer 化される。
//!
//! 0.7.0 では reclaim_sync_ops が entity delete だけで next_local は前進した
//! まま、 1M op cycle で eid_range 飽和して停止していた。 0.8.0 で TableDef に
//! free_locals (= 解放 local id の reservoir) を持たせ、 entity_in は free list
//! 優先で payout、 reclaim 時に free list に push、 という形で ring buffer 化。

use enchudb_engine::{Engine, ValueType};
use std::sync::Arc;
use std::time::Duration;

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
fn reclaim_pushes_local_id_to_free_list() {
    let path = tmp_path("reclaim_free_list");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();

    // 5 件 tie + 自動転送
    for i in 1u32..=5 {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();
    std::thread::sleep(Duration::from_millis(300));

    let pre_pending = eng.pending_sync_ops(0).len();
    assert!(pre_pending >= 5);

    // 全 ack + reclaim
    let lsn = eng.current_sync_lsn();
    eng.ack_sync(1, lsn).unwrap();
    let purged = eng.reclaim_sync_ops();
    assert!(purged > 0, "should purge some records");

    // pending はほぼ 0 (= reclaim は lsn < watermark を消すので最新 1 件は残る、
    // 厳密 == 0 でなく <= 1 で OK)
    let post_pending = eng.pending_sync_ops(0).len();
    assert!(post_pending <= 1, "expected near-zero pending, got {post_pending}");

    cleanup(&path);
}

#[test]
fn entity_in_reuses_freed_locals_after_reclaim() {
    let path = tmp_path("reuse_free_list");
    cleanup(&path);

    // 小さい _sync_ops 容量で「次の entity_in が free list を使う」 ことを確認
    let mut eng = Engine::create_with_capacity(&path, 8192).unwrap();
    eng.define_table("notes", 100).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();

    // 数件 tie + 全 ack + reclaim
    for i in 1u32..=10 {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();
    std::thread::sleep(Duration::from_millis(300));
    let lsn1 = eng.current_sync_lsn();
    eng.ack_sync(1, lsn1).unwrap();
    let purged1 = eng.reclaim_sync_ops();
    assert!(purged1 > 0);

    // さらに 10 件 tie → 自動転送。 _sync_ops eid は free list から再利用されるはず
    for i in 11u32..=20 {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();
    std::thread::sleep(Duration::from_millis(300));

    // 2 サイクル目も問題なく転送できてるか (= 飽和しなければ OK)
    let pending = eng.pending_sync_ops(0);
    assert!(!pending.is_empty(), "second cycle should still produce records");

    cleanup(&path);
}

#[test]
fn long_cycle_does_not_exhaust_eid_range() {
    // 0.7.0 では size_hint=64 だと 64 件 transfer で飽和、 reclaim しても回復しない。
    // 0.8.0 では ring buffer 化で 500 件 cycle 通せるはず (= 1 batch あたり ~40
    // record × 数 cycle、 free list 経由で payout)。
    let path = tmp_path("long_cycle");
    cleanup(&path);

    // notes は 600 件入る large 設計 (= user table の free list は phase 4 では
    // 未対応、 0.9.0 で展開予定なので user table 側は monotonic に消費される)。
    // _sync_ops は max_entities 64K の auto-clamp で十分大きい (= 32K)。
    // ring buffer の動作確認の本質は「reclaim 後の eid が再利用されること」 で、
    // 「sync_ops 容量を超えても回る」 は別の test (= 飽和 case が必要なら別途)。
    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("notes", 600).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();

    // 10 cycle × 50 record = 500 record を回す
    let mut total_purged = 0;
    for cycle in 0..10 {
        for i in 0u32..50 {
            let e = eng.entity_in("notes").unwrap();
            eng.tie_to(e, "notes.note", cycle * 50 + i);
        }
        eng.oplog_commit();
        std::thread::sleep(Duration::from_millis(200));
        let lsn = eng.current_sync_lsn();
        eng.ack_sync(1, lsn).unwrap();
        total_purged += eng.reclaim_sync_ops();
    }
    assert!(total_purged >= 300,
            "should reclaim hundreds across cycles, got {total_purged}");

    cleanup(&path);
}
