//! #191 — history floor は「reclaim で消えた record の最大 HLC」であること。
//!
//! 旧実装 (生存 record の最小 HLC、 空なら Hlc::MAX) は、 reclaim 分を全部
//! 消化済みの follower (cursor = max_reclaimed) まで gap と誤認し、 reclaim
//! 1 回で既追従 follower 全員が bootstrap 行きになっていた (sunsu2 Phase 2
//! chaos で発見)。
//!
//! 検証:
//! 1. `caught_up_follower_survives_reclaim` — 全消化済み follower は reclaim +
//!    新規 write 後も通常 pull を続けられる。 遅参 peer は truncation 通知。
//! 2. `floor_survives_reopen` — floor は body に永続し、 reopen 後の publish
//!    でも広告される (揮発だと reopen 後に遅参 peer の gap を見逃す)。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;
use std::time::Duration;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue191-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

fn make_engine(path: &str, peer: PeerId) -> Arc<Engine> {
    cleanup(path);
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

/// author が n 件 tie して bridge まで済ませる。
fn author_notes(eng: &Arc<Engine>, from: u32, n: u32) {
    for i in from..from + n {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
}

#[test]
fn caught_up_follower_survives_reclaim() {
    let pa = tmp_path("a");
    let pb = tmp_path("b");
    let pc = tmp_path("c");
    // 注意: set_history_floor (= advertise) は InMemoryTransport の peer registry に
    // author を登録するので、以降の publish は broadcast でなく registered peer 宛に
    // なる。実運用 harness と同じく全 peer を明示 register して使う。
    let mem = Arc::new(InMemoryTransport::new());
    for p in [1u32, 2, 3] {
        mem.register_peer(p);
    }
    let transport: Arc<dyn Transport> = mem.clone();

    let eng_a = make_engine(&pa, 1);
    let eng_b = make_engine(&pb, 2);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());

    // A が 10 件 author、 B が全部消化
    author_notes(&eng_a, 1, 10);
    sync_a.publish_since(Hlc::ZERO);
    let out = sync_b.pull_once(1);
    assert_eq!(out.applied, 10, "10 entity × Tie 1 本ずつ: {out:?}");

    // B の消化を ack として渡して reclaim (= pull-as-ack #149 相当を手動代行)
    let lsn = eng_a.current_sync_lsn();
    eng_a.ack_sync(2, lsn + 1).unwrap();
    let purged = eng_a.reclaim_sync_ops();
    assert!(purged > 0, "reclaim が走っていること");

    // floor が「reclaim された最大 HLC」で記録されている
    let floor = eng_a.sync_reclaimed_floor().expect("floor が記録される");

    // A が続きを author → publish (ここで floor が transport に広告される)
    author_notes(&eng_a, 11, 3);
    sync_a.publish_since(Hlc::ZERO);
    assert_eq!(
        transport.history_floor(1),
        Some(floor),
        "広告される floor は reclaim 済み最大 HLC"
    );

    // 既追従 B: cursor は reclaim 分を全部消化済み → 通常 pull が続けられる
    let out = sync_b.pull_once(1);
    assert!(
        !out.history_truncated,
        "#191: 全消化済み follower が reclaim に巻き込まれた: {out:?}"
    );
    assert!(out.applied > 0, "新規 3 件が届くこと: {out:?}");

    // 遅参 C (cursor = ZERO): 差分では埋まらない → truncation 通知
    let eng_c = make_engine(&pc, 3);
    let sync_c = Syncer::new(eng_c.clone(), transport.clone());
    let out = sync_c.pull_once(1);
    assert!(out.history_truncated, "遅参 peer には truncation 通知: {out:?}");
    assert_eq!(out.applied, 0, "truncation 時は一切適用しない");

    drop(sync_a);
    drop(sync_b);
    drop(sync_c);
    drop(eng_a);
    drop(eng_b);
    drop(eng_c);
    cleanup(&pa);
    cleanup(&pb);
    cleanup(&pc);
}

#[test]
fn floor_survives_reopen() {
    let pa = tmp_path("reopen");
    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());

    let floor = {
        let eng_a = make_engine(&pa, 1);
        author_notes(&eng_a, 1, 5);
        let sync_a = Syncer::new(eng_a.clone(), transport.clone());
        sync_a.publish_since(Hlc::ZERO);
        let lsn = eng_a.current_sync_lsn();
        eng_a.ack_sync(2, lsn + 1).unwrap();
        assert!(eng_a.reclaim_sync_ops() > 0);
        let floor = eng_a.sync_reclaimed_floor().expect("floor 記録");
        eng_a.flush_writes();
        eng_a.oplog_sync().unwrap();
        floor
    }; // drop = clean shutdown

    // 別 transport で reopen — 広告は publish 時に行われる
    std::thread::sleep(Duration::from_millis(50));
    let transport2: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let eng_a = Engine::open_concurrent_with_oplog(&pa, 16 * 1024 * 1024).unwrap();
    eng_a.set_peer_id(1);
    assert_eq!(
        eng_a.sync_reclaimed_floor(),
        Some(floor),
        "floor が reopen を跨いで永続していること"
    );
    let sync_a = Syncer::new(eng_a.clone(), transport2.clone());
    sync_a.publish_since(Hlc::ZERO);
    assert_eq!(transport2.history_floor(1), Some(floor), "reopen 後も floor が広告される");

    drop(sync_a);
    drop(eng_a);
    cleanup(&pa);
}
