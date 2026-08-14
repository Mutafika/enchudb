//! #160 Phase 1 (検知層) の回帰: LWW 記憶が欠けたままの `Hlc::ZERO` pull を止めること。
//!
//! `HlcStore` は in-memory で、 reopen 後は `hydrate_hlc_store` が WAL と `_sync_ops` を
//! 歩いて再構築する (#154)。 だが `_sync_ops` の row は **ack 後に reclaim される**ため、
//! reclaim 済み range の HLC はどこにも残らない。 その状態で cursor を持たない caller が
//! `Hlc::ZERO` から pull すると、 相手 ring に残る陳腐 record が LWW 比較の基準を欠いた
//! まま素通しで適用され、 **ローカルのより新しい行が古い値へ巻き戻る**。
//!
//! #140 の `history_floor` 判定は **publisher 側が reclaim している**ケースしか塞がない。
//! 相手の ring に古い record が残っていれば floor は広告されず、 この経路をそのまま踏む。
//!
//! 根治は `HlcStore` 自体の永続化 (#160 Phase 2 / hlc_store.rs doc の "Phase D")。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::Hlc;
use enchudb_sync::Syncer;
use std::sync::Arc;
use std::time::Duration;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue160-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for s in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".schema"] {
        let _ = std::fs::remove_file(format!("{}{}", path, s));
    }
}

fn open_engine(path: &str, peer: u32, fresh: bool) -> Arc<Engine> {
    let e = if fresh {
        let mut e = Engine::create_with_capacity(path, 65_536).unwrap();
        e.define_table("notes", 1000).unwrap();
        e.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
        e.enable_sync_tables().unwrap();
        Engine::concurrentize_with_oplog(e, 16 * 1024 * 1024).unwrap()
    } else {
        Engine::open(path).unwrap()
    };
    e.set_peer_id(peer);
    e
}

#[test]
fn zero_cursor_pull_is_refused_when_lww_memory_was_reclaimed() {
    let pa = tmp_path("A");
    let pb = tmp_path("B");
    cleanup(&pa);
    cleanup(&pb);

    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());

    // ── A: 行を作って publish。 A は reclaim しない (= ring に陳腐 record が残る) ──
    let a = open_engine(&pa, 1, true);
    let ea = a.entity_in("notes").unwrap();
    a.tie_to(ea, "notes.note", 111);
    a.oplog_commit();
    std::thread::sleep(Duration::from_millis(300));
    let sa = Syncer::new(a.clone(), transport.clone());
    assert!(sa.publish_since(Hlc::ZERO) > 0, "A が publish できていない");

    // ── B: A から pull → 111。 ローカルで 222 に更新し、 その記憶を reclaim で捨てる ──
    let eid_b;
    {
        let b = open_engine(&pb, 2, true);
        let sb = Syncer::new(b.clone(), transport.clone());
        assert_eq!(sb.pull_once(1).applied, 1, "A の record が B に apply されていない");

        eid_b = *b.pull("notes.note", 111).first().expect("A の行が B に来ていない");
        b.tie_to(eid_b.into(), "notes.note", 222);
        b.oplog_commit();
        std::thread::sleep(Duration::from_millis(400));
        assert_eq!(b.get(eid_b.into(), "notes.note"), Some(222));

        // 222 の record が `_sync_ops` へ bridge されたことを確認してから、
        // 「全 peer が ack した」状態を作って reclaim する。 これで 222 の HLC は
        // WAL (fold 済み) にも `_sync_ops` (reclaim 済み) にも残らない。
        assert!(!b.pending_sync_ops(0).is_empty(), "bridge が追いついていない");
        // reclaim は `lsn < watermark` を消すので、 先端 lsn で ack すると最新 row が残る。
        // 「全 peer が先端まで消化した」状態 = 先端 + 1 で ack して ring を空にする。
        b.ack_sync(1, b.current_sync_lsn() + 1).unwrap();
        let purged = b.reclaim_sync_ops();
        assert!(purged > 0, "reclaim されていない — テスト前提が壊れている");
        assert!(
            b.pending_sync_ops(0).is_empty(),
            "`_sync_ops` に row が残っている (残 {} 件)",
            b.pending_sync_ops(0).len()
        );
    }

    // ── B を reopen: hydrate の source が両方とも空 ──
    let b2 = open_engine(&pb, 2, false);
    assert_eq!(
        b2.get(eid_b.into(), "notes.note"),
        Some(222),
        "reopen で store の値が失われている — テスト前提が壊れている"
    );
    assert!(
        b2.sync_history_reclaimed(),
        "reclaim 済み判定が立っていない — テスト前提が壊れている"
    );

    // ── ZERO cursor pull: 何も適用せず、 記憶欠落を通知する ──
    let sb2 = Syncer::new(b2.clone(), transport.clone());
    let out = sb2.pull_once(1);
    assert!(
        out.lww_memory_incomplete,
        "LWW 記憶が欠けた状態の ZERO pull を検知していない (#160)"
    );
    assert_eq!(
        out.applied, 0,
        "検知したのに record を適用している (received={}, applied={})",
        out.received, out.applied
    );
    assert_eq!(
        b2.get(eid_b.into(), "notes.note"),
        Some(222),
        "ローカルの新しい値 222 が相手の陳腐 record 111 へ巻き戻った (#160)"
    );

    cleanup(&pa);
    cleanup(&pb);
}

/// 記憶が欠けていない (reclaim が起きていない) 通常経路は素通しであること。
/// 検知が過剰に効いて正常な ZERO pull まで止めていないかの確認。
#[test]
fn zero_cursor_pull_still_works_without_reclaim() {
    let pa = tmp_path("C");
    let pb = tmp_path("D");
    cleanup(&pa);
    cleanup(&pb);

    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());

    let a = open_engine(&pa, 1, true);
    let ea = a.entity_in("notes").unwrap();
    a.tie_to(ea, "notes.note", 111);
    a.oplog_commit();
    std::thread::sleep(Duration::from_millis(300));
    let sa = Syncer::new(a.clone(), transport.clone());
    assert!(sa.publish_since(Hlc::ZERO) > 0, "A が publish できていない");

    // B は一度も reclaim していない = LWW 記憶は hydrate で完全に戻せる
    let b = open_engine(&pb, 2, true);
    let sb = Syncer::new(b.clone(), transport.clone());
    let out = sb.pull_once(1);
    assert!(
        !out.lww_memory_incomplete,
        "reclaim していないのに ZERO pull を止めている (過剰検知)"
    );
    assert_eq!(out.applied, 1, "通常の ZERO pull が適用されていない");
    assert_eq!(b.get(*b.pull("notes.note", 111).first().unwrap() as u64, "notes.note"), Some(111));

    cleanup(&pa);
    cleanup(&pb);
}
