//! #154 回帰: WAL fold 済み record の HLC が reopen 後の hydrate で復元されること。
//!
//! `HlcStore` は in-memory なので reopen 後は空になり、 `Syncer::new` の
//! `hydrate_hlc_store` が LWW state を再構築する。 だが WAL ring は bridge 済み領域を
//! fold する (`Engine::wal_fold_safe`) ため、 **WAL だけを歩く hydrate は fold された
//! record を見ない**。 その状態で cursor を持たない caller が `Hlc::ZERO` から pull すると、
//! 相手 ring に残る陳腐 record が「未知」と判定されて再 apply され、 **ローカルのより
//! 新しい行が巻き戻る**。
//!
//! 罠が 2 つある:
//!
//! 1. fold 済み record は bridge 先の `_sync_ops` (永続) に残っているので、 そこも歩く
//! 2. `_sync_ops` の record は **逆写像で元 owner の世界番号に宛名が書き戻されている**
//!    (request10 / #76)。 生の eid を key にすると apply 側 (= local eid で lookup) と
//!    一致せず、 hydrate したのに LWW が効かない silent な取りこぼしになる

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::Hlc;
use enchudb_sync::Syncer;
use std::sync::Arc;
use std::time::Duration;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue154-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for s in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap", ".schema"] {
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
fn folded_record_hlc_survives_reopen_and_blocks_stale_rollback() {
    let pa = tmp_path("A");
    let pb = tmp_path("B");
    cleanup(&pa);
    cleanup(&pb);

    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());

    // ── A: 行を作って publish ──
    let a = open_engine(&pa, 1, true);
    let ea = a.entity_in("notes").unwrap();
    a.tie_to(ea, "notes.note", 111);
    a.oplog_commit();
    std::thread::sleep(Duration::from_millis(300));
    let sa = Syncer::new(a.clone(), transport.clone());
    assert!(sa.publish_since(Hlc::ZERO) > 0, "A が publish できていない");

    // ── B: A から pull → 111 が入る。 その後ローカルで 222 に更新 ──
    let eid_b;
    {
        let b = open_engine(&pb, 2, true);
        let sb = Syncer::new(b.clone(), transport.clone());
        let out = sb.pull_once(1);
        assert_eq!(out.applied, 1, "A の record が B に apply されていない");

        eid_b = *b
            .pull("notes.note", 111)
            .first()
            .expect("A の行が B に来ていない");

        b.tie_to(eid_b.into(), "notes.note", 222);
        b.oplog_commit();
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(b.get(eid_b.into(), "notes.note"), Some(222));

        // bridge させて WAL fold を成立させる (ack はしない = relay 型の実機構成)
        std::thread::sleep(Duration::from_millis(400));
        assert!(b.wal_fold_safe(), "bridge が追いついていない — テスト前提が壊れている");
        // drop で graceful shutdown → 最終 transfer + fold
    }

    // ── B を reopen ──
    let b2 = open_engine(&pb, 2, false);
    assert_eq!(
        b2.get(eid_b.into(), "notes.note"),
        Some(222),
        "reopen で store の値が失われている — テスト前提が壊れている"
    );

    // 前提確認: WAL は fold 済みで空、 record は `_sync_ops` にだけ残っている
    let wal_left = b2.oplog_arc().map(|w| w.iter_committed().len()).unwrap_or(0);
    assert_eq!(
        wal_left, 0,
        "WAL が fold されていない — この test は fold 済み前提 (残 {} 件)",
        wal_left
    );
    assert!(
        !b2.pending_sync_ops(0).is_empty(),
        "`_sync_ops` が空 — hydrate の source が無い (テスト前提が壊れている)"
    );

    // ── ZERO cursor で pull し直す: 陳腐な 111 は LWW で弾かれるべき ──
    let sb2 = Syncer::new(b2.clone(), transport.clone());
    let out2 = sb2.pull_once(1);
    assert_eq!(
        out2.applied, 0,
        "陳腐な record が再 apply された (received={}, applied={})",
        out2.received, out2.applied
    );
    assert_eq!(
        b2.get(eid_b.into(), "notes.note"),
        Some(222),
        "ローカルの新しい値 222 が相手の陳腐 record 111 へ巻き戻った (#154)"
    );

    cleanup(&pa);
    cleanup(&pb);
}
