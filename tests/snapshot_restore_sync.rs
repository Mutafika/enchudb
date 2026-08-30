//! snapshot → restore → sync の E2E。
//!
//! シナリオ:
//! 1. origin engine で署名付き書き込み、flush + oplog_sync
//! 2. snapshot_export で `{main, .wal, .crc}` を別パスにコピー
//! 3. restored engine を open_concurrent_with_oplog で開く(同 path)
//! 4. restored は snapshot 時点の全データを持つ(entity_count / get 一致)
//! 5. origin がさらに書き込んで publish、restored が pull で incremental sync
//!    できる(HLC 位置の整合、新規 record だけ apply)

use enchudb_oplog::keys::Keypair;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb::{AuditFilter, Engine, ValueType};
use enchudb_oplog::Hlc;
use std::sync::Arc;

fn tmp(tag: &str) -> String {
    let p = format!(
        "/tmp/enchudb-snap-restore-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let _ = std::fs::remove_dir_all(&p); // v10: DB は directory
    for suffix in ["", ".oplog", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", p, suffix));
    }
    p
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    for suffix in ["", ".oplog", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

/// **named table 必須** (β-light step 3): `enable_sync_tables()` が `define_table` を
/// 呼ぶため anonymous table が閉じ `entity()` は panic する。 加えて anonymous のままだと
/// 受信 op の foreign eid を確保する先が無く apply が skip される (#9)。
const TABLE: &str = "t";

/// `TABLE` 内 himo の qualified name。
fn q(himo: &str) -> String {
    format!("{}.{}", TABLE, himo)
}

/// 受信側で foreign eid を翻訳して読む (#9)。
fn get_remote(eng: &Engine, foreign_eid: u64, himo: &str) -> Option<u32> {
    let hid = eng.himo_id(&q(himo)).unwrap() as u16;
    let local = eng.resolve_remote_eid(foreign_eid, hid)?;
    eng.get(local, &q(himo))
}

fn prepare_db(path: &str) {
    let mut eng = Engine::create_standalone(path).unwrap();
    eng.define_table(TABLE, 1000).unwrap();
    eng.define_himo_in(TABLE, "val", ValueType::Number, 100).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
}

/// **現行の durability 設計と前提が食い違っている。**
///
/// このテストは「WAL に書いた signed record が reopen 後も `audit()` で全件見える」
/// ことを期待しているが、 oplog は **session をまたぐ audit log ではなく ring buffer**。
/// graceful shutdown 時に consumer thread が `advance_checkpoint(head)` するので
/// checkpoint == head になり、 次 open で `try_reset()` が head を HEADER_SIZE へ
/// 巻き戻す。 `audit()` は `iter_committed()` = head までの scan なので 0 件になる。
///
/// 0.8.0 以降、 session をまたいで残る sync record は `_sync_ops` 側。
/// 「reopen 後も署名付き履歴を追える」ことを保証すべきかは設計判断が要るため、
/// 期待値を黙って緩めず ignore で可視化する。
#[test]
#[ignore = "oplog ring は session をまたぐ audit log ではない — graceful shutdown で checkpoint が head に追いつき、次 open の try_reset で ring が畳まれるため audit() が空になる"]
fn snapshot_restore_recovers_signed_wal_state() {
    // origin: 10 件の signed tie を書いて snapshot。restored がそのまま開けて
    // 全件取れ、WAL レコードの署名も失われないことを確認。
    let origin_path = tmp("orig");
    let restored_path = tmp("restd");
    prepare_db(&origin_path);

    let kp = Arc::new(Keypair::from_bytes(&[11u8; 32]));
    let pub_bytes = kp.public_bytes();

    // origin: 書き込み + snapshot
    let mut eids = Vec::new();
    {
        let eng = Engine::open_concurrent_with_oplog(&origin_path, 16 * 1024 * 1024).unwrap();
        eng.set_peer_id(1);
        eng.set_keypair(Some(kp.clone()));
        for i in 0..10u32 {
            let e = eng.entity_in(TABLE).unwrap();
            eids.push(e);
            eng.tie_async(e, &q("val"), i);
        }
        eng.oplog_commit();
        eng.flush_writes();
        eng.oplog_sync().unwrap();
        eng.transfer_oplog_to_sync_ops();

        let files = eng.snapshot_export(&restored_path).unwrap();
        assert_eq!(files.main, restored_path);
        assert!(files.oplog.is_some(), "snapshot should include WAL");
        drop(eng);
    }

    // restored: 同 snapshot を開く
    let restored = Engine::open_concurrent_with_oplog(&restored_path, 16 * 1024 * 1024).unwrap();
    restored.set_peer_id(1);
    restored.pubkeys().force_register(1, &pub_bytes);

    // 全件復元 (snapshot は同じ eid 空間をそのまま持ってくるので翻訳不要)。
    // `entity_count()` は `_sync_ops` に bridge された op も 1 entity として数えるので
    // 「書いた entity 数」とは一致しない (10 tie → +10)。 実データで検証する。
    assert_eq!(eids.len(), 10);
    for (i, &e) in eids.iter().enumerate() {
        assert_eq!(restored.get(e, &q("val")), Some(i as u32));
    }

    // WAL レコードの署名も保持
    let recs = restored.audit(&AuditFilter::default());
    assert!(recs.len() >= 10, "restored should see all audit records");
    for r in &recs {
        assert_ne!(r.signature, [0u8; 64]);
        assert!(restored.pubkeys().verify(1, &r.signed_bytes, &r.signature));
    }

    drop(restored);
    cleanup(&origin_path);
    cleanup(&restored_path);
}

#[test]
fn restored_replica_syncs_incremental_from_origin_after_snapshot() {
    // snapshot 取得時点までは restored に DB コピーで入ってる。
    // その後 origin が追加書き込み → publish → restored が Syncer::pull_once で取得。
    // HLC 位置の整合で「snapshot 後に origin が書いた分のみ」が入る。
    let origin_path = tmp("orig_sync");
    let restored_path = tmp("restd_sync");
    prepare_db(&origin_path);
    prepare_db(&restored_path); // restored 側も himo 定義は必要

    let kp = Arc::new(Keypair::from_bytes(&[22u8; 32]));
    let pub_bytes = kp.public_bytes();

    // origin: 初期書き込み + snapshot
    let snap_hlc: Hlc;
    let mut snap_eids = Vec::new();
    {
        let eng = Engine::open_concurrent_with_oplog(&origin_path, 16 * 1024 * 1024).unwrap();
        eng.set_peer_id(1);
        eng.set_keypair(Some(kp.clone()));

        for i in 0..5u32 {
            let e = eng.entity_in(TABLE).unwrap();
            snap_eids.push(e);
            eng.tie_async(e, &q("val"), i * 10);
        }
        eng.oplog_commit();
        eng.flush_writes();
        eng.oplog_sync().unwrap();
        eng.transfer_oplog_to_sync_ops();

        // snapshot(restored_path を上書き)
        let _ = std::fs::remove_dir_all(&restored_path); // v10: DB は directory
        let _ = std::fs::remove_file(&restored_path);
        let _ = std::fs::remove_file(format!("{}.oplog", restored_path));
        eng.snapshot_export(&restored_path).unwrap();

        // snapshot 時点の max HLC を控える(Syncer::pull の since に使う)
        let recs = eng.audit(&AuditFilter::default());
        snap_hlc = recs.iter().map(|r| r.hlc).max().unwrap_or(Hlc::ZERO);

        drop(eng);
    }

    // origin を再 open(consumer スレッド生きたまま sync するなら再 open しなくても良いが
    // snapshot_export で engine を drop した形なのでもう一度開ける)
    let origin = Engine::open_concurrent_with_oplog(&origin_path, 16 * 1024 * 1024).unwrap();
    origin.set_peer_id(1);
    origin.set_keypair(Some(kp.clone()));

    // restored を open
    let restored = Engine::open_concurrent_with_oplog(&restored_path, 16 * 1024 * 1024).unwrap();
    restored.set_peer_id(9);
    restored.pubkeys().force_register(1, &pub_bytes);

    // snapshot 時点の状態確認
    // entity_count() は `_sync_ops` 分を含むので使わない (上と同じ理由)。
    assert_eq!(snap_eids.len(), 5);
    for (i, &e) in snap_eids.iter().enumerate() {
        assert_eq!(restored.get(e, &q("val")), Some(i as u32 * 10));
    }

    // origin が追加書き込み
    let mut new_eids = Vec::new();
    for i in 0..3u32 {
        let e = origin.entity_in(TABLE).unwrap();
        new_eids.push(e);
        origin.tie_async(e, &q("val"), 1000 + i);
    }
    origin.oplog_commit();
    origin.flush_writes();
    origin.oplog_sync().unwrap();
    origin.transfer_oplog_to_sync_ops();

    // transport 経由で origin → restored へ sync
    let transport = Arc::new(InMemoryTransport::new());
    let syncer_origin = Syncer::new(origin.clone(), transport.clone() as Arc<dyn Transport>);
    let syncer_restored = Syncer::new(restored.clone(), transport.clone() as Arc<dyn Transport>);
    syncer_restored.set_require_signature(true);

    // origin から "snapshot 後の分だけ" publish(since = snap_hlc)
    let pub_count = syncer_origin.publish_since(snap_hlc);
    assert!(
        pub_count >= 3,
        "should publish at least 3 new records (ties) + commit, got {}",
        pub_count
    );

    // restored が pull して apply
    let out = syncer_restored.pull_once(1);
    assert!(
        out.applied >= 3,
        "restored should apply at least 3 new ties, got {:?}",
        out
    );

    // 新規 entity も restored に反映。 restored は peer 9 なので、 origin (peer 1) の
    // eid は #9 の翻訳を通して読む。
    for &e in &new_eids {
        let v = get_remote(&restored, e, "val");
        assert!(v.is_some(), "eid {} should be synced to restored", e);
        assert!(v.unwrap() >= 1000);
    }

    drop(origin);
    drop(restored);
    cleanup(&origin_path);
    cleanup(&restored_path);
}
