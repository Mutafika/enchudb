//! snapshot → restore → sync の E2E。
//!
//! シナリオ:
//! 1. origin engine で署名付き書き込み、flush + wal_sync
//! 2. snapshot_export で `{main, .wal, .crc}` を別パスにコピー
//! 3. restored engine を open_concurrent_with_wal で開く(同 path)
//! 4. restored は snapshot 時点の全データを持つ(entity_count / get 一致)
//! 5. origin がさらに書き込んで publish、restored が pull で incremental sync
//!    できる(HLC 位置の整合、新規 record だけ apply)

#![cfg(feature = "v32")]

use enchudb::keys::Keypair;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb::{AuditFilter, Engine, HimoType, Hlc};
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
    for suffix in ["", ".wal", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", p, suffix));
    }
    p
}

fn cleanup(path: &str) {
    for suffix in ["", ".wal", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

fn prepare_db(path: &str) {
    let mut eng = Engine::create_standalone(path).unwrap();
    eng.define_himo("val", HimoType::Value, 100);
    eng.flush().unwrap();
}

#[test]
fn snapshot_restore_recovers_signed_wal_state() {
    // origin: 10 件の signed tie を書いて snapshot。restored がそのまま開けて
    // 全件取れ、WAL レコードの署名も失われないことを確認。
    let origin_path = tmp("orig");
    let restored_path = tmp("restd");
    prepare_db(&origin_path);

    let kp = Arc::new(Keypair::from_bytes(&[11u8; 32]));
    let pub_bytes = kp.public_bytes();

    // origin: 書き込み + snapshot
    {
        let eng = Engine::open_concurrent_with_wal(&origin_path, 16 * 1024 * 1024).unwrap();
        eng.set_peer_id(1);
        eng.set_keypair(Some(kp.clone()));
        for i in 0..10u32 {
            let e = eng.entity();
            eng.tie_async(e, "val", i);
        }
        eng.wal_commit();
        eng.flush_writes();
        eng.wal_sync().unwrap();

        let files = eng.snapshot_export(&restored_path).unwrap();
        assert_eq!(files.main, restored_path);
        assert!(files.wal.is_some(), "snapshot should include WAL");
        drop(eng);
    }

    // restored: 同 snapshot を開く
    let restored = Engine::open_concurrent_with_wal(&restored_path, 16 * 1024 * 1024).unwrap();
    restored.set_peer_id(1);
    restored.pubkeys().force_register(1, &pub_bytes);

    // 全件復元
    assert_eq!(restored.entity_count(), 10);
    for i in 0..10u64 {
        assert_eq!(restored.get(i, "val"), Some(i as u32));
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
    {
        let eng = Engine::open_concurrent_with_wal(&origin_path, 16 * 1024 * 1024).unwrap();
        eng.set_peer_id(1);
        eng.set_keypair(Some(kp.clone()));

        for i in 0..5u32 {
            let e = eng.entity();
            eng.tie_async(e, "val", i * 10);
        }
        eng.wal_commit();
        eng.flush_writes();
        eng.wal_sync().unwrap();

        // snapshot(restored_path を上書き)
        let _ = std::fs::remove_file(&restored_path);
        let _ = std::fs::remove_file(format!("{}.wal", restored_path));
        eng.snapshot_export(&restored_path).unwrap();

        // snapshot 時点の max HLC を控える(Syncer::pull の since に使う)
        let recs = eng.audit(&AuditFilter::default());
        snap_hlc = recs.iter().map(|r| r.hlc).max().unwrap_or(Hlc::ZERO);

        drop(eng);
    }

    // origin を再 open(consumer スレッド生きたまま sync するなら再 open しなくても良いが
    // snapshot_export で engine を drop した形なのでもう一度開ける)
    let origin = Engine::open_concurrent_with_wal(&origin_path, 16 * 1024 * 1024).unwrap();
    origin.set_peer_id(1);
    origin.set_keypair(Some(kp.clone()));

    // restored を open
    let restored = Engine::open_concurrent_with_wal(&restored_path, 16 * 1024 * 1024).unwrap();
    restored.set_peer_id(9);
    restored.pubkeys().force_register(1, &pub_bytes);

    // snapshot 時点の状態確認
    assert_eq!(restored.entity_count(), 5);
    for i in 0..5u64 {
        assert_eq!(restored.get(i, "val"), Some(i as u32 * 10));
    }

    // origin が追加書き込み
    for i in 0..3u32 {
        let e = origin.entity();
        origin.tie_async(e, "val", 1000 + i);
    }
    origin.wal_commit();
    origin.flush_writes();
    origin.wal_sync().unwrap();

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

    // 新規 entity も restored に反映
    // origin の next_eid は 5 (snapshot 後に使った entity は 5..8)
    for i in 5u64..8u64 {
        let v = restored.get(i, "val");
        assert!(v.is_some(), "eid {} should be synced to restored", i);
        assert!(v.unwrap() >= 1000);
    }

    drop(origin);
    drop(restored);
    cleanup(&origin_path);
    cleanup(&restored_path);
}
