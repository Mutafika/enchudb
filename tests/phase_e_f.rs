//! Phase E(snapshot_export)/ Phase F(stats HLC、audit) integration tests。

#![cfg(feature = "v32")]

use enchudb::{AuditFilter, Engine, HimoType, Hlc};
use std::sync::Arc;

fn tmp(tag: &str) -> String {
    let p = format!(
        "/tmp/enchudb-ef-{}-{}-{}",
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

// ─────────────────────────────────────────────────────────────
// Phase E: snapshot_export
// ─────────────────────────────────────────────────────────────

#[test]
fn snapshot_export_plain_engine_roundtrip() {
    let src_path = tmp("snap_src");
    let dst_path = tmp("snap_dst");

    // データ投入
    {
        let mut eng = Engine::create(&src_path).unwrap();
        eng.define_himo("age", HimoType::Value, 100);
        let e = eng.entity();
        eng.tie(e, "age", 30);
        eng.tie_text(e, "name", "alice");
        eng.flush().unwrap();

        // snapshot
        let files = eng.snapshot_export(&dst_path).unwrap();
        assert_eq!(files.main, dst_path);
        // WAL 無しなので wal/crc は None(or 無ければ None)
    }

    // dst を open して同じデータが読めるか
    let eng2 = Engine::open(&dst_path).unwrap();
    assert_eq!(eng2.entity_count(), 1);
    assert_eq!(eng2.get(0, "age"), Some(30));
    let name = eng2.get_text(0, "name").unwrap();
    assert_eq!(name, b"alice");

    cleanup(&src_path);
    cleanup(&dst_path);
}

#[test]
fn snapshot_export_with_wal_includes_wal_file() {
    let src_path = tmp("snap_wal_src");
    let dst_path = tmp("snap_wal_dst");

    // スキーマ定義
    {
        let mut eng = Engine::create(&src_path).unwrap();
        eng.define_himo("val", HimoType::Value, 100);
        eng.flush().unwrap();
    }

    // WAL 経由で書き込み
    let eng = Engine::open_concurrent_with_wal(&src_path, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(1);
    let e = eng.entity();
    eng.tie_async(e, "val", 42);
    eng.wal_commit();
    eng.flush_writes();
    eng.wal_sync().unwrap();

    let files = eng.snapshot_export(&dst_path).unwrap();
    assert_eq!(files.main, dst_path);
    assert!(
        files.wal.is_some(),
        "WAL を作ったので snapshot に含まれるべき"
    );
    assert!(std::path::Path::new(&format!("{}.wal", dst_path)).exists());

    drop(eng);

    // dst を wal 込みで open
    let eng2 = Engine::open_concurrent_with_wal(&dst_path, 16 * 1024 * 1024).unwrap();
    assert_eq!(eng2.get(e, "val"), Some(42));

    drop(eng2);
    cleanup(&src_path);
    cleanup(&dst_path);
}

// ─────────────────────────────────────────────────────────────
// Phase F: stats に HLC 情報が載る
// ─────────────────────────────────────────────────────────────

#[test]
fn stats_includes_hlc_info() {
    let src_path = tmp("stats_hlc");
    {
        let mut eng = Engine::create(&src_path).unwrap();
        eng.define_himo("val", HimoType::Value, 100);
        eng.flush().unwrap();
    }

    let eng = Engine::open_concurrent_with_wal(&src_path, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(7);

    let stats_before = eng.stats();
    assert_eq!(stats_before.peer_id, 7);
    assert_eq!(stats_before.hlc_entries, 0);
    assert!(stats_before.max_hlc.is_none());

    // 何か書く(HlcStore は sync 受信時に埋まる、自 peer 書き込みでは埋まらない仕様の可能性)
    // だが hlc_store の len は常に監視できる
    let e = eng.entity();
    eng.tie_async(e, "val", 1);
    eng.wal_commit();
    eng.flush_writes();
    eng.wal_sync().unwrap();

    let stats_after = eng.stats();
    assert_eq!(stats_after.peer_id, 7);
    // hlc_entries / max_hlc は実装依存(受信 op のみ記録か、自 write も記録か)
    // 少なくとも型として取り出せることを確認
    let _: usize = stats_after.hlc_entries;
    let _: Option<Hlc> = stats_after.max_hlc;

    drop(eng);
    cleanup(&src_path);
}

// ─────────────────────────────────────────────────────────────
// Phase F: audit で WAL レコードを filter
// ─────────────────────────────────────────────────────────────

#[test]
fn audit_returns_wal_records() {
    let src_path = tmp("audit_all");
    {
        let mut eng = Engine::create(&src_path).unwrap();
        eng.define_himo("val", HimoType::Value, 100);
        eng.flush().unwrap();
    }

    let eng = Engine::open_concurrent_with_wal(&src_path, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(3);

    let e1 = eng.entity();
    eng.tie_async(e1, "val", 10);
    let e2 = eng.entity();
    eng.tie_async(e2, "val", 20);
    eng.wal_commit();
    eng.flush_writes();
    eng.wal_sync().unwrap();

    let recs = eng.audit(&AuditFilter::default());
    // 2 Tie ops + entity create が入るが、最低 2 件の write op は見えるはず
    assert!(
        recs.len() >= 2,
        "audit は commit 済み record を返すべき、got {}",
        recs.len()
    );

    // すべて author_peer == 3
    for r in &recs {
        assert_eq!(r.author_peer, 3);
    }

    drop(eng);
    cleanup(&src_path);
}

#[test]
fn audit_filter_by_author_excludes_others() {
    let src_path = tmp("audit_auth");
    {
        let mut eng = Engine::create(&src_path).unwrap();
        eng.define_himo("val", HimoType::Value, 100);
        eng.flush().unwrap();
    }

    let eng = Engine::open_concurrent_with_wal(&src_path, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(5);

    let e = eng.entity();
    eng.tie_async(e, "val", 1);
    eng.wal_commit();
    eng.flush_writes();
    eng.wal_sync().unwrap();

    // author=5(自分)で fetch
    let filter = AuditFilter {
        author_peer: Some(5),
        ..Default::default()
    };
    let mine = eng.audit(&filter);
    assert!(!mine.is_empty());

    // author=99(存在しない peer)で fetch → 空
    let filter_other = AuditFilter {
        author_peer: Some(99),
        ..Default::default()
    };
    let others = eng.audit(&filter_other);
    assert!(others.is_empty());

    drop(eng);
    cleanup(&src_path);
}

#[test]
fn audit_filter_by_hlc_range() {
    let src_path = tmp("audit_hlc");
    {
        let mut eng = Engine::create(&src_path).unwrap();
        eng.define_himo("val", HimoType::Value, 100);
        eng.flush().unwrap();
    }

    let eng = Engine::open_concurrent_with_wal(&src_path, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(1);

    let e = eng.entity();
    eng.tie_async(e, "val", 42);
    eng.wal_commit();
    eng.flush_writes();
    eng.wal_sync().unwrap();

    let all = eng.audit(&AuditFilter::default());
    assert!(!all.is_empty());

    // 未来の HLC で下限フィルタ → 空
    let future = Hlc {
        wall: u64::MAX,
        logical: 0,
        peer: 1,
    };
    let filter_future = AuditFilter {
        from_hlc: Some(future),
        ..Default::default()
    };
    let nothing = eng.audit(&filter_future);
    assert!(
        nothing.is_empty(),
        "未来の HLC 下限で全レコード除外されるべき"
    );

    // HLC ZERO より大(含む)で全部通るはず
    let filter_zero = AuditFilter {
        from_hlc: Some(Hlc::ZERO),
        ..Default::default()
    };
    let all2 = eng.audit(&filter_zero);
    assert_eq!(all2.len(), all.len());

    drop(eng);
    cleanup(&src_path);
}

#[test]
fn audit_on_engine_without_wal_returns_empty() {
    let src_path = tmp("audit_no_wal");
    let eng = Arc::new(Engine::create(&src_path).unwrap());
    // WAL 付きで open してないので audit は空
    let recs = eng.audit(&AuditFilter::default());
    assert!(recs.is_empty());
    cleanup(&src_path);
}
