//! ChangeListener integration tests。
//!
//! 確認事項:
//! - 単一 listener で commit 後に on_changes が呼ばれる
//! - 複数 listener が全部呼ばれる
//! - HLC 昇順で渡される
//! - listener を追加するより前の commit は流れない(過去 fire は無し)
//! - 大量 record でも順序崩れない

#![cfg(feature = "v32")]

use enchudb::changefeed::ChangeListener;
use enchudb::transport::WireRecord;
use enchudb::wal::DecodedOp;
use enchudb::{Engine, HimoType};
use std::sync::{Arc, Mutex};

fn tmp(tag: &str) -> String {
    let p = format!(
        "/tmp/enchudb-cf-{}-{}-{}",
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

fn prepare_wal_engine(tag: &str) -> (String, Arc<Engine>) {
    let path = tmp(tag);
    {
        let mut eng = Engine::create_standalone(&path).unwrap();
        eng.define_himo("v", HimoType::Value, 100);
        eng.flush().unwrap();
    }
    let eng = Engine::open_concurrent_with_wal(&path, 8 * 1024 * 1024).unwrap();
    eng.set_peer_id(1);
    (path, eng)
}

#[derive(Default)]
struct CollectSink {
    records: Mutex<Vec<WireRecord>>,
}

impl ChangeListener for CollectSink {
    fn on_changes(&self, records: &[WireRecord]) {
        self.records.lock().unwrap().extend_from_slice(records);
    }
}

#[test]
fn single_listener_receives_after_wal_sync() {
    let (path, eng) = prepare_wal_engine("single");
    let sink = Arc::new(CollectSink::default());
    eng.add_change_listener(sink.clone());
    assert_eq!(eng.change_listener_count(), 1);

    let e = eng.entity();
    eng.tie_async(e, "v", 42);
    eng.wal_commit();
    eng.flush_writes();
    eng.wal_sync().unwrap();

    // shutdown 時 emit を確実に走らせる
    drop(eng);

    let recs = sink.records.lock().unwrap();
    assert!(
        !recs.is_empty(),
        "listener should receive at least 1 record"
    );
    let has_tie = recs.iter().any(|r| {
        matches!(r.op, DecodedOp::Tie { value: 42, .. })
    });
    assert!(has_tie, "should see Tie value=42");
    drop(recs);
    cleanup(&path);
}

#[test]
fn multiple_listeners_all_fire() {
    let (path, eng) = prepare_wal_engine("multi");
    let sink_a = Arc::new(CollectSink::default());
    let sink_b = Arc::new(CollectSink::default());
    eng.add_change_listener(sink_a.clone());
    eng.add_change_listener(sink_b.clone());
    assert_eq!(eng.change_listener_count(), 2);

    for i in 0..5u32 {
        let e = eng.entity();
        eng.tie_async(e, "v", i);
    }
    eng.wal_commit();
    eng.flush_writes();
    eng.wal_sync().unwrap();
    drop(eng);

    let a = sink_a.records.lock().unwrap();
    let b = sink_b.records.lock().unwrap();
    assert_eq!(a.len(), b.len(), "two listeners should see same count");
    assert!(a.len() >= 5, "should see >= 5 records, got {}", a.len());
    cleanup(&path);
}

#[test]
fn records_are_hlc_ascending() {
    let (path, eng) = prepare_wal_engine("hlc_order");
    let sink = Arc::new(CollectSink::default());
    eng.add_change_listener(sink.clone());

    for i in 0..50u32 {
        let e = eng.entity();
        eng.tie_async(e, "v", i);
    }
    eng.wal_commit();
    eng.flush_writes();
    eng.wal_sync().unwrap();
    drop(eng);

    let recs = sink.records.lock().unwrap();
    assert!(recs.len() >= 50);
    for w in recs.windows(2) {
        assert!(
            w[0].hlc <= w[1].hlc,
            "records must be HLC ascending: {:?} <= {:?}",
            w[0].hlc,
            w[1].hlc
        );
    }
    cleanup(&path);
}

#[test]
fn listener_added_after_first_commit_only_sees_subsequent() {
    // listener attach 前の commit は流れない(cursor は attach 時点の WAL head 位置で
    // フリーズしてないが、現状の実装は「過去分は流さない」設計)。
    let (path, eng) = prepare_wal_engine("late_attach");

    // 1 回目 commit (listener 無し)
    let e0 = eng.entity();
    eng.tie_async(e0, "v", 100);
    eng.wal_commit();
    eng.flush_writes();
    eng.wal_sync().unwrap();

    // ここで listener 追加
    let sink = Arc::new(CollectSink::default());
    eng.add_change_listener(sink.clone());

    // 2 回目 commit
    let e1 = eng.entity();
    eng.tie_async(e1, "v", 200);
    eng.wal_commit();
    eng.flush_writes();
    eng.wal_sync().unwrap();

    drop(eng);

    let recs = sink.records.lock().unwrap();
    // 100 (1 回目) は listener 加入前で過去分なので含まれない
    let has_100 = recs
        .iter()
        .any(|r| matches!(r.op, DecodedOp::Tie { value: 100, .. }));
    let has_200 = recs
        .iter()
        .any(|r| matches!(r.op, DecodedOp::Tie { value: 200, .. }));
    assert!(!has_100, "value=100 (listener 加入前) は流れちゃダメ");
    assert!(has_200, "value=200 (listener 加入後) は流れるべき");

    cleanup(&path);
}

#[test]
fn listener_does_not_fire_on_engine_without_wal() {
    // WAL 無し engine では fire しない(create 直 + add で何も起きないこと)
    let path = tmp("no_wal");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("v", HimoType::Value, 100);
    let eng = Arc::new(eng);
    // Note: add_change_listener は &self だが Arc<Engine> なので使える
    let sink = Arc::new(CollectSink::default());
    eng.add_change_listener(sink.clone());

    let e = eng.entity();
    // tie() は WAL 経由しない
    eng.tie_to(e, "v", 1);
    drop(eng);

    let recs = sink.records.lock().unwrap();
    assert!(recs.is_empty(), "no WAL なら listener は呼ばれない");
    cleanup(&path);
}
