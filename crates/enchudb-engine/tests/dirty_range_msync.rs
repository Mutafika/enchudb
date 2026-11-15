//! request3 regression: body_msync が dirty range だけを sync する。
//!
//! 旧実装は `flush(0, committed())` で committed 全体を msync していた。
//! 修正後は writer が触った範囲だけ msync される。 直接 dirty range の
//! offset を観測する API は出していないので、 ここでは
//!  - sustained 書き込み中の `body_msync()` が連続呼び出しでも fail しない
//!  - 書き込み後 `body_msync` → 再度 `body_msync` で 2 回目が clean (no-op)
//! の最低限の整合性のみテスト。 perf は本 test では確認しない (CI 不安定化回避)。

use enchudb_engine::{Engine, ValueType};
use std::sync::Arc;

/// 試験ごとの DB の path。 pid / 連番 / 時刻を入れて、 並べて回した別の process の試験と取り合わない (#906)。
fn tmp_path(tag: &str) -> &'static str {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/tmp/{tag}_{}_{seq}_{nanos}.db", std::process::id()).leak()
}

#[test]
fn body_msync_handles_dirty_range_correctly() {
    let path = tmp_path("test_dirty_range_msync");
    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.oplog"));
    let _ = std::fs::remove_file(format!("{path}.lock"));

    let eng: Arc<Engine> = Engine::create_concurrent_with_oplog(
        path, 16 * 1024 * 1024,
    ).expect("create");

    {
        let eng_mut = unsafe { &mut *(Arc::as_ptr(&eng) as *mut Engine) };
        eng_mut.define_himo("marker", ValueType::Tag, 100);
        eng_mut.define_himo("value", ValueType::Tag, 1_000_000);
    }
    let marker_hid = eng.himo_id("marker").unwrap() as u16;
    let value_hid = eng.himo_id("value").unwrap() as u16;

    // 何回か writes + body_msync を交互に走らせて、 異常終了しないこと。
    for batch in 0..5 {
        for i in 0..1000u32 {
            let e = eng.entity().unwrap();
            eng.tie_async_by_id(e, marker_hid, 1);
            eng.tie_async_by_id(e, value_hid, batch * 1000 + i);
        }
        eng.flush_writes();
        // body_msync 直接呼び — dirty range が空でも committed まで広がっても fail しないこと
        eng.body_msync().expect("body_msync 1");
        // 連続呼び (= 2 回目は dirty range 空)
        eng.body_msync().expect("body_msync 2 (idempotent)");
    }
    drop(eng);
    let _ = std::fs::remove_dir_all(path);
}

/// `oplog_sync` 経由でも dirty range path が正しく動くこと。 schema 層の
/// `flush_with_oplog` 系で実際にこのパスを通る。
#[test]
fn oplog_sync_with_dirty_range() {
    let path = tmp_path("test_wal_sync_dirty");
    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.oplog"));
    let _ = std::fs::remove_file(format!("{path}.lock"));

    let eng: Arc<Engine> = Engine::create_concurrent_with_oplog(
        path, 4 * 1024 * 1024,
    ).expect("create");

    {
        let eng_mut = unsafe { &mut *(Arc::as_ptr(&eng) as *mut Engine) };
        eng_mut.define_himo("k", ValueType::Tag, 100);
    }
    let k_hid = eng.himo_id("k").unwrap() as u16;

    for i in 0..200u32 {
        let e = eng.entity().unwrap();
        eng.tie_async_by_id(e, k_hid, i);
    }
    eng.oplog_sync().expect("oplog_sync");
    eng.oplog_sync().expect("oplog_sync idempotent");
    drop(eng);
    let _ = std::fs::remove_dir_all(path);
}
