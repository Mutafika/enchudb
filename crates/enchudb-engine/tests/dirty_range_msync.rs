//! request3 regression: body_msync が dirty range だけを sync する。
//!
//! 旧実装は `flush(0, committed())` で committed 全体を msync していた。
//! 修正後は writer が触った範囲だけ msync される。 直接 dirty range の
//! offset を観測する API は出していないので、 ここでは
//!  - sustained 書き込み中の `body_msync()` が連続呼び出しでも fail しない
//!  - 書き込み後 `body_msync` → 再度 `body_msync` で 2 回目が clean (no-op)
//! の最低限の整合性のみテスト。 perf は本 test では確認しない (CI 不安定化回避)。

use enchudb_engine::{Engine, HimoType};
use std::sync::Arc;

#[test]
fn body_msync_handles_dirty_range_correctly() {
    let path = "/tmp/test_dirty_range_msync.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.wal"));
    let _ = std::fs::remove_file(format!("{path}.lock"));

    let eng: Arc<Engine> = Engine::create_concurrent_with_wal(
        path, 16 * 1024 * 1024,
    ).expect("create");

    {
        let eng_mut = unsafe { &mut *(Arc::as_ptr(&eng) as *mut Engine) };
        eng_mut.define_himo("marker", HimoType::Tag, 100);
        eng_mut.define_himo("value", HimoType::Tag, 1_000_000);
    }
    let marker_hid = eng.himo_id("marker").unwrap() as u16;
    let value_hid = eng.himo_id("value").unwrap() as u16;

    // 何回か writes + body_msync を交互に走らせて、 異常終了しないこと。
    for batch in 0..5 {
        for i in 0..1000u32 {
            let e = eng.entity();
            eng.tie_async_by_id(e, marker_hid, 1);
            eng.tie_async_by_id(e, value_hid, batch * 1000 + i);
        }
        eng.flush_writes();
        // body_msync 直接呼び — dirty range が空でも committed まで広がっても fail しないこと
        eng.body_msync().expect("body_msync 1");
        // 連続呼び (= 2 回目は dirty range 空)
        eng.body_msync().expect("body_msync 2 (idempotent)");
    }
}

/// `wal_sync` 経由でも dirty range path が正しく動くこと。 schema 層の
/// `flush_with_wal` 系で実際にこのパスを通る。
#[test]
fn wal_sync_with_dirty_range() {
    let path = "/tmp/test_wal_sync_dirty.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.wal"));
    let _ = std::fs::remove_file(format!("{path}.lock"));

    let eng: Arc<Engine> = Engine::create_concurrent_with_wal(
        path, 4 * 1024 * 1024,
    ).expect("create");

    {
        let eng_mut = unsafe { &mut *(Arc::as_ptr(&eng) as *mut Engine) };
        eng_mut.define_himo("k", HimoType::Tag, 100);
    }
    let k_hid = eng.himo_id("k").unwrap() as u16;

    for i in 0..200u32 {
        let e = eng.entity();
        eng.tie_async_by_id(e, k_hid, i);
    }
    eng.wal_sync().expect("wal_sync");
    eng.wal_sync().expect("wal_sync idempotent");
}
