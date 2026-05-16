//! issue3 regression: sustained 並列 sync writer で undo cap (16M) を踏み抜いて
//! `range out of range` panic していたのが、
//!  - Phase 1: `entity()` の undo を consumer thread に逃がす
//!  - Phase 2: `UndoLog::record` に backpressure (cap 90% 超で force_commit 待機)
//!  - Phase 3: `create_concurrent_with_wal_undo_cap` で cap 可変
//! で耐えること。
//!
//! test は `undo_max_entries = 4096` の小 cap で動かして、 デフォルト 16M を
//! 待たずに backpressure / threshold-trigger path を踏ませる。

use enchudb_engine::{Engine, HimoType};
use std::sync::Arc;

/// Phase 1 regression: 多 entity 経路。 旧コードは writer thread の
/// `entity()` 内で `undo.record(local, 0xFFFF, ..)` を直接呼んでいて、
/// sustained 並列で 16M cap 突破 → OOB panic。
///
/// 修正後: `entity()` は `Op::EntityCreated` を queue に push、 consumer thread
/// が serial に `undo.record_unchecked` を呼ぶ。 consumer ループは threshold
/// 監視して必要なら自発的に `undo.commit()` を fire。
#[test]
fn entity_undo_offloaded_no_overflow() {
    let path = "/tmp/test_entity_undo_offload.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.wal"));
    let _ = std::fs::remove_file(format!("{path}.lock"));

    let eng: Arc<Engine> = Engine::create_concurrent_with_wal_undo_cap(
        path, 16 * 1024 * 1024, 4096,
    ).expect("create");

    {
        let eng_mut = unsafe { &mut *(Arc::as_ptr(&eng) as *mut Engine) };
        eng_mut.define_himo("marker", HimoType::Tag, 100);
        eng_mut.define_himo("value", HimoType::Tag, 100_000);
    }
    let marker_hid = eng.himo_id("marker").unwrap() as u16;
    let value_hid = eng.himo_id("value").unwrap() as u16;

    let n_writers = 4;
    let per = 2_000u32;
    let handles: Vec<_> = (0..n_writers).map(|_| {
        let eng = eng.clone();
        std::thread::spawn(move || {
            for i in 0..per {
                let e = eng.entity();
                eng.tie_async_by_id(e, marker_hid, 1);
                eng.tie_async_by_id(e, value_hid, i);
            }
        })
    }).collect();
    for h in handles { h.join().expect("writer panicked"); }
}

/// Phase 2 regression: `tie_text_to` (sync `&self` writer) 経路。
/// 旧コードは writer thread の `record_undo` が `undo.record` を直接叩いて
/// 16M cap 突破 → OOB panic (sinfohub-server の 100K user load test で発生)。
///
/// 修正後: `UndoLog::record` は cap の 90% で `force_commit` signal を立てて
/// `yield_now` ループ、 consumer が即時 fsync→commit → count reset で再開。
#[test]
fn sync_writer_backpressure_no_overflow() {
    let path = "/tmp/test_sync_writer_backpressure.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.wal"));
    let _ = std::fs::remove_file(format!("{path}.lock"));

    let eng: Arc<Engine> = Engine::create_concurrent_with_wal_undo_cap(
        path, 16 * 1024 * 1024, 4096,
    ).expect("create");

    {
        let eng_mut = unsafe { &mut *(Arc::as_ptr(&eng) as *mut Engine) };
        for i in 0..5 {
            eng_mut.define_himo(&format!("h{i}"), HimoType::Tag, 100_000);
        }
    }

    let n_writers: u32 = 16;
    let per: u32 = 1_000;
    let handles: Vec<_> = (0..n_writers).map(|w| {
        let eng = eng.clone();
        std::thread::spawn(move || {
            let base = w * 100_000;
            for i in 0..per {
                let eid = enchudb_wal::make_eid(eng.peer_id(), base + i);
                let himo = format!("h{}", i % 5);
                let value = format!("v_{w}_{i}");
                eng.tie_text_to(eid, &himo, &value);
            }
        })
    }).collect();
    for h in handles { h.join().expect("writer panicked"); }
}
