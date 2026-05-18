//! issue5 regression: `flush_writes()` が EntityCreated 込み全 op 完了まで待つ
//! 真の barrier として機能すること。
//!
//! 旧 bug: `entity()` で `push_count++` を呼んでなかったため、 1 iter で
//! 1 EntityCreated + 2 Tie を push しても push_count は += 2。 一方 apply_count
//! は EntityCreated でも += 1 で計上していたので、 `applied >= pushed` が
//! Ties 未 apply の状態で成立 → 早期 return → live query が直前の write を
//! 見落とす。

use enchudb_engine::{Engine, HimoType};
use std::sync::Arc;

#[test]
fn flush_writes_waits_for_all_ties_including_entity_created_path() {
    let path = "/tmp/test_flush_barrier.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.wal"));
    let _ = std::fs::remove_file(format!("{path}.lock"));

    // queue_cap を小さめに絞って consumer の drain が writer に追いつかない
    // 状況を作る。
    let eng: Arc<Engine> = Engine::create_concurrent_with_wal_queue_cap(
        path,
        16 * 1024 * 1024,
        1024,
    ).expect("create");

    {
        let eng_mut = unsafe { &mut *(Arc::as_ptr(&eng) as *mut Engine) };
        eng_mut.define_himo("marker", HimoType::Tag, 100);
        eng_mut.define_himo("value", HimoType::Tag, 1_000_000);
    }
    let marker_hid = eng.himo_id("marker").unwrap() as u16;
    let value_hid = eng.himo_id("value").unwrap() as u16;
    let marker_vid = 1u32;

    let n_writers = 4;
    let per = 5_000u32;
    let total_expected = (n_writers * per) as usize;

    let handles: Vec<_> = (0..n_writers).map(|_| {
        let eng = eng.clone();
        std::thread::spawn(move || {
            for i in 0..per {
                let e = eng.entity();
                eng.tie_async_by_id(e, marker_hid, marker_vid);
                eng.tie_async_by_id(e, value_hid, i);
            }
        })
    }).collect();
    for h in handles { h.join().expect("writer panicked"); }

    // flush_writes が真の barrier なら、 直後の query で全 push 結果が見える。
    eng.flush_writes();
    let live = eng.query_by_id(&[(marker_hid, marker_vid)]);
    assert_eq!(
        live.len(),
        total_expected,
        "flush_writes barrier broken: expected {total_expected} entities visible, got {}",
        live.len()
    );
}
