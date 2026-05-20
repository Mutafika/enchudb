//! issue4 regression: write_queue / oplog_record_queue を bounded ArrayQueue 化、
//! producer は queue 満杯時に yield-spin で block する。 small cap で writer 数 >>
//! consumer rate な状況を作って、 hang せず全 push が完走する事を確認。

use enchudb_engine::{Engine, HimoType};
use std::sync::Arc;

/// queue cap を 64 に絞って、 8 writer × 200 ops を投げる。 旧 unbounded queue
/// なら全部 push 通って終わる。 bounded 化後は cap 超え時に block するが、
/// consumer 進捗で drain → push 成功するはず。 hang しなければ pass。
#[test]
fn small_queue_cap_does_not_hang() {
    let path = "/tmp/test_queue_backpressure.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.oplog"));
    let _ = std::fs::remove_file(format!("{path}.lock"));

    let eng: Arc<Engine> = Engine::create_concurrent_with_oplog_queue_cap(
        path,
        4 * 1024 * 1024, // oplog_capacity 4 MB
        64,              // queue_capacity (tiny — 強制的に backpressure 発動)
    ).expect("create");

    {
        let eng_mut = unsafe { &mut *(Arc::as_ptr(&eng) as *mut Engine) };
        eng_mut.define_himo("marker", HimoType::Tag, 100);
    }
    let marker_hid = eng.himo_id("marker").unwrap() as u16;

    let n_writers: u32 = 8;
    let per: u32 = 200;
    let handles: Vec<_> = (0..n_writers).map(|w| {
        let eng = eng.clone();
        std::thread::spawn(move || {
            for i in 0..per {
                let e = eng.entity();
                eng.tie_async_by_id(e, marker_hid, w * 1000 + i);
            }
        })
    }).collect();
    for h in handles { h.join().expect("writer panicked"); }
}
