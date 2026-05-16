//! undo region overflow を最小再現 (server path 模倣)。
//!
//! sinfo-store-enchu (schema layer) は **tie_text_to** (sync &self writer) を
//! 経由する。 これは record_undo + 直接 wal.append を writer thread で行う。
//! consumer thread の役目は 100ms 毎の fsync + undo.commit() のみ。
//!
//! sustained 高並列 tie_text_to が走ると、 body_msync が遅くなり、 fsync 間隔が
//! 100ms を超え始める。 その間 undo は累積し、 16M を超えた時点で panic。

use enchudb_engine::{Engine, HimoType};
use std::sync::Arc;
use std::time::Instant;

fn main() {
    let path = "/tmp/undo_overflow.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.wal"));

    // create concurrent + WAL
    let eng: Arc<Engine> =
        Engine::create_concurrent_with_wal(path, 64 * 1024 * 1024).expect("create");

    // server 想定: たくさんの himo を pre-define (project / module / version 等)
    {
        // SAFETY: concurrentize 直後で writer 排他なし、 define_himo は &mut。
        // 並列開始前なので test では Arc::get_mut で取れる。
        let eng_mut = unsafe { &mut *(Arc::as_ptr(&eng) as *mut Engine) };
        for i in 0..20 {
            eng_mut.define_himo(&format!("h{i}"), HimoType::Tag, 100_000);
        }
    }

    // 256 writer × 100K tie_text_to を server と同じ pattern で叩く
    let n_writers: u32 = 256;
    let writes_per_writer: u32 = 100_000;
    let start = Instant::now();
    let handles: Vec<_> = (0..n_writers)
        .map(|w| {
            let eng = eng.clone();
            std::thread::spawn(move || {
                for i in 0..writes_per_writer {
                    // 1 push 相当: 17 ties に近づける (server avg)
                    // 簡略化: 1 writer = 1 logical "push" を 1 tie_text_to で表現、
                    // total = 256 × 100K = 25.6M tie。 各 tie は record_undo を経由。
                    let eid = enchudb_wal::make_eid(eng.peer_id(), (w * 1000 + (i % 1000)) as u32);
                    let himo = format!("h{}", i % 20);
                    let value = format!("v_{}_{}_{}",  w, i, i.wrapping_mul(2654435761));
                    eng.tie_text_to(eid, &himo, &value);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("writer thread panicked");
    }
    println!(
        "done {} ties in {:.2?}",
        n_writers * writes_per_writer,
        start.elapsed()
    );
}
