//! #506 の方向の確かめ: append の間に書き手ごとの仕事 (列の書きの代わり) を挟んで、 1 本の log を取り合う形と
//! 書き手ごとに log を持つ形を比べる。 usage: lane_bench <threads> <per_thread> <work_iters> <shared|lanes> [wal_mb]
use enchudb_oplog::oplog::{Op, OpLog};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Barrier;
use std::time::Instant;

// 書き手ごとの仕事: 自分の 4 KiB に 4 語書いて少し計算する (他の core と共有する line に触らない)
#[inline(never)]
fn work(buf: &mut [u64; 512], iters: u32, i: u64) {
    let mut x = i;
    for k in 0..iters {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(k as u64);
    }
    let j = (i as usize * 4) & 511;
    buf[j] = x;
    buf[(j + 1) & 511] = x ^ 1;
    buf[(j + 2) & 511] = x ^ 2;
    buf[(j + 3) & 511] = x ^ 3;
    black_box(buf);
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let threads: usize = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(1);
    let per: u64 = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
    let iters: u32 = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);
    let lanes = a.get(4).map(|s| s == "lanes").unwrap_or(false);
    let wal_mb: usize = a.get(5).and_then(|s| s.parse().ok()).unwrap_or(256);

    // 仕事だけの値段 (1 本、 append なし)
    let work_ns = {
        let mut buf = [0u64; 512];
        let n = 2_000_000u64;
        let t0 = Instant::now();
        for i in 0..n {
            work(&mut buf, iters, i);
        }
        t0.elapsed().as_secs_f64() * 1e9 / n as f64
    };

    let nlogs = if lanes { threads } else { 1 };
    let dir = std::env::temp_dir();
    let paths: Vec<_> = (0..nlogs)
        .map(|k| dir.join(format!("lane_bench_{}_{k}.oplog", std::process::id())))
        .collect();
    let logs: Vec<OpLog> = paths
        .iter()
        .map(|p| {
            let _ = std::fs::remove_file(p);
            let w = OpLog::create(p, wal_mb << 20).unwrap();
            w.set_peer_id(1);
            w.lock_session().unwrap();
            w
        })
        .collect();
    let stop = AtomicBool::new(false);
    let folds = AtomicU64::new(0);
    let barrier = Barrier::new(threads + 1);
    let done = AtomicUsize::new(0);
    let wall = std::thread::scope(|s| {
        let (logs, stop, folds, barrier, done) = (&logs, &stop, &folds, &barrier, &done);
        // 畳む係: 空きが 1/4 を切った log を全部 checkpoint して畳む (満杯で落とさない)
        s.spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                for w in logs {
                    if w.free_bytes() < (wal_mb as u64) << 18 && w.checkpoint_and_reset(|_| true, |_| true).1 {
                        folds.fetch_add(1, Ordering::Relaxed);
                    }
                }
                std::thread::sleep(std::time::Duration::from_micros(200));
            }
        });
        for t in 0..threads {
            s.spawn(move || {
                let wal = &logs[if lanes { t } else { 0 }];
                let mut buf = Box::new([0u64; 512]);
                barrier.wait();
                for i in 0..per {
                    work(&mut buf, iters, i);
                    while wal.append(Op::Tie { eid: (t as u64) << 32 | i, himo_id: 1, value: i }).is_err() {
                        std::thread::yield_now();
                    }
                }
                done.fetch_add(1, Ordering::Release);
            });
        }
        barrier.wait();
        let t0 = Instant::now();
        while done.load(Ordering::Acquire) < threads {
            std::thread::yield_now();
        }
        let wall = t0.elapsed();
        stop.store(true, Ordering::Relaxed);
        wall
    });
    let n = threads as u64 * per;
    println!(
        "{{\"mode\":\"{}\",\"threads\":{threads},\"iters\":{iters},\"work_ns\":{work_ns:.1},\"mops\":{:.3},\"ns_per_op_thread\":{:.1},\"folds\":{}}}",
        if lanes { "lanes" } else { "shared" },
        n as f64 / wall.as_secs_f64() / 1e6,
        wall.as_secs_f64() * 1e9 * threads as f64 / n as f64,
        folds.load(Ordering::Relaxed),
    );
    for p in &paths {
        let _ = std::fs::remove_file(p);
    }
}
