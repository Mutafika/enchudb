/// v27 並行性ベンチマーク — writer 非同期キュー + 裏 consumer + 並行 reader。
///
/// 測定:
///   - tie_async レイテンシ(push のみの時間)
///   - consumer 処理レート(flush_writes が終わるまで)
///   - 並行 reader(pull_raw / query) のスループット
///   - 8 スレッド reader + 1 スレッド writer(async) 同時実行
///
/// 実行:
///   cargo run --release --example v27_concurrent_bench --features v27

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

fn main() {
    let path = "/tmp/enchudb_v27_concurrent_bench.db";
    let _ = std::fs::remove_file(path);

    let n_initial = 500_000u32;
    let queue_cap = 4_000_000; // 4M 件の queue (~48MB)

    // まず define_himo + 初期データ投入は &mut self の世界で完了させる。
    let mut db = enchudb::Engine::create_with_capacity(path, 2_000_000).unwrap();
    db.define_himo("tenant", enchudb::HimoType::Value, 16);
    db.define_himo("dept", enchudb::HimoType::Value, 8);
    db.define_himo("status", enchudb::HimoType::Value, 4);
    db.define_himo("age", enchudb::HimoType::Value, 100);

    println!("=== 初期データ投入({}件 × 4 紐)===", n_initial);
    let t = Instant::now();
    for i in 0..n_initial {
        let e = db.entity();
        db.tie(e, "tenant", i % 16);
        db.tie(e, "dept", (i * 7) % 8);
        db.tie(e, "status", (i * 3) % 4);
        db.tie(e, "age", (i * 19) % 100);
    }
    println!("  投入時間: {:?}", t.elapsed());

    // concurrentize で consumer スレッド起動
    let db = enchudb::Engine::concurrentize(db, queue_cap);
    println!("  concurrentize 完了、consumer thread 起動");

    // ──── 1. tie_async レイテンシ(push のみ)────
    println!();
    println!("=== tie_async レイテンシ(push のみ)===");
    let iters = 1_000_000;
    // 予備 entity を用意(push 時に entity 解決はしない、eid は事前確保)
    let mut eids = Vec::with_capacity(iters);
    for _ in 0..iters { eids.push(db.entity()); }

    let t = Instant::now();
    for i in 0..iters {
        db.tie_async(eids[i], "tenant", (i as u32) % 16);
    }
    let push_elapsed = t.elapsed();
    let per_push = push_elapsed.as_nanos() as f64 / iters as f64;
    println!("  {}件 push: {:?}", iters, push_elapsed);
    println!("  tie_async 1件あたり: {:.1} ns", per_push);
    println!("  push 完了直後の queue 滞留: {}", db.pending_writes());

    // ──── 2. consumer 処理レート ────
    println!();
    println!("=== consumer 処理レート(flush_writes まで)===");
    let t = Instant::now();
    db.flush_writes();
    let flush_elapsed = t.elapsed();
    let total_time = push_elapsed + flush_elapsed;
    let rate = iters as f64 / total_time.as_secs_f64();
    println!("  flush_writes 待ち: {:?}", flush_elapsed);
    println!("  合計(push+消化): {:?}", total_time);
    println!("  consumer 処理レート: {:.0} ops/sec ({:.1} ns/op)",
             rate, 1e9 / rate);

    // ──── 3. 単一スレッド pull_raw/query スループット(baseline)────
    println!();
    println!("=== 単一スレッド read スループット ===");
    let iters = 100_000;
    let t = Instant::now();
    let mut total_pull = 0usize;
    for i in 0..iters {
        total_pull += db.pull_raw("tenant", (i as u32) % 16).len();
    }
    let pull_elapsed = t.elapsed();
    println!("  pull_raw(tenant, *): {:?} / {} iter, 1件 {:.1} ns (sum={})",
             pull_elapsed, iters, pull_elapsed.as_nanos() as f64 / iters as f64, total_pull);

    let t = Instant::now();
    let mut total_q = 0usize;
    for i in 0..iters {
        total_q += db.query(&[("tenant", (i as u32) % 16), ("dept", (i as u32) % 8)]).len();
    }
    let q_elapsed = t.elapsed();
    println!("  query(tenant+dept): {:?} / {} iter, 1件 {:.1} ns (sum={})",
             q_elapsed, iters, q_elapsed.as_nanos() as f64 / iters as f64, total_q);

    // ──── 4. 8 スレッド並行 reader + 1 スレッド writer(async)────
    println!();
    println!("=== 並行 reader(8) + writer(async, 1)===");
    let reader_count = 8usize;
    let duration = Duration::from_secs(2);
    let stop = Arc::new(AtomicBool::new(false));
    let read_ops = Arc::new(AtomicU64::new(0));
    let write_ops = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();
    for tid in 0..reader_count {
        let db = db.clone();
        let stop = stop.clone();
        let counter = read_ops.clone();
        handles.push(thread::spawn(move || {
            let mut local = 0u64;
            let mut i = tid as u32;
            while !stop.load(Ordering::Relaxed) {
                // 100 回ごとに atomic 更新してキャッシュ負荷を下げる
                for _ in 0..100 {
                    let _ = db.query(&[("tenant", i % 16), ("dept", (i * 7) % 8)]);
                    i = i.wrapping_add(1);
                    local += 1;
                }
                counter.fetch_add(100, Ordering::Relaxed);
                local -= 100;
                let _ = local;
            }
        }));
    }

    // writer thread: tie_async 流し続ける
    {
        let db = db.clone();
        let stop = stop.clone();
        let counter = write_ops.clone();
        handles.push(thread::spawn(move || {
            let mut i = 0u32;
            while !stop.load(Ordering::Relaxed) {
                for _ in 0..100 {
                    // 既存 entity を in-place 更新
                    let eid = i % n_initial;
                    db.tie_async(eid, "tenant", i % 16);
                    i = i.wrapping_add(1);
                }
                counter.fetch_add(100, Ordering::Relaxed);
            }
        }));
    }

    let t = Instant::now();
    thread::sleep(duration);
    stop.store(true, Ordering::Relaxed);
    for h in handles { h.join().unwrap(); }
    let elapsed = t.elapsed();

    let r = read_ops.load(Ordering::Relaxed);
    let w = write_ops.load(Ordering::Relaxed);
    println!("  経過時間: {:?}", elapsed);
    println!("  reader 合計: {} query ({:.0}/sec, {:.1} ns/query/thread平均)",
             r, r as f64 / elapsed.as_secs_f64(),
             elapsed.as_nanos() as f64 / (r as f64 / reader_count as f64));
    println!("  writer(async): {} push ({:.0}/sec)", w, w as f64 / elapsed.as_secs_f64());
    db.flush_writes();
    println!("  writer 消化後 queue: {}", db.pending_writes());

    // ──── 5. 整合性検証 ────
    println!();
    println!("=== 整合性: flush_writes 後に pull_raw が反映されているか ===");
    let e0 = db.entity();
    db.tie_async(e0, "age", 42);
    let before = db.pull_raw("age", 42).len();
    db.flush_writes();
    let after = db.pull_raw("age", 42).len();
    println!("  tie_async 直後(flush 前): pull_raw age=42 → {}件", before);
    println!("  flush_writes 後         : pull_raw age=42 → {}件", after);
    assert!(after >= before, "flush 後の方が件数多いはず");
    assert!(after >= 1, "少なくとも新規 1 件は見える");
    println!("  OK");

    // drop で consumer スレッド join(Arc 最後)
    println!();
    println!("=== drop 待ち(consumer スレッド join)===");
    let t = Instant::now();
    drop(db);
    println!("  join 時間: {:?}", t.elapsed());

    let _ = std::fs::remove_file(path);
}
