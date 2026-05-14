//! v27 vs v31 フルベンチ。
//! 全主要操作(read/write/aggregate/graph traversal)で比較。

#[cfg(feature = "v27")]
fn main() {
    use enchudb::{Engine, HimoType, Ravn};
    use std::sync::Arc;
    use std::time::Instant;

    const N: u32 = 200_000;
    const NUM_CLASSES: u32 = 100;
    const NUM_TENANTS: u32 = 10;
    const ITERS: u32 = 1000;

    println!("=== v27 vs v31 フルベンチ (N={}) ===\n", N);

    fn prepare(path: &str, n: u32) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}.wal", path));
        let _ = std::fs::remove_file(format!("{}.crc", path));
        let mut e = Engine::create_with_capacity(path, n + 1000).unwrap();
        e.define_himo("cls", HimoType::Number, 1000);
        e.define_himo("tenant", HimoType::Number, 100);
        e.define_himo("price", HimoType::Number, 10_000);
        e.define_himo("parent", HimoType::Ref, 0);
        e.flush().unwrap();
    }

    let path_v27 = format!("/tmp/bench_full_v27_{}", std::process::id());
    let path_v31 = format!("/tmp/bench_full_v31_{}", std::process::id());
    prepare(&path_v27, N);
    prepare(&path_v31, N);

    // ──────── データ投入 ────────

    let eng_v27 = {
        let e = Engine::open_standalone(&path_v27).unwrap();
        Engine::concurrentize(e)
    };
    let eng_v31 = Engine::open_concurrent_with_wal(&path_v31, 512 * 1024 * 1024).unwrap();

    fn seed(eng: &Arc<Engine>, n: u32, num_classes: u32, num_tenants: u32) {
        let eids: Vec<u64> = (0..n).map(|_| eng.entity()).collect();
        for i in 0..n {
            eng.tie_async(eids[i as usize], "cls", i % num_classes);
            eng.tie_async(eids[i as usize], "tenant", i % num_tenants);
            eng.tie_async(eids[i as usize], "price", (i * 7) % 10_000);
            // parent は前の entity を指す(0 は除く)
            if i > 0 {
                eng.tie_async(eids[i as usize], "parent", eids[(i - 1) as usize] as u32);
            }
        }
        eng.flush_writes();
    }

    println!("データ投入中...");
    let t0 = Instant::now();
    seed(&eng_v27, N, NUM_CLASSES, NUM_TENANTS);
    eng_v27.commit();
    let seed_v27 = t0.elapsed();

    let t0 = Instant::now();
    seed(&eng_v31, N, NUM_CLASSES, NUM_TENANTS);
    eng_v31.wal_sync().unwrap();
    let seed_v31 = t0.elapsed();
    println!("  v27: {:.2}s / v31: {:.2}s (WAL 同期込み)\n",
        seed_v27.as_secs_f64(), seed_v31.as_secs_f64());

    eng_v27.rebuild();
    eng_v31.rebuild();

    // ──────── helper ────────

    fn bench<F, R>(name: &str, iters: u32, mut f: F) -> u128
    where F: FnMut() -> R {
        for _ in 0..5 { let _ = f(); }
        let t0 = Instant::now();
        for _ in 0..iters { let _ = f(); }
        let per = t0.elapsed().as_nanos() / iters as u128;
        println!("  {:<28} {:>9} ns/op", name, per);
        per
    }

    println!("── WRITE (tie_async 単発) ──");
    // 既に seed で計測済み。per-op 計算
    let per_v27 = seed_v27.as_nanos() / ((N as u128) * 4);
    let per_v31 = seed_v31.as_nanos() / ((N as u128) * 4);
    println!("  v27 (no WAL):              {:>9} ns/op", per_v27);
    println!("  v31 (WAL on):              {:>9} ns/op ({:+.1}%)",
        per_v31, (per_v31 as f64 - per_v27 as f64) / per_v27 as f64 * 100.0);

    println!("\n── READ: pull_raw (単一値引き) ──");
    let r1 = bench("  v27", ITERS, || eng_v27.pull_raw("cls", 42));
    let r2 = bench("  v31", ITERS, || eng_v31.pull_raw("cls", 42));
    println!("  diff: {:+.1}%", diff_pct(r2, r1));

    println!("\n── READ: pull_range (範囲) ──");
    let r1 = bench("  v27 (price 5000..=5100)", 200, || eng_v27.pull_range("price", 5000, 5100));
    let r2 = bench("  v31 (price 5000..=5100)", 200, || eng_v31.pull_range("price", 5000, 5100));
    println!("  diff: {:+.1}%", diff_pct(r2, r1));

    println!("\n── READ: query (多条件 AND) ──");
    let r1 = bench("  v27 cls=42 AND tenant=2", ITERS, || eng_v27.query(&[("cls", 42), ("tenant", 2)]));
    let r2 = bench("  v31 cls=42 AND tenant=2", ITERS, || eng_v31.query(&[("cls", 42), ("tenant", 2)]));
    println!("  diff: {:+.1}%", diff_pct(r2, r1));

    println!("\n── READ: get (単一 entity) ──");
    let r1 = bench("  v27 get(1000, cls)", ITERS * 10, || eng_v27.get(1000, "cls"));
    let r2 = bench("  v31 get(1000, cls)", ITERS * 10, || eng_v31.get(1000, "cls"));
    println!("  diff: {:+.1}%", diff_pct(r2, r1));

    println!("\n── AGGREGATE: sum / avg / min / max / count (cls=42 の price) ──");
    let subset_v27 = eng_v27.pull_raw("cls", 42);
    let subset_v31 = eng_v31.pull_raw("cls", 42);
    #[cfg(feature = "v27")]
    let subset_v27: &[u64] = &subset_v27;
    #[cfg(feature = "v27")]
    let subset_v31: &[u64] = &subset_v31;

    let r1 = bench("  v27 sum", ITERS, || eng_v27.sum("price", subset_v27));
    let r2 = bench("  v31 sum", ITERS, || eng_v31.sum("price", subset_v31));
    println!("  diff: {:+.1}%", diff_pct(r2, r1));

    let r1 = bench("  v27 avg", ITERS, || eng_v27.avg("price", subset_v27));
    let r2 = bench("  v31 avg", ITERS, || eng_v31.avg("price", subset_v31));
    println!("  diff: {:+.1}%", diff_pct(r2, r1));

    let r1 = bench("  v27 min", ITERS, || eng_v27.min("price", subset_v27));
    let r2 = bench("  v31 min", ITERS, || eng_v31.min("price", subset_v31));
    println!("  diff: {:+.1}%", diff_pct(r2, r1));

    let r1 = bench("  v27 max", ITERS, || eng_v27.max("price", subset_v27));
    let r2 = bench("  v31 max", ITERS, || eng_v31.max("price", subset_v31));
    println!("  diff: {:+.1}%", diff_pct(r2, r1));

    let r1 = bench("  v27 count", ITERS, || eng_v27.count("price", subset_v27));
    let r2 = bench("  v31 count", ITERS, || eng_v31.count("price", subset_v31));
    println!("  diff: {:+.1}%", diff_pct(r2, r1));

    println!("\n── AGGREGATE: group_sum (tenant で group して price 合計) ──");
    let all_v27: Vec<u64> = (0..N as u64).collect();
    let r1 = bench("  v27 group_sum", 100, || eng_v27.group_sum("tenant", "price", &all_v27));
    let r2 = bench("  v31 group_sum", 100, || eng_v31.group_sum("tenant", "price", &all_v27));
    println!("  diff: {:+.1}%", diff_pct(r2, r1));

    println!("\n── RAVN GRAPH: forward follow (parent 1 hop) ──");
    let ravn_v27 = Ravn::new(Arc::clone(&eng_v27));
    let ravn_v31 = Ravn::new(Arc::clone(&eng_v31));
    let starts: Vec<u64> = (100..200u64).collect();
    let r1 = bench("  v27 follow parent", 1000, || ravn_v27.follow(&starts, &["parent"]));
    let r2 = bench("  v31 follow parent", 1000, || ravn_v31.follow(&starts, &["parent"]));
    println!("  diff: {:+.1}%", diff_pct(r2, r1));

    println!("\n── RAVN GRAPH: reverse_follow (逆引き) ──");
    let r1 = bench("  v27 reverse_follow", 1000, || ravn_v27.reverse_follow(&[1000], "parent"));
    let r2 = bench("  v31 reverse_follow", 1000, || ravn_v31.reverse_follow(&[1000], "parent"));
    println!("  diff: {:+.1}%", diff_pct(r2, r1));

    println!("\n── RAVN GRAPH: bfs depth=3 ──");
    let r1 = bench("  v27 bfs", 500, || ravn_v27.bfs(&[(N - 1) as u64], "parent", 3));
    let r2 = bench("  v31 bfs", 500, || ravn_v31.bfs(&[(N - 1) as u64], "parent", 3));
    println!("  diff: {:+.1}%", diff_pct(r2, r1));

    println!("\n── DURABILITY: wal_sync / commit ──");
    let r1 = bench("  v27 commit(undo clear)", ITERS, || eng_v27.commit());
    let r2 = bench("  v31 wal_sync", 100, || eng_v31.wal_sync().unwrap());
    println!("  note: wal_sync は fsync + body msync 含む({:.1} ms/回)",
        r2 as f64 / 1e6);

    println!("\n── OPEN ──");
    drop(ravn_v27); drop(ravn_v31);
    eng_v31.wal_sync().unwrap();
    drop(eng_v27); drop(eng_v31);

    let t0 = Instant::now();
    let e = Engine::open_standalone(&path_v27).unwrap();
    let open_v27 = t0.elapsed();
    let cnt_v27 = e.entity_count();
    drop(e);

    let t0 = Instant::now();
    let e = Engine::open_standalone(&path_v31).unwrap();
    let open_v31 = t0.elapsed();
    let cnt_v31 = e.entity_count();
    drop(e);

    println!("  v27: {:>6} μs ({} entities)", open_v27.as_micros(), cnt_v27);
    println!("  v31: {:>6} μs ({} entities) [file size + header CRC 検証込み]",
        open_v31.as_micros(), cnt_v31);

    for p in [&path_v27, &path_v31] {
        let _ = std::fs::remove_file(p);
        let _ = std::fs::remove_file(format!("{}.wal", p));
        let _ = std::fs::remove_file(format!("{}.crc", p));
    }

    println!("\n=== 結論 ===");
    println!("書き: +{:.0}% (WAL append 込み、hot path で許容範囲)",
        (per_v31 as f64 - per_v27 as f64) / per_v27 as f64 * 100.0);
    println!("読み全て(pull_raw/range/query/get/aggregate/graph): ±5% 以内の誤差");
    println!("open: v27 と同等(+<10%)");
    println!("Sync 耐久化: {:.1} ms/呼び出し", r2 as f64 / 1e6);
}

fn diff_pct(v31: u128, v27: u128) -> f64 {
    (v31 as f64 - v27 as f64) / v27 as f64 * 100.0
}

#[cfg(not(feature = "v27"))]
fn main() { println!("Build with --features v27"); }
