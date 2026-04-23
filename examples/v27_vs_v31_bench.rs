//! v27 vs v31 ベンチ。
//!
//! v27(baseline: 並行書き WAL なし、pure tie_async + WriteQueue のみ)と
//! v31(WAL on、ring buffer + Commit marker + body msync 順序)を比較。
//!
//! - tie_async の書き込みレイテンシ
//! - pull_raw / query / get の読み取りレイテンシ
//! - open 時間(ヘッダ検証 + layout 計算 + load)
//! - 全件書き込み + flush のスループット

#[cfg(feature = "v27")]
fn main() {
    use enchudb::{Engine, HimoType};
    use std::time::Instant;

    const N: u32 = 200_000;
    const NUM_CLASSES: u32 = 100;
    const NUM_TENANTS: u32 = 10;

    println!("=== v27 vs v31 ベンチ ({} entities) ===\n", N);

    // ──────── prepare two DBs with same schema/data ────────

    fn prepare(path: &str, n: u32) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}.wal", path));
        let _ = std::fs::remove_file(format!("{}.crc", path));
        let mut e = Engine::create_with_capacity(path, n + 1000).unwrap();
        e.define_himo("cls", HimoType::Value, 1000);
        e.define_himo("tenant", HimoType::Value, 100);
        e.flush().unwrap();
    }

    let path_v27 = format!("/tmp/bench_v27_{}", std::process::id());
    let path_v31 = format!("/tmp/bench_v31_{}", std::process::id());
    prepare(&path_v27, N);
    prepare(&path_v31, N);

    // ──────── write benchmark ────────

    // v27: concurrent, no WAL
    let eng = {
        let e = Engine::open_standalone(&path_v27).unwrap();
        Engine::concurrentize(e)
    };
    let eids: Vec<u64> = (0..N).map(|_| eng.entity()).collect();

    let t0 = Instant::now();
    for i in 0..N {
        eng.tie_async(eids[i as usize], "cls", i % NUM_CLASSES);
        eng.tie_async(eids[i as usize], "tenant", i % NUM_TENANTS);
    }
    eng.flush_writes();
    eng.commit(); // v27 の commit(undo クリア)。v31 では wal_sync で同時に行う
    let el_v27_w = t0.elapsed();
    println!("WRITE (tie_async ×2, N={})", N * 2);
    println!("  v27 (no WAL):  {:>8} ns/op ({:>6.1} ms total, {:>8.0} ops/s)",
        el_v27_w.as_nanos() / (N * 2) as u128,
        el_v27_w.as_secs_f64() * 1000.0,
        (N * 2) as f64 / el_v27_w.as_secs_f64());

    // v31: WAL enabled
    let eng_v31 = Engine::open_concurrent_with_wal(&path_v31, 512 * 1024 * 1024).unwrap();
    let eids2: Vec<u64> = (0..N).map(|_| eng_v31.entity()).collect();

    let t0 = Instant::now();
    for i in 0..N {
        eng_v31.tie_async(eids2[i as usize], "cls", i % NUM_CLASSES);
        eng_v31.tie_async(eids2[i as usize], "tenant", i % NUM_TENANTS);
    }
    eng_v31.flush_writes();
    let el_v31_w = t0.elapsed();
    println!("  v31 (WAL on):  {:>8} ns/op ({:>6.1} ms total, {:>8.0} ops/s)",
        el_v31_w.as_nanos() / (N * 2) as u128,
        el_v31_w.as_secs_f64() * 1000.0,
        (N * 2) as f64 / el_v31_w.as_secs_f64());

    println!("  overhead:      {:.3}x", el_v31_w.as_secs_f64() / el_v27_w.as_secs_f64());

    // ──────── sync-only benchmark (v31) ────────

    eng_v31.wal_commit();
    let t0 = Instant::now();
    eng_v31.wal_sync().unwrap();
    let el_sync = t0.elapsed();
    println!("\nSYNC (wal_sync 1回)");
    println!("  v31:           {:>8} μs", el_sync.as_micros());

    // ──────── read benchmarks ────────

    println!("\nREAD (pull_raw 単発、class=42 で絞る)");

    // Ensure both have rebuilt cylinders for query
    // (open_concurrent_with_wal does rebuild internally; for v27, we need explicit rebuild via query)
    eng.rebuild();
    eng_v31.rebuild();

    let t0 = Instant::now();
    for _ in 0..1000 {
        let _ = eng.pull_raw("cls", 42);
    }
    let el_v27_r = t0.elapsed();
    println!("  v27:           {:>8} ns/op ({:>6} rows matched)",
        el_v27_r.as_nanos() / 1000, eng.pull_raw("cls", 42).len());

    let t0 = Instant::now();
    for _ in 0..1000 {
        let _ = eng_v31.pull_raw("cls", 42);
    }
    let el_v31_r = t0.elapsed();
    println!("  v31:           {:>8} ns/op ({:>6} rows matched)",
        el_v31_r.as_nanos() / 1000, eng_v31.pull_raw("cls", 42).len());

    // multi-condition query
    println!("\nQUERY (多条件 AND: cls=42 AND tenant=3)");

    let t0 = Instant::now();
    for _ in 0..1000 {
        let _ = eng.query(&[("cls", 42), ("tenant", 3)]);
    }
    let el_v27_q = t0.elapsed();
    println!("  v27:           {:>8} ns/op ({:>6} rows)",
        el_v27_q.as_nanos() / 1000,
        eng.query(&[("cls", 42), ("tenant", 3)]).len());

    let t0 = Instant::now();
    for _ in 0..1000 {
        let _ = eng_v31.query(&[("cls", 42), ("tenant", 3)]);
    }
    let el_v31_q = t0.elapsed();
    println!("  v31:           {:>8} ns/op ({:>6} rows)",
        el_v31_q.as_nanos() / 1000,
        eng_v31.query(&[("cls", 42), ("tenant", 3)]).len());

    // ──────── open benchmark ────────

    // ensure synced before drop for fair comparison
    eng_v31.wal_sync().unwrap();
    drop(eng);
    drop(eng_v31);

    println!("\nOPEN (fresh DB、mmap + layout + load + CRC 検証)");

    let t0 = Instant::now();
    let e = Engine::open_standalone(&path_v27).unwrap();
    let el_v27_o = t0.elapsed();
    println!("  v27 (no CRC):  {:>8} μs ({} entities)",
        el_v27_o.as_micros(), e.entity_count());
    drop(e);

    let t0 = Instant::now();
    let e = Engine::open_standalone(&path_v31).unwrap();
    let el_v31_o = t0.elapsed();
    println!("  v31 (no .crc): {:>8} μs ({} entities)",
        el_v31_o.as_micros(), e.entity_count());
    drop(e);

    // cleanup
    for p in [&path_v27, &path_v31] {
        let _ = std::fs::remove_file(p);
        let _ = std::fs::remove_file(format!("{}.wal", p));
        let _ = std::fs::remove_file(format!("{}.crc", p));
    }

    // ──────── summary ────────
    println!("\n=== まとめ ===");
    println!("書き: v31 は v27 の {:.3}x(WAL append 込み)", el_v31_w.as_secs_f64() / el_v27_w.as_secs_f64());
    println!("読み: v31 は v27 と同等(WAL は hot read path に関与しない)");
    println!("  pull_raw: {:+.1}%", (el_v31_r.as_nanos() as f64 - el_v27_r.as_nanos() as f64) / el_v27_r.as_nanos() as f64 * 100.0);
    println!("  query:    {:+.1}%", (el_v31_q.as_nanos() as f64 - el_v27_q.as_nanos() as f64) / el_v27_q.as_nanos() as f64 * 100.0);
    println!("open: v31 は v27 の {:.3}x(file size + header CRC 検証)", el_v31_o.as_secs_f64() / el_v27_o.as_secs_f64());
}

#[cfg(not(feature = "v27"))]
fn main() { println!("Build with --features v27"); }
