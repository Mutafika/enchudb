//! v28 WAL ベンチ。WAL 有効 vs 無効で tie_async の速度を比較。
//!
//! 目標: WAL 有効時でも tie_async は 2μs 以下(無効時の 2 倍以内)。

#[cfg(feature = "v27")]
fn main() {
    use enchudb::{Engine, HimoType};
    use std::time::Instant;

    const N: u32 = 100_000;

    // --- WAL 無し ---
    let path_no = format!("/tmp/enchudb-v28-nowal-{}", std::process::id());
    let _ = std::fs::remove_file(&path_no);
    let _ = std::fs::remove_file(format!("{}.wal", path_no));

    {
        let mut e = Engine::create_with_capacity(&path_no, N + 100).unwrap();
        e.define_himo("v", HimoType::Value, 10_000);
        e.flush().unwrap();
    }

    let eng = {
        let e = Engine::open(&path_no).unwrap();
        Engine::concurrentize(e)
    };

    // 全 entity 事前生成(entity() のコストを除外)
    let eids: Vec<u32> = (0..N).map(|_| eng.entity()).collect();

    let t0 = Instant::now();
    for i in 0..N {
        eng.tie_async(eids[i as usize], "v", i);
    }
    eng.flush_writes();
    let el_no = t0.elapsed();
    println!("WAL off : {:>8} ns/op ({:>6.2} ms total)",
        el_no.as_nanos() / N as u128, el_no.as_secs_f64() * 1000.0);
    drop(eng);
    let _ = std::fs::remove_file(&path_no);
    let _ = std::fs::remove_file(format!("{}.wal", path_no));

    // --- WAL 有り ---
    let path_wal = format!("/tmp/enchudb-v28-wal-{}", std::process::id());
    let _ = std::fs::remove_file(&path_wal);
    let _ = std::fs::remove_file(format!("{}.wal", path_wal));

    {
        let mut e = Engine::create_with_capacity(&path_wal, N + 100).unwrap();
        e.define_himo("v", HimoType::Value, 10_000);
        e.flush().unwrap();
    }

    let eng = Engine::open_concurrent_with_wal(&path_wal, 256 * 1024 * 1024).unwrap();
    let eids: Vec<u32> = (0..N).map(|_| eng.entity()).collect();

    let t0 = Instant::now();
    for i in 0..N {
        eng.tie_async(eids[i as usize], "v", i);
    }
    eng.flush_writes();
    eng.wal_commit();
    let el_wal = t0.elapsed();
    println!("WAL on  : {:>8} ns/op ({:>6.2} ms total)",
        el_wal.as_nanos() / N as u128, el_wal.as_secs_f64() * 1000.0);

    // fsync 付き(Sync モード相当)
    let t0 = Instant::now();
    for i in 0..1000u32 {
        eng.tie_async(eids[(i % N as u32) as usize], "v", 42);
        eng.wal_commit();
        eng.wal_sync().unwrap();
    }
    let el_sync = t0.elapsed();
    println!("WAL sync: {:>8} ns/op ({:>6.2} ms total, 1k ops)",
        el_sync.as_nanos() / 1000, el_sync.as_secs_f64() * 1000.0);

    drop(eng);

    let ratio = el_wal.as_nanos() as f64 / el_no.as_nanos() as f64;
    println!("\n{}x slowdown with WAL (target: <= 2x)", ratio);

    let _ = std::fs::remove_file(&path_wal);
    let _ = std::fs::remove_file(format!("{}.wal", path_wal));
}

#[cfg(not(feature = "v27"))]
fn main() { println!("Build with --features v27"); }
