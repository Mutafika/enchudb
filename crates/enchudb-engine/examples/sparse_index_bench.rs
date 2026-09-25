//! 値の種類が多い列 (秒の時刻など 2^20 以上) の索引: メモリ (RSS) / 書き込み / 等値検索。
use enchudb_engine::{Engine, ValueType};
use std::time::Instant;

fn rss_mb() -> f64 {
    let out = std::process::Command::new("ps").args(["-o", "rss=", "-p", &std::process::id().to_string()]).output().unwrap();
    String::from_utf8_lossy(&out.stdout).trim().parse::<f64>().unwrap() / 1024.0
}

fn main() {
    let n: u32 = std::env::var("N").ok().and_then(|v| v.parse().ok()).unwrap_or(1_000_000);
    let p = format!("{}/sparse-bench-{}", std::env::temp_dir().display(), std::process::id());
    let _ = std::fs::remove_dir_all(&p);
    let mut eng = Engine::create_growable_with_capacity(&p, 3 * n).unwrap();
    eng.define_himo("ts", ValueType::Number, 0);
    let base = 1_700_000_000u32;
    let mut es = Vec::with_capacity(2 * n as usize);
    let t = Instant::now();
    for i in 0..n {
        let e = eng.entity().unwrap();
        eng.tie(e, "ts", base + i);
        es.push(e);
    }
    let bulk = t.elapsed();
    let r0 = rss_mb();
    let t = Instant::now();
    assert_eq!(eng.pull_raw("ts", base).len(), 1); // 索引を組む
    let build = t.elapsed();
    let r1 = rss_mb();
    let t = Instant::now();
    for i in 0..n {
        let e = eng.entity().unwrap();
        eng.tie(e, "ts", base + n + i);
        es.push(e);
    }
    let live = t.elapsed();
    let r2 = rss_mb();
    let t = Instant::now();
    let mut x = 0x1234_5678u64;
    let q = 200_000;
    let mut hits = 0;
    for _ in 0..q {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        hits += eng.pull_raw("ts", base + (x % (2 * n as u64)) as u32).len();
    }
    let look = t.elapsed();
    assert_eq!(hits, q);
    // 書き換え (古い entry が出る) 後の検索
    let t = Instant::now();
    for (i, &e) in es.iter().enumerate().take(n as usize) {
        eng.tie(e, "ts", base + 3 * n + i as u32);
    }
    let churn = t.elapsed();
    let r3 = rss_mb();
    println!(
        "N={n}: bulk {:.0} ns/tie, build {:.0} ms, index RSS +{:.0} MB (build) +{:.0} MB (+N live), live tie {:.0} ns, pull {:.0} ns, rewrite {:.0} ns/tie, RSS after rewrite {:.0} MB",
        bulk.as_nanos() as f64 / n as f64,
        build.as_secs_f64() * 1e3,
        r1 - r0,
        r2 - r1,
        live.as_nanos() as f64 / n as f64,
        look.as_nanos() as f64 / q as f64,
        churn.as_nanos() as f64 / n as f64,
        r3
    );
    drop(eng);
    let _ = std::fs::remove_dir_all(&p);
}
