//! 0.8.9 (#39): bulk column scan の seq vs par 実測 bench。
//!
//! suzukapulse dominance(all) で 12M sample scan が 1.95s かかってた hot path
//! を rayon 並列化で改善できるかの実測。 M2 Max (4 perf core + 6 efficiency)
//! で 3x 前後の改善を期待。
//!
//! 実行: `cargo run --release --example par_scan_bench`

use enchudb_engine::{Engine, ValueType};
use std::time::Instant;

const N: u32 = 12_000_000; // 12M row scan
const REPEAT: usize = 5;

fn tmp_path() -> String {
    format!(
        "/tmp/enchudb-par-bench-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suffix in ["", ".oplog", ".crc", ".db.lock", ".tables", ".tables.tmp", ".schema"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

fn fmt_ms(d: std::time::Duration) -> String {
    format!("{:>8.1} ms", d.as_secs_f64() * 1000.0)
}

fn time_n<F: FnMut()>(mut f: F) -> std::time::Duration {
    // warmup
    f();
    let t = Instant::now();
    for _ in 0..REPEAT {
        f();
    }
    t.elapsed() / REPEAT as u32
}

fn main() {
    let path = tmp_path();
    cleanup(&path);

    // ─── setup ───
    print!("setup: {} rows ... ", N);
    let t = Instant::now();
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_table("t", N).unwrap();
    eng.define_himo_in("t", "val", ValueType::Number, 0).unwrap();
    eng.define_himo_in("t", "dept", ValueType::Tag, 0).unwrap();
    let depts = ["a", "b", "c", "d", "e", "f", "g", "h"];
    for i in 0..N {
        let e = eng.entity_in("t").unwrap();
        eng.tie(e, "t.val", i % 1000);
        eng.tie_text(e, "t.dept", depts[(i as usize) % depts.len()]);
    }
    eng.flush().unwrap();
    let (lo, hi) = eng.table_eid_range("t").unwrap();
    println!("{}", fmt_ms(t.elapsed()));
    println!();
    let n_thr = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    println!("hardware threads = {}", n_thr);
    println!();
    println!("query                                   seq           par      speedup");
    println!("───────────────────────────────────────────────────────────────────────");

    let bench = |label: &str, seq_d: std::time::Duration, par_d: std::time::Duration| {
        let speedup = seq_d.as_secs_f64() / par_d.as_secs_f64();
        println!(
            "{:<38} {}  {}  {:>5.2}x",
            label,
            fmt_ms(seq_d),
            fmt_ms(par_d),
            speedup
        );
    };

    // ─── sum_range ───
    let s = time_n(|| { let _ = eng.sum_range("t.val", lo, hi); });
    let p = time_n(|| { let _ = eng.sum_range_par("t.val", lo, hi); });
    bench("sum_range (12M)", s, p);

    // ─── count_range ───
    let s = time_n(|| { let _ = eng.count_range("t.val", lo, hi); });
    let p = time_n(|| { let _ = eng.count_range_par("t.val", lo, hi); });
    bench("count_range (12M)", s, p);

    // ─── min_range ───
    let s = time_n(|| { let _ = eng.min_range("t.val", lo, hi); });
    let p = time_n(|| { let _ = eng.min_range_par("t.val", lo, hi); });
    bench("min_range (12M)", s, p);

    // ─── max_range ───
    let s = time_n(|| { let _ = eng.max_range("t.val", lo, hi); });
    let p = time_n(|| { let _ = eng.max_range_par("t.val", lo, hi); });
    bench("max_range (12M)", s, p);

    // ─── group_sum_range ───
    let s = time_n(|| { let _ = eng.group_sum_range("t.dept", "t.val", lo, hi); });
    let p = time_n(|| { let _ = eng.group_sum_range_par("t.dept", "t.val", lo, hi); });
    bench("group_sum_range (12M, 8 group)", s, p);

    // ─── group_min_range ───
    let s = time_n(|| { let _ = eng.group_min_range("t.dept", "t.val", lo, hi); });
    let p = time_n(|| { let _ = eng.group_min_range_par("t.dept", "t.val", lo, hi); });
    bench("group_min_range (12M, 8 group)", s, p);

    // ─── group_max_range ───
    let s = time_n(|| { let _ = eng.group_max_range("t.dept", "t.val", lo, hi); });
    let p = time_n(|| { let _ = eng.group_max_range_par("t.dept", "t.val", lo, hi); });
    bench("group_max_range (12M, 8 group)", s, p);

    // ─── range_scan ───
    let s = time_n(|| { let _ = eng.range_scan("t.val", 100, 200); });
    let p = time_n(|| { let _ = eng.range_scan_par("t.val", 100, 200); });
    bench("range_scan (12M, hit ~10%)", s, p);

    // ─── histogram_range ───
    let s = time_n(|| { let _ = eng.histogram_range("t.val", lo, hi, 0, 999, 10); });
    let p = time_n(|| { let _ = eng.histogram_range_par("t.val", lo, hi, 0, 999, 10); });
    bench("histogram_range (12M, 10 bucket)", s, p);

    println!();

    drop(eng);
    cleanup(&path);
}
