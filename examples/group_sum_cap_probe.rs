//! Probe: schema 層は himo を cardinality 0 で define するので group_sum が
//! HashMap fallback (= ~5ms/1M)。 ここでは engine 直で dept に dense cap 20 を
//! 与えて group_sum_range (seq dense) / group_sum_range_par (par dense) を測り、
//! 「dense cap さえあれば速いのか」 を実測する。 throwaway probe。
//!
//! cargo run --release --example group_sum_cap_probe

use enchudb::{Engine, ValueType};
use std::time::Instant;

fn main() {
    let n: u32 = 1_000_000;
    let path = "/tmp/gscap_probe.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));

    let mut eng = Engine::create_growable_with_capacity(path, n + 100).unwrap();
    eng.define_himo("dept", ValueType::Number, 20); // dense cap 20 (schema 層は 0)
    eng.define_himo("salary", ValueType::Number, 1000);
    for i in 0..n {
        let e = eng.entity();
        eng.tie(e, "dept", i % 20);
        eng.tie(e, "salary", (i * 7 + 13) % 1000);
    }
    eng.rebuild();

    let (lo, hi) = (0u32, n);
    let iters = 100u128;

    // warmup
    for _ in 0..3 {
        let _ = eng.group_sum_range("dept", "salary", lo, hi);
        let _ = eng.group_sum_range_par("dept", "salary", lo, hi);
    }

    let t = Instant::now();
    for _ in 0..iters { let _ = eng.group_sum_range("dept", "salary", lo, hi); }
    let seq = t.elapsed().as_nanos() / iters;

    let t = Instant::now();
    for _ in 0..iters { let _ = eng.group_sum_range_par("dept", "salary", lo, hi); }
    let par = t.elapsed().as_nanos() / iters;

    let g = eng.group_sum_range_par("dept", "salary", lo, hi);
    println!("groups = {}", g.len());
    println!("group_sum_range     (seq, DENSE cap=20): {:.1}µs", seq as f64 / 1000.0);
    println!("group_sum_range_par (par, DENSE cap=20): {:.1}µs", par as f64 / 1000.0);
    println!("(schema 層 cap=0 の HashMap path は ~5.2ms、 DuckDB は ~580µs)");

    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
}
