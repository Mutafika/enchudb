//! single-consumer write ceiling — 耐久並行 write スループットの実測。
//!
//! これまでの workload_rss_1m は build-phase の `tie()` (&mut, oplog を通らない
//! 非耐久 fast path) を測っていた。 こっちは **本番の並行書き込み path**:
//!
//!   tie_async → oplog append (memcpy) → WriteQueue push → 単一 consumer が apply
//!
//! producer thread 数 P を 1/2/4/8 と変えて、 throughput が P で伸びるか
//! (＝ scale する) / 頭打ちになるか (＝ single-consumer bound) を見る。
//!
//!   - push M/s   : producer 側が tie_async を投げ終わるまで (enqueue rate)
//!   - drain M/s  : flush_writes() で consumer が全 op を apply し切るまで
//!                  (＝ 耐久並行 write の実効 throughput、 これが「壁」)
//!
//! Usage:
//!   cargo run --release --example write_ceiling_bench [TOTAL]   (default 2,000,000)

use enchudb::{Engine, ValueType};
use std::sync::Arc;
use std::time::Instant;

fn parse_total() -> u32 {
    std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000)
}

/// TOTAL 本の tie を P writer で分担して投げ、
/// (push throughput, drain throughput) を返す。
fn run(total: u32, writers: u32) -> (f64, f64) {
    let path = format!("/tmp/enchu_write_ceiling_{writers}.db");
    for suf in ["", ".oplog", ".lock"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }

    // build phase: himo を定義してから concurrent + oplog へ遷移
    let mut eng = Engine::create_growable_with_capacity(&path, total + 100).unwrap();
    eng.define_himo("v", ValueType::Number, 0);
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 256 * 1024 * 1024).unwrap();
    let hid = eng.himo_id("v").unwrap() as u16;

    let per = total / writers;
    let t0 = Instant::now();
    let handles: Vec<_> = (0..writers)
        .map(|w| {
            let eng = eng.clone();
            std::thread::spawn(move || {
                for i in 0..per {
                    let e = eng.entity();
                    // 値は 10k cardinality に散らす (bucket 平均 ~200)
                    let val = (w.wrapping_mul(per).wrapping_add(i)) % 10_000;
                    eng.tie_async_by_id(e, hid, val);
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let push_s = t0.elapsed().as_secs_f64(); // 全 producer が push 完了

    eng.flush_writes(); // consumer が全 op を apply し切るまでの真の barrier
    let drain_s = t0.elapsed().as_secs_f64(); // end-to-end applied

    eng.oplog_sync().unwrap(); // durable 化 (fsync)

    let applied = (per * writers) as f64;
    let push_tps = applied / push_s;
    let drain_tps = applied / drain_s;

    drop(eng);
    for suf in ["", ".oplog", ".lock"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
    (push_tps, drain_tps)
}

fn main() {
    let total = parse_total();
    println!(
        "write ceiling bench: TOTAL={total} ties  (tie_async → oplog → single consumer)\n"
    );
    println!(
        "{:>8} | {:>14} | {:>16} | {:>10}",
        "writers", "push M/s", "drain M/s", "ns/tie"
    );
    println!("{}", "-".repeat(58));
    for &w in &[1u32, 2, 4, 8] {
        let (push, drain) = run(total, w);
        let ns_per_tie = 1e9 / drain;
        println!(
            "{:>8} | {:>14.2} | {:>16.2} | {:>10.1}",
            w,
            push / 1e6,
            drain / 1e6,
            ns_per_tie
        );
    }
    println!(
        "\n読み方: drain M/s が P で伸びれば scale、 頭打ちなら single-consumer bound。"
    );
}
