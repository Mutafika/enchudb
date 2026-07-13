//! lazy-verify tax probe — append-only 化で read が
//! 「bucket copy」→「bucket copy + column verify (+dedup)」になる。
//! その per-element tax を pure copy と比較して実測する。
//!
//! column[eid] へのアクセスは random なので cache 次第。 bucket 内 eid の
//! 密度（= himo の cardinality）で cache 挙動が変わるので数パターン測る。
//!
//! Usage: cargo run --release --example verify_tax_probe

use std::time::Instant;

fn measure(n: usize, cardinality: u32, v: u32, iters: usize) {
    // column[eid] = eid % cardinality （bench と同じ散らし方）
    let column: Vec<u32> = (0..n).map(|i| (i as u32) % cardinality).collect();
    // value v の bucket = column[eid]==v の eid 全部（eid は cardinality 間隔で散る）
    let bucket: Vec<u32> = (0..n as u32).filter(|&e| column[e as usize] == v).collect();
    let m = bucket.len();
    if m == 0 { return; }

    let stride = cardinality; // bucket 内の隣接 eid 間隔（cache locality の指標）

    // baseline: pure copy（現状 pull の to_vec 相当）
    let mut sink = 0u64;
    let t = Instant::now();
    for _ in 0..iters {
        let copy = bucket.clone();
        sink = sink.wrapping_add(copy.len() as u64);
    }
    let copy_ns = t.elapsed().as_nanos() as f64 / (iters as f64 * m as f64);

    // copy + verify: column[eid]==v で filter
    let t = Instant::now();
    for _ in 0..iters {
        let out: Vec<u32> = bucket
            .iter()
            .copied()
            .filter(|&e| column[e as usize] == v)
            .collect();
        sink = sink.wrapping_add(out.len() as u64);
    }
    let verify_ns = t.elapsed().as_nanos() as f64 / (iters as f64 * m as f64);

    // + dedup（churn 時のみ必要。 append-only なら不要だが cost 参考に）
    let t = Instant::now();
    for _ in 0..iters {
        let mut out: Vec<u32> = bucket
            .iter()
            .copied()
            .filter(|&e| column[e as usize] == v)
            .collect();
        out.sort_unstable();
        out.dedup();
        sink = sink.wrapping_add(out.len() as u64);
    }
    let dedup_ns = t.elapsed().as_nanos() as f64 / (iters as f64 * m as f64);

    println!(
        "card={:>6} bucket_m={:>7} stride={:>6} | copy {:>5.2} | +verify {:>5.2} (tax +{:>5.2}) | +dedup {:>6.2} (tax +{:>6.2})  [ns/elem]  sink={}",
        cardinality, m, stride, copy_ns, verify_ns, verify_ns - copy_ns, dedup_ns, dedup_ns - copy_ns, sink & 0xff
    );
}

fn main() {
    let n = 4_000_000usize; // column を大きめに（cache に載り切らない = realistic）
    println!("n(entities) = {n}, column = {} MB (u32)", n * 4 / 1024 / 1024);
    println!("読み: verify tax = 「+verify」-「copy」= append-only bucket が払わずに済む分\n");
    // 低カーディナリティ（巨大 bucket・eid 密）→ 高カーディナリティ（小 bucket・eid 疎）
    measure(n, 2, 0, 200); // bool himo: bucket ~2M, stride 2 (cache friendly)
    measure(n, 100, 7, 500); // bucket ~40k, stride 100
    measure(n, 10_000, 7, 5000); // bucket ~400, stride 10k (cache miss 毎回)
    measure(n, 1_000_000, 7, 20000); // bucket ~4, stride 1M (完全 scatter)
}
