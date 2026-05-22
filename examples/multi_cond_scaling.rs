//! 多条件 AND の谷カーブ実測。
//!
//! bitmap_and の理論コスト:
//!   total = (cond - 1) × (N/64) × word_AND_ns + result_size × bit_extract_ns
//!         ≈ (cond - 1) × 7.5 µs              + result_size × 4 ns
//!
//! 条件追加で +7.5 µs (word AND)、 結果絞り込みで -4 ns/hit。
//! 1875 hits 以上減るなら net で下がる、 それ未満なら net で上がる。
//!
//! cargo run --release --example multi_cond_scaling

use enchudb::schema::Database;
use std::time::Instant;

const N: u32 = 1_000_000;
const ITER: u32 = 100;

fn main() {
    let path = "/tmp/multi_cond_scaling.db";
    let _ = std::fs::remove_file(path);

    let mut db = Database::create_growable_with_capacity(path, N + 1000).unwrap();
    {
        let _ = db.table("t")
            .number("id")
            .number("a")   // 20 values, ~50K hits each
            .number("b")   //  5 values, ~200K hits each
            .number("c")   // 50 values, ~20K hits each
            .number("d")   // 10 values, ~100K hits each
            .number("e")   //  8 values, ~125K hits each
            .number("f")   // 40 values, ~25K hits each
            .number("g")   // 1000 values, ~1K hits each
            .primary_key("id")
            .with_capacity(N as u32 + 100)
            .build().unwrap();
    }
    let tbl = db.get_table("t").unwrap();

    println!("inserting {} rows × 7 cols...", N);
    let setup = Instant::now();
    // deterministic xorshift で各 row に独立タプルを作る
    let mut rng = Rng::new(42);
    for i in 0..N {
        tbl.insert()
            .set("id", i as i64)
            .set("a", (rng.next_u32() % 20) as i64)
            .set("b", (rng.next_u32() % 5) as i64)
            .set("c", (rng.next_u32() % 50) as i64)
            .set("d", (rng.next_u32() % 10) as i64)
            .set("e", (rng.next_u32() % 8) as i64)
            .set("f", (rng.next_u32() % 40) as i64)
            .set("g", (rng.next_u32() % 1000) as i64)
            .commit().unwrap();
    }
    println!("setup: {:.1}s", setup.elapsed().as_secs_f64());
    println!();

    // 選択率の高い順に積む (=結果が単調減少するように)
    // 値域 N=20,5,50,10,8,40,1000 を選択率 (1/N) でソート: g(1/1000), c(1/50), f(1/40), a(1/20), d(1/10), e(1/8), b(1/5)
    // でも極端な絞り込みより、 まず大きいバケットから狭めていく方が「カーブ」が見やすい:
    // b(1/5) → a(1/20) → d(1/10) → e(1/8) → f(1/40) → c(1/50) → g(1/1000)
    let progression: &[(&str, i64)] = &[
        ("b", 0),
        ("a", 0),
        ("d", 0),
        ("e", 0),
        ("f", 0),
        ("c", 0),
        ("g", 0),
    ];

    println!("| cond | conditions               | hits  | time     | per hit |");
    println!("|-----:|--------------------------|------:|---------:|--------:|");

    for n_cond in 1..=7 {
        let cs = &progression[..n_cond];
        let label: String = cs.iter().map(|(c, v)| format!("{c}={v}")).collect::<Vec<_>>().join(" AND ");

        // warmup
        for _ in 0..3 {
            let mut q = tbl.where_eq(cs[0].0, cs[0].1);
            for (col, val) in &cs[1..] { q = q.where_eq(*col, *val); }
            let _ = q.find().unwrap();
        }

        // measure hits once
        let hits = {
            let mut q = tbl.where_eq(cs[0].0, cs[0].1);
            for (col, val) in &cs[1..] { q = q.where_eq(*col, *val); }
            q.find().unwrap().len()
        };

        // measure latency
        let t = Instant::now();
        for _ in 0..ITER {
            let mut q = tbl.where_eq(cs[0].0, cs[0].1);
            for (col, val) in &cs[1..] { q = q.where_eq(*col, *val); }
            let _ = q.find().unwrap();
        }
        let ns = t.elapsed().as_nanos() / ITER as u128;

        let per_hit = if hits > 0 {
            format!("{:.2} ns", ns as f64 / hits as f64)
        } else {
            "—".into()
        };

        println!(
            "| {:>4} | {:<24} | {:>5} | {:>8} | {:>7} |",
            n_cond, label, format_hits(hits), format_ns(ns), per_hit
        );
    }

    let _ = std::fs::remove_file(path);
}

/// 簡易 xorshift PRNG。 deterministic な独立サンプリングが欲しい時用。
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self { Self(seed.max(1)) }
    fn next_u32(&mut self) -> u32 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        (x & 0xFFFF_FFFF) as u32
    }
}

fn format_ns(ns: u128) -> String {
    if ns >= 1_000_000 { format!("{:.2}ms", ns as f64 / 1_000_000.0) }
    else if ns >= 1_000 { format!("{:.1}µs", ns as f64 / 1_000.0) }
    else { format!("{ns}ns") }
}

fn format_hits(n: usize) -> String {
    if n >= 1_000_000 { format!("{:.1}M", n as f64 / 1_000_000.0) }
    else if n >= 1_000 { format!("{}K", n / 1_000) }
    else { format!("{n}") }
}
