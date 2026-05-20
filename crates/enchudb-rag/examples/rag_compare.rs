//! enchudb-rag vs naive brute force ベンチ。
//!
//! 仮説検証: 「メタフィルタ先行 + brute force cosine は ANN 不要で sub-ms 出る」
//!
//! 測定軸:
//! - 速度: p50 / p99 latency
//! - 正確性: recall@K (naive baseline を ground truth として比較)
//! - スケール: N = 10K / 100K
//! - 次元: dim = 384 / 768 (実 embedding に近い)
//! - フィルタ選択率: 100% / 50% / 10% / 1%
//!
//! 走らせ方:
//!   cargo run --example rag_compare --release
//!   cargo run --example rag_compare --release -- --scale 100k --dim 768

use enchudb_rag::{
    distance::cosine_sim, Chunk, Filter, Meta, Query, RagStore,
};
use std::collections::BinaryHeap;
use std::time::Instant;

// ─── データ生成 ─────────────────────────────────────────────

struct Dataset {
    vectors: Vec<Vec<f32>>,
    tenants: Vec<u32>,
    langs: Vec<&'static str>,
    queries: Vec<Vec<f32>>,
    dim: usize,
    n: u32,
    num_tenants: u32,
}

/// 簡易 xorshift で deterministic な f32 列を生成。
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
    fn next_f32_unit(&mut self) -> f32 {
        (self.next_u32() as f32 / u32::MAX as f32) * 2.0 - 1.0
    }
}

fn make_vector(rng: &mut Rng, dim: usize) -> Vec<f32> {
    let mut v: Vec<f32> = (0..dim).map(|_| rng.next_f32_unit()).collect();
    let n: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if n > 0.0 {
        let inv = 1.0 / n;
        for x in &mut v { *x *= inv; }
    }
    v
}

fn generate(n: u32, dim: usize, num_tenants: u32, num_queries: usize, seed: u64) -> Dataset {
    let mut rng = Rng::new(seed);
    let mut vectors = Vec::with_capacity(n as usize);
    let mut tenants = Vec::with_capacity(n as usize);
    let mut langs = Vec::with_capacity(n as usize);
    for i in 0..n {
        vectors.push(make_vector(&mut rng, dim));
        tenants.push(i % num_tenants);
        langs.push(if i % 2 == 0 { "en" } else { "ja" });
    }
    let mut queries = Vec::with_capacity(num_queries);
    for _ in 0..num_queries {
        queries.push(make_vector(&mut rng, dim));
    }
    Dataset { vectors, tenants, langs, queries, dim, n, num_tenants }
}

// ─── ground truth (naive brute force) ─────────────────────────

/// ground truth top-K: フィルタ通過した index に対する全件 cosine、降順 top-K。
/// 戻り値は (eid_index, score) の Vec、score は cos 類似度（大きいほど近い）。
fn naive_topk(
    ds: &Dataset,
    query: &[f32],
    keep: impl Fn(u32) -> bool,
    k: usize,
) -> Vec<(u32, f32)> {
    // BinaryHeap は最大 heap なので、min-heap として「負の score」を入れて k 個保つ。
    #[derive(PartialEq)]
    struct Entry(f32, u32);
    impl Eq for Entry {}
    impl PartialOrd for Entry {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            // 「score が小さい方が大きい」反転 = min-heap として振る舞う
            other.0.partial_cmp(&self.0)
        }
    }
    impl Ord for Entry {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
        }
    }
    let mut heap: BinaryHeap<Entry> = BinaryHeap::with_capacity(k + 1);
    for i in 0..ds.n {
        if !keep(i) { continue; }
        let s = cosine_sim(query, &ds.vectors[i as usize]);
        if heap.len() < k {
            heap.push(Entry(s, i));
        } else if let Some(top) = heap.peek() {
            // top は heap 内最小（score 最小）。新しいのがそれより大きければ入れ替え。
            if s > top.0 {
                heap.pop();
                heap.push(Entry(s, i));
            }
        }
    }
    let mut out: Vec<(u32, f32)> = heap.into_iter().map(|e| (e.1, e.0)).collect();
    out.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    out
}

// ─── フィルタ pattern ─────────────────────────────────────────

#[derive(Clone, Copy)]
struct FilterPattern {
    name: &'static str,
    selectivity_pct: u32, // 期待選択率
}

impl FilterPattern {
    fn rag_filter(&self) -> Filter {
        match self.name {
            "none" => Filter::all(),
            "lang=en (50%)" => Filter::symbol("lang", "en"),
            "tenant 0..9 (10%)" => Filter::range("tenant", 0..=9),
            "tenant=1 (1%)" => Filter::value("tenant", 1),
            _ => unreachable!(),
        }
    }
    /// naive 側で同じフィルタを再現する predicate。
    fn naive_keep<'a>(&self, ds: &'a Dataset) -> Box<dyn Fn(u32) -> bool + 'a> {
        let tenants = &ds.tenants;
        let langs = &ds.langs;
        match self.name {
            "none" => Box::new(|_i: u32| true),
            "lang=en (50%)" => Box::new(move |i: u32| langs[i as usize] == "en"),
            "tenant 0..9 (10%)" => Box::new(move |i: u32| tenants[i as usize] <= 9),
            "tenant=1 (1%)" => Box::new(move |i: u32| tenants[i as usize] == 1),
            _ => unreachable!(),
        }
    }
}

const PATTERNS: &[FilterPattern] = &[
    FilterPattern { name: "none", selectivity_pct: 100 },
    FilterPattern { name: "lang=en (50%)", selectivity_pct: 50 },
    FilterPattern { name: "tenant 0..9 (10%)", selectivity_pct: 10 },
    FilterPattern { name: "tenant=1 (1%)", selectivity_pct: 1 },
];

// ─── 計測 ─────────────────────────────────────────────────

#[derive(Default, Clone)]
struct Latencies(Vec<u128>); // nanoseconds

impl Latencies {
    fn push(&mut self, ns: u128) { self.0.push(ns); }
    fn percentile(&self, p: f64) -> u128 {
        let mut v = self.0.clone();
        v.sort_unstable();
        if v.is_empty() { return 0; }
        let idx = ((v.len() as f64 - 1.0) * p) as usize;
        v[idx]
    }
    fn p50(&self) -> u128 { self.percentile(0.50) }
    fn p99(&self) -> u128 { self.percentile(0.99) }
}

struct Row {
    impl_name: &'static str,
    n: u32,
    dim: usize,
    pattern: &'static str,
    p50_us: f64,
    p99_us: f64,
    recall_at_k: f64,
}

fn ns_to_us(ns: u128) -> f64 { ns as f64 / 1e3 }

// ─── enchudb-rag 計測 ────────────────────────────────────────

fn measure_enchu(ds: &Dataset, k: usize) -> Vec<Row> {
    let path = "/tmp/enchudb-rag-compare";
    let _ = std::fs::remove_file(format!("{}.db", path));
    let _ = std::fs::remove_file(format!("{}.vec", path));

    let mut store = RagStore::builder()
        .path(path)
        .dim(ds.dim)
        .meta_value("tenant", ds.num_tenants)
        .meta_symbol("lang")
        .max_entities(ds.n + 1000)
        .build()
        .unwrap();

    println!("  loading {} chunks into enchudb-rag...", ds.n);
    let t0 = Instant::now();
    for i in 0..ds.n as usize {
        store.insert(Chunk {
            text: String::new(),
            vector: ds.vectors[i].clone(),
            meta: Meta::new()
                .value("tenant", ds.tenants[i])
                .symbol("lang", ds.langs[i]),
        }).unwrap();
    }
    let load_ms = t0.elapsed().as_millis();
    println!("    loaded in {} ms", load_ms);

    let mut rows = Vec::new();
    for p in PATTERNS {
        let mut lat = Latencies::default();
        let mut recall_acc = 0.0;
        // warmup
        for q in ds.queries.iter().take(3) {
            let _ = store.search(Query::new(q).filter(p.rag_filter()).top_k(k)).unwrap();
        }
        for q in &ds.queries {
            let t = Instant::now();
            let hits = store.search(Query::new(q).filter(p.rag_filter()).top_k(k)).unwrap();
            lat.push(t.elapsed().as_nanos());

            let truth = naive_topk(ds, q, p.naive_keep(ds), k);
            let truth_set: std::collections::HashSet<u32> =
                truth.iter().map(|(i, _)| *i).collect();
            let hit_local: Vec<u32> = hits.iter()
                .map(|h| enchudb_oplog::eid_local(h.eid))
                .collect();
            let intersect = hit_local.iter().filter(|i| truth_set.contains(i)).count();
            recall_acc += intersect as f64 / k.max(1) as f64;
        }
        let recall = recall_acc / ds.queries.len() as f64;
        rows.push(Row {
            impl_name: "enchudb-rag",
            n: ds.n,
            dim: ds.dim,
            pattern: p.name,
            p50_us: ns_to_us(lat.p50()),
            p99_us: ns_to_us(lat.p99()),
            recall_at_k: recall,
        });
    }
    rows
}

// ─── naive baseline 計測 ─────────────────────────────────────

fn measure_naive(ds: &Dataset, k: usize) -> Vec<Row> {
    let mut rows = Vec::new();
    for p in PATTERNS {
        let mut lat = Latencies::default();
        // warmup
        for q in ds.queries.iter().take(3) {
            let _ = naive_topk(ds, q, p.naive_keep(ds), k);
        }
        for q in &ds.queries {
            let t = Instant::now();
            let _ = naive_topk(ds, q, p.naive_keep(ds), k);
            lat.push(t.elapsed().as_nanos());
        }
        rows.push(Row {
            impl_name: "naive (baseline)",
            n: ds.n,
            dim: ds.dim,
            pattern: p.name,
            p50_us: ns_to_us(lat.p50()),
            p99_us: ns_to_us(lat.p99()),
            recall_at_k: 1.0, // 自分自身が ground truth
        });
    }
    rows
}

// ─── 出力 ─────────────────────────────────────────────────

fn print_markdown(rows: &[Row]) {
    println!();
    println!("| Impl              | N      | Dim | Filter             | p50 (µs) | p99 (µs) | Recall@K |");
    println!("|-------------------|--------|-----|--------------------|----------|----------|----------|");
    for r in rows {
        println!(
            "| {:<17} | {:>6} | {:>3} | {:<18} | {:>8.1} | {:>8.1} | {:>8.3} |",
            r.impl_name, r.n, r.dim, r.pattern, r.p50_us, r.p99_us, r.recall_at_k,
        );
    }
    println!();
}

// ─── main ─────────────────────────────────────────────────

fn main() {
    let scales: &[u32] = &[10_000, 100_000];
    let dims: &[usize] = &[384, 768];
    let num_tenants = 100;
    let num_queries = 100;
    let k = 10;

    let mut all_rows: Vec<Row> = Vec::new();
    for &n in scales {
        for &dim in dims {
            println!("=== N={}, dim={} ===", n, dim);
            let ds = generate(n, dim, num_tenants, num_queries, 42);

            println!("  naive baseline...");
            all_rows.extend(measure_naive(&ds, k));

            println!("  enchudb-rag...");
            all_rows.extend(measure_enchu(&ds, k));
        }
    }

    print_markdown(&all_rows);
}
