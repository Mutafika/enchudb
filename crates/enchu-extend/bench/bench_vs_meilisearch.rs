//! Enchu v20 vs Meilisearch — マルチテナント ファセット検索ベンチ
//!
//! シナリオ: ECサイト、100テナント × 10,000商品 = 1,000,000件
//! 各商品: tenant_id, category(20種), color(10色), size(5種), price(0-9999)
//!
//! Meilisearch は HTTP サーバーとして起動済みが前提:
//!   meilisearch --master-key bench123 --db-path /tmp/meili_bench
//!
//! v20 Cylinder = prefix sum O(1) スライス。交差もゼロアロケーション。

use std::hint::black_box;
use std::time::Instant;

use enchu_v15::v20::engine::*;

const TENANTS: u32 = 100;
const PRODUCTS_PER_TENANT: u32 = 10_000;
const N: u32 = TENANTS * PRODUCTS_PER_TENANT; // 1,000,000

const CATEGORIES: u32 = 20;
const COLORS: u32 = 10;
const SIZES: u32 = 5;
const MAX_PRICE: u32 = 10_000;

const MEILI_URL: &str = "http://localhost:7700";
const MEILI_KEY: &str = "bench123";

fn main() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async { run().await });
}

async fn run() {
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║  Enchu v20 vs Meilisearch — マルチテナント ファセット検索     ║");
    println!("║  {} テナント × {} 商品 = {} 件                       ║", TENANTS, PRODUCTS_PER_TENANT, N);
    println!("╚══════════════════════════════════════════════════════════════════╝\n");

    // ══════ Meilisearch check ══════
    print!("  Meilisearch 接続確認...");
    let client = reqwest::Client::new();
    match client.get(format!("{MEILI_URL}/health")).send().await {
        Ok(r) if r.status().is_success() => println!(" OK"),
        _ => {
            println!(" NG");
            println!("\n  ⚠  Meilisearch が起動していません。先に以下を実行:");
            println!("     meilisearch --master-key bench123 --db-path /tmp/meili_bench");
            return;
        }
    }

    // ══════ Enchu v20 setup ══════
    print!("  Enchu v20 setup...");
    let t = Instant::now();
    let enchu_dir = "/tmp/bench_vs_meili_enchu_v20";
    let _ = std::fs::remove_dir_all(enchu_dir);

    let schema = Schema::new("shop",
        TableDef::leaf("products", vec![
            ("tenant_id", FieldType::Int),
            ("category", FieldType::Int),
            ("color", FieldType::Int),
            ("size", FieldType::Int),
            ("price", FieldType::Int),
        ]),
    );
    let mut enchu = Engine::create(enchu_dir, schema).unwrap();

    for tenant in 0..TENANTS {
        for p in 0..PRODUCTS_PER_TENANT {
            let eid = enchu.insert("products").unwrap();
            enchu.write_i64("products", eid, "tenant_id", tenant as i64).unwrap();
            enchu.write_i64("products", eid, "category", (p % CATEGORIES) as i64).unwrap();
            enchu.write_i64("products", eid, "color", (p % COLORS) as i64).unwrap();
            enchu.write_i64("products", eid, "size", (p % SIZES) as i64).unwrap();
            enchu.write_i64("products", eid, "price", (p % MAX_PRICE) as i64).unwrap();
        }
    }
    enchu.flush().unwrap();
    println!(" {:.1}s", t.elapsed().as_secs_f64());

    // ID事前解決（ホットループ用）
    let tid = enchu.resolve_table("products");
    let f_tenant = enchu.resolve_field("products", "tenant_id");
    let f_cat = enchu.resolve_field("products", "category");
    let f_color = enchu.resolve_field("products", "color");
    let f_size = enchu.resolve_field("products", "size");
    let f_price = enchu.resolve_field("products", "price");

    // ══════ Meilisearch setup ══════
    print!("  Meilisearch setup...");
    let t = Instant::now();

    let _ = client.delete(format!("{MEILI_URL}/indexes/products"))
        .header("Authorization", format!("Bearer {MEILI_KEY}"))
        .send().await;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    client.post(format!("{MEILI_URL}/indexes"))
        .header("Authorization", format!("Bearer {MEILI_KEY}"))
        .json(&serde_json::json!({"uid": "products", "primaryKey": "id"}))
        .send().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    client.put(format!("{MEILI_URL}/indexes/products/settings/filterable-attributes"))
        .header("Authorization", format!("Bearer {MEILI_KEY}"))
        .json(&serde_json::json!(["tenant_id", "category", "color", "size", "price"]))
        .send().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let mut id = 0u32;
    for tenant in 0..TENANTS {
        let mut docs = Vec::with_capacity(PRODUCTS_PER_TENANT as usize);
        for p in 0..PRODUCTS_PER_TENANT {
            docs.push(serde_json::json!({
                "id": id,
                "tenant_id": tenant,
                "category": p % CATEGORIES,
                "color": p % COLORS,
                "size": p % SIZES,
                "price": p % MAX_PRICE,
            }));
            id += 1;
        }
        client.post(format!("{MEILI_URL}/indexes/products/documents"))
            .header("Authorization", format!("Bearer {MEILI_KEY}"))
            .header("Content-Type", "application/json")
            .json(&docs)
            .send().await.unwrap();
    }

    print!(" indexing...");
    loop {
        let resp = client.get(format!("{MEILI_URL}/tasks?statuses=enqueued,processing&limit=1"))
            .header("Authorization", format!("Bearer {MEILI_KEY}"))
            .send().await.unwrap()
            .json::<serde_json::Value>().await.unwrap();
        if resp["total"].as_u64().unwrap_or(0) == 0 { break; }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    println!(" {:.1}s", t.elapsed().as_secs_f64());

    // ══════ Benchmark ══════
    let w = 50;
    let r = 1_000;

    println!("\n  ┌───────────────────────────┬──────────────┬──────────────┬──────────┐");
    println!("  │ クエリ                    │    Enchu v20 │  Meilisearch │     倍率 │");
    println!("  ├───────────────────────────┼──────────────┼──────────────┼──────────┤");

    // 1. 単条件: tenant_id = 42
    let en = bench(w, r, || {
        black_box(enchu.slice_by_id(tid, f_tenant, 42).len());
    });
    let me = bench_async(w, r, &client, "tenant_id = 42").await;
    row("tenant=42 (1cond)      ", en, me);

    // 2. 2条件: tenant_id=42 AND category=5
    let en = bench(w, r, || {
        let a = enchu.slice_by_id(tid, f_tenant, 42);
        let b = enchu.slice_by_id(tid, f_cat, 5);
        black_box(sorted_intersect_count(a, b));
    });
    let me = bench_async(w, r, &client, "tenant_id = 42 AND category = 5").await;
    row("tenant+cat (2cond)     ", en, me);

    // 3. 3条件: tenant_id=42 AND category=5 AND color=3
    let en = bench(w, r, || {
        let a = enchu.slice_by_id(tid, f_tenant, 42);
        let b = enchu.slice_by_id(tid, f_cat, 5);
        let c = enchu.slice_by_id(tid, f_color, 3);
        let ab = galloping_intersect(a, b);
        black_box(galloping_intersect_count(&ab, c));
    });
    let me = bench_async(w, r, &client, "tenant_id = 42 AND category = 5 AND color = 3").await;
    row("tenant+cat+col (3cond) ", en, me);

    // 4. 4条件: tenant_id=42 AND category=5 AND color=3 AND size=1
    let en = bench(w, r, || {
        let a = enchu.slice_by_id(tid, f_tenant, 42);
        let b = enchu.slice_by_id(tid, f_cat, 5);
        let c = enchu.slice_by_id(tid, f_color, 3);
        let d = enchu.slice_by_id(tid, f_size, 1);
        let ab = sorted_intersect(a, b);
        let abc = sorted_intersect(&ab, c);
        black_box(sorted_intersect_count(&abc, d));
    });
    let me = bench_async(w, r, &client, "tenant_id = 42 AND category = 5 AND color = 3 AND size = 1").await;
    row("tenant+3facets (4cond) ", en, me);

    // 5. range: tenant_id=42 AND price < 1000
    let en = bench(w, r, || {
        let a = enchu.slice_by_id(tid, f_tenant, 42);
        let b = enchu.slice_range("products", "price", 0, 999);
        black_box(sorted_intersect_count(a, b));
    });
    let me = bench_async(w, r, &client, "tenant_id = 42 AND price < 1000").await;
    row("tenant+price<1K (range)", en, me);

    println!("  └───────────────────────────┴──────────────┴──────────────┴──────────┘");

    let _ = std::fs::remove_dir_all(enchu_dir);
    let _ = client.delete(format!("{MEILI_URL}/indexes/products"))
        .header("Authorization", format!("Bearer {MEILI_KEY}"))
        .send().await;
    println!("\n  Done.");
}

fn bench<F: FnMut()>(w: usize, r: usize, mut f: F) -> f64 {
    for _ in 0..w { f(); }
    let s = Instant::now();
    for _ in 0..r { f(); }
    s.elapsed().as_nanos() as f64 / r as f64
}

async fn bench_async(w: usize, r: usize, client: &reqwest::Client, filter: &str) -> f64 {
    for _ in 0..w { meili_search(client, filter).await; }
    let s = Instant::now();
    for _ in 0..r { meili_search(client, filter).await; }
    s.elapsed().as_nanos() as f64 / r as f64
}

async fn meili_search(client: &reqwest::Client, filter: &str) {
    let resp = client.post(format!("{MEILI_URL}/indexes/products/search"))
        .header("Authorization", format!("Bearer {MEILI_KEY}"))
        .json(&serde_json::json!({
            "filter": filter,
            "limit": 10000,
            "q": ""
        }))
        .send().await.unwrap();
    let body = resp.json::<serde_json::Value>().await.unwrap();
    black_box(body["estimatedTotalHits"].as_u64().unwrap_or(0));
}

// ════════════════ ギャロッピング交差 ════════════════

#[inline]
fn gallop_ge(big: &[u32], val: u32, lo: usize) -> usize {
    let n = big.len();
    if lo >= n { return n; }
    if big[lo] >= val { return lo; }
    let mut step = 1usize;
    let mut hi = lo + step;
    while hi < n && big[hi] < val { step *= 2; hi = (lo + step).min(n); }
    let from = lo + step / 2;
    let to = hi.min(n);
    from + big[from..to].partition_point(|&x| x < val)
}

fn galloping_intersect(a: &[u32], b: &[u32]) -> Vec<u32> {
    let (small, big) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    if small.is_empty() { return vec![]; }
    let mut result = Vec::with_capacity(small.len());
    let mut lo = 0usize;
    for &val in small {
        lo = gallop_ge(big, val, lo);
        if lo >= big.len() { break; }
        if big[lo] == val { result.push(val); lo += 1; }
    }
    result
}

fn galloping_intersect_count(a: &[u32], b: &[u32]) -> u32 {
    let (small, big) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    if small.is_empty() { return 0; }
    let mut count = 0u32;
    let mut lo = 0usize;
    for &val in small {
        lo = gallop_ge(big, val, lo);
        if lo >= big.len() { break; }
        if big[lo] == val { count += 1; lo += 1; }
    }
    count
}

fn fmt(ns: f64) -> String {
    if ns >= 1_000_000.0 { format!("{:.1}ms", ns / 1_000_000.0) }
    else if ns >= 1_000.0 { format!("{:.1}µs", ns / 1_000.0) }
    else { format!("{:.0}ns", ns) }
}

fn row(label: &str, en: f64, me: f64) {
    let ratio = me / en;
    println!("  │ {} │ {:>12} │ {:>12} │ {:>6.0}x │", label, fmt(en), fmt(me), ratio);
}
