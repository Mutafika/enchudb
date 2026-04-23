//! メタ filter + brute force cosine のベンチ。
//!
//! 10 万 chunk に対して、tenant で絞ってから cosine top-10 を取る速度を測る。
//! メタで絞れるほど速くなることを確認する。

use enchudb_rag::{Chunk, Embedder, Filter, HashEmbedder, Meta, Query, RagStore};
use std::time::Instant;

fn main() {
    let path = "/tmp/enchudb-rag-bench";
    let _ = std::fs::remove_file(format!("{}.db", path));
    let _ = std::fs::remove_file(format!("{}.vec", path));

    const N: u32 = 100_000;
    const DIM: usize = 384; // 小さめの実 embedding と同等
    const NUM_TENANTS: u32 = 100;

    let embedder = HashEmbedder::new(DIM);
    let mut store = RagStore::builder()
        .path(path)
        .dim(DIM)
        .meta_value("tenant", NUM_TENANTS)
        .meta_symbol("lang")
        .max_entities(N + 1000)
        .build()
        .unwrap();

    println!("inserting {} chunks (dim={})...", N, DIM);
    let t0 = Instant::now();
    for i in 0..N {
        let text = format!("doc {} about topic {}", i, i % 100);
        let tenant = i % NUM_TENANTS;
        let lang = if i % 2 == 0 { "en" } else { "ja" };
        store.insert(Chunk {
            text,
            vector: embedder.embed(&format!("{} {}", i, i % 100)),
            meta: Meta::new().value("tenant", tenant).symbol("lang", lang),
        }).unwrap();
    }
    let insert_ms = t0.elapsed().as_millis();
    println!("insert: {} ms ({:.1} ns/doc)", insert_ms, (insert_ms as f64 * 1e6) / N as f64);

    let q = embedder.embed("topic 42");

    // 絞り込みなし（brute force 全件 cosine）
    bench("no filter, brute force all", 20, || {
        store.search(Query::new(&q).top_k(10)).unwrap()
    });

    // tenant 1 件（1/100 に絞れる）
    bench("tenant=1 (1/100)", 20, || {
        store.search(
            Query::new(&q).filter(Filter::value("tenant", 1)).top_k(10)
        ).unwrap()
    });

    // tenant=1 AND lang=en（1/200）
    bench("tenant=1 AND lang=en (1/200)", 20, || {
        store.search(
            Query::new(&q)
                .filter(Filter::value("tenant", 1).and(Filter::symbol("lang", "en")))
                .top_k(10)
        ).unwrap()
    });

    // tenant range
    bench("tenant in 0..=9 (10/100)", 20, || {
        store.search(
            Query::new(&q).filter(Filter::range("tenant", 0..=9)).top_k(10)
        ).unwrap()
    });
}

fn bench<F, R>(name: &str, iters: u32, mut f: F)
where F: FnMut() -> R {
    // warmup
    for _ in 0..3 { let _ = f(); }
    let t0 = Instant::now();
    for _ in 0..iters { let _ = f(); }
    let el = t0.elapsed();
    let per = el.as_nanos() / iters as u128;
    println!("  {:<40} {:>8} ns  ({:>6.2} ms/iter)", name, per, per as f64 / 1e6);
}
