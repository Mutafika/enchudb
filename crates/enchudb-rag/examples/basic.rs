//! 最小の使い方。HashEmbedder でベクトル化 → 検索まで。

use enchudb_rag::{Chunk, Embedder, Filter, HashEmbedder, Meta, Query, RagStore};

fn main() {
    // 前回の残骸を掃除
    let path = "/tmp/enchudb-rag-basic";
    let _ = std::fs::remove_file(format!("{}.db", path));
    let _ = std::fs::remove_file(format!("{}.vec", path));

    let embedder = HashEmbedder::new(128);
    let mut store = RagStore::builder()
        .path(path)
        .dim(128)
        .meta_value("tenant", 100)
        .meta_symbol("lang")
        .meta_value("year", 3000)
        .max_entities(10_000)
        .build()
        .unwrap();

    let docs = [
        ("enchudb is a himo based embedded database", 1, "en", 2025),
        ("ベクトル検索はメタフィルタで高速化できる", 1, "ja", 2025),
        ("rust makes systems programming safe", 1, "en", 2024),
        ("tenant 2 private doc", 2, "en", 2025),
        ("RAG stands for retrieval augmented generation", 1, "en", 2024),
        ("himo data model separates indexing from source of truth", 1, "en", 2025),
    ];

    for (text, tenant, lang, year) in docs.iter() {
        store.insert(Chunk {
            text: (*text).into(),
            vector: embedder.embed(text),
            meta: Meta::new()
                .value("tenant", *tenant)
                .symbol("lang", *lang)
                .value("year", *year),
        }).unwrap();
    }

    // tenant=1 の英語、2025 年のみ
    let query_text = "himo based embedded database";
    let q = embedder.embed(query_text);
    let hits = store.search(
        Query::new(&q)
            .filter(
                Filter::value("tenant", 1)
                    .and(Filter::symbol("lang", "en"))
                    .and(Filter::value("year", 2025))
            )
            .top_k(3)
    ).unwrap();

    println!("query: {}\n", query_text);
    for (i, h) in hits.iter().enumerate() {
        println!("{}. [dist {:.4}] {}", i + 1, h.score, h.text.as_deref().unwrap_or(""));
    }

    store.flush().unwrap();
}
