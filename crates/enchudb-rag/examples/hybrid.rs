//! ハイブリッド検索（ベクトル類似 + BM25、RRF 融合）。

use enchudb_rag::{Chunk, Embedder, Filter, HashEmbedder, HybridQuery, Meta, RagStore};

fn main() {
    let path = "/tmp/enchudb-rag-hybrid";
    let _ = std::fs::remove_file(format!("{}.db", path));
    let _ = std::fs::remove_file(format!("{}.vec", path));

    let embedder = HashEmbedder::new(128);
    let mut store = RagStore::builder()
        .path(path)
        .dim(128)
        .meta_symbol("category")
        .build()
        .unwrap();

    let docs = [
        ("cosine similarity for vector retrieval", "search"),
        ("rust ownership model and borrow checker", "rust"),
        ("BM25 is a bag of words ranking function", "search"),
        ("enchudb uses himo based column store", "db"),
        ("reciprocal rank fusion combines rankings", "search"),
        ("async await in tokio runtime", "rust"),
    ];

    for (text, cat) in docs.iter() {
        store.insert(Chunk {
            text: (*text).into(),
            vector: embedder.embed(text),
            meta: Meta::new().symbol("category", *cat),
        }).unwrap();
    }

    // クエリ: ベクトルとキーワードの両方使う
    let q_text = "ranking fusion bm25";
    let q_vec = embedder.embed(q_text);

    let hits = store.hybrid_search(
        HybridQuery::new(&q_vec, q_text)
            .filter(Filter::symbol("category", "search"))
            .top_k(3)
    ).unwrap();

    println!("hybrid query: {}\n", q_text);
    for (i, h) in hits.iter().enumerate() {
        println!("{}. [rrf {:.4}] {}", i + 1, h.score, h.text.as_deref().unwrap_or(""));
    }
}
