//! 統合テスト。tempdir にストアを作って一通り操作。

use enchudb_rag::{Chunk, Filter, HashEmbedder, HybridQuery, Meta, Query, RagStore, Embedder};
use std::path::PathBuf;

fn tmp_path(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("enchudb-rag-test-{}-{}", name, std::process::id()));
    // 前回の残骸を掃除
    let _ = std::fs::remove_file(p.with_extension("db"));
    let _ = std::fs::remove_file(p.with_extension("vec"));
    p
}

#[test]
fn insert_and_search_no_filter() {
    let path = tmp_path("basic");
    let embedder = HashEmbedder::new(64);
    let mut store = RagStore::builder()
        .path(&path)
        .dim(64)
        .meta_symbol("lang")
        .meta_value("year", 3000)
        .max_entities(1000)
        .build()
        .unwrap();

    let texts = [
        "the quick brown fox jumps over the lazy dog",
        "rust is a systems programming language",
        "enchudb is a himo based database engine",
        "vector search with brute force cosine",
    ];

    for t in &texts {
        let v = embedder.embed(t);
        store.insert(Chunk {
            text: t.to_string(),
            vector: v,
            meta: Meta::new().symbol("lang", "en").value("year", 2024),
        }).unwrap();
    }

    // 同じテキストを再埋め込みすれば cos 距離 0 でヒット top
    let q = embedder.embed(texts[2]);
    let hits = store.search(Query::new(&q).top_k(2)).unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].text.as_deref(), Some(texts[2]));
}

#[test]
fn meta_filter_narrows_candidates() {
    let path = tmp_path("filter");
    let embedder = HashEmbedder::new(32);
    let mut store = RagStore::builder()
        .path(&path)
        .dim(32)
        .meta_value("tenant", 100)
        .meta_symbol("lang")
        .max_entities(1000)
        .build()
        .unwrap();

    // tenant 1 (lang ja) × 3
    for i in 0..3 {
        let t = format!("テナント1のドキュメント {}", i);
        store.insert(Chunk {
            text: t.clone(),
            vector: embedder.embed(&t),
            meta: Meta::new().value("tenant", 1).symbol("lang", "ja"),
        }).unwrap();
    }
    // tenant 2 (lang en) × 3
    for i in 0..3 {
        let t = format!("tenant 2 document {}", i);
        store.insert(Chunk {
            text: t.clone(),
            vector: embedder.embed(&t),
            meta: Meta::new().value("tenant", 2).symbol("lang", "en"),
        }).unwrap();
    }

    let q = embedder.embed("document");

    // tenant=1 のみ
    let hits = store.search(
        Query::new(&q).filter(Filter::value("tenant", 1)).top_k(10)
    ).unwrap();
    assert_eq!(hits.len(), 3);
    for h in &hits {
        assert!(h.text.as_ref().unwrap().starts_with("テナント1"));
    }

    // tenant=2 AND lang=en → 3 件（全部）
    let hits = store.search(
        Query::new(&q)
            .filter(Filter::value("tenant", 2).and(Filter::symbol("lang", "en")))
            .top_k(10)
    ).unwrap();
    assert_eq!(hits.len(), 3);
}

#[test]
fn range_filter() {
    let path = tmp_path("range");
    let embedder = HashEmbedder::new(16);
    let mut store = RagStore::builder()
        .path(&path)
        .dim(16)
        .meta_value("year", 3000)
        .max_entities(1000)
        .build()
        .unwrap();

    for y in 2020..=2025 {
        let t = format!("year {}", y);
        store.insert(Chunk {
            text: t.clone(),
            vector: embedder.embed(&t),
            meta: Meta::new().value("year", y as u32),
        }).unwrap();
    }

    let q = embedder.embed("year");
    let hits = store.search(
        Query::new(&q).filter(Filter::range("year", 2022..=2024)).top_k(10)
    ).unwrap();
    assert_eq!(hits.len(), 3);
}

#[test]
fn hybrid_search_combines_vector_and_bm25() {
    let path = tmp_path("hybrid");
    let embedder = HashEmbedder::new(32);
    let mut store = RagStore::builder()
        .path(&path)
        .dim(32)
        .meta_symbol("lang")
        .max_entities(1000)
        .build()
        .unwrap();

    let texts = [
        "enchudb is a himo database",
        "rust cargo package manager",
        "cosine distance for vector similarity",
        "the quick brown fox",
        "himo based engine with bucket cylinder",
    ];
    for t in &texts {
        store.insert(Chunk {
            text: t.to_string(),
            vector: embedder.embed(t),
            meta: Meta::new().symbol("lang", "en"),
        }).unwrap();
    }

    // "himo" を含むドキュメントが BM25 で高スコア、ベクトルも一応効く
    let q = embedder.embed("himo");
    let hits = store.hybrid_search(HybridQuery::new(&q, "himo").top_k(3)).unwrap();
    assert!(!hits.is_empty());
    // 少なくとも 1 つは "himo" を含む
    assert!(hits.iter().any(|h| h.text.as_ref().unwrap().contains("himo")));
}

#[test]
fn delete_and_upsert() {
    let path = tmp_path("mutate");
    let embedder = HashEmbedder::new(16);
    let mut store = RagStore::builder()
        .path(&path)
        .dim(16)
        .meta_value("tag", 10)
        .max_entities(1000)
        .build()
        .unwrap();

    let eid = store.insert(Chunk {
        text: "original".into(),
        vector: embedder.embed("original"),
        meta: Meta::new().value("tag", 1),
    }).unwrap();

    store.upsert(eid, Chunk {
        text: "updated".into(),
        vector: embedder.embed("updated"),
        meta: Meta::new().value("tag", 2),
    }).unwrap();

    assert_eq!(store.text(eid), Some("updated"));
    assert_eq!(store.meta_value(eid, "tag"), Some(2));

    store.delete(eid);
    // delete 後、同じ field で引いても 0 件
    let q = embedder.embed("updated");
    let hits = store.search(
        Query::new(&q).filter(Filter::value("tag", 2)).top_k(10)
    ).unwrap();
    assert!(hits.is_empty() || hits.iter().all(|h| h.eid != eid));
}

#[test]
fn persistence_roundtrip() {
    let path = tmp_path("persist");
    let embedder = HashEmbedder::new(16);

    // 書き込み → flush → drop
    {
        let mut store = RagStore::builder()
            .path(&path)
            .dim(16)
            .meta_symbol("lang")
            .max_entities(100)
            .build()
            .unwrap();
        store.insert(Chunk {
            text: "persisted text".into(),
            vector: embedder.embed("persisted text"),
            meta: Meta::new().symbol("lang", "en"),
        }).unwrap();
        store.flush().unwrap();
    }

    // 再 open
    let store = RagStore::builder()
        .path(&path)
        .dim(16)
        .meta_symbol("lang")
        .max_entities(100)
        .build()
        .unwrap();
    // BM25 は揮発なので再構築しないと search はマッチしないが、
    // ベクトルとメタは永続化されている
    let q = embedder.embed("persisted text");
    let hits = store.search(Query::new(&q).top_k(5)).unwrap();
    assert!(!hits.is_empty());
    assert_eq!(hits[0].text.as_deref(), Some("persisted text"));
}
