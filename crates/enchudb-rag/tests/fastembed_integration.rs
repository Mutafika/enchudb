//! FastEmbedder × RagStore の end-to-end テスト。
//!
//! 初回実行時に BGE-small-en v1.5（約 130MB）を hf-hub 経由で DL する。
//! 以後 `~/.cache/fastembed/` にキャッシュ。
//!
//! 実行: `cargo test --features fastembed --test fastembed_integration -- --nocapture`

#![cfg(feature = "fastembed")]

use enchudb_rag::{
    Chunk, Embedder, FastEmbedder, HybridQuery, Meta, Query, RagStore,
};

fn tmpdir() -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!(
        "rag-fastembed-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// BGE-small は 384 dim を返すか、2 回 embed した同じ文で同一ベクトルか。
#[test]
fn fastembed_default_dim_and_determinism() {
    let emb = FastEmbedder::new().expect("model init");
    assert_eq!(emb.dim(), 384, "BGESmallENV15 は 384 dim のはず");

    let a = emb.embed("cats are small carnivorous mammals");
    let b = emb.embed("cats are small carnivorous mammals");
    assert_eq!(a.len(), 384);
    assert_eq!(a, b, "同じテキストは決定的");
}

/// バッチと単発が一致する。
#[test]
fn fastembed_batch_matches_single() {
    let emb = FastEmbedder::new().expect("model init");
    let texts = vec!["hello world", "foxes hunt at night", "quantum entanglement"];

    let batch = emb.embed_batch(&texts);
    assert_eq!(batch.len(), 3);
    for v in &batch {
        assert_eq!(v.len(), 384);
    }

    // BGE はバッチサイズで結果が変わらないはず（L2 正規化済み）。
    // 厳密 eq だと浮動小数の micro 差で落ちるので cosine で確認。
    for (i, t) in texts.iter().enumerate() {
        let single = emb.embed(t);
        let sim = cosine(&single, &batch[i]);
        assert!(sim > 0.999, "batch vs single cosine={} for {:?}", sim, t);
    }
}

/// セマンティック類似が反映されている。
/// 「犬」と「子犬」は「量子力学」より近いはず。
#[test]
fn fastembed_semantic_similarity() {
    let emb = FastEmbedder::new().expect("model init");

    let dog = emb.embed("a dog barks at the mailman");
    let puppy = emb.embed("the puppy wags its tail");
    let quantum = emb.embed("quantum field theory in curved spacetime");

    let dog_puppy = cosine(&dog, &puppy);
    let dog_quantum = cosine(&dog, &quantum);

    assert!(
        dog_puppy > dog_quantum + 0.05,
        "dog↔puppy ({}) > dog↔quantum ({}) が期待値",
        dog_puppy, dog_quantum
    );
}

/// RagStore に実 embed を流し込んで、検索がセマンティック順に並ぶ。
#[test]
fn fastembed_end_to_end_search_ranks_semantically() {
    let emb = FastEmbedder::new().expect("model init");
    let dim = emb.dim();

    let tmp = tmpdir();
    let mut store = RagStore::builder()
        .path(tmp.join("rag"))
        .dim(dim)
        .max_entities(1024)
        .meta_symbol("topic")
        .build()
        .unwrap();

    let docs = [
        ("cats", "Cats are small carnivorous mammals often kept as pets."),
        ("dogs", "Dogs are loyal companions and descendants of wolves."),
        ("astro", "Black holes warp spacetime and trap light."),
        ("chem",  "Acids donate protons while bases accept them."),
        ("birds", "Sparrows are small songbirds found across Europe."),
    ];

    for (topic, text) in &docs {
        let v = emb.embed(text);
        store
            .insert(Chunk {
                text: (*text).into(),
                vector: v,
                meta: Meta::new().symbol("topic", *topic),
            })
            .unwrap();
    }
    store.engine_mut().rebuild();

    // クエリ「家で飼う動物」→ cats/dogs が上位、black hole は下のはず。
    let q_vec = emb.embed("pets that live in human homes");
    let hits = store
        .search(Query::new(&q_vec).top_k(5))
        .unwrap();

    assert!(!hits.is_empty());
    // top 2 は cats か dogs のどちらか
    let top2_texts: Vec<String> = hits.iter().take(2)
        .map(|h| h.text.clone().unwrap_or_default()).collect();
    let has_cats = top2_texts.iter().any(|t| t.contains("Cats"));
    let has_dogs = top2_texts.iter().any(|t| t.contains("Dogs"));
    assert!(
        has_cats || has_dogs,
        "top2 に cats か dogs が入るべき: {:?}",
        top2_texts
    );

    // black hole は top2 には絶対来ない
    assert!(
        !top2_texts.iter().any(|t| t.contains("Black holes")),
        "black holes が上位は不自然: {:?}",
        top2_texts
    );
}

/// ハイブリッド検索（ベクトル + BM25 + RRF）も動く。
#[test]
fn fastembed_hybrid_search_works() {
    let emb = FastEmbedder::new().expect("model init");
    let dim = emb.dim();

    let tmp = tmpdir();
    let mut store = RagStore::builder()
        .path(tmp.join("rag"))
        .dim(dim)
        .max_entities(1024)
        .build()
        .unwrap();

    let docs = [
        "The Eiffel Tower is located in Paris, France.",
        "Mount Fuji is the tallest mountain in Japan.",
        "The Great Wall of China stretches over 13,000 miles.",
        "The Statue of Liberty was a gift from France to the United States.",
    ];

    for text in &docs {
        let v = emb.embed(text);
        store
            .insert(Chunk {
                text: (*text).into(),
                vector: v,
                meta: Meta::new(),
            })
            .unwrap();
    }
    store.engine_mut().rebuild();

    let q_text = "Paris landmark tower";
    let q_vec = emb.embed(q_text);
    let hits = store
        .hybrid_search(HybridQuery::new(&q_vec, q_text).top_k(2))
        .unwrap();

    assert!(!hits.is_empty());
    let top = hits[0].text.as_deref().unwrap_or("");
    assert!(
        top.contains("Eiffel") || top.contains("Paris"),
        "hybrid top should be Eiffel Tower: {:?}",
        top
    );
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    dot / (na.sqrt() * nb.sqrt()).max(1e-12)
}
