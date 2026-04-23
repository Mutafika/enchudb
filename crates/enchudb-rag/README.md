# enchudb-rag

EnchuDB の上に積む RAG ストア。メタデータフィルタが ns オーダーで速い → ANN（HNSW 等）無しで brute force cosine が現実的になる、という設計。

## なにこれ

- **単一ファイル級の組み込み RAG**: enchudb（`*.db`）+ ベクトル mmap（`*.vec`）の 2 ファイルのみ
- **ベクトル類似検索**: brute force cosine / dot / L2
- **メタフィルタ**: eq, range, in-set, AND, OR, NOT を Filter DSL で
- **ハイブリッド検索**: ベクトル + BM25 を RRF（Reciprocal Rank Fusion）で融合
- **Chunker**: LangChain 流 RecursiveCharacterSplitter（マルチバイト安全）
- **Embedder / Reranker**: trait だけ提供、実装は呼び出し側

## なぜ ANN を使わないか

一般的な RAG ワークロードは「tenant / lang / date / doc_type で絞ってから類似検索」。メタで 1% に絞れるなら、100 万 chunk → 1 万件の brute force cosine は数 ms で済む。HNSW の恩恵は「絞れない場合」にしか出ない。

EnchuDB v27 はこのメタフィルタ部分を ns オーダーで返せるので、brute force で十分勝てる。

## ベンチ（100k chunks, dim=384, MacBook）

| クエリ | 候補数 | 時間 |
|---|---|---|
| 絞り込みなし（全件 cosine） | 100,000 | 22.7 ms |
| `tenant = 1` | ~1,000 | 0.66 ms |
| `tenant = 1 AND lang = "en"` | ~500 | 0.49 ms |
| `tenant in 0..=9` (range) | ~10,000 | 2.83 ms |

メタで絞るほど線形に速くなる。Qdrant / Pinecone のような「ベクトル速いがメタフィルタが弱点」の構造とは逆。

## 使い方

```rust
use enchudb_rag::{RagStore, Chunk, Meta, Query, Filter, HashEmbedder, Embedder};

let embedder = HashEmbedder::new(384); // 実運用では OpenAI / fastembed / Candle 等

let mut store = RagStore::builder()
    .path("./rag")
    .dim(384)
    .meta_value("tenant", 1024)
    .meta_symbol("lang")
    .meta_value("date", 0)
    .max_entities(1_000_000)
    .build()?;

// 挿入
store.insert(Chunk {
    text: "enchudb uses himo based column store".into(),
    vector: embedder.embed("enchudb uses himo based column store"),
    meta: Meta::new()
        .value("tenant", 3)
        .symbol("lang", "en")
        .value("date", 9100),
})?;

// 検索（メタで絞って top-10）
let q = embedder.embed("himo database");
let hits = store.search(
    Query::new(&q)
        .filter(
            Filter::value("tenant", 3)
                .and(Filter::symbol("lang", "en"))
                .and(Filter::range("date", 9000..=9500))
        )
        .top_k(10)
)?;

for h in &hits {
    println!("[{:.4}] {}", h.score, h.text.as_deref().unwrap_or(""));
}
```

### ハイブリッド検索

```rust
use enchudb_rag::HybridQuery;

let hits = store.hybrid_search(
    HybridQuery::new(&q_vec, "himo column store")
        .filter(Filter::symbol("lang", "en"))
        .top_k(10)
)?;
```

BM25 はインメモリで保持（揮発）。再起動時は `insert` を再実行するか、起動時に全チャンクを回して `bm25().add_document()` を呼ぶ。

### Chunker

```rust
use enchudb_rag::{RecursiveCharSplitter, Chunker};

let splitter = RecursiveCharSplitter::new(500, 50); // max 500 文字、50 文字オーバーラップ
for chunk in splitter.split(long_text) {
    let v = embedder.embed(&chunk);
    store.insert(Chunk { text: chunk, vector: v, meta: Meta::new() })?;
}
```

## ファイル構成

- `*.db` — enchudb 本体（メタ紐、テキスト content、entity 管理）
- `*.vec` — ベクトル mmap（16B ヘッダ + `eid * dim * 4` オフセット、sparse）

仮想サイズは `max_entities * dim * 4` 分確保するが、実ディスクは書いた entity 分だけ。

## 制約

- ベクトル次元は固定。途中変更不可
- BM25 はインメモリ（再起動で再構築必要）
- Embedder / Reranker は呼び出し側で用意（OpenAI API、fastembed、Candle 等）
- 現状 single-writer（`&mut RagStore`）。`engine_mut()` で concurrent API に降りられるが未検証

## なぜ enchudb に組み込まずに別クレートか

- 紐の設計原則（「紐が本質、伝播は Ravn」）にベクトル距離を持ち込みたくない
- `bytemuck` / SIMD 等の依存を enchudb コアに漏らしたくない
- RAG やらないユーザーには完全にノイズ

enchudb-rag は enchudb の公開 API だけを使う**外側のレイヤー**。

## ステータス

v0.1。動く。ベンチ通る。テスト全緑。

- [ ] SIMD cosine（AVX2 / NEON）
- [ ] Reranker の実実装（bge-reranker / Cohere）
- [ ] i8 / binary 量子化
- [ ] BM25 の永続化
- [ ] concurrent insert / search
