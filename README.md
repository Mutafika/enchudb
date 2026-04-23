# EnchuDB

紐ベース円柱エンジンを中核に据えた組み込み DB と、その上に積む検索 / RAG / トランスポート層。

単一ファイル mmap、prefix-sum O(1) lookup、ロックフリー並行 read。多条件 AND 検索が ns オーダーで返る。

## Workspace

| Crate | 役割 |
|---|---|
| [`enchudb`](./src) | コアストレージエンジン。Cylinder + PairTable + WAL + Ravn |
| [`enchudb-rag`](./crates/enchudb-rag) | RAG ストア。メタフィルタ先行 + brute force cosine、ANN 不要 |
| [`enchudb-text`](./crates/enchudb-text) | bigram 転置インデックスによる全文検索 |
| [`enchudb-transport`](./crates/enchudb-transport) | HTTP / WebSocket 分散トランスポート |
| [`enchu-extend`](./crates/enchu-extend) | Node.js バインディング (napi-rs)。PostgreSQL 透過キャッシュ |

## Quick start

```rust
use enchudb::{Engine, HimoType};

let mut db = Engine::create("/tmp/my.db")?;
db.define_himo("age", HimoType::Value, 100);

let alice = db.entity();
db.tie(alice, "age", 30);
db.tie_text(alice, "city", "東京");

db.rebuild();
let result = db.query(&[("age", 30)]);
```

### 耐久性（WAL、v28+）

```rust
let db = Engine::create_concurrent_with_wal("/tmp/my.db", 256 * 1024 * 1024)?;

let e = db.entity();
db.tie_async(e, "age", 30);
db.wal_sync()?;      // fsync + msync、Sync モード 148μs
// または wal_commit() で Async モード 100ns
```

### グラフ辿り（Ravn、v31+）

```rust
use enchudb::Ravn;
let ravn = Ravn::new(db.clone());

// 「Alice が Next.js で話したセッションの決定事項」
let nextjs = db.query(&[("name", vocab("Next.js")), ("kind", 2)]);
let sessions = ravn.reverse_follow(&nextjs, "topic");
let alice_sessions = ravn.filter_by(&sessions, "speaker", alice_id);
let decisions = ravn.extract_text(&alice_sessions, "decision");
```

DSL:

```
kind:2 name:"Next.js" | reverse topic | where speaker:<alice_eid> | get decision
```

### RAG（`enchudb-rag`）

```rust
use enchudb_rag::{RagStore, Chunk, Meta, Query, Filter, Embedder};

let mut store = RagStore::builder()
    .path("./rag")
    .dim(384)
    .meta_symbol("lang")
    .build()?;

let hits = store.search(
    Query::new(&query_vec)
        .filter(Filter::symbol("lang", "en").and(Filter::value("tenant", 3)))
        .top_k(10)
)?;
```

100k chunks / dim=384 で `tenant=1 AND lang=en` フィルタ付き類似検索が **0.49ms**。

## ベンチマーク

### コアエンジン（vs v27 → v31、200k entities）

| 操作 | v27 | v31 | 差 |
|---|---:|---:|---:|
| tie_async | 73 ns | 85 ns | +16% |
| pull_raw | 105 ns | 105 ns | 0% |
| query（多条件 AND） | 197 ns | 187 ns | −5% |
| sum / avg / min / max / count | ±5% 以内 | | |
| follow / reverse_follow / bfs | ±5% 以内 | | |
| wal_sync | — | 148 μs | 新規 |

### vs SQLite（100万件、多条件フィルタ）

25,830 倍。

### vs Elasticsearch 8.17（profile API 純クエリ時間、マルチテナント 100万件）

| クエリ | EnchuDB | Elasticsearch | 倍率 |
|---|---:|---:|---:|
| 単条件 | 2.3µs | 95µs | 41x |
| 2条件 | 760ns | 320µs | **421x** |
| 4条件 | 19.7µs | 196µs | 10x |

## アーキテクチャ要点

- **紐（himo）**: entity に張る属性 = 索引の単位
- **Column**: source of truth、mmap 永続化
- **BucketCylinder**（v27）: O(1) insert/remove、未ソートバケット
- **PairTable**（v26）: 2 次元ペアの dense cell、多条件 AND 用
- **WAL**（v28+）: crash consistency、背景 fsync、リングバッファ再利用
- **Ravn**（v31+）: グラフ辿り DSL、多段 reverse_follow + filter_by

哲学詳細は [`CLAUDE.md`](./CLAUDE.md) 参照。

## Testing

```bash
# コア unit + integration（約 450 件、20 秒程度）
cargo test --workspace

# 重いスケーリング・ストレステスト（25 件、手動実行）
cargo test --workspace -- --ignored

# ベンチ
cargo run --release --features v32 --example v27_vs_v31_full
```

## ファイル構成

```
{path}        メイン DB
{path}.wal    WAL（v28+、有効時）
{path}.crc    region CRC（seal_integrity 時のみ）
```

## 開発状況

0.x 段階。SemVer 1.0 未到達、破壊的変更あり得る。v1 → v31 を約 1 ヶ月（v28 → v31 は 2 時間弱）で駆け抜けた実験的プロジェクト。

## License

Licensed under either of

 * Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
