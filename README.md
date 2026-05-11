# EnchuDB

紐ベース円柱エンジンを中核に据えた組み込み DB と、その上に積む SQL / FFI / 検索 / RAG / トランスポート層。

単一ファイル mmap、BucketCylinder (値 → eid バケット) で **lookup decision が ns 級**、 ロックフリー並行 read。 結果の返却は memcpy 律速で結果サイズに比例 (µs スケール、 物理限界)。

## Workspace

| Crate | 役割 | meta crate での opt-in |
|---|---|---|
| [`enchudb-engine`](./crates/enchudb-engine) | コアストレージエンジン。Cylinder + PairTable + WAL + Ravn | always |
| [`enchudb-sync`](./crates/enchudb-sync) | HLC-LWW Syncer、ShardRouter | `features = ["v32"]` |
| [`enchudb-transport`](./crates/enchudb-transport) | HTTP relay / WebSocket push hub | runtime 直接依存 |
| [`enchudb-text`](./crates/enchudb-text) | bigram 転置インデックスによる全文検索 | 直接依存 |
| [`enchudb-rag`](./crates/enchudb-rag) | RAG ストア。メタフィルタ先行 + brute force cosine、ANN 不要 | 直接依存 |
| [`enchudb-sql`](./crates/enchudb-sql) | SQLite 上位互換の SQL frontend (CRUD + ORDER BY / LIMIT / 範囲 / IS NULL / INSERT OR REPLACE)、 **schema 永続化** | `features = ["sql"]` |
| [`enchudb-ffi`](./crates/enchudb-ffi) | SQLite 風 C ABI 12 関数、`cdylib + staticlib`、Python / Node / Swift から叩ける土台 | `features = ["ffi"]` |

各 sub-crate の `README.md` に詳細あり。Node.js バインディングは独立 repo [`mutafika/enchu-extend`](https://github.com/mutafika/enchu-extend) (v33 で sibling として fork)。

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

### SQL frontend (`features = ["sql"]`)

```rust
use enchudb_sql::{Database, Output, Value};

// state-log preset (apparent ~0.7 MB、Time Machine / rsync 互換サイズ)
let mut db = Database::create_growable_tiny("/tmp/notif.db")?;

db.execute("CREATE TABLE notif (key TEXT PRIMARY KEY, dismissed_at INTEGER)")?;
db.execute("INSERT OR REPLACE INTO notif VALUES ('uuid-abc', 1715174400)")?;

match db.execute("
    SELECT key, dismissed_at FROM notif
    WHERE dismissed_at > 1715000000 AND dismissed_at IS NOT NULL
    ORDER BY dismissed_at DESC
    LIMIT 10
")? {
    Output::Rows { rows, columns } => { /* rows[i][j] = Value::Integer | Text | Null */ }
    _ => {}
}
drop(db);  // close (Drop で自動 flush)

// 再 open — CREATE TABLE 再呼出は不要、 schema は DB ファイルに永続化されてる
let mut db = Database::open("/tmp/notif.db")?;
assert_eq!(db.list_tables().len(), 1);
db.execute("SELECT key FROM notif")?;  // そのまま使える
```

非 SQL コンシューマ (enchu studio など) は `Database::list_tables()` で schema を直接読める。

### C ABI (`features = ["ffi"]`)

```bash
cargo build --release -p enchudb-ffi
# → target/release/libenchudb_ffi.{dylib,so,a}
```

```c
#include "enchudb.h"
enchudb_db* db;
enchudb_open("/tmp/x.db", &db);
enchudb_exec(db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");

enchudb_result* r;
enchudb_query(db, "SELECT * FROM t", &r);
for (size_t i = 0; i < enchudb_result_rows(r); i++) {
    int64_t id = enchudb_result_int(r, i, 0);
    const char* name = enchudb_result_text(r, i, 1);
}
enchudb_result_free(r);
enchudb_close(db);
```

ヘッダ: `crates/enchudb-ffi/include/enchudb.h`、デモ: `crates/enchudb-ffi/examples/demo.c`。

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

再現コマンド・実測値・ハードウェア記述は [`benches/README.md`](./benches/README.md) に集約。
誇大な数字を README に書かない方針 — 興味ある人は自分で走らせて検証できる。

## アーキテクチャ要点

- **紐（himo）**: entity に張る属性 = 索引の単位、 任意の文字列名 (UTF-8)
- **Column**: source of truth、 mmap 永続化、 `Column[himo][eid] = value`
- **BucketCylinder**（v27、 default）: 値ごとの eid バケット、 値 → バケット O(1)、 cylinder slice + Column filter で多条件 AND
- **PairTable**（v26、 opt-in）: 紐 2 本のペア cell、 v27 で多くを吸収済み、 現在 dormant
- **WAL**（v28+）: crash consistency、 背景 fsync、 リングバッファ再利用
- **Ravn**（v31+）: グラフ辿り DSL、 多段 reverse_follow + filter_by
- **仮想 2D テーブル層** (`enchudb-sql`): N 個の紐を 1 つの table 名で束ねる、 schema は DB ファイル内に永続化

メンタルモデル: engine 全体は **モンジャラ** (蔓 = 紐、 entity = 蔓の交差点に隠れた本体)。 1 つの紐の中身は **2D table** (値 × eid バケット)。

各 sub-crate の `README.md` に詳細あり。

## Testing

```bash
# コア unit + integration (~400 件、 1 分程度)
cargo test --workspace

# 重いスケーリング・ストレステスト (25 件、 手動実行)
cargo test --workspace -- --ignored

# ベンチ — 詳細は benches/README.md
cargo run --release --features "v27 v32" --example vs_sqlite
cargo run --release --features "v27 v32" --example v27_vs_v31_full
cargo run --release --features "v33 v26" --example v33_vs_pairs
cargo bench --features v32 --bench core
```

## ファイル構成

```
{path}         メイン DB (mmap、 layout v3、 FILE_VERSION 3)
{path}.wal     WAL (v28+、 有効時、 sparse file)
{path}.crc    region CRC (seal_integrity 時のみ)
<blob_root>/   BlobStore (別ディレクトリ、 content-addressed、 大 blob 外出し用)
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
