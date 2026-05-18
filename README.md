# EnchuDB

**Embedded graph engine with multi-condition AND in nanoseconds.**

紐 (himo) ベースの円柱エンジンを中核に据えた組み込み DB。単一ファイル mmap、BucketCylinder (値 → eid バケット) で **lookup decision が ns 級**、ロックフリー並行 read。結果の返却は memcpy 律速で結果サイズに比例 (µs スケール、物理限界)。

その上に **schema / SQL / FFI / 全文検索 / RAG / P2P sync / transport** を積めるワークスペース。

## なぜ

- **SQLite より速い lookup**: BucketCylinder で値 → eid が ns、多条件 AND がバケット交差で爆速
- **Rocks / Lmdb と違って relations 持てる**: 紐 + 円柱で graph 的 traversal (Ravn)
- **Append-only WAL + HLC**: P2P sync (`enchudb-sync`) が built-in、partial sync (`SubscriptionFilter`) も
- **メモリフットプリント小**: mmap 単一ファイル、ページキャッシュ依存

## Workspace

| Crate | 役割 | meta crate での opt-in |
|---|---|---|
| [`enchudb-wal`](./crates/enchudb-wal) | append-only WAL、HLC、peer keys。engine / sync / transport の共通 primitive | always |
| [`enchudb-engine`](./crates/enchudb-engine) | コアストレージエンジン。Cylinder + PairTable + WAL + Ravn | always |
| [`enchudb-schema`](./crates/enchudb-schema) | **native API**。 仮想 2D テーブル + himo_id pre-resolve + schema 永続化 | always |
| [`enchudb-sync`](./crates/enchudb-sync) | HLC-LWW Syncer、ShardRouter、SubscriptionFilter | always |
| [`enchudb-transport`](./crates/enchudb-transport) | HTTP relay / WebSocket push hub | 直接依存 |
| [`enchudb-text`](./crates/enchudb-text) | bigram 転置インデックスによる全文検索 | 直接依存 |
| [`enchudb-rag`](./crates/enchudb-rag) | RAG ストア。メタフィルタ先行 + brute force cosine、ANN 不要 | 直接依存 |
| [`enchudb-sql`](./crates/enchudb-sql) | SQLite 上位互換の SQL frontend (CRUD + ORDER BY / LIMIT / 範囲 / IS NULL / INSERT OR REPLACE)、 **schema 永続化** | `features = ["sql"]` |
| [`enchudb-ffi`](./crates/enchudb-ffi) | SQLite 風 C ABI 12 関数、`cdylib + staticlib`、Python / Node / Swift から叩ける土台 | `features = ["ffi"]` |
| [`enchudb-cli`](./crates/enchudb-cli) | `enchu` REPL。 `query_lang` 構文 + dot command で engine を直接叩く (SQL 経由しない) | `cargo install --path crates/enchudb-cli` |

各 sub-crate の `README.md` に詳細あり。Node.js バインディングは独立 repo [`mutafika/enchu-extend`](https://github.com/mutafika/enchu-extend)。

## Quick start

推奨は **schema 層**。 仮想 2D テーブルを宣言して CRUD、 schema は DB ファイル内に永続化されるので reopen 時に CREATE 不要。

```rust
use enchudb::schema::Database;

let mut db = Database::create("/tmp/app.db")?;

let users = db.table("users")
    .number("id")
    .tag("name")
    .number("age")
    .primary_key("id")
    .build()?;

let alice = users.insert()
    .set("id", 1i64).set("name", "Alice").set("age", 30i64)
    .commit()?;

let hits = users.where_eq("age", 30i64)
    .where_eq("name", "Alice")
    .find()?;
```

`build()` で col → himo_id が pre-resolve されるので、 hot path の string lookup は内部で消える。 「最適化したいから engine 直叩き」は通常不要 (v0.3.0 で schema 層を zero-cost 化済み)。

reopen:

```rust
let db = Database::open("/tmp/app.db")?;
let users = db.get_table("users").unwrap();   // CREATE 不要、schema 復元済み
```

詳細は [`crates/enchudb-schema/README.md`](./crates/enchudb-schema/README.md)。

### Engine 層直叩き (graph 操作 / 自前 dispatch がしたい時)

```rust
use enchudb::{Engine, HimoType};

let db = Engine::create_standalone("/tmp/my.db")?;
db.define_himo("age", HimoType::Number, 100);

let alice = db.entity();
db.tie(alice, "age", 30);
db.tie_text(alice, "city", "東京");

db.rebuild();
let result = db.query(&[("age", 30)]);
```

### SQL frontend (`features = ["sql"]`)

```rust
use enchudb_sql::{Database, Output};

let mut db = Database::create_growable_tiny("/tmp/notif.db")?;

db.execute("CREATE TABLE notif (key TEXT PRIMARY KEY, dismissed_at INTEGER)")?;
db.execute("INSERT OR REPLACE INTO notif VALUES ('uuid-abc', 1715174400)")?;

if let Output::Rows { rows, columns } = db.execute("
    SELECT key, dismissed_at FROM notif
    WHERE dismissed_at > 1715000000 AND dismissed_at IS NOT NULL
    ORDER BY dismissed_at DESC
    LIMIT 10
")? {
    // rows[i][j] = Value::Integer | Text | Null
}
drop(db);

let mut db = Database::open("/tmp/notif.db")?;
assert_eq!(db.list_tables().len(), 1);    // schema 復元済み
```

非 SQL コンシューマは `Database::list_tables()` で schema を読める。

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

### CLI (`enchu`)

`sqlite3` 風 REPL で engine を直接叩く。 `query_lang` 構文 (`age:30 city:"東京" | group dept | sum salary`) と dot command (`.himos` / `.entity <eid>` / `.dump` …)。 **SQL は経由しない**。

```bash
cargo install --path crates/enchudb-cli
# binary 名は `enchu`

enchu --create --tiny /tmp/state.db          # 新規 DB (1024 行 preset)
enchu /tmp/state.db                          # REPL に入る
enchu /tmp/state.db -e 'age:30 | count'      # one-shot
enchu --readonly /tmp/state.db               # read-only open
```

REPL の例:

```
enchu> .define name tag
defined name (tag, max_values=0)
enchu> .define age num
defined age (num, max_values=0)
enchu> + name:"alice" age:30
+0
enchu> + name:"bob" age:25
+1
enchu> age:30
0
enchu> age:30 | count
1
enchu> .entity 0
eid=0
  name: "alice"
  age: 30
enchu> .quit
```

create preset: `--default` / `--compact` / `--growable` (default) / `--tiny`。

### 耐久性 (WAL)

```rust
let db = Engine::create_concurrent_with_wal("/tmp/my.db", 256 * 1024 * 1024)?;

let e = db.entity();
db.tie_async(e, "age", 30);
db.wal_sync()?;       // fsync + msync
// または wal_commit() で背景 fsync (Async モード)
```

### グラフ辿り (Ravn)

```rust
use enchudb::Ravn;

let ravn = Ravn::new(db.clone());

// 「Alice が Next.js について話したセッションの決定事項」
let nextjs       = db.query(&[("name", vocab_id_for("Next.js")), ("kind", 2)]);
let sessions     = ravn.reverse_follow(&nextjs, "topic");
let by_alice     = ravn.filter_by(&sessions, "speaker", alice_id);
let decisions    = ravn.extract_text(&by_alice, "decision");
```

DSL も使える:

```
kind:2 name:"Next.js" | reverse topic | where speaker:<alice_eid> | get decision
```

### RAG (`enchudb-rag`)

```rust
use enchudb_rag::{RagStore, Chunk, Meta, Query, Filter};

let mut store = RagStore::builder()
    .path("./rag")
    .dim(384)
    .meta_value("tenant", 100)
    .meta_symbol("lang")
    .build()?;

let hits = store.search(
    Query::new(&query_vec)
        .filter(Filter::symbol("lang", "en").and(Filter::value("tenant", 3)))
        .top_k(10)
)?;
```

個人スケール (〜1M chunks) + メタフィルタ先行で sub-ms RAG。実測は [`crates/enchudb-rag/examples/`](./crates/enchudb-rag/examples) を参照。

## ベンチマーク

再現コマンド・実測値・ハードウェア記述は [`benches/README.md`](./benches/README.md) に集約。誇大な数字を README に書かない方針 — 興味ある人は自分で走らせて検証できる。

```bash
cargo bench --bench core
cargo run --release --example vs_sqlite
```

## アーキテクチャ要点

- **紐 (himo)**: entity に張る属性 = 索引の単位、 任意の文字列名 (UTF-8)
- **Column**: source of truth、 mmap 永続化、 `Column[himo][eid] = value`
- **BucketCylinder**: 値ごとの eid バケット、 値 → バケット O(1)、 cylinder slice + Column filter で多条件 AND
- **WAL**: crash consistency、 背景 fsync、 リングバッファ再利用
- **Ravn**: グラフ辿り DSL、 多段 reverse_follow + filter_by
- **仮想 2D テーブル層** (`enchudb-schema`): N 個の紐を 1 つの table 名で束ねる、 col → himo_id を pre-resolve、 schema は DB ファイル内に永続化。 SQL frontend と FFI もこの上に乗る。

メンタルモデル: engine 全体は **モンジャラ** (蔓 = 紐、 entity = 蔓の交差点に隠れた本体)。 1 つの紐の中身は **2D table** (値 × eid バケット)。

各 sub-crate の `README.md` に詳細あり。`docs/` に architecture / concurrency / migration 各種ノートも。

## ファイル構成

```
{path}         メイン DB (mmap、 layout v3、 FILE_VERSION 3)
{path}.wal     WAL (有効時、 sparse file)
{path}.lock    writer 排他 sidecar (writer open 時に flock、 close で release)
{path}.crc     region CRC (seal_integrity 時のみ)
<blob_root>/   BlobStore (別ディレクトリ、 content-addressed、 大 blob 外出し用)
```

## 並行アクセス

「**writer 1 process + reader 無制限**」 の SQLite WAL 相当モデル。

| やりたい事 | API | lock | 同時 process |
|---|---|---|---|
| 書く + 読む | `Engine::open_concurrent_with_wal` / `Engine::open_standalone` | exclusive | 1 |
| 読むだけ | `Engine::open_readonly` / `Database::open_readonly` | なし | 無制限 |

writer は `.db.lock` sidecar に `flock(LOCK_EX)` を engine 寿命中保持。 2 つ目の writer は drop されるまで block (sqlite default 動作と同じ)。 readonly は lock を取らないので writer と共存可、 write API を呼ぶと panic で即気付く。

GUI app + CLI を同 DB で共存する場合は **GUI が `open_readonly`、 CLI が subprocess として writer で開く**のが推奨パターン。 詳細は [`docs/concurrency.md`](./docs/concurrency.md)。

## Testing

```bash
# unit + integration (~400 件、 1 分程度)
cargo test --workspace

# 重いスケーリング・ストレステスト (手動実行)
cargo test --workspace -- --ignored

# bench
cargo bench --bench core
```

## 開発状況

0.x 段階。SemVer 1.0 未到達、 API / on-disk format に破壊的変更が入る可能性あり。 プロダクション利用は自己責任で。

## License

Licensed under the [Functional Source License, Version 1.1, Apache 2.0 Future License](LICENSE.md) (FSL-1.1-Apache-2.0).

In short:

- You may **use, modify, and redistribute** EnchuDB for any purpose **other than offering it as a competing product or service**.
- **Each released version converts to Apache 2.0 two years after that version's release.** The project as a whole stays under FSL while continuously updated; only the specific past releases roll into Apache 2.0.

See [`LICENSE.md`](LICENSE.md) for the full text, and <https://fsl.software/> for background on the FSL.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you shall be licensed under FSL-1.1-Apache-2.0 as above, without any additional terms or conditions.
