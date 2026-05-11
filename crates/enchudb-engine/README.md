# enchudb-engine

EnchuDB のコアエンジン。 紐ベース円柱の本体、 単独で完結する組み込み DB。

## なにこれ

- 単一ファイル mmap、 layout v3 (FILE_VERSION 3)
- BucketCylinder (v27、 default) で値 → eid バケットを O(1) 直返し
- Column 直読みで attribute fetch も O(1)
- ロックフリー並行 read (`Arc<Engine>` 共有可、 reader は `&self` で動く)
- WAL + crash consistency (v32+)
- Ed25519 署名 + HLC + p2p sync 用の primitive (v32+)

## メンタルモデル

| Layer | 構造 | 喩え |
|---|---|---|
| engine 全体 | `(himo, value, eid)` の 3D 空間 | **モンジャラ** (蔓の交差点 = entity) |
| 1 himo の cylinder | `(value, eid)` の 2D jagged table | 色違いの **毛糸玉** |
| 2 himo の PairTable (v26 opt-in) | `(value_a, value_b, eid)` の 3D slice | 名前 = 古い遺物 |

詳細は本リポジトリの `CLAUDE.md` 参照 (内部資料、 公開対象外)。

## 主要 API

```rust
use enchudb_engine::{Engine, HimoType};

let mut db = Engine::create_standalone("/tmp/my.db")?;
db.define_himo("age", HimoType::Value, 100);

// 書き込み
let alice = db.entity();
db.tie(alice, "age", 30);
db.tie_text(alice, "city", "東京");

// 読み出し
db.rebuild();  // BucketCylinder のキャッシュ構築 (v27 では大半 no-op)
let result = db.query(&[("age", 30)]);          // 多条件 AND
let alice_age = db.get(alice, "age");           // attribute fetch
let in_tokyo = db.pull_raw("city", db.vocab_id("東京").unwrap());

// 集計
db.sum("age", &result);
db.group_count("city", &db.entities());
db.distinct("city", &db.entities());

// 永続化
db.flush()?;
```

## create 系統

| メソッド | apparent size | 用途 |
|---|---|---|
| `create_standalone` | 88 GB sparse | デフォルト、 mmap 仮想空間大 |
| `create_compact` | ~305 MB | CLI / 中規模 |
| `create_growable` | layout total | grow-on-write、 通常用途 |
| `create_growable_tiny` | ~0.5 MB | state-log preset (1024 entities) |
| `create_concurrent_with_wal` | layout total | WAL 有効、 並行 writer |

## feature flag

| feature | 提供物 | 依存 |
|---|---|---|
| `v26` | PairTable (現在は v27 で吸収済み、 dormant) | — |
| `v27` (default 含) | BucketCylinder + concurrent writer | `crossbeam-queue` |
| `v32` (default 含) | WAL + HLC + Ed25519 + BlobStore + changefeed + snapshot + audit | v27 + `ed25519-dalek` + `rand_core` |
| `v33` (**default**) | text/ref の cross-peer sync 修正 | v32 |
| `async-blob` | AsyncBlobStore trait + tokio-based default adapter | `async-trait` + `tokio` |

default は `v33` (= v33 → v32 → v27 連鎖)。 `--no-default-features` で v24 cylinder の fallback path に落ちる (legacy 互換のため残してある)。

## DSL (`query_lang`)

REPL / プログラマブル用の独自 DSL。 SQL より直接的:

```
age:30 city:"東京"               # AND 検索
age:20..30                        # 範囲
age:30 | count                    # 件数
age:30 | sum salary
age:30 | group dept | count       # グループ集計
age:30 | group dept | sum salary
age:30 | distinct city

+ age:30 city:"田中"              # insert
~ 42 age:31 city:"福岡"           # 既存 entity の紐を置換
- 42                              # delete
```

```rust
use enchudb_engine::query_lang::execute;
let result = execute(&mut db, r#"age:30 | group dept | sum salary"#);
```

## Ravn (graph traversal)

```rust
use enchudb_engine::Ravn;
let ravn = Ravn::new(db.clone());

let alice_sessions = ravn.reverse_follow(&db.query(&[("name", alice_vid)]), "speaker");
let decisions = ravn.extract_text(&alice_sessions, "decision");
```

## ファイル構成

```
{path}        メイン DB (mmap)
{path}.wal    WAL (v32 以降の有効時)
{path}.crc    region CRC sidecar (seal_integrity 時のみ)
```

詳細な on-disk layout / 保証セマンティクスはルートの `SPEC.md` 参照 (内部資料)。

## 制約

- `tie()` の value は `< u32::MAX` (u32::MAX は sentinel 予約)
- ContentStore data 上限 512 MB (超えるなら `BlobStore` へ)
- `max_himos` デフォルト 256 (`create_growable_tiny` は 16)
- `max_entities` デフォルト 16M (`create_with_capacity` で変更可)
