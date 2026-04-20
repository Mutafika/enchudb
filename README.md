# EnchuDB

紐ベース円柱エンジン。組み込み DB。単一ファイル、全 mmap、ロックフリー並行 read。

## 特徴

- **多条件 AND 検索が ns オーダー**（100万件で SQLite の 25,830 倍）
- **WAL + crash consistency**（書き込み +16% のみ、読み無影響）
- **グラフ辿り対応**（tie_ref + Ravn で Cypher 相当の多段クエリ）
- **単一ファイル mmap**（+ WAL sidecar、+ optional CRC sidecar）
- **Rust ピュア**（`memmap2`, `crossbeam-queue` 以外なし）

## 最小例

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

## 耐久性（v28+）

```rust
// WAL 有効で作成
let db = Engine::create_concurrent_with_wal("/tmp/my.db", 256 * 1024 * 1024)?;

let e = db.entity();
db.tie_async(e, "age", 30);
db.wal_sync()?;  // fsync + msync(Sync モード、148μs)
// または wal_commit() で非同期(Async モード、100ns)
```

## グラフ辿り（v31 Ravn）

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

## ベンチ vs v27

200k entities、tie_async + WAL vs v27 純粋書き：

| 操作 | v27 | v31 | 差 |
|---|---:|---:|---:|
| tie_async | 73 ns | 85 ns | +16% |
| pull_raw | 105 ns | 105 ns | 0% |
| query (多条件) | 197 ns | 187 ns | -5% |
| sum/avg/min/max/count | 全て ±5% 以内 | | |
| follow/reverse_follow/bfs | 全て ±5% 以内 | | |
| open | 5780 μs | 5847 μs | +1.2% |
| wal_sync | — | 148 μs | 新規 |

## アーキテクチャ要点

- **紐（himo）**: entity に張る属性 = 索引の単位
- **Column**: source of truth、mmap 永続化
- **BucketCylinder**（v27）: O(1) insert/remove、未ソートバケット
- **PairTable**（v26）: 2次元ペアの dense cell、多条件 AND 用
- **WAL**（v28+）: crash consistency、背景 fsync、リングバッファ再利用

紐の哲学は CLAUDE.md 参照。

## ファイル

```
{path}        メイン DB
{path}.wal    WAL(v28 有効時)
{path}.crc    region CRC(seal_integrity 時のみ)
```

## 開発状況

0.x 段階。SemVer 1.0 未到達、API 破壊的変更あり得る。
v1 → v31 を約 1 ヶ月（v28→v31 は 2 時間弱）で駆け抜けた。

## テスト

```
cargo test --features v27 --lib                # unit 110
cargo test --features v27 --test '*'           # integration 29
cargo test --features v27 --test v29_stress -- --ignored  # heavy(重い)
cargo run --release --features v27 --example v27_vs_v31_full  # bench
```
