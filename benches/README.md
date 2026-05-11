# EnchuDB Benchmarks

再現可能なベンチマーク集。 各シナリオに **コマンド + 実測値 + 計測環境** を併記。
誇大な数字を README に書かないために、 数字はここでだけ管理する。

## 環境 disclosure (このページの数字を取った時の)

| 項目 | 値 |
|---|---|
| CPU | Apple M4 Max |
| Memory | 64 GB |
| OS | macOS 26.0.1 (Build 25A362) |
| rustc | 1.93.0 (2026-01-19) |
| commit | `af0398a` (2026-05-11) |
| build profile | `--release` |
| feature flags | `--features "v27 v32"` (個別に明記) |

数字は **走らせる度に ±5-10% 揺れる** (thermal / background load / mmap warm 状態)。
ハードウェアが違えば桁は同じでも絶対値は変わる。 比較は **倍率** で読むこと。

---

## メインベンチ

### 1. vs SQLite (1M entities)

組込 DB として SQLite と直接比較。 `examples/vs_sqlite.rs`。

```bash
cargo run --release --features "v27 v32" --example vs_sqlite
```

実測 (1,000,000 entities、 4 himo: dept / status / salary / age):

| クエリ | EnchuDB | SQLite | 倍率 |
|---|---:|---:|---:|
| 1 条件 (dept=3) | 11.5 µs | 2.00 ms | **174x** |
| 2 条件 (dept=0 AND status=1) | 12.8 µs | 49.55 ms | **3,878x** |
| 3 条件 (dept=0 AND status=1 AND age=20) | 314 ns | 13.81 ms | **43,972x** |
| 範囲 (age 30..40) | 43.3 µs | 12.08 ms | 279x |
| COUNT (status=2) | 70.4 µs | 5.06 ms | 72x |
| SUM (dept=3) | 109.4 µs | 19.33 ms | 177x |
| SUM (全件) | 2.23 ms | 26.62 ms | 12x |
| GROUP BY dept SUM salary (全件) | 6.85 ms | 366.25 ms | 53x |
| MIN/MAX salary (dept=5) | 216.8 µs | 34.60 ms | 160x |
| get (単一 entity) | 12 ns | 3.7 µs | 309x |
| insert 1M | 938 ms | 4,870 ms | 5.2x |

倍率は **条件数が増えると上がる** (cylinder の AND は条件絞り込みで効く)。
全件走査系 (SUM 全件 / GROUP BY 全件) は SQLite と差が縮む。

### 2. v27 vs v31 perf regression

v27 (バケット円柱) と v31 (WAL + concurrent writer) の hot path 比較。
`examples/v27_vs_v31_full.rs`。

```bash
cargo run --release --features "v27 v32" --example v27_vs_v31_full
```

実測 (200,000 entities):

| 操作 | v27 (no WAL) | v31 (WAL on) | 差 |
|---|---:|---:|---:|
| tie (insert) | 148 ns/op | 340 ns/op | +130% |
| pull_raw (単一値引き) | 615 ns/op | 605 ns/op | −1.6% |
| pull_range (範囲) | 7,326 ns/op | 6,186 ns/op | −15.6% |
| query (多条件 AND) | 696 ns/op | 695 ns/op | −0.1% |
| get (単一 entity) | 8 ns/op | 8 ns/op | 0% |
| sum / avg / min / max / count | ±5% | | |
| follow / reverse_follow | ±5% | | |
| bfs depth=3 | 894 ns/op | 919 ns/op | +2.8% |
| wal_sync | — | 1.28 ms | 新規 (fsync + body msync 込み) |
| open (200K entities) | 15.4 ms | 14.9 ms | 同等 |

**読みは全部 ±5% 以内、書きは WAL append の代償で +130%**。 durability が欲しい場面で
払う代価としては妥当。 wal_sync を呼ばない async モードは "fire and forget" で 100ns 級。

### 3. criterion regression suite

`benches/core.rs`。 主要 op の **退行検出** が目的、 数字そのものではなく
ΔTime% に注目する。

```bash
# 初回 (baseline 記録)
cargo bench --features v32 --bench core -- --save-baseline main

# 変更後 (比較)
cargo bench --features v32 --bench core -- --baseline main
```

criterion が ±10% 以上の劣化を自動で flag する。 CI に組み込む用。

---

## その他のベンチ (`examples/` 配下)

| ファイル | 用途 |
|---|---|
| `agentic_workload_bench.rs` | LLM agent 風の高頻度 read/write mix |
| `pair_table_bench.rs` | v26 ペアテーブルの多条件 AND |
| `v26_bench.rs` / `v26_integrated_bench.rs` / `v26_many_conds.rs` | v26 系の詳細 |
| `v27_bench.rs` | v27 BucketCylinder の挙動 |
| `column_read_bench.rs` | Column 直読みパスのみ |
| `blob_bench.rs` | BlobStore (大 blob 外出し) |
| `cas_experiment.rs` / `combo_experiment.rs` / `delta_sync_experiment.rs` | 設計時 R&D の名残 |
| `hybrid_experiment.rs` / `unified_delta_experiment.rs` / `zorder_experiment.rs` | 同上 (採用見送り含む) |
| `v26_cas_test.rs` / `v26_destruction_test.rs` / `v26_full_test.rs` | v26 全機能 test 兼 bench |

各ファイルの先頭コメントに目的・走り方が書いてある。

---

## 数字を扱う上での注意

- **`--release` 必須**。 debug build は別世界。
- **mmap warm-up** で初回 op はキャッシュ未ヒット、 ファイル全体を touch してから測ること
  (vs_sqlite と v27_vs_v31_full はすでに warm 状態で測る作り)。
- **thermal**: M シリーズ MacBook はノート筐体だと長時間負荷で thermal throttle が入る。
  Mac mini / 据え置きと比べて 10-20% 遅くなる場合がある。
- **bench 同士の比較**: ある条件で速くても別条件で遅いことはよくある。 cylinder 設計上、
  **条件が増えるほど絞り込みが効いて速くなる** (典型的な RDB と挙動が逆) ので、
  「単条件で 174x」 と 「3 条件で 43,972x」 が同じシステムで両立する。

## 公平に書いておくこと

- **EnchuDB が常に勝つ訳ではない**。 BTree-friendly な range scan (`WHERE id > 100 AND id < 200`)
  で sorted leaf を読む SQLite は強い。 cylinder は等値 AND が桁違いに速い反面、 範囲は
  pull_range で min..=max を線形走査するので幅が広いと負ける。
- **持続性のセマンティクス**: SQLite は ACID をデフォルトで提供、 EnchuDB の async モード
  は durability を捨ててる。 wal_sync を毎回呼べば SQLite と同等の durability になるが
  その分遅くなる (1.28 ms/sync) ので、 比較するなら durability mode を揃えること。
- **Elasticsearch との比較**: 過去に README に書いていた 41x/421x/10x の数字は repo 内に
  bench が無いので一旦撤去。 再測定するなら別途 `examples/vs_elasticsearch.rs` を作って
  ここに数字を載せる。
