# EnchuDB Benchmarks

再現可能なベンチマーク集。 各シナリオに **コマンド + 実測値 + 計測環境** を併記。
誇大な数字を README に書かないために、 数字はここでだけ管理する。

## 環境 disclosure

| 項目 | 値 |
|---|---|
| CPU | Apple M2 Max |
| Memory | (記入) |
| OS | macOS |
| rustc | stable (記入) |
| build profile | `--release` |

数字は **走らせる度に ±5-10% 揺れる** (thermal / background load / mmap warm 状態)。
ハードウェアが違えば桁は同じでも絶対値は変わる。 比較は **倍率** で読むこと。

過去 commit (M4 Max で計測) の表は git 履歴を参照。 機材が変わったので
本ページは M2 Max で再測した値に随時差し替える。

---

## メインベンチ

### 1. vs SQLite / DuckDB / LMDB (1M entities) — schema 層

組込 DB と直接比較。 `examples/vs_db.rs` (4-way、 全部 in-process binding)。
**schema 層** (`Database::create` + `table.where_eq` 等) で計測 — 公開 README が推奨するパス。

```bash
cargo run --release --example vs_db
```

実測 2026-08-29 (M2 Max、 1,000,000 entities、 dept / status / salary / age 全列 index。
0.25.1 (単一 file) と 0.26.0 v10 (directory) を同日に同条件で。 差は run 間の揺れの範囲):

| クエリ | hits | EnchuDB 0.25.1 | **EnchuDB v10** | SQLite | DuckDB | LMDB |
|---|---:|---:|---:|---:|---:|---:|
| point-by-PK | 1 | 195 ns | **153 ns** | 2.5 µs | 223 µs | 241 ns |
| 1 条件 (dept=3) | 50K | 12.7 µs | **11.3 µs** | 1.81 ms | 1.63 ms | 533 µs |
| 2 条件 (dept AND status) | 50K | 151 µs | **150 µs** | 41.1 ms | 1.76 ms | 19.2 ms |
| 3 条件 (+ age) | 10K | 108 µs | **80 µs** | 8.67 ms | 1.08 ms | 27.4 ms |
| 範囲 (age 30..40) | 220K | 491 µs | **480 µs** | 8.84 ms | 4.84 ms | 2.35 ms |
| COUNT (status=2) | 200K | 38 µs | **44 µs** | 3.26 ms | 416 µs | 2.08 ms |
| SUM salary (dept=3) | 50K | 63 µs | **66 µs** | 14.1 ms | 731 µs | 18.4 ms |
| SUM salary (全件) | 1M | 59 µs | **60 µs** | 24.2 ms | 362 µs | 9.30 ms |
| GROUP BY dept SUM (全件) | 20 | 642 µs | **665 µs** | 297 ms | 948 µs | 9.65 ms |
| MIN/MAX salary (dept=5) | 50K | — | **143 µs** | 14.5 ms | 644 µs | 18.2 ms |
| setup (1M insert) | — | 500 ms | **527 ms** | 4.8 s | 370 ms | 1.38 s |

`multi_cond_scaling` (1M × 7 列、 谷カーブ) と `rag_compare` (enchudb-rag、 N=10K/100K) も
0.25.1 と v10 で一致 (±3%)。

挙動メモ:
- **条件 AND は絞り込みが進むほど速い** (cylinder 交差): 3 条件 (10K hits, 89µs) は
  2 条件 (50K hits, 238µs) より絶対時間が短い。 RDB と挙動が逆。
- **多条件 AND の主戦略は bitmap_and**: 全 himo の bitmap word AND (O(N/64)) →
  bit extraction で eid 列構築。 per-hit ~5 ns のオーバーヘッド。
- **範囲 (BETWEEN) は SQLite と互角**: cylinder は等値 AND が得意、 連続 range は pull_range
  で min..=max を線形走査するので幅が広いと負ける。 220K hits を 12ms (55 ns/hit) で出すのが限界。
- **GROUP BY / 全件 SUM は差が縮む** が、 それでも 14-29x: 全件走査でも cylinder バケット
  読みが SQLite の B-tree leaf walk より速い。
- **`insert 1M`**: enchudb 608 ms / SQLite 6803 ms (約 11x、 cylinder の incremental insert が
  index 化込みで十分速い)。 単発計測なので表からは除外、 setup 行で表示。

### 2. RAG vs naive baseline (`enchudb-rag`)

`crates/enchudb-rag/examples/rag_compare.rs`。 enchudb-rag が naive な
`Vec<Vec<f32>>` linear scan に対してどう振る舞うか、 メタフィルタ選択率を
変えながら測る。

```bash
cargo run --release --example rag_compare -p enchudb-rag
```

軸:
- スケール: 10K / 100K
- 次元: 384 / 768
- フィルタ選択率: 100% (なし) / 50% / 10% / 1%
- 計測: p50 / p99 latency + recall@10 (naive を ground truth として)

知見:
- 1% フィルタ + 100K × 768d で **sub-ms** (M2 Max)
- enchudb-rag と naive はほぼ同等 — cosine FLOPs が dominant で、
  ns 級メタ lookup の優位は **RAG では物理的に見えない** (cosine 計算時間 >>
  lookup 時間)
- recall@10 は両方とも 100% (両方 brute force)

ns lookup の優位が見えるのは RAG ではなく **構造クエリ / KV / counter / token validation** 系。

### 3. 多条件 AND の cond 数スケーリング

`examples/multi_cond_scaling.rs`。 7 himo (値域 5/20/10/8/40/50/1000) のテーブルで
cond 数を 1→7 と増やして latency を測る。 「複合条件で速くなるのか」 の検証。

```bash
cargo run --release --example multi_cond_scaling
```

実測 (M2 Max、 1M rows、 deterministic xorshift で独立サンプリング):

| cond | hits | time | per hit |
|---:|---:|---:|---:|
| 1 | 199K | 58.7 µs | 0.29 ns |
| 2 | 9K | 285.2 µs | 28.7 ns |
| 3 | 956 | 312.0 µs | 326 ns |
| 4 | 128 | 359.7 µs | 2,810 ns |
| 5 | 3 | 192.2 µs | 64,051 ns |
| 6 | 0 | 135.0 µs | — |
| 7 | 0 | 4.0 µs | — |

挙動:
- **cond=1**: pull 直叩き fast path (`query_resolved` の `conds.len() == 1`)、 memcpy 律速 (0.29 ns/hit)
- **cond=2..4**: bitmap_and 経路。 cond 追加で word AND コスト +30 µs くらい乗る (理論値 7.5 µs より大きい — メモリアクセスがキャッシュにフィットしない)。 結果サイズが減っても extract コスト節約で大きく相殺できない
- **cond=5..6**: 結果がほぼ 0 hits、 extract が誤差、 base bitmap AND だけ残って ~150 µs に落ちる
- **cond=7**: g (1000 値域) は schema が max_values=0 で define_himo するので bitmap 非生成 →
  `all_bitmap` 判定が false、 **column_filter 経路に降りて** pivot (~1000 hits) × 6 cond で 4 µs。
  別アルゴリズムなので比較対象外

**「条件追加で常に速くなる」 は錯覚**: bitmap_and では cond 追加に対して word AND コストが線形に
乗る (M2 Max では実測 ~30 µs/cond)。 結果サイズ減による extract 節約は ~4 ns/hit なので、
**結果が 7500+ hits 減らないと cond 追加は net で遅くなる**。 RDB 的に「絞り込めば速い」 の
直感とは違う。

将来の改善余地:
- bitmap word AND の SIMD 化 (AVX-512 / NEON) で 4-8x 速くなる → 谷曲線が浅くなる
- bit extract の bulk extraction (一度に 64 bit popcount + scan)
- 大値域 himo (1000+) でも bitmap を許容するオプション (今は column_filter 経由)

### 4. criterion regression suite

`benches/core.rs`。 主要 op の **退行検出** が目的、 数字そのものではなく
ΔTime% に注目する。

```bash
# 初回 (baseline 記録)
cargo bench --bench core -- --save-baseline main

# 変更後 (比較)
cargo bench --bench core -- --baseline main
```

criterion が ±10% 以上の劣化を自動で flag する。 CI に組み込む用。

---

## その他のベンチ (`examples/`)

v10 (0.26.0) で DB は **directory** になった。 example の 「前回の残骸を掃除」 と 「disk 使用量」 は
`enchudb::db_files::remove_db` / `disk_usage` (apparent / physical 両方) を使う。 単一 file 前提の
`remove_file` / `metadata(path).len()` は書かないこと。

| ファイル | 用途 |
|---|---|
| `bench_compare` (bin, `cargo run --release -p enchudb-engine --bin bench_compare`) | 1M entity の実用ベンチ (bulk write / rebuild / query / get) |
| `v10_lifecycle_bench.rs` | **v10 必須**: create / define_himo / 順次 write (新 page) / reopen / snapshot / disk 使用量。 criterion の同一 cell tie では見えない grow・page fault のコストを拾う |
| `batch_read_under_rebuild.rs` | double-buffer は concurrent rebuild 下で reader を守るか |
| `bridge_scaling.rs` | oplog → `_sync_ops` bridge の scaling |
| `dump.rs` | DB 内容ダンプツール (markdown / json) |
| `group_sum_cap_probe.rs` | schema 層 (cardinality 0) の group_sum probe |
| `growable_rss_repro.rs` | create_growable の起動 RSS / VSZ / teardown |
| `lockfree_bucket_probe.rs` / `lockfree_engine_bench.rs` | #95 lock-free append bucket の PoC と出荷経路の実測 |
| `multi_cond_scaling.rs` | 多条件 AND の谷カーブ |
| `open_profile.rs` | open 経路の page reclaim を step 別に分解 |
| `par_scan_bench.rs` | bulk column scan の seq vs par |
| `reopen_eager_rebuild_bench.rs` | open 時の eager cylinder rebuild cost |
| `sync_centralized.rs` / `sync_local_first.rs` / `sync_per_user.rs` | 0.7.0 sync pattern A / B / C の demo bench |
| `verify_tax_probe.rs` | lazy-verify の read tax |
| `vs_db.rs` | schema 層 EnchuDB vs SQLite vs DuckDB vs LMDB 4-way |
| `workload_rss_1m.rs` / `workload_segmented_rss.rs` / `workload_sparse_rss.rs` | RSS / VSZ / disk 使用量のモデル検証 |
| `write_ceiling_bench.rs` | single-consumer write ceiling |
| `crates/enchudb-engine/examples/issue*_*.rs` | #88 / #92 / #116 / #127 の footprint 再現 harness |
| `crates/enchudb-engine/examples/local_ns_bench.rs` | ns 級操作の分離計測 |
| `crates/enchudb-schema/examples/schema_overhead_bench.rs` / `scope_demo.rs` | schema 層 / Scope の overhead |

各ファイルの先頭コメントに目的・走り方が書いてある。

---|---|
| `agentic_workload_bench.rs` | LLM agent 風の高頻度 read/write mix |
| `column_read_bench.rs` | Column 直読みパスのみ |
| `dump.rs` | DB 内容ダンプツール |
| `growable_rss_repro.rs` | growable map の RSS bug 再現 (issue tracking) |
| `open_profile.rs` | open のプロファイル |

各ファイルの先頭コメントに目的・走り方が書いてある。

---

## カバレッジの穴 (未測定)

現状ベンチが**ない**領域:

- **WAL throughput** (sync / async / fsync 込み):
  かつての `v28_wal_bench.rs` 系は internal version 番号付きで撤去済み。
  必要なら `examples/wal_throughput.rs` を新規で。
- **concurrent writer scaling**: writer 1 + reader N、 writer N (排他で 1 のみ可能だが切替コスト)
- **`enchudb-sync` Syncer throughput**: publish_since / pull_since
- **`SubscriptionFilter`** (0.2.8 新規): per-peer publish のフィルタコスト
- **`enchudb-transport`** HTTP relay / WS push のスループット
- **`enchudb-rag` hybrid (BM25 + vector)**: `crates/enchudb-rag/examples/hybrid.rs` は demo のみ、bench 化されてない
- **HNSW 等 ANN との RAG 比較**: enchudb-rag が brute force で十分強い領域はどこか定量化したい

---

## 数字を扱う上での注意

- **`--release` 必須**。 debug build は別世界。
- **mmap warm-up** で初回 op はキャッシュ未ヒット、 ファイル全体を touch してから測ること。
- **thermal**: M シリーズ MacBook はノート筐体だと長時間負荷で thermal throttle が入る。
  Mac mini / 据え置きと比べて 10-20% 遅くなる場合がある。
- **bench 同士の比較**: ある条件で速くても別条件で遅いことはよくある。 cylinder 設計上、
  **条件が増えるほど絞り込みが効いて速くなる** (典型的な RDB と挙動が逆) ので、
  「単条件で N x」 と 「3 条件で N×× x」 が同じシステムで両立する。

## 公平に書いておくこと

- **EnchuDB が常に勝つ訳ではない**。 BTree-friendly な range scan (`WHERE id > 100 AND id < 200`)
  で sorted leaf を読む SQLite は強い。 cylinder は等値 AND が桁違いに速い反面、 範囲は
  pull_range で min..=max を線形走査するので幅が広いと負ける。
- **持続性のセマンティクス**: SQLite は ACID をデフォルトで提供、 EnchuDB の async モード
  は durability を捨ててる。 `wal_sync()` を毎回呼べば SQLite と同等の durability になるが
  その分遅くなるので、 比較するなら durability mode を揃えること。
- **RAG の速さは cosine FLOPs 律速**: enchudb の ns lookup は RAG では見えない。
  「個人スケールで sub-ms RAG が brute force で出る」という主張は naive baseline でも同じく成立する。
  enchudb-rag の優位は速さじゃなく **統合性 (メタフィルタ + BM25 + vector + sync が同じ DB primitive 上)**。
