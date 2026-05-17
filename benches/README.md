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

### 1. vs SQLite (1M entities) — schema 層

組込 DB として SQLite と直接比較。 `examples/vs_sqlite.rs`。
**schema 層** (`Database::create` + `table.where_eq` 等) で計測 — 公開 README が推奨するパス。
aggregates (SUM / MIN / MAX / GROUP BY) は schema 層に未提供なので `db.engine()` に降りる
(「declarative で書きつつ hot loop だけ engine 直叩き」の典型例)。

```bash
cargo run --release --example vs_sqlite
```

実測 (1,000,000 entities、 4 列: dept / status / salary / age、 全列に index):

| クエリ | hits | EnchuDB (schema) | SQLite | 倍率 | per hit |
|---|---:|---:|---:|---:|---:|
| 1 条件 (dept=3) | 50K | 13.9 µs | 2.43 ms | **175x** | 0.28 ns |
| 2 条件 (dept=0 AND status=1) | 50K | 237.9 µs | 53.18 ms | **224x** | 4.8 ns |
| 3 条件 (dept=0 AND status=1 AND age=20) | 10K | 89.0 µs | 10.52 ms | **118x** | 8.9 ns |
| 範囲 (age 30..40) | 220K | 12.06 ms | 12.34 ms | 1x | 55 ns |
| COUNT (status=2) | 200K | 53.1 µs | 4.74 ms | **89x** | — |
| SUM salary (dept=3) | 50K | 88.2 µs | 24.24 ms | **275x** | 1.8 ns |
| SUM salary (全件) | 1M | 2.38 ms | 32.29 ms | **14x** | 2.4 ns |
| GROUP BY dept SUM salary (全件 → 20 groups) | 1M scan | 13.81 ms | 397.44 ms | **29x** | 14 ns |
| MIN/MAX salary (dept=5) | 50K | 188.9 µs | 20.00 ms | **106x** | 3.8 ns |

`per hit` は **結果サイズに対する latency**。 単純 `find` は 0.28 ns/hit
(メモリ帯域に張り付いた memcpy 速度) — 「結果返却は memcpy 律速」の実証。 cylinder 交差や
集計が入ると per-hit cost が増える。

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

| ファイル | 用途 |
|---|---|
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
