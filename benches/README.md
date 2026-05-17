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

| クエリ | EnchuDB (schema, M2 Max) | SQLite (M2 Max) | 倍率 |
|---|---:|---:|---:|
| 1 条件 (dept=3) | 25.3 µs | 4.39 ms | **174x** |
| 2 条件 (dept=0 AND status=1) | 453.6 µs | 90.30 ms | **199x** |
| 3 条件 (dept=0 AND status=1 AND age=20) | 210.9 µs | 21.19 ms | **100x** |
| 範囲 (age 30..40) | 18.87 ms | 22.38 ms | 1.2x |
| COUNT (status=2) | 88.1 µs | 7.03 ms | **80x** |
| SUM salary (dept=3) | 145.4 µs | 27.33 ms | **188x** |
| SUM salary (全件) | 3.62 ms | 53.72 ms | 15x |
| GROUP BY dept SUM salary (全件) | 27.73 ms | 637.81 ms | **23x** |
| MIN/MAX salary (dept=5) | 273.2 µs | 26.70 ms | **98x** |

挙動メモ:
- **条件 AND は絞り込みが進むほど速い** (cylinder 交差): 3 条件は 2 条件より速い。 RDB と逆。
- **範囲 (BETWEEN) は SQLite と互角**: cylinder は等値 AND が得意、 連続 range は pull_range
  で min..=max を線形走査するので幅が広いと負ける。 README で公平に明記。
- **GROUP BY や全件 SUM は差が縮む**: 全件走査 dominant なワークロードでは差が 10-20x。
- **`insert 1M`**: enchudb 1083ms / SQLite 10709ms (約 10x、 cylinder の incremental insert が
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

### 3. criterion regression suite

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
