# EnchuDB Changelog

紐ベース円柱エンジンの進化記録。 詳細設計は各 `*_PLAN.md` を、 実装は git log を見ること。 このファイルは「いつ何が起きたか」 の **時系列インデックス**。

## 形式について

- 新しいエントリほど上。
- `[FILE_VERSION X]` は on-disk フォーマットのメジャー番号 (engine.rs の `FILE_VERSION` 定数)。
- `[v##]` (v26 / v27 / v32 / v33 ...) は **feature flag** — Cargo の `--features v##` で opt-in する機能セット。 上位互換 (v33 を有効にすれば v32 機能も入る)。
- これら 2 軸は独立。 同じ FILE_VERSION 内で feature flag を増やせるし、 feature flag を変えずに format を bump することもある。

---

## [未リリース / 将来計画]

### v100 stability cutoff (未実装、 検討中)

format 安定の宣言ポイントとして `FILE_VERSION = 100` への jump を計画。 詳細は本ファイル末尾の「Stability Lock-In Plan」 を参照。 ロックイン後は:

- on-disk layout offset table (header に各 region の offset を直書き)
- major.minor split された version (minor は additive、 major は migrate 必須)
- `Engine::migrate(src, dst)` を engine 内蔵 (Phase C 相当)
- SemVer 開始 + golden-file regression tests

---

## [FILE_VERSION 3] — Phase B (Layout reorg + lazy init)

**日付**: 2026-05-09
**commits**: `28de052` → `0a15862` → `559e407` → `3030e50`
**詳細計画**: [GROWABLE_MMAP_PLAN.md](./GROWABLE_MMAP_PLAN.md)
**破壊的変更**: あり (FILE_VERSION 2 → 3、 v2 DB は recreate / migrate 必要)

### 動機
組み込み DB として致命的だった 「fresh DB が 88 GB sparse」 問題を architectural に解決。 backup tools (Time Machine 等) や trunk dist のような apparent-size 依存パイプラインで詰まる症状を解消する。

### 実測効果
| 経路 | apparent size before | apparent size after |
|---|---|---|
| `create_growable_tiny` (no use) | 5,259,264 bytes (5.26 MB) | 569,344 bytes (0.54 MB) |
| matcha 実 use case | 5,259,264 bytes (5.26 MB) | 765,952 bytes (0.73 MB) |

**85% 縮小**、 query 性能は ±2% 以内のノイズ範囲、 tiny preset の query は EntitySet free_stack 縮小の二次効果で 39% 高速化。

### Step 1 — Layout v3 reorg (`0a15862`)
- `Layout::from_params_with_undo` の region 配置順を変更:
  - 旧 v2: `[header][固定 + variable 散在]`
  - 新 v3: `[header][固定 cluster][variable cluster (tail)]`
- variable region (`vocab_data` / `himoreg_data` / `content_data`) を末尾に集約
- `FILE_VERSION` 2 → 3 hard break

### Step 2+3 — Variable lazy init + initial_commit 縮小 (`559e407`)
- `Vocabulary::init` の data 領域 magic 書きを `insert` 初回まで遅延
- `Vocabulary::load` に "MAGIC missing → fresh" 検出経路追加
- `ContentStore::init` の data 領域 magic 書きを `set` 初回まで遅延 (CAS で data_end を 0 → DATA_HEADER に bump)
- `create_growable_full::initial_commit` を `layout.total_size` から `layout.vocab_data_off` に短縮

### Step 4 — tiny preset MB-scale 化 (`3030e50`)
- **EntitySet free_stack を `min(FREE_STACK_MAX, max_entities)` で cap** — tiny preset で 4 MB → 4 KB の最大削減
- tiny preset の `vocab_max_entries` multiplier を default ×16 から ×2 に絞る (新 `Layout::compute_with_caps` 経由)
- `ensure_himo` を `with_grower` 経由に変更し、 himo_slots の lazy commit を有効化
- `grow_amortized` の doubling 戦略を hybrid 化 (cur < 1MB は doubling、 ≥ 1MB は 1MB chunks linear)
- `validate_file_size` を sparse extend に変更 — growable DB を `open_standalone` で開く際に file が部分書き込みでも自動拡張

### 関連: wasm32 対応 (`28de052`)
- `growable_map` モジュールを `#[cfg(not(target_arch = "wasm32"))]` で gate (POSIX mmap 依存のため wasm 移植不可)
- `Region` の grower field / `with_grower` ctor / `ensure_committed` body を同様に gate
- 真の wasm backing (`Backing::WasmHeap`) は別タスク

### Tests
115/115 unit test pass。 `growable_then_open_standalone` test は validate_file_size の sparse extend 対応で引き続き通る。

---

## [SQL frontend / FFI / DSL mutate] — 2026-05-09

**commits**: `848c0c5` (大物) + `90dc0c1` (拡張)

- `enchudb-sql` crate 新設 — sqlparser SQLite dialect で `SELECT` / `INSERT` / `UPDATE` / `DELETE` を engine native API に dispatch
  - ORDER BY / LIMIT / range WHERE (`<`, `<=`, `>`, `>=`, `BETWEEN`) / IS NULL / IS NOT NULL もカバー
- `enchudb-ffi` crate 新設 — C ABI for cross-language access
- `query_lang` (DSL) に in-place mutate operators (`+`, `~`, `-`) 追加
- 三段アーキテクチャ確立:
  - 内部 Rust (matcha 等) → `enchudb-engine` 直
  - 人間が書くアドホック query → `enchudb-sql`
  - 他言語連携 → `enchudb-ffi`

---

## [Phase A growable mmap backing] — 2026-05-09

**commit**: `d9962b9`
**詳細**: [GROWABLE_MMAP_PLAN.md](./GROWABLE_MMAP_PLAN.md) Phase A section

- `crates/enchudb-engine/src/growable_map.rs` 新設 — POSIX `mmap(MAP_FIXED)` で仮想予約 + on-demand commit primitive
- `Engine::Backing::Growable(Arc<GrowableMap>)` バリアント追加
- `Region::with_grower` 新設 — file offset と grower への back-reference を持つ region
- `Vocabulary::insert` / `ContentStore::set` の append 境界に `ensure_committed` plumbing 挿入
- macOS 仕様で whole-file remap pattern 採用 (隣接 slice の MAP_FIXED は EINVAL)
- 6 unit test 追加

**Phase A の制約**: `initial_commit = layout.total_size` のため apparent size は変わらず (Phase B で解決)。

---

## [enchu-extend 分離] — 2026-05-03

**commit**: `3aecaf7`

Node binding (`enchu-extend`) を `mutafika/` 配下の sibling repo に切り出し。 enchudb 本体は組み込み DB に専念し、 言語バインディング層は別 lifecycle で進化する方針。

---

## [HTTP transport refinements] — 2026-04-26 〜 27

**commits**: `d7c2a1a`, `5868cd6`, `ec461b5`

- HTTP relay/transport を room-scoped path (`/room/<id>/...`) に拡張
- `HttpTransport` に `extra_headers` サポート追加 (認証ヘッダ等)
- consumer thread の busy-spin を condvar に修正

---

## [Crate split + ContentStore bug fix] — 2026-04-24

**commits**: `538b943` (Step 1/2) + `bbc960f` (Step 2/2) + `c9d1819` (ShardRouter) + `16136ee` (bug fix)

### Crate 分割
monolithic `enchudb` crate を:
- `enchudb-engine` (core engine) — Step 1
- `enchudb-sync` (peer sync layer) — Step 2

物理的に切り出し。 各 crate が独立して develop / version-bump できる。

### ShardRouter
跨 peer query routing を `enchudb-sync` に追加。

### ContentStore cross-process bug 修正
`data_end` を `mmap` 上の `AtomicU32` に移動 — 別プロセスが同じ DB を開いたときの content 上書きバグを解消。

---

## [v33 peer sync via WAL] — 2026-04-23

**commits**: `f2a9e20` → `332826f` → `e18fd5f` → `84772ca` → `850df56`
**詳細**: (V33_PLAN.md は未作成、 v33 commits の Step 1-5 を参照)

- `Engine::open_standalone` / `create_standalone` 系 API を v33 feature flag で分離 (旧 `open` / `create` は WAL 付き `Arc<Self>` 返しに統合)
- `WalOp::Vocab` 追加 — peer 間 text 運送の基盤 (Step 2)
- `tie_text_async` 実装 — peer 間 text sync E2E 成立 (Step 3)
- `tie_ref_async` + `commit() / wal_commit()` 統合 (Step 4+5)
- `Syncer::new` で WAL 無し Engine が panic、 silent 0 件配送を loud 化 (`decc781`)

---

## [v32 distributed + HLC + signing + Blob] — 2026-04-22

**commits**: 約 20 commit、 主なもの: `2734bc6`, `9ce3113`, `7fb7b2d`, `c1f9ba5`, `d234abd`, `a38f798`, `7c44fd5`
**詳細**: [V32_PLAN.md](./V32_PLAN.md)

### Phase D — BlobStore
- `BlobStore` trait + `LocalBlobStore` 実装
- `blob_bench` — 大 blob 500 MB/s、 dedup 5µs

### Phase E/F — snapshot / audit / stats
- `snapshot_export` — signed WAL の全経路 E2E 検証
- `audit` — WAL commit 済みレコードの走査
- `stats` 拡張

### transport 分離
- http/ws 実装を `enchu-transport` crate に切り出し
- feature 整理 — `ws` を `v32` に吸収、 `chaos`/`crdt` を `tests/common/` に退避

### 安定性強化
- `v32_byzantine` flaky 修正 — WAL `auto_reset` 既定 off、 publish 前に消える race 解消
- v32 crash/recovery E2E — 署名付き WAL 下の SIGKILL 復旧検証
- API 網羅 audit — 漏れてた `pub fn` に最小 smoke test 追加
- 性能 regression benches — criterion で主要 op の baseline 計測 (`benches/core.rs`)
- doctest 実行可能化 — lib/acl/blob_store/sync の rustdoc 例が compile/run

### ChangeListener (v32 changefeed)
- WAL durable record の自動 push API (`fb21433`)
- `Syncer::apply_records` を pub に (`bab985b`)
- CLAUDE.md に v32 changefeed セクション追加

---

## [分散系探索] — 2026-04-21

**commits**: `db50a2f`, `9f4b8a1`, `d998add`, `def7bef`, `77a9666`

- WebSocket push / Byzantine / CRDT / Chaos sim の 4 モジュール (探索)
- `chaos_sim` — crash / 一方向 partition / pair delay / Byzantine 注入
- `dist_dashboard` — 伝播の可視化強化

---

## [v27 concurrency + n-tuple views] — 2026-04-15

**commits**: `1234078`, `46105d0`, `2f0227e`, `129d86b`, `74e4945`, `6bc29a9`
**詳細**: (V27_PLAN.md は未作成)

- **n-tuple 観測窓** — 複合クエリ最大 33x 高速化 (`1234078`)
- **BucketCylinder** — per-value bucket、 ソート/rebuild 不要 (`46105d0`)
- **並行化** — writer queue + async tie + reader 並行 (`2f0227e`)
- **観測窓の永続化** — `define_view` をスキーマ API に昇格 (`129d86b`)
- **テスト拡充** — 実運用/並行/エッジ/プロパティの 4 軸で 52 件追加 (`74e4945`)
- `flush_writes` race 修正 — push/apply カウンタで同期 (`6bc29a9`)

---

## [v26 pair table + delta sync] — 2026-04-14

**commits**: `0283872`, `af736fb`, `272b87d`

- **ペアテーブル + HashMap 削除** — 多条件クエリ最大 4395 倍速 (`0283872`)
- **CAS基盤 + デルタシンク** — 差分更新 225 ns/件、 リアルタイム同期対応 (`af736fb`)
- v26 DeltaCell 廃止 — ペアテーブルを `Vec<u32>` 直接操作に統一 (`272b87d`)

---

## [v28 / v29 plan documents]

**docs**: [V28_PLAN.md](./V28_PLAN.md), [V29_PLAN.md](./V29_PLAN.md)

V28 = WAL + crash consistency、 V29 = 耐久性強化。 plan docs は残ってるが、 commit log との対応は v32 commits に吸収されてる (v32 が WAL/durability 込みでまとめて landing したため、 v28/v29 が独立 commit として現れない可能性)。

---

## [基盤期] — 2026-04-04 〜 2026-04-09

### v0.1.0 birth (`8f22dee`, 2026-04-04)
紐ベース円柱エンジン。 prefix sum O(1)。 全 mmap。

### Query 最適化 (2026-04-06)
- `cd88fe8`: 3 条件目以降を Column 直読みフィルタに変更
- `38cbb55`: galloping intersect 廃止、 Column 直読みフィルタに統一
- `0ed282a`: bitmap AND 追加 (非選択的クエリの高速化)
- `de9e338`: 単一ファイル化 — 全コンポーネントを 1 つの mmap に統合

### Entity lifecycle + lockfree rebuild (2026-04-07)
- `409229b`: entity allocate 欠番方式 — free stack TOCTOU 競合修正
- `756ecf5`: entity lifecycle を undo log に記録 — rollback/crash 復旧の完全化
- `d3636c0`: ロックフリー並行 rebuild — `compare_exchange` で排他、 reader 止まらず
- `c7ae509`: rebuild を `&mut self` に変更、 query から rebuild を分離
- `4870dad`: fxhash 衝突バグ + free_stack アライメント SIGBUS 修正
- `fae5e0a`: content_store data 領域 64MB → 512MB に拡大

### `tie_to` API + メンタルモデル (2026-04-08)
- `c75788d`: `tie_to` / `tie_text_to` / `tie_ref_to` 追加 — `&self` で書き込み可能
- `bf71547`: メンタルモデル修正 — entity と紐の交差点に値をぶら下げる
- `d774897`: `Column.count` を `AtomicU32` 化 — 並列 write 時の count 上書き競合修正
- `00c6450`: bitmap AND: `vocab_id > max_values` で 0 件返すバグ修正
- `4356da2`: vocab `get_or_insert` 並列競合修正 — 負けたスレッドは勝者の id を使う
- `fc68805`: `Vocabulary::load` でハッシュインデックス再構築

### vocab header write-through + create_compact (2026-04-09)
- `ada06b4`: vocab insert 時に count/data_end を mmap header に即書き戻し
- `0324799`: `get_entity` 追加 — 1 回で全フィールド取得、 O(himo 数)
- `10d3475`: `create_compact` 追加 + `cyl_max_values` パラメータ化
- `372d047`: `find_value` 追加

---

## Stability Lock-In Plan (将来案、 未実装)

### 動機
v3 layout はロックインに適した natural cutoff。 これ以降は構造変更を避け、 拡張は all-additive で済むようにすると、 consumer 側の format break 対応コストが激減する。

### 計画
1. **`FILE_VERSION = 100`** への jump — visual marker として 「ここから SemVer」
2. **on-disk layout offset table** — header に各 region の offset を直書き (今は runtime 計算)。 これで future reorg を non-breaking 化
3. **`H_VERSION_MAJOR / H_VERSION_MINOR` split** — minor 追加は backward-compat
4. **`Engine::migrate` 内蔵** — `open` 時に古い fmt を検出 → 透過 migrate (Phase C 相当を engine 内へ)
5. **SPEC.md の format section を契約化** — region byte layout を逐一明文化
6. **golden-file regression tests** — `tests/fixtures/v100.enchu` を commit、 layout の意図せぬ変更を検出
7. **CHANGELOG.md (this file)** — SemVer 開始後はリリースごとに entry 追加

### 実施タイミング
未定。 「破壊変更を許容できる残り 1 回分」 を意図的にこの lock-in に使う。 wasm backing (Backing::WasmHeap) や `H_VERSION_MAJOR.MINOR` 導入と同時に landing するのが効率的。

---

## 参考リンク

- [README.md](./README.md) — 使い方の入口
- [SPEC.md](./SPEC.md) — 機能の網羅 (単一情報源)
- [CLAUDE.md](./CLAUDE.md) — 設計判断と philosophy
- [BUGS.md](./BUGS.md) — 既知バグの修正履歴
- [issue.md](./issue.md) — 未解決課題
- [GROWABLE_MMAP_PLAN.md](./GROWABLE_MMAP_PLAN.md) — Phase A/B 詳細
- [V28_PLAN.md](./V28_PLAN.md) — WAL + crash consistency
- [V29_PLAN.md](./V29_PLAN.md) — 耐久性強化
- [V32_PLAN.md](./V32_PLAN.md) — 分散化 (HLC + 署名 + Sync + BlobStore)
- [TEST_DESIGN.md](./TEST_DESIGN.md) — DB としての網羅テスト設計
