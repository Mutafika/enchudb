# Changelog

EnchuDB の主要 release ごとの変更を時系列で記録。 0.x 段階につき **semver 厳密
ではない**が、 patch (z) は非 breaking、 minor (y) は API/format 変更を含む方針。

## 0.7.0 — 2026-05-22

mini-RDB semantics の **actually 確立** ([issue #11](https://github.com/Mutafika/enchudb/issues/11) +
[issue #15](https://github.com/Mutafika/enchudb/issues/15))。 0.5.0 で engine に
追加した table API を `enchudb-schema` crate / consumer 層が **1 度も使ってなかった**
(= 死荷物)、 同時に `enchudb-oplog` が 「local 耐久 log」 と 「sync 配信 stream」 を
兼任していた構造問題を一括解消。 計画書: `notes/requests/request7.md`。

### Breaking

- **schema crate の hot path が engine table API 経由に**: 新規 `Database::table().build()`
  は `define_table` + `define_himo_in` + `define_ref_in` を engine に発火、
  `RowBuilder::commit` は `entity_in(table_name)` で eid_range 内払出。 既存 v5/v6 DB
  は透過 open + lazy migrate (= 過去 anonymous entity は eid 不変で読める、 新規 row は
  table 内 eid_range から払出)
- **`TableDef.next_local` が `AtomicU32` 化、 `Engine::entity_in` が `&self` 化**:
  schema crate / Arc<Engine> 経由の concurrent mode から row insert で CAS-safe 払出
- **`define_table("_*")` を reject**: `_` 始まり名前は reserved namespace、 user 経路は
  `String` Err を返す
- **既存 `Engine::list_tables()` は reserved table も含む**: user code は 0.7.0 から
  `list_user_tables()` を使うべき (= reserved を除外)
- **semver**: 0.6.0 → 0.7.0

### Added

- **`TableBuilder::with_capacity(n)`**: 1 table に大量 row (= 1M+) を入れる workload で
  eid 空間を明示確保。 省略時の default は `remaining / 4` で 4 table 分残す妥協値
  (= multi-table workload 向け)。 1M entity を 1 table に入れる bench 系で
  `entity_in() failed: eid range exhausted` を防ぐため必須
- **`_sync_ops` / `_sync_peers` reserved table** ([issue #11](https://github.com/Mutafika/enchudb/issues/11)):
  - `Engine::enable_sync_tables()` / `Database::enable_sync()`: opt-in で sync 経路の
    reserved table を auto-define (= sync 不要な単独 DB は eid 空間も浪費しない)
  - `Engine::transfer_oplog_to_sync_ops()`: oplog の commit 済み record を `_sync_ops`
    table へ転送 (consumer thread から定期実行する想定)
  - `Engine::ack_sync(peer, lsn)`: peer の watermark を `_sync_peers` に upsert
  - `Engine::sync_watermark()`: 全 peer min(consumed_lsn) (= reclaim 安全点)
  - `Engine::reclaim_sync_ops()`: `lsn < watermark` の row を lazy purge
    (0.7.0 では entity delete のみ、 eid 空間は再利用せず — 0.8.0 で ring buffer 化検討)
  - `Engine::pending_sync_ops(since_lsn)`: peer publish 用、 (since, current] の payload bytes
  - `Engine::current_sync_lsn()`: snapshot 取得時の 「ここまで配信済み」 マーカー
  - `Syncer::mark_initial_sync_complete(peer, lsn)`: snapshot 後の watermark 初期化
- **engine table API 拡張**:
  - `define_reserved_table(name, size_hint)`: `_` 始まり強制の internal table API
  - `list_user_tables()`: anonymous + reserved を除外する user 向け列挙
  - `has_reserved_table(name)`: 状態判定
  - `vocab_intern_text(text)`: entity 経路を一切触らずに vocab inject (= schema crate
    の `intern_table_name` で dummy entity → delete の roundtrip を排除)
  - `remaining_eid_space()` / `max_entities()`: schema crate が `define_table` size_hint
    を auto-clamp する用
  - `is_readonly()`: panic せず bool で返す getter
  - `tie_bytes_to_by_id(eid, himo_id, &[u8])`: Leaf himo に任意 binary を tie
    (= UTF-8 制約のない wire bytes 用、 `_sync_ops.payload` で使用)
- **`snapshot_export` が `.tables` sidecar も含める**: receiver で table 構造 +
  reserved table を復元可能
- **3 deployment pattern reference example** ([issue #11](https://github.com/Mutafika/enchudb/issues/11)):
  `examples/sync_centralized.rs` (中央集権) / `sync_per_user.rs` (per-user DB) /
  `sync_local_first.rs` (privacy-first + blob offload)

### Changed

- **0.5.0 / 0.6.0 CHANGELOG 文言の訂正**: 「mini-RDB semantics の確立」 →
  「engine 基盤の確立」 (= consumer 層への配線は 0.7.0 で完成、 という事実を反映)
- **`tie_to_by_id` / `tie_text_to_by_id` / `tie_bytes_to_by_id` の reserved table skip**:
  `_*` table への write は oplog 再 append を skip (= `_sync_ops` への内部 mirror が
  oplog → `_sync_ops` → oplog の無限ループにならない設計)
- **`enable_sync_tables` の reserved table サイズを auto-clamp**: `remaining_eid_space`
  ベース、 tiny preset でも overflow しない

### Migration

[`docs/migration-0.6.0-to-0.7.0.md`](docs/migration-0.6.0-to-0.7.0.md) に既存 v6 DB の
透過 open / consumer code への影響 / sync 経路の opt-in 化手順あり。

API 不変 (= schema crate 公開 API は 0.6.0 から変わらない) なので、 schema crate
経由の consumer (opyula / bisquit / sinfo / matcha / t5ug3 / sinfohub-server 等) は
**再 build で済む**。 sync 経路を活用する consumer は `Database::enable_sync()` を
build phase で呼ぶ opt-in 切替で `_sync_ops` table 機構の恩恵を受けられる。

### Unchanged

- wire record format (v2 layout) 不変
- file magic `EWAL` 不変 (= 0.6.0 と binary-compat)
- HLC / EntityId / PeerId / keys / 署名 layout 不変
- sync 経路 (publish_since / pull_since) の wire protocol 不変 (oplog 経路で並走)

## 0.6.0 — 2026-05-20

`enchudb-wal` crate を `enchudb-oplog` にリネーム ([issue #8](https://github.com/Mutafika/enchudb/issues/8))。
実態が write-ahead log ではなく oplog (MongoDB oplog と同パターン: mmap が primary state、
oplog は peer sync 配信 + audit + crash recovery 用の append-only op stream) なので、
命名と実装の乖離を解消。 wire format / record encoding / file magic は不変、
file 拡張子と API 名のみ変更。

### Breaking

- **crate rename**: `enchudb-wal` → `enchudb-oplog` (`Cargo.toml` の dep 名 + import 全置換)
- **API rename** (主要):
  - `Wal` → `OpLog`、 `WalOp` → `Op`、 `WalRecord` → `OwnedOp`、 `RecoveredRecord` → `Record`
  - `wal_sync()` → `oplog_sync()`、 `wal_commit()` → `oplog_commit()`、 `wal()` → `oplog()`、 `wal_arc()` → `oplog_arc()`
  - `create_concurrent_with_wal*` / `open_concurrent_with_wal*` / `concurrentize_with_wal` → `*_with_oplog*`
  - schema 層 `Database::open_with_wal` / `finish_with_wal` → `open_with_oplog` / `finish_with_oplog`
  - 公開フィールド `Stats { wal_head, wal_checkpoint, ... }` → `oplog_head, oplog_checkpoint, ...`
  - 詳細マッピングは [`docs/migration-wal-to-oplog.md`](docs/migration-wal-to-oplog.md)
- **file 拡張子**: `{db_path}.wal` → `{db_path}.oplog`、 fallback open なし (clean break)
  - 既存 `.wal` ファイルが居る場合は手動 rename (`mv x.wal x.oplog`) で OK、 中身 binary は不変
- **semver**: 0.5.0 → 0.6.0

### Unchanged

- wire record format (v2 layout) 不変
- file magic `EWAL` (歴史的経緯で binary-compat、 0.5.0 で書いた `.wal` を `.oplog` に rename すればそのまま読める)
- HLC / EntityId / PeerId / keys / 署名 layout 不変
- sync 経路 / publish_since / pull_since 等の wire protocol 不変

### Migration

[`docs/migration-wal-to-oplog.md`](docs/migration-wal-to-oplog.md) に consumer crate
向けの完全 import 置換 sed + ファイル拡張子 rename 手順あり。 enchudb meta crate の
re-export (`enchudb::{EntityId, Hlc, PeerId}` / `enchudb::keys::*`) は path 不変なので、
これらだけ使う consumer は import 修正不要。

## 0.5.0 — 2026-05-20

β-light: engine 自身が **table 概念** を持つ。 旧 flat な eid 空間 + himo 群の上に、
名前付き table の `eid_range`、 table-namespaced himo、 FK validation を engine 直下に降ろした。
mini-RDB semantics の **engine 基盤**。 consumer layer (`enchudb-schema` / SQL / FFI /
RAG) への配線は 0.7.0 で完成 ([request7](https://github.com/Mutafika/enchudb/issues/15))。

### Breaking

- **file format v4 → v5**: header の version 値が 5 に上がる。
  - v4 DB は **透過 open** (= 旧 DB は何もせずそのまま使える、 anonymous-only として扱う)。
  - 0.5.0 で作った v5 DB を 0.4.x で open はできない (= `unsupported file version` で reject)。
  - ダウングレードしたい場合は `snapshot_export` → 0.4.x で recreate の手動 flow。
- **`entity()` が `define_table` 呼出後に panic**: anonymous table を 1 度 close した DB
  では、 旧 API `entity()` は使えなくなる (= 新 API `entity_in(table)` に統一が必要)。
  define_table を呼ばないコードは引き続き旧 API で動く。

### Added

- **engine が table を認知** (新 API):
  - `define_table(name, size_hint)`: named table を作る (`size_hint` 個分の eid range を予約、 0 で 1M default)。
  - `entity_in(table)`: 指定 table の eid_range 内に entity を allocate。
  - `define_himo_in(table, himo, ht, mv)`: table-namespaced himo (`"users.age"` のような `{table}.{himo}` 命名)。
  - `define_ref_in(table, himo, target_table)`: `HimoType::Ref` の FK 宣言、 tie 時に target eid が target_table 内かを engine が validate。
  - `list_tables()`: 全 table メタデータ列挙 (`Vec<(TableId, name, lo, hi)>`)。
- tables 定義の永続化: 新 sidecar file `{path}.tables` に binary encode (table 数 × ~64 byte)。
  - sidecar 不在 = anonymous-only (v4 DB 互換動作)。
- `EntitySet::allocate_at(eid)`: 任意位置 mark + CAS-safe next_eid 進行。 table-aware allocation の bottom-half。
- bench `scale_tables` group を core suite に追加 (= 10 table × 5 himo × 10k entity scale を `bench_scale` (anonymous flat) と A/B 比較する用)。

### Internal

- `TableDef { name, himo_ids, eid_range_lo/hi, fk_refs, next_local }`: 1 table 分のメタ。
- `Engine.tables: Vec<TableDef>` + `himo_to_table: Vec<TableId>`: index 0 が anonymous (open-ended)、 1+ が named table。 anonymous は `define_table` 初回呼出で `eid_range_hi = cur_next_eid` で close される。
- tie hot path に `validate_eid_for_himo` + `validate_ref_tie` を `#[inline(always)]` で追加。 table 数 ≤ 1 (= anonymous only) 時は 1 atomic load の fast path で抜ける。

### 性能影響

bench (criterion、 baseline = 0.4.x = master `51ee42e`):

| bench | 0.4.x | 0.5.0 | Δ |
|---|---|---|---|
| `tie/plain_value` | 18.6 ns | 18.6 ns | ±0% |
| `tie_async/wal_signed_off` | 52.6 ns | 46.4 ns | -12% |
| `pull_raw/single_value` | 82.5 ns | 82.7 ns | +0.2% |
| `query/two_cond_and` | 429.9 ns | 433 ns | +0.7% |
| `scale_*` 群 | baseline | ±2% 内 | -- |

hot path に新規分岐を入れない設計のため regression は noise floor 内。

### Migration: 0.4.x → 0.5.0

#### パターン A: そのまま動かす (推奨、 既存コード)

旧 API は anonymous table へ自動 dispatch される。 何も書き換えなくて OK、 v4 DB はそのまま open できる。

```rust
let mut eng = Engine::create_standalone("db")?;
eng.define_himo("age", HimoType::Number, 100);
let e = eng.entity();
eng.tie(e, "age", 30);
```

#### パターン B: table-aware に書き換え

```rust
// 旧 flat
eng.define_himo("user_age", HimoType::Number, 100);
let alice = eng.entity();
eng.tie(alice, "user_age", 30);

// 新 table-aware
eng.define_table("users", 100_000)?;
eng.define_himo_in("users", "age", HimoType::Number, 100)?;
let alice = eng.entity_in("users")?;
eng.tie(alice, "users.age", 30);
```

#### パターン C: FK 付き

```rust
eng.define_table("users", 10_000)?;
eng.define_table("posts", 100_000)?;
eng.define_himo_in("users", "name", HimoType::Tag, 1000)?;
eng.define_ref_in("posts", "author", "users")?;  // FK 宣言

let alice = eng.entity_in("users")?;
let post = eng.entity_in("posts")?;
eng.tie(post, "posts.author", alice as u32);  // alice が users 範囲外なら engine が panic
```

#### file 互換性まとめ

- v4 DB を 0.5.0 で open: **可** (透過 migrate、 anonymous-only として扱う)
- v5 DB を 0.4.x で open: **不可** (`unsupported file version`)
- WAL record format は不変 → 0.4.x peer と sync 可
- `EntityId = (peer:32, local:32)` bit layout 不変

#### sync 互換性

`Engine::audit` / `Wal` 経路は 0.4.x と完全互換。 dual-engine 運用 (= 0.4.x ↔ 0.5.0 peer 間 sync) は WAL format 不変なので可。 ただし 0.4.x 受信側は table 概念を持たないので、 0.5.0 peer が `entity_in("users")` で作った eid (= eid_range 内の値) を anonymous な eid として扱う (= flat な eid 空間としては整合)。

### Future work (0.6.0 候補、 branch `feat-engine-heavy` で実験中)

- **column file table 別分離** (`{path}.t.{name}.col`): drop_table O(unlink) 化
- **positions の mmap-back** (`{path}.positions`): 100M scale で RSS 削減見込み
- **EntityId bit layout 変更** (`peer:24, table_id:16, table_local:24`): sync 経路まで table 認知
- 全 3 phase 完走済み (branch f5d9be7) だが hot path に regression あり (tie +34%、 query +12%、 scale_tables_open +224%)、 master merge 未決。 詳細は [notes/requests/request6.md](notes/requests/request6.md)。

## 0.4.0 — 2026-05-18

### Breaking

- **file format v3 → v4 hard break**: `UndoLog` 撤去に伴い undo region を layout から削除。
  旧 v3 DB を open すると `unsupported EnchuDB file version 3 (expected 4)` で弾かれる。
  pre-1.0 / pre-public release window で許容、 自動 migration は未提供
  (旧 DB は recreate して `snapshot_export` 経由で持ち越し)。
- **standalone mode の crash semantics 変化**: 旧コードは flush 済み未 `commit()`
  の書き込みを open 時に undo replay で巻き戻していた。 v4 では undo log
  自体が無いので **standalone (WAL 無効) では crash 時に途中状態が残る**。
  巻き戻しが必要な caller は `Engine::create_concurrent_with_wal` 経由で WAL
  有効化を推奨 (Commit marker 未到達なら open 時の WAL recover で drop される)。
- **`Engine::rollback()` を削除**: workspace 内に caller ゼロ (engine 内テスト
  3 件 + `enchudb-rag` の pass-through wrapper 1 個のみ) のため breakage 実害なし。
  `enchudb-rag::RagStore::rollback` も同時撤去。
- **API 削除**: `create_full_with_cyl_undo` / `create_concurrent_with_wal_undo_cap`。
  `create_concurrent_with_wal_queue_cap` は `undo_max_entries` 引数を落とした形で残る。
- header offset `72..76` (旧 `H_UNDO_MAX_ENTRIES`) は予約済み (zero 保持) — 後続
  フィールドを追加するなら 80.. を使うこと。

### Removed

- `crates/enchudb-engine/src/undo.rs` を完全削除 (175 行)。
- 旧 test (`rollback_reverts` / `rollback_insert` / `crash_recovery_rollback` /
  `prefix_sum_rollback` / `tests/undo_overflow.rs`) を削除。 削除対象の保証は
  もう存在しない (WAL Commit marker による drop で代替)。

### Perf

- **`BucketCylinder::positions` に `eid_offset` を導入** (`cylinder_v27.rs`):
  - 旧: `positions: Vec<(u32, u32)>` を eid で直 index → 「最初に tie される eid が
    N」 の himo は 0..N の prefix が空気で確保される (= モデル違反)
  - 新: `positions[i]` が eid `(i + eid_offset)` を表す。 各 himo が自分の
    tied range だけ確保
  - bench (6 table × 23 himos × 6M entities, table-segmented insert):
    ```
    before: 861 MB RSS
    after : 336 MB RSS   (−61%)
    ```
  - 副次: undo region 削除と合わせて `snapshot_export` が 270 µs → 151 µs (−44%)

### Fixed

- **issue #1: `UndoLog::record` の backpressure spin が standalone mode で
  permanent hang する**。 consumer thread を持たない構成では `force_commit`
  signal を立てても誰も commit しないので無限 yield に入る。 undo log 自体を
  削除することで根本解消。 issue #1 の再現ケース (10M entity insert × 7 ties)
  が **72+ 分 hang → 2.13 秒で完走**。

### Internal

- `Op::EntityCreated` は v4 以降 no-op、 但し `flush_writes` の barrier counter
  (`push_count` / `apply_count`) と整合を取るために queue を通す経路は残す
  (issue5 対応の延長)。
- `examples/workload_rss_1m.rs` / `workload_segmented_rss.rs` / `workload_sparse_rss.rs`
  を追加 (RSS 計測 / モデル検証 repro)。
- regression test `tests/eid_offset_descending.rs` 追加 — 100k entity を eid
  降順 tie してもO(N²) realloc に落ちず query 結果が正しいことを確認。

## 0.3.0 — 2026-05-17

### Breaking

- **schema layer: row 識別 marker convention を完全廃止**。 「table = 紐の束
  declaration」 という enchudb 本来の世界観に揃え、 row への明示的 table 名
  marker tie / query 毎の marker cond を削除。 column 名は元から
  `{table}.{col}` で内部 prefix されてて他 table と衝突しないので、 marker は
  本質的に不要だった。
  - 公開 API `Database::marker_himo_id() -> u16` を削除
  - 公開 API `Table::table_vid() -> u32` を削除
  - 内部 const `TABLE_MARKER_HIMO` → `SCHEMA_META_HIMO` rename (schema blob
    persistence 専用、 row には触れない)
  - `RowBuilder::commit()` での marker tie 削除
  - `Query::find()` の eq_conds に marker push する経路削除
  - `Query::all()` (eq_conds 空ケース) は PK or first col の
    `entities_with_himo` で代用
  - storage 互換性: 既存 DB の `__enchu_table` himo は無視されるだけで害なし
    (= read 経路は marker を参照しない)。 新規 DB ではそもそも作られない

### Added

- **engine: `get_by_id(eid, hid)` / `entities_with_himo(hid)`** を新規 export
  (`enchudb-engine/src/engine.rs`)。 schema layer の bindings 経路 (= 起動時に
  himo_id を pre-resolve した hot path) で名前 lookup を完全に skip 可能
- **engine: `entities_with_himo`** は「ある himo に値が tie された全 entity」
  を column 走査 (`himo_store::entities_with_value`) で O(next_eid) で列挙。
  schema の `Query::all()` 経由で使う
- **examples 2 個追加**:
  - `enchudb-engine/examples/local_ns_bench.rs` — single get / 結果サイズ別
    query / list UI (1000 行 × 5 attr) を計測。 ns 級性能を可視化
  - `enchudb-engine/examples/battle_vs_duckdb.rs` — 1M rows / 5 query
    (point lookup / filter list / filter SUM / full SUM / GROUP BY SUM) で
    enchudb vs DuckDB vs sqlite3 を CLI 直接呼びで benchmark
  - `enchudb-schema/examples/schema_overhead_bench.rs` — 4 経路 (raw name /
    raw id / schema DSL / schema bindings) で schema layer が zero-cost か
    実測

### Perf

- **`group_*` aggregation を dense Vec / HashMap 化** (`engine.rs:2330+`):
  - 従来: `Vec<(u32, T)>::find` 線形探索 (100 group × 1M eid = 100M 比較)
  - 修正後: himo の `max_values` を見て dense path (Vec 直接 index, cap ≤ 64K) /
    sparse path (HashMap) で切替え。 dense 時は per-eid 2 命令
  - `group_sum / group_count / group_min / group_max / group_avg` 全部対象
  - bench (1M rows / 100 dept で GROUP BY SUM):
    ```
    before: 58.35 ms
    after :  2.38 ms   (24×)
    ```
  - これにより DuckDB (2.05 ms) と互角ラインまで詰まる
- **engine query_column_filter で全件相当 cond を always-true として skip**
  (`engine.rs:3033+`):
  - slice_lens を pre-compute して per-eid 呼ばないように外出し
  - `slice_lens[i] >= total` (= cond の slice が全 entity 数以上) なら
    必ず true なので filter から除外。 marker 廃止後の意味薄まったが
    汎用 cardinality 最適化として残す
- **schema layer の filter query が raw と完全同等** (zero-cost) に:
  ```
  bench (1M rows / 100 dept、 dept_id=42 で 10K filter):
                            before    after
    raw(id 経路)             4.7 μs    4.55 μs   (= 同等)
    schema (DSL)           46.3 μs    4.68 μs   (10×)
    schema (id 経路)        46.0 μs    4.61 μs   (10×)
  ```

### Benchmarks (1M rows / Apple Silicon, 全文 battle_vs_duckdb.rs より)

| Query | enchudb | DuckDB | sqlite3 | enchudb 倍率 (vs sqlite) |
|---|---|---|---|---|
| Q1 point lookup | 8.6 ns | 212 μs | 13.3 μs | **1547×** |
| Q2 filter list 10K | 3.7 μs | 5.1 ms | 15.6 ms | **4217×** |
| Q3 filter SUM | 18.1 μs | 2.6 ms | 14.7 ms | **810×** |
| Q4 full SUM 1M | 1.46 ms | 0.82 ms | 64.6 ms | **44×** |
| Q5 GROUP BY SUM | 2.38 ms | 2.05 ms | 1591 ms | **668×** |

→ **sqlite3 互換目標 (= compatibility ではなく性能上のリプレース) として全勝、
44× 〜 4200×**。 DuckDB は OLAP 専 (full-scan SIMD) のみ Q4/Q5 で僅差勝ち。

### Tests

- `bindings_extract_table_vid_and_himo_id` を `bindings_extract_himo_id_and_engine_direct_write`
  に rename + marker 抜きに書換え。 「column 名 → himo_id」 だけで engine 直叩き
  write/read が schema find に揃うことを検証
- 既存 schema layer test 15 個 全 pass、 marker 削除による regression なし

### Docs

- `enchudb-schema/README.md` を marker 廃止後の世界観に全面更新。 bindings
  例から marker / table_vid を除去、 「table = 紐の束 declaration、 row 識別
  marker は存在しない」 を冒頭に明記

## 0.2.8 — 2026-05-16

### Added

- **request4: `SubscriptionFilter` trait と per-peer publish** — partial sync
  (SNS の followee 限定配信等) を `Syncer` の hook として policy 化。 既存
  caller は API 不変、 default `AllRecords` で旧 broadcast 挙動を維持
  - `enchudb-sync/src/subscription.rs` 新規:
    - `pub trait SubscriptionFilter { fn should_send(&self, target_peer, record) -> bool }`
    - `pub struct AllRecords` (default impl)
  - `Syncer::set_subscription_filter(Arc<dyn SubscriptionFilter>)`
  - `Syncer::publish_since_for_peer(target, since)` — single-target publish
  - `Syncer::publish_since(since)` を `known_peers().for_each(publish_since_for_peer)`
    で per-peer 経路にラップ。 known_peers が空なら旧 broadcast 経路 (= backward
    compatible)
- **`Transport` trait に 3 method 追加** (`enchudb-engine/src/transport.rs`):
  - `publish_to(from, to, records)` — single-target、 default は broadcast
    フォールバック
  - `pull_as(to, from, since)` — to peer 視点で broadcast + targeted log を merge、
    default は broadcast pull フォールバック
  - `known_peers() -> Vec<PeerId>` — default 空
- **`InMemoryTransport`** で 3 method を実装、 `targeted: HashMap<(from, to),
  Vec<WireRecord>>` で per-target log を保持

### Tests

- `subscription.rs` の 2 test (AllRecords 全送り、 author 別 drop filter)
- `sync.rs` の 3 test (default filter backward compat、 自前 filter で peer 別
  partition、 `publish_since_for_peer` 単独呼び)

### Migration

- 既存 caller (`sinfo` / `matcha` / `bisquit` / sunsu の broadcast 経路) は
  **API 完全不変**。 何もしなくても旧挙動で動く
- SNS partial sync を作りたい caller (sunsu の次の段階) は:
  ```rust
  struct SnsFilter { /* peer 別 follow set 等 */ }
  impl SubscriptionFilter for SnsFilter {
      fn should_send(&self, target: PeerId, rec: &WireRecord) -> bool { /* ... */ }
  }
  syncer.set_subscription_filter(Arc::new(SnsFilter::new(...)));
  ```
- 自前 `Transport` 実装を持ってる caller は `publish_to` / `pull_as` /
  `known_peers` の default impl で旧挙動を維持。 partial sync を機能させたい
  なら override (HTTP/WS push 系で必要)

## 0.2.7 — 2026-05-16

### Fixed

- **issue6: 0.2.6 の dirty range tracking が writer hot path で cache line
  contention → 18-34% スループット退化** — 0.2.6 で導入した `mark_dirty` を
  writer thread の `EntitySet::allocate` / `set_bit` / `free` からも呼んで
  いたため、 256 writer + 1 consumer が共有 atomic (`dirty_lo` / `dirty_hi`)
  の cache line を激しく bouncing
  - `GrowableMap::mark_dirty` を CAS loop から `fetch_min` / `fetch_max` 1 命令
    + fast-path skip (両 atomic が既に範囲をカバーしてれば atomic op を完全 skip) に変更
  - `EntitySet` writer paths から `mark_dirty` 撤廃 (`allocate` / `set_bit` /
    `free` / `allocate_from_free_stack`)
  - 代わりに `Engine::body_msync` で `entity_set` region を **無条件 msync**
    (small fixed region なので cheap)。 `GrowableMap::flush_aligned` を
    hardware page 境界に揃える helper として追加
  - `Vocabulary::insert` の `mark_dirty` は残置 (数値書き込み hot path には
    出ない、 text-based caller のみ)
  - `dirty_lo` / `dirty_hi` は consumer thread (apply_op 経由の `Column::set` /
    `UndoLog::record_unchecked` / `ContentStore::set`) のみが書くので
    single-thread atomic で contention が消える

## 0.2.6 — 2026-05-16

### Performance

- **request3: `body_msync` を dirty range 限定化** — 旧実装は consumer thread の
  `body_msync` が `flush(0, committed)` で committed 全体を msync。 sustained
  workload で committed が伸びるたびに線形に遅くなる症状 (sinfohub-server 10K user
  ×100KB load test で body_msync **6 ms → 3.6 s** に増大、 fsync_interval=100 ms
  が実質機能せず producer 全体が consumer に律速)
  - `GrowableMap` に `dirty_lo` / `dirty_hi` の atomic ペアを追加。 hot write path
    が `Region::mark_dirty(off, len)` で union、 consumer は `flush_dirty` で
    swap+reset して [lo, hi) だけ msync
  - 計装した write 経路: `Column::set` / `Column::clear` / `UndoLog::record_unchecked` /
    `UndoLog::commit` / `EntitySet::{allocate, set_bit, free, allocate_from_free_stack}` /
    `Vocabulary::insert` (+ index_insert) / `ContentStore::set` / `ContentStore::remove`
  - sustained throughput は dirty 化 rate の関数になり、 committed 全体の大きさには
    依存しなくなる (= 100K user スケールでも 1000 push/sec 維持を期待)

### Fixed

- **Apple Silicon (16 KB hardware page) での msync EINVAL** — 旧 `PAGE_SIZE=4096`
  compile-time const で 4 KB-aligned 境界に揃えていたが、 macOS arm64 の hardware
  page size は **16 KB** なので msync が EINVAL を返していた (request3 実装中に踏んだ)。
  `sysconf(_SC_PAGESIZE)` で起動時に取って cache する `runtime_page_size()` を導入

### Tests

- `tests/dirty_range_msync.rs::body_msync_handles_dirty_range_correctly` —
  writes + body_msync 交互 5 batch + 連続 (idempotent) 呼びで pass
- `tests/dirty_range_msync.rs::wal_sync_with_dirty_range` — wal_sync 経由でも
  dirty range path が正しく動く

## 0.2.5 — 2026-05-16

### Fixed

- **issue5: `flush_writes()` が live query barrier として機能していなかった** —
  0.2.3 (issue3) で `entity()` から `Op::EntityCreated` を WriteQueue に逃がした
  際、 push_count の counter 連動を入れ忘れていた。 apply_count は EntityCreated
  でも +1 されるので、 `applied >= pushed` が Ties 未 apply の段階で成立 →
  早期 return → flush_writes 直後の live query が 5-12% の write を見落とす
  bug (sunsu Docker scenario 01 medium/large で panic していた症状)
  - `entity()` で `Op::EntityCreated` を push した直後に
    `push_count.fetch_add(1, Ordering::Release)` を呼ぶように修正
  - durability は **影響なし** (WAL は正しく書かれていたので drop+reopen で正しい
    count が出ていた)。 影響は live read のラグだけ

### Tests

- `tests/flush_writes_barrier.rs::flush_writes_waits_for_all_ties_including_entity_created_path` —
  queue_cap=1024 (極小、 backpressure 強制) で 4 writer × 5K iter (= 20K entity +
  40K ties) を流して、 `flush_writes()` 後の `query_by_id` が **20K entity 全部
  返す** 事を検証

## 0.2.4 — 2026-05-16

### Fixed

- **issue4: sustained async writer で queue が unbounded で OOM** — option 1
  (bounded queue + producer block) で対応。 旧 unbounded `SegQueue` では
  writer >> consumer rate になると queue 内 record が線形成長 → RSS 線形成長
  → OOM kill (sunsu Docker scenario 03 で 14s / 8M posts / 3.38 GB → 4 GB 突破)
  - `WriteQueue` を `crossbeam_queue::ArrayQueue` に変更、 push 満杯時は
    `std::thread::yield_now` ループで consumer の drain を待つ (自然な
    backpressure)
  - `wal_record_queue` も `ArrayQueue` 化、 helper `push_wal_record_blocking`
    を経由
  - **新 API**: `Engine::create_concurrent_with_wal_queue_cap(path, wal_cap,
    undo_cap, queue_cap)`。 default は 1 M ops (= 旧挙動に近い緩い setting)

### Added

- `WriteQueue::with_capacity(cap)` / `WriteQueue::capacity()` 公開
- 定数 `write_queue::DEFAULT_WRITE_QUEUE_CAP = 1_048_576`

### Tests

- `tests/queue_backpressure.rs::small_queue_cap_does_not_hang` — queue_cap=64
  (極小) で 8 writer × 200 ops が hang せず完走する事を確認

### Consumer migration notes

- 既存 caller (`create_concurrent_with_wal` 等) は API 不変、 内部だけ
  bounded queue 化。 default 1 M cap が旧 unbounded 挙動の近似なので、 100K
  ops/sec 級の writer なら latency 体感不変
- writer rate が consumer 上限を恒常的に超える app (sustained SNS post 等) は
  **producer 側で push が block する** ようになるので、 throughput が
  consumer 上限に張り付く (= sunsu 等で実測 ~500K posts/sec)
- RSS を更に絞りたいなら `create_concurrent_with_wal_queue_cap(.., queue_cap=10_000)`
  等で明示

## 0.2.3 — 2026-05-16

### Fixed

- **issue3: sustained 並列 sync writer で undo region (16 M) overflow → panic** —
  3 段階で対応。 sinfohub-server の 100K user load test / sunsu scenario 03 が
  完走できる
  - **Phase 1**: `Engine::entity()` の `undo.record` を consumer thread に逃がす。
    新規 `Op::EntityCreated { local }` を WriteQueue に push、 consumer thread の
    `apply_op` が `undo.record_unchecked` で serial に記録 (writer thread を
    速い側に保つ)
  - **Phase 2**: `UndoLog::record` に backpressure。 count が `max_entries` の
    90% 超で writer thread が `force_commit` AtomicBool を立てて `yield_now`
    ループ、 consumer は loop 先頭でこの signal を check し fsync_interval を
    待たず即時 fsync→commit。 consumer 自身は `record_unchecked` 経由なので
    self-deadlock しない
  - **Phase 3 (new API)**: `Engine::create_concurrent_with_wal_undo_cap(path,
    wal_capacity, undo_max_entries)` 追加。 default 16 M で足りない sustained
    workload は 64 M / 128 M 等に上げられる (1 entry = 10 B、 64 M で 640 MB)
- **entity-only ops で undo が clear されない bug** — 多 `entity()` 経路では
  `wal.head` が動かないので、 consumer の fsync 節が `wal.head() > checkpoint`
  だけ見ていた旧 path だと undo.commit が永久に走らず over_threshold が解除
  されなかった。 `pending_count > 0` なら `body_msync + undo.commit` で undo を
  clear する path を追加
- **`tie_to_by_id` の debug_assert を緩和** — Tag/Leaf 型 (vocab_id を value と
  して持つ) の himo を hot path で直接張る用途を許可。 schema 層の marker tie
  を起動時 pre-resolve した `table_vid` で書ける (= request2.md の README 例が
  debug build で panic していたのを解消)

### Tests

- `tests/undo_overflow.rs`:
  - `entity_undo_offloaded_no_overflow` — 4 writer × 2K entity + 2 ties で
    undo cap 4096 (= default の 1/4096) に絞っても panic しない
  - `sync_writer_backpressure_no_overflow` — 16 writer × 1K `tie_text_to` で
    backpressure path を 4+ 回踏ませても panic しない

## 0.2.2 — 2026-05-15

### Performance

- **`Engine::open` が 1219 ms → 6 ms (200× 速い)** — `Vocabulary` データ領域 header
  に「clean shutdown 後の index は無事」 マーク (= clean flag) を追加。 前回の
  graceful close 後に再 open する時、 これまで毎回走っていた 3.49 GB の index
  zero-clear + rebuild を skip するようにした。 crash 後 open は従来通り rebuild
  (= 安全性は不変)
  - default max_entities (16M) で `vocabulary.rs:97-98` の `for b in &mut xm[..] { *b = 0 }`
    が memory bandwidth で律速されて 500-700 ms 消費していたのが原因
  - cap65k 等の小容量でも 75 ms → 1 ms に縮む
- **`define_himo` 直後の heap RSS が 25 GB → 5 MB (5000× 縮)** — `BucketCylinder::positions`
  の eager allocation (`vec![..; max_entities]` = 16M × 8 byte = 128 MB / himo)
  を lazy 化 (`Vec::new()` start、 `ensure_positions` で on-demand 伸長)。 同時に
  v33 以降 dead weight になっていた `PairTable` (= `ensure_himo` で card_a × card_b
  cells を pre-allocate していた、 ~4.6 GB / 200 himos) を全削除
  - sinfo (26 tables / ~156 himos) の OOM kill が解消
  - on-disk layout / API は不変、 consumer 側は `cargo update` だけで効果あり
- **WAL append を consumer thread で batch 化** — 従来 record 1 件ごとに `flock` を
  取って `head` を進めていたのを、 consumer thread が複数 record をまとめて 1 度の
  flock + head 更新で flush するように変更。 raw `tie_async` 経路で per-record flock
  コストが消えて **`tie_async_by_id` 1.42 M op/sec を実測** (WAL on)
- **`tie_*_by_id` / `untie_*_by_id` の 8 関数追加 (request.md)** — 高頻度 writer が
  起動時に解決済みの `himo_id: u16` を持ち回ることで、 per-call の `HashMap<String, u16>`
  lookup を完全に skip。 既存の string 版 8 関数 (`tie_async` 等) は内部で
  `himo_id(name)` を 1 度だけ resolve → `_by_id` 版に委譲する thin wrapper に書き換え
  (API は壊れない)。 同様に `query_by_id(&[(u16, u32)])` も使える

### Added

- **`Engine::open_readonly(path)` + `Database::open_readonly(path)`** — writer lock を
  取らない read 専用 open。 別 process が writer として開いていても並行 open 可、
  reader 同士も無制限。 write API を呼ぶと panic
  - 用途: GUI の表示専用 process、 監視ツール、 backup-reader
- **writer 排他 lock** — `create_*` / `open_standalone` / `open_concurrent_with_wal`
  が `.db.lock` sidecar に `flock(LOCK_EX)` を engine 寿命中保持。 2 つ目の
  writer process は block する (sqlite WAL モード相当の挙動)
- **`Database::create_growable_with_capacity(path, max_entities)`** — default 16M
  (= layout 25 GB、 apparent file 24 GB) を絞れる。 sinfo 等の中規模 app で
  65K 程度に指定すると layout 1.3 GB / apparent 765 MB に縮む
- **WAL `append_inner` に `flock` 排他** — 同 .wal を別 process が直接開いて append
  する場合の data race を防ぐ defense-in-depth
- **schema 層に bindings 取り出し API (request2.md)**:
  - `Table::himo_id(col: &str) -> Option<u16>` — build 時に解決済みの himo_id
  - `Table::table_vid() -> u32` — `__enchu_table` marker に張る vocab_id
  - `Database::marker_himo_id() -> u16` — schema 全体の table marker himo_id
  - これで app は schema layer の private const (`__enchu_table` 等) に触らずに
    bindings struct を組める。 起動時に 1 度抽出 → runtime は engine 直叩き
- **dev tools (examples)**:
  - `growable_rss_repro` — mode 別 (default/cap1M/cap65k/tiny) で VSZ delta /
    drop 時間を計測
  - `open_profile` — `ENCHU_OPEN_PROFILE=1` で各 load step の Δreclaim / Δt を
    eprintln (clean / dirty 両 path)

### Tests

- `engine::readonly_does_not_block_other_opens` — writer + 3 reader 並行
- `engine::readonly_write_panics` — readonly で write API 呼ぶと panic
- `engine::writer_blocks_concurrent_writer` — 2 writer 同時起動で 2nd が block
  (200 ms timeout 後 1st drop で unblock)
- `schema::open_readonly_coexists_with_writer` — schema 層から writer + 3 reader 共存
- `schema::create_growable_with_capacity_apparent_size_scales_down` — cap=65K で
  apparent file が default の 10× 以上縮む事を検証
- `wal::multi_process_append_no_offset_collision` — 同 .wal を 2 Wal instance で
  交互 append しても record 破壊なし

### Internal

- `Backing::flush_range(offset, len)` 追加 — page-aligned で targeted msync。
  clean flag のような小領域 (16 byte) を 25 GB 全体 msync せずに 4 KB だけに絞る
  用途
- `Vocabulary::mark_index_clean(bool)` 公開 method — Engine が flush / open
  境界で flag を書き換える
- `Engine` に `is_readonly: AtomicBool` 追加。 既存 `is_replica` パターンと並列。
  `check_writable` で両方チェック

### Internal docs / files

- `docs/concurrency.md` — writer / reader / multi-process モデルを 1 枚で
- README に **「並行アクセス」 章** 追加 + concurrency.md への link
- `tests/wal_mmap_race.rs` — WAL-vs-mmap race の deterministic 再現テスト
  (`#[ignore]`、 fix 未着手の known issue として固定)

### Positioning / docs

- **schema 層を declarator + bindings 専門に位置付け直した** — README / schema crate
  README を rewrite。 高頻度 writer / reader (sunsu の SNS bench、 sinfo の concurrent
  job 等) は **「起動時に schema declare → bindings 抽出 → runtime は engine 直叩き」**
  が公式推奨。 schema 層の `insert().commit()` / `where_eq().find()` は declarative
  convenience として残るが、 hot path で経由する想定ではない
- runtime hot path 推奨形:
  ```rust
  // 起動時 1 回
  let users = db.get_table("users").unwrap();
  let marker_hid = db.marker_himo_id();
  let table_vid  = users.table_vid();
  let name_hid   = users.himo_id("name").unwrap();

  // runtime: bindings + engine 直叩き
  let eng = db.arc_engine();
  let e = eng.entity();
  eng.tie_to_by_id(e, marker_hid, table_vid);
  eng.tie_text_to_by_id(e, name_hid, "Alice");
  ```

### Consumer migration notes

- **schema commit 経路を使ってる app は何もしなくていい** (内部で `_by_id` 経路に
  切り替え済み、 API 不変)
- **hot path で perf を出したい app** は次に bindings 抽出 + engine 直叩きに移行:
  - `sunsu` の concurrent_posts: 113k posts/sec → ~1.4 M posts/sec (estimate ~12×)
  - sinfo の SNS 系 writer 全般
- `sinfo` の sf CLI が持っている `fs2::FileExt::lock_exclusive` (acquire_db_lock)
  は本 release の enchudb 内蔵 lock と二重になる。 動作は壊れないが、 redundant
  なので sinfo 側で別途 cleanup PR を出す予定
- `Sinfo Studio` は `Database::open` を `Database::open_readonly` に切り替えれば、
  sf CLI 起動中でも block されない (= 既存 race を完全解消)

## 0.2.1 — 2026-05-13

### Internal

- example: dump CLI 追加 (DB 中身を markdown / json で stdout dump)
- transport: relay log で (peer, hlc) dedupe (gossip 増殖防止)
- sync + engine: gossip 整合性修正 (delete 復活防止 + identity 保持)
- wal: append_relayed + RelayedHeader 追加 (gossip 用)
