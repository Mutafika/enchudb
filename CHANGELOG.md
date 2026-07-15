# Changelog

EnchuDB の主要 release ごとの変更を時系列で記録。 0.x 段階につき **semver 厳密
ではない**が、 patch (z) は非 breaking、 minor (y) は API/format 変更を含む方針。

## Unreleased

### Fixed — graceful close で clean flag を永続化 (#101): 毎 open の vocab/himo index rebuild を解消

clean flag（index↔data 整合性マーク）を書く経路が `flush()`（`&mut`、実質 `seal_integrity`
専用）にしか無く、通常の close は dirty のまま終了 → **writer open 毎に vocab/himo_reg の
`rebuild_index` が O(count) で走っていた**（sf のような 1 コマンド 1 open の使い方で全コマンドに
乗る固定税。readonly open も shadow index を毎回 heap rebuild）。vocab は回収なし単調増加なので
税は unbounded に育つ（20万 entry で実測 +20ms/コマンド）。

- **`Engine::flush_clean(&self)`** 追加: 滞留 write を全 apply → 全 region msync →
  vocab/himo_reg の clean マーク → 再 msync。プロセス生存中の checkpoint 用（sinfo の
  `sync()` 等から呼べる `&self` 版）。readonly open では no-op。
- **`Engine::Drop` で best-effort clean-flush**: graceful close だけで次 open が rebuild を
  skip できる。panic unwinding 中 / consumer 死亡時 / readonly は書かない（= dirty のまま
  → 次 open の rebuild が正しい recovery）。
- 観測 API: `vocab_index_rebuilt_on_load()`（open で rebuild が走ったか）/
  `vocab_index_is_clean()`（disk 上の clean flag）。
- writer open が open 直後に flag を 0 へ戻す #56 の write-crash 保護は不変。
  semantics は「graceful exit → clean → 次 open skip / crash → dirty → rebuild」。
- regression test: `issue101_clean_reopen`（graceful close で skip・crash 相当 copy で
  rebuild・readonly 非破壊。Drop hook を無効化すると落ちることを確認済み）。

## 0.13.0 — 2026-07-14

### Changed — Cylinder read を lock-free 化（#95、RwLock 撤去）

`HimoStore` の `RwLock<BucketCylinder>` は read が write と per-himo で相互排他になり、
長い read（巨大 bucket の clone）が write を stall させ、read↔write が取り合っていた
（sinfo 等「開いたまま read しつつ write する」アプリで問題）。CLAUDE.md が謳う
「ロックフリー並行 read / ダブルバッファ + AtomicBool swap」は**実装されておらず**
（履歴上 double-buffer は一度も存在せず）、実物は `std::sync::RwLock` だった。

これを **append-only + epoch（crossbeam-epoch）** の `LockFreeCylinder` に置換:

- **read は完全 lock-free**（writer を一切待たない、epoch pin）。value→eid の各 bucket は
  append-only（publish 範囲は不変、contiguous `&[u32]` を維持 → query 無改造）。
- dense（value < 1M）は `Atomic<Vec<Arc<AppendBucket>>>` を epoch-swap で on-demand 成長、
  sparse（value ≥ 1M、稀）は `Mutex`。realloc の旧 backing は epoch で全 reader 通過後に解放。
- **write は per-himo writer lock で直列**（append O(1) amortized、メモリ ~1 倍）。writer の
  呼び出し元は consumer 1 本ではない — **同期 tie（`tie_to_by_id` 系）/ schema
  `RowBuilder::commit` は任意の user thread が呼ぶ**（master では RwLock write が直列化）。
  初版はここを見落として無 lock にしており、多 thread schema commit（sunsu matrix bench）で
  epoch defer_destroy の double free → malloc abort していた。writer lock で master と同じ
  write 直列度に復元（reader は lock を一切取らないまま = 本 fix の目的は維持）。
  lock は `parking_lot::Mutex`（critical section ~100ns に対し std::sync::Mutex は競合で即
  カーネル休眠 = psynch 待ちが支配項になり schema write が ~40% 落ちた。adaptive spin で回復）。
  insert の epoch pin も 3 回 → 1 回に集約（`push_in`、pin を insert 全体で共有）。

  sunsu matrix（100k posts / 4 thread、warm、同一機）で master 同等を確認:

  | 構成 | master | 本 branch |
  |---|---|---|
  | raw tie_async / oplog off | 8〜15M ties/s | 8〜26M ties/s（同等） |
  | raw tie_async / oplog on | 5.0M | 5.1M |
  | schema commit / oplog off | 5.6〜8.0M | 5.6〜7.6M |
  | schema commit / oplog on | 0.4M | 0.4M |
- 削除/更新は Cylinder を触らず（append-only）、read 側が **Column verify で stale を
  filter**（conditional: append-only himo は verify を skip = fast path）。churn 由来の
  dup は read で dedup。compaction は後付け最適化（未実装、#99 で追跡）。
- `rebuild()` は no-op 化（互換で残置）。Cylinder は open 後 lazy build + live 維持。

**互換性**: 公開 API・on-disk format・wire 不変。`unique_count` / `total` は churn した
himo で over-count になる（append 数ベース、compaction #99 まで）。append-only では従来通り正確。
読みは live semantics（snapshot なし）— read-while-write の held snapshot は #100 へ切り出し。

**観測 API 追加**: `Engine::himo_cylinder_backing_bytes(himo)` — Cylinder が確保する eid
backing の総 bytes（メモリ会計・double-buffer 検知）。

**検証**:
- **unit**: `append_bucket` / `lockfree_cylinder` の並行 test（1 writer + N reader、破損なし）、
  `no_double_buffer_backing_bound`（backing < 2× = double-buffer していない厳密証明、実測 1.28×）。
- **統合**: `issue95_lockfree_read`（並行 pull・値更新 stale の verify filter・**同期 tie
  ×4 thread の並行 write** = write_lock regression test、修正を外すと crash することを確認済み）。
- **loom model check** (`tests/loom_append_publish.rs`、`#![cfg(loom)]`): AppendBucket の
  publish protocol（writer が slot 書込→`len.store(Release)`、reader が `len.load(Acquire)`→
  prefix 読み）を de-epoch した model で全 interleaving を model check。**範囲は単一 backing の
  publish handshake のみ**（grow/swap + epoch 解放は loom 非対応につき対象外、そちらは Miri +
  `grow_under_read` stress で補完）。1 writer + 1/2 reader で torn read / data race ゼロ。
  `Release`→`Relaxed` に落とすと loom が torn read を検出して落ちることを確認済み
  （再現手順は test 冒頭コメント）。手書き model なので `append_bucket.rs` の ordering 変更時は
  model の同期が必要（相互参照コメントあり）。
  実行: `RUSTFLAGS="--cfg loom" cargo test --test loom_append_publish --release`。
- **model-based property test** (`tests/engine_model_proptest.rs`): tie/tie_text/untie/delete/
  reopen のランダム op 列（proptest、200 case × ≤40 op）を参照 oracle（`BTreeMap`）と毎 op 後に
  厳密照合。**Number と Tag himo を跨いで**（Number は Column 直値、Tag は Vocabulary intern
  した vid で read path が違う）、`pull_raw` / `get` / `get_text` / 2-cond `query` を網羅。
  値更新→削除→再 tie→reopen の組み合わせで stale/dedup/verify/rebuild を自動生成 + shrink。
  engine crate に proptest dev-dep を追加。
- **破壊テスト** (`tests/issue95_stress.rs`): `churn_storm_exact`（20k×40 round の値更新で
  bucket を stale だらけにし、並行 read の構造 invariant を保ちつつ quiesce 後の pull が
  live 集合と厳密一致）、`crash_recovery_compacts`（churn→drop→reopen で Cylinder が column
  から rebuild され stale が消える）、`grow_under_read`（dense 配列 realloc 多発 × 旧配列を
  掴む reader で epoch 解放が安全）。
- **fault-injection** (`tests/oplog_recovery_fault.rs`): ① file 縮小 truncate（7 点）— WAL は
  固定容量 pre-allocate なので capacity guard の **clean Err が仕様**（crash しないことを検証）。
  ② tail zero 化（5 点、file size 不変 = torn write の現実的模擬）/ ③ byte-flip（8 offset）—
  graceful かつ Ok なら pull が**書いた集合と完全一致**（body msync 済み = 開けた以上 1 件も
  欠けない）、**最低 1 case は Ok**（全 case Err の vacuous pass を弾く guard。この guard 導入で
  旧 truncate テストが実は全 Err の空振り passだったことを検出し、② を追加した）。
  body sync 前 crash（未 checkpoint tail の replay）は subprocess crash harness が要るので
  #98 ④ の範囲。
- **Miri UB 検証**: `append_bucket` 全 test + `lockfree_cylinder` の dense 系 test を Miri
  **Tree Borrows** で回し UB なしを確認（unsafe 表面 = `from_raw_parts` over `UnsafeCell`、
  epoch `defer_destroy` はこの範囲で全て踏む。sparse path は `Mutex` + `HashMap` で unsafe なし）。
  crossbeam-epoch 0.9 は Stacked Borrows 非互換（内部 intrusive list、既知）なので TB +
  `-Zmiri-ignore-leaks` で回す（epoch 遅延解放の exit 時 garbage を除外）。concurrent/大量 test は
  `cfg!(miri)` で縮小。
- **bench** (`examples/lockfree_engine_bench.rs`、実 Engine 経路): A. 巨大 bucket を 4 reader が
  clone し続けても drain は同オーダー（write は long read に stall しない、減少分は CPU 帯域
  contention であって lock 待ちではない）。B. writer が同 bucket を叩き続けても pull_raw の
  p50/p99 latency が idle と同オーダー（read は write に stall しない）。C. cylinder backing
  1.28×（メモリ ~1x、double-buffer なし）。
- engine lib 200 + 統合/破壊 5 + schema/sql/sync 全 green。prototype 実測（`examples/lockfree_bucket_probe`）で
  write starvation 214ms → 解消、verify tax 0.85〜4.5 ns/elem（`examples/verify_tax_probe`）。

## 0.12.2 — 2026-07-11

### Fixed — dirty open が vocab index 予約全域を物理コミットする問題 (#92 / #56 ③)

crash 相当の落ち方 (flush せず drop / `process::exit` = `clean_flag=0`) をした DB を
write open するたび、 `Vocabulary::rebuild_index_into` が **index 予約全域
(`index_cap`×13B) を zero-fill** してから live entry を再挿入していた。 index region は
fixed cluster で mmap 済みだが **sparse** (物理ブロック未確保) なので、 この全域
書き込みが sparse ページを 1 枚残らず物理化 → **live vocab 数と無関係に `index_cap`
比例の物理コミット**が起きていた (#56 で ①② は fix 済だが ③「rebuild は used slot
だけ touch する」提案が未実装のまま残存)。 `create_growable_with_capacity` は
`vocab_max_entries = cap×16` なので大 pool ほど深刻 (cap=1M で dirty reopen 一発
+~200MB、 cap=16M で ~3.5GB)。 sinfo CLI (sf) の「空 DB でも起動が pool 比例で重い」
の主因。

- **全域 zero-fill を廃止**し、 既存 on-disk index の上へ live entry (id 0..count) を
  **再挿入するだけ** に (used-slot only touch)。 append-only vocab の count が単調な
  ことを利用:
  - 通常の落ち方では on-disk index は data と consistent → 全 entry が dup 一致で
    **書き込みゼロ** (触るのは live slot が載る数ページのみ)。
  - torn write で index が count より遅れ (slot 欠落) → 空 slot へ再挿入して self-heal。
  - torn write で index が count より先行 (`vid >= count` の「未来」slot 残存) →
    live entry ではないので `slot_hash` 一致でも `get(vid)` を呼ばず読み飛ばす guard を
    `rebuild` / `lookup` / `index_insert` に一貫追加 (OOB と誤 dedup を防ぐ)。 通常運用の
    committed slot は必ず `vid < count` なので **hot path は無影響** (guard は crash
    復旧後のみ発火)。
- **readonly open の RAM 肥大も同時に解消**: dirty DB の readonly open は heap の
  shadow index に rebuild するが、 同じ zero-fill が shadow (calloc で sparse) の全
  ページを touch し `index_cap` 比例の RSS を食っていた。 zero-fill 撤廃で O(count) に。

**互換性**: on-disk format 不変 (v7 のまま)、 wire 不変、 API 不変。 純粋な内部 open
経路の fix。 既に旧挙動で膨らんだ DB の**コミット済み物理を回収**する shrink 経路は
別スコープ (本 fix は今後の bloat を防ぐ)。

**効果 (`issue92_dirty_footprint` bench, live vocab 5 件)**:

| cap | index region | dirty reopen 増分 (before → after) |
|----:|----:|---|
| 256K | ~54MB | +18,112 KB → **+0 KB** |
| 1M | ~218MB | +204,720 KB → **+0 KB** |
| 4M | ~832MB | +843,680 KB → **+0 KB** |

**検証**: `issue92_dirty_index_full_commit` (別プロセス `process::exit` で dirty 化 →
reopen 増分 < 4MB、 fix を戻すと fail する regression guard)、 `vocabulary` unit 3
(consistent 再挿入 / torn-behind self-heal / torn-ahead の OOB 回避)、 全 workspace
test green。

## 0.12.1 — 2026-07-11

### Changed — `LeafStore` の cell offset を word 単位化 + region cap を選択式に (#90)

`LeafStore` (#88 / 0.12.0) の cell 参照・`high_water`・free-list が **生 byte offset
の u32** だったため leaf region が ~4.29GB でハードキャップだった。 slot は
`2^off_shift` aligned で確保されるので **offset の下位 bit は常に 0** = **word offset
(`byte >> off_shift`)** で持てば、 列幅も indirection も増やさず cap を拡げられる。

- cell handle / `high_water` / free-list を **word 単位** に (slot header の
  `slot_size` / `len` は byte のまま)。 region cap = `u32::MAX << off_shift`:

  | scale (`LeafScale`) | off_shift / align | cap |
  |---------------------|-------------------|-----|
  | `Gb16` (default)    | 2 / 4B            | ~16GB |
  | `Gb32`              | 3 / 8B            | ~32GB |
  | `Gb64`              | 4 / 16B           | ~64GB |

- **選択式**: `create_full_with_leaf_scale` / `create_growable_with_leaf` で
  `LeafScale` を指定 (default `Gb16`)。 大きい scale ほど slot alignment (padding)
  が粗くなるので、 小さい payload は `Gb16`、 wikipulse 型の大 payload × 巨大
  working set は `Gb32`/`Gb64`。 予約 `leaf_data_size` は選んだ scale の cap 以下を検証。
- `off_shift` は leaf region header に self-describing に記録。
- `leaf_footprint()` / `MigrationStats.leaf_footprint` を **`u64` (byte)** に
  (16GB 超を表せるよう u32 から拡張 — 呼び出し側の型注釈のみ影響)。

**互換性 (patch に収まる理由)**: on-disk format は v6 → v7 に上がるが、

- **既存 v6 DB は無改変で動く**: v7 engine は v6 region を header の `off_shift == 0`
  (= byte offset) として **read-through** で開く (migration 不要、 4GB cap のまま)。
- **wire / sync は完全に不変**: offset は各 node ローカルの storage 詳細で wire に
  乗らない (`TieLeaf` は生 bytes を運ぶ)。 → **peer 同時アップグレード不要**、
  v6/v7 node 混在でも収束する。
- 既存の create API・tie/read 挙動・reclaim は不変 (additive)。
- **唯一の非互換**: v7 で作った DB は **0.12.0 engine では開けない** (word offset を
  byte と誤読するため version gate で reject)。 = 新規 DB は 0.12.1 以降が必要。
  既存 DB を 4GB 超に伸ばしたい場合は v7 で作り直す (v5→v6 migration の出力は
  従来通り byte-offset の v6)。

**検証**: `leaf_store` unit 10 (word encoding / cap 16-64GB / 5GB sparse で 4GB 超
offset の往復 / shift 2·4 の churn reclaim / v6 byte 互換)、 `issue90_leaf_scale` 3
(scale reopen 永続 / reclaim / cap 超過 reject)、 全 workspace test green。

## 0.12.0 — 2026-07-11

### Changed — `Leaf` を vocab から剥がし reclaim 対応 store に載せる (#88): high-churn Leaf の単一 DB 無限運用

`content()` → `Leaf` 統合 (0.9.0 #81) 以降、 高 churn な `Leaf` 値 (wikipulse の
毎 event content 等) が **共有辞書 vocab に単調 append され、 delete しても回収
されない** 問題 (#88)。 `Leaf` は先が無い終端ノード = 単一所有・dedup 不要なのに、
append-only-never-reclaim の vocab に入るから貯まる。 対策は「vocab に reclaim を
後付け」ではなく **`Leaf` を vocab から出し、 単一所有・reclaim 対応の専用 store に
載せる** こと。

- **`LeafStore`**: 単一所有・dedup 無し・reclaim 対応の可変長 value store。
  free-list (offset→size の BTreeMap) + 隣接空き coalesce + 末尾空きは high_water
  を retract。 live は動かさない (compaction 非搭載 = footprint 有界化には不要)。
  free-list は非永続 = open 時に live cell から再構成。
- **routing**: `Leaf` の tie / read (`get_text` / `get_content` / `get_entity`) /
  delete / untie / sync が `LeafStore` を経由。 **`Tag` は従来通り vocab** (dedup
  される共有辞書性が活きる)。 reserved-table 内の `Leaf` (`_sync_ops.payload` 等)
  も vocab 据え置き。
- **wire**: `Op::TieLeaf{eid, himo_name, himo_kind, bytes}` を新設し、 旧 `Leaf`
  sync (`Op::Vocab{vid}` + `Op::Tie`/`TieNamed`) を置換。 受信側の
  `(author,vid)→local_vid` remap が消える。
- **create tunable**: `Engine::create_full_with_leaf(.., leaf_data_size)` で
  leaf region 予約サイズを指定可 (`Some(0)` = leaf region 無し = v5 相当)。

**footprint bench** (rolling retention, 20000 rounds / window 200 / content 1KB):

| rounds | before (`Leaf`→vocab, 回収なし) | after (`LeafStore`, reclaim) |
|-------:|-------------------------------:|-----------------------------:|
|   4000 |  3.91 MiB | 0.20 MiB |
|  20000 | 19.53 MiB | 0.20 MiB |

before は round に線形増加 (retention delete でも vocab は戻らない)、 after は
live 集合 ~200×1KB で **平坦・有界** (20000 rounds で 99x)。 = 単一 DB の無限運用が
可能に。 perf: query / scan / `Number` get は不変、 text read (`Tag`/`Leaf`) のみ
routing 分岐で ~+1 ns/attr。

**format v5 → v6 / wire は breaking**: v6 は末尾に leaf region を持ち、 `Leaf` sync
は `TieLeaf` に変わる。 **全 peer 同時アップグレード必須** (0.11 peer とは Leaf 経路が
非互換)。 v5 DB は open 自体は可能だが leaf region が無いため **`Leaf` reclaim は
効かない** (= 下記 migration が必要)。

#### Migration ガイド (v5 → v6)

既存 v5 DB を移送して `Leaf` reclaim を有効化する:

```rust
// src は不変、 dst に新 v6 DB を書く (default leaf region size)。
Engine::migrate_file_v5_to_v6("old.ecdb", "new.ecdb")?;
// leaf region size を指定する版 / bytes 版もあり:
Engine::migrate_file_v5_to_v6_with_leaf(src, dst, leaf_data_size)?;
let (v6_bytes, stats) = Engine::migrate_bytes_v5_to_v6(src_bytes, leaf_data_size, &[])?;
```

- 末尾に leaf region を新設し、 各 `Leaf` himo の live cell (旧 vocab vid) を辿って
  vocab bytes を `LeafStore` へ移し、 cell を leaf offset に書換える。 vocab / entity
  / himo / content は byte 単位で引き継ぐ (in-place file 手術ではなく copy + tail
  追加)。
- `.tables` sidecar はコピーし、 **reserved-table の `Leaf` は移送しない**
  (reopen で `.tables` 復元 → vocab 経路に戻るため)。
- **旧 `.oplog` は引き継がない** (dst の stale `.oplog` は削除)。 v5 の `Leaf` tie op
  は旧 wire 形で移送後の cell と不整合になり replay が巻き戻すため、 dst は fresh
  oplog で開く (= main file の現在状態を checkpoint とみなす)。
- **既知 trade-off**: 移送した旧 `Leaf` bytes は vocab に orphan として残る
  (`stats.vocab_orphan_bytes_left`)。 vocab 自体の compaction は本 migration の
  対象外 = 目的は「以後の `Leaf` 書込みを reclaim 対象にし、 成長を止める」こと。
  既に満杯の DB は「dead vocab (一度きりの sunk) + reclaim される `LeafStore`」に
  なり成長は止まる。 dead vocab の回収が要るなら別途 vocab-compact。

**検証**: `LeafStore` churn の footprint 有界性 (free 無効化で growth 再現の
falsify 付き)、 `issue54` の orphan test 更新 (`Leaf` は vocab を汚さない)、
`issue88_migration` 4 test (移送 read 整合 / reclaim 稼働 falsify / already_v6
no-op / file 非破壊)、 sync の `TieLeaf` 収束、 全 workspace test green。

## 0.11.1 — 2026-07-06

### Added — postings-only な `.etxt` build + 生候補 API (#84 の第一歩): 索引が本文を二重化しない

全文検索の `.etxt` (ETXT) が原文を自前保持していた分を、 DB 本体の本文 (0.9.0
#81 の `_c_` Leaf 値) と二重化しない経路を追加。 driving consumer (naruhodo 判例)
では incremental index ではなく **この冗長性解消**が #84 の実要件だった。

- `TextSearch::save_postings_only` / `write_to_postings_only` (下層 `NgramIndex` /
  `storage` にも): Doc Index / Text Data を省いた原文非保持 index を書き出す。
  substring 検証は caller が DB 本体の原文で行う前提
- `TextSearch::candidates` (生 bigram 候補、 `.contains()` 検証なし) を公開 —
  postings-only index で偽陽性除去を caller 側に委ねる入口
- `has_text()` を `MappedIndex` / `NgramIndex` / `TextSearch` に追加
- postings-only index を `open_mut` / `from_bytes_mut` で in-memory rebuild
  しようとすると `Unsupported` で弾く (原文が無い = index からは再構築不可、
  source から作り直す方針)

**format 互換**: ETXT header の `reserved[0]` に `FLAG_TEXT_OMITTED` を立てるだけで
**version bump なし**。 原文保持の書き出しはバイト等価、 旧 reader も postings-only
file を `doc_count=0` として無害に読める (非 breaking = patch)。 consumer 側
(naruhodo の `build_hanrei_etxt` / search handler) の差し替えは別途。

## 0.11.0 — 2026-07-06

### Added — 逆写像 (request10 / #76 根治): write-back 正式サポート、 multi-writer p2p 完成

どの peer もどの entity に書けるようになり、 衝突はカード (himo) 単位の HLC LWW
が裁く。 0.9.0 #76 の single-writer guard (= translated foreign entity への
write は local-only) を根治で撤去。

- `EidTranslator` に逆写像 (translated local → 元 entity の世界番号) を追加。
  `.eidmap` sidecar から両方向を復元 (**sidecar format 不変**)
- 翻訳キーを record の書き手から **eid の産みの親 (`eid_peer`)** に統一 —
  `Engine::resolve_remote_eid` / `resolve_remote_eid_existing` から author 引数を
  撤去 (= engine public API breaking)
- oplog → `_sync_ops` bridge が replica への self-authored write を **元 entity の
  世界番号に宛名を書き戻して再署名・発送** (`oplog::resign_with_eid` 新設。
  lsn / HLC / author は維持 = LWW identity 不変)
- **残る制約**: Ref 値が translated local を指す write は発送されず local-only +
  一度だけ warn (wire の u32 value に世界番号 u64 が入らない、 wire 拡張の
  follow-up)。 0.10.x まではこの経路が silent に断片化していたので封鎖でもある
- **全 peer 同時アップグレード必須**: wire format は不変だが意味論が変わる
  (0.10 peer は書き手キーで翻訳するため、 非 author write を受けると断片化する)
- 検証: 3 peer 収束 / reopen 永続 / LWW 双方向 round-trip / ref guard の 4 test
  (`writeback_reverse_eidmap.rs`) + falsify 実演 (逆写像を無効化すると収束系
  3 test が「元 entity に着弾しない」で正しく落ちる)

## 0.10.0 — 2026-07-06

命名の一括整理 release。 breaking rename 2 本 (`TenantView → Scope` /
`HimoType → ValueType`) を同梱。 **file format / wire format は完全不変 —
データ migration・peer 同時アップグレードとも不要**、 ソースコードの機械的
置換のみで移行できる。

note: 0.9.0 の migration ガイドで 「0.10 予定」 と言及していた逆写像実装
([[request9]]) は本 release には**入っていない** — 0.11 以降にスライド
(replica write-back の local-only 制約は 0.9.0 記載のまま)。

### Changed — `HimoType` → `ValueType` rename

engine の `HimoType` は 「himo の型」 に見えて実は 「value (= カード裏面) の
格納方式」 を選ぶ enum だった (名前と実態の乖離、 glossary §12.10)。 全 crate で
rename、 **variant (`Number` / `Tag` / `Leaf` / `Ref`) は不変**。

| 旧 | 新 |
|---|---|
| `HimoType` | `ValueType` |
| `Engine::himo_type(himo)` | `Engine::value_type(himo)` |
| `Engine::himo_type_at(idx)` | `Engine::value_type_at(idx)` |

schema 層の `ColumnType` は名前・variant とも不変 (column は schema 層の
正規語彙のため)。

### Changed — `TenantView` → `Scope` rename (#24)

`enchudb-schema` の tenant view API を rename。 **schema crate の public API
breaking (= minor bump 必須)、 file format / wire format / engine API は完全
不変 — データ migration は不要**で、 下記のソースコード置換のみ。

「Tenant」 という use-case 名が 「engine 内に tenant 概念がある」 という誤読を
実際に引き起こしたため (#24 起票の経緯)、 機構そのものの名前に変更: 実体は
**table 名前空間の prefix レンズ**であり、 multi-tenant はそのユースケースの
1 つにすぎない。 旧 `as_view` の 「view」 も SQL VIEW (仮想 row 集合) と紛らわしい
ため同時に廃止 (「view」 という語は将来の仮想 row 集合系機能のために温存)。

#### Migration ガイド

機械的置換で完了する (挙動変更なし):

| 旧 | 新 |
|---|---|
| `TenantView<'a>` | `Scope<'a>` |
| `TenantViewMut<'a>` | `ScopeMut<'a>` |
| `db.tenant(name)` | `db.scope(name)` |
| `db.tenant_mut(name)` | `db.scope_mut(name)` |
| `db.as_view()` | `db.as_scope()` |
| `db.as_view_mut()` | `db.as_scope_mut()` |

`Scope::prefix()` / `get_table` / `list_tables` の意味論・prefix 規約
(`{name}.`)・overhead は不変。 example は `tenant_view_demo` → `scope_demo` に
rename。

## 0.9.0 — 2026-07-03

content store を Leaf himo に統合する構造改定 (#81) + 2026-07-03 全体監査
(31 findings、 GitHub #74-#79) の一括修正。 **wire format 変更 (`Op::TieNamed`
追加) と挙動変更 (create/INSERT のエラー化) を含むため minor**。 DB file format
は不変 — 既存 DB はそのまま開ける (migration 手順は下記)。

### Migration ガイド

- **全 peer 同時アップグレード必須**: sync wire に `TieNamed` (tag 6) が増えた。
  0.8 peer は未知 op として skip するため、 混在運用すると content 系 op が
  サイレントに落ちる。 wire version 混在は非サポート (運用判断 2026-07-03)。
- **既存 DB file はそのまま開ける**: 旧 content region は読み取り専用アーカイブ
  として凍結され、 `get_content` は新経路 (`_c_{key}` himo) → 旧 region の順で
  read-through する。 データ書き換え・再構築は不要。 新規書き込みは全て Leaf 側に
  入る (旧 region が育つことはもう無い)。
- **エラーハンドリングの追加が要る箇所**:
  - `Engine::create*` は既存 path に対して `io::ErrorKind::AlreadyExists` で
    失敗する (今までは既存 DB を**サイレントに clobber** していた)。 「あれば
    開く、 なければ作る」は open → 失敗時 create に書き換えること。
  - SQL の plain `INSERT` は重複 PK で `DuplicatePk` エラーを返す (今までは
    重複行がそのまま入った)。 upsert 意図なら従来通り `INSERT OR REPLACE`。
- **`_c_` prefix の列名は予約**: schema 層の列名 validation が `_c_` 始まりを
  reject する (content 互換 himo の名前空間)。
- **レプリカへの write-back は local-only (#76)**: 翻訳済み foreign entity への
  非 author peer の書き込みは、 その peer のローカルでは効くが **sync には
  流れない** (silent 発散を guard で停止 + 初回 warn)。 双方向編集が要る場合は
  0.10 予定の逆写像実装 ([[request9]]) を待つこと。
- **制約 (known limitation)**: concurrent DB への Ref 列追加は不可 (`add_column`
  が Unsupported を返す)。 `add_column` の再実行は名前一致のみで冪等判定し、
  型違いは検出しない。

### Changed — content store → Leaf 統合 (#81, #79 の根治)

- **`content()` / `content_async` / `get_content` は互換 API として維持**しつつ、
  実体を「`_c_{key}` Leaf himo の lazy 定義 + tie」に変更。 consumer 16 repo は
  再コンパイルのみで移行 (key が静的リテラルのみである事は横断調査で確認済み)。
- 専用 content store 構造 (mod-16 key hash index) を書き込み経路から撤去。
  監査で content 系に集中していた 7 findings — key hash 衝突・8B index torn
  read (#79)、 hash15 tombstone sentinel 衝突、 Content reorder buffer 非永続 +
  eviction ロスト (#78)、 sync `content()` の WAL 漏れ、 delete 時の content
  残留 — は**構造ごと消滅**。
- WAL/wire の `Op::Content` は emit 廃止 (decode は残置 = 旧 oplog は読める)。
  代わりに `Op::TieNamed` (op_type 7 / wire tag 6) を追加: himo full name +
  kind + vid を運ぶ self-describing tie で、 peer 間で himo_id 空間が揃わない
  動的定義 himo を同期できる。 受信側は `ensure_himo_named` で lazy 定義。
- sync の Content reorder buffer (0.8.19) を撤去 — TieNamed は自力で entity
  写像を作れるため退避が不要になった。
- 内部: himo registry を `AppendVec` (固定 capacity・lock-free read) 化し、
  `&self` で himo を動的定義できるようにした (`ensure_himo_dynamic`)。

### Fixed — 監査 findings (#74-#79)

- **#74 (critical)**: `GrowableMap::grow_to` の並行呼び出しで ftruncate が
  file を**縮小**しえた (mmap 済み領域が SIGBUS / silent data loss)。 grow を
  Mutex 化し、 fstat で現サイズ未満への ftruncate を禁止。
- **#75 (critical)**: oplog の同一プロセス並行 append が flock (open file
  description 単位 = プロセス内は素通し) で直列化されず、 offset 衝突で record
  を相互破壊しえた。 プロセス内 `append_lock` Mutex を flock の前段に追加。
- **#77 (durability/並行性)**: `open_readonly` が index rebuild で mmap に
  書いていたのを真の非破壊に (shadow index へ rebuild)。 recovery は body
  msync 完了後にのみ checkpoint を前進。 checkpoint head は Commit append 前に
  snapshot、 cursor は committed 終端まで。 `next_sync_lsn` を open 時に
  rehydrate。 EntitySet bitmap を AtomicU8 化 (free の二重投入防止)。 consumer
  thread panic を poisoned 状態として伝搬 (無限 spin 廃止)。 vocab 4 GiB 超の
  create を reject。 flush 後最初の write で clean flag を確実に落とす。
- **#78 (sync)**: 署名/ACL reject された record を pull cursor が飛び越えて
  永久 gap になる問題 — cursor を `min_rejected_hlc` で clamp し次回再検証。
  HTTP bootstrap に `.eidmap` / `.tables` sidecar を同梱 (`GET
  /bootstrap/{eidmap,tables}`、 旧 server は 404 → fallback)。 Delete 適用
  条件の doc を実装に合わせ訂正。
- **#76 (sync)**: レプリカ側で翻訳済み entity に書いた op が author peer の
  op として bridge され silent 発散する経路を guard (local-only 化 +
  warn-once)。 逆写像による本対応は 0.10 候補。
- **#79 / #59 (API 契約・破損耐性)**: create 系の clobber (上記 migration 参照)。
  oplog open 時の header 検証 + truncate された末尾の安全 recover。 未知
  op_type の警告を rate-limit。 engine header の sanity check (CRC=0 経路の
  bounds)。 ngram `MappedIndex` の全 bounds 検証 (panic → `io::Error`)。 blob
  put の fsync + 既存 blob の内容検証。 query_lang `~ <eid>` の存在チェック。
- **#73 (schema)**: 既存 table への列追加 — `add_column` API と、 既存 table を
  含む superset での `define_table` 再宣言が末尾列を自動 migration する経路を
  追加。 名前衝突で型が異なる再宣言は `SchemaConflict` で loud に失敗。
  concurrent (oplog) DB でも列追加可 (Ref 列を除く、 上記制約参照)。
- **`oplog_sync` の同期契約 2 件** (0.9.0 release 検証中に sync テストの稀な
  取りこぼしとして発見・根治):
  - `flush_writes` が op queue の apply しか待たず、 WAL record queue (op 先行・
    record 後追い #77-H4) に record が残ったまま返る窓があった。 直後の
    `oplog_sync` が record の入っていない WAL に Commit + fsync + checkpoint し、
    「fsync 済みのはずの write」が crash で消える / sync 転送から 1 tick 消える。
    WAL record queue にも push/append counter 対を追加し両 barrier を待つ。
  - `oplog_sync` (caller thread) は checkpoint を進めるが `_sync_ops` への
    transfer をしないため、 consumer tick の ring reset (head == checkpoint で
    発火) が bridge 未了の committed record を wipe しえた — その record は
    **sync から永久に消える**。 `oplog_sync` 内と consumer の `try_reset` 直前の
    両方で transfer を走らせ、 「bridge が追いつくまで ring を畳まない」を
    構造的に保証。 回帰テスト:
    `oplog_sync_bridges_all_records_pushed_before_it`。
- SQL: `OFFSET` を実装。 `DISTINCT` / `GROUP BY` / `HAVING` は silent に
  無視せず `Unsupported` を明示 return。
- RAG: reopen 時に BM25 index を再構築 (今までは空のまま検索 0 件)。
- ACL: 未配線なのに permission 制御があるかのように読めた docs を実挙動
  (in-memory・非永続・未 enforce) に訂正。 実装は需要が出たら別 request。

## 0.8.21 — 2026-07-03

同一プロセス writer 二重 open の無期限 flock ハングを fast-fail に変える bugfix (#80)。
file format / wire format 不変。 挙動変更は「ハングしていたケースがエラーを返す」のみで、
正常動作していたコードには影響しないため **patch** として release。

### Fixed

- **同一プロセスで writer を二重 open すると flock(LOCK_EX) で無期限ブロックする**
  ([#80](https://github.com/Mutafika/enchudb/issues/80)): flock は open file
  description 単位のロックのため、 同一プロセスからの 2 回目の writer open は
  block 検知できず、 fast-fail もタイムアウトも無い診断不能なハングになっていた。
  プロセス内 registry (canonicalize した lock path の set) を flock の前段に追加し、
  重複 open は `ErrorKind::WouldBlock` +
  `"... is already open for writing in this process"` で**即エラー**を返す。
  - **別プロセス** writer との排他は従来通り blocking flock (sqlite 互換、 不変)。
  - readonly open は writer lock を取らないので従来通り併存可。
  - migration: 同一プロセス二重 open の旧挙動は実質デッドロックなので、 依存して
    いた正常コードは無いはず。 pool / cache 層で並行 cold-open が起き得る consumer
    は `WouldBlock` を catch して既存ハンドルの reuse (single-flight) に繋ぐこと。
  - 「既存の共有ハンドルを返す」 (プロセス内 open-file レジストリの完全形) は
    第 2 段として別途検討。

## 0.8.20 — 2026-06-26

`enchudb-text` を `enchudb-ngram`(primitive) + `enchudb-textsearch`(policy) に分離する
refactor (#69/#70)。 crate 名は変わるが file format / magic `ETXT` は不変で既存 `.etxt` は
そのまま読めるため、 データ互換は保たれる **patch** として release。 downstream は dep
差し替え + 型名リネームのみ。

### Changed

- **`enchudb-text` を `enchudb-ngram`(primitive) に改名し、テキスト検索を
  `enchudb-textsearch`(policy) に分離**
  ([#69](https://github.com/Mutafika/enchudb/issues/69)): `enchudb-text` は実体が
  bigram 部分一致エンジンだが名前が「検索」という正体を隠していた。lawgraph の機械検索で
  断片 `出力` が `入出力` の部分文字列として無関係条文を引き込むノイズ調査から、用途が
  逆である事が判明（**人間の対話検索 = 部分一致が正解** `接地`→`接地極` ／ **機械 =
  フレーズ完全一致が欲しい**）。これは bug でなく substring の正しい挙動なので、関心を
  分離した:
  - `enchudb-ngram` = index プリミティブ。bigram 抽出 / posting / intersect →
    **候補 doc id**（`NgramIndex::candidates` / `scan`）。検索意味論は持たない。
  - `enchudb-textsearch` = ポリシー。候補 + `.contains()` 検証 → 正確な部分一致
    （`TextSearch::search`）。クレート名は「`text` が検索を隠す」不満を直す意図で
    `textsearch`（= search over text）。機械向けフレーズ完全一致は入力フレーズを 1 単位で
    渡せば同じ path で扱える（issue option (a)、専用 `enchudb-phrase` は未実装）。
  - file format / magic `ETXT` は不変。既存 `.etxt` はそのまま読める。
  - 旧 `TextEngine` は `TextSearch` にほぼ同型で移行（dep 差し替え + 型名リネーム）。
    downstream（`lawgraph-explorer` / `naruhodo` / `bisquit`）は別 repo で dep 差し替えが要る。

## 0.8.19 — 2026-06-23

cross-peer eid 翻訳 (#9)。 foreign eid のサイレント上書きを直す bugfix。 public API +
`.eidmap` sidecar を足すが file format / wire format は不変、 既存 DB は再 build のみで
上がれる (sidecar 不在 = 空 translator = 旧挙動)。 完全 backward-compatible・migration
不要のため **patch** として release。

### Fixed

- **cross-peer sync で foreign eid がサイレント上書きを起こす**
  ([#9](https://github.com/Mutafika/enchudb/issues/9)):
  EntityId は peer ごとの空間だが、 `Syncer::apply_one` が受信 record の eid を
  翻訳せず raw で apply していた。 foreign eid の local 部が受信側の既存 entity の
  local slot と衝突すると、 その entity をサイレントに上書きしてデータを失っていた
  (LWW の `HlcStore` も foreign eid を local として keying していた)。 engine に
  `EidTranslator` (`(author_peer, foreign_local) → local_eid` 写像) を内蔵し、 apply
  時に 4 op (Tie/Untie/Delete/Content) 全てを翻訳。 初見の foreign entity には himo の
  table 内に fresh な local eid を払い出す (= local entity と同じ allocator、 衝突しない)。
- **cross-peer ref が壊れる** (#9): Ref himo の value 自体が foreign target eid なので、
  同じ translator で ref の target table 空間に翻訳。 forward ref も target entity 自身の
  Tie と同じ local に収束する。

### Added

- `Engine::resolve_remote_eid` / `resolve_remote_eid_existing` / `himo_is_ref` /
  `resolve_remote_ref_value` / `eid_translator` — sync 層が apply 時に foreign eid を
  翻訳するための primitive。
- `.eidmap` sidecar — 翻訳写像を `.tables` と同じ trigger で atomic 永続化。 reopen /
  `snapshot_export` で復元され、 再 sync で重複 entity を払い出さない。

### Hardened (post-merge review pass)

- **破損 sidecar で open 時 OOM abort を防ぐ**: `.eidmap` / `.tables` の deserialize が
  header の `count` を信用して `Vec::with_capacity(count)` していた。 torn / 破損で巨大な
  count を引くと数 GB の確保要求で process が abort しえた。 `count` を残りバッファ長で
  cap し、 `.eidmap` は上限超過を破損とみなして空 translator に fallback。
  回帰テスト: `huge_count_eidmap_sidecar_does_not_oom`。
- **`snapshot_export` の sidecar 整合性**: body msync 後に古い on-disk `.tables` /
  `.eidmap` をコピーしていたため、 直近 consumer tick (≤100ms) 以降に翻訳された entity が
  snapshot の `.eidmap` に載らず、 restore 後の再 sync で重複 entity を払い出しえた。 copy
  前に現 in-memory 状態を再 persist して body と sidecar を整合させる。
- **`peer_id == 0` で sync する foot-gun を検知**: 未設定 (= 0) の node 同士が sync すると
  author 0 == self 0 が identity 翻訳に落ち #9 の衝突が再発する。 `apply_records` が
  self_peer == 0 で foreign record を apply する時に一度だけ警告する。 own-op replay は
  author == self が正しく identity なので翻訳 semantics は変えず、 設定漏れだけ surface する。
- **Content-before-Tie の配送順序ロスを解消**: entity の Tie より先に Content が別 pull で
  届くと、 確保先写像が無く skip → cursor 前進で永久ロストしていた。 未着 entity 宛の
  Content を `(author_peer, foreign_local)` 別の pending buffer へ退避し、 対応する Tie /
  Untie が写像を作った直後に drain して apply する。 Content は key 単位 LWW で他 op と
  独立なので遅延適用しても可換。 buffer は `MAX_PENDING_OPS` で bound。 回帰テスト:
  `content_before_tie_is_buffered_then_applied`。
- **foreign Delete tombstone を `.eidmap` v2 で永続化 (削除済み entity の復活を防止)**:
  `HlcStore` は永続化されず gossip-off では foreign op が local oplog に載らないので、
  reopen 後に foreign tombstone が消え、 削除済み entity が stale Tie で復活しえた。 `.eidmap`
  を v2 に拡張し各写像 entry に foreign Delete の HLC を載せて persist、 reopen 時 (peer_id は
  header から復元済み) に HlcStore tombstone を seed する。 v1 ファイルは tombstone 無しとして
  読める (後方互換)。 回帰テスト: `foreign_delete_tombstone_survives_reopen`。

### Notes / 残る制約

- 翻訳写像は **peer-local**。 oplog / sync wire には載らない (各 peer が独立に翻訳)。
- **`set_peer_id` を sync 前に必ず呼ぶこと**: multi-peer では各 node に非 0 の peer_id を
  設定する (未設定だと上記 foot-gun ガードが warn する)。
- **foreign LWW watermark の永続化は tombstone のみ**: 削除の復活 (resurrection) は v2 で
  塞いだが、 非 tombstone の per-himo watermark は依然 reopen 時に local oplog からのみ
  再構築する。 full re-pull は HLC 順序非依存で収束するので通常は問題ないが、 stale な
  非削除 Tie が partial に再配送されると一時的に古い値が載りうる (= 次の新しい op で
  上書きされる、 削除復活より軽微)。 完全な watermark 永続化は follow-up。
- **配送順序の残り**: Content は buffer したが、 **Tie より先に届いた Delete** (新しい
  Delete が entity の Tie より先に来るケース) は依然 skip する (Delete を buffer すると
  tombstone-slot LWW と順序干渉するため意図的に保留)。 一度も Tie されない content-only
  entity 宛の Content は cap まで buffer に残る。 汎用的な retry は follow-up。 なお Untie は
  確保 (`resolve_remote_eid`) 経由で slot を anchor し untie HLC を記録するので、 out-of-order
  な古い Tie は LWW で正しく弾かれる (skip ではない)。
- **cross-peer ref は phantom target を作る**: ref value は wire に target の local 部しか
  載らない (peer bits drop)。 ref が **author 以外の peer** の entity を指す場合、 翻訳は
  `(author_peer, local)` で誤った peer に解決し target table に phantom entity を払い出す。
  本物の target が後で sync されても別 slot に落ちて収束しない。 現状 **同一 peer 内 ref のみ**
  正しく sync される。 wire に ref target peer を載せる format 拡張は follow-up。
- `.eidmap` / `.tables` に per-file CRC は無い。 破損 / torn (bogus huge count 含む) は parse
  失敗 → 空 fallback + 再 sync で復旧 (open は abort しない)。 CRC 付与は将来候補。
- `gossip_remote_apply` が ON の場合の relayed append は現状 local_eid で append する。
  gossip 転送の厳密な正しさには元の foreign eid で append すべきで、 別 commit で body-eid /
  relay-eid を分離予定。 default は off。

## 0.8.18 — 2026-06-22

robustness fix 4 件。 **file format / wire format / public API いずれも不変**、
0.8.17 から再 build のみで上がれる patch release。

### Fixed

- **oplog ring buffer が production で reclaim されず 16MB で全 append が drop**
  ([#63](https://github.com/Mutafika/enchudb/issues/63) /
  [#64](https://github.com/Mutafika/enchudb/pull/64)):
  `try_reset` が `auto_reset` フラグで gate されていたが、 `set_auto_reset(true)`
  は test でしか呼ばれず production では常に no-op。 ring が一度も reset されず
  `head` が 16MB (= 既定 oplog capacity) に達した時点で以降の append が全て
  `WAL full` で drop され、 long-running writer の変更が静かに失われていた。
  gate を撤去し、 `head == checkpoint && pending == 0` の領域を無条件 reclaim。
  consumer tick / graceful drain の両経路で発火。
- **ring reset 後に書いた record が sync から無言で欠落** (#64 の回帰):
  `try_reset` が head/checkpoint だけ巻き戻して bridge cursor (`sync_ops_offset`)
  を放置するため、 reset 後の append が `_sync_ops` へ転送されず `publish_since`
  が取りこぼしていた。 `Engine::reset_sync_ops_offset()` を追加し try_reset 成功時に
  cursor も巻き戻す。 回帰テスト `records_after_ring_reset_are_still_synced` を追加。
- **readonly open が DB を dirty 化する** ([#56](https://github.com/Mutafika/enchudb/issues/56)):
  `open_readonly` でも共通 open path が無条件で clean flag を 0 に倒し msync して
  いたため、 read-only のはずの open が file を物理的に書き換え、 次回 open で
  full index rebuild を誘発して DB を太らせていた (wiki.ecdb: live ~70KB に対し
  physical 155MB)。 readonly では clean flag を一切触らない真の非破壊 open に。
  (② Drop で flush せず / ③ dirty rebuild の予約全域 zero-fill は別途。)
- **schema upsert の PK 一意性が並行で破れて重複行ができる**
  ([#60](https://github.com/Mutafika/enchudb/issues/60)):
  並行 mode で 2 thread が同一 PK を upsert すると、 両方が lookup→allocate で
  別 eid を払い出す TOCTOU で重複行ができていた。 `TableInner` の per-table
  `upsert_lock` で lookup→allocate→PK tie を直列化 (別 table は並行のまま)。
  16 thread が同一 PK を同時 upsert → 1 行になる回帰テストを追加。
  (SQL 層は独立した別 table 実装のため対象外。)

### Notes

- [#36](https://github.com/Mutafika/enchudb/issues/36) (`schema_meta_entity`
  panic) は 0.8.7 で既に撤去済みだったため、 再現シナリオを test で確認した上で
  close。 コード変更なし。

## 0.8.17 — 2026-06-10

全コード監査 (issue #57–#61) で洗い出した **設計判断不要・後方互換** な堅牢性
fix を 1 本にまとめた safe cluster。 file format / wire format 完全不変、
**0.8.16 から再 build のみで上がれる**。 header CRC 拡張 (#58①) / PK 一意性の
所有 (#60) / capacity panic の Result 化 (#59 の API 破壊部分) は format or API
変更を伴うため本 release から除外、 別途。

### Fixed

- **oplog append 失敗が silent な op 欠落になる** ([#57](https://github.com/Mutafika/enchudb/issues/57)):
  engine の tie/untie/delete 経路は `let _ = wal.append(..)` で失敗を握り潰す。
  oplog 容量到達時 (`OutOfMemory`) に **mmap body は更新されるが op が stream に
  載らない** ため、 `publish_since` で tail する peer が変更を恒久的に取りこぼす。
  完全な伝播 / 修復経路は別 issue だが、 まず**検知可能**にするため
  `OpLog::wal_full_err()` の単一地点で **1 秒 1 行** rate-limit の warning を
  emit (0.8.15 の persist warning と同温度)。 silent loss を止める。
- **`OpLog` の `pending_writes` が panic で leak** ([#58](https://github.com/Mutafika/enchudb/issues/58) ②):
  `fetch_add` → `append_inner` → `fetch_sub` 直列のため `append_inner` panic で
  counter が +1 のまま残り、 `try_reset` (`pending_writes == 0` 条件) が永久に
  発火しなくなる。 RAII ガード (`pending_guard`) で panic 経路でも均衡させ、
  `append` / `append_many` / `append_relayed` の 3 経路に適用。
- **schema sidecar の serialize がエスケープなし** ([#61](https://github.com/Mutafika/enchudb/issues/61)):
  `.schema` の format は table/column 名を `|` `;` `:` 改行・ relation `->` で
  連結するため、 名前にこれらが入ると round-trip で schema 破損。
  `TableBuilder::build()` で table 名 + 全 column 名を `validate_schema_name` で
  検証し、 予約文字を含む名前を `BadValue` で弾いて silent corruption を止める。
- **FFI が `catch_unwind` ゼロで panic が UB に化ける** ([#59](https://github.com/Mutafika/enchudb/issues/59) の安全部分):
  engine 層の panic (capacity / 破損 file / edge 値) が `extern "C"` 境界を
  unwind して越えると未定義動作。 engine に触れる 6 関数 (`open` / `create` /
  `close` / `exec` / `query` / `result_free`) を `catch_unwind` でくるみ、
  panic を error code に潰す。 result accessor 8 関数は bounds-check 済みの
  materialized data を読むだけで panic しないため guard 不要。

### Tests

各 fix の **新挙動**に対する回帰テストを追加 (= 既存テストの「壊れてない」ではなく
「fix が効いてる」を固定):

- #61 — `issue61_name_validation` 5 件 (予約文字を弾く / 正常名は通る)。
- #57 — `append_returns_err_when_full_not_panic`: 極小 capacity で満杯にして
  `append` が panic せず `OutOfMemory` の `Err` を返すことを固定。
- #58② — `pending_writes_balanced_on_append_panic`: `#[cfg(test)]` の fault
  injection で `append_inner` を deterministic に panic させ、 RAII ガードにより
  `pending_writes` が leak しない (counter が 0 に戻る) ことを検証。 旧来の
  `fetch_add → inner → fetch_sub` 直列ならこのテストは fail する。
- #59 — `guard_i32_converts_panic_to_error_without_aborting` /
  `guard_i32_passes_through_normal_return`: FFI guard が closure の panic を
  `ENCHUDB_ERROR` に潰し process を abort させない / 正常時は素通しすることを固定。

### 除外 (別 issue / 設計判断あり)

- #58① record header の CRC 拡張 — 既存 `.oplog` の CRC 再計算が必要で
  on-disk 互換に影響。 `REC_VERSION` gate 込みで別途。
- #60 PK 一意性の所有層決定 (SQL 素 INSERT 無検査 + schema upsert TOCTOU) —
  engine が atomic upsert primitive を持つか「保証しない」と明文化するかの判断。
- #59 capacity panic (`entity_set` / `content_store` / `vocabulary`) の Result 化 —
  public write API の signature 破壊を伴う。

## 0.8.16 — 2026-06-09

`HimoType::Leaf` の re-tie / remove で発生する vocab orphan (= 死蔵 vid) を
**読み取り専用**で実測する `Engine::vocab_orphan_stats()` API + CLI `.orphans`
を追加。 issue [#54](https://github.com/Mutafika/enchudb/issues/54) の scope に
合わせ **検出のみ**、 reclaim / compact は将来 release。 file format / wire
format 完全不変、 **0.8.15 から再 build のみで上がれる**。

### Added

- `enchudb_engine::VocabOrphanStats` struct (`vocab_total` / `live_vids` /
  `orphan_vids` / `live_bytes` / `orphan_bytes` + `dead_ratio()` helper)。
- `Engine::vocab_orphan_stats()` — Tag / Leaf 全 himo の `unique_values()` を
  union して live vid 集合を作り、 `(0..vocab.count())` との差を orphan として
  返す。 vocab / himo は一切変更しない pure read-only。 計算量は
  `O(vocab_total + Σ unique_values.len())`。
- CLI `.orphans` — REPL の dot command として上記 stats を表示。

### 背景

`vocab.insert` (= Leaf 用の dedup なし append) は re-tie / remove で旧 vid を
回収しないため、 long-lived な curated store (= 元ソースから rebuild しない
タイプ、 例: opyula の memory / room store) で vocab data が単調増加。 opyula
`wiki.ecdb` は live 45 entity に対し物理 155 MB (~3.4 MB/entity) と観測され
ており、 大半が orphan と推定 (= この API で初めて実測可能に)。

### scope 外 (follow-up)

- vocab `compact()` API: live vid だけ残して data / offsets / lookup index を
  詰め直す reclaim 経路。 oplog watermark との grace (= 未消化 WAL が旧 vid を
  replay に要する間は free 不可) が前提。 別 issue で扱う。
- 大規模 DB 向けの BitVec / streaming 化: 現実装は `Vec<bool>` で
  `vocab.count()` bit 確保するため、 vocab 1B vid で 1 GB RAM。 巨大 DB は
  別 issue で対応。

## 0.8.15 — 2026-06-04

ENOSPC 起因の warning スパムと sidecar 破損時の DB 読取不能 (issue
[#52](https://github.com/Mutafika/enchudb/issues/52)) を fail-readable / self-heal
で根治。 file format / wire format 完全不変、 **0.8.14 から再 build のみで
上がれる**。

### Fixed

- **persist warning の rate-limit** (= ターミナル不能化): `try_persist_tables`
  の失敗時 `eprintln` を **1 秒 1 行**に rate-limit (`Engine.last_persist_warn_ms:
  AtomicU64` + CAS 前進)。 disk full 等で consumer thread が毎 batch 失敗 →
  ターミナルに warning が秒間数百件流れる現象を抑止。 メッセージ末尾に
  ` (rate-limited to 1/s)` を付与して抑止中であることを明示。
- **sidecar 破損で全 DB 読取不能** (= 致命的 fail-closed):
  - `.tables` (engine sidecar) parse 失敗を `InvalidData` で識別、
    `.tables.corrupt-<unix_ts>` に rename して退避 → anonymous fallback で
    engine open を続行 (`crates/enchudb-engine/src/engine.rs`)。
  - `.schema` (schema sidecar) も同様に parse 失敗を catch、
    `.schema.corrupt-<unix_ts>` に rename → 下流の legacy blob / engine
    synthesize fallback に流す (`crates/enchudb-schema/src/lib.rs`)。
  - これまで `.schema` 破損は `Database::open` 全体を fail させていたため、
    sinfo / opyula 等の consumer が **disk full からの recovery 後も DB を
    全く開けなくなる** 状態に陥っていた。 今回 fail-readable 化で engine の
    `list_user_tables` から table 定義を再 synthesize できるようになり、
    破損 sidecar を「警告 + 退避ファイル」 として処理して継続。
- **`.tables.tmp` / `.schema.tmp` self-heal**: open 時に残骸 tmp file を明示削除。
  既存 persist 経路は `truncate(true)` で上書きするため通常は不要だが、 disk
  full → recovery 後の確実な clean state を保証する safety net。

### 影響範囲

すべての user が恩恵。 特に:
- 高頻度 write workload (= opyula / sinfo / suzukapulse 等 ingest 系) が
  ENOSPC を踏んでも terminal を失わない。
- disk full → recovery で `.schema` 破損が起きた DB を、 再 deploy なしで
  そのまま再 open 可能 (= synthesize で table 復元)。

### 残課題 (将来 release)

- mmap body (`.ecdb`) は default 16M entities で予約サイズ ~25 GiB (sparse)。
  ENOSPC で touched page が全 reserve に行き渡って疎→密に化けるため、
  default を小さく (= `create` の最小値を採用) するか lazy growth に
  変更する検討は別 issue 推奨。
- WAL append が ENOSPC で partial write を残す可能性 → atomic-rename と同等の
  fsync-before-checkpoint pattern は維持されているが、 head pointer の rollback は
  入っていない。 mid-record corruption は WAL recover (`OpLog::recover`) が
  最後の commit までで truncate するので read path には影響しない。

## 0.8.14 — 2026-06-03

`TableBuilder::cardinality(n)` を追加。 schema 列に distinct 値数の hint を渡せる
ようになり、 **その列を group key にした集計 (`group_sum` / `group_min` /
`group_max` / `histogram`) が dense + 並列 fast path に乗る** ([#46])。

これまで `Database::build()` は全列を `define_himo(.., 0)` で定義していたため、
engine の `group_dense_cap` が常に `None` を返し、 table 層の group 集計が HashMap
fallback + 並列無効に固定されていた (= 1M 行の全件 GROUP BY で ~5.2ms)。
`.cardinality(20)` で hint を渡すと dense `acc[g] += v` path に乗り、 同 workload で
**~443µs (seq dense) / ~152µs (par dense)** に短縮、 in-process DuckDB
(~580µs–1.6ms) を上回る。 `examples/vs_db.rs` の 4-way bench (enchudb / sqlite /
duckdb / lmdb) で GROUP BY を含む全 10 項目を enchudb が制覇。

cap=0 (= hint 未指定) の挙動は不変で **後方互換 100%、 既存コードは影響なし**。
reopen でも `max_values` は engine metadata に persist + restore されるため、 build
時に `.cardinality()` を渡せば hint は保持される。

file format / wire format 完全不変、 **0.8.13 から再 build のみで上がれる**。

### Added

- `enchudb_schema::TableBuilder::cardinality(n)` — 直前に宣言した列の cardinality
  hint。 `BucketCylinder` の size hint になり、 `n ≤ 65536` のとき group dense path
  を有効化する。
- `examples/vs_db.rs` に LMDB (heed, in-process mmap KV) を追加して 4-way 化
  (enchudb / sqlite / duckdb / lmdb)。
- `examples/group_sum_cap_probe.rs` — cap 有無での `group_sum` 速度差の probe。

### 既知の制約

- dense path は `n ≤ 65536` (= `cyl_max_values`) の low-cardinality group key 限定。
  高 cardinality group key では HashMap path のまま ([#46] で議論)。
- `Table::add_column` / reopen 経路は依然 cap=0 で define するが、 build 時に
  `.cardinality()` を渡せば reopen は engine 側 persist で hint を保持する。

[#46]: https://github.com/Mutafika/enchudb/issues/46

## 0.8.13 — 2026-06-03

`TableBuilder::build()` を reopen 時 idempotent に。 issue
[#50](https://github.com/Mutafika/enchudb/issues/50) で sinfohub-server が踏んだ
crash-loop bug を根治。 file format / wire format 完全不変、 **0.8.12 から再
build のみで上がれる**。

### Fixed

- **`TableBuilder::build()` が "already exists" を recoverable に扱わない** (=
  migration crash): `load_schema` (`crates/enchudb-schema/src/lib.rs:622-630`)
  は `define_table` の `"already exists"` を recoverable として handle してたが、
  public `TableBuilder::build()` 経路は同じ error を bail させていた非対称。
  multi-tenant shared-pool で deploy 越しに engine sidecar (`.tables`) が
  table 定義を蓄積していく場合、 schema blob (`.schema`) と divergence した
  状態で `db.table("foo").build()` が
  `define_table(foo) failed: table 'foo' already exists` で fail して
  server crash-loop に陥っていた (sinfohub production で観測)。
  既に `v0.8.2-flush-patch` branch に `5ebc5b6` として fix 済だったが master
  に merge 漏れ。 今回 cherry-pick で master に取り込み。
- regression test を `crates/enchudb-schema/tests/issue50_build_idempotent.rs`
  に追加 (= 修正前 fail 確認済み、 修正後 pass)。

### 影響範囲

`Database::open` (or `open_with_oplog`) 直後に `db.table(...).build()` を呼ぶ
migration / re-declare pattern を使う user のみ。 single-shot `Database::create`
+ declare + finalize の通常 flow は影響なし。

## 0.8.12 — 2026-06-03

**CRITICAL data-loss fix**。 sync 経路で foreign peer から届く Tie / Content op を
apply する際、 `next_local` を前進させていなかったため、 後続の `entity_in` が
既に live な local id を再払出 → user の新規 save が既存 entity を上書きする
silent data loss。 issue [#47](https://github.com/Mutafika/enchudb/issues/47) で
bisquit が踏んだ症状。 0.8.5 以降の全 sync user に影響、 0.8.12 へ即時更新を推奨。

file format / wire format 完全不変、 **0.8.11 から再 build のみで上がれる**。

### Fixed

- **`remote_tie_apply` / `remote_content_apply` が `next_local` を進めない** (=
  data loss): WAL recover 経路 (`apply_oplog_op`) は 0.8.1 で
  `advance_table_next_local_for` を呼ぶよう修正済みだが、 sync 経路
  (`remote_*_apply`) には同 fix が入っておらず非対称だった。 結果として、
  Mac↔Android 等の peer sync で foreign eid が `entities.ensure_live(local)`
  された後も `table.next_local` が古いまま据置 → 次の `entity_in` が
  `fetch_add(1)` で live local を払出し、 schema 層の `tie_value` で既存 entity
  に被せ書きしてしまう。 両 `remote_*_apply` に
  `Self::advance_table_next_local_for(&self.tables, local)` を 1 行追加して根治。
  regression test を `crates/enchudb-engine/tests/issue47_remote_tie_next_local.rs`
  に追加 (= 修正前 `new_locals=[20, 21, 22]` collide with `foreign_locals=[20, 21]`
  で fail、 修正後 fresh local を返して pass)。

### 影響範囲

sync mode (`open_with_oplog` + `enable_sync`) で、 自 peer が複数の foreign peer
から記録を受信している環境のみ。 単一 peer / 非 sync 利用は無影響。 production
で bisquit (Mac-Android 2 peer) が「Share Intent で URL 追加するたびに 1 件
silent loss」 として観測した重大 bug。

## 0.8.11 — 2026-05-31

`transfer_oplog_to_sync_ops` の **lock 不在** と **自己再帰 sync 循環** の
2 件の sync bug を fix。 stress_10k_cycle test の flaky 化 (= 0.8.0 以降ずっと
潜在化) を根治。 production sync running で `_sync_peers` / `_sync_ops` 自身の
write が peer に飛ばないよう正しく除外される。 file format / wire format
完全不変、 **0.8.10 から再 build のみで上がれる**。

### Fixed

- **`transfer_oplog_to_sync_ops` の race condition** (= 重複転送): 0.8.0 で
  `concurrentize_with_oplog` の background consumer thread が自動 transfer を
  呼ぶようになったが、 手動呼び出しと並列実行すると `from = sync_ops_offset.load`
  → records pull → row insert → `offset.store` の 4 step が race し、 同じ
  records が複数回 row insert される (= reclaim 後の残骸として残る) bug。
  `Engine` に `transfer_lock: Arc<Mutex<()>>` を追加し、 transfer 全期間を排他化。
  lock 競合は per-fsync 頻度 (= 100ms 周期) で hot path 影響なし。

- **自己再帰 sync 循環** (= `_sync_peers` / `_sync_ops` 自身の write が queue に
  入る): `ack_sync` の watermark update が `_sync_peers` table への row write、
  および `transfer_oplog_to_sync_ops` 自体が `_sync_ops` table への row write
  を生み、 これらが WAL に積まれて次の自動 transfer で `_sync_ops` queue に
  入る循環構造だった。 結果として:
  - `_sync_peers` 残骸が peer に sync record として配信される (= local-only
    state なのに transmit、 設計上意味なし)
  - `_sync_ops` 自身が無限ループ的に self-transfer される可能性
  - reclaim 後の残骸蓄積 (= stress_10k_cycle の `final_pending` が ~2500 に
    膨れる根本原因)

  fix: transfer 内の records loop で `_sync_ops` / `_sync_peers` の eid_range に
  入る `Tie` / `Untie` / `Delete` / `Content` op を skip。 `Commit` (= barrier)
  と `Vocab` (= global sync 必須) はそのまま通す。

### Tests

- `crates/enchudb-engine/tests/destructive_0_7_0.rs` の `stress_10k_cycle`:
  手動 transfer の `transferred >= 10_000` assert を `pending_sync_ops().len()`
  ベースに書き換え (= 0.8.0+ semantics で 手動 transfer の return value は race
  で 0 になりうるため)。 10/10 連続実行で安定 pass。

### 互換性

- **file format / wire format 完全不変**
- **0.8.10 から再 build のみで上がれる**
- 既存の `transfer_oplog_to_sync_ops` 公開 API は不変、 内部実装のみ変更
- ack の `_sync_peers` write は変わらず local 永続化される (= 復旧用)、
  ただ peer に sync record として配信されない (= 正しい挙動)

### sync 経路への影響 (= positive)

- production sync running で peer 間 traffic から無駄な `_sync_peers` /
  `_sync_ops` records が消える (= sync queue の純度向上)
- reclaim path の残骸蓄積が解消、 long-running sync で 安定的に sync queue
  size が抑えられる


## 0.8.10 — 2026-05-31

#43 対応。 **Schema `Query` の終端に集計 chain API を追加**。 `where_*` で絞った
sub-set に対する scalar 集計 (`min` / `max` / `sum` / `count_col`) と GROUP BY
集計 (`group_sum` / `group_min` / `group_max`) と `histogram` が、 schema 層の
fluent chain として完結できるようになった。 これまで sub-set 集計をしたいアプリは
engine 直叩き (= cylinder + `pull_himo_stored_many_into` + 手書き loop) に堕ちて
いたが、 schema が責務として持つ layout / 並列化を吸収できる。 file format / wire
format 完全不変、 **0.8.9 から再 build のみで上がれる**。

### Added (Engine、 9 API)

既存 eids 版 (`sum` / `count` / `min` / `max` / `group_*`) は seq のみだったので、
0.8.10 で並列版を追加 + histogram_eids 系を新規追加:

- `Engine::sum_eids_par(himo, eids) -> u64`
- `Engine::count_eids_par(himo, eids) -> u32`
- `Engine::min_eids_par(himo, eids) -> Option<u32>`
- `Engine::max_eids_par(himo, eids) -> Option<u32>`
- `Engine::group_sum_eids_par(group, sum, eids) -> Vec<(u32, u64)>`
- `Engine::group_min_eids_par(group, val, eids) -> Vec<(u32, u32)>`
- `Engine::group_max_eids_par(group, val, eids) -> Vec<(u32, u32)>`
- `Engine::histogram_eids(himo, eids, vmin, vmax, n_buckets) -> Vec<u32>` (seq)
- `Engine::histogram_eids_par(himo, eids, vmin, vmax, n_buckets) -> Vec<u32>`

### Added (Schema `Query` 終端、 8 API)

`where_*` chain の終端として:

- `Query::count_col(col) -> Result<u32>` — sub-set 内で col が tie された数
- `Query::sum(col) -> Result<u64>`
- `Query::min(col) -> Result<Option<u32>>`
- `Query::max(col) -> Result<Option<u32>>`
- `Query::group_sum(group, sum) -> Result<Vec<(u32, u64)>>`
- `Query::group_min(group, val) -> Result<Vec<(u32, u32)>>`
- `Query::group_max(group, val) -> Result<Vec<(u32, u32)>>`
- `Query::histogram(col, vmin, vmax, n_buckets) -> Result<Vec<u32>>`

使用例 (= suzukapulse dominance v3 の書き換え想定):

```rust
// 0.8.9 までは engine 直叩きが必要だった (= schema 層を素通り)
// 0.8.10:
tel.where_eq("session", sess).min("speed")?
tel.where_eq("session", sess).group_min("driver", "speed")?
tel.where_eq("session", sess).histogram("speed", 0, 360, 30)?
tel.where_range("speed", 100, 300).group_max("lap_no", "elapsed_ms")?
```

### 実装方針

- 並列化は `eids.par_chunks(16k)` で chunk 並列、 stored_slice 直 view を
  indirect access (= `col[eid_local(e)]`) で scatter read
- `_range_par` (= sequential SIMD) と違い cache-unfriendly な scatter access に
  なるが、 thread 並列度で稼ぐ
- 閾値 `PAR_RANGE_THRESHOLD = 64_000` 未満では seq fallback (= API 透明)
- Schema `Query` 終端は `find()` で eids を取得 → engine `_eids_par` に bind の
  薄い wrapper、 col 名は `{table}.{col}` の himo 名に解決

### Tests

- `crates/enchudb-engine/tests/eids_par_aggregations.rs` (7 件): par == seq、
  連続 eid 集合では `_range_par` と一致、 飛び飛び (= 不連続) 動作、
  histogram edge case、 閾値以下で seq fallback。
- `crates/enchudb-schema/tests/query_aggregations.rs` (6 件): scalar /
  GROUP BY / histogram の各終端、 空 sub-set、 unknown col の Err、
  `where_range` との組み合わせ。

### 互換性

- **file format / wire format 完全不変**
- **0.8.9 から再 build のみで上がれる**
- 既存 `Query::find` / `find_one` / `count` / `limit` は変更なし
- 既存 eids 版 `Engine::sum/min/max/group_*` は引き続き seq 版として使える

### Reference

- [#43] design: enchudb-schema の Query 層に集計 chain API を追加


## 0.8.9 — 2026-05-31

#39 対応。 bulk column scan の **rayon 並列化** 系 API を `_par` suffix で追加。
12M row scan で `min_range` / `max_range` が **9-15x** 高速化、 `histogram_range`
が **6.2x**、 reduce 系 (`sum` / `count`) が 2x。 callsite が並列で OK と分かって
る場面 (= 大規模 read-only scan、 suzukapulse / mlbpulse の analytical hot path)
向け。 file format / wire format 完全不変、 **0.8.8 から再 build のみで上がれる**。

### Added (Engine、 9 API)

- `Engine::sum_range_par(himo, lo, hi) -> u64`
- `Engine::count_range_par(himo, lo, hi) -> u32`
- `Engine::min_range_par(himo, lo, hi) -> Option<u32>`
- `Engine::max_range_par(himo, lo, hi) -> Option<u32>`
- `Engine::group_sum_range_par(group, sum, lo, hi) -> Vec<(u32, u64)>`
- `Engine::group_min_range_par(group, val, lo, hi) -> Vec<(u32, u32)>`
- `Engine::group_max_range_par(group, val, lo, hi) -> Vec<(u32, u32)>`
- `Engine::range_scan_par(himo, lo, hi) -> Vec<EntityId>`
- `Engine::histogram_range_par(himo, lo, hi, vmin, vmax, n_buckets) -> Vec<u32>`

### 実装方針

- 閾値 `PAR_RANGE_THRESHOLD = 64_000` 要素未満では **seq fallback** (= 既存
  `_range` API を呼ぶ)。 thread spawn overhead が利益を上回らないため、 callsite
  は規模を意識せず `_par` を呼んで良い (= API として透明)。
- chunk 粒度は `16_384` 要素 (= 64KB、 L2 cache friendly)。 `par_chunks` または
  `chunk index` の `par_iter` 経由で並列。
- HimoStore は内部に `RwLock<BucketCylinder>` を持つが、 `stored_slice` は
  immutable な mmap view (= read 中 lock 不要) なので thread-safe。
- group 系の sparse path (= HashMap merge コストが重い) は seq fallback。
  dense path のみ並列化 (= thread-local `Vec<u64>` で scatter add → reduce で
  要素ごと加算)。

### 実測 (12M row、 M2 Max、 12 hardware thread)

| query | seq | par | speedup |
|---|---|---|---|
| `sum_range` | 1.3 ms | 0.7 ms | 1.96x |
| `count_range` | 1.2 ms | 0.7 ms | 1.68x |
| `min_range` | 16.6 ms | 1.8 ms | **9.33x** |
| `max_range` | 8.5 ms | 0.6 ms | **14.79x** |
| `group_sum_range` (8 group) | 193.8 ms | 184.9 ms | 1.05x |
| `group_min_range` (8 group) | 198.7 ms | 188.3 ms | 1.06x |
| `group_max_range` (8 group) | 162.1 ms | 157.4 ms | 1.03x |
| `range_scan` (hit ~10%) | 9.3 ms | 4.9 ms | 1.90x |
| `histogram_range` (10 bucket) | 38.6 ms | 6.3 ms | **6.17x** |

`group_*` 系は cap = 8 で per-chunk acc 構築 + reduce merge の orchestration cost
が支配的、 並列メリットが小さい (regress していないので OK 扱い)。 `min`/`max`
は branch ありで NEON auto-vec が効かないため seq が遅く、 並列化で局所 reduce
ができて大爆速。 `sum`/`count` は 12M row を 1ms で完走するほど NEON が効いて
おり、 並列化の orchestration が 50% 食う。

### Tests

- `crates/enchudb-engine/tests/range_par.rs` (8 件): par 結果が seq と一致、
  閾値以下で seq fallback、 空範囲 / 大規模での動作確認。

### bench

- `examples/par_scan_bench.rs`: 12M row で 9 query の seq / par 比較を一発実行。
  `cargo run --release --example par_scan_bench` で再現可能。

### 期待 impact (= suzukapulse / mlbpulse)

- suzukapulse dominance: lap 別 column scan の min/max 系が dominant cost
  だったので、 9-15x の improvement で全体 1.95s → 数百 ms 級になる見込み。
- mlbpulse 球種別 max velo / 投手別 min ERA: 同様に大幅改善。

### Reference

- [#39] perf: bulk column scan の rayon 並列化


## 0.8.8 — 2026-05-31

#38 対応。 0.8.6 の `sum_range` / `group_sum_range` pattern を **min / max /
group_min / group_max / histogram** に拡張。 suzukapulse / mlbpulse で callsite
に散在していた手書き min/max loop を engine primitive に集約できる。 file
format / wire format 完全不変、 **0.8.7 から再 build のみで上がれる**。

### Added

- `Engine::min_range(himo, lo, hi) -> Option<u32>` — `[lo, hi)` eid 範囲の
  column 直 scan で最小値を求める。 全 missing なら None。 stored 形式の
  `0 = missing` を skip する以外は branchless tight loop。
- `Engine::max_range(himo, lo, hi) -> Option<u32>` — 同様に最大値。
- `Engine::group_min_range(group, val, lo, hi) -> Vec<(u32, u32)>` — 2 column
  lockstep scan で group 別 min。 `group_sum_range` と同じ dense / sparse
  切替 (= `group_dense_cap` 経由)。 dense path では `mins_stored[g] != u32::MAX`
  を「データ有り」判定に使い、 seen tracking を省略。
- `Engine::group_max_range(group, val, lo, hi) -> Vec<(u32, u32)>` — 同様に max。
  dense path は `maxs_stored[g] != 0` を判定に使用。
- `Engine::histogram_range(himo, lo, hi, vmin, vmax, n_buckets) -> Vec<u32>` —
  値域 `[vmin, vmax]` を `n_buckets` 等分した頻度ヒストグラム。 範囲外の値は
  drop (clip ではない)、 戻り値長は常に `n_buckets`。 `n_buckets == 0` /
  `vmin > vmax` のときは空 Vec。
- Schema `Table` API:
  - `Table::min(col) -> Option<u32>`
  - `Table::max(col) -> Option<u32>`
  - `Table::group_min(group, val) -> Vec<(u32, u32)>`
  - `Table::group_max(group, val) -> Vec<(u32, u32)>`
  - `Table::histogram(col, vmin, vmax, n_buckets) -> Vec<u32>`

  いずれも `sum` / `group_sum` と同じく、 `table_eid_range` を auto-bind して
  engine の `_range` primitive を呼ぶだけの薄い wrapper。

### Tests

- `crates/enchudb-engine/tests/range_min_max_histogram.rs` (8 test): 基本動作、
  値 0 を含むケース、 dense path、 部分範囲、 histogram edge case
  (`n_buckets == 0` / `vmin > vmax` / 値域外 drop / `vmin == vmax`)、
  eids 版 `min` / `max` との整合性。
- `crates/enchudb-schema/tests/aggregations_min_max_histogram.rs` (5 test):
  Table API wrapper の基本動作、 空 table、 group_min / group_max、 histogram
  基本、 histogram edge case、 sum / count_col との整合性。

### Performance impact

- suzukapulse dominance (lap 別 segment min / corner max): callsite の per-lap
  手書き loop が 1 関数呼び出しに圧縮可能。 30〜50% の追加短縮見込み (= [#38])。
- mlbpulse (球種別 max velo / 投手別 min ERA 等): 同様に手書き loop 撲滅。
- 詳細 bench は次 patch で計測予定。

### Reference

- [#38] feat: group_min / group_max / histogram の column scan primitive 追加


## 0.8.7 — 2026-05-30

schema 永続化を **`.schema` sidecar に移行**、 0.6.x 以来の `schema_meta_entity`
(= anonymous entity に blob を載せる方式) を撤去。 mlbpulse 等の engine 直構築
DB を `Database::open` した時の panic を根治。 file format / wire format 完全
不変、 **0.8.6 から再 build のみで上がれる**。

### Fixed

- **engine 直構築 DB を `Database::open` で開くと panic** (= mlbpulse の 4.5M
  pitch DB で表面化): 旧実装は `__enchu_schema_meta__` marker himo の存在を
  前提に `ensure_schema_entity` で `eng.entity()` (= anonymous) を呼んでいたが、
  `define_table` 後 anonymous は close されており panic していた。
  `.schema` sidecar に永続化方式を切替えることで marker 依存を撤廃、
  engine 直 DB でも `.tables` + himo_types から fallback 復元できる
- **schema 永続化が anonymous entity 経由だった構造問題**: 0.7.0 で table API
  が engine 層に降り `.tables` sidecar 化された時点で本来撤去すべき残骸 (=
  issue note "schema_meta_entity は 0.7.0 の名残") を解消

### Added

- **`.schema` sidecar** (= `{db_path}.schema`): schema 情報を tmp file → fsync →
  rename で atomic write、 `Database::create` / `finish_*` / Drop で書き出し、
  `Database::open` で読み込み。 PK / column type / relations を完全に持つ
- **`Engine::db_path() -> &str`**: schema crate が sidecar path を組み立てる用
- **`Engine::himo_count() / himo_name_at(idx) / himo_type_at(idx)`**: engine 直
  DB からの schema synthesize fallback で iterate するための accessor
- **`Engine::fk_refs_for_table_named(name)`**: relation 復元用 accessor

### Changed (= 旧 API 全部内部撤去、 公開 API 不変)

- `Database` struct から `marker_himo_id` / `schema_meta_entity` field 削除
- `SCHEMA_META_HIMO` / `SCHEMA_BLOB_HIMO` / `SCHEMA_MARKER` 定数を `LEGACY_*`
  にリネーム (= 0.8.6 以前で書かれた DB 読み込み path 専用)
- `ensure_schema_entity` 削除 (= panic source 撤去)
- `Database::create*` / `open*` / `wrap_concurrent` の `define_himo(SCHEMA_META_HIMO, ...)`
  撤去
- `TableBuilder::build` の eager `ensure_schema_entity()` 呼出撤去

### Migration (= 自動)

| 経路 | 動作 |
|---|---|
| 0.8.7 で新規作成 | `.schema` sidecar に保存 |
| 0.8.6 以前で作成 → 0.8.7 で open | legacy blob 読み込み → 次 persist で `.schema` sidecar に migrate |
| engine 直構築 → 0.8.7 で open | engine `.tables` + himo_types から fallback 復元 (= **PK は不明扱い**、 schema rebuild で upsert 可) |

### Unchanged

- file format / wire format / `WireRecord` encode / HLC / signature 完全不変
- 0.8.6 で追加された Table::sum / group_sum / count_col API 不変
- sidecar fsync coalesce (= 0.8.2) の挙動も不変

### test

- workspace 全体: **439 passed / 0 failed / 26 ignored** (= 0.8.6 比 +2)
- 新規 `engine_built_db_open.rs` 2 件: engine 直 DB の Database::open / 新規 DB
  の `.schema` sidecar 書き出し検証

### 0.8.6 consumer 向け migration

なし。 `cargo build` で 0.8.7 binary。 既存 DB は自動 migrate (= legacy blob
読み込み → `.schema` sidecar 書き出し)。 mlbpulse のような engine 直 DB は
fallback 復元で開けるようになる。

## 0.8.6 — 2026-05-30

table-scoped 集計の primitive と schema API を整備、 vs DuckDB bench を
3-way で実測。 「DuckDB に負ける」 と言われていた **範囲 BETWEEN / SUM** を
enchudb 上回りに反転、 GROUP BY だけ scatter write の NEON 制約で
DuckDB に届かず (= 別 algorithmic work)。 **file format / wire format 完全
不変、 0.8.5 から再 build で上がれる**。

### Added — Engine 層 (primitive)

- **`Engine::sum_range(himo, lo, hi) -> u64`**: `[lo, hi)` eid 範囲の
  column 直 scan で sum。 stored_slice (= mmap u32 view、 zero-alloc) を
  branchless tight loop で reduce、 LLVM が NEON 4-wide SIMD に
  auto-vectorize。 1M rows / M2 Max で ~100µs
- **`Engine::group_sum_range(group_himo, sum_himo, lo, hi)`**: 同 range で
  GROUP BY + SUM。 dense cap 経路は acc[g] += v の scatter accumulate
  (= NEON native scatter なし、 algorithmic 制約あり)
- **`Engine::count_range(himo, lo, hi) -> u32`**: stored != 0 を branchless
  cast で popcount
- **`Engine::range_scan(himo, lo, hi) -> Vec<EntityId>`**: column 直線 scan
  で範囲 filter (= BucketCylinder reverse union を避ける fast path)。 hit 率
  高い range query で 18x 高速化
- **`Engine::table_eid_range(name) -> Option<(u32, u32)>`**: table 名で
  eid range を引く schema 連携用

### Added — Schema 層 (= README 推奨 user-facing API)

- **`Table::sum(col) -> u64`**: 当該 table の column 合計。 内部で
  `engine.sum_range(table_himo, eid_range_lo, eid_range_hi)` に bind
- **`Table::group_sum(group, sum) -> Vec<(u32, u64)>`**: 同じく
  group_sum_range に bind
- **`Table::count_col(col) -> u32`**: count_range に bind

→ user code は 1 行: `orders.sum("amount")` / `employees.group_sum("dept", "salary")`

### Added — 基盤 primitive

- **`Column::values_u32() -> &[u32]`**: packed mmap → u32 slice view (= zero
  copy、 pointer cast)
- **`HimoStore::stored_slice() -> &[u32]`**: stored 形式 (0 = missing) のまま
  callsite に露出。 SIMD 集計の入口

### Bench (= 真の vs DuckDB)

- `examples/vs_sqlite.rs` を `examples/vs_db.rs` に rename + DuckDB
  in-process (= duckdb crate bundled feature) を追加。 旧 stale な
  `crates/enchudb-engine/examples/battle_vs_duckdb.rs` (= CLI subprocess、
  公正でない) は削除
- 9 query を schema 層 API 経由で 3-way 比較 (= enchudb / sqlite / duckdb)
- **8/9 で enchudb 勝利** (= filter / lookup / 範囲 / SUM / COUNT / MIN/MAX)、
  GROUP BY のみ DuckDB が 8x ↑ (= scatter write 制約、 別 work)

### Measurements (M2 Max / 1M rows / same thermal state)

| query           | 0.8.5     | 0.8.6     | duckdb     | 変化         |
|-----------------|----------:|----------:|-----------:|--------------|
| 範囲 BETWEEN    |  14.62ms  |  897µs    |   7.96ms   | **18x ↑** (DuckDB を 8.9x 上回り) |
| SUM (table)     |  ~1.65ms  |   99µs    |   508µs    | **30x ↑** (DuckDB を 5x 上回り) |
| GROUP BY        |  9.70ms   |  12.24ms  |   1.47ms   | ~noise (DuckDB 未達) |

### Unchanged

- file format / wire format / 公開 API (= 既存 sum / group_sum / where_range
  は不変、 新 API は追加)
- 0.8.5 で追加された sync vocab dedupe / query_by_id peer prefix 不変

### 0.8.5 consumer 向け migration

なし。 `cargo build` で 0.8.6 binary。 bisquit / sinfo / suzukapulse / mlbpulse
等の consumer も再 build のみで上がれる。 集計が遅かった code は
`Table::sum` / `Table::group_sum` に書き換えで 10-100x 改善見込み。

## 0.8.5 — 2026-05-30

sync 経路の 2 件の bug fix patch release。 bisquit (dogfood) の Mac ↔ Android
mesh sync で表面化した amplification loop と、 schema 層の `where_eq().find_one()`
が壊れた eid を返してた cast bug を fix。 file format / wire format / 公開 API
変更なし、 **0.8.4 から再 build のみで上がれる**。

### Fixed

- **#30 `apply_one::DecodedOp::Vocab` の HLC dedupe 欠落** (= bisquit dogfood で
  amplification loop): 旧 behavior では同じ vocab record の再受信を毎回
  `applied++` 扱いし、 `gossip_remote_apply` ON 構成で WAL 再追記 → 再 publish
  → 再受信 の cycle に見える状態だった。 受信前に `Engine::has_remote_vocab`
  で `(author_peer, vid, bytes)` 一致を check、 既登録なら `skipped++` に振り分け
- **#32 `Engine::query_by_id` の peer prefix 落ち** (= schema 層 `where_eq` 系の
  PK lookup が壊れた eid 返却): 旧 behavior は `query_resolved -> Vec<u32>` を
  `as EntityId` (= u32→u64 widen) で変換、 高 32bit (= peer_id) が 0 のままで
  `engine.get(eid, ...)` 等が dangling になる。 `entities_with_himo` と同じ
  `make_eid(self.peer_id(), e)` で peer prefix を付与

### Added

- **`Engine::has_remote_vocab(author_peer, remote_vid, bytes) -> bool`** public
  API: 受信 vocab record の dedupe 判定用 (= sync crate が呼ぶ)。 `(author_peer,
  remote_vid)` が `peer_vocab_map` に登録済みかつ map 先 local_vid の bytes が
  受信 bytes と一致するなら true

### test

- workspace 全体: **437 passed / 0 failed / 26 ignored** (= 0.8.4 比 +4)
- `enchudb-engine/tests/query_by_id_peer_prefix.rs` 2 件 (= #32 検証)
- `enchudb-sync/tests/vocab_dedupe.rs` 2 件 (= #30 検証)

### Unchanged

- file format / wire format / `WireRecord` encode 形式 不変
- 0.8.4 で追加された `create_growable_with_options` / 同一 himo bulk column
  scan API は無触
- HLC / signature / pubkey_fp layout 不変

### 0.8.4 consumer 向け migration

なし。 `cargo build` で 0.8.5 binary になる。 bisquit / sinfo / suzukapulse /
mlbpulse 等の consumer は再 build のみで上がれる。 `gossip_remote_apply(true)`
構成は 0.8.5 以降で amplification loop 解消。

## 0.8.4 — 2026-05-25

`Database` / `Engine` から `vocab_data_size` を明示できる
`create_growable_with_options` を公開。`Leaf` 列の値も `vocab.insert`
経由で vocab data に積まれる仕様のため、大規模 text を持つアプリ
(議事録 / 論文 / 全文 archive 系) で `create_growable*` 系の default
512 MiB cap に当たって `vocabulary.rs:175` で panic していたが、
明示指定で回避可能になった。新規 method の追加のみで既存 API には
触らない (後方互換 100%、再 build のみで上がれる)。

### Added

- `Engine::create_growable_with_options(path, max_entities, vocab_data_size)`
- `Database::create_growable_with_options(path, max_entities, vocab_data_size)`

setagaya-pwa の世田谷区議会議事録 archive (4,844 会議 / 554,092 発言 /
466,383 theme、本文 vocab ~531 MB) で検証。`vocab_data_size = 2 GiB`
で全量 import 2.4 秒 (230K rows/sec) 完走、search レスポンス 0.8 秒。
closes #26

## 0.8.3 — 2026-05-25

`wasm32-unknown-unknown` build が 0.8.2 で壊れていた問題を 1 行修正。 native
build / 公開 API / file format / wire format は完全不変、 wasm consumer
(naruhodo/web 等) は再 build のみで上がれる。

### Fixed

- **wasm32 build E0560**: `Engine::load_from_backing` 内の `_writer_lock: None`
  初期化に `#[cfg(not(target_arch = "wasm32"))]` が漏れていた。 field 定義
  (`engine.rs:923`) は wasm32 で除外されているが、 `load_from_backing`
  (`engine.rs:1699`) の初期化は無条件で field 代入していたため、 wasm32 target
  では `struct \`Engine\` has no field named \`_writer_lock\`` で fail
  (issue #22)。 closes #22

## 0.8.2 — 2026-05-23

`Database::create → build×N → finish_with_oplog` の cold-open perf を
N table 数 linear から定数時間に圧縮。 sinfo (sinfohub-server) の multi-tenant
scope DB cold-open がボトルネックで、 60 user 同時 push の bench で 5+ 秒
latency 出てた issue #19 を fix。 file format / wire format / 公開 API
変更なし、 0.8.1 から **再 build のみで上がれる**。

### Fixed

- **`Database::create → build×N` の N×fsync 問題**: `TableBuilder::build()` の
  末尾で呼んでた `persist_schema()` (= `eng.flush()` = body msync ≒ 47ms on
  APFS) が N 回走ってた。 build 中の schema blob は誰も読まない (= `load_schema()`
  は open path でのみ) ので中間 persist は無駄。 `finish_with_oplog` /
  `finish_concurrent` / Drop の 1 箇所に coalesce
- **`define_table` / `define_himo_in` の per-call sidecar fsync**: engine 側
  `try_persist_tables()` が毎回 `f.sync_all()` を呼んでた (= 各 ~5ms × 105 call
  for N=15 × 7 col = ~600ms)。 build phase 中は `defer_tables_persist` flag で
  skip、 finish 時に 1 度 explicit に persist

### Added

- **`Engine::set_defer_tables_persist(&self, bool)` API**: build phase の
  sidecar fsync を抑止する toggle。 schema crate が `wrap_new` で立てて、
  `finish_*` / Drop で解除して explicit fsync を 1 度走らせる。 Engine 直利用
  (= schema 層なし) で叩く必要は無い、 default は false (= 既存 behavior 維持)
- 回帰防止 test `cold_open_coalesce.rs` 3 件: declare phase が 200ms 以下、
  finish 経由で schema が disk に persist、 Drop safety net が機能

### perf 確認 (M2 Max / APFS, declare phase / N=15 table × 7 col)

| | 0.8.1 | 0.8.2 | 改善 |
|---|---:|---:|---|
| declare phase | 663.9 ms | 1.1 ms | **600x** |
| per-table | 44.3 ms | 0.07 ms | 600x |
| finish | 13.6 ms | 37.6 ms | -2.8x (= 1 回に集約された fsync 分) |
| **total cold-open** | **677 ms** | **39 ms** | **17x** |

issue #19 の予測値 (~720ms → ~70ms, 10x) を更に上回る改善。 sinfo の 60 user
同時 push bench は scope DB cold-open がボトルネックだったので、 これで unblock。

### Unchanged

- file format / wire format / 公開 API 完全不変、 0.7.x / 0.8.x DB は全て open 可
- `add_column` (= alter path) は引き続き per-call で persist_schema (post-build
  なので fsync 1 回が正しい挙動)
- HLC / signature / pubkey_fp layout 不変

### 0.8.1 consumer 向け migration

なし。 `cargo build` で 0.8.2 binary になる。 sinfo / opyula 等の schema 経由
consumer も再 build のみ。

## 0.8.1 — 2026-05-22

short-lived CLI consumer 連携で表面化した recover 不完全の patch release。 sinfo
の sf CLI (= open → 1 write → drop) で entity 状態 (`next_local` + `entities`
live bitmap) が次 open に持ち越せず eid 衝突が出ていた。 file format / wire
format / API 変更なし、 **0.8.0 から再 build のみで上がれる**。

### Fixed

- **`apply_oplog_op` の recover 不完全**: Tie / Content op で `entities.ensure_live`
  + table `next_local` の max 推進が呼ばれていなかった (= 0.8.0 以前から続く
  defect)。 crash recovery (= consumer shutdown 前に kill された場合) で oplog
  replay は走るが、 entity 状態が body の himo data と整合せず、 次 `entity_in`
  が重複 eid を払い出す問題を fix
- **graceful shutdown で tables sidecar 未 persist**: 0.8.0 consumer thread の
  shutdown path は `body_msync` のみで `tables` sidecar (= `next_local` の永続化先)
  を更新していなかった。 short-lived CLI で flush(&mut) を呼ばずに drop すると
  次 open で `next_local=0` のまま戻り、 既存 eid と衝突する root cause。 shutdown
  + 周期 fsync (= 100ms) の両方で `try_persist_tables()` を呼ぶように変更

### Added

- **`Engine::persist_tables(&self) -> io::Result<()>`** public API: `Arc<Engine>`
  (= concurrent mode) でも tables sidecar を強制 persist できる。 既存 `flush(&mut)`
  が取れない context (= sinfo 等の embed consumer で long-lived process が任意
  tick で固めたい場合) 用、 wasm / memory-only では Ok(()) no-op
- **`apply_oplog_op` 内で `advance_table_next_local_for`**: recover 中に与えられた
  global eid を含む table の `next_local` を `(eid - lo) + 1` まで前進させる
  private helper、 上記 fix の実装本体

### Unchanged

- file format / wire format / 公開 API 完全不変、 0.7.0 ↔ 0.8.1 wire 互換は
  0.8.0 と同条件 (= 非互換)
- 0.7.x consumer 経路 (`pending_sync_ops` etc) は前 release 通り
- HLC / signature / pubkey_fp layout 不変

### 0.8.0 consumer 向け migration

なし。 `cargo build` で 0.8.1 binary になる。 sinfo / opyula 等の schema 経由
consumer も再 build のみ。

## 0.8.0 — 2026-05-22

sync 並走の解消 — oplog publish path 撤去 + `_sync_ops` 一本化 + ring buffer
化。 0.7.0 で並走可能化した `_sync_ops` reserved table を sync 配信の primary
source にし、 oplog は local crash recovery 専用に役割を絞る。 0.7.0 で「移行
猶予期」 として残してた並走モードを 0.8.0 で完全解消。 計画書: `notes/requests/request8.md`。

### Breaking

- **sync wire format 変更**: `_sync_ops.payload` の wire layout を
  `signature(64) + pubkey_fp(8) + signed_bytes(rest)` の concat 形式に拡張
  (= 0.7.0 では signed_bytes のみだったが、 sync 経路で署名検証できなかった
  defect を fix)。 **0.7.x peer との sync 互換は失う**
- **file format 互換**: 維持 (= 既存 v6/v7 DB はそのまま open 可能)、 ただし
  0.7.x で書いた `_sync_ops` row は 0.8.0 で peer publish 時に rejected (=
  signature 抜きで verify 不可)。 既存 row は手動 reclaim or DB 作り直しで対処

### Added

- **`Engine::transfer_oplog_to_sync_ops` の自動化**: consumer thread が fsync
  interval (= 100ms) 経過時 + shutdown 時に `sync_tables_enabled()` なら自動で
  bridge を発火。 user は手動呼び出し不要、 0.7.0 互換のため API は idempotent
  に残る
- **`TableDef.free_locals`**: reclaim で解放された local id の reservoir。
  `entity_in(table)` は free list 優先で payout (= ring buffer 化)、 `_sync_ops`
  の長期運用で eid 飽和を防ぐ
- **`enchudb_oplog::decode_sync_ops_payload(bytes)`**: `_sync_ops.payload` の
  concat 形式を `Record` に復元する公開関数。 sync crate の publish path で使う
- **`enchudb_oplog::SIGNED_PAYLOAD_HEADER_SIZE` / `SYNC_OPS_PAYLOAD_PREFIX`**:
  wire layout の sized const、 transport / sync 層が固定 offset で parse する用

### Changed

- **`Syncer::publish_since` / `publish_since_for_peer` の内部実装**: `_sync_ops`
  経由 (= `pending_sync_ops` + `decode_sync_ops_payload`) に切替。 公開 API は
  不変、 既存 consumer は再 build で済む。 `sync_tables_enabled()` 未呼出の DB
  では legacy oplog iter 経路に自動 fallback (= 0.7.x DB 互換)
- **`reclaim_sync_ops` が free list に push**: 解放 row の local id を `_sync_ops`
  table の `free_locals` に積む (= 次回 `entity_in("_sync_ops")` で再利用)
- **`Syncer.published_lsn: AtomicU32`** field 追加: 将来 (= 0.9.0) で
  watermark-driven reclaim を Syncer 経由で駆動する準備

### Unchanged

- `WireRecord` encode 形式 (= peer 間 transport の wire schema) 不変
- HLC / EntityId / PeerId / ed25519 signature / pubkey_fp layout 不変
- crash recovery semantic (= oplog commit marker + recover replay) 不変
- file magic `ECDB` / version 5 / 全 region layout 不変
- schema crate 公開 API (`Database::table().build()` 等) 完全不変

### 0.7.x consumer 向け migration

[`docs/migration-0.7.0-to-0.8.0.md`](docs/migration-0.7.0-to-0.8.0.md) (=
local 専用) に詳細あり、 要約:

- **schema 経由 consumer** (opyula / bisquit / sinfo / matcha / t5ug3 等):
  再 build で済む、 公開 API 完全不変。 `Database::enable_sync()` 呼んでいた
  consumer は自動 transfer 化により `transfer_oplog_to_sync_ops()` の手動
  呼び出しが不要に (= 残しても idempotent で no-op)
- **`enchudb-sync` 直接 consumer**: `Syncer::publish_since` の戻り値は同じ、
  ただし source が `_sync_ops` 経由に。 sync_tables_enabled な engine では
  watermark + reclaim が効くので長期運用で `.oplog` の線形成長が止まる
- **peer 同士**: 0.7.x ↔ 0.8.0 sync は wire 互換切れ、 同時に upgrade すること

## 0.7.0 — 2026-05-22

mini-RDB semantics の **actually 確立** ([issue #11](https://github.com/Mutafika/enchudb/issues/11) +
[issue #15](https://github.com/Mutafika/enchudb/issues/15))、 加えて `enchudb-schema`
に **deployment topology を隠す view layer** ([issue #12](https://github.com/Mutafika/enchudb/issues/12))
を導入。 0.5.0 で engine に追加した table API を `enchudb-schema` crate / consumer 層が
**1 度も使ってなかった** (= 死荷物)、 同時に `enchudb-oplog` が 「local 耐久 log」
と 「sync 配信 stream」 を兼任していた構造問題を一括解消。 計画書: `notes/requests/request7.md`。

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

- **`TenantView` / `TenantViewMut`** ([issue #12](https://github.com/Mutafika/enchudb/issues/12)、 PR #13) — `enchudb-schema` に「物理 layout を隠す view layer」 を追加。 `Database::tenant(name)` / `tenant_mut(name)` で tenant scope view、 `as_view()` / `as_view_mut()` で root view を取り出す。 内部で table 名に `{name}.` prefix を自動付与する薄い wrapper、 storage layout は変えない。
  - 不変式: `tenant("alice")` から取った view を pattern A (centralized container) でも pattern B (per-user DB ファイル) でも同じ closure で操作できる。 deployment topology を app に隠す。
  - 既存 API は完全不変、 追加のみ。
  - overhead: `tenant().get_table()` ≈ 50 ns/op (format! 1 回)、 `as_view().get_table()` ≈ 18 ns/op、 raw 7 ns baseline。 schema-layer `get_table` は hot path じゃない (起動時 1 回引いて handle 保持) ので実用上 0 影響。
  - example: `crates/enchudb-schema/examples/tenant_view_demo.rs`
  - test: `crates/enchudb-schema/tests/tenant_view.rs` (6 件、 invariant / isolation / round-trip / multi-tenant scenario / interleaved build-read / root view)
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
