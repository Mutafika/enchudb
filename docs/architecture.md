# Architecture

EnchuDB は Rust で書かれた **mmap ベースの embedded DB**。 1 ファイル = 1 DB、
storage 経路に `Vec` / heap alloc を持ち込まず、 値 → eid 引きを **ns 級**で
返すことを最優先に設計されている。 同梱の crate で `Database` API (= schema
層、 2D テーブル) / SQL 層 / p2p sync (HLC + oplog relay) / RAG (BM25 + vector)
を提供。

このドキュメントは「中で何が起きてるか」 を 1 枚で掴むための略図。 個別 detail
は `crates/*/README.md` と `docs/migration-*.md` を参照。

## 1. データモデル — Column と Cylinder の双対

EnchuDB の中核は **同じ tie 事実 (= eid, himo, value) を 2 形態の inverted で
冗長に保持する** こと:

```
事実: (eid=42, himo="dept", value=3)

Column (forward, eid 軸):           Cylinder (inverse, value 軸):
  eid 0  → dept=0                     dept=0 → [eid_a, eid_b, ...]
  eid 1  → dept=2                     dept=1 → [eid_c, ...]
  eid 42 → dept=3                     dept=2 → [eid_d, ...]
  ...                                 dept=3 → [..., 42, ...]
  "この eid の値は?"  O(1)             "この値の eid は誰?"  O(1)
```

両方とも mmap 上の連続領域。 tie 1 回で同期的に両方 update される (= write 2x
だが lookup decision は 1-step、 ns 級)。 どちらかが壊れても他方から
`rebuild_cylinder` で再構築できる (= 整合性回復用の経路)。

**全 himo (= column) が自動的に Cylinder を持つ** = 全 column が index 等価。
RDB の `CREATE INDEX` 相当が不要で、 「index 無くて full scan」 が原理的に
発生しない。 storage が 2x 必要な代わりに、 等値 AND が桁違いに速い。

### 用語

| 用語 | 意味 |
|---|---|
| **entity_id (eid, u32)** | row id 相当の primary 識別子 |
| **himo (紐)** | column 相当の属性軸 (= 文字列名 → u16 id) |
| **value (u32)** | Number は値そのまま、 Tag/Leaf は vocab id、 Ref は target eid |
| **Column** | `Column[eid] = u32` の forward 配列 (mmap、 4 byte/eid 固定) |
| **Cylinder** | `Cylinder[value] = Vec<eid>` の inverse bucket (mmap) |
| **HimoStore** | 1 himo = 1 Column + 1 Cylinder のペア |

## 2. HimoType — value の意味付け

`define_himo(name, type, max_values_hint)` で 4 種類:

| HimoType | value 解釈 | dedupe | 典型用途 |
|---|---|---|---|
| `Number` | u32 そのまま (Cylinder で値域 bucket) | — | id / age / status / flag |
| `Tag` | vocab id (= 文字列 dedupe あり) | あり | enum / カテゴリ / 名前 |
| `Leaf` | vocab id (= 文字列 dedupe なし) | なし | 本文 / メモ / 自由記述 / 任意 bytes |
| `Ref` | target entity_id (= FK) | — | relation / graph edge |

Tag と Leaf は同じ vocab 機構を使うが、 **Leaf は dedupe しない** (= 同じ文字列を
2 回 tie すると別 vid)。 本文系で dedupe が無意味で、 むしろ「他の row と
text が一致して逆引きされるのは prevent したい」 ケースのため。

## 3. table layer (0.7.0 hot path)

**table = 名前付きの eid 範囲予約** ([request7](../notes/requests/request7.md)、
0.7.0 で schema crate が table API 経由になった)。

```rust
eng.define_table("users", size_hint=10_000)?;
// → eid_range_lo .. eid_range_hi を予約、 以降 entity_in("users") はこの範囲内
eng.define_himo_in("users", "name", HimoType::Tag, 0)?;
eng.define_himo_in("users", "age", HimoType::Number, 100)?;
eng.entity_in("users")?;  // → eid 払出 (AtomicU32 で CAS-safe)
```

table の物理:
- **eid_range_lo .. eid_range_hi**: u32 partition、 row が table 内部に閉じる
- **next_local: AtomicU32**: table 内で次に払出す local id (= range 内 monotonic)
- **fk_refs**: Ref himo の target_table 一覧 (= FK validation)
- 永続化: `.tables` sidecar に "TBL1" magic + 全 table の (name, range, himos)

`schema crate` (= `Database` / `Table` API) はこの table 機構の上に「2D mini-RDB」
を実装。 公開 API は `Database::table("posts").number("id").tag("title").build()`
のように chain。 0.7.0 で内部実装が engine table API 経由に切り替わり、 全
row が `entity_in(table_name)` で eid_range 内に収まる。

### reserved table (`_*`)

`_` 始まり名は internal 専用 namespace。 user の `define_table` は弾く、
engine 内部 (= `define_reserved_table`) のみ作れる。 用途:

- `_sync_ops` — sync 配信用 op stream (= 0.7.0 opt-in、 後述 §6)
- `_sync_peers` — per-peer watermark

user 向け列挙は `list_user_tables()` (= reserved を除外)。 `list_tables` は全
table を返す low-level API。

### anonymous table (legacy 互換)

0.5.0 以前は table 概念が無く、 全 entity が 1 つの flat 空間にあった。 0.5.0+ は
「最初の `define_table` を呼ぶ瞬間まで anonymous table が open」 という挙動。
既存 v6 DB を 0.7.0 で open すると anonymous は close され、 過去 row は eid
不変で読める、 新規 row は table 内 eid_range から払出。 詳細:
[`migration-0.6.0-to-0.7.0.md`](migration-0.6.0-to-0.7.0.md)。

## 4. file 全体図

```
{path}            メイン DB     (mmap MAP_SHARED)
{path}.oplog      op log         (mmap MAP_SHARED、 ring buffer)
{path}.tables     table sidecar  ("TBL1" magic、 0.5.0+)
{path}.crc        region CRC     (seal_integrity 時)
{path}.db.lock    writer 排他    (flock LOCK_EX)
<blob_root>/      大 blob 外出し (content-addressed、 optional)
```

DB 本体 (`{path}`) は **1 つの mmap 領域** に全 region が並ぶ:

```
┌──────────── header (256 B) ─────────────┐
│ magic="ECDB" / version=5 / layout 情報   │
├──── 固定 cluster (create 時に確定) ───────┤
│ entities      (bitset + free list)       │  ← eid 割り当て
│ vocab_offsets / vocab_index              │  ← string → vid
│ himoreg_*                                │  ← himo_name → himo_id
│ content_index                            │  ← entity の free text
│ himo_slots[himo_id] = Column + Cylinder  │  ← 全 himo の双対構造
├──── 可変 cluster (末尾、 grow) ───────────┤
│ vocab_data       ↑                       │  ← string バイト列
│ himoreg_data     ↑                       │  ← himo 名のバイト列
│ content_data     ↑                       │  ← free text 本体
└──────────────────────────────────────────┘
```

- **固定 cluster** は `create_with_capacity(path, max_entities)` 時の layout
  計算で物理位置が決まる。 ここの max_entities が上限。
- **可変 cluster** は `GrowableMap` 経由で write に応じて mmap を拡張。 file
  size が伸びる (= mmap remap)。
- Cylinder 内部の `positions[eid] = (bucket_id, idx)` 逆引き表は **lazy on-demand
  grow** (v0.2.2+)。 旧 eager allocation で 200 himos × 128 MB = 25 GB heap だった
  問題を解消、 RSS 5 MB まで縮んだ。

## 5. query path

EnchuDB の query path は `query_column_filter` 一本 (= 4c28c29 で旧 bitmap
分岐の dead path を撤去、 詳細 §9)。

多条件 AND の流れ:

```
WHERE dept=3 AND status=1 AND age=20

1. 各 cond の Cylinder bucket slice_len を測る
     → dept=3: 50K, status=1: 200K, age=20: 20K
2. 最小 slice (= pivot) を選ぶ
     → age=20 (20K eids)
3. pivot の Cylinder bucket を pull (= 20K eid の配列)
4. 残り cond は **Column 直読み**で per-eid filter
     → for eid in pivot: Column[dept][eid]==3 && Column[status][eid]==1
5. pass した eid を Vec に push
```

cylinder で「最も絞れる軸」 から開始 → 残りは mmap 上の Column を 7 ns/eid で
直読み filter、 という構造。 RDB 的「絞り込みが効くほど速い」 が桁違いに効く
理由はここ。 vs SQLite で等値 AND が 100-275x 勝つ。

**得意 / 苦手**:

| 形状 | 挙動 | 例 |
|---|---|---|
| 等値 AND | **Cylinder pull + Column filter** で爆速 | `WHERE dept=3 AND status=1` |
| 等値 1 個 | Cylinder bucket そのまま return (memcpy 律速) | `WHERE dept=3` |
| COUNT / SUM / GROUP BY | aggregate は engine 直叩き、 SQLite に 14-90x | `SUM(salary) GROUP BY dept` |
| 連続 range | Cylinder の min..=max を線形走査 (= B-tree leaf walk に負ける) | `WHERE age BETWEEN 30 AND 40` |
| LIKE / 部分一致 | 未対応 (= 別 index 機構必要、 0.x 段階では yagni) | — |

ベンチ数値の最新値は [`benches/README.md`](../benches/README.md)。

## 6. oplog と sync

`{path}.oplog` は append-only log。 命名は WAL だが**実態は MongoDB の oplog**
(= mmap が primary state、 log は peer 配信 + audit + crash recovery 用)。 0.6.0 で
crate 名も `enchudb-wal` → `enchudb-oplog` に rename ([issue #8](https://github.com/Mutafika/enchudb/issues/8))。

役割:

1. **crash recovery** — open 時に commit 済み record を replay、 未 commit 末尾は drop
2. **peer sync 配信** — 0.7.0 までの primary publish source (legacy)
3. **audit** — 「いつ誰が何を書いたか」 を `audit()` で後から閲覧

各 record:
- magic / version
- LSN (u64、 process 内 monotonic)
- HLC (wall_ms, logical, peer_id) — 真の record identity
- author_peer / signature / pubkey_fp (ed25519、 任意)
- op (Tie / Untie / Delete / Commit / VocabInsert / ...)
- CRC

ring buffer 構造 (= 固定容量、 checkpoint 済みの後ろを再利用)。

### sync 経路 (0.8.0+ で primary 一本化)

旧 oplog は「local 耐久 + peer 配信 + audit」 兼任で、 **全 peer が ack 済みの
地点 (watermark) を engine が知らない** ため `.oplog` が線形成長する課題が
あった ([issue #11](https://github.com/Mutafika/enchudb/issues/11))。 0.7.0 で
並走可能化、 0.8.0 で **publish path を `_sync_ops` 一本に primary 切替**。

```
0.6.0 まで:  consumer ─ publish_since ─→ oplog ring buffer (= 線形成長)

0.7.0 (並走): consumer ─ publish_since ─→ oplog (legacy)
              consumer ─ transfer ─→ _sync_ops ─→ pending_sync_ops (新、 opt-in)

0.8.0 (一本):  consumer thread が fsync 後に _sync_ops へ自動 transfer
              publish_since の内部実装が _sync_ops 経由 (= primary)
              ack 駆動 reclaim + ring buffer 化で _sync_ops 容量制限なし
              oplog は local crash recovery 専用に shrink
```

`Database::enable_sync()` で opt-in:
- `_sync_ops`: lsn / peer_id / op_type / hlc_wall_lo / payload (= peer 配信用、
  payload = `signature + pubkey_fp + signed_bytes` の concat)
- `_sync_peers`: peer_id / consumed_lsn / last_seen_at (= watermark)
- engine API: `ack_sync` / `sync_watermark` / `reclaim_sync_ops` / `pending_sync_ops`
- ring buffer: `TableDef.free_locals` で reclaim 解放 eid を再利用 (= 長期運用
  でも `_sync_ops` table が容量飽和しない)

呼ばない consumer は何も変わらない (= reserved table 自体作られない、 eid 空間も
食わない)。 詳細: [`migration-0.7.0-to-0.8.0.md`](migration-0.7.0-to-0.8.0.md)
 / [`migration-0.6.0-to-0.7.0.md`](migration-0.6.0-to-0.7.0.md)。

### peer-to-peer sync

`enchudb-sync` crate が `_sync_ops` を peer 間で relay (= 0.8.0 で oplog 直読み
撤去)。 受信側は HLC で LWW (Last Writer Wins) で merge。 mesh (gossip) と
client-server (集約) 両 topology に対応。 transport は `enchudb-transport`
(HTTP / WebSocket / HttpRelay / push hub) を切り替えて使う。

0.8.0 で wire format は変更 (`_sync_ops.payload` に signature + pubkey_fp 込み
の concat)、 **0.7.x peer との互換は失う** (= 同時 upgrade 必須)。

## 7. concurrency

**writer 1 process + reader 無制限** モデル (SQLite WAL モード相当)。

- writer 系 open は `.db.lock` に **flock(LOCK_EX)**、 engine drop で release
  (= 2 つ目の writer は **block する**、 SQLite と同じ、 timeout 無し)
- `open_readonly` は lock 取らない、 reader 同士 + writer と共存 OK
- in-process は consumer thread + WriteQueue で並列 write を受ける (= server 用、
  `concurrentize_with_oplog`)
- 0.7.0: `entity_in(table)` が `&self` 化 (= AtomicU32 CAS 払出)、 Arc<Engine>
  経由の concurrent mode から row insert で race しなくなった

詳細: [`concurrency.md`](concurrency.md)。

### crash recovery セマンティクス (v4+)

v3 以前は `undo` region で flush 済み未 commit を巻き戻していた。 v4 (= 0.4.0)
で `UndoLog` を撤去し、 oplog の commit marker で代替:

- **oplog 有効** (`create_concurrent_with_oplog` / `open_concurrent_with_oplog`):
  Commit marker が oplog に append されてない write は open 時の recover で drop
- **standalone (oplog 無効)**: 巻き戻し無し、 crash 時に途中状態が残る可能性

巻き戻しが必要な caller は oplog を有効化する。 `Engine::rollback()` は同時に
削除されており、 「過去状態の検査 / 復元」 は `snapshot_export` + audit() で
カバー。

## 8. 比較

| | EnchuDB | SQLite | LMDB | RocksDB |
|---|---|---|---|---|
| storage | mmap 単一ファイル | b-tree mmap | b-tree mmap | LSM tree |
| index | 全 column 自動 inverted (Cylinder) | b-tree `CREATE INDEX` で明示 | b-tree | LSM merge iter |
| クエリ | 値 → eid 引き O(1) + AND 爆速 | b-tree O(log N) | b-tree O(log N) | merge iter |
| schema | mini-RDB (2D table、 0.7.0+) | full SQL | none | none |
| sync | HLC + oplog/`_sync_ops` 内蔵 | external | external | external |
| writer 並行 | 1 process | 1 connection | 1 transaction | many |
| reader 並行 | 無制限 | many | many | many |
| 範囲 query | 弱い (= 線形走査) | 強い (= sorted leaf) | 強い | 強い |

**「embedded で sync ファースト + 全 column inverted」** が enchudb の差別化軸。
SQLite は同期手段を持たず litestream 等を別途要する。 LMDB / RocksDB は KV、
そもそも relation を持てない。

## 9. 設計境界 / 限界

- **max_entities が file create 時固定**: Column が `base + eid * 4` の eager
  layout に依存。 capacity を超えたら snapshot_export + create_with_capacity 再
  import で拡張するしかない (= 動的拡張は構造上不可)
- **2+ process writer 不可**: `.db.lock` で物理排他、 2 つ目は block (= 仕様)。
  multi-writer 必要なら server を 1 つ立てて clients は HTTP/WS で経由する
- **連続 range query が弱い**: Cylinder の min..=max を線形走査するので幅が広い
  と SQLite b-tree leaf walk に負ける。 等値 AND が桁違いに速い反面の trade-off
- **ALTER TABLE ADD COLUMN 弱い**: `define_himo_in(table, col, ...)` で後追い
  追加は可能だが、 既存 row には `Column[eid] = 0` (= 未 tie) で入る。 default 値
  の遡及 fill は user code 側でやる
- **oplog は local filesystem 前提**: NFS / CIFS は flock semantics 不安定
  (= SQLite と同じ)
- **bitmap word AND path は撤去済**: 0.4.x までの dead path。 0.9.0+ で per-value
  bitmap を本気で持つなら query 戦略選択を復活させる ([commit 4c28c29](
  https://github.com/Mutafika/enchudb/commit/4c28c29) 参照)
- **user table の free list / ring buffer は未対応**: 0.8.0 で `_sync_ops` のみ
  ring buffer 化。 user table の自動 reclaim 機構 (= delete 後 eid 再利用) は
  0.9.0+ で展開検討、 現状は monotonic に next_local が増える

## さらに

- **用語集 (glossary)**: [`docs/glossary.md`](glossary.md) — layer 別に用語を索引、 §11 で混同しやすい同形語 (table / tenant / view / schema 等) の disambiguation
- **API 詳細**: 各 crate の `README.md` (`enchudb-engine` / `enchudb-schema` /
  `enchudb-sync` / `enchudb-sql` / `enchudb-rag` / `enchudb-transport` / `enchudb-ffi`)
- **ベンチ**: [`benches/README.md`](../benches/README.md)
- **concurrency 詳細**: [`docs/concurrency.md`](concurrency.md)
- **migration history**: [`docs/migration-*.md`](.)
- **設計提案 (proposals)**: [`docs/proposals/`](proposals)
- **release notes**: [`CHANGELOG.md`](../CHANGELOG.md)
