# Glossary

EnchuDB の用語集 (2026-05-25 / 0.8.3 時点)。 各 entry は **layer タグ** +
**短い定義** + **see-also** の 3 要素。 末尾 §11 に「混同しやすい用語」 を集約
(= layer をまたいで同形語が衝突するケース、 reading 事故の常連)。

文脈 doc は [architecture.md](architecture.md)、 個別 detail は各 crate の
`README.md`。 ここは「単語の意味だけ素早く引く」 ための索引。

---

## 1. data model (engine core)

| 用語 | layer | 定義 |
|---|---|---|
| **entity_id (eid, u32)** | engine | row id 相当の primary 識別子。 `0..max_entities` の固定空間に bitmap で生存管理。 EntityId 全体は `(peer_id:32, local_id:32)` の u64 だが、 engine 内 hot path は local 部分の u32 だけ扱うことが多い |
| **himo (紐, u16)** | engine | column 相当の属性軸。 「entity に値をぶら下げる ひも」 のメタファ。 文字列名 → u16 id を `HimoRegistry` で解決 |
| **value (u32)** | engine | tie 1 個の右辺。 解釈は `HimoType` 依存 (Number は値そのまま、 Tag/Leaf は vid、 Ref は target eid) |
| **tie / 紐 (動詞)** | engine | `(eid, himo, value)` の事実 1 個を engine に立てる操作。 Column と Cylinder を同期的に両方 update する |
| **untie** | engine | 既存の tie を 1 個外す。 Column[eid] = `u32::MAX` (= sentinel)、 Cylinder bucket からも remove |
| **Column** | engine | `Column[eid] = value` の forward 配列 (mmap、 4 byte/eid 固定)。 「この eid の値?」 を O(1) で返す |
| **Cylinder (= BucketCylinder)** | engine | `Cylinder[value] = Vec<eid>` の inverse bucket (mmap)。 「この値の eid は誰?」 を O(1) で返す。 全 column が index 等価 = `CREATE INDEX` 不要の原理。 メンタルモデルとしては「モンジャラ」 (= 蔓の交差点に隠れた本体)、 詳細は CLAUDE.md |
| **HimoStore** | engine | 1 himo = 1 Column + 1 Cylinder のペア。 全 himo 分が `himo_slots[himo_id]` として layout される |
| **HimoRegistry** | engine | `himo_name (String) ↔ himo_id (u16)` の双方向 mapping。 hash table 不使用 ([feedback-no-hash])、 線形探索で十分速い (himo 数は数百が上限) |
| **Vocabulary (vocab)** | engine | 文字列 → `vid (u32)` の dedupe 辞書。 Tag himo の値はここを経由 |
| **vid** | engine | Vocabulary の id。 同じ string は同じ vid (Tag 系)、 Leaf は dedupe しないので別 vid |
| **FreeStore** | engine | Leaf himo 用の dedupe しない文字列領域。 同じ string でも tie ごとに別 vid |

### HimoType (= value の意味付け)

| HimoType | 用途 | dedupe | 典型 |
|---|---|---|---|
| `Number` | u32 そのまま | — | id / age / flag |
| `Tag` | vocab id (dedupe あり) | あり | enum / category / name |
| `Leaf` | vocab id (dedupe なし) | なし | 本文 / メモ / 自由記述 |
| `Ref` | target entity_id | — | relation / FK |

---

## 2. table layer (engine, 0.5.0+)

`define_table` で engine が table を一級市民として持つ層。 0.5.0 β-light で
導入、 0.7.0 で schema crate も engine table API 経由になった
([engine-knows-tables])。

| 用語 | layer | 定義 |
|---|---|---|
| **table** | engine | 名前付きの eid 範囲予約 + 所属 himo のメタ集合。 storage は 1 mmap で共有、 table は eid_range で論理 partition するだけ |
| **TableDef** | engine | `(name, eid_range_lo..hi, himo_ids, fk_refs, next_local)` の内部構造体 |
| **eid_range_lo / eid_range_hi** | engine | table の eid 払出範囲 (u32 partition)。 row が table 内部に閉じる |
| **next_local: AtomicU32** | engine | table 内で次に払出す local id。 `&self` で CAS 払出可能 (0.7.0+) |
| **fk_refs** | engine | この table 内の Ref himo が指す target_table 一覧 (= FK 宣言)、 tie hot path で validate |
| **define_table** | engine | `eng.define_table(name, size_hint)` で table を 1 つ予約 |
| **define_himo_in** | engine | `eng.define_himo_in(table, himo, type, hint)` で table-namespaced な himo を作る。 内部名は `{table}.{himo}` |
| **define_ref_in** | engine | Ref himo を target table 付きで宣言、 FK validation の根拠 |
| **entity_in** | engine | `eng.entity_in(table)` で table 内の eid_range から 1 個払出 (`&self`、 CAS-safe) |
| **anonymous table** | engine | 0.5.0 以前互換の flat 空間。 最初の `define_table` を呼ぶ瞬間まで open、 以降は close。 既存 v6 DB を 0.7.0 で open すると透過 migrate |
| **reserved table** | engine | `_` で始まる名前の internal 専用 table。 user の `define_table` は弾く、 `define_reserved_table` のみ作れる。 `_sync_ops` / `_sync_peers` 等 |
| **.tables sidecar** | engine | `{path}.tables` に "TBL1" magic + 全 table 定義を binary persist (= 0.5.0+) |
| **list_tables / list_user_tables** | engine | 前者は reserved 含む全 table、 後者は user 向け (= reserved 除外) |

---

## 3. schema layer (mini-RDB)

`enchudb-schema` crate。 engine の table API 上に「2D mini-RDB」 を被せる
declarative API 層。 0.7.0 で内部実装が engine table API 経由に切り替わった
(= [engine-knows-tables] の app 統合)。

| 用語 | layer | 定義 |
|---|---|---|
| **Database** | schema | `enchudb-schema` の最上位 handle。 内部 `Arc<Engine>`、 build phase で `&mut`、 runtime で `Arc<Database>` clone 共有 |
| **Table** | schema | `db.table(name)...build()` で declarative に作る 2D table 抽象。 column → himo_id を build 時に pre-resolve |
| **TableBuilder** | schema | `.integer("id").text("name").primary_key("id")` の chain API、 `.build()` で engine に commit |
| **insert / upsert / where_eq / where_range** | schema | row-shaped CRUD API。 内部で engine の id-keyed primitives (`query_by_id` / `pull_in_by_id`) を直叩き |
| **TenantView** | schema | curated, named subset of tables の handle。 `db.tenant(name)` で取得、 `{name}.` prefix で table 名を filter する string-prefix 抽象。 ⚠ 名前 leak で誤読される (= engine 内に tenant 概念があるように見える、 [issue #24])、 **`DeckView` に rename 予定** |
| **TenantViewMut** | schema | build phase 用 (= prefix 付きで table を建てる)。 → `DeckViewMut` 予定 |
| **as_view / as_view_mut** | schema | root scope の view (= prefix なし、 全 table 見える)。 名称は rename 後も不変予定 |
| **DeckView (proposed)** | schema | `TenantView` の rename 先 (issue #24)。 「単語カードリング」 メタファ — 名前付き portable subset of tables、 use-case neutral |
| **finish_with_oplog / finish_concurrent** | schema | build phase → runtime phase 遷移。 前者は oplog + concurrent writer、 後者は concurrent のみ |
| **open_with_oplog** | schema | 既存 DB を直接 concurrent + oplog で reopen、 recovery 込み |

---

## 4. oplog + crash recovery

| 用語 | layer | 定義 |
|---|---|---|
| **oplog (旧 WAL)** | oplog | `{path}.oplog` の append-only log。 命名は WAL だが実態は MongoDB の oplog (= mmap が primary state、 log は配信 + audit + recovery)。 0.6.0 で crate 名 `enchudb-wal` → `enchudb-oplog` ([project-wal-renamed-to-oplog]) |
| **LSN** | oplog | process 内 monotonic な log sequence number (u64)。 record 順序の物理 anchor |
| **HLC** | oplog | Hybrid Logical Clock = `(wall_ms, logical, peer_id)`、 record の真の identity。 LWW (Last Writer Wins) merge の判断軸 |
| **Commit marker** | oplog | transaction 境界。 commit 済み record のみ recovery で replay される、 未 commit 末尾は drop |
| **ring buffer** | oplog | 固定容量、 checkpoint 済みの後ろを再利用する循環構造。 `auto_reset` flag (default off) で reset 可否を制御 |
| **audit** | oplog | `audit(AuditFilter)` で「いつ誰が何を書いたか」 を後から閲覧 |
| **snapshot_export** | oplog | flush + msync 済みの mmap を別 path に atomic copy (= 過去状態の dump) |
| **.crc sidecar** | oplog | `{path}.crc` に各 region の CRC を持つ optional sidecar。 cold storage の corruption 検知用 |
| **wire format** | oplog | record の on-wire 表現 (LSN / HLC / author_peer / signature / pubkey_fp / op / CRC)。 0.8.0 で `_sync_ops.payload` に signature + pubkey_fp 込みの concat 形式に変更 (= 0.7.x peer と互換切り) |

---

## 5. sync (peer-to-peer)

| 用語 | layer | 定義 |
|---|---|---|
| **_sync_ops** | sync | 0.7.0+ の reserved table、 peer 配信用 op stream の primary source。 0.8.0 で oplog 直読み撤去、 _sync_ops が唯一の publish path |
| **_sync_peers** | sync | per-peer watermark (= `peer_id, consumed_lsn, last_seen_at`) を持つ reserved table |
| **watermark** | sync | 「peer X はここまで配信済み (= ack 済み)」 の cursor。 _sync_peers が永続化、 reclaim の判断に使う |
| **ack** | sync | 受信側 peer が「ここまで処理した」 と返信する LSN。 sender が watermark 更新 |
| **LWW** | sync | Last Writer Wins。 HLC 大の record を勝たせる merge 戦略 |
| **publish_since** | sync | 「LSN X 以降の commit 済み op を流す」 配信 API。 0.8.0 で内部実装が _sync_ops 経由に primary 切替 |
| **enable_sync** | sync | `Database::enable_sync()` で opt-in、 _sync_ops / _sync_peers が初めて作られる。 呼ばない consumer は無影響 |
| **transport** | sync | peer 間 wire 配送 (HTTP / WebSocket / HttpRelay / WS push hub)、 `enchudb-transport` crate |
| **mesh / gossip** | sync | 全 peer が任意の peer と直接通信する topology (= 中央なし) |
| **client-server (集約)** | sync | 1 中央 server に全 client が集約する topology |
| **peer_id (u32)** | sync | peer の物理 identity。 0 は単独 / anonymous 運用、 1+ は分散運用 |
| **keypair (ed25519)** | sync | 各 peer の署名鍵。 oplog append 時に署名、 受信側 verify |
| **pubkey_fp** | sync | public key の fingerprint。 ACL や TOFU 登録の key |
| **ACL** | sync | writer 許可 list。 未定義なら全 peer pass、 定義時は match 必須 |

---

## 6. deployment patterns

| 用語 | layer | 定義 |
|---|---|---|
| **Pattern A (centralized)** | dist | 1 中央 DB に全 user / tenant を multi-tenant で集約。 schema 層 TenantView (= 将来 DeckView) で scope 分離。 SNS / メールサーバー 等 |
| **Pattern B (per-user DB)** | dist | user ごとに別 DB file、 1:1 で sync。 SaaS 等。 `Database::open_with_oplog` を user 数分 instance 化 |
| **Pattern C (local-first)** | dist | user の手元に全 data、 sync は optional。 client は通常 local read/write、 接続時に peer と LWW merge |

具体例: `examples/sync_centralized.rs` / `sync_per_user.rs` / `sync_local_first.rs`
(= 0.7.0 phase 6 で landed)。

---

## 7. storage primitives

| 用語 | layer | 定義 |
|---|---|---|
| **mmap / MAP_SHARED** | storage | DB 本体 / oplog ともに mmap MAP_SHARED、 reader 並行 + writer 1 の前提 |
| **fixed cluster** | storage | create 時に layout 計算で物理位置確定する region (entities / vocab_offsets / himoreg_* / himo_slots etc)。 max_entities がここを律速 |
| **variable cluster** | storage | write に応じて mmap remap で grow する region (vocab_data / himoreg_data / content_data)。 GrowableMap が管理 |
| **GrowableMap** | storage | mmap を amortized grow させる backing。 `grow_amortized` で write 時に commit を伸ばす |
| **content** | storage | entity に紐付ける検索対象外の blob (上限 512 MB/blob)。 `content() / get_content()` |
| **BlobStore** | storage | 大 blob 外出し用 content-addressed store (sha-256)。 DB ファイルとは別 root (`<blob_root>/`)、 紐の値 = `BlobId` で参照 |
| **GrowableTiny / create_growable_tiny** | storage | 軽量プリセット (max_entities=1024 / max_himos=16 / 各 64 KB)、 状態 DB 用途 (= state.db ポジション) |

---

## 8. concurrency

| 用語 | layer | 定義 |
|---|---|---|
| **writer 1 process + reader 無制限** | concurrency | SQLite WAL モード相当。 writer 系 open は `.db.lock` を flock(LOCK_EX)、 2 つ目は block |
| **.db.lock sidecar** | concurrency | `{path}.db.lock` ファイル、 flock 排他用。 Engine drop で fd close = lock release |
| **concurrentize_with_oplog** | concurrency | build 済み Engine を Arc + consumer thread + oplog 配送モードに遷移する関数 |
| **WriteQueue** | concurrency | in-process の write を consumer thread に渡す lock-free queue (SegQueue)、 oplog mode で使う |
| **AtomicBool swap** | concurrency | Cylinder rebuild のロックフリー切替に使う double buffer + atomic compare_exchange |
| **&self vs &mut self** | concurrency | API 表で峻別。 build phase は `&mut self`、 runtime は `&self` のみで concurrent 読み書き |

---

## 9. integrity / signing

| 用語 | layer | 定義 |
|---|---|---|
| **seal_integrity** | integrity | flush + msync + 全 region CRC を `.crc` sidecar に書く。 cold backup 用の封緘、 oplog 活動中は意味なし |
| **header CRC** | integrity | DB header 64 byte の FNV-1a。 open 時に必ず検証、 改竄で open エラー |
| **file size 検証** | integrity | open 時に `file_size < Layout.total_size` なら truncated と判定して reject (SIGBUS 防止) |
| **signature (ed25519)** | integrity | oplog record の peer 署名。 受信側 verify、 失敗で reject |

---

## 10. EntityId (u64) の bit 構造

```
u64 = [peer_id: 32bit][local_id: 32bit]
make_eid(peer, local) / eid_peer(eid) / eid_local(eid)
```

単独 peer 運用は `peer_id = 0`、 分散は各 peer が一意な peer_id を持つ。

---

## 11. 混同しやすい用語 (= reading 事故の常連)

複数 layer で同形語が出てきて誤読される事例の disambiguation。 **本 glossary 新設の
直接の動機** (= 2026-05-25 に「TenantView」 を engine 概念と誤読された事案、
issue #24)。

### 11.1. table

| 文脈 | 意味 |
|---|---|
| engine 層 (本 glossary §2) | 名前付き eid_range 予約 + 所属 himo メタ集合、 storage は flat だが論理 partition |
| SQL 層 (`enchudb-sql`) | SQL TABLE = row 集合の宣言、 engine 層 table に 1:1 mapping |
| schema 層 (`enchudb-schema::Table`) | declarative builder の output、 内部で engine table API を call |

3 つは **layer 違いだが同じ実体を指す**。 「table」 と言えば engine の named partition、 と読んで大体合う。

### 11.2. tenant

| 文脈 | 意味 |
|---|---|
| **engine 層** | **存在しない** (`grep -ri 'tenant' crates/enchudb-engine/src/` = 0 hit) |
| schema 層 (`TenantView`) | string-prefix-based table 名 filter の use-case 名。 「tenant」 という engine 概念があるわけではない |
| deployment (Pattern A) | 1 中央 DB に複数 user / org を host する運用パターン |

⚠ **engine に tenant 概念は無い**。 「per-table eid_range が multi-tenant の基盤になってる」 という設計動機の話は true だが、 「engine の中に tenant という名前の concept がある」 は **false**。 issue #24 で `TenantView` → `DeckView` rename 予定 (= 名前 leak 解消)。

### 11.3. view

| 文脈 | 意味 |
|---|---|
| SQL VIEW | `CREATE VIEW v AS SELECT ...`、 保存された query (= 仮想 row 集合)。 enchudb-sql では **未実装** |
| schema 層 `TenantView` / `DeckView` | curated subset of tables の handle (= table 集合への lens)。 row レベルの filter は持たない |
| UI / mental | 「table 一覧の見え方」 みたいな緩い「view」 (= 日本語の「ビュー」 はこれが多い) |

⚠ enchudb で「view」 と言ったら **table 集合の lens**、 SQL VIEW (row 集合) ではない。

### 11.4. schema

| 文脈 | 意味 |
|---|---|
| SQL SCHEMA | namespace (= `CREATE SCHEMA alice` で `alice.notes` の `alice` 部分) |
| `enchudb-schema` crate | mini-RDB layer の crate 名 (= declarative table builder + tenant scope) |
| schema declaration | CREATE TABLE / define_himo の type / column 宣言群 (= 構造定義) |

3 義あり、 文脈で判断。 「schema crate」 = crate 名、 「DB の schema」 = declaration、 「SQL SCHEMA」 = namespace。

### 11.5. cylinder

| 文脈 | 意味 |
|---|---|
| engine 内部 (`BucketCylinder`) | `Cylinder[value] = Vec<eid>` の inverse bucket index (per himo) |
| mental model (CLAUDE.md) | 「1 entity からぶら下がる全紐の束」 (= 暗記カードの束)。 内部実装とは見方が異なる |
| メタファ (モンジャラ) | engine 全体の 3D 空間 (= himo, value, eid) を「絡まる蔓 + 交差点に隠れた本体」 で表現、 [project-cylinder-is-tangela] |

型名は `Cylinder` で固定 (= 0.x dev tree でも rename しない、 stability lock-in 前まで寝かせる)、 説明用に「モンジャラ」 / 「カードの束」 を補助的に使う。

### 11.6. WAL vs oplog

| 文脈 | 意味 |
|---|---|
| 0.5.x までの code 内 | `enchudb-wal` crate、 `Wal` 型 |
| 0.6.0+ | **`enchudb-oplog` crate に rename**、 `OpLog` 型のみ。 file 拡張子も `.wal` → `.oplog`、 wire format / magic `EWAL` は binary 互換のため不変 |

[project-wal-renamed-to-oplog] 参照。 「WAL」 は historical reference として残るが、 新規 doc / API は **oplog** で統一。

### 11.7. peer vs root

| 文脈 | 意味 |
|---|---|
| peer | sync 参加する独立 DB instance (= 1 process / 1 file)。 peer_id で識別 |
| root | 文脈による。 (a) `BlobStore` の `<blob_root>/` ディレクトリ、 (b) `DeckView` の root scope (= prefix なし)、 (c) HLC の `peer_id=0` (= 単独運用) |

「root」 単独では曖昧、 必ず文脈付き ("root deck" / "blob root" / "root peer") で使う。

---

## 参照

- doc 全体図: [architecture.md](architecture.md)
- mental model + 設計原則: [`CLAUDE.md`](../CLAUDE.md)
- API detail: 各 crate の `README.md`
- migration: `docs/migration-*.md`
- release notes: [`CHANGELOG.md`](../CHANGELOG.md)
