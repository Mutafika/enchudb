# Glossary

EnchuDB の用語集 (2026-05-25 / 0.8.3 時点)。 各 entry は **layer タグ** +
**短い定義** + **see-also** の 3 要素。

- **§1〜§10** は **layer 別の用語索引** (= 単語の意味を素早く引く)
- **§11** は **メンタルモデル** (= 物理メタファ階層、 card / deck / pile / table の picture)
- **§12** は **混同しやすい用語** (= layer をまたいで同形語が衝突する reading 事故対策)

文脈 doc は [architecture.md](architecture.md)、 個別 detail は各 crate の
`README.md`。

---

## 1. data model (engine core)

| 用語 | layer | 定義 |
|---|---|---|
| **entity_id (eid, u32)** | engine | row id 相当の primary 識別子。 `0..max_entities` の固定空間に bitmap で生存管理。 EntityId 全体は `(peer_id:32, local_id:32)` の u64 だが、 engine 内 hot path は local 部分の u32 だけ扱うことが多い |
| **himo (紐, u16)** | engine | column 相当の属性軸。 「entity に値をぶら下げる ひも」 のメタファ。 文字列名 → u16 id を `HimoRegistry` で解決 |
| **value (u32)** | engine | tie 1 個の右辺。 解釈は `HimoType` 依存 (Number は値そのまま、 Tag/Leaf は vid、 Ref は target eid) |
| **tie / 紐 (動詞)** | engine | `(eid, himo, value)` の事実 1 個を engine に立てる操作。 Column と Cylinder を同期的に両方 update する。 metaphor では「1 枚のカードを置く」 (§11.2) |
| **untie** | engine | 既存の tie を 1 個外す。 Column[eid] = `u32::MAX` (= sentinel)、 Cylinder bucket からも remove |
| **Column** | engine | `Column[eid] = value` の forward 配列 (mmap、 4 byte/eid 固定)。 「この eid の値?」 を O(1) で返す |
| **Cylinder (= BucketCylinder)** | engine | `Cylinder[value] = Vec<eid>` の inverse bucket (mmap)。 「この値の eid は誰?」 を O(1) で返す。 全 column が index 等価 = `CREATE INDEX` 不要の原理。 mental model 詳細は §11、 「cylinder」 が指す異なる軸 slice の disambig は §12.5 |
| **HimoStore** | engine | 1 himo = 1 Column + 1 Cylinder のペア。 全 himo 分が `himo_slots[himo_id]` として layout される |
| **HimoRegistry** | engine | `himo_name (String) ↔ himo_id (u16)` の双方向 mapping。 hash table 不使用、 線形探索で十分速い (himo 数は数百が上限) |
| **Vocabulary (vocab)** | engine | 文字列 → `vid (u32)` の dedupe 辞書。 Tag himo の値はここを経由 |
| **vid** | engine | Vocabulary の id。 同じ string は同じ vid (Tag 系)、 Leaf は dedupe しないので別 vid |
| **FreeStore** | engine | Leaf himo 用の dedupe しない文字列領域。 同じ string でも tie ごとに別 vid |

### HimoType (= value の格納方式、 0.9.0 で `ValueType` rename 候補)

現 `HimoType` は名前と実態が乖離してる (= 「Himo の型」 を declare してるように見えるが、 実は「value の格納方式」 を選んでる)。 0.9.0 で **`ValueType` rename 候補** + 各 variant の見直しあり (§12.10)。

直交 2 軸を 1D enum に conflate した構造:

| variant | 値の表現 | storage 戦略 | 典型 |
|---|---|---|---|
| `Number` | 生 u32 (= 直値) | inline | id / age / flag / enum index |
| `Tag` | vid (= 文字列経由) | **dedupe あり** (= vocab 共有、 hub 紐) | 分類 / city / shared label |
| `Leaf` | vid (= 文字列経由) | **dedupe なし** (= per-tie 個別、 終端紐) | 本文 / メモ / 自由記述 |
| `Ref` | target eid | inline | relation / FK |

graph-role 視点では `Tag` (= 多 entity を連結する hub) と `Leaf` (= 1 entity 固有の terminal) が graph-position pair (§12.10 参照)。

---

## 2. table layer (engine, 0.5.0+)

`define_table` で engine が table を一級市民として持つ層。 0.5.0 β-light で
導入、 0.7.0 で schema crate も engine table API 経由になった。

| 用語 | layer | 定義 |
|---|---|---|
| **table** | engine | 名前付きの eid 範囲予約 + 所属 himo のメタ集合。 storage は 1 mmap で共有、 table は eid_range で論理 partition するだけ。 metaphor では「同型 deck を unroll + column 整列した jagged sparse 2D」 (§11.6) |
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
| **declared schema** | engine | table に `define_himo_in` で declare された紐 namespace (= 静的 candidate 紐 list)、 §12.4 参照 |
| **actual schema** | engine | その table 内の entity が実際に tie してる紐の union (= 動的 派生、 sparse model 由来)、 §12.4 参照 |

---

## 3. schema layer (mini-RDB)

`enchudb-schema` crate。 engine の table API 上に「2D mini-RDB」 を被せる
declarative API 層。 0.7.0 で内部実装が engine table API 経由に切り替わった
(= engine table API への app 層統合)。

| 用語 | layer | 定義 |
|---|---|---|
| **Database** | schema | `enchudb-schema` の最上位 handle。 内部 `Arc<Engine>`、 build phase で `&mut`、 runtime で `Arc<Database>` clone 共有 |
| **Table** | schema | `db.table(name)...build()` で declarative に作る 2D table 抽象。 column → himo_id を build 時に pre-resolve |
| **TableBuilder** | schema | `.integer("id").text("name").primary_key("id")` の chain API、 `.build()` で engine に commit |
| **insert / upsert / where_eq / where_range** | schema | row-shaped CRUD API。 内部で engine の id-keyed primitives (`query_by_id` / `pull_in_by_id`) を直叩き |
| **TenantView** | schema | curated, named subset of tables の handle。 `db.tenant(name)` で取得、 `{name}.` prefix で table 名を filter する string-prefix 抽象。 ⚠ 名前 leak で誤読される (= engine 内に tenant 概念があるように見える、 §12.2)、 **rename 議論中** ([issue #24]、 「DeckView」 案は撤回 — §11.3 で deck が entity-cylinder 専用に確定したため。 候補は `Drawer` / `Scope` / `Namespace` 系で再検討中) |
| **TenantViewMut** | schema | build phase 用 (= prefix 付きで table を建てる)。 rename 連動予定 |
| **as_view / as_view_mut** | schema | root scope の view (= prefix なし、 全 table 見える)。 名称は rename 後も不変予定 |
| **finish_with_oplog / finish_concurrent** | schema | build phase → runtime phase 遷移。 前者は oplog + concurrent writer、 後者は concurrent のみ |
| **open_with_oplog** | schema | 既存 DB を直接 concurrent + oplog で reopen、 recovery 込み |

[issue #24]: https://github.com/Mutafika/enchudb/issues/24

---

## 4. oplog + crash recovery

| 用語 | layer | 定義 |
|---|---|---|
| **oplog (旧 WAL)** | oplog | `{path}.oplog` の append-only log。 命名は WAL だが実態は MongoDB の oplog (= mmap が primary state、 log は配信 + audit + recovery)。 0.6.0 で crate 名 `enchudb-wal` → `enchudb-oplog` ([issue #8](https://github.com/Mutafika/enchudb/issues/8)) |
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
| **Pattern A (centralized)** | dist | 1 中央 DB に全 user / tenant を multi-tenant で集約。 schema 層 TenantView (rename 議論中、 §3) で scope 分離。 SNS / メールサーバー 等 |
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

## 11. メンタルモデル (= 物理メタファ階層)

enchudb の中身を物理的に思い描くための統一 metaphor。 全 layer は同じ
3D 空間 (= `himo × value × eid`) を異なる軸 / scale で切ったもので、 矛盾しない。

### 11.0. 全体俯瞰 — 3D 空間としての engine

engine の本質は `(himo, value, eid)` 3 軸からなる **3D 空間**。 `tie(eid, himo, value)` は 1 つの 3D 点を立てる操作。 各種データ構造 (Column / Cylinder / 各種 view) は、 この 3D 空間を異なる軸で slice した index に過ぎない。

全体像のメタファは **モンジャラ** (= 蔓 = 紐、 entity = 蔓の交差点に隠れた本体)。

### 11.1. 4 階層 metaphor

| 階層 | metaphor | enchudb 実体 | pivot 軸 |
|---|---|---|---|
| **card** | 1 枚の暗記カード (表 = 紐名、 裏 = 値) | 1 tie = `(himo, value)` ペア 1 個 | (1 点) |
| **deck** | 1 軸を pivot にした card の束 (= ring 構造) | 後述 (entity-deck と pile の dual) | 1 軸固定 |
| **table** | 同型 deck を unroll + column 整列した jagged 2D | engine の named table の物理表現 | eid + himo の 2D |
| **モンジャラ** | 全 3D 空間 (slice しない) | engine 全体 | — |

### 11.2. card の構造

```
            表 (front)              裏 (back)
          ┌──────────┐           ┌──────────┐
          │   himo   │           │  value   │
          │   "足"   │           │   "4"    │
          └──────────┘           └──────────┘

           card = 1 tie = (himo, value) ペア
```

「表 = 紐名、 裏 = 値」 は全 ValueType (Number / Tag / Leaf / Ref) で共通の構造。 ValueType は「裏 (= value) の格納方式」 の type tag、 card 構造自体には影響しない。

### 11.3. deck の dual (entity-deck と pile / value-deck)

deck は「**ある軸を pivot にして card を束ねたもの**」 の総称。 pivot 軸の取り方で 2 種類:

#### entity-deck (= mental「円柱」)

pivot = eid (= 1 entity)、 cards = (himo, value) ペア群

```
       cat の entity-deck (= 「猫の円柱」):
       ┌─────────┐
       │ 足: 4   │
       │ 読み:ネコ│
       │ 分類:哺 │
       │ ...     │
       └─────────┘
       「猫はどんな属性持ってる?」 への答え (= 全 himo を eid 軸で集めたもの)
```

#### pile / value-deck (= code `BucketCylinder` bucket)

pivot = (himo, value) ペア (= 1 軸の 1 値)、 cards = eid 名札群

```
       「分類=哺乳類」 pile (= 哺乳類山):
       ┌──────────┐
       │ "cat"    │   ← eid 名札のみ、 deck 本体は別場所
       │ "dog"    │
       │ "wolf"   │
       │ ...      │
       └──────────┘
       「(分類, 哺乳類) を tie してる entity は誰?」 への答え
```

### 11.4. duality の本質 — Column / Cylinder の物理表現

entity-deck と pile は **Column / Cylinder duality を metaphor で直接表現**:

| | entity-deck | pile (value-deck) |
|---|---|---|
| pivot | eid | (himo, value) |
| card | (himo, value) | eid 名札 |
| 質問 | 「この entity の cards?」 | 「この値を持つ entity は誰?」 |
| storage | Column[himo][eid] の集合 | Cylinder[himo][value] の bucket |
| 操作 | `get(eid, himo)` を全 himo 分 | `pull_raw(himo, value)` 1 発 |

両方とも **「1 軸固定 + 軸周りに card 群」 構造の dual**。 enchudb の中核 Column / Cylinder 双対が物理 metaphor にそのまま現れる。

### 11.5. 退化 case で duality が収束

pile に **1 件しか入ってない** (= `pull_raw(himo, value)` 結果が 1 件) と、 その pile は **物理的に 1 entity-deck を指す** ことになる。 ここで 2 つの「cylinder」 解釈が同じ object に収束:

```
両生類山 (= rare な値):
┌──────────┐
│ "frog"   │   ← 1 件
└──────────┘
   = frog の entity-deck そのものを指す (1 件 pile → 1 deck の参照)
```

「最小の cylinder = 1-deck pile」 で 2 つの解釈が一致。 §12.5 cylinder disambig の根拠。

### 11.6. table = jagged sparse 2D

table は **同型 entity-deck を unroll + column 整列したもの**。 ただし enchudb は **sparse data model** (= 各 entity が紐 subset 自由) なので、 strict RDB の rectangular table と違って **jagged (= 行ごとに card 数違う)**:

```
table "animals" を column 整列で展開:

       足   読み      体重   分類    犬種    尻尾長  猫種  ...
cat:    4   ネコ       5      哺乳類    —      15      三毛   ← cat の deck unrolled
dog:    4   イヌ       20     哺乳類    柴犬    30      —     ← dog の deck unrolled
wolf:   4   オオカミ    40     哺乳類    —      50      —
bird:   2   トリ       0.3    鳥類      —      —       —
                                         ↑      ↑       ↑
                                  dog 専用  共通   cat 専用
```

- 各 row = 1 entity-deck の unrolled 表示
- 各 column = 1 himo の値の縦並び (himo の Column 直読み)
- **「持ってない」 cell = card そのものが存在しない** (= NULL 値じゃない、 §12.8)

### 11.7. card 表示枚数 = query 紐数 (SQL projection と等価)

table 全体は jagged だが、 **query で「どの紐を見るか」 を指定**すると、 各 deck から **その紐の card だけ** が unroll されて返る:

```
[query: name のみ] (= 1 紐 projection)
  cat の deck:    [name:ネコ]              ← 1 card 表示
  dog の deck:    [name:イヌ]              ← 1 card
  wolf の deck:   [name:オオカミ]          ← 1 card

[query: name + 足] (= 2 紐 projection)
  cat の deck:    [name:ネコ] [足:4]      ← 2 cards
  dog の deck:    [name:イヌ] [足:4]      ← 2 cards
  wolf の deck:   [name:オオカミ] [足:4]   ← 2 cards

[query: name + 犬種] (= 2 紐 projection、 cat/wolf に 犬種 なし)
  cat の deck:    [name:ネコ]              ← 1 card だけ (犬種 card 不在)
  dog の deck:    [name:イヌ] [犬種:柴犬]  ← 2 cards
  wolf の deck:   [name:オオカミ]          ← 1 card だけ
```

= **「deck の表示枚数 = query 紐数 ∩ entity が tie してる紐数」**。 SQL projection (`SELECT name, legs FROM ...`) と同じ semantics、 ただし「持ってない」 場合の挙動が違う (= card 不在 vs NULL 値、 §12.8)。

### 11.8. クエリ視点での「mammals 横断」

「全 mammals を出して」 のような cross-entity query も card metaphor で自然:

```rust
let mammals = eng.pull_raw("分類", 哺乳類_vid);   // ns、 pile を grab
for eid in mammals {
    let weight = eng.get(eid, "体重");           // ns、 deck から 1 card 引く
    // ...
}
```

= **1 pile pull (= ns) + 各 deck から指定 card 引く (= ns × N)**。 table 跨ぎ union とか親子 relation とか **不要** (= RDB の JOIN 直感は持ち込まなくていい、 §12.9 参照)。

---

## 12. 混同しやすい用語 (= reading 事故の常連)

複数 layer で同形語が出てきて誤読される事例の disambiguation。 **本 glossary 新設の
直接の動機** (= 2026-05-25 に「TenantView」 を engine 概念と誤読された事案、
[issue #24])。

### 12.1. table

| 文脈 | 意味 |
|---|---|
| engine 層 (本 glossary §2) | 名前付き eid_range 予約 + 所属 himo メタ集合、 storage は flat だが論理 partition |
| SQL 層 (`enchudb-sql`) | SQL TABLE = row 集合の宣言、 engine 層 table に 1:1 mapping |
| schema 層 (`enchudb-schema::Table`) | declarative builder の output、 内部で engine table API を call |

3 つは **layer 違いだが同じ実体を指す**。 「table」 と言えば engine の named partition、 と読んで大体合う。

### 12.2. tenant

| 文脈 | 意味 |
|---|---|
| **engine 層** | **存在しない** (`grep -ri 'tenant' crates/enchudb-engine/src/` = 0 hit) |
| schema 層 (`TenantView`) | string-prefix-based table 名 filter の use-case 名。 「tenant」 という engine 概念があるわけではない |
| deployment (Pattern A) | 1 中央 DB に複数 user / org を host する運用パターン |

⚠ **engine に tenant 概念は無い**。 「per-table eid_range が multi-tenant の基盤になってる」 という設計動機の話は true だが、 「engine の中に tenant という名前の concept がある」 は **false**。 [issue #24] で `TenantView` rename 検討中 (= 名前 leak 解消)。

### 12.3. view

| 文脈 | 意味 |
|---|---|
| SQL VIEW | `CREATE VIEW v AS SELECT ...`、 保存された query (= 仮想 row 集合)。 enchudb-sql では **未実装** |
| schema 層 `TenantView` | curated subset of tables の handle (= table 集合への lens)。 row レベルの filter は持たない |
| UI / mental | 「table 一覧の見え方」 みたいな緩い「view」 (= 日本語の「ビュー」 はこれが多い) |

⚠ enchudb で「view」 と言ったら **table 集合の lens**、 SQL VIEW (row 集合) ではない。

### 12.4. schema

「schema」 は 4 義 — SQL canon と enchudb 独自で乖離する:

| 文脈 | 意味 |
|---|---|
| SQL SCHEMA | namespace (= `CREATE SCHEMA alice` で `alice.notes` の `alice` 部分) |
| `enchudb-schema` crate | mini-RDB layer の crate 名 (= declarative table builder + view layer) |
| **declared schema** | table に `define_himo_in` で declare された紐 namespace (= 静的 candidate 紐 list) |
| **actual schema** | その table 内の entity が実際に tie してる紐の union (= 動的 派生、 sparse model 由来) |

enchudb は sparse data model なので **declared と actual が乖離**する余地あり (= 多くの entity が一部の declared 紐を skip)。 **良い modeling では table 単位で declared ≈ actual** に保つのが目安、 declared ≫ actual な状況は table 分割 / 親子関係への refactor signal (関連 §12.9)。

### 12.5. cylinder

「cylinder」 は **2 つの異なる軸 slice** を指す同名語、 §11 のメンタルモデル参照:

| 文脈 | 中身 | pivot 軸 |
|---|---|---|
| mental「円柱」 (= entity-deck) | 1 entity の card 束 | eid 軸 |
| code `BucketCylinder` (= pile / value-deck) | 1 (himo, value) の eid 名札 bucket | himo + value 軸 |

両者は dual で、 **最小 case (= pile に 1 件) で物理的に同じ object に収束**する (§11.5)。 型名は `Cylinder` で固定 (= 0.x dev tree でも rename しない、 stability lock-in 前まで寝かせる)、 mental model 上は entity-deck と pile を区別して使う。

### 12.6. WAL vs oplog

| 文脈 | 意味 |
|---|---|
| 0.5.x までの code 内 | `enchudb-wal` crate、 `Wal` 型 |
| 0.6.0+ | **`enchudb-oplog` crate に rename**、 `OpLog` 型のみ。 file 拡張子も `.wal` → `.oplog`、 wire format / magic `EWAL` は binary 互換のため不変 |

「WAL」 は historical reference として残るが、 新規 doc / API は **oplog** で統一。 詳細は [issue #8](https://github.com/Mutafika/enchudb/issues/8)。

### 12.7. peer vs root

| 文脈 | 意味 |
|---|---|
| peer | sync 参加する独立 DB instance (= 1 process / 1 file)。 peer_id で識別 |
| root | 文脈による。 (a) `BlobStore` の `<blob_root>/` ディレクトリ、 (b) schema 層 view の root scope (= TenantView の prefix なし状態 = `as_view`)、 (c) HLC の `peer_id=0` (= 単独運用) |

「root」 単独では曖昧、 必ず文脈付き ("root view" / "blob root" / "root peer") で使う。

### 12.8. NULL — 値じゃなく state で表現

SQL は `NULL` を column 内に格納する value とするが、 enchudb は **NULL value そのものを持たない**。 代わりに「**tie の有無**」 で同じ semantic を表現:

| | SQL | enchudb |
|---|---|---|
| 格納方式 | row 内に NULL 値が居る | tie 自体が存在しない |
| schema 上の field 存在性 | column 必ず存在、 NULL 値で空表現 | himo が declare されてれば「存在」、 entity 個別に tie 有無 |
| 「未入力 field」 (SaaS form 等) | `WHERE col IS NULL` | `get(eid, col)` が `None` 返す |
| 3-valued logic | あり (TRUE / FALSE / NULL)、 NULL 伝搬 | 無い (2-valued)、 absence は absence、 app 側で policy 決める |
| metaphor | row に NULL カード | **card そのものが存在しない** (§11.7) |

⚠ **「enchudb に NULL は無い」 は正確には「NULL value は無い」**。 SaaS form の「未入力 field」 等の **NULL semantic** は「declare 済み himo の untie 状態」 で機能的に充足できる、 表現が違うだけ。

### 12.9. sparse single-table の落とし穴 (RDB 直感)

RDB は「NULL 多発 = 悪手」 ルールがあるが、 enchudb はそれを **そのまま当てはめてはいけない**:

| 観点 | sparse single-table (= 全部入り animals) | split (= dogs/cats/wolves 別 table) |
|---|---|---|
| query 速度 | **ns (= 影響ほぼ無し)** | ns (= 同等) |
| storage | Column の未 tie cell も sentinel で確保 (= 4 byte × N) | table 内に閉じる |
| declared schema 見通し | 全種属性が膨らむ | table 名で species 明示、 clean |
| cross-species query (= mammals 横断) | **1 pull で済む** (§11.8) | table 跨ぎ必要 |
| species 増えた時 | `define_himo_in` 追加 | 新 table 作成 + relation 整理 |

= **enchudb では sparse single-table が perf-neutral、 むしろ cross-species query が trivial**。 table 分割は **perf のためじゃなく、 名前空間 / concept 明示のため** にやる。 RDB の「NULL 多発悪手」 directive は enchudb には当てはまらない (= NULL value 持たない設計のおかげ、 §12.8 参照)。

「全部入り single-table も valid choice、 cross-axis query が頻発するなら single-table が筋」 が enchudb design principle。

### 12.10. Tag / Leaf の命名 awkward さ

`HimoType::Tag` / `HimoType::Leaf` は実態と名前が乖離してる:

| 現名 | 期待される意味 | 実態 |
|---|---|---|
| **`Tag`** | 「categorical label」 「hashtag」 のイメージ | **dedupe 済み文字列値** (= vocab 共有、 hub 紐) |
| **`Leaf`** | 「tree leaf」 「terminal node」 (graph 用語) | **dedupe なし文字列値** (= per-tie 個別、 終端紐) |

graph-role 視点では命名は半分正しい (= Leaf は graph terminal、 Tag は categorical hub) が、 「Tag」 という単語自体が dedupe semantics を伝えないので reading 事故源。

0.9.0 候補の rename direction (= breaking change で issue 別途検討):
- `HimoType` → **`ValueType`** (= 名前と実態の乖離解消、 「value の格納方式」 を直接表現)
- `Tag` → **`Himo`** (= 「canonical な紐 / hub 連結」、 原 enchudb metaphor の 紐 直承) または **`Symbol`** (= CS canonical な interned string)
- `Leaf` → 現状維持 (= graph leaf として OK、 `Himo` と pair で graph-role 対称)
- `Number` / `Ref` → 現状維持

---

## 参照

- doc 全体図: [architecture.md](architecture.md)
- API detail: 各 crate の `README.md`
- release notes: [`CHANGELOG.md`](../CHANGELOG.md)
