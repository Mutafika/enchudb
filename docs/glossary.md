# Glossary

EnchuDB の用語集 (2026-05-25 / 0.8.3 時点、 **2026-07-05 / 0.9.0 で §11 全面改訂**)。
各 entry は **layer タグ** + **短い定義** + **see-also** の 3 要素。

- **§1〜§10** は **layer 別の用語索引** (= 単語の意味を素早く引く)
- **§11** は **メンタルモデル** (= 事実 / 予約 / ビュー の 3 レジスタ、 2026-07-05 改訂)
- **§12** は **混同しやすい用語** (= layer をまたいで同形語が衝突する reading 事故対策)
- **§13** は **命名憲法** (= 以後の命名を機械的に決める規則、 2026-07-06 制定)

文脈 doc は [architecture.md](architecture.md)、 個別 detail は各 crate の
`README.md`。

---

## 1. data model (engine core)

| 用語 | layer | 定義 |
|---|---|---|
| **entity_id (eid, u32)** | engine | row id 相当の primary 識別子。 `0..max_entities` の固定空間に bitmap で生存管理。 EntityId 全体は `(peer_id:32, local_id:32)` の u64 だが、 engine 内 hot path は local 部分の u32 だけ扱うことが多い |
| **himo (紐, u16)** | engine | **宣言された質問** (= 最小のクエリパターン)。 カードの表に印刷される名前で、 回答を 1 つも含まない — 予約レジスタの住人 (§11.2)。 文字列名 → u16 id を `HimoRegistry` で解決。 「紐」 は cord 期の鋳造語で語源 imagery は引退済み — **code token は固有名詞として凍結** (2026-07-06 確定、 憲法 §13 条 7)。 概念を語るときは 「質問」 を使う。 外殻 (schema 層) では column |
| **value (u32)** | engine | tie 1 個の右辺。 解釈は `HimoType` 依存 (Number は値そのまま、 Tag/Leaf は vid、 Ref は target eid) |
| **tie / 紐 (動詞)** | engine | `(eid, himo, value)` の事実 1 個を engine に立てる操作。 Column と Cylinder を同期的に両方 update する。 metaphor では「1 枚のカードを置く」 = その entity がその質問にこう回答した、 の記録 (§11.1)。 同時に円柱の維持費 (= 前払い、 §11.2) を支払う |
| **untie** | engine | 既存の tie を 1 個外す。 Column[eid] = `u32::MAX` (= sentinel)、 Cylinder bucket からも remove |
| **Column** | engine | `Column[eid] = value` の forward 配列 (mmap、 4 byte/eid 固定)。 「この eid の値?」 を O(1) で返す |
| **Cylinder (= BucketCylinder)** | engine | `Cylinder[value] = Vec<eid>` の inverse bucket (mmap)。 「この値の eid は誰?」 を O(1) で返す。 全 column が index 等価 = `CREATE INDEX` 不要の原理。 mental model は「前払い済の名札の山」 (二面性は §11.3)。 旧 2 義問題は解消済み (§12.5) |
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
| **table** | engine | 名前付きの eid 範囲予約 + 所属 himo のメタ集合。 storage は 1 mmap で共有、 table は eid_range で論理 partition するだけ。 モデル上は **区画予約** (§11.2)。 unroll した「表の絵」 は別概念 (= view / 展開図、 §12.1) |
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
| **TableBuilder** | schema | `.number("id").tag("name").primary_key("id")` の chain API (= variant は `ColumnType`: Number/Tag/Leaf/Ref の鏡写し)、 `.build()` で engine に commit |
| **insert / upsert / where_eq / where_range** | schema | row-shaped CRUD API。 内部で engine の id-keyed primitives (`query_by_id` / `pull_in_by_id`) を直叩き |
| **Scope (旧 TenantView)** | schema | table 名前空間の prefix レンズ。 `db.scope(name)` で取得、 `{name}.` prefix で table 名を filter する string-prefix 抽象。 tenant はこの機構のユースケース名にすぎない (docs では引き続き使ってよい)。 **2026-07-05 rename 実施** ([issue #24]、 branch `rename-tenant-scope`)。 deck 系は撤回 (deck = entity の束で確定、 §11.3)、 drawer 等の容れ物メタファも不採用 (= membership は表紙の名前由来で、 容れ物に「入れる」 操作は存在しない) |
| **ScopeMut (旧 TenantViewMut)** | schema | build phase 用 (= prefix 付きで table を建てる)。 `db.scope_mut(name)` |
| **as_scope / as_scope_mut (旧 as_view / as_view_mut)** | schema | 絞りなしレンズ (= prefix なし、 全 table 見える)。 「root」 等の階層語は不採用 — scope に木構造は存在しない |
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
| **Pattern A (centralized)** | dist | 1 中央 DB に全 user / tenant を multi-tenant で集約。 schema 層 Scope (§3) で scope 分離。 SNS / メールサーバー 等 |
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

## 11. メンタルモデル (= 事実 / 予約 / ビュー)

> **2026-07-05 全面改訂 (0.9.0)**。 旧版 (card / deck / pile / table の 4 階層) は
> 「円柱」 を entity 束に割り当てていて code (`BucketCylinder` = 値の束) と逆、
> §12.5 の 2 義問題を自作していた。 本版で 事実 / 予約 / ビュー の 3 レジスタに
> 再編し、 **デッキ = entity の束、 円柱 = 値の束** で確定。

### 11.0. 3 レジスタ — 物理に実在する情報はカードだけ

engine の本質は `(eid, himo, value)` 3 軸の **3D 空間**。 `tie` は 1 点 =
**カード 1 枚** を立てる操作。 カードの **表は印刷された質問** (= 紐名、 予約済み)、
**裏だけが手書きの回答** (= 値、 世界からきた情報)。 宇宙全体は 3 レジスタで書ける:

| レジスタ | Q&A 読み | 中身 | 実在性 |
|---|---|---|---|
| **事実** | 回答 | カード 1 枚 = 1 tie = 3D 空間の 1 点 | 唯一の情報。 1 枚ずつしか無い (§11.1) |
| **予約** (= schema 族) | 質問の宣言 | 「将来何をどう尋ねるか」 の事前書き留め | カードを 1 枚も保管しない (§11.2) |
| **ビュー** | 尋ねる (= 観測) | 参照でその場で組む束。 **全部こちらが書くクエリ — 生えてこない** | 仮想。 観測時に生まれ、 保存されない (§11.3) |

予約とビューは対立概念ではなく、 **同じ質問の 2 つの支払いタイミング** —
前払い (= 予約、 write/create 時に固定費) と都度払い (= ビュー、 観測時に組む)。
どの質問を前払いするかはクエリ (= 観測の頻度と形) が決める。

「持ってない」 = カード不在 (NULL 値ではない、 §12.8)。

歴史注記: 旧全体メタファ 「モンジャラ」 と 「紐 = ぶら下げ縄」 の imagery は
cord 期の遺物で、 card モデル確定 (2026-07-05) に伴い引退。 code token
`himo` / `tie` は固有名詞として凍結 (§1、 憲法 §13 条 7)。

### 11.1. 事実 — カードと、 カードの積み方

```
            表 (front)              裏 (back)
          ┌──────────┐           ┌──────────┐
          │   himo   │           │  value   │
          │   "足"   │           │   "4"    │
          └──────────┘           └──────────┘

           card = 1 tie = (himo, value) ペア
```

「表 = 紐名、 裏 = 値」 は全 ValueType (Number / Tag / Leaf / Ref) で共通の構造。 ValueType は「裏 (= value) の格納方式」 の type tag、 card 構造自体には影響しない。

カードは物理的にどこかに積む必要がある。 enchudb の積み方は **紐名フォルダ・
番地 (eid) 順** = code の `Column`:

- `Column[himo][eid] = value` はカードとは別の家具ではなく、 **カードの物理配置そのもの**
- この積み方の選択が 「紐名で見る観測はタダ」 を作る (= column scan が速い理由)
- 「猫のリング」 は物理には存在しない — **デッキすらビュー** (§11.3)

### 11.2. 予約 — schema 族

カード以外の engine 構造物は、 すべて 「将来何をどう観測するか」 の事前宣言:

| 予約 | 宣言内容 | code 実体 | 対価 |
|---|---|---|---|
| **円柱の前払い** | 「値で見る観測を常に即答せよ」 | `BucketCylinder` (名札の山) | tie 毎の維持費 (write hot path) |
| **table** | 番地ブロックの区画 + フォルダ品揃え (= declared schema) | `TableDef` (eid_range + himo_ids + fk_refs) | 背番号が予約ブロックから払い出され、 生後変更不能 |
| **棚サイズ** | 全体 capacity | create 時 fixed cluster layout (max_entities) | create 時確定 |
| **scope** | 表紙名の規約 (table 束ねの prefix) | `Scope` (旧 `TenantView`、 §12.2) | — |

共通性質:

- **カードを 1 枚も保管しない** = 情報ゼロ。 円柱の名札はカードから再構築可能
  (rebuild 経路が実在、 §8 AtomicBool swap)
- **無料ではない** (対価列)。 予約の粒度を細かくする戦いが 0.5.0+ の歴史
  (positions 80 GB 問題 → table-local eid range 化 等)
- declared schema (= 予約) と actual schema (= 実カード) は乖離してよい (§12.4)
  — 予約と実態だから

### 11.3. ビュー — 観測時に参照で組む束

実カードは 1 枚ずつしか無いが、 **名札 (参照) は何枚でも書ける** ので
組み合わせは無限に生まれる:

| ビュー | 組み方 | 答える質問 |
|---|---|---|
| **デッキ (暗記帳)** | eid で全フォルダから 1 枚ずつ引く (`get(eid, himo)` × N) | 「猫は何を持ってる?」 |
| **円柱 (観測面)** | 前払い済の名札の山を読む (`pull_raw(himo, value)` 1 発、 ns) | 「哺乳類は誰?」 |
| **多条件 AND** | 円柱 slice + Column filter | 「哺乳類 かつ 4 本足?」 |
| **射影** | 指定紐のカードだけ unroll (§11.5) | SQL projection 相当 |
| **展開図** | 束を unroll + column 整列した jagged 2D (§11.4) | 「表として眺める」 |

- **デッキ = entity の束、 円柱 = 値の束** (2026-07-05 確定、 §12.5)。 デッキは
  1 entity 専属、 円柱は同じ entity の名札を複数の山に置ける
  (= 猫は 哺乳類 + ペット + 4本足 に同時所属)
- **円柱の二面性**: 前払い (= 予約、 §11.2) と観測面 (= ビュー)。 維持費は write 時に
  支払い済みで、 **束としての円柱は観測の瞬間に立ち上がる**。 「観測するときに
  できるもの」 はビューの顔、 「常設の名札の山」 は予約の顔 — 同じものの両面
- デッキと円柱は Column / Cylinder 双対の metaphor 表現でもある
  (デッキ = Column[himo][eid] を eid で串刺し、 円柱 = Cylinder[himo][value] の bucket)
- 上表は分類学ではなく **定番クエリの例示** — ビューは名詞のメニューではなく、
  こちらが書く質問文 (憲法 §13 条 5: クエリは名詞化しない)
- DB 界の canon 名 (橋レジスタ): デッキ = column store の **row** (組む行為 =
  **tuple reconstruction**)、 円柱 = **inverted index**、 前払い / 都度払い =
  index・materialized view / **late materialization**

### 11.4. 展開図 = jagged sparse 2D (旧 「table の絵」)

デッキの集まりを **unroll + column 整列** した見え方。 これは **view であって
engine の table (= 区画予約) とは別概念** (§12.1) — 哺乳類円柱を unroll すれば
区画なしで 「哺乳類の表」 が得られる。 enchudb は **sparse data model** (= 各 entity が紐 subset 自由) なので、 strict RDB の rectangular table と違って **jagged (= 行ごとに card 数違う)** が正常、 長方形は特殊ケース:

```
"animals" のデッキ群を column 整列で展開:

       足   読み      体重   分類    犬種    尻尾長  猫種  ...
cat:    4   ネコ       5      哺乳類    —      15      三毛   ← cat のデッキ unrolled
dog:    4   イヌ       20     哺乳類    柴犬    30      —     ← dog のデッキ unrolled
wolf:   4   オオカミ    40     哺乳類    —      50      —
bird:   2   トリ       0.3    鳥類      —      —       —
                                         ↑      ↑       ↑
                                  dog 専用  共通   cat 専用
```

- 各 row = 1 デッキの unrolled 表示
- 各 column = 1 紐名フォルダの直読み (himo の Column)
- **「持ってない」 cell = card そのものが存在しない** (= NULL 値じゃない、 §12.8)

### 11.5. 射影 — card 表示枚数 = query 紐数 (SQL projection と等価)

展開図全体は jagged だが、 **query で「どの紐を見るか」 を指定**すると、 各デッキから **その紐の card だけ** が unroll されて返る:

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

### 11.6. 例: mammals 横断 (= 円柱 1 観測 + デッキから card 引き)

「全 mammals を出して」 のような cross-entity query も自然に書ける:

```rust
let mammals = eng.pull_raw("分類", 哺乳類_vid);   // 円柱を観測 (ns、 前払い済)
for eid in mammals {
    let weight = eng.get(eid, "体重");           // デッキから 1 card 引く (ns)
    // ...
}
```

= **円柱 1 観測 (= ns) + 各デッキから指定 card (= ns × N)**。

哺乳類 ⊃ 犬 のような分類は **内包** (= 円柱同士の集合包含) であって、 区画の境界を
「跨ぐ」 関係ではない。 跨ぎ (= union の都度払い) が現れるのは、 そのクエリパターンを
前払いせず区画側で表現した時だけ (§12.9)。 RDB の JOIN 直感は持ち込まなくていい。

---

## 12. 混同しやすい用語 (= reading 事故の常連)

複数 layer で同形語が出てきて誤読される事例の disambiguation。 **本 glossary 新設の
直接の動機** (= 2026-05-25 に「TenantView」 を engine 概念と誤読された事案、
[issue #24])。

### 12.1. table

| 文脈 | 意味 |
|---|---|
| engine 層 (本 glossary §2) | **区画予約** (§11.2) = 名前付き eid_range + 所属 himo メタ集合、 storage は flat だが論理 partition |
| SQL 層 (`enchudb-sql`) | SQL TABLE = row 集合の宣言、 engine 層 table に 1:1 mapping |
| schema 層 (`enchudb-schema::Table`) | declarative builder の output、 内部で engine table API を call |
| **メタファの「表の絵」** | **view (= 展開図、 §11.4)** — 任意の束を unroll した見え方。 区画とは**別概念** |

上 3 つは layer 違いの同一実体 (= 区画予約)。 ⚠ 4 つ目だけ別物: 「哺乳類円柱を unroll した表」 は table (= 区画) なしで作れる view。 旧版はこの 2 概念を区別せず 「同じ実体」 と書いていた (2026-07-05 訂正)。 語の割り当て: **table = 区画、 表の絵 = view / 展開図**。

### 12.2. tenant

| 文脈 | 意味 |
|---|---|
| **engine 層** | **存在しない** (`grep -ri 'tenant' crates/enchudb-engine/src/` = 0 hit) |
| schema 層 | **API からも消滅** — 旧 `TenantView` は 2026-07-05 に `Scope` へ rename 済み ([issue #24])。 tenant は Pattern A を説明する docs 上のユースケース語としてのみ残る |
| deployment (Pattern A) | 1 中央 DB に複数 user / org を host する運用パターン |

⚠ **engine に tenant 概念は無い**。 「per-table eid_range が multi-tenant の基盤になってる」 という設計動機の話は true だが、 「engine の中に tenant という名前の concept がある」 は **false**。 この名前 leak が本 glossary 新設の動機で、 rename により根治。

### 12.3. view

| 文脈 | 意味 |
|---|---|
| モデル用語 (§11.3) | **観測時に参照で組む束の総称** (デッキ / 円柱観測面 / 射影 / 展開図)。 enchudb での 「view」 の正しい既定義 |
| SQL VIEW | `CREATE VIEW v AS SELECT ...`、 保存された query (= 仮想 row 集合)。 enchudb-sql では **未実装** だが、 意味論はモデル用語と同族 (= 仮想の見え方) |
| schema 層 旧 `TenantView` | ⚠ **view ではなかった** (= 予約レジスタの scope、 §11.2)。 2026-07-05 に `Scope` へ rename 済み ([issue #24])、 view という語は仮想の見え方全般に返還 |

⚠ 2026-07-05 訂正: 旧版は 「view = table 集合の lens」 と定義していたが**逆転**。
view は 「仮想の見え方全般」 (= あなたが観測時に組む束) に返し、 table 束ねの
機構の方が view という語を明け渡す。

### 12.4. schema

「schema」 は 4 義 — SQL canon と enchudb 独自で乖離する:

| 文脈 | 意味 |
|---|---|
| SQL SCHEMA | namespace (= `CREATE SCHEMA alice` で `alice.notes` の `alice` 部分) |
| `enchudb-schema` crate | mini-RDB layer の crate 名 (= declarative table builder + view layer) |
| **declared schema** | table に `define_himo_in` で declare された紐 namespace (= 静的 candidate 紐 list) |
| **actual schema** | その table 内の entity が実際に tie してる紐の union (= 動的 派生、 sparse model 由来) |

enchudb は sparse data model なので **declared と actual が乖離**する余地あり (= 多くの entity が一部の declared 紐を skip)。 **良い modeling では table 単位で declared ≈ actual** に保つのが目安、 declared ≫ actual な状況は table 分割 / 親子関係への refactor signal (関連 §12.9)。

2026-07-05 追記: 4 義は **予約レジスタ (§11.2) の総称** と置くと入れ子に整理される
— **schema = 予約全般、 table = 予約の一種 (= 区画)、 view = 観測結果 (§11.3)**。
「schema と table の意味被り」 は被りではなく nesting。 declared / actual の乖離も
「予約と実態」 と読めば当然の現象。

### 12.5. cylinder

**解消済み (2026-07-05)**: 割り当てを確定して 2 義を廃止 —

| 語 | 束ね方 | code 実体 |
|---|---|---|
| **デッキ (暗記帳)** | 1 entity の card 束 (eid 軸) | 物理実体なし (= view、 §11.3) |
| **円柱** | 1 (himo, value) の名札の山 (値軸) | `BucketCylinder` — **一致** |

旧版は mental 「円柱」 を entity 束に割り当てていて (= code と逆)、 それが 2 義問題の
原因だった。 現行は 1 語 1 義。 円柱の二面性 (= 前払い / 観測面) は §11.3 参照。
型名 `Cylinder` はそのままで意味も一致するため、 懸案だった型 rename は不要になった。

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
| root | 文脈による。 (a) `BlobStore` の `<blob_root>/` ディレクトリ、 (b) **廃語** — 旧「root view」(= `as_view`) は 0.10.0 で `as_scope` (絞りなし) に rename、 root という階層語自体を不採用 (scope に木構造は無い)、 (c) HLC の `peer_id=0` (= 単独運用) |

「root」 単独では曖昧、 必ず文脈付き ("root view" / "blob root" / "root peer") で使う。

### 12.8. NULL — 値じゃなく state で表現

SQL は `NULL` を column 内に格納する value とするが、 enchudb は **NULL value そのものを持たない**。 代わりに「**tie の有無**」 で同じ semantic を表現:

| | SQL | enchudb |
|---|---|---|
| 格納方式 | row 内に NULL 値が居る | tie 自体が存在しない |
| schema 上の field 存在性 | column 必ず存在、 NULL 値で空表現 | himo が declare されてれば「存在」、 entity 個別に tie 有無 |
| 「未入力 field」 (SaaS form 等) | `WHERE col IS NULL` | `get(eid, col)` が `None` 返す |
| 3-valued logic | あり (TRUE / FALSE / NULL)、 NULL 伝搬 | 無い (2-valued)、 absence は absence、 app 側で policy 決める |
| metaphor | row に NULL カード | **card そのものが存在しない** (§11.4) |

⚠ **「enchudb に NULL は無い」 は正確には「NULL value は無い」**。 SaaS form の「未入力 field」 等の **NULL semantic** は「declare 済み himo の untie 状態」 で機能的に充足できる、 表現が違うだけ。

### 12.9. sparse single-table の落とし穴 (RDB 直感)

RDB は「NULL 多発 = 悪手」 ルールがあるが、 enchudb はそれを **そのまま当てはめてはいけない**:

| 観点 | sparse single-table (= 全部入り animals) | split (= dogs/cats/wolves 別 table) |
|---|---|---|
| query 速度 | **ns (= 影響ほぼ無し)** | ns (= 同等) |
| storage | Column の未 tie cell も sentinel で確保 (= 4 byte × N) | table 内に閉じる |
| declared schema 見通し | 全種属性が膨らむ | table 名で species 明示、 clean |
| cross-species query (= mammals 横断) | **円柱 1 観測で済む** (§11.6) | union 必要 (= 「跨ぎ」 が発生) |
| species 増えた時 | `define_himo_in` 追加 | 新 table 作成 + relation 整理 |

= **enchudb では sparse single-table が perf-neutral、 むしろ cross-species query が trivial**。 table 分割は **perf のためじゃなく、 名前空間 / concept 明示のため** にやる。 RDB の「NULL 多発悪手」 directive は enchudb には当てはまらない (= NULL value 持たない設計のおかげ、 §12.8 参照)。

「全部入り single-table も valid choice、 cross-axis query が頻発するなら single-table が筋」 が enchudb design principle。

2026-07-05 追記 — **区画もビューもクエリパターン、 決めるのはクエリ**: 区画 (table)
は eid range により構築上 **排他・不重複**、 分類 (哺乳類 / ペット / 4本足) は
**重複・内包** する集合で円柱が無償でくれる性質。 どちらで表現するかに正解はなく、
**そのクエリパターンをどれだけ前払いしたいか** (§11.0 / §11.2 の対価表) で決める —
per-species クエリが主なら分割は正当、 cross-species が頻発するなら single-table が
安い。 「table 跨ぎ union」 は設計の罪ではなく、 前払いしなかったクエリパターンの
都度払いコストにすぎない。 なお構造上、 円柱は必ず 1 つの table に内包される
(himo の内部名は `{table}.{himo}` で table 所属、 §2) ため、 円柱自体が table を
跨ぐことは起きない。

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

> **2026-07-05 追記: `Tag → Himo` 案は veto**。 himo (= 属性軸、 本 DB の最重要語) を
> ValueType variant 名に二重予約することになり、 TenantView と同型の reading 事故を
> 中心語で再演する。 再検討は `Symbol` (= CS 慣習語) or `Tag` 現状維持の 2 択で。
> `HimoType → ValueType` は引き続き有効な候補。

> **2026-07-06 追記 (結論)**: `Tag → Symbol` も**不採用** — Symbol は CS 文化語で
> 外殻読者に何も伝えず、 実は Tag のハッシュタグ直感 (= 同じタグを大勢が共有して
> 貼る) が dedupe / hub を正しく伝えている (冤罪だった)。 variant は全維持。
> 残る有効案は **`HimoType → ValueType` のみ**、 時期未定 (#24 の Scope rename とは
> 切り離し済み、 実施時は breaking minor に同乗させる)。

---

## 13. 命名憲法 (2026-07-06 制定)

以後の命名は本条で機械的に決める。 層ごとに基準が違うのは **意図的** (= ぶれではなく規則)。

### 3 レジスタ構造

| レジスタ | 語彙 | 基準 |
|---|---|---|
| **核** (engine 内部 + 思想 docs) | 紐・円柱・カード・質問/回答 | **正確さ** — 鋳造語は許す推論を完全に制御できる |
| **外殻** (公開 API: schema / SQL / FFI) | table・column・insert・scope | **わかりやすさ** — 採用コスト最小化 |
| **橋** (glossary・技術 docs) | inverted index・tuple reconstruction・late materialization | **接続** — 既存文献の正しい推論を借りる |

### 条文

1. **1 語 = 1 レジスタ = 1 義**
2. 語のレジスタ越境は **1:1 対応 + 本 glossary 記載** がある時のみ (例: column = 紐)
3. **機構名 > ユースケース名** (Tenant の教訓、 #24)
4. **メタファ語は核専用**、 外殻 API に出さない (deck の教訓)
5. **名詞を得るのは機構 (物理か予約) だけ**。 クエリ / ビューは名詞化しない — こちらが書くものだから
6. **質量テスト**: 候補語が読者に許す推論の **すべてが、 ここで真になるか**。 「root scope」 はこれで棄却 (= 木構造が無いのに木を主張する)
7. **識別子は文ではなく住所**: code の識別子は固有名詞で、 何も主張しないので質量テストの対象外。 識別子の rename を正当化するのは **実測の誤読事故のみ** (himo 凍結の根拠)。 構造の伴わない rename は見出しだけ進んで本体が追いつかない (wal → oplog、 0.6.0 の教訓)

### 適用実績

- `TenantView → Scope` (#24, PR #82): 条 3 違反の根治
- `DeckView` 撤回: 条 1 + 条 4 違反 (deck = entity 束と二重予約)
- `as_root_scope` 棄却 → `as_scope`: 条 6
- `Tag → Himo` veto: 条 1 (himo の二重予約)
- `himo` / `tie` / `Cylinder` 凍結: 条 7 (誤読事故ゼロ、 rename 不当)

---

## 参照

- doc 全体図: [architecture.md](architecture.md)
- API detail: 各 crate の `README.md`
- release notes: [`CHANGELOG.md`](../CHANGELOG.md)
