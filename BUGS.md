# EnchuDB バグ記録

## [未修正] v32 Sync が text フィールドで根本的に機能しない (API 設計バグ + 機能穴)
**日付**: 2026-04-23 (初版) / 2026-04-23 追記 (深掘り後)
**箇所**: `engine.rs` — `open` / `create` / `tie_text_to` / `tie_async`、`sync.rs` — `Syncer::publish_since`、`wal.rs` — `WalOp`

v32 で Sync サポートが入ったが、以下 **3 層** の問題が重なり、**text を含む schema の peer 間 sync は現状動かない**。

---

### 層 1: `Engine::open` 経路は WAL 無しでも Syncer を受け付ける

Engine の open 系 API は 2 系統:

| API | WAL | mutation 呼び方 | 返り値 | Sync |
|---|---|---|---|---|
| `Engine::open(path)` / `Engine::create(path)` | **無効** | `&mut self` (tie_text 等) | `Engine` | **機能しない** |
| `Engine::open_concurrent_with_wal(path, cap)` / `create_concurrent_with_wal` | 有効 | `&self` (tie_text_to / tie_async 等) | `Arc<Engine>` | 部分的に機能 (下記参照) |

前者で開くと `write_queue=None` / `wal=None`。mutation は `himo_store` 直書き、WAL には 1 バイトも書かれない。`Syncer::publish_since` が `wal.iter_committed()` を読むが常に空 → transport.publish に 0 件 → peer 間 sync 不成立。

**現状 `sync.rs:80-86` に `Syncer::new` の panic guard が入っており (`engine.wal_arc().unwrap_or_else(|| panic!(...))`)、少なくとも silent には壊れない。ただし下流は `Engine::open` ではなく `open_concurrent_with_wal` への切り替えが必須。**

---

### 層 2: `tie_text_to` / `tie_to` / `tie_ref_to` は **&self だが WAL に書かない**

名前から「concurrent (Arc<Engine> から呼べる) 版 = WAL 経由」と誤解しやすいが、実際は:

```rust
pub fn tie_text_to(&self, eid, himo, value: &str) {
    let vid = self.vocab.get_or_insert(value.as_bytes());  // vocab 直接
    self.himos[hid].set(eid, vid);                          // himo 直書き
    self.record_undo(eid, hid);                             // undo のみ
    // WAL append は無い
}
```

同様に `tie_to` / `tie_ref_to` / `entity()` も WAL 無書き込み。`commit()` (行 1677) も `self.undo.commit()` だけで WAL は触らない。

WAL に書くのは以下 4 つのみ (すべて u32 / bytes 指向で &self):
- `tie_async(eid, himo, value: u32)` — 行 3224
- `untie_async(eid, himo)` — 行 3243
- `delete_async(eid)` — 行 3259
- `content_async(eid, key, data: &[u8])` — 行 3275

そして WAL の Commit marker を打つのは `wal_commit()` (行 3298) のみ。`commit()` と `wal_commit()` が別物で、片方だけ呼ぶと状態が割れる。

**下流が `Engine::open` → `open_concurrent_with_wal` に切り替えても、`tie_text_to` を使う限り publish_since は依然空。**

---

### 層 3: `tie_text_async` が存在しない + vocab は peer 間 sync されない

WAL に text を流す API が無い。`tie_async` は u32 のみ。`vocab.get_or_insert` は pub ではない (`engine.rs:815` で private field)。

仮に `tie_text_async` を実装しても、次の壁:

- text は vocab (peer-local ハッシュテーブル) を介して u32 `vid` に変換される
- WAL の `WalOp::Tie { eid, himo_id, value: u32 }` が運ぶのは vid のみ (`wal.rs:134-194`)
- `WalOp` に `Vocab { vid, bytes }` 的な op は無い (列挙: Tie / Untie / Delete / Content / Commit のみ)
- 結果、peer A の `vid=5 = "hello"` は peer B で何の意味も持たない。`apply_wal_op` の `Tie { value }` は `himos[hid].set(local, value)` で vid を直書きする (`engine.rs:3194-3199`) が、peer B の vocab にその vid は未登録

**text を含む schema は `tie_text` (& mut self, WAL 無し) しか書く手段がなく、かつそれは sync されない。**

---

### opyula での発症事例 (2026-04-23)

opyula (`/Users/kubo/Desktop/mutafika/opyula`) は peer entity (`pk_text=peer_id`, `name`, `pubkey_hex`, `endpoint`), room entity (`pk_text=uuid`, `name`), membership entity (`pk_text=<room>:<peer>`, `role`) すべて text フィールドに依存。Phase 7 の 2 peer smoke test で `room not found after pull (received=0)` を踏んで判明。

`opyula-core/src/room.rs:185-188` に FIXME コメント: 「opyula-core 全体を Arc<Engine> + tie_*_to API に refactor する必要あり」と書かれているが、上記 層 2・3 の事実に照らして **tie_*_to への移行だけでは解決しない**。

---

### 症状の顕在化条件

- `Engine::open` / `Engine::create` で開く (docs / README のサンプルコードが大半これ) → 層 1
- `open_concurrent_with_wal` に切り替え + `tie_*_to` で text を書く → 層 2
- Text 以外 (u32 value のみ) を `tie_async` で書く → 部分的に動く (peer B に届く)
- Text を WAL に載せる手段が存在しない → 層 3

層 1 の guard は入ったが、層 2・3 は対応なし。**ローカル単体では全て動いて見え、2 peer 繋いだ瞬間に text データが届かない現象として発覚する**。

---

### 修正方針 (案)

**Option A (API 整理、v33 破壊的):**
1. `Engine::open` を WAL 有効 + `Arc<Engine>` 返す現 `open_concurrent_with_wal` の挙動に統合。旧挙動は `open_standalone` にリネーム
2. `tie_text` / `tie_text_to` 両方を削除。代わりに単一の `tie_text(&self, ...)` が内部で vocab 登録 + WAL Tie + write_queue push を行う実装に
3. `tie_text` が `WalOp::Vocab { vid, bytes }` を先に append してから `WalOp::Tie { eid, himo_id, value: vid }` を append するように拡張
4. WAL recovery / apply_wal_op で `Vocab` op を受けたら peer B 側の vocab に `force_insert(vid, bytes)` する経路を追加 (vid は peer-local でなく、WAL 送信者側の vid をそのまま受け入れる → 衝突回避のため peer A の vid と peer B の vid は名前空間を分ける設計に)
5. 同様に `tie_ref_async` を追加 (target_eid を u64 で運ぶ)
6. `commit()` と `wal_commit()` を統合 (片方だけ呼ぶと壊れる現状を解消)
7. `Syncer::new` の WAL guard は既に有るので維持

**Option B (穴を塞がず記録のみ、短期):**
- 下流に「v32 は text sync 未サポート」と明記
- `tie_text_to` / `tie_text` のドキュメントに「sync 対象外」と追記
- 層 1 の guard が入ったので `Engine::open` 経路の silent 破綻は既に解消

**Option C (schema 側で回避):**
- 下流 (opyula) 側で text フィールドを撤廃、全て u32 の fingerprint に置き換え (例: `pk_text=uuid` → `pk_hi=u32, pk_lo=u32` の 2 himo 分割、human-readable name は content blob へ)
- content blob は `content_async` で WAL に載るため sync 可
- ただし schema 大改定 + query 側の書き換え大

---

### 影響範囲 (修正時に波及する下流 crate)

- opyula (`opyula-core/src/{peer,store,room}.rs` + `opyula-mcp/src/daemon.rs`, `crates/oboro/src/{registry,recorder}.rs`) — peer / room / membership entity すべて text 依存
- syncretic (`syncretic/src/main.rs` 他) — memory / task entity が text 依存
- sinfo (`sinfo-store-enchu` 等)

下流は「Option A が入るまで 2 peer sync は実装不能」と見做すべき。単独 peer 機能だけ先行実装する選択肢はある。

---

### 推奨

Option A を v33 で実装する。層 3 (vocab sync) が最難関 — WAL op に `Vocab` を足す + 全 peer が同じ vid 空間を共有するか、peer ごとに vocab namespace を分けて remote vid ↔ local vid mapping を持つかの設計判断が要る。

短期 (今 sprint) は Option B で silent 破綻だけ防ぎ、下流には「text sync は v33 まで待つ」と伝達。

---

## [修正済み] v27 BucketCylinder の silent value clamp(データ破壊)
**日付**: 2026-04-15
**箇所**: `cylinder_v27.rs` — `bucket_index()`、`himo_store.rs` (v27)、`write_queue.rs`、`engine.rs`(`create_concurrent`/`concurrentize`/`tie_async`)

`BucketCylinder::bucket_index` が `value > max_values` の時に最終バケットに silent clamp していた。tie した値と pull できる値が暗黙にズレるためデータ破壊の温床。さらに `max_values` が「値制限」のように誤解されていた。

- **症状**: define_himo("x", Value, 10) のあと tie(e, "x", 100) すると、pull_raw("x", 10) と pull_raw("x", 100) で同じ entity が返る。get(e, "x") は元値 100 を返すので不一致。
- **修正**:
  1. `BucketCylinder` を動的拡張(`Vec::resize`)。`bucket_index` から clamp を撤廃、範囲外は空 slice。
  2. `max_values` は「索引サイズのヒント」と再定義。任意の u32 を tie 可能。
  3. `unique_count: AtomicU32` を `HimoStore` に追加。`himo_cardinality(name)` API として公開(O(1))。
  4. `WriteQueue` を `ArrayQueue` → `SegQueue` 化(unbounded)。`create_concurrent`/`concurrentize` の `queue_cap` 引数を削除、`tie_async` の back-pressure 撤廃。

## [修正済み] Vocabulary ハッシュテーブルの無限ループ
**日付**: 2026-04-04
**箇所**: `vocabulary.rs` — `lookup()`, `index_insert()`

`DEFAULT_INDEX_CAP = 131072` が小さすぎ、大量の `tie_text` でハッシュテーブルの充填率が上がると `lookup()` の線形探索が無限ループに陥る。`Engine::create_with_capacity` では `max_entities` をキャパシティに渡すが、`Vocabulary::create()` を直接呼ぶパス（`_himoreg` 等）はデフォルト値のまま。

- **症状**: CPU 100% で停止、DB サイズ・出力が増えない
- **修正**: index_cap を十分大きく確保

## [修正済み] `get_text` が Value 型紐でゴミを返す
**日付**: 2026-04-04
**箇所**: `engine.rs` — `get_text()`

`get_text` が紐の型（Symbol/Value）をチェックせず、Value 型紐に格納された entity ID を vocab_id として Vocabulary を引いていた。無関係な語彙エントリが返る。

- **症状**: `base_form_of: ザオウオンセン`、`slot_count: ヒゲ` のようなデタラメな表示
- **修正**: `HimoType::Symbol` でなければ `None` を返す

## [修正済み] `HimoStore::open` が `max_values` を復元しない
**日付**: 2026-04-04
**箇所**: `himo_store.rs` — `open()`

`HimoStore::open()` は `max_values: 0` をハードコードする。`define_himo` で prefix sum O(1) を指定して作成した紐も、再オープン後は binary search O(log n) にフォールバックする。

- **症状**: `define_himo` + `flush` + `open` + `rebuild` 後に O(1) ルックアップが効かない
- **修正**: 単一ファイル化でヘッダに max_values を保存、open時に復元

## [修正済み] 単一ファイル化
**日付**: 2026-04-06
**箇所**: 全ファイル

複数ファイル/ディレクトリ構成から単一 `.db` ファイルに統合。Region型（生ポインタ+長さ）で各コンポーネントがmmapの一部分を参照。

- **変更**: 42ファイル → 1ファイル。ヘッダ4KB + 全Region連続配置
- **性能**: INSERT/QUERY同等、FLUSH/OPEN数十ms増（スパースファイルmsync）

## [修正済み] u32::MAX sentinel 衝突
**日付**: 2026-04-07
**箇所**: `engine.rs` — `tie()`, `tie_ref()`

Column は value+1 を格納し、0 を「紐なし」sentinel として使う。value=u32::MAX だと value+1=0（オーバーフロー）で「紐なし」と区別不能。格納した値が消える。

- **症状**: `tie(e, "x", u32::MAX)` → `get(e, "x")` が `None`
- **修正**: `tie()`/`tie_ref()` で `value < u32::MAX` を assert。u32::MAX は sentinel として予約

## [修正済み] ContentStore data overflow（サイレント破壊）
**日付**: 2026-04-07
**箇所**: `content_store.rs` — `set()`

data領域（64MB→512MBに拡大済み）を超えてcontentを書き込んでも境界チェックがなく、サイレントにデータが壊れる。

- **症状**: 大量の content 書き込みでデータ破損、検知不能
- **修正**: `set()` で data_end + len > region_len をチェック、超過時 panic

## [修正済み] open後に pull_raw が空を返す
**日付**: 2026-04-07
**箇所**: `engine.rs` — `open()`, `himo_store.rs` — `load()`

`HimoStore::load()` が `dirty=true` を設定するが、`pull_raw()` は rebuild を呼ばないため、Cylinder が空のまま検索される。flush時にどちらの Cylinder（a/b）がactiveだったか不定のため、open後は必ずrebuildが必要。

- **症状**: PLATEAU 27万件で `pull_raw("usage", 411)` → 0件（期待: 166,576件）
- **修正**: `Engine::open()` 末尾で `rebuild()` を強制実行

## [修正済み] 並列 rebuild で SIGABRT（メモリ破壊）
**日付**: 2026-04-07
**箇所**: `himo_store.rs` — `rebuild_cylinder()`

複数スレッドが同時に `rebuild_cylinder()` を呼ぶと、同じ standby Cylinder に並行書き込みが発生しメモリ破壊→クラッシュ。`query()` が内部で `rebuild()` を呼ぶため、並列 query でも発生。

- **症状**: 4+ スレッドの並列 query/rebuild で SIGABRT (exit code 134)
- **修正**: `AtomicBool` の `compare_exchange` で rebuild を排他。1スレッドだけ rebuild、他はスキップして active Cylinder をそのまま読む。ロックフリー

## [修正済み] query が galloping intersect で遅い
**日付**: 2026-04-07
**箇所**: `engine.rs` — `query()`

2条件以上の query で galloping intersect（ソート済み配列交差）を使っていたが、Column直読みフィルタの方が全レンジで高速。さらに非選択的クエリ（候補50万件超）では pre-computed bitmap AND で O(n/64) に高速化。

- **症状**: 2条件 query が 60μs（Column直読みなら 14μs、bitmap AND なら 5μs）
- **修正**: 最小Cylinderスライスをpull + 残りColumn直読み。全条件bitmap有なら AND 1パス

## [修正済み] fxhash 系統的衝突でvocabエントリ消失
**日付**: 2026-04-07
**箇所**: `vocabulary.rs` — `index_insert()`

`index_insert` がハッシュ一致で重複と判定し、実際の値を比較せずにreturnしていた。fxhashの系統的衝突（例: "entity_19800" と "entity_24400" が同一ハッシュ）で後発エントリが消失。

- **症状**: 50K unique strings で5000件がvocab_idで引けない
- **修正**: ハッシュ一致時に実際の値を比較、不一致ならlinear probe続行

## [修正済み] EntitySet free_offset 未アライメント → SIGBUS
**日付**: 2026-04-07
**箇所**: `entity_set.rs` — `init()`, `load()`

max_entities が 8 の倍数でない場合、bitset_size が 4 byte 非整列になり、free_offset が奇数アドレスに。AtomicU32 アクセスで SIGBUS（Apple Silicon）。

- **症状**: `create_with_capacity(100)` で entity 上限到達時にSIGBUS (exit 138)
- **修正**: `free_offset` を 4 byte アライメント

## [修正済み] entity lifecycle がundo logに未記録
**日付**: 2026-04-07
**箇所**: `engine.rs` — `entity()`, `delete()`, `rollback()`

entity() と delete() の EntitySet 変更がundo logに記録されず、rollback時にentityの生死が戻らなかった。

- **症状**: create→rollback でentityが残る。delete→rollback でentityが復活しない
- **修正**: dim_id=0xFFFF マーカーでentity create/delete をundo log に記録。replay_undo で EntitySet::free/revive

## [修正済み] Vocabulary ハッシュインデックスが open 後に一部エントリを引けない
**日付**: 2026-04-08
**箇所**: `vocabulary.rs` — `load()`

`Vocabulary::load()` がmmap上のハッシュテーブルをそのまま信頼するが、一部スロットのflagバイトが永続化後に0（空）として読まれ、`lookup()` がそこで探索を打ち切る。`get_text()` による全entityスキャンでは見つかるが `vocab_id()` では見つからない。

Cylinderは `rebuild()` で再構築されるが、Vocabularyのハッシュインデックスには再構築パスが存在しなかった。

- **症状**: `tie_text` → `flush` → `open` → `rebuild` 後、一部の `vocab_id()` が `None` を返す。29件中2件が詳細表示不可（mkd-nyusatsu P003 世田谷区 2026年度で発覚）
- **再現**: DB作成 → 数十件 `tie_text` → `flush` → 再open → `vocab_id` で特定キーが見つからない
- **修正**: `Vocabulary::load()` でハッシュインデックスをゼロクリア後、全エントリ（data/offsets）から `index_insert` で再構築。Cylinderと同様にインデックスをキャッシュとして扱う

## [修正済み] bitmap AND が Symbol 型 himo の2条件クエリで0件を返す
**日付**: 2026-04-08
**箇所**: `engine.rs` — `query()`

Symbol型himoの値はvocab_id（動的に増加）。`define_himo("color", Symbol, 10)` でmax_values=10としても、vocabエントリが増えるとvocab_id=20等になる。bitmap配列は0..=max_valuesしか持たないため `bitmap(20)` → None → 結果0件。

3条件以上ではColumn直読みフィルタに落ちるため正常動作し、2条件のbitmap ANDパスだけで発症。

- **症状**: 2条件queryが0件（3条件以上やmax_values=0なら正常）
- **再現**: `define_himo("color", Symbol, 10)` → 10,000件投入 → `query(&[("color", red), ("size", m)])` → 0件
- **修正**: all_bitmap判定で `val <= max_values` を確認。範囲外はColumn直読みにfallback

## [修正済み] Column.count の並列write競合
**日付**: 2026-04-08
**箇所**: `column.rs` — `write_count()`

Column.countが非atomicなu32で、複数スレッドが同時にset()するとwrite_countが低い値で上書きされ、高いeidがrebuild時の走査範囲から漏れる。

- **症状**: 8 writer並列で26,000件期待が23,856件（2,144件消失）
- **修正**: count を AtomicU32 化、write_count → ensure_count(fetch_max)。col_mut() 廃止

## [機能追加] tie_to / tie_text_to / tie_ref_to（&self 書き込み）
**日付**: 2026-04-08
**箇所**: `engine.rs`

define_himo 済みの紐に対して &self で書き込み可能なメソッド追加。Arc<Engine> 共有のまま Mutex 不要で並行書き込み。紐が未定義なら panic。

## [修正済み] delta buffer — 重複・欠損・溢れ検知の3件
**日付**: 2026-04-09
**箇所**: `engine.rs` — `apply_delta()`, `himo_store.rs` — `delta_push()`, `delta_needs_rebuild()`

### 重複
apply_delta の結果に dedup が無く、同一 eid が複数回返る場合があった。
- **症状**: sinfo で同じモジュールが複数回表示
- **修正**: `result.sort_unstable(); result.dedup();`

### delta_push 競合
fetch_add 方式ではスロット確保と書き込みが非原子的で、並列 push で stale データを読む可能性があった。
- **修正**: CAS方式（compare_exchange でスロット確保→書き込み→Release fence）

### 溢れ検知
delta_push が CAS 方式で DELTA_CAP ちょうどで停止するが、delta_needs_rebuild が `> DELTA_CAP` でチェックしていたため、delta_len == DELTA_CAP の時に溢れが検知されなかった。新しい eid が delta にも Cylinder にも無い状態になり query 結果が欠損。
- **症状**: 4096件以上のぶら下げ後にquery結果が正解より少ない（property-based testで460/500エラー検出）
- **修正**: `> DELTA_CAP` → `>= DELTA_CAP`

### vocab 並列競合
並列 tie_text_to で同一文字列に異なる vocab_id が振られる TOCTOU。
- **修正**: get_or_insert で insert 後に lookup で先着確認。負けたスレッドは勝者の id を使う

## [修正済み] open 後に vocab_id が引けず query が空になる
**日付**: 2026-04-09
**箇所**: `vocabulary.rs` — `insert()`

Vocabulary の count/data_end は AtomicU32 フィールドに持つが、mmap header への書き戻しは `sync()` (flush経由) でしか行われなかった。flush なしで drop すると header が古いまま残り、reopen 時の `load()` が `count=0` を読んで `rebuild_index` が何も復元しない。

- **症状**: flush なしの drop → reopen で vocab_id() が None
- **原因**: insert 時に mmap header の count/data_end を更新していなかった
- **修正**: `insert()` のたびに count/data_end を mmap header に即書き戻し。flush 不要で reopen 可能に

## v25: delete + insert 後の query で古いエンティティが残る
**日付**: 2026-04-12
**箇所**: `engine.rs` — delta buffer / query

v25 で同一プロセス内で delete → insert → query すると、削除したはずのエンティティが query 結果に含まれる。旧バージョンでは正常。

- **症状**: sinfo の module insert（同名モジュール上書き: delete → insert）後の list に古いモジュールと新しいモジュールの両方が返る
- **再現**: sinfo-store-enchu の以下テストが FAILED:
  - `integrity_duplicate_insert_overwrites` — delete 後に list で1件のはずが2件
  - `boundary_empty_string_fields` — 同様
  - `stress_delete_then_reinsert` — 同様
  - 1000並行テスト 2件 — 同様

## [修正済み] v27: multi-cond `query()` が pair table 空セルで silent 0件返却
**日付**: 2026-04-20
**箇所**: `engine.rs` — `query()`
**Feature**: v27

2条件以上の `query()` が、内部で pair table (`best_lookup_ref`) の結果長で pivot を決めるため、pair table が当該 `(value_a, value_b)` セルを空として返すと「pair_len=0 が最小 → 即 return empty」になってしまう。Column には該当データがあり、`pull_raw` 単独ならちゃんと引ける。静かに 0件で返るので外から見えず debug 困難。

- **症状**: midoriban で `query(&[("type", TYPE_THREAD), ("board", 0)])` が 0件を返す。`pull_raw("type", 1)` = 10,000、`pull_raw("board", 0)` = 2,505,000 と両方 cylinder にちゃんと乗っているのに、2-cond query だけが空。
- **再現**:
  - midoriban で 10,000 スレッド × 1,000 レス（合計 10,010,000 entity）を `tie_async` で投入
  - `define_himo("type", Value, 4)`、`define_himo("board", Value, 64)`
  - `type=1 AND board=0` の `query()` → 0件
- **疑い**:
  1. `tie_async` / concurrent apply が pair table を更新しない（`update_pair_tie` 相当の呼び出しが無い）。tie_async 経由のエントリは pair table に入らない
  2. `open` 時の `rebuild_pairs` がこのスケールで何も入れずに終わる、または何らかの条件でスキップされる
- **影響**: `query` で書いたコードが条件次第で silent に全件落ちる。1条件 `pull_raw` は正常なので回避可能（pivot を自分で決めて Column 直読みフィルタ）
- **暫定回避 (midoriban 側)**: `pull_raw("type", TYPE_THREAD)` + Rust 側で board フィルタ。10k スレで 4.4ms/req、実用速度。
- **修正方針候補**:
  - (a) `query` で pair 使うときは「pair が Some でセルが空」の場合 cylinder slice pivot に fallback
  - (b) `tie_async` の consumer thread が apply 時に pair table も同期更新
  - (c) open 時の `rebuild_pairs` が v27 multi-cond パスで本当に機能してるか検証するテスト追加

## v26: 並行 read/write 中に rebuild_cylinder が delta を取りこぼす
**日付**: 2026-04-15
**箇所**: `himo_store.rs` — `rebuild_cylinder()` / `delta_*` 周り
**Feature**: v26

複数スレッドが `tie_to` で書き込み中に、別スレッドの `query` が内部で `rebuild_cylinder()` を呼ぶと、rebuild と並行して push された delta エントリが「base に統合されず」かつ「次の delta バッファにも残らず」消失する。Column には正しく書かれているが、Cylinder からは見えなくなる。後から `db.rebuild()` を明示的に呼んでも回復しない（base が確定状態として扱われ、Column との差分再計算がない）。

- **症状**: 並行投入後、`query(type=POST)` の件数が Column 全走査と乖離する
- **再現**: sunsu (`/Users/kubo/Desktop/mutafika/sunsu`) の `--features v26` 並行ベンチ:
  - 16 スレッド: 8 writer (`tie_to(POST...)`) × 8 reader (`query(FOLLOW...)`) を同時実行
  - 25,000 件の POST 投稿のうち約 21,000 件が Cylinder に反映されない
  - `db.pull("type", POST)` (Cylinder経由) = 115,097
  - `for eid in 0..cap { db.get(eid, "type") }` (Column 全走査) = **136,000** ← ground truth
  - その後 `db.rebuild()` + `db.rebuild_pairs()` を呼んでも 115,097 のまま回復しない
- **疑い**: rebuild_cylinder の流れで「delta スナップショット → base に merge → swap」する間に、新規 push された delta が swap 対象外のバッファに残り、次回 rebuild ではそのバッファを見ない
- **影響**: ドキュメント (`CLAUDE.md`) で「ロックフリー並行 read/write 可能」と明記されているが、実際は writer と並行する read を回避するか、rebuild を明示的に手前で1回打ってから write フェーズに入る等の運用が必要
- **暫定回避**: 並行書き込み中は query を打たない / writer フェーズと reader フェーズを分離する
- **Note**: pure write-only の並行 (`tie_to` のみ、query なし) では `db.rebuild()` 後の count が一致する。read を混ぜたときだけ発症
