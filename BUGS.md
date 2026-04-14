# EnchuDB バグ記録

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
