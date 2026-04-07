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
