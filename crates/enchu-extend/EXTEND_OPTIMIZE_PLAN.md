# Enchu Extend 最適化計画 — 読み込みキャッシュ特化

## コンセプト

PGに被せる透過キャッシュ。書き込みは同期のみ。ユーザーAPIは `db.table.filter()` だけ。

## 削除対象

### ファイル削除
- `undo.rs` — トランザクション不要
- `content_store.rs` — 非索引BLOB、使ってない
- `query_lang.rs` — REPL、JSから使わない

### engine.rs から削除
- `UndoLog` フィールド + import
- `ContentStore` フィールド + import
- `commit()` / `rollback()` / `recover()` / `record_undo()`
- `content()` / `get_content()`
- `tie_ref()` — entity参照は不要
- `delete()` — 同期は上書き方式で十分
- `flush()` 内の undo/content 関連処理

### lib.rs から削除するnapi関数
- `commit` / `rollback`
- `delete`
- `insert` (entity の v20互換エイリアス、entity() に統一)
- `write_text` / `write_int` (v20互換、tie/tieText に統一)
- `get_column_int` / `get_column_text` (v20互換、個別get で十分)
- `resolve_table` (常に0返すだけ)
- `query` (文字列版、queryFast で十分)
- `slice_fast` (pullRaw で十分)
- `auto_init` (init と同じ)

### lib.rs に残すnapi関数
- `init` / `open`
- `defineHimo`
- `entity` / `entityCount`
- `tie` / `tieText`
- `getText` / `getValue`
- `rebuild`
- `pullRaw`
- `queryFast` / `queryCount`
- `resolveHimo` / `resolveField`
- `vocabLookup`
- `flush`

### mod.rs 更新
- undo, content_store, query_lang のmod宣言削除

### client.ts への影響
- なし（client.ts は native.entity/tie/tieText/queryFast/vocabLookup/flush/rebuild しか使ってない）

## 期待効果
- コード量 30% 削減
- バイナリサイズ縮小
- メンテ対象が減る
- mmapサイズ縮小（content_store + undo の領域不要）
