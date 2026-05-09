# EnchuDB Issue Log

matcha の永続化レイヤを enchudb で組もうとして見えた機能・到達度のギャップ記録。
順次潰したらこのファイルから消す。

---

## [closed] SQL frontend が存在しない

**日付**: 2026-05-08 → **解決 2026-05-08**
**該当**: `crates/enchudb-sql/` (新設)

### 解決

`crates/enchudb-sql` を新設、sqlparser (SQLite dialect) で parse → engine メソッド直 dispatch。
`enchudb` meta crate に `features = ["sql"]` で opt-in。

```rust
use enchudb_sql::{Database, Output, Value};
let mut db = Database::create("/tmp/notif.db")?;
db.execute("CREATE TABLE notif (key TEXT PRIMARY KEY, dismissed_at INTEGER)")?;
db.execute("INSERT OR REPLACE INTO notif VALUES ('uuid-abc', 1715174400)")?;
match db.execute("SELECT * FROM notif WHERE key = 'uuid-abc'")? {
    Output::Rows { rows, columns } => { /* ... */ }
    _ => {}
}
```

### v0 サポート

```sql
CREATE TABLE t (col TYPE [PRIMARY KEY], ...)         -- INTEGER / TEXT
INSERT INTO t [(col, ...)] VALUES (...) [, (...)]
INSERT OR REPLACE INTO t VALUES (...)
SELECT [* | col, ...] FROM t [WHERE col = val [AND col = val]*]
UPDATE t SET col = val [, col = val]* WHERE col = val
DELETE FROM t WHERE col = val
```

### マッピング

- table → marker himo `__sql_table` (Symbol) の値が table 名
- column → himo `<table>.<col>` (Integer→Value, Text→Symbol)
- WHERE col=val AND col=val → `eng.query(&[(table_himo, vid), (col1, v1), ...])` に 1:1
- INSERT OR REPLACE → PK で query → 既存 eid に re-tie / なければ新規 entity

### 未対応 (今後)

- JOIN / subquery / 集計 (集計は `query_lang` DSL を使う)
- ORDER BY / LIMIT / 範囲比較 (`>`, `<`, `BETWEEN`)
- 複数カラム PK / DEFAULT / NOT NULL / CHECK
- スキーマ永続化 (現状 in-memory、reopen 時に CREATE TABLE 再呼出が必要、`define_himo` は idempotent)

10 unit test green、`matcha_use_case` test で実際の通知 state シナリオを検証済み。

---

## [closed] in-place mutate オペレータが query_lang に無い

**日付**: 2026-05-08 → **解決 2026-05-08**
**該当**: `crates/enchudb-engine/src/query_lang.rs`

### 解決

`~ <eid> himo:val [himo:val ...]` を追加。既存 entity の紐を置換 (`tie` を再呼び出し)。

```
~ 42 age:31 city:"福岡"   → age と city を置換
```

`QueryResult::Updated(eid)` を返し、Display は `~42` 形式。

### matcha 側の使い方

- 通知 key が引ければ (例: `key:"uuid-abc"` で eid 取得) → `~ <eid> dismissed_at:<ts>` で上書き可能
- key で引く部分は 1 条件 query なので O(1) or O(log n)

「key → upsert」の高層 API が欲しくなったら SQL `INSERT OR REPLACE` で網羅される(issue #1)。
DSL 段は eid 直指定のみ、複合的な upsert は SQL 層に任せる切り分け。

---

## [closed] FFI / C ABI が存在しない (マルチ言語対応への道)

**日付**: 2026-05-08 → **解決 2026-05-08**
**該当**: `crates/enchudb-ffi/` (新設、`cdylib` + `staticlib` + `rlib`)

### 解決

`crates/enchudb-ffi` を新設、SQLite 風の C ABI を 12 関数で露出:

```
enchudb_open / enchudb_create / enchudb_close
enchudb_exec / enchudb_query / enchudb_last_error
enchudb_result_rows / cols / col_name / is_null / int / text / free
enchudb_version
```

`crates/enchudb-ffi/include/enchudb.h` を同梱、`enchudb` meta crate に
`features = ["ffi"]` で opt-in (sql feature を自動引き込む)。

### 動作確認

- 7 unit test green (`open / close / exec / query / error / null_args / matcha_use_case`)
- `crates/enchudb-ffi/examples/demo.c` を実 C ファイルとして用意、コンパイル + 実行で
  enchudb 0.1.0 を読み込み INSERT OR REPLACE + SELECT が正しく動くことを確認:

```
$ cargo build --release -p enchudb-ffi
$ cc -I crates/enchudb-ffi/include -L target/release -lenchudb_ffi \
     crates/enchudb-ffi/examples/demo.c -o /tmp/enchudb_demo
$ DYLD_LIBRARY_PATH=target/release /tmp/enchudb_demo
enchudb 0.1.0
2 rows, 2 cols:
  id |   name
  1 | alice2
  2 | bob
```

### 次の段 (open)

- Python (cffi) / Node (N-API) / Swift wrapper の薄い層を別 repo / `bindings/` 配下に切り出し
- `enchudb_prepare / enchudb_step` 形式の prepared statement (現状 `enchudb_query` 一括取得のみ)
- 大量行用のストリーミング API (現状 全行 heap alloc)

### 設計判断ログ

- ABI を凍結する以上、内部設計が枯れるまで関数増やさない方針 (現状 12 関数のみ)
- prepared statement は `enchudb_query` 一括取得で代替可能、実際のユースで足りないと判明したら追加
- `enchudb_db` / `enchudb_result` は opaque、内部構造を C 側に露出しない (内部リファクタを許容)
- 文字列は NUL 終端 UTF-8、length-prefixed は使わない (C エコシステム互換)

---

## [open] Lazy region init for growable backing

**日付**: 2026-05-08
**該当**: 各 store の `init()` (`vocabulary.rs`, `content_store.rs`, `entity_set.rs`, `himo_store.rs`, `cylinder*.rs`)

### 状況

`Engine::create_growable` のプラミング (Phase A) は完備したが、 fresh DB の
apparent size を layout total 以下に縮められない。 原因:

各 store の `init()` が **region の offset 0 に magic を eager に書く** ため、
初期 commit が小さいと layout 中盤の region init で SIGBUS する。

実測:
```
Engine::create_growable() + initial_commit = 1 MB
  → Vocabulary::init at vocab_data_off ≈ 16.5 MB → SIGBUS
```

### 提案

各 store を **lazy init** に書き換える:

- `init()` は in-memory の Vocabulary/EntitySet 構造体を作るだけ、 mmap には触らない
- 初書き込み時 (`insert`, `set`, `add_entity` etc) に:
  1. region の magic を check
  2. uninit ならまず magic を書く (grow_amortized で commit を伸ばしながら)
  3. その後本来の write を実行

### 関連

- Layout reorg (variable セクションを末尾に集める) も lazy init と組み合わせ
  ると効果倍増。 ただし lazy init だけでも fresh DB ~ 4 KB は達成できる
  (各 region の magic write が遅延されるので、 layout total まで commit する
  必要が無くなる)。

### 工数

5-7 日。 store ごとの init/check 設計、 既存 open 経路の magic verify との
互換性、 全 254 テスト pass の audit を含む。

---

## [open] state-log サイズの "tiny" preset が欲しい

**日付**: 2026-05-08
**踏んだ場所**: matcha-shell の通知 state DB (`notif.enchu`)
**該当**: `crates/enchudb-engine/src/engine.rs` の `create_compact` / `create_standalone`

### 状況

`Engine::create_standalone` (デフォルト)
- mmap 確保: 巨大 (実測で apparent 88 GB / 実消費 6 MB)
- Time Machine や `du --apparent-size` 使うツール、Backblaze 系の容量カウンタが
  バックアップ対象を 88 GB と認識して fail / バックアップ巨大化する

`Engine::create_compact`
- mmap 確保: それなり (apparent 305 MB / 実消費 46 MB)
- 64K entities + 16MB vocab + 16MB content
- Time Machine 的には許容範囲、ただし 1 KB 弱の state log にしては実消費 46MB は重い

### matcha 側の影響

`enchudb-sql::Database::create_compact()` を新設して回避済み (matcha は今これを使用)。
ただし「数十～数百行の小さい state log」用途では `create_compact` でも過大。

### 要望

`Engine::create_tiny` 相当のさらに小さい preset:
- max_entities: 4_096
- vocab_data: 1MB
- content_data: 1MB
- max_himos: 32
- cyl_max_values: 64

期待: apparent ~10 MB / 実消費 ~1 MB 程度。

**用途**: アプリの設定永続化、UI state log、 dismissed list、 recently-used list、
など「SQLite を持ち出すほどでもないけど永続化したい」レベルの軽量ストア。これを
押さえると enchudb が `~/.local/share/<app>/state.db` ポジションも取れる
(SQLite が今ここを独占してる)。

### 判定の罠

`create_compact` の文書は "CLI/小規模用途向け。ファイルサイズ数 MB" だが、
実測では数十 MB 規模で、 "数 MB" は言い過ぎ。文書側もアップデート要 (実測値に揃える)。

---
