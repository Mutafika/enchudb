# enchudb-sql

EnchuDB 上に積む SQL frontend。 SQLite の **上位互換 (superset)** を狙う、 薄い syntactic sugar 層。

## なにこれ

- SQLite dialect の SQL をパースして engine メソッドに 1:1 dispatch
- **schema は DB ファイル内に永続化** (reopen 後 CREATE TABLE 再呼出不要)
- 仮想 2D table 抽象 — N 個の紐 (himo) を 1 つの table 名で束ねる
- 非 SQL コンシューマ (`enchu studio` など) は `Database::list_tables()` で schema を直接読める
- 中間 AST 評価層を作らない設計、 SQL のオーバーヘッドは parse のみ

## なぜ SQLite 上位互換 (drop-in じゃなく)

- SQLite のエッジケース挙動 (型 affinity、 NULL handling、 WITHOUT ROWID 等) を完全模倣しない
- SQLite で書ける SQL は enchudb でも書ける、 意味的に同じ or より良い結果
- enchudb 独自の SQL 拡張は可 (例: graph traversal)
- ファイルフォーマット互換は不要

## v0.1 サポート

```sql
CREATE TABLE t (col TYPE [PRIMARY KEY], ...)        -- INTEGER / TEXT
INSERT INTO t [(col, ...)] VALUES (...) [, (...)]
INSERT OR REPLACE INTO t VALUES (...)

SELECT [* | col, ...] FROM t
  [WHERE <cond> [AND <cond>]*]
  [ORDER BY col [ASC|DESC]]
  [LIMIT n]

UPDATE t SET col = val [, col = val]* WHERE <cond> [AND <cond>]*
DELETE FROM t WHERE <cond> [AND <cond>]*
```

`<cond>` は等値 / 範囲 (`>` `>=` `<` `<=`) / `BETWEEN` / `IS NULL` / `IS NOT NULL`。

## 使い方

```rust
use enchudb_sql::{Database, Output, Value};

// 新規 DB を state-log preset (apparent ~0.7 MB) で作成
let mut db = Database::create_growable_tiny("/tmp/notif.db")?;

db.execute("CREATE TABLE notif (key TEXT PRIMARY KEY, dismissed_at INTEGER)")?;
db.execute("INSERT INTO notif VALUES ('a', 100), ('b', 200), ('c', 300)")?;

match db.execute("SELECT * FROM notif WHERE dismissed_at > 150 ORDER BY dismissed_at DESC")? {
    Output::Rows { rows, columns } => {
        for row in rows {
            // row[i] = Value::Integer(_) | Value::Text(_) | Value::Null
        }
    }
    _ => {}
}

// INSERT OR REPLACE — PK で既存行を置換
db.execute("INSERT OR REPLACE INTO notif VALUES ('a', 999)")?;

drop(db);  // Drop で自動 flush

// 再 open — schema 自動復元、 CREATE TABLE 再呼出不要
let mut db = Database::open("/tmp/notif.db")?;
assert_eq!(db.list_tables().len(), 1);
db.execute("SELECT * FROM notif")?;  // そのまま動く
```

## Database lifecycle

| メソッド | 用途 |
|---|---|
| `Database::create(path)` | デフォルト (大きい mmap、 88 GB sparse) |
| `Database::create_compact(path)` | CLI / 中規模 (apparent ~305 MB) |
| `Database::create_growable(path)` | grow-on-write、 通常サイズの上限 |
| `Database::create_growable_tiny(path)` | アプリ state-log preset (~0.7 MB、 1024 entities まで) |
| `Database::open(path)` | 既存 DB を開く、 schema 自動復元 |

## schema 永続化の仕組み

- `CREATE TABLE` のたびに `Vec<TableDef>` を簡易 text format で serialize
- 内部用 entity (marker = `__enchu_schema_v1__`) に `content()` blob として保存
- `Database::open` 時に blob を読んで schema を復元、 関連 himo を `define_himo` で再登録 (idempotent)
- DB ファイル 1 つに全部入る、 sidecar 不要、 178 テーブルでも 512 MB 上限内

## 非 SQL コンシューマ向け API

```rust
// schema を直接読む (enchu studio で table 一覧表示など)
for (table_name, cols) in db.list_tables() {
    println!("{}: {:?}", table_name, cols);
    // cols は Vec<(col_name, SqlType, is_pk)>
}

// engine 直接アクセスも可 (低レベル操作用)
let eng = db.engine();
```

## 未対応 (今後)

- JOIN / subquery
- 集計 SQL (`SUM` / `COUNT` / `GROUP BY`) — 使うなら `query_lang` DSL に流す
- 複数カラム PK
- `DEFAULT` / `NOT NULL` / `CHECK` 制約
- DateTime / Decimal / UUID 型
- `ALTER TABLE` migration

## meta crate での opt-in

```toml
enchudb = { path = "..", features = ["sql"] }
```

## C / Python / Node から叩く

`enchudb-ffi` が C ABI で露出する `enchudb_exec` / `enchudb_query` は内部で `Database::execute` を呼ぶ。 つまり同じ schema 永続化が C 経由でも有効。
