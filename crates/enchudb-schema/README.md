# enchudb-schema

EnchuDB の **declarator + bindings** 層。 仮想 2D テーブル (= N 個の紐の束) を declare して `build()` すると、 col 名 → himo_id / table 名 → table_vid が pre-resolve され、 schema は DB ファイル内に永続化される。

runtime hot path (高頻度 writer / reader) は schema 層を経由せず、 **build 時に取り出した bindings (`himo_id` u16 + `table_vid` u32) を engine 直叩き** (`tie_*_by_id` / `query_by_id`) する設計。 schema の `commit` / `find` は declarative DSL で convenience 用途 (REPL / 低頻度 / 試作)。

SQL crate (`enchudb-sql`) はこの schema 層の上に乗る parser。

## なにこれ

- `Database::table(...).column(...).primary_key(...).build()` で **仮想 2D テーブル** を宣言
- `build()` 時に column → himo_id / table → table_vid を pre-resolve
- bindings 取り出し: `Table::himo_id(col)`, `Table::table_vid()`, `Database::marker_himo_id()`
- convenience API: fluent な insert / where / find / entity().set() / delete (declarative、 低頻度向け)
- relation (Ref 型 col) を `ref_to(col, "to_table")` で declare → 逆引き O(1)
- schema は DB ファイル内に永続化 (reopen で自動復元、 himo_id も再 resolve)
- `Drop` で自動 flush — 手動 flush 不要

## 役割分担

```
[起動時]    schema       declarator + 永続化 + bindings 抽出
            ↓
[runtime]   engine        bindings + tie_*_by_id / query_by_id 直叩き
```

| Layer | crate | 用途 |
|---|---|---|
| SQL parser | `enchudb-sql` | SQLite 互換、 初心者 / migration 向け |
| schema (declarator) | `enchudb-schema` | DDL 宣言、 schema 永続化、 declarative CRUD (低頻度) |
| **engine (runtime)** | **`enchudb-engine`** | **hot path、 ns 級 lookup、 `_by_id` API** |

「全 himo は何かの table に所属する」 という app modeling は **100% のケース** で正しい (matcha / t5ug3 / sinfo / enchu studio すべて) ので、 declarator + bindings として schema 層を持つ価値はある。 ただし runtime path で経由する必要はない (perf を犠牲にするだけ)。

## 起動時: schema で declare + 永続化

```rust
use enchudb_schema::{Database, ColumnType};

let mut db = Database::create("/tmp/app.db")?;

// schema declare — build() 時に himo_id / table_vid 全部 pre-resolve
let _ = db.table("users")
    .integer("id")
    .text("name")
    .integer("age")
    .text("city")
    .primary_key("id")
    .build()?;
```

## runtime hot path: bindings + engine 直叩き (推奨)

行数 KO/sec 級の writer / reader は **bindings を起動時に取り出して engine 直叩き**。 string lookup / dispatch ゼロ、 raw async 上限近くまで出る。

```rust
let users = db.get_table("users").unwrap();
// bindings 抽出 (起動時 1 回)
let marker_hid = db.marker_himo_id();
let table_vid  = users.table_vid();
let name_hid   = users.himo_id("name").unwrap();
let age_hid    = users.himo_id("age").unwrap();
let city_hid   = users.himo_id("city").unwrap();
drop(users);

// runtime: bindings + engine 直叩き
let eng = db.arc_engine();
let e = eng.entity();
eng.tie_to_by_id(e, marker_hid, table_vid);        // 「user 所属」 marker
eng.tie_text_to_by_id(e, name_hid, "Alice");
eng.tie_to_by_id(e, age_hid, 30);
eng.tie_text_to_by_id(e, city_hid, "Tokyo");

// query も engine 直叩き
let tokyo_30 = eng.query_by_id(&[(marker_hid, table_vid), (age_hid, 30), (city_hid, eng.vocab_id("Tokyo").unwrap())]);
```

## convenience API: declarative CRUD (低頻度 / REPL / 試作)

書き味重視。 内部で _by_id 経路に切替えてあるので極端に遅くはないが、 hot path で経由する想定ではない。

```rust
let users = db.get_table("users").unwrap();

let alice = users.insert()
    .set("id", 1i64)
    .set("name", "Alice")
    .set("age", 30i64)
    .set("city", "Tokyo")
    .commit()?;

let young  = users.where_eq("age", 30i64).find()?;
let multi  = users.where_eq("age", 30i64).where_eq("city", "Tokyo").find()?;
let count  = users.where_eq("city", "Tokyo").count()?;
let one    = users.where_eq("id", 1i64).find_one()?;

let prime  = users.where_range("age", 25, 35).find()?;
let adults = users.where_ge("age", 18).find()?;

let age = users.entity(alice).get("age");
users.entity(alice).set("age", 31i64).commit()?;
users.entity(alice).update()
    .set("city", "Osaka")
    .set("age", 32i64)
    .commit()?;
users.entity(alice).delete()?;
```

## relation (cross-table ref)

```rust
let mut db = Database::create("/tmp/x.db")?;

// referenced 側 table を先に build
db.table("companies").integer("id").text("name").primary_key("id").build()?;

let users = db.table("users")
    .integer("id")
    .text("name")
    .ref_to("company", "companies")   // users.company : Ref → companies.eid
    .primary_key("id")
    .build()?;

// 使い方
let companies = db.get_table("companies").unwrap();
let ant = companies.where_eq("name", "Anthropic").find_one()?.unwrap();

users.insert().set("id", 1i64).set("name", "Alice")
    .set("company", enchudb_schema::Value::Ref(ant))
    .commit()?;

// ref で逆引き
let staff = users.where_ref("company", ant).find()?;
```

## upsert (`INSERT OR REPLACE` 相当)

```rust
let kv = db.table("kv").text("key").integer("ts").primary_key("key").build()?;

// PK 一致 row があれば update、 無ければ insert
kv.upsert().set("key", "k1").set("ts", 100i64).commit()?;
kv.upsert().set("key", "k1").set("ts", 200i64).commit()?;  // 既存 row を update

let rows = kv.where_eq("key", "k1").find()?;
assert_eq!(rows.len(), 1);
```

## 永続化 + reopen

```rust
{
    let mut db = Database::create("/tmp/app.db")?;
    let users = db.table("users").integer("id").text("name").primary_key("id").build()?;
    users.insert().set("id", 1i64).set("name", "Alice").commit()?;
}
// 1 行も schema declare せず reopen
{
    let db = Database::open("/tmp/app.db")?;
    assert_eq!(db.list_tables().len(), 1);

    let users = db.get_table("users").unwrap();
    let alice = users.where_eq("name", "Alice").find_one()?.unwrap();
    // table の col → himo_id mapping も復元済み
}
```

schema は engine の content blob に serialize して保存。 178 列の prisma schema レベルでも 512 MB 上限内に収まる。

## concurrent / WAL モード

`Database` は内部で `Arc<Engine>` を持ち、 2 phase で運用する:

| phase | 状態 | 何ができる |
|---|---|---|
| build | Arc count = 1、 consumer thread なし | `db.table(...).build()` で schema 拡張、 `db.engine_mut()` で `&mut Engine` 取得可 |
| concurrent | `Arc<Database>` 経由で共有可能、 consumer thread 起動 | 全 write は WAL に append (有効時)、 background fsync、 `&Database` のみで操作 |

```rust
use std::sync::Arc;
use enchudb_schema::Database;

// 1. build phase で schema declare
let mut db = Database::create_growable_tiny("/tmp/store.db")?;
db.table("notes").integer("id").text("body").primary_key("id").build()?;
db.table("kv").text("key").integer("ts").primary_key("key").build()?;

// 2. concurrent + WAL モードに遷移、 `Arc<Database>` を取得
let db: Arc<Database> = db.finish_with_wal(256 * 1024 * 1024)?;

// 3. 各 thread / sub-store で clone 共有、 全部 `&Database` 経由
let db_clone = db.clone();
std::thread::spawn(move || {
    let kv = db_clone.get_table("kv").unwrap();
    kv.insert().set("key", "alpha").set("ts", 1000i64).commit().unwrap();
});

// 4. 同期書き出しが必要なら wal_sync (~148µs)
db.engine().wal_sync()?;
```

| API | 戻り値 | 用途 |
|---|---|---|
| `Database::create*(path)` | `Database` | build phase 開始 (single-thread) |
| `Database::open(path)` | `Database` | reopen、 standalone (consumer なし) |
| `db.finish_with_wal(cap)` | `Arc<Database>` | build phase → concurrent + WAL |
| `db.finish_concurrent()` | `Arc<Database>` | build phase → concurrent (WAL なし) |
| `Database::open_with_wal(path, cap)` | `Arc<Database>` | reopen を直接 concurrent + WAL、 schema は自動復元 |

`finish_*` は `self` を consume するので呼んだ後の元 `db` は無効。 失敗条件は Arc 共有後の呼び出し (build phase で既に `Arc::new(db)` した後など)。

### sinfo / multi-store パターン

```rust
// 1 個の Database を 18 個の sub-store で Arc 共有
let db = Database::create("/var/sinfo/store.db")?;
// ... build phase で 70+ himo を含む全 table を declare ...
let db = db.finish_with_wal(256 * 1024 * 1024)?;

// 各 sub-store に Arc<Database> を clone して渡す
let store1 = SubStore::new(db.clone());
let store2 = SubStore::new(db.clone());
// ...
```

crash 時は次回 `open_with_wal` で WAL から recover、 commit 済み write は durable。

## column 型

| `ColumnType` | tie 経路 | 値の Rust 型 |
|---|---|---|
| `Integer` | `Engine::tie_to` | `i64` (内部 u32、 `< u32::MAX`) |
| `Text` | `Engine::tie_text_to` | `String` / `&str` (Vocabulary 経由) |
| `Ref` | `Engine::tie_ref_to` | `EntityId` (= u64) |

`Value::from(...)` で各種 Rust 値から自動変換 (`i64` / `i32` / `u32` / `&str` / `String`)。 `Ref` だけは `Value::Ref(eid)` で明示。

## 何が pre-resolve されるのか

| 項目 | 解決タイミング |
|---|---|
| col 名 → himo_id (u16) | `build()` で 1 回、 以降 cache hit |
| table 名 → vocab_id (u32) | `build()` で intern |
| himo の type 検証 | `build()` (型不一致は build 時 error) |
| relation 先 table の存在 | `build()` |

これにより、 query 時の string lookup は **完全に skip**。 engine 側で `query_by_id(&[(u16, u32)])` を直で呼ぶ。

## 速度

- dispatch (名前解決込み) — 50〜100 ns 削減 (string scan を skip)
- 1 query の latency は engine と同じ (memcpy 律速)、 schema 化で結果返却が速くなるわけではない
- 売りは **「全 app コードで raw engine よりも遅くならない、 必ず同等以上」**

## meta crate での opt-in

`enchudb-schema` は meta crate (`enchudb`) の **always-on 依存**。 feature 不要:

```toml
enchudb = { path = ".." }
```

```rust
// 主要 API は enchudb::schema:: にも生えてる
use enchudb::schema::{Database, ColumnType};
```

## SQL crate との関係

- `enchudb-sql::Database::execute("SELECT ...")` も schema layer の上に乗る (将来統合)
- 現状は SQL crate が自分で TableDef を持ってるが、 dep だけ張ってあるので逐次 schema crate へ委譲する移行 path がある
- アプリは SQL を使わず schema crate 直で書いた方が高速 (parse 不要、 静的型保証)

## 制約 + 未対応

- 集計 (`sum` / `count` / `group by`) は engine API 直 (`db.engine().sum(...)`) か DSL (`query_lang`)
- JOIN は relation + `where_ref` の組合せ (multi-step は manual)
- 1 table の col 上限 = engine の `max_himos` (default 256)
- ALTER TABLE 相当はなし (schema は append-only、 column 削除は untie で代用)
- `Null` 値の tie は不可 (untie で代用、 unset = engine.get が None)
