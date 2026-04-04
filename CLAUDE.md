# EnchuDB — 紐ベース円柱エンジン

## これは何？
組み込みDBエンジン。SQLite/DuckDBの代替。テーブルではなく「紐（himo）」でデータを管理する。
SQLite比 36〜44,000倍高速。

## 依存
```toml
[dependencies]
enchudb = { path = "../enchudb" }
```

## 基本API

```rust
use enchudb::{Engine, HimoType};

// 作成 or オープン
let mut db = Engine::create("/path/to/db").unwrap();
// let mut db = Engine::open("/path/to/db").unwrap(); // 既存DBを開く

// 紐を定義（max_values指定でprefix sum O(1)が有効になる）
db.define_himo("age", HimoType::Value, 100);    // 整数、値域0-100
db.define_himo("dept", HimoType::Value, 20);     // 整数、値域0-20
// define_himo しなくても tie 時に自動作成される（その場合 binary search O(log n)）

// entity作成 + 紐を張る
let e = db.entity();
db.tie(e, "age", 30);                    // u32値
db.tie_text(e, "city", "東京");           // 文字列（Vocabulary経由）
db.tie(e, "company", other_entity_id);   // entity参照もu32値として格納

// 値を読む
db.get(e, "age");                        // → Some(30)
db.get_text(e, "city");                  // → Some(b"東京")

// 紐を引く（検索）— rebuild() 後に使う
db.rebuild();                            // Column → Cylinder キャッシュ構築
let entities = db.pull_raw("age", 30);   // age=30 の全entity。O(1) or O(log n)

// 複数条件AND（query内部でrebuildも呼ばれる）
let result = db.query(&[("age", 30), ("dept", 5)]);  // age=30 AND dept=5

// 紐を外す / entity削除
db.untie(e, "age");
db.delete(e);

// トランザクション
db.commit();     // 変更確定（undo クリア）
db.rollback();   // 直前の commit まで巻き戻し

// 永続化
db.flush().unwrap();  // mmap を sync + メタデータ書き出し

// 非索引コンテンツ（検索対象外のblob）
db.content(e, "memo", b"hello");
db.get_content(e, "memo");  // → Some(b"hello")
```

## 大量データ（1億entity超）
```rust
let mut db = Engine::create_with_capacity("/path/to/db", 100_000_000).unwrap();
```

## 文字列検索
```rust
// tie_text で張った値は Vocabulary でIDに変換済み
// 検索時は vocab_id で先にIDを引く
let tokyo_id = db.vocab_id("東京").unwrap();
let result = db.pull_raw("city", tokyo_id);
```

## HimoType
- `HimoType::Value` — 整数値（age, dept, price など）
- `HimoType::Symbol` — 文字列（city, name など、Vocabulary経由）

## クエリ言語（REPL用）
```rust
use enchudb::query_lang::{execute, QueryResult};
let result = execute(&mut db, "age:30 city:\"東京\" | count");
```

## 設計原則
- **紐が本質、円柱はキャッシュ。** Column（紐）がソースオブトゥルース。Cylinderはrebuildで構築されるキャッシュ。
- **伝播はEnchuの仕事ではない。** JOIN相当の伝播はRavn（クエリ言語）が担当。
- **全mmap。** ロックなし。書き込みはColumnに直接。読み込みはCylinderのスナップショット。
