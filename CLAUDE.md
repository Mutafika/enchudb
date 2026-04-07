# EnchuDB — 紐ベース円柱エンジン

## これは何？
組み込みDBエンジン。SQLite/DuckDBの代替。テーブルではなく「紐（himo）」でデータを管理する。
単一ファイル。全mmap。ロックフリー並行read。

## 依存
```toml
[dependencies]
enchudb = { path = "../enchudb" }
```

## 基本API

```rust
use enchudb::{Engine, HimoType};

// 作成 or オープン（単一ファイル）
let mut db = Engine::create("/path/to/db.db").unwrap();
// let db = Engine::open("/path/to/db.db").unwrap(); // 既存DBを開く（auto rebuild）

// 紐を定義（max_values指定でprefix sum O(1) + bitmap AND が有効になる）
db.define_himo("age", HimoType::Value, 100);    // 整数、値域0-100
db.define_himo("dept", HimoType::Value, 20);     // 整数、値域0-20
// define_himo しなくても tie 時に自動作成される（その場合 binary search O(log n)）

// entity作成 + 紐を張る
let e = db.entity();
db.tie(e, "age", 30);                    // u32値（< u32::MAX）
db.tie_text(e, "city", "東京");           // 文字列（Vocabulary経由）
db.tie(e, "company", other_entity_id);   // entity参照もu32値として格納

// 値を読む（Column直読み、rebuild不要）
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

// 非索引コンテンツ（検索対象外のblob、上限512MB）
db.content(e, "memo", b"hello");
db.get_content(e, "memo");  // → Some(b"hello")
```

## 大量データ（1億entity超）
```rust
let mut db = Engine::create_with_capacity("/path/to/db.db", 100_000_000).unwrap();
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
- `HimoType::Ref` — entity参照

## マルチスレッド
```rust
use std::sync::Arc;

let mut db = Engine::create("db.db").unwrap();
// write phase（&mut self、排他）
db.tie(e, "age", 30);
db.rebuild();  // キャッシュ構築

// read phase（&self、Arc共有、並列安全）
let db = Arc::new(db);
// 複数スレッドから同時にquery/pull_raw/get OK
// rebuild()も&selfで並列安全（内部でcompare_exchange排他）
```

## クエリ戦略（自動選択）
1. **全条件bitmap有** → bitmap AND（非選択的クエリ ~5μs）
2. **それ以外** → 最小Cylinderスライスpull + Column直読みフィルタ（選択的 ~2-12μs）
3. **1条件** → Cylinder slice_one 直返し（10ns）

## クエリ言語（REPL用）
```rust
use enchudb::query_lang::{execute, QueryResult};
let result = execute(&mut db, "age:30 city:\"東京\" | count");
```

## ファイル構造（単一ファイル）
```
[Header 4KB]     レイアウト + himoメタデータ
[EntitySet]      生存ビットマップ + free stack
[UndoLog]        トランザクション用
[Vocabulary]     文字列辞書（data + offsets + index）
[HimoRegistry]   紐名辞書
[ContentStore]   非索引blob（index + data）
[Himo 0..N]      各紐: Column + Cylinder×2（ダブルバッファ）
```

## 設計原則
- **紐が本質、円柱はキャッシュ。** Column（紐）がソースオブトゥルース。Cylinderはrebuildで構築されるキャッシュ。
- **伝播はEnchuの仕事ではない。** JOIN相当の伝播はRavn（クエリ言語）が担当。
- **単一ファイル、全mmap。** 1つのスパースファイルに全領域配置。仮想サイズ大、実ディスク使用量は書いたページ分だけ。
- **ロックフリー並行。** ダブルバッファCylinder + AtomicBool swap。rebuild中もreaderは止まらない。

## 制約
- `tie()` の value は `< u32::MAX`（u32::MAX は sentinel 予約）
- content data 上限 512MB
- max_himos デフォルト 256
- max_entities デフォルト 16M（create_with_capacity で変更可）
