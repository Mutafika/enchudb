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
// スキーマ定義（&mut self、起動時に1回）
db.define_himo("age", HimoType::Value, 100);
db.define_himo("city", HimoType::Symbol, 0);

// 初期データ投入（&mut self）
db.tie(e, "age", 30);
db.rebuild();

// Arc共有（以降 &self のみ）
let db = Arc::new(db);

// 読み書き並行（全部 &self）
db.entity();                          // entity作成
db.tie_to(e, "age", 30);             // 定義済み紐への書き込み
db.tie_text_to(e, "city", "東京");    // 定義済み紐へのテキスト書き込み
db.query(&[("age", 30)]);            // 検索（内部でrebuild）
db.pull_raw("age", 30);              // Cylinder直引き
db.get(e, "age");                    // Column直読み
db.rebuild();                        // ロックフリー（compare_exchange排他）
```

### &mut self vs &self
| メソッド | self | 用途 |
|----------|------|------|
| `define_himo` | `&mut` | スキーマ定義（起動時） |
| `tie` / `tie_text` / `tie_ref` | `&mut` | 書き込み（紐の自動作成あり） |
| `tie_to` / `tie_text_to` / `tie_ref_to` | `&self` | 書き込み（定義済み紐のみ） |
| `entity` / `delete` / `untie` | `&self` | entity操作 |
| `query` / `pull_raw` / `get` | `&self` | 読み取り |
| `rebuild` | `&self` | キャッシュ構築（ロックフリー排他） |
| `commit` / `rollback` | `&self` | トランザクション |
| `flush` | `&mut` | 永続化 |

## クエリ戦略（自動選択）

### v24（デフォルト）
1. **全条件bitmap有** → bitmap AND（非選択的クエリ ~5μs）
2. **それ以外** → 最小Cylinderスライスpull + Column直読みフィルタ（選択的 ~2-12μs）
3. **1条件** → Cylinder slice_one 直返し（10ns）

### v26（`--features v26`）
ペアテーブル + デルタシンクによる高速クエリ。v24 の上に積む。

```toml
enchudb = { path = "../enchudb", features = ["v26"] }
```

```rust
db.rebuild();
db.rebuild_pairs();  // ペアテーブル構築（rebuild 後に呼ぶ）

// クエリは同じ API。内部でペアテーブルを自動選択
let result = db.query(&[("tenant", 3), ("dept", 2), ("status", 1)]);

// 差分更新（tie のたびにペアテーブルを即時更新）
let old_val = db.get(eid, "dept").unwrap();
db.tie(eid, "dept", new_val);
db.update_pair_tie(eid, db.himo_id("dept").unwrap(), old_val, new_val);

// compact（デルタをベースに統合）
db.compact_pairs();
```

#### 戦略
1. **2条件以上** → 全紐ペアテーブルから最小セルを O(1) 選択 → 残りは Column 直読みフィルタ
2. **ペアテーブルより Cylinder スライスが小さい場合** → v24 パスに fallthrough
3. **1条件** → Cylinder slice_one 直返し（v24 と同じ）

#### 速度（100万 entity, 7紐）
| クエリ | v24 | v26 | 倍率 |
|---|---|---|---|
| 自社+部署+ステータス | 19.8μs | 69ns | 287x |
| 5条件全部 | 37.2μs | 79ns | 471x |
| 自社+給与帯 | 1.8μs | 72ns | 25x |
| 7条件全部 | 1.8μs | 99ns | 18x |
| ミス | 1.8μs | 50ns | 36x |

#### ペアテーブルの仕組み
- rebuild_pairs で全紐ペアの二次元テーブルを構築
- セル = (紐A の値, 紐B の値) → ソート済み entity リスト (Vec<u32>)
- define_himo で max_values 指定した紐のみ対象
- セル数 100万超のペアはスキップ
- デルタシンク: adds/removes リストで差分管理、compact でベースに統合
- 差分更新: 164ns/件

#### メモリ
- 100万 entity: 約 60MB（ペアテーブル全体）
- entity 数とペア数に比例
- mmap ではなくヒープ上（揮発、open 時に rebuild_pairs で再構築）

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

## 紐のメンタルモデル
- **ぶら下げる** = 猫と"足"を引っ張って、交差したところに値（4）をぶら下げる
- **引く** = "足"を値4で引く → 猫,犬。その交差点にぶら下がってるもの全部出てくる
- **円柱** = ある entity からぶら下がってる紐を全部束ねたもの。猫の円柱 = [足|4], [読み|ネコ], [分類|哺乳類]...
- **暗記カードの束** = 円柱の別表現。1枚のカード = 1本の紐（表=紐名、裏=値）。束が円柱

概念を説明する時はAPIメソッド名を使わない。「ぶら下げる」「引く」で統一。

## 設計原則
- **紐が本質、円柱はキャッシュ。** Column（紐）がソースオブトゥルース。Cylinderはrebuildで構築されるキャッシュ。ペアテーブルもキャッシュ。
- **伝播はEnchuの仕事ではない。** JOIN相当の伝播はRavn（クエリ言語）が担当。
- **単一ファイル、全mmap。** 1つのスパースファイルに全領域配置。仮想サイズ大、実ディスク使用量は書いたページ分だけ。
- **ロックフリー並行。** ダブルバッファCylinder + AtomicBool swap。rebuild中もreaderは止まらない。
- **HashMap 不使用。** 紐名の解決は線形探索（himo_id）。紐数は高々数百なので十分速い。

## アーキテクチャ（v26）
```
tie(entity, himo, value)
  ↓
Column（ソースオブトゥルース）→ mmap 永続化
  ↓ rebuild
Cylinder（1次元キャッシュ）→ mmap ダブルバッファ
  ↓ rebuild_pairs
PairTable（2次元キャッシュ）→ ヒープ（揮発）
  ↓ update_pair_tie
デルタシンク（即時反映）→ adds/removes → compact
```

### v26 で試して却下したもの
- CAS（コンテンツアドレッサブル）→ to_vec + hash 再計算で 46μs/件。デルタシンクの 164ns に負け
- 多次元ビットマップ Cylinder → 24GB メモリ爆発
- リンクドリスト（v20 Reverse）→ remove O(k) で遅い
- Z-order 曲線（v6）→ exact match にはペアテーブルが速い
- ペアセル同士の sorted intersect → Column 直読みの 8 倍遅い
- フーリエ変換 → 交差（積）を速くする道具ではない

## 制約
- `tie()` の value は `< u32::MAX`（u32::MAX は sentinel 予約）
- content data 上限 512MB
- max_himos デフォルト 256
- max_entities デフォルト 16M（create_with_capacity で変更可）
