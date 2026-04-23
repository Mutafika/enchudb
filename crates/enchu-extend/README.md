# Enchu Extend

PostgreSQL に被せるだけで検索が **24〜400倍速くなる**透過キャッシュ。

Elasticsearch や Meilisearch の代わりに使える。PGのデータをそのまま高速検索できる。

## インストール

```bash
npm install enchu-extend
```

## 使い方

```ts
import { enchu } from 'enchu-extend/client'

const db = await enchu('postgresql://user:pass@localhost:5432/mydb')

// これだけ。PGのテーブルがそのまま使える
const results = db.products.filter({ color: 'red', size: 'M' })
// → [{ _id: 0, color: 'red', size: 'M', price: 2000, ... }, ...]
```

## API

### `enchu(pgConn, options?)`

PGに接続してキャッシュを初期化。全テーブルのデータを自動ロード。

```ts
const db = await enchu('postgresql://user:pass@localhost:5432/mydb', {
  tables: ['products', 'users'],  // 特定テーブルだけキャッシュ（省略で全部）
  syncInterval: 1000,             // PG差分同期の間隔ms（デフォルト1秒、0で無効）
  cacheDir: '/tmp/enchu_cache',   // キャッシュ保存先
})
```

### `db.テーブル名.filter(conditions, options?)`

条件に一致するレコードを返す。

```ts
// 単条件
db.products.filter({ color: 'red' })

// 複合条件（AND）
db.products.filter({ color: 'red', size: 'M', category: 'shirt' })

// limit / offset
db.products.filter({ color: 'red' }, { limit: 20, offset: 40 })
```

### `db.テーブル名.filterIds(conditions, options?)`

IDだけ返す高速パス。データ取得は PG に任せる。

```ts
// ID リストだけ取得（2万件で 253µs）
const ids = db.products.filterIds({ color: 'red' })

// PG から実データ取得
const rows = await pg.query(
  `SELECT * FROM products WHERE id IN (${ids.map(id => id + 1).join(',')})`
)
```

検索は Enchu（sub-ms）、データ取得は PG。Elasticsearch と同じアーキテクチャ。

### `db.テーブル名.count(conditions?)`

条件に一致するレコード数を返す。

```ts
db.products.count({ color: 'red' })  // → 234
db.products.count()                   // → 全件数
```

### `db.テーブル名.all(options?)`

全レコードを返す。

```ts
db.products.all()
db.products.all({ limit: 10 })
```

### `db.close()`

接続を閉じて同期を停止。

```ts
await db.close()
```

## 動的クエリ

条件はただの `Record<string, string | number>` なので、動的に組み立てられる。

```ts
// 変数で指定
const key = 'color'
const val = 'red'
db.products.filter({ [key]: val })

// ユーザー入力から動的に構築
const conditions: Record<string, string | number> = {}
if (req.query.color) conditions.color = req.query.color
if (req.query.size) conditions.size = req.query.size
if (req.query.category) conditions.category = req.query.category
db.products.filter(conditions)

// 配列からまとめて
const filters = [['color', 'red'], ['size', 'M']] as const
const cond = Object.fromEntries(filters)
db.products.filter(cond)
```

## 自動同期

`syncInterval`（デフォルト1秒）で PG の変更を自動反映する。`updated_at` カラムを持つテーブルが対象。

```sql
-- テーブルに updated_at があれば自動同期される
CREATE TABLE products (
  id SERIAL PRIMARY KEY,
  color TEXT,
  size TEXT,
  price INTEGER,
  updated_at TIMESTAMP DEFAULT NOW()
);
```

```ts
const db = await enchu(pgConn, { syncInterval: 2000 })  // 2秒間隔
```

## ベンチマーク

100万件、マルチテナント（100テナント × 10,000商品）:

### vs Elasticsearch 8.17（ES profile API 純クエリ時間）

| クエリ | Enchu Extend | Elasticsearch | 倍率 |
|---|---|---|---|
| 単条件 (tenant=42) | 2.3µs | 95µs | **41x** |
| 2条件 (tenant+category) | 760ns | 320µs | **421x** |
| 4条件 (+color, +size) | 19.7µs | 196µs | **10x** |

### API 別速度（10万件）

| 操作 | 速度 |
|---|---|
| `count({ color: 'red' })` | **2µs** |
| `filterIds({ color: 'red' })` 2万件 | **253µs** |
| `filterIds({ color: 'red', size: 'M' })` 5千件 | **80µs** |
| `filter({ color: 'red' })` 2万件（オブジェクト返却） | 7.4ms |
| `filter({ color: 'red' }, { limit: 20 })` | 276µs |

## 仕組み

```
PostgreSQL  ──(1秒ポーリング)──▶  Enchu Extend (mmap キャッシュ)
                                       │
                              Pair Table (O(1) 2条件)
                              + Cylinder (prefix sum)
                              + Column filter (fallback)
                                       │
                            db.products.filterIds({ ... })
                                   760ns〜253µs
```

- PG のテーブル構造を自動検出してインデックスを生成
- ペアテーブルで 2条件クエリを O(1) で解決
- 3条件以上はペアテーブル + Column 直読みのハイブリッド
- PG 接続切れ時は自動再接続
- mmap ベースなのでプロセス再起動してもキャッシュが残る

## ライセンス

MIT
