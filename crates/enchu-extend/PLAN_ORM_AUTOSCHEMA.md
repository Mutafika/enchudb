## Drizzle / Prisma 自動スキーマ変換

ユーザーが手動でJSON書かずに、既存ORMスキーマから1行でEnchu初期化できるようにする。

### ゴールAPI

```ts
await enchu.fromDrizzle(schema, pgConnStr, { rootTable: 'projects' })
await enchu.fromPrisma('./prisma/schema.prisma', pgConnStr, { rootModel: 'Project' })
```

### 方針

- 全部TypeScript側。Rust側変更なし
- 変換結果は既存 `init()` が食えるJSON
- Prismaは軽量自前パーサー（`@prisma/internals`は重いので不要）

### 新規ファイル

| ファイル | 役割 |
|---|---|
| `src/drizzle.ts` | Drizzleスキーマ → CylinderSchema JSON |
| `src/prisma.ts` | `.prisma`パース → CylinderSchema JSON |
| `src/loader.ts` | PGから初回一括読み込み（バッチ1000件） |
| `src/index.ts` | `fromDrizzle()` / `fromPrisma()` 統合API |

### 型マッピング

- integer/bigint/serial, Int/BigInt → `int`
- real/doublePrecision, Float/Decimal → `float`
- boolean, Boolean → `bool`
- それ以外 → `text`

### リレーション（完全自動）

デフォルトで全テーブル全リレーション取る。設定不要。

- 1:N → nested tables
- 1:1 → 1行nested table（スキップしない）
- N:M → junction table自動検出、JOINで解決
- 深い階層 → フラット化（`project__task__comment` 形式）
- 複合PK → 全カラムを `|` で連結

```ts
// デフォルト: 全部キャッシュ（設定不要）
await enchu.fromDrizzle(schema, pgConnStr, { rootTable: 'projects' })

// 巨大スキーマで絞りたい時だけ（オプション）
await enchu.fromDrizzle(schema, pgConnStr, {
  rootTable: 'projects',
  include: { billing: true, members: true }
})
```

### 実装順序

1. `src/drizzle.ts` — スキーマ変換（Phase 1と2は並行可能）
2. `src/prisma.ts` — .prismaパーサー
3. `src/loader.ts` — PG一括読み込み
4. `src/index.ts` — 統合API
5. index.d.ts, package.json更新、テスト

### package.json追加

- dependencies: `pg ^8`
- peerDependencies: `drizzle-orm >=0.30`（optional）

### 注意点

- 単一インスタンス制約（Rust側OnceLock）
- スキーマ変更時はキャッシュ消してinitからやり直し
- 大量データはカーソルベースバッチ＋プログレス
