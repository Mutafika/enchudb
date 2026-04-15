# Ravn 化メモ — 観測窓を Enchu から Ravn に引き上げる

## 現状の層違反

v27 の Enchu には **観測窓(仮想テーブル物化)** が埋まってる:

- `PairTable`: 全 2-tuple 自動物化 + tie 内自動反映
- `NTupleTable`: n-tuple 明示宣言物化 + tie 内自動反映
- `register_tuple_view` API
- query プランナ(観測窓優先のクエリ戦略)

これらは **世界線を物理化するロジック**で、本来は Ravn 層の責務。Enchu は「紐の素材」だけ持つ純粋な層であるべき(CLAUDE.md: 「伝播は Enchu の仕事ではない」)。

## 理想の層分離

| 層 | 責務 |
|---|---|
| **Enchu** | 紐、entity、Column、1-tuple Cylinder(BucketCylinder) |
| **Ravn** | クエリ = 世界線、観測窓(2-tuple 以上)の選択・物化・伝播 |
| **Studio** | UI 層。テーブル表示、ビュー定義 |

## 境界 API(Enchu → Ravn が使う)

Ravn は Enchu の以下プリミティブを叩く:

```rust
// Enchu が Ravn に提供するもの
impl Engine {
    // 素材
    pub fn define_himo(&mut self, name: &str, ht: HimoType, max_values: u32);
    pub fn tie(&self, eid: u32, himo: &str, value: u32);
    pub fn untie(&self, eid: u32, himo: &str);
    pub fn delete(&self, eid: u32);

    // 読み取り
    pub fn get(&self, eid: u32, himo: &str) -> Option<u32>;
    pub fn pull_raw(&self, himo: &str, value: u32) -> &[u32];
    pub fn iter_entities(&self) -> impl Iterator<Item = u32>;  // 追加想定

    // tie のフック(Ravn が観測窓を更新するため)
    pub fn on_tie(&self, callback: impl Fn(u32, usize, Option<u32>, u32));  // 追加想定
}
```

## Ravn 側が持つもの

```rust
// crates/ravn/src/view.rs など
pub struct TupleView {
    himos: Vec<HimoId>,
    cells: Vec<Vec<EntityId>>,  // flat cell array
}

pub struct Ravn {
    engine: Arc<Engine>,       // Enchu への参照
    views: Vec<TupleView>,     // 観測窓の集合
}

impl Ravn {
    pub fn register_view(&mut self, himos: &[&str]);
    pub fn query(&self, conds: &[(&str, u32)]) -> &[u32];  // slice 返し
    pub fn query_vec(&self, conds: &[(&str, u32)]) -> Vec<u32>;  // fallback
}
```

Ravn が Engine の tie を直接呼ぶのではなく、**ラップ**する:

```rust
impl Ravn {
    pub fn tie(&mut self, eid: u32, himo: &str, value: u32) {
        let old = self.engine.get(eid, himo);
        self.engine.tie(eid, himo, value);
        self.propagate_to_views(eid, himo, old, value);
    }
}
```

これで Enchu は **紐の即時反映**だけ責任持ち、Ravn が **観測窓の同期**を担う。

## 移行段階

### 段階 1: crates 分離(物理的に別 crate)

```
enchudb/              ← workspace root
  crates/
    enchu/            ← 紐だけの純粋エンジン
    ravn/             ← 観測窓 + クエリ言語
  examples/
```

enchu crate は `BucketCylinder`, `Column`, `HimoStore`, `Engine` 基本 API のみ。
ravn crate は `PairTable`, `NTupleTable`, `register_tuple_view` を持つ。

### 段階 2: 観測窓ロジックを ravn に移植

v27 の `src/engine.rs` から:

- `PairEntry`, `PairTable` → `crates/ravn/src/pair_table.rs`
- `NTupleEntry`, `NTupleTable` → `crates/ravn/src/tuple_view.rs`
- `apply_pair_delta_internal`, `apply_tuple_delta_internal` → Ravn 側の tie ラッパーに
- `register_tuple_view` API → Ravn のメソッドに
- query プランナ → Ravn に引き上げ(Enchu の query は 1 条件のみに)

### 段階 3: Enchu API 簡素化

Enchu の `Engine::query` は **1 紐のみ**をサポート。多条件は Ravn 側で組み立てる。

```rust
// Enchu(簡素化後)
impl Engine {
    pub fn pull_raw(&self, himo: &str, value: u32) -> &[u32];
    // query は削除 or 単一紐のみ
}

// Ravn(多条件を担当)
impl Ravn {
    pub fn query(&self, conds: &[(&str, u32)]) -> Vec<u32>;
    pub fn query_slice(&self, conds: &[(&str, u32)]) -> Option<&[u32]>;
}
```

### 段階 4: Enchu の tie に hook を追加

Ravn が観測窓を更新するため、Enchu の tie が「old→new の変化」を外部に知らせる必要がある。選択肢:

- **A. Callback 登録**: `engine.on_tie(|eid, himo_id, old, new| { ... })`
- **B. 値取得 + tie を 2 段に分ける**: Ravn 側で `let old = engine.get(...); engine.tie(...); ravn.propagate(old, new);`(既存の apply_pair_delta パターン)
- **C. Ravn の Engine ラッパーに tie メソッドを置く**: ユーザーは Ravn.tie を使う、Ravn 内部で Engine.tie + 観測窓更新

C が API 的にクリーン。ユーザーは Ravn 越しに操作、Enchu 直接叩きは避ける。

## 懸念点

- **API 破壊**: 現状 `db.query(&[(...)])` で直接多条件が動くが、Ravn 分離後は Ravn 経由になる。既存コードの書き換え必要
- **ベンチ互換**: v27_bench は Enchu 直叩き、Ravn 化後は Ravn 経由のベンチが別途必要
- **永続化**: 観測窓の登録情報を Ravn 側でどう永続化するか。Enchu のファイルに相乗りか、Ravn 専用ファイルか
- **並行性**: 観測窓の更新と読み取りの排他を Ravn が担う。Enchu は紐レベルの並行性だけ面倒見る
- **Studio への影響**: Studio はテーブル(紐の整列 + ビュー)を扱うので Ravn と接続。Enchu と直接対話しない

## 移行のタイミング

現時点では Ravn の実体がまだない。v27 は Enchu 内に観測窓を抱えた状態で動かし、以下のいずれかで移行する:

- Ravn を別プロジェクトとして立ち上げるタイミング
- EnchuDB を multi-crate workspace に再編するタイミング
- Studio との接続を設計するタイミング(Studio → Ravn → Enchu の層が必要になる)

それまでは Enchu に仮置き、思想的には Ravn 層の責務として扱う。
