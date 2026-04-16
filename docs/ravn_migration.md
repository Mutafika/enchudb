# Ravn と Enchu の層分離

## 要旨

Ravn は **クエリ言語層**。Enchu のプリミティブ(tie/pull_raw/get)を連鎖させて、パス式や多条件
クエリ、伝播(JOIN 相当)を組み立てる。Prisma Client や SQL の位置。

Ravn は **物化層ではない**。観測窓(n-tuple 仮想テーブル定義)や PairTable は Enchu の
**スキーマの一部** として Enchu 側に残る。`define_himo` と同じレベルで `define_view` も
Enchu のスキーマ API。DB ファイルに永続化され、open 後に自動復元される。

## 層構造

| 層 | 責務 |
|---|---|
| **Enchu** | 紐 + 観測窓(スキーマ) + 物理ストレージ + プリミティブ API |
| **Ravn** | クエリ言語(文法パーサ + プランナ + 実行エンジン)。パス式、多条件、伝播を組み立てる |
| **Studio** | UI 層。テーブル表示、ビュー定義の入力 |

Enchu は「紐と観測窓を持つストレージ」。Ravn は「クエリを組み立てて Enchu に投げる言語」。

## Enchu が持つもの(Ravn はこれを使う)

```rust
impl Engine {
    // ──── スキーマ ────
    pub fn define_himo(&mut self, name: &str, ht: HimoType, max_values: u32);
    pub fn define_view(&mut self, himos: &[&str]) -> Result<(), String>;  // 観測窓(永続化)

    // ──── 書き込み(内部で観測窓に自動反映) ────
    pub fn tie(&mut self, eid: u32, himo: &str, value: u32);
    pub fn untie(&self, eid: u32, himo: &str);
    pub fn delete(&self, eid: u32);

    // ──── 読み取り(観測窓があれば自動利用) ────
    pub fn get(&self, eid: u32, himo: &str) -> Option<u32>;
    pub fn pull_raw(&self, himo: &str, value: u32) -> Vec<u32>;  // 1 紐
    pub fn query(&self, conds: &[(&str, u32)]) -> Vec<u32>;      // 多条件(観測窓を自動選択)

    // ──── スキーマ問い合わせ ────
    pub fn himo_type(&self, himo: &str) -> Option<HimoType>;
    pub fn himo_cardinality(&self, himo: &str) -> Option<u32>;   // v27、現在の unique 値数(O(1))
}
```

観測窓は `define_himo` 同様、スキーマとして DB ファイルに書き込まれる。open 後に
`define_view` を再実行する必要はない。

## Ravn が担うもの

Ravn は Enchu のプリミティブを叩いて、より高水準のクエリ言語を提供する。

```rust
// crates/ravn などで
pub struct Ravn {
    engine: Arc<Engine>,
}

impl Ravn {
    // パス式: user.dept.company.name のようなたどり
    pub fn path(&self, eid: u32, path: &[&str]) -> Vec<EntityValue>;

    // 文法パーサ: "age:30 city:\"東京\" | count" のような DSL
    pub fn exec(&self, query: &str) -> QueryResult;

    // JOIN 相当の伝播(Enchu の pull_raw/get を連鎖)
    pub fn follow(&self, start: &[u32], path: &[&str]) -> Vec<u32>;
}
```

Ravn は Enchu の内部構造(PairTable, NTupleTable)には触れない。Enchu が公開する
`query/pull_raw/get/tie` だけを使う。

## なぜ観測窓を Enchu に置くのか

- **スキーマは Enchu の責務**。紐定義(`define_himo`)と観測窓(`define_view`)は同じ粒度。
  両方とも「データの物理配置の宣言」であり、永続化が必要。
- **書き込み時の自動反映**。`tie` は Enchu の API。tie 時に観測窓を同期するには Enchu 内で
  観測窓を持つのが素直。Ravn 側に持たせると Ravn.tie ラッパー必須となり、Enchu.tie を
  隠蔽することになる(API 分離の目的に反する)。
- **Ravn は stateless なクエリ言語**。Ravn が観測窓(mutable state)を持つと、Ravn インスタンスに
  データが紐づき、Ravn を作り直すたびに再構築が必要になる。Enchu に置けば DB ファイルが
  truth of source。

## Ravn の実装メモ(将来)

- Ravn crate を別に切る場合、Enchu の `Engine` を `Arc<Engine>` で持つだけ
- Ravn 独自の状態は **なし**(あるいはパースキャッシュなど軽いもののみ)
- 文法は既存の `src/query_lang.rs` をベースに拡張

## 旧メモ(破棄)

以前のバージョンでは「観測窓を Ravn に引き上げる」方向で書かれていたが、上記の理由で
方針転換。Enchu が観測窓を永続化スキーマとして持ち続ける。
