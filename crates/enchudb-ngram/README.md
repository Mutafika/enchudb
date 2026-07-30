# enchudb-ngram

n-gram 転置インデックスの **index プリミティブ**。mmap 永続化対応。entity ID は **u64**（EnchuDB v32 以降の eid 幅）。

担当は「n-gram 抽出 → posting → intersect → **候補 doc id**」までの汎用部分で、
**検索意味論は持たない**。部分一致 (`.contains()` 検証) や単一文字フォールバックといった
ポリシーは上位の [`enchudb-textsearch`](../enchudb-textsearch) が乗せる
（[issue #69](https://github.com/Mutafika/enchudb/issues/69) のレイヤリング:
`ngram`(primitive) → `textsearch`(policy) [→ `phrase`]）。

## 使い方

```rust
use enchudb_ngram::NgramIndex;

// 構築（n は既定の 2）
let mut idx = NgramIndex::new();
idx.index(0, "国民は法の下に平等であって");
idx.index(1, "すべて国民は個人として尊重される");
idx.index(2, "法の支配は民主主義の基盤である");

// 候補探索（n-gram intersect、substring 検証は無し）
idx.candidates("国民")   // → [0, 1]  (n 文字ちょうどは候補 == 正確一致)
idx.candidates("法の下") // → 候補（n 文字超は偽陽性を含みうる）

// 全 doc 走査（単一文字や substring 以外のポリシー用フック）
idx.scan(|t| t.contains("猫"))

// 原文取得
idx.get_text(0) // → Some("国民は法の下に平等であって")

// 保存
idx.save("search.etxt").unwrap();
```

```rust
// 別プロセスで即起動（mmap）
let idx = NgramIndex::open("search.etxt").unwrap();
idx.candidates("法の下");
```

## n を選ぶ (#121)

n は **index 自身が覚えている**。`with_n` で build 時に決め、`.etxt` header に焼かれ、
`open` した側は `n()` で取れる。呼び出し側が n を覚える必要はなく、build と query で
n がズレる事故が構造的に起きない。

```rust
let mut idx = NgramIndex::with_n(3).unwrap();   // 2..=4。既定は 2
idx.index(0, "hello world");
idx.save("corpus.etxt").unwrap();

let idx = NgramIndex::open("corpus.etxt").unwrap();
assert_eq!(idx.n(), 3);
assert_eq!(idx.min_query_len(), 3);   // これ未満のクエリは index で絞れない
```

最適な n は **スクリプトとコーパス依存**で、一律の正解は無い:

- **ASCII / 英語** — bigram のエントロピーが低い（`th` `he` `in` が全 doc に出る）ので
  候補が絞れず、上位層の `.contains()` 検証コストが爆発する。n を上げると効く。
- **CJK / 日本語** — bigram で既に十分エントロピーが高い。しかも `国民` のような
  **2 文字クエリが最頻**なので、n=3 にすると index で絞れず O(N) 全走査に落ちる。

`examples/ngram_n_bench.rs` が同じコーパスで n = 2/3/4 の index サイズ・偽陽性率・
レイテンシを出す。判断はこれで取る。

```sh
cargo run --release -p enchudb-ngram --example ngram_n_bench -- ja=corpus.txt
```

**key は hash ではない** — 文字を 16bit ずつ詰めた exact 値なので `n ≤ 4` なら衝突ゼロ。
「n 文字ちょうどのクエリは候補がそのまま正確一致（検証不要）」が n によらず成立する。

部分一致（substring）検索が欲しいなら `enchudb-textsearch` を使う:

```rust
use enchudb_textsearch::TextSearch;

let mut eng = TextSearch::new();
eng.index(0, "法の解釈と下書き");
eng.index(1, "法の下に平等");
eng.search("法の下") // → [1]  (偽陽性を .contains() で除外)
```

## 仕組み

1. `index()` — 文字列を n-gram（n 文字ずつ）に分割して逆引きインデックスに登録
2. `candidates()` — クエリを n-gram に分割 → 全 gram を持つ entity を AND で絞り込み（**候補**）
3. `scan()` — 全 doc を述語で走査（O(N)、n-gram で絞れないケース用）
4. `save()` — インデックスをファイルに書き出し
5. `open()` — mmap でファイルをマッピング。ロード不要、即起動

## API

```rust
// インメモリ（構築用）
NgramIndex::new() -> NgramIndex                    // n = 2
NgramIndex::with_n(n: usize) -> io::Result<NgramIndex>  // n = 2..=4

// mmap（読み取り専用、即起動）
NgramIndex::open(path: &str) -> io::Result<NgramIndex>

// 書き込み（インメモリのみ）
idx.index(eid: u64, text: &str)
idx.remove(eid: u64)
idx.save(path: &str) -> io::Result<()>
idx.compact()

// 候補探索 / 走査（両モード）
idx.candidates(query: &str) -> Vec<u64>          // n-gram intersect（候補）
idx.scan(pred: impl Fn(&str) -> bool) -> Vec<u64> // 全 doc 走査
idx.get_text(eid: u64) -> Option<&str>
idx.doc_count() -> usize
idx.gram_count() -> usize                        // 旧名 bigram_count も残置
idx.n() -> usize                                 // この index の n
idx.min_query_len() -> usize                     // = n
```

## ファイル形式 (.etxt, version 2 / 3)

```
[Header 32B] magic "ETXT" + version + メタデータ (+ v3 は n)
[Gram Index]    gram_count × 12B     v2: key u32 + offset u32 + len u32
                gram_count × 16B     v3: key u64 + offset u32 + len u32
[Padding]       0..=7B                Posting Data を 8-byte 境界に揃える
[Posting Data]  posting_total × 8B    flat array of u64 entity IDs
[Doc Index]     doc_count × 16B       eid u64 + offset u32 + len u32
[Text Data]     text_total B          UTF-8 bytes
```

v2 と v3 の違いは **gram key の幅だけ**。n = 2 の key は u64 でも上位 32bit が 0 なので、
v2 はゼロ拡張するだけで読め、昇順ソート順も保たれる（二分探索がそのまま効く）。

**互換性:**

- **v2 (`n = 2`) は読み書きとも従来どおり。** `NgramIndex::new()` の出力は #121 以前と
  **バイト等価**（`tests/issue121_variable_n.rs` が手組みバイト列に対して固定している）。
  v3 が出るのは `with_n(3)` / `with_n(4)` を明示したときだけ。
- version 1（eid u32 時代）の `.etxt` は読めない。アプリ側で再生成する必要がある。
