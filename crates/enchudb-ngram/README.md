# enchudb-ngram

bigram 転置インデックスの **index プリミティブ**。mmap 永続化対応。entity ID は **u64**（EnchuDB v32 以降の eid 幅）。

担当は「bigram 抽出 → posting → intersect → **候補 doc id**」までの汎用部分で、
**検索意味論は持たない**。部分一致 (`.contains()` 検証) や単一文字フォールバックといった
ポリシーは上位の [`enchudb-textsearch`](../enchudb-textsearch) が乗せる
（[issue #69](https://github.com/Mutafika/enchudb/issues/69) のレイヤリング:
`ngram`(primitive) → `textsearch`(policy) [→ `phrase`]）。

## 使い方

```rust
use enchudb_ngram::NgramIndex;

// 構築
let mut idx = NgramIndex::new();
idx.index(0, "国民は法の下に平等であって");
idx.index(1, "すべて国民は個人として尊重される");
idx.index(2, "法の支配は民主主義の基盤である");

// 候補探索（bigram intersect、substring 検証は無し）
idx.candidates("国民")   // → [0, 1]  (1 bigram は候補 == 正確一致)
idx.candidates("法の下") // → 候補（2 bigram 以上は偽陽性を含みうる）

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

部分一致（substring）検索が欲しいなら `enchudb-textsearch` を使う:

```rust
use enchudb_textsearch::TextSearch;

let mut eng = TextSearch::new();
eng.index(0, "法の解釈と下書き");
eng.index(1, "法の下に平等");
eng.search("法の下") // → [1]  (偽陽性を .contains() で除外)
```

## 仕組み

1. `index()` — 文字列を bigram（2文字ずつ）に分割して逆引きインデックスに登録
2. `candidates()` — クエリを bigram に分割 → 全 bigram を持つ entity を AND で絞り込み（**候補**）
3. `scan()` — 全 doc を述語で走査（O(N)、bigram で絞れないケース用）
4. `save()` — インデックスをファイルに書き出し
5. `open()` — mmap でファイルをマッピング。ロード不要、即起動

## API

```rust
// インメモリ（構築用）
NgramIndex::new() -> NgramIndex

// mmap（読み取り専用、即起動）
NgramIndex::open(path: &str) -> io::Result<NgramIndex>

// 書き込み（インメモリのみ）
idx.index(eid: u64, text: &str)
idx.remove(eid: u64)
idx.save(path: &str) -> io::Result<()>
idx.compact()

// 候補探索 / 走査（両モード）
idx.candidates(query: &str) -> Vec<u64>          // bigram intersect（候補）
idx.scan(pred: impl Fn(&str) -> bool) -> Vec<u64> // 全 doc 走査
idx.get_text(eid: u64) -> Option<&str>
idx.doc_count() -> usize
idx.bigram_count() -> usize
```

## ファイル形式 (.etxt, version 2)

```
[Header 32B] magic "ETXT" + version=2 + メタデータ
[Bigram Index]  bigram_count × 12B    key u32 + offset u32 + len u32
[Padding]       0..=7B                Posting Data を 8-byte 境界に揃える
[Posting Data]  posting_total × 8B    flat array of u64 entity IDs
[Doc Index]     doc_count × 16B       eid u64 + offset u32 + len u32
[Text Data]     text_total B          UTF-8 bytes
```

**互換性:** version 1（eid u32 時代）の `.etxt` は読めない。アプリ側で再生成する必要がある。
ファイル format / magic `ETXT` は `enchudb-text` から不変なので、既存の `.etxt` はそのまま読める。
