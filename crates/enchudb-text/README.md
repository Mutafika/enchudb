# enchu-text

bigram 転置インデックスによる全文検索エンジン。mmap 永続化対応。

## 使い方

```rust
use enchu_text::TextEngine;

// 構築
let mut eng = TextEngine::new();
eng.index(0, "国民は法の下に平等であって");
eng.index(1, "すべて国民は個人として尊重される");
eng.index(2, "法の支配は民主主義の基盤である");

// 検索
eng.search("国民")   // → [0, 1]
eng.search("法の下") // → [0]

// 原文取得
eng.get_text(0) // → Some("国民は法の下に平等であって")

// 保存
eng.save("search.etxt").unwrap();
```

```rust
// 別プロセスで即起動（mmap）
let eng = TextEngine::open("search.etxt").unwrap();
eng.search("法の下") // → [0]
```

## enchudb と組み合わせる

```rust
use enchudb::Engine;
use enchu_text::TextEngine;

let db = Engine::open("app.db").unwrap();
let text = TextEngine::open("app.etxt").unwrap();

// テキスト検索 → 構造化フィルタ → 集計
let hits = text.search("平等");
let active: Vec<u32> = hits.iter()
    .filter(|&&eid| db.get(eid, "status") == Some(1))
    .copied().collect();
let total = db.sum("score", &active);
```

## 仕組み

1. `index()` — 文字列を bigram（2文字ずつ）に分割して逆引きインデックスに登録
2. `search()` — クエリを bigram に分割 → 全 bigram を持つ entity を AND で絞り込み → 原文照合で偽陽性を除外
3. `save()` — インデックスをファイルに書き出し
4. `open()` — mmap でファイルをマッピング。ロード不要、即検索可能

## API

```rust
// インメモリ（構築用）
TextEngine::new() -> TextEngine

// mmap（読み取り専用、即起動）
TextEngine::open(path: &str) -> io::Result<TextEngine>

// 書き込み（インメモリのみ）
eng.index(eid: u32, text: &str)
eng.remove(eid: u32)
eng.save(path: &str) -> io::Result<()>
eng.compact()

// 読み取り（両モード）
eng.search(query: &str) -> Vec<u32>
eng.get_text(eid: u32) -> Option<&str>
eng.doc_count() -> usize
eng.bigram_count() -> usize
```

## ファイル形式 (.etxt)

```
[Header 32B] magic "ETXT" + メタデータ
[Bigram Index] key → (offset, len) のソート済み配列
[Posting Data] entity ID の flat 配列
[Doc Index] eid → (offset, len) のソート済み配列
[Text Data] 原文バイト列
```
