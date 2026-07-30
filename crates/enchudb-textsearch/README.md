# enchudb-textsearch

[`enchudb-ngram`](../enchudb-ngram) の上に乗る **テキスト検索ポリシー**。
n-gram 候補を `.contains()` で検証して正確な部分一致 (substring) を返す。

クレート名は「`text` が検索という正体を隠していた」という
[issue #69](https://github.com/Mutafika/enchudb/issues/69) の元の不満を直す意図で
`textsearch`（= search over text）。人間の部分一致と機械のフレーズ完全一致、両モードの傘。

部分一致は**人間の対話検索に正しい挙動**: `接地` で `接地極` / `接地工事` が出るのは望ましい UX。
**機械向けフレーズ完全一致**（LLM grounding 等）も、入力フレーズを **1 単位**で `search()` に
渡せば同じ path で扱える（断片に割らないこと）。

## 使い方

```rust
use enchudb_textsearch::TextSearch;

let mut eng = TextSearch::new();
eng.index(0, "法の解釈と下書き");
eng.index(1, "法の下に平等");
eng.compact();

eng.search("法の下"); // → [1]  (doc 0 は候補に入るが .contains() 検証で落ちる)

eng.save("search.etxt").unwrap();
```

```rust
// 別プロセスで即起動（mmap）
let eng = TextSearch::open("search.etxt").unwrap();
eng.search("法の下");
```

## クエリ長と n (#121)

n は下層の index が覚えているので、**このレイヤの挙動は n に自動追従する**。
`with_n(n)` で build した index を開けば `n()` / `min_query_len()` で取れる。

| クエリ長 | 原文あり | postings-only (原文なし、#84) |
|---|---|---|
| 0 | `[]` | `[]` |
| < n | O(N) 全走査 (`scan`) | **`try_search` が `Unsupported`** |
| == n | 候補 = 正確一致（検証スキップ） | 同じ（検証が要らないので OK） |
| > n | 候補 → `.contains()` 検証 | **`try_search` が `Unsupported`** |

`search()` は互換のため `Vec` を返し続けるので、**答えを出せない組合せでは空を返す**。
「空 = 該当なし」と「空 = 答えられなかった」を区別したい呼び出し元は `try_search()` を使う
（postings-only index に対する `search()` が静かに空を返すのは #121 以前からの挙動で、
`try_search` はそれを明示エラーにする）。

```rust
let eng = TextSearch::open("postings_only.etxt").unwrap();
match eng.try_search("法") {
    Ok(hits) => { /* ... */ }
    Err(_) => {
        // index では答えられない → 生候補を取って自分の source で検証する
        let cand = eng.candidates("法の下");
    }
}
```

API は旧 `enchudb-text::TextEngine` とほぼ同型（`new` / `open` / `open_mut` / `from_bytes` /
`save` / `index` / `remove` / `compact` / `search` / `get_text` / `doc_count` /
`bigram_count`）なので、移行は dep 差し替え + `TextEngine` → `TextSearch` のリネームで済む。
（`bigram_count` は `gram_count` に改名。旧名も残置。）

候補探索（n-gram intersect）や全 doc 走査を直接叩きたい場合は `eng.ngram()` で内包する
[`NgramIndex`](../enchudb-ngram) に降りられる。

## レイヤリング (#69)

```
enchudb-ngram      (primitive)  n-gram 抽出 / posting / intersect → 候補 doc id
      ↓
enchudb-textsearch (policy)     候補 + .contains() 検証 → 正確な部分一致（人間）
      ↓ (optional)
enchudb-phrase     (policy)     「分割しない・断片を投げない」を API で強制（機械、未実装）
```
