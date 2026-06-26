//! enchudb-ngram — bigram 転置インデックスの **index プリミティブ**。
//!
//! 担当は「bigram 抽出 / posting / intersect → 候補 doc id」までの汎用部分で、
//! **検索意味論 (substring / phrase) は一切持たない**。部分一致 (`.contains()` 検証) や
//! 単一文字フォールバックといった「ポリシー」は上位の `enchudb-textsearch` が乗せる。
//!
//! 候補探索 ([`NgramIndex::candidates`]) と全 doc 走査 ([`NgramIndex::scan`]) を公開する
//! ので、`substring` 以外のポリシー（phrase 完全一致など）も同じ index の上に組める。

pub mod bigram;
mod posting;
pub(crate) mod storage;
mod index;

pub use index::NgramIndex;
