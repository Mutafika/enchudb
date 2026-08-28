//! enchudb-ngram — n-gram 転置インデックスの **index プリミティブ**。
//!
//! 担当は「n-gram 抽出 / posting / intersect → 候補 doc id」までの汎用部分で、
//! **検索意味論 (substring / phrase) は一切持たない**。部分一致 (`.contains()` 検証) や
//! 短いクエリのフォールバックといった「ポリシー」は上位の `enchudb-textsearch` が乗せる。
//!
//! 候補探索 ([`NgramIndex::candidates`]) と全 doc 走査 ([`NgramIndex::scan`]) を公開する
//! ので、`substring` 以外のポリシー（phrase 完全一致など）も同じ index の上に組める。
//!
//! ## n は index 自身が覚えている (#121)
//!
//! n は [`NgramIndex::with_n`] で選び、`.etxt` header に焼かれ、[`NgramIndex::open`] で
//! 戻る。呼び出し側が n を覚える必要はなく、build と query で n がズレる事故が構造的に
//! 起きない。既定は 2（現行互換 — 出力バイト列も #121 以前と同一）。
//!
//! 最適な n はスクリプトとコーパス依存で、一律の正解は無い:
//! - **ASCII / 英語**: bigram のエントロピーが低い（`th` `he` `in` が全 doc に出る）ので
//!   posting list が肥大化して候補が絞れない。trigram で分布が散る。
//! - **CJK / 日本語**: bigram で既に十分エントロピーが高く、しかも `国民` のような
//!   2 文字クエリが最頻。n=3 にすると index で絞れず O(N) 全走査に落ちる。
//!
//! 実測して選ぶための道具として `examples/ngram_n_bench.rs` を同梱している。

pub mod bigram;
pub mod gram;
mod posting;
pub(crate) mod storage;
mod index;

pub use index::NgramIndex;
// #188: merge に要る 2 型だけを出す。 モジュールごと公開すると
// `save` / `write_to` 系 4 本の生 writer 入口が NgramIndex の wrapper と
// 二重に露出し、 以後あれを触るたび breaking 判定になる。
pub use storage::{MappedIndex, MergeStats};
