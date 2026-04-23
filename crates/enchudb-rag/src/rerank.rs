//! Reranker trait。
//!
//! 1st-stage（ベクトル + BM25 の RRF）で top-K を取ってきた後、
//! よりコストのかかる cross-encoder などで並び替えるフェーズ。
//!
//! 提供はトレイトのみ。実実装はユーザー側で Cohere Rerank / bge-reranker / voyage 等を叩く。

use crate::Hit;

pub trait Reranker: Send + Sync {
    /// クエリ文字列と hits を受け取り、並び替えた hits を返す。
    /// score は reranker 固有のスコール（大きいほど良いことが多い）に差し替えて良い。
    fn rerank(&self, query: &str, hits: Vec<Hit>) -> Vec<Hit>;
}

/// 何もしない reranker（NOP）。テスト用。
pub struct NoopReranker;

impl Reranker for NoopReranker {
    fn rerank(&self, _query: &str, hits: Vec<Hit>) -> Vec<Hit> { hits }
}

/// テキストの長さで並び替えるだけの reranker（長い方を上位に）。
/// デモ・単体テスト用。実用価値はない。
pub struct LengthReranker;

impl Reranker for LengthReranker {
    fn rerank(&self, _query: &str, mut hits: Vec<Hit>) -> Vec<Hit> {
        hits.sort_by_key(|h| std::cmp::Reverse(h.text.as_ref().map(|s| s.len()).unwrap_or(0)));
        hits
    }
}
