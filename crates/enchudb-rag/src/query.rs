//! クエリと結果型。

use crate::Filter;

/// ベクトル検索クエリ。
pub struct Query<'a> {
    pub vector: &'a [f32],
    pub filter: Filter,
    pub top_k: usize,
}

impl<'a> Query<'a> {
    pub fn new(vector: &'a [f32]) -> Self {
        Self { vector, filter: Filter::all(), top_k: 10 }
    }

    pub fn filter(mut self, f: Filter) -> Self { self.filter = f; self }
    pub fn top_k(mut self, k: usize) -> Self { self.top_k = k; self }
}

/// ハイブリッド検索クエリ（ベクトル + BM25 + RRF）。
pub struct HybridQuery<'a> {
    pub vector: &'a [f32],
    pub text: &'a str,
    pub filter: Filter,
    pub top_k: usize,
    /// RRF の k 定数（一般的に 60）。
    pub rrf_k: usize,
}

impl<'a> HybridQuery<'a> {
    pub fn new(vector: &'a [f32], text: &'a str) -> Self {
        Self {
            vector,
            text,
            filter: Filter::all(),
            top_k: 10,
            rrf_k: 60,
        }
    }

    pub fn filter(mut self, f: Filter) -> Self { self.filter = f; self }
    pub fn top_k(mut self, k: usize) -> Self { self.top_k = k; self }
    pub fn rrf_k(mut self, k: usize) -> Self { self.rrf_k = k; self }
}

/// 検索結果の 1 行。
#[derive(Clone, Debug)]
pub struct Hit {
    pub eid: u64,
    /// 距離（metric 依存、小さいほど近い）。ハイブリッド時は RRF スコアが入り大きいほど良い。
    pub score: f32,
    /// テキスト（search 側で詰める）。
    pub text: Option<String>,
}
