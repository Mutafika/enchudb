//! BM25 インデックスとハイブリッド検索。
//!
//! # 設計判断
//!
//! - インデックスは**揮発インメモリ**。enchudb のメタ紐ではなくハッシュマップ。
//!   理由: BM25 は tf/df/長さ統計を頻繁に参照する。全部紐に持たせると rebuild コストが
//!   メリットを食う。20 万 chunk くらいまでは全部オンメモリで十分。
//! - `add_document(eid, text)` で差分追加。`rebuild_from_store()` で一括構築。
//! - トークナイザは空白 + 基本的な句読点分割。日本語は形態素解析しない（MeCab 呼ぶなら自前で）。
//!
//! # ハイブリッド: RRF (Reciprocal Rank Fusion)
//!
//! 2 つの順位付けリストを順位ベースでフュージョン。スコアスケールが異なる
//! ベクトル距離と BM25 を安全に混ぜられる。
//!
//!   rrf_score(d) = Σ_l 1 / (k + rank_l(d))

use enchudb_oplog::EntityId;
use std::collections::HashMap;

const BM25_K1: f32 = 1.2;
const BM25_B: f32 = 0.75;

pub struct Bm25Index {
    /// term → [(eid, tf), ...]
    postings: HashMap<String, Vec<(EntityId, u32)>>,
    /// eid → 文書長（トークン数）
    doc_len: HashMap<EntityId, u32>,
    total_len: u64,
    num_docs: u32,
}

impl Default for Bm25Index {
    fn default() -> Self { Self::new() }
}

impl Bm25Index {
    pub fn new() -> Self {
        Self {
            postings: HashMap::new(),
            doc_len: HashMap::new(),
            total_len: 0,
            num_docs: 0,
        }
    }

    pub fn num_docs(&self) -> u32 { self.num_docs }

    pub fn avg_doc_len(&self) -> f32 {
        if self.num_docs == 0 { 0.0 } else { self.total_len as f32 / self.num_docs as f32 }
    }

    pub fn add_document(&mut self, eid: EntityId, text: &str) {
        let tokens = tokenize(text);
        if tokens.is_empty() { return; }
        let mut local: HashMap<String, u32> = HashMap::new();
        for t in &tokens { *local.entry(t.clone()).or_insert(0) += 1; }
        for (term, tf) in local {
            self.postings.entry(term).or_default().push((eid, tf));
        }
        self.doc_len.insert(eid, tokens.len() as u32);
        self.total_len += tokens.len() as u64;
        self.num_docs += 1;
    }

    pub fn remove_document(&mut self, eid: EntityId) {
        if let Some(len) = self.doc_len.remove(&eid) {
            self.total_len = self.total_len.saturating_sub(len as u64);
            self.num_docs = self.num_docs.saturating_sub(1);
        }
        for plist in self.postings.values_mut() {
            plist.retain(|(e, _)| *e != eid);
        }
    }

    /// 全文検索。`candidates` が Some なら、その集合内に限定（メタフィルタ済み集合）。
    /// 返り値は (eid, score) の**スコア降順**。
    pub fn search(&self, query: &str, candidates: Option<&[EntityId]>) -> Vec<(EntityId, f32)> {
        let tokens = tokenize(query);
        if tokens.is_empty() { return Vec::new(); }
        let avgdl = self.avg_doc_len();
        let n = self.num_docs as f32;

        let mut scores: HashMap<EntityId, f32> = HashMap::new();
        for t in tokens {
            let Some(plist) = self.postings.get(&t) else { continue; };
            let df = plist.len() as f32;
            let idf = (((n - df + 0.5) / (df + 0.5)) + 1.0).ln();
            for (eid, tf) in plist {
                if let Some(set) = candidates {
                    // set はソート済み前提。線形スキャンを避けるなら二分探索。
                    if set.binary_search(eid).is_err() { continue; }
                }
                let dl = *self.doc_len.get(eid).unwrap_or(&0) as f32;
                let tf = *tf as f32;
                let score = idf * (tf * (BM25_K1 + 1.0))
                    / (tf + BM25_K1 * (1.0 - BM25_B + BM25_B * dl / avgdl.max(1.0)));
                *scores.entry(*eid).or_insert(0.0) += score;
            }
        }

        let mut out: Vec<(EntityId, f32)> = scores.into_iter().collect();
        out.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        out
    }
}

/// ASCII 空白 + 句読点 + 日本語句読点で分割。lowercase 化。
/// 本格的な日本語対応はユーザー側で lindera/vibrato 等を挟んで、
/// pre-tokenized テキスト（空白区切り）を add_document に渡すのが無難。
fn tokenize(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    for c in text.chars() {
        if c.is_ascii_whitespace()
            || matches!(c, '.' | ',' | '!' | '?' | ';' | ':' | '(' | ')' | '[' | ']' | '{' | '}'
                      | '"' | '\'' | '/' | '\\' | '|' | '-' | '_' | '~' | '`'
                      | '。' | '、' | '！' | '？' | '「' | '」' | '『' | '』')
        {
            if !buf.is_empty() { out.push(std::mem::take(&mut buf)); }
        } else {
            for lc in c.to_lowercase() { buf.push(lc); }
        }
    }
    if !buf.is_empty() { out.push(buf); }
    out
}

/// RRF で 2 つのランキングを結合する。
/// 返り値は `(eid, rrf_score)` の**スコア降順**。
pub fn rrf_fuse(
    a: &[(EntityId, f32)],
    b: &[(EntityId, f32)],
    k: usize,
) -> Vec<(EntityId, f32)> {
    let mut scores: HashMap<EntityId, f32> = HashMap::new();
    for (rank, (eid, _)) in a.iter().enumerate() {
        *scores.entry(*eid).or_insert(0.0) += 1.0 / (k as f32 + (rank + 1) as f32);
    }
    for (rank, (eid, _)) in b.iter().enumerate() {
        *scores.entry(*eid).or_insert(0.0) += 1.0 / (k as f32 + (rank + 1) as f32);
    }
    let mut out: Vec<(EntityId, f32)> = scores.into_iter().collect();
    out.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bm25_basic() {
        let mut ix = Bm25Index::new();
        ix.add_document(1u64, "the quick brown fox");
        ix.add_document(2u64, "the lazy dog sleeps");
        ix.add_document(3u64, "fox jumps over the lazy dog");
        let r = ix.search("fox dog", None);
        assert!(!r.is_empty());
        // doc 3 は fox と dog 両方を含むので top
        assert_eq!(r[0].0, 3u64);
    }

    #[test]
    fn bm25_candidates_filter() {
        let mut ix = Bm25Index::new();
        ix.add_document(1u64, "fox");
        ix.add_document(2u64, "fox");
        ix.add_document(3u64, "fox");
        let r = ix.search("fox", Some(&[1u64, 3]));
        let ids: Vec<EntityId> = r.iter().map(|(e, _)| *e).collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&1));
        assert!(ids.contains(&3));
        assert!(!ids.contains(&2));
    }

    #[test]
    fn rrf_combines() {
        let a: Vec<(EntityId, f32)> = vec![(1, 1.0), (2, 0.9), (3, 0.8)];
        let b: Vec<(EntityId, f32)> = vec![(3, 10.0), (1, 5.0), (4, 1.0)];
        let r = rrf_fuse(&a, &b, 60);
        // 1 と 3 が両リスト上位にいるので先頭に来る
        let top2: Vec<EntityId> = r.iter().take(2).map(|(e, _)| *e).collect();
        assert!(top2.contains(&1));
        assert!(top2.contains(&3));
    }
}
