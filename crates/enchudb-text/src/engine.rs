use std::collections::HashMap;
use std::io;
use std::path::Path;

use crate::bigram;
use crate::posting::PostingList;
use crate::storage::MappedIndex;

enum Backend {
    Memory {
        postings: PostingList,
        originals: HashMap<u32, String>,
    },
    Mapped(MappedIndex),
}

/// 全文検索エンジン。bigram 転置インデックス。
pub struct TextEngine {
    backend: Backend,
}

impl TextEngine {
    /// インメモリモード（構築用）
    pub fn new() -> Self {
        Self {
            backend: Backend::Memory {
                postings: PostingList::new(),
                originals: HashMap::new(),
            },
        }
    }

    /// mmap モードで開く（読み取り専用、即起動）
    pub fn open(path: &str) -> io::Result<Self> {
        let mapped = MappedIndex::open(Path::new(path))?;
        Ok(Self { backend: Backend::Mapped(mapped) })
    }

    /// ファイルに書き出し
    pub fn save(&mut self, path: &str) -> io::Result<()> {
        self.compact();
        match &self.backend {
            Backend::Memory { postings, originals } => {
                crate::storage::save(Path::new(path), postings.raw(), originals)
            }
            Backend::Mapped(_) => {
                Err(io::Error::new(io::ErrorKind::Unsupported, "mapped engine is read-only"))
            }
        }
    }

    /// entity にテキストを登録
    pub fn index(&mut self, eid: u32, text: &str) {
        let (postings, originals) = self.memory_mut();
        // 既存を削除
        if let Some(old) = originals.remove(&eid) {
            for bg in bigram::extract(&old) {
                postings.remove(bigram::to_key(bg), eid);
            }
        }
        for bg in bigram::extract(text) {
            postings.add(bigram::to_key(bg), eid);
        }
        originals.insert(eid, text.to_string());
    }

    /// entity のテキストを削除
    pub fn remove(&mut self, eid: u32) {
        let (postings, originals) = self.memory_mut();
        if let Some(old) = originals.remove(&eid) {
            for bg in bigram::extract(&old) {
                postings.remove(bigram::to_key(bg), eid);
            }
        }
    }

    /// 部分一致検索
    pub fn search(&self, query: &str) -> Vec<u32> {
        let bgs = bigram::extract(query);

        if bgs.is_empty() {
            if query.is_empty() { return vec![]; }
            return self.search_single_char(query);
        }

        let keys: Vec<u32> = bgs.iter().map(|bg| bigram::to_key(*bg)).collect();

        let candidates = match &self.backend {
            Backend::Memory { postings, .. } => postings.intersect(&keys),
            Backend::Mapped(m) => m.intersect(&keys),
        };

        // 原文照合で偽陽性を除外
        candidates.into_iter()
            .filter(|&eid| {
                self.get_text(eid).is_some_and(|text| text.contains(query))
            })
            .collect()
    }

    /// 原文を取得
    pub fn get_text(&self, eid: u32) -> Option<&str> {
        match &self.backend {
            Backend::Memory { originals, .. } => originals.get(&eid).map(|s| s.as_str()),
            Backend::Mapped(m) => m.get_text(eid),
        }
    }

    /// posting list を最適化
    pub fn compact(&mut self) {
        if let Backend::Memory { postings, .. } = &mut self.backend {
            postings.compact();
        }
    }

    /// 統計
    pub fn bigram_count(&self) -> usize {
        match &self.backend {
            Backend::Memory { postings, .. } => postings.key_count(),
            Backend::Mapped(m) => m.bigram_count() as usize,
        }
    }

    pub fn doc_count(&self) -> usize {
        match &self.backend {
            Backend::Memory { originals, .. } => originals.len(),
            Backend::Mapped(m) => m.doc_count() as usize,
        }
    }

    // ── internal ──

    fn memory_mut(&mut self) -> (&mut PostingList, &mut HashMap<u32, String>) {
        match &mut self.backend {
            Backend::Memory { postings, originals } => (postings, originals),
            Backend::Mapped(_) => panic!("mapped engine is read-only"),
        }
    }

    fn search_single_char(&self, query: &str) -> Vec<u32> {
        match &self.backend {
            Backend::Memory { originals, .. } => {
                originals.iter()
                    .filter(|(_, text)| text.contains(query))
                    .map(|(&eid, _)| eid)
                    .collect()
            }
            Backend::Mapped(m) => {
                // mmap の全 doc を走査（1文字検索は稀なので許容）
                let mut result = Vec::new();
                for eid in 0..u32::MAX {
                    match m.get_text(eid) {
                        Some(text) if text.contains(query) => result.push(eid),
                        Some(_) => {}
                        None => {
                            // doc_index はソート済みなので、存在しない eid が来たら
                            // 全部チェック済みかは分からない。全 doc 走査に切り替え。
                            break;
                        }
                    }
                }
                // フォールバック: doc_index を直接走査
                m.search_all(|text| text.contains(query))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_and_search() {
        let mut eng = TextEngine::new();
        eng.index(0, "国民は法の下に平等であって");
        eng.index(1, "すべて国民は個人として尊重される");
        eng.index(2, "法の支配は民主主義の基盤である");
        eng.compact();

        let r = eng.search("国民");
        assert!(r.contains(&0));
        assert!(r.contains(&1));
        assert!(!r.contains(&2));

        let r = eng.search("法の");
        assert!(r.contains(&0));
        assert!(r.contains(&2));

        let r = eng.search("法の下");
        assert_eq!(r, vec![0]);
    }

    #[test]
    fn search_no_match() {
        let mut eng = TextEngine::new();
        eng.index(0, "テスト文字列");
        eng.compact();
        assert_eq!(eng.search("存在しない"), vec![]);
    }

    #[test]
    fn search_single_char() {
        let mut eng = TextEngine::new();
        eng.index(0, "猫は動物");
        eng.index(1, "犬も動物");
        eng.compact();

        let r = eng.search("猫");
        assert_eq!(r, vec![0]);
    }

    #[test]
    fn false_positive_filtered() {
        let mut eng = TextEngine::new();
        eng.index(0, "法の解釈と下書き");
        eng.index(1, "法の下に平等");
        eng.compact();

        let r = eng.search("法の下");
        assert_eq!(r, vec![1]);
    }

    #[test]
    fn update_text() {
        let mut eng = TextEngine::new();
        eng.index(0, "古いテキスト");
        eng.compact();
        assert_eq!(eng.search("古い").len(), 1);

        eng.index(0, "新しいテキスト");
        eng.compact();
        assert_eq!(eng.search("古い").len(), 0);
        assert_eq!(eng.search("新しい").len(), 1);
    }

    #[test]
    fn remove_text() {
        let mut eng = TextEngine::new();
        eng.index(0, "削除対象テキスト");
        eng.compact();

        eng.remove(0);
        assert_eq!(eng.search("削除").len(), 0);
        assert_eq!(eng.doc_count(), 0);
    }

    #[test]
    fn stats() {
        let mut eng = TextEngine::new();
        eng.index(0, "あいう");
        eng.index(1, "いうえ");
        eng.compact();
        assert_eq!(eng.doc_count(), 2);
        assert!(eng.bigram_count() > 0);
    }

    #[test]
    fn ascii_search() {
        let mut eng = TextEngine::new();
        eng.index(0, "hello world");
        eng.index(1, "hello rust");
        eng.index(2, "goodbye world");
        eng.compact();

        let r = eng.search("hello");
        assert!(r.contains(&0));
        assert!(r.contains(&1));
        assert!(!r.contains(&2));
    }

    #[test]
    fn mixed_jp_ascii() {
        let mut eng = TextEngine::new();
        eng.index(0, "Rust言語でDB構築");
        eng.compact();
        assert_eq!(eng.search("Rust言語").len(), 1);
        assert_eq!(eng.search("Python").len(), 0);
    }

    #[test]
    fn save_and_open() {
        let path = "/tmp/enchu_text_test_save.etxt";
        let _ = std::fs::remove_file(path);

        // 構築 → 保存
        let mut eng = TextEngine::new();
        eng.index(0, "国民は法の下に平等であって");
        eng.index(1, "すべて国民は個人として尊重される");
        eng.index(2, "法の支配は民主主義の基盤である");
        eng.save(path).unwrap();

        // mmap で開く → 検索
        let eng2 = TextEngine::open_standalone(path).unwrap();
        assert_eq!(eng2.doc_count(), 3);

        let r = eng2.search("国民");
        assert!(r.contains(&0));
        assert!(r.contains(&1));
        assert!(!r.contains(&2));

        let r = eng2.search("法の下");
        assert_eq!(r, vec![0]);

        assert_eq!(eng2.get_text(0), Some("国民は法の下に平等であって"));
        assert_eq!(eng2.get_text(99), None);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn open_nonexistent() {
        assert!(TextEngine::open_standalone("/tmp/does_not_exist.etxt").is_err());
    }
}
