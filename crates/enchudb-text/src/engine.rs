use std::collections::HashMap;
use std::io;
use std::path::Path;

use crate::bigram;
use crate::posting::PostingList;
use crate::storage::MappedIndex;

enum Backend {
    Memory {
        postings: PostingList,
        originals: HashMap<u64, String>,
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

    /// 既存の .etxt を読み込んで in-memory mutable engine に再構築する。
    /// `open()` と違って後から `index()` / `remove()` できる。
    /// コストは「全 doc を読んで再 index」する分。
    pub fn open_mut(path: &str) -> io::Result<Self> {
        let mapped = MappedIndex::open(Path::new(path))?;
        let mut eng = TextEngine::new();
        mapped.for_each_doc(|eid, text| eng.index(eid, text));
        eng.compact();
        Ok(eng)
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
    pub fn index(&mut self, eid: u64, text: &str) {
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
    pub fn remove(&mut self, eid: u64) {
        let (postings, originals) = self.memory_mut();
        if let Some(old) = originals.remove(&eid) {
            for bg in bigram::extract(&old) {
                postings.remove(bigram::to_key(bg), eid);
            }
        }
    }

    /// 部分一致検索
    pub fn search(&self, query: &str) -> Vec<u64> {
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
    pub fn get_text(&self, eid: u64) -> Option<&str> {
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

    fn memory_mut(&mut self) -> (&mut PostingList, &mut HashMap<u64, String>) {
        match &mut self.backend {
            Backend::Memory { postings, originals } => (postings, originals),
            Backend::Mapped(_) => panic!("mapped engine is read-only"),
        }
    }

    fn search_single_char(&self, query: &str) -> Vec<u64> {
        // 1 文字クエリは bigram で絞れないので doc_index を全走査する。
        // 稀なケースなので O(N) で許容。
        match &self.backend {
            Backend::Memory { originals, .. } => {
                originals.iter()
                    .filter(|(_, text)| text.contains(query))
                    .map(|(&eid, _)| eid)
                    .collect()
            }
            Backend::Mapped(m) => {
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
        assert_eq!(r, vec![0u64]);
    }

    #[test]
    fn search_no_match() {
        let mut eng = TextEngine::new();
        eng.index(0, "テスト文字列");
        eng.compact();
        assert_eq!(eng.search("存在しない"), Vec::<u64>::new());
    }

    #[test]
    fn search_single_char() {
        let mut eng = TextEngine::new();
        eng.index(0, "猫は動物");
        eng.index(1, "犬も動物");
        eng.compact();

        let r = eng.search("猫");
        assert_eq!(r, vec![0u64]);
    }

    #[test]
    fn false_positive_filtered() {
        let mut eng = TextEngine::new();
        eng.index(0, "法の解釈と下書き");
        eng.index(1, "法の下に平等");
        eng.compact();

        let r = eng.search("法の下");
        assert_eq!(r, vec![1u64]);
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
        let eng2 = TextEngine::open(path).unwrap();
        assert_eq!(eng2.doc_count(), 3);

        let r = eng2.search("国民");
        assert!(r.contains(&0));
        assert!(r.contains(&1));
        assert!(!r.contains(&2));

        let r = eng2.search("法の下");
        assert_eq!(r, vec![0u64]);

        assert_eq!(eng2.get_text(0), Some("国民は法の下に平等であって"));
        assert_eq!(eng2.get_text(99), None);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn save_and_open_wide_eid() {
        // v32 の peer_id を含む u64 eid でも round-trip する
        let path = "/tmp/enchu_text_test_wide_eid.etxt";
        let _ = std::fs::remove_file(path);

        let mut eng = TextEngine::new();
        let peer1_local0 = 1u64 << 32;
        let peer2_local5 = (2u64 << 32) | 5;
        eng.index(peer1_local0, "国民は法の下に平等");
        eng.index(peer2_local5, "個人として尊重される");
        eng.save(path).unwrap();

        let eng2 = TextEngine::open(path).unwrap();
        assert_eq!(eng2.doc_count(), 2);
        assert_eq!(eng2.get_text(peer1_local0), Some("国民は法の下に平等"));
        assert_eq!(eng2.get_text(peer2_local5), Some("個人として尊重される"));

        let r = eng2.search("国民");
        assert_eq!(r, vec![peer1_local0]);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn open_nonexistent() {
        assert!(TextEngine::open("/tmp/does_not_exist.etxt").is_err());
    }

    #[test]
    fn open_mut_round_trip() {
        let path = "/tmp/enchu_text_test_open_mut.etxt";
        let _ = std::fs::remove_file(path);

        let mut eng = TextEngine::new();
        eng.index(10, "国民は法の下に");
        eng.index(20, "個人として尊重");
        eng.save(path).unwrap();

        let mut eng2 = TextEngine::open_mut(path).unwrap();
        assert_eq!(eng2.doc_count(), 2);
        assert!(eng2.search("国民").contains(&10));

        // 開いた後でも mutate できる
        eng2.index(30, "民主主義の基盤");
        eng2.compact();
        assert_eq!(eng2.doc_count(), 3);
        assert!(eng2.search("民主").contains(&30));

        let _ = std::fs::remove_file(path);
    }
}
