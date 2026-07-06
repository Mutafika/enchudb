use std::collections::HashMap;
use std::io;
#[cfg(not(target_arch = "wasm32"))]
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

/// bigram 転置インデックスの **index プリミティブ**。
///
/// 「bigram 抽出 → posting → intersect → 候補 doc id」までを提供し、検索意味論
/// (substring の `.contains()` 検証 / 単一文字フォールバック) は持たない。それらは
/// [`candidates`](NgramIndex::candidates) / [`scan`](NgramIndex::scan) を土台に
/// `enchudb-textsearch` 側のポリシーとして組む。
pub struct NgramIndex {
    backend: Backend,
}

impl NgramIndex {
    /// インメモリモード（構築用）
    pub fn new() -> Self {
        Self {
            backend: Backend::Memory {
                postings: PostingList::new(),
                originals: HashMap::new(),
            },
        }
    }

    /// mmap モードで開く（読み取り専用、即起動）。native のみ。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(path: &str) -> io::Result<Self> {
        let mapped = MappedIndex::open(Path::new(path))?;
        Ok(Self { backend: Backend::Mapped(mapped) })
    }

    /// 既存の .etxt を読み込んで in-memory mutable index に再構築する。native のみ。
    /// `open()` と違って後から `index()` / `remove()` できる。
    /// コストは「全 doc を読んで再 index」する分。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_mut(path: &str) -> io::Result<Self> {
        let mapped = MappedIndex::open(Path::new(path))?;
        require_text_for_rebuild(&mapped)?;
        let mut idx = NgramIndex::new();
        mapped.for_each_doc(|eid, text| idx.index(eid, text));
        idx.compact();
        Ok(idx)
    }

    /// バイト列から読み取り専用 index を作る。wasm で fetch したレスポンスを直接渡せる。
    pub fn from_bytes(bytes: Vec<u8>) -> io::Result<Self> {
        let mapped = MappedIndex::from_bytes(bytes)?;
        Ok(Self { backend: Backend::Mapped(mapped) })
    }

    /// バイト列から in-memory mutable index を作る（`open_mut` の wasm 版）。
    pub fn from_bytes_mut(bytes: Vec<u8>) -> io::Result<Self> {
        let mapped = MappedIndex::from_bytes(bytes)?;
        require_text_for_rebuild(&mapped)?;
        let mut idx = NgramIndex::new();
        mapped.for_each_doc(|eid, text| idx.index(eid, text));
        idx.compact();
        Ok(idx)
    }

    /// ファイルに書き出し。native のみ。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn save(&mut self, path: &str) -> io::Result<()> {
        self.compact();
        match &self.backend {
            Backend::Memory { postings, originals } => {
                crate::storage::save(Path::new(path), postings.raw(), originals)
            }
            Backend::Mapped(_) => {
                Err(io::Error::new(io::ErrorKind::Unsupported, "mapped index is read-only"))
            }
        }
    }

    /// 任意の Writer に書き出す（wasm でも動くが、wasm に index 構築のニーズは普通ない）。
    pub fn write_to<W: std::io::Write>(&mut self, w: &mut W) -> io::Result<()> {
        self.compact();
        match &self.backend {
            Backend::Memory { postings, originals } => {
                crate::storage::write_to(w, postings.raw(), originals)
            }
            Backend::Mapped(_) => {
                Err(io::Error::new(io::ErrorKind::Unsupported, "mapped index is read-only"))
            }
        }
    }

    /// 原文非保持 (postings-only) でファイルに書き出す。native のみ。
    ///
    /// Doc Index / Text Data を省くので `.etxt` が DB 本体の本文を二重化しない
    /// (#84)。 検索は [`candidates`](NgramIndex::candidates) の生候補を caller が
    /// DB 本体の原文で `.contains()` 検証する前提。 この file を `open` した index は
    /// `get_text` が常に None、 `open_mut` は不可 (rebuild は source から)。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn save_postings_only(&mut self, path: &str) -> io::Result<()> {
        self.compact();
        match &self.backend {
            Backend::Memory { postings, .. } => {
                crate::storage::save_postings_only(Path::new(path), postings.raw())
            }
            Backend::Mapped(_) => {
                Err(io::Error::new(io::ErrorKind::Unsupported, "mapped index is read-only"))
            }
        }
    }

    /// `save_postings_only` の Writer 版。
    pub fn write_to_postings_only<W: std::io::Write>(&mut self, w: &mut W) -> io::Result<()> {
        self.compact();
        match &self.backend {
            Backend::Memory { postings, .. } => {
                crate::storage::write_to_postings_only(w, postings.raw())
            }
            Backend::Mapped(_) => {
                Err(io::Error::new(io::ErrorKind::Unsupported, "mapped index is read-only"))
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

    /// クエリの **候補** doc id（bigram 抽出 → intersect）。
    ///
    /// これは index プリミティブの仕事の終点で、**substring 検証は行わない**。
    /// - 2 bigram 以上のクエリは偽陽性を含みうる（`"AB" "BC"` を両方持つが連続 `"ABBC"`
    ///   を含まない doc も候補に入る）。正確な部分一致が要るなら呼び出し側で
    ///   [`get_text`](NgramIndex::get_text) を引いて `.contains()` 検証する（= substring ポリシー）。
    /// - 1 bigram（2 文字）のクエリは bigram == query なので候補がそのまま正確一致。
    /// - 2 文字未満のクエリは bigram が作れないので空を返す（単一文字は
    ///   [`scan`](NgramIndex::scan) で扱う）。
    pub fn candidates(&self, query: &str) -> Vec<u64> {
        let bgs = bigram::extract(query);
        if bgs.is_empty() {
            return vec![];
        }
        let keys: Vec<u32> = bgs.iter().map(|bg| bigram::to_key(*bg)).collect();
        match &self.backend {
            Backend::Memory { postings, .. } => postings.intersect(&keys),
            Backend::Mapped(m) => m.intersect(&keys),
        }
    }

    /// 全 doc を述語で走査して合致する entity を返す（O(N)）。
    ///
    /// bigram で絞れないケース（単一文字クエリなど）や、`substring` 以外のポリシーを
    /// index の上に組むための汎用フック。検索意味論は呼び出し側の `pred` が決める。
    pub fn scan(&self, pred: impl Fn(&str) -> bool) -> Vec<u64> {
        match &self.backend {
            Backend::Memory { originals, .. } => originals
                .iter()
                .filter(|(_, text)| pred(text))
                .map(|(&eid, _)| eid)
                .collect(),
            Backend::Mapped(m) => m.search_all(pred),
        }
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

    /// この index が原文を保持しているか。 postings-only で開いた index は false
    /// = `get_text` が使えず、substring 検証は caller が DB 本体で行う必要がある。
    /// 構築中 (Memory) は常に true。
    pub fn has_text(&self) -> bool {
        match &self.backend {
            Backend::Memory { .. } => true,
            Backend::Mapped(m) => m.has_text(),
        }
    }

    // ── internal ──

    fn memory_mut(&mut self) -> (&mut PostingList, &mut HashMap<u64, String>) {
        match &mut self.backend {
            Backend::Memory { postings, originals } => (postings, originals),
            Backend::Mapped(_) => panic!("mapped index is read-only"),
        }
    }
}

/// postings-only index (原文非保持) を in-memory へ rebuild しようとしたら弾く。
/// 原文が無いので index からは再構築できない — source (DB 本体) から作り直すべき。
fn require_text_for_rebuild(mapped: &MappedIndex) -> io::Result<()> {
    if !mapped.has_text() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "cannot rebuild from a postings-only .etxt (no original text); rebuild from source",
        ));
    }
    Ok(())
}

impl Default for NgramIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_and_candidates() {
        let mut idx = NgramIndex::new();
        idx.index(0, "国民は法の下に平等であって");
        idx.index(1, "すべて国民は個人として尊重される");
        idx.index(2, "法の支配は民主主義の基盤である");
        idx.compact();

        // 1 bigram クエリは候補がそのまま正確一致
        let r = idx.candidates("国民");
        assert!(r.contains(&0));
        assert!(r.contains(&1));
        assert!(!r.contains(&2));

        let r = idx.candidates("法の");
        assert!(r.contains(&0));
        assert!(r.contains(&2));

        // 多 bigram は候補（substring 検証は呼び出し側）。少なくとも 0 を含む。
        let r = idx.candidates("法の下");
        assert!(r.contains(&0));
    }

    #[test]
    fn candidates_no_match() {
        let mut idx = NgramIndex::new();
        idx.index(0, "テスト文字列");
        idx.compact();
        assert_eq!(idx.candidates("存在しない"), Vec::<u64>::new());
    }

    #[test]
    fn candidates_short_query_is_empty() {
        // 2 文字未満は bigram を作れないので候補なし（単一文字は scan の領分）
        let mut idx = NgramIndex::new();
        idx.index(0, "猫は動物");
        idx.compact();
        assert_eq!(idx.candidates("猫"), Vec::<u64>::new());
        assert_eq!(idx.candidates(""), Vec::<u64>::new());
    }

    #[test]
    fn scan_single_char() {
        let mut idx = NgramIndex::new();
        idx.index(0, "猫は動物");
        idx.index(1, "犬も動物");
        idx.compact();

        let r = idx.scan(|t| t.contains("猫"));
        assert_eq!(r, vec![0u64]);
    }

    #[test]
    fn update_text() {
        let mut idx = NgramIndex::new();
        idx.index(0, "古いテキスト");
        idx.compact();
        assert!(idx.candidates("古い").contains(&0));

        idx.index(0, "新しいテキスト");
        idx.compact();
        assert!(!idx.candidates("古い").contains(&0));
        assert!(idx.candidates("新しい").contains(&0));
    }

    #[test]
    fn remove_text() {
        let mut idx = NgramIndex::new();
        idx.index(0, "削除対象テキスト");
        idx.compact();

        idx.remove(0);
        assert_eq!(idx.candidates("削除"), Vec::<u64>::new());
        assert_eq!(idx.doc_count(), 0);
    }

    #[test]
    fn stats() {
        let mut idx = NgramIndex::new();
        idx.index(0, "あいう");
        idx.index(1, "いうえ");
        idx.compact();
        assert_eq!(idx.doc_count(), 2);
        assert!(idx.bigram_count() > 0);
    }

    #[test]
    fn save_and_open() {
        let path = "/tmp/enchu_ngram_test_save.etxt";
        let _ = std::fs::remove_file(path);

        // 構築 → 保存
        let mut idx = NgramIndex::new();
        idx.index(0, "国民は法の下に平等であって");
        idx.index(1, "すべて国民は個人として尊重される");
        idx.index(2, "法の支配は民主主義の基盤である");
        idx.save(path).unwrap();

        // mmap で開く → 候補探索
        let idx2 = NgramIndex::open(path).unwrap();
        assert_eq!(idx2.doc_count(), 3);

        let r = idx2.candidates("国民");
        assert!(r.contains(&0));
        assert!(r.contains(&1));
        assert!(!r.contains(&2));

        assert_eq!(idx2.get_text(0), Some("国民は法の下に平等であって"));
        assert_eq!(idx2.get_text(99), None);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn save_and_open_wide_eid() {
        // v32 の peer_id を含む u64 eid でも round-trip する
        let path = "/tmp/enchu_ngram_test_wide_eid.etxt";
        let _ = std::fs::remove_file(path);

        let mut idx = NgramIndex::new();
        let peer1_local0 = 1u64 << 32;
        let peer2_local5 = (2u64 << 32) | 5;
        idx.index(peer1_local0, "国民は法の下に平等");
        idx.index(peer2_local5, "個人として尊重される");
        idx.save(path).unwrap();

        let idx2 = NgramIndex::open(path).unwrap();
        assert_eq!(idx2.doc_count(), 2);
        assert_eq!(idx2.get_text(peer1_local0), Some("国民は法の下に平等"));
        assert_eq!(idx2.get_text(peer2_local5), Some("個人として尊重される"));

        let r = idx2.candidates("国民");
        assert_eq!(r, vec![peer1_local0]);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn open_nonexistent() {
        assert!(NgramIndex::open("/tmp/does_not_exist.etxt").is_err());
    }

    #[test]
    fn postings_only_candidates_no_text() {
        let path = "/tmp/enchu_ngram_postings_only.etxt";
        let _ = std::fs::remove_file(path);

        let mut idx = NgramIndex::new();
        idx.index(0, "国民は法の下に平等であって");
        idx.index(1, "すべて国民は個人として尊重される");
        idx.save_postings_only(path).unwrap();

        let idx2 = NgramIndex::open(path).unwrap();
        assert!(!idx2.has_text());
        // 候補 (生 posting intersect) は引ける
        assert!(idx2.candidates("国民").contains(&0));
        assert!(idx2.candidates("国民").contains(&1));
        // 原文非保持
        assert_eq!(idx2.get_text(0), None);

        // postings-only は index からの rebuild 不可 (source から作り直す)
        let e = NgramIndex::open_mut(path).err().expect("postings-only は open_mut 不可");
        assert_eq!(e.kind(), io::ErrorKind::Unsupported);

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn postings_only_write_to_bytes_guard_mut() {
        // Writer 版 + from_bytes_mut ガード
        let mut idx = NgramIndex::new();
        idx.index(7, "個人として尊重される");
        let mut buf: Vec<u8> = Vec::new();
        idx.write_to_postings_only(&mut buf).unwrap();

        let idx2 = NgramIndex::from_bytes(buf.clone()).unwrap();
        assert!(!idx2.has_text());
        assert!(idx2.candidates("尊重").contains(&7));

        let e = NgramIndex::from_bytes_mut(buf).err().expect("postings-only は mut 化不可");
        assert_eq!(e.kind(), io::ErrorKind::Unsupported);
    }

    #[test]
    fn from_bytes_round_trip() {
        // Build → write to Vec<u8> → read back via from_bytes
        let mut idx = NgramIndex::new();
        idx.index(0, "国民は法の下に");
        idx.index((1u64 << 32) | 5, "個人として尊重");
        idx.compact();

        let mut buf: Vec<u8> = Vec::new();
        idx.write_to(&mut buf).unwrap();

        let idx2 = NgramIndex::from_bytes(buf).unwrap();
        assert_eq!(idx2.doc_count(), 2);
        assert_eq!(idx2.candidates("国民"), vec![0u64]);
        assert_eq!(idx2.candidates("個人"), vec![(1u64 << 32) | 5]);
        assert_eq!(idx2.get_text((1u64 << 32) | 5), Some("個人として尊重"));
    }

    #[test]
    fn open_mut_round_trip() {
        let path = "/tmp/enchu_ngram_test_open_mut.etxt";
        let _ = std::fs::remove_file(path);

        let mut idx = NgramIndex::new();
        idx.index(10, "国民は法の下に");
        idx.index(20, "個人として尊重");
        idx.save(path).unwrap();

        let mut idx2 = NgramIndex::open_mut(path).unwrap();
        assert_eq!(idx2.doc_count(), 2);
        assert!(idx2.candidates("国民").contains(&10));

        // 開いた後でも mutate できる
        idx2.index(30, "民主主義の基盤");
        idx2.compact();
        assert_eq!(idx2.doc_count(), 3);
        assert!(idx2.candidates("民主").contains(&30));

        let _ = std::fs::remove_file(path);
    }
}
