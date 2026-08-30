//! enchudb-textsearch — `enchudb-ngram` の上に乗る **テキスト検索ポリシー**。
//!
//! [`NgramIndex`](enchudb_ngram::NgramIndex) が返す n-gram 候補を `.contains()` で検証して
//! **正確な部分一致 (substring)** にする。これは「人間の対話検索」に正しい挙動（`接地` で
//! `接地極` / `接地工事` が出るのは望ましい UX）。
//!
//! **機械向けフレーズ完全一致**（LLM が条文を grounding に引くケース等）も、入力フレーズを
//! **1 単位**で [`search`](TextSearch::search) に渡せば同じ path で扱える
//! ([issue #69](https://github.com/Mutafika/enchudb/issues/69) の option (a))。要は
//! 断片に分割して別々に投げない、という呼び出し規律の問題。これを API で構造的に縛りたく
//! なったら専用の `enchudb-phrase` を被せる（現状は不要、option (b)）。
//!
//! クレート名は「`text` が検索という正体を隠していた」という #69 の元の不満を直す意図で
//! `textsearch`（= search over text）。両モード（人間の部分一致／機械のフレーズ）の傘。
//!
//! ## なぜ別クレートか
//!
//! 人間 = 部分一致と 機械 = フレーズ完全一致は「正しい」が用途が逆。同一エンジンに両立
//! させるとどちらかが歪むので関心を分離する。index プリミティブ（n-gram / posting /
//! intersect）は `enchudb-ngram` が持ち、その上に検索ポリシーが乗る。
//!
//! n は index が覚えているので (#121)、このレイヤの挙動 —— どの長さのクエリを index で
//! 絞れるか、どこから `.contains()` 検証が要るか —— は n に自動追従する。

use std::io;

use enchudb_ngram::NgramIndex;

/// テキスト検索エンジン。`NgramIndex` を内包し、候補に `.contains()` 検証を足して
/// 正確な部分一致 (substring) を返す。build / 永続化 API は `NgramIndex` に委譲する。
pub struct TextSearch {
    idx: NgramIndex,
}

impl TextSearch {
    /// インメモリモード（構築用）。n は既定の 2。
    pub fn new() -> Self {
        Self { idx: NgramIndex::new() }
    }

    /// n を指定してインメモリ構築する (#121)。対応範囲は 2..=4。
    ///
    /// n は `.etxt` に焼かれるので、[`open`](Self::open) した側は n を知らなくてよい。
    /// 検索の挙動は n に自動追従する（[`min_query_len`](Self::min_query_len) 参照）。
    pub fn with_n(n: usize) -> io::Result<Self> {
        Ok(Self { idx: NgramIndex::with_n(n)? })
    }

    /// mmap モードで開く（読み取り専用、即起動）。native のみ。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open(path: &str) -> io::Result<Self> {
        Ok(Self { idx: NgramIndex::open(path)? })
    }

    /// 既存の .etxt を読み込んで in-memory mutable engine に再構築する。native のみ。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_mut(path: &str) -> io::Result<Self> {
        Ok(Self { idx: NgramIndex::open_mut(path)? })
    }

    /// バイト列から読み取り専用 engine を作る。wasm で fetch したレスポンスを直接渡せる。
    pub fn from_bytes(bytes: Vec<u8>) -> io::Result<Self> {
        Ok(Self { idx: NgramIndex::from_bytes(bytes)? })
    }

    /// バイト列から in-memory mutable engine を作る（`open_mut` の wasm 版）。
    pub fn from_bytes_mut(bytes: Vec<u8>) -> io::Result<Self> {
        Ok(Self { idx: NgramIndex::from_bytes_mut(bytes)? })
    }

    /// ファイルに書き出し。native のみ。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn save(&mut self, path: &str) -> io::Result<()> {
        self.idx.save(path)
    }

    /// 任意の Writer に書き出す。
    pub fn write_to<W: std::io::Write>(&mut self, w: &mut W) -> io::Result<()> {
        self.idx.write_to(w)
    }

    /// 原文非保持 (postings-only) で書き出す。native のみ。
    ///
    /// `.etxt` が DB 本体の本文を二重化しなくなる (#84)。 この file を開いた engine は
    /// 原文を持たないので [`search`](TextSearch::search) の substring 検証ができない。
    /// caller は [`candidates`](TextSearch::candidates) の生候補を DB 本体の原文で
    /// 検証する (naruhodo hanrei の body lookup がその経路)。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn save_postings_only(&mut self, path: &str) -> io::Result<()> {
        self.idx.save_postings_only(path)
    }

    /// 複数の `.etxt` を 1 本に統合する (#188)。
    ///
    /// 索引の作り直しを「小さい segment を並べて後から統合する」形に置き換えるための口。
    /// segment は `index()` → `save()` で作った普通の `.etxt` で、専用形式は無い。
    /// **後の input が勝つ** (同じ eid が複数 segment に居たら最後のものが採用され、
    /// それ以前の posting は落ちる)。
    ///
    /// これを使うと build のピークがコーパス量から独立する — 統合中にメモリへ載るのは
    /// 1 gram ぶんの posting run と Gram/Doc Index 相当だけ。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn merge_files(inputs: &[&str], out: &str) -> io::Result<enchudb_ngram::MergeStats> {
        NgramIndex::merge_files(inputs, out)
    }

    /// `save_postings_only` の Writer 版。
    pub fn write_to_postings_only<W: std::io::Write>(&mut self, w: &mut W) -> io::Result<()> {
        self.idx.write_to_postings_only(w)
    }

    /// entity にテキストを登録
    pub fn index(&mut self, eid: u64, text: &str) {
        self.idx.index(eid, text);
    }

    /// entity のテキストを削除
    pub fn remove(&mut self, eid: u64) {
        self.idx.remove(eid);
    }

    /// posting list を最適化
    pub fn compact(&mut self) {
        self.idx.compact();
    }

    /// 部分一致検索 (substring)。
    ///
    /// `ngram` の候補（n-gram intersect）を `.contains()` で検証して偽陽性を除外する。
    /// n は index が覚えているので呼び出し側は意識しなくてよい (#121)。
    /// - n 文字未満のクエリは index で絞れないので全 doc 走査（`scan`）にフォールバック。
    /// - ちょうど n 文字のクエリは gram == query（key は文字を詰めた exact 値で衝突が
    ///   無い）なので、候補がそのまま正確一致。検証を省く。
    /// - 機械向けフレーズ完全一致は、フレーズ全体を 1 単位でここに渡す（断片に割らない）。
    ///
    /// **原文を持たない index（postings-only、#84）では答えを出せない組合せがある**
    /// — その場合ここは黙って空を返す。区別したいなら
    /// [`try_search`](Self::try_search) を使う。
    pub fn search(&self, query: &str) -> Vec<u64> {
        self.try_search(query).unwrap_or_default()
    }

    /// [`search`](Self::search) の「答えられないなら Err」版 (#121)。
    ///
    /// postings-only index (`has_text() == false`) は原文を持たないので、
    /// **substring 検証も全走査 fallback もできない**。該当するのは:
    /// - クエリ長 < n — 絞り込む gram が作れず `scan` に落ちるが、走査する原文が無い
    /// - クエリ長 > n — 候補は出るが偽陽性を落とす照合ができない
    ///
    /// どちらも `ErrorKind::Unsupported`。呼び出し側は
    /// [`candidates`](Self::candidates) を取って自分の source（DB 本体）で検証する。
    /// クエリ長 == n だけは検証不要なので postings-only でも `Ok`。
    pub fn try_search(&self, query: &str) -> io::Result<Vec<u64>> {
        let n = self.idx.n();
        let qlen = query.chars().count();

        if qlen == 0 {
            return Ok(vec![]);
        }

        if qlen < n {
            // n 文字未満は gram を作れないので全 doc を走査するしかない。
            if !self.idx.has_text() {
                return Err(no_text(format!(
                    "query {qlen} chars < n = {n}: index で絞れず全走査が必要だが、\
                     この index は原文を持たない (postings-only)。source 側で走査すること"
                )));
            }
            return Ok(self.idx.scan(|text| text.contains(query)));
        }

        let candidates = self.idx.candidates(query);

        // ちょうど n 文字のクエリは gram == query なので、候補に居る ⟺ query が
        // substring として存在する。検証は無駄なのでスキップ。key は hash ではなく
        // 文字を詰めた exact 値なので、この同値は n がいくつでも成り立つ。
        if qlen == n {
            return Ok(candidates);
        }

        // n 文字超のクエリは原文照合で偽陽性を除外（"ABXYBC" は "AB" "BC" の bigram を
        // 両方持つが、連続した "ABBC" は含まない）。
        if !self.idx.has_text() {
            return Err(no_text(format!(
                "query {qlen} chars > n = {n}: 候補の偽陽性を落とすのに原文照合が要るが、\
                 この index は原文を持たない (postings-only)。candidates() を source で検証すること"
            )));
        }
        Ok(candidates
            .into_iter()
            .filter(|&eid| self.idx.get_text(eid).is_some_and(|text| text.contains(query)))
            .collect())
    }

    /// クエリの **生候補** doc id（bigram intersect、`.contains()` 検証なし）。
    ///
    /// [`search`](TextSearch::search) が内部で行う原文照合を **しない**。原文を
    /// index が持たない postings-only モード (#84) で、substring 検証を caller 側
    /// (DB 本体の原文) に委ねるための入口:
    /// - 多 bigram クエリは偽陽性を含みうる（`"AB" "BC"` を持つが連続 `"ABBC"` 無しの
    ///   doc も入る）。caller が原文を引いて `.contains()` で落とす。
    /// - 1 bigram（2 文字）は候補がそのまま正確一致。
    /// - 2 文字未満は bigram を作れず空（単一文字は caller が source を全走査する）。
    pub fn candidates(&self, query: &str) -> Vec<u64> {
        self.idx.candidates(query)
    }

    /// この engine が原文を保持しているか。false = postings-only で開いた
    /// (= `search` の検証不可、`candidates` + caller 検証を使う)。
    pub fn has_text(&self) -> bool {
        self.idx.has_text()
    }

    /// この engine の n (#121)。`open` した場合は `.etxt` header 由来。
    pub fn n(&self) -> usize {
        self.idx.n()
    }

    /// index で絞り込める最短のクエリ長（文字数）= n。
    /// これ未満のクエリは O(N) 全走査に落ちる（postings-only では
    /// [`try_search`](Self::try_search) が Err）。
    pub fn min_query_len(&self) -> usize {
        self.idx.min_query_len()
    }

    /// 原文を取得
    pub fn get_text(&self, eid: u64) -> Option<&str> {
        self.idx.get_text(eid)
    }

    /// 統計 — 相異なる gram key の数。
    pub fn gram_count(&self) -> usize {
        self.idx.gram_count()
    }

    /// [`gram_count`](Self::gram_count) の旧名 (n = 2 前提の名前)。
    pub fn bigram_count(&self) -> usize {
        self.gram_count()
    }

    pub fn doc_count(&self) -> usize {
        self.idx.doc_count()
    }

    /// 内包する index プリミティブへの参照（候補探索や走査を直接叩きたい場合）
    pub fn ngram(&self) -> &NgramIndex {
        &self.idx
    }
}

/// postings-only index に原文照合を求めたときのエラー。
fn no_text(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, msg)
}

impl Default for TextSearch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_and_search() {
        let mut eng = TextSearch::new();
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
        let mut eng = TextSearch::new();
        eng.index(0, "テスト文字列");
        eng.compact();
        assert_eq!(eng.search("存在しない"), Vec::<u64>::new());
    }

    #[test]
    fn search_single_char() {
        let mut eng = TextSearch::new();
        eng.index(0, "猫は動物");
        eng.index(1, "犬も動物");
        eng.compact();

        let r = eng.search("猫");
        assert_eq!(r, vec![0u64]);
    }

    #[test]
    fn false_positive_filtered() {
        let mut eng = TextSearch::new();
        eng.index(0, "法の解釈と下書き");
        eng.index(1, "法の下に平等");
        eng.compact();

        // "法の下" は "法の" "の下" の bigram を両方持つ doc 0 を候補に入れるが、
        // 連続した "法の下" は含まないので contains 検証で落ちる。
        let r = eng.search("法の下");
        assert_eq!(r, vec![1u64]);
    }

    #[test]
    fn update_text() {
        let mut eng = TextSearch::new();
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
        let mut eng = TextSearch::new();
        eng.index(0, "削除対象テキスト");
        eng.compact();

        eng.remove(0);
        assert_eq!(eng.search("削除").len(), 0);
        assert_eq!(eng.doc_count(), 0);
    }

    #[test]
    fn ascii_search() {
        let mut eng = TextSearch::new();
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
        let mut eng = TextSearch::new();
        eng.index(0, "Rust言語でDB構築");
        eng.compact();
        assert_eq!(eng.search("Rust言語").len(), 1);
        assert_eq!(eng.search("Python").len(), 0);
    }

    #[test]
    fn two_char_query_skips_filter() {
        // 2 文字クエリは bigram == query なので filter は不要。出力の正しさを保証する。
        let mut eng = TextSearch::new();
        eng.index(0, "国民は法の下");
        eng.index(1, "個人として尊重");
        eng.index(2, "民主主義の基盤");
        eng.compact();

        assert_eq!(eng.search("国民"), vec![0u64]);
        assert_eq!(eng.search("民主"), vec![2u64]);
        assert_eq!(eng.search("青空"), Vec::<u64>::new());
    }

    #[test]
    fn substring_match_is_correct_for_humans() {
        // #69 の例: 部分一致が「正しい」人間向け挙動。`接地` が `接地極` / `接地工事` を出す。
        let mut eng = TextSearch::new();
        eng.index(0, "接地極の施工");
        eng.index(1, "接地工事の方法");
        eng.index(2, "配線の絶縁");
        eng.compact();

        let r = eng.search("接地");
        assert!(r.contains(&0));
        assert!(r.contains(&1));
        assert!(!r.contains(&2));
    }

    #[test]
    fn save_and_open() {
        let path = "/tmp/enchu_textsearch_test_save.etxt";
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(path);

        let mut eng = TextSearch::new();
        eng.index(0, "国民は法の下に平等であって");
        eng.index(1, "すべて国民は個人として尊重される");
        eng.index(2, "法の支配は民主主義の基盤である");
        eng.save(path).unwrap();

        let eng2 = TextSearch::open(path).unwrap();
        assert_eq!(eng2.doc_count(), 3);

        let r = eng2.search("国民");
        assert!(r.contains(&0));
        assert!(r.contains(&1));
        assert!(!r.contains(&2));

        let r = eng2.search("法の下");
        assert_eq!(r, vec![0u64]);

        assert_eq!(eng2.get_text(0), Some("国民は法の下に平等であって"));
        assert_eq!(eng2.get_text(99), None);

        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn postings_only_caller_side_verification() {
        // 原文を DB (ここでは source map で代用) が持ち、index は postings-only。
        // 多 bigram の偽陽性は index では落ちない → caller が source の原文で検証して落とす。
        // eid 0 は "法の" "の下" の bigram を両方持つが substring "法の下" を含まない = 偽陽性。
        let source: std::collections::HashMap<u64, &str> =
            [(0u64, "机の下に法の本"), (1u64, "法の下の平等")]
                .into_iter()
                .collect();

        let mut eng = TextSearch::new();
        for (&eid, &text) in &source {
            eng.index(eid, text);
        }
        let path = "/tmp/enchu_textsearch_postings_only.etxt";
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(path);
        eng.save_postings_only(path).unwrap();

        let eng2 = TextSearch::open(path).unwrap();
        assert!(!eng2.has_text());
        assert_eq!(eng2.get_text(0), None, "postings-only は原文非保持");

        // 生候補は両方 (偽陽性 eid 0 込み)
        let mut cand = eng2.candidates("法の下");
        cand.sort_unstable();
        assert_eq!(cand, vec![0, 1]);

        // caller 側で source の原文照合すると偽陽性が落ちる (= 現 search() の検証を外出し)
        let verified: Vec<u64> = cand
            .into_iter()
            .filter(|eid| source.get(eid).is_some_and(|t| t.contains("法の下")))
            .collect();
        assert_eq!(verified, vec![1]);

        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn from_bytes_round_trip() {
        let mut eng = TextSearch::new();
        eng.index(0, "国民は法の下に");
        eng.index((1u64 << 32) | 5, "個人として尊重");
        eng.compact();

        let mut buf: Vec<u8> = Vec::new();
        eng.write_to(&mut buf).unwrap();

        let eng2 = TextSearch::from_bytes(buf).unwrap();
        assert_eq!(eng2.doc_count(), 2);
        assert_eq!(eng2.search("国民"), vec![0u64]);
        assert_eq!(eng2.search("個人"), vec![(1u64 << 32) | 5]);
        assert_eq!(eng2.get_text((1u64 << 32) | 5), Some("個人として尊重"));
    }

    #[test]
    fn open_mut_round_trip() {
        let path = "/tmp/enchu_textsearch_test_open_mut.etxt";
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(path);

        let mut eng = TextSearch::new();
        eng.index(10, "国民は法の下に");
        eng.index(20, "個人として尊重");
        eng.save(path).unwrap();

        let mut eng2 = TextSearch::open_mut(path).unwrap();
        assert_eq!(eng2.doc_count(), 2);
        assert!(eng2.search("国民").contains(&10));

        eng2.index(30, "民主主義の基盤");
        eng2.compact();
        assert_eq!(eng2.doc_count(), 3);
        assert!(eng2.search("民主").contains(&30));

        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(path);
    }
}
