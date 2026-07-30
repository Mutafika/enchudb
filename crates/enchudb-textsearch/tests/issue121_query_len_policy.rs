//! #121 (c): クエリ長 と n の関係で検索の挙動が変わる境界を固定する。
//!
//! | クエリ長 | 原文あり | postings-only (原文なし) |
//! |---|---|---|
//! | 0 | `[]` | `[]` |
//! | < n | O(N) 全走査 (`scan`) | **`try_search` が Unsupported** |
//! | == n | 候補 = 正確一致 (検証スキップ) | 同じ (検証不要なので OK) |
//! | > n | 候補 → `.contains()` 検証 | **`try_search` が Unsupported** |
//!
//! `search()` は互換のため `Vec` を返し続けるので、答えを出せない組合せでは空になる。
//! 「空 = 該当なし」と「空 = 答えられなかった」を区別したい呼び出し元が
//! `try_search()` を使う。

use enchudb_textsearch::TextSearch;

const DOCS: [(u64, &str); 3] = [
    (10, "国民は法の下に平等であって"),
    (20, "すべて国民は個人として尊重される"),
    (30, "法の支配は民主主義の基盤である"),
];

fn build(n: usize, postings_only: bool) -> TextSearch {
    let mut eng = TextSearch::with_n(n).unwrap();
    for (eid, text) in DOCS {
        eng.index(eid, text);
    }
    let mut buf = Vec::new();
    if postings_only {
        eng.write_to_postings_only(&mut buf).unwrap();
    } else {
        eng.write_to(&mut buf).unwrap();
    }
    TextSearch::from_bytes(buf).unwrap()
}

/// n が file から戻り、`min_query_len` として公開されること。
#[test]
fn min_query_len_follows_the_index() {
    for n in 2..=4 {
        let eng = build(n, false);
        assert_eq!(eng.n(), n);
        assert_eq!(eng.min_query_len(), n, "caller が n を覚えずに判定できる");
    }
}

/// 原文を持つ index は、n 未満のクエリを O(N) 全走査で救う。
#[test]
fn short_query_falls_back_to_scan_when_text_is_present() {
    for n in 2..=4 {
        let eng = build(n, false);
        // 1 文字は必ず n 未満
        let r = eng.try_search("猫").expect("原文があるので走査できる");
        assert_eq!(r, Vec::<u64>::new(), "n={n}: 該当なしは空");

        let mut r = eng.try_search("法").expect("原文があるので走査できる");
        r.sort_unstable();
        assert_eq!(r, vec![10, 30], "n={n}: 1 文字でも全走査で正しく出る");

        if n > 2 {
            // 2 文字も n 未満 → 走査経路
            let mut r = eng.try_search("国民").unwrap();
            r.sort_unstable();
            assert_eq!(r, vec![10, 20], "n={n}: n 未満のクエリも走査で救われる");
        }
    }
}

/// postings-only は原文が無いので、n 未満のクエリに **明示エラー**を返す。
/// 旧挙動（silent な空 Vec）だと「該当なし」と区別できない。
#[test]
fn short_query_on_postings_only_is_an_explicit_error() {
    for n in 2..=4 {
        let eng = build(n, true);
        assert!(!eng.has_text());

        let err = eng
            .try_search("法")
            .err()
            .unwrap_or_else(|| panic!("n={n}: 1 文字クエリが通ってしまった"));
        assert_eq!(err.kind(), std::io::ErrorKind::Unsupported, "n={n}");
        assert!(
            err.to_string().contains("postings-only"),
            "n={n}: 理由を示すこと: {err}"
        );
        // 互換 API は空を返す（= 区別できない、が壊れない）
        assert_eq!(eng.search("法"), Vec::<u64>::new());
    }
}

/// ちょうど n 文字は検証不要なので postings-only でも答えが出る。
/// key を hash に潰していない（exact な pack）ことが根拠 (#121 (b))。
#[test]
fn exactly_n_chars_needs_no_verification() {
    for n in 2..=4 {
        let query: String = "国民は法".chars().take(n).collect();
        let with_text = build(n, false);
        let without = build(n, true);

        let mut a = with_text.try_search(&query).unwrap();
        let mut b = without
            .try_search(&query)
            .unwrap_or_else(|e| panic!("n={n}: {n} 文字ちょうどは検証不要なのに Err: {e}"));
        a.sort_unstable();
        b.sort_unstable();
        assert_eq!(a, b, "n={n}: 原文の有無で結果が変わってはいけない");

        // 正解は総当たりの substring 判定 (index を介さない独立した基準)
        let mut truth: Vec<u64> = DOCS
            .iter()
            .filter(|(_, t)| t.contains(&query))
            .map(|(eid, _)| *eid)
            .collect();
        truth.sort_unstable();
        assert!(!truth.is_empty(), "n={n}: {query} が誰にも当たらない test data");
        assert_eq!(a, truth, "n={n}: {query} の候補が正確一致になっていない");
    }
}

/// n 超のクエリは偽陽性を落とす照合が要るので、postings-only では明示エラー。
#[test]
fn long_query_on_postings_only_is_an_explicit_error() {
    // eid 0 は "法の" と "の下" を両方持つが、連続した "法の下" は含まない = 偽陽性。
    let mut eng = TextSearch::new();
    eng.index(0, "机の下に法の本");
    eng.index(1, "法の下の平等");
    let mut buf = Vec::new();
    eng.write_to_postings_only(&mut buf).unwrap();
    let eng = TextSearch::from_bytes(buf).unwrap();

    let err = eng.try_search("法の下").expect_err("照合できないので Err");
    assert_eq!(err.kind(), std::io::ErrorKind::Unsupported);
    assert!(err.to_string().contains("candidates()"), "代替手段を示すこと: {err}");
    assert_eq!(eng.search("法の下"), Vec::<u64>::new(), "互換 API は空を返す");

    // 生候補は引ける（偽陽性込み。caller が source で検証する #84 の経路）
    let mut cand = eng.candidates("法の下");
    cand.sort_unstable();
    assert_eq!(cand, vec![0, 1], "eid 0 は '法の' + 'の下' の偽陽性");
}

/// n 超のクエリは、原文があれば偽陽性が落ちる（n を変えても成り立つ）。
#[test]
fn false_positives_are_filtered_at_every_n() {
    // "法の解釈と下記" は "法の" "の解" ... を持つが "法の下" は含まない
    for n in 2..=4 {
        let mut eng = TextSearch::with_n(n).unwrap();
        eng.index(0, "法の解釈と下記のとおり");
        eng.index(1, "法の下に平等である");
        eng.compact();
        assert_eq!(eng.try_search("法の下に").unwrap(), vec![1u64], "n={n}");
    }
}

/// 空クエリはどの構成でも空（エラーにしない）。
#[test]
fn empty_query_is_always_empty() {
    for n in 2..=4 {
        for postings_only in [false, true] {
            let eng = build(n, postings_only);
            assert_eq!(eng.try_search("").unwrap(), Vec::<u64>::new());
            assert_eq!(eng.search(""), Vec::<u64>::new());
        }
    }
}

/// `search` と `try_search` は「答えられる」場合には必ず一致する。
#[test]
fn search_agrees_with_try_search_when_answerable() {
    for n in 2..=4 {
        let eng = build(n, false);
        for q in ["法", "国民", "国民は", "国民は法", "民主主義の基盤", "存在しない"] {
            let mut a = eng.search(q);
            let mut b = eng.try_search(q).unwrap();
            a.sort_unstable();
            b.sort_unstable();
            assert_eq!(a, b, "n={n} q={q}");
        }
    }
}

/// n が range 外なら engine を作る前に落ちる。
#[test]
fn out_of_range_n_rejected() {
    for n in [0usize, 1, 5] {
        assert_eq!(
            TextSearch::with_n(n).err().map(|e| e.kind()),
            Some(std::io::ErrorKind::InvalidInput),
            "n={n}"
        );
    }
}
