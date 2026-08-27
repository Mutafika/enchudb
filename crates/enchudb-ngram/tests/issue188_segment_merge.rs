//! #188 — `.etxt` の segment merge。
//!
//! 中心の主張は 1 つ: **「segment に刻んで統合したもの」と「一度に索引したもの」が
//! バイト列として同一**。これが成り立つ限り、build を segment に刻んでもう索引の
//! 作り直しにコーパス全量のメモリを要求しない、と言い切れる。

use enchudb_ngram::NgramIndex;

/// テスト用の一時パス（衝突しないように名前で分ける）。
fn tmp(name: &str) -> String {
    let dir = std::env::temp_dir().join("enchudb_issue188");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(format!("{name}.etxt")).to_string_lossy().into_owned()
}

/// 条文っぽい日本語（bigram が十分散る長さ）。
const DOCS: &[(u64, &str)] = &[
    (10, "国民は、すべての基本的人権の享有を妨げられない。"),
    (20, "この憲法が国民に保障する自由及び権利は、国民の不断の努力によつて保持しなければならない。"),
    (30, "すべて国民は、個人として尊重される。"),
    (40, "生命、自由及び幸福追求に対する国民の権利については、公共の福祉に反しない限り尊重を必要とする。"),
    (50, "法律上の争訟を裁判し、その他法律において特に定める権限を有する。"),
    (60, "何人も、法律の定める手続によらなければ、その生命若しくは自由を奪はれない。"),
];

fn build_one_shot(docs: &[(u64, &str)], path: &str) {
    let mut idx = NgramIndex::new();
    for (eid, text) in docs {
        idx.index(*eid, text);
    }
    idx.save(path).unwrap();
}

fn build_segment(docs: &[(u64, &str)], path: &str) {
    build_one_shot(docs, path);
}

#[test]
fn merge_of_two_segments_is_byte_identical_to_one_shot() {
    let (a, b, m, one) = (tmp("2seg_a"), tmp("2seg_b"), tmp("2seg_merged"), tmp("2seg_oneshot"));
    build_segment(&DOCS[..3], &a);
    build_segment(&DOCS[3..], &b);
    let stats = NgramIndex::merge_files(&[&a, &b], &m).unwrap();
    build_one_shot(DOCS, &one);

    assert_eq!(stats.docs, DOCS.len() as u32);
    assert_eq!(stats.superseded_docs, 0, "doc が分割されていれば上書きは起きない");
    assert_eq!(
        std::fs::read(&m).unwrap(),
        std::fs::read(&one).unwrap(),
        "segment 統合の結果が一度に索引したものとバイト一致しない"
    );
}

#[test]
fn merge_of_many_segments_is_byte_identical_to_one_shot() {
    // 1 doc = 1 segment という極端な刻み方でも同じものになる。
    let mut segs = Vec::new();
    for (i, d) in DOCS.iter().enumerate() {
        let p = tmp(&format!("many_{i}"));
        build_segment(std::slice::from_ref(d), &p);
        segs.push(p);
    }
    let refs: Vec<&str> = segs.iter().map(|s| s.as_str()).collect();
    let (m, one) = (tmp("many_merged"), tmp("many_oneshot"));
    NgramIndex::merge_files(&refs, &m).unwrap();
    build_one_shot(DOCS, &one);
    assert_eq!(std::fs::read(&m).unwrap(), std::fs::read(&one).unwrap());
}

#[test]
fn later_segment_supersedes_earlier_doc() {
    // 同じ eid が 2 つの segment に居る = 日次 delta の形。後勝ちで、
    // **旧本文由来の posting も残らない**（残ると古い語で当たってしまう）。
    let (a, b, m, one) = (tmp("sup_a"), tmp("sup_b"), tmp("sup_merged"), tmp("sup_oneshot"));
    build_segment(&[(10, "旧本文は財産権について定める。"), (20, DOCS[1].1)], &a);
    build_segment(&[(10, "新本文は納税の義務について定める。")], &b);

    let stats = NgramIndex::merge_files(&[&a, &b], &m).unwrap();
    assert_eq!(stats.superseded_docs, 1);
    assert_eq!(stats.docs, 2);

    // 期待値 = 「新本文で最初から索引した場合」
    build_one_shot(&[(10, "新本文は納税の義務について定める。"), (20, DOCS[1].1)], &one);
    assert_eq!(
        std::fs::read(&m).unwrap(),
        std::fs::read(&one).unwrap(),
        "上書き後の index が、新本文で組み直したものと一致しない"
    );

    let merged = NgramIndex::open(&m).unwrap();
    assert_eq!(merged.get_text(10), Some("新本文は納税の義務について定める。"));
    assert!(merged.candidates("財産権").is_empty(), "旧本文由来の候補が残っている");
    assert_eq!(merged.candidates("納税"), vec![10]);
}

#[test]
fn merge_postings_only_keeps_the_flag() {
    let (a, b, m) = (tmp("po_a"), tmp("po_b"), tmp("po_merged"));
    let mut ia = NgramIndex::new();
    for (eid, text) in &DOCS[..3] { ia.index(*eid, text); }
    ia.save_postings_only(&a).unwrap();
    let mut ib = NgramIndex::new();
    for (eid, text) in &DOCS[3..] { ib.index(*eid, text); }
    ib.save_postings_only(&b).unwrap();

    let stats = NgramIndex::merge_files(&[&a, &b], &m).unwrap();
    assert_eq!(stats.docs, 0, "postings-only は doc index を持たない");

    let merged = NgramIndex::open(&m).unwrap();
    assert_eq!(merged.get_text(10), None, "postings-only のまま統合されること");
    // 候補は両 segment ぶんが引ける（検証は caller 側 = #84 の前提）。
    assert!(merged.candidates("国民").contains(&10));
    assert!(merged.candidates("裁判").contains(&50));
}

#[test]
fn merge_rejects_mixed_text_flags() {
    let (a, b, m) = (tmp("mix_a"), tmp("mix_b"), tmp("mix_merged"));
    build_segment(&DOCS[..3], &a);
    let mut ib = NgramIndex::new();
    for (eid, text) in &DOCS[3..] { ib.index(*eid, text); }
    ib.save_postings_only(&b).unwrap();

    let err = NgramIndex::merge_files(&[&a, &b], &m).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    assert!(err.to_string().contains("postings-only"), "何が食い違ったか言うこと: {err}");
}

#[test]
fn merge_rejects_mismatched_n() {
    let (a, b, m) = (tmp("n_a"), tmp("n_b"), tmp("n_merged"));
    let mut ia = NgramIndex::new(); // n = 2
    for (eid, text) in &DOCS[..3] { ia.index(*eid, text); }
    ia.save(&a).unwrap();
    let mut ib = NgramIndex::with_n(3).unwrap();
    for (eid, text) in &DOCS[3..] { ib.index(*eid, text); }
    ib.save(&b).unwrap();

    let err = NgramIndex::merge_files(&[&a, &b], &m).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    assert!(err.to_string().contains("n が食い違う"), "{err}");
}

#[test]
fn merge_v3_is_byte_identical_to_one_shot() {
    // n ≥ 3 は key u64 の v3 format。同じ主張が成り立つこと。
    let (a, b, m, one) = (tmp("v3_a"), tmp("v3_b"), tmp("v3_merged"), tmp("v3_oneshot"));
    let build = |docs: &[(u64, &str)], path: &str| {
        let mut i = NgramIndex::with_n(3).unwrap();
        for (eid, text) in docs { i.index(*eid, text); }
        i.save(path).unwrap();
    };
    build(&DOCS[..3], &a);
    build(&DOCS[3..], &b);
    NgramIndex::merge_files(&[&a, &b], &m).unwrap();
    build(DOCS, &one);
    assert_eq!(std::fs::read(&m).unwrap(), std::fs::read(&one).unwrap());
    assert_eq!(NgramIndex::open(&m).unwrap().n(), 3);
}

#[test]
fn merge_single_input_round_trips() {
    let (a, m) = (tmp("one_a"), tmp("one_merged"));
    build_segment(DOCS, &a);
    NgramIndex::merge_files(&[&a], &m).unwrap();
    assert_eq!(std::fs::read(&m).unwrap(), std::fs::read(&a).unwrap());
}

#[test]
fn merge_rejects_empty_input_list() {
    let err = NgramIndex::merge_files(&[], &tmp("empty_out")).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
}
