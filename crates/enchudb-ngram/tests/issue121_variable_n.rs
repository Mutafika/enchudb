//! #121: n = 2 固定の解消。
//!
//! - `with_n(3)` で build → save → open → candidates が n=3 で一致する round-trip
//! - **旧 v2 `.etxt`（12B entry / key u32）が n=2 として読める**後方互換。
//!   fixture は crate の writer を使わず **手組みのバイト列**で作る（writer と reader が
//!   同時に壊れたら気づけないので、reader を独立した基準に対して固定する）
//! - 既定 (n=2) の出力が #121 以前と **バイト等価**であること
//! - クエリ長 < n の挙動

use enchudb_ngram::{gram, NgramIndex};

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/tmp/issue121_{}_{}_{}.etxt", tag, std::process::id(), nanos)
}

const DOCS: [(u64, &str); 3] = [
    (10, "国民は法の下に平等であって"),
    (20, "すべて国民は個人として尊重される"),
    (30, "法の支配は民主主義の基盤である"),
];

// ── 手組み v2 fixture ────────────────────────────────────────────────
// crate の writer を一切使わず、#121 以前の format 仕様だけを見て組み立てる。
// key の計算も `(c1 << 16) | c2` を直に書き、crate の `to_key` に依存しない。

/// #121 以前の bigram key（2 文字を 16bit ずつ pack）。
fn legacy_key(a: char, b: char) -> u32 {
    ((a as u32 & 0xFFFF) << 16) | (b as u32 & 0xFFFF)
}

/// version 2 の `.etxt` をバイト列で組み立てる（原文保持）。
fn build_legacy_v2_bytes(docs: &[(u64, &str)]) -> Vec<u8> {
    use std::collections::BTreeMap;

    // bigram → eids
    let mut postings: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
    for (eid, text) in docs {
        let chars: Vec<char> = text.chars().collect();
        for w in chars.windows(2) {
            let list = postings.entry(legacy_key(w[0], w[1])).or_default();
            if !list.contains(eid) {
                list.push(*eid);
            }
        }
    }
    for list in postings.values_mut() {
        list.sort_unstable();
    }
    let mut docs_sorted: Vec<(u64, &str)> = docs.to_vec();
    docs_sorted.sort_by_key(|(eid, _)| *eid);

    let gram_count = postings.len() as u32;
    let posting_total: u32 = postings.values().map(|v| v.len() as u32).sum();
    let doc_count = docs_sorted.len() as u32;
    let text_total: u32 = docs_sorted.iter().map(|(_, t)| t.len() as u32).sum();

    let mut buf = Vec::new();
    buf.extend_from_slice(b"ETXT");
    buf.extend_from_slice(&2u32.to_le_bytes());
    buf.extend_from_slice(&gram_count.to_le_bytes());
    buf.extend_from_slice(&posting_total.to_le_bytes());
    buf.extend_from_slice(&doc_count.to_le_bytes());
    buf.extend_from_slice(&text_total.to_le_bytes());
    buf.extend_from_slice(&[0u8; 8]); // flags + n + reserved: 旧 file は全 0

    // Bigram Index: key u32 + offset u32 + len u32 (key 昇順 = BTreeMap の順)
    let mut offset: u32 = 0;
    for (key, eids) in &postings {
        buf.extend_from_slice(&key.to_le_bytes());
        buf.extend_from_slice(&offset.to_le_bytes());
        buf.extend_from_slice(&(eids.len() as u32).to_le_bytes());
        offset += eids.len() as u32;
    }

    // Padding: Posting Data の先頭を 8-byte 境界へ
    while buf.len() % 8 != 0 {
        buf.push(0);
    }

    for eids in postings.values() {
        for eid in eids {
            buf.extend_from_slice(&eid.to_le_bytes());
        }
    }

    let mut text_offset: u32 = 0;
    for (eid, text) in &docs_sorted {
        buf.extend_from_slice(&eid.to_le_bytes());
        buf.extend_from_slice(&text_offset.to_le_bytes());
        buf.extend_from_slice(&(text.len() as u32).to_le_bytes());
        text_offset += text.len() as u32;
    }
    for (_, text) in &docs_sorted {
        buf.extend_from_slice(text.as_bytes());
    }
    buf
}

// ── テスト ──────────────────────────────────────────────────────────

/// 手組みの旧 v2 file が n=2 の index として読め、検索できること。
#[test]
fn legacy_v2_file_reads_as_n2() {
    let bytes = build_legacy_v2_bytes(&DOCS);
    let idx = NgramIndex::from_bytes(bytes).expect("旧 v2 file が読めない");

    assert_eq!(idx.n(), 2, "v2 は n=2 と解釈される");
    assert_eq!(idx.min_query_len(), 2);
    assert_eq!(idx.doc_count(), 3);

    let mut r = idx.candidates("国民");
    r.sort_unstable();
    assert_eq!(r, vec![10, 20]);
    assert_eq!(idx.candidates("青空"), Vec::<u64>::new());
    assert_eq!(idx.get_text(10), Some(DOCS[0].1));
}

/// 既定 (n=2) の書き出しが **#121 以前とバイト等価**であること。
/// これが崩れると既存の `.etxt` 生成パイプライン / 外部 reader が影響を受ける。
#[test]
fn default_n_output_is_byte_identical_to_v2() {
    let expected = build_legacy_v2_bytes(&DOCS);

    let mut idx = NgramIndex::new();
    for (eid, text) in DOCS {
        idx.index(eid, text);
    }
    let mut got = Vec::new();
    idx.write_to(&mut got).unwrap();

    assert_eq!(
        got.len(),
        expected.len(),
        "サイズが変わっている (v2 のはずが別 format)"
    );
    assert_eq!(got, expected, "既定 n=2 の出力が旧 format とバイト等価でない");
}

/// n=3 の build → save → open → candidates round-trip（受け入れ条件その 1）。
#[test]
fn with_n3_round_trip_through_file() {
    let path = tmp_path("n3");
    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    let _ = std::fs::remove_file(&path);

    let mut idx = NgramIndex::with_n(3).unwrap();
    assert_eq!(idx.n(), 3);
    for (eid, text) in DOCS {
        idx.index(eid, text);
    }
    let in_memory = idx.candidates("国民は");
    idx.save(&path).unwrap();

    let opened = NgramIndex::open(&path).unwrap();
    assert_eq!(opened.n(), 3, "n が file から戻ること (caller は覚えなくてよい)");
    assert_eq!(opened.min_query_len(), 3);
    assert_eq!(opened.doc_count(), 3);
    assert_eq!(opened.gram_count(), idx.gram_count(), "gram 数が一致");

    let mut r = opened.candidates("国民は");
    r.sort_unstable();
    assert_eq!(r, vec![10, 20], "n=3 の候補が in-memory と一致");
    assert_eq!(r, { let mut m = in_memory.clone(); m.sort_unstable(); m });

    // 3 文字ちょうど = 1 gram なので候補がそのまま正確一致
    assert_eq!(opened.candidates("民主主"), vec![30]);
    assert_eq!(opened.candidates("存在しない語"), Vec::<u64>::new());
    assert_eq!(opened.get_text(30), Some(DOCS[2].1));

    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    let _ = std::fs::remove_file(&path);
}

/// n=4 も同じく round-trip する（u64 key の上限 = 16bit × 4）。
#[test]
fn with_n4_round_trip_through_bytes() {
    let mut idx = NgramIndex::with_n(4).unwrap();
    for (eid, text) in DOCS {
        idx.index(eid, text);
    }
    let mut buf = Vec::new();
    idx.write_to(&mut buf).unwrap();

    let opened = NgramIndex::from_bytes(buf).unwrap();
    assert_eq!(opened.n(), 4);
    let mut r = opened.candidates("国民は法");
    r.sort_unstable();
    assert_eq!(r, vec![10]);
    // 4 文字未満は絞れない
    assert_eq!(opened.candidates("国民は"), Vec::<u64>::new());
}

/// n が範囲外なら **build する前に** 落ちる。
#[test]
fn out_of_range_n_is_rejected_at_construction() {
    for n in [0usize, 1, 5, 64] {
        let err = NgramIndex::with_n(n)
            .err()
            .unwrap_or_else(|| panic!("n={n} が通ってしまった"));
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput, "n={n}");
        assert!(
            err.to_string().contains(&gram::MAX_N.to_string()),
            "上限を示すこと: {err}"
        );
    }
    for n in gram::MIN_N..=gram::MAX_N {
        assert_eq!(NgramIndex::with_n(n).unwrap().n(), n);
    }
}

/// クエリ長 < n は候補ゼロ（gram を作れない）。index プリミティブ層は fallback しない
/// —— それは `enchudb-textsearch` のポリシー。
#[test]
fn query_shorter_than_n_yields_no_candidates() {
    for n in gram::MIN_N..=gram::MAX_N {
        let mut idx = NgramIndex::with_n(n).unwrap();
        for (eid, text) in DOCS {
            idx.index(eid, text);
        }
        idx.compact();

        let long: String = "国民は法".chars().take(n).collect();
        assert!(!idx.candidates(&long).is_empty(), "n={n}: n 文字ちょうどは引ける");

        let short: String = "国民は法".chars().take(n - 1).collect();
        assert_eq!(
            idx.candidates(&short),
            Vec::<u64>::new(),
            "n={n}: {n} 文字未満 ({short:?}) は候補ゼロ"
        );
        // 全走査 fallback は使える（原文を持つ index なので）
        assert!(!idx.scan(|t| t.contains(&short)).is_empty(), "n={n}");
    }
}

/// `open_mut` / `from_bytes_mut` の rebuild が file の n を引き継ぐこと。
/// ここで n=2 に戻ると、追記した doc だけ別 n で index されて silent に壊れる。
#[test]
fn rebuild_inherits_n_from_file() {
    let path = tmp_path("rebuild");
    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    let _ = std::fs::remove_file(&path);

    let mut idx = NgramIndex::with_n(3).unwrap();
    for (eid, text) in DOCS {
        idx.index(eid, text);
    }
    idx.save(&path).unwrap();

    let mut reopened = NgramIndex::open_mut(&path).unwrap();
    assert_eq!(reopened.n(), 3, "rebuild で n が既定に戻っている");
    reopened.index(40, "追記された文書の本文");
    reopened.compact();

    // 既存 doc と追記 doc が同じ n で引ける
    assert!(reopened.candidates("国民は").contains(&10));
    assert_eq!(reopened.candidates("追記さ"), vec![40]);
    // 保存し直しても n=3 のまま
    let mut buf = Vec::new();
    reopened.write_to(&mut buf).unwrap();
    assert_eq!(NgramIndex::from_bytes(buf).unwrap().n(), 3);

    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    let _ = std::fs::remove_file(&path);
}

/// n≥3 の key は 32bit を超えるが、**hash に潰していない**ので衝突ゼロ。
/// 相異なる gram は必ず相異なる posting list になる。
#[test]
fn wide_keys_are_exact_not_hashed() {
    let mut idx = NgramIndex::with_n(3).unwrap();
    // 先頭 2 文字が同じ trigram を大量に作る（下位ビットだけ見る実装なら潰れる）
    let mut texts = Vec::new();
    for i in 0..400u32 {
        let c = char::from_u32(0x4E00 + i).unwrap();
        texts.push(format!("共通{c}尾"));
    }
    for (i, t) in texts.iter().enumerate() {
        idx.index(i as u64, t);
    }
    idx.compact();
    assert_eq!(idx.gram_count(), 2 * texts.len(), "gram が衝突して減っている");

    for (i, t) in texts.iter().enumerate() {
        assert_eq!(idx.candidates(t), vec![i as u64], "{t} が他と混ざった");
    }
}

/// postings-only でも n は保存され、候補は引ける（原文だけが無い）。
#[test]
fn postings_only_keeps_n() {
    let mut idx = NgramIndex::with_n(3).unwrap();
    for (eid, text) in DOCS {
        idx.index(eid, text);
    }
    let mut buf = Vec::new();
    idx.write_to_postings_only(&mut buf).unwrap();

    let opened = NgramIndex::from_bytes(buf).unwrap();
    assert_eq!(opened.n(), 3);
    assert!(!opened.has_text());
    assert!(opened.candidates("国民は").contains(&10));
    assert_eq!(opened.get_text(10), None);
}
