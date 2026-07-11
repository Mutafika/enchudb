//! #88 (0.12.0): v5 (leaf region 無し = `Leaf` 値が vocab に単調 append) DB を
//! v6 (`LeafStore` あり = reclaim 対応) へ移送する migration の検証。
//!
//! fixture は `create_full_with_leaf(.., Some(0))` で「leaf region 無し」= v5 相当
//! DB を実際に作り、 Leaf を tie する (leaf_for()==None なので vocab に載る)。
//! これを migrate して reopen し、 read 整合 + reclaim 稼働を確認する。

use enchudb_engine::{Engine, ValueType};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue88mig-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
    let _ = std::fs::remove_file(format!("{}.tables", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.db.lock", path));
    let _ = std::fs::remove_file(format!("{}.migrating", path));
    let _ = std::fs::remove_file(format!("{}.eidmap", path));
}

/// v5 相当 DB (leaf region 無し) を作り、 Leaf を数件 + Tag を tie して、
/// flush → drop → bytes を返す。 併せて (leaf の期待値, tag の期待値) も返す。
fn make_v5_bytes(path: &str) -> (Vec<u8>, Vec<String>, String) {
    cleanup(path);
    // 小さい region で軽量な fixture (fs::read が速い)。 Some(0) = leaf region 無し。
    let mut eng = Engine::create_full_with_leaf(
        path,
        1000,
        Some(256 * 1024), // vocab_data
        Some(64),         // max_himos
        Some(64 * 1024),  // content_data
        None,             // cyl_max_values
        Some(0),          // leaf_data_size = 0 → v5 相当
    )
    .unwrap();

    eng.define_himo("body", ValueType::Leaf, 0);
    eng.define_himo("kind", ValueType::Tag, 0);

    let leaves = vec![
        "first leaf payload".to_string(),
        "second — a somewhat longer leaf value here".to_string(),
        "third".to_string(),
    ];
    let tag = "note".to_string();
    for lv in &leaves {
        let e = eng.entity();
        eng.tie_text(e, "body", lv);
        eng.tie_text(e, "kind", &tag);
    }

    // fixture が本当に v5 相当か falsify: leaf region 無し & Leaf は vocab に載る。
    assert!(eng.leaf_footprint().is_none(), "fixture は leaf region 無しのはず");
    let pre = eng.vocab_orphan_stats();
    // 3 distinct Leaf vid + 1 shared Tag vid = 4。
    assert_eq!(pre.vocab_total, 4, "Leaf(3)+Tag(1) が vocab に載っているはず");

    eng.flush().unwrap();
    drop(eng);
    let bytes = std::fs::read(path).unwrap();
    cleanup(path);
    (bytes, leaves, tag)
}

#[test]
fn migrate_bytes_moves_leaf_and_reads_back() {
    let path = tmp_path("bytes-read");
    let (src, leaves, tag) = make_v5_bytes(&path);

    // 小さい leaf region (1 MiB) で移送。
    let (dst, stats) =
        Engine::migrate_bytes_v5_to_v6(src, 1024 * 1024, &[]).expect("migrate");

    assert!(!stats.already_v6);
    assert_eq!(stats.leaf_himos, 1, "Leaf himo は body の 1 本");
    assert_eq!(stats.cells_moved, 3, "3 entity 分の Leaf cell を移送");
    let expect_bytes: u64 = leaves.iter().map(|s| s.len() as u64).sum();
    assert_eq!(stats.bytes_moved, expect_bytes);
    assert!(stats.leaf_footprint > 12, "leaf footprint が伸びているはず");
    assert_eq!(stats.vocab_orphan_bytes_left, expect_bytes);

    // 移送後の DB を reopen して read 整合を確認。
    let eng2 = Engine::from_bytes(dst).unwrap();
    assert_eq!(
        eng2.leaf_footprint(),
        Some(stats.leaf_footprint),
        "reopen 後 leaf region あり & footprint 一致",
    );
    // Leaf は LeafStore から、 Tag は依然 vocab から読めること。
    for (i, lv) in leaves.iter().enumerate() {
        let e = i as u64; // anonymous eid は 0,1,2
        assert_eq!(
            eng2.get_text(e, "body").map(|b| b.to_vec()),
            Some(lv.as_bytes().to_vec()),
            "entity {i} の Leaf が移送後も読める",
        );
        assert_eq!(
            eng2.get_text(e, "kind").map(|b| b.to_vec()),
            Some(tag.as_bytes().to_vec()),
            "Tag は移送で壊れない",
        );
    }

    // 旧 vocab の Leaf bytes は orphan として残る (既知 trade-off)。 Tag は live。
    let post = eng2.vocab_orphan_stats();
    assert_eq!(post.orphan_bytes, expect_bytes, "移送した Leaf bytes が vocab orphan に");
    assert_eq!(post.live_vids, 1, "Tag vid のみ live");
}

#[test]
fn migrate_enables_reclaim() {
    // v5 では Leaf は回収不能。 移送後は同サイズ re-tie で footprint 有界化する
    // (= reclaim が稼働) ことを確認 = migration の主目的の falsification。
    let path = tmp_path("reclaim");
    let (src, _leaves, _tag) = make_v5_bytes(&path);
    let (dst, _stats) = Engine::migrate_bytes_v5_to_v6(src, 1024 * 1024, &[]).unwrap();

    let mut eng2 = Engine::from_bytes(dst).unwrap();
    let fp0 = eng2.leaf_footprint().unwrap();

    // entity 0 の body を同サイズで 30 回 re-tie → 毎回旧 slot free → 再利用で不変。
    let same_len = "first leaf payloaX"; // "first leaf payload" と同長 (18)
    assert_eq!(same_len.len(), "first leaf payload".len());
    for _ in 0..30 {
        eng2.tie_text(0, "body", same_len);
    }
    let fp1 = eng2.leaf_footprint().unwrap();
    assert_eq!(fp0, fp1, "同サイズ re-tie で footprint が増えた = reclaim してない");

    // untie で末尾 slot を回収して footprint 後退。
    // entity 2 ("third") が最後に insert された = 末尾なので retract する。
    eng2.untie(2, "body");
    let fp2 = eng2.leaf_footprint().unwrap();
    assert!(fp2 < fp1, "untie で footprint 回収されず (before={fp1}, after={fp2})");
}

#[test]
fn migrate_bytes_already_v6_is_noop() {
    let path = tmp_path("already-v6");
    cleanup(&path);
    // leaf region あり (small) の v6 DB を作る。
    let mut eng = Engine::create_full_with_leaf(
        &path, 1000, Some(256 * 1024), Some(64), Some(64 * 1024), None, Some(64 * 1024),
    )
    .unwrap();
    eng.define_himo("body", ValueType::Leaf, 0);
    let e = eng.entity();
    eng.tie_text(e, "body", "already v6");
    eng.flush().unwrap();
    drop(eng);
    let src = std::fs::read(&path).unwrap();
    cleanup(&path);

    let src_clone = src.clone();
    let (dst, stats) = Engine::migrate_bytes_v5_to_v6(src, 1024 * 1024, &[]).unwrap();
    assert!(stats.already_v6, "既に v6 なら already_v6");
    assert_eq!(stats.cells_moved, 0);
    assert_eq!(dst, src_clone, "already_v6 は bytes を変えない");
}

#[test]
fn migrate_file_roundtrip() {
    let src_path = tmp_path("file-src");
    let dst_path = tmp_path("file-dst");
    cleanup(&src_path);
    cleanup(&dst_path);

    // v5 相当 file を作る。
    {
        let mut eng = Engine::create_full_with_leaf(
            &src_path, 1000, Some(256 * 1024), Some(64), Some(64 * 1024), None, Some(0),
        )
        .unwrap();
        eng.define_himo("body", ValueType::Leaf, 0);
        for lv in ["alpha", "beta value", "gamma"] {
            let e = eng.entity();
            eng.tie_text(e, "body", lv);
        }
        assert!(eng.leaf_footprint().is_none());
        eng.flush().unwrap();
    }

    let stats =
        Engine::migrate_file_v5_to_v6_with_leaf(&src_path, &dst_path, 1024 * 1024).unwrap();
    assert_eq!(stats.leaf_himos, 1);
    assert_eq!(stats.cells_moved, 3);

    // dst を open して読めること (src は不変)。
    {
        let eng = Engine::open(&dst_path).unwrap();
        assert!(eng.leaf_footprint().is_some(), "dst は v6 (leaf region あり)");
        for (i, lv) in ["alpha", "beta value", "gamma"].iter().enumerate() {
            assert_eq!(
                eng.get_text(i as u64, "body").map(|b| b.to_vec()),
                Some(lv.as_bytes().to_vec()),
            );
        }
    }
    // src は依然 v5 (非破壊)。
    {
        let src_bytes = std::fs::read(&src_path).unwrap();
        let leaf_size = u64::from_le_bytes(src_bytes[80..88].try_into().unwrap());
        assert_eq!(leaf_size, 0, "src は移送で書き換わっていない (leaf region 無しのまま)");
    }

    cleanup(&src_path);
    cleanup(&dst_path);
}
