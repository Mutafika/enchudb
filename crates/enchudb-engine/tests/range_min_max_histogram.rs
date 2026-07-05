//! 0.8.8 (#38): `min_range` / `max_range` / `group_min_range` / `group_max_range` /
//! `histogram_range` の動作検証。
//!
//! 既存 `sum_range` / `group_sum_range` と同じ `_range` pattern。 dense path
//! (= Tag with max_values 小) と sparse path (= Number with cap=0) の両方を網羅。

use enchudb_engine::{Engine, ValueType};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-range-mmh-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suffix in ["", ".oplog", ".crc", ".db.lock", ".tables", ".tables.tmp", ".schema"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

/// テーブル + 数値 column + group(Tag) column を 1 個作って tie する helper。
/// 戻り値は (eid_lo, eid_hi) — `Engine::table_eid_range("t")` 相当。
fn build_db(path: &str, pairs: &[(u32, &str)]) -> (Engine, u32, u32) {
    let mut eng = Engine::create_standalone(path).unwrap();
    eng.define_table("t", pairs.len() as u32).unwrap();
    eng.define_himo_in("t", "val", ValueType::Number, 0).unwrap();
    // dept は Tag (= vocab 経由)、 cardinality 小なので dense path に乗る。
    eng.define_himo_in("t", "dept", ValueType::Tag, 0).unwrap();

    for &(val, dept) in pairs {
        let e = eng.entity_in("t").unwrap();
        eng.tie(e, "t.val", val);
        eng.tie_text(e, "t.dept", dept);
    }
    eng.flush().unwrap();

    let (lo, hi) = eng.table_eid_range("t").expect("table eid range");
    (eng, lo, hi)
}

#[test]
fn min_max_range_basic() {
    let path = tmp_path("min_max");
    cleanup(&path);

    let pairs: Vec<(u32, &str)> = vec![
        (50, "eng"), (30, "eng"), (90, "sales"),
        (20, "eng"), (70, "sales"), (10, "ops"),
    ];
    let (eng, lo, hi) = build_db(&path, &pairs);

    assert_eq!(eng.min_range("t.val", lo, hi), Some(10));
    assert_eq!(eng.max_range("t.val", lo, hi), Some(90));

    // 部分範囲: 最初 3 件のみ
    assert_eq!(eng.min_range("t.val", lo, lo + 3), Some(30));
    assert_eq!(eng.max_range("t.val", lo, lo + 3), Some(90));

    // 空範囲
    assert_eq!(eng.min_range("t.val", lo, lo), None);
    assert_eq!(eng.max_range("t.val", lo, lo), None);

    // 未定義 himo
    assert_eq!(eng.min_range("t.nope", lo, hi), None);
    assert_eq!(eng.max_range("t.nope", lo, hi), None);

    drop(eng);
    cleanup(&path);
}

#[test]
fn min_max_range_with_zero_value() {
    // 値 0 は stored=1 になるので missing と区別される (= valid データ扱い)。
    let path = tmp_path("zero");
    cleanup(&path);

    let pairs: Vec<(u32, &str)> = vec![(0, "a"), (5, "a"), (3, "b")];
    let (eng, lo, hi) = build_db(&path, &pairs);

    assert_eq!(eng.min_range("t.val", lo, hi), Some(0));
    assert_eq!(eng.max_range("t.val", lo, hi), Some(5));

    drop(eng);
    cleanup(&path);
}

#[test]
fn group_min_range_dense() {
    let path = tmp_path("gmin_dense");
    cleanup(&path);

    let pairs: Vec<(u32, &str)> = vec![
        (50, "eng"), (30, "eng"), (20, "eng"),
        (90, "sales"), (70, "sales"),
        (10, "ops"),
    ];
    let (eng, lo, hi) = build_db(&path, &pairs);

    let mut got = eng.group_min_range("t.dept", "t.val", lo, hi);
    got.sort_by_key(|x| x.0);

    // dept vocab id は順序 (1: eng, 2: sales, 3: ops) 相当だが、
    // ID 順は内部 vocab に依存するので、 group min の値だけ検証。
    let values: Vec<u32> = got.iter().map(|(_, v)| *v).collect();
    let mut sorted_vals = values.clone();
    sorted_vals.sort();
    assert_eq!(sorted_vals, vec![10, 20, 70], "min per dept = [ops:10, eng:20, sales:70]");

    drop(eng);
    cleanup(&path);
}

#[test]
fn group_max_range_dense() {
    let path = tmp_path("gmax_dense");
    cleanup(&path);

    let pairs: Vec<(u32, &str)> = vec![
        (50, "eng"), (30, "eng"), (20, "eng"),
        (90, "sales"), (70, "sales"),
        (10, "ops"),
    ];
    let (eng, lo, hi) = build_db(&path, &pairs);

    let got = eng.group_max_range("t.dept", "t.val", lo, hi);
    let mut max_values: Vec<u32> = got.iter().map(|(_, v)| *v).collect();
    max_values.sort();
    assert_eq!(max_values, vec![10, 50, 90], "max per dept = [ops:10, eng:50, sales:90]");

    drop(eng);
    cleanup(&path);
}

#[test]
fn group_min_max_partial_range() {
    let path = tmp_path("gmm_partial");
    cleanup(&path);

    let pairs: Vec<(u32, &str)> = vec![
        (50, "eng"), (30, "eng"), (20, "eng"),
        (90, "sales"), (70, "sales"),
        (10, "ops"),
    ];
    let (eng, lo, hi) = build_db(&path, &pairs);
    let _ = hi;

    // 最初 3 件のみ → 全部 "eng"、 min=20 / max=50 の 1 group のみ
    let gmin = eng.group_min_range("t.dept", "t.val", lo, lo + 3);
    let gmax = eng.group_max_range("t.dept", "t.val", lo, lo + 3);
    assert_eq!(gmin.len(), 1);
    assert_eq!(gmax.len(), 1);
    assert_eq!(gmin[0].1, 20);
    assert_eq!(gmax[0].1, 50);

    drop(eng);
    cleanup(&path);
}

#[test]
fn histogram_range_basic() {
    // 値域 [0, 99] を 10 bucket に分割 → 各 bucket は 10 width。
    let path = tmp_path("hist_basic");
    cleanup(&path);

    // bucket 0: [0,10), bucket 1: [10,20), ..., bucket 9: [90,100)
    let pairs: Vec<(u32, &str)> = vec![
        (5, "x"), (15, "x"), (25, "x"), (35, "x"), (45, "x"),
        (55, "x"), (65, "x"), (75, "x"), (85, "x"), (95, "x"),
        // 各 bucket に 1 件ずつ
        (50, "x"), (50, "x"), (50, "x"), // bucket 5 に 3 件追加 = 計 4 件
    ];
    let (eng, lo, hi) = build_db(&path, &pairs);

    let hist = eng.histogram_range("t.val", lo, hi, 0, 99, 10);
    assert_eq!(hist.len(), 10);
    // bucket 0..4 は 1, bucket 5 は 4, bucket 6..9 は 1
    assert_eq!(hist[0], 1, "bucket 0 (5)");
    assert_eq!(hist[1], 1, "bucket 1 (15)");
    assert_eq!(hist[4], 1, "bucket 4 (45)");
    assert_eq!(hist[5], 4, "bucket 5 (55 + 50×3)");
    assert_eq!(hist[9], 1, "bucket 9 (95)");

    drop(eng);
    cleanup(&path);
}

#[test]
fn histogram_range_edge_cases() {
    let path = tmp_path("hist_edge");
    cleanup(&path);

    let pairs: Vec<(u32, &str)> = vec![(5, "x"), (15, "x"), (25, "x")];
    let (eng, lo, hi) = build_db(&path, &pairs);

    // n_buckets = 0 → 空 Vec
    let h0 = eng.histogram_range("t.val", lo, hi, 0, 100, 0);
    assert!(h0.is_empty());

    // vmin > vmax → 空 Vec
    let h_inv = eng.histogram_range("t.val", lo, hi, 100, 0, 10);
    assert!(h_inv.is_empty());

    // 値域 外の値はカウントしない (= clipping ではなく drop)
    let h_narrow = eng.histogram_range("t.val", lo, hi, 10, 20, 1);
    assert_eq!(h_narrow.len(), 1);
    assert_eq!(h_narrow[0], 1, "値 15 のみ [10,20] 範囲内");

    // vmin == vmax (= span 1) でも crash しない
    let h_point = eng.histogram_range("t.val", lo, hi, 15, 15, 5);
    assert_eq!(h_point.len(), 5);
    let total: u32 = h_point.iter().sum();
    assert_eq!(total, 1, "値 15 が 1 件、 bucket index は floor((15-15)*5/1) = 0");
    assert_eq!(h_point[0], 1);

    drop(eng);
    cleanup(&path);
}

#[test]
fn min_max_consistency_with_per_eid_api() {
    // `min_range` と既存 eids 版 `min` が等価な結果を返すこと
    let path = tmp_path("consistency");
    cleanup(&path);

    let pairs: Vec<(u32, &str)> = vec![(50, "a"), (30, "b"), (90, "a"), (20, "b")];
    let (eng, lo, hi) = build_db(&path, &pairs);

    let eids: Vec<_> = (lo..hi)
        .map(|i| enchudb_oplog::make_eid(eng.peer_id(), i))
        .collect();

    assert_eq!(eng.min_range("t.val", lo, hi), eng.min("t.val", &eids));
    assert_eq!(eng.max_range("t.val", lo, hi), eng.max("t.val", &eids));

    drop(eng);
    cleanup(&path);
}
