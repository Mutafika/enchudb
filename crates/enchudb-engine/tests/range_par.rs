//! 0.8.9 (#39): `_par` 系 API が seq 版と同じ結果を返すこと、 閾値以下では
//! seq fallback されること、 大規模 input でも正しく動くことを検証。
//!
//! 並列化対象は 9 API:
//!   sum_range_par / count_range_par / min_range_par / max_range_par /
//!   group_sum_range_par / group_min_range_par / group_max_range_par /
//!   range_scan_par / histogram_range_par

use enchudb_engine::{Engine, ValueType};

/// 並列実行が起きる閾値 (= engine 側の `PAR_RANGE_THRESHOLD` と同値)。
const PAR_THRESHOLD: u32 = 64_000;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-par-{}-{}-{}",
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

/// 閾値超え (= 並列化が動く) の規模で、 数値 + 8 group の Tag column を持つ
/// テーブルを作る。 n は閾値以上を指定すること。
fn build_large_db(path: &str, n: u32) -> (Engine, u32, u32) {
    let mut eng = Engine::create_standalone(path).unwrap();
    eng.define_table("t", n).unwrap();
    eng.define_himo_in("t", "val", ValueType::Number, 0).unwrap();
    eng.define_himo_in("t", "dept", ValueType::Tag, 0).unwrap();
    let depts = ["a", "b", "c", "d", "e", "f", "g", "h"];
    for i in 0..n {
        let e = eng.entity_in("t").unwrap();
        eng.tie(e, "t.val", i % 1000);
        eng.tie_text(e, "t.dept", depts[(i as usize) % depts.len()]);
    }
    eng.flush().unwrap();
    let (lo, hi) = eng.table_eid_range("t").unwrap();
    (eng, lo, hi)
}

/// 閾値未満 (= seq fallback が走る) の小規模テーブル。
fn build_small_db(path: &str) -> (Engine, u32, u32) {
    let n = 100;
    let mut eng = Engine::create_standalone(path).unwrap();
    eng.define_table("t", n).unwrap();
    eng.define_himo_in("t", "val", ValueType::Number, 0).unwrap();
    eng.define_himo_in("t", "dept", ValueType::Tag, 0).unwrap();
    let depts = ["a", "b", "c"];
    for i in 0..n {
        let e = eng.entity_in("t").unwrap();
        eng.tie(e, "t.val", i % 50);
        eng.tie_text(e, "t.dept", depts[(i as usize) % depts.len()]);
    }
    eng.flush().unwrap();
    let (lo, hi) = eng.table_eid_range("t").unwrap();
    (eng, lo, hi)
}

#[test]
fn sum_count_range_par_matches_seq_large() {
    let path = tmp_path("sum_count_par");
    cleanup(&path);
    let n = PAR_THRESHOLD + 5_000;
    let (eng, lo, hi) = build_large_db(&path, n);

    assert_eq!(eng.sum_range_par("t.val", lo, hi), eng.sum_range("t.val", lo, hi));
    assert_eq!(eng.count_range_par("t.val", lo, hi), eng.count_range("t.val", lo, hi));

    // 部分範囲でも一致
    let mid = lo + (hi - lo) / 2;
    assert_eq!(eng.sum_range_par("t.val", lo, mid), eng.sum_range("t.val", lo, mid));
    assert_eq!(eng.count_range_par("t.val", lo, mid), eng.count_range("t.val", lo, mid));

    drop(eng);
    cleanup(&path);
}

#[test]
fn min_max_range_par_matches_seq_large() {
    let path = tmp_path("min_max_par");
    cleanup(&path);
    let n = PAR_THRESHOLD + 5_000;
    let (eng, lo, hi) = build_large_db(&path, n);

    assert_eq!(eng.min_range_par("t.val", lo, hi), eng.min_range("t.val", lo, hi));
    assert_eq!(eng.max_range_par("t.val", lo, hi), eng.max_range("t.val", lo, hi));
    // val % 1000 なので min=0、 max=999
    assert_eq!(eng.min_range_par("t.val", lo, hi), Some(0));
    assert_eq!(eng.max_range_par("t.val", lo, hi), Some(999));

    drop(eng);
    cleanup(&path);
}

#[test]
fn group_sum_range_par_matches_seq_large() {
    let path = tmp_path("group_sum_par");
    cleanup(&path);
    let n = PAR_THRESHOLD + 5_000;
    let (eng, lo, hi) = build_large_db(&path, n);

    let mut par = eng.group_sum_range_par("t.dept", "t.val", lo, hi);
    let mut seq = eng.group_sum_range("t.dept", "t.val", lo, hi);
    par.sort_by_key(|x| x.0);
    seq.sort_by_key(|x| x.0);
    assert_eq!(par, seq);

    drop(eng);
    cleanup(&path);
}

#[test]
fn group_min_max_range_par_matches_seq_large() {
    let path = tmp_path("group_mm_par");
    cleanup(&path);
    let n = PAR_THRESHOLD + 5_000;
    let (eng, lo, hi) = build_large_db(&path, n);

    let mut par_min = eng.group_min_range_par("t.dept", "t.val", lo, hi);
    let mut seq_min = eng.group_min_range("t.dept", "t.val", lo, hi);
    par_min.sort_by_key(|x| x.0);
    seq_min.sort_by_key(|x| x.0);
    assert_eq!(par_min, seq_min);

    let mut par_max = eng.group_max_range_par("t.dept", "t.val", lo, hi);
    let mut seq_max = eng.group_max_range("t.dept", "t.val", lo, hi);
    par_max.sort_by_key(|x| x.0);
    seq_max.sort_by_key(|x| x.0);
    assert_eq!(par_max, seq_max);

    drop(eng);
    cleanup(&path);
}

#[test]
fn range_scan_par_matches_seq_large() {
    let path = tmp_path("range_scan_par");
    cleanup(&path);
    let n = PAR_THRESHOLD + 5_000;
    let (eng, _lo, _hi) = build_large_db(&path, n);

    let par = eng.range_scan_par("t.val", 100, 200);
    let seq = eng.range_scan("t.val", 100, 200);
    // seq は eid 昇順、 par も chunk 順なので昇順、 順序一致を期待
    assert_eq!(par, seq);

    // 結果規模が妥当 (= 値域 [100, 200] は 101 個の値、 n の約 1/10)
    assert!(par.len() > 0);
    assert!(par.len() < n as usize);

    drop(eng);
    cleanup(&path);
}

#[test]
fn histogram_range_par_matches_seq_large() {
    let path = tmp_path("hist_par");
    cleanup(&path);
    let n = PAR_THRESHOLD + 5_000;
    let (eng, lo, hi) = build_large_db(&path, n);

    let par = eng.histogram_range_par("t.val", lo, hi, 0, 999, 10);
    let seq = eng.histogram_range("t.val", lo, hi, 0, 999, 10);
    assert_eq!(par, seq);
    assert_eq!(par.len(), 10);

    // 合計件数は count_range と一致
    let total: u32 = par.iter().sum();
    assert_eq!(total, eng.count_range("t.val", lo, hi));

    drop(eng);
    cleanup(&path);
}

#[test]
fn par_falls_back_to_seq_under_threshold() {
    // 閾値未満では seq fallback、 同じ結果が出ることを確認 (= API として
    // 透明、 callsite は規模を意識せず呼んでよい)。
    let path = tmp_path("small_fallback");
    cleanup(&path);
    let (eng, lo, hi) = build_small_db(&path);

    assert_eq!(eng.sum_range_par("t.val", lo, hi), eng.sum_range("t.val", lo, hi));
    assert_eq!(eng.count_range_par("t.val", lo, hi), eng.count_range("t.val", lo, hi));
    assert_eq!(eng.min_range_par("t.val", lo, hi), eng.min_range("t.val", lo, hi));
    assert_eq!(eng.max_range_par("t.val", lo, hi), eng.max_range("t.val", lo, hi));
    assert_eq!(
        eng.histogram_range_par("t.val", lo, hi, 0, 50, 5),
        eng.histogram_range("t.val", lo, hi, 0, 50, 5)
    );

    drop(eng);
    cleanup(&path);
}

#[test]
fn par_handles_empty_range_gracefully() {
    let path = tmp_path("empty_par");
    cleanup(&path);
    let (eng, lo, _hi) = build_small_db(&path);

    // 空範囲
    assert_eq!(eng.sum_range_par("t.val", lo, lo), 0);
    assert_eq!(eng.count_range_par("t.val", lo, lo), 0);
    assert_eq!(eng.min_range_par("t.val", lo, lo), None);
    assert_eq!(eng.max_range_par("t.val", lo, lo), None);

    drop(eng);
    cleanup(&path);
}
