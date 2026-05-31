//! 0.8.10 (#43): engine の `_eids_par` 系 (= 不連続 eids への並列集計) と
//! `histogram_eids` / `histogram_eids_par` の動作検証。
//!
//! 検証ポイント:
//!   - par 版が seq 版と同じ結果を返す
//!   - 閾値以下では seq fallback (= API 透明)
//!   - 連続 eid 集合 (= range 全件) でも `_range_par` と一致
//!   - 不連続 (= 飛び飛び) でも正しく動く

use enchudb_engine::{Engine, HimoType};

const PAR_THRESHOLD: usize = 64_000;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-eids-par-{}-{}-{}",
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

fn build(path: &str, n: u32) -> (Engine, u32, u32) {
    let mut eng = Engine::create_standalone(path).unwrap();
    eng.define_table("t", n).unwrap();
    eng.define_himo_in("t", "val", HimoType::Number, 0).unwrap();
    eng.define_himo_in("t", "dept", HimoType::Tag, 0).unwrap();
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

fn make_eids(eng: &Engine, lo: u32, hi: u32) -> Vec<enchudb_oplog::EntityId> {
    (lo..hi).map(|i| enchudb_oplog::make_eid(eng.peer_id(), i)).collect()
}

#[test]
fn eids_par_matches_seq_large() {
    let path = tmp_path("large");
    cleanup(&path);
    let n = PAR_THRESHOLD as u32 + 5_000;
    let (eng, lo, hi) = build(&path, n);
    let eids = make_eids(&eng, lo, hi);

    assert_eq!(eng.sum_eids_par("t.val", &eids), eng.sum("t.val", &eids));
    assert_eq!(eng.count_eids_par("t.val", &eids), eng.count("t.val", &eids));
    assert_eq!(eng.min_eids_par("t.val", &eids), eng.min("t.val", &eids));
    assert_eq!(eng.max_eids_par("t.val", &eids), eng.max("t.val", &eids));

    drop(eng);
    cleanup(&path);
}

#[test]
fn eids_par_matches_range_par_when_contiguous() {
    // 連続 eid 集合では eids 版 = range 版 と一致するべき (= mmap の場所 / 順序が同じ)
    let path = tmp_path("contig");
    cleanup(&path);
    let n = PAR_THRESHOLD as u32 + 5_000;
    let (eng, lo, hi) = build(&path, n);
    let eids = make_eids(&eng, lo, hi);

    assert_eq!(eng.sum_eids_par("t.val", &eids), eng.sum_range_par("t.val", lo, hi));
    assert_eq!(eng.count_eids_par("t.val", &eids), eng.count_range_par("t.val", lo, hi));
    assert_eq!(eng.min_eids_par("t.val", &eids), eng.min_range_par("t.val", lo, hi));
    assert_eq!(eng.max_eids_par("t.val", &eids), eng.max_range_par("t.val", lo, hi));

    drop(eng);
    cleanup(&path);
}

#[test]
fn group_eids_par_matches_seq_large() {
    let path = tmp_path("group_large");
    cleanup(&path);
    let n = PAR_THRESHOLD as u32 + 5_000;
    let (eng, lo, hi) = build(&path, n);
    let eids = make_eids(&eng, lo, hi);

    let mut par = eng.group_sum_eids_par("t.dept", "t.val", &eids);
    let mut seq = eng.group_sum("t.dept", "t.val", &eids);
    par.sort_by_key(|x| x.0);
    seq.sort_by_key(|x| x.0);
    assert_eq!(par, seq);

    let mut par_min = eng.group_min_eids_par("t.dept", "t.val", &eids);
    let mut seq_min = eng.group_min("t.dept", "t.val", &eids);
    par_min.sort_by_key(|x| x.0);
    seq_min.sort_by_key(|x| x.0);
    assert_eq!(par_min, seq_min);

    let mut par_max = eng.group_max_eids_par("t.dept", "t.val", &eids);
    let mut seq_max = eng.group_max("t.dept", "t.val", &eids);
    par_max.sort_by_key(|x| x.0);
    seq_max.sort_by_key(|x| x.0);
    assert_eq!(par_max, seq_max);

    drop(eng);
    cleanup(&path);
}

#[test]
fn histogram_eids_matches_range() {
    // 連続 eid 集合での histogram_eids = histogram_range
    let path = tmp_path("hist");
    cleanup(&path);
    let n = PAR_THRESHOLD as u32 + 5_000;
    let (eng, lo, hi) = build(&path, n);
    let eids = make_eids(&eng, lo, hi);

    let seq_eids = eng.histogram_eids("t.val", &eids, 0, 999, 10);
    let par_eids = eng.histogram_eids_par("t.val", &eids, 0, 999, 10);
    let seq_range = eng.histogram_range("t.val", lo, hi, 0, 999, 10);

    assert_eq!(seq_eids, par_eids);
    assert_eq!(seq_eids, seq_range);
    assert_eq!(seq_eids.len(), 10);

    let total: u32 = seq_eids.iter().sum();
    assert_eq!(total, eng.count_range("t.val", lo, hi));

    drop(eng);
    cleanup(&path);
}

#[test]
fn histogram_eids_edge_cases() {
    let path = tmp_path("hist_edge");
    cleanup(&path);
    let n = 100u32;
    let (eng, lo, hi) = build(&path, n);
    let eids = make_eids(&eng, lo, hi);

    // n_buckets = 0
    assert!(eng.histogram_eids("t.val", &eids, 0, 100, 0).is_empty());

    // vmin > vmax
    assert!(eng.histogram_eids("t.val", &eids, 100, 0, 5).is_empty());

    // 空 eids
    let empty: Vec<_> = Vec::new();
    let hist = eng.histogram_eids("t.val", &empty, 0, 100, 5);
    assert_eq!(hist, vec![0, 0, 0, 0, 0]);

    drop(eng);
    cleanup(&path);
}

#[test]
fn eids_par_sparse_subset() {
    // 飛び飛び (= 偶数 index のみ) で正しく動くか
    let path = tmp_path("sparse");
    cleanup(&path);
    let n = PAR_THRESHOLD as u32 + 5_000;
    let (eng, lo, hi) = build(&path, n);

    let eids: Vec<_> = (lo..hi)
        .filter(|i| i % 2 == 0)
        .map(|i| enchudb_oplog::make_eid(eng.peer_id(), i))
        .collect();

    assert_eq!(eng.sum_eids_par("t.val", &eids), eng.sum("t.val", &eids));
    assert_eq!(eng.count_eids_par("t.val", &eids), eng.count("t.val", &eids));
    assert_eq!(eng.min_eids_par("t.val", &eids), eng.min("t.val", &eids));
    assert_eq!(eng.max_eids_par("t.val", &eids), eng.max("t.val", &eids));

    drop(eng);
    cleanup(&path);
}

#[test]
fn eids_par_falls_back_to_seq_under_threshold() {
    let path = tmp_path("small");
    cleanup(&path);
    let (eng, lo, hi) = build(&path, 100);
    let eids = make_eids(&eng, lo, hi);

    // 閾値未満で seq fallback、 同じ結果
    assert_eq!(eng.sum_eids_par("t.val", &eids), eng.sum("t.val", &eids));
    assert_eq!(eng.count_eids_par("t.val", &eids), eng.count("t.val", &eids));
    assert_eq!(eng.min_eids_par("t.val", &eids), eng.min("t.val", &eids));
    assert_eq!(eng.max_eids_par("t.val", &eids), eng.max("t.val", &eids));

    drop(eng);
    cleanup(&path);
}
