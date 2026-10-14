//! #474 の 「ついでに」: 和が 0 の group (row はあるが値が全部 0) を、 group_sum の 4 つの道 (逐次 / `_range` / `_range_par` /
//! `_eids_par`) が、 cardinality の hint の有無によらずそろって返すこと。 前は hint 付きの `_range` / `_range_par` / `_eids_par`
//! だけが返さなかった (和 > 0 で選んでいた) ので、 `Query::group_sum` の答えが件数 64000 を境に変わり、 `Table::group_sum` とも違った。
//!
//! hint (10) の中 (group 3) と外 (group 20) の両方に、 値が全部 0 の group を作る。 行数は並列の閾値の下と上、 eid は全部と飛び飛び。

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::EntityId;
use std::collections::BTreeMap;

const PAR_THRESHOLD: usize = 64_000;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-group-zero-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path); // v10: DB は directory
    for suffix in ["", ".oplog", ".crc", ".db.lock", ".tables", ".tables.tmp", ".schema"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

/// assert で落ちても DB を消す。 engine より先に作って、 engine の後に drop させる。
struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        cleanup(&self.0);
    }
}

fn sorted(mut v: Vec<(u32, u64)>) -> Vec<(u32, u64)> {
    v.sort();
    v
}

fn run(n: u32, hint: u32) {
    let path = tmp_path(&format!("{n}-{hint}"));
    cleanup(&path);
    let _cleanup = Cleanup(path.clone());
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_table("t", n).unwrap();
    eng.define_himo_in("t", "g", ValueType::Number, hint).unwrap();
    eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
    let mut eids: Vec<EntityId> = Vec::new();
    let mut rows: Vec<(u32, u32)> = Vec::new();
    for i in 0..n {
        let e = eng.entity_in("t").unwrap();
        // group は 2 行ずつ同じ (偶数番目だけの部分集合にも全部の group が入る)。 25 行に 1 行は hint の外の group 20 (偶数番目と
        // 奇数番目に交互に入る)。 group 5 は最後の 300 行にだけある (並列の版で、 後ろの塊にしか無い group の印を足し合わせで
        // 落とさないかを見る)。 3 / 5 / 20 は値が全部 0
        let g = if i % 25 == 24 {
            20
        } else if i + 300 >= n {
            5
        } else if (i / 2) % 10 == 5 {
            6
        } else {
            (i / 2) % 10
        };
        let v = if g == 3 || g == 5 || g == 20 { 0 } else { 1 + i % 7 };
        eng.tie(e, "t.g", g);
        eng.tie(e, "t.v", v);
        eids.push(e);
        rows.push((g, v));
    }
    eng.flush().unwrap();
    let (lo, hi) = eng.table_eid_range("t").unwrap();
    let half: Vec<EntityId> = eids.iter().step_by(2).copied().collect();
    let want = |rs: &mut dyn Iterator<Item = &(u32, u32)>| -> Vec<(u32, u64)> {
        let mut m: BTreeMap<u32, u64> = BTreeMap::new();
        for &(g, v) in rs {
            *m.entry(g).or_insert(0) += v as u64;
        }
        m.into_iter().collect()
    };
    let w_all = want(&mut rows.iter());
    let w_half = want(&mut rows.iter().step_by(2));
    // 対照: 和が 0 の group が写しにある (hint の中の 3 / 5 と外の 20)
    for w in [&w_all, &w_half] {
        assert!(w.contains(&(3, 0)) && w.contains(&(5, 0)) && w.contains(&(20, 0)), "対照: 和が 0 の group が写しに無い");
    }
    let t = |f: &str| format!("n={n} hint={hint} {f}");
    assert_eq!(sorted(eng.group_sum("t.g", "t.v", &eids)), w_all, "{}", t("group_sum"));
    assert_eq!(sorted(eng.group_sum("t.g", "t.v", &half)), w_half, "{}", t("group_sum (飛び飛び)"));
    assert_eq!(sorted(eng.group_sum_eids_par("t.g", "t.v", &eids)), w_all, "{}", t("group_sum_eids_par"));
    assert_eq!(sorted(eng.group_sum_eids_par("t.g", "t.v", &half)), w_half, "{}", t("group_sum_eids_par (飛び飛び)"));
    assert_eq!(sorted(eng.group_sum_range("t.g", "t.v", lo, hi)), w_all, "{}", t("group_sum_range"));
    assert_eq!(sorted(eng.group_sum_range_par("t.g", "t.v", lo, hi)), w_all, "{}", t("group_sum_range_par"));
}

#[test]
fn zero_sum_groups_small() {
    run(1_000, 10);
    run(1_000, 0);
}

/// 並列の版 (閾値の上、 飛び飛びの部分集合も閾値を超える)
#[test]
fn zero_sum_groups_parallel() {
    let n = (2 * PAR_THRESHOLD + 10_000) as u32;
    run(n, 10);
    run(n, 0);
}
