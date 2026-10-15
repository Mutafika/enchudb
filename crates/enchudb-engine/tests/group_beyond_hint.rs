//! #474: 列の cardinality の hint (`max_values`) 以上の group の値が、 group 集計の dense の道で panic したり
//! (逐次の版)、 黙って落ちたり (`_range` / `_range_par` / `_eids_par`) しないこと。
//!
//! 14 個の関数を、 (a) 書いた値を覚えた写しから数えた答え、 (b) hint の無い同じ値の列 (HashMap の道) の答え、 の
//! 2 つと比べる。 行数は並列の閾値 (64000) の下と上の両方。 hint の道の答えが group の昇順なことも見る。
//! 和が 0 の group はここでは作らない (それは `group_zero_sum.rs` で見る)。

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::EntityId;
use std::collections::BTreeMap;

const PAR_THRESHOLD: usize = 64_000;
const HINT: u32 = 10;
/// hint の外の group: hint ちょうど、 少し上、 dense の道の上限 (65536) の手前と先、 遠い値。
const BEYOND: [u32; 5] = [10, 20, 65_535, 70_000, 4_000_000_000];

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-group-beyond-{}-{}-{}",
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

/// assert で落ちても DB を消す (変異試験で落とすたびに /tmp に残っていた)。 engine より先に作って、 engine の後に drop させる。
struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        cleanup(&self.0);
    }
}

/// 写しの 1 row: (group, val)。 どちらも無いことがある。
type Row = (Option<u32>, Option<u32>);

/// `t.g` (hint 10) と `t.g_nh` (hint 無し) に同じ group の値、 `t.v` に値を書く。
fn build(path: &str, n: u32) -> (Engine, Vec<EntityId>, Vec<Row>, u32, u32) {
    let mut eng = Engine::create_standalone(path).unwrap();
    eng.define_table("t", n).unwrap();
    eng.define_himo_in("t", "g", ValueType::Number, HINT).unwrap();
    eng.define_himo_in("t", "g_nh", ValueType::Number, 0).unwrap();
    eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
    let (mut eids, mut rows) = (Vec::new(), Vec::new());
    for i in 0..n {
        let e = eng.entity_in("t").unwrap();
        // hint の外の row は 25 行に 1 つ (偶数番目と奇数番目に交互に入るので、 偶数番目の部分集合にも 5 つの値が全部入る)
        let g = if i % 89 == 88 {
            None
        } else if i % 25 == 24 {
            Some(BEYOND[(i as usize / 25) % BEYOND.len()])
        } else {
            Some(i % HINT)
        };
        let v = if i % 97 == 96 { None } else { Some(1 + (i.wrapping_mul(7919)) % 1000) };
        if let Some(g) = g {
            eng.tie(e, "t.g", g);
            eng.tie(e, "t.g_nh", g);
        }
        if let Some(v) = v {
            eng.tie(e, "t.v", v);
        }
        eids.push(e);
        rows.push((g, v));
    }
    eng.flush().unwrap();
    let (lo, hi) = eng.table_eid_range("t").unwrap();
    (eng, eids, rows, lo, hi)
}

struct Want {
    sum: Vec<(u32, u64)>,
    count: Vec<(u32, u32)>,
    min: Vec<(u32, u32)>,
    max: Vec<(u32, u32)>,
    avg: Vec<(u32, u64)>,
}

fn want(rows: &[&Row]) -> Want {
    let mut acc: BTreeMap<u32, (u64, u64, u32, u32)> = BTreeMap::new(); // (和, 件数, 最小, 最大)
    let mut count: BTreeMap<u32, u32> = BTreeMap::new();
    for &&(g, v) in rows {
        let Some(g) = g else { continue };
        *count.entry(g).or_insert(0) += 1;
        let Some(v) = v else { continue };
        let a = acc.entry(g).or_insert((0, 0, u32::MAX, 0));
        a.0 += v as u64;
        a.1 += 1;
        a.2 = a.2.min(v);
        a.3 = a.3.max(v);
    }
    Want {
        sum: acc.iter().map(|(&g, a)| (g, a.0)).collect(),
        count: count.into_iter().collect(),
        min: acc.iter().map(|(&g, a)| (g, a.2)).collect(),
        max: acc.iter().map(|(&g, a)| (g, a.3)).collect(),
        avg: acc.iter().map(|(&g, a)| (g, a.0 / a.1)).collect(),
    }
}

/// hint の道の答え (並びも見る) と、 hint 無しの道の答え (並べてから) が、 写しの答えと同じ。
fn check<T: PartialEq + std::fmt::Debug + Copy>(what: &str, hinted: Vec<(u32, T)>, plain: Vec<(u32, T)>, want: &[(u32, T)]) {
    assert!(hinted.windows(2).all(|w| w[0].0 < w[1].0), "{what}: hint の道の答えが group の昇順でない: {hinted:?}");
    assert_eq!(hinted, want, "{what}: hint の道");
    let mut plain = plain;
    plain.sort_by_key(|x| x.0);
    assert_eq!(plain, want, "{what}: hint 無しの道");
}

fn run(n: u32) {
    let path = tmp_path(&n.to_string());
    cleanup(&path);
    let _cleanup = Cleanup(path.clone());
    let (eng, eids, rows, lo, hi) = build(&path, n);
    // eid の飛び飛びの部分集合 (偶数番目) と全部
    let half: Vec<EntityId> = eids.iter().step_by(2).copied().collect();
    let half_rows: Vec<&Row> = rows.iter().step_by(2).collect();
    let all_rows: Vec<&Row> = rows.iter().collect();
    let (wa, wh) = (want(&all_rows), want(&half_rows));
    // 対照: hint の外の group が全部、 写しの答えに入っている (= この test は dense の道の外を踏む)
    for g in BEYOND {
        assert!(wa.sum.iter().any(|&(k, _)| k == g) && wh.count.iter().any(|&(k, _)| k == g), "group {g} が写しに無い");
    }
    assert!(wa.sum.iter().all(|&(_, s)| s > 0), "和が 0 の group を作らない前提");

    for (tag, set, w) in [("all", &eids, &wa), ("half", &half, &wh)] {
        let t = |f: &str| format!("n={n} {tag} {f}");
        check(&t("group_sum"), eng.group_sum("t.g", "t.v", set), eng.group_sum("t.g_nh", "t.v", set), &w.sum);
        check(&t("group_count"), eng.group_count("t.g", set), eng.group_count("t.g_nh", set), &w.count);
        check(&t("group_min"), eng.group_min("t.g", "t.v", set), eng.group_min("t.g_nh", "t.v", set), &w.min);
        check(&t("group_max"), eng.group_max("t.g", "t.v", set), eng.group_max("t.g_nh", "t.v", set), &w.max);
        check(&t("group_avg"), eng.group_avg("t.g", "t.v", set), eng.group_avg("t.g_nh", "t.v", set), &w.avg);
        check(&t("group_sum_eids_par"), eng.group_sum_eids_par("t.g", "t.v", set), eng.group_sum_eids_par("t.g_nh", "t.v", set), &w.sum);
        check(&t("group_min_eids_par"), eng.group_min_eids_par("t.g", "t.v", set), eng.group_min_eids_par("t.g_nh", "t.v", set), &w.min);
        check(&t("group_max_eids_par"), eng.group_max_eids_par("t.g", "t.v", set), eng.group_max_eids_par("t.g_nh", "t.v", set), &w.max);
    }
    let t = |f: &str| format!("n={n} range {f}");
    check(&t("group_sum_range"), eng.group_sum_range("t.g", "t.v", lo, hi), eng.group_sum_range("t.g_nh", "t.v", lo, hi), &wa.sum);
    check(&t("group_min_range"), eng.group_min_range("t.g", "t.v", lo, hi), eng.group_min_range("t.g_nh", "t.v", lo, hi), &wa.min);
    check(&t("group_max_range"), eng.group_max_range("t.g", "t.v", lo, hi), eng.group_max_range("t.g_nh", "t.v", lo, hi), &wa.max);
    check(&t("group_sum_range_par"), eng.group_sum_range_par("t.g", "t.v", lo, hi), eng.group_sum_range_par("t.g_nh", "t.v", lo, hi), &wa.sum);
    check(&t("group_min_range_par"), eng.group_min_range_par("t.g", "t.v", lo, hi), eng.group_min_range_par("t.g_nh", "t.v", lo, hi), &wa.min);
    check(&t("group_max_range_par"), eng.group_max_range_par("t.g", "t.v", lo, hi), eng.group_max_range_par("t.g_nh", "t.v", lo, hi), &wa.max);
}

/// 逐次の版 (並列の閾値の下)。 master では `group_sum` / `group_count` / `group_min` / `group_max` / `group_avg` が
/// panic し、 `_range` が hint の外の group を落としていた。
#[test]
fn group_values_beyond_hint_small() {
    run(1_000);
}

/// 並列の版 (閾値の上、 偶数番目だけの部分集合も閾値を超える)。 master では `_range_par` / `_eids_par` が
/// hint の外の group を落としていた。
#[test]
fn group_values_beyond_hint_parallel() {
    let n = (2 * PAR_THRESHOLD + 10_000) as u32;
    run(n);
}
