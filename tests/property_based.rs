//! Property-based tests for EnchuDB v27.
//!
//! ランダムな操作列を shadow model (HashMap/HashSet) と並走させ、
//! db の観測結果が shadow と一致することを検証する。
//!
//! 実行: `cargo test --features v27 --test property_based`

#![cfg(feature = "v27")]

use enchudb::{Engine, HimoType};
use proptest::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn fresh_db_path(tag: &str) -> String {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = format!(
        "/tmp/enchudb_prop_{}_{}_{}.db",
        std::process::id(),
        tag,
        n
    );
    let _ = std::fs::remove_file(&path);
    path
}

fn make_db(path: &str) -> Engine {
    let mut db = Engine::create_standalone(path).unwrap();
    db.define_himo("a", HimoType::Value, 10);
    db.define_himo("b", HimoType::Value, 10);
    db.define_himo("c", HimoType::Value, 10);
    // 100 個 entity を確保。eid 0..100 が allocate される。
    for _ in 0..100 {
        let _ = db.entity();
    }
    db
}

fn himo_name(idx: usize) -> &'static str {
    match idx {
        0 => "a",
        1 => "b",
        _ => "c",
    }
}

#[derive(Debug, Clone)]
enum Op {
    Tie(u32, usize, u32),  // (eid, himo_idx, value)
    Untie(u32, usize),
    Delete(u32),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0u32..100, 0usize..3, 0u32..10).prop_map(|(e, h, v)| Op::Tie(e, h, v)),
        2 => (0u32..100, 0usize..3).prop_map(|(e, h)| Op::Untie(e, h)),
        1 => (0u32..100).prop_map(Op::Delete),
    ]
}

/// 全 himo の (eid, himo_idx) → value を shadow で持つ。
type Shadow = HashMap<(u32, usize), u32>;

fn apply_ops(
    db: &mut Engine,
    ops: &[Op],
) -> (Shadow, HashSet<u64>) {
    let mut shadow: Shadow = HashMap::new();
    let mut deleted: HashSet<u64> = HashSet::new();

    for op in ops {
        match *op {
            Op::Tie(e, h, v) => {
                if !deleted.contains(&(e as u64)) {
                    db.tie(e as u64, himo_name(h), v);
                    shadow.insert((e, h), v);
                }
            }
            Op::Untie(e, h) => {
                if !deleted.contains(&(e as u64)) {
                    db.untie(e as u64, himo_name(h));
                    shadow.remove(&(e, h));
                }
            }
            Op::Delete(e) => {
                if !deleted.contains(&(e as u64)) {
                    db.delete(e as u64);
                    deleted.insert(e as u64);
                    // 全 himo から消える
                    for h in 0..3 {
                        shadow.remove(&(e, h));
                    }
                }
            }
        }
    }

    (shadow, deleted)
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 128,
        .. ProptestConfig::default()
    })]

    /// tie → get の往復で値が一致する。
    #[test]
    fn tie_get_roundtrip(ops in prop::collection::vec(op_strategy(), 0..200)) {
        let path = fresh_db_path("get");
        let mut db = make_db(&path);
        let (shadow, deleted) = apply_ops(&mut db, &ops);

        // shadow にある値はすべて db から read できる
        for (&(e, h), &v) in &shadow {
            prop_assert_eq!(db.get(e as u64, himo_name(h)), Some(v),
                "expected get(eid={}, himo={}) = Some({})", e, himo_name(h), v);
        }

        // deleted な eid は全 himo で None
        for &e in &deleted {
            for h in 0..3 {
                prop_assert_eq!(db.get(e as u64, himo_name(h)), None,
                    "expected get(deleted eid={}, himo={}) = None", e, himo_name(h));
            }
        }

        let _ = std::fs::remove_file(&path);
    }

    /// pull_raw が shadow から抽出した entity 集合と一致する。
    #[test]
    fn pull_raw_matches_shadow(ops in prop::collection::vec(op_strategy(), 0..200)) {
        let path = fresh_db_path("pull");
        let mut db = make_db(&path);
        let (shadow, _deleted) = apply_ops(&mut db, &ops);

        // 各 (himo, value) について pull_raw 結果 == shadow から抽出した eid 集合
        for h in 0..3 {
            for v in 0..10u32 {
                let expected: HashSet<u64> = shadow.iter()
                    .filter(|&(&(_, hh), &vv)| hh == h && vv == v)
                    .map(|(&(e, _), _)| e as u64)
                    .collect();

                let actual: HashSet<u64> = db.pull_raw(himo_name(h), v).into_iter().collect();

                prop_assert_eq!(&actual, &expected,
                    "pull_raw({}, {}) mismatch", himo_name(h), v);
            }
        }

        let _ = std::fs::remove_file(&path);
    }

    /// untie 後、その eid は pull_raw に出ない。
    #[test]
    fn untie_removes_from_pull(
        ties in prop::collection::vec((0u32..50, 0usize..3, 0u32..5), 1..100),
        untie_indices in prop::collection::vec(0usize..200, 0..50),
    ) {
        let path = fresh_db_path("untie");
        let mut db = make_db(&path);

        // tie だけ適用
        let mut shadow: Shadow = HashMap::new();
        for &(e, h, v) in &ties {
            db.tie(e as u64, himo_name(h), v);
            shadow.insert((e, h), v);
        }

        // 一部の tie を untie
        let mut untied: HashSet<(u32, usize)> = HashSet::new();
        for &idx in &untie_indices {
            if let Some(&(e, h, _)) = ties.get(idx % ties.len().max(1)) {
                db.untie(e as u64, himo_name(h));
                untied.insert((e, h));
                shadow.remove(&(e, h));
            }
        }

        // untie した (eid, himo) は、その元の値で pull しても出ない
        for &(e, h) in &untied {
            // 同じ (eid, himo) を後で再 tie してれば shadow に入ってる
            if shadow.contains_key(&(e, h)) {
                continue;
            }
            for v in 0..5u32 {
                let result: Vec<u64> = db.pull_raw(himo_name(h), v);
                prop_assert!(!result.contains(&(e as u64)),
                    "untied eid={} appeared in pull_raw({}, {})", e, himo_name(h), v);
            }
        }

        let _ = std::fs::remove_file(&path);
    }

    /// delete 後、その eid はどの紐の pull_raw にも出ない。
    #[test]
    fn delete_removes_from_all_himos(
        ties in prop::collection::vec((0u32..50, 0usize..3, 0u32..5), 1..100),
        delete_eids in prop::collection::vec(0u32..50, 0..30),
    ) {
        let path = fresh_db_path("del");
        let mut db = make_db(&path);

        for &(e, h, v) in &ties {
            db.tie(e as u64, himo_name(h), v);
        }

        let mut deleted: HashSet<u64> = HashSet::new();
        for &e in &delete_eids {
            if !deleted.contains(&(e as u64)) {
                db.delete(e as u64);
                deleted.insert(e as u64);
            }
        }

        // 削除した eid はどの (himo, value) でも pull_raw に現れない
        for &e in &deleted {
            for h in 0..3 {
                for v in 0..5u32 {
                    let result: Vec<u64> = db.pull_raw(himo_name(h), v);
                    prop_assert!(!result.contains(&(e as u64)),
                        "deleted eid={} appeared in pull_raw({}, {})",
                        e, himo_name(h), v);
                }
            }
        }

        let _ = std::fs::remove_file(&path);
    }

    /// query(&[(a,va),(b,vb)]) == pull_raw(a,va) ∩ pull_raw(b,vb)
    #[test]
    fn query_matches_intersection(
        ops in prop::collection::vec(op_strategy(), 0..150),
        a_val in 0u32..10,
        b_val in 0u32..10,
    ) {
        let path = fresh_db_path("query");
        let mut db = make_db(&path);
        apply_ops(&mut db, &ops);

        let pa: HashSet<u64> = db.pull_raw("a", a_val).into_iter().collect();
        let pb: HashSet<u64> = db.pull_raw("b", b_val).into_iter().collect();
        let intersection: HashSet<u64> = pa.intersection(&pb).copied().collect();

        let q: HashSet<u64> = db
            .query(&[("a", a_val), ("b", b_val)])
            .into_iter()
            .collect();

        prop_assert_eq!(&q, &intersection,
            "query[a={}, b={}] != pull_raw(a) ∩ pull_raw(b)", a_val, b_val);

        let _ = std::fs::remove_file(&path);
    }
}
