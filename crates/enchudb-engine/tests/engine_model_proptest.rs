//! model-based property test — engine を参照 oracle と差分照合する。
//!
//! ランダムな op 列（tie / untie / delete / reopen）を engine と、単純な
//! `BTreeMap<(entity, himo), value>` の oracle の両方に適用し、各 op の後で
//!   - `pull_raw(himo, v)` == oracle で値 v を持つ entity 集合（#95 の stale/dedup/verify）
//!   - `get(e, himo)`      == oracle の値
//! を厳密照合する。手書き test が数え落とす op 順序（値更新→削除→再 tie→reopen …）を
//! proptest が自動生成 + shrink するので、engine ロジックの網羅が一気に上がる。
//!
//! standalone（oplog なし・single-thread）で回す: 論理正当性の検証なので consumer
//! thread や配送タイミングは排除し、決定的に差分を取る。

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::eid_local;
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet, HashSet};

const HIMOS: &[&str] = &["a", "b", "c"];
const POOL: usize = 32; // 事前確保する entity 数
const VMAX: u32 = 10; // 値域 0..VMAX（小さくして bucket 衝突 = stale を誘発）
const PATH: &str = "/tmp/test_engine_model_proptest.db";

#[derive(Debug, Clone)]
enum Op {
    Tie { e: usize, h: usize, v: u32 },
    Untie { e: usize, h: usize },
    Delete { e: usize },
    Reopen,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        // tie を厚めに（値更新の stale を稼ぐ）
        4 => (0..POOL, 0..HIMOS.len(), 0..VMAX).prop_map(|(e, h, v)| Op::Tie { e, h, v }),
        1 => (0..POOL, 0..HIMOS.len()).prop_map(|(e, h)| Op::Untie { e, h }),
        1 => (0..POOL).prop_map(|e| Op::Delete { e }),
        1 => Just(Op::Reopen),
    ]
}

/// 参照 oracle。engine が「本当は」何を返すべきかを単純なデータ構造で持つ。
#[derive(Default)]
struct Model {
    vals: BTreeMap<(usize, usize), u32>, // (entity_idx, himo_idx) -> value
    dead: HashSet<usize>,                // delete された entity_idx（slot freed、以後 op skip）
}

fn fresh() {
    for suf in ["", ".oplog", ".lock", ".tables"] {
        let _ = std::fs::remove_file(format!("{PATH}{suf}"));
    }
}

fn build() -> (Engine, Vec<enchudb_oplog::EntityId>) {
    fresh();
    let mut eng = Engine::create_growable_with_capacity(PATH, POOL as u32 + 100).unwrap();
    for h in HIMOS {
        eng.define_himo(h, ValueType::Number, 0);
    }
    let eids: Vec<_> = (0..POOL).map(|_| eng.entity()).collect();
    (eng, eids)
}

/// engine 全状態を oracle と厳密照合。不一致は TestCaseError で shrink に回す。
fn verify(
    eng: &Engine,
    eids: &[enchudb_oplog::EntityId],
    model: &Model,
) -> Result<(), TestCaseError> {
    // (1) pull: 各 himo・各値で、engine の集合が oracle の集合と一致（stale/dup なし）。
    for (h, &hname) in HIMOS.iter().enumerate() {
        let mut want: BTreeMap<u32, BTreeSet<u32>> = BTreeMap::new();
        for (&(e, hh), &v) in &model.vals {
            if hh == h {
                want.entry(v).or_default().insert(eid_local(eids[e]));
            }
        }
        for v in 0..VMAX {
            let got: BTreeSet<u32> = eng
                .pull_raw(hname, v)
                .iter()
                .map(|&x| eid_local(x))
                .collect();
            let exp = want.get(&v).cloned().unwrap_or_default();
            prop_assert_eq!(
                &got,
                &exp,
                "pull_raw({}, {}) mismatch: got {:?} want {:?}",
                hname,
                v,
                got,
                exp
            );
        }
    }
    // (2) get: 各 entity・各 himo で Column 直読みが oracle と一致。
    for (e, &eid) in eids.iter().enumerate() {
        let dead = model.dead.contains(&e);
        for (h, &hname) in HIMOS.iter().enumerate() {
            let got = eng.get(eid, hname);
            let exp = if dead {
                None
            } else {
                model.vals.get(&(e, h)).copied()
            };
            prop_assert_eq!(got, exp, "get(e{}, {}) mismatch", e, hname);
        }
    }
    Ok(())
}

/// 2-cond AND query（minimum-slice pivot + value_eq filter）を oracle 交差と照合。
/// pull/get とは別の engine code path なので独立に検証する。case 末尾で 1 回。
fn verify_queries(
    eng: &Engine,
    eids: &[enchudb_oplog::EntityId],
    model: &Model,
) -> Result<(), TestCaseError> {
    for ha in 0..HIMOS.len() {
        for hb in (ha + 1)..HIMOS.len() {
            for va in 0..VMAX {
                for vb in 0..VMAX {
                    let exp: BTreeSet<u32> = (0..POOL)
                        .filter(|&e| {
                            !model.dead.contains(&e)
                                && model.vals.get(&(e, ha)) == Some(&va)
                                && model.vals.get(&(e, hb)) == Some(&vb)
                        })
                        .map(|e| eid_local(eids[e]))
                        .collect();
                    let got: BTreeSet<u32> = eng
                        .query(&[(HIMOS[ha], va), (HIMOS[hb], vb)])
                        .iter()
                        .map(|&x| eid_local(x))
                        .collect();
                    prop_assert_eq!(
                        &got,
                        &exp,
                        "query({}={} AND {}={}) mismatch",
                        HIMOS[ha],
                        va,
                        HIMOS[hb],
                        vb
                    );
                }
            }
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 200, ..ProptestConfig::default() })]

    /// ランダム op 列を engine + oracle に流し、毎 op 後に厳密一致を要求。
    #[test]
    fn engine_matches_oracle(ops in prop::collection::vec(op_strategy(), 1..40)) {
        let (mut eng, eids) = build();
        let mut model = Model::default();

        for op in ops {
            match op {
                Op::Tie { e, h, v } => {
                    if model.dead.contains(&e) { continue; } // freed slot は触らない
                    eng.tie(eids[e], HIMOS[h], v);
                    model.vals.insert((e, h), v);
                }
                Op::Untie { e, h } => {
                    if model.dead.contains(&e) { continue; }
                    eng.untie(eids[e], HIMOS[h]);
                    model.vals.remove(&(e, h));
                }
                Op::Delete { e } => {
                    if model.dead.contains(&e) { continue; }
                    eng.delete(eids[e]);
                    for h in 0..HIMOS.len() {
                        model.vals.remove(&(e, h));
                    }
                    model.dead.insert(e);
                }
                Op::Reopen => {
                    // flush → drop → open。Cylinder が column から lazy rebuild される
                    // 経路を挟み、永続化 + 再構築の正当性も同時に検証する。
                    eng.flush().unwrap();
                    drop(eng);
                    eng = Engine::open_standalone(PATH).unwrap();
                }
            }
            verify(&eng, &eids, &model)?;
        }
        // 末尾で多条件 AND path も oracle 交差と照合。
        verify_queries(&eng, &eids, &model)?;
        fresh();
    }
}
