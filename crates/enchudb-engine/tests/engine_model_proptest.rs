//! model-based property test — engine を参照 oracle と差分照合する。
//!
//! ランダムな op 列（set / untie / delete / reopen）を engine と、単純な
//! `BTreeMap<(entity, himo), value>` の oracle の両方に適用し、各 op の後で
//!   - `pull_raw` == oracle でその値を持つ entity 集合（stale/dedup/verify）
//!   - `get` / `get_text` == oracle の値
//! を厳密照合する。手書き test が数え落とす op 順序（値更新→削除→再 tie→reopen …）を
//! proptest が自動生成 + shrink するので、engine ロジックの網羅が一気に上がる。
//!
//! **himo 型を跨いで網羅する**（#95 の lock-free read は型非依存だが、Number と Tag で
//! read path が違う — Number は Column 直値、Tag は Vocabulary intern した vid）:
//!   - Number himo: `tie` / `get` / `pull_raw(value)`
//!   - Tag himo:    `tie_text` / `get_text` / `pull_raw(vocab_id(s))`（vid の stale verify も）
//!
//! standalone（oplog なし・single-thread）で回す: 論理正当性の検証なので consumer
//! thread や配送タイミングは排除し、決定的に差分を取る。

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::eid_local;
use proptest::prelude::*;
use std::collections::{BTreeMap, BTreeSet, HashSet};

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Num,
    Tag,
}

// (name, kind)。Number 2 本 + Tag 2 本を跨いで検証する。
const HIMOS: &[(&str, Kind)] = &[
    ("a", Kind::Num),
    ("b", Kind::Num),
    ("t", Kind::Tag),
    ("u", Kind::Tag),
];
const POOL: usize = 24; // 事前確保する entity 数
const VMAX: u32 = 8; // Number の値域 0..VMAX（小さくして bucket 衝突 = stale を誘発）
const STRINGS: &[&str] = &["p", "q", "r", "s"]; // Tag の小語彙（vid 再利用 = stale を誘発）
const PATH: &str = "/tmp/test_engine_model_proptest.db";

/// oracle が持つ値。Number は u32、Tag は文字列。
#[derive(Debug, Clone, PartialEq)]
enum Val {
    Num(u32),
    Txt(&'static str),
}

#[derive(Debug, Clone)]
enum Op {
    // code を himo の型で解釈（Num: code%VMAX、Tag: STRINGS[code%len]）。
    Set { e: usize, h: usize, code: u32 },
    Untie { e: usize, h: usize },
    Delete { e: usize },
    Reopen,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        // set を厚めに（値更新の stale を稼ぐ）
        4 => (0..POOL, 0..HIMOS.len(), 0..8u32).prop_map(|(e, h, code)| Op::Set { e, h, code }),
        1 => (0..POOL, 0..HIMOS.len()).prop_map(|(e, h)| Op::Untie { e, h }),
        1 => (0..POOL).prop_map(|e| Op::Delete { e }),
        1 => Just(Op::Reopen),
    ]
}

/// code を himo 型の値に写す。
fn decode(h: usize, code: u32) -> Val {
    match HIMOS[h].1 {
        Kind::Num => Val::Num(code % VMAX),
        Kind::Tag => Val::Txt(STRINGS[(code as usize) % STRINGS.len()]),
    }
}

/// 参照 oracle。engine が「本当は」何を返すべきかを単純なデータ構造で持つ。
#[derive(Default)]
struct Model {
    vals: BTreeMap<(usize, usize), Val>, // (entity_idx, himo_idx) -> value
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
    for &(name, kind) in HIMOS {
        let vt = match kind {
            Kind::Num => ValueType::Number,
            Kind::Tag => ValueType::Tag,
        };
        eng.define_himo(name, vt, 0);
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
    for (h, &(hname, kind)) in HIMOS.iter().enumerate() {
        match kind {
            Kind::Num => {
                for v in 0..VMAX {
                    let want: BTreeSet<u32> = (0..POOL)
                        .filter(|&e| model.vals.get(&(e, h)) == Some(&Val::Num(v)))
                        .map(|e| eid_local(eids[e]))
                        .collect();
                    let got: BTreeSet<u32> =
                        eng.pull_raw(hname, v).iter().map(|&x| eid_local(x)).collect();
                    prop_assert_eq!(&got, &want, "pull_raw({}, {}) mismatch", hname, v);
                }
            }
            Kind::Tag => {
                for &s in STRINGS {
                    let want: BTreeSet<u32> = (0..POOL)
                        .filter(|&e| model.vals.get(&(e, h)) == Some(&Val::Txt(s)))
                        .map(|e| eid_local(eids[e]))
                        .collect();
                    // vid が無い（一度も intern されてない）なら want も空のはず。
                    let got: BTreeSet<u32> = match eng.vocab_id(s) {
                        Some(vid) => eng.pull_raw(hname, vid).iter().map(|&x| eid_local(x)).collect(),
                        None => BTreeSet::new(),
                    };
                    prop_assert_eq!(&got, &want, "pull_raw({}, text {:?}) mismatch", hname, s);
                }
            }
        }
    }
    // (2) get / get_text: 各 entity・各 himo で直読みが oracle と一致。
    for (e, &eid) in eids.iter().enumerate() {
        let dead = model.dead.contains(&e);
        for (h, &(hname, kind)) in HIMOS.iter().enumerate() {
            let exp = if dead { None } else { model.vals.get(&(e, h)) };
            match kind {
                Kind::Num => {
                    let got = eng.get(eid, hname);
                    let want = exp.map(|v| match v {
                        Val::Num(n) => *n,
                        Val::Txt(_) => unreachable!(),
                    });
                    prop_assert_eq!(got, want, "get(e{}, {}) mismatch", e, hname);
                }
                Kind::Tag => {
                    let got = eng.get_text(eid, hname).map(|b| b.to_vec());
                    let want = exp.map(|v| match v {
                        Val::Txt(s) => s.as_bytes().to_vec(),
                        Val::Num(_) => unreachable!(),
                    });
                    prop_assert_eq!(got, want, "get_text(e{}, {}) mismatch", e, hname);
                }
            }
        }
    }
    Ok(())
}

/// 2-cond AND query（minimum-slice pivot + value_eq filter）を oracle 交差と照合。
/// Number himo 同士で検証（query は u32 keyed）。case 末尾で 1 回。
fn verify_queries(
    eng: &Engine,
    eids: &[enchudb_oplog::EntityId],
    model: &Model,
) -> Result<(), TestCaseError> {
    let nums: Vec<usize> = (0..HIMOS.len())
        .filter(|&h| HIMOS[h].1 == Kind::Num)
        .collect();
    for i in 0..nums.len() {
        for j in (i + 1)..nums.len() {
            let (ha, hb) = (nums[i], nums[j]);
            for va in 0..VMAX {
                for vb in 0..VMAX {
                    let exp: BTreeSet<u32> = (0..POOL)
                        .filter(|&e| {
                            !model.dead.contains(&e)
                                && model.vals.get(&(e, ha)) == Some(&Val::Num(va))
                                && model.vals.get(&(e, hb)) == Some(&Val::Num(vb))
                        })
                        .map(|e| eid_local(eids[e]))
                        .collect();
                    let got: BTreeSet<u32> = eng
                        .query(&[(HIMOS[ha].0, va), (HIMOS[hb].0, vb)])
                        .iter()
                        .map(|&x| eid_local(x))
                        .collect();
                    prop_assert_eq!(
                        &got,
                        &exp,
                        "query({}={} AND {}={}) mismatch",
                        HIMOS[ha].0,
                        va,
                        HIMOS[hb].0,
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
                Op::Set { e, h, code } => {
                    if model.dead.contains(&e) { continue; } // freed slot は触らない
                    let val = decode(h, code);
                    match &val {
                        Val::Num(n) => eng.tie(eids[e], HIMOS[h].0, *n),
                        Val::Txt(s) => eng.tie_text(eids[e], HIMOS[h].0, s),
                    }
                    model.vals.insert((e, h), val);
                }
                Op::Untie { e, h } => {
                    if model.dead.contains(&e) { continue; }
                    eng.untie(eids[e], HIMOS[h].0);
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
                    // 経路を挟み、永続化 + 再構築の正当性も同時に検証する（Vocabulary 込み）。
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
