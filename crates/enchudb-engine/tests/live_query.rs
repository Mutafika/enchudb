//! live query (クエリ購読): poll の差分を積分した集合が、 **毎回ゼロから全件走査した
//! 結果** と常に一致すること。
//!
//! oracle は Column の全件走査 (`get` の値を条件に当てるだけ) で、 購読側の bitset /
//! route / barrier を一切通らない。 write は全経路 (build phase の `tie` / `tie_to` /
//! `untie` / `delete` / remote apply / async consumer / Tag 文字列) を混ぜる。
//!
//! 取りこぼし検出の実測: `Engine::live_set` / `live_remove` のどちらか片方の `touch` を
//! 外すだけで、 この file の 4 本中 3 本が落ちる。 登録 barrier の検出はここでは**できない**
//! (外しても 10 run 中 0 回) — その gate は `loom_live_subscribe.rs`。

use enchudb_engine::{Engine, GrowableOptions, LiveDelta, LivePred, LiveQuery, ValueType};
use std::collections::BTreeSet;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/tmp/live_query_{}_{}_{}.enchu", tag, std::process::id(), nanos)
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    let _ = std::fs::remove_file(path);
    for ext in ["lock", "oplog", "schema", "tables"] {
        let _ = std::fs::remove_file(format!("{}.{}", path, ext));
    }
}

/// 決定論の擬似乱数 (xorshift64*)。
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// 呼び手側の積分 (removed → added の順に当てる)。
fn integrate(set: &mut BTreeSet<u64>, d: LiveDelta) {
    for e in d.removed {
        assert!(set.remove(&e), "removed に未報告の eid {e}");
    }
    for e in d.added {
        assert!(set.insert(e), "added に報告済みの eid {e}");
    }
}

/// 1 entity を Column から読んで条件を当てる oracle。
type Oracle = Box<dyn Fn(&Engine, u64) -> bool>;

struct Sub {
    name: &'static str,
    q: LiveQuery,
    seen: BTreeSet<u64>,
    /// oracle: 1 entity を Column から読んで条件を当てる。
    oracle: Oracle,
}

fn full_scan(eng: &Engine, max_eid: u64, f: &dyn Fn(&Engine, u64) -> bool) -> BTreeSet<u64> {
    (0..max_eid).filter(|&e| f(eng, e)).collect()
}

fn check(eng: &Engine, subs: &mut [Sub], max_eid: u64, step: usize) {
    for s in subs.iter_mut() {
        integrate(&mut s.seen, s.q.poll());
        let want = full_scan(eng, max_eid, &*s.oracle);
        assert_eq!(s.seen, want, "[{}] step {step}: 積分結果 != 全件走査", s.name);
        assert_eq!(s.q.count(), want.len(), "[{}] step {step}: count", s.name);
    }
}

#[test]
fn all_write_paths_match_full_scan() {
    let path = tmp_path("paths");
    cleanup(&path);
    let mut eng = Engine::create_growable_opts(&path, GrowableOptions::default()).unwrap();
    eng.define_himo("age", ValueType::Number, 100);
    eng.define_himo("dept", ValueType::Number, 10);
    eng.define_himo("city", ValueType::Tag, 0);
    let age = eng.himo_id("age").unwrap() as u16;
    let dept = eng.himo_id("dept").unwrap() as u16;
    let city = eng.himo_id("city").unwrap() as u16;

    const N: u64 = 300;
    let mut eids = Vec::new();
    for _ in 0..N {
        eids.push(eng.entity().unwrap());
    }
    let mut rng = Rng(0x5eed_1234_abcd_0001);
    // build phase の半分を購読前に書く (初期集合 = seed の検証)
    for &e in &eids[..(N / 2) as usize] {
        eng.tie(e, "age", rng.below(40) as u32);
        eng.tie(e, "dept", rng.below(5) as u32);
    }

    let get = |eng: &Engine, e: u64, h: &str| eng.get(e, h);
    let mut subs = vec![
        Sub {
            name: "age=30",
            q: eng.subscribe(vec![LivePred::Eq { himo_id: age, value: 30 }]).unwrap(),
            seen: BTreeSet::new(),
            oracle: Box::new(move |eng, e| get(eng, e, "age") == Some(30)),
        },
        Sub {
            name: "20<=age<=29 AND dept=2",
            q: eng
                .subscribe(vec![
                    LivePred::Range { himo_id: age, lo: 20, hi: 29 },
                    LivePred::Eq { himo_id: dept, value: 2 },
                ])
                .unwrap(),
            seen: BTreeSet::new(),
            oracle: Box::new(move |eng, e| {
                matches!(get(eng, e, "age"), Some(a) if (20..=29).contains(&a))
                    && get(eng, e, "dept") == Some(2)
            }),
        },
        Sub {
            // 登録時点で vocab に無い文字列
            name: "city=東京",
            q: eng
                .subscribe(vec![LivePred::EqText { himo_id: city, text: "東京".into() }])
                .unwrap(),
            seen: BTreeSet::new(),
            oracle: Box::new(move |eng, e| eng.get_text(e, "city") == Some(&b"\xe6\x9d\xb1\xe4\xba\xac"[..])),
        },
        Sub {
            name: "dept IN {1,3} AND city present",
            q: eng
                .subscribe(vec![
                    LivePred::In { himo_id: dept, values: vec![3, 1, 3] },
                    LivePred::Present { himo_id: city },
                ])
                .unwrap(),
            seen: BTreeSet::new(),
            oracle: Box::new(move |eng, e| {
                matches!(get(eng, e, "dept"), Some(1 | 3)) && get(eng, e, "city").is_some()
            }),
        },
    ];
    check(&eng, &mut subs, N, 0);

    // build phase (&mut) の残り
    for &e in &eids[(N / 2) as usize..] {
        eng.tie(e, "age", rng.below(40) as u32);
    }
    check(&eng, &mut subs, N, 1);

    // concurrent 化して &self の全経路を混ぜる
    let eng = Engine::concurrentize(eng);
    let cities = ["東京", "大阪", "福岡"];
    let mut hlc_wall = 1u64;
    let mut max_eid = N;
    for step in 2..2000 {
        let e = eids[rng.below(eids.len() as u64) as usize];
        match rng.below(8) {
            0 => {
                eng.tie_to(e, "age", rng.below(40) as u32);
            }
            1 => {
                eng.tie_to(e, "dept", rng.below(5) as u32);
            }
            2 => {
                eng.tie_text_to(e, "city", cities[rng.below(3) as usize]);
            }
            3 => {
                eng.untie(e, if rng.below(2) == 0 { "age" } else { "city" });
            }
            4 => {
                hlc_wall += 1;
                let hlc = enchudb_oplog::Hlc { wall: hlc_wall, logical: 0, peer: 9 };
                eng.remote_tie_apply(e, age, rng.below(40) as u32, hlc);
            }
            5 => {
                eng.tie_async(e, "dept", rng.below(5) as u32);
                while eng.pending_writes() > 0 {
                    std::thread::yield_now();
                }
            }
            6 => {
                // 削除 → 新 entity に条件を満たす値をぶら下げる (slot 再利用は
                // 容量上限でしか起きない — reused_slot_* が別に見る)
                eng.delete(e);
                let n = eng.entity().unwrap();
                eng.tie_to(n, "age", 30);
                eng.tie_to(n, "dept", 2);
                let i = eids.iter().position(|&x| x == e).unwrap();
                eids[i] = n;
                max_eid = max_eid.max(enchudb_oplog::eid_local(n) as u64 + 1);
            }
            _ => {
                eng.delete(e);
                let n = eng.entity().unwrap();
                let i = eids.iter().position(|&x| x == e).unwrap();
                eids[i] = n;
                max_eid = max_eid.max(enchudb_oplog::eid_local(n) as u64 + 1);
            }
        }
        if step % 7 == 0 {
            check(&eng, &mut subs, max_eid, step);
        }
    }
    check(&eng, &mut subs, max_eid, usize::MAX);
    for s in &subs {
        assert!(!s.seen.is_empty(), "[{}] 一度も当たらない条件は試験になっていない", s.name);
    }
    drop(subs);
    drop(eng);
    cleanup(&path);
}

/// 削除された slot が poll の間に再利用されて再び条件を満たしたら、 removed と added の
/// 両方に出る (呼び手が別 entity として読み直せる)。
#[test]
fn reused_slot_is_reported_as_leave_and_reenter() {
    let path = tmp_path("reuse");
    cleanup(&path);
    // slot の再利用は max_entities 到達後だけ (普段は monotonic 払い出し) — 容量 1 で起こす
    let mut eng = Engine::create_with_capacity(&path, 1).unwrap();
    eng.define_himo("age", ValueType::Number, 100);
    let age = eng.himo_id("age").unwrap() as u16;
    let e = eng.entity().unwrap();
    eng.tie(e, "age", 30);
    let q = eng.subscribe(vec![LivePred::Eq { himo_id: age, value: 30 }]).unwrap();
    assert_eq!(q.poll().added, vec![e]);

    eng.delete(e);
    let n = eng.entity().unwrap();
    assert_eq!(n, e, "前提: 容量 1 なら削除した slot が再利用される");
    eng.tie(n, "age", 30);
    let d = q.poll();
    assert_eq!(d.removed, vec![e]);
    assert_eq!(d.added, vec![e]);
    drop(q);
    drop(eng);
    cleanup(&path);
}

/// 登録と並行する書き込みを取りこぼさない (barrier)。 writer を回したまま購読を
/// 何度も張り直し、 writer 停止後に積分結果 == 全件走査を確かめる。
///
/// 並行 test は interleaving の一部しか踏まないので、 これは barrier の **根拠ではない**
/// (根拠は `enchudb_engine::live` module doc の lock 順序)。 実測で barrier を外しても
/// 10 run 中 0 回しか落ちない — barrier の gate は `loom_live_subscribe.rs`。 ここが
/// 見ているのは 「書き込み中の購読 + poll の積分」 の end-to-end の形。
#[test]
fn subscribe_while_writing_loses_nothing() {
    let path = tmp_path("race");
    cleanup(&path);
    let mut eng = Engine::create_growable_opts(&path, GrowableOptions::default()).unwrap();
    eng.define_himo("age", ValueType::Number, 100);
    let age = eng.himo_id("age").unwrap() as u16;
    let eids: Vec<u64> = (0..2000).map(|_| eng.entity().unwrap()).collect();
    let eng = Engine::concurrentize(eng);

    for round in 0..20 {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writers: Vec<_> = (0..4)
            .map(|t| {
                let eng = eng.clone();
                let eids = eids.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let mut rng = Rng(0x9e37_79b9_7f4a_7c15 ^ ((round * 4 + t) as u64 + 1));
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        let e = eids[rng.below(eids.len() as u64) as usize];
                        if rng.below(4) == 0 {
                            eng.untie(e, "age");
                        } else {
                            eng.tie_to(e, "age", rng.below(4) as u32);
                        }
                    }
                })
            })
            .collect();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let q = eng.subscribe(vec![LivePred::Eq { himo_id: age, value: 1 }]).unwrap();
        let mut seen = BTreeSet::new();
        for _ in 0..5 {
            integrate(&mut seen, q.poll());
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for w in writers {
            w.join().unwrap();
        }
        integrate(&mut seen, q.poll());
        let want: BTreeSet<u64> = eids.iter().copied().filter(|&e| eng.get(e, "age") == Some(1)).collect();
        assert_eq!(seen, want, "round {round}: 登録と並行した書き込みの取りこぼし");
    }
    drop(eng);
    cleanup(&path);
}

#[test]
fn rejects_empty_and_unknown_himo() {
    let path = tmp_path("reject");
    cleanup(&path);
    let mut eng = Engine::create_growable_opts(&path, GrowableOptions::default()).unwrap();
    eng.define_himo("age", ValueType::Number, 100);
    assert!(eng.subscribe(vec![]).is_err());
    assert!(eng.subscribe(vec![LivePred::Present { himo_id: 999 }]).is_err());
    drop(eng);
    cleanup(&path);
}
