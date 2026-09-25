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
        integrate(&mut s.seen, s.q.poll(eng));
        let want = full_scan(eng, max_eid, &*s.oracle);
        assert_eq!(s.seen, want, "[{}] step {step}: 積分結果 != 全件走査", s.name);
        assert_eq!(s.q.count(eng), want.len(), "[{}] step {step}: count", s.name);
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
    assert_eq!(q.poll(&eng).added, vec![e]);

    eng.delete(e);
    let n = eng.entity().unwrap();
    assert_eq!(n, e, "前提: 容量 1 なら削除した slot が再利用される");
    eng.tie(n, "age", 30);
    let d = q.poll(&eng);
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
            integrate(&mut seen, q.poll(&eng));
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for w in writers {
            w.join().unwrap();
        }
        integrate(&mut seen, q.poll(&eng));
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

/// ref をたどる購読 (user → company → city) を、 hub の値の往復と ref の付け替えを並行で
/// 書きながら poll し続け、 writer 停止後の積分 == 手でたどった結果を確かめる。
///
/// 「真偽の記録値を不明に戻す」 ルール (live.rs module doc) の **決定論の gate は
/// `live::tests::concurrent_flip_and_back_is_not_swallowed`**。 これは実スレッドでの
/// end-to-end の形を見る。 ルールを外すとこの test も落ちる (実測 release で 10 run 中 10 回。
/// 行って戻る書き込みを 3 thread で回しているので、 実スレッド上でも競合は十分起きる)。
#[test]
fn via_subscription_under_concurrent_flips() {
    let path = tmp_path("via_race");
    cleanup(&path);
    let mut eng = Engine::create_growable_opts(&path, GrowableOptions::default()).unwrap();
    eng.define_himo("company", ValueType::Ref, 0);
    eng.define_himo("city", ValueType::Number, 4);
    let company = eng.himo_id("company").unwrap() as u16;
    let city = eng.himo_id("city").unwrap() as u16;
    let hubs: Vec<u64> = (0..8).map(|_| eng.entity().unwrap()).collect();
    let users: Vec<u64> = (0..400).map(|_| eng.entity().unwrap()).collect();
    for (i, &h) in hubs.iter().enumerate() {
        eng.tie(h, "city", (i % 2) as u32);
    }
    for (i, &u) in users.iter().enumerate() {
        eng.tie(u, "company", enchudb_oplog::eid_local(hubs[i % hubs.len()]));
    }
    let eng = Engine::concurrentize(eng);
    let oracle = |eng: &Engine| -> BTreeSet<u64> {
        users
            .iter()
            .copied()
            .filter(|&u| {
                let Some(c) = eng.get(u, "company") else { return false };
                eng.get(enchudb_oplog::make_eid(eng.peer_id(), c), "city") == Some(1)
            })
            .collect()
    };
    let pred = || vec![LivePred::Via { path: vec![company], pred: Box::new(LivePred::Eq { himo_id: city, value: 1 }) }];

    for round in 0..20u64 {
        let fast = eng.subscribe(pred()).unwrap();
        let naive = eng.subscribe_expand_always(pred()).unwrap();
        let (mut seen_fast, mut seen_naive) = (BTreeSet::new(), BTreeSet::new());
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writers: Vec<_> = (0..3u64)
            .map(|t| {
                let eng = eng.clone();
                let (hubs, users) = (hubs.clone(), users.clone());
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let mut rng = Rng(0xabcd_ef01_2345_6789 ^ (round * 3 + t + 1));
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        let h = hubs[rng.below(hubs.len() as u64) as usize];
                        if rng.below(3) == 0 {
                            let u = users[rng.below(users.len() as u64) as usize];
                            eng.tie_to(u, "company", enchudb_oplog::eid_local(h));
                        } else {
                            // 行って戻る: 1 → 0 → 1 を素早く
                            eng.tie_to(h, "city", 0);
                            eng.tie_to(h, "city", 1);
                            if rng.below(2) == 0 {
                                eng.tie_to(h, "city", 0);
                            }
                        }
                    }
                })
            })
            .collect();
        let t0 = std::time::Instant::now();
        while t0.elapsed() < std::time::Duration::from_millis(40) {
            integrate(&mut seen_fast, fast.poll(&eng));
            integrate(&mut seen_naive, naive.poll(&eng));
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for w in writers {
            w.join().unwrap();
        }
        integrate(&mut seen_fast, fast.poll(&eng));
        integrate(&mut seen_naive, naive.poll(&eng));
        let want = oracle(&eng);
        assert_eq!(seen_naive, want, "round {round}: 常に展開 != 手でたどった結果");
        assert_eq!(seen_fast, want, "round {round}: 既定 (真偽が変わった時だけ展開) != 手でたどった結果");
    }
    drop(eng);
    cleanup(&path);
}

/// 新しい種類の購読 (Or / 範囲の穴 / 集計 / 上位 k 件、 ref の先を含む) を **書き込みと並行して
/// 登録し**、 書き込み 4 thread + 束の poll / 個別 poll / count を並行で回した後、 writer 停止後の
/// 積分が手で数えた結果と一致すること。 lock 順 (Or の購読は family の lock を離してから自分を取る)
/// を破ると deadlock で止まる形でもある。
///
/// 検出の実測: 印付けで dirty bit を印より先に立てる (+ 間に `yield_now`) 変異で 5 run 中 5 回落ちる。
/// 順序そのものの gate は loom `loom_live_dirty.rs`。
#[test]
fn new_kinds_under_concurrent_writes() {
    let path = tmp_path("kinds_race");
    cleanup(&path);
    let mut eng = Engine::create_growable_opts(&path, GrowableOptions::default()).unwrap();
    for (h, t) in [("company", ValueType::Ref), ("city", ValueType::Number), ("revenue", ValueType::Number), ("age", ValueType::Number), ("score", ValueType::Number)] {
        eng.define_himo(h, t, 0);
    }
    let id = |eng: &Engine, h: &str| eng.himo_id(h).unwrap() as u16;
    let (company, city, revenue, age, score) =
        (id(&eng, "company"), id(&eng, "city"), id(&eng, "revenue"), id(&eng, "age"), id(&eng, "score"));
    let companies: Vec<u64> = (0..12).map(|_| eng.entity().unwrap()).collect();
    let users: Vec<u64> = (0..600).map(|_| eng.entity().unwrap()).collect();
    for (i, &c) in companies.iter().enumerate() {
        eng.tie(c, "city", (i % 4) as u32);
        eng.tie(c, "revenue", (i * 7 % 50) as u32);
    }
    for (i, &u) in users.iter().enumerate() {
        eng.tie(u, "company", enchudb_oplog::eid_local(companies[i % companies.len()]));
        eng.tie(u, "age", (i * 13 % 40) as u32);
        eng.tie(u, "score", (i * 31 % 200) as u32);
    }
    let eng = Engine::concurrentize(eng);
    let via = |p: LivePred| LivePred::Via { path: vec![company], pred: Box::new(p) };
    let get = |eng: &Engine, e: u64, h: &str| eng.get(e, h);
    let co = |eng: &Engine, e: u64| get(eng, e, "company").map(|c| enchudb_oplog::make_eid(eng.peer_id(), c));

    for round in 0..10u64 {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writers: Vec<_> = (0..4u64)
            .map(|t| {
                let eng = eng.clone();
                let (companies, users) = (companies.clone(), users.clone());
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let mut rng = Rng(0x5eed_0000_1111_2222 ^ (round * 4 + t + 1));
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        let u = users[rng.below(users.len() as u64) as usize];
                        let c = companies[rng.below(companies.len() as u64) as usize];
                        match rng.below(8) {
                            0 => eng.tie_to(u, "age", rng.below(40) as u32),
                            1 => eng.tie_to(u, "score", rng.below(200) as u32),
                            2 => eng.tie_to(u, "company", enchudb_oplog::eid_local(c)),
                            3 => eng.tie_to(c, "city", rng.below(4) as u32),
                            4 => eng.tie_to(c, "revenue", rng.below(50) as u32),
                            5 if rng.below(4) == 0 => eng.untie(u, "score"),
                            6 if rng.below(4) == 0 => eng.untie(u, "age"),
                            _ => eng.tie_to(u, "age", rng.below(40) as u32),
                        }
                    }
                })
            })
            .collect();
        std::thread::sleep(std::time::Duration::from_millis(3));
        // 書き込みと並行して登録
        let or = eng
            .subscribe(vec![LivePred::Or(vec![
                vec![via(LivePred::Eq { himo_id: city, value: 1 })],
                vec![LivePred::Range { himo_id: age, lo: 31, hi: 39 }],
            ])])
            .unwrap();
        let range = eng.subscribe(vec![LivePred::Range { himo_id: age, lo: 10, hi: 20 }]).unwrap();
        let range_via = eng.subscribe(vec![via(LivePred::Range { himo_id: revenue, lo: 10, hi: 30 })]).unwrap();
        let counts = eng.subscribe_counts(vec![LivePred::Present { himo_id: age }], vec![company], city).unwrap();
        let sums = eng.subscribe_sums(vec![LivePred::Range { himo_id: age, lo: 0, hi: 25 }], vec![company], city, score).unwrap();
        let top = eng.subscribe_top(vec![LivePred::Present { himo_id: score }], vec![], score, false, 15).unwrap();
        let top_via = eng.subscribe_top(vec![LivePred::Present { himo_id: age }], vec![company], revenue, true, 20).unwrap();
        let top_in = eng
            .subscribe_top(vec![via(LivePred::In { himo_id: city, values: vec![0, 2] })], vec![], score, true, 10)
            .unwrap();
        let qs: [&LiveQuery; 6] = [&or, &range, &range_via, &top, &top_via, &top_in];
        let mut seen: Vec<BTreeSet<u64>> = vec![BTreeSet::new(); qs.len()];
        let mut groups: std::collections::BTreeMap<u64, u64> = Default::default();
        let mut sum_groups: std::collections::BTreeMap<u64, enchudb_engine::Agg> = Default::default();
        let group = eng.live_group();
        for q in qs {
            group.add(q);
        }
        let absorb = |seen: &mut Vec<BTreeSet<u64>>, eng: &Engine| {
            for (id, d) in group.poll(eng) {
                let i = qs.iter().position(|q| q.id() == id).expect("知らない購読の id");
                integrate(&mut seen[i], d);
            }
        };
        let t0 = std::time::Instant::now();
        let mut n = 0u64;
        while t0.elapsed() < std::time::Duration::from_millis(40) {
            n += 1;
            if n.is_multiple_of(3) {
                // count / ranked が先に settle して積んだ分も束の poll に届く
                let _ = (or.count(&eng), top.ranked(&eng), counts.total(&eng));
            }
            if n.is_multiple_of(2) {
                absorb(&mut seen, &eng);
            } else {
                for (i, q) in qs.iter().enumerate() {
                    integrate(&mut seen[i], q.poll(&eng));
                }
            }
            for (v, c) in counts.poll(&eng) {
                if c == 0 { groups.remove(&v); } else { groups.insert(v, c); }
            }
            for (v, a) in sums.poll_sums(&eng) {
                if a.count == 0 { sum_groups.remove(&v); } else { sum_groups.insert(v, a); }
            }
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for w in writers {
            w.join().unwrap();
        }
        absorb(&mut seen, &eng);
        for (v, c) in counts.poll(&eng) {
            if c == 0 { groups.remove(&v); } else { groups.insert(v, c); }
        }
        for (v, a) in sums.poll_sums(&eng) {
            if a.count == 0 { sum_groups.remove(&v); } else { sum_groups.insert(v, a); }
        }

        let has_age = |e: u64, lo: u32, hi: u32| get(&eng, e, "age").is_some_and(|a| lo <= a && a <= hi);
        let city_of = |e: u64| co(&eng, e).and_then(|c| get(&eng, c, "city"));
        let rev_of = |e: u64| co(&eng, e).and_then(|c| get(&eng, c, "revenue"));
        let set = |f: &dyn Fn(u64) -> bool| users.iter().copied().filter(|&e| f(e)).collect::<BTreeSet<u64>>();
        let top_k = |key: &dyn Fn(u64) -> Option<u32>, keep: &dyn Fn(u64) -> bool, desc: bool, k: usize| {
            let mut v: Vec<(u32, u64)> = users
                .iter()
                .copied()
                .filter(|&e| keep(e))
                .filter_map(|e| key(e).map(|x| (if desc { u32::MAX - x } else { x }, e)))
                .collect();
            v.sort_unstable();
            v.into_iter().take(k).map(|x| x.1).collect::<BTreeSet<u64>>()
        };
        let want = [
            set(&|e| city_of(e) == Some(1) || has_age(e, 31, 39)),
            set(&|e| has_age(e, 10, 20)),
            set(&|e| rev_of(e).is_some_and(|r| (10..=30).contains(&r))),
            top_k(&|e| get(&eng, e, "score"), &|_| true, false, 15),
            top_k(&rev_of, &|e| get(&eng, e, "age").is_some(), true, 20),
            top_k(&|e| get(&eng, e, "score"), &|e| matches!(city_of(e), Some(0 | 2)), true, 10),
        ];
        let names = ["or", "range", "range via", "top", "top via", "top in"];
        for i in 0..qs.len() {
            assert_eq!(seen[i], want[i], "round {round}: [{}] 積分 != 手で数えた結果", names[i]);
            assert_eq!(qs[i].count(&eng), want[i].len(), "round {round}: [{}] count", names[i]);
        }
        let mut want_groups: std::collections::BTreeMap<u64, u64> = Default::default();
        for &e in &users {
            if get(&eng, e, "age").is_some() && let Some(c) = city_of(e) {
                *want_groups.entry(c as u64).or_insert(0) += 1;
            }
        }
        assert_eq!(groups, want_groups, "round {round}: [counts] 積分 != 手で数えた件数");
        assert_eq!(counts.all(&eng), want_groups.into_iter().collect::<Vec<_>>(), "round {round}: [counts] all");
        let mut want_sums: std::collections::BTreeMap<u64, enchudb_engine::Agg> = Default::default();
        for &e in &users {
            if get(&eng, e, "age").is_some_and(|a| a <= 25) && let Some(c) = city_of(e) {
                let w = want_sums.entry(c as u64).or_default();
                w.count += 1;
                if let Some(sc) = get(&eng, e, "score") {
                    w.sum += sc as u128;
                    w.summed += 1;
                }
            }
        }
        assert_eq!(sum_groups, want_sums, "round {round}: [sums] 積分 != 手で数えた件数 / 合計");
    }
    drop(eng);
    cleanup(&path);
}
