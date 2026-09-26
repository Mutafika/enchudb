//! 鍵付きの購読 (`Engine::subscribe_keyed`): poll の差分を積分した `(entity, 鍵)` の集合が、 総当たり
//! (`get` で読んで当てる) と常に一致する。 鍵は根の列 / ref の先の列、 条件は範囲 / In / ref の先 / 否定。
//! 書き込みと並行した登録・poll でも止めた後の積分が一致する。

use enchudb_engine::{Engine, GrowableOptions, KeyedDelta, LivePred, ValueType};
use enchudb_oplog::eid_local;
use std::collections::BTreeSet;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/live_keyed_{}_{}_{}.enchu", tag, std::process::id(), nanos)
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    let _ = std::fs::remove_file(path);
    for ext in ["lock", "oplog", "schema", "tables"] {
        let _ = std::fs::remove_file(format!("{}.{}", path, ext));
    }
}

struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
    }
}

fn integrate(set: &mut BTreeSet<(u64, u64)>, d: KeyedDelta) {
    for x in d.removed {
        assert!(set.remove(&x), "removed に未報告の組 {x:?}");
    }
    for x in d.added {
        assert!(set.insert(x), "added に報告済みの組 {x:?}");
    }
}

struct Ids {
    ucity: u16,
    uage: u16,
    ucomp: u16,
    ccity: u16,
}

fn setup(path: &str, rng: &mut Rng) -> (Engine, Vec<u64>, Vec<u64>, Ids) {
    let mut eng = Engine::create_growable_opts(path, GrowableOptions::default()).unwrap();
    eng.define_himo("u.city", ValueType::Number, 0);
    eng.define_himo("u.age", ValueType::Number, 0);
    eng.define_himo("u.company", ValueType::Ref, 0);
    eng.define_himo("c.city", ValueType::Number, 0);
    let id = |eng: &Engine, h: &str| eng.himo_id(h).unwrap() as u16;
    let ids = Ids { ucity: id(&eng, "u.city"), uage: id(&eng, "u.age"), ucomp: id(&eng, "u.company"), ccity: id(&eng, "c.city") };
    let companies: Vec<u64> = (0..6).map(|_| eng.entity().unwrap()).collect();
    for (i, &c) in companies.iter().enumerate() {
        eng.tie(c, "c.city", (i % 4) as u32);
    }
    let users: Vec<u64> = (0..60).map(|_| eng.entity().unwrap()).collect();
    for &u in &users {
        if rng.below(6) != 0 {
            eng.tie(u, "u.city", rng.below(5) as u32);
        }
        eng.tie(u, "u.age", rng.below(50) as u32);
        if rng.below(5) != 0 {
            eng.tie(u, "u.company", eid_local(companies[rng.below(6) as usize]));
        }
    }
    (eng, users, companies, ids)
}

/// user の鍵を総当たりで返す関数 (条件を満たさなければ None)。
type Oracle = Box<dyn Fn(&Engine, u64) -> Option<u64>>;

/// (条件, 鍵の道, 鍵の紐, 総当たり)。
type Case = (Vec<LivePred>, Vec<u16>, u16, Oracle);

fn cases(ids: &Ids, companies: &[u64]) -> Vec<(String, Case)> {
    let (ucity, uage, ucomp, ccity) = (ids.ucity, ids.uage, ids.ucomp, ids.ccity);
    let _ = companies;
    // ref の値がそのまま指している entity (peer 0 なので eid = local)。 削除済みの会社を指したままなら、
    // その会社の列は無い (engine と同じく 「会社はあるが所在地が無い」)
    let company_of = move |eng: &Engine, u: u64| -> Option<u64> { eng.get(u, "u.company") };
    let (company_of2, company_of3) = (company_of, company_of);
    vec![
        (
            "age >= 20 を住む街で".into(),
            (
                vec![LivePred::Range { himo_id: uage, lo: 20, hi: 1000 }],
                vec![],
                ucity,
                Box::new(move |eng: &Engine, u| eng.get(u, "u.age").filter(|&a| a >= 20).and(eng.get(u, "u.city"))),
            ),
        ),
        (
            "会社がある人を会社の所在地で".into(),
            (
                vec![LivePred::Present { himo_id: ucomp }],
                vec![ucomp],
                ccity,
                Box::new(move |eng: &Engine, u| company_of(eng, u).and_then(|c| eng.get(c, "c.city"))),
            ),
        ),
        (
            "age IN (1..=25) かつ会社の所在地が 2 でない人を住む街で".into(),
            (
                vec![
                    LivePred::In { himo_id: uage, values: (1..=25).collect() },
                    LivePred::Via { path: vec![ucomp], pred: Box::new(LivePred::Not(Box::new(LivePred::Eq { himo_id: ccity, value: 2 }))) },
                ],
                vec![],
                ucity,
                Box::new(move |eng: &Engine, u| {
                    let a = eng.get(u, "u.age")?;
                    let c = company_of2(eng, u)?;
                    ((1..=25).contains(&a) && eng.get(c, "c.city") != Some(2)).then_some(())?;
                    eng.get(u, "u.city")
                }),
            ),
        ),
        (
            "会社の所在地が 1 の人を会社の所在地で".into(),
            (
                vec![LivePred::Via { path: vec![ucomp], pred: Box::new(LivePred::Eq { himo_id: ccity, value: 1 }) }],
                vec![ucomp],
                ccity,
                Box::new(move |eng: &Engine, u| company_of3(eng, u).and_then(|c| eng.get(c, "c.city")).filter(|&v| v == 1)),
            ),
        ),
    ]
}

#[test]
fn keyed_subscriptions_match_oracle() {
    let path = tmp_path("oracle");
    cleanup(&path);
    let mut rng = Rng(0x6b65_7965_6400_0001);
    let (eng, mut users, mut companies, ids) = setup(&path, &mut rng);
    let eng = Engine::concurrentize(eng);
    struct Sub {
        name: String,
        q: enchudb_engine::LiveKeyed,
        seen: BTreeSet<(u64, u64)>,
        want: Oracle,
    }
    let make = |i: usize, companies: &[u64]| -> Sub {
        let (name, (preds, path, himo, want)) = cases(&ids, companies).swap_remove(i);
        Sub { name, q: eng.subscribe_keyed(preds, path, himo).unwrap(), seen: BTreeSet::new(), want }
    };
    let mut subs: Vec<Sub> = (0..8).map(|i| make(i % 4, &companies)).collect();
    for step in 0..1500usize {
        let u = users[rng.below(users.len() as u64) as usize];
        let c = companies[rng.below(companies.len() as u64) as usize];
        match rng.below(10) {
            0 | 1 => eng.tie_to(u, "u.city", rng.below(5) as u32),
            2 => eng.untie(u, "u.city"),
            3 => eng.tie_to(u, "u.age", rng.below(50) as u32),
            4 | 5 => eng.tie_to(u, "u.company", eid_local(c)),
            6 => eng.tie_to(c, "c.city", rng.below(4) as u32),
            7 => {
                let i = rng.below(users.len() as u64) as usize;
                eng.delete(users[i]);
                let e = eng.entity().unwrap();
                eng.tie_to(e, "u.city", rng.below(5) as u32);
                eng.tie_to(e, "u.age", rng.below(50) as u32);
                eng.tie_to(e, "u.company", eid_local(c));
                users[i] = e;
            }
            8 => {
                let i = rng.below(companies.len() as u64) as usize;
                eng.delete(companies[i]);
                let e = eng.entity().unwrap();
                eng.tie_to(e, "c.city", rng.below(4) as u32);
                companies[i] = e;
                // 総当たりの会社の一覧を差し替える
                for (k, s) in subs.iter_mut().enumerate() {
                    let (name, (.., want)) = cases(&ids, &companies).swap_remove(k % 4);
                    debug_assert_eq!(name, s.name);
                    s.want = want;
                }
            }
            _ => eng.untie(u, "u.company"),
        }
        if step % 50 == 7 {
            let i = rng.below(subs.len() as u64) as usize;
            subs[i] = make(i % 4, &companies);
        }
        if step % 3 == 0 {
            for s in subs.iter_mut() {
                integrate(&mut s.seen, s.q.poll(&eng));
                let want: BTreeSet<(u64, u64)> = users.iter().filter_map(|&u| (s.want)(&eng, u).map(|k| (u, k))).collect();
                assert_eq!(s.seen, want, "[{}] step {step}", s.name);
            }
        }
    }
    drop(subs);
    drop(eng);
    cleanup(&path);
}

/// 書き込み (街・会社・会社の所在地・age) と並行して登録・poll し、 止めた後の積分が総当たりと一致する。
#[test]
fn keyed_under_concurrent_writes() {
    let path = tmp_path("race");
    cleanup(&path);
    let mut rng = Rng(0x6b65_7965_6400_0002);
    let (eng, users, companies, ids) = setup(&path, &mut rng);
    let eng = Engine::concurrentize(eng);
    for round in 0..10u64 {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writers: Vec<_> = (0..4u64)
            .map(|t| {
                let eng = eng.clone();
                let (users, companies) = (users.clone(), companies.clone());
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let mut rng = Rng(0x7ace_0000_0000_0001 ^ (round * 4 + t + 1));
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        let u = users[rng.below(users.len() as u64) as usize];
                        let c = companies[rng.below(companies.len() as u64) as usize];
                        match rng.below(5) {
                            0 => eng.tie_to(u, "u.city", rng.below(5) as u32),
                            1 => eng.tie_to(u, "u.age", rng.below(50) as u32),
                            2 => eng.tie_to(u, "u.company", eid_local(c)),
                            3 => eng.tie_to(c, "c.city", rng.below(4) as u32),
                            _ => eng.untie(u, "u.city"),
                        }
                    }
                })
            })
            .collect();
        std::thread::sleep(std::time::Duration::from_millis(3));
        let mut subs: Vec<_> = cases(&ids, &companies)
            .into_iter()
            .map(|(name, (preds, path, himo, want))| (name, eng.subscribe_keyed(preds, path, himo).unwrap(), BTreeSet::new(), want))
            .collect();
        let t0 = std::time::Instant::now();
        while t0.elapsed() < std::time::Duration::from_millis(40) {
            for (_, q, seen, _) in subs.iter_mut() {
                integrate(seen, q.poll(&eng));
            }
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for w in writers {
            w.join().unwrap();
        }
        for (name, q, seen, want) in subs.iter_mut() {
            integrate(seen, q.poll(&eng));
            let w: BTreeSet<(u64, u64)> = users.iter().filter_map(|&u| want(&eng, u).map(|k| (u, k))).collect();
            assert_eq!(*seen, w, "round {round}: {name}");
        }
    }
    drop(eng);
    cleanup(&path);
}

/// 報告済みの entity が削除され、 同じ eid が別の entity に使い回されて同じ鍵を持った: 集合としては同じでも
/// 別物なので、 removed と added の両方に出る (普通の購読の 「出て入り直した」 と同じ)。
#[test]
fn reused_eid_with_the_same_key_leaves_and_reenters() {
    let path = tmp_path("reuse");
    cleanup(&path);
    // slot の再利用は max_entities 到達後だけ — 容量 1 で起こす
    let mut eng = Engine::create_with_capacity(&path, 1).unwrap();
    eng.define_himo("u.city", ValueType::Number, 0);
    let city = eng.himo_id("u.city").unwrap() as u16;
    let a = eng.entity().unwrap();
    eng.tie(a, "u.city", 3u32);
    let q = eng.subscribe_keyed(vec![LivePred::Present { himo_id: city }], vec![], city).unwrap();
    assert_eq!(q.poll(&eng).added, vec![(a, 3)]);
    eng.delete(a);
    let b = eng.entity().unwrap();
    assert_eq!(b, a, "前提: 容量 1 なら削除した slot が再利用される");
    eng.tie(b, "u.city", 3u32);
    let d = q.poll(&eng);
    assert_eq!((d.removed, d.added, d.reentered), (vec![(a, 3)], vec![(b, 3)], vec![b]));
    drop(q);
    drop(eng);
    cleanup(&path);
}
