//! 会社単位の購読 (`subscribe_grouped`) のまとめ読み (`members_many` / `count_in` / `count` / `flatten`) の検査。
//!
//! 購読は 「ref の先 (会社) の city が c」 + 根 (社員) への条件 (年齢の範囲・役割・部署の語・否定・`In`・`Present`)。
//! 書き込み (city・年齢・年齢を外す・異動・会社を外す・役割・部署・社員の削除と作り直し) のたびに poll し、 group の
//! 集合、 各会社の members / count_in、 members_many、 count、 flatten を、 テストの中で持った写し (shadow) から数えた
//! 答えと比べる。 2 本目の経路として `find_by` とも比べる。 会社を匿名表に先に作る並べ方 (会社の一覧は値ごとの bucket) と、
//! 表を 2 つ作って会社の eid を 2^20 の先に置く並べ方 (会社の一覧は sparse) の両方で回す。

use enchudb_engine::{Engine, GroupedLiveQuery, GrowableOptions, LiveDelta, LivePred, ValueType};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    // 並行に走る test が同じ時刻を引いても衝突しないよう、 通し番号も付ける
    static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/grouped_batch_{}_{}_{}_{}.enchu", tag, std::process::id(), nanos, n)
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

fn integrate(set: &mut BTreeSet<u64>, d: LiveDelta) {
    for e in d.removed {
        assert!(set.remove(&e), "removed に未報告の eid {e}");
    }
    for e in d.added {
        assert!(set.insert(e), "added に報告済みの eid {e}");
    }
}

const DEPTS: [&str; 3] = ["sales", "dev", "ops"];

#[derive(Clone, Copy)]
struct Row {
    company: Option<u64>,
    age: Option<u64>,
    role: u64,
    dept: usize,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Layout {
    /// 会社を匿名表に先に作る (会社の eid は小さい = 値ごとの bucket)
    Anon,
    /// users 表 → posts 表 (空) → companies 表 (会社の eid は 2,000,000 から = 2^20 の先 = sparse)
    Tables,
}

struct World {
    path: String,
    eng: Arc<Engine>,
    comps: Vec<u64>,
    users: Vec<u64>,
    city: BTreeMap<u64, u64>,
    rows: BTreeMap<u64, Row>,
    /// 紐の名前 (表の並べ方なら "表.列")
    n_company: &'static str,
    n_city: &'static str,
    n_age: &'static str,
    n_role: &'static str,
    n_dept: &'static str,
    h_company: u16,
    h_city: u16,
    h_age: u16,
    h_role: u16,
    h_dept: u16,
    /// users を作る表 (None = 匿名)
    user_table: Option<&'static str>,
}

impl Drop for World {
    fn drop(&mut self) {
        cleanup(&self.path);
    }
}

fn world(layout: Layout, seed: u64) -> World {
    let path = tmp_path("w");
    cleanup(&path);
    let opts = GrowableOptions { max_entities: 1 << 22, ..Default::default() };
    let mut eng = Engine::create_growable_opts(&path, opts).unwrap();
    let (names, user_table, comps, users): ([&'static str; 5], Option<&'static str>, Vec<u64>, Vec<u64>);
    let mut rng = Rng(seed);
    match layout {
        Layout::Anon => {
            eng.define_himo("company", ValueType::Ref, 0);
            eng.define_himo("city", ValueType::Number, 4);
            eng.define_himo("age", ValueType::Number, 0);
            eng.define_himo("role", ValueType::Number, 0);
            eng.define_himo("dept", ValueType::Tag, 0);
            names = ["company", "city", "age", "role", "dept"];
            user_table = None;
            comps = (0..40).map(|_| eng.entity().unwrap()).collect();
            users = (0..2000).map(|_| eng.entity().unwrap()).collect();
        }
        Layout::Tables => {
            eng.define_table("users", 0).unwrap();
            eng.define_table("posts", 0).unwrap();
            eng.define_table("companies", 0).unwrap();
            eng.define_himo_in("companies", "city", ValueType::Number, 4).unwrap();
            eng.define_himo_in("users", "age", ValueType::Number, 0).unwrap();
            eng.define_himo_in("users", "role", ValueType::Number, 0).unwrap();
            eng.define_himo_in("users", "dept", ValueType::Tag, 0).unwrap();
            eng.define_ref_in("users", "company", "companies").unwrap();
            names = ["users.company", "companies.city", "users.age", "users.role", "users.dept"];
            user_table = Some("users");
            users = (0..2000).map(|_| eng.entity_in("users").unwrap()).collect();
            comps = (0..40).map(|_| eng.entity_in("companies").unwrap()).collect();
            assert!(comps.iter().all(|&c| enchudb_oplog::eid_local(c) >= 1 << 20), "前提: 会社の eid が 2^20 の先");
        }
    }
    let [n_company, n_city, n_age, n_role, n_dept] = names;
    let mut city = BTreeMap::new();
    for &c in &comps {
        let v = rng.below(4);
        eng.tie(c, n_city, v as u32);
        city.insert(c, v);
    }
    let mut rows = BTreeMap::new();
    for &u in &users {
        let c = comps[rng.below(comps.len() as u64) as usize];
        let (a, r, d) = (18 + rng.below(62), rng.below(3), rng.below(3) as usize);
        eng.tie(u, n_company, enchudb_oplog::eid_local(c));
        eng.tie(u, n_age, a as u32);
        eng.tie(u, n_role, r as u32);
        eng.tie_text(u, n_dept, DEPTS[d]);
        rows.insert(u, Row { company: Some(c), age: Some(a), role: r, dept: d });
    }
    let h = |n: &str| eng.himo_id(n).unwrap() as u16;
    let (h_company, h_city, h_age, h_role, h_dept) = (h(n_company), h(n_city), h(n_age), h(n_role), h(n_dept));
    let eng = Engine::concurrentize(eng);
    World {
        path,
        eng,
        comps,
        users,
        city,
        rows,
        n_company,
        n_city,
        n_age,
        n_role,
        n_dept,
        h_company,
        h_city,
        h_age,
        h_role,
        h_dept,
        user_table,
    }
}

/// 根への条件の種類 (購読ごとに 1 つ)。
#[derive(Clone, Copy, Debug)]
enum Root {
    None,
    Age(u64, u64),
    AgeAndRole(u64, u64, u64),
    Dept(usize),
    /// 部署がまだ辞書に無い語 (誰も満たさない)
    DeptUnknown,
    RoleIn,
    NotRole(u64),
    AgePresent,
}

const SPECS: [(u64, Root); 8] = [
    (1, Root::Age(30, 1000)),
    (1, Root::None),
    (2, Root::AgeAndRole(25, 50, 1)),
    (1, Root::Dept(1)),
    (3, Root::DeptUnknown),
    (2, Root::RoleIn),
    (1, Root::NotRole(0)),
    (0, Root::AgePresent),
];

fn preds(w: &World, city: u64, root: Root) -> Vec<LivePred> {
    let mut p = vec![LivePred::Via { path: vec![w.h_company], pred: Box::new(LivePred::Eq { himo_id: w.h_city, value: city }) }];
    match root {
        Root::None => {}
        Root::Age(lo, hi) => p.push(LivePred::Range { himo_id: w.h_age, lo, hi }),
        Root::AgeAndRole(lo, hi, r) => {
            p.push(LivePred::Range { himo_id: w.h_age, lo, hi });
            p.push(LivePred::Eq { himo_id: w.h_role, value: r });
        }
        Root::Dept(d) => p.push(LivePred::EqText { himo_id: w.h_dept, text: DEPTS[d].into() }),
        Root::DeptUnknown => p.push(LivePred::EqText { himo_id: w.h_dept, text: "nobody-uses-this-dept".into() }),
        Root::RoleIn => p.push(LivePred::In { himo_id: w.h_role, values: vec![2, 0] }),
        Root::NotRole(r) => p.push(LivePred::Not(Box::new(LivePred::Eq { himo_id: w.h_role, value: r }))),
        Root::AgePresent => p.push(LivePred::Present { himo_id: w.h_age }),
    }
    p
}

fn root_holds(r: &Row, root: Root) -> bool {
    let age_in = |lo: u64, hi: u64| r.age.is_some_and(|a| lo <= a && a <= hi);
    match root {
        Root::None => true,
        Root::Age(lo, hi) => age_in(lo, hi),
        Root::AgeAndRole(lo, hi, role) => age_in(lo, hi) && r.role == role,
        Root::Dept(d) => r.dept == d,
        Root::DeptUnknown => false,
        Root::RoleIn => r.role == 2 || r.role == 0,
        Root::NotRole(role) => r.role != role,
        Root::AgePresent => r.age.is_some(),
    }
}

fn want_members(w: &World, root: Root, g: u64) -> Vec<u64> {
    w.rows.iter().filter(|(_, r)| r.company == Some(g) && root_holds(r, root)).map(|(&u, _)| u).collect()
}

fn pick_user(w: &World, rng: &mut Rng) -> (usize, u64) {
    let i = rng.below(w.users.len() as u64) as usize;
    (i, w.users[i])
}

/// 書き込みを 1 つ (shadow も同じに直す)。
fn write_one(w: &mut World, rng: &mut Rng) {
    match rng.below(22) {
        0..=3 => {
            let c = w.comps[rng.below(w.comps.len() as u64) as usize];
            let v = rng.below(4);
            w.eng.tie_to(c, w.n_city, v as u32);
            w.city.insert(c, v);
        }
        4..=8 => {
            let (_, u) = pick_user(w, rng);
            let a = 18 + rng.below(62);
            w.eng.tie_to(u, w.n_age, a as u32);
            w.rows.get_mut(&u).unwrap().age = Some(a);
        }
        9 => {
            let (_, u) = pick_user(w, rng);
            w.eng.untie(u, w.n_age);
            w.rows.get_mut(&u).unwrap().age = None;
        }
        10..=13 => {
            let (_, u) = pick_user(w, rng);
            let c = w.comps[rng.below(w.comps.len() as u64) as usize];
            w.eng.tie_to(u, w.n_company, enchudb_oplog::eid_local(c));
            w.rows.get_mut(&u).unwrap().company = Some(c);
        }
        14..=15 => {
            let (_, u) = pick_user(w, rng);
            let r = rng.below(3);
            w.eng.tie_to(u, w.n_role, r as u32);
            w.rows.get_mut(&u).unwrap().role = r;
        }
        16 => {
            let (_, u) = pick_user(w, rng);
            let d = rng.below(3) as usize;
            w.eng.tie_text_to(u, w.n_dept, DEPTS[d]);
            w.rows.get_mut(&u).unwrap().dept = d;
        }
        17 => {
            let (_, u) = pick_user(w, rng);
            w.eng.untie(u, w.n_company);
            w.rows.get_mut(&u).unwrap().company = None;
        }
        18 => {
            let (i, u) = pick_user(w, rng);
            w.eng.delete(u);
            w.rows.remove(&u);
            w.users.swap_remove(i);
        }
        _ => {
            // 作り直し (消した slot の使い回しを含む)
            let u = match w.user_table {
                Some(t) => w.eng.entity_in(t).unwrap(),
                None => w.eng.entity().unwrap(),
            };
            let c = w.comps[rng.below(w.comps.len() as u64) as usize];
            let (a, r, d) = (18 + rng.below(62), rng.below(3), rng.below(3) as usize);
            w.eng.tie_to(u, w.n_company, enchudb_oplog::eid_local(c));
            w.eng.tie_to(u, w.n_age, a as u32);
            w.eng.tie_to(u, w.n_role, r as u32);
            w.eng.tie_text_to(u, w.n_dept, DEPTS[d]);
            w.rows.insert(u, Row { company: Some(c), age: Some(a), role: r, dept: d });
            w.users.push(u);
        }
    }
}

/// 全購読について、 poll の積分・groups・members・members_many・count_in・count・flatten を shadow と比べ、 flatten を
/// `find_by` (別の経路) とも比べる。
fn check_all(w: &World, qs: &[GroupedLiveQuery], seen: &mut [BTreeSet<u64>], round: usize) {
    for (i, q) in qs.iter().enumerate() {
        let (city, root) = SPECS[i];
        integrate(&mut seen[i], q.poll(&w.eng));
        let want_g: BTreeSet<u64> = w.city.iter().filter(|&(_, &v)| v == city).map(|(&c, _)| c).collect();
        assert_eq!(seen[i], want_g, "round {round} 購読 {i} {root:?}: poll の積分");
        assert_eq!(q.groups(&w.eng).into_iter().collect::<BTreeSet<_>>(), want_g, "round {round} 購読 {i}: groups");
        let many = q.members_many(&w.eng, &w.comps);
        assert_eq!(many.len(), w.comps.len());
        let mut flat = Vec::new();
        for (k, &c) in w.comps.iter().enumerate() {
            let want = if want_g.contains(&c) { want_members(w, root, c) } else { Vec::new() };
            assert_eq!(q.members(&w.eng, c), want, "round {round} 購読 {i} {root:?}: members({c})");
            assert_eq!(many[k], want, "round {round} 購読 {i} {root:?}: members_many の {k} 番目 ({c})");
            assert_eq!(q.count_in(&w.eng, c), want.len(), "round {round} 購読 {i} {root:?}: count_in({c})");
            flat.extend(want);
        }
        flat.sort_unstable();
        assert_eq!(q.count(&w.eng), flat.len(), "round {round} 購読 {i} {root:?}: count");
        assert_eq!(q.flatten(&w.eng), flat, "round {round} 購読 {i} {root:?}: flatten");
        let found = w.eng.find_by(preds(w, city, root)).unwrap();
        assert_eq!(found, flat, "round {round} 購読 {i} {root:?}: find_by");
    }
}

fn run(layout: Layout, seed: u64) {
    let mut w = world(layout, seed);
    let qs: Vec<GroupedLiveQuery> =
        SPECS.iter().map(|&(city, root)| w.eng.subscribe_grouped(preds(&w, city, root)).unwrap()).collect();
    let mut seen = vec![BTreeSet::new(); qs.len()];
    let mut rng = Rng(seed ^ 0xba7c);
    check_all(&w, &qs, &mut seen, 0);
    for round in 1..=300 {
        for _ in 0..1 + rng.below(5) {
            write_one(&mut w, &mut rng);
        }
        check_all(&w, &qs, &mut seen, round);
    }
}

#[test]
fn grouped_batch_reads_match_shadow_with_dense_company_lists() {
    run(Layout::Anon, 0x5eed_2001);
}

#[test]
fn grouped_batch_reads_match_shadow_with_sparse_company_lists() {
    run(Layout::Tables, 0x5eed_2002);
}

/// `members_many` に同じ group を 2 度渡しても、 今条件を満たさない group や会社でない entity を渡しても、
/// 1 つずつ引いた `members` と同じ並びで返る。
#[test]
fn members_many_follows_the_given_order() {
    let w = world(Layout::Anon, 0x5eed_2003);
    let q = w.eng.subscribe_grouped(preds(&w, 1, Root::Age(30, 1000))).unwrap();
    let mut ask: Vec<u64> = w.comps.iter().rev().copied().collect();
    ask.push(w.comps[0]);
    ask.push(w.users[0]); // 会社でない
    let many = q.members_many(&w.eng, &ask);
    let one: Vec<Vec<u64>> = ask.iter().map(|&g| q.members(&w.eng, g)).collect();
    assert_eq!(many, one);
    assert!(many.last().unwrap().is_empty(), "会社でない entity の members は空");
    assert!(many.iter().any(|m| !m.is_empty()), "対照: 空でない会社が 1 つはある");
}

/// 書き手 3 本と並行に members_many / count / flatten を読み、 止めた後に `find_by` と一致する。
#[test]
fn grouped_batch_reads_under_concurrent_writes() {
    let w = world(Layout::Tables, 0x5eed_2004);
    let qs: Vec<GroupedLiveQuery> =
        SPECS.iter().map(|&(city, root)| w.eng.subscribe_grouped(preds(&w, city, root)).unwrap()).collect();
    for round in 0..8u64 {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writers: Vec<_> = (0..3u64)
            .map(|t| {
                let eng = w.eng.clone();
                let (users, comps) = (w.users.clone(), w.comps.clone());
                let (n_city, n_age, n_company) = (w.n_city, w.n_age, w.n_company);
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let mut rng = Rng(0xc0c0_0000_0000_0021 ^ (round * 3 + t + 1));
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        let u = users[rng.below(users.len() as u64) as usize];
                        match rng.below(10) {
                            0..=2 => eng.tie_to(comps[rng.below(comps.len() as u64) as usize], n_city, rng.below(4) as u32),
                            3..=5 => eng.tie_to(u, n_age, (18 + rng.below(62)) as u32),
                            6..=8 => eng.tie_to(u, n_company, enchudb_oplog::eid_local(comps[rng.below(comps.len() as u64) as usize])),
                            _ => eng.untie(u, n_age),
                        }
                    }
                })
            })
            .collect();
        let t0 = std::time::Instant::now();
        while t0.elapsed() < std::time::Duration::from_millis(40) {
            for q in &qs {
                let _ = q.poll(&w.eng);
                let _ = (q.members_many(&w.eng, &w.comps), q.count(&w.eng), q.flatten(&w.eng));
            }
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for t in writers {
            t.join().unwrap();
        }
        for (i, q) in qs.iter().enumerate() {
            let (city, root) = SPECS[i];
            let found = w.eng.find_by(preds(&w, city, root)).unwrap();
            assert_eq!(q.flatten(&w.eng), found, "round {round} 購読 {i}: flatten");
            assert_eq!(q.count(&w.eng), found.len(), "round {round} 購読 {i}: count");
            let many = q.members_many(&w.eng, &w.comps);
            let mut flat: Vec<u64> = many.into_iter().flatten().collect();
            flat.sort_unstable();
            assert_eq!(flat, found, "round {round} 購読 {i}: members_many");
        }
    }
}
