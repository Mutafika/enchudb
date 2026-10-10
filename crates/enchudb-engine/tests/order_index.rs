//! 並びの索引 (`Engine::declare_order`) の検査。 ref 紐 (社員 → 会社) の逆引きを年齢の帯で分けて持つ索引を、 会社単位の
//! 購読 (`subscribe_grouped` / `subscribe_touched`) と社員単位の購読 (`subscribe`) が使う時に、 答えが変わらないこと。
//!
//! 書き込み (city・年齢 (1000 / 1001 をまたぐもの含む)・年齢を外す・異動・会社を外す・役割・部署・社員の削除と作り直し) の
//! たびに、 group の集合・各会社の members / count_in・members_many・count・flatten を shadow と `find_by` の 2 経路で比べる。
//! 目盛りが範囲の端に合う宣言 / 合わない宣言、 購読の後から宣言、 会社を匿名表に先に作る並べ方 (base 0) と、 表を 3 つ作って
//! 会社の eid を 2^20 の先に置く並べ方 (base = companies 表の始まり) の両方。 索引を本当に読んだか (正の対照) も見る。

use enchudb_engine::{Engine, GroupedLiveQuery, GrowableOptions, LiveDelta, LivePred, TouchedDelta, TouchedLiveQuery, ValueType};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    // 並行に走る test が同じ時刻を引いても衝突しないよう、 通し番号も付ける
    static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/order_index_{}_{}_{}_{}.enchu", tag, std::process::id(), nanos, n)
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
    /// 匿名表で社員の後に会社を作る (base 0 のまま、 会社の eid が後ろに固まる)
    AnonUsersFirst,
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
        Layout::Anon | Layout::AnonUsersFirst => {
            eng.define_himo("company", ValueType::Ref, 0);
            eng.define_himo("city", ValueType::Number, 4);
            eng.define_himo("age", ValueType::Number, 0);
            eng.define_himo("role", ValueType::Number, 0);
            eng.define_himo("dept", ValueType::Tag, 0);
            names = ["company", "city", "age", "role", "dept"];
            user_table = None;
            if layout == Layout::Anon {
                comps = (0..40).map(|_| eng.entity().unwrap()).collect();
                users = (0..2000).map(|_| eng.entity().unwrap()).collect();
            } else {
                users = (0..2000).map(|_| eng.entity().unwrap()).collect();
                comps = (0..40).map(|_| eng.entity().unwrap()).collect();
            }
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

/// 書き込みを 1 つ (shadow も同じに直す)。 年齢は 1000 / 1001 の境目をまたぐものも混ぜる。
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
            let a = if rng.below(10) == 0 { 990 + rng.below(110) } else { 18 + rng.below(62) };
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

fn integrate(set: &mut BTreeSet<u64>, d: LiveDelta) {
    for e in d.removed {
        assert!(set.remove(&e), "removed に未報告の eid {e}");
    }
    for e in d.added {
        assert!(set.insert(e), "added に報告済みの eid {e}");
    }
}

/// 年齢の範囲の購読 (並びの索引が効くもの) と、 効かないもの (年齢の条件なし・語・In・Not・年齢が有るか)。 効かない
/// ものは、 索引を宣言した DB でも答えが変わらないことを見る。
const SPECS: [(u64, Root); 10] = [
    (1, Root::Age(30, 1000)),
    (1, Root::Age(25, 50)),
    (2, Root::AgeAndRole(30, 1000, 1)),
    (3, Root::Age(1001, u32::MAX as u64 - 1)),
    (1, Root::None),
    (2, Root::Dept(1)),
    (3, Root::AgePresent),
    (1, Root::NotRole(1)),
    (2, Root::RoleIn),
    (3, Root::DeptUnknown),
];

fn declare(w: &World, ticks: &[u64]) {
    w.eng.declare_order(w.n_company, w.n_age, ticks).unwrap();
}

/// 会社単位の購読: group の集合・members・members_many・count_in・count・flatten・find_by。
fn check_grouped(w: &World, qs: &[GroupedLiveQuery], seen: &mut [BTreeSet<u64>], round: usize) {
    for (i, q) in qs.iter().enumerate() {
        let (city, root) = SPECS[i];
        integrate(&mut seen[i], q.poll(&w.eng));
        let want_g: BTreeSet<u64> = w.city.iter().filter(|&(_, &v)| v == city).map(|(&c, _)| c).collect();
        assert_eq!(seen[i], want_g, "round {round} 購読 {i} {root:?}: group");
        let many = q.members_many(&w.eng, &w.comps);
        let mut flat = Vec::new();
        for (k, &c) in w.comps.iter().enumerate() {
            let want = if want_g.contains(&c) { want_members(w, root, c) } else { Vec::new() };
            assert_eq!(q.members(&w.eng, c), want, "round {round} 購読 {i} {root:?}: members({c})");
            assert_eq!(many[k], want, "round {round} 購読 {i} {root:?}: members_many({c})");
            assert_eq!(q.count_in(&w.eng, c), want.len(), "round {round} 購読 {i} {root:?}: count_in({c})");
            flat.extend(want);
        }
        flat.sort_unstable();
        assert_eq!(q.count(&w.eng), flat.len(), "round {round} 購読 {i} {root:?}: count");
        assert_eq!(q.flatten(&w.eng), flat, "round {round} 購読 {i} {root:?}: flatten");
        assert_eq!(w.eng.find_by(preds(w, city, root)).unwrap(), flat, "round {round} 購読 {i} {root:?}: find_by");
    }
}

/// `ticks` = None なら宣言しない (対照)。 `late` なら購読を張って 150 round 書いた後に宣言する。
fn run_grouped(layout: Layout, ticks: Option<&[u64]>, late: bool, seed: u64) -> (u64, u64) {
    let mut w = world(layout, seed);
    if let (Some(t), false) = (ticks, late) {
        declare(&w, t);
    }
    let qs: Vec<GroupedLiveQuery> =
        SPECS.iter().map(|&(city, root)| w.eng.subscribe_grouped(preds(&w, city, root)).unwrap()).collect();
    let mut seen = vec![BTreeSet::new(); qs.len()];
    let mut rng = Rng(seed ^ 0x0de5);
    check_grouped(&w, &qs, &mut seen, 0);
    for round in 1..=300 {
        if let (Some(t), true, 150) = (ticks, late, round) {
            declare(&w, t);
        }
        for _ in 0..1 + rng.below(5) {
            write_one(&mut w, &mut rng);
        }
        check_grouped(&w, &qs, &mut seen, round);
    }
    order_hits(&w)
}

/// 索引を読んだ回数と、 読んだ entity の数。
fn order_hits(w: &World) -> (u64, u64) {
    w.eng.order_stats().iter().fold((0, 0), |(h, r), &(_, _, h2, r2, _)| (h + h2, r + r2))
}

#[test]
fn grouped_with_aligned_order_dense_companies() {
    let (hits, read) = run_grouped(Layout::Anon, Some(&[30, 1001]), false, 0x5eed_4001);
    assert!(hits > 0 && read > 0, "索引を読んでいない (hits {hits}, read {read})");
}

#[test]
fn grouped_with_aligned_order_sparse_companies() {
    let (hits, _) = run_grouped(Layout::Tables, Some(&[30, 1001]), false, 0x5eed_4002);
    assert!(hits > 0, "索引を読んでいない (会社の eid が 2^20 の先 = base の付け替え)");
}

#[test]
fn grouped_with_order_companies_after_users() {
    let (hits, read) = run_grouped(Layout::AnonUsersFirst, Some(&[30, 1001]), false, 0x5eed_4007);
    assert!(hits > 0 && read > 0, "索引を読んでいない (会社が社員の後ろ、 base 0)");
}

#[test]
fn grouped_with_misaligned_order() {
    // 目盛りが範囲の端と合わない (帯の分だけ広く読んで、 範囲の条件で絞る)
    let (hits, _) = run_grouped(Layout::Anon, Some(&[25, 40, 61]), false, 0x5eed_4003);
    assert!(hits > 0);
}

#[test]
fn grouped_with_order_declared_late() {
    let (hits, _) = run_grouped(Layout::Tables, Some(&[30, 1001]), true, 0x5eed_4004);
    assert!(hits > 0, "後から宣言した索引を読んでいない");
}

#[test]
fn grouped_without_order_is_the_control() {
    assert_eq!(run_grouped(Layout::Anon, None, false, 0x5eed_4005), (0, 0), "宣言していないのに索引を読んだ");
}

/// 正の対照: 目盛りが範囲の端に合う年齢の範囲 1 本の購読なら、 count_in は索引の件数だけで答え (entity を 1 件も読まない)、
/// members は帯だけ読む (読んだ数 = 件数)。
#[test]
fn count_in_reads_no_entities_when_ticks_align() {
    let w = world(Layout::Anon, 0x5eed_4006);
    declare(&w, &[30, 1001]);
    let q = w.eng.subscribe_grouped(preds(&w, 1, Root::Age(30, 1000))).unwrap();
    let _ = q.poll(&w.eng);
    let g = *q.groups(&w.eng).iter().find(|&&g| !want_members(&w, Root::Age(30, 1000), g).is_empty()).expect("対照の会社");
    let (h0, r0) = order_hits(&w);
    let n = q.count_in(&w.eng, g);
    let (h1, r1) = order_hits(&w);
    assert_eq!(n, want_members(&w, Root::Age(30, 1000), g).len());
    assert_eq!((h1 - h0, r1 - r0), (1, 0), "合った目盛りの count_in は件数の和だけで済むはず");
    let m = q.members(&w.eng, g);
    let (h2, r2) = order_hits(&w);
    assert_eq!(m.len(), n);
    assert_eq!((h2 - h1, r2 - r1), (1, n as u64), "合った目盛りの members は帯 (= 件数) だけ読むはず");
}

/// 社員単位の購読: 積分した結果が毎回 find_by と同じ。 会社の答えが変わった時に索引の帯だけ引く道を通る。
#[test]
fn row_subscription_with_order() {
    for (layout, ticks, seed) in [(Layout::Anon, &[30u64, 1001][..], 0x5eed_4011u64), (Layout::Tables, &[25, 40, 61][..], 0x5eed_4012)] {
        let mut w = world(layout, seed);
        declare(&w, ticks);
        let qs: Vec<_> = SPECS.iter().map(|&(city, root)| w.eng.subscribe(preds(&w, city, root)).unwrap()).collect();
        let mut seen = vec![BTreeSet::new(); qs.len()];
        let mut rng = Rng(seed ^ 0x40e);
        for round in 0..=300 {
            if round > 0 {
                for _ in 0..1 + rng.below(5) {
                    write_one(&mut w, &mut rng);
                }
            }
            for (i, q) in qs.iter().enumerate() {
                let (city, root) = SPECS[i];
                integrate(&mut seen[i], q.poll(&w.eng));
                let want: BTreeSet<u64> = w.eng.find_by(preds(&w, city, root)).unwrap().into_iter().collect();
                assert_eq!(seen[i], want, "{layout:?} round {round} 購読 {i} {root:?}");
            }
        }
        let (hits, _) = order_hits(&w);
        assert!(hits > 0, "{layout:?}: 社員単位の購読が索引を読んでいない");
    }
}

/// 見直す場所だけ返す購読 + 並びの索引: アプリの表 (社員 → 会社) が毎回 shadow と find_by に一致する。
#[test]
fn touched_with_order() {
    let mut w = world(Layout::Tables, 0x5eed_4021);
    declare(&w, &[30, 1001]);
    let qs: Vec<TouchedLiveQuery> =
        SPECS.iter().map(|&(city, root)| w.eng.subscribe_touched(preds(&w, city, root)).unwrap()).collect();
    let mut views: Vec<BTreeMap<u64, u64>> = vec![BTreeMap::new(); qs.len()];
    let mut rng = Rng(0x7e1);
    for round in 0..=300 {
        if round > 0 {
            for _ in 0..1 + rng.below(5) {
                write_one(&mut w, &mut rng);
            }
        }
        for (i, q) in qs.iter().enumerate() {
            let (city, root) = SPECS[i];
            let d: TouchedDelta = q.poll(&w.eng);
            if !d.removed.is_empty() {
                let gone: BTreeSet<u64> = d.removed.iter().copied().collect();
                views[i].retain(|_, g| !gone.contains(g));
            }
            for (&g, staff) in d.added.iter().zip(q.members_many(&w.eng, &d.added)) {
                for u in staff {
                    views[i].insert(u, g);
                }
            }
            for u in d.touched {
                match q.group_of(&w.eng, u) {
                    Some(g) => {
                        views[i].insert(u, g);
                    }
                    None => {
                        views[i].remove(&u);
                    }
                }
            }
            let want: BTreeMap<u64, u64> = w
                .rows
                .iter()
                .filter_map(|(&u, r)| {
                    let c = r.company?;
                    (w.city.get(&c) == Some(&city) && root_holds(r, root)).then_some((u, c))
                })
                .collect();
            assert_eq!(views[i], want, "round {round} 購読 {i} {root:?}: アプリの表");
        }
    }
    assert!(order_hits(&w).0 > 0);
}

/// 書き手 3 本 (年齢・異動・city) と並行に読みと作るのを走らせ (宣言は書き手が動き始めてから = 作る間も書かれる)、
/// 止めた後に索引から読んだ答えが find_by と一致する。
#[test]
fn order_under_concurrent_writes_and_build() {
    let w = world(Layout::Tables, 0x5eed_4031);
    let qs: Vec<GroupedLiveQuery> =
        SPECS.iter().map(|&(city, root)| w.eng.subscribe_grouped(preds(&w, city, root)).unwrap()).collect();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writers: Vec<_> = (0..3u64)
        .map(|t| {
            let eng = w.eng.clone();
            let (users, comps) = (w.users.clone(), w.comps.clone());
            let (n_city, n_age, n_company) = (w.n_city, w.n_age, w.n_company);
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut rng = Rng(0xc0c0_0000_0000_0041 ^ (t + 1));
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let u = users[rng.below(users.len() as u64) as usize];
                    match rng.below(10) {
                        0..=1 => eng.tie_to(comps[rng.below(comps.len() as u64) as usize], n_city, rng.below(4) as u32),
                        2..=5 => eng.tie_to(u, n_age, (18 + rng.below(62)) as u32),
                        6..=8 => eng.tie_to(u, n_company, enchudb_oplog::eid_local(comps[rng.below(comps.len() as u64) as usize])),
                        _ => eng.untie(u, n_age),
                    }
                }
            })
        })
        .collect();
    std::thread::sleep(std::time::Duration::from_millis(5));
    declare(&w, &[30, 1001]);
    let t0 = std::time::Instant::now();
    while t0.elapsed() < std::time::Duration::from_millis(200) {
        for q in &qs {
            let _ = q.poll(&w.eng);
            let gs = q.groups(&w.eng);
            let _ = (q.members_many(&w.eng, &gs), q.count(&w.eng));
        }
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for t in writers {
        t.join().unwrap();
    }
    for (i, q) in qs.iter().enumerate() {
        let (city, root) = SPECS[i];
        let found = w.eng.find_by(preds(&w, city, root)).unwrap();
        assert_eq!(q.flatten(&w.eng), found, "購読 {i} {root:?}: flatten");
        assert_eq!(q.count(&w.eng), found.len(), "購読 {i} {root:?}: count");
        for g in q.groups(&w.eng) {
            let mut p = preds(&w, city, root);
            p.push(LivePred::Eq { himo_id: w.h_company, value: enchudb_oplog::eid_local(g) as u64 });
            let want = w.eng.find_by(p).unwrap();
            assert_eq!(q.members(&w.eng, g), want, "購読 {i} {root:?}: members({g})");
            assert_eq!(q.count_in(&w.eng, g), want.len(), "購読 {i} {root:?}: count_in({g})");
        }
    }
    assert!(order_hits(&w).0 > 0);
}

/// 宣言の検査: Ref でない via、 数でない key、 空・昇順でない目盛り、 同じ via への違う宣言は Err。 同じ宣言の繰り返しは Ok。
#[test]
fn declare_order_validates() {
    let w = world(Layout::Anon, 0x5eed_4041);
    assert!(w.eng.declare_order(w.n_age, w.n_age, &[30]).is_err(), "Ref でない via");
    assert!(w.eng.declare_order(w.n_company, w.n_dept, &[30]).is_err(), "Tag の key");
    assert!(w.eng.declare_order(w.n_company, w.n_age, &[]).is_err(), "空の目盛り");
    assert!(w.eng.declare_order(w.n_company, w.n_age, &[30, 30]).is_err(), "昇順でない目盛り");
    assert!(w.eng.declare_order(w.n_company, "no_such_himo", &[30]).is_err(), "無い紐");
    w.eng.declare_order(w.n_company, w.n_age, &[30, 1001]).unwrap();
    w.eng.declare_order(w.n_company, w.n_age, &[30, 1001]).unwrap();
    assert!(w.eng.declare_order(w.n_company, w.n_age, &[40]).is_err(), "同じ via に違う目盛り");
    assert!(w.eng.declare_order(w.n_company, w.n_role, &[1]).is_err(), "同じ via に違う key");
}

/// 円柱の大きさ (0 = 作っていない)。
fn company_cylinder_bytes(w: &World) -> usize {
    w.eng.himo_cylinder_backing_bytes(w.n_company).expect("company の紐")
}

/// 会社 `c` を指している社員 (local eid、 昇順) を shadow から。 `role` があれば役割でも絞る。
fn staff_of(w: &World, c: u64, role: Option<u64>) -> Vec<u32> {
    w.rows
        .iter()
        .filter(|(_, r)| r.company == Some(c) && role.is_none_or(|x| r.role == x))
        .map(|(&u, _)| enchudb_oplog::eid_local(u))
        .collect()
}

fn sorted_locals(v: impl IntoIterator<Item = u64>) -> Vec<u32> {
    let mut v: Vec<u32> = v.into_iter().map(enchudb_oplog::eid_local).collect();
    v.sort_unstable();
    v
}

/// C2: 並びの索引がある via の紐の逆引き (`pull` / `pull_in` / `query` の 1 条件と 2 条件 (件数で軸を選ぶ)) は索引から読み、
/// その紐の円柱を作らない。 書き込みのたびに shadow と比べる (会社を外した社員、 年齢の無い社員 = 帯 0 も入る)。
/// 負の対照: 宣言しない DB では同じ読みで円柱ができる。
#[test]
fn via_reads_use_the_order_index_and_leave_the_cylinder_unbuilt() {
    for (layout, seed) in [(Layout::Anon, 0x5eed_4051u64), (Layout::AnonUsersFirst, 0x5eed_4052), (Layout::Tables, 0x5eed_4053)] {
        let mut w = world(layout, seed);
        declare(&w, &[30, 1001]);
        assert_eq!(company_cylinder_bytes(&w), 0, "{layout:?}: 前提");
        let mut rng = Rng(seed ^ 0xc2);
        for round in 0..=150 {
            if round > 0 {
                for _ in 0..1 + rng.below(5) {
                    write_one(&mut w, &mut rng);
                }
            }
            for (k, &c) in w.comps.iter().enumerate() {
                let local = enchudb_oplog::eid_local(c);
                let mut got = w.eng.pull(w.n_company, local);
                got.sort_unstable();
                assert_eq!(got, staff_of(&w, c, None), "{layout:?} round {round}: pull({c})");
                let q = sorted_locals(w.eng.query(&[(w.n_company, local)]));
                assert_eq!(q, staff_of(&w, c, None), "{layout:?} round {round}: query 1 条件 ({c})");
                let q2 = sorted_locals(w.eng.query(&[(w.n_company, local), (w.n_role, 1)]));
                assert_eq!(q2, staff_of(&w, c, Some(1)), "{layout:?} round {round}: query 2 条件 ({c})");
                if k + 1 < w.comps.len() {
                    let d = w.comps[k + 1];
                    let both = sorted_locals(w.eng.pull_in(w.n_company, &[local, enchudb_oplog::eid_local(d)]));
                    let mut want = staff_of(&w, c, None);
                    want.extend(staff_of(&w, d, None));
                    want.sort_unstable();
                    assert_eq!(both, want, "{layout:?} round {round}: pull_in({c}, {d})");
                }
            }
        }
        assert!(order_hits(&w).0 > 0, "{layout:?}: 索引を読んでいない");
        assert_eq!(company_cylinder_bytes(&w), 0, "{layout:?}: 円柱を作った");
    }
    // 負の対照: 宣言しなければ、 同じ pull で円柱ができる (この test が円柱を作ったことを見分けられる)
    let w = world(Layout::Anon, 0x5eed_4054);
    let _ = w.eng.pull(w.n_company, enchudb_oplog::eid_local(w.comps[0]));
    assert!(company_cylinder_bytes(&w) > 0, "対照: 宣言なしの pull が円柱を作っていない");
}

/// C2: 購読 (社員単位 / 見直す場所だけ / 会社単位) を張っても、 書き込みと poll と find_by を回しても、 宣言した via の紐の
/// 円柱を作らない (購読の登録が ref の逆引きを先に作る所は索引を作る)。 答えは find_by と shadow の 2 経路で比べる。
/// 負の対照: 宣言しなければ社員単位の購読の登録で円柱ができる。
#[test]
fn subscriptions_and_find_by_leave_the_via_cylinder_unbuilt() {
    let mut w = world(Layout::Tables, 0x5eed_4061);
    declare(&w, &[30, 1001]);
    let row = w.eng.subscribe(preds(&w, 1, Root::Age(30, 1000))).unwrap();
    let touched = w.eng.subscribe_touched(preds(&w, 2, Root::Age(30, 1000))).unwrap();
    let grouped = w.eng.subscribe_grouped(preds(&w, 3, Root::None)).unwrap();
    assert_eq!(company_cylinder_bytes(&w), 0, "購読の登録が円柱を作った");
    let mut rng = Rng(0x5eed_4062);
    let mut seen = BTreeSet::new();
    for round in 0..=150 {
        if round > 0 {
            for _ in 0..1 + rng.below(5) {
                write_one(&mut w, &mut rng);
            }
        }
        integrate(&mut seen, row.poll(&w.eng));
        let _ = touched.poll(&w.eng);
        let _ = grouped.poll(&w.eng);
        let found: BTreeSet<u64> = w.eng.find_by(preds(&w, 1, Root::Age(30, 1000))).unwrap().into_iter().collect();
        let want: BTreeSet<u64> = w
            .rows
            .iter()
            .filter(|(_, r)| r.company.is_some_and(|c| w.city.get(&c) == Some(&1)) && root_holds(r, Root::Age(30, 1000)))
            .map(|(&u, _)| u)
            .collect();
        assert_eq!(found, want, "round {round}: find_by");
        assert_eq!(seen, want, "round {round}: 社員単位の購読");
        let in_city3 = w.rows.values().filter(|r| r.company.is_some_and(|c| w.city.get(&c) == Some(&3))).count();
        assert_eq!(grouped.count(&w.eng), in_city3, "round {round}: 会社単位の count (根の条件なし = 帯の件数の和)");
    }
    assert!(order_hits(&w).0 > 0);
    assert_eq!(company_cylinder_bytes(&w), 0, "書き込みと読みの間に円柱を作った");
    let c = world(Layout::Tables, 0x5eed_4063);
    let _q = c.eng.subscribe(preds(&c, 1, Root::Age(30, 1000))).unwrap();
    assert!(company_cylinder_bytes(&c) > 0, "対照: 宣言なしの社員単位の購読が円柱を作っていない");
}

/// C2: find_by の Via + 範囲は、 会社から社員へ降りる段で範囲に当たる帯だけ読む。 書き込み前 (stale なし) なら、 読んだ
/// entity の数 = 答えの数、 読んだ回数 = city の合う会社の数。 対照: 範囲の条件が無いと、 その会社の全員を読む。
#[test]
fn find_by_via_and_range_reads_only_the_range_bands() {
    let w = world(Layout::Anon, 0x5eed_4071);
    declare(&w, &[30, 1001]);
    let n_comp = w.city.values().filter(|&&v| v == 1).count() as u64;
    let (h0, r0) = order_hits(&w);
    let got = w.eng.find_by(preds(&w, 1, Root::Age(30, 1000))).unwrap();
    let (h1, r1) = order_hits(&w);
    let want: Vec<u64> = w
        .rows
        .iter()
        .filter(|(_, r)| r.company.is_some_and(|c| w.city.get(&c) == Some(&1)) && root_holds(r, Root::Age(30, 1000)))
        .map(|(&u, _)| u)
        .collect();
    assert_eq!(got, want);
    assert!(!got.is_empty() && n_comp > 0, "前提");
    assert_eq!((h1 - h0, r1 - r0), (n_comp, got.len() as u64), "範囲の帯だけ読むはず");
    let all = w.eng.find_by(preds(&w, 1, Root::None)).unwrap();
    let (h2, r2) = order_hits(&w);
    assert!(all.len() > got.len(), "前提: 範囲の外の社員が居る");
    assert_eq!((h2 - h1, r2 - r1), (n_comp, all.len() as u64), "対照: 範囲の条件が無ければ全員を読む");
}
