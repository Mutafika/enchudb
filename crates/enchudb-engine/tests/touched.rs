//! 見直す場所だけを返す購読 (`subscribe_touched`) の検査。
//!
//! 購読は 「ref の先 (会社) の city が c」 + 根 (社員) への条件 (年齢の範囲・役割・部署の語・否定・`In`・`Present`)。
//! アプリの持ち方 (社員 → 会社 の表) を、 poll ごとに 「出た会社の社員を外す → 入った会社の社員を `members_many` で
//! 足す → `touched` の社員を `group_of` で引き直す」 で進め、 毎回 shadow から数えた答え (社員 → 会社) と `find_by` の
//! 2 経路に一致することを見る。 `touched` が 「前回 poll 以降に見る紐 (所属会社の ref と根への条件の列) が書かれた社員」 と
//! ちょうど同じことも見る (足りなければ表がずれる、 余計なら見直しが無駄に増える)。 会社を匿名表に先に作る並べ方と、
//! 会社の eid を 2^20 の先に置く並べ方の両方で回す。

use enchudb_engine::{Engine, GrowableOptions, LivePred, TouchedDelta, TouchedLiveQuery, ValueType};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    // 並行に走る test が同じ時刻を引いても衝突しないよう、 通し番号も付ける
    static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/touched_{}_{}_{}_{}.enchu", tag, std::process::id(), nanos, n)
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

/// 社員の列 (見る紐かどうかを決める)。
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Col {
    Company,
    Age,
    Role,
    Dept,
}

/// 購読が見る紐 (所属会社の ref と、 根への条件の列)。
fn notice_cols(root: Root) -> &'static [Col] {
    match root {
        Root::None => &[Col::Company],
        Root::Age(..) | Root::AgePresent => &[Col::Company, Col::Age],
        Root::AgeAndRole(..) => &[Col::Company, Col::Age, Col::Role],
        Root::Dept(_) | Root::DeptUnknown => &[Col::Company, Col::Dept],
        Root::RoleIn | Root::NotRole(_) => &[Col::Company, Col::Role],
    }
}

/// 書き込みを 1 つ (shadow も同じに直す)。 書いた / 外した (社員, 列) を `wrote` に積む (同じ値の書き直しも、 値の無い列を
/// 外すのも 「書いた」。 削除は値のあった列ぶん)。
fn write_one(w: &mut World, rng: &mut Rng, wrote: &mut Vec<(u64, Col)>) {
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
            wrote.push((u, Col::Age));
        }
        9 => {
            let (_, u) = pick_user(w, rng);
            w.eng.untie(u, w.n_age);
            w.rows.get_mut(&u).unwrap().age = None;
            wrote.push((u, Col::Age));
        }
        10..=13 => {
            let (_, u) = pick_user(w, rng);
            let c = w.comps[rng.below(w.comps.len() as u64) as usize];
            w.eng.tie_to(u, w.n_company, enchudb_oplog::eid_local(c));
            w.rows.get_mut(&u).unwrap().company = Some(c);
            wrote.push((u, Col::Company));
        }
        14..=15 => {
            let (_, u) = pick_user(w, rng);
            let r = rng.below(3);
            w.eng.tie_to(u, w.n_role, r as u32);
            w.rows.get_mut(&u).unwrap().role = r;
            wrote.push((u, Col::Role));
        }
        16 => {
            let (_, u) = pick_user(w, rng);
            let d = rng.below(3) as usize;
            w.eng.tie_text_to(u, w.n_dept, DEPTS[d]);
            w.rows.get_mut(&u).unwrap().dept = d;
            wrote.push((u, Col::Dept));
        }
        17 => {
            let (_, u) = pick_user(w, rng);
            w.eng.untie(u, w.n_company);
            w.rows.get_mut(&u).unwrap().company = None;
            wrote.push((u, Col::Company));
        }
        18 => {
            let (i, u) = pick_user(w, rng);
            let r = w.rows[&u];
            w.eng.delete(u);
            // 削除は値のある列を 1 つずつ外してから slot を解放する
            if r.company.is_some() {
                wrote.push((u, Col::Company));
            }
            if r.age.is_some() {
                wrote.push((u, Col::Age));
            }
            wrote.push((u, Col::Role));
            wrote.push((u, Col::Dept));
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
            wrote.extend([(u, Col::Company), (u, Col::Age), (u, Col::Role), (u, Col::Dept)]);
        }
    }
}

fn pick_user(w: &World, rng: &mut Rng) -> (usize, u64) {
    let i = rng.below(w.users.len() as u64) as usize;
    (i, w.users[i])
}

/// アプリの持ち方を 1 回進める: 出た会社の社員を外し、 入った会社の社員を足し、 見直しの社員を引き直す。
/// `use_touched = false` は負の対照 (見直しを無視するアプリ)。
fn apply(view: &mut BTreeMap<u64, u64>, q: &TouchedLiveQuery, eng: &Engine, d: TouchedDelta, use_touched: bool) {
    if !d.removed.is_empty() {
        let gone: BTreeSet<u64> = d.removed.iter().copied().collect();
        view.retain(|_, g| !gone.contains(g));
    }
    for (&g, staff) in d.added.iter().zip(q.members_many(eng, &d.added)) {
        for u in staff {
            view.insert(u, g);
        }
    }
    if use_touched {
        for u in d.touched {
            match q.group_of(eng, u) {
                Some(g) => {
                    view.insert(u, g);
                }
                None => {
                    view.remove(&u);
                }
            }
        }
    }
}

/// shadow から数えた答え (社員 → 会社)。
fn want_view(w: &World, city: u64, root: Root) -> BTreeMap<u64, u64> {
    w.rows
        .iter()
        .filter_map(|(&u, r)| {
            let c = r.company?;
            (w.city.get(&c) == Some(&city) && root_holds(r, root)).then_some((u, c))
        })
        .collect()
}

fn run(layout: Layout, seed: u64, use_touched: bool) {
    let mut w = world(layout, seed);
    let qs: Vec<TouchedLiveQuery> =
        SPECS.iter().map(|&(city, root)| w.eng.subscribe_touched(preds(&w, city, root)).unwrap()).collect();
    let mut views = vec![BTreeMap::new(); qs.len()];
    let mut rng = Rng(seed ^ 0x70c4);
    for round in 0..=300 {
        let mut wrote = Vec::new();
        if round > 0 {
            for _ in 0..1 + rng.below(5) {
                write_one(&mut w, &mut rng, &mut wrote);
            }
        }
        for (i, q) in qs.iter().enumerate() {
            let (city, root) = SPECS[i];
            let d = q.poll(&w.eng);
            if round > 0 {
                let cols = notice_cols(root);
                let want_t: BTreeSet<u64> = wrote.iter().filter(|(_, c)| cols.contains(c)).map(|&(u, _)| u).collect();
                let got_t: BTreeSet<u64> = d.touched.iter().copied().collect();
                assert_eq!(got_t, want_t, "round {round} 購読 {i} {root:?}: touched");
                assert_eq!(d.touched.len(), got_t.len(), "round {round} 購読 {i}: touched に重複");
            }
            apply(&mut views[i], q, &w.eng, d, use_touched);
            let want = want_view(&w, city, root);
            assert_eq!(views[i], want, "round {round} 購読 {i} {root:?}: アプリの表 (社員 → 会社)");
            let found = w.eng.find_by(preds(&w, city, root)).unwrap();
            assert_eq!(views[i].keys().copied().collect::<Vec<_>>(), found, "round {round} 購読 {i} {root:?}: find_by");
            for &u in want.keys().take(5) {
                assert!(q.contains(&w.eng, u), "round {round} 購読 {i}: contains({u})");
            }
        }
    }
}

#[test]
fn touched_keeps_the_app_view_with_dense_company_lists() {
    run(Layout::Anon, 0x5eed_3001, true);
}

#[test]
fn touched_keeps_the_app_view_with_sparse_company_lists() {
    run(Layout::Tables, 0x5eed_3002, true);
}

/// 負の対照: `touched` を無視するアプリは、 社員の異動・年齢の変化・削除を取りこぼして表がずれる。
#[test]
#[should_panic(expected = "round")]
fn touched_negative_control_app_ignoring_touched_drifts() {
    run(Layout::Anon, 0x5eed_3003, false);
}

/// 中身を数える条件 (`Exists` 系) と、 ref の先の条件の分かれ (`Or`) は断る。 断った後の engine はそのまま使える。
#[test]
fn touched_rejects_conditions_it_cannot_watch() {
    let w = world(Layout::Anon, 0x5eed_3004);
    let mut p = preds(&w, 1, Root::None);
    p.push(LivePred::Exists { via: w.h_company, preds: vec![LivePred::Eq { himo_id: w.h_city, value: 1 }] });
    assert!(w.eng.subscribe_touched(p).is_err(), "Exists を根への条件に取った");
    let or = vec![LivePred::Via {
        path: vec![w.h_company],
        pred: Box::new(LivePred::Or(vec![
            vec![LivePred::Eq { himo_id: w.h_city, value: 1 }],
            vec![LivePred::Eq { himo_id: w.h_city, value: 2 }],
        ])),
    }];
    assert!(w.eng.subscribe_touched(or).is_err(), "ref の先の Or を取った");
    let q = w.eng.subscribe_touched(preds(&w, 1, Root::Age(30, 1000))).unwrap();
    let d = q.poll(&w.eng);
    assert!(!d.added.is_empty(), "対照: 断った後の購読も動く");
}

/// 書き手 3 本と並行に poll してアプリの表を進め、 止めた後の 1 回で `find_by` と一致する。
#[test]
fn touched_keeps_the_app_view_under_concurrent_writes() {
    let w = world(Layout::Tables, 0x5eed_3005);
    let qs: Vec<TouchedLiveQuery> =
        SPECS.iter().map(|&(city, root)| w.eng.subscribe_touched(preds(&w, city, root)).unwrap()).collect();
    let mut views = vec![BTreeMap::new(); qs.len()];
    for round in 0..8u64 {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writers: Vec<_> = (0..3u64)
            .map(|t| {
                let eng = w.eng.clone();
                let (users, comps) = (w.users.clone(), w.comps.clone());
                let (n_city, n_age, n_company, n_role) = (w.n_city, w.n_age, w.n_company, w.n_role);
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let mut rng = Rng(0xc0c0_0000_0000_0031 ^ (round * 3 + t + 1));
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        let u = users[rng.below(users.len() as u64) as usize];
                        match rng.below(10) {
                            0..=2 => eng.tie_to(comps[rng.below(comps.len() as u64) as usize], n_city, rng.below(4) as u32),
                            3..=4 => eng.tie_to(u, n_age, (18 + rng.below(62)) as u32),
                            5 => eng.tie_to(u, n_role, rng.below(3) as u32),
                            6..=8 => eng.tie_to(u, n_company, enchudb_oplog::eid_local(comps[rng.below(comps.len() as u64) as usize])),
                            _ => eng.untie(u, n_age),
                        }
                    }
                })
            })
            .collect();
        let t0 = std::time::Instant::now();
        while t0.elapsed() < std::time::Duration::from_millis(40) {
            for (i, q) in qs.iter().enumerate() {
                let d = q.poll(&w.eng);
                apply(&mut views[i], q, &w.eng, d, true);
            }
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for t in writers {
            t.join().unwrap();
        }
        for (i, q) in qs.iter().enumerate() {
            let (city, root) = SPECS[i];
            let d = q.poll(&w.eng);
            apply(&mut views[i], q, &w.eng, d, true);
            let found = w.eng.find_by(preds(&w, city, root)).unwrap();
            if views[i].keys().copied().collect::<Vec<_>>() != found {
                // 調べる: 表にだけ居る / find_by にだけ居る社員の今の状態
                let fs: BTreeSet<u64> = found.iter().copied().collect();
                let groups: BTreeSet<u64> = q.groups(&w.eng).into_iter().collect();
                for (&u, &g) in views[i].iter().filter(|(u, _)| !fs.contains(u)).take(5) {
                    let c = w.eng.get_by_id(u, w.h_company);
                    eprintln!(
                        "表にだけ: u={u} 表の会社={g} (今 group? {}) 今の会社 (local)={c:?} (group? {}) 年齢={:?} group_of={:?}",
                        groups.contains(&g),
                        c.is_some_and(|c| groups.iter().any(|&x| enchudb_oplog::eid_local(x) as u64 == c)),
                        w.eng.get_by_id(u, w.h_age),
                        q.group_of(&w.eng, u)
                    );
                }
                for &u in found.iter().filter(|u| !views[i].contains_key(u)).take(5) {
                    eprintln!("find_by にだけ: u={u} group_of={:?}", q.group_of(&w.eng, u));
                }
            }
            assert_eq!(views[i].keys().copied().collect::<Vec<_>>(), found, "round {round} 購読 {i} {root:?}: find_by");
            for (&u, &g) in &views[i] {
                assert_eq!(q.group_of(&w.eng, u), Some(g), "round {round} 購読 {i}: 表の会社が今の会社と違う ({u})");
            }
        }
    }
}
