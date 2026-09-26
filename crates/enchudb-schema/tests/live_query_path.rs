//! ref をたどる live query (`where_eq("company.city", ..)`): poll の差分を積分した集合が、
//! **テスト側で ref を 1 段ずつ手でたどって計算した結果** と常に一致すること。
//!
//! oracle は `entity(e).get(col)` で ref を読んで先の row を読むだけ — engine の live 評価
//! (候補の逆引き / 真偽の記録値 / 展開) を一切通らない。 `find()` も同じ oracle と比べる
//! (`find()` は ref 条件の時 engine の評価を使うので、 oracle 代わりにはしない)。
//!
//! 各条件は 「真偽が変わった hub だけ展開」 (既定) と 「常に展開」 (ablation) の 2 本を
//! 購読して、 両方とも oracle と一致することを見る。

use enchudb_schema::{Database, GroupedLiveQuery, LiveDelta, LiveQuery, Table, Value};
use std::collections::BTreeSet;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/tmp/schema_live_path_{}_{}_{}.db", tag, std::process::id(), nanos)
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

fn get_ref(t: &Table, e: u64, col: &str) -> Option<u64> {
    match t.entity(e).get(col) {
        Some(Value::Ref(x)) => Some(x),
        _ => None,
    }
}

fn get_text(t: &Table, e: u64, col: &str) -> Option<String> {
    match t.entity(e).get(col) {
        Some(Value::Text(s)) => Some(s),
        _ => None,
    }
}

fn get_num(t: &Table, e: u64, col: &str) -> Option<i64> {
    match t.entity(e).get(col) {
        Some(Value::Number(n)) => Some(n),
        _ => None,
    }
}

struct Tables<'a> {
    users: Table<'a>,
    companies: Table<'a>,
    regions: Table<'a>,
    depts: Table<'a>,
}

impl Tables<'_> {
    fn city(&self, u: u64) -> Option<String> {
        get_text(&self.companies, get_ref(&self.users, u, "company")?, "city")
    }
    fn region_name(&self, u: u64) -> Option<String> {
        let c = get_ref(&self.users, u, "company")?;
        get_text(&self.regions, get_ref(&self.companies, c, "region")?, "name")
    }
    fn dept_name(&self, u: u64) -> Option<String> {
        get_text(&self.depts, get_ref(&self.users, u, "dept")?, "name")
    }
}

type Oracle = Box<dyn Fn(&Tables, u64) -> bool>;
type QueryFn<'a> = Box<dyn Fn() -> enchudb_schema::Query<'a> + 'a>;

struct Sub {
    name: &'static str,
    fast: LiveQuery,
    naive: LiveQuery,
    seen_fast: BTreeSet<u64>,
    seen_naive: BTreeSet<u64>,
    /// 途中で一度でも結果が空でなかったか (一度も当たらない条件は試験にならない)。
    ever_hit: bool,
    oracle: Oracle,
}

#[test]
fn ref_path_subscriptions_match_hand_followed_oracle() {
    let path = tmp_path("chain");
    cleanup(&path);
    run_chain(&path);
    cleanup(&path);
}

fn run_chain(path: &str) {
    let mut db = Database::create_growable_tiny(path).unwrap();
    db.table("regions").number("id").tag("name").primary_key("id").build().unwrap();
    db.table("companies")
        .number("id")
        .tag("city")
        .ref_to("region", "regions")
        .primary_key("id")
        .build()
        .unwrap();
    db.table("depts").number("id").tag("name").primary_key("id").build().unwrap();
    db.table("users")
        .number("id")
        .number("age")
        .ref_to("company", "companies")
        .ref_to("dept", "depts")
        .primary_key("id")
        .build()
        .unwrap();
    let t = Tables {
        users: db.get_table("users").unwrap(),
        companies: db.get_table("companies").unwrap(),
        regions: db.get_table("regions").unwrap(),
        depts: db.get_table("depts").unwrap(),
    };
    let region_names = ["Kanto", "Kansai", "Kyushu"];
    let cities = ["Tokyo", "Osaka", "Fukuoka", "Kyoto"];
    let dept_names = ["Sales", "Dev", "Ops"];
    let mut rng = Rng(0x1234_5678_9abc_def1);

    let mut regions: Vec<u64> = (0..4)
        .map(|i| t.regions.insert().set("id", i as i64).set("name", region_names[i % 3]).commit().unwrap())
        .collect();
    let mut companies: Vec<u64> = (0..12i64)
        .map(|i| {
            t.companies
                .insert()
                .set("id", i)
                .set("city", cities[(i % 4) as usize])
                .set("region", Value::Ref(regions[(i % 4) as usize]))
                .commit()
                .unwrap()
        })
        .collect();
    let depts: Vec<u64> = (0..3)
        .map(|i| t.depts.insert().set("id", i as i64).set("name", dept_names[i]).commit().unwrap())
        .collect();
    let mut users: Vec<u64> = (0..300i64)
        .map(|i| {
            t.users
                .insert()
                .set("id", i)
                .set("age", 20 + (i % 30))
                .set("company", Value::Ref(companies[(i % 12) as usize]))
                .set("dept", Value::Ref(depts[(i % 3) as usize]))
                .commit()
                .unwrap()
        })
        .collect();

    let u = &t.users;
    let queries: Vec<(&'static str, QueryFn<'_>, Oracle)> = vec![
        (
            "company.city=Tokyo",
            Box::new(move || u.where_eq("company.city", "Tokyo")),
            Box::new(|t, e| t.city(e).as_deref() == Some("Tokyo")),
        ),
        (
            "company.city=Tokyo AND age>30",
            Box::new(move || u.where_eq("company.city", "Tokyo").where_gt("age", 30)),
            Box::new(|t, e| t.city(e).as_deref() == Some("Tokyo") && get_num(&t.users, e, "age").is_some_and(|a| a > 30)),
        ),
        (
            "company.region.name=Kanto (2 段)",
            Box::new(move || u.where_eq("company.region.name", "Kanto")),
            Box::new(|t, e| t.region_name(e).as_deref() == Some("Kanto")),
        ),
        (
            "company.city=Osaka AND company.region.name=Kansai (同じ道の 2 段)",
            Box::new(move || u.where_eq("company.city", "Osaka").where_eq("company.region.name", "Kansai")),
            Box::new(|t, e| t.city(e).as_deref() == Some("Osaka") && t.region_name(e).as_deref() == Some("Kansai")),
        ),
        (
            "company.city=Tokyo AND dept.name=Sales (分岐)",
            Box::new(move || u.where_eq("company.city", "Tokyo").where_eq("dept.name", "Sales")),
            Box::new(|t, e| t.city(e).as_deref() == Some("Tokyo") && t.dept_name(e).as_deref() == Some("Sales")),
        ),
    ];
    let mut subs = Vec::new();
    let mut finds = Vec::new();
    for (name, q, oracle) in queries {
        let fast = q().subscribe().unwrap();
        let naive = q().subscribe_expand_always().unwrap();
        subs.push(Sub { name, fast, naive, seen_fast: BTreeSet::new(), seen_naive: BTreeSet::new(), ever_hit: false, oracle });
        finds.push(q);
    }

    // group (会社) 単位の購読: 差分 = 会社、 社員は members で逆引き
    struct GSub {
        name: &'static str,
        q: GroupedLiveQuery,
        groups: BTreeSet<u64>,
        oracle: Oracle,
    }
    let grouped: Vec<(&'static str, QueryFn<'_>, Oracle)> = vec![
        (
            "grouped company.city=Tokyo",
            Box::new(move || u.where_eq("company.city", "Tokyo")),
            Box::new(|t, e| t.city(e).as_deref() == Some("Tokyo")),
        ),
        (
            "grouped company.city=Tokyo AND age>30",
            Box::new(move || u.where_eq("company.city", "Tokyo").where_gt("age", 30)),
            Box::new(|t, e| t.city(e).as_deref() == Some("Tokyo") && get_num(&t.users, e, "age").is_some_and(|a| a > 30)),
        ),
        (
            "grouped company.region.name=Kanto (2 段)",
            Box::new(move || u.where_eq("company.region.name", "Kanto")),
            Box::new(|t, e| t.region_name(e).as_deref() == Some("Kanto")),
        ),
        (
            "grouped company.city=Osaka AND company.region.name=Kansai",
            Box::new(move || u.where_eq("company.city", "Osaka").where_eq("company.region.name", "Kansai")),
            Box::new(|t, e| t.city(e).as_deref() == Some("Osaka") && t.region_name(e).as_deref() == Some("Kansai")),
        ),
    ];
    let mut gsubs: Vec<GSub> = grouped
        .into_iter()
        .map(|(name, q, oracle)| GSub { name, q: q().subscribe_grouped().unwrap(), groups: BTreeSet::new(), oracle })
        .collect();
    assert!(
        u.where_eq("company.city", "Tokyo").where_eq("dept.name", "Sales").subscribe_grouped().is_err(),
        "別の ref から始まる条件は group 化できない"
    );
    assert!(u.where_eq("age", 30i64).subscribe_grouped().is_err(), "ref をたどらない条件は group 化できない");

    let gcheck = |gsubs: &mut Vec<GSub>, users: &[u64], companies: &[u64], step: usize| {
        for g in gsubs.iter_mut() {
            integrate(&mut g.groups, g.q.poll());
            for c in companies.iter().filter(|c| !g.groups.contains(c)) {
                assert!(g.q.members(*c).is_empty(), "[{}] step {step}: 条件を満たさない会社の members", g.name);
            }
            assert_eq!(g.groups, g.q.groups().into_iter().collect(), "[{}] step {step}: group の積分 != groups()", g.name);
            let flat: BTreeSet<u64> = g.groups.iter().flat_map(|&c| g.q.members(c)).collect();
            let want: BTreeSet<u64> = users.iter().copied().filter(|&e| (g.oracle)(&t, e)).collect();
            assert_eq!(flat, want, "[{}] step {step}: group の members の和 != 手でたどった結果", g.name);
            assert_eq!(g.q.count(), want.len(), "[{}] step {step}: count", g.name);
            assert_eq!(g.q.flatten().into_iter().collect::<BTreeSet<_>>(), want, "[{}] step {step}: flatten", g.name);
        }
    };
    gcheck(&mut gsubs, &users, &companies, 0);

    let check = |subs: &mut Vec<Sub>, users: &[u64], step: usize| {
        for (s, find) in subs.iter_mut().zip(&finds) {
            integrate(&mut s.seen_fast, s.fast.poll());
            integrate(&mut s.seen_naive, s.naive.poll());
            let want: BTreeSet<u64> = users.iter().copied().filter(|&e| (s.oracle)(&t, e)).collect();
            s.ever_hit |= !want.is_empty();
            assert_eq!(s.seen_fast, want, "[{}] step {step}: 積分 (既定) != 手でたどった結果", s.name);
            assert_eq!(s.seen_naive, want, "[{}] step {step}: 積分 (常に展開) != 手でたどった結果", s.name);
            assert_eq!(s.fast.count(), want.len(), "[{}] step {step}: count", s.name);
            let found: BTreeSet<u64> = find().find().unwrap().into_iter().collect();
            assert_eq!(found, want, "[{}] step {step}: find() != 手でたどった結果", s.name);
        }
    };
    check(&mut subs, &users, 0);

    let mut next_id = 1000i64;
    for step in 1..1500 {
        match rng.below(12) {
            0 | 1 => {
                let c = companies[rng.below(companies.len() as u64) as usize];
                t.companies.entity(c).set("city", cities[rng.below(4) as usize]).commit().unwrap();
            }
            2 => {
                let c = companies[rng.below(companies.len() as u64) as usize];
                let r = regions[rng.below(regions.len() as u64) as usize];
                t.companies.entity(c).set("region", Value::Ref(r)).commit().unwrap();
            }
            3 => {
                let r = regions[rng.below(regions.len() as u64) as usize];
                t.regions.entity(r).set("name", region_names[rng.below(3) as usize]).commit().unwrap();
            }
            4 | 5 => {
                let e = users[rng.below(users.len() as u64) as usize];
                let c = companies[rng.below(companies.len() as u64) as usize];
                t.users.entity(e).set("company", Value::Ref(c)).commit().unwrap();
            }
            6 => {
                let e = users[rng.below(users.len() as u64) as usize];
                t.users.entity(e).set("age", 20 + rng.below(30) as i64).commit().unwrap();
            }
            7 => {
                let d = depts[rng.below(depts.len() as u64) as usize];
                t.depts.entity(d).set("name", dept_names[rng.below(3) as usize]).commit().unwrap();
            }
            8 => {
                let e = users[rng.below(users.len() as u64) as usize];
                let d = depts[rng.below(depts.len() as u64) as usize];
                t.users.entity(e).set("dept", Value::Ref(d)).commit().unwrap();
            }
            9 => {
                // user の入れ替え
                let i = rng.below(users.len() as u64) as usize;
                t.users.entity(users[i]).delete().unwrap();
                users[i] = t
                    .users
                    .insert()
                    .set("id", next_id)
                    .set("age", 20 + rng.below(30) as i64)
                    .set("company", Value::Ref(companies[rng.below(companies.len() as u64) as usize]))
                    .set("dept", Value::Ref(depts[rng.below(depts.len() as u64) as usize]))
                    .commit()
                    .unwrap();
                next_id += 1;
            }
            10 => {
                // 会社の入れ替え (旧会社を指す user の ref は宙に浮く = 条件は偽)
                let i = rng.below(companies.len() as u64) as usize;
                t.companies.entity(companies[i]).delete().unwrap();
                companies[i] = t
                    .companies
                    .insert()
                    .set("id", next_id)
                    .set("city", cities[rng.below(4) as usize])
                    .set("region", Value::Ref(regions[rng.below(regions.len() as u64) as usize]))
                    .commit()
                    .unwrap();
                next_id += 1;
            }
            _ => {
                // region の入れ替え
                let i = rng.below(regions.len() as u64) as usize;
                t.regions.entity(regions[i]).delete().unwrap();
                regions[i] = t
                    .regions
                    .insert()
                    .set("id", next_id)
                    .set("name", region_names[rng.below(3) as usize])
                    .commit()
                    .unwrap();
                next_id += 1;
            }
        }
        if step % 3 == 0 {
            check(&mut subs, &users, step);
            gcheck(&mut gsubs, &users, &companies, step);
        }
    }
    check(&mut subs, &users, usize::MAX);
    gcheck(&mut gsubs, &users, &companies, usize::MAX);
    for s in &subs {
        assert!(s.ever_hit, "[{}] 一度も当たらない条件は試験になっていない", s.name);
    }
}

#[test]
fn dotted_column_errors() {
    let path = tmp_path("err");
    cleanup(&path);
    let mut db = Database::create_growable_tiny(&path).unwrap();
    db.table("companies").number("id").tag("city").primary_key("id").build().unwrap();
    db.table("users").number("id").number("age").ref_to("company", "companies").primary_key("id").build().unwrap();
    let users = db.get_table("users").unwrap();
    assert!(users.where_eq("company.nope", "x").subscribe().is_err(), "ref 先に無い列");
    assert!(users.where_eq("age.city", "x").subscribe().is_err(), "ref でない列をたどる");
    assert!(users.where_eq("company.city", 3i64).subscribe().is_err(), "ref 先の列と型が合わない");
    assert_eq!(users.where_eq("company.nope", "x").find().unwrap(), Vec::<u64>::new());
    drop(users);
    drop(db);
    cleanup(&path);
}

/// 形が同じで値だけ違う購読 (engine 内では 1 本の family に束ねられる) を多数張り、 途中で
/// 購読の解除 / 追加を混ぜても、 各購読の積分が手でたどった結果と一致すること。
///
/// - 穴 1 個 (`company.city = ?`)、 まだ誰も書いていない値 (`Nagoya`) を含む
/// - 穴 2 個の分岐 (`company.city = ? AND dept.name = ?`)、 根の穴 (`age = ?`) + ref の穴
/// - 同じ条件の重複購読 (後から加わった方も初回 poll で全件を受け取る)
#[test]
fn shared_shape_subscriptions_match_oracle() {
    let path = tmp_path("family");
    cleanup(&path);
    run_family(&path);
    cleanup(&path);
}

struct FamSub {
    name: String,
    q: LiveQuery,
    seen: BTreeSet<u64>,
    oracle: Oracle,
}

fn run_family(path: &str) {
    let mut db = Database::create_growable_tiny(path).unwrap();
    db.table("regions").number("id").tag("name").primary_key("id").build().unwrap();
    db.table("companies")
        .number("id")
        .tag("city")
        .ref_to("region", "regions")
        .primary_key("id")
        .build()
        .unwrap();
    db.table("depts").number("id").tag("name").primary_key("id").build().unwrap();
    db.table("users")
        .number("id")
        .number("age")
        .ref_to("company", "companies")
        .ref_to("dept", "depts")
        .primary_key("id")
        .build()
        .unwrap();
    let t = Tables {
        users: db.get_table("users").unwrap(),
        companies: db.get_table("companies").unwrap(),
        regions: db.get_table("regions").unwrap(),
        depts: db.get_table("depts").unwrap(),
    };
    // Nagoya は途中まで誰も書かない (vocab に無い値の購読)。 Sendai は最初から書くが、 購読は
    // 途中の入れ替えで初めて張られる (購読の無い値として記録された hub が後から購読される)
    let cities = ["Tokyo", "Osaka", "Fukuoka", "Kyoto", "Nagoya", "Sendai"];
    let dept_names = ["Sales", "Dev", "Ops"];
    let mut rng = Rng(0x0fed_cba9_8765_4321);
    let mut companies: Vec<u64> = (0..10i64)
        .map(|i| t.companies.insert().set("id", i).set("city", cities[(i % 4) as usize]).commit().unwrap())
        .collect();
    let depts: Vec<u64> = (0..3)
        .map(|i| t.depts.insert().set("id", i as i64).set("name", dept_names[i]).commit().unwrap())
        .collect();
    let mut users: Vec<u64> = (0..200i64)
        .map(|i| {
            t.users
                .insert()
                .set("id", i)
                .set("age", 20 + (i % 5))
                .set("company", Value::Ref(companies[(i % 10) as usize]))
                .set("dept", Value::Ref(depts[(i % 3) as usize]))
                .commit()
                .unwrap()
        })
        .collect();

    let u = &t.users;
    // 購読の種類: 0 = city、 1 = city × dept、 2 = age × city
    let make = |kind: u64, a: usize, b: usize| -> FamSub {
        let (city, dept, age) = (cities[a % 6], dept_names[b % 3], 20 + (b % 5) as i64);
        match kind {
            0 => FamSub {
                name: format!("company.city={city}"),
                q: u.where_eq("company.city", city).subscribe().unwrap(),
                seen: BTreeSet::new(),
                oracle: Box::new(move |t, e| t.city(e).as_deref() == Some(city)),
            },
            1 => FamSub {
                name: format!("company.city={city} AND dept.name={dept}"),
                q: u.where_eq("dept.name", dept).where_eq("company.city", city).subscribe().unwrap(),
                seen: BTreeSet::new(),
                oracle: Box::new(move |t, e| t.city(e).as_deref() == Some(city) && t.dept_name(e).as_deref() == Some(dept)),
            },
            _ => FamSub {
                name: format!("age={age} AND company.city={city}"),
                q: u.where_eq("company.city", city).where_eq("age", age).subscribe().unwrap(),
                seen: BTreeSet::new(),
                oracle: Box::new(move |t, e| {
                    t.city(e).as_deref() == Some(city) && get_num(&t.users, e, "age") == Some(age)
                }),
            },
        }
    };
    let mut subs: Vec<FamSub> = Vec::new();
    for a in 0..5 {
        subs.push(make(0, a, 0));
        for b in 0..3 {
            subs.push(make(1, a, b));
        }
        for b in 0..5 {
            subs.push(make(2, a, b));
        }
    }
    // 重複 (同じ鍵の member が 2 本)
    subs.push(make(0, 0, 0));
    subs.push(make(1, 1, 1));

    // 奇数回は束の poll (出入りのあった購読だけ) で受け取り、 偶数回は 1 本ずつ poll
    // 購読は束に入れて束の poll で受け取る (入れ直しは何度でもよい)
    let group = db.live_group();
    let check = |subs: &mut Vec<FamSub>, users: &[u64], step: usize| {
        if !step.is_multiple_of(2) {
            let deltas = {
                for s in subs.iter() {
                    group.add(&s.q);
                }
                group.poll()
            };
            assert!(deltas.windows(2).all(|w| w[0].0 < w[1].0), "id 昇順・重複なし");
            for (id, d) in deltas {
                assert!(!d.is_empty(), "空の差分は返さない");
                let s = subs.iter_mut().find(|s| s.q.id() == id).expect("生きている購読の id");
                integrate(&mut s.seen, d);
            }
        }
        for s in subs.iter_mut() {
            if step.is_multiple_of(2) {
                integrate(&mut s.seen, s.q.poll());
            }
            let want: BTreeSet<u64> = users.iter().copied().filter(|&e| (s.oracle)(&t, e)).collect();
            assert_eq!(s.seen, want, "[{}] step {step}: 積分 != 手でたどった結果", s.name);
            assert!(s.q.poll().is_empty(), "[{}] step {step}: 受け取り済みの差分がまた届く", s.name);
            assert_eq!(s.q.count(), want.len(), "[{}] step {step}: count", s.name);
        }
    };
    check(&mut subs, &users, 0);

    let mut next_id = 1000i64;
    let mut nagoya_hit = false;
    let mut sendai_hit = false;
    for step in 1..1200 {
        // 前半は Nagoya (4) を書かない
        let pick_city = |rng: &mut Rng| loop {
            let c = rng.below(6) as usize;
            if step >= 400 || c != 4 {
                break cities[c];
            }
        };
        match rng.below(8) {
            0 | 1 => {
                let c = companies[rng.below(companies.len() as u64) as usize];
                t.companies.entity(c).set("city", pick_city(&mut rng)).commit().unwrap();
            }
            2 | 3 => {
                let e = users[rng.below(users.len() as u64) as usize];
                let c = companies[rng.below(companies.len() as u64) as usize];
                t.users.entity(e).set("company", Value::Ref(c)).commit().unwrap();
            }
            4 => {
                let e = users[rng.below(users.len() as u64) as usize];
                t.users.entity(e).set("age", 20 + rng.below(5) as i64).commit().unwrap();
            }
            5 => {
                let d = depts[rng.below(depts.len() as u64) as usize];
                t.depts.entity(d).set("name", dept_names[rng.below(3) as usize]).commit().unwrap();
            }
            6 => {
                let i = rng.below(users.len() as u64) as usize;
                t.users.entity(users[i]).delete().unwrap();
                users[i] = t
                    .users
                    .insert()
                    .set("id", next_id)
                    .set("age", 20 + rng.below(5) as i64)
                    .set("company", Value::Ref(companies[rng.below(companies.len() as u64) as usize]))
                    .set("dept", Value::Ref(depts[rng.below(depts.len() as u64) as usize]))
                    .commit()
                    .unwrap();
                next_id += 1;
            }
            _ => {
                let i = rng.below(companies.len() as u64) as usize;
                t.companies.entity(companies[i]).delete().unwrap();
                companies[i] = t
                    .companies
                    .insert()
                    .set("id", next_id)
                    .set("city", pick_city(&mut rng))
                    .commit()
                    .unwrap();
                next_id += 1;
            }
        }
        // 購読の入れ替え: 1 本落として同じ種類の別の値で張り直す (新しい方は空から積分)
        if step % 7 == 0 {
            let i = rng.below(subs.len() as u64) as usize;
            let kind = rng.below(3);
            subs[i] = make(kind, rng.below(6) as usize, rng.below(5) as usize);
        }
        if step % 3 == 0 {
            check(&mut subs, &users, step);
            nagoya_hit |= subs.iter().any(|s| s.name.contains("Nagoya") && !s.seen.is_empty());
            sendai_hit |= subs.iter().any(|s| s.name.contains("Sendai") && !s.seen.is_empty());
        }
    }
    check(&mut subs, &users, 1_000_001);
    check(&mut subs, &users, 1_000_002);
    assert!(nagoya_hit, "vocab に後から現れた値の購読が一度も当たっていない");
    assert!(sendai_hit, "後から購読された値の購読が一度も当たっていない");
}

/// 範囲の違う `Range` の購読 (engine 内では範囲を穴にした 1 本の family に束ねられる) を多数張り、
/// 途中で購読を張り替え続けても (帯が割れる / 併さる)、 各購読の積分が手でたどった結果と一致すること。
///
/// - 根の範囲 (`age ∈ [a, b]`)、 入れ子の閾値 (`age > a`)、 空の範囲 (`age < 0`)
/// - ref の先の範囲 (`company.revenue ∈ [a, b]`)、 値の穴と同じ節の範囲、 根の範囲 + ref の先の値の穴
/// - 範囲 2 本 (1 本目だけが穴、 2 本目は値を固定 = 範囲ごとに別の family)
#[test]
fn range_subscriptions_match_oracle() {
    let path = tmp_path("range");
    cleanup(&path);
    run_range(&path);
    cleanup(&path);
}

fn run_range(path: &str) {
    let mut db = Database::create_growable_tiny(path).unwrap();
    db.table("companies").number("id").tag("city").number("revenue").primary_key("id").build().unwrap();
    db.table("users").number("id").number("age").ref_to("company", "companies").primary_key("id").build().unwrap();
    let users_t = db.get_table("users").unwrap();
    let companies_t = db.get_table("companies").unwrap();
    let cities = ["Tokyo", "Osaka", "Kyoto"];
    let mut rng = Rng(0x7a9e_5eed_0bad_cafe);
    let mut companies: Vec<u64> = (0..12i64)
        .map(|i| {
            companies_t
                .insert()
                .set("id", i)
                .set("city", cities[(i % 3) as usize])
                .set("revenue", (i * 83) % 1000)
                .commit()
                .unwrap()
        })
        .collect();
    let mut users: Vec<u64> = (0..240i64)
        .map(|i| {
            users_t
                .insert()
                .set("id", i)
                .set("age", (i * 7) % 60)
                .set("company", Value::Ref(companies[(i % 12) as usize]))
                .commit()
                .unwrap()
        })
        .collect();
    let (u, c) = (&users_t, &companies_t);
    let age = move |e: u64| get_num(u, e, "age");
    let company = move |e: u64| get_ref(u, e, "company");
    let revenue = move |e: u64| company(e).and_then(|x| get_num(c, x, "revenue"));
    let city = move |e: u64| company(e).and_then(|x| get_text(c, x, "city"));
    let within = |v: Option<i64>, lo: u32, hi: u32| v.is_some_and(|v| lo as i64 <= v && v <= hi as i64);

    type RangeOracle<'a> = Box<dyn Fn(u64) -> bool + 'a>;
    struct RSub<'a> {
        name: String,
        q: LiveQuery,
        seen: BTreeSet<u64>,
        oracle: RangeOracle<'a>,
    }
    let make = |kind: u64, rng: &mut Rng| -> RSub {
        let a = rng.below(60) as u32;
        let w = rng.below(20) as u32;
        let ra = rng.below(1000) as u32;
        let rw = rng.below(400) as u32;
        let ct = cities[rng.below(3) as usize];
        let (name, q, oracle): (String, LiveQuery, RangeOracle) = match kind {
            0 => (
                format!("age in [{a}, {}]", a + w),
                u.where_range("age", a, a + w).subscribe().unwrap(),
                Box::new(move |e| within(age(e), a, a + w)),
            ),
            1 => (format!("age > {a}"), u.all().where_gt("age", a).subscribe().unwrap(), Box::new(move |e| age(e).is_some_and(|v| v > a as i64))),
            2 => (
                format!("company.revenue in [{ra}, {}]", ra + rw),
                u.where_range("company.revenue", ra, ra + rw).subscribe().unwrap(),
                Box::new(move |e| within(revenue(e), ra, ra + rw)),
            ),
            3 => (
                format!("company.city = {ct} AND company.revenue >= {ra}"),
                u.where_eq("company.city", ct).where_ge("company.revenue", ra).subscribe().unwrap(),
                Box::new(move |e| city(e).as_deref() == Some(ct) && revenue(e).is_some_and(|v| v >= ra as i64)),
            ),
            4 => (
                format!("company.city = {ct} AND age in [{a}, {}]", a + w),
                u.where_eq("company.city", ct).where_range("age", a, a + w).subscribe().unwrap(),
                Box::new(move |e| city(e).as_deref() == Some(ct) && within(age(e), a, a + w)),
            ),
            5 => (
                format!("age in [{a}, {}] AND company.revenue <= {ra}", a + w),
                u.where_range("age", a, a + w).where_le("company.revenue", ra).subscribe().unwrap(),
                Box::new(move |e| within(age(e), a, a + w) && revenue(e).is_some_and(|v| v <= ra as i64)),
            ),
            _ => ("age < 0".to_string(), u.all().where_lt("age", 0).subscribe().unwrap(), Box::new(|_| false)),
        };
        RSub { name, q, seen: BTreeSet::new(), oracle }
    };
    let mut subs: Vec<RSub> = (0..42).map(|i| make(i % 7, &mut rng)).collect();

    // 購読は束に入れて束の poll で受け取る (入れ直しは何度でもよい)
    let group = db.live_group();
    let check = |subs: &mut Vec<RSub>, users: &[u64], step: usize| {
        if !step.is_multiple_of(2) {
            for (id, d) in {
                for s in subs.iter() {
                    group.add(&s.q);
                }
                group.poll()
            } {
                let s = subs.iter_mut().find(|s| s.q.id() == id).expect("生きている購読の id");
                integrate(&mut s.seen, d);
            }
        }
        for s in subs.iter_mut() {
            if step.is_multiple_of(2) {
                integrate(&mut s.seen, s.q.poll());
            }
            let want: BTreeSet<u64> = users.iter().copied().filter(|&e| (s.oracle)(e)).collect();
            assert_eq!(s.seen, want, "[{}] step {step}: 積分 != 手でたどった結果", s.name);
            assert!(s.q.poll().is_empty(), "[{}] step {step}: 受け取り済みの差分がまた届く", s.name);
            assert_eq!(s.q.count(), want.len(), "[{}] step {step}: count", s.name);
        }
    };
    check(&mut subs, &users, 0);

    let mut next_id = 1000i64;
    let mut hits = [0usize; 7];
    for step in 1..1500 {
        match rng.below(8) {
            0 | 1 => {
                let e = users[rng.below(users.len() as u64) as usize];
                users_t.entity(e).set("age", rng.below(60) as i64).commit().unwrap();
            }
            2 | 3 => {
                let x = companies[rng.below(companies.len() as u64) as usize];
                companies_t.entity(x).set("revenue", rng.below(1000) as i64).commit().unwrap();
            }
            4 => {
                let e = users[rng.below(users.len() as u64) as usize];
                let x = companies[rng.below(companies.len() as u64) as usize];
                users_t.entity(e).set("company", Value::Ref(x)).commit().unwrap();
            }
            5 => {
                let x = companies[rng.below(companies.len() as u64) as usize];
                companies_t.entity(x).set("city", cities[rng.below(3) as usize]).commit().unwrap();
            }
            6 => {
                let i = rng.below(users.len() as u64) as usize;
                users_t.entity(users[i]).delete().unwrap();
                users[i] = users_t
                    .insert()
                    .set("id", next_id)
                    .set("age", rng.below(60) as i64)
                    .set("company", Value::Ref(companies[rng.below(companies.len() as u64) as usize]))
                    .commit()
                    .unwrap();
                next_id += 1;
            }
            _ => {
                let i = rng.below(companies.len() as u64) as usize;
                companies_t.entity(companies[i]).delete().unwrap();
                companies[i] = companies_t
                    .insert()
                    .set("id", next_id)
                    .set("city", cities[rng.below(3) as usize])
                    .set("revenue", rng.below(1000) as i64)
                    .commit()
                    .unwrap();
                next_id += 1;
            }
        }
        // 購読の張り替え: 範囲の端が増減する (帯が割れる / 併さる)
        if step % 4 == 0 {
            let i = rng.below(subs.len() as u64) as usize;
            let kind = rng.below(7);
            subs[i] = make(kind, &mut rng);
        }
        if step % 3 == 0 {
            check(&mut subs, &users, step);
            for s in &subs {
                if !s.seen.is_empty() {
                    let k = ["age in", "age >", "company.revenue in", "company.city = ", "", "", ""]
                        .iter()
                        .position(|p| !p.is_empty() && s.name.starts_with(p))
                        .unwrap_or(6);
                    hits[k] += 1;
                }
            }
        }
    }
    check(&mut subs, &users, 1_000_001);
    check(&mut subs, &users, 1_000_002);
    assert!(hits[..4].iter().all(|&h| h > 0), "当たらなかった種類がある: {hits:?}");
}

/// `Query::or` の購読 (engine 内では枝ごとに family の member、 枝の差分を積んで和を取る) が、
/// 張り替え・行の入れ替え (slot 再利用) を混ぜても手でたどった結果と一致すること。 `find()` も比べる。
///
/// - 同じ形の枝 (`city = A OR city = B`、 同じ family の 2 member)、 形の違う枝 (`city OR age > k`)
/// - ref の先の枝、 `(a OR b) AND c`、 両方の枝に居る row (片方から出ても残る)、 `all().or(..)`
#[test]
fn or_subscriptions_match_oracle() {
    let path = tmp_path("or");
    cleanup(&path);
    run_or(&path);
    cleanup(&path);
}

fn run_or(path: &str) {
    let mut db = Database::create_growable_tiny(path).unwrap();
    db.table("companies").number("id").tag("city").primary_key("id").build().unwrap();
    db.table("users")
        .number("id")
        .number("age")
        .tag("city")
        .ref_to("company", "companies")
        .primary_key("id")
        .build()
        .unwrap();
    let users_t = db.get_table("users").unwrap();
    let companies_t = db.get_table("companies").unwrap();
    let cities = ["Tokyo", "Osaka", "Kyoto", "Nagoya"];
    let mut rng = Rng(0x0a0b_0c0d_1234_5678);
    let mut companies: Vec<u64> = (0..8i64)
        .map(|i| companies_t.insert().set("id", i).set("city", cities[(i % 4) as usize]).commit().unwrap())
        .collect();
    let mut users: Vec<u64> = (0..160i64)
        .map(|i| {
            users_t
                .insert()
                .set("id", i)
                .set("age", (i * 7) % 60)
                .set("city", cities[(i % 3) as usize])
                .set("company", Value::Ref(companies[(i % 8) as usize]))
                .commit()
                .unwrap()
        })
        .collect();
    let (u, c) = (&users_t, &companies_t);
    let age = move |e: u64| get_num(u, e, "age");
    let home = move |e: u64| get_text(u, e, "city");
    let work = move |e: u64| get_ref(u, e, "company").and_then(|x| get_text(c, x, "city"));
    let is = |v: Option<String>, s: &str| v.as_deref() == Some(s);

    type OrOracle<'a> = Box<dyn Fn(u64) -> bool + 'a>;
    type OrQuery<'a> = Box<dyn Fn() -> enchudb_schema::Query<'a> + 'a>;
    struct OSub<'a> {
        name: String,
        q: LiveQuery,
        query: OrQuery<'a>,
        seen: BTreeSet<u64>,
        oracle: OrOracle<'a>,
    }
    let make = |kind: u64, rng: &mut Rng| -> OSub {
        let a = cities[rng.below(4) as usize];
        let b = cities[rng.below(4) as usize];
        let k = rng.below(60) as u32;
        let (name, query, oracle): (String, OrQuery, OrOracle) = match kind {
            0 => (
                format!("city = {a} OR city = {b}"),
                Box::new(move || u.where_eq("city", a).or(u.where_eq("city", b))),
                Box::new(move |e| is(home(e), a) || is(home(e), b)),
            ),
            1 => (
                format!("city = {a} OR age > {k}"),
                Box::new(move || u.where_eq("city", a).or(u.all().where_gt("age", k))),
                Box::new(move |e| is(home(e), a) || age(e).is_some_and(|v| v > k as i64)),
            ),
            2 => (
                format!("company.city = {a} OR city = {b}"),
                Box::new(move || u.where_eq("company.city", a).or(u.where_eq("city", b))),
                Box::new(move |e| is(work(e), a) || is(home(e), b)),
            ),
            3 => (
                format!("(company.city = {a} OR company.city = {b}) AND age <= {k}"),
                Box::new(move || u.where_eq("company.city", a).or(u.where_eq("company.city", b)).where_le("age", k)),
                Box::new(move |e| (is(work(e), a) || is(work(e), b)) && age(e).is_some_and(|v| v <= k as i64)),
            ),
            4 => (
                // 同じ row が両方の枝に居やすい: 住所と勤務先が同じ都市
                format!("city = {a} OR company.city = {a}"),
                Box::new(move || u.where_eq("city", a).or(u.where_eq("company.city", a))),
                Box::new(move |e| is(home(e), a) || is(work(e), a)),
            ),
            6 => {
                let vs: Vec<u32> = (0..3).map(|_| rng.below(60) as u32).collect();
                let v2 = vs.clone();
                (
                    format!("age IN {vs:?} OR city = {a}"),
                    Box::new(move || u.where_in("age", &vs).or(u.where_eq("city", a))),
                    Box::new(move |e| age(e).is_some_and(|x| v2.contains(&(x as u32))) || is(home(e), a)),
                )
            }
            7 => {
                let vs: Vec<u32> = (0..4).map(|_| rng.below(60) as u32).collect();
                let x = rng.below(60) as u32;
                let v2 = vs.clone();
                (
                    // 同じ形 (age の値の穴) = 鍵を複数持つ member 1 つ
                    format!("age IN {vs:?} OR age = {x}"),
                    Box::new(move || u.where_in("age", &vs).or(u.where_eq("age", x as i64))),
                    Box::new(move |e| age(e).is_some_and(|y| v2.contains(&(y as u32)) || y == x as i64)),
                )
            }
            _ => (
                format!("all OR city = {a}"),
                Box::new(move || u.all().or(u.where_eq("city", a))),
                Box::new(move |e| age(e).is_some() || is(home(e), a)),
            ),
        };
        let q = query().subscribe().unwrap();
        OSub { name, q, query, seen: BTreeSet::new(), oracle }
    };
    let mut subs: Vec<OSub> = (0..40).map(|i| make(i % 8, &mut rng)).collect();

    // 購読は束に入れて束の poll で受け取る (入れ直しは何度でもよい)
    let group = db.live_group();
    let check = |subs: &mut Vec<OSub>, users: &[u64], step: usize| {
        if step % 4 == 1 {
            // count が枝の差分を積んだ後でも束の poll に届く
            for s in subs.iter() {
                s.q.count();
            }
        }
        if !step.is_multiple_of(2) {
            let deltas = {
                for s in subs.iter() {
                    group.add(&s.q);
                }
                group.poll()
            };
            assert!(deltas.windows(2).all(|w| w[0].0 < w[1].0), "id 昇順・重複なし");
            for (id, d) in deltas {
                assert!(!d.is_empty(), "空の差分は返さない");
                let s = subs.iter_mut().find(|s| s.q.id() == id).expect("生きている購読の id (枝の id が漏れた)");
                integrate(&mut s.seen, d);
            }
        }
        for s in subs.iter_mut() {
            if step.is_multiple_of(2) {
                integrate(&mut s.seen, s.q.poll());
            }
            let want: BTreeSet<u64> = users.iter().copied().filter(|&e| (s.oracle)(e)).collect();
            assert_eq!(s.seen, want, "[{}] step {step}: 積分 != 手でたどった結果", s.name);
            assert!(s.q.poll().is_empty(), "[{}] step {step}: 受け取り済みの差分がまた届く", s.name);
            assert_eq!(s.q.count(), want.len(), "[{}] step {step}: count", s.name);
            let found: BTreeSet<u64> = (s.query)().find().unwrap().into_iter().collect();
            assert_eq!(found, want, "[{}] step {step}: find", s.name);
        }
    };
    check(&mut subs, &users, 0);

    let mut next_id = 1000i64;
    for step in 1..1200 {
        match rng.below(7) {
            0 | 1 => {
                let e = users[rng.below(users.len() as u64) as usize];
                users_t.entity(e).set("age", rng.below(60) as i64).commit().unwrap();
            }
            2 => {
                let e = users[rng.below(users.len() as u64) as usize];
                users_t.entity(e).set("city", cities[rng.below(4) as usize]).commit().unwrap();
            }
            3 => {
                let x = companies[rng.below(companies.len() as u64) as usize];
                companies_t.entity(x).set("city", cities[rng.below(4) as usize]).commit().unwrap();
            }
            4 => {
                let e = users[rng.below(users.len() as u64) as usize];
                let x = companies[rng.below(companies.len() as u64) as usize];
                users_t.entity(e).set("company", Value::Ref(x)).commit().unwrap();
            }
            5 => {
                let i = rng.below(users.len() as u64) as usize;
                users_t.entity(users[i]).delete().unwrap();
                users[i] = users_t
                    .insert()
                    .set("id", next_id)
                    .set("age", rng.below(60) as i64)
                    .set("city", cities[rng.below(4) as usize])
                    .set("company", Value::Ref(companies[rng.below(companies.len() as u64) as usize]))
                    .commit()
                    .unwrap();
                next_id += 1;
            }
            _ => {
                let i = rng.below(companies.len() as u64) as usize;
                companies_t.entity(companies[i]).delete().unwrap();
                companies[i] =
                    companies_t.insert().set("id", next_id).set("city", cities[rng.below(4) as usize]).commit().unwrap();
                next_id += 1;
            }
        }
        if step % 5 == 0 {
            let i = rng.below(subs.len() as u64) as usize;
            let kind = rng.below(8);
            subs[i] = make(kind, &mut rng);
        }
        if step % 3 == 0 {
            check(&mut subs, &users, step);
        }
    }
    check(&mut subs, &users, 1_000_001);
    check(&mut subs, &users, 1_000_002);
}

/// `subscribe_counts` (group ごとの件数の購読) の poll を上書きで積んだ件数・`all()`・`get()`・
/// `total()` が、 手でたどって数えた件数と一致すること。
///
/// - 根の列で group (`city`)、 ref の先の列で group (`company.city` — 会社の移転で配下がまとめて
///   動く)、 Ref 列で group (`company`)、 数値列で group (`age`)
/// - 条件も ref の先・範囲・値の穴 (値だけ違う条件の購読は件数を共有する)、 同じ条件の購読が
///   後から張られても初回 poll で全 group を受け取る
#[test]
fn count_subscriptions_match_oracle() {
    let path = tmp_path("counts");
    cleanup(&path);
    run_counts(&path);
    cleanup(&path);
}

fn run_counts(path: &str) {
    let mut db = Database::create_growable_tiny(path).unwrap();
    db.table("regions").number("id").tag("name").primary_key("id").build().unwrap();
    db.table("companies").number("id").tag("city").ref_to("region", "regions").primary_key("id").build().unwrap();
    db.table("users")
        .number("id")
        .number("age")
        .number("salary")
        .tag("city")
        .ref_to("company", "companies")
        .primary_key("id")
        .build()
        .unwrap();
    let users_t = db.get_table("users").unwrap();
    let companies_t = db.get_table("companies").unwrap();
    let regions_t = db.get_table("regions").unwrap();
    let cities = ["Tokyo", "Osaka", "Kyoto", "Nagoya"];
    let names = ["Kanto", "Kansai", "Chubu"];
    let mut rng = Rng(0xc0c0_1234_abcd_0001);
    let regions: Vec<u64> = (0..4i64)
        .map(|i| regions_t.insert().set("id", i).set("name", names[(i % 3) as usize]).commit().unwrap())
        .collect();
    let mut companies: Vec<u64> = (0..8i64)
        .map(|i| {
            companies_t
                .insert()
                .set("id", i)
                .set("city", cities[(i % 4) as usize])
                .set("region", Value::Ref(regions[(i % 4) as usize]))
                .commit()
                .unwrap()
        })
        .collect();
    let mut users: Vec<u64> = (0..160i64)
        .map(|i| {
            let mut b = users_t.insert().set("id", i).set("company", Value::Ref(companies[(i % 8) as usize]));
            // 一部は age / city を持たない (group の列に値が無い row は数えない)
            if i % 11 != 0 {
                b = b.set("age", (i * 7) % 30);
            }
            if i % 13 != 0 {
                b = b.set("city", cities[(i % 3) as usize]);
            }
            if i % 7 != 0 {
                b = b.set("salary", (i * 13) % 100);
            }
            b.commit().unwrap()
        })
        .collect();
    let (u, c) = (&users_t, &companies_t);
    let age = move |e: u64| get_num(u, e, "age");
    let salary = move |e: u64| get_num(u, e, "salary").unwrap_or(0) as u64;
    let home = move |e: u64| get_text(u, e, "city");
    let company = move |e: u64| get_ref(u, e, "company");
    let work = move |e: u64| company(e).and_then(|x| get_text(c, x, "city"));
    let rg = &regions_t;
    let region = move |e: u64| company(e).and_then(|x| get_ref(c, x, "region")).and_then(|r| get_text(rg, r, "name"));

    type Cond<'a> = Box<dyn Fn(u64) -> bool + 'a>;
    type Key<'a> = Box<dyn Fn(u64) -> Option<Value> + 'a>;
    struct CSub<'a> {
        name: String,
        q: enchudb_schema::LiveCounts,
        /// group → (件数, 合計)
        seen: std::collections::BTreeMap<String, (u64, u64)>,
        /// salary の合計も持つ購読 (subscribe_sums)
        sum: bool,
        cond: Cond<'a>,
        key: Key<'a>,
    }
    let show = |v: &Value| format!("{v:?}");
    let make = |kind: u64, rng: &mut Rng| -> CSub {
        let a = cities[rng.below(4) as usize];
        let k = rng.below(30) as u32;
        let sum = rng.below(2) == 0;
        let agg = |q: enchudb_schema::Query, col: &str| {
            if sum { q.subscribe_sums(col, "salary") } else { q.subscribe_counts(col) }.unwrap()
        };
        let (name, q, cond, key): (String, enchudb_schema::LiveCounts, Cond, Key) = match kind {
            0 => (
                "all by city".into(),
                agg(u.all(), "city"),
                Box::new(move |e| age(e).is_some() || home(e).is_some() || company(e).is_some()),
                Box::new(move |e| home(e).map(Value::Text)),
            ),
            1 => (
                format!("age > {k} by company.city"),
                agg(u.all().where_gt("age", k), "company.city"),
                Box::new(move |e| age(e).is_some_and(|v| v > k as i64)),
                Box::new(move |e| work(e).map(Value::Text)),
            ),
            2 => (
                format!("company.city = {a} by age"),
                agg(u.where_eq("company.city", a), "age"),
                Box::new(move |e| work(e).as_deref() == Some(a)),
                Box::new(move |e| age(e).map(Value::Number)),
            ),
            3 => (
                format!("age in [{k}, {}] by company.city", k + 8),
                agg(u.where_range("age", k, k + 8), "company.city"),
                Box::new(move |e| age(e).is_some_and(|v| k as i64 <= v && v <= k as i64 + 8)),
                Box::new(move |e| work(e).map(Value::Text)),
            ),
            6 => {
                let vs: Vec<u32> = (0..4).map(|_| rng.below(30) as u32).collect();
                let v2 = vs.clone();
                (
                    format!("age IN {vs:?} by company.city"),
                    agg(u.where_in("age", &vs), "company.city"),
                    Box::new(move |e| age(e).is_some_and(|x| v2.contains(&(x as u32)))),
                    Box::new(move |e| work(e).map(Value::Text)),
                )
            }
            7 => {
                let b = cities[rng.below(4) as usize];
                (
                    format!("company.city = {a} OR company.city = {b} by age"),
                    agg(u.where_eq("company.city", a).or(u.where_eq("company.city", b)), "age"),
                    Box::new(move |e| work(e).is_some_and(|w| w == a || w == b)),
                    Box::new(move |e| age(e).map(Value::Number)),
                )
            }
            5 => (
                format!("age > {k} by company.region.name"),
                agg(u.all().where_gt("age", k), "company.region.name"),
                Box::new(move |e| age(e).is_some_and(|v| v > k as i64)),
                Box::new(move |e| region(e).map(Value::Text)),
            ),
            _ => (
                format!("city = {a} by company"),
                agg(u.where_eq("city", a), "company"),
                Box::new(move |e| home(e).as_deref() == Some(a)),
                Box::new(move |e| company(e).map(Value::Ref)),
            ),
        };
        let name = if sum { format!("{name} sum salary") } else { name };
        CSub { name, q, seen: Default::default(), sum, cond, key }
    };
    let mut subs: Vec<CSub> = (0..40).map(|i| make(i % 8, &mut rng)).collect();
    // 形の違う枝の Or は集計できない
    assert!(u.where_eq("city", "Tokyo").or(u.all().where_gt("age", 3)).subscribe_counts("company.city").is_err());

    let check = |subs: &mut Vec<CSub>, users: &[u64], step: usize| {
        for s in subs.iter_mut() {
            let got: Vec<(Value, u64, u64)> = if s.sum {
                s.q.poll_sums()
            } else {
                s.q.poll().into_iter().map(|(v, n)| (v, n, 0)).collect()
            };
            for (v, n, t) in got {
                if n == 0 {
                    assert!(s.seen.remove(&show(&v)).is_some(), "[{}] 知らない group の 0", s.name);
                } else {
                    assert_ne!(s.seen.insert(show(&v), (n, t)), Some((n, t)), "[{}] 変わらない group を報告", s.name);
                }
            }
            let mut want: std::collections::BTreeMap<String, (u64, u64)> = Default::default();
            for &e in users {
                if (s.cond)(e)
                    && let Some(k) = (s.key)(e)
                {
                    let w = want.entry(show(&k)).or_insert((0, 0));
                    w.0 += 1;
                    if s.sum {
                        w.1 += salary(e);
                    }
                }
            }
            assert_eq!(s.seen, want, "[{}] step {step}: 積分 != 手で数えた件数 / 合計", s.name);
            assert!(s.q.poll_sums().is_empty(), "[{}] step {step}: 受け取り済みの集計がまた届く", s.name);
            let all: std::collections::BTreeMap<String, (u64, u64)> =
                s.q.all_sums().into_iter().map(|(v, n, t)| (show(&v), (n, t))).collect();
            assert_eq!(all, want, "[{}] step {step}: all_sums", s.name);
            let counts: std::collections::BTreeMap<String, u64> = s.q.all().into_iter().map(|(v, n)| (show(&v), n)).collect();
            assert_eq!(counts, want.iter().map(|(k, w)| (k.clone(), w.0)).collect(), "[{}] step {step}: all", s.name);
            assert_eq!(s.q.total() as u64, want.values().map(|w| w.0).sum::<u64>(), "[{}] step {step}: total", s.name);
            if let Some(e) = users.iter().copied().find(|&e| (s.cond)(e))
                && let Some(k) = (s.key)(e)
            {
                assert_eq!((s.q.get(&k), s.q.get_sum(&k)), want[&show(&k)], "[{}] step {step}: get", s.name);
            }
        }
    };
    check(&mut subs, &users, 0);

    let mut next_id = 1000i64;
    for step in 1..1200 {
        match rng.below(11) {
            9 => {
                let e = users[rng.below(users.len() as u64) as usize];
                users_t.entity(e).set("salary", rng.below(100) as i64).commit().unwrap();
            }
            10 => {
                let e = users[rng.below(users.len() as u64) as usize];
                db.engine().untie(e, "users.salary");
            }
            7 => {
                let x = regions[rng.below(regions.len() as u64) as usize];
                regions_t.entity(x).set("name", names[rng.below(3) as usize]).commit().unwrap();
            }
            8 => {
                let x = companies[rng.below(companies.len() as u64) as usize];
                let r = regions[rng.below(regions.len() as u64) as usize];
                companies_t.entity(x).set("region", Value::Ref(r)).commit().unwrap();
            }
            0 | 1 => {
                let e = users[rng.below(users.len() as u64) as usize];
                users_t.entity(e).set("age", rng.below(30) as i64).commit().unwrap();
            }
            2 => {
                let e = users[rng.below(users.len() as u64) as usize];
                users_t.entity(e).set("city", cities[rng.below(4) as usize]).commit().unwrap();
            }
            3 => {
                let x = companies[rng.below(companies.len() as u64) as usize];
                companies_t.entity(x).set("city", cities[rng.below(4) as usize]).commit().unwrap();
            }
            4 => {
                let e = users[rng.below(users.len() as u64) as usize];
                let x = companies[rng.below(companies.len() as u64) as usize];
                users_t.entity(e).set("company", Value::Ref(x)).commit().unwrap();
            }
            5 => {
                let i = rng.below(users.len() as u64) as usize;
                users_t.entity(users[i]).delete().unwrap();
                users[i] = users_t
                    .insert()
                    .set("id", next_id)
                    .set("age", rng.below(30) as i64)
                    .set("city", cities[rng.below(4) as usize])
                    .set("company", Value::Ref(companies[rng.below(companies.len() as u64) as usize]))
                    .commit()
                    .unwrap();
                next_id += 1;
            }
            _ => {
                let i = rng.below(companies.len() as u64) as usize;
                companies_t.entity(companies[i]).delete().unwrap();
                companies[i] = companies_t
                    .insert()
                    .set("id", next_id)
                    .set("city", cities[rng.below(4) as usize])
                    .set("region", Value::Ref(regions[rng.below(regions.len() as u64) as usize]))
                    .commit()
                    .unwrap();
                next_id += 1;
            }
        }
        if step % 5 == 0 {
            let i = rng.below(subs.len() as u64) as usize;
            let kind = rng.below(8);
            subs[i] = make(kind, &mut rng);
        }
        if step % 3 == 0 {
            check(&mut subs, &users, step);
        }
    }
    check(&mut subs, &users, 1_000_001);
}

/// `order_by(..).limit(k).subscribe()` (先頭 k 件の購読) の積分・`ranked()`・`count()`・`find()` が、
/// 手で並べて切った結果と一致すること。 値の重なりが多い (同じ値は eid の昇順)。
///
/// - 根の列で並べる (昇順 / 降順)、 ref の先の列で並べる (会社の売上が変わると配下の順位がまとめて動く)
/// - 条件も ref の先・範囲・値の穴、 k は 1〜12、 同じ条件で k の違う購読が同じ family を共有する
#[test]
fn top_k_subscriptions_match_oracle() {
    let path = tmp_path("topk");
    cleanup(&path);
    run_top(&path);
    cleanup(&path);
}

fn run_top(path: &str) {
    let mut db = Database::create_growable_tiny(path).unwrap();
    db.table("companies").number("id").tag("city").number("revenue").primary_key("id").build().unwrap();
    db.table("users")
        .number("id")
        .number("age")
        .tag("city")
        .ref_to("company", "companies")
        .primary_key("id")
        .build()
        .unwrap();
    let users_t = db.get_table("users").unwrap();
    let companies_t = db.get_table("companies").unwrap();
    let cities = ["Tokyo", "Osaka", "Kyoto"];
    let mut rng = Rng(0x70b0_70b0_1111_2222);
    let mut companies: Vec<u64> = (0..10i64)
        .map(|i| {
            companies_t
                .insert()
                .set("id", i)
                .set("city", cities[(i % 3) as usize])
                .set("revenue", (i * 37) % 50)
                .commit()
                .unwrap()
        })
        .collect();
    let mut users: Vec<u64> = (0..150i64)
        .map(|i| {
            let mut b = users_t
                .insert()
                .set("id", i)
                .set("city", cities[(i % 3) as usize])
                .set("company", Value::Ref(companies[(i % 10) as usize]));
            if i % 9 != 0 {
                b = b.set("age", (i * 7) % 30);
            }
            b.commit().unwrap()
        })
        .collect();
    let (u, c) = (&users_t, &companies_t);
    let age = move |e: u64| get_num(u, e, "age");
    let home = move |e: u64| get_text(u, e, "city");
    let company = move |e: u64| get_ref(u, e, "company");
    let work = move |e: u64| company(e).and_then(|x| get_text(c, x, "city"));
    let revenue = move |e: u64| company(e).and_then(|x| get_num(c, x, "revenue"));

    type Cond<'a> = Box<dyn Fn(u64) -> bool + 'a>;
    type OrderKey<'a> = Box<dyn Fn(u64) -> Option<i64> + 'a>;
    type Q<'a> = Box<dyn Fn() -> enchudb_schema::Query<'a> + 'a>;
    struct TSub<'a> {
        name: String,
        q: LiveQuery,
        query: Q<'a>,
        seen: BTreeSet<u64>,
        cond: Cond<'a>,
        key: OrderKey<'a>,
        desc: bool,
        k: usize,
    }
    let make = |kind: u64, rng: &mut Rng| -> TSub {
        let a = cities[rng.below(3) as usize];
        let x = rng.below(30) as u32;
        let k = 1 + rng.below(12) as usize;
        let (name, query, cond, key, desc): (String, Q, Cond, OrderKey, bool) = match kind {
            0 => (
                format!("all order by age limit {k}"),
                Box::new(move || u.all().order_by("age").limit(k)),
                Box::new(|_| true),
                Box::new(age),
                false,
            ),
            1 => (
                format!("city = {a} order by age desc limit {k}"),
                Box::new(move || u.where_eq("city", a).order_by_desc("age").limit(k)),
                Box::new(move |e| home(e).as_deref() == Some(a)),
                Box::new(age),
                true,
            ),
            2 => (
                format!("age > {x} order by company.revenue limit {k}"),
                Box::new(move || u.all().where_gt("age", x).order_by("company.revenue").limit(k)),
                Box::new(move |e| age(e).is_some_and(|v| v > x as i64)),
                Box::new(revenue),
                false,
            ),
            4 => {
                let vs: Vec<u32> = (0..1 + rng.below(4)).map(|_| rng.below(30) as u32).collect();
                let set = vs.clone();
                (
                    format!("age in {vs:?} order by company.revenue limit {k}"),
                    Box::new(move || u.where_in("age", &vs).order_by("company.revenue").limit(k)),
                    Box::new(move |e| age(e).is_some_and(|v| set.contains(&(v as u32)))),
                    Box::new(revenue),
                    false,
                )
            }
            5 => {
                let b = cities[rng.below(3) as usize];
                (
                    format!("(company.city = {a} or company.city = {b}) order by age desc limit {k}"),
                    Box::new(move || u.where_eq("company.city", a).or(u.where_eq("company.city", b)).order_by_desc("age").limit(k)),
                    Box::new(move |e| work(e).is_some_and(|w| w == a || w == b)),
                    Box::new(age),
                    true,
                )
            }
            _ => (
                format!("company.city = {a} order by company.revenue desc limit {k}"),
                Box::new(move || u.where_eq("company.city", a).order_by_desc("company.revenue").limit(k)),
                Box::new(move |e| work(e).as_deref() == Some(a)),
                Box::new(revenue),
                true,
            ),
        };
        let q = query().subscribe().unwrap();
        TSub { name, q, query, seen: BTreeSet::new(), cond, key, desc, k }
    };
    let mut subs: Vec<TSub> = (0..30).map(|i| make(i % 6, &mut rng)).collect();
    // 形の違う枝の Or は上位 k 件にできない
    assert!(u.where_eq("city", "Tokyo").or(u.all().where_gt("age", 3)).order_by("age").limit(3).subscribe().is_err());

    // 購読は束に入れて束の poll で受け取る (入れ直しは何度でもよい)
    let group = db.live_group();
    let check = |subs: &mut Vec<TSub>, users: &[u64], step: usize| {
        if !step.is_multiple_of(2) {
            for (id, d) in {
                for s in subs.iter() {
                    group.add(&s.q);
                }
                group.poll()
            } {
                let s = subs.iter_mut().find(|s| s.q.id() == id).expect("生きている購読の id");
                integrate(&mut s.seen, d);
            }
        }
        for s in subs.iter_mut() {
            if step.is_multiple_of(2) {
                integrate(&mut s.seen, s.q.poll());
            }
            let mut want: Vec<(i64, u32, u64)> = users
                .iter()
                .copied()
                .filter(|&e| (s.cond)(e))
                .filter_map(|e| (s.key)(e).map(|v| (if s.desc { -v } else { v }, (e & 0xFFFF_FFFF) as u32, e)))
                .collect();
            want.sort_unstable();
            want.truncate(s.k);
            let ranked: Vec<u64> = want.iter().map(|x| x.2).collect();
            let set: BTreeSet<u64> = ranked.iter().copied().collect();
            assert_eq!(s.seen, set, "[{}] step {step}: 積分 != 手で並べて切った結果", s.name);
            assert_eq!(s.q.ranked(), ranked, "[{}] step {step}: ranked", s.name);
            assert_eq!(s.q.count(), ranked.len(), "[{}] step {step}: count", s.name);
            assert!(s.q.poll().is_empty(), "[{}] step {step}: 受け取り済みの差分がまた届く", s.name);
            assert_eq!((s.query)().find().unwrap(), ranked, "[{}] step {step}: find", s.name);
        }
    };
    check(&mut subs, &users, 0);

    let mut next_id = 1000i64;
    for step in 1..1500 {
        match rng.below(8) {
            0..=2 => {
                let e = users[rng.below(users.len() as u64) as usize];
                users_t.entity(e).set("age", rng.below(30) as i64).commit().unwrap();
            }
            3 => {
                let x = companies[rng.below(companies.len() as u64) as usize];
                companies_t.entity(x).set("revenue", rng.below(50) as i64).commit().unwrap();
            }
            4 => {
                let e = users[rng.below(users.len() as u64) as usize];
                let x = companies[rng.below(companies.len() as u64) as usize];
                users_t.entity(e).set("company", Value::Ref(x)).commit().unwrap();
            }
            5 => {
                let x = companies[rng.below(companies.len() as u64) as usize];
                companies_t.entity(x).set("city", cities[rng.below(3) as usize]).commit().unwrap();
            }
            6 => {
                let i = rng.below(users.len() as u64) as usize;
                users_t.entity(users[i]).delete().unwrap();
                users[i] = users_t
                    .insert()
                    .set("id", next_id)
                    .set("age", rng.below(30) as i64)
                    .set("city", cities[rng.below(3) as usize])
                    .set("company", Value::Ref(companies[rng.below(companies.len() as u64) as usize]))
                    .commit()
                    .unwrap();
                next_id += 1;
            }
            _ => {
                let i = rng.below(companies.len() as u64) as usize;
                companies_t.entity(companies[i]).delete().unwrap();
                companies[i] = companies_t
                    .insert()
                    .set("id", next_id)
                    .set("city", cities[rng.below(3) as usize])
                    .set("revenue", rng.below(50) as i64)
                    .commit()
                    .unwrap();
                next_id += 1;
            }
        }
        if step % 5 == 0 {
            let i = rng.below(subs.len() as u64) as usize;
            let kind = rng.below(6);
            subs[i] = make(kind, &mut rng);
        }
        if step % 3 == 0 {
            check(&mut subs, &users, step);
        }
    }
    check(&mut subs, &users, 1_000_001);
    check(&mut subs, &users, 1_000_002);
}

/// 否定 (`where_ne` / `where_not_in` / `where_null` / `where_not_null`、 ref の先の列も、 `or` との組み合わせも)
/// の購読の積分・`count()`・`find()` が、 手で数えた結果と一致すること。 値を外す (untie) 書き込み・ref の
/// 付け替え / 外し・row と会社の削除を混ぜる (否定は 「値が無い」 への遷移で真になる)。
#[test]
fn not_subscriptions_match_oracle() {
    let path = tmp_path("not");
    cleanup(&path);
    run_not(&path);
    cleanup(&path);
}

fn run_not(path: &str) {
    let mut db = Database::create_growable_tiny(path).unwrap();
    db.table("companies").number("id").tag("city").primary_key("id").build().unwrap();
    db.table("users")
        .number("id")
        .number("age")
        .tag("city")
        .ref_to("company", "companies")
        .primary_key("id")
        .build()
        .unwrap();
    let users_t = db.get_table("users").unwrap();
    let companies_t = db.get_table("companies").unwrap();
    let cities = ["Tokyo", "Osaka", "Kyoto"];
    let mut rng = Rng(0x0707_0707_1234_5678);
    let mut companies: Vec<u64> = (0..8i64)
        .map(|i| {
            let mut b = companies_t.insert().set("id", i);
            if i % 4 != 0 {
                b = b.set("city", cities[(i % 3) as usize]);
            }
            b.commit().unwrap()
        })
        .collect();
    let mut users: Vec<u64> = (0..120i64)
        .map(|i| {
            let mut b = users_t.insert().set("id", i);
            if i % 5 != 0 {
                b = b.set("age", (i * 7) % 20);
            }
            if i % 7 != 0 {
                b = b.set("city", cities[(i % 3) as usize]);
            }
            if i % 9 != 0 {
                b = b.set("company", Value::Ref(companies[(i % 8) as usize]));
            }
            b.commit().unwrap()
        })
        .collect();
    let (u, c) = (&users_t, &companies_t);
    let age = move |e: u64| get_num(u, e, "age");
    let home = move |e: u64| get_text(u, e, "city");
    let company = move |e: u64| get_ref(u, e, "company");
    let work = move |e: u64| company(e).map(|x| get_text(c, x, "city"));

    type Cond<'a> = Box<dyn Fn(u64) -> bool + 'a>;
    type Q<'a> = Box<dyn Fn() -> enchudb_schema::Query<'a> + 'a>;
    struct NSub<'a> {
        name: String,
        q: LiveQuery,
        query: Q<'a>,
        seen: BTreeSet<u64>,
        cond: Cond<'a>,
    }
    let make = |kind: u64, rng: &mut Rng| -> NSub {
        let a = cities[rng.below(3) as usize];
        let x = rng.below(20) as u32;
        let vs: Vec<u32> = (0..1 + rng.below(3)).map(|_| rng.below(20) as u32).collect();
        let (name, query, cond): (String, Q, Cond) = match kind {
            0 => (format!("city != {a}"), Box::new(move || u.all().where_ne("city", a)), Box::new(move |e| home(e).is_some_and(|h| h != a))),
            1 => {
                let v2 = vs.clone();
                (
                    format!("age not in {vs:?}"),
                    Box::new(move || u.all().where_not_in("age", &vs)),
                    Box::new(move |e| age(e).is_some_and(|g| !v2.contains(&(g as u32)))),
                )
            }
            2 => ("age is null".into(), Box::new(move || u.all().where_null("age")), Box::new(move |e| age(e).is_none())),
            3 => (
                format!("company.city != {a}"),
                Box::new(move || u.all().where_ne("company.city", a)),
                Box::new(move |e| matches!(work(e), Some(Some(w)) if w != a)),
            ),
            4 => (
                "company.city is null".into(),
                Box::new(move || u.all().where_null("company.city")),
                Box::new(move |e| matches!(work(e), Some(None))),
            ),
            5 => (
                format!("age is null or city = {a}"),
                Box::new(move || u.all().where_null("age").or(u.where_eq("city", a))),
                Box::new(move |e| age(e).is_none() || home(e).as_deref() == Some(a)),
            ),
            6 => (
                format!("age is not null and city != {a}"),
                Box::new(move || u.all().where_not_null("age").where_ne("city", a)),
                Box::new(move |e| age(e).is_some() && home(e).is_some_and(|h| h != a)),
            ),
            _ => (
                format!("company.city != {a} and age > {x}"),
                Box::new(move || u.all().where_ne("company.city", a).where_gt("age", x)),
                Box::new(move |e| matches!(work(e), Some(Some(w)) if w != a) && age(e).is_some_and(|g| g > x as i64)),
            ),
        };
        let q = query().subscribe().unwrap();
        NSub { name, q, query, seen: BTreeSet::new(), cond }
    };
    let mut subs: Vec<NSub> = (0..32).map(|i| make(i % 8, &mut rng)).collect();
    let group = db.live_group();
    let check = |subs: &mut Vec<NSub>, users: &[u64], step: usize| {
        if step % 2 == 1 {
            for s in subs.iter() {
                group.add(&s.q);
            }
            for (id, d) in group.poll() {
                let s = subs.iter_mut().find(|s| s.q.id() == id).expect("生きている購読の id");
                integrate(&mut s.seen, d);
            }
        }
        for s in subs.iter_mut() {
            if step.is_multiple_of(2) {
                integrate(&mut s.seen, s.q.poll());
            }
            let want: BTreeSet<u64> = users.iter().copied().filter(|&e| (s.cond)(e)).collect();
            assert_eq!(s.seen, want, "[{}] step {step}: 積分 != 手で数えた結果", s.name);
            assert_eq!(s.q.count(), want.len(), "[{}] step {step}: count", s.name);
            let found: BTreeSet<u64> = (s.query)().find().unwrap().into_iter().collect();
            assert_eq!(found, want, "[{}] step {step}: find", s.name);
        }
    };
    check(&mut subs, &users, 0);
    let eng = db.engine();
    let mut next_id = 1000i64;
    for step in 1..1500 {
        let e = users[rng.below(users.len() as u64) as usize];
        match rng.below(10) {
            0 => users_t.entity(e).set("age", rng.below(20) as i64).commit().unwrap(),
            1 => eng.untie(e, "users.age"),
            2 => users_t.entity(e).set("city", cities[rng.below(3) as usize]).commit().unwrap(),
            3 => eng.untie(e, "users.city"),
            4 => {
                let x = companies[rng.below(companies.len() as u64) as usize];
                users_t.entity(e).set("company", Value::Ref(x)).commit().unwrap();
            }
            5 => eng.untie(e, "users.company"),
            6 => {
                let x = companies[rng.below(companies.len() as u64) as usize];
                if rng.below(3) == 0 {
                    eng.untie(x, "companies.city");
                } else {
                    companies_t.entity(x).set("city", cities[rng.below(3) as usize]).commit().unwrap();
                }
            }
            7 => {
                let i = rng.below(users.len() as u64) as usize;
                users_t.entity(users[i]).delete().unwrap();
                let mut b = users_t.insert().set("id", next_id);
                if rng.below(2) == 0 {
                    b = b.set("age", rng.below(20) as i64);
                }
                if rng.below(2) == 0 {
                    b = b.set("city", cities[rng.below(3) as usize]);
                }
                users[i] = b.commit().unwrap();
                next_id += 1;
            }
            8 => {
                let i = rng.below(companies.len() as u64) as usize;
                companies_t.entity(companies[i]).delete().unwrap();
                companies[i] = companies_t.insert().set("id", next_id).commit().unwrap();
                next_id += 1;
            }
            _ => {}
        }
        if step % 5 == 0 {
            let i = rng.below(subs.len() as u64) as usize;
            let kind = rng.below(8);
            subs[i] = make(kind, &mut rng);
        }
        if step % 3 == 0 {
            check(&mut subs, &users, step);
        }
    }
    check(&mut subs, &users, 1_000_001);
    check(&mut subs, &users, 1_000_002);
    // 否定だけの枝は engine が断る (schema は代表列を足すので通る)
    assert!(u.all().where_null("age").subscribe().is_ok());
}

/// `where_exists` / `where_not_exists` (指している row が 1 つ以上ある / 1 つも無い) の購読の積分・`count()`・
/// `find()` が、 手で数えた結果と一致すること。 中身の条件に否定・ref の道も、 外側に `or` も。 指している
/// row の中身の変化・付け替え・外し・削除と、 指されている row の削除を混ぜる。
#[test]
fn exists_subscriptions_match_oracle() {
    let path = tmp_path("exists");
    cleanup(&path);
    run_exists(&path);
    cleanup(&path);
}

fn run_exists(path: &str) {
    let mut db = Database::create_growable_tiny(path).unwrap();
    db.table("companies").number("id").tag("city").primary_key("id").build().unwrap();
    db.table("users")
        .number("id")
        .number("age")
        .tag("city")
        .ref_to("company", "companies")
        .primary_key("id")
        .build()
        .unwrap();
    let users_t = db.get_table("users").unwrap();
    let companies_t = db.get_table("companies").unwrap();
    let cities = ["Tokyo", "Osaka", "Kyoto"];
    let mut rng = Rng(0xe815_7515_0000_0001);
    let mut companies: Vec<u64> = (0..12i64)
        .map(|i| companies_t.insert().set("id", i).set("city", cities[(i % 3) as usize]).commit().unwrap())
        .collect();
    let mut users: Vec<u64> = (0..60i64)
        .map(|i| {
            let mut b = users_t.insert().set("id", i).set("city", cities[(i % 3) as usize]);
            if i % 5 != 0 {
                b = b.set("age", (i * 7) % 40);
            }
            // 会社は前半 8 社にだけ (残りは誰も指していない)
            if i % 6 != 0 {
                b = b.set("company", Value::Ref(companies[(i % 8) as usize]));
            }
            b.commit().unwrap()
        })
        .collect();
    let (u, c) = (&users_t, &companies_t);
    let age = move |e: u64| get_num(u, e, "age");
    let home = move |e: u64| get_text(u, e, "city");
    let company = move |e: u64| get_ref(u, e, "company");
    let ccity = move |x: u64| get_text(c, x, "city");

    type Cond<'a> = Box<dyn Fn(u64, &[u64]) -> bool + 'a>;
    type Q<'a> = Box<dyn Fn() -> enchudb_schema::Query<'a> + 'a>;
    struct ESub<'a> {
        name: String,
        q: LiveQuery,
        query: Q<'a>,
        seen: BTreeSet<u64>,
        /// (会社, 全社員) → 入るか
        cond: Cond<'a>,
    }
    let make = |kind: u64, rng: &mut Rng| -> ESub {
        let a = cities[rng.below(3) as usize];
        let b = cities[rng.below(3) as usize];
        let x = rng.below(40) as u32;
        let staff = move |co: u64, us: &[u64]| -> Vec<u64> { us.iter().copied().filter(|&e| company(e) == Some(co)).collect() };
        let (name, query, cond): (String, Q, Cond) = match kind {
            0 => (
                format!("exists user age > {x}"),
                Box::new(move || c.all().where_exists(u.all().where_gt("age", x), "company")),
                Box::new(move |co, us| staff(co, us).iter().any(|&e| age(e).is_some_and(|g| g > x as i64))),
            ),
            1 => (
                "not exists user".into(),
                Box::new(move || c.all().where_not_exists(u.all(), "company")),
                Box::new(move |co, us| staff(co, us).is_empty()),
            ),
            2 => (
                format!("city = {a} and exists user city = {b}"),
                Box::new(move || c.where_eq("city", a).where_exists(u.where_eq("city", b), "company")),
                Box::new(move |co, us| ccity(co).as_deref() == Some(a) && staff(co, us).iter().any(|&e| home(e).as_deref() == Some(b))),
            ),
            3 => (
                "not exists user age is null".into(),
                Box::new(move || c.all().where_not_exists(u.all().where_null("age"), "company")),
                Box::new(move |co, us| !staff(co, us).iter().any(|&e| age(e).is_none())),
            ),
            4 => (
                format!("exists user where company.city = {a}"),
                Box::new(move || c.all().where_exists(u.where_eq("company.city", a), "company")),
                Box::new(move |co, us| ccity(co).as_deref() == Some(a) && !staff(co, us).is_empty()),
            ),
            _ => (
                format!("exists user age > {x} or city = {a}"),
                Box::new(move || c.all().where_exists(u.all().where_gt("age", x), "company").or(c.where_eq("city", a))),
                Box::new(move |co, us| {
                    ccity(co).as_deref() == Some(a) || staff(co, us).iter().any(|&e| age(e).is_some_and(|g| g > x as i64))
                }),
            ),
        };
        let q = query().subscribe().unwrap();
        ESub { name, q, query, seen: BTreeSet::new(), cond }
    };
    let mut subs: Vec<ESub> = (0..24).map(|i| make(i % 6, &mut rng)).collect();
    let group = db.live_group();
    let check = |subs: &mut Vec<ESub>, users: &[u64], companies: &[u64], step: usize| {
        if step % 2 == 1 {
            for s in subs.iter() {
                group.add(&s.q);
            }
            for (id, d) in group.poll() {
                let s = subs.iter_mut().find(|s| s.q.id() == id).expect("生きている購読の id");
                integrate(&mut s.seen, d);
            }
        }
        for s in subs.iter_mut() {
            if step.is_multiple_of(2) {
                integrate(&mut s.seen, s.q.poll());
            }
            let want: BTreeSet<u64> = companies.iter().copied().filter(|&co| (s.cond)(co, users)).collect();
            assert_eq!(s.seen, want, "[{}] step {step}: 積分 != 手で数えた結果", s.name);
            assert_eq!(s.q.count(), want.len(), "[{}] step {step}: count", s.name);
            let found: BTreeSet<u64> = (s.query)().find().unwrap().into_iter().collect();
            assert_eq!(found, want, "[{}] step {step}: find", s.name);
        }
    };
    check(&mut subs, &users, &companies, 0);
    let eng = db.engine();
    let mut next_id = 1000i64;
    for step in 1..1500 {
        let e = users[rng.below(users.len() as u64) as usize];
        match rng.below(10) {
            0 | 1 => users_t.entity(e).set("age", rng.below(40) as i64).commit().unwrap(),
            2 => eng.untie(e, "users.age"),
            3 => users_t.entity(e).set("city", cities[rng.below(3) as usize]).commit().unwrap(),
            4 | 5 => {
                let x = companies[rng.below(companies.len() as u64) as usize];
                users_t.entity(e).set("company", Value::Ref(x)).commit().unwrap();
            }
            6 => eng.untie(e, "users.company"),
            7 => {
                let i = rng.below(users.len() as u64) as usize;
                users_t.entity(users[i]).delete().unwrap();
                let mut b = users_t.insert().set("id", next_id).set("city", cities[rng.below(3) as usize]);
                if rng.below(2) == 0 {
                    b = b.set("age", rng.below(40) as i64);
                }
                if rng.below(3) != 0 {
                    b = b.set("company", Value::Ref(companies[rng.below(companies.len() as u64) as usize]));
                }
                users[i] = b.commit().unwrap();
                next_id += 1;
            }
            8 => {
                let x = companies[rng.below(companies.len() as u64) as usize];
                companies_t.entity(x).set("city", cities[rng.below(3) as usize]).commit().unwrap();
            }
            _ => {
                // 会社を消して作り直す (指していた社員の ref は宙に浮く)
                let i = rng.below(companies.len() as u64) as usize;
                companies_t.entity(companies[i]).delete().unwrap();
                companies[i] = companies_t.insert().set("id", next_id).set("city", cities[rng.below(3) as usize]).commit().unwrap();
                next_id += 1;
            }
        }
        if step % 5 == 0 {
            let i = rng.below(subs.len() as u64) as usize;
            let kind = rng.below(6);
            subs[i] = make(kind, &mut rng);
        }
        if step % 3 == 0 {
            check(&mut subs, &users, &companies, step);
        }
    }
    check(&mut subs, &users, &companies, 1_000_001);
    check(&mut subs, &users, &companies, 1_000_002);
    // 指していない ref 列 / 別の table を指す列は常に 0 件
    assert_eq!(c.all().where_exists(u.all(), "city").count().unwrap(), 0);
}
