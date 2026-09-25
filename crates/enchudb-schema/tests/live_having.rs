//! 件数の閾値 (`where_count_ge` / `where_count_lt` / `where_value_count_ge` / `where_value_count_lt` /
//! `subscribe_having`、 SQL の `HAVING COUNT(*) >= n`): poll の差分を積分した集合・`count()`・`find()` が、
//! **テスト側で全 row を総当たりして数えた結果** と常に一致すること。 oracle は `entity(e).get(col)` で
//! 値を読んで数えるだけ (engine の live 評価を通らない)。
//!
//! 閾値は 1〜4 (同じ形で閾値だけ違う購読を並べる = 閾値ごとに別の family であること)。 数える側 (社員・店) の
//! 出入り・付け替え・中身の変化と、 数えられる側 (会社) の作り直し、 自分の列 (住む街・会社の所在地) の変化を混ぜる。

use enchudb_schema::{Database, GroupedLiveQuery, HavingDelta, LiveDelta, LiveHaving, LiveQuery, Table, Value};
use std::collections::{BTreeMap, BTreeSet};

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/schema_live_having_{}_{}_{}.db", tag, std::process::id(), nanos)
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

fn integrate_groups(set: &mut BTreeSet<String>, d: HavingDelta) {
    let name = |v: Value| match v {
        Value::Text(t) => t,
        other => panic!("city is a Tag column: {other:?}"),
    };
    for v in d.removed {
        let v = name(v);
        assert!(set.remove(&v), "removed に未報告の group {v}");
    }
    for v in d.added {
        let v = name(v);
        assert!(set.insert(v.clone()), "added に報告済みの group {v}");
    }
}

fn get_ref(t: &Table, e: u64, col: &str) -> Option<u64> {
    match t.entity(e).get(col) {
        Some(Value::Ref(x)) => Some(x),
        _ => None,
    }
}

fn get_val(t: &Table, e: u64, col: &str) -> Option<Value> {
    t.entity(e).get(col)
}

const CITIES: [&str; 4] = ["Tokyo", "Osaka", "Kyoto", "Nara"];

#[test]
fn count_thresholds_match_oracle() {
    let path = tmp_path("oracle");
    cleanup(&path);
    run(&path);
    cleanup(&path);
}

/// 状態 (全 row の今の値) から数える総当たり。
struct World<'a> {
    users: Vec<u64>,
    shops: Vec<u64>,
    companies: Vec<u64>,
    u: &'a Table<'a>,
    s: &'a Table<'a>,
    c: &'a Table<'a>,
}

impl World<'_> {
    fn age(&self, e: u64) -> i64 {
        match get_val(self.u, e, "age") {
            Some(Value::Number(a)) => a,
            _ => -1,
        }
    }
    /// 会社 co を指す社員のうち cond を満たす数
    fn staff(&self, co: u64, cond: &dyn Fn(u64) -> bool) -> u64 {
        self.users.iter().filter(|&&e| get_ref(self.u, e, "company") == Some(co) && cond(e)).count() as u64
    }
    fn open(&self, x: u64) -> bool {
        get_val(self.s, x, "open") == Some(Value::Number(1))
    }
    /// 街 v にある店のうち cond を満たす数
    fn shops_in(&self, v: &Value, cond: &dyn Fn(u64) -> bool) -> u64 {
        self.shops.iter().filter(|&&x| get_val(self.s, x, "city").as_ref() == Some(v) && cond(x)).count() as u64
    }
    fn ccity(&self, e: u64) -> Option<Value> {
        get_ref(self.u, e, "company").and_then(|co| get_val(self.c, co, "city"))
    }
}

fn run(path: &str) {
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
    db.table("shops").number("id").tag("city").number("open").primary_key("id").build().unwrap();
    let (users_t, shops_t, companies_t) =
        (db.get_table("users").unwrap(), db.get_table("shops").unwrap(), db.get_table("companies").unwrap());
    let (u, s, c) = (&users_t, &shops_t, &companies_t);
    let mut rng = Rng(0x4a71_0000_0000_0001);
    let companies: Vec<u64> =
        (0..8i64).map(|i| c.insert().set("id", i).set("city", CITIES[(i % 4) as usize]).commit().unwrap()).collect();
    let new_user = |rng: &mut Rng, id: i64, companies: &[u64]| {
        let mut b = u.insert().set("id", id).set("age", rng.below(50) as i64);
        if rng.below(6) != 0 {
            b = b.set("city", CITIES[rng.below(4) as usize]);
        }
        if rng.below(6) != 0 {
            b = b.set("company", Value::Ref(companies[rng.below(companies.len() as u64) as usize]));
        }
        b.commit().unwrap()
    };
    let new_shop = |rng: &mut Rng, id: i64| {
        let mut b = s.insert().set("id", id).set("open", rng.below(2) as i64);
        if rng.below(6) != 0 {
            b = b.set("city", CITIES[rng.below(4) as usize]);
        }
        b.commit().unwrap()
    };
    let users: Vec<u64> = (0..40i64).map(|i| new_user(&mut rng, i, &companies)).collect();
    let shops: Vec<u64> = (0..16i64).map(|i| new_shop(&mut rng, 1000 + i)).collect();
    let mut w = World { users, shops, companies, u, s, c };

    type Cond<'a> = Box<dyn Fn(&World, u64) -> bool + 'a>;
    type Q<'a> = Box<dyn Fn() -> enchudb_schema::Query<'a> + 'a>;
    struct Sub<'a> {
        name: String,
        /// 結果が会社 (true) か社員 (false) か
        of_companies: bool,
        q: LiveQuery,
        query: Q<'a>,
        seen: BTreeSet<u64>,
        cond: Cond<'a>,
    }
    let make = |kind: u64, rng: &mut Rng| -> Sub {
        let k = 1 + rng.below(4);
        let x = rng.below(50) as i64;
        let a = CITIES[rng.below(4) as usize];
        let (name, of_companies, query, cond): (String, bool, Q, Cond) = match kind {
            0 => (
                format!("社員が {k} 人以上の会社"),
                true,
                Box::new(move || c.all().where_count_ge(u.all(), "company", k)),
                Box::new(move |w, co| w.staff(co, &|_| true) >= k),
            ),
            1 => (
                format!("{x} 歳より上の社員が {k} 人以上の会社"),
                true,
                Box::new(move || c.all().where_count_ge(u.all().where_gt("age", x), "company", k)),
                Box::new(move |w, co| w.staff(co, &|e| w.age(e) > x) >= k),
            ),
            2 => (
                format!("社員が {k} 人未満の会社 (0 人も)"),
                true,
                Box::new(move || c.all().where_count_lt(u.all(), "company", k)),
                Box::new(move |w, co| w.staff(co, &|_| true) < k),
            ),
            3 => (
                format!("開いた店が {k} 軒以上ある街の住人"),
                false,
                Box::new(move || u.all().where_value_count_ge("city", s.where_eq("open", 1i64), "city", k)),
                Box::new(move |w, e| get_val(w.u, e, "city").is_some_and(|v| w.shops_in(&v, &|x| w.open(x)) >= k)),
            ),
            4 => (
                format!("店が {k} 軒未満の街の住人 (街が無い人も)"),
                false,
                Box::new(move || u.all().where_value_count_lt("city", s.all(), "city", k)),
                Box::new(move |w, e| get_val(w.u, e, "city").is_none_or(|v| w.shops_in(&v, &|_| true) < k)),
            ),
            5 => (
                format!("会社の所在地に開いた店が {k} 軒以上ある社員"),
                false,
                Box::new(move || u.all().where_value_count_ge("company.city", s.where_eq("open", 1i64), "city", k)),
                Box::new(move |w, e| w.ccity(e).is_some_and(|v| w.shops_in(&v, &|x| w.open(x)) >= k)),
            ),
            _ => (
                format!("所在地が {a} または社員が {k} 人以上の会社"),
                true,
                Box::new(move || c.where_eq("city", a).or(c.all().where_count_ge(u.all(), "company", k))),
                Box::new(move |w, co| {
                    get_val(w.c, co, "city") == Some(Value::Text(a.into())) || w.staff(co, &|_| true) >= k
                }),
            ),
        };
        let q = query().subscribe().unwrap();
        Sub { name, of_companies, q, query, seen: BTreeSet::new(), cond }
    };
    const KINDS: u64 = 7;
    let mut subs: Vec<Sub> = (0..21).map(|i| make(i % KINDS, &mut rng)).collect();
    // 開いた店が k 軒以上の街 (group の値の購読)
    let mut havings: Vec<(u64, LiveHaving, BTreeSet<String>)> = (1..=4u64)
        .map(|k| (k, s.where_eq("open", 1i64).subscribe_having("city", k).unwrap(), BTreeSet::new()))
        .collect();
    // 会社単位の購読の社員への条件に件数の閾値 (members / count が 1 回の評価で数える): 所在地が a の会社の、
    // 住む街に開いた店が k 軒以上ある社員
    let grouped: Vec<(&str, u64, GroupedLiveQuery)> = [("Tokyo", 1u64), ("Osaka", 2), ("Kyoto", 3)]
        .into_iter()
        .map(|(a, k)| {
            let q = u.where_eq("company.city", a).where_value_count_ge("city", s.where_eq("open", 1i64), "city", k);
            (a, k, q.subscribe_grouped().unwrap())
        })
        .collect();
    let check_grouped = |w: &World, step: usize| {
        for (a, k, g) in &grouped {
            g.poll();
            let want: BTreeSet<u64> = w
                .users
                .iter()
                .copied()
                .filter(|&e| {
                    w.ccity(e) == Some(Value::Text((*a).into()))
                        && get_val(w.u, e, "city").is_some_and(|v| w.shops_in(&v, &|x| w.open(x)) >= *k)
                })
                .collect();
            let got: BTreeSet<u64> = g.groups().into_iter().flat_map(|co| g.members(co)).collect();
            assert_eq!(got, want, "step {step}: 会社 {a} の、 開いた店が {k} 軒以上の街の社員 (members)");
            assert_eq!(g.count(), want.len(), "step {step}: grouped count ({a}, {k})");
        }
    };
    let check = |subs: &mut Vec<Sub>, havings: &mut Vec<(u64, LiveHaving, BTreeSet<String>)>, w: &World, step: usize| {
        check_grouped(w, step);
        for sub in subs.iter_mut() {
            integrate(&mut sub.seen, sub.q.poll());
            let rows = if sub.of_companies { &w.companies } else { &w.users };
            let want: BTreeSet<u64> = rows.iter().copied().filter(|&e| (sub.cond)(w, e)).collect();
            assert_eq!(sub.seen, want, "[{}] step {step}: 積分 != 総当たり", sub.name);
            assert_eq!(sub.q.count(), want.len(), "[{}] step {step}: count", sub.name);
            let found: BTreeSet<u64> = (sub.query)().find().unwrap().into_iter().collect();
            assert_eq!(found, want, "[{}] step {step}: find", sub.name);
            assert_eq!((sub.query)().count().unwrap(), want.len(), "[{}] step {step}: Query::count", sub.name);
        }
        for (k, h, seen) in havings.iter_mut() {
            integrate_groups(seen, h.poll());
            let mut n: BTreeMap<String, u64> = BTreeMap::new();
            for &x in &w.shops {
                if let (Some(Value::Text(city)), true) = (get_val(w.s, x, "city"), w.open(x)) {
                    *n.entry(city).or_default() += 1;
                }
            }
            let want: BTreeSet<String> = n.iter().filter(|&(_, &m)| m >= *k).map(|(c, _)| c.clone()).collect();
            assert_eq!(*seen, want, "step {step}: 開いた店が {k} 軒以上の街");
            let groups: BTreeSet<String> =
                h.groups().into_iter().map(|v| if let Value::Text(t) = v { t } else { unreachable!() }).collect();
            assert_eq!(groups, want, "step {step}: groups (k = {k})");
            for city in CITIES {
                assert_eq!(h.count(&Value::Text(city.into())), n.get(city).copied().unwrap_or(0), "step {step}: count {city}");
            }
        }
    };
    check(&mut subs, &mut havings, &w, 0);
    let eng = db.engine();
    let mut next_id = 5000i64;
    for step in 1..1500 {
        let e = w.users[rng.below(w.users.len() as u64) as usize];
        let x = w.shops[rng.below(w.shops.len() as u64) as usize];
        let co = w.companies[rng.below(w.companies.len() as u64) as usize];
        match rng.below(12) {
            0 | 1 => u.entity(e).set("company", Value::Ref(co)).commit().unwrap(),
            2 => eng.untie(e, "users.company"),
            3 => u.entity(e).set("age", rng.below(50) as i64).commit().unwrap(),
            4 => u.entity(e).set("city", CITIES[rng.below(4) as usize]).commit().unwrap(),
            5 => {
                let i = rng.below(w.users.len() as u64) as usize;
                u.entity(w.users[i]).delete().unwrap();
                w.users[i] = new_user(&mut rng, next_id, &w.companies);
                next_id += 1;
            }
            6 | 7 => s.entity(x).set("open", rng.below(2) as i64).commit().unwrap(),
            8 => s.entity(x).set("city", CITIES[rng.below(4) as usize]).commit().unwrap(),
            9 => {
                let i = rng.below(w.shops.len() as u64) as usize;
                s.entity(w.shops[i]).delete().unwrap();
                w.shops[i] = new_shop(&mut rng, next_id);
                next_id += 1;
            }
            10 => c.entity(co).set("city", CITIES[rng.below(4) as usize]).commit().unwrap(),
            _ => {
                // 会社の作り直し (指していた社員の ref は消えた会社を指したまま)
                let i = w.companies.iter().position(|&k| k == co).unwrap();
                c.entity(co).delete().unwrap();
                w.companies[i] = c.insert().set("id", next_id).set("city", CITIES[rng.below(4) as usize]).commit().unwrap();
                next_id += 1;
            }
        }
        if step % 7 == 0 {
            let i = rng.below(subs.len() as u64) as usize;
            subs[i] = make(rng.below(KINDS), &mut rng);
        }
        if step % 3 == 0 {
            check(&mut subs, &mut havings, &w, step);
        }
    }
    check(&mut subs, &mut havings, &w, 1_000_001);
    // 閾値 0: ge は条件なし、 lt は常に 0 件。 subscribe_having(0) は BadValue
    assert_eq!(c.all().where_count_ge(u.all(), "company", 0).count().unwrap(), w.companies.len());
    assert_eq!(c.all().where_count_lt(u.all(), "company", 0).count().unwrap(), 0);
    assert!(s.all().subscribe_having("city", 0).is_err());
}
