//! 値で結ぶ準結合 (`where_exists_eq` / `where_not_exists_eq`): poll の差分を積分した集合・`count()`・`find()` が、
//! **テスト側で全 row を総当たりして計算した結果** と常に一致すること。 oracle は `entity(e).get(col)` で
//! 値を読んで比べるだけ (engine の live 評価を通らない)。
//!
//! 結ぶ列は Tag (vocab id)・Number・BigInt、 自分の列が ref の先 (`company.city`) の場合も。 書き込みは user
//! (値の書き換え・外し・作り直し)、 店 (開閉・街の移動・作り直し)、 会社 (所在地) を混ぜる。

use enchudb_schema::{Database, LiveDelta, LiveQuery, Table, Value};
use std::collections::BTreeSet;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/schema_live_join_{}_{}_{}.db", tag, std::process::id(), nanos)
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

fn get_val(t: &Table, e: u64, col: &str) -> Option<Value> {
    t.entity(e).get(col)
}

const CITIES: [&str; 4] = ["Tokyo", "Osaka", "Kyoto", "Nara"];
const TS: [i64; 5] = [i64::MIN, -3, 0, 7, 1 << 40];

#[test]
fn value_join_subscriptions_match_oracle() {
    let path = tmp_path("join");
    cleanup(&path);
    run(&path);
    cleanup(&path);
}

fn run(path: &str) {
    let mut db = Database::create_growable_tiny(path).unwrap();
    db.table("companies").number("id").tag("city").primary_key("id").build().unwrap();
    db.table("users")
        .number("id")
        .number("age")
        .tag("city")
        .tag("home")
        .number("zip")
        .bigint("t")
        .ref_to("company", "companies")
        .primary_key("id")
        .build()
        .unwrap();
    db.table("shops").number("id").tag("city").number("zip").bigint("t").number("open").primary_key("id").build().unwrap();
    let (users_t, shops_t, companies_t) =
        (db.get_table("users").unwrap(), db.get_table("shops").unwrap(), db.get_table("companies").unwrap());
    let mut rng = Rng(0x701e_0000_0000_0001);
    let mut companies: Vec<u64> = (0..6i64)
        .map(|i| companies_t.insert().set("id", i).set("city", CITIES[(i % 4) as usize]).commit().unwrap())
        .collect();
    let new_user = |rng: &mut Rng, id: i64, companies: &[u64]| {
        let mut b = users_t.insert().set("id", id).set("age", rng.below(50) as i64);
        if rng.below(6) != 0 {
            b = b.set("city", CITIES[rng.below(4) as usize]);
        }
        if rng.below(3) != 0 {
            b = b.set("home", CITIES[rng.below(4) as usize]);
        }
        if rng.below(5) != 0 {
            b = b.set("zip", rng.below(6) as i64);
        }
        if rng.below(4) != 0 {
            b = b.set("t", TS[rng.below(5) as usize]);
        }
        if rng.below(5) != 0 {
            b = b.set("company", Value::Ref(companies[rng.below(companies.len() as u64) as usize]));
        }
        b.commit().unwrap()
    };
    let new_shop = |rng: &mut Rng, id: i64| {
        let mut b = shops_t.insert().set("id", id).set("open", rng.below(2) as i64);
        if rng.below(6) != 0 {
            b = b.set("city", CITIES[rng.below(3) as usize]); // Nara には最初は店が無い
        }
        if rng.below(4) != 0 {
            b = b.set("zip", rng.below(6) as i64);
        }
        if rng.below(3) != 0 {
            b = b.set("t", TS[rng.below(5) as usize]);
        }
        b.commit().unwrap()
    };
    let mut users: Vec<u64> = (0..60i64).map(|i| new_user(&mut rng, i, &companies)).collect();
    let mut shops: Vec<u64> = (0..10i64).map(|i| new_shop(&mut rng, 1000 + i)).collect();
    let (u, s, c) = (&users_t, &shops_t, &companies_t);
    let uv = move |e: u64, col: &str| get_val(u, e, col);
    let sv = move |x: u64, col: &str| get_val(s, x, col);
    let open = move |x: u64| sv(x, "open") == Some(Value::Number(1));
    let ccity = move |e: u64| get_ref(u, e, "company").and_then(|co| get_val(c, co, "city"));
    // 値 v を列 col に持ち、 cond を満たす店があるか
    let any_shop = move |shops: &[u64], col: &str, v: Option<Value>, cond: &dyn Fn(u64) -> bool| {
        v.is_some_and(|v| shops.iter().any(|&x| sv(x, col).as_ref() == Some(&v) && cond(x)))
    };

    type Cond<'a> = Box<dyn Fn(u64, &[u64]) -> bool + 'a>;
    type Q<'a> = Box<dyn Fn() -> enchudb_schema::Query<'a> + 'a>;
    struct JSub<'a> {
        name: String,
        q: LiveQuery,
        query: Q<'a>,
        seen: BTreeSet<u64>,
        /// (user, 全店) → 入るか
        cond: Cond<'a>,
    }
    let make = |kind: u64, rng: &mut Rng| -> JSub {
        let a = CITIES[rng.below(4) as usize];
        let x = rng.below(50) as i64;
        let (name, query, cond): (String, Q, Cond) = match kind {
            0 => (
                "city に開いた店".into(),
                Box::new(move || u.all().where_exists_eq("city", s.where_eq("open", 1i64), "city")),
                Box::new(move |e, sh| any_shop(sh, "city", uv(e, "city"), &open)),
            ),
            1 => (
                "city に店が無い".into(),
                Box::new(move || u.all().where_not_exists_eq("city", s.all(), "city")),
                Box::new(move |e, sh| !any_shop(sh, "city", uv(e, "city"), &|_| true)),
            ),
            2 => (
                format!("age > {x} かつ zip に店"),
                Box::new(move || u.all().where_gt("age", x).where_exists_eq("zip", s.all(), "zip")),
                Box::new(move |e, sh| {
                    matches!(uv(e, "age"), Some(Value::Number(g)) if g > x) && any_shop(sh, "zip", uv(e, "zip"), &|_| true)
                }),
            ),
            3 => (
                "会社の city に開いた店".into(),
                Box::new(move || u.all().where_exists_eq("company.city", s.where_eq("open", 1i64), "city")),
                Box::new(move |e, sh| any_shop(sh, "city", ccity(e), &open)),
            ),
            4 => (
                "t (BigInt) が同じ店".into(),
                Box::new(move || u.all().where_exists_eq("t", s.all(), "t")),
                Box::new(move |e, sh| any_shop(sh, "t", uv(e, "t"), &|_| true)),
            ),
            5 => (
                format!("city = {a} または zip に開いた店"),
                Box::new(move || u.where_eq("city", a).or(u.all().where_exists_eq("zip", s.where_eq("open", 1i64), "zip"))),
                Box::new(move |e, sh| {
                    uv(e, "city") == Some(Value::Text(a.into())) || any_shop(sh, "zip", uv(e, "zip"), &open)
                }),
            ),
            6 => (
                "会社はあって、 その city に開いた店が無い".into(),
                Box::new(move || u.all().where_not_exists_eq("company.city", s.where_eq("open", 1i64), "city")),
                Box::new(move |e, sh| get_ref(u, e, "company").is_some() && !any_shop(sh, "city", ccity(e), &open)),
            ),
            7 => (
                // 0 と中身・相手の列が同じで自分の列だけ違う (family を分けること)
                "home に開いた店".into(),
                Box::new(move || u.all().where_exists_eq("home", s.where_eq("open", 1i64), "city")),
                Box::new(move |e, sh| any_shop(sh, "city", uv(e, "home"), &open)),
            ),
            _ => (
                format!("city に {a} の店 (= 自分も {a} で {a} に店がある)"),
                Box::new(move || u.all().where_exists_eq("city", s.where_eq("city", a), "city")),
                Box::new(move |e, sh| {
                    uv(e, "city") == Some(Value::Text(a.into()))
                        && any_shop(sh, "city", uv(e, "city"), &|x| sv(x, "city") == Some(Value::Text(a.into())))
                }),
            ),
        };
        let q = query().subscribe().unwrap();
        JSub { name, q, query, seen: BTreeSet::new(), cond }
    };
    let mut subs: Vec<JSub> = (0..27).map(|i| make(i % 9, &mut rng)).collect();
    let check = |subs: &mut Vec<JSub>, users: &[u64], shops: &[u64], step: usize| {
        for sub in subs.iter_mut() {
            integrate(&mut sub.seen, sub.q.poll());
            let want: BTreeSet<u64> = users.iter().copied().filter(|&e| (sub.cond)(e, shops)).collect();
            assert_eq!(sub.seen, want, "[{}] step {step}: 積分 != 総当たり", sub.name);
            assert_eq!(sub.q.count(), want.len(), "[{}] step {step}: count", sub.name);
            let found: BTreeSet<u64> = (sub.query)().find().unwrap().into_iter().collect();
            assert_eq!(found, want, "[{}] step {step}: find", sub.name);
            assert_eq!((sub.query)().count().unwrap(), want.len(), "[{}] step {step}: Query::count", sub.name);
        }
    };
    check(&mut subs, &users, &shops, 0);
    // 街単位 (`where_exists_eq` の doc の書き方): 開いた店の街ごとの件数を購読し、 0 ↔ 1 以上をまたいだ街を積む。
    // 積んだ街の住人を引いた集合が 「city に開いた店」 と一致する
    let hub = s.where_eq("open", 1i64).subscribe_counts("city").unwrap();
    let mut hubs: BTreeSet<String> = BTreeSet::new();
    let check_hub = |hubs: &mut BTreeSet<String>, users: &[u64], shops: &[u64], step: usize| {
        for (city, n) in hub.poll() {
            let Value::Text(city) = city else { panic!("city is a Tag column") };
            if n == 0 {
                assert!(hubs.remove(&city), "step {step}: 開いた店の無かった街 {city} が消えた");
            } else {
                hubs.insert(city);
            }
        }
        let got: BTreeSet<u64> = hubs.iter().flat_map(|c| u.where_eq("city", c.as_str()).find().unwrap()).collect();
        let want: BTreeSet<u64> = users.iter().copied().filter(|&e| any_shop(shops, "city", uv(e, "city"), &open)).collect();
        assert_eq!(got, want, "step {step}: 街単位の購読から引いた住人");
    };
    check_hub(&mut hubs, &users, &shops, 0);
    let eng = db.engine();
    let mut next_id = 5000i64;
    for step in 1..1500 {
        let e = users[rng.below(users.len() as u64) as usize];
        let x = shops[rng.below(shops.len() as u64) as usize];
        match rng.below(14) {
            0 => users_t.entity(e).set("city", CITIES[rng.below(4) as usize]).commit().unwrap(),
            1 => {
                if rng.below(2) == 0 {
                    eng.untie(e, "users.city")
                } else {
                    users_t.entity(e).set("home", CITIES[rng.below(4) as usize]).commit().unwrap()
                }
            }
            2 => users_t.entity(e).set("zip", rng.below(6) as i64).commit().unwrap(),
            3 => users_t.entity(e).set("t", TS[rng.below(5) as usize]).commit().unwrap(),
            4 => users_t.entity(e).set("company", Value::Ref(companies[rng.below(companies.len() as u64) as usize])).commit().unwrap(),
            5 => users_t.entity(e).set("age", rng.below(50) as i64).commit().unwrap(),
            6 => {
                let i = rng.below(users.len() as u64) as usize;
                users_t.entity(users[i]).delete().unwrap();
                users[i] = new_user(&mut rng, next_id, &companies);
                next_id += 1;
            }
            7 | 8 => shops_t.entity(x).set("open", rng.below(2) as i64).commit().unwrap(),
            9 => shops_t.entity(x).set("city", CITIES[rng.below(4) as usize]).commit().unwrap(),
            10 => shops_t.entity(x).set("zip", rng.below(6) as i64).commit().unwrap(),
            11 => shops_t.entity(x).set("t", TS[rng.below(5) as usize]).commit().unwrap(),
            12 => {
                let i = rng.below(shops.len() as u64) as usize;
                shops_t.entity(shops[i]).delete().unwrap();
                shops[i] = new_shop(&mut rng, next_id);
                next_id += 1;
            }
            _ => {
                let co = companies[rng.below(companies.len() as u64) as usize];
                if rng.below(4) == 0 {
                    let i = companies.iter().position(|&k| k == co).unwrap();
                    companies_t.entity(co).delete().unwrap();
                    companies[i] = companies_t.insert().set("id", next_id).set("city", CITIES[rng.below(4) as usize]).commit().unwrap();
                    next_id += 1;
                } else {
                    companies_t.entity(co).set("city", CITIES[rng.below(4) as usize]).commit().unwrap();
                }
            }
        }
        if step % 7 == 0 {
            let i = rng.below(subs.len() as u64) as usize;
            subs[i] = make(rng.below(9), &mut rng);
        }
        if step % 3 == 0 {
            check(&mut subs, &users, &shops, step);
            check_hub(&mut hubs, &users, &shops, step);
        }
    }
    check(&mut subs, &users, &shops, 1_000_001);
    // 型の違う列 / Leaf / Ref は常に 0 件
    assert_eq!(u.all().where_exists_eq("city", s.all(), "zip").count().unwrap(), 0);
    assert_eq!(u.all().where_exists_eq("company", s.all(), "city").count().unwrap(), 0);
}
