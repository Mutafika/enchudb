//! 組を返す JOIN (`join_ref` / `join_eq`): poll の組の差分を積分した集合・`find()`・`count()` が、
//! **テスト側で全 row を総当たりして組んだ結果** と常に一致すること。 oracle は `entity(e).get(col)` で
//! 値を読んで組むだけ (engine の live 評価を通らない)。
//!
//! 書き込みは両側の row の出入り (条件の列の書き換え・作り直し)、 結ぶ列の書き換え (ref の付け替え、 値の
//! 書き換え・外し)、 ref の先の値の変化 (会社の所在地) を混ぜる。

use enchudb_schema::{Database, PairDelta, Table, Value};
use std::collections::BTreeSet;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/schema_live_pairs_{}_{}_{}.db", tag, std::process::id(), nanos)
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

type Pair = (u64, u64);

fn integrate(set: &mut BTreeSet<Pair>, d: PairDelta) {
    // row の作り直しは eid を使い回さない (growable) ので、 同じ組が removed と added の両方に出るのは誤り
    // (両方に居たまま動いた組を 「消えて入り直した」 と報告している)
    let rs: BTreeSet<Pair> = d.removed.iter().copied().collect();
    assert!(d.added.iter().all(|p| !rs.contains(p)), "同じ組が removed と added の両方に居る: {d:?}");
    for p in d.removed {
        assert!(set.remove(&p), "removed に未報告の組 {p:?}");
    }
    for p in d.added {
        assert!(set.insert(p), "added に報告済みの組 {p:?}");
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

struct World<'a> {
    users: Vec<u64>,
    shops: Vec<u64>,
    posts: Vec<u64>,
    companies: Vec<u64>,
    u: &'a Table<'a>,
    s: &'a Table<'a>,
    p: &'a Table<'a>,
    c: &'a Table<'a>,
}

impl World<'_> {
    fn age(&self, e: u64) -> i64 {
        match get_val(self.u, e, "age") {
            Some(Value::Number(a)) => a,
            _ => -1,
        }
    }
    fn num(t: &Table, e: u64, col: &str) -> Option<i64> {
        match get_val(t, e, col) {
            Some(Value::Number(n)) => Some(n),
            _ => None,
        }
    }
    /// 今生きている row か (削除済みの row を指したままの ref は組に入らない)
    fn alive(rows: &[u64], e: u64) -> bool {
        rows.contains(&e)
    }
}

#[test]
fn pair_joins_match_oracle() {
    let path = tmp_path("oracle");
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
        .ref_to("company", "companies")
        .primary_key("id")
        .build()
        .unwrap();
    db.table("shops").number("id").tag("city").number("open").primary_key("id").build().unwrap();
    db.table("posts").number("id").number("published").ref_to("author", "users").primary_key("id").build().unwrap();
    let (ut, st, pt, ct) =
        (db.get_table("users").unwrap(), db.get_table("shops").unwrap(), db.get_table("posts").unwrap(), db.get_table("companies").unwrap());
    let (u, s, p, c) = (&ut, &st, &pt, &ct);
    let mut rng = Rng(0x9a12_0000_0000_0001);
    let companies: Vec<u64> =
        (0..6i64).map(|i| c.insert().set("id", i).set("city", CITIES[(i % 4) as usize]).commit().unwrap()).collect();
    let new_user = |rng: &mut Rng, id: i64, companies: &[u64]| {
        let mut b = u.insert().set("id", id).set("age", rng.below(50) as i64);
        if rng.below(6) != 0 {
            b = b.set("city", CITIES[rng.below(4) as usize]);
        }
        if rng.below(5) != 0 {
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
    let new_post = |rng: &mut Rng, id: i64, users: &[u64]| {
        let mut b = p.insert().set("id", id).set("published", rng.below(2) as i64);
        if rng.below(6) != 0 {
            b = b.set("author", Value::Ref(users[rng.below(users.len() as u64) as usize]));
        }
        b.commit().unwrap()
    };
    let users: Vec<u64> = (0..30i64).map(|i| new_user(&mut rng, i, &companies)).collect();
    let shops: Vec<u64> = (0..10i64).map(|i| new_shop(&mut rng, 1000 + i)).collect();
    let posts: Vec<u64> = (0..25i64).map(|i| new_post(&mut rng, 2000 + i, &users)).collect();
    let mut w = World { users, shops, posts, companies, u, s, p, c };

    type Oracle<'a> = Box<dyn Fn(&World) -> BTreeSet<Pair> + 'a>;
    type Q<'a> = Box<dyn Fn() -> enchudb_schema::JoinQuery<'a> + 'a>;
    struct Sub<'a> {
        name: String,
        live: enchudb_schema::LiveJoin,
        query: Q<'a>,
        seen: BTreeSet<Pair>,
        oracle: Oracle<'a>,
    }
    let make = |kind: u64, rng: &mut Rng| -> Sub {
        let x = rng.below(50) as i64;
        let a = CITIES[rng.below(4) as usize];
        let (name, query, oracle): (String, Q, Oracle) = match kind {
            0 => (
                format!("公開済みの投稿と {x} 歳より上の作者"),
                Box::new(move || p.where_eq("published", 1i64).join_ref("author", u.all().where_gt("age", x))),
                Box::new(move |w| {
                    w.posts
                        .iter()
                        .filter(|&&q| World::num(w.p, q, "published") == Some(1))
                        .filter_map(|&q| get_ref(w.p, q, "author").map(|a| (q, a)))
                        .filter(|&(_, a)| World::alive(&w.users, a) && w.age(a) > x)
                        .collect()
                }),
            ),
            1 => (
                "住人と、 住む街の開いた店".into(),
                Box::new(move || u.all().join_eq("city", s.where_eq("open", 1i64), "city")),
                Box::new(move |w| {
                    let mut out = BTreeSet::new();
                    for &e in &w.users {
                        let Some(v) = get_val(w.u, e, "city") else { continue };
                        for &x in &w.shops {
                            if get_val(w.s, x, "city").as_ref() == Some(&v) && World::num(w.s, x, "open") == Some(1) {
                                out.insert((e, x));
                            }
                        }
                    }
                    out
                }),
            ),
            2 => (
                format!("{x} 歳より上の社員と、 会社の所在地の店"),
                Box::new(move || u.all().where_gt("age", x).join_eq("company.city", s.all(), "city")),
                Box::new(move |w| {
                    let mut out = BTreeSet::new();
                    for &e in &w.users {
                        if w.age(e) <= x {
                            continue;
                        }
                        let Some(v) = get_ref(w.u, e, "company").and_then(|co| get_val(w.c, co, "city")) else { continue };
                        for &sh in &w.shops {
                            if get_val(w.s, sh, "city").as_ref() == Some(&v) {
                                out.insert((e, sh));
                            }
                        }
                    }
                    out
                }),
            ),
            _ => (
                format!("社員と、 所在地が {a} の会社"),
                Box::new(move || u.all().join_ref("company", c.where_eq("city", a))),
                Box::new(move |w| {
                    w.users
                        .iter()
                        .filter_map(|&e| get_ref(w.u, e, "company").map(|co| (e, co)))
                        .filter(|&(_, co)| World::alive(&w.companies, co) && get_val(w.c, co, "city") == Some(Value::Text(a.into())))
                        .collect()
                }),
            ),
        };
        let live = query().subscribe().unwrap();
        Sub { name, live, query, seen: BTreeSet::new(), oracle }
    };
    const KINDS: u64 = 4;
    let mut subs: Vec<Sub> = (0..12).map(|i| make(i % KINDS, &mut rng)).collect();
    let check = |subs: &mut Vec<Sub>, w: &World, step: usize| {
        for sub in subs.iter_mut() {
            integrate(&mut sub.seen, sub.live.poll());
            let want = (sub.oracle)(w);
            assert_eq!(sub.seen, want, "[{}] step {step}: 積分 != 総当たり", sub.name);
            let found: BTreeSet<Pair> = (sub.query)().find().unwrap().into_iter().collect();
            assert_eq!(found, want, "[{}] step {step}: find", sub.name);
            assert_eq!((sub.query)().count().unwrap(), want.len(), "[{}] step {step}: count", sub.name);
        }
    };
    check(&mut subs, &w, 0);
    let eng = db.engine();
    let mut next_id = 5000i64;
    for step in 1..1200 {
        let e = w.users[rng.below(w.users.len() as u64) as usize];
        let x = w.shops[rng.below(w.shops.len() as u64) as usize];
        let q = w.posts[rng.below(w.posts.len() as u64) as usize];
        let co = w.companies[rng.below(w.companies.len() as u64) as usize];
        match rng.below(16) {
            0 => u.entity(e).set("city", CITIES[rng.below(4) as usize]).commit().unwrap(),
            1 => eng.untie(e, "users.city"),
            2 => u.entity(e).set("age", rng.below(50) as i64).commit().unwrap(),
            3 => u.entity(e).set("company", Value::Ref(co)).commit().unwrap(),
            4 => {
                let i = rng.below(w.users.len() as u64) as usize;
                u.entity(w.users[i]).delete().unwrap();
                w.users[i] = new_user(&mut rng, next_id, &w.companies);
                next_id += 1;
            }
            5 | 6 => s.entity(x).set("open", rng.below(2) as i64).commit().unwrap(),
            7 => s.entity(x).set("city", CITIES[rng.below(4) as usize]).commit().unwrap(),
            8 => {
                let i = rng.below(w.shops.len() as u64) as usize;
                s.entity(w.shops[i]).delete().unwrap();
                w.shops[i] = new_shop(&mut rng, next_id);
                next_id += 1;
            }
            9 | 10 => p.entity(q).set("author", Value::Ref(e)).commit().unwrap(),
            11 => p.entity(q).set("published", rng.below(2) as i64).commit().unwrap(),
            12 => eng.untie(q, "posts.author"),
            13 => {
                let i = rng.below(w.posts.len() as u64) as usize;
                p.entity(w.posts[i]).delete().unwrap();
                w.posts[i] = new_post(&mut rng, next_id, &w.users);
                next_id += 1;
            }
            14 => c.entity(co).set("city", CITIES[rng.below(4) as usize]).commit().unwrap(),
            _ => {
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
            check(&mut subs, &w, step);
        }
    }
    check(&mut subs, &w, 1_000_001);
    // 結べない列は BadValue
    assert!(u.all().join_eq("city", s.all(), "open").subscribe().is_err(), "Tag と Number");
    assert!(u.all().join_ref("city", s.all()).find().is_err(), "ref 列でない");
    assert!(p.all().join_ref("author", s.all()).find().is_err(), "別の table を指す ref");
}
