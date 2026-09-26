//! 3 つ以上の table の組 (`then_ref` / `then_eq`): poll の組の差分を積分した集合・`find()`・`count()` が、
//! **テスト側で全 row を総当たりして組んだ結果** と常に一致すること。 oracle は `entity(e).get(col)` で
//! 値を読んで組むだけ (engine の live 評価を通らない)。
//!
//! 組は ref と値のつなぎを混ぜた 3〜4 段。 書き込みは全 table の row の出入り・作り直し、 つなぐ列の書き換え
//! (ref の付け替え・値の書き換え・外し)、 ref の先の値の変化を混ぜる。

use enchudb_schema::{Database, Table, TupleDelta, Value};
use std::collections::BTreeSet;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/schema_live_multi_{}_{}_{}.db", tag, std::process::id(), nanos)
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

type Tuple = Vec<u64>;

fn integrate(set: &mut BTreeSet<Tuple>, d: TupleDelta) {
    let rs: BTreeSet<Tuple> = d.removed.iter().cloned().collect();
    assert!(d.added.iter().all(|t| !rs.contains(t)), "同じ組が removed と added の両方に居る: {d:?}");
    for t in d.removed {
        assert!(set.remove(&t), "removed に未報告の組 {t:?}");
    }
    for t in d.added {
        assert!(set.insert(t.clone()), "added に報告済みの組 {t:?}");
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
    fn num(t: &Table, e: u64, col: &str) -> Option<i64> {
        match get_val(t, e, col) {
            Some(Value::Number(n)) => Some(n),
            _ => None,
        }
    }
    /// 投稿 q の作者 (生きている user)
    fn author(&self, q: u64) -> Option<u64> {
        get_ref(self.p, q, "author").filter(|a| self.users.contains(a))
    }
    /// user e の会社 (生きている会社)
    fn company(&self, e: u64) -> Option<u64> {
        get_ref(self.u, e, "company").filter(|co| self.companies.contains(co))
    }
    /// 街 v の店のうち cond を満たすもの
    fn shops_in(&self, v: &Value, open_only: bool) -> Vec<u64> {
        self.shops
            .iter()
            .copied()
            .filter(|&x| get_val(self.s, x, "city").as_ref() == Some(v) && (!open_only || World::num(self.s, x, "open") == Some(1)))
            .collect()
    }
}

#[test]
fn multi_joins_match_oracle() {
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
    let mut rng = Rng(0x3a17_0000_0000_0001);
    let companies: Vec<u64> =
        (0..5i64).map(|i| c.insert().set("id", i).set("city", CITIES[(i % 4) as usize]).commit().unwrap()).collect();
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
    let users: Vec<u64> = (0..20i64).map(|i| new_user(&mut rng, i, &companies)).collect();
    let shops: Vec<u64> = (0..8i64).map(|i| new_shop(&mut rng, 1000 + i)).collect();
    let posts: Vec<u64> = (0..16i64).map(|i| new_post(&mut rng, 2000 + i, &users)).collect();
    let mut w = World { users, shops, posts, companies, u, s, p, c };

    type Oracle<'a> = Box<dyn Fn(&World) -> BTreeSet<Tuple> + 'a>;
    type Q<'a> = Box<dyn Fn() -> enchudb_schema::MultiJoin<'a> + 'a>;
    struct Sub<'a> {
        name: String,
        live: enchudb_schema::LiveMultiJoin,
        query: Q<'a>,
        seen: BTreeSet<Tuple>,
        oracle: Oracle<'a>,
    }
    let make = |kind: u64, rng: &mut Rng| -> Sub {
        let x = rng.below(50) as i64;
        let a = CITIES[rng.below(4) as usize];
        let (name, query, oracle): (String, Q, Oracle) = match kind {
            0 => (
                format!("(公開済みの投稿, {x} 歳より上の作者, 作者の会社)"),
                Box::new(move || {
                    p.where_eq("published", 1i64).join_ref("author", u.all().where_gt("age", x)).then_ref("users", "company", c.all())
                }),
                Box::new(move |w| {
                    let mut out = BTreeSet::new();
                    for &q in &w.posts {
                        if World::num(w.p, q, "published") != Some(1) {
                            continue;
                        }
                        let Some(a) = w.author(q).filter(|&a| World::num(w.u, a, "age").is_some_and(|g| g > x)) else { continue };
                        if let Some(co) = w.company(a) {
                            out.insert(vec![q, a, co]);
                        }
                    }
                    out
                }),
            ),
            1 => (
                format!("(住人, 住む街の開いた店, 所在地が {a} の住人の会社)"),
                Box::new(move || {
                    u.all().join_eq("city", s.where_eq("open", 1i64), "city").then_ref("users", "company", c.where_eq("city", a))
                }),
                Box::new(move |w| {
                    let mut out = BTreeSet::new();
                    for &e in &w.users {
                        let Some(v) = get_val(w.u, e, "city") else { continue };
                        let Some(co) = w.company(e).filter(|&co| get_val(w.c, co, "city") == Some(Value::Text(a.into()))) else {
                            continue;
                        };
                        for x in w.shops_in(&v, true) {
                            out.insert(vec![e, x, co]);
                        }
                    }
                    out
                }),
            ),
            2 => (
                "(投稿, 作者, 作者の住む街の店)".into(),
                Box::new(move || p.all().join_ref("author", u.all()).then_eq("users", "city", s.all(), "city")),
                Box::new(move |w| {
                    let mut out = BTreeSet::new();
                    for &q in &w.posts {
                        let Some(au) = w.author(q) else { continue };
                        let Some(v) = get_val(w.u, au, "city") else { continue };
                        for x in w.shops_in(&v, false) {
                            out.insert(vec![q, au, x]);
                        }
                    }
                    out
                }),
            ),
            3 => (
                "(投稿, 作者, 作者の会社の所在地の店) — つなぐ列が ref の先".into(),
                Box::new(move || p.all().join_ref("author", u.all()).then_eq("users", "company.city", s.all(), "city")),
                Box::new(move |w| {
                    let mut out = BTreeSet::new();
                    for &q in &w.posts {
                        let Some(au) = w.author(q) else { continue };
                        // 削除済みの会社を指したままなら所在地は無い
                        let Some(v) = get_ref(w.u, au, "company").and_then(|co| get_val(w.c, co, "city")) else { continue };
                        for x in w.shops_in(&v, false) {
                            out.insert(vec![q, au, x]);
                        }
                    }
                    out
                }),
            ),
            _ => (
                "(投稿, 作者, 作者の会社, 会社の街の開いた店)".into(),
                Box::new(move || {
                    p.all()
                        .join_ref("author", u.all())
                        .then_ref("users", "company", c.all())
                        .then_eq("companies", "city", s.where_eq("open", 1i64), "city")
                }),
                Box::new(move |w| {
                    let mut out = BTreeSet::new();
                    for &q in &w.posts {
                        let Some(au) = w.author(q) else { continue };
                        let Some(co) = w.company(au) else { continue };
                        let Some(v) = get_val(w.c, co, "city") else { continue };
                        for x in w.shops_in(&v, true) {
                            out.insert(vec![q, au, co, x]);
                        }
                    }
                    out
                }),
            ),
        };
        let live = query().subscribe().unwrap();
        Sub { name, live, query, seen: BTreeSet::new(), oracle }
    };
    const KINDS: u64 = 5;
    let mut subs: Vec<Sub> = (0..10).map(|i| make(i % KINDS, &mut rng)).collect();
    let check = |subs: &mut Vec<Sub>, w: &World, step: usize| {
        for sub in subs.iter_mut() {
            integrate(&mut sub.seen, sub.live.poll());
            let want = (sub.oracle)(w);
            assert_eq!(sub.seen, want, "[{}] step {step}: 積分 != 総当たり", sub.name);
            let found: BTreeSet<Tuple> = (sub.query)().find().unwrap().into_iter().collect();
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
    // 組に居ない table からはつなげない / 同じ table が 2 度居たら決められない
    assert!(p.all().join_ref("author", u.all()).then_ref("shops", "city", c.all()).find().is_err());
    assert!(u.all().join_eq("city", u.all(), "city").then_ref("users", "company", c.all()).find().is_err());
}

/// 前の段の組の親の row (住人) と子の row (店) が、 同じ poll の中で同じ街から同じ街へ一緒に移った: 組は居続けて
/// いるので差分に出ない (removed と added の両方に出したら 「消えて入り直した」 になる)。 親の鍵が変わっても子が
/// 追いかけなければ組は出る。
#[test]
fn tuple_whose_rows_move_together_stays() {
    let path = tmp_path("together");
    cleanup(&path);
    let mut db = Database::create_growable_tiny(&path).unwrap();
    db.table("users").number("id").tag("city").primary_key("id").build().unwrap();
    db.table("shops").number("id").tag("city").primary_key("id").build().unwrap();
    db.table("posts").number("id").ref_to("author", "users").primary_key("id").build().unwrap();
    let (u, s, p) = (db.get_table("users").unwrap(), db.get_table("shops").unwrap(), db.get_table("posts").unwrap());
    let a = u.insert().set("id", 1i64).set("city", "Tokyo").commit().unwrap();
    let x = s.insert().set("id", 1i64).set("city", "Tokyo").commit().unwrap();
    let y = s.insert().set("id", 2i64).set("city", "Tokyo").commit().unwrap();
    let q = p.insert().set("id", 1i64).set("author", Value::Ref(a)).commit().unwrap();
    let live = p.all().join_ref("author", u.all()).then_eq("users", "city", s.all(), "city").subscribe().unwrap();
    let mut seen = BTreeSet::new();
    integrate(&mut seen, live.poll());
    assert_eq!(seen, BTreeSet::from([vec![q, a, x], vec![q, a, y]]));
    // 住人と店 x が一緒に Osaka へ、 店 y は Tokyo に残る
    u.entity(a).set("city", "Osaka").commit().unwrap();
    s.entity(x).set("city", "Osaka").commit().unwrap();
    let d = live.poll();
    assert_eq!((d.removed.clone(), d.added.clone()), (vec![vec![q, a, y]], vec![]), "(q, a, x) は居続ける");
    integrate(&mut seen, d);
    assert_eq!(seen, BTreeSet::from([vec![q, a, x]]));
    drop(live);
    drop((u, s, p));
    drop(db);
    cleanup(&path);
}
