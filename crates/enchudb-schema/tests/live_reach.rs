//! 到達 (`reachable`、 辺の table をたどる再帰): poll の差分を積分した集合・`find()`・`count()` が、 **テスト側で辺を
//! 読んで BFS した届く row** と常に一致すること。 oracle は `entity(e).get(col)` で辺の始点 / 終点を読むだけ
//! (engine の live 評価を通らない)。
//!
//! 書き込みは辺の追加・削除・付け替え (始点も終点も)・終点を外す・辺の種類 (辺の条件) の変化、 seed / 結果を絞る条件の
//! 変化、 row の作り直し (辺はその eid を指したまま)。 輪も多重辺もある。

use enchudb_schema::{Database, LiveDelta, Table, Value};
use std::collections::{BTreeMap, BTreeSet};

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/schema_live_reach_{}_{}_{}.db", tag, std::process::id(), nanos)
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

fn num(t: &Table, e: u64, col: &str) -> Option<i64> {
    match t.entity(e).get(col) {
        Some(Value::Number(n)) => Some(n),
        _ => None,
    }
}

fn rf(t: &Table, e: u64, col: &str) -> Option<u64> {
    match t.entity(e).get(col) {
        Some(Value::Ref(x)) => Some(x),
        _ => None,
    }
}

#[test]
fn reachable_matches_oracle() {
    let path = tmp_path("oracle");
    cleanup(&path);
    run(&path);
    cleanup(&path);
}

fn run(path: &str) {
    // slot の再利用 (同じ eid が別の row になる) は容量に着いてからだけ — 容量を小さくして起こす
    let mut db = Database::create_with_capacity(path, 128).unwrap();
    db.table("users").number("id").number("dept").number("age").primary_key("id").build().unwrap();
    db.table("follows").number("kind").ref_to("from", "users").ref_to("to", "users").build().unwrap();
    let (ut, ft) = (db.get_table("users").unwrap(), db.get_table("follows").unwrap());
    let (u, f) = (&ut, &ft);
    let mut rng = Rng(0x2eac_0000_0000_0001);
    let mut users: Vec<u64> = Vec::new();
    for i in 0..64i64 {
        users.push(u.insert().set("id", i).set("dept", rng.below(4) as i64).set("age", rng.below(50) as i64).commit().unwrap());
    }
    let mut edges: Vec<u64> = Vec::new();
    let new_edge = |rng: &mut Rng, users: &[u64]| -> u64 {
        let a = users[rng.below(users.len() as u64) as usize];
        let b = users[rng.below(users.len() as u64) as usize];
        f.insert().set("kind", rng.below(3) as i64).set("from", Value::Ref(a)).set("to", Value::Ref(b)).commit().unwrap()
    };
    for _ in 0..40 {
        edges.push(new_edge(&mut rng, &users));
    }
    // 総当たり: 辺 (kind が edge_ok) を 1 本以上たどって seed から届く row
    let oracle = |users: &[u64], edges: &[u64], seed: &dyn Fn(u64) -> bool, edge_ok: &dyn Fn(u64) -> bool, keep: &dyn Fn(u64) -> bool| {
        let mut adj: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        for &e in edges.iter().filter(|&&e| edge_ok(e)) {
            if let (Some(a), Some(b)) = (rf(f, e, "from"), rf(f, e, "to")) {
                adj.entry(a).or_default().push(b);
            }
        }
        let mut reach = BTreeSet::new();
        let mut stack: Vec<u64> = users.iter().copied().filter(|&s| seed(s)).collect();
        while let Some(x) = stack.pop() {
            for &y in adj.get(&x).into_iter().flatten() {
                if reach.insert(y) {
                    stack.push(y);
                }
            }
        }
        users.iter().copied().filter(|x| reach.contains(x) && keep(*x)).collect::<BTreeSet<u64>>()
    };
    type Q<'a> = Box<dyn Fn() -> enchudb_schema::ReachQuery<'a> + 'a>;
    type Pred<'a> = Box<dyn Fn(u64) -> bool + 'a>;
    struct Sub<'a> {
        name: String,
        live: enchudb_schema::LiveReach,
        query: Q<'a>,
        seen: BTreeSet<u64>,
        seed: Pred<'a>,
        edge_ok: Pred<'a>,
        keep: Pred<'a>,
    }
    let make = |kind: u64, rng: &mut Rng, users: &[u64]| -> Sub {
        let d = rng.below(4) as i64;
        let x = rng.below(50) as i64;
        let k = rng.below(3) as i64;
        let who = users[rng.below(users.len() as u64) as usize];
        let id = num(u, who, "id").unwrap();
        let (name, query, seed, edge_ok, keep): (String, Q, Pred, Pred, Pred) = match kind {
            0 => (
                format!("id {id} から届く人"),
                Box::new(move || u.all().reachable(f.all(), "from", "to", u.where_eq("id", id))),
                Box::new(move |e| num(u, e, "id") == Some(id)),
                Box::new(|_| true),
                Box::new(|_| true),
            ),
            1 => (
                format!("部署 {d} から種類 {k} の辺で届く {x} 歳より上"),
                Box::new(move || u.all().where_gt("age", x).reachable(f.where_eq("kind", k), "from", "to", u.where_eq("dept", d))),
                Box::new(move |e| num(u, e, "dept") == Some(d)),
                Box::new(move |e| num(f, e, "kind") == Some(k)),
                Box::new(move |e| num(u, e, "age").is_some_and(|a| a > x)),
            ),
            _ => (
                format!("{x} 歳より上から種類 {k} 以外の辺で届く部署 {d}"),
                Box::new(move || u.where_eq("dept", d).reachable(f.all().where_ne("kind", k), "from", "to", u.all().where_gt("age", x))),
                Box::new(move |e| num(u, e, "age").is_some_and(|a| a > x)),
                Box::new(move |e| num(f, e, "kind").is_some_and(|v| v != k)),
                Box::new(move |e| num(u, e, "dept") == Some(d)),
            ),
        };
        let live = query().subscribe().unwrap();
        Sub { name, live, query, seen: BTreeSet::new(), seed, edge_ok, keep }
    };
    let mut subs: Vec<Sub> = (0..9).map(|i| make(i % 3, &mut rng, &users)).collect();
    // reborn = 前回の check から作り直した row の eid (同じ eid が別の row になった)。 前も後も結果に居るなら
    // 「出て入り直した」 (removed と added の両方) として届くこと
    let check = |subs: &mut Vec<Sub>, users: &[u64], edges: &[u64], reborn: &mut BTreeSet<u64>, step: usize| {
        for s in subs.iter_mut() {
            let before = s.seen.clone();
            let d = s.live.poll();
            let (rm, ad): (BTreeSet<u64>, BTreeSet<u64>) = (d.removed.iter().copied().collect(), d.added.iter().copied().collect());
            integrate(&mut s.seen, d);
            let want = oracle(users, edges, &s.seed, &s.edge_ok, &s.keep);
            for x in reborn.iter().filter(|x| before.contains(x) && want.contains(x)) {
                assert!(rm.contains(x) && ad.contains(x), "[{}] step {step}: 作り直した row {x} が入り直していない", s.name);
            }
            assert_eq!(s.seen, want, "[{}] step {step}: 積分 != 総当たり", s.name);
            let found: BTreeSet<u64> = (s.query)().find().unwrap().into_iter().collect();
            assert_eq!(found, want, "[{}] step {step}: find", s.name);
            assert_eq!((s.query)().count().unwrap(), want.len(), "[{}] step {step}: count", s.name);
        }
        reborn.clear();
    };
    let mut reborn = BTreeSet::new();
    let mut reborn_seen = 0;
    check(&mut subs, &users, &edges, &mut reborn, 0);
    let eng = db.engine();
    let mut next_id = 1000i64;
    for step in 1..1500 {
        let a = users[rng.below(users.len() as u64) as usize];
        let ei = rng.below(edges.len() as u64) as usize;
        let e = edges[ei];
        match rng.below(12) {
            0 | 1 if edges.len() < 44 => edges.push(new_edge(&mut rng, &users)),
            2 if edges.len() > 5 => {
                f.entity(e).delete().unwrap();
                edges.swap_remove(ei);
            }
            3 => f.entity(e).set("to", Value::Ref(a)).commit().unwrap(),
            4 => f.entity(e).set("from", Value::Ref(a)).commit().unwrap(),
            5 => eng.untie(e, "follows.to"),
            6 => f.entity(e).set("kind", rng.below(3) as i64).commit().unwrap(),
            7 => u.entity(a).set("dept", rng.below(4) as i64).commit().unwrap(),
            8 => u.entity(a).set("age", rng.below(50) as i64).commit().unwrap(),
            9 => {
                // 作り直し (辺は消えた row を指したまま)
                let i = users.iter().position(|&k| k == a).unwrap();
                u.entity(a).delete().unwrap();
                users[i] = u.insert().set("id", next_id).set("dept", rng.below(4) as i64).set("age", rng.below(50) as i64).commit().unwrap();
                if users[i] == a {
                    reborn.insert(a);
                    reborn_seen += 1;
                }
                next_id += 1;
            }
            _ => {
                // 辺の作り直し (同じ eid が別の辺になることもある)
                f.entity(e).delete().unwrap();
                edges[ei] = new_edge(&mut rng, &users);
            }
        }
        if step % 11 == 0 {
            let i = rng.below(subs.len() as u64) as usize;
            subs[i] = make(rng.below(3), &mut rng, &users);
        }
        if step % 3 == 0 {
            check(&mut subs, &users, &edges, &mut reborn, step);
        }
    }
    check(&mut subs, &users, &edges, &mut reborn, 1_000_001);
    assert!(reborn_seen > 20, "eid の使い回しが起きていない ({reborn_seen})");
    // 辺の列がこの table を指す ref 列でない / seed が別の table なら BadValue
    assert!(u.all().reachable(f.all(), "kind", "to", u.all()).find().is_err());
    assert!(u.all().reachable(f.all(), "from", "to", f.all()).subscribe().is_err());
}

/// 同じ poll で支えを探し直す row が 2 つ: X (支えの辺 B → X が消えた) を先に探すと、 もう 1 つの入り口 A も支えを
/// 探している最中で届かない。 A はあとで別の支え C で届き直すので、 X も A から届き直すこと。
#[test]
fn resupported_row_reaches_rows_decided_before_it() {
    let path = tmp_path("pending");
    cleanup(&path);
    let mut db = Database::create_growable_tiny(&path).unwrap();
    db.table("users").number("id").number("seed").primary_key("id").build().unwrap();
    db.table("follows").ref_to("from", "users").ref_to("to", "users").build().unwrap();
    let (u, f) = (db.get_table("users").unwrap(), db.get_table("follows").unwrap());
    let node = |id: i64, seed: i64| u.insert().set("id", id).set("seed", seed).commit().unwrap();
    let (s, a, b, c, x) = (node(0, 1), node(1, 0), node(2, 0), node(3, 0), node(4, 0));
    let edge = |p: u64, q: u64| f.insert().set("from", Value::Ref(p)).set("to", Value::Ref(q)).commit().unwrap();
    // 消す辺の row は S → A が先 (支えを探す順は後に積んだ X が先)
    let sa = edge(s, a);
    let bx = edge(b, x);
    edge(s, b);
    edge(s, c);
    edge(c, a);
    let live = u.all().reachable(f.all(), "from", "to", u.where_eq("seed", 1i64)).subscribe().unwrap();
    let mut seen = BTreeSet::new();
    integrate(&mut seen, live.poll());
    assert_eq!(seen, BTreeSet::from([a, b, c, x]));
    // X は B から届いている。 A → X を足しても X の支えは B のまま
    edge(a, x);
    integrate(&mut seen, live.poll());
    f.entity(sa).delete().unwrap();
    f.entity(bx).delete().unwrap();
    let d = live.poll();
    assert!(d.added.is_empty() && d.removed.is_empty(), "A は C から、 X は A から届いたまま: {d:?}");
    drop(live);
    drop((u, f));
    drop(db);
    cleanup(&path);
}
