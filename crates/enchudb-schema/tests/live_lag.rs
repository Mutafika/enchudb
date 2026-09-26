//! window の 1 つ前の row (`lag`): poll の差分を積分した組の集合・`find()`・`count()` が、 **テスト側で group に分けて
//! 並べた隣どうしの組** と常に一致すること。 oracle は `entity(e).get(col)` を読むだけ (engine の live 評価を通らない)。
//!
//! 書き込みは並びの値 (Number / BigInt) の書き換え・外す、 group の付け替え (ref の先の値も)、 条件の変化、 row の作り直し
//! (容量を決めた DB で同じ eid を使い回す、 半分は同じ値で)。 同じ値の row が多い (並びは eid の昇順で決まる)。

use enchudb_schema::{Database, PairDelta, Table, Value};
use std::collections::{BTreeMap, BTreeSet};

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/schema_live_lag_{}_{}_{}.db", tag, std::process::id(), nanos)
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

fn get(t: &Table, e: u64, col: &str) -> Option<Value> {
    t.entity(e).get(col)
}

fn num(t: &Table, e: u64, col: &str) -> Option<i64> {
    match get(t, e, col) {
        Some(Value::Number(n)) => Some(n),
        _ => None,
    }
}

fn rf(t: &Table, e: u64, col: &str) -> Option<u64> {
    match get(t, e, col) {
        Some(Value::Ref(x)) => Some(x),
        _ => None,
    }
}

#[test]
fn lag_matches_oracle() {
    let path = tmp_path("oracle");
    cleanup(&path);
    run(&path);
    cleanup(&path);
}

fn run(path: &str) {
    let mut db = Database::create_with_capacity(path, 128).unwrap();
    db.table("users").number("id").tag("team").primary_key("id").build().unwrap();
    db.table("events").number("id").number("at").bigint("big").number("kind").ref_to("user", "users").primary_key("id").build().unwrap();
    let (ut, et) = (db.get_table("users").unwrap(), db.get_table("events").unwrap());
    let (u, ev) = (&ut, &et);
    let mut rng = Rng(0x1a90_0000_0000_0001);
    let teams = ["red", "blue", "green"];
    let users: Vec<u64> = (0..6i64).map(|i| u.insert().set("id", i).set("team", teams[rng.below(3) as usize]).commit().unwrap()).collect();
    let mut next_id = 1000i64;
    let new_event = |rng: &mut Rng, id: i64| -> u64 {
        let mut b = ev.insert().set("id", id).set("at", rng.below(20) as i64).set("big", rng.below(20) as i64 - 10).set("kind", rng.below(3) as i64);
        if rng.below(6) != 0 {
            b = b.set("user", Value::Ref(users[rng.below(users.len() as u64) as usize]));
        }
        b.commit().unwrap()
    };
    let cap = db.engine().table_eid_usage("events").unwrap().capacity as usize;
    let mut events: Vec<u64> = (0..cap).map(|i| new_event(&mut rng, i as i64)).collect();
    type Val<'a> = Box<dyn Fn(u64) -> Option<Value> + 'a>;
    type Pred<'a> = Box<dyn Fn(u64) -> bool + 'a>;
    // 総当たり: group ごとに (並び, eid) で並べた隣どうし
    let oracle = |events: &[u64], part: &Val, ord: &Val, keep: &Pred| -> BTreeSet<Pair> {
        let mut groups: BTreeMap<String, Vec<(i64, u64)>> = BTreeMap::new();
        for &e in events.iter().filter(|&&e| keep(e)) {
            if let (Some(p), Some(Value::Number(o))) = (part(e), ord(e)) {
                groups.entry(format!("{p:?}")).or_default().push((o, e));
            }
        }
        let mut out = BTreeSet::new();
        for g in groups.values_mut() {
            g.sort();
            out.extend(g.windows(2).map(|w| (w[1].1, w[0].1)));
        }
        out
    };
    type Q<'a> = Box<dyn Fn() -> enchudb_schema::LagQuery<'a> + 'a>;
    struct Sub<'a> {
        name: String,
        live: enchudb_schema::LiveJoin,
        query: Q<'a>,
        seen: BTreeSet<Pair>,
        part: Val<'a>,
        ord: Val<'a>,
        keep: Pred<'a>,
    }
    let make = |kind: u64, rng: &mut Rng| -> Sub {
        let k = rng.below(3) as i64;
        let (name, query, part, ord, keep): (String, Q, Val, Val, Pred) = match kind {
            0 => (
                "user ごとの at".into(),
                Box::new(move || ev.all().lag("user", "at")),
                Box::new(move |e| get(ev, e, "user")),
                Box::new(move |e| get(ev, e, "at")),
                Box::new(|_| true),
            ),
            1 => (
                format!("種類 {k} の user ごとの at"),
                Box::new(move || ev.where_eq("kind", k).lag("user", "at")),
                Box::new(move |e| get(ev, e, "user")),
                Box::new(move |e| get(ev, e, "at")),
                Box::new(move |e| num(ev, e, "kind") == Some(k)),
            ),
            2 => (
                format!("種類 {k} 以外の全体の at"),
                Box::new(move || ev.all().where_ne("kind", k).lag_all("at")),
                Box::new(|_| Some(Value::Number(0))),
                Box::new(move |e| get(ev, e, "at")),
                Box::new(move |e| num(ev, e, "kind").is_some_and(|x| x != k)),
            ),
            _ => (
                "user.team ごとの big (BigInt)".into(),
                Box::new(move || ev.all().lag("user.team", "big")),
                Box::new(move |e| rf(ev, e, "user").and_then(|x| get(u, x, "team"))),
                Box::new(move |e| get(ev, e, "big")),
                Box::new(|_| true),
            ),
        };
        let live = query().subscribe().unwrap();
        Sub { name, live, query, seen: BTreeSet::new(), part, ord, keep }
    };
    let mut subs: Vec<Sub> = (0..8).map(|i| make(i % 4, &mut rng)).collect();
    let check = |subs: &mut Vec<Sub>, events: &[u64], reborn: &mut BTreeSet<u64>, step: usize| {
        for s in subs.iter_mut() {
            let before = s.seen.clone();
            let d: PairDelta = s.live.poll();
            let (rm, ad): (BTreeSet<Pair>, BTreeSet<Pair>) = (d.removed.iter().copied().collect(), d.added.iter().copied().collect());
            assert_eq!(rm.len(), d.removed.len(), "[{}] step {step}: removed に同じ組が 2 度", s.name);
            assert_eq!(ad.len(), d.added.len(), "[{}] step {step}: added に同じ組が 2 度", s.name);
            for p in &d.removed {
                assert!(s.seen.remove(p), "[{}] step {step}: removed に未報告の組 {p:?}", s.name);
            }
            for p in &d.added {
                assert!(s.seen.insert(*p), "[{}] step {step}: added に報告済みの組 {p:?}", s.name);
            }
            let want = oracle(events, &s.part, &s.ord, &s.keep);
            assert_eq!(s.seen, want, "[{}] step {step}: 積分 != 総当たり", s.name);
            for p in rm.intersection(&ad) {
                assert!(reborn.contains(&p.0) || reborn.contains(&p.1), "[{}] step {step}: 居続けた組 {p:?} が出て入り直した", s.name);
            }
            for p in before.intersection(&want).filter(|p| reborn.contains(&p.0) || reborn.contains(&p.1)) {
                assert!(rm.contains(p) && ad.contains(p), "[{}] step {step}: 作り直した row の組 {p:?} が入り直していない", s.name);
            }
            let found: BTreeSet<Pair> = (s.query)().find().unwrap().into_iter().collect();
            assert_eq!(found, want, "[{}] step {step}: find", s.name);
            assert_eq!((s.query)().count().unwrap(), want.len(), "[{}] step {step}: count", s.name);
        }
        reborn.clear();
    };
    let mut reborn = BTreeSet::new();
    let mut reborn_seen = 0;
    check(&mut subs, &events, &mut reborn, 0);
    let eng = db.engine();
    for step in 1..1500 {
        let ai = rng.below(events.len() as u64) as usize;
        let a = events[ai];
        match rng.below(10) {
            0..=2 => ev.entity(a).set("at", rng.below(20) as i64).commit().unwrap(),
            3 => ev.entity(a).set("big", rng.below(20) as i64 - 10).commit().unwrap(),
            4 => ev.entity(a).set("kind", rng.below(3) as i64).commit().unwrap(),
            5 => ev.entity(a).set("user", Value::Ref(users[rng.below(users.len() as u64) as usize])).commit().unwrap(),
            6 => eng.untie(a, if rng.below(2) == 0 { "events.at" } else { "events.user" }),
            7 => u.entity(users[rng.below(users.len() as u64) as usize]).set("team", teams[rng.below(3) as usize]).commit().unwrap(),
            _ => {
                // 作り直し (半分は同じ値で = 組の相手も同じ、 作り直しでしか入り直さない)
                let same = rng.below(2) == 0;
                let old: Vec<(&str, Option<Value>)> = ["at", "big", "kind", "user"].iter().map(|&k| (k, get(ev, a, k))).collect();
                ev.entity(a).delete().unwrap();
                events[ai] = if same {
                    let mut b = ev.insert().set("id", next_id);
                    for (k, v) in old.into_iter().filter_map(|(k, v)| v.map(|v| (k, v))) {
                        b = b.set(k, v);
                    }
                    b.commit().unwrap()
                } else {
                    new_event(&mut rng, next_id)
                };
                next_id += 1;
                if events[ai] == a {
                    reborn.insert(a);
                    reborn_seen += 1;
                }
            }
        }
        if step % 13 == 0 {
            let i = rng.below(subs.len() as u64) as usize;
            subs[i] = make(rng.below(4), &mut rng);
        }
        if step % 3 == 0 {
            check(&mut subs, &events, &mut reborn, step);
        }
    }
    check(&mut subs, &events, &mut reborn, 1_000_001);
    assert!(reborn_seen > 50, "eid の使い回しが起きていない ({reborn_seen})");
    // 並べられない列 / 知らない列は BadValue
    assert!(ev.all().lag("user", "user").find().is_err());
    assert!(ev.all().lag("nope", "at").subscribe().is_err());
}
