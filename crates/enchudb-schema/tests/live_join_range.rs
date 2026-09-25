//! 範囲で結ぶ組 (`join_range`): poll の差分を積分した組の集合・`find()`・`count()` が、 **テスト側で値と区間を
//! 読んで総当たりした組** と常に一致すること。 oracle は `entity(e).get(col)` を読むだけ (engine の live 評価を通らない)。
//!
//! 書き込みは値 / 始点 / 終点の書き換え・外す (始点 > 終点の空の区間も)・両側の条件の変化・ref の先の値の変化・
//! 両側の row の作り直し (容量を決めた DB で枠を埋め、 同じ eid が別の row になる)。 Number と BigInt (負の数)。

use enchudb_schema::{Database, PairDelta, Table, Value};
use std::collections::BTreeSet;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/schema_live_join_range_{}_{}_{}.db", tag, std::process::id(), nanos)
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
fn range_joins_match_oracle() {
    let path = tmp_path("oracle");
    cleanup(&path);
    run(&path);
    cleanup(&path);
}

/// 各 table の row の数 (table の枠を埋める数: 作り直しで同じ eid が使い回される)。
const EVENTS: usize = 64;

fn run(path: &str) {
    let mut db = Database::create_with_capacity(path, 192).unwrap();
    db.table("companies").number("id").number("founded").primary_key("id").build().unwrap();
    db.table("events").number("id").number("at").bigint("at64").number("kind").ref_to("company", "companies").primary_key("id").build().unwrap();
    db.table("sessions").number("id").number("start").number("end").bigint("s64").bigint("e64").number("open").primary_key("id").build().unwrap();
    let (ct, et, st) = (db.get_table("companies").unwrap(), db.get_table("events").unwrap(), db.get_table("sessions").unwrap());
    let (c, ev, se) = (&ct, &et, &st);
    let mut rng = Rng(0x2a2e_0000_0000_0001);
    let comps: Vec<u64> = (0..8i64).map(|i| c.insert().set("id", i).set("founded", rng.below(100) as i64).commit().unwrap()).collect();
    let mut next_id = 1000i64;
    let new_event = |rng: &mut Rng, id: i64| -> u64 {
        let mut b = ev.insert().set("id", id).set("at", rng.below(100) as i64).set("at64", rng.below(100) as i64 - 50).set("kind", rng.below(3) as i64);
        if rng.below(4) != 0 {
            b = b.set("company", Value::Ref(comps[rng.below(comps.len() as u64) as usize]));
        }
        b.commit().unwrap()
    };
    let new_session = |rng: &mut Rng, id: i64| -> u64 {
        let s = rng.below(100) as i64;
        let s64 = rng.below(100) as i64 - 50;
        // たまに始点 > 終点 (空の区間)
        let e = if rng.below(8) == 0 { s.saturating_sub(3) } else { s + rng.below(30) as i64 };
        se.insert()
            .set("id", id)
            .set("start", s)
            .set("end", e.max(0))
            .set("s64", s64)
            .set("e64", s64 + rng.below(30) as i64 - 3)
            .set("open", rng.below(2) as i64)
            .commit()
            .unwrap()
    };
    let mut events: Vec<u64> = (0..EVENTS).map(|i| new_event(&mut rng, i as i64)).collect();
    let cap = db.engine().table_eid_usage("sessions").unwrap().capacity as usize;
    let mut sessions: Vec<u64> = (0..cap).map(|i| new_session(&mut rng, i as i64)).collect();
    // events の枠も埋める (枠が 64 より広ければ、 組にならない row で)
    let ecap = db.engine().table_eid_usage("events").unwrap().capacity as usize;
    let mut filler = Vec::new();
    while events.len() + filler.len() < ecap {
        filler.push(ev.insert().set("id", next_id).commit().unwrap());
        next_id += 1;
    }
    // 組の総当たり
    type Val<'a> = Box<dyn Fn(u64) -> Option<i64> + 'a>;
    type Pred<'a> = Box<dyn Fn(u64) -> bool + 'a>;
    let oracle = |events: &[u64], sessions: &[u64], v: &Val, lo: &Val, hi: &Val, lk: &Pred, rk: &Pred| -> BTreeSet<Pair> {
        let mut out = BTreeSet::new();
        for &b in sessions.iter().filter(|&&b| rk(b)) {
            let (Some(l), Some(h)) = (lo(b), hi(b)) else { continue };
            for &a in events.iter().filter(|&&a| lk(a)) {
                if v(a).is_some_and(|x| l <= x && x <= h) {
                    out.insert((a, b));
                }
            }
        }
        out
    };
    type Q<'a> = Box<dyn Fn() -> enchudb_schema::JoinQuery<'a> + 'a>;
    struct Sub<'a> {
        name: String,
        live: enchudb_schema::LiveJoin,
        query: Q<'a>,
        seen: BTreeSet<Pair>,
        v: Val<'a>,
        lo: Val<'a>,
        hi: Val<'a>,
        lk: Pred<'a>,
        rk: Pred<'a>,
    }
    let make = |kind: u64, rng: &mut Rng| -> Sub {
        let k = rng.below(3) as i64;
        let o = rng.below(2) as i64;
        let (name, query, v, lo, hi, lk, rk): (String, Q, Val, Val, Val, Pred, Pred) = match kind {
            0 => (
                "at ∈ [start, end]".into(),
                Box::new(move || ev.all().join_range("at", se.all(), "start", "end")),
                Box::new(move |a| num(ev, a, "at")),
                Box::new(move |b| num(se, b, "start")),
                Box::new(move |b| num(se, b, "end")),
                Box::new(|_| true),
                Box::new(|_| true),
            ),
            1 => (
                format!("種類 {k} の at ∈ open {o} の [start, end]"),
                Box::new(move || ev.where_eq("kind", k).join_range("at", se.where_eq("open", o), "start", "end")),
                Box::new(move |a| num(ev, a, "at")),
                Box::new(move |b| num(se, b, "start")),
                Box::new(move |b| num(se, b, "end")),
                Box::new(move |a| num(ev, a, "kind") == Some(k)),
                Box::new(move |b| num(se, b, "open") == Some(o)),
            ),
            2 => (
                "company.founded ∈ [start, end]".into(),
                Box::new(move || ev.all().join_range("company.founded", se.all(), "start", "end")),
                Box::new(move |a| rf(ev, a, "company").and_then(|x| num(c, x, "founded"))),
                Box::new(move |b| num(se, b, "start")),
                Box::new(move |b| num(se, b, "end")),
                Box::new(|_| true),
                Box::new(|_| true),
            ),
            _ => (
                format!("at64 ∈ open {o} の [s64, e64] (BigInt)"),
                Box::new(move || ev.all().join_range("at64", se.where_eq("open", o), "s64", "e64")),
                Box::new(move |a| num(ev, a, "at64")),
                Box::new(move |b| num(se, b, "s64")),
                Box::new(move |b| num(se, b, "e64")),
                Box::new(|_| true),
                Box::new(move |b| num(se, b, "open") == Some(o)),
            ),
        };
        let live = query().subscribe().unwrap();
        Sub { name, live, query, seen: BTreeSet::new(), v, lo, hi, lk, rk }
    };
    let mut subs: Vec<Sub> = (0..8).map(|i| make(i % 4, &mut rng)).collect();
    let check = |subs: &mut Vec<Sub>, events: &[u64], sessions: &[u64], reborn: &mut BTreeSet<u64>, step: usize| {
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
            let want = oracle(events, sessions, &s.v, &s.lo, &s.hi, &s.lk, &s.rk);
            assert_eq!(s.seen, want, "[{}] step {step}: 積分 != 総当たり", s.name);
            // 居続けた組は出さない (どちらの row も作り直していなければ)
            for p in rm.intersection(&ad) {
                assert!(reborn.contains(&p.0) || reborn.contains(&p.1), "[{}] step {step}: 居続けた組 {p:?} が出て入り直した", s.name);
            }
            // 作り直した row の組は、 前も後も居れば出て入り直す
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
    check(&mut subs, &events, &sessions, &mut reborn, 0);
    let eng = db.engine();
    for step in 1..1500 {
        let ai = rng.below(events.len() as u64) as usize;
        let bi = rng.below(sessions.len() as u64) as usize;
        let (a, b) = (events[ai], sessions[bi]);
        match rng.below(14) {
            0 | 1 => ev.entity(a).set("at", rng.below(100) as i64).commit().unwrap(),
            2 => ev.entity(a).set("at64", rng.below(100) as i64 - 50).commit().unwrap(),
            3 => ev.entity(a).set("kind", rng.below(3) as i64).commit().unwrap(),
            4 => ev.entity(a).set("company", Value::Ref(comps[rng.below(comps.len() as u64) as usize])).commit().unwrap(),
            5 => eng.untie(a, "events.at"),
            6 | 7 => se.entity(b).set("start", rng.below(100) as i64).commit().unwrap(),
            8 => se.entity(b).set("end", rng.below(100) as i64).commit().unwrap(),
            9 => se.entity(b).update().set("s64", rng.below(100) as i64 - 50).set("e64", rng.below(100) as i64 - 50).commit().unwrap(),
            10 => se.entity(b).set("open", rng.below(2) as i64).commit().unwrap(),
            11 => c.entity(comps[rng.below(comps.len() as u64) as usize]).set("founded", rng.below(100) as i64).commit().unwrap(),
            12 => {
                // 半分は同じ値で作り直す (組の相手も値も同じ = 作り直しでしか入り直さない)
                let same = rng.below(2) == 0;
                let old: Vec<(&str, Option<Value>)> = ["at", "at64", "kind", "company"].iter().map(|&k| (k, ev.entity(a).get(k))).collect();
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
            _ => {
                if rng.below(4) == 0 {
                    eng.untie(b, "sessions.end");
                } else {
                    let same = rng.below(2) == 0;
                    let old: Vec<(&str, Option<Value>)> =
                        ["start", "end", "s64", "e64", "open"].iter().map(|&k| (k, se.entity(b).get(k))).collect();
                    se.entity(b).delete().unwrap();
                    sessions[bi] = if same {
                        let mut nb = se.insert().set("id", next_id);
                        for (k, v) in old.into_iter().filter_map(|(k, v)| v.map(|v| (k, v))) {
                            nb = nb.set(k, v);
                        }
                        nb.commit().unwrap()
                    } else {
                        new_session(&mut rng, next_id)
                    };
                    next_id += 1;
                    if sessions[bi] == b {
                        reborn.insert(b);
                        reborn_seen += 1;
                    }
                }
            }
        }
        if step % 13 == 0 {
            let i = rng.below(subs.len() as u64) as usize;
            subs[i] = make(rng.below(4), &mut rng);
        }
        if step % 3 == 0 {
            check(&mut subs, &events, &sessions, &mut reborn, step);
        }
    }
    check(&mut subs, &events, &sessions, &mut reborn, 1_000_001);
    assert!(reborn_seen > 50, "eid の使い回しが起きていない ({reborn_seen})");
    // 型の違う列 / 知らない列は BadValue
    assert!(ev.all().join_range("at64", se.all(), "start", "end").find().is_err());
    assert!(ev.all().join_range("at", se.all(), "start", "nope").subscribe().is_err());
    assert!(ev.all().join_range("at", se.all(), "start", "end").subscribe_counts("at").is_err());
    drop(filler);
}
