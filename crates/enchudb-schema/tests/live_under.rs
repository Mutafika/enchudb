//! 階層の配下 (`under`) と上 (`above`、 SQL の再帰 CTE): poll の差分を積分した集合・`find()`・`count()` が、
//! **テスト側で親をたどって決めた配下 / 上** と常に一致すること。 oracle は `entity(e).get(col)` で親を読んで seed まで上るだけ
//! (engine の live 評価を通らない)。
//!
//! 書き込みは上司の付け替え (輪になるものも)・外し、 seed の条件の変化、 結果を絞る条件の変化、 row の作り直し。

use enchudb_schema::{Database, LiveDelta, Table, Value};
use std::collections::BTreeSet;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/schema_live_under_{}_{}_{}.db", tag, std::process::id(), nanos)
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

fn boss(t: &Table, e: u64) -> Option<u64> {
    match t.entity(e).get("boss") {
        Some(Value::Ref(x)) => Some(x),
        _ => None,
    }
}

#[test]
fn under_matches_oracle() {
    let path = tmp_path("oracle");
    cleanup(&path);
    run(&path);
    cleanup(&path);
}

/// row の数 (table の枠を埋める数: 作り直しで必ず同じ eid が使い回される)。
const SIZE: i64 = 64;

fn run(path: &str) {
    // slot の再利用 (同じ eid が別の row になる) は table の枠が埋まってからだけ — 容量を小さくし、 row の数を枠に合わせて起こす
    let mut db = Database::create_with_capacity(path, 128).unwrap();
    db.table("emps").number("id").number("dept").number("age").ref_to("boss", "emps").primary_key("id").build().unwrap();
    let et = db.get_table("emps").unwrap();
    let t = &et;
    let mut rng = Rng(0x7ee5_0000_0000_0001);
    // 最初は木 (上司は自分より前の人)
    let mut emps: Vec<u64> = Vec::new();
    for i in 0..SIZE {
        let mut b = t.insert().set("id", i).set("dept", rng.below(4) as i64).set("age", rng.below(50) as i64);
        if i > 0 && rng.below(8) != 0 {
            b = b.set("boss", Value::Ref(emps[rng.below(i as u64) as usize]));
        }
        emps.push(b.commit().unwrap());
    }
    // 総当たり: e が配下か = 上司をたどって seed に着く (生きていない row / 輪で止まる)
    // 上: x が seed s (x 以外) から上司をたどった道 (輪は 1 周で止まる) に居る
    let above = |emps: &[u64], seed: &dyn Fn(u64) -> bool, keep: &dyn Fn(u64) -> bool| -> BTreeSet<u64> {
        let mut out = BTreeSet::new();
        for &s in emps.iter().filter(|&&s| seed(s)) {
            let mut seen = BTreeSet::from([s]);
            let mut cur = s;
            while let Some(b) = boss(t, cur) {
                if !emps.contains(&b) || !seen.insert(b) {
                    break;
                }
                out.insert(b);
                cur = b;
            }
        }
        out.into_iter().filter(|&e| keep(e)).collect()
    };
    let oracle = |emps: &[u64], seed: &dyn Fn(u64) -> bool, keep: &dyn Fn(u64) -> bool, up: bool| -> BTreeSet<u64> {
        if up {
            return above(emps, seed, keep);
        }
        emps.iter()
            .copied()
            .filter(|&e| {
                let mut cur = e;
                let mut seen = BTreeSet::new();
                while let Some(b) = boss(t, cur) {
                    if !emps.contains(&b) {
                        return false;
                    }
                    if seed(b) {
                        return true;
                    }
                    if !seen.insert(b) || b == e {
                        return false;
                    }
                    cur = b;
                }
                false
            })
            .filter(|&e| keep(e))
            .collect()
    };
    type Q<'a> = Box<dyn Fn() -> enchudb_schema::UnderQuery<'a> + 'a>;
    type Pred<'a> = Box<dyn Fn(u64) -> bool + 'a>;
    struct Sub<'a> {
        name: String,
        live: enchudb_schema::LiveUnder,
        query: Q<'a>,
        seen: BTreeSet<u64>,
        seed: Pred<'a>,
        keep: Pred<'a>,
        up: bool,
    }
    let make = |kind: u64, rng: &mut Rng, emps: &[u64]| -> Sub {
        let d = rng.below(4) as i64;
        let x = rng.below(50) as i64;
        let who = emps[rng.below(emps.len() as u64) as usize];
        let id = num(t, who, "id").unwrap();
        let (name, query, seed, keep): (String, Q, Pred, Pred) = match kind {
            0 => (
                format!("id {id} の配下全員"),
                Box::new(move || t.all().under("boss", t.where_eq("id", id))),
                Box::new(move |e| num(t, e, "id") == Some(id)),
                Box::new(|_| true),
            ),
            1 => (
                format!("部署 {d} の人の配下で {x} 歳より上"),
                Box::new(move || t.all().where_gt("age", x).under("boss", t.where_eq("dept", d))),
                Box::new(move |e| num(t, e, "dept") == Some(d)),
                Box::new(move |e| num(t, e, "age").is_some_and(|a| a > x)),
            ),
            3 => (
                format!("id {id} の上司全員"),
                Box::new(move || t.all().above("boss", t.where_eq("id", id))),
                Box::new(move |e| num(t, e, "id") == Some(id)),
                Box::new(|_| true),
            ),
            4 => (
                format!("部署 {d} の人の上で {x} 歳より上"),
                Box::new(move || t.all().where_gt("age", x).above("boss", t.where_eq("dept", d))),
                Box::new(move |e| num(t, e, "dept") == Some(d)),
                Box::new(move |e| num(t, e, "age").is_some_and(|a| a > x)),
            ),
            5 => (
                format!("{x} 歳より上の人の上で部署 {d}"),
                Box::new(move || t.where_eq("dept", d).above("boss", t.all().where_gt("age", x))),
                Box::new(move |e| num(t, e, "age").is_some_and(|a| a > x)),
                Box::new(move |e| num(t, e, "dept") == Some(d)),
            ),
            _ => (
                format!("{x} 歳より上の人の配下で部署 {d}"),
                Box::new(move || t.where_eq("dept", d).under("boss", t.all().where_gt("age", x))),
                Box::new(move |e| num(t, e, "age").is_some_and(|a| a > x)),
                Box::new(move |e| num(t, e, "dept") == Some(d)),
            ),
        };
        let live = query().subscribe().unwrap();
        Sub { name, live, query, seen: BTreeSet::new(), seed, keep, up: (3..=5).contains(&kind) }
    };
    let mut subs: Vec<Sub> = (0..12).map(|i| make(i % 6, &mut rng, &emps)).collect();
    // reborn = 前回の check から作り直した row の eid。 前も後も結果に居るなら 「出て入り直した」 として届くこと
    let check = |subs: &mut Vec<Sub>, emps: &[u64], reborn: &mut BTreeSet<u64>, step: usize| {
        for s in subs.iter_mut() {
            let before = s.seen.clone();
            let d = s.live.poll();
            let (rm, ad): (BTreeSet<u64>, BTreeSet<u64>) = (d.removed.iter().copied().collect(), d.added.iter().copied().collect());
            integrate(&mut s.seen, d);
            let want = oracle(emps, &s.seed, &s.keep, s.up);
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
    check(&mut subs, &emps, &mut reborn, 0);
    let eng = db.engine();
    let mut next_id = 1000i64;
    for step in 1..1500 {
        let e = emps[rng.below(emps.len() as u64) as usize];
        let b = emps[rng.below(emps.len() as u64) as usize];
        match rng.below(10) {
            // 付け替え (輪になることもある)
            0..=3 => t.entity(e).set("boss", Value::Ref(b)).commit().unwrap(),
            4 => eng.untie(e, "emps.boss"),
            5 => t.entity(e).set("dept", rng.below(4) as i64).commit().unwrap(),
            6 => t.entity(e).set("age", rng.below(50) as i64).commit().unwrap(),
            7 => {
                // 作り直し (配下は消えた上司を指したまま)
                let i = emps.iter().position(|&k| k == e).unwrap();
                t.entity(e).delete().unwrap();
                let mut nb = t.insert().set("id", next_id).set("dept", rng.below(4) as i64).set("age", rng.below(50) as i64);
                if rng.below(2) == 0 {
                    nb = nb.set("boss", Value::Ref(b));
                }
                emps[i] = nb.commit().unwrap();
                if emps[i] == e {
                    reborn.insert(e);
                    reborn_seen += 1;
                }
                next_id += 1;
            }
            _ => {
                // 輪を作りにくい付け替え (上司を自分より前の人に)
                let i = emps.iter().position(|&k| k == e).unwrap();
                if i > 0 {
                    t.entity(e).set("boss", Value::Ref(emps[rng.below(i as u64) as usize])).commit().unwrap();
                }
            }
        }
        if step % 11 == 0 {
            let i = rng.below(subs.len() as u64) as usize;
            subs[i] = make(rng.below(6), &mut rng, &emps);
        }
        if step % 3 == 0 {
            check(&mut subs, &emps, &mut reborn, step);
        }
    }
    check(&mut subs, &emps, &mut reborn, 1_000_001);
    assert!(reborn_seen > 50, "eid の使い回しが起きていない ({reborn_seen})");
    // 自分の table を指す ref 列でない / seed が別の table なら BadValue
    assert!(t.all().under("dept", t.all()).find().is_err());
    assert!(t.all().above("dept", t.all()).subscribe().is_err());
}
