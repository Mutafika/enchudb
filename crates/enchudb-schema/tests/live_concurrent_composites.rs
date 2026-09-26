//! 組 / 再帰 / window の購読 (中で複数の購読を順に poll して合わせるもの) を、 別 thread が書き込み続けている間に
//! poll し続けても、 差分の積分が壊れない (未報告の削除・報告済みの追加が無い) こと、 書き込みが止まった後の poll で
//! `find()` と一致すること。
//!
//! 書き込みは値の書き換え・ref の付け替え・row の削除と追加。 購読は ref / 値 / ref の先の値 / 範囲 (2 本) で結ぶ組、
//! LAG、 下向き / 上向きの再帰、 グラフの到達、 3 table の組。 時間で区切るので、 回るたびに書き込みと poll の
//! 重なり方は変わる (壊れる並びを必ず通る試験ではない)。

use enchudb_schema::{Database, Value};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// 1 回に書く数。
const WRITES: usize = 20_000;

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

fn integ<T: Ord + Copy + std::fmt::Debug>(set: &mut BTreeSet<T>, rm: &[T], ad: &[T], what: &str) {
    for p in rm {
        assert!(set.remove(p), "{what}: removed に未報告 {p:?}");
    }
    for p in ad {
        assert!(set.insert(*p), "{what}: added に報告済み {p:?}");
    }
}

#[test]
fn composite_subscriptions_survive_concurrent_writes() {
    for round in 0..4u64 {
        let path = format!("/tmp/schema_live_concurrent_{}_{}", std::process::id(), round);
        cleanup(&path);
        run(&path, round);
        cleanup(&path);
    }
}

fn run(path: &str, round: u64) {
    {
        let mut db = Database::create_growable_tiny(path).unwrap();
        db.table("companies").number("id").tag("city").number("founded").primary_key("id").build().unwrap();
        db.table("users").number("id").tag("city").number("at").number("age").ref_to("company", "companies").ref_to("boss", "users").primary_key("id").build().unwrap();
        db.table("shops").number("id").tag("city").number("lo").number("hi").primary_key("id").build().unwrap();
        db.table("follows").ref_to("from", "users").ref_to("to", "users").build().unwrap();
        let db: Arc<Database> = db.finish_concurrent().unwrap();
        let cities = ["Tokyo", "Osaka", "Kyoto"];
        let mut rng = Rng(0x7ee5_0000_0000_0001 + round);
        let (comps, users, shops, edges) = {
            let (c, u, s, f) = (db.get_table("companies").unwrap(), db.get_table("users").unwrap(), db.get_table("shops").unwrap(), db.get_table("follows").unwrap());
            let comps: Vec<u64> = (0..10i64).map(|i| c.insert().set("id", i).set("city", cities[rng.below(3) as usize]).set("founded", rng.below(50) as i64).commit().unwrap()).collect();
            let mut users: Vec<u64> = Vec::new();
            for i in 0..60i64 {
                let mut b = u.insert().set("id", i).set("city", cities[rng.below(3) as usize]).set("at", rng.below(50) as i64).set("age", rng.below(50) as i64).set("company", Value::Ref(comps[rng.below(10) as usize]));
                if i > 0 {
                    b = b.set("boss", Value::Ref(users[rng.below(i as u64) as usize]));
                }
                users.push(b.commit().unwrap());
            }
            let shops: Vec<u64> = (0..15i64).map(|i| { let lo = rng.below(50) as i64; s.insert().set("id", i).set("city", cities[rng.below(3) as usize]).set("lo", lo).set("hi", lo + rng.below(15) as i64).commit().unwrap() }).collect();
            let edges: Vec<u64> = (0..80).map(|_| f.insert().set("from", Value::Ref(users[rng.below(60) as usize])).set("to", Value::Ref(users[rng.below(60) as usize])).commit().unwrap()).collect();
            (comps, users, shops, edges)
        };
        // 書き終わりの印。 書き込みは回数で区切る (多すぎると、 取りこぼした row も後の書き込みで上書きされて
        // 最後には直ってしまい、 取りこぼしが見えない)
        let done = Arc::new(AtomicBool::new(false));
        // 購読を作ってから書き始める
        let go = Arc::new(AtomicBool::new(false));
        let writer = {
            let db = db.clone();
            let (done, go) = (done.clone(), go.clone());
            let (comps, mut users, shops, mut edges) = (comps.clone(), users.clone(), shops.clone(), edges.clone());
            std::thread::spawn(move || {
                let (c, u, s, f) = (db.get_table("companies").unwrap(), db.get_table("users").unwrap(), db.get_table("shops").unwrap(), db.get_table("follows").unwrap());
                let mut rng = Rng(0x0dd0_0000_0000_0001 + round);
                let mut n = 0;
                while !go.load(Ordering::Acquire) {
                    std::thread::yield_now();
                }
                while n < WRITES {
                    // poll と重なるように、 ときどき譲る
                    if n % 20 == 0 {
                        std::thread::sleep(std::time::Duration::from_micros(50));
                    }
                    let ai = rng.below(users.len() as u64) as usize;
                    let a = users[ai];
                    match rng.below(12) {
                        10 => {
                            u.entity(a).delete().unwrap();
                            let mut b = u.insert().set("id", 100_000 + n as i64).set("city", cities[rng.below(3) as usize]).set("at", rng.below(50) as i64).set("age", rng.below(50) as i64);
                            if rng.below(3) != 0 {
                                b = b.set("company", Value::Ref(comps[rng.below(comps.len() as u64) as usize]));
                            }
                            if rng.below(3) != 0 {
                                b = b.set("boss", Value::Ref(users[rng.below(users.len() as u64) as usize]));
                            }
                            users[ai] = b.commit().unwrap();
                        }
                        11 => {
                            let ei = rng.below(edges.len() as u64) as usize;
                            f.entity(edges[ei]).delete().unwrap();
                            edges[ei] = f.insert().set("from", Value::Ref(users[rng.below(users.len() as u64) as usize])).set("to", Value::Ref(users[rng.below(users.len() as u64) as usize])).commit().unwrap();
                        }
                        0 => u.entity(a).set("city", cities[rng.below(3) as usize]).commit().unwrap(),
                        1 => u.entity(a).set("at", rng.below(50) as i64).commit().unwrap(),
                        2 => u.entity(a).set("age", rng.below(50) as i64).commit().unwrap(),
                        3 => u.entity(a).set("company", Value::Ref(comps[rng.below(comps.len() as u64) as usize])).commit().unwrap(),
                        4 => u.entity(a).set("boss", Value::Ref(users[rng.below(users.len() as u64) as usize])).commit().unwrap(),
                        5 => c.entity(comps[rng.below(comps.len() as u64) as usize]).update().set("city", cities[rng.below(3) as usize]).set("founded", rng.below(50) as i64).commit().unwrap(),
                        6 => { let lo = rng.below(50) as i64; s.entity(shops[rng.below(shops.len() as u64) as usize]).update().set("lo", lo).set("hi", lo + rng.below(15) as i64).set("city", cities[rng.below(3) as usize]).commit().unwrap() }
                        7 => f.entity(edges[rng.below(edges.len() as u64) as usize]).set("to", Value::Ref(users[rng.below(users.len() as u64) as usize])).commit().unwrap(),
                        _ => f.entity(edges[rng.below(edges.len() as u64) as usize]).set("from", Value::Ref(users[rng.below(users.len() as u64) as usize])).commit().unwrap(),
                    }
                    n += 1;
                }
                done.store(true, Ordering::Release);
                n
            })
        };
        let (c, u, s, f) = (db.get_table("companies").unwrap(), db.get_table("users").unwrap(), db.get_table("shops").unwrap(), db.get_table("follows").unwrap());
        type P = (u64, u64);
        type Jq<'x> = Box<dyn Fn() -> enchudb_schema::JoinQuery<'x> + 'x>;
        let joins: Vec<(&str, Jq)> = vec![
            ("ref", Box::new(|| u.all().where_gt("age", 20i64).join_ref("company", c.where_eq("city", "Tokyo")))),
            ("eq", Box::new(|| u.all().join_eq("city", s.all(), "city"))),
            ("eq path", Box::new(|| u.all().join_eq("company.city", s.all(), "city"))),
            ("range", Box::new(|| u.all().join_range("at", s.all(), "lo", "hi"))),
            ("range path", Box::new(|| u.all().join_range("company.founded", s.where_eq("city", "Osaka"), "lo", "hi"))),
        ];
        let jl: Vec<_> = joins.iter().map(|(_, q)| q().subscribe().unwrap()).collect();
        let lag = u.all().lag("company", "at").subscribe().unwrap();
        let under = u.all().under("boss", u.where_eq("city", "Tokyo")).subscribe().unwrap();
        let above = u.all().above("boss", u.all().where_gt("age", 40i64)).subscribe().unwrap();
        let reach = u.all().reachable(f.all(), "from", "to", u.where_eq("city", "Kyoto")).subscribe().unwrap();
        let multi = u.all().join_ref("company", c.all()).then_eq("companies", "city", s.all(), "city").subscribe().unwrap();
        let mut js: Vec<BTreeSet<P>> = vec![BTreeSet::new(); jl.len()];
        let (mut lg, mut un, mut ab, mut re): (BTreeSet<P>, BTreeSet<u64>, BTreeSet<u64>, BTreeSet<u64>) = Default::default();
        let mut mt: BTreeSet<Vec<u64>> = BTreeSet::new();
        go.store(true, Ordering::Release);
        let mut polls = 0;
        let mut pass = |final_: bool| {
            for (i, l) in jl.iter().enumerate() {
                let d = l.poll();
                integ(&mut js[i], &d.removed, &d.added, joins[i].0);
            }
            let d = lag.poll();
            integ(&mut lg, &d.removed, &d.added, "lag");
            let d = under.poll();
            integ(&mut un, &d.removed, &d.added, "under");
            let d = above.poll();
            integ(&mut ab, &d.removed, &d.added, "above");
            let d = reach.poll();
            integ(&mut re, &d.removed, &d.added, "reach");
            let d = multi.poll();
            for t in &d.removed {
                assert!(mt.remove(t), "multi removed 未報告 {t:?}");
            }
            for t in d.added {
                assert!(mt.insert(t.clone()), "multi added 報告済み {t:?}");
            }
            if final_ {
                for (i, (name, q)) in joins.iter().enumerate() {
                    let want: BTreeSet<P> = q().find().unwrap().into_iter().collect();
                    assert_eq!(js[i], want, "round {round} {name}");
                }
                assert_eq!(lg, u.all().lag("company", "at").find().unwrap().into_iter().collect(), "round {round} lag");
                assert_eq!(un, u.all().under("boss", u.where_eq("city", "Tokyo")).find().unwrap().into_iter().collect(), "round {round} under");
                assert_eq!(ab, u.all().above("boss", u.all().where_gt("age", 40i64)).find().unwrap().into_iter().collect(), "round {round} above");
                assert_eq!(re, u.all().reachable(f.all(), "from", "to", u.where_eq("city", "Kyoto")).find().unwrap().into_iter().collect(), "round {round} reach");
                let want: BTreeSet<Vec<u64>> = u.all().join_ref("company", c.all()).then_eq("companies", "city", s.all(), "city").find().unwrap().into_iter().collect();
                assert_eq!(mt, want, "round {round} multi");
            }
        };
        while !done.load(Ordering::Acquire) {
            pass(false);
            polls += 1;
        }
        let n = writer.join().unwrap();
        pass(true);
        assert!(n == WRITES && polls > 10, "round {round}: 書き込み {n} / poll {polls} が少なすぎる");
        drop((jl, lag, under, above, reach, multi));
    }
}
