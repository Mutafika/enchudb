//! review: poll_since / snapshot / poll / 束の poll を乱数で混ぜ、 読み手ごとに 「版 pos の時の集合 + poll_since(pos) の差分」 が
//! 今の答え (模型から計算) と合うかを見る。 集合 / OR / 上位 k / 範囲 / 集計。 途中で開き直す。

use enchudb_schema::{Answer, Database, LiveCounts, LiveQuery, Value};
use std::collections::{BTreeMap, BTreeSet};

const SCRATCH: &str = "/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/eedfc333-d87a-4a47-a3b0-daf4b6d7337a/scratchpad";

fn tmp_path(name: &str) -> String {
    format!("{SCRATCH}/rl-since-{}-{}", name, std::process::id())
}
fn cleanup(path: &str) {
    let _ = enchudb_engine::db_files::remove_db(path);
    let _ = std::fs::remove_dir_all(path);
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 { self.next() % n }
}

#[derive(Clone, Debug, Default)]
struct Row {
    eid: u64,
    age: Option<i64>,
    city: Option<String>,
    score: Option<i64>,
}

type Model = BTreeMap<i64, Row>;

fn set_of(model: &Model, f: impl Fn(&Row) -> bool) -> BTreeSet<u64> {
    model.values().filter(|r| f(r)).map(|r| r.eid).collect()
}

/// 上位 k 件 (値の順、 同じ値は eid の昇順)。
fn top_of(model: &Model, k: usize, desc: bool, f: impl Fn(&Row) -> bool) -> BTreeSet<u64> {
    let mut v: Vec<(i128, u64)> = model.values().filter(|r| f(r)).filter_map(|r| r.score.map(|s| (if desc { -(s as i128) } else { s as i128 }, r.eid))).collect();
    v.sort_unstable();
    v.truncate(k);
    v.into_iter().map(|x| x.1).collect()
}

const N_SETS: usize = 6;

fn want_set(model: &Model, i: usize) -> BTreeSet<u64> {
    match i {
        0 => set_of(model, |r| r.age.is_some_and(|a| a > 30)),
        1 => set_of(model, |r| r.city.as_deref() == Some("c1") || r.age.is_some_and(|a| a < 15)),
        2 => top_of(model, 5, true, |_| true),
        3 => set_of(model, |r| r.age.is_some_and(|a| (20..=40).contains(&a))),
        4 => top_of(model, 3, false, |r| r.city.as_deref() == Some("c2")),
        5 => set_of(model, |r| r.age.is_some() && r.city.is_none()),
        _ => unreachable!(),
    }
}

fn declare(db: &Database) -> Vec<LiveQuery> {
    let t = db.get_table("users").unwrap();
    vec![
        t.all().where_gt("age", 30i64).answer("s0").unwrap(),
        t.where_eq("city", "c1").or(t.all().where_lt("age", 15i64)).answer("s1").unwrap(),
        t.all().order_by_desc("score").limit(5).answer("s2").unwrap(),
        // 宣言でない購読
        t.where_range("age", 20i64, 40i64).subscribe().unwrap(),
        t.where_eq("city", "c2").order_by("score").limit(3).answer("s4").unwrap(),
        t.all().where_not_null("age").where_null("city").answer("s5").unwrap(),
    ]
}

fn reopen_handles(db: &Database) -> Vec<LiveQuery> {
    let t = db.get_table("users").unwrap();
    let get = |n: &str| db.answer(n).and_then(Answer::into_query).unwrap_or_else(|| panic!("答え {n} が開き直しで戻らない"));
    vec![get("s0"), get("s1"), get("s2"), t.where_range("age", 20i64, 40i64).subscribe().unwrap(), get("s4"), get("s5")]
}

fn want_counts(model: &Model, sums: bool) -> BTreeMap<String, (u64, i128)> {
    let mut m: BTreeMap<String, (u64, i128)> = BTreeMap::new();
    for r in model.values() {
        if sums && !r.age.is_some_and(|a| a > 10) {
            continue;
        }
        if let Some(c) = &r.city {
            let e = m.entry(c.clone()).or_default();
            e.0 += 1;
            if sums {
                e.1 += r.score.unwrap_or(0) as i128;
            }
        }
    }
    m
}

fn text(v: &Value) -> String {
    match v {
        Value::Text(s) => s.clone(),
        o => panic!("{o:?}"),
    }
}

#[derive(Clone, Default)]
struct Reader {
    pos: Option<u64>,
    set: BTreeSet<u64>,
}
#[derive(Clone, Default)]
struct CReader {
    pos: Option<u64>,
    map: BTreeMap<String, (u64, i128)>,
}

fn read_set(q: &LiveQuery, rd: &mut Reader, want: &BTreeSet<u64>, ctx: &str, stats: &mut (u64, u64)) {
    let r = rd.pos.map(|p| q.poll_since(p));
    match r {
        Some(Ok((d, v))) => {
            stats.0 += 1;
            for e in &d.removed {
                assert!(rd.set.remove(e), "{ctx}: removed に pos の時点で居ない eid {e:#x} (delta {d:?})");
            }
            for e in &d.added {
                assert!(rd.set.insert(*e), "{ctx}: added に pos の時点で居る eid {e:#x} (delta {d:?})");
            }
            assert!(v >= rd.pos.unwrap(), "{ctx}: 版が戻った");
            rd.pos = Some(v);
        }
        _ => {
            stats.1 += 1;
            let (all, v) = q.snapshot();
            rd.set = all.into_iter().collect();
            rd.pos = Some(v);
        }
    }
    assert_eq!(&rd.set, want, "{ctx}: 積んだ集合が今の答えと違う");
}

fn read_counts(q: &LiveCounts, rd: &mut CReader, want: &BTreeMap<String, (u64, i128)>, ctx: &str, sums: bool) {
    let r = rd.pos.map(|p| q.poll_since(p));
    match r {
        Some(Ok((gs, v))) => {
            for (g, n, s) in gs {
                if n == 0 {
                    rd.map.remove(&text(&g));
                } else {
                    rd.map.insert(text(&g), (n, if sums { s } else { 0 }));
                }
            }
            rd.pos = Some(v);
        }
        _ => {
            let (all, v) = q.snapshot();
            rd.map = all.into_iter().filter(|x| x.1 != 0).map(|(g, n, s)| (text(&g), (n, if sums { s } else { 0 }))).collect();
            rd.pos = Some(v);
        }
    }
    assert_eq!(&rd.map, want, "{ctx}: 積んだ集計が今の答えと違う");
}

fn pick_age(r: &mut Rng) -> i64 { [5, 10, 14, 15, 19, 20, 21, 30, 31, 35, 40, 41, 50][r.below(13) as usize] }
fn pick_city(r: &mut Rng) -> String { format!("c{}", r.below(4)) }
fn pick_score(r: &mut Rng) -> i64 {
    match r.below(4) {
        0 => [i64::MIN, -1, 0, 1, i64::MAX - 1][r.below(5) as usize],
        _ => r.below(12) as i64 - 6,
    }
}

fn step(db: &Database, model: &mut Model, r: &mut Rng, next_id: &mut i64) -> String {
    let t = db.get_table("users").unwrap();
    let live: Vec<i64> = model.keys().copied().collect();
    let k = r.below(100);
    if live.is_empty() || k < 25 {
        let id = *next_id;
        *next_id += 1;
        let mut row = Row::default();
        let mut b = t.insert().set("id", id);
        if r.below(8) != 0 { let a = pick_age(r); row.age = Some(a); b = b.set("age", a); }
        if r.below(5) != 0 { let c = pick_city(r); b = b.set("city", c.as_str()); row.city = Some(c); }
        if r.below(8) != 0 { let s = pick_score(r); row.score = Some(s); b = b.set("score", s); }
        row.eid = b.commit().unwrap();
        let d = format!("insert {id} {row:?}");
        model.insert(id, row);
        return d;
    }
    let id = live[r.below(live.len() as u64) as usize];
    let eid = model[&id].eid;
    if k < 40 {
        model.remove(&id);
        t.entity(eid).delete().unwrap();
        return format!("delete {id} eid {eid:#x}");
    }
    let row = model.get_mut(&id).unwrap();
    let col = ["age", "city", "score"][r.below(3) as usize];
    if k < 52 {
        match col { "age" => row.age = None, "city" => row.city = None, _ => row.score = None }
        db.engine().untie_by_id(eid, t.himo_id(col).unwrap());
        return format!("untie {id}.{col}");
    }
    match col {
        "age" => { let a = pick_age(r); row.age = Some(a); t.entity(eid).set("age", a).commit().unwrap(); format!("set {id}.age={a}") }
        "city" => { let c = pick_city(r); t.entity(eid).set("city", c.as_str()).commit().unwrap(); let d = format!("set {id}.city={c}"); row.city = Some(c); d }
        _ => { let s = pick_score(r); row.score = Some(s); t.entity(eid).set("score", s).commit().unwrap(); format!("set {id}.score={s}") }
    }
}

fn run(seed: u64, log_limit: Option<usize>) -> (u64, u64) {
    let path = tmp_path(&format!("s{seed}-{}", log_limit.unwrap_or(0)));
    cleanup(&path);
    let mut r = Rng(seed.wrapping_mul(0x9E3779B97F4A7C15) | 1);
    let mut model = Model::new();
    let mut next_id = 0i64;
    let mut stats = (0u64, 0u64);
    const READERS: usize = 3;
    let mut readers = vec![vec![Reader::default(); READERS]; N_SETS];
    let mut creaders = vec![vec![CReader::default(); READERS]; 2];
    let mut hist: Vec<String> = Vec::new();
    for round in 0..3 {
        let db = if round == 0 {
            let mut db = Database::create(&path).unwrap();
            db.table("users").number("id").number("age").tag("city").bigint("score").primary_key("id").build().unwrap();
            db
        } else {
            Database::open(&path).unwrap()
        };
        if let Some(l) = log_limit {
            db.engine().set_answer_log_limit(l);
        }
        let (qs, cq, sq) = if round == 0 {
            let t = db.get_table("users").unwrap();
            let qs = declare(&db);
            (qs, t.all().answer_counts("cnt", "city").unwrap(), t.all().where_gt("age", 10i64).answer_sums("sum", "city", "score").unwrap())
        } else {
            // 前の process の版は全部 Truncated のはず
            let qs = reopen_handles(&db);
            for (i, q) in qs.iter().enumerate() {
                if i == 3 { continue; }
                for rd in &readers[i] {
                    if let Some(p) = rd.pos {
                        assert!(q.poll_since(p).is_err(), "seed {seed} round {round} s{i}: 前の process の版 {p} が Truncated でない");
                    }
                }
            }
            for rs in readers.iter_mut() { for rd in rs.iter_mut() { rd.pos = None; } }
            for rs in creaders.iter_mut() { for rd in rs.iter_mut() { rd.pos = None; } }
            (qs, db.answer("cnt").and_then(Answer::into_counts).unwrap(), db.answer("sum").and_then(Answer::into_counts).unwrap())
        };
        let group = db.live_group();
        group.add(&qs[0]);
        group.add(&qs[2]);
        for i in 0..500 {
            let d = step(&db, &mut model, &mut r, &mut next_id);
            hist.push(d);
            let ctx_tail = || hist[hist.len().saturating_sub(6)..].join(" | ");
            // 読み手を乱数で動かす
            for _ in 0..r.below(4) {
                let which = r.below(10);
                match which {
                    0 => { let i = r.below(N_SETS as u64) as usize; let _ = qs[i].poll(); }
                    1 => { let _ = group.poll(); }
                    2 => { let _ = cq.poll(); }
                    3 => { let _ = sq.poll_sums(); }
                    4 => {
                        let c = r.below(2) as usize;
                        let k = r.below(READERS as u64) as usize;
                        let ctx = format!("seed {seed} round {round} op {i} counts{c} reader {k} [{}]", ctx_tail());
                        if c == 0 {
                            read_counts(&cq, &mut creaders[0][k], &want_counts(&model, false), &ctx, false);
                        } else {
                            read_counts(&sq, &mut creaders[1][k], &want_counts(&model, true), &ctx, true);
                        }
                    }
                    _ => {
                        let s = r.below(N_SETS as u64) as usize;
                        let k = r.below(READERS as u64) as usize;
                        let ctx = format!("seed {seed} round {round} op {i} s{s} reader {k} [{}]", ctx_tail());
                        read_set(&qs[s], &mut readers[s][k], &want_set(&model, s), &ctx, &mut stats);
                    }
                }
            }
        }
        // round の終わり: 全部の読み手を進めて合わせる。 find とも比べる
        for s in 0..N_SETS {
            for k in 0..READERS {
                let ctx = format!("seed {seed} round {round} end s{s} reader {k}");
                read_set(&qs[s], &mut readers[s][k], &want_set(&model, s), &ctx, &mut stats);
            }
        }
        let t = db.get_table("users").unwrap();
        let f: BTreeSet<u64> = t.all().where_gt("age", 30i64).find().unwrap().into_iter().collect();
        assert_eq!(f, want_set(&model, 0), "seed {seed}: find と模型");
        let f: BTreeSet<u64> = t.where_eq("city", "c1").or(t.all().where_lt("age", 15i64)).find().unwrap().into_iter().collect();
        assert_eq!(f, want_set(&model, 1), "seed {seed}: OR の find と模型");
        let f: BTreeSet<u64> = t.all().where_not_null("age").where_null("city").find().unwrap().into_iter().collect();
        assert_eq!(f, want_set(&model, 5), "seed {seed}: null の find と模型");
        for k in 0..READERS {
            read_counts(&cq, &mut creaders[0][k], &want_counts(&model, false), &format!("seed {seed} round {round} end cnt"), false);
            read_counts(&sq, &mut creaders[1][k], &want_counts(&model, true), &format!("seed {seed} round {round} end sum"), true);
        }
        drop(group);
        drop(qs);
        drop(cq);
        drop(sq);
        drop(db);
    }
    cleanup(&path);
    stats
}

fn seeds() -> Vec<u64> {
    match std::env::var("REVIEW_SEED") {
        Ok(s) => s.split(',').map(|x| x.parse().unwrap()).collect(),
        Err(_) => (1..=20).collect(),
    }
}

#[test]
fn poll_since_composes_under_random_ops() {
    for seed in seeds() {
        let (ok, snap) = run(seed, None);
        eprintln!("seed {seed} ok (poll_since {ok} 回, snapshot {snap} 回)");
    }
}

/// 記録の上限を小さくして、 切れた直後 (Truncated → snapshot) を多く踏む。
#[test]
fn poll_since_composes_with_a_tiny_log() {
    for seed in seeds() {
        let (ok, snap) = run(seed + 500, Some(64));
        eprintln!("seed {} ok tiny log (poll_since {ok} 回, snapshot {snap} 回)", seed + 500);
    }
}
