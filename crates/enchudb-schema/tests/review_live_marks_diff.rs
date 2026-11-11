//! review: 位置の印を宣言した表 (m) と宣言しない表 (u) に同じ乱数の操作列を当て、 模型 (手で持つ行の表) と比べる。
//! 種は REVIEW_SEED (既定は下の一覧)。

use enchudb_schema::{Database, Table, Value};
use std::collections::BTreeMap;

const SCRATCH: &str = "/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/eedfc333-d87a-4a47-a3b0-daf4b6d7337a/scratchpad";

fn tmp_path(name: &str) -> String {
    format!("{SCRATCH}/rl-marks-{}-{}", name, std::process::id())
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

#[derive(Clone, Debug, Default, PartialEq)]
struct Row {
    a: Option<u32>,
    b: Option<u32>,
    c: Option<i64>,
    t: Option<String>,
}

fn pick_a(r: &mut Rng) -> u32 { r.below(40) as u32 }
fn pick_b(r: &mut Rng) -> u32 {
    const EDGE: [u32; 10] = [0, 1, (1 << 20) - 1, 1 << 20, (1 << 20) + 1, (1 << 20) + 2, u32::MAX - 2, u32::MAX - 1, 4096, 1 << 21];
    match r.below(3) {
        0 => EDGE[r.below(EDGE.len() as u64) as usize],
        1 => r.below(64) as u32,
        _ => (1 << 20) - 32 + r.below(64) as u32,
    }
}
fn pick_c(r: &mut Rng) -> i64 {
    const EDGE: [i64; 9] = [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX - 2, 1 << 20, -(1 << 20)];
    match r.below(3) {
        0 => EDGE[r.below(EDGE.len() as u64) as usize],
        1 => r.below(50) as i64 - 25,
        _ => 1_790_000_000_000 + r.below(30) as i64,
    }
}
fn pick_t(r: &mut Rng) -> String { format!("t{}", r.below(8)) }

struct Side<'a> {
    name: &'static str,
    t: Table<'a>,
    db: &'a Database,
    eids: BTreeMap<i64, u64>,
}

impl Side<'_> {
    fn hid(&self, col: &str) -> u16 { self.t.himo_id(col).unwrap() }
}

fn apply(side: &mut Side, op: &Op) {
    match op {
        Op::Insert(id, row) => {
            let mut b = side.t.insert().set("id", *id);
            if let Some(a) = row.a { b = b.set("a", a as i64); }
            if let Some(x) = row.b { b = b.set("b", x as i64); }
            if let Some(c) = row.c { b = b.set("c", c); }
            if let Some(t) = &row.t { b = b.set("t", t.as_str()); }
            let e = b.commit().unwrap();
            side.eids.insert(*id, e);
        }
        Op::Set(id, col, v) => {
            let e = side.eids[id];
            side.t.entity(e).set(col, v.clone()).commit().unwrap();
        }
        Op::Upsert(id, col, v) => {
            side.t.upsert().set("id", *id).set(col, v.clone()).commit().unwrap();
        }
        Op::Untie(id, col) => {
            let e = side.eids[id];
            side.db.engine().untie_by_id(e, side.hid(col));
        }
        Op::Delete(id) => {
            let e = side.eids.remove(id).unwrap();
            side.t.entity(e).delete().unwrap();
        }
    }
}

#[derive(Clone, Debug)]
enum Op {
    Insert(i64, Row),
    Set(i64, &'static str, Value),
    Upsert(i64, &'static str, Value),
    Untie(i64, &'static str),
    Delete(i64),
}

fn gen_op(r: &mut Rng, model: &mut BTreeMap<i64, Row>, next_id: &mut i64, id_base: i64) -> Op {
    let live: Vec<i64> = model.keys().copied().collect();
    let k = r.below(100);
    if live.is_empty() || k < 22 {
        // insert (時々、 消した id の使い回し)
        let id = *next_id;
        *next_id += 1;
        let row = Row {
            a: (r.below(10) != 0).then(|| pick_a(r)),
            b: (r.below(10) != 0).then(|| pick_b(r)),
            c: (r.below(10) != 0).then(|| pick_c(r)),
            t: (r.below(10) != 0).then(|| pick_t(r)),
        };
        model.insert(id, row.clone());
        let _ = id_base;
        return Op::Insert(id, row);
    }
    let id = live[r.below(live.len() as u64) as usize];
    if k < 34 {
        model.remove(&id);
        return Op::Delete(id);
    }
    let col = ["a", "b", "c", "t"][r.below(4) as usize];
    if k < 46 {
        let row = model.get_mut(&id).unwrap();
        match col { "a" => row.a = None, "b" => row.b = None, "c" => row.c = None, _ => row.t = None }
        return Op::Untie(id, col);
    }
    let row = model.get_mut(&id).unwrap();
    // 同じ値の書き直しを 1/5
    let same = r.below(5) == 0;
    let v = match col {
        "a" => { let v = if same { row.a.unwrap_or(0) } else { pick_a(r) }; row.a = Some(v); Value::Number(v as i64) }
        "b" => { let v = if same { row.b.unwrap_or(0) } else { pick_b(r) }; row.b = Some(v); Value::Number(v as i64) }
        "c" => { let v = if same { row.c.unwrap_or(0) } else { pick_c(r) }; row.c = Some(v); Value::Number(v) }
        _ => { let v = if same { row.t.clone().unwrap_or("t0".into()) } else { pick_t(r) }; row.t = Some(v.clone()); Value::Text(v) }
    };
    if r.below(2) == 0 { Op::Set(id, col, v) } else { Op::Upsert(id, col, v) }
}

/// eid の集合を id の昇順に。 重複・模型に無い eid は panic。
fn ids_of(side: &Side, eids: &[u64], what: &str) -> Vec<i64> {
    let rev: BTreeMap<u64, i64> = side.eids.iter().map(|(k, v)| (*v, *k)).collect();
    let mut out: Vec<i64> = eids
        .iter()
        .map(|e| *rev.get(e).unwrap_or_else(|| panic!("[{}] {what}: eid {e:#x} は生きた行でない", side.name)))
        .collect();
    out.sort_unstable();
    let n = out.len();
    out.dedup();
    assert_eq!(n, out.len(), "[{}] {what}: 重複した eid", side.name);
    out
}

fn check(side: &Side, model: &BTreeMap<i64, Row>, r: &mut Rng, ctx: &str) {
    let t = &side.t;
    let sel = |f: &dyn Fn(&Row) -> bool| -> Vec<i64> { model.iter().filter(|(_, row)| f(row)).map(|(k, _)| *k).collect() };
    macro_rules! same_set {
        ($q:expr, $want:expr, $what:expr) => {{
            let what = format!("{ctx} {}", $what);
            let got = ids_of(side, &$q.find().unwrap(), &what);
            let want: Vec<i64> = $want;
            assert_eq!(got, want, "[{}] find {what}", side.name);
            assert_eq!($q.count().unwrap(), want.len(), "[{}] count {what}", side.name);
        }};
    }
    // 全件
    same_set!(t.all(), sel(&|_| true), "all");
    // 等値 a
    for _ in 0..3 {
        let v = pick_a(r);
        same_set!(t.where_eq("a", v as i64), sel(&|x| x.a == Some(v)), format!("a={v}"));
    }
    // 等値 b / c / t
    for _ in 0..3 {
        let v = pick_b(r);
        same_set!(t.where_eq("b", v as i64), sel(&|x| x.b == Some(v)), format!("b={v}"));
        let v = pick_c(r);
        same_set!(t.where_eq("c", v), sel(&|x| x.c == Some(v)), format!("c={v}"));
        let v = pick_t(r);
        same_set!(t.where_eq("t", v.as_str()), sel(&|x| x.t.as_deref() == Some(v.as_str())), format!("t={v}"));
    }
    // 範囲
    for _ in 0..4 {
        let (x, y) = (pick_a(r), pick_a(r));
        let (lo, hi) = (x.min(y), x.max(y));
        same_set!(t.where_range("a", lo as i64, hi as i64), sel(&|x| x.a.is_some_and(|v| v >= lo && v <= hi)), format!("a in {lo}..={hi}"));
        let (x, y) = (pick_b(r), pick_b(r));
        let (lo, hi) = (x.min(y), x.max(y));
        same_set!(t.where_range("b", lo as i64, hi as i64), sel(&|x| x.b.is_some_and(|v| v >= lo && v <= hi)), format!("b in {lo}..={hi}"));
        same_set!(t.all().where_gt("b", lo as i64), sel(&|x| x.b.is_some_and(|v| v > lo)), format!("b > {lo}"));
        same_set!(t.all().where_le("b", hi as i64), sel(&|x| x.b.is_some_and(|v| v <= hi)), format!("b <= {hi}"));
        let (x, y) = (pick_c(r), pick_c(r));
        let (lo, hi) = (x.min(y), x.max(y));
        same_set!(t.where_range("c", lo, hi), sel(&|x| x.c.is_some_and(|v| v >= lo && v <= hi)), format!("c in {lo}..={hi}"));
        same_set!(t.all().where_lt("c", hi), sel(&|x| x.c.is_some_and(|v| v < hi)), format!("c < {hi}"));
        same_set!(t.all().where_ge("c", lo), sel(&|x| x.c.is_some_and(|v| v >= lo)), format!("c >= {lo}"));
        // 等値 + 範囲
        let av = pick_a(r);
        let (x, y) = (pick_b(r), pick_b(r));
        let (lo, hi) = (x.min(y), x.max(y));
        same_set!(
            t.where_eq("a", av as i64).where_range("b", lo as i64, hi as i64),
            sel(&|x| x.a == Some(av) && x.b.is_some_and(|v| v >= lo && v <= hi)),
            format!("a={av} & b in {lo}..={hi}")
        );
        let tv = pick_t(r);
        let (x, y) = (pick_c(r), pick_c(r));
        let (lo, hi) = (x.min(y), x.max(y));
        same_set!(
            t.where_eq("t", tv.as_str()).where_range("c", lo, hi),
            sel(&|x| x.t.as_deref() == Some(tv.as_str()) && x.c.is_some_and(|v| v >= lo && v <= hi)),
            format!("t={tv} & c in {lo}..={hi}")
        );
        // 否定 / 値なし
        same_set!(t.all().where_ne("a", av as i64), sel(&|x| x.a.is_some_and(|v| v != av)), format!("a != {av}"));
        same_set!(t.all().where_null("b"), sel(&|x| x.b.is_none()), "b is null");
        // where_in
        let (v1, v2) = (pick_a(r), pick_a(r));
        same_set!(t.where_in("a", &[v1, v2]), sel(&|x| x.a == Some(v1) || x.a == Some(v2)), format!("a in [{v1},{v2}]"));
    }
    // 上位 k 件 (値の並びを比べる。 同じ値の中の順は決まらないので値の列で比べる)
    let rev: BTreeMap<u64, i64> = side.eids.iter().map(|(k, v)| (*v, *k)).collect();
    for &k in &[1usize, 3, 10, 1000] {
        for desc in [false, true] {
            // b
            let q = if desc { t.all().order_by_desc("b") } else { t.all().order_by("b") };
            let got: Vec<u32> = q.limit(k).find().unwrap().iter().map(|e| model[&rev[e]].b.unwrap()).collect();
            let mut want: Vec<u32> = model.values().filter_map(|x| x.b).collect();
            want.sort_unstable();
            if desc { want.reverse(); }
            want.truncate(k);
            assert_eq!(got, want, "[{}] {ctx} top{k} b desc={desc}", side.name);
            // c
            let q = if desc { t.all().order_by_desc("c") } else { t.all().order_by("c") };
            let got_e = q.limit(k).find().unwrap();
            let mut de = got_e.clone();
            de.sort_unstable();
            de.dedup();
            assert_eq!(de.len(), got_e.len(), "[{}] {ctx} top{k} c: 重複", side.name);
            let got: Vec<i64> = got_e.iter().map(|e| model[&rev[e]].c.unwrap()).collect();
            let mut want: Vec<i64> = model.values().filter_map(|x| x.c).collect();
            want.sort_unstable();
            if desc { want.reverse(); }
            want.truncate(k);
            assert_eq!(got, want, "[{}] {ctx} top{k} c desc={desc}", side.name);
            // 等値 + 並び
            let av = pick_a(r);
            let q = t.where_eq("a", av as i64);
            let q = if desc { q.order_by_desc("b") } else { q.order_by("b") };
            let got: Vec<u32> = q.limit(k).find().unwrap().iter().map(|e| model[&rev[e]].b.unwrap()).collect();
            let mut want: Vec<u32> = model.values().filter(|x| x.a == Some(av)).filter_map(|x| x.b).collect();
            want.sort_unstable();
            if desc { want.reverse(); }
            want.truncate(k);
            assert_eq!(got, want, "[{}] {ctx} a={av} top{k} b desc={desc}", side.name);
            // 範囲 + 並び
            let (x, y) = (pick_b(r), pick_b(r));
            let (lo, hi) = (x.min(y), x.max(y));
            let q = t.where_range("b", lo as i64, hi as i64);
            let q = if desc { q.order_by_desc("b") } else { q.order_by("b") };
            let got: Vec<u32> = q.limit(k).find().unwrap().iter().map(|e| model[&rev[e]].b.unwrap()).collect();
            let mut want: Vec<u32> = model.values().filter_map(|x| x.b).filter(|v| *v >= lo && *v <= hi).collect();
            want.sort_unstable();
            if desc { want.reverse(); }
            want.truncate(k);
            assert_eq!(got, want, "[{}] {ctx} b in {lo}..={hi} top{k} b desc={desc}", side.name);
        }
    }
    // min / max
    let bs: Vec<u32> = model.values().filter_map(|x| x.b).collect();
    assert_eq!(t.all().min("b").unwrap(), bs.iter().copied().min(), "[{}] {ctx} min b", side.name);
    assert_eq!(t.all().max("b").unwrap(), bs.iter().copied().max(), "[{}] {ctx} max b", side.name);
    let as_: Vec<u32> = model.values().filter_map(|x| x.a).collect();
    assert_eq!(t.all().min("a").unwrap(), as_.iter().copied().min(), "[{}] {ctx} min a", side.name);
    assert_eq!(t.all().max("a").unwrap(), as_.iter().copied().max(), "[{}] {ctx} max a", side.name);
    assert_eq!(t.min("a"), as_.iter().copied().min(), "[{}] {ctx} table min a", side.name);
    assert_eq!(t.max("a"), as_.iter().copied().max(), "[{}] {ctx} table max a", side.name);
    assert_eq!(t.min("b"), bs.iter().copied().min(), "[{}] {ctx} table min b", side.name);
    assert_eq!(t.max("b"), bs.iter().copied().max(), "[{}] {ctx} table max b", side.name);
    let cs: Vec<i64> = model.values().filter_map(|x| x.c).collect();
    assert_eq!(t.all().min_i64("c").unwrap(), cs.iter().copied().min(), "[{}] {ctx} min c", side.name);
    assert_eq!(t.all().max_i64("c").unwrap(), cs.iter().copied().max(), "[{}] {ctx} max c", side.name);
    let av = pick_a(r);
    let bsa: Vec<u32> = model.values().filter(|x| x.a == Some(av)).filter_map(|x| x.b).collect();
    assert_eq!(t.where_eq("a", av as i64).min("b").unwrap(), bsa.iter().copied().min(), "[{}] {ctx} a={av} min b", side.name);
    assert_eq!(t.where_eq("a", av as i64).max("b").unwrap(), bsa.iter().copied().max(), "[{}] {ctx} a={av} max b", side.name);
    let csa: Vec<i64> = model.values().filter(|x| x.a == Some(av)).filter_map(|x| x.c).collect();
    assert_eq!(t.where_eq("a", av as i64).min_i64("c").unwrap(), csa.iter().copied().min(), "[{}] {ctx} a={av} min c", side.name);
    assert_eq!(t.where_eq("a", av as i64).max_i64("c").unwrap(), csa.iter().copied().max(), "[{}] {ctx} a={av} max c", side.name);
    assert_eq!(
        t.all().sum_i128("c").unwrap(),
        cs.iter().map(|v| *v as i128).sum::<i128>(),
        "[{}] {ctx} sum c",
        side.name
    );
    // group 集計 (a ごとの b の和 / 最小 / 最大)
    let mut want: BTreeMap<u32, (u64, u32, u32)> = BTreeMap::new();
    for x in model.values() {
        if let (Some(a), Some(b)) = (x.a, x.b) {
            let e = want.entry(a).or_insert((0, u32::MAX, 0));
            e.0 += b as u64;
            e.1 = e.1.min(b);
            e.2 = e.2.max(b);
        }
    }
    let mut got = t.all().group_sum("a", "b").unwrap();
    got.sort_unstable();
    assert_eq!(got, want.iter().map(|(k, v)| (*k, v.0)).collect::<Vec<_>>(), "[{}] {ctx} group_sum", side.name);
    let mut got = t.all().group_min("a", "b").unwrap();
    got.sort_unstable();
    assert_eq!(got, want.iter().map(|(k, v)| (*k, v.1)).collect::<Vec<_>>(), "[{}] {ctx} group_min", side.name);
    let mut got = t.all().group_max("a", "b").unwrap();
    got.sort_unstable();
    assert_eq!(got, want.iter().map(|(k, v)| (*k, v.2)).collect::<Vec<_>>(), "[{}] {ctx} group_max", side.name);
    let mut got = t.group_sum("a", "b");
    got.sort_unstable();
    assert_eq!(got, want.iter().map(|(k, v)| (*k, v.0)).collect::<Vec<_>>(), "[{}] {ctx} table group_sum", side.name);
}

fn build(db: &mut Database) {
    db.table("m").number("id").number("a").number("b").bigint("c").tag("t").primary_key("id")
        .marks("a").marks("b").marks("c").marks("t").marks("id")
        .build().unwrap();
    db.table("u").number("id").number("a").number("b").bigint("c").tag("t").primary_key("id").build().unwrap();
}

fn run_seed(seed: u64, ops: usize, check_every: usize) {
    let path = tmp_path(&format!("s{seed}"));
    cleanup(&path);
    let mut r = Rng(seed.wrapping_mul(0x9E3779B97F4A7C15) | 1);
    let mut model: BTreeMap<i64, Row> = BTreeMap::new();
    let mut next_id = 0i64;
    let mut eids_m = BTreeMap::new();
    let mut eids_u = BTreeMap::new();
    let mut done = 0usize;
    for round in 0..3 {
        let mut db = if round == 0 { Database::create(&path).unwrap() } else { Database::open(&path).unwrap() };
        if round == 0 {
            build(&mut db);
        } else {
            assert_eq!(db.engine().marks_declarations().len(), 5, "seed {seed}: 開き直しで印が戻る");
        }
        let mut m = Side { name: "marks", t: db.get_table("m").unwrap(), db: &db, eids: std::mem::take(&mut eids_m) };
        let mut u = Side { name: "plain", t: db.get_table("u").unwrap(), db: &db, eids: std::mem::take(&mut eids_u) };
        // 開いた直後 (索引を組む前の書き込みも混ぜる: 奇数の round は先に書く)
        if round % 2 == 0 {
            let mut cr = Rng(r.next() | 1);
            let mut cr2 = Rng(cr.0);
            check(&m, &model, &mut cr, &format!("seed {seed} round {round} open"));
            check(&u, &model, &mut cr2, &format!("seed {seed} round {round} open"));
        }
        for i in 0..ops {
            let op = gen_op(&mut r, &mut model, &mut next_id, 0);
            apply(&mut m, &op);
            apply(&mut u, &op);
            done += 1;
            if (i + 1) % check_every == 0 {
                let mut cr = Rng(r.next() | 1);
                let mut cr2 = Rng(cr.0);
                let ctx = format!("seed {seed} round {round} op {done} (last {op:?})");
                check(&m, &model, &mut cr, &ctx);
                check(&u, &model, &mut cr2, &ctx);
            }
        }
        eids_m = std::mem::take(&mut m.eids);
        eids_u = std::mem::take(&mut u.eids);
    }
    cleanup(&path);
}

fn seeds() -> Vec<u64> {
    match std::env::var("REVIEW_SEED") {
        Ok(s) => s.split(',').map(|x| x.parse().unwrap()).collect(),
        Err(_) => (1..=12).collect(),
    }
}

#[test]
fn marks_vs_plain_vs_model_small_steps() {
    for seed in seeds() {
        run_seed(seed, 400, 7);
        eprintln!("seed {seed} ok (small)");
    }
}

#[test]
fn marks_vs_plain_vs_model_long() {
    for seed in seeds() {
        run_seed(seed + 1000, 6000, 500);
        eprintln!("seed {} ok (long)", seed + 1000);
    }
}

/// 書き手 4 本 (id は thread ごとに別) の後、 終わってから比べる。
#[test]
fn marks_vs_model_after_4_writers() {
    for seed in seeds().into_iter().take(4) {
        let path = tmp_path(&format!("w{seed}"));
        cleanup(&path);
        let mut db = Database::create(&path).unwrap();
        build(&mut db);
        // 先に少し入れて索引を組む
        {
            let m = db.get_table("m").unwrap();
            let u = db.get_table("u").unwrap();
            for t in [&m, &u] {
                t.insert().set("id", 1_000_000i64).set("a", 1i64).set("b", 5i64).set("c", 5i64).set("t", "t0").commit().unwrap();
                t.where_range("a", 0i64, 39i64).find().unwrap();
                t.where_range("b", 0i64, 100i64).find().unwrap();
                t.where_range("c", -5i64, 100i64).find().unwrap();
                t.where_eq("t", "t0").find().unwrap();
            }
        }
        let db = db.finish_with_oplog(64 * 1024 * 1024).unwrap();
        let mut handles = Vec::new();
        for w in 0..4u64 {
            let db = db.clone();
            handles.push(std::thread::spawn(move || {
                let mut r = Rng((seed * 100 + w + 1).wrapping_mul(0x9E3779B97F4A7C15) | 1);
                let mut model: BTreeMap<i64, Row> = BTreeMap::new();
                let mut next_id = (w as i64) * 10_000_000;
                let mut m = Side { name: "marks", t: db.get_table("m").unwrap(), db: &db, eids: BTreeMap::new() };
                let mut u = Side { name: "plain", t: db.get_table("u").unwrap(), db: &db, eids: BTreeMap::new() };
                for i in 0..4000 {
                    let op = gen_op(&mut r, &mut model, &mut next_id, 0);
                    apply(&mut m, &op);
                    apply(&mut u, &op);
                    if i % 97 == 0 {
                        // 読みを混ぜる (詰め直しを起こす)
                        let _ = m.t.where_range("a", 0i64, 39i64).count().unwrap();
                        let _ = m.t.where_range("b", 0i64, 1i64 << 21).find().unwrap();
                        let _ = m.t.all().order_by("c").limit(5).find().unwrap();
                    }
                }
                (model, m.eids, u.eids)
            }));
        }
        let mut model: BTreeMap<i64, Row> = BTreeMap::new();
        model.insert(1_000_000, Row { a: Some(1), b: Some(5), c: Some(5), t: Some("t0".into()) });
        let mut em = BTreeMap::new();
        let mut eu = BTreeMap::new();
        {
            let m = db.get_table("m").unwrap();
            let u = db.get_table("u").unwrap();
            em.insert(1_000_000i64, m.where_eq("id", 1_000_000i64).find_one().unwrap().unwrap());
            eu.insert(1_000_000i64, u.where_eq("id", 1_000_000i64).find_one().unwrap().unwrap());
        }
        for h in handles {
            let (mo, a, b) = h.join().unwrap();
            model.extend(mo);
            em.extend(a);
            eu.extend(b);
        }
        let m = Side { name: "marks", t: db.get_table("m").unwrap(), db: &db, eids: em };
        let u = Side { name: "plain", t: db.get_table("u").unwrap(), db: &db, eids: eu };
        for rep in 0..3 {
            let mut cr = Rng(seed * 7 + rep + 1);
            let mut cr2 = Rng(cr.0);
            check(&m, &model, &mut cr, &format!("writers seed {seed} rep {rep}"));
            check(&u, &model, &mut cr2, &format!("writers seed {seed} rep {rep}"));
        }
        drop(m);
        drop(u);
        drop(db);
        cleanup(&path);
        eprintln!("seed {seed} ok (4 writers)");
    }
}

#[test]
fn marks_vs_plain_vs_model_medium() {
    for seed in seeds() {
        run_seed(seed + 2000, 900, 45);
        eprintln!("seed {} ok (medium)", seed + 2000);
    }
}
