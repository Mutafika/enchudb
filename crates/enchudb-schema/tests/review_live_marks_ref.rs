//! review: Ref の列 (dense の窓が先の表の eid の始まりから、 #463) と Tag の列に位置の印を付けた表と付けない表を、 同じ乱数の
//! 付け替え・消し・作り直しで比べる (模型と)。 開き直しも。

use enchudb_schema::{Database, Value};
use std::collections::BTreeMap;

const SCRATCH: &str = "/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/eedfc333-d87a-4a47-a3b0-daf4b6d7337a/scratchpad";

fn tmp_path(name: &str) -> String {
    let p = format!("{SCRATCH}/rl-marksref-{}-{}", name, std::process::id());
    cleanup(&p);
    p
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

const NC: usize = 9;

fn check(db: &Database, table: &str, model: &BTreeMap<i64, (u64, Option<usize>, Option<usize>)>, cs: &[u64], ctx: &str) {
    let t = db.get_table(table).unwrap();
    for (ci, &c) in cs.iter().enumerate() {
        let mut want: Vec<u64> = model.values().filter(|r| r.1 == Some(ci)).map(|r| r.0).collect();
        want.sort_unstable();
        let mut got = t.where_ref("company", c).find().unwrap();
        got.sort_unstable();
        assert_eq!(got, want, "[{table}] {ctx}: where_ref(company {ci})");
        assert_eq!(t.where_ref("company", c).count().unwrap(), want.len(), "[{table}] {ctx}: count company {ci}");
        // ref + tag
        let tag = format!("g{}", ci % 3);
        let mut want: Vec<u64> = model.values().filter(|r| r.1 == Some(ci) && r.2 == Some(ci % 3)).map(|r| r.0).collect();
        want.sort_unstable();
        let mut got = t.where_ref("company", c).where_eq("grp", tag.as_str()).find().unwrap();
        got.sort_unstable();
        assert_eq!(got, want, "[{table}] {ctx}: where_ref(company {ci}) & grp={tag}");
        // ref の先の条件 (逆引き)
    }
    for g in 0..3usize {
        let tag = format!("g{g}");
        let mut want: Vec<u64> = model.values().filter(|r| r.2 == Some(g)).map(|r| r.0).collect();
        want.sort_unstable();
        let mut got = t.where_eq("grp", tag.as_str()).find().unwrap();
        got.sort_unstable();
        assert_eq!(got, want, "[{table}] {ctx}: grp={tag}");
        assert_eq!(t.where_eq("grp", tag.as_str()).count().unwrap(), want.len(), "[{table}] {ctx}: count grp={tag}");
    }
    for city in ["A", "B", "C"] {
        let mut want: Vec<u64> = model.values().filter(|r| r.1.is_some_and(|ci| ["A", "B", "C"][ci % 3] == city)).map(|r| r.0).collect();
        want.sort_unstable();
        let mut got = t.where_eq("company.city", city).find().unwrap();
        got.sort_unstable();
        assert_eq!(got, want, "[{table}] {ctx}: company.city={city}");
    }
    assert_eq!(t.all().count().unwrap(), model.len(), "[{table}] {ctx}: all count");
}

#[test]
fn marked_ref_and_tag_columns_match_the_model() {
    let seeds: Vec<u64> = match std::env::var("REVIEW_SEED") {
        Ok(s) => s.split(',').map(|x| x.parse().unwrap()).collect(),
        Err(_) => (1..=8).collect(),
    };
    for seed in seeds {
        let path = tmp_path(&format!("s{seed}"));
        let mut r = Rng(seed.wrapping_mul(0x9E3779B97F4A7C15) | 1);
        // (eid, company, grp) を表ごとに
        let mut mm: BTreeMap<i64, (u64, Option<usize>, Option<usize>)> = BTreeMap::new();
        let mut mu: BTreeMap<i64, (u64, Option<usize>, Option<usize>)> = BTreeMap::new();
        let mut cs: Vec<u64> = Vec::new();
        let mut next = 0i64;
        for round in 0..3 {
            let db = if round == 0 {
                let mut db = Database::create(&path).unwrap();
                // companies を 3 つ目以降の表に (eid が 2^20 の先)
                db.table("pad1").number("id").primary_key("id").build().unwrap();
                db.table("pad2").number("id").primary_key("id").build().unwrap();
                db.table("companies").number("id").tag("city").primary_key("id").build().unwrap();
                db.table("m").number("id").ref_to("company", "companies").tag("grp").primary_key("id").marks("company").marks("grp").build().unwrap();
                db.table("u").number("id").ref_to("company", "companies").tag("grp").primary_key("id").build().unwrap();
                let c = db.get_table("companies").unwrap();
                cs = (0..NC as i64).map(|i| c.insert().set("id", i).set("city", ["A", "B", "C"][(i % 3) as usize]).commit().unwrap()).collect();
                assert!(cs[0] as u32 >= 1 << 20, "companies の eid が 2^20 の先でない: {:#x}", cs[0]);
                db
            } else {
                let db = Database::open(&path).unwrap();
                assert_eq!(db.engine().marks_declarations().len(), 2);
                db
            };
            if round != 1 {
                check(&db, "m", &mm, &cs, &format!("seed {seed} round {round} open"));
                check(&db, "u", &mu, &cs, &format!("seed {seed} round {round} open"));
            }
            for i in 0..1500 {
                let k = r.below(100);
                let live: Vec<i64> = mm.keys().copied().collect();
                let what;
                if live.is_empty() || k < 25 {
                    let id = next;
                    next += 1;
                    let c = (r.below(8) != 0).then(|| r.below(NC as u64) as usize);
                    let g = (r.below(8) != 0).then(|| r.below(3) as usize);
                    for (name, model) in [("m", &mut mm), ("u", &mut mu)] {
                        let t = db.get_table(name).unwrap();
                        let mut b = t.insert().set("id", id);
                        if let Some(c) = c { b = b.set("company", Value::Ref(cs[c])); }
                        if let Some(g) = g { b = b.set("grp", format!("g{g}")); }
                        model.insert(id, (b.commit().unwrap(), c, g));
                    }
                    what = format!("insert {id} c={c:?} g={g:?}");
                } else {
                    let id = live[r.below(live.len() as u64) as usize];
                    if k < 38 {
                        for (name, model) in [("m", &mut mm), ("u", &mut mu)] {
                            let (e, ..) = model.remove(&id).unwrap();
                            db.get_table(name).unwrap().entity(e).delete().unwrap();
                        }
                        what = format!("delete {id}");
                    } else if k < 48 {
                        let col = if r.below(2) == 0 { "company" } else { "grp" };
                        for (name, model) in [("m", &mut mm), ("u", &mut mu)] {
                            let t = db.get_table(name).unwrap();
                            let row = model.get_mut(&id).unwrap();
                            db.engine().untie_by_id(row.0, t.himo_id(col).unwrap());
                            if col == "company" { row.1 = None } else { row.2 = None }
                        }
                        what = format!("untie {id}.{col}");
                    } else if k < 80 {
                        let c = r.below(NC as u64) as usize;
                        for (name, model) in [("m", &mut mm), ("u", &mut mu)] {
                            let t = db.get_table(name).unwrap();
                            let row = model.get_mut(&id).unwrap();
                            t.entity(row.0).set("company", Value::Ref(cs[c])).commit().unwrap();
                            row.1 = Some(c);
                        }
                        what = format!("set {id}.company={c}");
                    } else {
                        let g = r.below(3) as usize;
                        for (name, model) in [("m", &mut mm), ("u", &mut mu)] {
                            let t = db.get_table(name).unwrap();
                            let row = model.get_mut(&id).unwrap();
                            t.upsert().set("id", id).set("grp", format!("g{g}")).commit().unwrap();
                            row.2 = Some(g);
                        }
                        what = format!("set {id}.grp=g{g}");
                    }
                }
                if i % 23 == 0 {
                    let ctx = format!("seed {seed} round {round} op {i} ({what})");
                    check(&db, "m", &mm, &cs, &ctx);
                    check(&db, "u", &mu, &cs, &ctx);
                }
            }
            let ctx = format!("seed {seed} round {round} end");
            check(&db, "m", &mm, &cs, &ctx);
            check(&db, "u", &mu, &cs, &ctx);
        }
        cleanup(&path);
        eprintln!("seed {seed} ok (ref / tag marks)");
    }
}
