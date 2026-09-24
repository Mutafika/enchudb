//! schema 層の live query (`Query::subscribe`): poll の差分を積分した集合が、 毎回の
//! `find()` と常に一致すること。 row 操作は insert / upsert / update / set / delete を
//! 混ぜ、 条件は Tag の等値 (登録時に未知の文字列) / Ref / 範囲 / 比較 / IN / 全 row。

use enchudb_schema::{Database, LiveDelta, LiveQuery, Query, Value};
use std::collections::BTreeSet;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("/tmp/schema_live_{}_{}_{}.db", tag, std::process::id(), nanos)
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

struct Sub<'a> {
    name: &'static str,
    q: LiveQuery,
    seen: BTreeSet<u64>,
    find: Box<dyn Fn() -> Query<'a> + 'a>,
}

#[test]
fn subscribe_tracks_find_through_row_operations() {
    let path = tmp_path("rows");
    cleanup(&path);
    let mut db = Database::create_growable_tiny(&path).unwrap();
    db.table("companies").number("id").tag("name").primary_key("id").build().unwrap();
    db.table("users")
        .number("id")
        .tag("city")
        .number("age")
        .ref_to("company", "companies")
        .primary_key("id")
        .build()
        .unwrap();
    let companies = db.get_table("companies").unwrap();
    let ant = companies.insert().set("id", 1i64).set("name", "Anthropic").commit().unwrap();
    let other = companies.insert().set("id", 2i64).set("name", "Other").commit().unwrap();
    let users = db.get_table("users").unwrap();
    // 購読前の row (初期集合)
    for id in 0..40i64 {
        users.insert().set("id", id).set("city", "Osaka").set("age", id % 50).commit().unwrap();
    }

    fn mk<'a>(name: &'static str, f: Box<dyn Fn() -> Query<'a> + 'a>) -> Sub<'a> {
        Sub { name, q: f().subscribe().unwrap(), seen: BTreeSet::new(), find: f }
    }
    let u = &users;
    let mut subs = vec![
        mk("city=Tokyo (未知の文字列)", Box::new(move || u.where_eq("city", "Tokyo"))),
        mk("company=ant", Box::new(move || u.where_ref("company", ant))),
        mk("20<=age<=29", Box::new(move || u.where_range("age", 20, 29))),
        mk("age>40 AND city=Osaka", Box::new(move || u.where_eq("city", "Osaka").where_gt("age", 40))),
        mk("age<5", Box::new(move || u.all().where_lt("age", 5))),
        mk("age IN {7,8}", Box::new(move || u.where_in("age", &[7, 8]))),
        mk("all", Box::new(move || u.all())),
    ];

    let check = |subs: &mut Vec<Sub>, step: usize| {
        for s in subs.iter_mut() {
            integrate(&mut s.seen, s.q.poll());
            let want: BTreeSet<u64> = (s.find)().find().unwrap().into_iter().collect();
            assert_eq!(s.seen, want, "[{}] step {step}: 積分結果 != find()", s.name);
            assert_eq!(s.q.count(), want.len(), "[{}] step {step}: count", s.name);
        }
    };
    check(&mut subs, 0);

    let mut rng = Rng(0xdead_beef_1234_5678);
    let cities = ["Tokyo", "Osaka", "Kyoto"];
    let mut next_id = 40i64;
    for step in 1..800 {
        let id = rng.below(next_id as u64) as i64;
        let row = users.where_eq("id", id).find_one().unwrap();
        match rng.below(6) {
            0 => {
                users
                    .insert()
                    .set("id", next_id)
                    .set("city", cities[rng.below(3) as usize])
                    .set("age", rng.below(60) as i64)
                    .set("company", Value::Ref(if rng.below(2) == 0 { ant } else { other }))
                    .commit()
                    .unwrap();
                next_id += 1;
            }
            1 => {
                users
                    .upsert()
                    .set("id", id)
                    .set("age", rng.below(60) as i64)
                    .commit()
                    .unwrap();
            }
            2 => {
                if let Some(r) = row {
                    users.entity(r).set("city", cities[rng.below(3) as usize]).commit().unwrap();
                }
            }
            3 => {
                if let Some(r) = row {
                    users
                        .entity(r)
                        .update()
                        .set("company", Value::Ref(if rng.below(2) == 0 { ant } else { other }))
                        .set("age", rng.below(60) as i64)
                        .commit()
                        .unwrap();
                }
            }
            4 => {
                if let Some(r) = row {
                    users.entity(r).delete().unwrap();
                }
            }
            _ => {}
        }
        if step % 5 == 0 {
            check(&mut subs, step);
        }
    }
    check(&mut subs, usize::MAX);
    for s in &subs {
        assert!(!s.seen.is_empty(), "[{}] 一度も当たらない条件は試験になっていない", s.name);
    }
    drop(subs);
    drop(users);
    drop(companies);
    drop(db);
    cleanup(&path);
}

#[test]
fn subscribe_rejects_what_find_silently_empties() {
    let path = tmp_path("reject");
    cleanup(&path);
    let mut db = Database::create_growable_tiny(&path).unwrap();
    db.table("t").number("id").tag("name").primary_key("id").build().unwrap();
    let t = db.get_table("t").unwrap();
    // find() は 0 件を返すだけの書き間違い
    assert!(t.where_eq("nmae", "x").subscribe().is_err(), "未知の列");
    assert!(t.where_eq("id", "x").subscribe().is_err(), "型の合わない値");
    assert!(t.all().limit(3).subscribe().is_err(), "limit");
    drop(t);
    drop(db);
    cleanup(&path);
}
