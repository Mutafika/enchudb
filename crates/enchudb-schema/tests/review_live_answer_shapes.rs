//! review: 宣言された答えを色々な形 (Not / Or / Via / Exists / ExistsEq / CountAtLeast / SumAtLeast / In / Top の ref の先 /
//! 集計) で宣言し、 開き直した後に (1) poll の前の読み (count / members / contains / ranked / all) (2) 最初の poll の種
//! (3) 書き込みの後の差分 が、 同じ条件の find と合うかを見る。 handle の clone 2 つ、 置き換えた後の古い handle も。

use enchudb_schema::{Answer, Database, LiveQuery, Query, Value};
use std::collections::BTreeSet;

const SCRATCH: &str = "/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/eedfc333-d87a-4a47-a3b0-daf4b6d7337a/scratchpad";

fn tmp_path(name: &str) -> String {
    let p = format!("{SCRATCH}/rl-shapes-{}-{}", name, std::process::id());
    cleanup(&p);
    p
}
fn cleanup(path: &str) {
    let _ = enchudb_engine::db_files::remove_db(path);
    let _ = std::fs::remove_dir_all(path);
}

fn build(db: &mut Database) {
    db.table("companies").number("id").tag("city").number("revenue").primary_key("id").build().unwrap();
    db.table("shops").number("id").tag("city").number("open").primary_key("id").build().unwrap();
    db.table("users")
        .number("id")
        .number("age")
        .tag("city")
        .bigint("score")
        .ref_to("company", "companies")
        .primary_key("id")
        .build()
        .unwrap();
}

const CITIES: [&str; 4] = ["Tokyo", "Osaka", "Nagoya", "Kyoto"];

fn fill(db: &Database) {
    let companies = db.get_table("companies").unwrap();
    let cs: Vec<u64> = (0..6i64)
        .map(|i| companies.insert().set("id", i).set("city", CITIES[(i % 4) as usize]).set("revenue", (i * 37) % 11).commit().unwrap())
        .collect();
    let shops = db.get_table("shops").unwrap();
    for i in 0..8i64 {
        shops.insert().set("id", i).set("city", CITIES[(i % 3) as usize]).set("open", i % 2).commit().unwrap();
    }
    let users = db.get_table("users").unwrap();
    for i in 0..60i64 {
        let mut b = users.insert().set("id", i).set("age", 10 + (i * 7) % 40).set("score", (i * 13) % 17 - 8);
        if i % 5 != 0 {
            b = b.set("city", CITIES[(i % 4) as usize]);
        }
        if i % 7 != 0 {
            b = b.set("company", Value::Ref(cs[(i % 6) as usize]));
        }
        b.commit().unwrap();
    }
}

/// 形の一覧 (名前, 同じ条件の query を作る関数)。
fn shapes<'a>(db: &'a Database) -> Vec<(&'static str, Query<'a>)> {
    let u = db.get_table("users").unwrap();
    let c = db.get_table("companies").unwrap();
    let s = db.get_table("shops").unwrap();
    vec![
        ("ne", u.all().where_ne("city", "Tokyo")),
        ("not_in", u.all().where_not_in("age", &[10, 17, 24])),
        ("null", u.all().where_null("city")),
        ("via", u.where_eq("company.city", "Tokyo")),
        ("via_ne", u.all().where_ne("company.city", "Tokyo")),
        ("or", u.where_eq("city", "Tokyo").or(u.all().where_gt("age", 40i64)).where_le("age", 45i64)),
        ("in", u.where_in("age", &[10, 17, 24, 31])),
        ("range_big", u.all().where_range("score", -3i64, 3i64)),
        ("exists", c.all().where_exists(u.all().where_gt("age", 30i64), "company")),
        ("not_exists", c.all().where_not_exists(u.all().where_gt("age", 44i64), "company")),
        ("exists_eq", u.all().where_exists_eq("city", s.where_eq("open", 1i64), "city")),
        ("not_exists_eq", u.all().where_not_exists_eq("city", s.where_eq("open", 1i64), "city")),
        ("count_ge", c.all().where_count_ge(u.all().where_gt("age", 20i64), "company", 6)),
        ("count_lt", c.all().where_count_lt(u.all().where_gt("age", 20i64), "company", 6)),
        ("value_count_ge", u.all().where_value_count_ge("city", s.where_eq("open", 1i64), "city", 2)),
        ("sum_ge", c.all().where_sum_ge(u.all().where_gt("age", 15i64), "company", "score", 0)),
        ("sum_lt", c.all().where_sum_lt(u.all().where_gt("age", 15i64), "company", "score", 0)),
        ("via_exists", u.all().where_exists_eq("company.city", s.where_eq("open", 1i64), "city")),
    ]
}

fn set(v: Vec<u64>) -> BTreeSet<u64> {
    let n = v.len();
    let s: BTreeSet<u64> = v.into_iter().collect();
    assert_eq!(s.len(), n, "重複した eid");
    s
}

fn churn(db: &Database, round: i64) {
    let users = db.get_table("users").unwrap();
    let companies = db.get_table("companies").unwrap();
    let shops = db.get_table("shops").unwrap();
    for i in 0..60i64 {
        if (i + round) % 3 == 0 {
            users.upsert().set("id", i).set("age", 10 + (i * 11 + round * 5) % 40).set("score", (i * 5 + round) % 17 - 8).commit().unwrap();
        }
        if (i + round) % 11 == 0 {
            if let Some(e) = users.where_eq("id", i).find_one().unwrap() {
                users.entity(e).delete().unwrap();
            }
        }
        if (i + round) % 13 == 0 && users.where_eq("id", i).find_one().unwrap().is_none() {
            let c = companies.where_eq("id", (i + round) % 6).find_one().unwrap().unwrap();
            users.insert().set("id", i).set("age", 33i64).set("city", CITIES[((i + round) % 4) as usize]).set("company", Value::Ref(c)).set("score", 2i64).commit().unwrap();
        }
    }
    for i in 0..6i64 {
        if (i + round) % 2 == 0 {
            companies.upsert().set("id", i).set("city", CITIES[((i + round) % 4) as usize]).set("revenue", (i * 3 + round) % 11).commit().unwrap();
        }
    }
    for i in 0..8i64 {
        shops.upsert().set("id", i).set("open", (i + round) % 2).set("city", CITIES[((i * round) % 4) as usize]).commit().unwrap();
    }
}

#[test]
fn every_shape_survives_reopen_and_matches_find() {
    let path = tmp_path("all");
    {
        let mut db = Database::create(&path).unwrap();
        build(&mut db);
        fill(&db);
        for (name, q) in shapes(&db) {
            q.answer(name).unwrap_or_else(|e| panic!("{name}: 宣言できない: {e:?}"));
        }
        let u = db.get_table("users").unwrap();
        u.all().where_gt("age", 12i64).order_by_desc("company.revenue").limit(7).answer("top_via").unwrap();
        u.all().answer_counts("cnt_via", "company.city").unwrap();
        u.all().where_gt("age", 12i64).answer_sums("sum_city", "city", "score").unwrap();
        assert_eq!(db.answers().len(), shapes(&db).len() + 3);
    }
    for round in 0..4i64 {
        let db = Database::open(&path).unwrap();
        assert_eq!(db.answers().len(), shapes(&db).len() + 3, "round {round}: 開き直しで宣言が減った: {:?}", db.answers());
        // 奇数の round は、 最初の poll より前に書く (種と書き込みの印が重なる)
        if round % 2 == 1 {
            churn(&db, round);
        }
        let mut lives: Vec<(&str, LiveQuery, BTreeSet<u64>)> = Vec::new();
        for (name, q) in shapes(&db) {
            let want = set(q.find().unwrap());
            let a = db.answer(name).and_then(Answer::into_query).unwrap_or_else(|| panic!("{name} が無い"));
            // (1) poll の前の読み
            assert_eq!(a.count(), want.len(), "round {round} {name}: poll の前の count");
            assert_eq!(set(a.members()), want, "round {round} {name}: poll の前の members");
            for &e in want.iter().take(3) {
                assert!(a.contains(e), "round {round} {name}: poll の前の contains({e:#x})");
            }
            // (2) 種: handle の clone 2 つで poll (差分はどちらか一方にだけ)
            let b = db.answer(name).and_then(Answer::into_query).unwrap();
            let d1 = a.poll();
            let d2 = b.poll();
            assert!(d1.removed.is_empty() && d2.is_empty(), "round {round} {name}: 種の poll: {d1:?} / 2 つ目 {d2:?}");
            assert_eq!(set(d1.added), want, "round {round} {name}: 最初の poll の種");
            lives.push((name, a, want));
        }
        let u = db.get_table("users").unwrap();
        let top = db.answer("top_via").and_then(Answer::into_query).unwrap();
        let top_find = |db: &Database| -> Vec<i128> {
            let u = db.get_table("users").unwrap();
            let c = db.get_table("companies").unwrap();
            let _ = &c;
            u.all().where_gt("age", 12i64)
                .order_by_desc("company.revenue")
                .limit(7)
                .find()
                .unwrap()
                .into_iter()
                .map(|e| match u.entity(e).get("company") {
                    Some(Value::Ref(c)) => match db.get_table("companies").unwrap().entity(c).get("revenue") {
                        Some(Value::Number(n)) => n as i128,
                        o => panic!("revenue {o:?}"),
                    },
                    o => panic!("company {o:?}"),
                })
                .collect()
        };
        let rev_of = |db: &Database, es: &[u64]| -> Vec<i128> {
            let u = db.get_table("users").unwrap();
            es.iter()
                .map(|&e| match u.entity(e).get("company") {
                    Some(Value::Ref(c)) => match db.get_table("companies").unwrap().entity(c).get("revenue") {
                        Some(Value::Number(n)) => n as i128,
                        o => panic!("revenue {o:?}"),
                    },
                    o => panic!("company {o:?}"),
                })
                .collect()
        };
        assert_eq!(rev_of(&db, &top.ranked()), top_find(&db), "round {round}: poll の前の ranked (値の並び)");
        let mut top_set = set(top.poll().added);
        assert_eq!(top_set, set(top.ranked()), "round {round}: top の種");
        let cnt = db.answer("cnt_via").and_then(Answer::into_counts).unwrap();
        let sum = db.answer("sum_city").and_then(Answer::into_counts).unwrap();
        let fresh_counts = |db: &Database| {
            let u = db.get_table("users").unwrap();
            let a = u.all().subscribe_counts("company.city").unwrap();
            let b = u.all().where_gt("age", 12i64).subscribe_sums("city", "score").unwrap();
            let f = |mut v: Vec<(Value, u64, i128)>| {
                v.sort_by_key(|x| format!("{:?}", x.0));
                v.into_iter().map(|x| (format!("{:?}", x.0), x.1, x.2)).collect::<Vec<_>>()
            };
            (f(a.all_sums()), f(b.all_sums()))
        };
        let norm = |mut v: Vec<(Value, u64, i128)>| {
            v.sort_by_key(|x| format!("{:?}", x.0));
            v.into_iter().map(|x| (format!("{:?}", x.0), x.1, x.2)).collect::<Vec<_>>()
        };
        let (wc, ws) = fresh_counts(&db);
        assert_eq!(norm(cnt.all_sums()), wc, "round {round}: poll の前の集計 (ref の先の group)");
        assert_eq!(norm(sum.all_sums()), ws, "round {round}: poll の前の合計");
        assert_eq!(norm(cnt.poll_sums()), wc, "round {round}: 集計の種");
        assert_eq!(norm(sum.poll_sums()), ws, "round {round}: 合計の種");
        // (3) 書き込みの後の差分を積む
        for step in 0..3 {
            churn(&db, round * 10 + step);
            let now = shapes(&db);
            for ((name, a, have), (_, q)) in lives.iter_mut().zip(now) {
                let d = a.poll();
                for e in &d.removed {
                    assert!(have.remove(e), "round {round} step {step} {name}: 居ない eid の removed {e:#x}");
                }
                for e in &d.added {
                    assert!(have.insert(*e), "round {round} step {step} {name}: 居る eid の added {e:#x}");
                }
                let want = set(q.find().unwrap());
                assert_eq!(*have, want, "round {round} step {step} {name}: 積んだ差分と find");
                assert_eq!(a.count(), want.len(), "round {round} step {step} {name}: count");
            }
            let d = top.poll();
            for e in &d.removed {
                assert!(top_set.remove(e), "round {round} step {step} top: 居ない eid の removed");
            }
            for e in &d.added {
                assert!(top_set.insert(*e), "round {round} step {step} top: 居る eid の added");
            }
            assert_eq!(top_set, set(top.ranked()), "round {round} step {step}: top の差分と ranked");
            assert_eq!(rev_of(&db, &top.ranked()), top_find(&db), "round {round} step {step}: ranked と find (値の並び)");
            let (wc, ws) = fresh_counts(&db);
            let _ = cnt.poll();
            let _ = sum.poll_sums();
            assert_eq!(norm(cnt.all_sums()), wc, "round {round} step {step}: 集計");
            assert_eq!(norm(sum.all_sums()), ws, "round {round} step {step}: 合計");
        }
        let _ = u;
    }
    cleanup(&path);
}

/// 同じ名前を違う形で置き換えた時: 古い handle は古い形のまま動き続け、 名前で取り直した handle は新しい形。 開き直すと新しい形。
#[test]
fn replacing_a_name_keeps_the_old_handle_on_the_old_shape() {
    let path = tmp_path("replace");
    {
        let mut db = Database::create(&path).unwrap();
        build(&mut db);
        fill(&db);
        let u = db.get_table("users").unwrap();
        let old = u.all().where_gt("age", 30i64).answer("x").unwrap();
        let _ = old.poll();
        let new = u.all().where_le("age", 30i64).answer("x").unwrap();
        let _ = new.poll();
        churn(&db, 1);
        let want_old = set(u.all().where_gt("age", 30i64).find().unwrap());
        let want_new = set(u.all().where_le("age", 30i64).find().unwrap());
        let _ = old.poll();
        let _ = new.poll();
        assert_eq!(set(old.members()), want_old, "古い handle");
        assert_eq!(set(new.members()), want_new, "新しい handle");
        let again = db.answer("x").and_then(Answer::into_query).unwrap();
        assert_eq!(set(again.members()), want_new, "名前で取り直した handle は新しい形");
        assert_eq!(db.answers(), vec!["x".to_string()]);
        // 集合の名前を集計で置き換える
        let c = u.all().answer_counts("x", "city").unwrap();
        assert!(db.answer("x").and_then(Answer::into_query).is_none(), "集計で置き換えた後に集合として取れる");
        let _ = c.poll();
    }
    {
        let db = Database::open(&path).unwrap();
        assert!(db.answer("x").and_then(Answer::into_counts).is_some(), "開き直した後は最後の形 (集計)");
    }
    cleanup(&path);
}

/// 集計の答えを名前で取り直すたびに poll する (`db.answer(name)` は呼ぶたびに新しい wrapper): group が消えた報告 (件数 0) が
/// どの handle にも届かない。 engine の購読の状態は 1 つで、 wrapper ごとの 「最後に渡した値」 (`LiveCounts::last`) は空から始まる。
#[test]
fn a_counts_answer_taken_by_name_again_loses_the_group_removal() {
    let path = tmp_path("refetch");
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").tag("city").primary_key("id").build().unwrap();
    let t = db.get_table("t").unwrap();
    let rows: Vec<u64> = (0..6i64).map(|i| t.insert().set("id", i).set("city", ["Tokyo", "Osaka"][(i % 2) as usize]).commit().unwrap()).collect();
    let first = t.all().answer_counts("per_city", "city").unwrap();
    let seed = first.poll();
    eprintln!("種: {seed:?}");
    assert_eq!(seed.len(), 2);
    // Osaka の行を全部消す
    for (i, e) in rows.iter().enumerate() {
        if i % 2 == 1 {
            t.entity(*e).delete().unwrap();
        }
    }
    // 名前で取り直した handle で poll (開き直した後に doc が勧める取り方と同じ)
    let again = db.answer("per_city").and_then(Answer::into_counts).unwrap();
    let d = again.poll();
    eprintln!("取り直した handle の poll: {d:?}");
    let d1 = first.poll();
    eprintln!("最初の handle の poll: {d1:?}");
    eprintln!("今の全 group: {:?}", again.all());
    let got_removal = d.iter().chain(&d1).any(|(v, n)| matches!(v, Value::Text(s) if s == "Osaka") && *n == 0);
    drop((first, again, t));
    drop(db);
    cleanup(&path);
    assert!(got_removal, "Osaka が消えた報告 (件数 0) がどちらの handle にも届かなかった: 取り直した handle {d:?} / 最初の handle {d1:?}");
}

/// 対照: 同じ handle で poll し続ければ届く。
#[test]
fn a_counts_answer_polled_through_one_handle_reports_the_removal() {
    let path = tmp_path("onehandle");
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").tag("city").primary_key("id").build().unwrap();
    let t = db.get_table("t").unwrap();
    let rows: Vec<u64> = (0..6i64).map(|i| t.insert().set("id", i).set("city", ["Tokyo", "Osaka"][(i % 2) as usize]).commit().unwrap()).collect();
    let first = t.all().answer_counts("per_city", "city").unwrap();
    assert_eq!(first.poll().len(), 2);
    for (i, e) in rows.iter().enumerate() {
        if i % 2 == 1 {
            t.entity(*e).delete().unwrap();
        }
    }
    let d = first.poll();
    drop((first, t));
    drop(db);
    cleanup(&path);
    assert!(d.iter().any(|(v, n)| matches!(v, Value::Text(s) if s == "Osaka") && *n == 0), "{d:?}");
}
