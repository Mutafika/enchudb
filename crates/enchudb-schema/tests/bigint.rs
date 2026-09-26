//! BigInt 列 (64 bit 整数、 engine の 64 bit 列 1 本に大小の順を保つ符号化で): 書いた値が読め、 条件 (等値 /
//! 範囲 / In / 否定 / NULL / 並び / ref をたどる列) と購読 (group / 合計 / 上位 k 件も) が、 手で数えた結果と
//! 一致すること。 値は境界 (値域の両端、 0 と ±2^31 / ±2^32 の前後)、 負の数、 ms の時刻を混ぜる。

use enchudb_schema::{BIGINT_MAX, BIGINT_MIN, ColumnType, Database, LiveQuery, Value};
use std::collections::BTreeSet;

fn tmp_path(name: &str) -> String {
    format!("/tmp/enchudb-bigint-{}-{}", name, std::process::id())
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    for suffix in ["", ".oplog", ".crc", ".tables", ".schema", ".eidmap", ".vocabmap", ".db.lock"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
    }
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// 境界を多めに混ぜた値。
fn pick(rng: &mut Rng, pool: &[i64]) -> i64 {
    match rng.below(4) {
        0 => pool[rng.below(pool.len() as u64) as usize],
        1 => {
            // 下位の紐の境目 (2^31 の倍数) の前後
            let k = rng.below(64) as i64 - 32;
            k * (1 << 31) + rng.below(3) as i64 - 1
        }
        2 => 1_700_000_000_000 + rng.below(1_000_000) as i64, // ms の時刻
        _ => (rng.next() as i64) >> (rng.below(40) + 1),      // 正負いろいろの大きさ
    }
}

fn pool() -> Vec<i64> {
    vec![BIGINT_MIN, BIGINT_MIN + 1, BIGINT_MAX, BIGINT_MAX - 1, 0, -1, 1, (1 << 31) - 1, 1 << 31, -(1 << 31), -(1 << 31) - 1, u32::MAX as i64]
}

#[test]
fn bigint_values_and_queries_match_oracle() {
    let path = tmp_path("oracle");
    cleanup(&path);
    let mut db = Database::create_growable_tiny(&path).unwrap();
    db.table("companies").number("id").bigint("founded").primary_key("id").build().unwrap();
    db.table("events")
        .number("id")
        .bigint("at")
        .tag("kind")
        .ref_to("company", "companies")
        .primary_key("id")
        .build()
        .unwrap();
    let ev = db.get_table("events").unwrap();
    let co = db.get_table("companies").unwrap();
    assert_eq!(ev.columns().iter().find(|c| c.name == "at").map(|c| c.ty), Some(ColumnType::BigInt));
    // 下位の紐は schema の列に出ない
    assert_eq!(ev.columns().len(), 4);

    let pool = pool();
    let mut rng = Rng(0xb161_7e57_0000_0001);
    let companies: Vec<u64> = (0..6i64)
        .map(|i| {
            let mut b = co.insert().set("id", i);
            if i != 5 {
                b = b.set("founded", pick(&mut rng, &pool));
            }
            b.commit().unwrap()
        })
        .collect();
    let mut rows: Vec<u64> = Vec::new();
    for i in 0..300i64 {
        let mut b = ev.insert().set("id", i).set("kind", if i % 3 == 0 { "a" } else { "b" });
        if i % 7 != 0 {
            b = b.set("at", pick(&mut rng, &pool));
        }
        if i % 4 != 0 {
            b = b.set("company", Value::Ref(companies[rng.below(6) as usize]));
        }
        rows.push(b.commit().unwrap());
    }
    // 値域外は書けない (どちらの半分も書かない)
    let e0 = rows[1];
    let before = ev.entity(e0).get("at");
    assert!(ev.entity(e0).set("at", BIGINT_MAX + 1).commit().is_err());
    assert_eq!(ev.entity(e0).get("at"), before);

    let at = |e: u64| match ev.entity(e).get("at") {
        Some(Value::Number(n)) => Some(n),
        _ => None,
    };
    let founded = |e: u64| -> Option<i64> {
        let Some(Value::Ref(c)) = ev.entity(e).get("company") else { return None };
        match co.entity(c).get("founded") {
            Some(Value::Number(n)) => Some(n),
            _ => None,
        }
    };
    let set = |v: Vec<u64>| -> BTreeSet<u64> { v.into_iter().collect() };
    let want = |f: &dyn Fn(u64) -> bool| -> BTreeSet<u64> { rows.iter().copied().filter(|&e| f(e)).collect() };

    for round in 0..300 {
        // 書き換え (値の入れ替え / 値を外す)
        let e = rows[rng.below(rows.len() as u64) as usize];
        if rng.below(5) == 0 {
            db.engine().untie(e, "events.at");
        } else {
            ev.entity(e).set("at", pick(&mut rng, &pool)).commit().unwrap();
        }
        let a = pick(&mut rng, &pool);
        let b = pick(&mut rng, &pool);
        let (lo, hi) = (a.min(b), a.max(b));
        let who = format!("round {round} a={a} lo={lo} hi={hi}");
        assert_eq!(set(ev.where_eq("at", a).find().unwrap()), want(&|e| at(e) == Some(a)), "{who}: eq");
        assert_eq!(
            set(ev.all().where_range("at", lo, hi).find().unwrap()),
            want(&|e| at(e).is_some_and(|v| lo <= v && v <= hi)),
            "{who}: range"
        );
        assert_eq!(set(ev.all().where_gt("at", a).find().unwrap()), want(&|e| at(e).is_some_and(|v| v > a)), "{who}: gt");
        assert_eq!(set(ev.all().where_ge("at", a).find().unwrap()), want(&|e| at(e).is_some_and(|v| v >= a)), "{who}: ge");
        assert_eq!(set(ev.all().where_lt("at", a).find().unwrap()), want(&|e| at(e).is_some_and(|v| v < a)), "{who}: lt");
        assert_eq!(set(ev.all().where_le("at", a).find().unwrap()), want(&|e| at(e).is_some_and(|v| v <= a)), "{who}: le");
        assert_eq!(set(ev.all().where_ne("at", a).find().unwrap()), want(&|e| at(e).is_some_and(|v| v != a)), "{who}: ne");
        assert_eq!(set(ev.all().where_null("at").find().unwrap()), want(&|e| at(e).is_none()), "{who}: null");
        // 他の条件との AND / OR
        assert_eq!(
            set(ev.where_eq("kind", "a").where_ge("at", a).find().unwrap()),
            want(&|e| ev.entity(e).get("kind") == Some(Value::Text("a".into())) && at(e).is_some_and(|v| v >= a)),
            "{who}: kind = a AND at >= a"
        );
        assert_eq!(
            set(ev.all().where_lt("at", lo).or(ev.all().where_gt("at", hi)).find().unwrap()),
            want(&|e| at(e).is_some_and(|v| v < lo || v > hi)),
            "{who}: at < lo OR at > hi"
        );
        // ref の先の BigInt
        assert_eq!(
            set(ev.all().where_ge("company.founded", a).find().unwrap()),
            want(&|e| founded(e).is_some_and(|v| v >= a)),
            "{who}: company.founded >= a"
        );
        // In (u32 の値) / NOT IN
        let small: Vec<u32> = (0..3).map(|_| rng.below(4) as u32).collect();
        assert_eq!(
            set(ev.all().where_in("at", &small).find().unwrap()),
            want(&|e| at(e).is_some_and(|v| small.iter().any(|&s| s as i64 == v))),
            "{who}: in {small:?}"
        );
        assert_eq!(
            set(ev.all().where_not_in("at", &small[..2]).find().unwrap()),
            want(&|e| at(e).is_some_and(|v| small[..2].iter().all(|&s| s as i64 != v))),
            "{who}: not in {:?}",
            &small[..2]
        );
        // 並び (昇順 / 降順、 同じ値は eid の昇順) + limit
        let mut asc: Vec<(i64, u64)> = rows.iter().filter_map(|&e| at(e).map(|v| (v, e))).collect();
        asc.sort_by_key(|&(v, e)| (v, enchudb_oplog::eid_local(e)));
        let got: Vec<u64> = ev.all().order_by("at").limit(20).find().unwrap();
        assert_eq!(got, asc.iter().take(20).map(|x| x.1).collect::<Vec<_>>(), "{who}: order_by");
        let mut desc = asc.clone();
        desc.sort_by_key(|&(v, e)| (std::cmp::Reverse(v), enchudb_oplog::eid_local(e)));
        let got: Vec<u64> = ev.all().order_by_desc("at").limit(20).find().unwrap();
        assert_eq!(got, desc.iter().take(20).map(|x| x.1).collect::<Vec<_>>(), "{who}: order_by_desc");
    }
    // 値域外の等値は常に 0 件、 範囲は値域で切る
    assert!(ev.where_eq("at", BIGINT_MAX + 1).find().unwrap().is_empty());
    assert_eq!(
        set(ev.all().where_ge("at", i64::MIN).find().unwrap()),
        want(&|e| at(e).is_some()),
        "at >= i64::MIN = 値のある row 全部"
    );
    assert!(ev.all().where_gt("at", i64::MAX).find().unwrap().is_empty());
    drop(ev);
    drop(co);
    drop(db);
    cleanup(&path);
}

#[test]
fn bigint_subscriptions_match_oracle() {
    let path = tmp_path("live");
    cleanup(&path);
    let mut db = Database::create_growable_tiny(&path).unwrap();
    db.table("events").number("id").bigint("at").tag("kind").primary_key("id").build().unwrap();
    let ev = db.get_table("events").unwrap();
    let pool = pool();
    let mut rng = Rng(0x11fe_b161_0000_0003);
    let mut rows: Vec<u64> = (0..120i64)
        .map(|i| {
            let mut b = ev.insert().set("id", i).set("kind", if i % 2 == 0 { "a" } else { "b" });
            if i % 5 != 0 {
                b = b.set("at", pick(&mut rng, &pool));
            }
            b.commit().unwrap()
        })
        .collect();
    let at = |e: u64| match ev.entity(e).get("at") {
        Some(Value::Number(n)) => Some(n),
        _ => None,
    };
    type Cond = Box<dyn Fn(Option<i64>, bool) -> bool>;
    struct Sub {
        name: String,
        q: LiveQuery,
        seen: BTreeSet<u64>,
        cond: Cond,
    }
    let make = |rng: &mut Rng| -> Sub {
        let a = pick(rng, &pool);
        let b = pick(rng, &pool);
        let (lo, hi) = (a.min(b), a.max(b));
        let (name, q, cond): (String, _, Cond) = match rng.below(5) {
            0 => (format!("eq {a}"), ev.where_eq("at", a), Box::new(move |v, _| v == Some(a))),
            1 => (format!("range {lo}..={hi}"), ev.all().where_range("at", lo, hi), Box::new(move |v, _| v.is_some_and(|v| lo <= v && v <= hi))),
            2 => (format!("gt {a} and kind a"), ev.where_eq("kind", "a").where_gt("at", a), Box::new(move |v, k| k && v.is_some_and(|v| v > a))),
            3 => (format!("ne {a}"), ev.all().where_ne("at", a), Box::new(move |v, _| v.is_some_and(|v| v != a))),
            _ => (format!("lt {a} or null"), ev.all().where_lt("at", a).or(ev.all().where_null("at")), Box::new(move |v, _| v.is_none_or(|v| v < a))),
        };
        Sub { name, q: q.subscribe().unwrap(), seen: BTreeSet::new(), cond }
    };
    let mut subs: Vec<Sub> = (0..20).map(|_| make(&mut rng)).collect();
    let mut next_id = 1000i64;
    for step in 0..800 {
        let i = rng.below(rows.len() as u64) as usize;
        match rng.below(6) {
            0 => db.engine().untie(rows[i], "events.at"),
            1 => {
                ev.entity(rows[i]).delete().unwrap();
                rows[i] = ev.insert().set("id", next_id).set("kind", "a").set("at", pick(&mut rng, &pool)).commit().unwrap();
                next_id += 1;
            }
            2 => ev.entity(rows[i]).set("kind", if rng.below(2) == 0 { "a" } else { "b" }).commit().unwrap(),
            _ => ev.entity(rows[i]).set("at", pick(&mut rng, &pool)).commit().unwrap(),
        }
        if step % 40 == 0 {
            let k = rng.below(subs.len() as u64) as usize;
            subs[k] = make(&mut rng);
        }
        if step % 3 == 0 {
            for s in subs.iter_mut() {
                let d = s.q.poll();
                for e in d.removed {
                    assert!(s.seen.remove(&e), "[{}] 持っていない row の removed", s.name);
                }
                for e in d.added {
                    assert!(s.seen.insert(e), "[{}] 持っている row の added", s.name);
                }
                let want: BTreeSet<u64> = rows
                    .iter()
                    .copied()
                    .filter(|&e| (s.cond)(at(e), ev.entity(e).get("kind") == Some(Value::Text("a".into()))))
                    .collect();
                assert_eq!(s.seen, want, "[{}] step {step}: 積分 != 手で数えた結果", s.name);
                assert_eq!(s.q.count(), want.len(), "[{}] step {step}: count", s.name);
            }
        }
    }
    drop(subs);
    drop(ev);
    drop(db);
    cleanup(&path);
}

#[test]
fn bigint_survives_reopen() {
    let path = tmp_path("reopen");
    cleanup(&path);
    let vals = [BIGINT_MIN, -1, 0, 1_700_000_000_123, BIGINT_MAX];
    {
        let mut db = Database::create_growable_tiny(&path).unwrap();
        let t = db.table("t").number("id").bigint("v").primary_key("id").build().unwrap();
        for (i, v) in vals.iter().enumerate() {
            t.insert().set("id", i as i64).set("v", *v).commit().unwrap();
        }
    }
    {
        let db = Database::open(&path).unwrap();
        let t = db.get_table("t").unwrap();
        for (i, v) in vals.iter().enumerate() {
            let e = t.where_eq("id", i as i64).find_one().unwrap().unwrap();
            assert_eq!(t.entity(e).get("v"), Some(Value::Number(*v)));
            assert_eq!(t.where_eq("v", *v).find().unwrap(), vec![e]);
        }
        assert_eq!(t.all().where_lt("v", 0).count().unwrap(), 2);
    }
    // 後から足した BigInt 列も reopen で戻る
    {
        let mut db = Database::open(&path).unwrap();
        db.add_column("t", "w", ColumnType::BigInt).unwrap();
        let t = db.get_table("t").unwrap();
        let e = t.where_eq("id", 0i64).find_one().unwrap().unwrap();
        t.entity(e).set("w", -5i64).commit().unwrap();
    }
    {
        let db = Database::open(&path).unwrap();
        let t = db.get_table("t").unwrap();
        let e = t.where_eq("id", 0i64).find_one().unwrap().unwrap();
        assert_eq!(t.entity(e).get("w"), Some(Value::Number(-5)));
        assert_eq!(t.all().where_lt("w", 0).find().unwrap(), vec![e]);
    }
    cleanup(&path);
}

#[test]
fn bigint_keys_aggregates_and_rejections() {
    let path = tmp_path("keys");
    cleanup(&path);
    let mut db = Database::create_growable_tiny(&path).unwrap();
    // BigInt の主キー (upsert は主キーで同じ row を書き換える)
    db.table("p").bigint("id").tag("name").primary_key("id").build().unwrap();
    db.table("t").number("id").bigint("v").number("n").primary_key("id").build().unwrap();
    let p = db.get_table("p").unwrap();
    let a = p.upsert().set("id", -(1i64 << 40)).set("name", "a").commit().unwrap();
    let b = p.upsert().set("id", 1i64 << 40).set("name", "b").commit().unwrap();
    assert_ne!(a, b);
    assert_eq!(p.upsert().set("id", -(1i64 << 40)).set("name", "a2").commit().unwrap(), a, "同じ主キーは同じ row");
    assert_eq!(p.entity(a).get("name"), Some(Value::Text("a2".into())));
    assert_eq!(p.where_eq("id", 1i64 << 40).find_one().unwrap(), Some(b));
    let t = db.get_table("t").unwrap();
    let vals = [-5i64, 7, -(1 << 50), 1 << 45];
    for (i, v) in vals.iter().enumerate() {
        t.insert().set("id", i as i64).set("v", *v).set("n", 2i64).commit().unwrap();
    }
    t.insert().set("id", 99i64).set("n", 2i64).commit().unwrap(); // v の無い row
    // 64 bit の集計 (u32 の集計は BadValue)
    assert!(t.all().sum("v").is_err());
    assert!(t.all().max("v").is_err());
    assert_eq!(t.all().sum_i128("v").unwrap(), vals.iter().map(|&v| v as i128).sum::<i128>());
    assert_eq!(t.all().min_i64("v").unwrap(), Some(-(1 << 50)));
    assert_eq!(t.all().max_i64("v").unwrap(), Some(1 << 45));
    assert_eq!(t.all().sum_i128("n").unwrap(), 10);
    assert_eq!(t.all().count_col("v").unwrap(), 4);
    // group / 合計 / 並びの購読
    let by_v = t.all().subscribe_counts("v").unwrap();
    assert_eq!(by_v.get(&Value::Number(-(1 << 50))), 1);
    let sums = t.all().subscribe_sums("n", "v").unwrap();
    assert_eq!(sums.get_sum(&Value::Number(2)), vals.iter().map(|&v| v as i128).sum::<i128>(), "値の無い row は足さない");
    let top = t.all().order_by("v").limit(2).subscribe().unwrap();
    let want: Vec<u64> = [2i64, 0].iter().map(|&i| t.where_eq("id", i).find_one().unwrap().unwrap()).collect();
    assert_eq!(top.ranked(), want);
    // find の並びも
    assert_eq!(t.all().order_by_desc("v").limit(1).find().unwrap(), vec![t.where_eq("id", 3i64).find_one().unwrap().unwrap()]);
    drop((p, t, by_v, sums, top));
    drop(db);
    cleanup(&path);
}

/// BigInt で group の件数・合計 (負の数を含む)・上位 k 件 (昇順 / 降順) を購読し、 書き換え / 外す / 削除の中で
/// 手で数えた結果と一致する。
#[test]
fn bigint_live_aggregates_match_oracle() {
    use std::collections::BTreeMap;
    let path = tmp_path("liveagg");
    cleanup(&path);
    let mut db = Database::create_growable_tiny(&path).unwrap();
    db.table("events").number("id").bigint("at").tag("kind").bigint("amount").primary_key("id").build().unwrap();
    let ev = db.get_table("events").unwrap();
    let pool = pool();
    let mut rng = Rng(0xa99_b161_0000_0005);
    let small = |rng: &mut Rng| rng.below(7) as i64 - 3; // group 用の少数の値 (負の数も)
    let mut rows: Vec<u64> = (0..150i64)
        .map(|i| {
            let mut b = ev.insert().set("id", i).set("kind", if i % 2 == 0 { "a" } else { "b" }).set("at", small(&mut rng));
            if i % 4 != 0 {
                b = b.set("amount", pick(&mut rng, &pool));
            }
            b.commit().unwrap()
        })
        .collect();
    let by_at = ev.all().subscribe_counts("at").unwrap();
    let sums = ev.all().subscribe_sums("kind", "amount").unwrap();
    let top_asc = ev.all().order_by("amount").limit(6).subscribe().unwrap();
    let top_desc = ev.where_eq("kind", "a").order_by_desc("amount").limit(6).subscribe().unwrap();
    let mut groups: BTreeMap<i64, u64> = BTreeMap::new();
    let mut sum_seen: BTreeMap<String, (u64, i128)> = BTreeMap::new();
    let num = |v: Option<Value>| match v {
        Some(Value::Number(n)) => Some(n),
        _ => None,
    };
    let mut next_id = 1000i64;
    for step in 0..900 {
        let i = rng.below(rows.len() as u64) as usize;
        match rng.below(7) {
            0 => db.engine().untie(rows[i], "events.amount"),
            1 => ev.entity(rows[i]).set("at", small(&mut rng)).commit().unwrap(),
            2 => ev.entity(rows[i]).set("kind", if rng.below(2) == 0 { "a" } else { "b" }).commit().unwrap(),
            3 => {
                ev.entity(rows[i]).delete().unwrap();
                rows[i] = ev.insert().set("id", next_id).set("kind", "a").set("at", small(&mut rng)).set("amount", pick(&mut rng, &pool)).commit().unwrap();
                next_id += 1;
            }
            _ => ev.entity(rows[i]).set("amount", pick(&mut rng, &pool)).commit().unwrap(),
        }
        if step % 30 != 0 {
            continue;
        }
        for (v, n) in by_at.poll() {
            let Value::Number(v) = v else { panic!("group の値が数でない") };
            if n == 0 { groups.remove(&v); } else { groups.insert(v, n); }
        }
        let mut want: BTreeMap<i64, u64> = BTreeMap::new();
        for &e in &rows {
            if let Some(v) = num(ev.entity(e).get("at")) {
                *want.entry(v).or_default() += 1;
            }
        }
        assert_eq!(groups, want, "step {step}: BigInt で group の件数");
        for (k, n, t) in sums.poll_sums() {
            let Value::Text(k) = k else { panic!() };
            if n == 0 { sum_seen.remove(&k); } else { sum_seen.insert(k, (n, t)); }
        }
        let mut want: BTreeMap<String, (u64, i128)> = BTreeMap::new();
        for &e in &rows {
            if let Some(Value::Text(k)) = ev.entity(e).get("kind") {
                let w = want.entry(k).or_default();
                w.0 += 1;
                w.1 += num(ev.entity(e).get("amount")).map_or(0, |v| v as i128);
            }
        }
        assert_eq!(sum_seen, want, "step {step}: BigInt の合計 (負の数・値の無い row)");
        let top = |keep: &dyn Fn(u64) -> bool, desc: bool| -> Vec<u64> {
            let mut v: Vec<(i128, u64, u64)> = rows
                .iter()
                .copied()
                .filter(|&e| keep(e))
                .filter_map(|e| num(ev.entity(e).get("amount")).map(|a| (if desc { -(a as i128) } else { a as i128 }, enchudb_oplog::eid_local(e) as u64, e)))
                .collect();
            v.sort_unstable();
            v.into_iter().take(6).map(|x| x.2).collect()
        };
        assert_eq!(top_asc.ranked(), top(&|_| true, false), "step {step}: 上位 k 件 昇順");
        assert_eq!(top_desc.ranked(), top(&|e| ev.entity(e).get("kind") == Some(Value::Text("a".into())), true), "step {step}: 上位 k 件 降順");
    }
    drop((by_at, sums, top_asc, top_desc, ev));
    drop(db);
    cleanup(&path);
}
