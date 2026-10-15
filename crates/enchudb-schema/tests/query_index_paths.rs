//! `Query` の索引の道 (2026-10-14):
//!
//! - `count()`: 条件が 1 つの列の等値 / IN / 範囲だけなら、 値ごとの bucket の live の数を足すだけ (entity を読まない)
//! - `find()`: 範囲は範囲の索引を引き、 条件が 2 種類以上なら索引の数が一番少ない条件を種にする
//! - `order_by` + `limit`: 並びの列を値の順にたどって limit 件で止める (全部を引いて並べない)
//! - `min` / `max` / `min_i64` / `max_i64` / `Table::min` / `Table::max`: 値の順にたどった最初の row の値
//!
//! を、 手元の写し (書いた値を全部覚えたもの) をなめた答え (別の道) と比べる。 書き換え・消し・主キーの無い row・
//! BigInt の負の数と大きな数 (dense と sparse の両方)・Tag の列を混ぜる。
//!
//! 何が row か: 条件がある時は 「条件に合う entity」 (等値・IN・範囲で同じ。 主キーの無い row も入る)、 条件の無い
//! `.all()` は代表列 (主キー) のある row だけ (`Table::recent` の doc と同じ)。 `Table::min` / `max` は table の eid の
//! 枠の中の値のある row 全部。

use enchudb_schema::Database;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-query-index-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path); // v10: DB は directory
    for suffix in ["", ".oplog", ".crc", ".db.lock", ".tables", ".tables.tmp", ".schema"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
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

const CITIES: [&str; 5] = ["tokyo", "osaka", "nagoya", "sapporo", "fukuoka"];

/// 手元の写しの 1 row。
#[derive(Clone, Default)]
struct Row {
    eid: u64,
    alive: bool,
    id: Option<i64>,
    age: Option<u32>,
    score: Option<u32>,
    grp: Option<u32>,
    ts: Option<i64>,
    city: Option<&'static str>,
}

fn ts_of(r: &mut Rng) -> i64 {
    // 0 の近く (BigInt の符号化で MID の上下 = dense) と、 遠い正負 (sparse)
    match r.below(3) {
        0 => r.below(101) as i64 - 50,
        1 => (1i64 << 30) + r.below(1 << 30) as i64,
        _ => -(1i64 << 30) - r.below(1 << 30) as i64,
    }
}

fn local(e: u64) -> u32 {
    e as u32
}

fn sorted(mut v: Vec<u64>) -> Vec<u64> {
    v.sort_unstable();
    v
}

/// 写しの row を (key, eid の下位) の昇順に並べて先頭 `k` 件の eid。
fn top(rows: &[Row], k: usize, key: impl Fn(&Row) -> Option<i128>) -> Vec<u64> {
    let mut v: Vec<((i128, u32), u64)> = rows.iter().filter(|r| r.alive).filter_map(|r| key(r).map(|x| ((x, local(r.eid)), r.eid))).collect();
    v.sort_unstable();
    v.into_iter().take(k).map(|x| x.1).collect()
}

/// order_by の答えを、 同じ値の中の順を問わずに写しと比べる (同じ値の中は 「置いてある順」): 並びの値の列が写しの先頭 k 件と
/// 同じ・eid に重複が無い・どれも生きていて key のある row。
fn assert_top(rows: &[Row], got: Vec<u64>, k: usize, key: impl Fn(&Row) -> Option<i128>, msg: &str) {
    let want = top(rows, k, &key);
    let by_eid: std::collections::HashMap<u64, &Row> = rows.iter().filter(|r| r.alive).map(|r| (r.eid, r)).collect();
    let keys = |v: &[u64]| -> Vec<Option<i128>> { v.iter().map(|e| by_eid.get(e).and_then(|r| key(r))).collect() };
    let (kg, kw) = (keys(&got), keys(&want));
    let mut u = got.clone();
    u.sort_unstable();
    u.dedup();
    assert!(kg.iter().all(Option::is_some) && kg == kw && u.len() == got.len(), "{msg}: got {got:?} want {want:?}");
}

#[test]
fn index_paths_match_mirror() {
    let path = tmp_path("mirror");
    cleanup(&path);
    let mut db = Database::create(&path).unwrap();
    db.table("p")
        .number("id")
        .number("age")
        .number("score")
        .number("grp")
        .bigint("ts")
        .tag("city")
        .primary_key("id")
        .with_capacity(5_000)
        .build()
        .unwrap();
    let t = db.get_table("p").unwrap();
    let mut rng = Rng(0x2545_F491_4F6C_DD1D);
    let n = 3_000usize;
    let mut rows: Vec<Row> = Vec::with_capacity(n);
    for i in 0..n {
        let mut row = Row { alive: true, ..Default::default() };
        let mut b = t.insert();
        // 17 行に 1 行は主キーなし、 13 行に 1 行は age なし
        if i % 17 != 3 {
            row.id = Some(i as i64);
            b = b.set("id", i as i64);
        }
        if i % 13 != 5 {
            let a = rng.below(100) as u32;
            row.age = Some(a);
            b = b.set("age", a as i64);
        }
        let (s, g, ts, c) = (rng.below(10_000) as u32, rng.below(40) as u32, ts_of(&mut rng), CITIES[rng.below(5) as usize]);
        row.score = Some(s);
        row.grp = Some(g);
        row.ts = Some(ts);
        row.city = Some(c);
        row.eid = b.set("score", s as i64).set("grp", g as i64).set("ts", ts).set("city", c).commit().unwrap();
        rows.push(row);
    }

    let check = |rows: &[Row], stage: &str, rng: &mut Rng| {
        let alive = || rows.iter().filter(|r| r.alive);
        let ids = |f: &dyn Fn(&Row) -> bool| -> Vec<u64> { sorted(alive().filter(|r| f(r)).map(|r| r.eid).collect()) };
        let inr = |v: Option<u32>, a: u32, b: u32| v.is_some_and(|v| a <= v && v <= b);
        for _ in 0..12 {
            let g = rng.below(40) as u32;
            let a = rng.below(100) as u32;
            let b = a + rng.below(30) as u32;
            let sa = rng.below(10_000) as u32;
            let sb = sa + rng.below(2_000) as u32;
            let city = CITIES[rng.below(5) as usize];
            // count: 1 つの列 (索引の数だけ)
            assert_eq!(t.where_eq("grp", g as i64).count().unwrap(), alive().filter(|r| r.grp == Some(g)).count(), "{stage} c_eq");
            let gs: Vec<u32> = (g..g + 4).collect();
            assert_eq!(t.where_in("grp", &gs).count().unwrap(), alive().filter(|r| r.grp.is_some_and(|x| gs.contains(&x))).count(), "{stage} c_in");
            assert_eq!(t.where_range("age", a as i64, b as i64).count().unwrap(), alive().filter(|r| inr(r.age, a, b)).count(), "{stage} c_rng");
            assert_eq!(
                t.all().where_ge("age", a as i64).where_lt("age", b as i64 + 1).count().unwrap(),
                alive().filter(|r| inr(r.age, a, b)).count(),
                "{stage} c_ge_lt (同じ列の範囲 2 つ)"
            );
            assert_eq!(t.where_eq("city", city).count().unwrap(), alive().filter(|r| r.city == Some(city)).count(), "{stage} c_tag");
            let (tl, th) = (-(1i64 << 31), 40);
            assert_eq!(
                t.where_range("ts", tl, th).count().unwrap(),
                alive().filter(|r| r.ts.is_some_and(|x| tl <= x && x <= th)).count(),
                "{stage} c_rng_bigint"
            );
            // count: 2 つの列 (find の道)、 limit
            assert_eq!(
                t.where_eq("grp", g as i64).where_range("age", a as i64, b as i64).count().unwrap(),
                alive().filter(|r| r.grp == Some(g) && inr(r.age, a, b)).count(),
                "{stage} c_eq_rng"
            );
            let all_rng = alive().filter(|r| inr(r.age, 0, 99)).count();
            assert_eq!(t.where_range("age", 0, 99).limit(5).count().unwrap(), all_rng.min(5), "{stage} c_limit");
            // find (集合)
            assert_eq!(sorted(t.where_range("age", a as i64, b as i64).find().unwrap()), ids(&|r| inr(r.age, a, b)), "{stage} f_rng");
            assert_eq!(
                sorted(t.where_eq("grp", g as i64).where_range("score", sa as i64, sb as i64).find().unwrap()),
                ids(&|r| r.grp == Some(g) && inr(r.score, sa, sb)),
                "{stage} f_eq_rng"
            );
            assert_eq!(
                sorted(t.where_eq("age", a as i64).where_range("score", sa as i64, sa as i64 + 300).find().unwrap()),
                ids(&|r| r.age == Some(a) && inr(r.score, sa, sa + 300)),
                "{stage} f_rng_small"
            );
            assert_eq!(
                sorted(t.where_in("grp", &gs).where_range("age", a as i64, b as i64).where_range("score", sa as i64, sb as i64).find().unwrap()),
                ids(&|r| r.grp.is_some_and(|x| gs.contains(&x)) && inr(r.age, a, b) && inr(r.score, sa, sb)),
                "{stage} f_in_rng_rng"
            );
            assert_eq!(
                sorted(t.where_eq("city", city).where_range("age", a as i64, b as i64).find().unwrap()),
                ids(&|r| r.city == Some(city) && inr(r.age, a, b)),
                "{stage} f_tag_rng"
            );
            // order_by + limit (並び)
            let k = 1 + rng.below(20) as usize;
            assert_top(rows, t.where_range("age", a as i64, b as i64).order_by("age").limit(k).find().unwrap(), k, |r| inr(r.age, a, b).then(|| r.age.unwrap() as i128), &format!("{stage} o_rng_same_col"));
            assert_top(rows, t.where_eq("city", city).order_by("age").limit(k).find().unwrap(), k, |r| (r.city == Some(city)).then_some(r.age).flatten().map(i128::from), &format!("{stage} o_tag"));
            assert_top(rows, t.where_range("score", 0, sb as i64).order_by_desc("age").limit(k).find().unwrap(), k, |r| inr(r.score, 0, sb).then_some(r.age).flatten().map(|x| -(x as i128)), &format!("{stage} o_rng_other_col_desc"));
            assert_top(rows, t.where_eq("grp", g as i64).order_by("score").limit(k).find().unwrap(), k, |r| (r.grp == Some(g)).then_some(r.score).flatten().map(i128::from), &format!("{stage} o_eq_small (引いて並べる道)"));
            // 最小 / 最大 (条件つき)
            let want_min = alive().filter(|r| inr(r.age, a, b)).filter_map(|r| r.age).min();
            assert_eq!(t.where_range("age", a as i64, b as i64).min("age").unwrap(), want_min, "{stage} q_min_rng");
            let want_max = alive().filter(|r| r.city == Some(city)).filter_map(|r| r.score).max();
            assert_eq!(t.where_eq("city", city).max("score").unwrap(), want_max, "{stage} q_max_tag");
        }
        // 条件なし (`.all()` = 主キーのある row)
        let has_pk = |r: &Row| r.id.is_some();
        assert_eq!(t.all().count().unwrap(), alive().filter(|r| has_pk(r)).count(), "{stage} c_all");
        for k in [1, 10, 100] {
            assert_top(rows, t.all().order_by("age").limit(k).find().unwrap(), k, |r| has_pk(r).then_some(r.age).flatten().map(i128::from), &format!("{stage} top_age {k}"));
            assert_top(rows, t.all().order_by_desc("score").limit(k).find().unwrap(), k, |r| has_pk(r).then_some(r.score).flatten().map(|x| -(x as i128)), &format!("{stage} top_score_desc {k}"));
            assert_top(rows, t.all().order_by("ts").limit(k).find().unwrap(), k, |r| has_pk(r).then_some(r.ts).flatten().map(i128::from), &format!("{stage} top_ts {k}"));
            assert_top(rows, t.all().order_by_desc("ts").limit(k).find().unwrap(), k, |r| has_pk(r).then_some(r.ts).flatten().map(|x| -(x as i128)), &format!("{stage} top_ts_desc {k}"));
            assert_top(rows, t.all().order_by("id").limit(k).find().unwrap(), k, |r| r.id.map(i128::from), &format!("{stage} top_id {k}"));
        }
        // limit なしの並び (全部)
        assert_top(rows, t.all().order_by("age").find().unwrap(), usize::MAX, |r| has_pk(r).then_some(r.age).flatten().map(i128::from), &format!("{stage} order_all"));
        assert_top(rows, t.where_range("age", 10, 20).order_by("age").find().unwrap(), usize::MAX, |r| inr(r.age, 10, 20).then(|| r.age.unwrap() as i128), &format!("{stage} order_rng_all"));
        // 最小 / 最大 (条件なし = 主キーのある row)
        assert_eq!(t.all().min("age").unwrap(), alive().filter(|r| has_pk(r)).filter_map(|r| r.age).min(), "{stage} q_min_age");
        assert_eq!(t.all().max("score").unwrap(), alive().filter(|r| has_pk(r)).filter_map(|r| r.score).max(), "{stage} q_max_score");
        assert_eq!(t.all().min_i64("ts").unwrap(), alive().filter(|r| has_pk(r)).filter_map(|r| r.ts).min(), "{stage} q_min_ts");
        assert_eq!(t.all().max_i64("ts").unwrap(), alive().filter(|r| has_pk(r)).filter_map(|r| r.ts).max(), "{stage} q_max_ts");
        assert_eq!(t.all().max_i64("age").unwrap(), alive().filter(|r| has_pk(r)).filter_map(|r| r.age).max().map(i64::from), "{stage} q_max_i64_number");
        // Table の最小 / 最大 (値のある row 全部)
        assert_eq!(t.min("age"), alive().filter_map(|r| r.age).min(), "{stage} t_min_age");
        assert_eq!(t.max("score"), alive().filter_map(|r| r.score).max(), "{stage} t_max_score");
        // 常に 0 件
        assert_eq!(t.where_range("age", 50, 40).count().unwrap(), 0, "{stage} 空の範囲");
        assert_eq!(t.where_eq("city", "nowhere").count().unwrap(), 0, "{stage} 無い語");
        assert_eq!(t.where_in("grp", &[]).count().unwrap(), 0, "{stage} 空の IN");
        assert_eq!(t.where_range("age", 200, 300).order_by("age").limit(3).find().unwrap(), Vec::<u64>::new(), "{stage} 範囲の外");
        assert_eq!(t.where_range("age", 200, 300).min("age").unwrap(), None, "{stage} 範囲の外の最小");
        assert!(t.where_eq("nope", 1i64).count().unwrap() == 0, "{stage} 未知の列");
    };
    check(&rows, "static", &mut rng);

    // 書き換え (bucket に古い entry を残す)、 消し
    for _ in 0..n / 3 {
        let i = rng.below(n as u64) as usize;
        if !rows[i].alive {
            continue;
        }
        let (a, s, ts) = (rng.below(100) as u32, rng.below(10_000) as u32, ts_of(&mut rng));
        let c = CITIES[rng.below(5) as usize];
        t.entity(rows[i].eid).update().set("age", a as i64).set("score", s as i64).set("ts", ts).set("city", c).commit().unwrap();
        rows[i].age = Some(a);
        rows[i].score = Some(s);
        rows[i].ts = Some(ts);
        rows[i].city = Some(c);
    }
    for _ in 0..n / 15 {
        let i = rng.below(n as u64) as usize;
        if rows[i].alive {
            t.entity(rows[i].eid).delete().unwrap();
            rows[i].alive = false;
        }
    }
    check(&rows, "churn", &mut rng);

    drop(t);
    drop(db);
    cleanup(&path);
}

/// `count()` の誤り・並びの誤りは今までどおり: 2 つ目の `where_in` は BadValue、 未知の列の `order_by` は BadValue。
#[test]
fn index_paths_keep_errors() {
    let path = tmp_path("errors");
    cleanup(&path);
    let mut db = Database::create(&path).unwrap();
    db.table("q").number("id").number("a").number("b").primary_key("id").build().unwrap();
    let t = db.get_table("q").unwrap();
    t.insert().set("id", 1i64).set("a", 1i64).set("b", 2i64).commit().unwrap();
    assert!(t.where_in("a", &[1]).where_in("b", &[2]).count().is_err());
    assert!(t.where_in("a", &[1]).where_in("b", &[2]).find().is_err());
    assert!(t.where_eq("a", 1i64).order_by("nope").count().is_err());
    assert!(t.where_eq("a", 1i64).order_by("nope").limit(1).find().is_err());
    drop(t);
    drop(db);
    cleanup(&path);
}

/// Tag の列では並べられない (値は辞書の番号 = 辞書に初めて入った順で、 意味が無い): find / count / limit 付き / 購読がどれも
/// BadValue。 ref の道をたどった先が Tag の列でも同じ。 対照: 同じ表の Number の列 (順番を表す数を結んだ列) なら並べられる。
#[test]
fn order_by_a_tag_column_is_an_error() {
    let path = tmp_path("tag-order");
    cleanup(&path);
    let mut db = Database::create(&path).unwrap();
    db.table("prefs").number("id").tag("name").number("code").primary_key("id").build().unwrap();
    db.table("shops").number("id").tag("pref").ref_to("at", "prefs").primary_key("id").build().unwrap();
    let prefs = db.get_table("prefs").unwrap();
    let shops = db.get_table("shops").unwrap();
    // 辞書に入る順 (北海道 → 沖縄 → 島根) と、 結んだ順番の数 (code) は違う
    let mut p = Vec::new();
    for (i, (name, code)) in [("北海道", 1i64), ("沖縄", 47), ("島根", 32)].into_iter().enumerate() {
        p.push(prefs.insert().set("id", i as i64).set("name", name).set("code", code).commit().unwrap());
    }
    for i in 0..6i64 {
        let at = p[(i % 3) as usize];
        let name = ["北海道", "沖縄", "島根"][(i % 3) as usize];
        shops.insert().set("id", i).set("pref", name).set("at", enchudb_schema::Value::Ref(at)).commit().unwrap();
    }
    let bad = |r: Result<usize, enchudb_schema::SchemaError>, what: &str| {
        assert!(matches!(r, Err(enchudb_schema::SchemaError::BadValue(_))), "{what}: {r:?}");
    };
    bad(shops.all().order_by("pref").find().map(|v| v.len()), "find");
    bad(shops.all().order_by_desc("pref").limit(2).find().map(|v| v.len()), "limit 付きの find");
    bad(shops.all().order_by("pref").count(), "count");
    bad(shops.all().order_by("at.name").find().map(|v| v.len()), "ref の道の先の Tag");
    assert!(shops.all().order_by("pref").limit(2).subscribe().is_err(), "購読");
    // 対照: 順番を表す数の列 (ref の道の先の Number) なら並べられる
    let got = shops.all().order_by("at.code").find().unwrap();
    let codes: Vec<i64> = got
        .iter()
        .map(|&e| match prefs.entity(match shops.entity(e).get("at") { Some(enchudb_schema::Value::Ref(r)) => r, v => panic!("{v:?}") }).get("code") {
            Some(enchudb_schema::Value::Number(c)) => c,
            v => panic!("{v:?}"),
        })
        .collect();
    assert_eq!(codes, vec![1, 1, 32, 32, 47, 47], "対照: code の順");
    drop((prefs, shops));
    drop(db);
    cleanup(&path);
}

/// 範囲の当たりが表の半分を超える時は、 範囲の列を eid の順になめる道 (範囲の索引から引いて並べ直すより速い)。 その道でも
/// 答えが同じ: 範囲だけ (全部 / 7 割)、 範囲 + 等値、 範囲 2 つ、 書き換えと消しの後。 当たりが少ない時 (索引の道) とも比べる。
#[test]
fn wide_range_find_scans_the_column() {
    let path = tmp_path("wide");
    cleanup(&path);
    let mut db = Database::create(&path).unwrap();
    db.table("w").number("id").number("age").number("grp").primary_key("id").with_capacity(20_000).build().unwrap();
    let t = db.get_table("w").unwrap();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let n = 12_000usize;
    let mut rows: Vec<(u64, bool, u32, u32)> = Vec::with_capacity(n);
    for i in 0..n {
        let (a, g) = (rng.below(100) as u32, rng.below(4) as u32);
        let e = t.insert().set("id", i as i64).set("age", a as i64).set("grp", g as i64).commit().unwrap();
        rows.push((e, true, a, g));
    }
    let check = |rows: &[(u64, bool, u32, u32)], stage: &str| {
        let ids = |f: &dyn Fn(u32, u32) -> bool| -> Vec<u64> { sorted(rows.iter().filter(|r| r.1 && f(r.2, r.3)).map(|r| r.0).collect()) };
        for (lo, hi) in [(0u32, 99u32), (0, 69), (30, 99), (10, 12)] {
            assert_eq!(t.where_range("age", lo as i64, hi as i64).find().unwrap(), ids(&|a, _| lo <= a && a <= hi), "{stage} 範囲 {lo}..={hi}");
            assert_eq!(
                t.where_range("age", lo as i64, hi as i64).count().unwrap(),
                ids(&|a, _| lo <= a && a <= hi).len(),
                "{stage} 件数 {lo}..={hi}"
            );
            for g in 0..4u32 {
                assert_eq!(
                    sorted(t.where_range("age", lo as i64, hi as i64).where_eq("grp", g as i64).find().unwrap()),
                    ids(&|a, gg| lo <= a && a <= hi && gg == g),
                    "{stage} 範囲 {lo}..={hi} + grp {g}"
                );
            }
            assert_eq!(
                sorted(t.where_range("age", lo as i64, hi as i64).where_range("grp", 1, 3).find().unwrap()),
                ids(&|a, gg| lo <= a && a <= hi && (1..=3).contains(&gg)),
                "{stage} 範囲 2 つ {lo}..={hi}"
            );
        }
    };
    check(&rows, "static");
    for _ in 0..n / 4 {
        let i = rng.below(n as u64) as usize;
        if !rows[i].1 {
            continue;
        }
        let a = rng.below(100) as u32;
        t.entity(rows[i].0).update().set("age", a as i64).commit().unwrap();
        rows[i].2 = a;
    }
    for _ in 0..n / 20 {
        let i = rng.below(n as u64) as usize;
        if rows[i].1 {
            t.entity(rows[i].0).delete().unwrap();
            rows[i].1 = false;
        }
    }
    check(&rows, "churn");
    drop(t);
    drop(db);
    cleanup(&path);
}
