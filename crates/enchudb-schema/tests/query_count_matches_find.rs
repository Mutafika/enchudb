//! `Query::count()` は同じ query の `find()?.len()` と同じ数 (`LiveQuery::count` などほかの count と同じ約束) で、
//! 手元の写し (書いた値を全部覚えたもの) を数えた数とも同じ。
//!
//! - 条件の無い `.all()` は代表列 (主キー) に値のある row (`Table::recent` の doc と同じ)。 書き換え・消し・列の値を外す
//!   (`untie`)・開き直し・開き直した後の書き込みを混ぜる
//! - `order_by` は並びの列に値の無い row を外す (`order_by` の doc) ので、 並びのある query の件数はその row を数えない

use enchudb_schema::Database;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-count-find-{}-{}-{}",
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

/// assert で落ちても DB を消す。 db より先に作って、 db の後に drop させる。
struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        cleanup(&self.0);
    }
}

struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// 手元の写しの 1 row。
#[derive(Clone, Copy)]
struct Row {
    eid: u64,
    alive: bool,
    id: Option<u32>,
    a: Option<u32>,
    x: Option<u32>,
    g: u32,
}

fn insert(t: &enchudb_schema::Table<'_>, rng: &mut Rng, i: u32) -> Row {
    let mut row = Row { eid: 0, alive: true, id: None, a: None, x: None, g: rng.below(50) as u32 };
    let mut b = t.insert().set("g", row.g as i64);
    // 7 行に 1 行は a なし、 5 行に 1 行は x なし。 名前 (主キー) の無い行は schema では作れない (#548) ので、 名前の無い
    // row は churn の engine の untie でだけできる
    row.id = Some(i);
    b = b.set("id", i as i64);
    if i % 7 != 2 {
        let a = rng.below(6) as u32;
        row.a = Some(a);
        b = b.set("a", a as i64);
    }
    if i % 5 != 1 {
        let x = rng.below(1000) as u32;
        row.x = Some(x);
        b = b.set("x", x as i64);
    }
    row.eid = b.commit().unwrap();
    row
}

/// 書き換え・列の値を外す・消し。 `untie` は engine の API (schema の update は値を外せない)。
fn churn(db: &Database, t: &enchudb_schema::Table<'_>, rows: &mut [Row], rng: &mut Rng) {
    let n = rows.len() as u64;
    for _ in 0..n / 3 {
        let r = &mut rows[rng.below(n) as usize];
        if !r.alive {
            continue;
        }
        match rng.below(6) {
            0 => {
                db.engine().untie(r.eid, "c.x");
                r.x = None;
            }
            1 => {
                db.engine().untie(r.eid, "c.id");
                r.id = None;
            }
            2 => {
                db.engine().untie(r.eid, "c.a");
                r.a = None;
            }
            _ => {
                let (a, x) = (rng.below(6) as u32, rng.below(1000) as u32);
                t.entity(r.eid).update().set("a", a as i64).set("x", x as i64).commit().unwrap();
                r.a = Some(a);
                r.x = Some(x);
            }
        }
    }
    for _ in 0..n / 10 {
        let r = &mut rows[rng.below(n) as usize];
        if r.alive {
            t.entity(r.eid).delete().unwrap();
            r.alive = false;
        }
    }
    // 外した主キーを付け直す row (値の無い所から有る所へ戻る)
    for r in rows.iter_mut().filter(|r| r.alive && r.id.is_none()).take(20) {
        let id = 1_000_000 + local(r.eid);
        t.entity(r.eid).update().set("id", id as i64).commit().unwrap();
        r.id = Some(id);
    }
}

fn local(e: u64) -> u32 {
    e as u32
}

fn check(t: &enchudb_schema::Table<'_>, rows: &[Row], stage: &str, rng: &mut Rng) {
    let alive = || rows.iter().filter(|r| r.alive);
    let want = |f: &dyn Fn(&Row) -> bool| alive().filter(|r| f(r)).count();
    // 条件なし
    let q = || t.all();
    assert_eq!(q().count().unwrap(), want(&|r| r.id.is_some()), "{stage} all");
    assert_eq!(q().count().unwrap(), q().find().unwrap().len(), "{stage} all = find");
    assert_eq!(q().limit(7).count().unwrap(), want(&|r| r.id.is_some()).min(7), "{stage} all limit");
    assert_eq!(q().order_by("x").count().unwrap(), want(&|r| r.id.is_some() && r.x.is_some()), "{stage} all order_by x");
    assert_eq!(q().order_by("x").count().unwrap(), q().order_by("x").find().unwrap().len(), "{stage} all order_by x = find");
    for _ in 0..6 {
        let v = rng.below(6) as u32;
        let vs = [v, (v + 1) % 6];
        let (lo, hi) = {
            let lo = rng.below(50) as u32;
            (lo, lo + rng.below(20) as u32)
        };
        let (xl, xh) = {
            let xl = rng.below(1000) as u32;
            (xl, xl + rng.below(300) as u32)
        };
        let k = 1 + rng.below(10) as usize;
        let eq = || t.where_eq("a", v as i64);
        let cases: Vec<(&str, enchudb_schema::Query<'_>, enchudb_schema::Query<'_>, usize)> = vec![
            ("eq", eq(), eq(), want(&|r| r.a == Some(v))),
            ("eq order_by x", eq().order_by("x"), eq().order_by("x"), want(&|r| r.a == Some(v) && r.x.is_some())),
            (
                "eq order_by_desc x limit",
                eq().order_by_desc("x").limit(k),
                eq().order_by_desc("x").limit(k),
                want(&|r| r.a == Some(v) && r.x.is_some()).min(k),
            ),
            (
                "in order_by x",
                t.where_in("a", &vs).order_by("x"),
                t.where_in("a", &vs).order_by("x"),
                want(&|r| r.a.is_some_and(|a| vs.contains(&a)) && r.x.is_some()),
            ),
            (
                "range g order_by x",
                t.where_range("g", lo as i64, hi as i64).order_by("x"),
                t.where_range("g", lo as i64, hi as i64).order_by("x"),
                want(&|r| (lo..=hi).contains(&r.g) && r.x.is_some()),
            ),
            (
                "range x order_by a",
                t.where_range("x", xl as i64, xh as i64).order_by("a"),
                t.where_range("x", xl as i64, xh as i64).order_by("a"),
                want(&|r| r.x.is_some_and(|x| (xl..=xh).contains(&x)) && r.a.is_some()),
            ),
            (
                "range x order_by x",
                t.where_range("x", xl as i64, xh as i64).order_by("x"),
                t.where_range("x", xl as i64, xh as i64).order_by("x"),
                want(&|r| r.x.is_some_and(|x| (xl..=xh).contains(&x))),
            ),
            (
                "eq + range g order_by x",
                eq().where_range("g", lo as i64, hi as i64).order_by("x"),
                eq().where_range("g", lo as i64, hi as i64).order_by("x"),
                want(&|r| r.a == Some(v) && (lo..=hi).contains(&r.g) && r.x.is_some()),
            ),
        ];
        for (name, qc, qf, w) in cases {
            let c = qc.count().unwrap();
            let f = qf.find().unwrap().len();
            assert_eq!(c, f, "{stage} {name}: count と find の数が違う");
            assert_eq!(c, w, "{stage} {name}: 写しの数と違う");
        }
    }
}

#[test]
fn count_matches_find_and_mirror() {
    let path = tmp_path("main");
    cleanup(&path);
    let _cleanup = Cleanup(path.clone());
    let mut rng = Rng(0x2545_F491_4F6C_DD1D);
    let mut rows: Vec<Row> = Vec::new();
    {
        let mut db = Database::create(&path).unwrap();
        db.table("c").number("id").number("a").number("x").number("g").primary_key("id").with_capacity(10_000).build().unwrap();
        let t = db.get_table("c").unwrap();
        for i in 0..2_000 {
            rows.push(insert(&t, &mut rng, i));
        }
        check(&t, &rows, "static", &mut rng);
        churn(&db, &t, &mut rows, &mut rng);
        check(&t, &rows, "churn", &mut rng);
    }
    // 開き直し: 最初に引く数は all の件数 (開いた時の数を数える道)
    {
        let db = Database::open(&path).unwrap();
        let t = db.get_table("c").unwrap();
        assert_eq!(t.all().count().unwrap(), rows.iter().filter(|r| r.alive && r.id.is_some()).count(), "reopen 最初の all");
        check(&t, &rows, "reopen", &mut rng);
        for i in 2_000..2_600 {
            rows.push(insert(&t, &mut rng, i));
        }
        churn(&db, &t, &mut rows, &mut rng);
        check(&t, &rows, "reopen + churn", &mut rng);
    }
    // 開き直した後、 数を引く前に書く (数える前の書き込みも数に入る)
    {
        let db = Database::open(&path).unwrap();
        let t = db.get_table("c").unwrap();
        for i in 2_600..2_700 {
            rows.push(insert(&t, &mut rng, i));
        }
        churn(&db, &t, &mut rows, &mut rng);
        check(&t, &rows, "reopen 2 (数える前に書く)", &mut rng);
    }
}
