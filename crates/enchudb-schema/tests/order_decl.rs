//! 並びの索引の宣言 (`TableBuilder::order`): build での検査、 engine への宣言、 開き直し、 再宣言 (目盛りを変える /
//! 並びだけ書く / 列だけ書いて外す / handle を取るだけ)。 答えは手で数えた結果 (shadow) と比べ、 索引を読んだか
//! (正の対照: 読んだ回数) と、 宣言しない表 (負の対照: 読んだ回数 0、 答えは同じ) も見る。

use enchudb_oplog::EntityId;
use enchudb_schema::{Database, SchemaError, Value};
use std::collections::BTreeMap;

fn tmp_path(name: &str) -> String {
    format!("/tmp/enchudb-order-decl-{}-{}", name, std::process::id())
}

fn cleanup(path: &str) {
    let _ = enchudb_engine::db_files::remove_db(path);
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

/// companies (city) と users (company → companies、 age) を宣言する。 `order` は users の並び (列の宣言より前に書く =
/// 書く順に依らないことも見る)。
fn declare(db: &mut Database, order: Option<&[i64]>) -> Result<(), SchemaError> {
    db.table("companies").number("city").build()?;
    let mut b = db.table("users");
    if let Some(t) = order {
        b = b.order("company", "age", t);
    }
    b.ref_to("company", "companies").number("age").build()?;
    Ok(())
}

/// 社員 → (会社, 年齢)、 会社 → city。
struct Shadow {
    comps: Vec<EntityId>,
    rows: BTreeMap<EntityId, (EntityId, i64)>,
    city: BTreeMap<EntityId, i64>,
}

fn fill(db: &Database, seed: u64) -> Shadow {
    let (companies, users) = (db.get_table("companies").unwrap(), db.get_table("users").unwrap());
    let mut rng = Rng(seed);
    let mut city = BTreeMap::new();
    let comps: Vec<EntityId> = (0..12)
        .map(|_| {
            let v = rng.below(3) as i64;
            let c = companies.insert().set("city", v).commit().unwrap();
            city.insert(c, v);
            c
        })
        .collect();
    let mut rows = BTreeMap::new();
    for _ in 0..300 {
        let c = comps[rng.below(comps.len() as u64) as usize];
        let a = 18 + rng.below(62) as i64;
        let u = users.insert().set("company", Value::Ref(c)).set("age", a).commit().unwrap();
        rows.insert(u, (c, a));
    }
    Shadow { comps, rows, city }
}

/// 社員を何人か異動 / 年を取らせる (shadow も)。
fn churn(db: &Database, s: &mut Shadow, seed: u64) {
    let users = db.get_table("users").unwrap();
    let mut rng = Rng(seed);
    let ids: Vec<EntityId> = s.rows.keys().copied().collect();
    for _ in 0..60 {
        let u = ids[rng.below(ids.len() as u64) as usize];
        let r = s.rows.get_mut(&u).unwrap();
        if rng.below(2) == 0 {
            r.0 = s.comps[rng.below(s.comps.len() as u64) as usize];
            users.entity(u).set("company", Value::Ref(r.0)).commit().unwrap();
        } else {
            r.1 = 18 + rng.below(62) as i64;
            users.entity(u).set("age", r.1).commit().unwrap();
        }
    }
}

fn sorted(mut v: Vec<EntityId>) -> Vec<EntityId> {
    v.sort_unstable();
    v
}

/// city と年齢の範囲 (ref をたどる列 = find_by の Via + Range) と、 会社と年齢の範囲 (`where_ref`) の答えを shadow と比べる。
fn check(db: &Database, s: &Shadow, what: &str) {
    let users = db.get_table("users").unwrap();
    for (lo, hi) in [(30, 64), (0, 29), (65, 200), (40, 50)] {
        for city in 0..3 {
            let got = sorted(users.where_eq("company.city", city).where_range("age", lo, hi).find().unwrap());
            let want: Vec<EntityId> = s
                .rows
                .iter()
                .filter(|(_, r)| s.city[&r.0] == city && (lo..=hi).contains(&r.1))
                .map(|(&u, _)| u)
                .collect();
            assert_eq!(got, want, "{what}: city {city} age {lo}..={hi}");
        }
        for &c in &s.comps {
            let got = sorted(users.where_ref("company", c).where_range("age", lo, hi).find().unwrap());
            let want: Vec<EntityId> = s.rows.iter().filter(|(_, r)| r.0 == c && (lo..=hi).contains(&r.1)).map(|(&u, _)| u).collect();
            assert_eq!(got, want, "{what}: company {c} age {lo}..={hi}");
        }
    }
}

fn hits(db: &Database) -> u64 {
    db.engine().order_stats().iter().map(|s| s.2).sum()
}

fn decls(db: &Database) -> Vec<(String, String, Vec<u64>)> {
    db.engine().order_declarations()
}

fn decl(ticks: &[u64]) -> Vec<(String, String, Vec<u64>)> {
    vec![("users.company".to_string(), "users.age".to_string(), ticks.to_vec())]
}

/// build で engine に宣言され、 ref をたどる検索と `where_ref` の検索が索引を読んで正しく答える。 対照: 宣言しない表は
/// 索引を読まず、 答えは同じ。
#[test]
fn order_is_declared_at_build_and_used_by_queries() {
    for with_order in [true, false] {
        let path = tmp_path(if with_order { "build" } else { "build-plain" });
        cleanup(&path);
        let mut db = Database::create_growable(&path).unwrap();
        declare(&mut db, with_order.then_some(&[30, 65][..])).unwrap();
        assert_eq!(decls(&db), if with_order { decl(&[30, 65]) } else { Vec::new() });
        let mut s = fill(&db, 0x5eed_0d01);
        check(&db, &s, "書いた直後");
        churn(&db, &mut s, 0x5eed_0d02);
        check(&db, &s, "異動の後");
        if with_order {
            assert!(hits(&db) > 0, "索引を読んでいない");
        } else {
            assert_eq!(hits(&db), 0, "対照: 宣言しない表で索引を読んだ");
        }
        drop(db);
        cleanup(&path);
    }
}

/// build でまとめて検査する: 列が無い、 via が ref でない、 key が数でない、 目盛りが空 / 昇順でない / 列に入らない、
/// 同じ via に 2 つ。 落ちた build は表も宣言も作らない。
#[test]
fn order_validation_errors() {
    let path = tmp_path("errors");
    cleanup(&path);
    let mut db = Database::create_growable(&path).unwrap();
    db.table("companies").number("city").build().unwrap();
    let users = |db: &mut Database, via: &str, key: &str, ticks: &[i64]| {
        db.table("users")
            .ref_to("company", "companies")
            .number("age")
            .tag("dept")
            .bigint("at")
            .order(via, key, ticks)
            .build()
            .err()
    };
    assert!(matches!(users(&mut db, "boss", "age", &[30]), Some(SchemaError::UnknownColumn(c)) if c == "boss"));
    assert!(matches!(users(&mut db, "company", "height", &[30]), Some(SchemaError::UnknownColumn(c)) if c == "height"));
    assert!(matches!(users(&mut db, "age", "age", &[30]), Some(SchemaError::TypeMismatch(_))), "via が ref でない");
    assert!(matches!(users(&mut db, "company", "dept", &[30]), Some(SchemaError::TypeMismatch(_))), "key が tag");
    assert!(matches!(users(&mut db, "company", "company", &[30]), Some(SchemaError::TypeMismatch(_))), "key が ref");
    assert!(matches!(users(&mut db, "company", "age", &[]), Some(SchemaError::BadValue(_))), "空の目盛り");
    assert!(matches!(users(&mut db, "company", "age", &[30, 30]), Some(SchemaError::BadValue(_))), "昇順でない");
    assert!(matches!(users(&mut db, "company", "age", &[-1, 30]), Some(SchemaError::BadValue(_))), "number に負の目盛り");
    assert!(matches!(users(&mut db, "company", "age", &[u32::MAX as i64]), Some(SchemaError::BadValue(_))), "number の外");
    assert!(matches!(users(&mut db, "company", "at", &[i64::MAX]), Some(SchemaError::BadValue(_))), "bigint の外");
    let two = db
        .table("users")
        .ref_to("company", "companies")
        .number("age")
        .number("rank")
        .order("company", "age", &[30])
        .order("Company", "rank", &[3])
        .build()
        .err();
    assert!(matches!(two, Some(SchemaError::BadValue(_))), "同じ via に 2 つ");
    assert!(db.get_table("users").is_none(), "落ちた build が表を作った");
    assert!(decls(&db).is_empty(), "落ちた build が宣言した");
    // 正の対照: 同じ形で目盛りが正しければ通る (bigint の負の目盛りも)
    assert!(users(&mut db, "company", "at", &[-5, 0, 10]).is_none());
    let raw: Vec<u64> = [-5i64, 0, 10].iter().map(|&t| enchudb_schema::bigint_raw(t).unwrap()).collect();
    assert_eq!(decls(&db), vec![("users.company".to_string(), "users.at".to_string(), raw)]);
    drop(db);
    cleanup(&path);
}

/// bigint の列で負の目盛り: 範囲 (負の数・0 をまたぐ) の答えが shadow と一致し、 索引を読む。
#[test]
fn bigint_key_with_negative_ticks() {
    let path = tmp_path("bigint");
    cleanup(&path);
    let mut db = Database::create_growable(&path).unwrap();
    db.table("companies").number("city").build().unwrap();
    db.table("events").ref_to("company", "companies").bigint("at").order("company", "at", &[-100, 0, 1 << 40]).build().unwrap();
    let (companies, events) = (db.get_table("companies").unwrap(), db.get_table("events").unwrap());
    let mut rng = Rng(0x5eed_0d21);
    let comps: Vec<EntityId> = (0..6).map(|i| companies.insert().set("city", (i % 2) as i64).commit().unwrap()).collect();
    let mut rows = Vec::new();
    for _ in 0..400 {
        let c = comps[rng.below(comps.len() as u64) as usize];
        let at = match rng.below(4) {
            0 => -(rng.below(1000) as i64),
            1 => rng.below(1000) as i64,
            2 => -100 + rng.below(3) as i64 - 1,
            _ => (1 << 40) + rng.below(3) as i64 - 1,
        };
        let e = events.insert().set("company", Value::Ref(c)).set("at", at).commit().unwrap();
        rows.push((e, c, at));
    }
    for (lo, hi) in [(-100, -1), (-1000, -101), (0, (1 << 40) - 1), (-50, 50), (1 << 40, i64::MAX - 1)] {
        for city in 0..2i64 {
            let got = sorted(events.where_eq("company.city", city).where_range("at", lo, hi).find().unwrap());
            let mut want: Vec<EntityId> = rows
                .iter()
                .filter(|&&(_, c, at)| comps.iter().position(|&x| x == c).unwrap() as i64 % 2 == city && (lo..=hi).contains(&at))
                .map(|&(e, _, _)| e)
                .collect();
            want.sort_unstable();
            assert_eq!(got, want, "city {city} at {lo}..={hi}");
        }
    }
    assert!(hits(&db) > 0);
    drop(db);
    cleanup(&path);
}

/// 宣言は開き直しても engine が戻す (schema を宣言し直さなくても効く)。 handle を取るだけの build と同じ宣言の再宣言は
/// 何も変えない (`{db}/tables` の中身も同じ)。 目盛りを変えると置き換わり、 並びだけ書く build も宣言どおりにし、 列だけ
/// 書いて並びを書かなければ外れる。 どれも開き直した後に残る。 間に書き込みを挟んで答えを比べ続ける。
#[test]
fn declarations_survive_reopen_and_follow_redeclaration() {
    let path = tmp_path("reopen");
    cleanup(&path);
    let tables = || std::fs::read(enchudb_engine::db_files::path_for(&path, enchudb_engine::db_files::TABLES)).unwrap();
    let mut s = {
        let mut db = Database::create_growable(&path).unwrap();
        declare(&mut db, Some(&[30, 65])).unwrap();
        fill(&db, 0x5eed_0d31)
    }; // Drop で schema / tables を書く
    {
        let db = Database::open(&path).unwrap();
        assert_eq!(decls(&db), decl(&[30, 65]), "開き直しで宣言が戻らない");
        check(&db, &s, "開き直し");
        assert!(hits(&db) > 0);
    }
    {
        let before = tables();
        let mut db = Database::open(&path).unwrap();
        db.table("users").build().unwrap();
        assert_eq!(decls(&db), decl(&[30, 65]), "handle を取るだけの build が宣言を変えた");
        declare(&mut db, Some(&[30, 65])).unwrap();
        assert_eq!(decls(&db), decl(&[30, 65]));
        drop(db);
        assert_eq!(tables(), before, "同じ宣言で tables sidecar の中身が変わった");
    }
    {
        let mut db = Database::open(&path).unwrap();
        declare(&mut db, Some(&[50])).unwrap();
        assert_eq!(decls(&db), decl(&[50]), "目盛りを変えたのに置き換わらない");
        churn(&db, &mut s, 0x5eed_0d32);
        check(&db, &s, "目盛りを変えた後");
        assert!(hits(&db) > 0);
    }
    {
        let mut db = Database::open(&path).unwrap();
        assert_eq!(decls(&db), decl(&[50]), "置き換えが開き直しで戻らない");
        db.table("users").order("company", "age", &[40]).build().unwrap();
        assert_eq!(decls(&db), decl(&[40]), "並びだけの build が宣言どおりにならない");
    }
    {
        let mut db = Database::open(&path).unwrap();
        assert_eq!(decls(&db), decl(&[40]));
        declare(&mut db, None).unwrap();
        assert!(decls(&db).is_empty(), "並びを書かない再宣言で外れない");
        churn(&db, &mut s, 0x5eed_0d33);
        check(&db, &s, "外した後");
        assert_eq!(hits(&db), 0);
    }
    {
        let db = Database::open(&path).unwrap();
        assert!(decls(&db).is_empty(), "外した宣言が開き直しで戻った");
        check(&db, &s, "外した後の開き直し");
    }
    cleanup(&path);
}
