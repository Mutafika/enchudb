//! 作り直した row (消した row の eid が別の row に使い回された) は、 組の中で同じ位置に同じ相手と居ても別物:
//! 組の購読 (`join_ref` / `join_eq` / 3 table 以上の組) は、 その組を removed と added の両方で届けること。
//!
//! eid の使い回しは table の枠が埋まってからだけ起きる (成長する DB では起きない) ので、 容量を決めた DB で
//! 枠を埋めてから作り直す。

use enchudb_schema::{Database, Table, Value};

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/schema_live_reborn_{}_{}_{}.db", tag, std::process::id(), nanos)
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    let _ = std::fs::remove_file(path);
    for ext in ["lock", "oplog", "schema", "tables"] {
        let _ = std::fs::remove_file(format!("{}.{}", path, ext));
    }
}

/// `t` の枠が埋まるまで `fill` で row を足す。
fn fill(db: &Database, t: &Table, mut fill: impl FnMut(i64) -> u64) {
    let cap = db.engine().table_eid_usage(t.name()).unwrap().capacity;
    let mut i = 1000;
    while db.engine().table_eid_usage(t.name()).unwrap().live < cap {
        fill(i);
        i += 1;
    }
}

fn sorted<T: Ord>(mut v: Vec<T>) -> Vec<T> {
    v.sort();
    v
}

fn schema(path: &str) -> Database {
    let mut db = Database::create_with_capacity(path, 128).unwrap();
    db.table("companies").number("id").tag("city").primary_key("id").build().unwrap();
    db.table("users").number("id").tag("city").ref_to("company", "companies").primary_key("id").build().unwrap();
    db.table("shops").number("id").tag("city").primary_key("id").build().unwrap();
    db
}

#[test]
fn reborn_rows_leave_and_reenter_pairs_and_tuples() {
    let path = tmp_path("pairs");
    cleanup(&path);
    let db = schema(&path);
    let (c, u, s) = (db.get_table("companies").unwrap(), db.get_table("users").unwrap(), db.get_table("shops").unwrap());
    let ant = c.insert().set("id", 1i64).set("city", "Tokyo").commit().unwrap();
    let alice = u.insert().set("id", 1i64).set("city", "Tokyo").set("company", Value::Ref(ant)).commit().unwrap();
    let shop = s.insert().set("id", 1i64).set("city", "Tokyo").commit().unwrap();
    // carol: 同じ会社だが左の条件 (Tokyo) に合わない / dave: 会社の作り直しと同じ poll で会社に入る
    let carol = u.insert().set("id", 3i64).set("city", "Osaka").set("company", Value::Ref(ant)).commit().unwrap();
    let dave = u.insert().set("id", 4i64).set("city", "Tokyo").commit().unwrap();
    fill(&db, &u, |i| u.insert().set("id", i).commit().unwrap());
    fill(&db, &c, |i| c.insert().set("id", i).commit().unwrap());
    let by_ref = u.all().join_ref("company", c.all()).subscribe().unwrap();
    let tokyo = u.where_eq("city", "Tokyo").join_ref("company", c.all()).subscribe().unwrap();
    let by_val = u.all().join_eq("city", s.all(), "city").subscribe().unwrap();
    let tri = u.all().join_ref("company", c.all()).then_eq("companies", "city", s.all(), "city").subscribe().unwrap();
    assert_eq!(sorted(by_ref.poll().added), vec![(alice, ant), (carol, ant)]);
    assert_eq!(tokyo.poll().added, vec![(alice, ant)]);
    assert_eq!(sorted(by_val.poll().added), vec![(alice, shop), (dave, shop)]);
    assert_eq!(sorted(tri.poll().added), vec![vec![alice, ant, shop], vec![carol, ant, shop]]);

    // 左の row の作り直し: 同じ eid・同じ相手
    u.entity(alice).delete().unwrap();
    let bob = u.insert().set("id", 2i64).set("city", "Tokyo").set("company", Value::Ref(ant)).commit().unwrap();
    assert_eq!(bob, alice, "前提: 枠が埋まっていれば消した eid が使い回される");
    let d = by_ref.poll();
    assert_eq!((d.removed, d.added), (vec![(alice, ant)], vec![(bob, ant)]), "ref の組");
    let d = tokyo.poll();
    assert_eq!((d.removed, d.added), (vec![(alice, ant)], vec![(bob, ant)]), "ref の組 (Tokyo)");
    let d = by_val.poll();
    assert_eq!((d.removed, d.added), (vec![(alice, shop)], vec![(bob, shop)]), "値の組");
    let d = tri.poll();
    assert_eq!((d.removed, d.added), (vec![vec![alice, ant, shop]], vec![vec![bob, ant, shop]]), "3 table の組");

    // 右 (と真ん中) の row の作り直し
    c.entity(ant).delete().unwrap();
    let ant2 = c.insert().set("id", 2i64).set("city", "Tokyo").commit().unwrap();
    assert_eq!(ant2, ant, "前提: 枠が埋まっていれば消した eid が使い回される");
    u.entity(dave).set("company", Value::Ref(ant2)).commit().unwrap();
    let d = by_ref.poll();
    assert_eq!(
        (sorted(d.removed), sorted(d.added)),
        (vec![(bob, ant), (carol, ant)], vec![(bob, ant2), (carol, ant2), (dave, ant2)]),
        "ref の組 (右): 居続けた組は出て入り直す、 dave は入るだけ"
    );
    let d = tokyo.poll();
    assert_eq!((sorted(d.removed), sorted(d.added)), (vec![(bob, ant)], vec![(bob, ant2), (dave, ant2)]), "ref の組 (右、 Tokyo): carol は入らない");
    assert_eq!(by_val.poll(), Default::default(), "値の組は会社に関係しない (dave は最初から Tokyo の店と組)");
    let d = tri.poll();
    assert_eq!(
        (sorted(d.removed), sorted(d.added)),
        (vec![vec![bob, ant, shop], vec![carol, ant, shop]], vec![vec![bob, ant2, shop], vec![carol, ant2, shop], vec![dave, ant2, shop]]),
        "3 table の組 (真ん中)"
    );
    drop((by_ref, tokyo, by_val, tri, c, u, s));
    drop(db);
    cleanup(&path);
}
