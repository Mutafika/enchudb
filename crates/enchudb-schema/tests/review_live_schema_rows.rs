//! review: schema 層の行の書き込みの端。 各 test は 「今の挙動」 を出力し、 疑った不変量が破れたら panic する。

use enchudb_schema::{Cursor, Database, SchemaError, CURSOR_ANSWER};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const SCRATCH: &str = "/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/eedfc333-d87a-4a47-a3b0-daf4b6d7337a/scratchpad";

fn tmp_path(name: &str) -> String {
    let p = format!("{SCRATCH}/rl-rows-{}-{}", name, std::process::id());
    cleanup(&p);
    p
}
fn cleanup(path: &str) {
    let _ = enchudb_engine::db_files::remove_db(path);
    let _ = std::fs::remove_dir_all(path);
}

/// insert で名前の列を 2 回 set: 重複の確かめは 1 つ目の値でだけ行い、 書くのは両方 (最後が残る) → 既にある名前の 2 つ目の行。
#[test]
fn insert_setting_the_name_twice_makes_a_duplicate_name() {
    let path = tmp_path("twice");
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").number("a").primary_key("id").build().unwrap();
    let t = db.get_table("t").unwrap();
    let e2 = t.insert().set("id", 2i64).set("a", 20i64).commit().unwrap();
    // 素直な重複は断る
    assert!(matches!(t.insert().set("id", 2i64).set("a", 21i64).commit(), Err(SchemaError::DuplicatePk)));
    let r = t.insert().set("id", 1i64).set("id", 2i64).set("a", 22i64).commit();
    eprintln!("insert(id=1, id=2, a=22) -> {r:?}");
    let rows = t.where_eq("id", 2i64).find().unwrap();
    eprintln!("where id=2 -> {} 行 {rows:?} (最初の行 {e2:#x})", rows.len());
    let ones = t.where_eq("id", 1i64).find().unwrap();
    eprintln!("where id=1 -> {} 行", ones.len());
    let n = rows.len();
    drop(t);
    drop(db);
    cleanup(&path);
    assert!(n <= 1, "名前 2 の行が {n} つある (#601 の約束: 名前が同じなら同じデッキ / insert は DuplicatePk)");
}

/// 文字の名前でも同じ。 Tag の名前の 2 回 set。
#[test]
fn insert_setting_a_tag_name_twice_makes_a_duplicate_name() {
    let path = tmp_path("twice-tag");
    let mut db = Database::create(&path).unwrap();
    db.table("t").tag("key").number("a").primary_key("key").build().unwrap();
    let t = db.get_table("t").unwrap();
    t.insert().set("key", "x").set("a", 1i64).commit().unwrap();
    let r = t.insert().set("key", "fresh").set("a", 2i64).set("key", "x").commit();
    eprintln!("insert(key=fresh, a=2, key=x) -> {r:?}");
    let n = t.where_eq("key", "x").count().unwrap();
    eprintln!("where key=x -> {n} 行");
    drop(t);
    drop(db);
    cleanup(&path);
    assert!(n <= 1, "名前 x の行が {n} つある");
}

/// EntityUpdate: 後ろに未知の列があると、 前の列を書いた後で Err (RowBuilder は書く前に全部の列を確かめる)。
#[test]
fn entity_update_with_a_late_unknown_column_writes_the_earlier_ones() {
    let path = tmp_path("partial");
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").number("a").number("b").primary_key("id").build().unwrap();
    let t = db.get_table("t").unwrap();
    let e = t.insert().set("id", 1i64).set("a", 1i64).set("b", 1i64).commit().unwrap();
    let r = t.entity(e).update().set("a", 99i64).set("nope", 5i64).set("b", 99i64).commit();
    let got = t.entity(e).get_many(&["a", "b"]);
    eprintln!("update(a=99, nope=5, b=99) -> {r:?}, 行は {got:?}");
    let r2 = t.insert().set("id", 2i64).set("a", 99i64).set("nope", 5i64).commit();
    eprintln!("insert(id=2, a=99, nope=5) -> {r2:?}, id=2 は {} 行", t.where_eq("id", 2i64).count().unwrap());
    drop(t);
    drop(db);
    cleanup(&path);
}

/// upsert と delete が同じ名前で重なる: upsert は名前の lock の下で行を引き当てるが、 delete は名前の lock を取らない。
/// 終わってから状態で見る: (1) 他の列で引ける entity は全部 `all()` (名前のある行) にも居る (2) 名前ごとに 1 行 (3) 行の列の値は
/// その名前の書き手が書いた値だけ (4) 名前だけで作った行 (id >= 1000) に a / b の値が付いていない (消えた行の eid の使い回し)。
#[test]
fn upsert_racing_delete_of_the_same_name() {
    use std::collections::HashSet;
    use std::sync::atomic::AtomicU64;
    let mut bad_runs = 0;
    let mut detail = String::new();
    const RUNS: usize = 5;
    for run in 0..RUNS {
        let path = tmp_path(&format!("race{run}"));
        let mut db = Database::create(&path).unwrap();
        db.table("t").number("id").number("a").number("b").primary_key("id").build().unwrap();
        let db = db.finish_with_oplog(64 << 20).unwrap();
        const NAMES: i64 = 4;
        let stop = Arc::new(AtomicBool::new(false));
        let inherited = Arc::new(AtomicU64::new(0));
        let deletes = Arc::new(AtomicU64::new(0));
        let mut hs = Vec::new();
        for w in 0..NAMES {
            let (db, stop) = (db.clone(), stop.clone());
            hs.push(std::thread::spawn(move || {
                let t = db.get_table("t").unwrap();
                let mut i = 0i64;
                while !stop.load(Ordering::Relaxed) {
                    let v = w * 1000 + (i % 1000);
                    let _ = t.upsert().set("id", w).set("a", v).set("b", v).commit();
                    i += 1;
                }
            }));
        }
        for _ in 0..2 {
            let (db, stop, deletes) = (db.clone(), stop.clone(), deletes.clone());
            hs.push(std::thread::spawn(move || {
                let t = db.get_table("t").unwrap();
                let mut i = 0i64;
                while !stop.load(Ordering::Relaxed) {
                    if let Some(e) = t.where_eq("id", i % NAMES).find_one().unwrap() {
                        t.entity(e).delete().unwrap();
                        deletes.fetch_add(1, Ordering::Relaxed);
                    }
                    i += 1;
                }
            }));
        }
        // 名前だけの行を作って消す書き手 (消えた行の eid を使い回す側)
        {
            let (db, stop, inherited) = (db.clone(), stop.clone(), inherited.clone());
            hs.push(std::thread::spawn(move || {
                let t = db.get_table("t").unwrap();
                let mut i = 0i64;
                while !stop.load(Ordering::Relaxed) {
                    let e = t.insert().set("id", 1000 + i).commit().unwrap();
                    if t.entity(e).get("a").is_some() || t.entity(e).get("b").is_some() {
                        inherited.fetch_add(1, Ordering::Relaxed);
                    }
                    t.entity(e).delete().unwrap();
                    i += 1;
                }
            }));
        }
        {
            let t = db.get_table("t").unwrap();
            for i in 0..4_000i64 {
                let w = i % NAMES;
                let v = w * 1000 + 500;
                let _ = t.upsert().set("id", w).set("a", v).set("b", v).commit();
            }
        }
        stop.store(true, Ordering::Relaxed);
        for h in hs {
            h.join().unwrap();
        }
        let t = db.get_table("t").unwrap();
        let all: Vec<u64> = t.all().find().unwrap();
        let all_set: HashSet<u64> = all.iter().copied().collect();
        let mut problems = Vec::new();
        let mut by_name: BTreeMap<i64, Vec<u64>> = BTreeMap::new();
        for &e in &all {
            if let Some(enchudb_schema::Value::Number(id)) = t.entity(e).get("id") {
                by_name.entry(id).or_default().push(e);
            }
        }
        for (id, es) in &by_name {
            if es.len() > 1 {
                problems.push(format!("名前 {id} の行が {} つ", es.len()));
            }
        }
        let with_a: HashSet<u64> = t.all().where_ge("a", 0i64).find().unwrap().into_iter().collect();
        let nameless: Vec<u64> = with_a.iter().copied().filter(|e| !all_set.contains(e)).collect();
        if let Some(&e) = nameless.first() {
            problems.push(format!(
                "名前の無い行 {} つ (例 {e:#x}: id={:?} a={:?} b={:?})、 all().count()={} なのに where_ge(a,0).count()={}",
                nameless.len(),
                t.entity(e).get("id"),
                t.entity(e).get("a"),
                t.entity(e).get("b"),
                t.all().count().unwrap(),
                t.all().where_ge("a", 0i64).count().unwrap(),
            ));
        }
        for &e in &all {
            let v = t.entity(e).get_many(&["id", "a", "b"]);
            let n = |x: &Option<enchudb_schema::Value>| match x { Some(enchudb_schema::Value::Number(n)) => Some(*n), _ => None };
            let (id, a, b) = (n(&v[0]), n(&v[1]), n(&v[2]));
            if a != b || a.is_some_and(|a| Some(a / 1000) != id) {
                problems.push(format!("行 {e:#x} の中身が混ざった: id={id:?} a={a:?} b={b:?}"));
            }
        }
        let inh = inherited.load(Ordering::Relaxed);
        if inh > 0 {
            problems.push(format!("名前だけで insert した行に a / b の値が付いていた {inh} 回 (消えた行の eid の使い回し)"));
        }
        if !problems.is_empty() {
            bad_runs += 1;
            if detail.is_empty() {
                detail = format!("run {run}: {}", problems.join(" / "));
            }
            eprintln!("run {run} (delete {} 回): {}", deletes.load(Ordering::Relaxed), problems.join(" / "));
        } else {
            eprintln!("run {run}: ok (行 {}、 delete {} 回)", all.len(), deletes.load(Ordering::Relaxed));
        }
        drop(t);
        drop(db);
        cleanup(&path);
    }
    eprintln!("upsert × delete: {bad_runs}/{RUNS} 回で不変量が破れた");
    assert_eq!(bad_runs, 0, "{detail}");
}

/// cursor_set: 同じ新しい名前を 2 本の thread が同時に置く → 行が 2 つできるか。
#[test]
fn concurrent_first_cursor_set_of_the_same_name() {
    let mut dup_runs = 0;
    const RUNS: usize = 40;
    let path = tmp_path("cursors");
    let mut db = Database::create(&path).unwrap();
    db.prepare_cursors().unwrap();
    db.table("t").number("id").primary_key("id").build().unwrap();
    let db = db.finish_with_oplog(16 << 20).unwrap();
    let name_h = db.engine().himo_id("_cursors.name").unwrap() as u16;
    for run in 0..RUNS {
        let name = format!("c{run}");
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let hs: Vec<_> = (0..4u64)
            .map(|w| {
                let (db, name, barrier) = (db.clone(), name.clone(), barrier.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    db.cursor_set(&name, Cursor { kind: CURSOR_ANSWER, pos: 100 + w, hlc: None }).unwrap();
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        let vid = db.engine().vocab_id(&name).unwrap();
        let rows = db.engine().query_by_id(&[(name_h, vid)]);
        if rows.len() > 1 {
            dup_runs += 1;
            if dup_runs == 1 {
                eprintln!("名前 {name}: `_cursors` に {} 行 {rows:?}、 cursor_get = {:?}", rows.len(), db.cursor_get(&name));
            }
        }
    }
    eprintln!("cursor_set の同時の最初の 1 回: {dup_runs}/{RUNS} 回で同じ名前の行が 2 つ以上");
    drop(db);
    cleanup(&path);
    assert_eq!(dup_runs, 0);
}

/// 上の続き: 同じ名前の行が 2 つできた後に 1 本の thread で位置を置き直し、 開き直して読む。 置いた位置と違う (古い) 位置が
/// 返るか (行の並びは開き直しで eid の順に戻る = 引き当てる行が変わりうる)。
#[test]
fn duplicate_cursor_rows_can_return_an_older_position_after_reopen() {
    const NAMES: usize = 60;
    let path = tmp_path("cursors-reopen");
    let mut db = Database::create(&path).unwrap();
    db.prepare_cursors().unwrap();
    db.table("t").number("id").primary_key("id").build().unwrap();
    let db = db.finish_with_oplog(16 << 20).unwrap();
    for run in 0..NAMES {
        let name = format!("c{run}");
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let hs: Vec<_> = (0..4u64)
            .map(|w| {
                let (db, name, barrier) = (db.clone(), name.clone(), barrier.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    db.cursor_set(&name, Cursor { kind: CURSOR_ANSWER, pos: 100 + w, hlc: None }).unwrap();
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        db.cursor_set(&name, Cursor { kind: CURSOR_ANSWER, pos: 999, hlc: None }).unwrap();
    }
    let before: Vec<Option<u64>> = (0..NAMES).map(|i| db.cursor_get(&format!("c{i}")).map(|c| c.pos)).collect();
    db.engine().oplog_sync().unwrap();
    drop(db);
    let db = Database::open(&path).unwrap();
    let after: Vec<Option<u64>> = (0..NAMES).map(|i| db.cursor_get(&format!("c{i}")).map(|c| c.pos)).collect();
    let wrong_before = before.iter().filter(|p| **p != Some(999)).count();
    let wrong_after = after.iter().filter(|p| **p != Some(999)).count();
    eprintln!("置いた直後に 999 でない名前 {wrong_before}/{NAMES}、 開き直した後に 999 でない名前 {wrong_after}/{NAMES} (例 {:?})", after.iter().find(|p| **p != Some(999)));
    drop(db);
    cleanup(&path);
    assert_eq!((wrong_before, wrong_after), (0, 0));
}

/// 仕組みの確かめ (1 本の thread): 消した行の eid に列を書くと (upsert が名前で引き当てた後に delete が入った時と同じ書き込み)、
/// 名前の無い行が残り、 他の列の読みに出る。 開き直した後も残るか。
#[test]
fn a_write_to_a_deleted_row_leaves_a_nameless_row() {
    let path = tmp_path("dead-write");
    let (n_all, n_a, after_all, after_a);
    {
        let mut db = Database::create(&path).unwrap();
        db.table("t").number("id").number("a").primary_key("id").build().unwrap();
        let t = db.get_table("t").unwrap();
        let e = t.insert().set("id", 1i64).set("a", 10i64).commit().unwrap();
        t.insert().set("id", 2i64).set("a", 20i64).commit().unwrap();
        t.entity(e).delete().unwrap();
        // upsert が (名前の lock の下で) e を引き当てた後に上の delete が入ると、 upsert は名前以外の列だけを e に書く
        let r = t.entity(e).set("a", 11i64).commit();
        n_all = t.all().count().unwrap();
        n_a = t.all().where_ge("a", 0i64).count().unwrap();
        eprintln!("消した行に a を書く -> {r:?}。 all().count()={n_all}、 where_ge(a,0).count()={n_a}、 find={:?}、 get(id)={:?}", t.all().where_ge("a", 0i64).find().unwrap(), t.entity(e).get("id"));
    }
    {
        let db = Database::open(&path).unwrap();
        let t = db.get_table("t").unwrap();
        after_all = t.all().count().unwrap();
        after_a = t.all().where_ge("a", 0i64).count().unwrap();
        eprintln!("開き直した後: all().count()={after_all}、 where_ge(a,0).count()={after_a}");
    }
    cleanup(&path);
    assert_eq!((n_all, n_a, after_all, after_a), (1, 1, 1, 1));
}
