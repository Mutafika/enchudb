//! #701: upsert と delete が同じ名前で重なっても、 名前の無い行を作らない。
//!
//! 窓: upsert が名前で行 e を引く → delete が e を消す → upsert が e の行の lock を取って書く。 当たった行には名前のカードを
//! 書かない (#581) ので、 前は消えた e に名前以外の列だけが乗り、 `Ok(e)` が返って名前の無い行が残った (表の全部を引く時は
//! 出ず、 他の列で引く・数える時は出る。 開き直しても残る)。
//!
//! 直し: upsert は行の lock を握ってから 「まだ生きていて、 まだその名前か」 を確かめ、 違えば引き直す。 消えた行への update
//! (`EntityRef::set` / `update`) は断る。
//!
//! 窓を踏んだかは `Table::upsert_relookups` (引き直した回数) で見る。 試験は窓を踏むまで回し、 踏んだ回の結果を確かめる
//! (踏まずに通らない)。

use enchudb_schema::{Database, Value};
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

fn tmp_path(tag: &str) -> String {
    let dir = std::env::temp_dir();
    format!(
        "{}/enchudb-issue701-{}-{}-{}",
        dir.display(),
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn num(v: Option<Value>) -> Option<i64> {
    match v {
        Some(Value::Number(n)) => Some(n),
        _ => None,
    }
}

/// 消した行への update は断り、 何も書かない (前は Ok で、 名前の無い行が残った)。 開き直した後も同じ。
#[test]
fn an_update_of_a_deleted_row_is_refused_and_writes_nothing() {
    let path = tmp_path("dead-write");
    let _c = Cleanup(path.clone());
    {
        let mut db = Database::create(&path).unwrap();
        db.table("t").number("id").number("a").primary_key("id").build().unwrap();
        let t = db.get_table("t").unwrap();
        let e = t.insert().set("id", 1i64).set("a", 10i64).commit().unwrap();
        t.insert().set("id", 2i64).set("a", 20i64).commit().unwrap();
        t.entity(e).delete().unwrap();
        let r = t.entity(e).set("a", 11i64).commit();
        assert!(r.is_err(), "消えた行への update が通った: {r:?}");
        let r = t.entity(e).update().set("a", 12i64).set("id", 3i64).commit();
        assert!(r.is_err(), "消えた行への update (名前つき) が通った: {r:?}");
        assert_eq!(t.entity(e).get("a"), None, "消えた行に列が乗った");
        assert_eq!(t.entity(e).get("id"), None);
        assert_eq!(t.all().count().unwrap(), 1);
        assert_eq!(t.all().where_ge("a", 0i64).find().unwrap().len(), 1, "名前の無い行が他の列の読みに出る");
        assert_eq!(t.where_eq("id", 3i64).count().unwrap(), 0);
    }
    let db = Database::open(&path).unwrap();
    let t = db.get_table("t").unwrap();
    assert_eq!(t.all().count().unwrap(), 1);
    assert_eq!(t.all().where_ge("a", 0i64).find().unwrap().len(), 1, "開き直した後に名前の無い行が出る");
}

/// 窓を 1 回ずつ作る: A が行 e の lock を握る → B が同じ名前を upsert (名前で e を引き、 e の lock で待つ) → A が握ったまま
/// e を消して放す。 B が e を引いた後だったか (窓を踏んだか) は引き直しの数で分かる。 踏んだ回は、 upsert の返した行が
/// 生きていて、 その名前と書いた値を持ち、 消えた e には何も乗っていないこと。
#[test]
fn an_upsert_that_lost_its_row_to_a_delete_writes_a_named_row() {
    let path = tmp_path("window");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").number("a").number("b").primary_key("id").build().unwrap();
    let db = db.finish_with_oplog(16 << 20).unwrap();
    let t = db.get_table("t").unwrap();
    const WANT: u64 = 5;
    const MAX_ROUNDS: i64 = 20_000;
    let mut hits = 0u64;
    let mut rounds = 0i64;
    while hits < WANT && rounds < MAX_ROUNDS {
        let id = rounds;
        rounds += 1;
        let e = t.insert().set("id", id).set("a", 1i64).set("b", 1i64).commit().unwrap();
        let before = t.upsert_relookups();
        let started = AtomicBool::new(false);
        let (got, row_gone) = std::thread::scope(|s| {
            let row = db.engine().write_row(e);
            let h = s.spawn(|| {
                let t = db.get_table("t").unwrap();
                started.store(true, Ordering::Release);
                t.upsert().set("id", id).set("a", 7i64).set("b", 7i64).commit().unwrap()
            });
            while !started.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            // B が名前で e を引くまでの間を少し空ける (空けなくても正しい: 引く前に消えたら窓を踏まない回になるだけ)
            for _ in 0..(rounds % 64) * 8 {
                std::thread::yield_now();
            }
            t.entity(e).delete().unwrap(); // 行の lock は同じ thread からは入れ子で握れる
            let gone = !db.engine().is_live(e);
            drop(row);
            (h.join().unwrap(), gone)
        });
        assert!(row_gone);
        if t.upsert_relookups() == before {
            continue; // B が引く前に消えた (窓を踏んでいない)
        }
        hits += 1;
        assert_ne!(got, e, "upsert が消えた行の eid を返した");
        assert!(db.engine().is_live(got), "upsert の返した行が生きていない");
        let v = t.entity(got).get_many(&["id", "a", "b"]);
        assert_eq!(
            (num(v[0].clone()), num(v[1].clone()), num(v[2].clone())),
            (Some(id), Some(7), Some(7)),
            "upsert の返した行の中身"
        );
        assert_eq!(t.entity(e).get("a"), None, "消えた行に列が乗った");
        assert_eq!(t.where_eq("id", id).find().unwrap(), vec![got], "その名前の行は upsert の返した 1 行");
    }
    assert!(hits >= WANT, "{rounds} 回回して窓を {hits} 回しか踏めなかった (試験の前提)");
    // 名前の無い行が無い: 他の列で引ける行は全部、 表の全部にも居る
    let all: HashSet<u64> = t.all().find().unwrap().into_iter().collect();
    let with_a: Vec<u64> = t.all().where_ge("a", 0i64).find().unwrap();
    let nameless: Vec<&u64> = with_a.iter().filter(|e| !all.contains(e)).collect();
    assert!(nameless.is_empty(), "名前の無い行 {} つ (例 {:#x})", nameless.len(), nameless[0]);
    assert_eq!(t.all().count().unwrap(), t.all().where_ge("a", 0i64).count().unwrap());
}

/// 消えた eid が別の名前の insert に使い回された時: 窓の中の upsert は、 その行に自分の値を混ぜない。
/// 表を満杯にしておく (満杯の表の insert は消えた行の eid を使い回す)。 A が e を握ったまま消し、 同じ thread で別の名前 Y を
/// insert して e を使い回させてから放す。 B (名前 X の upsert) は e が生きているのを見るが、 名前が違うので引き直す。
#[test]
fn an_upsert_does_not_write_into_a_reused_eid_of_another_name() {
    const WANT: u64 = 3;
    const MAX_ROUNDS: u64 = 3_000;
    const CAP: i64 = 8;
    let mut hits = 0u64;
    let mut reused = 0u64;
    let mut rounds = 0u64;
    while hits < WANT && rounds < MAX_ROUNDS {
        rounds += 1;
        let path = tmp_path(&format!("reuse{rounds}"));
        let _c = Cleanup(path.clone());
        let mut db = Database::create(&path).unwrap();
        db.table("t").number("id").number("a").primary_key("id").with_capacity(CAP as u32).build().unwrap();
        let db = db.finish_with_oplog(1 << 20).unwrap();
        let t = db.get_table("t").unwrap();
        let mut rows = Vec::new();
        for id in 0..CAP {
            rows.push(t.insert().set("id", id).commit().unwrap());
        }
        let e = rows[3]; // 名前 3
        let started = AtomicBool::new(false);
        let (got, y) = std::thread::scope(|s| {
            let row = db.engine().write_row(e);
            let h = s.spawn(|| {
                let t = db.get_table("t").unwrap();
                started.store(true, Ordering::Release);
                t.upsert().set("id", 3i64).set("a", 7i64).commit()
            });
            while !started.load(Ordering::Acquire) {
                std::thread::yield_now();
            }
            for _ in 0..(rounds % 64) * 8 {
                std::thread::yield_now();
            }
            t.entity(e).delete().unwrap();
            let y = t.insert().set("id", 100i64).commit().unwrap(); // 満杯なので e を使い回す
            drop(row);
            (h.join().unwrap(), y)
        });
        if y != e {
            continue; // 使い回されなかった (この回は数えない)
        }
        reused += 1;
        if t.upsert_relookups() == 0 {
            // B が引く前に消えた (窓を踏んでいない)。 それでも Y の行に値は付かない
            assert_eq!(t.entity(y).get("a"), None);
            continue;
        }
        hits += 1;
        assert_eq!(num(t.entity(y).get("id")), Some(100), "使い回した行の名前");
        assert_eq!(t.entity(y).get("a"), None, "別の名前の行に upsert の値が混ざった");
        // B は新しい行を作るか、 表が満杯で断られるかのどちらか。 Ok なら、 その行は名前 3 と値を持つ
        if let Ok(got) = got {
            assert_ne!(got, y);
            assert_eq!(num(t.entity(got).get("id")), Some(3));
            assert_eq!(num(t.entity(got).get("a")), Some(7));
        }
        assert!(t.where_eq("id", 3i64).count().unwrap() <= 1);
        assert_eq!(t.where_eq("id", 100i64).find().unwrap(), vec![y]);
    }
    assert!(reused > 0, "{rounds} 回回して eid が 1 度も使い回されなかった (試験の前提)");
    assert!(hits >= WANT, "{rounds} 回回して窓を {hits} 回しか踏めなかった (試験の前提、 使い回し {reused} 回)");
}

/// 書き手を並べて回す (issue の再現)。 窓を決めた回数踏むまで回し、 終わってから状態で見る:
/// (1) 他の列で引ける行は全部、 表の全部 (名前のある行) にも居る (2) 名前ごとに 1 行 (3) 行の中身はその名前の書き手が書いた
/// 値だけ (4) 名前だけで作った行に a / b の値が付いていない。
#[test]
fn upserts_racing_deletes_of_the_same_names_leave_no_nameless_row() {
    let path = tmp_path("race");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").number("a").number("b").primary_key("id").build().unwrap();
    let db = db.finish_with_oplog(64 << 20).unwrap();
    const NAMES: i64 = 4;
    const WANT: u64 = 200;
    const MAX_DELETES: u64 = 2_000_000;
    let stop = Arc::new(AtomicBool::new(false));
    let inherited = Arc::new(AtomicU64::new(0));
    let deletes = Arc::new(AtomicU64::new(0));
    let start = Arc::new(Barrier::new(NAMES as usize + 3));
    let mut hs = Vec::new();
    for w in 0..NAMES {
        let (db, stop, start) = (db.clone(), stop.clone(), start.clone());
        hs.push(std::thread::spawn(move || {
            let t = db.get_table("t").unwrap();
            start.wait();
            let mut i = 0i64;
            while !stop.load(Ordering::Relaxed) {
                let v = w * 1000 + (i % 1000);
                t.upsert().set("id", w).set("a", v).set("b", v).commit().unwrap();
                i += 1;
            }
        }));
    }
    for _ in 0..2 {
        let (db, stop, deletes, start) = (db.clone(), stop.clone(), deletes.clone(), start.clone());
        hs.push(std::thread::spawn(move || {
            let t = db.get_table("t").unwrap();
            start.wait();
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
    // 名前だけの行を作って消す書き手 (消えた行の eid を使い回すことがあれば、 値が付いて見える)
    {
        let (db, stop, inherited, start) = (db.clone(), stop.clone(), inherited.clone(), start.clone());
        hs.push(std::thread::spawn(move || {
            let t = db.get_table("t").unwrap();
            start.wait();
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
    let t = db.get_table("t").unwrap();
    // 窓を WANT 回踏むまで (時間でなく状態で止める。 踏めないまま delete が上限に届いたら前提落ち)
    while t.upsert_relookups() < WANT && deletes.load(Ordering::Relaxed) < MAX_DELETES {
        std::thread::yield_now();
    }
    stop.store(true, Ordering::Relaxed);
    for h in hs {
        h.join().unwrap();
    }
    let hits = t.upsert_relookups();
    let n_del = deletes.load(Ordering::Relaxed);
    assert!(hits >= WANT, "delete {n_del} 回で窓を {hits} 回しか踏めなかった (試験の前提)");

    let all: Vec<u64> = t.all().find().unwrap();
    let all_set: HashSet<u64> = all.iter().copied().collect();
    let with_a: Vec<u64> = t.all().where_ge("a", 0i64).find().unwrap();
    let nameless: Vec<u64> = with_a.iter().copied().filter(|e| !all_set.contains(e)).collect();
    assert!(
        nameless.is_empty(),
        "名前の無い行 {} つ (例 {:#x}: id={:?} a={:?})、 all={} where_ge(a,0)={} (delete {n_del} 回、 窓 {hits} 回)",
        nameless.len(),
        nameless[0],
        t.entity(nameless[0]).get("id"),
        t.entity(nameless[0]).get("a"),
        all.len(),
        with_a.len(),
    );
    let mut by_name: BTreeMap<i64, Vec<u64>> = BTreeMap::new();
    for &e in &all {
        let v = t.entity(e).get_many(&["id", "a", "b"]);
        let (id, a, b) = (num(v[0].clone()), num(v[1].clone()), num(v[2].clone()));
        let id = id.unwrap_or_else(|| panic!("表の全部に出た行 {e:#x} に名前が無い"));
        by_name.entry(id).or_default().push(e);
        assert!(a == b && a.is_none_or(|a| a / 1000 == id), "行 {e:#x} の中身が混ざった: id={id} a={a:?} b={b:?}");
    }
    for (id, es) in &by_name {
        assert_eq!(es.len(), 1, "名前 {id} の行が {} つ", es.len());
    }
    assert_eq!(inherited.load(Ordering::Relaxed), 0, "名前だけで insert した行に a / b の値が付いていた");
}
