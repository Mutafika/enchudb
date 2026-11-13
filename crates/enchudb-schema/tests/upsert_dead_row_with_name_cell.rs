//! 消えた行に名前の cell が残っている DB で、 その名前の upsert が返ること。
//!
//! 前提の行: 0.37.4 以前は消えた行への update が Ok で通り、 名前の cell が消えた行に残った (#701 の直しの前)。 engine 直の
//! `tie_to_by_id` でも作れる。 名前で引く読み (`query_by_id64`) は行が生きているかを見ないので、 その行を返す。
//!
//! #701 の直しの後の upsert は、 当たった行を握ってから 「生きていて、 まだその名前か」 を確かめ、 違えば引き直す。 引き直しても
//! 同じ消えた行が返るので、 名前の lock を握ったまま回り続けた。

use enchudb_schema::{Database, Value};
use std::sync::mpsc;
use std::time::Duration;

fn tmp_path(tag: &str) -> String {
    let dir = std::env::temp_dir();
    format!(
        "{}/enchudb-upsert-dead-{}-{}-{}",
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

/// その名前の生きた行。 名前で引く読みは消えた行に残った cell も返す (この試験の前提の形の DB では今もそう) ので、 生存で絞る。
fn live_named(t: &enchudb_schema::Table<'_>, eng: &enchudb_engine::Engine, id: i64) -> Vec<u64> {
    t.where_eq("id", id).find().unwrap().into_iter().filter(|&e| eng.is_live(e)).collect()
}

#[test]
fn an_upsert_returns_when_a_dead_row_still_holds_the_name() {
    let path = tmp_path("stale-name");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").number("a").primary_key("id").build().unwrap();
    let db = db.finish_with_oplog(16 << 20).unwrap();
    let t = db.get_table("t").unwrap();
    let e = t.insert().set("id", 1i64).set("a", 10i64).commit().unwrap();
    t.insert().set("id", 2i64).set("a", 20i64).commit().unwrap();
    t.entity(e).delete().unwrap();
    // 前提の行を作る: 消えた行に名前の cell を戻す (0.37.4 以前の update が残した形)
    let eng = db.engine();
    let id_himo = eng.himo_id("t.id").expect("t.id の紐") as u16;
    eng.tie_to_by_id(e, id_himo, 1u32);
    assert!(!eng.is_live(e), "前提: 行は消えたまま");
    assert_eq!(eng.get_by_id(e, id_himo), Some(1), "前提: 消えた行に名前の cell がある");
    assert!(eng.query_by_id64(&[(id_himo, 1)]).contains(&e), "前提: 名前で引くと消えた行が返る");

    let (tx, rx) = mpsc::channel();
    let db2 = db.clone();
    std::thread::spawn(move || {
        let t = db2.get_table("t").unwrap();
        let r = t.upsert().set("id", 1i64).set("a", 11i64).commit();
        let _ = tx.send(r);
    });
    let got = rx
        .recv_timeout(Duration::from_secs(20))
        .expect("upsert が 20 秒で返らない (消えた行を引き直し続けている)")
        .expect("upsert");
    assert_ne!(got, e, "upsert が消えた行の eid を返した");
    assert!(eng.is_live(got));
    let v = t.entity(got).get_many(&["id", "a"]);
    assert_eq!((num(v[0].clone()), num(v[1].clone())), (Some(1), Some(11)));
    assert_eq!(live_named(&t, eng, 1), vec![got], "その名前の生きた行は upsert の返した 1 行");
    // もう一度 upsert すると同じ行に当たる (消えた行が先に返っても、 生きた行を見つける)
    let again = t.upsert().set("id", 1i64).set("a", 12i64).commit().unwrap();
    assert_eq!(again, got);
    assert_eq!(num(t.entity(got).get("a")), Some(12));
    // insert は生きた行があるので断る
    assert!(t.insert().set("id", 1i64).commit().is_err());
}

/// insert と改名も、 消えた行に残った名前の cell を 「その名前の行がある」 と見ない。
#[test]
fn an_insert_and_a_rename_ignore_a_dead_row_holding_the_name() {
    let path = tmp_path("insert");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").number("a").primary_key("id").build().unwrap();
    let db = db.finish_with_oplog(16 << 20).unwrap();
    let t = db.get_table("t").unwrap();
    let eng = db.engine();
    let id_himo = eng.himo_id("t.id").expect("t.id の紐") as u16;
    let mut dead = Vec::new();
    for id in [1i64, 5] {
        let e = t.insert().set("id", id).set("a", 10i64).commit().unwrap();
        t.entity(e).delete().unwrap();
        eng.tie_to_by_id(e, id_himo, id as u32);
        assert!(!eng.is_live(e) && eng.query_by_id64(&[(id_himo, id as u64)]).contains(&e), "前提");
        dead.push(e);
    }
    let got = t.insert().set("id", 1i64).set("a", 11i64).commit().expect("消えた行の名前で insert");
    assert!(!dead.contains(&got) && eng.is_live(got));
    assert_eq!(live_named(&t, eng, 1), vec![got]);
    assert!(matches!(t.insert().set("id", 1i64).commit(), Err(enchudb_schema::SchemaError::DuplicatePk)));
    // 改名: 消えた行だけが持つ名前 5 にはなれる、 生きた行の名前 1 にはなれない
    let other = t.insert().set("id", 2i64).commit().unwrap();
    t.entity(other).update().set("id", 5i64).commit().expect("消えた行の名前への改名");
    assert_eq!(live_named(&t, eng, 5), vec![other]);
    assert!(t.entity(other).update().set("id", 1i64).commit().is_err());
}
