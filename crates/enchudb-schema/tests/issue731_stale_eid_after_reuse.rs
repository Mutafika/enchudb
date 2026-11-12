//! #731: 満杯の表で消えた行の eid を、 すぐ別の行に渡さない。
//!
//! `with_capacity(n)` の枠を使い切った表への insert は、 消えた行の eid を拾っていた。 アプリが持ち続けている古い eid
//! (`find()` / `commit()` が返した裸の u64) への update / delete が、 その eid をもらった別の名前の行に当たった。
//!
//! 直し (案 1): 枠を使い切ったら、 消えた eid を拾う前に、 空きの eid 空間から枠を足す。 拾うのは eid 空間も尽きた時だけ
//! (その形は残る = 最後の試験が今の姿を書き留める)。

use enchudb_schema::{Database, Value};

fn tmp_path(tag: &str) -> String {
    format!(
        "{}/enchudb-issue731-{}-{}-{}",
        std::env::temp_dir().display(),
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

const CAP: u32 = 8;

fn build(db: &mut Database) {
    db.table("t").number("id").number("a").primary_key("id").with_capacity(CAP).build().unwrap();
}

/// 枠 8 の表を 8 行で埋める。 返すのは行の eid (名前 i の行が i 番目)。
fn fill(db: &Database) -> Vec<u64> {
    let t = db.get_table("t").unwrap();
    let rows: Vec<u64> = (0..CAP as i64).map(|i| t.insert().set("id", i).set("a", i * 10).commit().unwrap()).collect();
    let u = db.engine().table_eid_usage("t").unwrap();
    assert_eq!((u.capacity, u.live, u.free), (CAP, CAP, 0), "前提: 表は自分の枠を使い切っている");
    assert!(db.remaining_eid_capacity() > 0, "前提: 空きの eid 空間は残っている");
    rows
}

fn row(db: &Database, eid: u64) -> Vec<Option<Value>> {
    db.get_table("t").unwrap().entity(eid).get_many(&["id", "a"])
}

/// 古い eid への update / delete が、 後から足した行を変えない。
fn stale_writes_miss(db: &Database, old: u64, new: u64) {
    let t = db.get_table("t").unwrap();
    assert_ne!(old, new, "満杯の表の insert が、 消えた行の eid を別の名前の行に渡した");
    let before = row(db, new);
    assert_eq!(before, vec![Some(Value::Number(100)), Some(Value::Number(1000))], "前提: 足した行の中身");
    let n = t.all().count().unwrap();
    // 名前 3 の行のつもりの update
    let _ = t.entity(old).set("a", 7i64).commit();
    assert_eq!(row(db, new), before, "古い eid への update が別の名前の行に書いた");
    assert_eq!(t.where_eq("a", 7i64).count().unwrap(), 0, "古い eid への update が行を作った / 書き換えた");
    // 名前 3 の行のつもりの delete
    let _ = t.entity(old).delete();
    assert_eq!(t.where_eq("id", 100i64).find().unwrap(), vec![new], "古い eid への delete が別の名前の行を消した");
    assert_eq!(row(db, new), before);
    assert_eq!(t.all().count().unwrap(), n, "行の数");
}

#[test]
fn a_full_table_does_not_hand_a_deleted_eid_to_another_row() {
    let path = tmp_path("stale");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    build(&mut db);
    let rows = fill(&db);
    let t = db.get_table("t").unwrap();
    let old = rows[3]; // アプリが持ち続ける eid (名前 3 の行)
    t.entity(old).delete().unwrap();
    let new = t.insert().set("id", 100i64).set("a", 1000i64).commit().unwrap();
    assert!(!rows.contains(&new), "足した行の eid は、 この表が払い出した事のない eid");
    stale_writes_miss(&db, old, new);
    assert!(db.engine().table_eid_usage("t").unwrap().capacity > CAP, "枠を足した");
}

/// 開き直した後 (空きの一覧はメモリだけ、 払い出し位置は保存から) も同じ。
#[test]
fn the_same_after_reopen() {
    let path = tmp_path("reopen");
    let _c = Cleanup(path.clone());
    let rows;
    {
        let mut db = Database::create(&path).unwrap();
        build(&mut db);
        rows = fill(&db);
        db.get_table("t").unwrap().entity(rows[3]).delete().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let t = db.get_table("t").unwrap();
    assert_eq!(t.all().count().unwrap(), CAP as usize - 1, "前提: 消した行は開き直しても無い");
    let new = t.insert().set("id", 100i64).set("a", 1000i64).commit().unwrap();
    assert!(!rows.contains(&new));
    stale_writes_miss(&db, rows[3], new);
}

/// WAL 付き (concurrent) で、 きれいに閉じずに開き直しても同じ (WAL の再生が払い出し位置と足した枠を戻す)。
#[test]
fn the_same_with_a_wal_and_after_replay() {
    let path = tmp_path("wal");
    let _c = Cleanup(path.clone());
    let (rows, new);
    {
        let mut db = Database::create(&path).unwrap();
        build(&mut db);
        let db = db.finish_with_oplog(4 << 20).unwrap();
        rows = fill(&db);
        let t = db.get_table("t").unwrap();
        t.entity(rows[3]).delete().unwrap();
        new = t.insert().set("id", 100i64).set("a", 1000i64).commit().unwrap();
        stale_writes_miss(&db, rows[3], new);
        db.engine().oplog_sync().unwrap();
    }
    let db = Database::open_with_oplog(&path, 4 << 20).unwrap();
    let t = db.get_table("t").unwrap();
    assert_eq!(t.where_eq("id", 100i64).find().unwrap(), vec![new], "前提: 足した行は開き直しても同じ eid");
    // 開き直した後に消して足しても、 今までに払い出した eid は渡さない
    t.entity(rows[5]).delete().unwrap();
    let newer = t.insert().set("id", 200i64).set("a", 2000i64).commit().unwrap();
    assert!(!rows.contains(&newer) && newer != new, "開き直した後の insert が払い出し済みの eid を渡した ({newer:#x})");
    assert_eq!(row(&db, new), vec![Some(Value::Number(100)), Some(Value::Number(1000))]);
}

/// 消しては足すと、 生きている行は 8 のまま、 枠は足した回数に比例して伸びる (案 1 の値段)。 extent は 1 本に続く。
#[test]
fn churn_takes_eid_space_and_never_repeats_an_eid() {
    let path = tmp_path("churn");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    build(&mut db);
    let mut rows = fill(&db);
    let t = db.get_table("t").unwrap();
    let mut seen: std::collections::HashSet<u64> = rows.iter().copied().collect();
    const ROUNDS: usize = 5_000;
    for r in 0..ROUNDS {
        let i = r % rows.len();
        t.entity(rows[i]).delete().unwrap();
        let e = t.insert().set("id", 1_000 + r as i64).set("a", r as i64).commit().unwrap();
        assert!(seen.insert(e), "{r} 回目: 払い出し済みの eid {e:#x} をもう一度渡した");
        rows[i] = e;
    }
    let u = db.engine().table_eid_usage("t").unwrap();
    assert_eq!(u.live, CAP);
    assert!(u.capacity as usize >= CAP as usize + ROUNDS, "枠 {} (払い出した eid は {})", u.capacity, CAP as usize + ROUNDS);
    let ext = db.engine().table_eid_extents("t").unwrap();
    assert!(ext.len() <= 2, "続きの extent は 1 本に伸ばす: {} 本", ext.len());
}

/// 残る形: eid 空間も尽きた DB では、 今までどおり消えた eid を拾う (insert は失敗しない)。 その時だけ #731 は残る。
#[test]
fn reuse_starts_only_when_the_eid_space_is_gone() {
    let path = tmp_path("exhausted");
    let _c = Cleanup(path.clone());
    let mut db = Database::create_with_capacity(&path, 32).unwrap();
    build(&mut db);
    let mut rows = fill(&db);
    let t = db.get_table("t").unwrap();
    let mut seen: std::collections::HashSet<u64> = rows.iter().copied().collect();
    let mut fresh = 0;
    let mut reused_at = None;
    for r in 0..64usize {
        let i = r % rows.len();
        t.entity(rows[i]).delete().unwrap();
        let space = db.remaining_eid_capacity();
        let cap = db.engine().table_eid_usage("t").unwrap();
        let e = t.insert().set("id", 1_000 + r as i64).commit().expect("eid 空間が尽きた後も insert は通る");
        if seen.insert(e) {
            fresh += 1;
            assert!(reused_at.is_none(), "使い回しが始まった後に新しい eid");
        } else {
            // 使い回したのは、 足す枠が無く (eid 空間 0)、 今ある枠も払い出し切った時だけ
            assert_eq!(space, 0, "{r} 回目: eid 空間が {space} 残っているのに eid {e:#x} を使い回した");
            assert_eq!(cap.allocated, cap.capacity, "{r} 回目: 枠が残っているのに使い回した");
            reused_at.get_or_insert(r);
        }
        rows[i] = e;
    }
    assert_eq!(fresh, 32 - CAP as usize, "eid 空間 32 を全部払い出してから使い回す");
    assert!(reused_at.is_some(), "前提: eid 空間が尽きて使い回しが始まった");
    assert_eq!(t.all().count().unwrap(), CAP as usize);
}
