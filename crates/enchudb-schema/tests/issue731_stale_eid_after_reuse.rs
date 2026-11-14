//! #731: 消えた行の場所を使い回しても、 古い eid (アプリが持ち続けた裸の u64) は別の行に当たらない。
//!
//! 満杯の表 (`with_capacity(n)` の枠を使い切った表) への insert は、 消えた行の場所 (slot) を使い回す。 前は eid = 場所だったので、
//! 古い eid への update / delete / get が、 その場所をもらった別の名前の行に当たった。
//!
//! 直し: eid の local は 「世代 | 場所」。 使い回した行は世代の違う eid になり、 古い eid は入口の照合で 「消えた行」 になる
//! (get は None、 update / delete は何もしない)。 世代を使い切った場所は二度と渡さない。
//!
//! 試験は場所が使い回された事を状態で確かめる (表の枠が伸びていない = 消えた場所を拾った)。

use enchudb_schema::{Database, Value};

fn tmp_path(tag: &str) -> String {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    format!(
        "{}/enchudb-issue731-{}-{}-{}-{}",
        std::env::temp_dir().display(),
        tag,
        std::process::id(),
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let dest = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &dest);
        } else {
            std::fs::copy(entry.path(), dest).unwrap();
        }
    }
}

const CAP: u32 = 8;
/// 既定の DB (max_entities 2^24) の場所の bit。
const SLOT_MASK: u32 = (1 << 24) - 1;

fn slot(eid: u64) -> u32 {
    (eid as u32) & SLOT_MASK
}

fn build(db: &mut Database) {
    db.table("t").number("id").number("a").primary_key("id").with_capacity(CAP).build().unwrap();
}

/// 枠 8 の表を 8 行で埋める。 返すのは行の eid (名前 i の行が i 番目)。
fn fill(db: &Database) -> Vec<u64> {
    let t = db.get_table("t").unwrap();
    let rows: Vec<u64> = (0..CAP as i64).map(|i| t.insert().set("id", i).set("a", i * 10).commit().unwrap()).collect();
    let u = db.engine().table_eid_usage("t").unwrap();
    assert_eq!((u.capacity, u.live, u.free), (CAP, CAP, 0), "前提: 表は自分の枠を使い切っている");
    rows
}

fn row(db: &Database, eid: u64) -> Vec<Option<Value>> {
    db.get_table("t").unwrap().entity(eid).get_many(&["id", "a"])
}

/// `new` は `old` の場所を使い回した行 (前提) で、 古い eid への読み・update・delete がその行に当たらない。
fn stale_eid_misses(db: &Database, old: u64, new: u64) {
    let t = db.get_table("t").unwrap();
    assert_eq!(slot(new), slot(old), "前提: 足した行は消えた行の場所を使い回した");
    assert_eq!(db.engine().table_eid_usage("t").unwrap().capacity, CAP, "前提: 枠は足していない");
    assert_ne!(old, new, "使い回した場所の行に、 消えた行と同じ eid を渡した");
    let before = row(db, new);
    assert_eq!(before, vec![Some(Value::Number(100)), Some(Value::Number(1000))], "前提: 足した行の中身");
    let n = t.all().count().unwrap();
    // 名前 3 の行のつもりの読み
    assert_eq!(row(db, old), vec![None, None], "古い eid の読みが、 場所を使い回した行を読んだ");
    assert!(!db.engine().is_live(old), "古い eid が生きている");
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
fn a_reused_slot_gets_a_new_eid_and_the_old_eid_misses_it() {
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
    stale_eid_misses(&db, old, new);
    // find の出口も今の世代を付ける
    assert_eq!(t.where_eq("id", 100i64).find().unwrap(), vec![new]);
    assert!(t.all().find().unwrap().contains(&new));
}

/// 開き直した後 (空きの一覧はメモリだけ、 世代は gen.seg) も同じ。
#[test]
fn the_same_after_reopen() {
    let path = tmp_path("reopen");
    let _c = Cleanup(path.clone());
    let (rows, new);
    {
        let mut db = Database::create(&path).unwrap();
        build(&mut db);
        rows = fill(&db);
        let t = db.get_table("t").unwrap();
        t.entity(rows[3]).delete().unwrap();
        new = t.insert().set("id", 100i64).set("a", 1000i64).commit().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let t = db.get_table("t").unwrap();
    assert_eq!(t.where_eq("id", 100i64).find().unwrap(), vec![new], "開き直しても足した行は同じ eid");
    stale_eid_misses(&db, rows[3], new);
    // 開き直した後に消して足しても同じ
    t.entity(rows[5]).delete().unwrap();
    let newer = t.insert().set("id", 200i64).set("a", 2000i64).commit().unwrap();
    assert_eq!(slot(newer), slot(rows[5]), "前提: 開き直した後も場所を使い回す");
    assert_ne!(newer, rows[5]);
    assert_eq!(row(&db, rows[5]), vec![None, None]);
}

/// WAL 付き (concurrent) で、 きれいに閉じずに落ちた像を開いても同じ (世代は WAL の record の eid から戻る)。
#[test]
fn the_same_after_a_crash_with_a_wal() {
    let path = tmp_path("wal");
    let image = format!("{path}-image");
    let (_c1, _c2) = (Cleanup(path.clone()), Cleanup(image.clone()));
    let (rows, new);
    {
        let mut db = Database::create(&path).unwrap();
        build(&mut db);
        let db = db.finish_with_oplog(4 << 20).unwrap();
        let hold = db.engine().hold_consumer();
        rows = fill(&db);
        let t = db.get_table("t").unwrap();
        t.entity(rows[3]).delete().unwrap();
        new = t.insert().set("id", 100i64).set("a", 1000i64).commit().unwrap();
        db.engine().oplog_sync().unwrap();
        // 落ちた像: 書き出しの済んだ所で写す (閉じない)
        copy_dir(std::path::Path::new(&path), std::path::Path::new(&image));
        drop(hold);
    }
    let db = Database::open_with_oplog(&image, 4 << 20).unwrap();
    let t = db.get_table("t").unwrap();
    assert_eq!(t.where_eq("id", 100i64).find().unwrap(), vec![new], "落ちた後も足した行は同じ eid");
    stale_eid_misses(&db, rows[3], new);
}

/// 消しては足すと場所を使い回し、 eid は二度と同じにならない。 世代を使い切った場所は渡さず、 枠を足して続ける。
#[test]
fn churn_never_repeats_an_eid() {
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
        assert_eq!(row(&db, rows[i]), vec![None, None], "{r} 回目: 消えた行の eid が読める");
        rows[i] = e;
    }
    let u = db.engine().table_eid_usage("t").unwrap();
    assert_eq!(u.live, CAP);
    // 空きは、 まだ払い出していない場所だけ (払い出し済みの死んだ場所は全部引退 = 引退していない場所は枠を足す前に使い回す)。
    // 引退した場所 (二度と渡さない) は空きに数えない (CLAUDE.md の 「満杯 = remaining_eid_capacity() == 0 かつ free == 0」)
    assert_eq!(u.free, u.capacity - u.allocated, "引退した場所を空きに数えた ({u:?})");
    // 1 つの場所は 255 回 (世代 0..=254) 使える。 5000 回なら場所は 8 + 5000 / 254 + 1 個ほど
    assert!(u.capacity <= CAP + (ROUNDS as u32) / 254 + 8, "枠 {} (世代を使い切る前に場所を捨てた)", u.capacity);
}

/// eid 空間も尽きた DB では、 全部の場所の世代を使い切ったら insert を断る (同じ eid を二度渡さない)。
#[test]
fn an_exhausted_db_refuses_rather_than_repeating_an_eid() {
    let path = tmp_path("exhausted");
    let _c = Cleanup(path.clone());
    let mut db = Database::create_with_capacity(&path, 32).unwrap();
    build(&mut db);
    let mut rows = fill(&db);
    let t = db.get_table("t").unwrap();
    let mut seen: std::collections::HashSet<u64> = rows.iter().copied().collect();
    let mut refused = None;
    for r in 0..20_000usize {
        let i = r % rows.len();
        t.entity(rows[i]).delete().unwrap();
        match t.insert().set("id", 1_000 + r as i64).commit() {
            Ok(e) => {
                assert!(seen.insert(e), "{r} 回目: 払い出し済みの eid {e:#x} をもう一度渡した");
                rows[i] = e;
            }
            Err(_) => {
                refused = Some(r);
                break;
            }
        }
    }
    let r = refused.expect("全部の場所の世代を使い切ったら断る");
    assert!(r >= 8 * 254, "{r} 回目で断った (表の 8 場所だけでも 8 × 254 回は使い回せる)");
    assert_eq!(t.all().count().unwrap(), CAP as usize - 1, "断った insert は行を残さない");
    // 満杯の見分け (CLAUDE.md): 残りの eid 空間 0 かつ空き 0
    let u = db.engine().table_eid_usage("t").unwrap();
    assert_eq!((db.engine().remaining_eid_capacity(), u.free), (0, 0), "断った表の空きが 0 でない ({u:?})");
}

/// issue の再現 (前の形): 古い eid への update は、 場所を使い回した行に書かない。
#[test]
fn a_stale_eid_does_not_write_into_the_row_that_reused_its_slot() {
    let path = tmp_path("issue");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").number("a").primary_key("id").with_capacity(8).build().unwrap();
    let t = db.get_table("t").unwrap();
    let rows: Vec<u64> = (0..8i64).map(|i| t.insert().set("id", i).set("a", i * 10).commit().unwrap()).collect();
    let old = rows[3];
    t.entity(old).delete().unwrap();
    let new = t.insert().set("id", 100i64).set("a", 1000i64).commit().unwrap();
    assert_eq!(slot(old), slot(new), "前提: 満杯の表の insert が消えた場所を使い回す");
    let _ = t.entity(old).set("a", 7i64).commit();
    let _ = t.entity(old).delete();
    assert_eq!(t.entity(new).get_many(&["id", "a"]), vec![Some(Value::Number(100)), Some(Value::Number(1000))]);
    assert_eq!(t.where_eq("id", 100i64).count().unwrap(), 1);
}

/// 消えた行を指したままの ref は、 その場所を使い回した行を指さない。 読みは 「中身の無い行を指す ref」 のまま (世代を持つ前の
/// DB で消した行を指す ref と同じ): `company.city is null` は真、 `company.city = X` は偽、 `company is null` は偽。 find と購読の
/// どちらも、 世代を持つ前 (v14) に消した行・場所を使い回した後・世代を持った後 (v15) に消した行で同じ。
#[test]
fn a_ref_to_a_deleted_row_reads_as_a_ref_to_an_empty_row() {
    use std::collections::BTreeSet;
    let path = tmp_path("ref");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("companies").number("id").tag("city").primary_key("id").with_capacity(4).build().unwrap();
    db.table("users").number("id").ref_to("company", "companies").primary_key("id").build().unwrap();
    let c = db.get_table("companies").unwrap();
    let u = db.get_table("users").unwrap();
    let cs: Vec<u64> = (0..4i64).map(|i| c.insert().set("id", i).set("city", "Tokyo").commit().unwrap()).collect();
    let u0 = u.insert().set("id", 0i64).set("company", Value::Ref(cs[1])).commit().unwrap();
    let u1 = u.insert().set("id", 1i64).set("company", Value::Ref(cs[2])).commit().unwrap();
    let u2 = u.insert().set("id", 2i64).commit().unwrap();
    let null_city = || u.all().where_null("company.city");
    let tokyo = || u.where_eq("company.city", "Tokyo");
    let null_co = || u.all().where_null("company");
    let subs = [null_city().subscribe().unwrap(), tokyo().subscribe().unwrap(), null_co().subscribe().unwrap()];
    let mut seen: Vec<BTreeSet<u64>> = vec![BTreeSet::new(); 3];
    let check = |seen: &mut Vec<BTreeSet<u64>>, want: [&[u64]; 3], when: &str| {
        let finds = [null_city().find().unwrap(), tokyo().find().unwrap(), null_co().find().unwrap()];
        for (i, s) in subs.iter().enumerate() {
            let d = s.poll();
            for e in d.removed {
                assert!(seen[i].remove(&e), "{when}: [{i}] removed に渡していない eid");
            }
            seen[i].extend(d.added);
            let w: BTreeSet<u64> = want[i].iter().copied().collect();
            assert_eq!(seen[i], w, "{when}: [{i}] 購読 (0 = company.city is null, 1 = company.city = Tokyo, 2 = company is null)");
            assert_eq!(finds[i].iter().copied().collect::<BTreeSet<u64>>(), w, "{when}: [{i}] find");
        }
    };
    check(&mut seen, [&[], &[u0, u1], &[u2]], "最初");
    // 世代を持つ前 (使い回しの無い DB) に消した会社を指す ref
    c.entity(cs[1]).delete().unwrap();
    assert_eq!(db.engine().slot_bits(), 0, "前提: まだ使い回していない");
    assert_eq!(u.entity(u0).get("company"), Some(Value::Ref(cs[1])), "消えた会社を指したまま");
    check(&mut seen, [&[u0], &[u1], &[u2]], "消した後");
    // 満杯の会社の表に足す = 消えた会社の場所を使い回す (Tokyo の会社)
    let n = c.insert().set("id", 9i64).set("city", "Tokyo").commit().unwrap();
    assert_eq!(slot(n), slot(cs[1]), "前提: 消えた会社の場所を使い回した");
    assert_ne!(n, cs[1]);
    assert!(db.engine().slot_bits() > 0, "前提: 世代を持つ DB (v15) になった");
    assert_eq!(u.where_ref("company", n).find().unwrap(), Vec::<u64>::new(), "消えた会社を指す ref が、 場所を使い回した会社を指した");
    check(&mut seen, [&[u0], &[u1], &[u2]], "場所を使い回した後");
    // 世代を持った後に消した会社を指す ref (使い回しの前)
    c.entity(cs[2]).delete().unwrap();
    check(&mut seen, [&[u0, u1], &[], &[u2]], "v15 で消した後");
    // 新しい会社を指せば普通の ref
    u.entity(u0).set("company", Value::Ref(n)).commit().unwrap();
    check(&mut seen, [&[u1], &[u0], &[u2]], "使い回した会社を指し直した後");
}

/// 読み直し E: 見直しの購読 (`subscribe_touched`) の `touched` は、 アプリが `members_many` で受けた eid を出す。 前は今の世代の eid
/// を出したので、 社員を消すと (世代 + 1) の eid、 poll の前に場所を使い回すと新しい住人の eid だけが出て、 アプリは持っている eid を
/// 外せなかった。 v14 の DB で消して、 最初の使い回しで v15 に移る形 (移る時に上げる場所) と、 v15 の DB で消す形の両方。
#[test]
fn touched_reports_the_eid_the_app_holds_after_a_delete_and_a_reuse() {
    for v15_first in [false, true] {
        let path = tmp_path("touched");
        let _c = Cleanup(path.clone());
        let mut db = Database::create(&path).unwrap();
        db.table("companies").number("id").tag("city").primary_key("id").build().unwrap();
        db.table("users").number("id").ref_to("company", "companies").primary_key("id").with_capacity(2).build().unwrap();
        db.table("spare").number("id").primary_key("id").with_capacity(1).build().unwrap();
        let companies = db.get_table("companies").unwrap();
        let users = db.get_table("users").unwrap();
        if v15_first {
            // 別の表で先に使い回す (v15 に移る)
            let spare = db.get_table("spare").unwrap();
            let s = spare.insert().set("id", 1i64).commit().unwrap();
            spare.entity(s).delete().unwrap();
            let s2 = spare.insert().set("id", 2i64).commit().unwrap();
            assert_eq!(slot(s), slot(s2), "前提: 使い回した");
            assert_ne!(s, s2, "前提: v15");
        }
        let c = companies.insert().set("id", 1i64).set("city", "Tokyo").commit().unwrap();
        let a = users.insert().set("id", 1i64).set("company", Value::Ref(c)).commit().unwrap();
        let b = users.insert().set("id", 2i64).set("company", Value::Ref(c)).commit().unwrap();
        let t = users.where_eq("company.city", "Tokyo").subscribe_touched().unwrap();
        let d = t.poll();
        assert_eq!(d.added, vec![c]);
        let mut held: std::collections::BTreeSet<u64> = t.members_many(&d.added).into_iter().flatten().collect();
        assert_eq!(held, [a, b].into_iter().collect(), "前提");

        // 消して、 poll の前にその場所を使い回す (表は枠 2 で満杯)
        users.entity(a).delete().unwrap();
        let x = users.insert().set("id", 3i64).set("company", Value::Ref(c)).commit().unwrap();
        assert_eq!(slot(x), slot(a), "前提: 消えた場所を使い回した");
        assert_ne!(x, a);
        let d = t.poll();
        assert!(d.touched.contains(&a), "v15_first={v15_first}: touched に、 アプリが持つ消えた行の eid が無い: {:?}", d.touched);
        assert!(d.touched.contains(&x), "v15_first={v15_first}: touched に新しい住人が無い");
        // アプリの持ち方 (型の doc): touched を group_of で引き直す
        for e in d.touched {
            match t.group_of(e) {
                Some(_) => {
                    held.insert(e);
                }
                None => {
                    held.remove(&e);
                }
            }
        }
        assert_eq!(held, [b, x].into_iter().collect(), "v15_first={v15_first}: アプリの表が今の答えと違う");

        // 消すだけ (使い回さない)
        users.entity(b).delete().unwrap();
        let d = t.poll();
        assert!(d.touched.contains(&b), "v15_first={v15_first}: 消した行の eid が touched に無い: {:?}", d.touched);
    }
}
