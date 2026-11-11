//! #703: 集計の購読 (`answer_counts` / `answer_sums`) の 「最後に渡した値」 は購読ごとに 1 つ。 `db.answer(name)` で取り直した
//! handle から poll しても、 差分を値で上書きして積めば今の集計になる。
//!
//! 前は schema の handle ごとに 「最後に渡した値」 を持ち、 取り直した handle は空から始まった:
//! - group が消えた報告 (件数 0) を 「渡していない group」 として捨てた (engine の購読は渡した扱い = どの handle にも二度と来ない)
//! - 別の handle が渡した値を知らない handle が、 元の値に戻った報告を 「変わっていない」 として捨てた

use enchudb_schema::{Answer, Database, LiveCounts, Value};
use std::collections::BTreeMap;

fn tmp_path(tag: &str) -> String {
    format!(
        "{}/enchudb-issue703-{}-{}-{}",
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

fn text(v: &Value) -> String {
    match v {
        Value::Text(s) => s.clone(),
        other => panic!("group の値が文字でない: {other:?}"),
    }
}

/// 差分を値で上書きして積む (件数 0 は消す)。
fn fold(table: &mut BTreeMap<String, (u64, i128)>, delta: Vec<(Value, u64, i128)>) {
    for (v, n, s) in delta {
        if n == 0 {
            table.remove(&text(&v));
        } else {
            table.insert(text(&v), (n, s));
        }
    }
}

fn now(c: &LiveCounts) -> BTreeMap<String, (u64, i128)> {
    c.all_sums().into_iter().map(|(v, n, s)| (text(&v), (n, s))).collect()
}

fn by_name(db: &Database, name: &str) -> LiveCounts {
    db.answer(name).and_then(Answer::into_counts).unwrap()
}

/// issue の再現: 種を最初の handle で受け、 group を消し、 名前で取り直した handle で poll する。
#[test]
fn a_handle_taken_by_name_again_reports_the_group_removal() {
    let path = tmp_path("refetch");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").tag("city").primary_key("id").build().unwrap();
    let t = db.get_table("t").unwrap();
    let rows: Vec<u64> =
        (0..6i64).map(|i| t.insert().set("id", i).set("city", ["Tokyo", "Osaka"][(i % 2) as usize]).commit().unwrap()).collect();
    let first = t.all().answer_counts("per_city", "city").unwrap();
    let seed = first.poll();
    assert_eq!(seed.len(), 2, "{seed:?}");
    for (i, e) in rows.iter().enumerate() {
        if i % 2 == 1 {
            t.entity(*e).delete().unwrap();
        }
    }
    let again = by_name(&db, "per_city");
    let d = again.poll();
    assert_eq!(d, vec![(Value::Text("Osaka".into()), 0)], "取り直した handle に Osaka が消えた報告が届かない");
    assert_eq!(first.poll(), vec![], "同じ報告が 2 回届いた (報告は購読ごとに 1 回)");
    assert_eq!(again.poll(), vec![]);
}

/// poll のたびに名前で取る書き方 (doc が開き直した後に勧める取り方) で、 積んだ表がいつも今の集計と合う。
/// 合計つき。 group が現れる・消える・元の値に戻る・合計だけ動く、 を混ぜる。
#[test]
fn polling_through_a_fresh_handle_each_time_integrates_to_the_current_groups() {
    let path = tmp_path("each-time");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").tag("city").number("amount").primary_key("id").build().unwrap();
    let t = db.get_table("t").unwrap();
    const CITIES: [&str; 4] = ["Tokyo", "Osaka", "Nagoya", "Fukuoka"];
    drop(t.all().answer_sums("spent", "city", "amount").unwrap());
    let mut table = BTreeMap::new();
    let mut live: Vec<Option<u64>> = vec![None; 12];
    let mut saw_removal = 0;
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    for step in 0..400 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let slot = (x % 12) as usize;
        match live[slot] {
            Some(e) if x & 0x300 == 0 => {
                t.entity(e).delete().unwrap();
                live[slot] = None;
            }
            Some(e) => {
                // 街の付け替えか金額の書き換え
                if x & 0x400 == 0 {
                    t.entity(e).set("city", CITIES[((x >> 12) % 4) as usize]).commit().unwrap();
                } else {
                    t.entity(e).set("amount", ((x >> 16) % 5) as i64).commit().unwrap();
                }
            }
            None => {
                let e = t
                    .insert()
                    .set("id", step as i64 * 100 + slot as i64)
                    .set("city", CITIES[((x >> 12) % 4) as usize])
                    .set("amount", ((x >> 16) % 5) as i64)
                    .commit()
                    .unwrap();
                live[slot] = Some(e);
            }
        }
        // 3 回に 1 回だけ poll する (間の出入りは畳まれる)
        if step % 3 == 0 {
            let h = by_name(&db, "spent");
            let d = h.poll_sums();
            saw_removal += d.iter().filter(|(_, n, _)| *n == 0).count();
            fold(&mut table, d);
            assert_eq!(table, now(&h), "step {step}: 積んだ表が今の集計と違う");
        }
    }
    assert!(saw_removal > 0, "group が消える所を 1 度も通らなかった (試験の前提)");
}

/// 2 つの handle で交互に poll する: 片方が渡した値をもう片方も知っている (元の値に戻った報告を捨てない)。
#[test]
fn two_handles_share_what_was_handed_out() {
    let path = tmp_path("two");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").tag("city").primary_key("id").build().unwrap();
    let t = db.get_table("t").unwrap();
    for i in 0..3i64 {
        t.insert().set("id", i).set("city", "Tokyo").commit().unwrap();
    }
    let a = t.all().answer_counts("per_city", "city").unwrap();
    let b = by_name(&db, "per_city");
    let mut table = BTreeMap::new();
    let mut fold_counts = |d: Vec<(Value, u64)>| fold(&mut table, d.into_iter().map(|(v, n)| (v, n, 0)).collect());
    let d = a.poll();
    assert_eq!(d, vec![(Value::Text("Tokyo".into()), 3)]);
    fold_counts(d); // a が (Tokyo, 3) を渡した
    let e = t.insert().set("id", 9i64).set("city", "Tokyo").commit().unwrap();
    let d = b.poll();
    assert_eq!(d, vec![(Value::Text("Tokyo".into()), 4)]);
    fold_counts(d); // b が (Tokyo, 4) を渡した
    t.entity(e).delete().unwrap();
    // a が最後に自分で渡したのは 3。 今も 3 だが、 購読としては 4 を渡した後なので 3 に戻った報告が要る
    let d = a.poll();
    assert_eq!(d, vec![(Value::Text("Tokyo".into()), 3)], "元の値に戻った報告を捨てた");
    fold_counts(d);
    assert_eq!(b.poll(), vec![]);
    assert_eq!(table.get("Tokyo"), Some(&(3, 0)));
}

/// 開き直した後: 名前で取った handle を取り直しても、 消えた報告が届く。
#[test]
fn after_reopen_a_refetched_handle_reports_the_removal() {
    let path = tmp_path("reopen");
    let _c = Cleanup(path.clone());
    {
        let mut db = Database::create(&path).unwrap();
        db.table("t").number("id").tag("city").primary_key("id").build().unwrap();
        let t = db.get_table("t").unwrap();
        for i in 0..6i64 {
            t.insert().set("id", i).set("city", ["Tokyo", "Osaka"][(i % 2) as usize]).commit().unwrap();
        }
        drop(t.all().answer_counts("per_city", "city").unwrap());
    }
    let db = Database::open(&path).unwrap();
    let t = db.get_table("t").unwrap();
    let mut table = BTreeMap::new();
    let seed = by_name(&db, "per_city").poll();
    assert_eq!(seed.len(), 2, "{seed:?}");
    fold(&mut table, seed.into_iter().map(|(v, n)| (v, n, 0)).collect());
    for e in t.where_eq("city", "Osaka").find().unwrap() {
        t.entity(e).delete().unwrap();
    }
    let h = by_name(&db, "per_city");
    let d = h.poll();
    assert_eq!(d, vec![(Value::Text("Osaka".into()), 0)]);
    fold(&mut table, d.into_iter().map(|(v, n)| (v, n, 0)).collect());
    assert_eq!(table, now(&h));
}
