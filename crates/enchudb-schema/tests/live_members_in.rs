//! `LiveQuery::members_in` (束 × 購読): タグで引いた束 (`where_eq("addr", x).find()`) のうち購読の今の結果に入るものが、 手元に
//! 覚えた値をなめた答えと同じこと。 書き換え (年齢・住所・備考を後から付ける)・消しの後と、 購読の poll をはさんだ後と、 `or` の
//! 購読 (枝の和) でも見る。 束の順を保つこと (返す eid は束の部分列) も見る。 後から足す購読 (同じ形・同じ値 = 既にある鍵に加わる
//! 初回の報告の道と、 同じ形・別の値 = 新しい鍵の道) も、 足した直後と書き換えの後に見る。 備考を外す (untie) も混ぜる。 途中で
//! 同じ形の購読を外す (帯の端が減らない外し方と、 減る外し方)。

use enchudb_schema::{Database, LiveQuery, Table};
use std::collections::BTreeMap;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/schema_members_in_{}_{}_{}.db", tag, std::process::id(), nanos)
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    let _ = std::fs::remove_file(path);
    for ext in ["lock", "oplog", "schema", "tables"] {
        let _ = std::fs::remove_file(format!("{}.{}", path, ext));
    }
}

struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
    }
}

#[derive(Clone, Copy)]
struct Row {
    addr: u64,
    age: i64,
    remarks: bool,
}

const ADDRS: u64 = 7;

/// 後から足す購読: (購読, 手元の答えの条件, 名前)。
type Late = (LiveQuery, Box<dyn Fn(&Row) -> bool>, &'static str);

fn addr(i: u64) -> String {
    format!("p{i:02}")
}

/// 束 × 購読の答えを、 手元の値をなめた答えと比べる。 返り値 = 当たった数の合計 (対照用)。
fn check(t: &Table, q: &LiveQuery, rows: &BTreeMap<u64, Row>, want: &dyn Fn(&Row) -> bool, stage: &str) -> usize {
    let mut hits = 0;
    for a in 0..ADDRS {
        let deck = t.where_eq("addr", addr(a).as_str()).find().unwrap();
        let got = q.members_in(&deck);
        // 束の部分列 (順を保つ)
        let mut it = deck.iter();
        assert!(got.iter().all(|g| it.any(|d| d == g)), "{stage} addr {a}: 束の部分列でない");
        let mut got = got;
        got.sort_unstable();
        let mut exp: Vec<u64> = rows.iter().filter(|(_, r)| r.addr == a && want(r)).map(|(&e, _)| e).collect();
        exp.sort_unstable();
        assert_eq!(got, exp, "{stage} addr {a}");
        hits += exp.len();
    }
    hits
}

#[test]
fn members_in_matches_a_scan_of_the_written_values() {
    let path = tmp_path("deck");
    cleanup(&path);
    let mut db = Database::create_growable_with_capacity(&path, 65_536).unwrap();
    db.table("u").tag("addr").number("age").leaf("remarks").build().unwrap();
    let t = db.get_table("u").unwrap();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut rows: BTreeMap<u64, Row> = BTreeMap::new();
    for _ in 0..2000 {
        let r = Row { addr: rng.below(ADDRS), age: 18 + rng.below(62) as i64, remarks: rng.below(10) < 3 };
        let mut ins = t.insert().set("addr", addr(r.addr).as_str()).set("age", r.age);
        if r.remarks {
            ins = ins.set("remarks", "note");
        }
        let e = ins.commit().unwrap();
        rows.insert(e, r);
    }
    // 購読: 30〜64 歳 かつ 備考あり (AND)、 addr = p01 または 61 歳以上 (or = 枝の和)
    let q = t.where_range("age", 30, 64).where_not_null("remarks").subscribe().unwrap();
    let q_or = t.where_eq("addr", addr(1).as_str()).or(t.where_range("age", 61, 1000)).subscribe().unwrap();
    // 値の穴の購読 (住所 = p03 かつ 備考あり): 後から同じ値・別の値の購読を足す
    let q_eq = t.where_eq("addr", addr(3).as_str()).where_not_null("remarks").subscribe().unwrap();
    // 値の穴と範囲の穴の購読 (住所 = p01 かつ 30〜64 歳 かつ 備考あり): 後から別の住所・別の範囲を足すと、 鍵が 2 つの family で帯が割れる
    let q_ar = t.where_eq("addr", addr(1).as_str()).where_range("age", 30, 64).where_not_null("remarks").subscribe().unwrap();
    let want_ar = |r: &Row| r.addr == 1 && (30..=64).contains(&r.age) && r.remarks;
    // 同じ形で別の住所・q_ar の外まで届く範囲 (60〜75 歳): 帯が併さった後も 65〜75 歳が帯に残る (q_ar の鍵の帯の bit がずれていれば
    // 住所 p01 のカードが 70 歳に動いた時に出ない)
    let q_ar5 = t.where_eq("addr", addr(5).as_str()).where_range("age", 60, 75).where_not_null("remarks").subscribe().unwrap();
    let want_ar5 = |r: &Row| r.addr == 5 && (60..=75).contains(&r.age) && r.remarks;
    let want_q = |r: &Row| (30..=64).contains(&r.age) && r.remarks;
    let want_or = |r: &Row| r.addr == 1 || r.age > 60;
    let want_eq = |a: u64| move |r: &Row| r.addr == a && r.remarks;
    let h1 = check(&t, &q, &rows, &want_q, "書き換える前");
    let h2 = check(&t, &q_or, &rows, &want_or, "書き換える前 (or)");
    check(&t, &q_eq, &rows, &want_eq(3), "書き換える前 (値の穴)");
    check(&t, &q_ar, &rows, &want_ar, "書き換える前 (値の穴と範囲)");
    check(&t, &q_ar5, &rows, &want_ar5, "書き換える前 (値の穴と範囲 p05)");
    // 対照: 当たる束がある、 全部でもない
    assert!(h1 > 50 && h1 < rows.len() / 2, "test の前提: 当たり {h1}");
    assert!(h2 > 50 && h2 < rows.len(), "test の前提: or の当たり {h2}");
    // 書き換え: 年齢・住所、 備考を後から付ける、 消す。 間に 1 回だけ poll をはさむ。 1 回目の後に購読を足す
    let ids: Vec<u64> = rows.keys().copied().collect();
    let mut late: Vec<Late> = Vec::new();
    for round in 0..4 {
        match round {
            // 同じ形・同じ範囲の購読を外す (family の member が減る。 帯の端は q も持つので減らない)
            2 => drop(late.remove(1)),
            // 値の穴と範囲の形の 2 本目を外す (その購読だけが持つ帯の端が減る = 前からある住所 p01 の鍵の帯も変わる)
            3 => {
                drop(late.remove(3));
                // 住所 p01 の鍵の帯が変わった後に、 その鍵の範囲から出る書き換え (30〜64 歳 → 70 歳) を確かに入れる
                let mut n = 0;
                for (&e, r) in rows.iter_mut().filter(|(_, r)| r.addr == 1 && r.remarks && (30..=64).contains(&r.age)) {
                    r.age = 70;
                    t.entity(e).set("age", r.age).commit().unwrap();
                    n += 1;
                    if n == 20 {
                        break;
                    }
                }
                assert!(n > 0, "test の前提: 住所 p01 で範囲に居るカード");
            }
            _ => {}
        }
        for _ in 0..300 {
            let e = ids[rng.below(ids.len() as u64) as usize];
            let Some(r) = rows.get_mut(&e) else { continue };
            match rng.below(5) {
                0 => {
                    r.age = 18 + rng.below(62) as i64;
                    t.entity(e).set("age", r.age).commit().unwrap();
                }
                1 => {
                    r.addr = rng.below(ADDRS);
                    t.entity(e).set("addr", addr(r.addr).as_str()).commit().unwrap();
                }
                2 => {
                    r.remarks = true;
                    t.entity(e).set("remarks", "later").commit().unwrap();
                }
                3 => {
                    r.remarks = false;
                    db.engine().untie(e, "u.remarks");
                }
                _ => {
                    rows.remove(&e);
                    t.entity(e).delete().unwrap();
                }
            }
        }
        if round == 1 {
            let _ = q.poll();
            let _ = q_or.poll();
        }
        check(&t, &q, &rows, &want_q, &format!("書き換え {round} 回目の後"));
        check(&t, &q_or, &rows, &want_or, &format!("書き換え {round} 回目の後 (or)"));
        check(&t, &q_eq, &rows, &want_eq(3), &format!("書き換え {round} 回目の後 (値の穴)"));
        check(&t, &q_ar, &rows, &want_ar, &format!("書き換え {round} 回目の後 (値の穴と範囲)"));
        check(&t, &q_ar5, &rows, &want_ar5, &format!("書き換え {round} 回目の後 (値の穴と範囲 p05)"));
        for (lq, want, what) in &late {
            check(&t, lq, &rows, want.as_ref(), &format!("書き換え {round} 回目の後 ({what})"));
        }
        if round == 0 {
            // 同じ形・別の範囲 (帯が割れる)、 同じ形・同じ範囲 (既にある鍵、 帯は割れない)、 同じ形・同じ値 (既にある鍵)、 同じ形・
            // 別の値 (新しい鍵)、 値の穴と範囲の形で別の住所・別の範囲 (新しい鍵 + 帯が割れる = 前からある住所 p01 の鍵の帯も変わる)
            // 1 本足すごとに確かめる (読みが帯の bit を作るので、 次の購読が加わる時には作ってある)。 購読は足す時に作る
            type Make<'t> = Box<dyn Fn() -> LiveQuery + 't>;
            type Add<'t> = (Make<'t>, Box<dyn Fn(&Row) -> bool>, &'static str);
            let adds: Vec<Add> = vec![
                (
                    Box::new(|| t.where_range("age", 50, 70).where_not_null("remarks").subscribe().unwrap()),
                    Box::new(|r: &Row| (50..=70).contains(&r.age) && r.remarks),
                    "後から別の範囲",
                ),
                (Box::new(|| t.where_range("age", 30, 64).where_not_null("remarks").subscribe().unwrap()), Box::new(want_q), "後から同じ範囲"),
                (Box::new(|| t.where_eq("addr", addr(3).as_str()).where_not_null("remarks").subscribe().unwrap()), Box::new(want_eq(3)), "後から同じ値"),
                (Box::new(|| t.where_eq("addr", addr(4).as_str()).where_not_null("remarks").subscribe().unwrap()), Box::new(want_eq(4)), "後から別の値"),
                (
                    Box::new(|| t.where_eq("addr", addr(2).as_str()).where_range("age", 40, 50).where_not_null("remarks").subscribe().unwrap()),
                    Box::new(|r: &Row| r.addr == 2 && (40..=50).contains(&r.age) && r.remarks),
                    "後から別の住所と範囲",
                ),
            ];
            for (make, want, what) in adds {
                late.push((make(), want, what));
                for (lq, want, what) in &late {
                    check(&t, lq, &rows, want.as_ref(), &format!("足した直後 ({what})"));
                }
                check(&t, &q, &rows, &want_q, "足した直後 (q)");
                check(&t, &q_ar, &rows, &want_ar, "足した直後 (値の穴と範囲)");
            }
        }
    }
    // 束でない列 (空・知らない eid) も扱える
    assert!(q.members_in(&[]).is_empty());
    drop(late);
    drop((q, q_or, q_eq, q_ar, q_ar5, t));
    drop(db);
    cleanup(&path);
}
