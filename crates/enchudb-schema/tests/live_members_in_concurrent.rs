//! 束 × 購読 (`LiveQuery::members_in`) を書き手と並べて使う: 書き手 4 本が自分の row を書き換え (年齢・住所・備考を付ける / 外す・
//! 消す)、 その間に読み手 1 本が購読を読み続ける (members_in・count・poll・contains。 読むたびに印を評価し、 members_in を使った購読は
//! 今の集合の bit を立て直す)。 書き終えた後の答え (members と束 × 購読) が、 手元に覚えた値をなめた答えと同じこと。

use enchudb_schema::{Database, LiveQuery, Table};
use std::sync::atomic::{AtomicBool, Ordering};

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/schema_members_in_conc_{}_{}_{}.db", tag, std::process::id(), nanos)
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    let _ = std::fs::remove_file(path);
    for ext in ["lock", "oplog", "schema", "tables"] {
        let _ = std::fs::remove_file(format!("{}.{}", path, ext));
    }
}

#[derive(Clone)]
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
    alive: bool,
}

const ADDRS: u64 = 7;

fn addr(i: u64) -> String {
    format!("p{i:02}")
}

fn check(t: &Table, q: &LiveQuery, rows: &[(u64, Row)], want: &dyn Fn(&Row) -> bool, what: &str) {
    let mut exp: Vec<u64> = rows.iter().filter(|(_, r)| r.alive && want(r)).map(|&(e, _)| e).collect();
    exp.sort_unstable();
    assert_eq!(q.members(), exp, "{what}: members");
    for a in 0..ADDRS {
        let deck = t.where_eq("addr", addr(a).as_str()).find().unwrap();
        let mut got = q.members_in(&deck);
        got.sort_unstable();
        let mut e2: Vec<u64> = rows.iter().filter(|(_, r)| r.alive && r.addr == a && want(r)).map(|&(e, _)| e).collect();
        e2.sort_unstable();
        assert_eq!(got, e2, "{what}: addr {a}");
    }
}

#[test]
fn members_in_under_concurrent_writers_and_a_reader() {
    let path = tmp_path("conc");
    cleanup(&path);
    let mut db = Database::create_growable_with_capacity(&path, 65_536).unwrap();
    db.table("u").tag("addr").number("age").leaf("remarks").build().unwrap();
    let t = db.get_table("u").unwrap();
    let mut rng = Rng(0x2545_F491_4F6C_DD1D);
    let mut rows: Vec<(u64, Row)> = Vec::new();
    for _ in 0..4000 {
        let r = Row { addr: rng.below(ADDRS), age: 18 + rng.below(62) as i64, remarks: rng.below(10) < 3, alive: true };
        let mut ins = t.insert().set("addr", addr(r.addr).as_str()).set("age", r.age);
        if r.remarks {
            ins = ins.set("remarks", "note");
        }
        rows.push((ins.commit().unwrap(), r));
    }
    let q = t.where_range("age", 30, 64).where_not_null("remarks").subscribe().unwrap();
    let q_eq = t.where_eq("addr", addr(3).as_str()).where_not_null("remarks").subscribe().unwrap();
    let q_hi = t.where_range("age", 50, 70).where_not_null("remarks").subscribe().unwrap();
    let want_q = |r: &Row| (30..=64).contains(&r.age) && r.remarks;
    let want_eq = |r: &Row| r.addr == 3 && r.remarks;
    let want_hi = |r: &Row| (50..=70).contains(&r.age) && r.remarks;
    check(&t, &q, &rows, &want_q, "書く前");
    let hits: usize = rows.iter().filter(|(_, r)| want_q(r)).count();
    assert!(hits > 100 && hits < rows.len() / 2, "test の前提: 当たり {hits}");
    const W: usize = 4;
    let done = AtomicBool::new(false);
    // 書き手は読み手が読み始めてから書く
    let started = std::sync::Barrier::new(W + 1);
    let mut parts: Vec<Vec<(u64, Row)>> = vec![Vec::new(); W];
    for (i, x) in rows.iter().enumerate() {
        parts[i % W].push(*x);
    }
    let reads = std::thread::scope(|sc| {
        let reader = sc.spawn(|| {
            let mut n = 0u64;
            let mut r = Rng(0x9E37_79B9_7F4A_7C15);
            started.wait();
            while !done.load(Ordering::Acquire) {
                match r.below(8) {
                    0 => {
                        let deck = t.where_eq("addr", addr(r.below(ADDRS)).as_str()).find().unwrap();
                        std::hint::black_box(q.members_in(&deck));
                    }
                    1 => {
                        std::hint::black_box(q_hi.poll());
                    }
                    2 => {
                        std::hint::black_box(q_eq.count());
                    }
                    _ => {
                        std::hint::black_box(q.contains(r.below(5000)));
                    }
                }
                n += 1;
            }
            n
        });
        let writers: Vec<_> = parts
            .iter_mut()
            .enumerate()
            .map(|(w, part)| {
                let (t, db, started) = (&t, &db, &started);
                sc.spawn(move || {
                    let mut r = Rng(0xD1B5_4A32_D192_ED03 ^ ((w as u64 + 1) << 32));
                    started.wait();
                    for _ in 0..20_000 {
                        let j = r.below(part.len() as u64) as usize;
                        let (e, row) = &mut part[j];
                        if !row.alive {
                            continue;
                        }
                        match r.below(100) {
                            0..=29 => {
                                row.age = 18 + r.below(62) as i64;
                                t.entity(*e).set("age", row.age).commit().unwrap();
                            }
                            30..=59 => {
                                row.addr = r.below(ADDRS);
                                t.entity(*e).set("addr", addr(row.addr).as_str()).commit().unwrap();
                            }
                            60..=78 => {
                                row.remarks = true;
                                t.entity(*e).set("remarks", "later").commit().unwrap();
                            }
                            79..=97 => {
                                row.remarks = false;
                                db.engine().untie(*e, "u.remarks");
                            }
                            _ => {
                                row.alive = false;
                                t.entity(*e).delete().unwrap();
                            }
                        }
                    }
                })
            })
            .collect();
        for w in writers {
            w.join().unwrap();
        }
        done.store(true, Ordering::Release);
        reader.join().unwrap()
    });
    assert!(reads > 10, "test の前提: 読み手が書き込みの間に読んだ回数 {reads}");
    let rows: Vec<(u64, Row)> = parts.into_iter().flatten().collect();
    check(&t, &q, &rows, &want_q, "書いた後");
    check(&t, &q_eq, &rows, &want_eq, "書いた後 (値の穴)");
    check(&t, &q_hi, &rows, &want_hi, "書いた後 (別の範囲)");
    drop((q, q_eq, q_hi, t));
    drop(db);
    cleanup(&path);
}
