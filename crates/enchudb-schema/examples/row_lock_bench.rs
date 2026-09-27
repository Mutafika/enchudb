//! 行の書き込みの版 (#206 / #135) の書き込みコスト。 master と交互に回して比べる。
//! `cargo run --release -p enchudb-schema --example row_lock_bench -- <dir>`

use enchudb_schema::Database;
use std::time::Instant;

fn main() {
    let dir = std::env::args().nth(1).expect("dir");
    let path = format!("{dir}/row_lock_bench.db");
    let _ = std::fs::remove_dir_all(&path);
    const N: i64 = 1_000_000;
    let mut db = Database::create(&path).unwrap();
    db.table("u").number("id").number("age").tag("city").leaf("memo").primary_key("id").build().unwrap();
    db.table("n").number("id").number("a").number("b").build().unwrap();
    // 1) 4 列の insert
    let t = db.get_table("u").unwrap();
    let s = Instant::now();
    let mut eids = Vec::with_capacity(N as usize);
    for i in 0..N {
        eids.push(t.insert().set("id", i).set("age", i % 100).set("city", ["Tokyo", "Osaka", "Kyoto"][(i % 3) as usize]).set("memo", "hello memo").commit().unwrap());
    }
    let insert = s.elapsed().as_nanos() as f64 / N as f64;
    // 2) 2 列の update
    let s = Instant::now();
    for (i, &e) in eids.iter().enumerate() {
        t.entity(e).update().set("age", (i % 97) as i64).set("memo", "updated").commit().unwrap();
    }
    let update = s.elapsed().as_nanos() as f64 / N as f64;
    // 3) engine の tie 1 回 (Number)
    let nt = db.get_table("n").unwrap();
    let ne: Vec<u64> = (0..N).map(|i| nt.insert().set("id", i).commit().unwrap()).collect();
    let hid = nt.himo_id("a").unwrap();
    let eng = db.arc_engine();
    let s = Instant::now();
    for r in 0..5u32 {
        for &e in &ne {
            eng.try_tie_to_by_id(e, hid, r).unwrap();
        }
    }
    let tie = s.elapsed().as_nanos() as f64 / (5 * N) as f64;
    drop(t);
    drop(nt);
    drop(eng);
    // 4) 8 thread で別々の行を update
    let db = db.finish_concurrent().unwrap();
    let eids = std::sync::Arc::new(eids);
    let s = Instant::now();
    let hs: Vec<_> = (0..8)
        .map(|th| {
            let (db, eids) = (db.clone(), eids.clone());
            std::thread::spawn(move || {
                let t = db.get_table("u").unwrap();
                for (i, &e) in eids.iter().enumerate().skip(th).step_by(8) {
                    t.entity(e).update().set("age", (i % 89) as i64).set("city", "Nagoya").commit().unwrap();
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    let par = s.elapsed().as_nanos() as f64 / N as f64;
    println!("insert {insert:.0} ns/row  update {update:.0} ns/row  tie {tie:.1} ns  8thread-update {par:.0} ns/row");
    drop(db);
    let _ = std::fs::remove_dir_all(&path);
}
