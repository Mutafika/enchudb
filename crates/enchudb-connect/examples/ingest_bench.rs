//! 取り込みの速さ: JSON 行の upsert を N 件、 live 購読 (city = Tokyo) を張った状態で流す。
//! usage: cargo run --release -p enchudb-connect --example ingest_bench -- <rows> <batch>

use enchudb_connect::memory::Topic;
use enchudb_connect::{prepare, Ingest, JsonRows, LiveExport};
use enchudb_schema::Database;
use std::time::Instant;

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let n: u64 = a.get(1).map_or(1_000_000, |x| x.parse().unwrap());
    let batch: usize = a.get(2).map_or(10_000, |x| x.parse().unwrap());
    let path = format!("/tmp/enchu-connect-bench-{}.db", std::process::id());
    let mut db = Database::create_growable_with_capacity(&path, (n as u32 + n as u32 / 50 + 4096).next_power_of_two()).unwrap();
    db.table("companies").number("id").tag("city").primary_key("id").with_capacity(n as u32 / 50 + 1024).build().unwrap();
    db.table("users").number("id").number("age").tag("city").ref_to("company", "companies").primary_key("id").with_capacity(n as u32 / 2 + 1024).build().unwrap();
    prepare(&mut db).unwrap();
    let cities = ["Tokyo", "Osaka", "Kyoto", "Nagoya"];
    let topic = Topic::new("users");
    let mut x = 0x9e37_79b9u64;
    let mut rnd = |m: u64| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % m
    };
    for i in 0..n {
        // 前半は新しい行、 後半は既存の行の書き換え
        let id = if i < n / 2 { i } else { rnd(n / 2) };
        let row = format!(
            r#"{{"table":"users","row":{{"id":{id},"age":{},"city":"{}","company":{}}}}}"#,
            rnd(80),
            cities[rnd(4) as usize],
            rnd(n / 100 + 1)
        );
        topic.push(None, row.as_bytes());
    }
    let users = db.get_table("users").unwrap();
    let mut ex = LiveExport::new(&db);
    ex.add("tokyo", "users", users.where_eq("city", "Tokyo").subscribe().unwrap()).unwrap();
    ex.add_counts("by_city", users.all().subscribe_sums("city", "age").unwrap());
    let out = Topic::new("out");
    let ing = Ingest::new(&db, JsonRows).unwrap();
    let mut src = topic.source();
    let t0 = Instant::now();
    let (mut applied, mut pump_s, mut sent) = (0, 0.0, 0);
    loop {
        let r = ing.run_once(&mut src, batch).unwrap();
        if std::env::var("DEBUG").is_ok() { eprintln!("applied {} replayed {} rejected {} {:?}", r.applied, r.replayed, r.rejected.len(), r.rejected.first()); }
        if r.applied == 0 && r.rejected.is_empty() {
            break;
        }
        applied += r.applied;
        let t = Instant::now();
        sent += ex.pump(&mut out.sink()).unwrap();
        pump_s += t.elapsed().as_secs_f64();
    }
    let s = t0.elapsed().as_secs_f64();
    println!(
        "rows {applied} batch {batch}: {s:.2}s = {:.0} rows/s ({:.2} µs/row), export {pump_s:.2}s, {sent} messages out",
        applied as f64 / s,
        s * 1e6 / applied as f64
    );
    drop(ex);
    drop(db);
    for suffix in ["", ".oplog", ".tables", ".eidmap"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
    }
}
