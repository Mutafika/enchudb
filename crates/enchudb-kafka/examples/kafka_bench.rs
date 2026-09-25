//! Kafka → enchu の取り込みの速さ。 JSON 行を N 件 topic に書き、 読んで主キーで upsert する
//! (live 購読 2 本の差分を別の topic に書き出しながら)。
//! usage: ENCHU_KAFKA=localhost:19092 cargo run --release -p enchudb-kafka --example kafka_bench -- <rows>

use enchudb_connect::{prepare, Ingest, JsonRows, LiveExport, OutMessage, Sink};
use enchudb_kafka::{ensure_topic, KafkaSink, KafkaSource};
use enchudb_schema::Database;
use std::time::Instant;

fn main() {
    let brokers: Vec<String> = std::env::var("ENCHU_KAFKA").expect("ENCHU_KAFKA").split(',').map(str::to_string).collect();
    let n: u64 = std::env::args().nth(1).map_or(1_000_000, |x| x.parse().unwrap());
    let tag = std::process::id();
    let (tin, tout) = (format!("bench-in-{tag}"), format!("bench-out-{tag}"));
    ensure_topic(brokers.clone(), &tin, 3, 1).unwrap();
    ensure_topic(brokers.clone(), &tout, 3, 1).unwrap();
    let cities = ["Tokyo", "Osaka", "Kyoto", "Nagoya"];
    let t = Instant::now();
    let mut sink = KafkaSink::connect(brokers.clone(), &tin).unwrap();
    let mut buf = Vec::new();
    for i in 0..n {
        let id = if i < n / 2 { i } else { (i * 7919) % (n / 2) };
        let row = format!(r#"{{"table":"users","row":{{"id":{id},"age":{},"city":"{}"}}}}"#, i % 80, cities[(i % 4) as usize]);
        buf.push(OutMessage { key: id.to_string().into_bytes(), payload: row.into_bytes() });
        if buf.len() == 10_000 {
            sink.send(&buf).unwrap();
            buf.clear();
        }
    }
    sink.send(&buf).unwrap();
    println!("produce {n}: {:.2}s", t.elapsed().as_secs_f64());

    let path = format!("/tmp/enchu-kafka-bench-{tag}.db");
    let mut db = Database::create_growable_with_capacity(&path, (n as u32 + 4096).next_power_of_two()).unwrap();
    db.table("users").number("id").number("age").tag("city").primary_key("id").with_capacity(n as u32 / 2 + 1024).build().unwrap();
    prepare(&mut db).unwrap();
    let users = db.get_table("users").unwrap();
    let mut ex = LiveExport::new(&db);
    ex.add("tokyo", "users", users.where_eq("city", "Tokyo").subscribe().unwrap()).unwrap();
    ex.add_counts("by_city", users.all().subscribe_sums("city", "age").unwrap());
    let mut out = KafkaSink::connect(brokers.clone(), &tout).unwrap();
    let ing = Ingest::new(&db, JsonRows).unwrap();
    let mut src = KafkaSource::connect(brokers, &tin).unwrap().max_wait_ms(10);
    ing.resume(&mut src).unwrap();
    let t = Instant::now();
    let (mut done, mut sent) = (0u64, 0);
    while done < n {
        let r = ing.run_once(&mut src, 20_000).unwrap();
        done += (r.applied + r.rejected.len() + r.replayed) as u64;
        sent += ex.pump(&mut out).unwrap();
    }
    let s = t.elapsed().as_secs_f64();
    println!("ingest {n} + export {sent}: {s:.2}s = {:.0} rows/s", n as f64 / s);
    drop(ex);
    drop(db);
    for suffix in ["", ".oplog", ".tables", ".eidmap"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
    }
}
