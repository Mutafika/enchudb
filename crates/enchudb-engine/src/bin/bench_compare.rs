use enchudb_engine::{Engine, ValueType};
use std::time::Instant;

fn main() {
    let n = 1_000_000u32;
    let dir = "/tmp/enchu_bench_v24.db";
    let _ = std::fs::remove_file(dir);

    let mut eng = Engine::create_standalone(dir).unwrap();
    eng.define_himo("age", ValueType::Number, 50);
    eng.define_himo("dept", ValueType::Number, 8);
    eng.define_himo("company", ValueType::Number, 100);
    eng.define_himo("city", ValueType::Tag, 0);

    let t = Instant::now();
    for i in 0..n {
        let e = eng.entity();
        eng.tie(e, "age", i % 50);
        eng.tie(e, "dept", i % 8);
        eng.tie(e, "company", i % 100);
        eng.tie_text(e, "city", &format!("city_{}", i % 10));
    }
    let bulk_insert = t.elapsed();

    let t = Instant::now();
    eng.rebuild();
    let rebuild_time = t.elapsed();

    let iters = 1000;

    let t = Instant::now();
    for _ in 0..iters {
        let _ = eng.query(&[("age", 30)]);
    }
    let q1 = t.elapsed();

    let t = Instant::now();
    for _ in 0..iters {
        let _ = eng.query(&[("age", 0), ("dept", 0)]);
    }
    let q2 = t.elapsed();

    let city0 = eng.vocab_id("city_0").unwrap();
    let t = Instant::now();
    for _ in 0..iters {
        let _ = eng.query(&[("age", 0), ("dept", 0), ("company", 0), ("city", city0)]);
    }
    let q4 = t.elapsed();

    let t = Instant::now();
    let write_n = 10_000u32;
    for i in 0..write_n {
        let e = eng.entity();
        eng.tie(e, "age", i % 50);
        eng.tie_text(e, "city", &format!("city_{}", i % 10));
    }
    let incremental = t.elapsed();

    let t = Instant::now();
    for _ in 0..iters {
        let _ = eng.get(500, "age");
        let _ = eng.get_text(500, "city");
    }
    let read = t.elapsed();

    eprintln!("=== v24 実用ベンチ (1M entities) ===");
    eprintln!("");
    eprintln!("--- 書き込み ---");
    eprintln!("バルク 1M                    : {:>6}ms", bulk_insert.as_millis());
    eprintln!("逐次 10K (tie_text毎回)      : {:>6}ms", incremental.as_millis());
    eprintln!("rebuild                      : {:>6}ms", rebuild_time.as_millis());
    eprintln!("");
    eprintln!("--- クエリ ({iters}回平均) ---");
    eprintln!("1条件 (age:30)               : {:>8.1}μs", q1.as_micros() as f64 / iters as f64);
    eprintln!("2条件 (age+dept)             : {:>8.1}μs", q2.as_micros() as f64 / iters as f64);
    eprintln!("4条件 (age+dept+company+city): {:>8.1}μs", q4.as_micros() as f64 / iters as f64);
    eprintln!("");
    eprintln!("--- 読み取り ({iters}回平均) ---");
    eprintln!("get + get_text               : {:>8.1}μs", read.as_micros() as f64 / iters as f64);

    let _ = std::fs::remove_file(dir);
}
