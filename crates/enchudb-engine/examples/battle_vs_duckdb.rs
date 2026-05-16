//! enchudb vs DuckDB vs sqlite3 — 真っ向勝負ベンチ。
//!
//! schema: (user_id, dept_id, salary, year)、 1M rows
//! Q1: point lookup           (1 row 取得)
//! Q2: filter list            (dept_id = 42 → ~10K 行の eid 列)
//! Q3: filter aggregation     (SUM(salary) WHERE dept_id = 42)
//! Q4: full aggregation       (SUM(salary) 全行)
//! Q5: group by aggregation   (SUM(salary) GROUP BY dept_id → 100 グループ)

use enchudb_engine::{Engine, HimoType};
use std::io::Write;
use std::time::Instant;

const N: u32 = 1_000_000;
const DEPTS: u32 = 100;
const YEARS: u32 = 6;
const TARGET_DEPT: u32 = 42;

fn xorshift(mut x: u64) -> u64 {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

fn main() {
    let db_path = "/tmp/battle_enchu.db";
    let csv_path = "/tmp/battle_data.csv";
    let duck_path = "/tmp/battle_duck.db";
    let sqlite_path = "/tmp/battle_sqlite.db";
    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{}.wal", db_path));
    let _ = std::fs::remove_file(csv_path);
    let _ = std::fs::remove_file(duck_path);
    let _ = std::fs::remove_file(sqlite_path);

    eprintln!("=== generating {} rows ===", N);

    // ──── enchudb 側構築 ────
    let mut eng = Engine::create_standalone(db_path).unwrap();
    eng.define_himo("user_id", HimoType::Number, N);
    eng.define_himo("dept_id", HimoType::Number, DEPTS);
    eng.define_himo("salary",  HimoType::Number, 800_000);
    eng.define_himo("year",    HimoType::Number, YEARS);

    // CSV も同時に吐く
    let mut csv = std::fs::File::create(csv_path).unwrap();
    writeln!(csv, "user_id,dept_id,salary,year").unwrap();

    let mut eids: Vec<enchudb_wal::EntityId> = Vec::with_capacity(N as usize);
    let mut rng = 0x9E3779B97F4A7C15u64;
    for i in 0..N {
        rng = xorshift(rng);
        let dept = (i % DEPTS) as u32;
        let salary = 200_000u32 + (rng as u32 % 800_000);
        let year = 2020 + (i % YEARS);
        let e = eng.entity();
        eng.tie(e, "user_id", i);
        eng.tie(e, "dept_id", dept);
        eng.tie(e, "salary",  salary);
        eng.tie(e, "year",    year);
        eids.push(e);
        writeln!(csv, "{},{},{},{}", i, dept, salary, year).unwrap();
    }
    csv.flush().unwrap();
    drop(csv);
    eng.rebuild();
    eprintln!("    enchudb + csv ready");

    // ──── DuckDB / sqlite3 へロード ────
    eprintln!("=== loading into duckdb / sqlite3 ===");
    duckdb_setup(duck_path, csv_path);
    sqlite_setup(sqlite_path, csv_path);

    // ──── enchudb 各クエリ計測 ────
    let target_eid = eids[123_456];
    let iters_point: u32 = 1_000_000;
    let iters_med:   u32 = 10_000;
    let iters_heavy: u32 = 200;

    // Q1: point lookup
    let t = Instant::now();
    let mut s = 0u64;
    for _ in 0..iters_point {
        if let Some(v) = eng.get(target_eid, "salary") { s = s.wrapping_add(v as u64); }
    }
    let e_q1_ns = t.elapsed().as_nanos() as f64 / iters_point as f64;

    // Q2: filter list (dept_id = 42)
    let t = Instant::now();
    let mut len_acc = 0u64;
    for _ in 0..iters_med {
        let v = eng.pull_raw("dept_id", TARGET_DEPT);
        len_acc = len_acc.wrapping_add(v.len() as u64);
    }
    let e_q2_us = t.elapsed().as_micros() as f64 / iters_med as f64;

    // Q3: filter aggregation
    let t = Instant::now();
    let mut sum_acc = 0u64;
    for _ in 0..iters_med {
        let v = eng.pull_raw("dept_id", TARGET_DEPT);
        sum_acc = sum_acc.wrapping_add(eng.sum("salary", &v));
    }
    let e_q3_us = t.elapsed().as_micros() as f64 / iters_med as f64;

    // Q4: full aggregation (全 eid sum) — 全 eids は手元にある
    let t = Instant::now();
    let mut full_acc = 0u64;
    for _ in 0..iters_heavy {
        full_acc = full_acc.wrapping_add(eng.sum("salary", &eids));
    }
    let e_q4_ms = t.elapsed().as_micros() as f64 / iters_heavy as f64 / 1000.0;

    // Q5: group_sum dept → salary
    let t = Instant::now();
    let mut g_acc = 0u64;
    for _ in 0..iters_heavy {
        let g = eng.group_sum("dept_id", "salary", &eids);
        g_acc = g_acc.wrapping_add(g.len() as u64);
    }
    let e_q5_ms = t.elapsed().as_micros() as f64 / iters_heavy as f64 / 1000.0;

    // ──── DuckDB 計測 ────
    eprintln!("=== duckdb timing ===");
    let d_q1_ns = duckdb_time_loop(duck_path,
        &format!("SELECT salary FROM emp WHERE user_id = {}", 123_456),
        50_000);
    let d_q2_us = duckdb_time_loop(duck_path,
        &format!("SELECT user_id FROM emp WHERE dept_id = {}", TARGET_DEPT),
        iters_med) / 1_000.0;
    let d_q3_us = duckdb_time_loop(duck_path,
        &format!("SELECT SUM(salary) FROM emp WHERE dept_id = {}", TARGET_DEPT),
        iters_med) / 1_000.0;
    let d_q4_ms = duckdb_time_loop(duck_path,
        "SELECT SUM(salary) FROM emp",
        iters_heavy) / 1_000_000.0;
    let d_q5_ms = duckdb_time_loop(duck_path,
        "SELECT dept_id, SUM(salary) FROM emp GROUP BY dept_id",
        iters_heavy) / 1_000_000.0;

    // ──── sqlite3 計測 ────
    eprintln!("=== sqlite3 timing ===");
    let s_q1_ns = sqlite_time_loop(sqlite_path,
        &format!("SELECT salary FROM emp WHERE user_id = {};", 123_456),
        20_000);
    let s_q2_us = sqlite_time_loop(sqlite_path,
        &format!("SELECT user_id FROM emp WHERE dept_id = {};", TARGET_DEPT),
        iters_med) / 1_000.0;
    let s_q3_us = sqlite_time_loop(sqlite_path,
        &format!("SELECT SUM(salary) FROM emp WHERE dept_id = {};", TARGET_DEPT),
        iters_med) / 1_000.0;
    let s_q4_ms = sqlite_time_loop(sqlite_path,
        "SELECT SUM(salary) FROM emp;",
        iters_heavy) / 1_000_000.0;
    let s_q5_ms = sqlite_time_loop(sqlite_path,
        "SELECT dept_id, SUM(salary) FROM emp GROUP BY dept_id;",
        iters_heavy) / 1_000_000.0;

    // ──── 結果テーブル ────
    println!();
    println!("=========================================================================");
    println!("  enchudb vs DuckDB vs sqlite3 — 1M rows / Apple Silicon");
    println!("=========================================================================");
    println!();
    println!("                                |  enchudb  |   DuckDB  |  sqlite3  |  winner");
    println!("--------------------------------+-----------+-----------+-----------+--------");
    println!("Q1 point lookup    (ns/op)      | {:>9.1} | {:>9.1} | {:>9.1} |  {}",
        e_q1_ns, d_q1_ns, s_q1_ns, winner(&[e_q1_ns, d_q1_ns, s_q1_ns]));
    println!("Q2 filter list 10K (us/op)      | {:>9.1} | {:>9.1} | {:>9.1} |  {}",
        e_q2_us, d_q2_us, s_q2_us, winner(&[e_q2_us, d_q2_us, s_q2_us]));
    println!("Q3 filter SUM      (us/op)      | {:>9.1} | {:>9.1} | {:>9.1} |  {}",
        e_q3_us, d_q3_us, s_q3_us, winner(&[e_q3_us, d_q3_us, s_q3_us]));
    println!("Q4 full SUM 1M     (ms/op)      | {:>9.2} | {:>9.2} | {:>9.2} |  {}",
        e_q4_ms, d_q4_ms, s_q4_ms, winner(&[e_q4_ms, d_q4_ms, s_q4_ms]));
    println!("Q5 GROUP BY SUM    (ms/op)      | {:>9.2} | {:>9.2} | {:>9.2} |  {}",
        e_q5_ms, d_q5_ms, s_q5_ms, winner(&[e_q5_ms, d_q5_ms, s_q5_ms]));
    println!();
    println!("(_acc 抑止用: s={} len={} sum={} full={} g={})",
        s, len_acc, sum_acc, full_acc, g_acc);

    let _ = std::fs::remove_file(db_path);
    let _ = std::fs::remove_file(format!("{}.wal", db_path));
    let _ = std::fs::remove_file(csv_path);
    let _ = std::fs::remove_file(duck_path);
    let _ = std::fs::remove_file(sqlite_path);
}

fn winner(times: &[f64]) -> &'static str {
    // index 0 = enchudb, 1 = duckdb, 2 = sqlite
    let (mut min_i, mut min_v) = (0, f64::MAX);
    for (i, &v) in times.iter().enumerate() {
        if v < min_v { min_v = v; min_i = i; }
    }
    match min_i { 0 => "enchudb", 1 => "duckdb", 2 => "sqlite", _ => "?" }
}

fn duckdb_setup(path: &str, csv: &str) {
    let sql = format!(
        "CREATE TABLE emp AS SELECT * FROM read_csv_auto('{}'); \
         CREATE INDEX idx_dept ON emp(dept_id); \
         CREATE INDEX idx_user ON emp(user_id);",
        csv
    );
    let out = std::process::Command::new("duckdb")
        .arg(path).arg("-c").arg(&sql)
        .output().expect("duckdb setup failed");
    if !out.status.success() {
        panic!("duckdb setup err: {}", String::from_utf8_lossy(&out.stderr));
    }
}

fn sqlite_setup(path: &str, csv: &str) {
    let sql = format!(
        ".mode csv\n\
         CREATE TABLE emp(user_id INTEGER, dept_id INTEGER, salary INTEGER, year INTEGER);\n\
         .import --skip 1 {} emp\n\
         CREATE INDEX idx_dept ON emp(dept_id);\n\
         CREATE INDEX idx_user ON emp(user_id);\n",
        csv
    );
    let mut child = std::process::Command::new("sqlite3")
        .arg(path)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn().expect("sqlite spawn");
    child.stdin.as_mut().unwrap().write_all(sql.as_bytes()).unwrap();
    let out = child.wait_with_output().unwrap();
    if !out.status.success() {
        panic!("sqlite setup err: {}", String::from_utf8_lossy(&out.stderr));
    }
}

/// duckdb CLI を 1 度起動して同じクエリを iters 回投げ、 wall を iters で割る (ns/op)
fn duckdb_time_loop(path: &str, query: &str, iters: u32) -> f64 {
    // .timer は per-statement なので、 一括投げて全体時間を計る方が安定
    let mut script = String::new();
    // warmup
    for _ in 0..3 { script.push_str(query); script.push_str(";\n"); }
    // 実測
    let t = Instant::now();
    for _ in 0..iters { script.push_str(query); script.push_str(";\n"); }
    let _build = t.elapsed();

    let t = Instant::now();
    let mut child = std::process::Command::new("duckdb")
        .arg(path).arg("-noheader").arg("-csv")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn().expect("duckdb spawn");
    child.stdin.as_mut().unwrap().write_all(script.as_bytes()).unwrap();
    let _ = child.wait();
    let elapsed = t.elapsed();
    elapsed.as_nanos() as f64 / iters as f64
}

fn sqlite_time_loop(path: &str, query: &str, iters: u32) -> f64 {
    let mut script = String::new();
    for _ in 0..3 { script.push_str(query); script.push('\n'); }
    for _ in 0..iters { script.push_str(query); script.push('\n'); }

    let t = Instant::now();
    let mut child = std::process::Command::new("sqlite3")
        .arg(path)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn().expect("sqlite spawn");
    child.stdin.as_mut().unwrap().write_all(script.as_bytes()).unwrap();
    let _ = child.wait();
    let elapsed = t.elapsed();
    elapsed.as_nanos() as f64 / iters as f64
}
