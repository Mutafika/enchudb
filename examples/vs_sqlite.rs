//! EnchuDB vs SQLite ベンチマーク
//!
//! cargo run --features v27 --release --example vs_sqlite

use enchudb::*;
use rusqlite::Connection;
use std::time::Instant;

const ENTITY_COUNT: u32 = 1_000_000;
const DEPT_COUNT: u32 = 20;
const STATUS_COUNT: u32 = 5;
const SALARY_MAX: u32 = 1000;

fn main() {
    println!("═══ EnchuDB vs SQLite ═══");
    println!("entities: {ENTITY_COUNT}");
    println!();

    // ── セットアップ ──
    let enchu_path = "/tmp/vs_sqlite_enchu.db";
    let sqlite_path = "/tmp/vs_sqlite.sqlite";
    let _ = std::fs::remove_file(enchu_path);
    let _ = std::fs::remove_file(sqlite_path);

    // EnchuDB
    let t = Instant::now();
    let mut edb = Engine::create_with_capacity(enchu_path, ENTITY_COUNT + 100).unwrap();
    edb.define_himo("dept", HimoType::Value, DEPT_COUNT);
    edb.define_himo("status", HimoType::Value, STATUS_COUNT);
    edb.define_himo("salary", HimoType::Value, SALARY_MAX);
    edb.define_himo("age", HimoType::Value, 100);

    for i in 0..ENTITY_COUNT {
        let e = edb.entity();
        edb.tie(e, "dept", i % DEPT_COUNT);
        edb.tie(e, "status", (i * 3 + 1) % STATUS_COUNT);
        edb.tie(e, "salary", (i * 7 + 13) % SALARY_MAX);
        edb.tie(e, "age", 20 + i % 50);
    }
    edb.rebuild();
    let enchu_setup = t.elapsed();
    println!("setup enchudb: {:.1}ms", enchu_setup.as_secs_f64() * 1000.0);

    // SQLite
    let t = Instant::now();
    let sdb = Connection::open(sqlite_path).unwrap();
    sdb.execute_batch("
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = OFF;
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            dept INTEGER NOT NULL,
            status INTEGER NOT NULL,
            salary INTEGER NOT NULL,
            age INTEGER NOT NULL
        );
        CREATE INDEX idx_dept ON employees(dept);
        CREATE INDEX idx_status ON employees(status);
        CREATE INDEX idx_salary ON employees(salary);
        CREATE INDEX idx_age ON employees(age);
    ").unwrap();

    {
        let tx = sdb.unchecked_transaction().unwrap();
        let mut stmt = sdb.prepare("INSERT INTO employees (dept, status, salary, age) VALUES (?1, ?2, ?3, ?4)").unwrap();
        for i in 0..ENTITY_COUNT {
            stmt.execute(rusqlite::params![
                i % DEPT_COUNT,
                (i * 3 + 1) % STATUS_COUNT,
                (i * 7 + 13) % SALARY_MAX,
                20 + i % 50,
            ]).unwrap();
        }
        tx.commit().unwrap();
    }
    let sqlite_setup = t.elapsed();
    println!("setup sqlite:  {:.1}ms", sqlite_setup.as_secs_f64() * 1000.0);
    println!();

    // ── ベンチ ──
    let iterations = 100;

    // 1. 単一条件フィルタ
    bench("1条件 (dept=3)", iterations, || {
        let r = edb.pull_raw("dept", 3);
        assert!(!r.is_empty());
    }, || {
        let mut stmt = sdb.prepare_cached("SELECT id FROM employees WHERE dept = 3").unwrap();
        let ids: Vec<i64> = stmt.query_map([], |row| row.get(0)).unwrap()
            .filter_map(|r| r.ok()).collect();
        assert!(!ids.is_empty());
    });

    // 2. 2条件フィルタ
    bench("2条件 (dept=0 AND status=1)", iterations, || {
        let r = edb.query(&[("dept", 0), ("status", 1)]);
        assert!(!r.is_empty());
    }, || {
        let mut stmt = sdb.prepare_cached("SELECT id FROM employees WHERE dept = 0 AND status = 1").unwrap();
        let ids: Vec<i64> = stmt.query_map([], |row| row.get(0)).unwrap()
            .filter_map(|r| r.ok()).collect();
        assert!(!ids.is_empty());
    });

    // 3. 3条件フィルタ
    bench("3条件 (dept=0 AND status=1 AND age=20)", iterations, || {
        let r = edb.query(&[("dept", 0), ("status", 1), ("age", 0)]);
        let _ = r.len();
    }, || {
        let mut stmt = sdb.prepare_cached("SELECT id FROM employees WHERE dept = 0 AND status = 1 AND age = 20").unwrap();
        let ids: Vec<i64> = stmt.query_map([], |row| row.get(0)).unwrap()
            .filter_map(|r| r.ok()).collect();
        let _ = ids.len();
    });

    // 4. SUM
    bench("SUM (全件)", iterations, || {
        let all = edb.entities();
        let s = edb.sum("salary", &all);
        assert!(s > 0);
    }, || {
        let s: i64 = sdb.query_row("SELECT SUM(salary) FROM employees", [], |row| row.get(0)).unwrap();
        assert!(s > 0);
    });

    // 5. SUM + フィルタ
    bench("SUM (dept=3)", iterations, || {
        let ids = edb.pull_raw("dept", 3);
        let s = edb.sum("salary", &ids);
        assert!(s > 0);
    }, || {
        let s: i64 = sdb.query_row("SELECT SUM(salary) FROM employees WHERE dept = 3", [], |row| row.get(0)).unwrap();
        assert!(s > 0);
    });

    // 6. GROUP BY + SUM
    bench("GROUP BY dept SUM salary (全件)", iterations, || {
        let all = edb.entities();
        let g = edb.group_sum("dept", "salary", &all);
        assert_eq!(g.len(), DEPT_COUNT as usize);
    }, || {
        let mut stmt = sdb.prepare_cached("SELECT dept, SUM(salary) FROM employees GROUP BY dept").unwrap();
        let rows: Vec<(i64, i64)> = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?))).unwrap()
            .filter_map(|r| r.ok()).collect();
        assert_eq!(rows.len(), DEPT_COUNT as usize);
    });

    // 7. 範囲クエリ
    bench("範囲 (age 30..40)", iterations, || {
        let r = edb.pull_range("age", 10, 20);
        assert!(!r.is_empty());
    }, || {
        let mut stmt = sdb.prepare_cached("SELECT id FROM employees WHERE age BETWEEN 30 AND 40").unwrap();
        let ids: Vec<i64> = stmt.query_map([], |row| row.get(0)).unwrap()
            .filter_map(|r| r.ok()).collect();
        assert!(!ids.is_empty());
    });

    // 8. COUNT
    bench("COUNT (status=2)", iterations, || {
        let ids = edb.pull_raw("status", 2);
        let c = ids.len();
        assert!(c > 0);
    }, || {
        let c: i64 = sdb.query_row("SELECT COUNT(*) FROM employees WHERE status = 2", [], |row| row.get(0)).unwrap();
        assert!(c > 0);
    });

    // 9. MIN / MAX
    bench("MIN/MAX salary (dept=5)", iterations, || {
        let ids = edb.pull_raw("dept", 5);
        let mn = edb.min("salary", &ids);
        let mx = edb.max("salary", &ids);
        assert!(mn.is_some() && mx.is_some());
    }, || {
        let _: (i64, i64) = sdb.query_row(
            "SELECT MIN(salary), MAX(salary) FROM employees WHERE dept = 5", [],
            |row| Ok((row.get(0)?, row.get(1)?))
        ).unwrap();
    });

    // 10. ポイント読み取り
    bench("get (単一entity)", iterations, || {
        let v = edb.get(500, "salary");
        assert!(v.is_some());
    }, || {
        let _: i64 = sdb.query_row("SELECT salary FROM employees WHERE id = 501", [], |row| row.get(0)).unwrap();
    });

    // cleanup
    let _ = std::fs::remove_file(enchu_path);
    let _ = std::fs::remove_file(sqlite_path);
}

fn bench(label: &str, iterations: usize, enchu_fn: impl Fn(), sqlite_fn: impl Fn()) {
    // warmup
    for _ in 0..3 { enchu_fn(); sqlite_fn(); }

    let t = Instant::now();
    for _ in 0..iterations { enchu_fn(); }
    let enchu_ns = t.elapsed().as_nanos() / iterations as u128;

    let t = Instant::now();
    for _ in 0..iterations { sqlite_fn(); }
    let sqlite_ns = t.elapsed().as_nanos() / iterations as u128;

    let ratio = if enchu_ns > 0 { sqlite_ns as f64 / enchu_ns as f64 } else { f64::INFINITY };

    println!("{label}");
    println!("  enchu:  {}", format_ns(enchu_ns));
    println!("  sqlite: {}", format_ns(sqlite_ns));
    println!("  → {:.0}x", ratio);
    println!();
}

fn format_ns(ns: u128) -> String {
    if ns >= 1_000_000 { format!("{:.2}ms", ns as f64 / 1_000_000.0) }
    else if ns >= 1_000 { format!("{:.1}µs", ns as f64 / 1_000.0) }
    else { format!("{ns}ns") }
}
