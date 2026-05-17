//! Schema 層 EnchuDB vs SQLite。
//!
//! 0.3.0 で schema 層が zero-cost 化されたので、 schema layer のまま SQLite と
//! 比較する (公開 README が推奨するパスでの計測)。 aggregates (SUM / MIN / MAX /
//! GROUP BY) は schema 層に未提供なので `db.engine()` に降りる — これは
//! 「declarative で書きつつ hot loop だけ engine に降りる」の典型例。
//!
//! cargo run --release --example vs_sqlite

use enchudb::schema::Database;
use enchudb_wal::EntityId;
use rusqlite::Connection;
use std::time::Instant;

const ENTITY_COUNT: u32 = 1_000_000;
const DEPT_COUNT: u32 = 20;
const STATUS_COUNT: u32 = 5;
const SALARY_MAX: u32 = 1000;
const ITERATIONS: u32 = 100;

fn main() {
    println!("═══ EnchuDB (schema 層) vs SQLite ═══");
    println!("entities: {ENTITY_COUNT}");
    println!();

    let enchu_path = "/tmp/vs_sqlite_enchu.db";
    let sqlite_path = "/tmp/vs_sqlite.sqlite";
    let _ = std::fs::remove_file(enchu_path);
    let _ = std::fs::remove_file(sqlite_path);

    // ── EnchuDB セットアップ (schema 層) ──
    let t = Instant::now();
    let mut edb = Database::create_growable_with_capacity(enchu_path, ENTITY_COUNT + 100).unwrap();
    {
        let _ = edb.table("employees")
            .number("id")
            .number("dept")
            .number("status")
            .number("salary")
            .number("age")
            .primary_key("id")
            .build().unwrap();
    }
    let employees = edb.get_table("employees").unwrap();
    for i in 0..ENTITY_COUNT {
        employees.insert()
            .set("id", i as i64)
            .set("dept", (i % DEPT_COUNT) as i64)
            .set("status", ((i * 3 + 1) % STATUS_COUNT) as i64)
            .set("salary", ((i * 7 + 13) % SALARY_MAX) as i64)
            .set("age", (20 + i % 50) as i64)
            .commit().unwrap();
    }
    let enchu_setup = t.elapsed();
    println!("setup enchudb: {:.1}ms", enchu_setup.as_secs_f64() * 1000.0);

    // ── SQLite セットアップ ──
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
        let mut stmt = sdb.prepare(
            "INSERT INTO employees (id, dept, status, salary, age) VALUES (?1, ?2, ?3, ?4, ?5)"
        ).unwrap();
        for i in 0..ENTITY_COUNT {
            stmt.execute(rusqlite::params![
                i,
                i % DEPT_COUNT,
                (i * 3 + 1) % STATUS_COUNT,
                (i * 7 + 13) % SALARY_MAX,
                20 + i % 50,
            ]).unwrap();
        }
        drop(stmt);
        tx.commit().unwrap();
    }
    let sqlite_setup = t.elapsed();
    println!("setup sqlite:  {:.1}ms", sqlite_setup.as_secs_f64() * 1000.0);
    println!();

    // ── ベンチ ──

    // 1 条件 point query
    bench("1条件 (dept=3) — find", ITERATIONS, || {
        let r = employees.where_eq("dept", 3i64).find().unwrap();
        assert!(!r.is_empty());
    }, || {
        let mut stmt = sdb.prepare_cached("SELECT id FROM employees WHERE dept = 3").unwrap();
        let ids: Vec<i64> = stmt.query_map([], |row| row.get(0)).unwrap()
            .filter_map(|r| r.ok()).collect();
        assert!(!ids.is_empty());
    });

    // 2 条件 AND
    bench("2条件 (dept=0 AND status=1)", ITERATIONS, || {
        let r = employees
            .where_eq("dept", 0i64)
            .where_eq("status", 1i64)
            .find().unwrap();
        assert!(!r.is_empty());
    }, || {
        let mut stmt = sdb.prepare_cached(
            "SELECT id FROM employees WHERE dept = 0 AND status = 1"
        ).unwrap();
        let ids: Vec<i64> = stmt.query_map([], |row| row.get(0)).unwrap()
            .filter_map(|r| r.ok()).collect();
        assert!(!ids.is_empty());
    });

    // 3 条件 AND
    bench("3条件 (dept=0 AND status=1 AND age=20)", ITERATIONS, || {
        let r = employees
            .where_eq("dept", 0i64)
            .where_eq("status", 1i64)
            .where_eq("age", 20i64)
            .find().unwrap();
        let _ = r.len();
    }, || {
        let mut stmt = sdb.prepare_cached(
            "SELECT id FROM employees WHERE dept = 0 AND status = 1 AND age = 20"
        ).unwrap();
        let ids: Vec<i64> = stmt.query_map([], |row| row.get(0)).unwrap()
            .filter_map(|r| r.ok()).collect();
        let _ = ids.len();
    });

    // 範囲
    bench("範囲 (age 30..40)", ITERATIONS, || {
        let r = employees.where_range("age", 30, 40).find().unwrap();
        assert!(!r.is_empty());
    }, || {
        let mut stmt = sdb.prepare_cached(
            "SELECT id FROM employees WHERE age BETWEEN 30 AND 40"
        ).unwrap();
        let ids: Vec<i64> = stmt.query_map([], |row| row.get(0)).unwrap()
            .filter_map(|r| r.ok()).collect();
        assert!(!ids.is_empty());
    });

    // COUNT
    bench("COUNT (status=2)", ITERATIONS, || {
        let c = employees.where_eq("status", 2i64).count().unwrap();
        assert!(c > 0);
    }, || {
        let c: i64 = sdb.query_row(
            "SELECT COUNT(*) FROM employees WHERE status = 2", [],
            |row| row.get(0),
        ).unwrap();
        assert!(c > 0);
    });

    // ── aggregates: schema 層に sum / min / max / group_* が無いので engine 直叩き ──
    // 注: schema 層は himo を `"{table}.{col}"` 形式で命名するので、
    //     engine 直叩き時は table prefix が必要。
    let eng = edb.engine();
    let h_salary = "employees.salary";
    let h_dept = "employees.dept";

    // SUM (filtered)
    bench("SUM salary (dept=3)", ITERATIONS, || {
        let ids: Vec<EntityId> = employees.where_eq("dept", 3i64).find().unwrap();
        let s = eng.sum(h_salary, &ids);
        assert!(s > 0);
    }, || {
        let s: i64 = sdb.query_row(
            "SELECT SUM(salary) FROM employees WHERE dept = 3", [],
            |row| row.get(0),
        ).unwrap();
        assert!(s > 0);
    });

    // SUM (全件)
    bench("SUM salary (全件)", ITERATIONS, || {
        let all = eng.entities();
        let s = eng.sum(h_salary, &all);
        assert!(s > 0);
    }, || {
        let s: i64 = sdb.query_row(
            "SELECT SUM(salary) FROM employees", [],
            |row| row.get(0),
        ).unwrap();
        assert!(s > 0);
    });

    // GROUP BY + SUM
    bench("GROUP BY dept SUM salary (全件)", ITERATIONS, || {
        let all = eng.entities();
        let g = eng.group_sum(h_dept, h_salary, &all);
        assert_eq!(g.len(), DEPT_COUNT as usize);
    }, || {
        let mut stmt = sdb.prepare_cached(
            "SELECT dept, SUM(salary) FROM employees GROUP BY dept"
        ).unwrap();
        let rows: Vec<(i64, i64)> = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap().filter_map(|r| r.ok()).collect();
        assert_eq!(rows.len(), DEPT_COUNT as usize);
    });

    // MIN / MAX
    bench("MIN/MAX salary (dept=5)", ITERATIONS, || {
        let ids: Vec<EntityId> = employees.where_eq("dept", 5i64).find().unwrap();
        let _ = eng.min(h_salary, &ids);
        let _ = eng.max(h_salary, &ids);
    }, || {
        let _: (i64, i64) = sdb.query_row(
            "SELECT MIN(salary), MAX(salary) FROM employees WHERE dept = 5", [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).unwrap();
    });

    // cleanup
    let _ = std::fs::remove_file(enchu_path);
    let _ = std::fs::remove_file(sqlite_path);
}

fn bench<F1: Fn(), F2: Fn()>(label: &str, iterations: u32, enchu_fn: F1, sqlite_fn: F2) {
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
    println!("  enchudb (schema): {}", format_ns(enchu_ns));
    println!("  sqlite:           {}", format_ns(sqlite_ns));
    println!("  → {:.0}x", ratio);
    println!();
}

fn format_ns(ns: u128) -> String {
    if ns >= 1_000_000 { format!("{:.2}ms", ns as f64 / 1_000_000.0) }
    else if ns >= 1_000 { format!("{:.1}µs", ns as f64 / 1_000.0) }
    else { format!("{ns}ns") }
}
