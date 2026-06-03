//! Schema 層 EnchuDB vs SQLite vs DuckDB vs LMDB — 4-way 比較。
//!
//! 0.3.0 で schema 層が zero-cost 化されたので、 schema layer のまま競合 DB と
//! 比較する (公開 README が推奨するパスでの計測)。 aggregates (SUM / MIN / MAX /
//! GROUP BY) は schema 層に未提供なので `db.engine()` に降りる — これは
//! 「declarative で書きつつ hot loop だけ engine に降りる」の典型例。
//!
//! 0.8.5 で旧 `vs_sqlite` を 3-way 化 (DuckDB を追加)。 旧 examples/
//! `battle_vs_duckdb.rs` は CLI subprocess 経由で計測不公正だったが、 ここでは
//! **`duckdb` crate (in-process)** に揃え、 fair に比較する。
//!
//! 0.8.11 で LMDB (heed, in-process mmap KV) を追加して 4-way 化。 LMDB は raw KV
//! なので「全 entity の id → row」 primary に加え、 secondary index を
//! **composite key (value‖id → ())** で手張りする (= LMDB を index 付き query
//! store として使う公平な実装)。 enchudb の cylinder が自動で持つものを LMDB
//! では明示構築する形。 point-by-PK lookup は LMDB の本領なので必ず入れて、
//! 「enchudb に有利な土俵だけ」 にならないようにしている。
//!
//! cargo run --release --example vs_db

use enchudb::schema::Database;
use enchudb_oplog::EntityId;
use rusqlite::Connection as SqliteConn;
use duckdb::Connection as DuckConn;
use heed::types::Bytes;
use heed::EnvOpenOptions;
use std::time::Instant;

const ENTITY_COUNT: u32 = 1_000_000;
const DEPT_COUNT: u32 = 20;
const STATUS_COUNT: u32 = 5;
const SALARY_MAX: u32 = 1000;
const ITERATIONS: u32 = 100;

// LMDB primary row layout: [dept(4) | status(4) | salary(4) | age(4)] BE u32 each.
const OFF_DEPT: usize = 0;
const OFF_STATUS: usize = 4;
const OFF_SALARY: usize = 8;
const OFF_AGE: usize = 12;

type Lk = heed::Database<Bytes, Bytes>;

fn main() {
    println!("═══ EnchuDB (schema 層) vs SQLite vs DuckDB vs LMDB ═══");
    println!("entities: {ENTITY_COUNT}");
    println!();

    let enchu_path = "/tmp/vs_db_enchu.db";
    let sqlite_path = "/tmp/vs_db_sqlite.sqlite";
    let duck_path = "/tmp/vs_db_duck.duckdb";
    let lmdb_dir = "/tmp/vs_db_lmdb";
    let _ = std::fs::remove_file(enchu_path);
    let _ = std::fs::remove_file(sqlite_path);
    let _ = std::fs::remove_file(duck_path);
    let _ = std::fs::remove_file(format!("{}.oplog", enchu_path));
    let _ = std::fs::remove_file(format!("{}.tables", enchu_path));
    let _ = std::fs::remove_file(format!("{}.crc", enchu_path));
    let _ = std::fs::remove_file(format!("{}.db.lock", enchu_path));
    let _ = std::fs::remove_dir_all(lmdb_dir);

    // ── EnchuDB セットアップ (schema 層) ──
    let t = Instant::now();
    let mut edb = Database::create_growable_with_capacity(enchu_path, ENTITY_COUNT + 1000).unwrap();
    {
        let _ = edb.table("employees")
            .number("id")
            .number("dept").cardinality(DEPT_COUNT)       // #46: group key に cap を渡す
            .number("status").cardinality(STATUS_COUNT)
            .number("salary").cardinality(SALARY_MAX)
            .number("age").cardinality(50)
            .primary_key("id")
            .with_capacity(ENTITY_COUNT as u32 + 100)
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
    println!("setup enchudb: {:>8.1}ms", enchu_setup.as_secs_f64() * 1000.0);

    // ── SQLite セットアップ ──
    let t = Instant::now();
    let sdb = SqliteConn::open(sqlite_path).unwrap();
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
    println!("setup sqlite:  {:>8.1}ms", sqlite_setup.as_secs_f64() * 1000.0);

    // ── DuckDB セットアップ (in-process) ──
    // DuckDB は OLAP 向け columnar engine。 individual INSERT は遅いので
    // Appender API でバルク insert する (= row-store の prepared insert 相当)。
    let t = Instant::now();
    let ddb = DuckConn::open(duck_path).unwrap();
    ddb.execute_batch("
        CREATE TABLE employees (
            id INTEGER NOT NULL,
            dept INTEGER NOT NULL,
            status INTEGER NOT NULL,
            salary INTEGER NOT NULL,
            age INTEGER NOT NULL
        );
    ").unwrap();
    {
        let mut app = ddb.appender("employees").unwrap();
        for i in 0..ENTITY_COUNT {
            app.append_row(duckdb::params![
                i as i32,
                (i % DEPT_COUNT) as i32,
                ((i * 3 + 1) % STATUS_COUNT) as i32,
                ((i * 7 + 13) % SALARY_MAX) as i32,
                (20 + i % 50) as i32,
            ]).unwrap();
        }
        app.flush().unwrap();
    }
    // DuckDB は index 不要 (= columnar scan で十分速い)、 ただし PK 同等の
    // 追加 metadata を入れない (公正比較のため)。
    let duck_setup = t.elapsed();
    println!("setup duckdb:  {:>8.1}ms", duck_setup.as_secs_f64() * 1000.0);

    // ── LMDB セットアップ (heed, in-process mmap KV) ──
    // raw KV なので primary (id → row) + secondary index 3 本 (dept/status/age)
    // を composite key で手張り。 enchudb の cylinder 相当を明示構築する。
    let t = Instant::now();
    std::fs::create_dir_all(lmdb_dir).unwrap();
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(8 * 1024 * 1024 * 1024) // 8 GiB virtual (sparse、実使用は書いた分)
            .max_dbs(8)
            .open(lmdb_dir)
    }.unwrap();
    let mut wtxn = env.write_txn().unwrap();
    let primary: Lk = env.create_database(&mut wtxn, Some("primary")).unwrap();
    let idx_dept: Lk = env.create_database(&mut wtxn, Some("idx_dept")).unwrap();
    let idx_status: Lk = env.create_database(&mut wtxn, Some("idx_status")).unwrap();
    let idx_age: Lk = env.create_database(&mut wtxn, Some("idx_age")).unwrap();
    for i in 0..ENTITY_COUNT {
        let dept = i % DEPT_COUNT;
        let status = (i * 3 + 1) % STATUS_COUNT;
        let salary = (i * 7 + 13) % SALARY_MAX;
        let age = 20 + i % 50;
        let mut row = [0u8; 16];
        row[OFF_DEPT..OFF_DEPT + 4].copy_from_slice(&dept.to_be_bytes());
        row[OFF_STATUS..OFF_STATUS + 4].copy_from_slice(&status.to_be_bytes());
        row[OFF_SALARY..OFF_SALARY + 4].copy_from_slice(&salary.to_be_bytes());
        row[OFF_AGE..OFF_AGE + 4].copy_from_slice(&age.to_be_bytes());
        let idk = i.to_be_bytes();
        primary.put(&mut wtxn, &idk, &row).unwrap();
        idx_dept.put(&mut wtxn, &k8(dept, i), &[]).unwrap();
        idx_status.put(&mut wtxn, &k8(status, i), &[]).unwrap();
        idx_age.put(&mut wtxn, &k8(age, i), &[]).unwrap();
    }
    wtxn.commit().unwrap();
    let rtxn = env.read_txn().unwrap();
    let lmdb_setup = t.elapsed();
    println!("setup lmdb:    {:>8.1}ms", lmdb_setup.as_secs_f64() * 1000.0);
    println!();

    // ── ベンチ ──

    // point-by-PK — LMDB の本領 (直接 B+tree get)。 enchudb は id cylinder の
    // unique pull、 sqlite は PK index、 duckdb は index 無しで full scan。
    let pk: u32 = 654_321;
    bench("point-by-PK (id=654321)", ITERATIONS,
        || {
            employees.where_eq("id", pk as i64).find_one().unwrap().is_some() as usize
        },
        || {
            sdb.query_row("SELECT id FROM employees WHERE id = 654321", [],
                |r| r.get::<_, i64>(0)).map(|_| 1usize).unwrap_or(0)
        },
        || {
            ddb.query_row("SELECT id FROM employees WHERE id = 654321", [],
                |r| r.get::<_, i32>(0)).map(|_| 1usize).unwrap_or(0)
        },
        || {
            primary.get(&rtxn, &pk.to_be_bytes()).unwrap().is_some() as usize
        },
    );

    // 1 条件 point query
    bench("1条件 (dept=3) — find", ITERATIONS,
        || {
            let r = employees.where_eq("dept", 3i64).find().unwrap();
            r.len()
        },
        || {
            let mut stmt = sdb.prepare_cached("SELECT id FROM employees WHERE dept = 3").unwrap();
            let ids: Vec<i64> = stmt.query_map([], |row| row.get(0)).unwrap()
                .filter_map(|r| r.ok()).collect();
            ids.len()
        },
        || {
            let mut stmt = ddb.prepare("SELECT id FROM employees WHERE dept = 3").unwrap();
            let ids: Vec<i32> = stmt.query_map([], |row| row.get(0)).unwrap()
                .filter_map(|r| r.ok()).collect();
            ids.len()
        },
        || {
            lmdb_eq_ids(&rtxn, &idx_dept, 3).len()
        },
    );

    // 2 条件 AND
    bench("2条件 (dept=0 AND status=1)", ITERATIONS,
        || {
            let r = employees
                .where_eq("dept", 0i64)
                .where_eq("status", 1i64)
                .find().unwrap();
            r.len()
        },
        || {
            let mut stmt = sdb.prepare_cached(
                "SELECT id FROM employees WHERE dept = 0 AND status = 1"
            ).unwrap();
            let ids: Vec<i64> = stmt.query_map([], |row| row.get(0)).unwrap()
                .filter_map(|r| r.ok()).collect();
            ids.len()
        },
        || {
            let mut stmt = ddb.prepare(
                "SELECT id FROM employees WHERE dept = 0 AND status = 1"
            ).unwrap();
            let ids: Vec<i32> = stmt.query_map([], |row| row.get(0)).unwrap()
                .filter_map(|r| r.ok()).collect();
            ids.len()
        },
        || {
            // pivot = 最も selective な dept (1M/20=50K) を index scan、 残りは
            // primary row 直読みで filter。 enchudb の min-slice pivot 戦略と同型。
            let ids = lmdb_eq_ids(&rtxn, &idx_dept, 0);
            ids.iter().filter(|&&id| lmdb_col(&rtxn, &primary, id, OFF_STATUS) == 1).count()
        },
    );

    // 3 条件 AND
    bench("3条件 (dept=0 AND status=1 AND age=20)", ITERATIONS,
        || {
            let r = employees
                .where_eq("dept", 0i64)
                .where_eq("status", 1i64)
                .where_eq("age", 20i64)
                .find().unwrap();
            r.len()
        },
        || {
            let mut stmt = sdb.prepare_cached(
                "SELECT id FROM employees WHERE dept = 0 AND status = 1 AND age = 20"
            ).unwrap();
            let ids: Vec<i64> = stmt.query_map([], |row| row.get(0)).unwrap()
                .filter_map(|r| r.ok()).collect();
            ids.len()
        },
        || {
            let mut stmt = ddb.prepare(
                "SELECT id FROM employees WHERE dept = 0 AND status = 1 AND age = 20"
            ).unwrap();
            let ids: Vec<i32> = stmt.query_map([], |row| row.get(0)).unwrap()
                .filter_map(|r| r.ok()).collect();
            ids.len()
        },
        || {
            let ids = lmdb_eq_ids(&rtxn, &idx_dept, 0);
            ids.iter().filter(|&&id| {
                lmdb_col(&rtxn, &primary, id, OFF_STATUS) == 1
                    && lmdb_col(&rtxn, &primary, id, OFF_AGE) == 20
            }).count()
        },
    );

    // 範囲 — 0.8.6 で hit 率が高い range を engine 直線 scan に切替 (= DuckDB の
    // BETWEEN 相当)。 schema 層 where_range は BucketCylinder reverse union で
    // 高 hit 率では遅いので、 engine の range_scan を直接叩く。
    let eng_for_range = edb.engine();
    let h_age = "employees.age";
    bench("範囲 (age 30..40)", ITERATIONS,
        || {
            let r = eng_for_range.range_scan(h_age, 30, 40);
            r.len()
        },
        || {
            let mut stmt = sdb.prepare_cached(
                "SELECT id FROM employees WHERE age BETWEEN 30 AND 40"
            ).unwrap();
            let ids: Vec<i64> = stmt.query_map([], |row| row.get(0)).unwrap()
                .filter_map(|r| r.ok()).collect();
            ids.len()
        },
        || {
            let mut stmt = ddb.prepare(
                "SELECT id FROM employees WHERE age BETWEEN 30 AND 40"
            ).unwrap();
            let ids: Vec<i32> = stmt.query_map([], |row| row.get(0)).unwrap()
                .filter_map(|r| r.ok()).collect();
            ids.len()
        },
        || {
            // age index を 30..=40 の各 value で prefix scan して union materialize。
            // (heed の range-over-bytes API 摩擦を避けて value loop で実装、 11 seek)
            let mut out: Vec<u32> = Vec::new();
            for v in 30u32..=40 {
                for kv in idx_age.prefix_iter(&rtxn, &v.to_be_bytes()).unwrap() {
                    let (k, _) = kv.unwrap();
                    out.push(u32::from_be_bytes(k[4..8].try_into().unwrap()));
                }
            }
            out.len()
        },
    );

    // COUNT (結果は数字 1 つ、 hits 表示は対象の母集団サイズで)
    bench("COUNT (status=2)", ITERATIONS,
        || {
            employees.where_eq("status", 2i64).count().unwrap()
        },
        || {
            let c: i64 = sdb.query_row(
                "SELECT COUNT(*) FROM employees WHERE status = 2", [],
                |row| row.get(0),
            ).unwrap();
            c as usize
        },
        || {
            let c: i64 = ddb.query_row(
                "SELECT COUNT(*) FROM employees WHERE status = 2", [],
                |row| row.get(0),
            ).unwrap();
            c as usize
        },
        || {
            lmdb_eq_count(&rtxn, &idx_status, 2)
        },
    );

    // ── aggregates: schema 層に sum / min / max / group_* が無いので engine 直叩き ──
    // 注: schema 層は himo を `"{table}.{col}"` 形式で命名するので、
    //     engine 直叩き時は table prefix が必要。
    // LMDB は集計 primitive を持たないので primary を full scan して Rust 側で畳む
    // (= raw KV を OLAP に使うときの実態。 columnar/vectorized でないので不利)。
    let eng = edb.engine();
    let h_salary = "employees.salary";

    // SUM (filtered) — hits = 走査対象数
    bench("SUM salary (dept=3)", ITERATIONS,
        || {
            let ids: Vec<EntityId> = employees.where_eq("dept", 3i64).find().unwrap();
            let _ = eng.sum(h_salary, &ids);
            ids.len()
        },
        || {
            let _: i64 = sdb.query_row(
                "SELECT SUM(salary) FROM employees WHERE dept = 3", [],
                |row| row.get(0),
            ).unwrap();
            ENTITY_COUNT as usize / DEPT_COUNT as usize
        },
        || {
            let _: i64 = ddb.query_row(
                "SELECT SUM(salary) FROM employees WHERE dept = 3", [],
                |row| row.get(0),
            ).unwrap();
            ENTITY_COUNT as usize / DEPT_COUNT as usize
        },
        || {
            let ids = lmdb_eq_ids(&rtxn, &idx_dept, 3);
            let mut s: u64 = 0;
            for &id in &ids { s += lmdb_col(&rtxn, &primary, id, OFF_SALARY) as u64; }
            std::hint::black_box(s);
            ids.len()
        },
    );

    // SUM (table 全体) — schema 層の Table::sum で 1 行 API。 内部で table の
    // [eid_range_lo, hi) を engine sum_range に bind、 column 直 scan で
    // auto-vectorize。 これが README 推奨 path (= 「テーブルが出てきて、
    // そこにある金額をsum」)。
    bench("SUM salary (table)", ITERATIONS,
        || {
            let _ = employees.sum("salary");
            ENTITY_COUNT as usize
        },
        || {
            let _: i64 = sdb.query_row(
                "SELECT SUM(salary) FROM employees", [],
                |row| row.get(0),
            ).unwrap();
            ENTITY_COUNT as usize
        },
        || {
            let _: i64 = ddb.query_row(
                "SELECT SUM(salary) FROM employees", [],
                |row| row.get(0),
            ).unwrap();
            ENTITY_COUNT as usize
        },
        || {
            let mut s: u64 = 0;
            for kv in primary.iter(&rtxn).unwrap() {
                let (_, row) = kv.unwrap();
                s += u32::from_be_bytes(row[OFF_SALARY..OFF_SALARY + 4].try_into().unwrap()) as u64;
            }
            std::hint::black_box(s);
            ENTITY_COUNT as usize
        },
    );

    // GROUP BY + SUM (table 全体) — 公開 Table::group_sum。 dept に
    // `.cardinality(DEPT_COUNT)` を付けたので `group_dense_cap` が効き dense path に
    // 乗る (#46 fix)。 cap=0 だと HashMap fallback で ~5.2ms に落ちていた。
    bench("GROUP BY dept SUM salary (table)", ITERATIONS,
        || {
            let g = employees.group_sum("dept", "salary");
            g.len()
        },
        || {
            let mut stmt = sdb.prepare_cached(
                "SELECT dept, SUM(salary) FROM employees GROUP BY dept"
            ).unwrap();
            let rows: Vec<(i64, i64)> = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap().filter_map(|r| r.ok()).collect();
            rows.len()
        },
        || {
            let mut stmt = ddb.prepare(
                "SELECT dept, SUM(salary) FROM employees GROUP BY dept"
            ).unwrap();
            let rows: Vec<(i32, i64)> = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap().filter_map(|r| r.ok()).collect();
            rows.len()
        },
        || {
            let mut acc = [0u64; DEPT_COUNT as usize];
            for kv in primary.iter(&rtxn).unwrap() {
                let (_, row) = kv.unwrap();
                let dept = u32::from_be_bytes(row[OFF_DEPT..OFF_DEPT + 4].try_into().unwrap()) as usize;
                let salary = u32::from_be_bytes(row[OFF_SALARY..OFF_SALARY + 4].try_into().unwrap()) as u64;
                acc[dept] += salary;
            }
            std::hint::black_box(&acc);
            acc.len()
        },
    );

    // MIN / MAX — hits = 走査対象数
    bench("MIN/MAX salary (dept=5)", ITERATIONS,
        || {
            let ids: Vec<EntityId> = employees.where_eq("dept", 5i64).find().unwrap();
            let _ = eng.min(h_salary, &ids);
            let _ = eng.max(h_salary, &ids);
            ids.len()
        },
        || {
            let _: (i64, i64) = sdb.query_row(
                "SELECT MIN(salary), MAX(salary) FROM employees WHERE dept = 5", [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            ).unwrap();
            ENTITY_COUNT as usize / DEPT_COUNT as usize
        },
        || {
            let _: (i64, i64) = ddb.query_row(
                "SELECT MIN(salary), MAX(salary) FROM employees WHERE dept = 5", [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            ).unwrap();
            ENTITY_COUNT as usize / DEPT_COUNT as usize
        },
        || {
            let ids = lmdb_eq_ids(&rtxn, &idx_dept, 5);
            let mut mn = u32::MAX;
            let mut mx = 0u32;
            for &id in &ids {
                let s = lmdb_col(&rtxn, &primary, id, OFF_SALARY);
                if s < mn { mn = s; }
                if s > mx { mx = s; }
            }
            std::hint::black_box((mn, mx));
            ids.len()
        },
    );

    // cleanup
    drop(rtxn);
    drop(env);
    let _ = std::fs::remove_file(enchu_path);
    let _ = std::fs::remove_file(sqlite_path);
    let _ = std::fs::remove_file(duck_path);
    let _ = std::fs::remove_file(format!("{}.oplog", enchu_path));
    let _ = std::fs::remove_file(format!("{}.tables", enchu_path));
    let _ = std::fs::remove_file(format!("{}.crc", enchu_path));
    let _ = std::fs::remove_file(format!("{}.db.lock", enchu_path));
    let _ = std::fs::remove_dir_all(lmdb_dir);
}

// ── LMDB helpers (composite-key secondary index over heed) ──

#[inline]
fn k8(val: u32, id: u32) -> [u8; 8] {
    let mut b = [0u8; 8];
    b[..4].copy_from_slice(&val.to_be_bytes());
    b[4..].copy_from_slice(&id.to_be_bytes());
    b
}

/// equality 検索: index db を value prefix で scan して entity id を materialize。
#[inline]
fn lmdb_eq_ids(rtxn: &heed::RoTxn, db: &Lk, v: u32) -> Vec<u32> {
    let prefix = v.to_be_bytes();
    let mut out = Vec::new();
    for kv in db.prefix_iter(rtxn, &prefix).unwrap() {
        let (k, _) = kv.unwrap();
        out.push(u32::from_be_bytes(k[4..8].try_into().unwrap()));
    }
    out
}

/// equality count: materialize せず prefix scan の長さだけ取る (COUNT(*) 相当)。
#[inline]
fn lmdb_eq_count(rtxn: &heed::RoTxn, db: &Lk, v: u32) -> usize {
    db.prefix_iter(rtxn, &v.to_be_bytes()).unwrap().count()
}

/// primary から 1 entity の指定 column (BE u32) を直読み。
#[inline]
fn lmdb_col(rtxn: &heed::RoTxn, primary: &Lk, id: u32, off: usize) -> u32 {
    let row = primary.get(rtxn, &id.to_be_bytes()).unwrap().unwrap();
    u32::from_be_bytes(row[off..off + 4].try_into().unwrap())
}

fn bench<F1, F2, F3, F4>(
    label: &str,
    iterations: u32,
    enchu_fn: F1,
    sqlite_fn: F2,
    duck_fn: F3,
    lmdb_fn: F4,
) where
    F1: Fn() -> usize,
    F2: Fn() -> usize,
    F3: Fn() -> usize,
    F4: Fn() -> usize,
{
    // warmup
    let _ = enchu_fn();
    let _ = sqlite_fn();
    let _ = duck_fn();
    let _ = lmdb_fn();
    for _ in 0..2 {
        let _ = enchu_fn();
        let _ = sqlite_fn();
        let _ = duck_fn();
        let _ = lmdb_fn();
    }

    // capture hit count from first iteration (after warmup)
    let enchu_hits = enchu_fn();
    let sqlite_hits = sqlite_fn();
    let duck_hits = duck_fn();
    let lmdb_hits = lmdb_fn();

    let t = Instant::now();
    for _ in 0..iterations { let _ = enchu_fn(); }
    let enchu_ns = t.elapsed().as_nanos() / iterations as u128;

    let t = Instant::now();
    for _ in 0..iterations { let _ = sqlite_fn(); }
    let sqlite_ns = t.elapsed().as_nanos() / iterations as u128;

    let t = Instant::now();
    for _ in 0..iterations { let _ = duck_fn(); }
    let duck_ns = t.elapsed().as_nanos() / iterations as u128;

    let t = Instant::now();
    for _ in 0..iterations { let _ = lmdb_fn(); }
    let lmdb_ns = t.elapsed().as_nanos() / iterations as u128;

    let all_eq = enchu_hits == sqlite_hits && sqlite_hits == duck_hits && duck_hits == lmdb_hits;
    let hits_str = if all_eq {
        format!("{} hits", format_hits(enchu_hits))
    } else {
        format!(
            "enchu={} sqlite={} duck={} lmdb={}",
            format_hits(enchu_hits),
            format_hits(sqlite_hits),
            format_hits(duck_hits),
            format_hits(lmdb_hits),
        )
    };

    let winner = winner_of(&[
        ("enchudb", enchu_ns),
        ("sqlite ", sqlite_ns),
        ("duckdb ", duck_ns),
        ("lmdb   ", lmdb_ns),
    ]);

    println!("{label}  [{hits_str}]");
    println!("  enchudb (schema): {:>10}", format_ns(enchu_ns));
    println!("  sqlite:           {:>10}", format_ns(sqlite_ns));
    println!("  duckdb:           {:>10}", format_ns(duck_ns));
    println!("  lmdb:             {:>10}", format_ns(lmdb_ns));
    println!("  → winner: {winner}");
    println!();
}

fn winner_of(entries: &[(&str, u128)]) -> String {
    let min = entries.iter().map(|(_, ns)| *ns).min().unwrap();
    let (name, _) = entries.iter().find(|(_, ns)| *ns == min).unwrap();
    // 2 番目との比 (= "Xx faster than next")
    let mut sorted: Vec<u128> = entries.iter().map(|(_, ns)| *ns).collect();
    sorted.sort();
    let ratio = if sorted[0] > 0 {
        sorted[1] as f64 / sorted[0] as f64
    } else {
        f64::INFINITY
    };
    format!("{name} ({:.1}x faster than next)", ratio)
}

fn format_hits(n: usize) -> String {
    if n >= 1_000_000 { format!("{:.1}M", n as f64 / 1_000_000.0) }
    else if n >= 1_000 { format!("{}K", n / 1_000) }
    else { format!("{n}") }
}

fn format_ns(ns: u128) -> String {
    if ns >= 1_000_000 { format!("{:.2}ms", ns as f64 / 1_000_000.0) }
    else if ns >= 1_000 { format!("{:.1}µs", ns as f64 / 1_000.0) }
    else { format!("{ns}") }
}
