//! 0.8.10 (#43): Schema `Query` 終端の集計 chain API 検証。
//!
//! 各 method:
//!   - `Query::count_col(col)` — sub-set 内で col が tie された数
//!   - `Query::sum(col)` / `min` / `max`
//!   - `Query::group_sum(g, v)` / `group_min` / `group_max`
//!   - `Query::histogram(col, vmin, vmax, n)`
//!
//! 検証ポイント:
//!   - WHERE で絞った sub-set の集計が正しい
//!   - 既存 `Table::sum / min / ...` (= table 全体) と一致する条件 (= `.all()`) で照合
//!   - 空 sub-set / 全件 sub-set / 1 件 sub-set の動作

use enchudb_schema::Database;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-query-agg-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suffix in ["", ".oplog", ".crc", ".db.lock", ".tables", ".tables.tmp", ".schema"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

#[test]
fn query_scalar_aggregations() {
    let path = tmp_path("scalar");
    cleanup(&path);

    let mut db = Database::create(&path).unwrap();
    db.table("emp")
        .number("salary")
        .tag("dept")
        .build()
        .unwrap();

    let emp = db.get_table("emp").unwrap();
    let rows = [
        (50_000u32, "eng"), (30_000, "eng"), (20_000, "eng"),
        (90_000, "sales"), (70_000, "sales"),
        (10_000, "ops"),
    ];
    for (s, d) in &rows {
        emp.insert().set("salary", *s as i64).set("dept", *d).commit().unwrap();
    }

    // eng でフィルタした sub-set の集計
    let q = emp.where_eq("dept", "eng");
    assert_eq!(q.sum("salary").unwrap(), 50_000 + 30_000 + 20_000);

    let q = emp.where_eq("dept", "eng");
    assert_eq!(q.min("salary").unwrap(), Some(20_000));

    let q = emp.where_eq("dept", "eng");
    assert_eq!(q.max("salary").unwrap(), Some(50_000));

    let q = emp.where_eq("dept", "eng");
    assert_eq!(q.count_col("salary").unwrap(), 3);

    // sales sub-set
    let q = emp.where_eq("dept", "sales");
    assert_eq!(q.sum("salary").unwrap(), 90_000 + 70_000);
    let q = emp.where_eq("dept", "sales");
    assert_eq!(q.min("salary").unwrap(), Some(70_000));

    // .all() = table 全体、 Table::sum と一致
    assert_eq!(emp.all().sum("salary").unwrap(), emp.sum("salary"));
    assert_eq!(emp.all().min("salary").unwrap(), emp.min("salary"));
    assert_eq!(emp.all().max("salary").unwrap(), emp.max("salary"));

    drop(emp);
    drop(db);
    cleanup(&path);
}

#[test]
fn query_group_aggregations() {
    let path = tmp_path("group");
    cleanup(&path);

    let mut db = Database::create(&path).unwrap();
    db.table("emp")
        .number("salary")
        .tag("dept")
        .tag("region")
        .build()
        .unwrap();

    let emp = db.get_table("emp").unwrap();
    let rows = [
        (50_000u32, "eng", "us"), (30_000, "eng", "us"), (20_000, "eng", "jp"),
        (90_000, "sales", "us"), (70_000, "sales", "jp"),
        (10_000, "ops", "us"),
    ];
    for (s, d, r) in &rows {
        emp.insert()
            .set("salary", *s as i64)
            .set("dept", *d)
            .set("region", *r)
            .commit()
            .unwrap();
    }

    // us only でフィルタ → dept × sum(salary)
    let q = emp.where_eq("region", "us");
    let gs = q.group_sum("dept", "salary").unwrap();
    let mut totals: Vec<u64> = gs.iter().map(|(_, v)| *v).collect();
    totals.sort();
    // ops: 10000, eng: 50000+30000=80000, sales: 90000
    assert_eq!(totals, vec![10_000, 80_000, 90_000]);

    // us only で group_min
    let q = emp.where_eq("region", "us");
    let gm = q.group_min("dept", "salary").unwrap();
    let mut mins: Vec<u32> = gm.iter().map(|(_, v)| *v).collect();
    mins.sort();
    assert_eq!(mins, vec![10_000, 30_000, 90_000]);

    // .all() = table 全体、 Table::group_sum と一致 (sort 後比較)
    let mut q_all = emp.all().group_sum("dept", "salary").unwrap();
    let mut t_all = emp.group_sum("dept", "salary");
    q_all.sort_by_key(|x| x.0);
    t_all.sort_by_key(|x| x.0);
    assert_eq!(q_all, t_all);

    drop(emp);
    drop(db);
    cleanup(&path);
}

#[test]
fn query_histogram() {
    let path = tmp_path("hist");
    cleanup(&path);

    let mut db = Database::create(&path).unwrap();
    db.table("m").number("score").tag("region").build().unwrap();

    let m = db.get_table("m").unwrap();
    // us: 10, 20, 30, 40, 50, jp: 60, 70, 80, 90
    let rows = [
        (10u32, "us"), (20, "us"), (30, "us"), (40, "us"), (50, "us"),
        (60, "jp"), (70, "jp"), (80, "jp"), (90, "jp"),
    ];
    for (s, r) in &rows {
        m.insert().set("score", *s as i64).set("region", *r).commit().unwrap();
    }

    // us only で histogram [0, 99] / 10 bucket
    let q = m.where_eq("region", "us");
    let hist = q.histogram("score", 0, 99, 10).unwrap();
    assert_eq!(hist.len(), 10);
    // 値 10,20,30,40,50 → bucket 1,2,3,4,5 各 1
    assert_eq!(hist[0], 0);
    assert_eq!(hist[1], 1);
    assert_eq!(hist[2], 1);
    assert_eq!(hist[5], 1);
    assert_eq!(hist[6], 0);
    let total: u32 = hist.iter().sum();
    assert_eq!(total, 5);

    // .all() = table 全体
    let hist_all = m.all().histogram("score", 0, 99, 10).unwrap();
    let hist_table = m.histogram("score", 0, 99, 10);
    assert_eq!(hist_all, hist_table);

    drop(m);
    drop(db);
    cleanup(&path);
}

#[test]
fn query_empty_sub_set() {
    // どの row もマッチしない WHERE → 空 sub-set の集計
    let path = tmp_path("empty");
    cleanup(&path);

    let mut db = Database::create(&path).unwrap();
    db.table("emp").number("salary").tag("dept").build().unwrap();

    let emp = db.get_table("emp").unwrap();
    emp.insert().set("salary", 50_000i64).set("dept", "eng").commit().unwrap();

    // 存在しない dept でフィルタ
    let q = emp.where_eq("dept", "marketing");
    assert_eq!(q.sum("salary").unwrap(), 0);
    let q = emp.where_eq("dept", "marketing");
    assert_eq!(q.count_col("salary").unwrap(), 0);
    let q = emp.where_eq("dept", "marketing");
    assert_eq!(q.min("salary").unwrap(), None);
    let q = emp.where_eq("dept", "marketing");
    assert_eq!(q.max("salary").unwrap(), None);

    let q = emp.where_eq("dept", "marketing");
    let hist = q.histogram("salary", 0, 100_000, 5).unwrap();
    assert_eq!(hist, vec![0, 0, 0, 0, 0]);

    drop(emp);
    drop(db);
    cleanup(&path);
}

#[test]
fn query_unknown_col_returns_err() {
    let path = tmp_path("unknown");
    cleanup(&path);

    let mut db = Database::create(&path).unwrap();
    db.table("emp").number("salary").build().unwrap();

    let emp = db.get_table("emp").unwrap();
    emp.insert().set("salary", 50_000i64).commit().unwrap();

    let q = emp.all();
    assert!(q.sum("nope").is_err(), "unknown col should error");

    let q = emp.all();
    assert!(q.min("nope").is_err());

    drop(emp);
    drop(db);
    cleanup(&path);
}

#[test]
fn query_with_range_filter() {
    // where_range と組み合わせ
    let path = tmp_path("range_filt");
    cleanup(&path);

    let mut db = Database::create(&path).unwrap();
    db.table("emp").number("salary").number("age").build().unwrap();

    let emp = db.get_table("emp").unwrap();
    for (s, a) in [(30_000u32, 25u32), (50_000, 35), (70_000, 45), (90_000, 55)] {
        emp.insert().set("salary", s as i64).set("age", a as i64).commit().unwrap();
    }

    // 30 <= age <= 50 でフィルタ → salary 50000 + 70000
    let q = emp.where_range("age", 30, 50);
    assert_eq!(q.sum("salary").unwrap(), 50_000 + 70_000);

    let q = emp.where_range("age", 30, 50);
    assert_eq!(q.min("salary").unwrap(), Some(50_000));

    let q = emp.where_range("age", 30, 50);
    assert_eq!(q.max("salary").unwrap(), Some(70_000));

    drop(emp);
    drop(db);
    cleanup(&path);
}
