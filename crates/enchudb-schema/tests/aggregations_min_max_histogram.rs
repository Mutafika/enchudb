//! 0.8.8 (#38): Schema Table API の min / max / group_min / group_max / histogram
//! wrapper の動作確認。 engine の `_range` primitive に正しく bind されているか、
//! また Table の eid_range が auto-resolve されているかを検証。

use enchudb_schema::Database;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-schema-mmh-{}-{}-{}",
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
fn table_min_max_basic() {
    let path = tmp_path("min_max");
    cleanup(&path);

    let mut db = Database::create(&path).unwrap();
    db.table("emp")
        .number("salary")
        .tag("dept")
        .build()
        .unwrap();
    db.table("empty").number("v").build().unwrap();

    let emp = db.get_table("emp").unwrap();
    let salaries = [50_000u32, 30_000, 90_000, 20_000, 70_000];
    let depts = ["eng", "eng", "sales", "eng", "sales"];
    for (s, d) in salaries.iter().zip(depts.iter()) {
        emp.insert()
            .set("salary", *s as i64)
            .set("dept", *d)
            .commit()
            .unwrap();
    }

    assert_eq!(emp.min("salary"), Some(20_000));
    assert_eq!(emp.max("salary"), Some(90_000));

    // 空 table の column → None
    let empty = db.get_table("empty").unwrap();
    assert_eq!(empty.min("v"), None);
    assert_eq!(empty.max("v"), None);

    drop(emp);
    drop(empty);
    drop(db);
    cleanup(&path);
}

#[test]
fn table_group_min_max_basic() {
    let path = tmp_path("group_mm");
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
        emp.insert()
            .set("salary", *s as i64)
            .set("dept", *d)
            .commit()
            .unwrap();
    }

    let mins = emp.group_min("dept", "salary");
    let mut min_vals: Vec<u32> = mins.iter().map(|(_, v)| *v).collect();
    min_vals.sort();
    assert_eq!(min_vals, vec![10_000, 20_000, 70_000], "min per dept");

    let maxs = emp.group_max("dept", "salary");
    let mut max_vals: Vec<u32> = maxs.iter().map(|(_, v)| *v).collect();
    max_vals.sort();
    assert_eq!(max_vals, vec![10_000, 50_000, 90_000], "max per dept");

    drop(emp);
    drop(db);
    cleanup(&path);
}

#[test]
fn table_histogram_basic() {
    let path = tmp_path("hist");
    cleanup(&path);

    let mut db = Database::create(&path).unwrap();
    db.table("metrics").number("score").build().unwrap();

    let metrics = db.get_table("metrics").unwrap();
    // [0, 99] を 10 bucket に → 各 10 width
    for v in [5u32, 15, 25, 35, 45, 50, 50, 50, 55, 65, 75, 85, 95] {
        metrics.insert().set("score", v as i64).commit().unwrap();
    }

    let hist = metrics.histogram("score", 0, 99, 10);
    assert_eq!(hist.len(), 10);
    // bucket 5 には 55 + 50×3 = 4 件
    assert_eq!(hist[5], 4);
    let total: u32 = hist.iter().sum();
    assert_eq!(total, 13);

    drop(metrics);
    drop(db);
    cleanup(&path);
}

#[test]
fn table_histogram_edge_cases() {
    let path = tmp_path("hist_edge");
    cleanup(&path);

    let mut db = Database::create(&path).unwrap();
    db.table("t").number("v").build().unwrap();
    let t = db.get_table("t").unwrap();
    for v in [10u32, 50, 90] {
        t.insert().set("v", v as i64).commit().unwrap();
    }

    // n_buckets = 0
    assert!(t.histogram("v", 0, 100, 0).is_empty());

    // vmin > vmax
    assert!(t.histogram("v", 100, 0, 5).is_empty());

    // 値域外 drop
    let narrow = t.histogram("v", 40, 60, 1);
    assert_eq!(narrow, vec![1], "値 50 のみ範囲内");

    drop(t);
    drop(db);
    cleanup(&path);
}

#[test]
fn table_min_max_consistency_with_sum() {
    // sum/count とは別の eid_range scan path を使ってるが、 同じ table で
    // 同じ row 集合を見てることを cross check (= sum > 0 なら min も必ず Some)。
    let path = tmp_path("consistency");
    cleanup(&path);

    let mut db = Database::create(&path).unwrap();
    db.table("emp").number("salary").build().unwrap();
    let emp = db.get_table("emp").unwrap();
    for s in [50_000u32, 30_000, 90_000] {
        emp.insert().set("salary", s as i64).commit().unwrap();
    }

    assert_eq!(emp.sum("salary"), 50_000 + 30_000 + 90_000);
    assert_eq!(emp.count_col("salary"), 3);
    assert_eq!(emp.min("salary"), Some(30_000));
    assert_eq!(emp.max("salary"), Some(90_000));

    drop(emp);
    drop(db);
    cleanup(&path);
}
