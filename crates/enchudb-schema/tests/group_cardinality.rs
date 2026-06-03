//! #46: `TableBuilder::cardinality()` — group key に cardinality hint を渡すと
//! engine の `group_dense_cap` が効き、 group 集計が dense (+ 並列) fast path に
//! 乗る。 ここでは **正しさ** を検証する: hint を付けても結果が正しいこと、
//! hint 有無で結果が一致すること (dense path == HashMap fallback)、 reopen 後も
//! 正しいこと、 列宣言前に呼んでも no-op であること。 速度の検証は
//! `examples/vs_db.rs` / `examples/par_scan_bench.rs` で行う。

use enchudb_schema::Database;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-schema-card-{}-{}-{}",
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

// (dept, salary) rows: dept は 0..=3 の 4 group、 各 100 行。 salary に 0 を含めて
// missing sentinel (= stored 0) と値 0 の区別も間接的に踏む。
fn rows() -> Vec<(i64, i64)> {
    (0..400i64).map(|i| (i % 4, (i * 7 + 13) % 100)).collect()
}

fn expected_group_sum() -> Vec<(u32, u64)> {
    let mut acc = [0u64; 4];
    for (d, s) in rows() {
        acc[d as usize] += s as u64;
    }
    (0u32..4).map(|g| (g, acc[g as usize])).collect()
}

fn sorted(mut g: Vec<(u32, u64)>) -> Vec<(u32, u64)> {
    g.sort();
    g
}

#[test]
fn group_sum_correct_with_cardinality_hint() {
    let path = tmp_path("hint");
    cleanup(&path);

    let mut db = Database::create(&path).unwrap();
    db.table("emp")
        .number("dept").cardinality(4) // #46: dense path を有効化
        .number("salary")
        .build()
        .unwrap();
    let emp = db.get_table("emp").unwrap();
    for (d, s) in rows() {
        emp.insert().set("dept", d).set("salary", s).commit().unwrap();
    }

    assert_eq!(sorted(emp.group_sum("dept", "salary")), expected_group_sum());
    // group 合計 == table 全体の sum (encoding 非依存の cross check)
    let total: u64 = emp.group_sum("dept", "salary").iter().map(|(_, s)| *s).sum();
    assert_eq!(total, emp.sum("salary"));

    drop(emp);
    drop(db);
    cleanup(&path);
}

#[test]
fn group_sum_identical_with_and_without_hint() {
    // dense path (cap あり) と HashMap fallback (cap なし) が同一結果を返すこと。
    let run = |path: &str, with_hint: bool| -> Vec<(u32, u64)> {
        cleanup(path);
        let mut db = Database::create(path).unwrap();
        if with_hint {
            db.table("emp").number("dept").cardinality(4).number("salary").build().unwrap();
        } else {
            db.table("emp").number("dept").number("salary").build().unwrap();
        }
        let emp = db.get_table("emp").unwrap();
        for (d, s) in rows() {
            emp.insert().set("dept", d).set("salary", s).commit().unwrap();
        }
        let g = sorted(emp.group_sum("dept", "salary"));
        drop(emp);
        drop(db);
        cleanup(path);
        g
    };

    let with = run(&tmp_path("with"), true);
    let without = run(&tmp_path("without"), false);
    assert_eq!(with, without, "dense path と HashMap fallback は同一結果を返すべき");
    assert_eq!(with, expected_group_sum());
}

#[test]
fn cardinality_hint_survives_reopen() {
    // build phase で cap を渡し、 reopen 後も group_sum が正しいこと
    // (= max_values が persist + restore され、 reopen の define_himo_in(.., 0) が
    //  ensure_himo の early-return で既存 himo の cap を reset しない)。
    let path = tmp_path("reopen");
    cleanup(&path);

    {
        let mut db = Database::create(&path).unwrap();
        db.table("emp")
            .number("dept").cardinality(4)
            .number("salary")
            .build()
            .unwrap();
        let emp = db.get_table("emp").unwrap();
        for (d, s) in rows() {
            emp.insert().set("dept", d).set("salary", s).commit().unwrap();
        }
        // scope を抜けて Drop で flush
    }

    let db = Database::open(&path).unwrap();
    let emp = db.get_table("emp").unwrap();
    assert_eq!(sorted(emp.group_sum("dept", "salary")), expected_group_sum());

    drop(emp);
    drop(db);
    cleanup(&path);
}

#[test]
fn cardinality_before_any_column_is_noop() {
    // 列が一つも宣言されていない状態で cardinality を呼んでも panic せず no-op。
    let path = tmp_path("noop");
    cleanup(&path);

    let mut db = Database::create(&path).unwrap();
    db.table("t")
        .cardinality(10) // 列未宣言 → no-op
        .number("v")
        .build()
        .unwrap();
    let t = db.get_table("t").unwrap();
    t.insert().set("v", 5i64).commit().unwrap();
    assert_eq!(t.sum("v"), 5);

    drop(t);
    drop(db);
    cleanup(&path);
}
