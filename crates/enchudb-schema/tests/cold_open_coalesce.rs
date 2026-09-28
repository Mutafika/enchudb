//! 0.8.2 fix: `Database::create → build×N → finish_with_oplog` の cold-open
//! perf が table 数に linear で重い問題 (issue #19) の回帰防止 test。
//!
//! 旧 behavior: `TableBuilder::build()` 末尾で `persist_schema()` →
//! `eng.flush()` (= body msync ≒ 47ms on macOS APFS) を毎回呼ぶ → N table
//! 宣言で N × 47ms ≈ 700ms (N=15) の linear scaling。
//!
//! 新 behavior: build 中の persist_schema を `finish_*` に coalesce、
//! N table の cold-open でも fsync は 1 回。 N=15 で declare phase は
//! 200ms 以下になることを assert する。
//!
//! macOS APFS で実測: 修正前 ~687ms、 修正後 ~10ms。

use enchudb_schema::Database;
use std::time::Instant;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-cold-open-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
    let _ = std::fs::remove_file(format!("{}.tables", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.db.lock", path));
}

fn declare_n(db: &mut Database, n: usize) {
    for ti in 0..n {
        db.table(&format!("t{ti}"))
            .tag("id")
            .tag("name")
            .tag("kind")
            .tag("status")
            .number("created_at")
            .number("updated_at")
            .leaf("payload")
            .primary_key("id")
            .build()
            .expect("build");
    }
}

#[test]
fn declare_phase_does_not_scale_linearly_with_table_count() {
    let n = 15;
    let path = tmp_path("declare15");
    cleanup(&path);

    let mut db = Database::create_growable_with_capacity(&path, 65_536).unwrap();
    let t = Instant::now();
    declare_n(&mut db, n);
    let declare_ms = t.elapsed().as_secs_f64() * 1000.0;
    // #273: 壁時計の閾値 (旧: declare < 200 ms) は混んだ machine で偽 fail した。 coalesce そのものを見る:
    // build の途中では `.schema` を書かず (table ごとの persist + fsync の線形をやめた、 修正前 ~687 ms /
    // 修正後 ~10 ms)、 finish で 1 度だけ書く。 build の途中の `.schema` に最後の table はまだ無い
    let sidecar = enchudb_engine::db_files::path_for(&path, enchudb_engine::db_files::SCHEMA);
    let mid = std::fs::read_to_string(&sidecar).unwrap_or_default();
    assert!(
        !mid.contains(&format!("t{}", n - 1)),
        "build の途中で .schema を書いている (= persist_schema が build で coalesce されていない)"
    );

    let t = Instant::now();
    let _arc = db.finish_with_oplog(64 * 1024 * 1024).unwrap();
    let finish_ms = t.elapsed().as_secs_f64() * 1000.0;

    eprintln!(
        "n={} declare={:.1}ms ({:.1}ms/tbl) finish={:.1}ms",
        n,
        declare_ms,
        declare_ms / n as f64,
        finish_ms
    );

    let last = format!("t{}", n - 1);
    let after_finish = std::fs::read_to_string(&sidecar).unwrap_or_default();
    assert!(after_finish.contains(&last), "finish の後の .schema に {last} が無い");
    cleanup(&path);
}

#[test]
fn schema_persists_via_finish_with_oplog() {
    let path = tmp_path("finish_persists");
    cleanup(&path);

    {
        let mut db = Database::create_growable_with_capacity(&path, 65_536).unwrap();
        db.table("notes").tag("id").number("val").primary_key("id").build().unwrap();
        db.table("tags").tag("id").tag("label").primary_key("id").build().unwrap();
        let _arc = db.finish_with_oplog(64 * 1024 * 1024).unwrap();
        // _arc drop → consumer shutdown → schema 既に finish で persist 済み
    }

    let db2 = Database::open(&path).unwrap();
    let names: Vec<String> = db2.list_tables().into_iter().map(|t| t.name).collect();
    assert!(names.iter().any(|n| n == "notes"), "tables={:?}", names);
    assert!(names.iter().any(|n| n == "tags"), "tables={:?}", names);

    drop(db2);
    cleanup(&path);
}

#[test]
fn schema_persists_via_drop_safety_net() {
    // finish_* を呼ばずに drop された path でも schema が disk に残る (= Drop で persist)
    let path = tmp_path("drop_safety");
    cleanup(&path);

    {
        let mut db = Database::create_growable_with_capacity(&path, 65_536).unwrap();
        db.table("notes").tag("id").number("val").primary_key("id").build().unwrap();
        db.table("tags").tag("id").tag("label").primary_key("id").build().unwrap();
        // finish_* を呼ばずに drop
    }

    let db2 = Database::open(&path).unwrap();
    let names: Vec<String> = db2.list_tables().into_iter().map(|t| t.name).collect();
    assert!(names.iter().any(|n| n == "notes"), "tables={:?}", names);
    assert!(names.iter().any(|n| n == "tags"), "tables={:?}", names);

    drop(db2);
    cleanup(&path);
}
