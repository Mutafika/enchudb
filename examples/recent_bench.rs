//! #21: 「直近 N 件」 を取る経路の比較 (table の大きさを変えて)。
//!
//! - `entities_with_himo(..).rev().take(n)` — #21 の時点の経路 (列を先頭から全部なめる)
//! - `all().order_by_desc("ts").limit(n).find()` — 全 row を集めて並べてから切る
//! - `Table::recent(n)` — table の末尾から逆にたどって n 件で止まる (#21)
//! - SQLite `SELECT id FROM posts ORDER BY ts DESC LIMIT n` (ts に索引、 prepared)
//! - SQLite `SELECT id FROM posts ORDER BY id DESC LIMIT n` (rowid の逆順 = insert の逆順、 `Table::recent` と同じ意味)
//!
//! 実測 (M2 Max、 load average 60〜80 の中で 3 回、 直近 80 件、 中央値の幅):
//!
//! | rows | entities_with_himo | order_by_desc.limit | Table::recent | sqlite (ts) | sqlite (rowid) |
//! |---|---|---|---|---|---|
//! | 50,000 | 94〜105 µs | 584〜647 µs | 0.54〜0.58 µs | 7.7〜7.8 µs | 7.0〜7.1 µs |
//! | 1,000,000 | 1,990〜2,661 µs | 73,006〜76,317 µs | 0.54〜0.58 µs | 7.7〜7.8 µs | 7.0 µs |
//!
//! `Table::recent` と SQLite は table の大きさによらない。 前の 2 つは table の大きさに比例する。
//!
//! `cargo run --release --example recent_bench`

use enchudb::schema::Database;
use rusqlite::Connection;
use std::time::Instant;

const N: usize = 80;

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path); // v10: DB は directory
    for suf in ["", ".tables", ".oplog", ".crc", ".db.lock", ".eidmap", ".vocabmap", ".positions", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

/// `f` を `iters` 回回した時の、 1 回の時間の中央値 (µs)。
fn median_us<R>(iters: usize, mut f: impl FnMut() -> R) -> f64 {
    let mut times: Vec<f64> = (0..iters)
        .map(|_| {
            let t = Instant::now();
            std::hint::black_box(f());
            t.elapsed().as_secs_f64() * 1e6
        })
        .collect();
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    times[times.len() / 2]
}

fn run(rows: usize) {
    let path = format!("/tmp/enchudb-recent-bench-{}-{rows}", std::process::id());
    let spath = format!("{path}.sqlite");
    cleanup(&path);
    cleanup(&spath);

    let mut db = Database::create_growable_with_capacity(&path, (rows as u32 + 1024) * 2).unwrap();
    db.table("posts").number("id").number("ts").tag("author").primary_key("id").build().unwrap();
    let posts = db.get_table("posts").unwrap();
    for i in 0..rows as i64 {
        posts.insert().set("id", i).set("ts", 1_000_000 + i).set("author", format!("u{}", i % 1000)).commit().unwrap();
    }

    let sdb = Connection::open(&spath).unwrap();
    sdb.execute_batch(
        "PRAGMA journal_mode = WAL; PRAGMA synchronous = OFF;
         CREATE TABLE posts (id INTEGER PRIMARY KEY, ts INTEGER, author TEXT);
         CREATE INDEX posts_ts ON posts(ts);",
    )
    .unwrap();
    {
        let tx = sdb.unchecked_transaction().unwrap();
        let mut stmt = sdb.prepare("INSERT INTO posts VALUES (?1, ?2, ?3)").unwrap();
        for i in 0..rows as i64 {
            stmt.execute(rusqlite::params![i, 1_000_000 + i, format!("u{}", i % 1000)]).unwrap();
        }
        drop(stmt);
        tx.commit().unwrap();
    }

    let eng = db.engine();
    let id_hid = posts.himo_id("id").unwrap();

    // 4 つとも同じ row を返すこと (id で比べる)
    let ids = |eids: &[u64]| -> Vec<i64> { eids.iter().map(|&e| eng.get_by_id(e, id_hid).unwrap() as i64).collect() };
    let want: Vec<i64> = (0..rows as i64).rev().take(N).collect();
    let scan = || -> Vec<u64> { eng.entities_with_himo(id_hid).iter().rev().take(N).copied().collect() };
    let ordered = || posts.all().order_by_desc("ts").limit(N).find().unwrap();
    let recent = || posts.recent(N);
    let mut stmt = sdb.prepare_cached("SELECT id FROM posts ORDER BY ts DESC LIMIT 80").unwrap();
    let mut sqlite = || -> Vec<i64> { stmt.query_map([], |r| r.get::<_, i64>(0)).unwrap().map(|r| r.unwrap()).collect() };
    assert_eq!(ids(&scan()), want);
    assert_eq!(ids(&ordered()), want);
    assert_eq!(ids(&recent()), want);
    assert_eq!(sqlite(), want);
    let mut stmt_rowid = sdb.prepare_cached("SELECT id FROM posts ORDER BY id DESC LIMIT 80").unwrap();
    let mut sqlite_rowid =
        || -> Vec<i64> { stmt_rowid.query_map([], |r| r.get::<_, i64>(0)).unwrap().map(|r| r.unwrap()).collect() };
    assert_eq!(sqlite_rowid(), want);

    let slow_iters = if rows > 200_000 { 15 } else { 200 };
    let t_scan = median_us(slow_iters, scan);
    let t_ordered = median_us(slow_iters, ordered);
    let t_recent = median_us(20_000, recent);
    let t_sqlite = median_us(20_000, &mut sqlite);
    let t_sqlite_rowid = median_us(20_000, &mut sqlite_rowid);
    println!(
        "{rows:>9} rows | entities_with_himo.rev.take {t_scan:>10.1} µs | order_by_desc.limit {t_ordered:>10.1} µs | \
         Table::recent {t_recent:>7.2} µs | sqlite ts {t_sqlite:>7.2} µs | sqlite rowid {t_sqlite_rowid:>7.2} µs"
    );

    drop(stmt_rowid);
    drop(stmt);
    drop(sdb);
    drop(db);
    cleanup(&path);
    cleanup(&spath);
}

fn main() {
    println!("直近 {N} 件 (中央値)");
    for rows in [50_000, 1_000_000] {
        run(rows);
    }
}
