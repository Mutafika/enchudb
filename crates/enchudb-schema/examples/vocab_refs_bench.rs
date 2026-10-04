//! #385: 回収する DB を開く時間。 表 16 個 (Tag 2 列)、 行を入れる表と entity cap を変えて、
//! (a) きれいに閉じた後 (参照数の file をそのまま使う) と (b) file を消した後 (生きている行を数え直す) を測る。
//!
//! `cargo run --release -p enchudb-schema --example vocab_refs_bench`

use enchudb_schema::Database;
use std::time::Instant;

const TABLES: usize = 16;
const ROWS: usize = 1_000;

fn run(cap: u32, only: Option<usize>) {
    let dir = std::env::temp_dir().join(format!("refscan_{cap}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("enchu.db");
    let p = path.to_str().unwrap();
    {
        let mut b = Database::create_growable_with_capacity(p, cap).unwrap();
        for t in 0..TABLES {
            b.table(&format!("t{t}")).tag("id").tag("kind").primary_key("id").build().unwrap();
        }
        let db = b.finish_with_oplog(16 << 20).unwrap();
        for t in (0..TABLES).filter(|t| only.is_none_or(|o| o == *t)) {
            let tb = db.get_table(&format!("t{t}")).unwrap();
            for i in 0..ROWS {
                tb.insert().set("id", format!("t{t}_{i}")).set("kind", "k").commit().unwrap();
            }
        }
        db.engine().enable_vocab_reclaim().unwrap();
    }
    // 1 回開いて閉じる (回収を有効にした最初の open = 数えて file を作る)
    let t0 = Instant::now();
    drop(Database::open_with_oplog(p, 16 << 20).unwrap());
    let first = t0.elapsed();
    let t0 = Instant::now();
    let db = Database::open_with_oplog(p, 16 << 20).unwrap();
    let clean = t0.elapsed();
    assert!(db.engine().vocab_usage().reclaim_ready);
    drop(db);
    std::fs::remove_file(path.join("vocab.refs.seg")).unwrap();
    let t0 = Instant::now();
    let db = Database::open_with_oplog(p, 16 << 20).unwrap();
    let recount = t0.elapsed();
    drop(db);
    println!(
        "cap {cap:>11}  only {only:<9}  first open {:>7.1} ms  clean open {:>7.1} ms  recount open {:>7.1} ms",
        first.as_secs_f64() * 1e3,
        clean.as_secs_f64() * 1e3,
        recount.as_secs_f64() * 1e3,
        only = format!("{only:?}"),
    );
    let _ = std::fs::remove_dir_all(&dir);
}

fn main() {
    for cap in [1_000_000, 200_000_000] {
        for only in [Some(0), Some(TABLES - 1), None] {
            run(cap, only);
        }
    }
}
