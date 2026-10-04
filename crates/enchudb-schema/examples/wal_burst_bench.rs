//! #388: 1 thread で 1 行入れては古い行を消し続けた時の WAL (64 MiB) の様子。 issue の再現コード。
//!
//! `cargo run --release -p enchudb-schema --example wal_burst_bench`

use enchudb_schema::Database;
use std::collections::VecDeque;
use std::time::Instant;

fn main() {
    let dir = std::env::temp_dir().join(format!("walburst_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("enchu.db");
    let mut b = Database::create_growable_with_capacity(path.to_str().unwrap(), 1_000_000).unwrap();
    b.table("events").tag("id").tag("owner").leaf("title").number("created_at").primary_key("id").build().unwrap();
    let db = b.finish_with_oplog(64 << 20).unwrap();
    let t = db.get_table("events").unwrap();
    let e = db.engine();
    let mut live = VecDeque::new();
    let started = Instant::now();
    for i in 0..2_000_000u64 {
        let eid = t
            .insert()
            .set("id", format!("ev_{i:016x}"))
            .set("owner", format!("u{}", i % 1000))
            .set("title", "something happened in a project")
            .set("created_at", i as i64)
            .commit()
            .unwrap();
        live.push_back(eid);
        while live.len() > 100 {
            t.entity(live.pop_front().unwrap()).delete().unwrap();
        }
        if (i + 1) % 500_000 == 0 {
            println!(
                "{:>9} rows  {:>6.1} s  wal_free {:>9} B  dropped_records {:>2}  commit_failures {:>3}",
                i + 1,
                started.elapsed().as_secs_f64(),
                e.wal_free_bytes(),
                e.wal_dropped_records(),
                e.wal_commit_failures(),
            );
        }
    }
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}
