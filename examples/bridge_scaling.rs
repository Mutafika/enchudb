//! oplog → `_sync_ops` bridge (`transfer_oplog_to_sync_ops`) の scaling 計測。
//!
//! sunsu2 fanout probe で「48k op の bridge が数分」を観測した最小化。
//! N を倍々にして transfer の wall time を測る — 線形なら倍々、超線形なら発散。
//!
//! 使い方: `cargo run --release --example bridge_scaling [N1,N2,...]`

use enchudb::{Engine, ValueType};
use std::sync::Arc;
use std::time::Instant;

fn main() {
    let ns: Vec<u32> = std::env::args()
        .nth(1)
        .map(|s| s.split(',').filter_map(|x| x.parse().ok()).collect())
        .unwrap_or_else(|| vec![5_000, 10_000, 20_000, 40_000]);

    // ── stage 2: schema 層経由 (sunsu2 の posts と同型: tag PK + leaf + number×3、
    //    1 row = 1 commit) で同じ scaling を測る ──
    if std::env::args().any(|a| a == "--schema") {
        for n in ns {
            let path = format!("/tmp/enchudb-bridge-schema-{}-{}", std::process::id(), n);
            cleanup(&path);
            let mut db = enchudb::schema::Database::create_growable_with(
                &path,
                enchudb::GrowableOptions {
                    max_entities: 1_048_576,
                    max_himos: 64,
                    ..Default::default()
                },
            )
            .unwrap();
            db.table("posts")
                .tag("guid")
                .leaf("body")
                .number("created_s")
                .number("visibility")
                .number("author")
                .primary_key("guid")
                .with_capacity(200_000)
                .build()
                .unwrap();
            db.enable_sync().unwrap();
            let db = db.finish_with_oplog_with_queue(256 * 1024 * 1024, 4096).unwrap();
            db.arc_engine().set_peer_id(1);

            let posts = db.get_table("posts").unwrap();
            let t0 = Instant::now();
            for i in 0..n {
                posts
                    .insert()
                    .set("guid", format!("g{i:08}").as_str())
                    .set("body", format!("celebrity update number {i} — hello fans").as_str())
                    .set("created_s", 1_700_000_000i64 + i as i64)
                    .set("visibility", 0i64)
                    .set("author", 1i64)
                    .commit()
                    .unwrap();
                if i % 500 == 499 {
                    eprintln!("  .. {} rows in {:?} (lsn={})", i + 1, t0.elapsed(), db.engine().current_sync_lsn());
                }
            }
            let write_time = t0.elapsed();
            db.engine().oplog_sync().unwrap();
            let t1 = Instant::now();
            let mut lsn = db.engine().current_sync_lsn();
            let mut stable = 0;
            loop {
                std::thread::sleep(std::time::Duration::from_millis(50));
                let cur = db.engine().current_sync_lsn();
                if cur == lsn {
                    stable += 1;
                    if stable >= 3 {
                        break;
                    }
                } else {
                    stable = 0;
                    lsn = cur;
                }
            }
            eprintln!(
                "SCHEMA N={n:>6}: write {write_time:>10.3?}  bridge settle {:?} (lsn={lsn}, {:.1} ops/row)",
                t1.elapsed(),
                lsn as f64 / n as f64
            );
            drop(db);
            cleanup(&path);
        }
        return;
    }

    // toggles: --growable = growable layout、--q4096 = queue 4096 slot、
    // --percommit = 1 row ごとに oplog_commit、--tag = Tag(Vocab) 列も張る、
    // --leaf = Leaf 列も張る
    let growable = std::env::args().any(|a| a == "--growable");
    let q4096 = std::env::args().any(|a| a == "--q4096");
    let percommit = std::env::args().any(|a| a == "--percommit");
    let tag = std::env::args().any(|a| a == "--tag");
    let leaf = std::env::args().any(|a| a == "--leaf");

    for n in ns {
        let path = format!(
            "/tmp/enchudb-bridge-scaling-{}-{}",
            std::process::id(),
            n
        );
        cleanup(&path);
        let mut eng = if growable {
            Engine::create_growable_with_capacity(&path, 1_048_576).unwrap()
        } else {
            Engine::create_with_capacity(&path, 1_048_576).unwrap()
        };
        eng.define_table("t", 200_000).unwrap();
        eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
        if tag {
            eng.define_himo_in("t", "g", ValueType::Tag, 0).unwrap();
        }
        if leaf {
            eng.define_himo_in("t", "b", ValueType::Leaf, 0).unwrap();
        }
        eng.enable_sync_tables().unwrap();
        let eng: Arc<Engine> = if q4096 {
            Engine::concurrentize_with_oplog_queue(eng, 256 * 1024 * 1024, 4096).unwrap()
        } else {
            Engine::concurrentize_with_oplog(eng, 256 * 1024 * 1024).unwrap()
        };
        eng.set_peer_id(1);

        let t0 = Instant::now();
        for i in 0..n {
            let e = eng.entity_in("t").unwrap();
            eng.tie_to(e, "t.v", i % 1000);
            if tag {
                eng.tie_text_to(e, "t.g", &format!("g{i:08}"));
            }
            if leaf {
                eng.tie_text_to(e, "t.b", &format!("body text number {i} — hello"));
            }
            if percommit {
                eng.oplog_commit();
            }
        }
        eng.oplog_commit();
        eng.flush_writes();
        eng.oplog_sync().unwrap();
        let write_time = t0.elapsed();

        let t1 = Instant::now();
        let moved = eng.transfer_oplog_to_sync_ops();
        // lsn が 3 回連続で安定するまで待つ = bridge 完了。
        let mut lsn = eng.current_sync_lsn();
        let mut stable = 0;
        loop {
            std::thread::sleep(std::time::Duration::from_millis(50));
            let cur = eng.current_sync_lsn();
            if cur == lsn && cur >= n {
                stable += 1;
                if stable >= 3 {
                    break;
                }
            } else {
                stable = 0;
                lsn = cur;
            }
        }
        let bridge_time = t1.elapsed();
        eprintln!(
            "N={n:>6}: write {write_time:>10.3?}  bridge settle {bridge_time:>10.3?} (manual moved={moved}, lsn={lsn}, {:.1} rec/row)",
            lsn as f64 / n as f64
        );

        drop(eng);
        cleanup(&path);
    }
}

fn cleanup(path: &str) {
    let _ = enchudb::db_files::remove_db(&path);
}
