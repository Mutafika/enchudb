//! β-heavy phase 3: AuditFilter.table_id でレコードを table 単位に絞れる。
//!
//! sync の partial subscribe (= table X の record だけ feed したい) で使う想定。

#![cfg(not(target_arch = "wasm32"))]

use enchudb_engine::{AuditFilter, Engine, HimoType};

fn tmp(tag: &str) -> String {
    let p = format!(
        "/tmp/enchudb-audit-tid-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    cleanup(&p);
    p
}

fn cleanup(p: &str) {
    for suf in ["", ".wal", ".crc", ".db.lock", ".tables", ".positions"] {
        let _ = std::fs::remove_file(format!("{}{}", p, suf));
    }
    for name in ["users", "posts"] {
        let _ = std::fs::remove_file(format!("{}.t.{}.col", p, name));
    }
}

#[test]
fn audit_filter_by_table_id() {
    let p = tmp("filter");

    // standalone で schema + entity 確保まで済ます (entity_in が &mut self 要)
    let mut eng = Engine::create_standalone(&p).unwrap();
    eng.define_table("users", 100).unwrap();
    eng.define_table("posts", 100).unwrap();
    eng.define_himo_in("users", "age", HimoType::Number, 100)
        .unwrap();
    eng.define_himo_in("posts", "score", HimoType::Number, 100)
        .unwrap();
    let users_tid = eng
        .list_tables()
        .iter()
        .find(|(_, n, _, _)| n == "users")
        .unwrap()
        .0;
    let posts_tid = eng
        .list_tables()
        .iter()
        .find(|(_, n, _, _)| n == "posts")
        .unwrap()
        .0;
    let u_eids: Vec<_> = (0..3).map(|_| eng.entity_in("users").unwrap()).collect();
    let p_eids: Vec<_> = (0..2).map(|_| eng.entity_in("posts").unwrap()).collect();
    eng.flush().unwrap();

    // concurrent_with_wal に昇格して tie_async で WAL に書く
    let eng = Engine::concurrentize_with_wal(eng, 1024 * 1024).unwrap();
    eng.set_peer_id(1);
    for (i, &e) in u_eids.iter().enumerate() {
        eng.tie_async(e, "users.age", (20 + i) as u32);
    }
    for (i, &e) in p_eids.iter().enumerate() {
        eng.tie_async(e, "posts.score", (100 + i) as u32);
    }
    eng.flush_writes();
    eng.wal_sync().unwrap();

    // 全件
    let all = eng.audit(&AuditFilter::default());
    assert!(
        all.len() >= 5,
        "expected >= 5 records (3 user tie + 2 post tie), got {}",
        all.len()
    );

    // users のみ
    let users_only = eng.audit(&AuditFilter {
        table_id: Some(users_tid),
        ..Default::default()
    });
    let users_count = users_only
        .iter()
        .filter(|r| {
            matches!(
                r.op,
                enchudb_wal::wal::DecodedOp::Tie { .. }
            )
        })
        .count();
    assert_eq!(users_count, 3, "expected 3 user-table tie records");

    // posts のみ
    let posts_only = eng.audit(&AuditFilter {
        table_id: Some(posts_tid),
        ..Default::default()
    });
    let posts_count = posts_only
        .iter()
        .filter(|r| matches!(r.op, enchudb_wal::wal::DecodedOp::Tie { .. }))
        .count();
    assert_eq!(posts_count, 2, "expected 2 post-table tie records");

    drop(eng);
    cleanup(&p);
}
