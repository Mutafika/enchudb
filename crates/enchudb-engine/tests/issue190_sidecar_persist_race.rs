//! #190: 同一 sidecar への並行 persist が race しないこと。
//!
//! tmp 名が `{sidecar}.tmp` 固定なので、fix 前は 2 thread が同じ tmp を
//! truncate し合い、(a) rename ENOENT、(b) torn install、(c) 新旧逆転 install
//! が起きた。fix (= `sidecar_persist_lock` で serialize〜rename を直列化) 後は
//! 並行 persist_tables が全部 Ok で返る。

use std::sync::Arc;

use enchudb_engine::{Engine, ValueType};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue190-{}-{}-{}",
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
    for suf in ["", ".oplog", ".tables", ".tables.tmp", ".eidmap", ".eidmap.tmp", ".vocabmap", ".vocabmap.tmp", ".crc", ".db.lock"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

#[test]
fn concurrent_persist_tables_never_races() {
    let path = tmp_path("race");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("t", 1000).unwrap();
    eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
    let eng = Arc::new(eng);

    let threads: Vec<_> = (0..4)
        .map(|_| {
            let eng = eng.clone();
            std::thread::spawn(move || {
                for i in 0..100 {
                    eng.persist_tables().unwrap_or_else(|e| {
                        panic!("persist_tables raced at iter {i}: {e}");
                    });
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }

    // install された sidecar が torn でないこと (= reopen が table 定義を読める)
    drop(eng);
    let eng = Engine::open(&path).unwrap();
    assert!(
        eng.list_tables().iter().any(|(_, name, _, _)| name == "t"),
        "sidecar が torn だと table 定義が落ちる"
    );

    drop(eng);
    cleanup(&path);
}
