//! #119 Step 0: schema 経由の text 読みが writer 稼働中も壊れないこと。
//!
//! 旧挙動: `EntityRef::get` は engine の**借用返し** `get_text` を呼んでいた。 借用版は
//! #106 の slot gen seqlock verify を通らないため、 writer が Leaf を re-tie している間に
//! **torn bytes を掴み**、 直後の `from_utf8` が失敗して **silent に `None`** を返す
//! (「値が無い」と「壊れて読めなかった」が区別できない)。 SQL / RAG / ravn も同じ経路。
//!
//! 修正: 内部呼び出しを verify 付きの `get_text_owned` へ寄せる。 元々即コピーしていたので
//! コピー回数は不変、 増えるのは verify の再読のみ。
//!
//! 固定 /tmp 併用の偽 flaky を避けるため path は pid + nanos で unique 化。

use enchudb_schema::{Database, Value};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir()
        .join(format!("enchudb-issue119-{}-{}-{}.enchu", tag, std::process::id(), nanos))
        .to_string_lossy()
        .into_owned()
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    for ext in ["lock", "oplog", "schema", "tables"] {
        let _ = std::fs::remove_file(format!("{}.{}", path, ext));
    }
}

/// Leaf 列を re-tie し続ける writer と並行に schema 経由で読み、
/// **silent None も壊れた文字列も観測されない**こと。
#[test]
fn schema_text_read_survives_concurrent_rewrite() {
    let path = tmp_path("step0");
    cleanup(&path);

    // 長さの違う値を回すことで Leaf slot の free → 再利用 (借用が死ぬ経路) を誘発する。
    let bodies: Vec<String> = (0..6)
        .map(|i| format!("body-{}-{}", i, "x".repeat(64 + i * 97)))
        .collect();

    let db = {
        let mut db = Database::create_growable_tiny(&path).unwrap();
        db.table("doc").tag("key").leaf("body").primary_key("key").build().unwrap();
        db.finish_with_oplog(256 * 1024).unwrap()
    };

    let eids: Vec<u64> = {
        let t = db.get_table("doc").unwrap();
        (0..24)
            .map(|i| {
                t.insert()
                    .set("key", format!("k{i}").as_str())
                    .set("body", bodies[i % bodies.len()].as_str())
                    .commit()
                    .unwrap()
            })
            .collect()
    };

    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicUsize::new(0));

    let writer = {
        let db = db.clone();
        let stop = stop.clone();
        let writes = writes.clone();
        let bodies = bodies.clone();
        let eids = eids.clone();
        std::thread::spawn(move || {
            let t = db.get_table("doc").unwrap();
            let mut n = 0usize;
            while !stop.load(Ordering::Relaxed) {
                for &eid in &eids {
                    let body = &bodies[n % bodies.len()];
                    // re-tie = 新 slot 確保 + 旧 slot free → free-list 経由で再利用される
                    let _ = t.entity(eid).update().set("body", body.as_str()).commit();
                    n += 1;
                }
                writes.store(n, Ordering::Relaxed);
            }
        })
    };

    // reader: schema 経由 (= EntityRef::get → engine) で読み続ける
    let t = db.get_table("doc").unwrap();
    let mut reads = 0usize;
    let mut missing = 0usize;
    let mut corrupt = 0usize;
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(1500);
    while std::time::Instant::now() < deadline {
        for &eid in &eids {
            match t.entity(eid).get("body") {
                Some(Value::Text(s)) => {
                    if !bodies.iter().any(|b| *b == s) {
                        corrupt += 1;
                    }
                }
                Some(other) => panic!("Leaf 列が Text 以外を返した: {other:?}"),
                None => missing += 1,
            }
            reads += 1;
        }
    }
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    assert!(reads > 1000, "read が回っていない (reads={reads})");
    assert!(
        writes.load(Ordering::Relaxed) > 100,
        "writer が回っていない (writes={})",
        writes.load(Ordering::Relaxed)
    );
    assert_eq!(
        missing, 0,
        "writer 稼働中に silent None が {missing} 件 (reads={reads}) — #119 Step 0 の regression"
    );
    assert_eq!(corrupt, 0, "既知の値以外が読めた (torn bytes) {corrupt} 件 / reads={reads}");

    drop(t);
    drop(db);
    cleanup(&path);
}
