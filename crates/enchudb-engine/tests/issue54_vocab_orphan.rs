//! issue #54: leaf himo re-tie / remove で vocab に orphan が残ることを検出する
//! `Engine::vocab_orphan_stats()` API の regression test。

use enchudb_engine::{Engine, ValueType};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue54-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
    let _ = std::fs::remove_file(format!("{}.tables", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.db.lock", path));
}

#[test]
fn leaf_retie_creates_orphan() {
    let path = tmp_path("leaf-retie");
    cleanup(&path);

    let mut eng = Engine::create_growable_with_capacity(&path, 1000).unwrap();
    eng.define_table("notes", 100).unwrap();
    eng.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();

    let eid = eng.entity_in("notes").unwrap();
    eng.tie_text(eid, "notes.body", "first");
    // 1 vocab vid (= "first")、 orphan 0
    let s0 = eng.vocab_orphan_stats();
    assert_eq!(s0.vocab_total, 1, "after first tie");
    assert_eq!(s0.live_vids, 1);
    assert_eq!(s0.orphan_vids, 0);

    // re-tie: 旧 vid 0 (= "first") が orphan、 新 vid 1 (= "second") が live
    eng.tie_text(eid, "notes.body", "second");
    let s1 = eng.vocab_orphan_stats();
    assert_eq!(s1.vocab_total, 2, "after re-tie (= insert)");
    assert_eq!(s1.live_vids, 1);
    assert_eq!(s1.orphan_vids, 1);
    assert_eq!(s1.orphan_bytes, "first".len() as u64);
    assert_eq!(s1.live_bytes, "second".len() as u64);

    // 3 回目 re-tie: 旧 "second" も orphan に。
    eng.tie_text(eid, "notes.body", "third-longer-value");
    let s2 = eng.vocab_orphan_stats();
    assert_eq!(s2.vocab_total, 3);
    assert_eq!(s2.live_vids, 1);
    assert_eq!(s2.orphan_vids, 2);
    assert_eq!(s2.orphan_bytes, ("first".len() + "second".len()) as u64);

    cleanup(&path);
}

#[test]
fn leaf_remove_creates_orphan() {
    let path = tmp_path("leaf-remove");
    cleanup(&path);

    let mut eng = Engine::create_growable_with_capacity(&path, 1000).unwrap();
    eng.define_table("notes", 100).unwrap();
    eng.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();

    let eid = eng.entity_in("notes").unwrap();
    eng.tie_text(eid, "notes.body", "hello world");
    let s0 = eng.vocab_orphan_stats();
    assert_eq!(s0.orphan_vids, 0);

    // remove: himo cell は clear、 vocab vid 0 は残置 → orphan
    eng.untie(eid, "notes.body");
    let s1 = eng.vocab_orphan_stats();
    assert_eq!(s1.vocab_total, 1);
    assert_eq!(s1.live_vids, 0);
    assert_eq!(s1.orphan_vids, 1);
    assert_eq!(s1.orphan_bytes, "hello world".len() as u64);

    cleanup(&path);
}

#[test]
fn tag_dedup_no_orphan() {
    let path = tmp_path("tag-dedup");
    cleanup(&path);

    let mut eng = Engine::create_growable_with_capacity(&path, 1000).unwrap();
    eng.define_table("items", 100).unwrap();
    eng.define_himo_in("items", "kind", ValueType::Tag, 0).unwrap();

    // 同じ tag を 3 個 tie → vocab に 1 vid のみ (= dedup)、 orphan 0
    for _ in 0..3 {
        let eid = eng.entity_in("items").unwrap();
        eng.tie_text(eid, "items.kind", "fruit");
    }
    let s0 = eng.vocab_orphan_stats();
    assert_eq!(s0.vocab_total, 1);
    assert_eq!(s0.live_vids, 1);
    assert_eq!(s0.orphan_vids, 0);

    cleanup(&path);
}

#[test]
fn dead_ratio_helper() {
    let path = tmp_path("ratio");
    cleanup(&path);

    let mut eng = Engine::create_growable_with_capacity(&path, 1000).unwrap();
    eng.define_table("n", 100).unwrap();
    eng.define_himo_in("n", "v", ValueType::Leaf, 0).unwrap();

    let eid = eng.entity_in("n").unwrap();
    eng.tie_text(eid, "n.v", "a");
    eng.tie_text(eid, "n.v", "b");
    eng.tie_text(eid, "n.v", "c");
    eng.tie_text(eid, "n.v", "d");
    // 4 vid 中 3 が orphan
    let s = eng.vocab_orphan_stats();
    assert_eq!(s.vocab_total, 4);
    assert_eq!(s.orphan_vids, 3);
    assert!((s.dead_ratio() - 0.75).abs() < 1e-9);

    cleanup(&path);
}
