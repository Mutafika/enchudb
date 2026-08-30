//! issue12: `where_in` (Predicate::In) が単独使用で常に空を返す bug の regression test。
//!
//! 旧実装は IN を「候補集合への post-filter (retain)」 としてしか扱わず、
//! IN が唯一の述語のとき base candidates が `query_by_id(&[])` = 空になり、
//! 空集合を retain して常に 0 件だった。 fix 後は IN 集合自体が候補の seed になる。
//!
//! sunsu home-timeline (fan-out-on-read) の repro を enchudb-schema API のみで移植。

use enchudb_schema::{Database, Value};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue12-{}-{}-{}",
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
    for suffix in ["", ".oplog", ".tables", ".crc", ".schema", ".db.lock", ".lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

/// issue12 の観測表そのまま: where_ref / where_eq は効くのに where_in が 0 件。
#[test]
fn where_in_standalone_seeds_candidates() {
    let path = tmp_path("standalone");
    cleanup(&path);

    let mut db = Database::create_growable_with_capacity(&path, 10_000).unwrap();
    db.table("author").number("id").with_capacity(100).build().unwrap();
    db.table("post")
        .ref_to("by", "author")
        .number("v")
        .with_capacity(1000)
        .build()
        .unwrap();

    let authors_t = db.get_table("author").unwrap();
    let a0 = authors_t.insert().set("id", 0i64).commit().unwrap();
    let a1 = authors_t.insert().set("id", 1i64).commit().unwrap();

    let posts_t = db.get_table("post").unwrap();
    for i in 0..5 {
        posts_t.insert().set("by", Value::Ref(a0)).set("v", i as i64).commit().unwrap();
    }
    for i in 0..3 {
        posts_t.insert().set("by", Value::Ref(a1)).set("v", i as i64).commit().unwrap();
    }

    // 既存動作 (Eq 系) が壊れていないこと
    assert_eq!(posts_t.where_ref("by", a0).count().unwrap(), 5);
    assert_eq!(posts_t.where_eq("v", 0i64).count().unwrap(), 2);

    // issue12 本体: where_in 単独 (旧実装は全部 0 件)
    assert_eq!(posts_t.where_in("by", &[a0 as u32]).count().unwrap(), 5);
    assert_eq!(posts_t.where_in("by", &[a0 as u32, a1 as u32]).count().unwrap(), 8);
    assert_eq!(posts_t.where_in("v", &[0, 1]).count().unwrap(), 4);

    // 存在しない値だけの IN は空
    assert_eq!(posts_t.where_in("v", &[999]).count().unwrap(), 0);
    // 空 IN リストも空
    assert_eq!(posts_t.where_in("v", &[]).count().unwrap(), 0);

    cleanup(&path);
}

/// 従来から動いていた eq + in の組合せ (intersect 経路) が退行していないこと。
#[test]
fn where_in_combined_with_eq_still_intersects() {
    let path = tmp_path("combined");
    cleanup(&path);

    let mut db = Database::create_growable_with_capacity(&path, 10_000).unwrap();
    db.table("author").number("id").with_capacity(100).build().unwrap();
    db.table("post")
        .ref_to("by", "author")
        .number("v")
        .with_capacity(1000)
        .build()
        .unwrap();

    let authors_t = db.get_table("author").unwrap();
    let a0 = authors_t.insert().set("id", 0i64).commit().unwrap();
    let a1 = authors_t.insert().set("id", 1i64).commit().unwrap();

    let posts_t = db.get_table("post").unwrap();
    for i in 0..5 {
        posts_t.insert().set("by", Value::Ref(a0)).set("v", i as i64).commit().unwrap();
    }
    for i in 0..3 {
        posts_t.insert().set("by", Value::Ref(a1)).set("v", i as i64).commit().unwrap();
    }

    // by = a0 AND v IN (0, 1) → a0 の v=0, v=1 の 2 件
    assert_eq!(
        posts_t.where_eq("v", 0i64).where_in("by", &[a0 as u32]).count().unwrap(),
        1
    );
    let found = posts_t
        .where_ref("by", a0)
        .where_in("v", &[0, 1])
        .find()
        .unwrap();
    assert_eq!(found.len(), 2);

    // intersect が空になる組合せ
    assert_eq!(
        posts_t.where_ref("by", a1).where_in("v", &[4]).count().unwrap(),
        0
    );

    cleanup(&path);
}

/// find() の返す eid が where_ref 経路と同一 (peer prefix 込みで一致) であること。
/// pull_in_by_idx の旧 `e as EntityId` は #32 と同型の peer prefix 抜けだった。
#[test]
fn where_in_returns_same_eids_as_where_ref() {
    let path = tmp_path("eid_parity");
    cleanup(&path);

    let mut db = Database::create_growable_with_capacity(&path, 10_000).unwrap();
    db.table("author").number("id").with_capacity(100).build().unwrap();
    db.table("post")
        .ref_to("by", "author")
        .number("v")
        .with_capacity(1000)
        .build()
        .unwrap();

    let authors_t = db.get_table("author").unwrap();
    let a0 = authors_t.insert().set("id", 0i64).commit().unwrap();

    let posts_t = db.get_table("post").unwrap();
    for i in 0..5 {
        posts_t.insert().set("by", Value::Ref(a0)).set("v", i as i64).commit().unwrap();
    }

    let mut by_ref = posts_t.where_ref("by", a0).find().unwrap();
    let mut by_in = posts_t.where_in("by", &[a0 as u32]).find().unwrap();
    by_ref.sort_unstable();
    by_in.sort_unstable();
    assert_eq!(by_ref, by_in);

    cleanup(&path);
}
