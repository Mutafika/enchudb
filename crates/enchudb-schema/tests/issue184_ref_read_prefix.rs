//! #184: `Value::Ref` の round-trip で peer prefix が落ちないこと。
//!
//! storage の Ref 値は u32 (local 部) だが、schema 層の `find()` / `commit()` は
//! peer prefix 付きの full eid を返す。read (`EntityRef::get`) だけが素 cast で
//! local 部を返すと、同じ層の API 同士で eid 表現が食い違い `==` 比較が silent に
//! 外れる (発見経路: sunsu2 の timeline 重複 materialize)。

use enchudb_schema::{Database, Value};

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue184-{}-{}-{}",
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
    for suf in ["", ".oplog", ".tables", ".schema", ".crc", ".db.lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

fn build(path: &str, peer_id: u32) -> Database {
    let mut db = Database::create_with_capacity(path, 4096).unwrap();
    db.table("posts").tag("guid").primary_key("guid").build().unwrap();
    db.table("likes").tag("guid").ref_to("target", "posts").primary_key("guid").build().unwrap();
    db.engine().set_peer_id(peer_id);
    db
}

/// peer_id 付き DB: 書いた Ref がそのまま読める + find() の eid と一致する。
#[test]
fn ref_roundtrip_preserves_peer_prefix() {
    let path = tmp("prefix");
    cleanup(&path);
    let db = build(&path, 7);

    let post = db.get_table("posts").unwrap().insert().set("guid", "p1").commit().unwrap();
    assert_eq!(enchudb_oplog::eid_peer(post), 7, "precondition: eids carry the peer prefix");

    db.get_table("likes")
        .unwrap()
        .insert()
        .set("guid", "l1")
        .set("target", Value::Ref(post))
        .commit()
        .unwrap();

    let likes = db.get_table("likes").unwrap();
    let like = likes.where_eq("guid", "l1").find_one().unwrap().unwrap();
    assert_eq!(
        likes.entity(like).get("target"),
        Some(Value::Ref(post)),
        "Ref read must return the same full eid that commit()/find() hand out"
    );

    // 読めた Ref をそのまま where_ref に返しても一致する (相互運用)
    assert_eq!(likes.where_ref("target", post).count().unwrap(), 1);

    cleanup(&path);
}

/// peer_id = 0 (standalone): make_eid(0, local) == local なので従来と同値。
#[test]
fn ref_roundtrip_unchanged_for_peer_zero() {
    let path = tmp("zero");
    cleanup(&path);
    let db = build(&path, 0);

    let post = db.get_table("posts").unwrap().insert().set("guid", "p1").commit().unwrap();
    db.get_table("likes")
        .unwrap()
        .insert()
        .set("guid", "l1")
        .set("target", Value::Ref(post))
        .commit()
        .unwrap();

    let likes = db.get_table("likes").unwrap();
    let like = likes.where_eq("guid", "l1").find_one().unwrap().unwrap();
    assert_eq!(likes.entity(like).get("target"), Some(Value::Ref(post)));

    cleanup(&path);
}
