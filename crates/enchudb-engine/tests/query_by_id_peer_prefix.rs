//! 0.8.4 issue #32: `query_by_id` 戻り値が peer prefix を持つこと。
//!
//! 旧 behavior: `query_resolved -> Vec<u32>` を `as EntityId` (= u32→u64 widen)
//! で変換していたので peer prefix が 0 に潰れ、 schema 層の
//! `where_eq().find_one()` 等が壊れた eid を返していた。
//! 修正: `make_eid(self.peer_id(), local)` で正しく peer prefix を付ける。

use enchudb_engine::{Engine, ValueType};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-query-by-id-peer-{}-{}-{}",
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
fn query_by_id_returns_eid_with_peer_prefix() {
    let path = tmp_path("peer_prefix");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "val", ValueType::Number, 0).unwrap();
    eng.set_peer_id(7);
    let hid = eng.himo_id("notes.val").unwrap() as u16;

    let e1 = eng.entity_in("notes").unwrap();
    eng.tie_to(e1, "notes.val", 42);
    let e2 = eng.entity_in("notes").unwrap();
    eng.tie_to(e2, "notes.val", 99);

    // query_by_id で val == 42 を引く
    let hits = eng.query_by_id(&[(hid, 42)]);
    assert_eq!(hits.len(), 1, "should hit 1 entity with val=42");

    // peer prefix が 7 になっていること (= 旧 bug では 0)
    let peer = enchudb_oplog::eid_peer(hits[0]);
    assert_eq!(peer, 7, "query_by_id eid should have peer=7, got peer={}", peer);

    // local part も e1 と一致
    assert_eq!(hits[0], e1, "hit should equal the original e1 (full eid match)");

    cleanup(&path);
}

#[test]
fn query_by_id_default_peer_zero_still_works() {
    // peer_id 未設定 (= 0) でも従来通り動く、 ただし peer prefix は 0 のまま
    let path = tmp_path("peer_default");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "val", ValueType::Number, 0).unwrap();
    let hid = eng.himo_id("notes.val").unwrap() as u16;

    let e1 = eng.entity_in("notes").unwrap();
    eng.tie_to(e1, "notes.val", 42);

    let hits = eng.query_by_id(&[(hid, 42)]);
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0], e1);

    cleanup(&path);
}
