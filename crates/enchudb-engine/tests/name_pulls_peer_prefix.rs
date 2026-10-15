//! 名前で引く `pull_raw` / `pull_range` / `query` の戻り値が peer の頭を持つこと (`query_by_id_peer_prefix.rs` (#32) と同じ形)。
//!
//! 前は `HimoStore` の local の番号を `as EntityId` (u32 → u64 の widen) で返していたので、 peer の頭が 0 に潰れていた
//! (`entities()` / `query_by_id` / `pull_in` / `pull_range_by_id` は頭付き)。 頭付きの eid と比べたり集合で差を取ったりすると
//! 食い違っていた (RAG の NOT は `entities()` との差)。 修正: `make_eid(self.peer_id(), local)`。

use enchudb_engine::{Engine, ValueType};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-name-pulls-peer-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path); // v10: DB は directory
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
    let _ = std::fs::remove_file(format!("{}.tables", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.db.lock", path));
}

/// peer を `peer` にして 3 entity を作り、 名前で引く 3 つの API が元の eid (頭付き) をそのまま返すかを見る。
fn check(peer: Option<u32>, tag: &str) {
    let path = tmp_path(tag);
    cleanup(&path);
    let mut eng = Engine::create_with_capacity(&path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "val", ValueType::Number, 0).unwrap();
    eng.define_himo_in("notes", "ts", ValueType::Number64, 0).unwrap();
    if let Some(p) = peer {
        eng.set_peer_id(p);
    }
    let mut ids = Vec::new();
    for (v, ts) in [(42u32, 5u64 << 40), (42, 7u64 << 40), (99, 9u64 << 40)] {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.val", v);
        eng.tie_to(e, "notes.ts", ts);
        ids.push(e);
    }
    let want_peer = peer.unwrap_or(0);
    assert!(ids.iter().all(|&e| enchudb_oplog::eid_peer(e) == want_peer), "test の前提: entity の eid が peer {want_peer} の頭付き");

    let mut got = eng.pull_raw("notes.val", 42u32);
    got.sort_unstable();
    assert_eq!(got, vec![ids[0], ids[1]], "pull_raw (peer {want_peer})");

    let got = eng.pull_range("notes.val", 50u32, 100u32);
    assert_eq!(got, vec![ids[2]], "pull_range (peer {want_peer})");
    let got = eng.pull_range("notes.ts", 6u64 << 40, u64::MAX - 1);
    assert_eq!(got, vec![ids[1], ids[2]], "pull_range の 64 bit の列 (peer {want_peer})");

    let mut got = eng.query(&[("notes.val", 42)]);
    got.sort_unstable();
    assert_eq!(got, vec![ids[0], ids[1]], "query (peer {want_peer})");

    // 頭付きの eid は engine の読みにそのまま渡せる (中で local の番号に戻す)
    assert_eq!(eng.get(got[0], "notes.val"), Some(42));
    // entities() と同じ形 (差を取ると、 引いた分だけ消える)
    let all = eng.entities();
    let rest: Vec<u64> = all.iter().copied().filter(|e| !got.contains(e)).collect();
    assert_eq!(rest, vec![ids[2]], "entities() から引いた分を除くと残りだけ (peer {want_peer})");
    cleanup(&path);
}

#[test]
fn name_pulls_return_eids_with_peer_prefix() {
    check(Some(7), "peer7");
}

#[test]
fn name_pulls_default_peer_zero_still_works() {
    // peer 未設定 (= 0) では頭は 0 で、 前と同じ値
    check(None, "peer0");
}
