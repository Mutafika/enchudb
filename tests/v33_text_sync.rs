//! v33 text sync E2E: tie_text_async が peer 間で正しく伝搬することを確認。
//!
//! BUGS.md 層 2・3 の修正検証:
//! - 層 2: `tie_text_async` は WAL に Vocab + Tie の 2 op を流す
//! - 層 3: receiver は Vocab op で (author_peer, remote_vid) → local_vid mapping を張り、
//!   後続 Tie で symbol 型 himo の value を translate して apply する

#![cfg(feature = "v33")]

use std::sync::Arc;
use enchudb::{Engine, HimoType, Hlc};
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};

fn tmp(tag: &str) -> String {
    let p = format!("/tmp/enchudb-v33-text-{}-{}", tag, std::process::id());
    for suffix in ["", ".wal", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", p, suffix));
    }
    p
}

fn cleanup(path: &str) {
    for suffix in ["", ".wal", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

/// schema + WAL 付きで peer を作る。schema は同じ = 両 peer とも同じ himo_id が振られる。
fn make_peer(path: &str, peer: u32) -> Arc<Engine> {
    {
        let mut eng = Engine::create_standalone(path).unwrap();
        eng.define_himo("name", HimoType::Symbol, 0);
        eng.define_himo("age", HimoType::Value, 100);
        eng.flush().unwrap();
    }
    let eng = Engine::open_concurrent_with_wal(path, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

#[test]
fn single_text_tie_propagates_to_peer_b() {
    let pa = tmp("single_a");
    let pb = tmp("single_b");
    let eng_a = make_peer(&pa, 1);
    let eng_b = make_peer(&pb, 2);
    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());

    // peer A: Alice を書く
    let eid_alice = eng_a.entity();
    eng_a.tie_text_async(eid_alice, "name", "Alice");
    eng_a.wal_commit();
    eng_a.flush_writes();
    eng_a.wal_sync().unwrap();

    // A が publish → B が pull
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());
    let published = syncer_a.publish_since(Hlc::ZERO);
    assert!(published >= 2, "Vocab + Tie = 2 records, got {}", published);

    let out = syncer_b.pull_once(1);
    assert!(out.applied >= 2, "peer B should apply Vocab + Tie, got {:?}", out);

    // B から同じ text が読めること(vid 変換されて local vocab にあるはず)
    let text = eng_b.get_text(eid_alice, "name");
    assert_eq!(text.map(|b| b.to_vec()), Some(b"Alice".to_vec()));

    cleanup(&pa);
    cleanup(&pb);
}

#[test]
fn multiple_text_values_preserve_distinct_vids() {
    // 3 件の text を A から B へ。B 側でそれぞれ別々に読めること。
    let pa = tmp("multi_a");
    let pb = tmp("multi_b");
    let eng_a = make_peer(&pa, 1);
    let eng_b = make_peer(&pb, 2);
    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());

    let e1 = eng_a.entity();
    let e2 = eng_a.entity();
    let e3 = eng_a.entity();
    eng_a.tie_text_async(e1, "name", "Alice");
    eng_a.tie_text_async(e2, "name", "Bob");
    eng_a.tie_text_async(e3, "name", "Carol");
    eng_a.wal_commit();
    eng_a.flush_writes();
    eng_a.wal_sync().unwrap();

    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());
    syncer_a.publish_since(Hlc::ZERO);
    let out = syncer_b.pull_once(1);
    // Vocab 3 + Tie 3 = 6 (+ Commit markers)
    assert!(out.applied >= 6, "expected 6 applies, got {:?}", out);

    assert_eq!(eng_b.get_text(e1, "name").map(|b| b.to_vec()), Some(b"Alice".to_vec()));
    assert_eq!(eng_b.get_text(e2, "name").map(|b| b.to_vec()), Some(b"Bob".to_vec()));
    assert_eq!(eng_b.get_text(e3, "name").map(|b| b.to_vec()), Some(b"Carol".to_vec()));

    cleanup(&pa);
    cleanup(&pb);
}

#[test]
fn repeated_same_text_is_deduped_on_receiver() {
    // A が同じ値を 3 つ書く → B で 3 entity が全部 "Alice" を指す
    let pa = tmp("dedup_a");
    let pb = tmp("dedup_b");
    let eng_a = make_peer(&pa, 1);
    let eng_b = make_peer(&pb, 2);
    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());

    let e1 = eng_a.entity();
    let e2 = eng_a.entity();
    let e3 = eng_a.entity();
    eng_a.tie_text_async(e1, "name", "Alice");
    eng_a.tie_text_async(e2, "name", "Alice");
    eng_a.tie_text_async(e3, "name", "Alice");
    eng_a.wal_commit();
    eng_a.flush_writes();
    eng_a.wal_sync().unwrap();

    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());
    syncer_a.publish_since(Hlc::ZERO);
    syncer_b.pull_once(1);

    // B 側でも 3 entity 全部 "Alice"
    for eid in [e1, e2, e3] {
        assert_eq!(eng_b.get_text(eid, "name").map(|b| b.to_vec()), Some(b"Alice".to_vec()));
    }

    cleanup(&pa);
    cleanup(&pb);
}

#[test]
fn peer_a_and_b_share_text_even_if_each_coined_local_vid_first() {
    // 両 peer が「先に」ローカルで別 entity に同じ text を tie_text_async した場合、
    // それぞれ local vid は別物 (A で "Zed" が vid=3、B で "Zed" が vid=5 になる等)。
    // その後 A → B 方向の sync で B 側の (A_peer, 3) → B_local_vid が張られ、
    // 受信側は "正しい text" にたどり着ける。
    let pa = tmp("coin_a");
    let pb = tmp("coin_b");
    let eng_a = make_peer(&pa, 1);
    let eng_b = make_peer(&pb, 2);

    // B が先に "Zed" を別 eid で使う(B 側 local_vid が A とズレる可能性)
    let e_b_local = eng_b.entity();
    eng_b.tie_text_async(e_b_local, "name", "Zed");
    eng_b.wal_commit();
    eng_b.flush_writes();
    eng_b.wal_sync().unwrap();

    // A が自分の eid に "Zed" を tie
    let e_a = eng_a.entity();
    eng_a.tie_text_async(e_a, "name", "Zed");
    eng_a.wal_commit();
    eng_a.flush_writes();
    eng_a.wal_sync().unwrap();

    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());
    syncer_a.publish_since(Hlc::ZERO);
    syncer_b.pull_once(1);

    // B は A の e_a entity に対して "Zed" を読めるべき
    assert_eq!(eng_b.get_text(e_a, "name").map(|b| b.to_vec()), Some(b"Zed".to_vec()));
    // 自分の e_b_local も "Zed" のまま
    assert_eq!(eng_b.get_text(e_b_local, "name").map(|b| b.to_vec()), Some(b"Zed".to_vec()));

    cleanup(&pa);
    cleanup(&pb);
}
