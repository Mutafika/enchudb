//! v33 text sync E2E: tie_text_async が peer 間で正しく伝搬することを確認。
//!
//! BUGS.md 層 2・3 の修正検証:
//! - 層 2: `tie_text_async` は WAL に Vocab + Tie の 2 op を流す
//! - 層 3: receiver は Vocab op で (author_peer, remote_vid) → local_vid mapping を張り、
//!   後続 Tie で symbol 型 himo の value を translate して apply する

use std::sync::Arc;
use enchudb::{Engine, ValueType};
use enchudb_oplog::Hlc;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};

fn tmp(tag: &str) -> String {
    let p = format!("/tmp/enchudb-v33-text-{}-{}", tag, std::process::id());
    for suffix in ["", ".oplog", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", p, suffix));
    }
    p
}

fn cleanup(path: &str) {
    for suffix in ["", ".oplog", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

/// **named table 必須** (β-light step 3): `enable_sync_tables()` が `define_table` を
/// 呼ぶため anonymous table が閉じ `entity()` は panic する。 加えて anonymous のままだと
/// 受信 op の foreign eid を確保する先が無く `resolve_remote_eid` が None を返して
/// apply が丸ごと skip される (#9)。
const TABLE: &str = "t";

/// `TABLE` 内 himo の qualified name。
fn q(himo: &str) -> String {
    format!("{}.{}", TABLE, himo)
}

/// schema + WAL 付きで peer を作る。schema は同じ = 両 peer とも同じ himo_id が振られる。
fn make_peer(path: &str, peer: u32) -> Arc<Engine> {
    {
        let mut eng = Engine::create_standalone(path).unwrap();
        eng.define_table(TABLE, 1000).unwrap();
        eng.define_himo_in(TABLE, "name", ValueType::Tag, 0).unwrap();
        eng.define_himo_in(TABLE, "age", ValueType::Number, 100).unwrap();
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
    }
    let eng = Engine::open_concurrent_with_oplog(path, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

/// 受信側で foreign eid を自分の eid 空間へ翻訳する (#9)。
/// `Syncer::apply_one` が翻訳して格納するので、 送信側の eid のままでは読めない。
fn local_of(eng: &Engine, foreign_eid: u64, himo: &str) -> Option<u64> {
    let hid = eng.himo_id(&q(himo)).unwrap() as u16;
    eng.resolve_remote_eid(foreign_eid, hid)
}

/// 翻訳込みの text 読み出し。
fn get_text_remote(eng: &Engine, foreign_eid: u64, himo: &str) -> Option<Vec<u8>> {
    let local = local_of(eng, foreign_eid, himo)?;
    eng.get_text(local, &q(himo)).map(|b| b.to_vec())
}

#[test]
fn single_text_tie_propagates_to_peer_b() {
    let pa = tmp("single_a");
    let pb = tmp("single_b");
    let eng_a = make_peer(&pa, 1);
    let eng_b = make_peer(&pb, 2);
    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());

    // peer A: Alice を書く
    let eid_alice = eng_a.entity_in(TABLE).unwrap();
    eng_a.tie_text_async(eid_alice, &q("name"), "Alice");
    eng_a.oplog_commit();
    eng_a.flush_writes();
    eng_a.oplog_sync().unwrap();
    // 0.8.0: publish の primary は `_sync_ops`。 background 転送待ちに
    // 依存すると publish_since が空振りするので明示的に回す (sleep より決定的)。
    eng_a.transfer_oplog_to_sync_ops();

    // A が publish → B が pull
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());
    let published = syncer_a.publish_since(Hlc::ZERO);
    assert!(published >= 2, "Vocab + Tie = 2 records, got {}", published);

    let out = syncer_b.pull_once(1);
    assert!(out.applied >= 2, "peer B should apply Vocab + Tie, got {:?}", out);

    // B から同じ text が読めること(vid 変換されて local vocab にあるはず)
    let text = get_text_remote(&eng_b, eid_alice, "name");
    assert_eq!(text, Some(b"Alice".to_vec()));

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

    let e1 = eng_a.entity_in(TABLE).unwrap();
    let e2 = eng_a.entity_in(TABLE).unwrap();
    let e3 = eng_a.entity_in(TABLE).unwrap();
    eng_a.tie_text_async(e1, &q("name"), "Alice");
    eng_a.tie_text_async(e2, &q("name"), "Bob");
    eng_a.tie_text_async(e3, &q("name"), "Carol");
    eng_a.oplog_commit();
    eng_a.flush_writes();
    eng_a.oplog_sync().unwrap();
    // 0.8.0: publish の primary は `_sync_ops`。 background 転送待ちに
    // 依存すると publish_since が空振りするので明示的に回す (sleep より決定的)。
    eng_a.transfer_oplog_to_sync_ops();

    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());
    syncer_a.publish_since(Hlc::ZERO);
    let out = syncer_b.pull_once(1);
    // Vocab 3 + Tie 3 = 6 (+ Commit markers)
    assert!(out.applied >= 6, "expected 6 applies, got {:?}", out);

    assert_eq!(get_text_remote(&eng_b, e1, "name"), Some(b"Alice".to_vec()));
    assert_eq!(get_text_remote(&eng_b, e2, "name"), Some(b"Bob".to_vec()));
    assert_eq!(get_text_remote(&eng_b, e3, "name"), Some(b"Carol".to_vec()));

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

    let e1 = eng_a.entity_in(TABLE).unwrap();
    let e2 = eng_a.entity_in(TABLE).unwrap();
    let e3 = eng_a.entity_in(TABLE).unwrap();
    eng_a.tie_text_async(e1, &q("name"), "Alice");
    eng_a.tie_text_async(e2, &q("name"), "Alice");
    eng_a.tie_text_async(e3, &q("name"), "Alice");
    eng_a.oplog_commit();
    eng_a.flush_writes();
    eng_a.oplog_sync().unwrap();
    // 0.8.0: publish の primary は `_sync_ops`。 background 転送待ちに
    // 依存すると publish_since が空振りするので明示的に回す (sleep より決定的)。
    eng_a.transfer_oplog_to_sync_ops();

    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());
    syncer_a.publish_since(Hlc::ZERO);
    syncer_b.pull_once(1);

    // B 側でも 3 entity 全部 "Alice"
    for eid in [e1, e2, e3] {
        assert_eq!(get_text_remote(&eng_b, eid, "name"), Some(b"Alice".to_vec()));
    }

    cleanup(&pa);
    cleanup(&pb);
}

#[test]
fn tie_ref_async_propagates_between_peers() {
    // Ref himo で entity 参照 (同一 peer 内の ref) が sync されることを確認
    let pa = tmp("ref_a");
    let pb = tmp("ref_b");
    // Ref は `define_ref_in` で **target table まで宣言**する必要がある。
    // 素の `define_himo_in(.., ValueType::Ref, ..)` だと apply 側の
    // `resolve_remote_ref_value` が翻訳先 table を決められず None を返し、
    // Tie op ごと drop される (= B に何も届かない)。
    for path in [&pa, &pb] {
        let mut eng = Engine::create_standalone(path).unwrap();
        eng.define_table(TABLE, 1000).unwrap();
        eng.define_himo_in(TABLE, "tag", ValueType::Number, 0).unwrap();
        eng.define_ref_in(TABLE, "parent", TABLE).unwrap();
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
    }
    let eng_a = Engine::open_concurrent_with_oplog(&pa, 16 * 1024 * 1024).unwrap();
    eng_a.set_peer_id(1);
    let eng_b = Engine::open_concurrent_with_oplog(&pb, 16 * 1024 * 1024).unwrap();
    eng_b.set_peer_id(2);

    let parent = eng_a.entity_in(TABLE).unwrap();
    let child = eng_a.entity_in(TABLE).unwrap();
    // parent 自体にも 1 つ値を置いて「B にも届く entity」にしておく。
    // ref だけ送っても target entity が B 側に無ければ翻訳できない。
    eng_a.tie_async(parent, &q("tag"), 7);
    eng_a.tie_ref_async(child, &q("parent"), parent);
    eng_a.commit();  // v33: commit が WAL Commit marker も打つ
    eng_a.flush_writes();
    eng_a.oplog_sync().unwrap();
    // 0.8.0: publish の primary は `_sync_ops`。 background 転送待ちに
    // 依存すると publish_since が空振りするので明示的に回す (sleep より決定的)。
    eng_a.transfer_oplog_to_sync_ops();

    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());
    syncer_a.publish_since(Hlc::ZERO);
    syncer_b.pull_once(1);

    // #9 cross-peer ref: eid だけでなく **Ref himo の値そのもの** も B の eid 空間へ
    // 翻訳される (`resolve_remote_ref_value`)。 よって期待値は A の local ではなく、
    // B 側で parent に割り当てられた local。
    let child_local = local_of(&eng_b, child, "parent").expect("child must be mapped on B");
    let parent_local_on_b = local_of(&eng_b, parent, "parent")
        .map(|e| enchudb_oplog::eid_local(e) as u32);
    assert_eq!(
        eng_b.get(child_local, &q("parent")),
        parent_local_on_b,
        "cross-peer ref は B 側 local eid を指すこと",
    );

    cleanup(&pa);
    cleanup(&pb);
}

#[test]
fn commit_also_writes_wal_marker_under_v33() {
    // v33 で commit() が WAL Commit marker を打つ → publish_since が非ゼロを返す
    let pa = tmp("commit_a");
    let eng_a = make_peer(&pa, 1);

    let e = eng_a.entity_in(TABLE).unwrap();
    eng_a.tie_text_async(e, &q("name"), "Zed");
    eng_a.commit();  // v33: oplog_commit 相当も行うはず
    eng_a.flush_writes();
    eng_a.oplog_sync().unwrap();
    // 0.8.0: publish の primary は `_sync_ops`。 background 転送待ちに
    // 依存すると publish_since が空振りするので明示的に回す (sleep より決定的)。
    eng_a.transfer_oplog_to_sync_ops();

    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer = Syncer::new(eng_a.clone(), transport.clone());
    let published = syncer.publish_since(Hlc::ZERO);
    // Vocab + Tie + Commit >= 3
    assert!(published >= 2, "commit() should flush WAL records, got {}", published);

    cleanup(&pa);
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
    let e_b_local = eng_b.entity_in(TABLE).unwrap();
    eng_b.tie_text_async(e_b_local, &q("name"), "Zed");
    eng_b.oplog_commit();
    eng_b.flush_writes();
    eng_b.oplog_sync().unwrap();
    // 0.8.0: publish の primary は `_sync_ops`。 background 転送待ちに
    // 依存すると publish_since が空振りするので明示的に回す (sleep より決定的)。
    eng_b.transfer_oplog_to_sync_ops();

    // A が自分の eid に "Zed" を tie
    let e_a = eng_a.entity_in(TABLE).unwrap();
    eng_a.tie_text_async(e_a, &q("name"), "Zed");
    eng_a.oplog_commit();
    eng_a.flush_writes();
    eng_a.oplog_sync().unwrap();
    // 0.8.0: publish の primary は `_sync_ops`。 background 転送待ちに
    // 依存すると publish_since が空振りするので明示的に回す (sleep より決定的)。
    eng_a.transfer_oplog_to_sync_ops();

    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());
    syncer_a.publish_since(Hlc::ZERO);
    syncer_b.pull_once(1);

    // B は A の e_a entity に対して "Zed" を読めるべき
    assert_eq!(get_text_remote(&eng_b, e_a, "name"), Some(b"Zed".to_vec()));
    // 自分の e_b_local も "Zed" のまま
    assert_eq!(eng_b.get_text(e_b_local, &q("name")).map(|b| b.to_vec()), Some(b"Zed".to_vec()));

    cleanup(&pa);
    cleanup(&pb);
}
