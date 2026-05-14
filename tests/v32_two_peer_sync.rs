//! v32 Phase B: 2-peer 同期の E2E 統合テスト。
//!
//! peer A が tie した値が、pull 経由で peer B に届く。
//! その逆も。LWW で衝突解決。

#![cfg(feature = "v32")]

use std::sync::Arc;
use enchudb::{Engine, HimoType};
use enchudb_wal::Hlc;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport, WireRecord};
use enchudb_wal::wal::DecodedOp;

fn tmp(tag: &str) -> String {
    let p = format!("/tmp/enchudb-v32-2peer-{}-{}", tag, std::process::id());
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

/// Syncer には WAL が必須(B guard で panic するので)、全 peer を WAL 付きで作る。
/// `Engine::create_concurrent_with_wal` は Arc<Engine> を返すので define_himo が
/// 直に呼べない。一旦 plain create で define + flush → reopen で WAL 付き Arc を取る。
fn make_peer(path: &str, peer: u32) -> Arc<Engine> {
    {
        let mut eng = Engine::create_standalone(path).unwrap();
        eng.define_himo("val", HimoType::Value, 100);
        eng.define_himo("name", HimoType::Symbol, 0);
        eng.flush().unwrap();
    }
    let eng = Engine::open_concurrent_with_wal(path, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

fn make_peer_with_wal(path: &str, peer: u32) -> Arc<Engine> {
    make_peer(path, peer)
}

#[test]
fn peer_a_writes_peer_b_reads_via_pull() {
    let pa = tmp("a_to_b_a");
    let pb = tmp("a_to_b_b");

    let eng_a = make_peer(&pa, 1);
    let eng_b = make_peer(&pb, 2);
    let transport = Arc::new(InMemoryTransport::new());

    // peer A が tie する。transport に直接 publish するスタイルで Phase B 試験。
    let eid_a1 = eng_a.entity();
    // このテストでは peer A の WAL を使わず、直接 transport に publish する形で
    // apply 経路だけ検証する。
    transport.publish(1, vec![
        WireRecord::unsigned(Hlc { wall: 100, logical: 0, peer: 1 }, 1, DecodedOp::Tie { eid: eid_a1, himo_id: 0, value: 42 }),
    ]);

    let syncer_b = Syncer::new(eng_b.clone(), transport.clone() as Arc<dyn Transport>);
    let out = syncer_b.pull_once(1);
    assert_eq!(out.applied, 1, "peer B should apply peer A's tie");

    // peer B から同じ eid_a1 で value が読める
    assert_eq!(eng_b.get(eid_a1, "val"), Some(42));

    cleanup(&pa);
    cleanup(&pb);
}

#[test]
fn bidirectional_sync_no_conflict() {
    let pa = tmp("bidir_a");
    let pb = tmp("bidir_b");

    let eng_a = make_peer(&pa, 1);
    let eng_b = make_peer(&pb, 2);
    let transport = Arc::new(InMemoryTransport::new());

    // A と B がそれぞれ別 eid を tie
    let eid_a = eng_a.entity();
    let eid_b = eng_b.entity();

    transport.publish(1, vec![WireRecord::unsigned(Hlc { wall: 100, logical: 0, peer: 1 }, 1, DecodedOp::Tie { eid: eid_a, himo_id: 0, value: 10 })]);
    transport.publish(2, vec![WireRecord::unsigned(Hlc { wall: 110, logical: 0, peer: 2 }, 2, DecodedOp::Tie { eid: eid_b, himo_id: 0, value: 20 })]);

    let syncer_a = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone() as Arc<dyn Transport>);

    syncer_a.pull_once(2);
    syncer_b.pull_once(1);

    // A は B の eid を見れる、B は A の eid を見れる
    assert_eq!(eng_a.get(eid_b, "val"), Some(20));
    assert_eq!(eng_b.get(eid_a, "val"), Some(10));

    cleanup(&pa);
    cleanup(&pb);
}

#[test]
fn lww_concurrent_write_to_same_cell() {
    // peer A と peer B が同じ eid に違う値を書いた場合、HLC が大きい方が勝つ。
    let pa = tmp("lww_a");
    let pb = tmp("lww_b");

    let eng_a = make_peer(&pa, 1);
    let eng_b = make_peer(&pb, 2);
    let transport = Arc::new(InMemoryTransport::new());

    // 共有 eid を peer A が作る想定(peer 1 の local=0)
    let shared_eid = enchudb_wal::make_eid(1, 0);

    // A が wall=100 で value=10 を書いた
    transport.publish(1, vec![WireRecord::unsigned(Hlc { wall: 100, logical: 0, peer: 1 }, 1, DecodedOp::Tie { eid: shared_eid, himo_id: 0, value: 10 })]);

    // B は wall=200 で value=99 を書いた(concurrent、実時刻では後)
    transport.publish(2, vec![WireRecord::unsigned(Hlc { wall: 200, logical: 0, peer: 2 }, 2, DecodedOp::Tie { eid: shared_eid, himo_id: 0, value: 99 })]);

    let syncer_a = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone() as Arc<dyn Transport>);

    // A は自分の書き込みを再 pull しない(自 peer は既に適用済み扱い)が、
    // このテストでは A も B も両方の record を pull してみる。
    // A が peer 2 の op を pull → wall=200 の B の値が勝つ
    syncer_a.pull_once(2);
    // A が peer 1 の op を pull(自分の書き込み、後から届く想定)→ wall=100 は skip
    let out = syncer_a.pull_once(1);
    assert_eq!(out.applied, 0, "older HLC should be rejected");

    // B 側も同様
    syncer_b.pull_once(1);
    syncer_b.pull_once(2);

    // 両 peer とも value=99 を見える
    assert_eq!(eng_a.get(shared_eid, "val"), Some(99));
    assert_eq!(eng_b.get(shared_eid, "val"), Some(99));

    cleanup(&pa);
    cleanup(&pb);
}

#[test]
fn hlc_tie_broken_by_peer_id() {
    let pa = tmp("hlc_tie_a");
    let eng_a = make_peer(&pa, 1);
    let transport = Arc::new(InMemoryTransport::new());

    let shared_eid = enchudb_wal::make_eid(9, 0);

    // 同じ wall/logical、peer_id だけ違う → 大きい peer が勝つ
    transport.publish(5, vec![WireRecord::unsigned(Hlc { wall: 100, logical: 0, peer: 5 }, 5, DecodedOp::Tie { eid: shared_eid, himo_id: 0, value: 55 })]);
    transport.publish(7, vec![WireRecord::unsigned(Hlc { wall: 100, logical: 0, peer: 7 }, 7, DecodedOp::Tie { eid: shared_eid, himo_id: 0, value: 77 })]);

    let syncer = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);
    syncer.pull_once(5);
    syncer.pull_once(7);
    // peer 7 が勝つ
    assert_eq!(eng_a.get(shared_eid, "val"), Some(77));

    // 逆順でも同じ結果
    let pb = tmp("hlc_tie_b");
    let eng_b = make_peer(&pb, 2);
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone() as Arc<dyn Transport>);
    syncer_b.pull_once(7);
    syncer_b.pull_once(5);
    assert_eq!(eng_b.get(shared_eid, "val"), Some(77));

    cleanup(&pa);
    cleanup(&pb);
}

#[test]
fn e2e_write_publish_pull() {
    // tie_async → WAL → Syncer.publish_since → transport に入る → peer B が pull
    let pa = tmp("e2e_a");
    let pb = tmp("e2e_b");

    let eng_a = make_peer_with_wal(&pa, 1);
    let eng_b = make_peer(&pb, 2);
    let transport = Arc::new(InMemoryTransport::new());

    let syncer_a = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone() as Arc<dyn Transport>);

    // peer A が tie_async する(consumer thread 経由で本体に apply + WAL 残存)
    let eid = eng_a.entity();
    eng_a.tie_async(eid, "val", 42);
    eng_a.wal_commit();
    eng_a.flush_writes();
    eng_a.wal_sync().unwrap();

    // A が WAL 内容を transport に publish
    let published = syncer_a.publish_since(Hlc::ZERO);
    assert!(published >= 1, "should publish at least 1 op, got {}", published);

    // peer B が pull → 同じ値が読める
    let out = syncer_b.pull_once(1);
    assert!(out.applied >= 1, "peer B should apply peer A's tie");
    assert_eq!(eng_b.get(eid, "val"), Some(42));

    cleanup(&pa);
    cleanup(&pb);
}

#[test]
fn e2e_multiple_writes_published_and_pulled() {
    let pa = tmp("multi_a");
    let pb = tmp("multi_b");

    let eng_a = make_peer_with_wal(&pa, 1);
    let eng_b = make_peer(&pb, 2);
    let transport = Arc::new(InMemoryTransport::new());

    let syncer_a = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone() as Arc<dyn Transport>);

    // peer A が 10 件 tie_async
    let eids: Vec<u64> = (0..10).map(|i| {
        let eid = eng_a.entity();
        eng_a.tie_async(eid, "val", i as u32);
        eid
    }).collect();
    eng_a.wal_commit();
    eng_a.flush_writes();
    eng_a.wal_sync().unwrap();

    // publish → pull
    let published = syncer_a.publish_since(Hlc::ZERO);
    assert!(published >= 10);

    let out = syncer_b.pull_once(1);
    assert!(out.applied >= 10);

    for (i, &eid) in eids.iter().enumerate() {
        assert_eq!(eng_b.get(eid, "val"), Some(i as u32));
    }

    cleanup(&pa);
    cleanup(&pb);
}

#[test]
fn peer_id_persists_across_reopen() {
    // set_peer_id したあと DB を close → 再 open で peer_id が保たれる。
    let path = tmp("peer_persist");
    {
        let mut eng = Engine::create_standalone(&path).unwrap();
        eng.define_himo("val", HimoType::Value, 10);
        eng.set_peer_id(42);
        eng.flush().unwrap();
    }
    let mut eng = Engine::open_standalone(&path).unwrap();
    assert_eq!(eng.peer_id(), 42);

    // entity() が peer=42 を合成した eid を返す
    let e = eng.entity();
    assert_eq!(enchudb_wal::eid_peer(e), 42);

    cleanup(&path);
}

#[test]
fn signed_wal_verifies_between_peers() {
    // peer A が ed25519 鍵で署名 → peer B が pubkey 登録して verify を通す。
    use enchudb_wal::keys::Keypair;

    let pa = tmp("signed_a");
    let pb = tmp("signed_b");

    let eng_a = make_peer_with_wal(&pa, 1);
    let eng_b = make_peer(&pb, 2);

    // peer A に鍵ペアを与える
    let kp_a = Arc::new(Keypair::generate());
    let pk_a = kp_a.public_bytes();
    eng_a.set_keypair(Some(kp_a.clone()));

    // peer B は A の pubkey を bootstrap で受け取って登録
    eng_b.pubkeys().force_register(1, &pk_a);

    let transport = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone() as Arc<dyn Transport>);
    syncer_b.set_require_signature(true);

    let eid = eng_a.entity();
    eng_a.tie_async(eid, "val", 123);
    eng_a.wal_commit();
    eng_a.flush_writes();
    eng_a.wal_sync().unwrap();
    syncer_a.publish_since(Hlc::ZERO);

    let out = syncer_b.pull_once(1);
    assert!(out.applied >= 1, "signed op should be applied, got {:?}", out);
    assert_eq!(out.rejected_signature, 0);
    assert_eq!(eng_b.get(eid, "val"), Some(123));

    cleanup(&pa);
    cleanup(&pb);
}

#[test]
fn unsigned_rejected_when_require_signature() {
    let pa = tmp("unsigned_a");
    let eng_a = make_peer(&pa, 1);
    let transport = Arc::new(InMemoryTransport::new());

    // peer 2 が未署名で publish
    transport.publish(2, vec![
        WireRecord::unsigned(Hlc { wall: 100, logical: 0, peer: 2 }, 2,
            DecodedOp::Tie { eid: enchudb_wal::make_eid(2, 0), himo_id: 0, value: 99 }),
    ]);

    let syncer = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);
    syncer.set_require_signature(true);
    let out = syncer.pull_once(2);
    assert_eq!(out.applied, 0);
    assert_eq!(out.rejected_signature, 1);

    cleanup(&pa);
}

#[test]
fn tampered_signature_rejected() {
    use enchudb_wal::keys::Keypair;

    let pa = tmp("tamper_a");
    let pb = tmp("tamper_b");

    let eng_a = make_peer_with_wal(&pa, 1);
    let eng_b = make_peer(&pb, 2);

    let kp_a = Arc::new(Keypair::generate());
    eng_a.set_keypair(Some(kp_a.clone()));
    eng_b.pubkeys().force_register(1, &kp_a.public_bytes());

    let transport = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone() as Arc<dyn Transport>);
    syncer_b.set_require_signature(true);

    let eid = eng_a.entity();
    eng_a.tie_async(eid, "val", 7);
    eng_a.wal_commit();
    eng_a.flush_writes();
    eng_a.wal_sync().unwrap();
    syncer_a.publish_since(Hlc::ZERO);

    // transport からレコードを取り、署名を 1bit 反転させて入れ直す
    let mut records = transport.pull(1, Hlc::ZERO);
    assert!(!records.is_empty());
    records[0].signature[0] ^= 0x01;
    let t2 = Arc::new(InMemoryTransport::new());
    t2.publish(1, records);
    let syncer_b2 = Syncer::new(eng_b.clone(), t2.clone() as Arc<dyn Transport>);
    syncer_b2.set_require_signature(true);
    let out = syncer_b2.pull_once(1);
    assert_eq!(out.applied, 0);
    assert_eq!(out.rejected_signature, 1);

    cleanup(&pa);
    cleanup(&pb);
}

#[test]
fn wrong_peer_pubkey_rejected() {
    use enchudb_wal::keys::Keypair;

    let pa = tmp("wrong_a");
    let pb = tmp("wrong_b");

    let eng_a = make_peer_with_wal(&pa, 1);
    let eng_b = make_peer(&pb, 2);

    // peer A の真の鍵
    let kp_a = Arc::new(Keypair::generate());
    eng_a.set_keypair(Some(kp_a.clone()));

    // peer B は「peer 1 の pubkey はこれ」と別の pubkey を登録(なりすまし防止)
    let kp_fake = Keypair::generate();
    eng_b.pubkeys().force_register(1, &kp_fake.public_bytes());

    let transport = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone() as Arc<dyn Transport>);
    syncer_b.set_require_signature(true);

    let eid = eng_a.entity();
    eng_a.tie_async(eid, "val", 42);
    eng_a.wal_commit();
    eng_a.flush_writes();
    eng_a.wal_sync().unwrap();
    syncer_a.publish_since(Hlc::ZERO);

    let out = syncer_b.pull_once(1);
    // fake pubkey では verify 失敗
    assert_eq!(out.applied, 0);
    assert_eq!(out.rejected_signature, 1);

    cleanup(&pa);
    cleanup(&pb);
}

#[test]
fn acl_blocks_non_writer_peer() {
    let pa = tmp("acl_block_a");
    let eng_a = make_peer(&pa, 1);
    let transport = Arc::new(InMemoryTransport::new());

    // peer 1 だけ許可、peer 99 は未許可
    eng_a.acl().add_writer(1);
    eng_a.acl().add_writer(2);

    // peer 99 から op が届く
    transport.publish(99, vec![
        WireRecord::unsigned(Hlc { wall: 100, logical: 0, peer: 99 }, 99,
            DecodedOp::Tie { eid: enchudb_wal::make_eid(99, 0), himo_id: 0, value: 42 }),
    ]);

    let syncer = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);
    let out = syncer.pull_once(99);
    assert_eq!(out.applied, 0);
    assert_eq!(out.rejected_acl, 1);

    // peer 1 から op が来れば apply される
    transport.publish(1, vec![
        WireRecord::unsigned(Hlc { wall: 200, logical: 0, peer: 1 }, 1,
            DecodedOp::Tie { eid: enchudb_wal::make_eid(1, 0), himo_id: 0, value: 77 }),
    ]);
    let out2 = syncer.pull_once(1);
    assert_eq!(out2.applied, 1);
    assert_eq!(out2.rejected_acl, 0);

    cleanup(&pa);
}

#[test]
fn acl_empty_is_permissive() {
    // ACL が空のままなら全員許可(bootstrap/single peer 時の挙動)
    let pa = tmp("acl_empty_a");
    let eng_a = make_peer(&pa, 1);
    let transport = Arc::new(InMemoryTransport::new());

    transport.publish(42, vec![
        WireRecord::unsigned(Hlc { wall: 100, logical: 0, peer: 42 }, 42,
            DecodedOp::Tie { eid: enchudb_wal::make_eid(42, 0), himo_id: 0, value: 9 }),
    ]);

    let syncer = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);
    let out = syncer.pull_once(42);
    assert_eq!(out.applied, 1);
    assert_eq!(out.rejected_acl, 0);

    cleanup(&pa);
}

#[test]
fn delete_remote_removes_all_himos() {
    let pa = tmp("del_a");
    let eng_a = make_peer(&pa, 1);
    let transport = Arc::new(InMemoryTransport::new());

    let eid = enchudb_wal::make_eid(2, 0);

    // peer 2 が 3 つの himo に値を書いてから delete
    transport.publish(2, vec![
        WireRecord::unsigned(Hlc { wall: 100, logical: 0, peer: 2 }, 2, DecodedOp::Tie { eid, himo_id: 0, value: 1 }),
        WireRecord::unsigned(Hlc { wall: 101, logical: 0, peer: 2 }, 2, DecodedOp::Tie { eid, himo_id: 1, value: 2 }),
        WireRecord::unsigned(Hlc { wall: 200, logical: 0, peer: 2 }, 2, DecodedOp::Delete { eid }),
    ]);

    let syncer = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);
    let out = syncer.pull_once(2);
    assert_eq!(out.applied, 3);

    // delete 後は全 himo で None
    assert_eq!(eng_a.get(eid, "val"), None);

    cleanup(&pa);
}
