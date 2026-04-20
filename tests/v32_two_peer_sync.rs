//! v32 Phase B: 2-peer 同期の E2E 統合テスト。
//!
//! peer A が tie した値が、pull 経由で peer B に届く。
//! その逆も。LWW で衝突解決。

#![cfg(feature = "v32")]

use std::sync::Arc;
use enchudb::{Engine, HimoType, Hlc};
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport, WireRecord};
use enchudb::wal::DecodedOp;

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

fn make_peer(path: &str, peer: u32) -> Arc<Engine> {
    let mut eng = Engine::create(path).unwrap();
    eng.define_himo("val", HimoType::Value, 100);
    eng.define_himo("name", HimoType::Symbol, 0);
    let eng = Arc::new(eng);
    eng.set_peer_id(peer);
    eng
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
        WireRecord {
            hlc: Hlc { wall: 100, logical: 0, peer: 1 },
            author_peer: 1,
            op: DecodedOp::Tie { eid: eid_a1, himo_id: 0, value: 42 },
        },
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

    transport.publish(1, vec![WireRecord {
        hlc: Hlc { wall: 100, logical: 0, peer: 1 },
        author_peer: 1,
        op: DecodedOp::Tie { eid: eid_a, himo_id: 0, value: 10 },
    }]);
    transport.publish(2, vec![WireRecord {
        hlc: Hlc { wall: 110, logical: 0, peer: 2 },
        author_peer: 2,
        op: DecodedOp::Tie { eid: eid_b, himo_id: 0, value: 20 },
    }]);

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
    let shared_eid = enchudb::make_eid(1, 0);

    // A が wall=100 で value=10 を書いた
    transport.publish(1, vec![WireRecord {
        hlc: Hlc { wall: 100, logical: 0, peer: 1 },
        author_peer: 1,
        op: DecodedOp::Tie { eid: shared_eid, himo_id: 0, value: 10 },
    }]);

    // B は wall=200 で value=99 を書いた(concurrent、実時刻では後)
    transport.publish(2, vec![WireRecord {
        hlc: Hlc { wall: 200, logical: 0, peer: 2 },
        author_peer: 2,
        op: DecodedOp::Tie { eid: shared_eid, himo_id: 0, value: 99 },
    }]);

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

    let shared_eid = enchudb::make_eid(9, 0);

    // 同じ wall/logical、peer_id だけ違う → 大きい peer が勝つ
    transport.publish(5, vec![WireRecord {
        hlc: Hlc { wall: 100, logical: 0, peer: 5 },
        author_peer: 5,
        op: DecodedOp::Tie { eid: shared_eid, himo_id: 0, value: 55 },
    }]);
    transport.publish(7, vec![WireRecord {
        hlc: Hlc { wall: 100, logical: 0, peer: 7 },
        author_peer: 7,
        op: DecodedOp::Tie { eid: shared_eid, himo_id: 0, value: 77 },
    }]);

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
fn delete_remote_removes_all_himos() {
    let pa = tmp("del_a");
    let eng_a = make_peer(&pa, 1);
    let transport = Arc::new(InMemoryTransport::new());

    let eid = enchudb::make_eid(2, 0);

    // peer 2 が 3 つの himo に値を書いてから delete
    transport.publish(2, vec![
        WireRecord {
            hlc: Hlc { wall: 100, logical: 0, peer: 2 },
            author_peer: 2,
            op: DecodedOp::Tie { eid, himo_id: 0, value: 1 },
        },
        WireRecord {
            hlc: Hlc { wall: 101, logical: 0, peer: 2 },
            author_peer: 2,
            op: DecodedOp::Tie { eid, himo_id: 1, value: 2 },
        },
        WireRecord {
            hlc: Hlc { wall: 200, logical: 0, peer: 2 },
            author_peer: 2,
            op: DecodedOp::Delete { eid },
        },
    ]);

    let syncer = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);
    let out = syncer.pull_once(2);
    assert_eq!(out.applied, 3);

    // delete 後は全 himo で None
    assert_eq!(eng_a.get(eid, "val"), None);

    cleanup(&pa);
}
