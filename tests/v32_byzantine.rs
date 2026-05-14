//! v32 Byzantine tolerance テスト — 悪意/故障 peer 下での健全 peer の挙動検証。
//!
//! Phase C の基本テスト (v32_two_peer_sync.rs) で既にカバーしてるもの:
//! - unsigned op は require_signature で弾かれる
//! - 署名改竄 (1bit 反転) は弾かれる
//! - 異なる pubkey による impersonation は弾かれる
//! - ACL 外の peer は弾かれる
//!
//! このファイルが追加で検証:
//! - **Replay attack**: 同じ signed op の重複送信が LWW で idempotent か
//! - **Future HLC attack**: HLC=u64::MAX を送って以降の write をブロックする攻撃 (既知の制約を文書化)
//! - **Mixed batch**: 1 バッチ内で signed + unsigned 混在、signed のみ apply
//! - **Stuck peer**: 黙り peer は他の peer の sync を阻害しない
//! - **Keypair rotation**: 同じ peer_id が途中で別鍵に切り替え (TOFU 下で拒否される)

#![cfg(feature = "v32")]

use std::sync::Arc;
use enchudb::{Engine, HimoType};
use enchudb_wal::Hlc;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport, WireRecord};
use enchudb_wal::wal::DecodedOp;
use enchudb_wal::keys::Keypair;

fn tmp(tag: &str) -> String {
    let p = format!("/tmp/enchudb-v32-byz-{}-{}", tag, std::process::id());
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
    {
        let mut eng = Engine::create_standalone(path).unwrap();
        eng.define_himo("val", HimoType::Number, 100);
        eng.flush().unwrap();
    }
    let eng = Engine::open_concurrent_with_wal(path, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

fn make_peer_with_wal(path: &str, peer: u32) -> Arc<Engine> {
    let mut eng = Engine::create_standalone(path).unwrap();
    eng.define_himo("val", HimoType::Number, 100);
    eng.flush().unwrap();
    drop(eng);
    let eng = Engine::open_concurrent_with_wal(path, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

// ─────────────────────────────────────────────────────────────
// Replay attack
// ─────────────────────────────────────────────────────────────

#[test]
fn replay_of_signed_op_is_idempotent() {
    // 悪意 peer が同じ signed op を何度も送りつけても LWW で 1 回しか apply されない
    let pa = tmp("replay_a");
    let pb = tmp("replay_b");
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
    eng_a.tie_async(eid, "val", 99);
    eng_a.wal_commit();
    eng_a.flush_writes();
    eng_a.wal_sync().unwrap();
    syncer_a.publish_since(Hlc::ZERO);

    // 1 回目 apply
    let out1 = syncer_b.pull_once(1);
    assert!(out1.applied >= 1, "first apply");
    assert_eq!(eng_b.get(eid, "val"), Some(99));

    // 同じ record を 5 回 replay
    let records = transport.pull(1, Hlc::ZERO);
    assert!(!records.is_empty());
    let replay_transport = Arc::new(InMemoryTransport::new());
    for _ in 0..5 {
        replay_transport.publish(1, records.clone());
    }
    let syncer_b2 = Syncer::new(eng_b.clone(), replay_transport.clone() as Arc<dyn Transport>);
    syncer_b2.set_require_signature(true);
    let out2 = syncer_b2.pull_once(1);

    // 受信は 5 倍だが、LWW で apply は 0 (既存 HLC 以下なので skip)
    assert!(out2.received >= 5);
    assert_eq!(out2.applied, 0, "replays should be idempotent (LWW skip)");

    cleanup(&pa);
    cleanup(&pb);
}

// ─────────────────────────────────────────────────────────────
// Future HLC attack (既知の制約、ドキュメント目的)
// ─────────────────────────────────────────────────────────────

#[test]
fn future_hlc_attack_dominates_lww_known_limitation() {
    // 悪意 peer が HLC.wall=u64::MAX を送ると以降 peer 2 から来る正当な write が全部 LWW で棄却される。
    // これは LWW の根本的制約。防御するには HLC の reasonable upper bound check が要る (Phase D+)。
    // このテストは "この攻撃が効く" ことを記録し、将来 defender を入れた時の regression guard にする。
    // (署名要求は無いシナリオ。署名有りでも、正規 peer の鍵が漏れた場合に同じ攻撃が成立する。)
    let pb = tmp("future_b");
    let eng_b = make_peer(&pb, 2);
    let transport = Arc::new(InMemoryTransport::new());

    // 悪意 peer 1 が HLC=MAX で偽書き込み (署名無しで OK — signature 要求無しのシナリオ)
    let eid = enchudb_wal::make_eid(1, 7);
    let himo_id = eng_b.himo_id("val").unwrap() as u16;
    transport.publish(1, vec![
        WireRecord::unsigned(
            Hlc { wall: u64::MAX, logical: 0, peer: 1 }, 1,
            DecodedOp::Tie { eid, himo_id, value: 999 },
        ),
    ]);

    let syncer_b = Syncer::new(eng_b.clone(), transport.clone() as Arc<dyn Transport>);
    // require_signature 無し (このテストは LWW の生挙動を検証)
    let out = syncer_b.pull_once(1);
    assert_eq!(out.applied, 1);
    assert_eq!(eng_b.get(eid, "val"), Some(999));

    // 正当な後続 write (wall=100)。同じ syncer だと cursor が MAX に進んでて
    // since フィルタで received=0 になるので、LWW skip の挙動を見るために fresh syncer を使う。
    transport.publish(1, vec![
        WireRecord::unsigned(
            Hlc { wall: 100, logical: 0, peer: 1 }, 1,
            DecodedOp::Tie { eid, himo_id, value: 1 },
        ),
    ]);
    let fresh_syncer = Syncer::new(eng_b.clone(), transport.clone() as Arc<dyn Transport>);
    let out2 = fresh_syncer.pull_once(1);
    assert!(out2.received >= 2, "fresh syncer should see both records");
    assert_eq!(out2.applied, 0, "honest write skipped (LWW: wall=100 < MAX) + future-HLC record also already applied");
    assert_eq!(eng_b.get(eid, "val"), Some(999), "malicious value stuck");

    cleanup(&pb);
}

// ─────────────────────────────────────────────────────────────
// Mixed batch: signed + unsigned 混在
// ─────────────────────────────────────────────────────────────

#[test]
fn mixed_batch_signed_kept_unsigned_dropped() {
    let pa = tmp("mixed_a");
    let pb = tmp("mixed_b");
    let eng_a = make_peer_with_wal(&pa, 1);
    let eng_b = make_peer(&pb, 2);

    let kp_a = Arc::new(Keypair::generate());
    eng_a.set_keypair(Some(kp_a.clone()));
    eng_b.pubkeys().force_register(1, &kp_a.public_bytes());

    let transport = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone() as Arc<dyn Transport>);
    syncer_b.set_require_signature(true);

    // peer A が署名付きで 1 件
    let eid_good = eng_a.entity();
    eng_a.tie_async(eid_good, "val", 42);
    eng_a.wal_commit();
    eng_a.flush_writes();
    eng_a.wal_sync().unwrap();
    syncer_a.publish_since(Hlc::ZERO);

    // 同じ transport に悪意の unsigned record を混ぜ込む
    let himo_id = eng_b.himo_id("val").unwrap() as u16;
    let eid_bad = enchudb_wal::make_eid(1, 999);
    transport.publish(1, vec![
        WireRecord::unsigned(
            Hlc { wall: 9999, logical: 0, peer: 1 }, 1,
            DecodedOp::Tie { eid: eid_bad, himo_id, value: 666 },
        ),
    ]);

    let out = syncer_b.pull_once(1);
    assert!(out.applied >= 1, "signed should land");
    assert!(out.rejected_signature >= 1, "unsigned should be rejected");
    assert_eq!(eng_b.get(eid_good, "val"), Some(42));
    assert_eq!(eng_b.get(eid_bad, "val"), None, "unsigned op must not land");

    cleanup(&pa);
    cleanup(&pb);
}

// ─────────────────────────────────────────────────────────────
// Stuck peer: 黙り peer は他 peer の sync を阻害しない
// ─────────────────────────────────────────────────────────────

#[test]
fn stuck_peer_does_not_block_sync_from_others() {
    let pa = tmp("stuck_a");
    let pc = tmp("stuck_c");
    let eng_a = make_peer(&pa, 1);
    let eng_c = make_peer(&pc, 3);  // peer B (id=2) は存在するが publish しない "stuck" 役

    let transport = Arc::new(InMemoryTransport::new());

    // peer A が publish
    let eid = eng_a.entity();
    transport.publish(1, vec![
        WireRecord::unsigned(Hlc { wall: 100, logical: 0, peer: 1 }, 1,
            DecodedOp::Tie { eid, himo_id: 0, value: 123 }),
    ]);
    // peer B (id=2) は黙り、transport に何も入れない

    // peer C は peer A と peer B 両方から pull 試行
    let syncer_c = Syncer::new(eng_c.clone(), transport.clone() as Arc<dyn Transport>);
    let out_a = syncer_c.pull_once(1);
    let out_b = syncer_c.pull_once(2);  // 空 pull

    assert_eq!(out_a.applied, 1);
    assert_eq!(out_b.received, 0);
    assert_eq!(eng_c.get(eid, "val"), Some(123));

    cleanup(&pa);
    cleanup(&pc);
}

// ─────────────────────────────────────────────────────────────
// Keypair rotation: 同じ peer_id を途中で別鍵に → TOFU で拒否
// ─────────────────────────────────────────────────────────────

#[test]
fn keypair_rotation_rejected_under_tofu() {
    let pa = tmp("rotate_a");
    let pb = tmp("rotate_b");
    let eng_a = make_peer_with_wal(&pa, 1);
    let eng_b = make_peer(&pb, 2);

    // 初期鍵で署名した op を 1 件受け入れる
    let kp1 = Arc::new(Keypair::generate());
    eng_a.set_keypair(Some(kp1.clone()));
    eng_b.pubkeys().force_register(1, &kp1.public_bytes());

    let transport = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone() as Arc<dyn Transport>);
    syncer_b.set_require_signature(true);

    let eid1 = eng_a.entity();
    eng_a.tie_async(eid1, "val", 1);
    eng_a.wal_commit();
    eng_a.flush_writes();
    eng_a.wal_sync().unwrap();
    syncer_a.publish_since(Hlc::ZERO);
    let out1 = syncer_b.pull_once(1);
    assert!(out1.applied >= 1);

    // peer A が鍵を "rotate" (別鍵に切り替え、通常の運用では起きない異常)
    let kp2 = Arc::new(Keypair::generate());
    eng_a.set_keypair(Some(kp2.clone()));

    let eid2 = eng_a.entity();
    eng_a.tie_async(eid2, "val", 2);
    eng_a.wal_commit();
    eng_a.flush_writes();
    eng_a.wal_sync().unwrap();
    syncer_a.publish_since(Hlc::ZERO);

    // peer B は TOFU で kp1 の pubkey しか持ってない → kp2 署名は verify 失敗 → reject
    let out2 = syncer_b.pull_once(1);
    assert_eq!(out2.applied, 0);
    assert!(out2.rejected_signature >= 1, "rotated keypair must be rejected under TOFU");
    assert_eq!(eng_b.get(eid2, "val"), None, "rotated-key op must not land");

    cleanup(&pa);
    cleanup(&pb);
}
