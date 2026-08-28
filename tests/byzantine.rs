//! Byzantine tolerance テスト — 悪意/故障 peer 下での健全 peer の挙動検証。
//!
//! Phase C の基本テスト (two_peer_sync.rs) で既にカバーしてるもの:
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

use std::sync::Arc;
use enchudb::{Engine, ValueType};
use enchudb_oplog::Hlc;
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport, WireRecord};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::keys::Keypair;

fn tmp(tag: &str) -> String {
    let p = format!("/tmp/enchudb-byz-{}-{}", tag, std::process::id());
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
/// apply が丸ごと skip される (#9)。 `enable_sync_tables()` 自体は 0.8.0 以降 Syncer
/// attach の必須条件。
const TABLE: &str = "t";

fn define_schema(eng: &mut Engine) {
    eng.define_table(TABLE, 1000).unwrap();
    eng.define_himo_in(TABLE, "val", ValueType::Number, 100).unwrap();
    eng.enable_sync_tables().unwrap();
}

fn make_peer(path: &str, peer: u32) -> Arc<Engine> {
    {
        let mut eng = Engine::create_standalone(path).unwrap();
        define_schema(&mut eng);
        eng.flush().unwrap();
    }
    let eng = Engine::open_concurrent_with_oplog(path, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

fn make_peer_with_oplog(path: &str, peer: u32) -> Arc<Engine> {
    let mut eng = Engine::create_standalone(path).unwrap();
    define_schema(&mut eng);
    eng.flush().unwrap();
    drop(eng);
    let eng = Engine::open_concurrent_with_oplog(path, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

/// `TABLE` 内 himo の qualified name。
fn q(himo: &str) -> String {
    format!("{}.{}", TABLE, himo)
}

/// 受信側で foreign eid を自分の eid 空間へ翻訳して読む (#9)。
/// 翻訳が無い (= 一度も apply されていない) 場合は None。
fn get_remote(eng: &Engine, foreign_eid: u64, himo: &str) -> Option<u32> {
    let hid = eng.himo_id(&q(himo)).unwrap() as u16;
    let local = eng.resolve_remote_eid(foreign_eid, hid)?;
    eng.get(local, &q(himo))
}

// ─────────────────────────────────────────────────────────────
// Replay attack
// ─────────────────────────────────────────────────────────────

#[test]
fn replay_of_signed_op_is_idempotent() {
    // 悪意 peer が同じ signed op を何度も送りつけても LWW で 1 回しか apply されない
    let pa = tmp("replay_a");
    let pb = tmp("replay_b");
    let eng_a = make_peer_with_oplog(&pa, 1);
    let eng_b = make_peer(&pb, 2);

    let kp_a = Arc::new(Keypair::generate());
    eng_a.set_keypair(Some(kp_a.clone()));
    eng_b.pubkeys().force_register(1, &kp_a.public_bytes());

    let transport = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone() as Arc<dyn Transport>);
    syncer_b.set_require_signature(true);

    let eid = eng_a.entity_in(TABLE).unwrap();
    eng_a.tie_async(eid, &q("val"), 99);
    eng_a.oplog_commit();
    eng_a.flush_writes();
    eng_a.oplog_sync().unwrap();
    // 0.8.0: publish の primary path は `_sync_ops`。 oplog からの転送は
    // background thread 任せだと publish_since が空振りするので明示的に回す
    // (sleep より決定的)。
    eng_a.transfer_oplog_to_sync_ops();
    syncer_a.publish_since(Hlc::ZERO);

    // 1 回目 apply
    let out1 = syncer_b.pull_once(1);
    assert!(out1.applied >= 1, "first apply");
    assert_eq!(get_remote(&eng_b, eid, "val"), Some(99));

    // 同じ record を 5 回 replay。
    //
    // transport 経由で 5 回 publish しても `InMemoryTransport::publish` が
    // (peer, hlc) で dedupe する (transport.rs、 gossip の重複受信対策) ので
    // log には 1 件しか積まれず、 LWW 側の idempotency を素通ししてしまう。
    // ここで見たいのは **apply 経路の LWW skip** なので、 transport を挟まず
    // 同じ batch を直接 5 回 apply_records に食わせる。
    let records = transport.pull(1, Hlc::ZERO);
    assert!(!records.is_empty());
    let syncer_b2 = Syncer::new(eng_b.clone(), transport.clone() as Arc<dyn Transport>);
    syncer_b2.set_require_signature(true);
    let mut total_received = 0usize;
    let mut total_applied = 0usize;
    for _ in 0..5 {
        let out = syncer_b2.apply_records(&records);
        total_received += out.received;
        total_applied += out.applied;
    }

    // 受信は 5 バッチぶん、 だが apply は 0 (既存 HLC 以下なので全部 skip)
    assert!(total_received >= 5, "5 batches should all be received, got {total_received}");
    assert_eq!(total_applied, 0, "replays should be idempotent (LWW skip)");

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
    let eid = enchudb_oplog::make_eid(1, 7);
    let himo_id = eng_b.himo_id(&q("val")).unwrap() as u16;
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
    assert_eq!(get_remote(&eng_b, eid, "val"), Some(999));

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
    assert_eq!(get_remote(&eng_b, eid, "val"), Some(999), "malicious value stuck");

    cleanup(&pb);
}

// ─────────────────────────────────────────────────────────────
// Mixed batch: signed + unsigned 混在
// ─────────────────────────────────────────────────────────────

#[test]
fn mixed_batch_signed_kept_unsigned_dropped() {
    let pa = tmp("mixed_a");
    let pb = tmp("mixed_b");
    let eng_a = make_peer_with_oplog(&pa, 1);
    let eng_b = make_peer(&pb, 2);

    let kp_a = Arc::new(Keypair::generate());
    eng_a.set_keypair(Some(kp_a.clone()));
    eng_b.pubkeys().force_register(1, &kp_a.public_bytes());

    let transport = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone() as Arc<dyn Transport>);
    syncer_b.set_require_signature(true);

    // peer A が署名付きで 1 件
    let eid_good = eng_a.entity_in(TABLE).unwrap();
    eng_a.tie_async(eid_good, &q("val"), 42);
    eng_a.oplog_commit();
    eng_a.flush_writes();
    eng_a.oplog_sync().unwrap();
    // 0.8.0: publish の primary path は `_sync_ops`。 oplog からの転送は
    // background thread 任せだと publish_since が空振りするので明示的に回す
    // (sleep より決定的)。
    eng_a.transfer_oplog_to_sync_ops();
    syncer_a.publish_since(Hlc::ZERO);

    // 同じ transport に悪意の unsigned record を混ぜ込む
    let himo_id = eng_b.himo_id(&q("val")).unwrap() as u16;
    let eid_bad = enchudb_oplog::make_eid(1, 999);
    transport.publish(1, vec![
        WireRecord::unsigned(
            Hlc { wall: 9999, logical: 0, peer: 1 }, 1,
            DecodedOp::Tie { eid: eid_bad, himo_id, value: 666 },
        ),
    ]);

    let out = syncer_b.pull_once(1);
    assert!(out.applied >= 1, "signed should land");
    assert!(out.rejected_signature >= 1, "unsigned should be rejected");
    assert_eq!(get_remote(&eng_b, eid_good, "val"), Some(42));
    assert_eq!(get_remote(&eng_b, eid_bad, "val"), None, "unsigned op must not land");

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
    let eid = eng_a.entity_in(TABLE).unwrap();
    transport.publish(1, vec![
        WireRecord::unsigned(Hlc { wall: 100, logical: 0, peer: 1 }, 1,
            DecodedOp::Tie { eid, himo_id: eng_a.himo_id(&q("val")).unwrap() as u16, value: 123 }),
    ]);
    // peer B (id=2) は黙り、transport に何も入れない

    // peer C は peer A と peer B 両方から pull 試行
    let syncer_c = Syncer::new(eng_c.clone(), transport.clone() as Arc<dyn Transport>);
    let out_a = syncer_c.pull_once(1);
    let out_b = syncer_c.pull_once(2);  // 空 pull

    assert_eq!(out_a.applied, 1);
    assert_eq!(out_b.received, 0);
    assert_eq!(get_remote(&eng_c, eid, "val"), Some(123));

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
    let eng_a = make_peer_with_oplog(&pa, 1);
    let eng_b = make_peer(&pb, 2);

    // 初期鍵で署名した op を 1 件受け入れる
    let kp1 = Arc::new(Keypair::generate());
    eng_a.set_keypair(Some(kp1.clone()));
    eng_b.pubkeys().force_register(1, &kp1.public_bytes());

    let transport = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone() as Arc<dyn Transport>);
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone() as Arc<dyn Transport>);
    syncer_b.set_require_signature(true);

    let eid1 = eng_a.entity_in(TABLE).unwrap();
    eng_a.tie_async(eid1, &q("val"), 1);
    eng_a.oplog_commit();
    eng_a.flush_writes();
    eng_a.oplog_sync().unwrap();
    // 0.8.0: publish の primary path は `_sync_ops`。 oplog からの転送は
    // background thread 任せだと publish_since が空振りするので明示的に回す
    // (sleep より決定的)。
    eng_a.transfer_oplog_to_sync_ops();
    // request4 以降、 `publish_since` は transport の `known_peers()` が空なら
    // broadcast、 非空なら「自分以外の既知 peer」への per-peer 送信に切り替わる。
    // peer B は一度も publish しないので known_peers に載らず、 1 回目の publish で
    // peer A 自身が登録された時点から publish_since は宛先ゼロ = 黙って 0 件になる。
    // このテストは 2 回配信するので、 宛先を明示する for_peer 版を使う。
    assert_eq!(syncer_a.publish_since_for_peer(2, Hlc::ZERO), 1);
    let out1 = syncer_b.pull_once(1);
    assert!(out1.applied >= 1);

    // peer A が鍵を "rotate" (別鍵に切り替え、通常の運用では起きない異常)
    let kp2 = Arc::new(Keypair::generate());
    eng_a.set_keypair(Some(kp2.clone()));

    let eid2 = eng_a.entity_in(TABLE).unwrap();
    eng_a.tie_async(eid2, &q("val"), 2);
    eng_a.oplog_commit();
    eng_a.flush_writes();
    eng_a.oplog_sync().unwrap();
    eng_a.transfer_oplog_to_sync_ops();
    assert!(syncer_a.publish_since_for_peer(2, Hlc::ZERO) >= 2, "rotate 後の op も配信されること");

    // peer B は TOFU で kp1 の pubkey しか持ってない → kp2 署名は verify 失敗 → reject
    let out2 = syncer_b.pull_once(1);
    assert_eq!(out2.applied, 0);
    assert!(out2.rejected_signature >= 1, "rotated keypair must be rejected under TOFU");
    assert_eq!(get_remote(&eng_b, eid2, "val"), None, "rotated-key op must not land");

    cleanup(&pa);
    cleanup(&pb);
}
