//! #149 — pull-as-ack: publish/pull を回すだけで `_sync_ops` ring が回転すること。
//!
//! relay/gateway 経路には明示 ack が無く、 従来は誰も `ack_sync` を呼ばない →
//! `sync_watermark()` = 0 固定 → reclaim ゼロ → ring 満杯で bridge が stall し、
//! **以後の変更が一切配布されなくなる** (author の生涯 op 数 ≒ ring 容量)。
//!
//! fix: puller の pull cursor (durable barrier 通過後の確定値) を transport 経由で
//! author に還流し、 `publish_since` 冒頭で `ack_sync_up_to_hlc` に写す。
//! reclaim は **ring 使用率 50% 超の時だけ** — 履歴は容量が許す限り保持し、
//! まだ pull していない follower の差分追いつきを最大化する (eager reclaim は
//! 遅参 1 round の follower を即 bootstrap 送りにする)。
//!
//! 検証:
//! 1. `no_reclaim_below_pressure_late_joiner_gets_full_history` — 圧力が無ければ
//!    reclaim しない (floor 未広告)。 ack 自体は消化されて watermark は前進する。
//!    まだ一度も pull してない peer が後から全履歴を差分で取れる。
//! 2. `reclaims_under_pressure_without_manual_ack` — 使用率 50% 超で publish
//!    すると手動 ack ゼロで reclaim + floor 記録。 消化済み follower は
//!    truncation されない (#191 と噛み合うこと)。
//! 3. `slowest_puller_limits_reclaim` — 最遅 puller の cursor が watermark を
//!    pin し、 未消化 record は圧力下でも reclaim されない (過剰 reclaim なし)。
//! 4. `ring_overflow_survives_with_pull_loop` — E2E: ring 容量を超える生涯 op を
//!    publish/pull loop で走り切り、 follower が全件受信する (旧: ring 満杯で
//!    bridge stall → 以後 silent 未配布)。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue149-{}-{}-{}",
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
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

/// capacity と notes table の reserve を指定して engine を作る。
/// `_sync_ops` ring は enable_sync_tables 時点の remaining/2 なので、
/// capacity を絞ると ring も比例して小さくなる (圧力・溢れを作る用)。
/// capacity 8_192 + notes 6_500 で ring ≈ 850 row。
fn make_engine(path: &str, peer: PeerId, capacity: u32, notes_reserve: u32) -> Arc<Engine> {
    cleanup(path);
    let mut eng = Engine::create_with_capacity(path, capacity).unwrap();
    eng.define_table("notes", notes_reserve).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

/// author が n 件 tie して bridge まで済ませる。
fn author_notes(eng: &Arc<Engine>, from: u32, n: u32) {
    for i in from..from + n {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
}

/// 全 peer を明示 register した transport (set_history_floor が author を registry に
/// 登録して publish が registered 宛に切り替わる仕様のため、 実運用 harness と同様に
/// 最初から全員 register して使う)。
fn make_transport(peers: &[u32]) -> (Arc<InMemoryTransport>, Arc<dyn Transport>) {
    let mem = Arc::new(InMemoryTransport::new());
    for p in peers {
        mem.register_peer(*p);
    }
    let dy: Arc<dyn Transport> = mem.clone();
    (mem, dy)
}

#[test]
fn no_reclaim_below_pressure_late_joiner_gets_full_history() {
    let pa = tmp_path("a1");
    let pb = tmp_path("b1");
    let pc = tmp_path("c1");
    let (_mem, transport) = make_transport(&[1, 2, 3]);

    // ring ≈ 32k、 13 record では圧力なし
    let eng_a = make_engine(&pa, 1, 65_536, 1000);
    let eng_b = make_engine(&pb, 2, 65_536, 1000);
    let eng_c = make_engine(&pc, 3, 65_536, 1000);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());
    let sync_c = Syncer::new(eng_c.clone(), transport.clone());

    // A が 10 件 author、 B が全部消化 → A の次の publish で ack が消化される
    author_notes(&eng_a, 1, 10);
    sync_a.publish_since(Hlc::ZERO);
    assert_eq!(sync_b.pull_once(1).applied, 10);
    author_notes(&eng_a, 11, 3);
    sync_a.publish_since(Hlc::ZERO);

    // ack 自体は流れる (watermark 前進) が、 圧力が無いので reclaim はしない
    assert!(eng_a.sync_watermark() > 0, "#149: pull-as-ack が watermark を前進させること");
    assert!(
        eng_a.sync_reclaimed_floor().is_none(),
        "#149: 圧力が無いのに reclaim した (遅参 follower を不必要に bootstrap 送りにする)"
    );

    // まだ一度も pull していない C が、 後から全履歴を差分で取れる
    let out = sync_c.pull_once(1);
    assert!(!out.history_truncated, "{out:?}");
    assert_eq!(out.applied, 13, "遅参 peer が全履歴を取れること: {out:?}");

    cleanup(&pa);
    cleanup(&pb);
    cleanup(&pc);
}

#[test]
fn reclaims_under_pressure_without_manual_ack() {
    let pa = tmp_path("a2");
    let pb = tmp_path("b2");
    let (_mem, transport) = make_transport(&[1, 2]);

    // ring ≈ 850、 600 record で使用率 > 50%
    let eng_a = make_engine(&pa, 1, 8_192, 6_500);
    let eng_b = make_engine(&pb, 2, 8_192, 6_500);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());

    author_notes(&eng_a, 1, 600);
    sync_a.publish_since(Hlc::ZERO);
    assert_eq!(sync_b.pull_once(1).applied, 600);

    // 続きを author して publish — 手動 ack / reclaim 呼び出しは一切無しで、
    // B の pull ack が消化され、 圧力下なので reclaim が走る。
    author_notes(&eng_a, 601, 10);
    let ring_before = eng_a.table_eid_usage("_sync_ops").unwrap().live;
    sync_a.publish_since(Hlc::ZERO);

    assert!(eng_a.sync_watermark() > 0, "#149: watermark 前進 (手動 ack ゼロ)");
    assert!(
        eng_a.sync_reclaimed_floor().is_some(),
        "#149: 圧力下で reclaim が走って floor が記録されること"
    );
    let ring_after = eng_a.table_eid_usage("_sync_ops").unwrap().live;
    assert!(
        ring_after < ring_before,
        "#149: 消化済み record が reclaim されること ({ring_before} -> {ring_after})"
    );

    // 消化済み follower は reclaim に巻き込まれない (#191 と噛み合う)
    let out = sync_b.pull_once(1);
    assert!(!out.history_truncated, "{out:?}");
    assert_eq!(out.applied, 10, "{out:?}");

    cleanup(&pa);
    cleanup(&pb);
}

#[test]
fn slowest_puller_limits_reclaim() {
    let pa = tmp_path("a3");
    let pb = tmp_path("b3");
    let pc = tmp_path("c3");
    let (_mem, transport) = make_transport(&[1, 2, 3]);

    // ring ≈ 850
    let eng_a = make_engine(&pa, 1, 8_192, 6_500);
    let eng_b = make_engine(&pb, 2, 8_192, 6_500);
    let eng_c = make_engine(&pc, 3, 8_192, 6_500);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());
    let sync_c = Syncer::new(eng_c.clone(), transport.clone());

    // batch1: B も C も消化
    author_notes(&eng_a, 1, 500);
    sync_a.publish_since(Hlc::ZERO);
    assert_eq!(sync_b.pull_once(1).applied, 500);
    assert_eq!(sync_c.pull_once(1).applied, 500);

    // batch2: 使用率 (500+250)/850 > 50% → publish で両 ack が消化され
    // batch1 が reclaim される。 その後 B だけが batch2 を消化、 C は止まったまま。
    author_notes(&eng_a, 501, 250);
    sync_a.publish_since(Hlc::ZERO);
    assert!(
        eng_a.sync_reclaimed_floor().is_some(),
        "#149: 圧力下 + 両 peer 消化済みで reclaim が回ること"
    );
    assert_eq!(sync_b.pull_once(1).applied, 250);

    // batch3: publish は B の ack (head) を消化するが、 watermark は C の cursor
    // (batch1 末尾) に pin される — batch2/3 の record は reclaim されず生存。
    author_notes(&eng_a, 751, 60);
    sync_a.publish_since(Hlc::ZERO);

    // C が今から追いついても穴が無いこと: truncation されず batch2+3 の 310 件が届く。
    let out = sync_c.pull_once(1);
    assert!(
        !out.history_truncated,
        "#149: 最遅 puller の未消化分が reclaim された (過剰 reclaim): {out:?}"
    );
    assert_eq!(out.applied, 310, "batch2 (250) + batch3 (60): {out:?}");

    cleanup(&pa);
    cleanup(&pb);
    cleanup(&pc);
}

#[test]
fn ring_overflow_survives_with_pull_loop() {
    let pa = tmp_path("a4");
    let pb = tmp_path("b4");
    let (_mem, transport) = make_transport(&[1, 2]);

    // ring ≈ 850 row に対し総 op 数 6000 >> ring
    let eng_a = make_engine(&pa, 1, 8_192, 6_500);
    let eng_b = make_engine(&pb, 2, 8_192, 6_500);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());

    const ROUNDS: u32 = 20;
    const PER_ROUND: u32 = 300;
    let mut applied_total = 0usize;
    for r in 0..ROUNDS {
        author_notes(&eng_a, r * PER_ROUND + 1, PER_ROUND);
        sync_a.publish_since(Hlc::ZERO);
        let out = sync_b.pull_once(1);
        assert!(
            !out.history_truncated,
            "追従中の follower が truncation された (round {r}): {out:?}"
        );
        applied_total += out.applied;
    }

    // 旧挙動: ring (~850) が満杯になった round 以降、 bridge が stall して新規 op が
    // `_sync_ops` に載らず publish 対象外 → B には二度と届かない (これが #149 の本体)。
    assert_eq!(
        applied_total,
        (ROUNDS * PER_ROUND) as usize,
        "#149: ring 容量 (~850) を超える生涯 op が publish/pull loop で全配布されること"
    );

    // ring は定常で「未 ack の直近分」程度に保たれている (満杯で stall していない)
    let ring = eng_a.table_eid_usage("_sync_ops").unwrap().live;
    assert!(
        ring < 800,
        "#149: ring が回転していること (生存 {ring} row は満杯張り付き)"
    );

    cleanup(&pa);
    cleanup(&pb);
}
