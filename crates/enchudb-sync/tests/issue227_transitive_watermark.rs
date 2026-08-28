//! #227 — relay の pull-as-ack が下流の消化を無視していた (watermark が transitive
//! でない)。
//!
//! `pull_once` は `last_pulled` (= 自分が apply した位置) をそのまま author に
//! 返していた。 1-hop ならそれが消化証明だが、 **relay では「配った」でしかない**。
//! reclaim の安全条件は「全 follower が apply し切った」なので、 relay が配った
//! 直後に恒久消失すると author は履歴を捨て、 下流は永久欠落する (#191 の裏返しで
//! 1 段深い)。 実測: A が 5 note (= 10 row) を author し R だけが pull した時点で、
//! C が 1 件も消化していないのに A の watermark = 10。
//!
//! fix: relay の ack を `min(自分の cursor[a], 下流全員が消化し切った位置[a])` に。
//! 後者は既存の永続 state から導出する (`_sync_ops` の `lsn <= sync_watermark()` の
//! author 別 max HLC + reclaim 済み floor) ので、 新しい記録は増えない。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue227-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suf in
        ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap", ".cursors", ".cursors.tmp"]
    {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

fn make_engine(path: &str, peer: PeerId) -> Arc<Engine> {
    cleanup(path);
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

fn make_transport(peers: &[u32]) -> Arc<dyn Transport> {
    let mem = Arc::new(InMemoryTransport::new());
    for p in peers {
        mem.register_peer(*p);
    }
    mem
}

fn author_note(eng: &Arc<Engine>, i: u32) {
    let e = eng.entity_in("notes").unwrap();
    eng.tie_to(e, "notes.note", i);
    eng.tie_text_to(e, "notes.body", &format!("body {i}"));
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
}

fn has_note(eng: &Arc<Engine>, i: u32) -> bool {
    eng.pull_raw("notes.note", i).len() == 1
}

fn relay_cycle(eng: &Arc<Engine>, sync: &Syncer, from: PeerId) {
    sync.pull_once(from);
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
    sync.publish_since(Hlc::ZERO);
}

/// relay が配っただけでは author の watermark は動かず、 **下流が消化した分だけ**
/// 動くこと。
#[test]
fn relay_ack_waits_for_downstream() {
    let (pa, pr, pc) = (tmp_path("a1"), tmp_path("r1"), tmp_path("c1"));
    let transport = make_transport(&[1, 2, 3]);
    let eng_a = make_engine(&pa, 1);
    let eng_r = make_engine(&pr, 2);
    let eng_c = make_engine(&pc, 3);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_r = Syncer::new(eng_r.clone(), transport.clone());
    let sync_c = Syncer::new(eng_c.clone(), transport.clone());
    eng_r.set_gossip_remote_apply(true);

    // C を R の下流として登録する — R 自身の row を 1 件消化させる。
    author_note(&eng_r, 900);
    sync_r.publish_since(Hlc::ZERO);
    assert!(sync_c.pull_once(2).applied > 0, "前提: C が R の row を消化");
    sync_r.publish_since(Hlc::ZERO); // C の ack を absorb
    assert!(
        eng_r.sync_watermark() > 0,
        "前提: R に下流 (C) が登録され watermark が立つこと"
    );

    // A が 5 note author → R が中継。 C はまだ pull しない。
    for i in 1..=5 {
        author_note(&eng_a, i);
    }
    sync_a.publish_since(Hlc::ZERO);
    relay_cycle(&eng_r, &sync_r, 1);
    for i in 1..=5 {
        assert!(has_note(&eng_r, i), "前提: R に note {i} が届いている");
    }
    sync_a.publish_since(Hlc::ZERO); // R の ack を absorb

    assert!(
        !has_note(&eng_c, 1),
        "前提: C はまだ author 1 の row を 1 件も消化していない"
    );
    assert_eq!(
        eng_a.sync_watermark(),
        0,
        "下流 C が未消化なのに author の watermark が進んでいる (#227 の症状)"
    );

    // C が消化 → R が ack を absorb → R が改めて A に ack。
    let out = sync_c.pull_once(2);
    assert!(out.applied > 0, "C が relay 経由で受け取れること: {out:?}");
    for i in 1..=5 {
        assert!(has_note(&eng_c, i), "note {i} が C に届いていない");
    }
    sync_r.publish_since(Hlc::ZERO); // C の ack を absorb → R の watermark 前進
    sync_r.pull_once(1); // 前進した cap で A に ack し直す
    sync_a.publish_since(Hlc::ZERO); // A が absorb

    assert!(
        eng_a.sync_watermark() > 0,
        "下流まで届き切ったのに author の watermark が上がらない (= reclaim が永久に回らない)"
    );
}

/// 下流の居ない葉ノードは丸めないこと — ここで ZERO を返すと author の reclaim が
/// 永久に止まる。
#[test]
fn leaf_relay_acks_fully() {
    let (pa, pr) = (tmp_path("a2"), tmp_path("r2"));
    let transport = make_transport(&[1, 2]);
    let eng_a = make_engine(&pa, 1);
    let eng_r = make_engine(&pr, 2);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_r = Syncer::new(eng_r.clone(), transport.clone());
    eng_r.set_gossip_remote_apply(true); // relay だが下流はまだ居ない

    for i in 1..=3 {
        author_note(&eng_a, i);
    }
    sync_a.publish_since(Hlc::ZERO);
    relay_cycle(&eng_r, &sync_r, 1);
    sync_a.publish_since(Hlc::ZERO);

    assert!(
        eng_a.sync_watermark() > 0,
        "下流ゼロの relay が丸めてしまい author の reclaim が止まっている"
    );
}

/// 非 relay は丸めないこと — 他 author の row を転送しないので、 自分の下流が
/// それを待つはずが無い。
#[test]
fn non_relay_ack_is_not_capped() {
    let (pa, pf, pd) = (tmp_path("a3"), tmp_path("f3"), tmp_path("d3"));
    let transport = make_transport(&[1, 3, 4]);
    let eng_a = make_engine(&pa, 1);
    let eng_f = make_engine(&pf, 3);
    let eng_d = make_engine(&pd, 4);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_f = Syncer::new(eng_f.clone(), transport.clone());
    let sync_d = Syncer::new(eng_d.clone(), transport.clone());
    // F は gossip OFF = ただの follower。 D は F 自身の row を購読する下流。

    author_note(&eng_f, 900);
    sync_f.publish_since(Hlc::ZERO);
    assert!(sync_d.pull_once(3).applied > 0, "前提: D が F の row を消化");
    sync_f.publish_since(Hlc::ZERO);
    assert!(eng_f.sync_watermark() > 0, "前提: F に下流 D が登録されること");

    for i in 1..=3 {
        author_note(&eng_a, i);
    }
    sync_a.publish_since(Hlc::ZERO);
    sync_f.pull_once(1);
    sync_a.publish_since(Hlc::ZERO);

    assert!(
        eng_a.sync_watermark() > 0,
        "非 relay の ack が丸められている — F は author 1 の row を転送しないので \
         D がそれを待つことはない"
    );
}
