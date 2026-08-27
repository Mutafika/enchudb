//! #216 — relay の stream は HLC 非単調 (自 row = R の clock、 relayed row =
//! author の HLC 素通し #209) なので、 scalar HLC cursor の `hlc > since` filter は
//! relay された古い HLC の record を永久に落とす (silent data loss)。
//!
//! fix: cursor を link × author の vector に (author substream は relay を何 hop
//! 挟んでも HLC 単調、 がこの粒度を健全にする不変式)。 未知 author は ZERO 起点。
//!
//! 検証 (A(1) → relay R(2) → C(3)、 第 2 author B(4)):
//! 1. `relayed_old_hlc_record_reaches_advanced_cursor` — C の cursor が R 自身の
//!    新しい row で先に進んだ後に relay された古い HLC の A record が届くこと
//!    (旧実装は received: 0 の silent drop)。
//! 2. `new_author_starts_from_zero` — link に後から現れた author B の古い record
//!    が落ちないこと (「既知 author の min」への短絡は同じ穴を一段下で再現する)。
//! 3. `legacy_scalar_cursor_self_heals` — 旧 4-field cursor sidecar は author=link
//!    として読み、 他 author は ZERO 起点で再配送 = 旧実装が落とした record を
//!    upgrade が自己修復すること。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue216-{}-{}-{}",
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

/// relay 側: pull → relayed append を bridge → publish。
fn relay_cycle(relay_eng: &Arc<Engine>, relay_sync: &Syncer, from: PeerId) {
    relay_sync.pull_once(from);
    relay_eng.oplog_sync().unwrap();
    relay_eng.transfer_oplog_to_sync_ops();
    relay_sync.publish_since(Hlc::ZERO);
}

#[test]
fn relayed_old_hlc_record_reaches_advanced_cursor() {
    let pa = tmp_path("a1");
    let pr = tmp_path("r1");
    let pc = tmp_path("c1");
    let transport = make_transport(&[1, 2, 3]);

    let eng_a = make_engine(&pa, 1);
    let eng_r = make_engine(&pr, 2);
    let eng_c = make_engine(&pc, 3);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_r = Syncer::new(eng_r.clone(), transport.clone());
    let sync_c = Syncer::new(eng_c.clone(), transport.clone());
    eng_r.set_gossip_remote_apply(true);

    // 1. A が note 1 を author (HLC t1)。 まだ誰にも配られていない。
    author_note(&eng_a, 1);

    // 2. R が自分の row を author (HLC t2 > t1) して publish。
    author_note(&eng_r, 900);
    sync_r.publish_since(Hlc::ZERO);

    // 3. C が R から pull → C の R-author cursor は t2 まで前進。
    let out1 = sync_c.pull_once(2);
    assert!(out1.applied > 0, "R の own row が届く前提: {out1:?}");

    // 4. R が A から pull + relay + publish — note1@t1 (< t2) が R の stream に入る。
    sync_a.publish_since(Hlc::ZERO);
    relay_cycle(&eng_r, &sync_r, 1);
    assert!(has_note(&eng_r, 1), "前提: R には note 1 が適用済み");

    // 5. C が R から pull — author A の cursor は独立 (ZERO 起点) なので届く。
    //    旧実装 (scalar cursor) は t1 < t2 で filter し received: 0 だった。
    let out2 = sync_c.pull_once(2);
    assert!(
        out2.applied > 0,
        "#216: relay された古い HLC の record が cursor filter で落ちた: {out2:?}"
    );
    assert!(has_note(&eng_c, 1), "note 1 が C に届いていない");
    assert!(has_note(&eng_c, 900), "R の own row も揃っていること");

    // 差分 pull が壊れていないこと: 追加分だけが流れる。
    author_note(&eng_a, 2);
    sync_a.publish_since(Hlc::ZERO);
    relay_cycle(&eng_r, &sync_r, 1);
    let out3 = sync_c.pull_once(2);
    assert_eq!(out3.skipped, 0, "既消化分が再配送されている (cursor 前進漏れ): {out3:?}");
    assert!(has_note(&eng_c, 2), "追加の note 2 が届いていない");

    cleanup(&pa);
    cleanup(&pr);
    cleanup(&pc);
}

#[test]
fn new_author_starts_from_zero() {
    let pa = tmp_path("a2");
    let pb = tmp_path("b2");
    let pr = tmp_path("r2");
    let pc = tmp_path("c2");
    let transport = make_transport(&[1, 2, 3, 4]);

    let eng_a = make_engine(&pa, 1);
    let eng_b = make_engine(&pb, 4);
    let eng_r = make_engine(&pr, 2);
    let eng_c = make_engine(&pc, 3);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());
    let sync_r = Syncer::new(eng_r.clone(), transport.clone());
    let sync_c = Syncer::new(eng_c.clone(), transport.clone());
    eng_r.set_gossip_remote_apply(true);

    // B の record が最も古い HLC を持つ (先に author)。
    author_note(&eng_b, 20);
    sync_b.publish_since(Hlc::ZERO);

    // A → R → C を先に回して C の cursor (A, R の 2 author 分) を新しくする。
    author_note(&eng_a, 1);
    author_note(&eng_r, 900);
    sync_a.publish_since(Hlc::ZERO);
    relay_cycle(&eng_r, &sync_r, 1);
    let out1 = sync_c.pull_once(2);
    assert!(out1.applied > 0, "{out1:?}");
    assert!(has_note(&eng_c, 1) && has_note(&eng_c, 900));

    // R が B の relay を開始 — B は C にとって link 上の新 author で、 その record の
    // HLC は C の既知 cursor 全部より古い。 「既知 author の min」に短絡した実装は
    // ここで B を落とす。
    relay_cycle(&eng_r, &sync_r, 4);
    assert!(has_note(&eng_r, 20), "前提: R には B の note 20 が適用済み");
    let out2 = sync_c.pull_once(2);
    assert!(
        out2.applied > 0,
        "#216: 新 author B の古い record が届いていない: {out2:?}"
    );
    assert!(has_note(&eng_c, 20), "B の note 20 が C に無い");
    // reclaim (floor 広告) が起きていない通常経路では truncation は立たない。
    assert!(!out2.history_truncated, "{out2:?}");

    cleanup(&pa);
    cleanup(&pb);
    cleanup(&pr);
    cleanup(&pc);
}

#[test]
fn legacy_scalar_cursor_self_heals() {
    let pa = tmp_path("a3");
    let pr = tmp_path("r3");
    let pc = tmp_path("c3");
    let cursor_file = format!("{pc}.cursors");
    let transport = make_transport(&[1, 2, 3]);

    let eng_a = make_engine(&pa, 1);
    let eng_r = make_engine(&pr, 2);
    let eng_c = make_engine(&pc, 3);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_r = Syncer::new(eng_r.clone(), transport.clone());
    eng_r.set_gossip_remote_apply(true);

    // 旧実装で silent drop が起きた状況を再現する:
    // A@t1 が存在、 C は R own row (t2) まで pull 済みの scalar cursor を持つ。
    author_note(&eng_a, 1);
    author_note(&eng_r, 900);
    sync_r.publish_since(Hlc::ZERO);
    {
        let sync_c = Syncer::new(eng_c.clone(), transport.clone())
            .with_cursor_path(std::path::PathBuf::from(&cursor_file));
        let out = sync_c.pull_once(2);
        assert!(out.applied > 0, "{out:?}");
    }

    // sidecar を旧 4-field 書式に書き戻す (= 旧バージョンからの引き継ぎを模す)。
    let v2 = std::fs::read_to_string(&cursor_file).unwrap();
    let legacy: String = v2
        .lines()
        .filter_map(|l| {
            let p: Vec<&str> = l.split_whitespace().collect();
            // v2 `link author wall logical peer` → author==link の行だけを
            // legacy `link wall logical peer` へ。
            (p.len() == 5 && p[0] == p[1]).then(|| format!("{} {} {} {}\n", p[0], p[2], p[3], p[4]))
        })
        .collect();
    assert!(!legacy.is_empty(), "前提: link=author 行が sidecar にある\n{v2}");
    std::fs::write(&cursor_file, legacy).unwrap();

    // その後 R が A の record を relay (旧実装ならここで silent drop が確定する)。
    sync_a.publish_since(Hlc::ZERO);
    let sync_r = Syncer::new(eng_r.clone(), transport.clone());
    relay_cycle(&eng_r, &sync_r, 1);

    // 新実装で reopen — legacy 行は author=link cursor として読まれ、 A は ZERO
    // 起点なので relay 済み record が届く (= 自己修復)。 R own row は再配送されない。
    let sync_c = Syncer::new(eng_c.clone(), transport.clone())
        .with_cursor_path(std::path::PathBuf::from(&cursor_file));
    let out = sync_c.pull_once(2);
    assert!(out.applied > 0, "legacy cursor 移行後に A の record が届くこと: {out:?}");
    assert_eq!(out.skipped, 0, "R own row が再配送されている (legacy 行の読み損ね): {out:?}");
    assert!(has_note(&eng_c, 1), "note 1 が self-heal されていない");

    // 保存し直した sidecar は v2 (5-field) になっている。
    let saved = std::fs::read_to_string(&cursor_file).unwrap();
    assert!(
        saved.lines().all(|l| l.split_whitespace().count() == 5),
        "v2 書式で保存されること:\n{saved}"
    );

    cleanup(&pa);
    cleanup(&pr);
    cleanup(&pc);
}
