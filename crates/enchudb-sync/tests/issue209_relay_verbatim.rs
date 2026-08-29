//! #209 — gossip relay は record を**素通し** (原 eid / 原 value / 原署名) で
//! 再配布すること。
//!
//! 旧実装は翻訳後の姿 (relay-local slot / relay-local vid / translated ref) を
//! author 名義で `append_relayed` していた (sync.rs NOTE(#9) の既知穴)。relay
//! 経由のみなら「一貫して間違った namespace」で辻褄が合うが、direct 経路
//! (bootstrap / relay 死亡 fallback / 複数 relay) と混在すると row 重複と
//! vocab 写像汚染、署名は書き換え時点で常に不一致になる。
//!
//! 検証 (3-hop: author A(1) → relay R(2) → consumer C(3)):
//! 1. `relay_preserves_origin_identity_under_slot_collision` — R の slot 番号を
//!    ずらしても、C が relay 経由 + author 直の両方から受けて row が重複しない。
//!    Tag / Leaf / Ref も原文一致。
//! 2. `relayed_records_survive_relay_reopen` — R を reopen (WAL recovery) しても
//!    R の body が壊れず、reopen 後も relay として配布を続けられる。
//! 3. `relay_preserves_signature` — A の署名が R を素通りして C で verify 通る。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue209-{}-{}-{}",
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

fn make_engine(path: &str, peer: PeerId) -> Arc<Engine> {
    cleanup(path);
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.define_himo_in("notes", "city", ValueType::Tag, 0).unwrap();
    eng.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();
    eng.define_himo_in("notes", "parent", ValueType::Ref, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

fn reopen_engine(path: &str, peer: PeerId) -> Arc<Engine> {
    let eng = Engine::open_concurrent_with_oplog(path, 16 * 1024 * 1024).unwrap();
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

/// note=i, city="c{i%3}", body="body {i}", parent=先頭 note を n 件 author。
fn author_notes(eng: &Arc<Engine>, from: u32, n: u32, parent_of: Option<u64>) -> Vec<u64> {
    let mut eids = Vec::new();
    for i in from..from + n {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
        eng.tie_text_to(e, "notes.city", &format!("c{}", i % 3));
        eng.tie_text_to(e, "notes.body", &format!("body {i}"));
        if let Some(p) = parent_of {
            eng.tie_ref_to(e, "notes.parent", p);
        }
        eids.push(e);
    }
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
    eids
}

/// relay 側: pull → 自分の relayed append を bridge → publish。
fn relay_cycle(relay_eng: &Arc<Engine>, relay_sync: &Syncer, from: PeerId) {
    relay_sync.pull_once(from);
    relay_eng.oplog_sync().unwrap();
    relay_eng.transfer_oplog_to_sync_ops();
    relay_sync.publish_since(Hlc::ZERO);
}

fn note_row(eng: &Arc<Engine>, i: u32) -> Option<(u64, Vec<u8>, Vec<u8>, Option<u32>)> {
    let rows = eng.pull_raw("notes.note", i);
    if rows.len() != 1 {
        panic!("note {i}: {} rows (重複 or 欠落): {rows:?}", rows.len());
    }
    let e = rows[0];
    let city = eng.get_text_owned(e, "notes.city")?;
    let body = eng.get_text_owned(e, "notes.body")?;
    let parent_note =
        eng.get(e, "notes.parent").and_then(|p| eng.get(p as u64, "notes.note"));
    Some((e, city, body, parent_note))
}

#[test]
fn relay_preserves_origin_identity_under_slot_collision() {
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
    sync_a.serve_state();
    eng_r.set_gossip_remote_apply(true);

    // R に自分の行を先に作らせて、A の行の translated slot を A-local から
    // 確実にずらす (= 旧実装なら relay 名義の eid が author 原番号と乖離する)。
    author_notes(&eng_r, 900, 3, None);

    // A が混在 content を author、R が中継、C は R からのみ pull
    let first = author_notes(&eng_a, 1, 1, None)[0];
    author_notes(&eng_a, 2, 9, Some(first));
    sync_a.publish_since(Hlc::ZERO);
    relay_cycle(&eng_r, &sync_r, 1);
    let out = sync_c.pull_once(2);
    assert!(out.applied > 0, "relay 経由で届くこと: {out:?}");

    // C が author 直からも pull (mixed path)。origin identity が素通しなら
    // 全 record が LWW 重複扱いで、row は増えない。
    let out_direct = sync_c.pull_once(1);
    assert!(!out_direct.history_truncated, "{out_direct:?}");

    for i in 1..=10u32 {
        let a = note_row(&eng_a, i).unwrap();
        let c = note_row(&eng_c, i)
            .unwrap_or_else(|| panic!("C に note {i} が無い (または text 欠落)"));
        assert_eq!(a.1, c.1, "city (Tag) 原文不一致 (note {i})");
        assert_eq!(a.2, c.2, "body (Leaf) 原文不一致 (note {i})");
        assert_eq!(a.3, c.3, "parent (Ref) 解決先不一致 (note {i})");
    }
    // R の feed = own + relayed (絞り込みは SubscriptionFilter の仕事)。R の own 行は
    // author=2 名義の**別 identity** として届く — A の行と混ざらないことが本質。
    // A の note 1..=10 が 1 行ずつ (重複ゼロ) なのは上の note_row が検証済み。

    cleanup(&pa);
    cleanup(&pr);
    cleanup(&pc);
}

#[test]
fn relayed_records_survive_relay_reopen() {
    let pa = tmp_path("a2");
    let pr = tmp_path("r2");
    let pc = tmp_path("c2");
    let transport = make_transport(&[1, 2, 3]);

    let eng_a = make_engine(&pa, 1);
    let eng_r = make_engine(&pr, 2);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    {
        let sync_r = Syncer::new(eng_r.clone(), transport.clone());
        eng_r.set_gossip_remote_apply(true);
        author_notes(&eng_r, 900, 3, None); // slot ずらし
        author_notes(&eng_a, 1, 10, None);
        sync_a.publish_since(Hlc::ZERO);
        relay_cycle(&eng_r, &sync_r, 1);
        for i in 1..=10u32 {
            assert!(note_row(&eng_r, i).is_some(), "前提: R が apply 済み (note {i})");
        }
    }
    drop(eng_r);

    // R を reopen — WAL recovery が relayed record (原 eid) を翻訳経路で replay
    // しても body が壊れないこと。
    let eng_r = reopen_engine(&pr, 2);
    let sync_r = Syncer::new(eng_r.clone(), transport.clone());
    eng_r.set_gossip_remote_apply(true);
    for i in 1..=10u32 {
        let a = note_row(&eng_a, i).unwrap();
        let r = note_row(&eng_r, i)
            .unwrap_or_else(|| panic!("reopen 後の R に note {i} が無い"));
        assert_eq!(a.1, r.1, "reopen 後 city 不一致 (note {i})");
        assert_eq!(a.2, r.2, "reopen 後 body 不一致 (note {i})");
    }
    for i in 900..=902u32 {
        assert!(note_row(&eng_r, i).is_some(), "R の own 行 {i} が reopen で消えた");
    }

    // reopen 後も relay として機能する: A の追加分を中継して C が受かる
    let eng_c = make_engine(&pc, 3);
    let sync_c = Syncer::new(eng_c.clone(), transport.clone());
    author_notes(&eng_a, 11, 3, None);
    sync_a.publish_since(Hlc::ZERO);
    relay_cycle(&eng_r, &sync_r, 1);
    let out = sync_c.pull_once(2);
    assert!(out.applied > 0, "reopen 後の relay から C に届くこと: {out:?}");
    for i in 1..=13u32 {
        assert!(note_row(&eng_c, i).is_some(), "C に note {i} が無い");
    }

    cleanup(&pa);
    cleanup(&pr);
    cleanup(&pc);
}

#[test]
fn relay_preserves_signature() {
    use enchudb_oplog::keys::Keypair;
    let pa = tmp_path("a3");
    let pr = tmp_path("r3");
    let pc = tmp_path("c3");
    let transport = make_transport(&[1, 2, 3]);

    let eng_a = make_engine(&pa, 1);
    let eng_r = make_engine(&pr, 2);
    let eng_c = make_engine(&pc, 3);
    let kp = Arc::new(Keypair::from_bytes(&[9u8; 32]));
    eng_a.set_keypair(Some(kp.clone()));
    // R も自分の own 行には自鍵で署名する (relay された A の record は A の署名の
    // まま素通し — R の鍵では再署名しない、が本テストの主張)。
    let kp_r = Arc::new(Keypair::from_bytes(&[10u8; 32]));
    eng_r.set_keypair(Some(kp_r.clone()));
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_r = Syncer::new(eng_r.clone(), transport.clone());
    let sync_c = Syncer::new(eng_c.clone(), transport.clone());
    eng_r.set_gossip_remote_apply(true);
    // R / C とも A の pubkey を TOFU 登録、C は R の pubkey も登録して署名必須にする
    eng_r.pubkeys().force_register(1, &kp.public_bytes());
    eng_c.pubkeys().force_register(1, &kp.public_bytes());
    eng_c.pubkeys().force_register(2, &kp_r.public_bytes());
    sync_c.set_require_signature(true);

    author_notes(&eng_r, 900, 2, None); // slot ずらし
    author_notes(&eng_a, 1, 5, None);
    sync_a.publish_since(Hlc::ZERO);
    relay_cycle(&eng_r, &sync_r, 1);

    let out = sync_c.pull_once(2);
    assert_eq!(
        out.rejected_signature, 0,
        "#209: relay を挟むと author の署名が壊れる (書き換えの証拠): {out:?}"
    );
    assert!(out.applied > 0, "署名 verify 通過で適用されること: {out:?}");
    for i in 1..=5u32 {
        assert!(note_row(&eng_c, i).is_some(), "C に note {i} が無い");
    }

    cleanup(&pa);
    cleanup(&pr);
    cleanup(&pc);
}
