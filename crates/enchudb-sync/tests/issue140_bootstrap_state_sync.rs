//! #140 — bootstrap-first: truncated puller が author の live state で復旧できること。
//!
//! ring (`_sync_ops`) は「最近の差分」しか担保しない。 floor より古い cursor の
//! peer (新規 / 長期 offline) は差分では追いつけず、 0.22.0 までは「不完全と
//! 知らされる」だけで先に進めなかった。 fix: author が `Syncer::serve_state` で
//! live state を配布登録し、 truncated puller が `Syncer::bootstrap_pull` で
//! 現在状態を転写 → cursor を `as_of` に接続して差分 pull を再開する。
//!
//! 検証:
//! 1. `late_joiner_bootstraps_and_connects_to_diff_pull` — Number/Tag/Leaf/Ref
//!    混在の state が正しく転写され (ring 経由で追従した peer と一致)、 bootstrap
//!    後の差分 pull に隙間なく接続される。
//! 2. `ghost_rows_are_swept_on_bootstrap` — truncated 期間に delete の tombstone
//!    を取り逃した follower の亡霊行が、 bootstrap の sweep で消える (本 issue の
//!    原症状の直接再現)。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue140-{}-{}-{}",
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

/// Number / Tag / Leaf / Ref 混在の notes table を持つ engine。
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

fn make_transport(peers: &[u32]) -> (Arc<InMemoryTransport>, Arc<dyn Transport>) {
    let mem = Arc::new(InMemoryTransport::new());
    for p in peers {
        mem.register_peer(*p);
    }
    let dy: Arc<dyn Transport> = mem.clone();
    (mem, dy)
}

/// note=i, city="c{i%3}", body="body {i}", parent=先頭 note の行を n 件 author。
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

/// note=i の行を 1 件引いて (eid, city, body, parent 先の note 値) を返す。
fn read_note(eng: &Arc<Engine>, i: u32) -> Option<(u64, Vec<u8>, Vec<u8>, Option<u32>)> {
    let rows = eng.pull_raw("notes.note", i);
    let e = *rows.first()?;
    let city = eng.get_text_owned(e, "notes.city")?;
    let body = eng.get_text_owned(e, "notes.body")?;
    let parent_note = eng
        .get(e, "notes.parent")
        .and_then(|p| eng.get(p as u64, "notes.note"));
    Some((e, city, body, parent_note))
}

/// 手動 reclaim (全 ring を消化済み扱いにして floor を上げる)。
fn force_reclaim(eng: &Arc<Engine>, acked_peer: u32) {
    let lsn = eng.current_sync_lsn();
    eng.ack_sync(acked_peer, lsn + 1).unwrap();
    let purged = eng.reclaim_sync_ops();
    assert!(purged > 0, "前提: reclaim が実際に走ること");
}

#[test]
fn late_joiner_bootstraps_and_connects_to_diff_pull() {
    let pa = tmp_path("a1");
    let pb = tmp_path("b1");
    let pc = tmp_path("c1");
    let (_mem, transport) = make_transport(&[1, 2, 3]);

    let eng_a = make_engine(&pa, 1);
    let eng_b = make_engine(&pb, 2);
    let eng_c = make_engine(&pc, 3);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());
    let sync_c = Syncer::new(eng_c.clone(), transport.clone());
    sync_a.serve_state();

    // A が混在 content を author、 B が ring 経由で追従 (= 期待値の器)
    let first = author_notes(&eng_a, 1, 1, None)[0];
    author_notes(&eng_a, 2, 9, Some(first));
    sync_a.publish_since(Hlc::ZERO);
    let out = sync_b.pull_once(1);
    assert!(out.applied > 0 && out.dropped_vocab == 0, "{out:?}");

    // 全 ring を reclaim → 未登場の C は差分では追いつけない
    force_reclaim(&eng_a, 2);
    sync_a.publish_since(Hlc::ZERO); // floor 広告
    let out = sync_c.pull_once(1);
    assert!(out.history_truncated, "前提: C が truncated になること: {out:?}");

    // bootstrap — state 転写
    let boot = sync_c.bootstrap_pull(1).expect("serve_state 済みなので Some");
    assert!(boot.outcome.applied > 0, "{boot:?}");
    assert_eq!(boot.outcome.dropped_vocab, 0, "{boot:?}");
    assert_eq!(boot.swept, 0, "亡霊なしの bootstrap で sweep されるのはおかしい");

    // 転写結果が ring 経由の B と一致すること (Number / Tag / Leaf / Ref 全部)
    for i in 1..=10u32 {
        let b = read_note(&eng_b, i).unwrap_or_else(|| panic!("B に note {i} が無い"));
        let c = read_note(&eng_c, i).unwrap_or_else(|| panic!("C に note {i} が無い"));
        assert_eq!(b.1, c.1, "city (Tag) が一致しない (note {i})");
        assert_eq!(b.2, c.2, "body (Leaf) が一致しない (note {i})");
        assert_eq!(b.3, c.3, "parent (Ref) の解決先が一致しない (note {i})");
        if i > 1 {
            assert_eq!(c.3, Some(1), "parent ref が先頭 note に解決されること (note {i})");
        }
    }

    // 差分 pull への接続: A が続きを author → C は truncation なしで受かる
    author_notes(&eng_a, 11, 3, None);
    sync_a.publish_since(Hlc::ZERO);
    let out = sync_c.pull_once(1);
    assert!(!out.history_truncated, "bootstrap 後の差分 pull が拒否された: {out:?}");
    assert!(out.applied > 0, "差分が届いていない: {out:?}");
    for i in 11..=13u32 {
        assert!(read_note(&eng_c, i).is_some(), "差分の note {i} が C に無い");
    }

    cleanup(&pa);
    cleanup(&pb);
    cleanup(&pc);
}

#[test]
fn ghost_rows_are_swept_on_bootstrap() {
    let pa = tmp_path("a2");
    let pb = tmp_path("b2");
    let (_mem, transport) = make_transport(&[1, 2]);

    let eng_a = make_engine(&pa, 1);
    let eng_b = make_engine(&pb, 2);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());
    sync_a.serve_state();

    // A が 5 行 author、 B が追従
    author_notes(&eng_a, 1, 5, None);
    sync_a.publish_since(Hlc::ZERO);
    assert!(sync_b.pull_once(1).applied > 0);
    assert!(read_note(&eng_b, 3).is_some(), "前提: B は note 3 を持っている");

    // B がオフラインの間に A が note 3 を削除、 tombstone ごと reclaim される
    let victim = eng_a.pull_raw("notes.note", 3)[0];
    eng_a.delete(victim);
    eng_a.oplog_commit();
    eng_a.flush_writes();
    eng_a.oplog_sync().unwrap();
    eng_a.transfer_oplog_to_sync_ops();
    force_reclaim(&eng_a, 2);
    sync_a.publish_since(Hlc::ZERO); // floor 広告

    // B 復帰 → 差分では追いつけない (tombstone は ring から消えている)
    let out = sync_b.pull_once(1);
    assert!(out.history_truncated, "前提: B が truncated になること: {out:?}");
    assert!(read_note(&eng_b, 3).is_some(), "この時点では B に亡霊が居る");

    // bootstrap → 亡霊が sweep される
    let boot = sync_b.bootstrap_pull(1).expect("serve_state 済みなので Some");
    assert_eq!(boot.swept, 1, "note 3 の 1 行だけが sweep されること: {boot:?}");
    assert!(
        read_note(&eng_b, 3).is_none(),
        "#140: 削除済み行が bootstrap 後も B に残っている (亡霊)"
    );
    for i in [1u32, 2, 4, 5] {
        assert!(read_note(&eng_b, i).is_some(), "生存行 note {i} が消えている");
    }

    cleanup(&pa);
    cleanup(&pb);
}
