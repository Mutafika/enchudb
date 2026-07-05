//! #9 destructive + failure tests: concurrency races and corrupt-sidecar fallback.

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport, WireRecord};
use enchudb_engine::ValueType;
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{eid_local, make_eid, Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;
use std::time::Duration;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue9-destr-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suffix in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".eidmap.tmp"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

fn make_engine(path: &str, peer: PeerId) -> Arc<Engine> {
    cleanup(path);
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

/// DESTRUCTIVE: N threads concurrently resolve the SAME foreign entity. All must
/// converge on one local eid, and the translator must hold exactly one mapping
/// (a get-then-alloc-then-insert that isn't atomic would double-allocate, orphan
/// entities, and return different locals to different threads).
#[test]
fn concurrent_resolve_same_foreign_is_consistent() {
    let path = tmp_path("concurrent_same");
    let eng = make_engine(&path, 2);
    let himo = eng.himo_id("notes.note").unwrap() as u16;
    let foreign = make_eid(1, 5); // authored by peer 1, foreign local = 5

    let n = 16;
    let barrier = Arc::new(std::sync::Barrier::new(n));
    let mut handles = Vec::new();
    for _ in 0..n {
        let eng = eng.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            eid_local(eng.resolve_remote_eid(1, foreign, himo).expect("notes himo resolves to a table"))
        }));
    }
    let locals: Vec<u32> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let first = locals[0];
    assert!(
        locals.iter().all(|&l| l == first),
        "all threads must resolve the same foreign entity to the same local eid; \
         got {:?} (non-atomic get-or-allocate double-allocated)",
        locals
    );
    assert_eq!(
        eng.eid_translator().len(),
        1,
        "exactly one mapping for one foreign entity"
    );

    drop(eng);
    cleanup(&path);
}

/// DESTRUCTIVE: many distinct foreign entities resolved concurrently — every
/// distinct foreign maps to a distinct local, no collisions, translator len == N.
#[test]
fn concurrent_resolve_distinct_foreigns_no_collision() {
    let path = tmp_path("concurrent_distinct");
    let eng = make_engine(&path, 2);
    let himo = eng.himo_id("notes.note").unwrap() as u16;

    let n: u32 = 64;
    let barrier = Arc::new(std::sync::Barrier::new(n as usize));
    let mut handles = Vec::new();
    for i in 0..n {
        let eng = eng.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            eid_local(
                eng.resolve_remote_eid(1, make_eid(1, 100 + i), himo)
                    .expect("notes himo resolves to a table"),
            )
        }));
    }
    let mut locals: Vec<u32> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    locals.sort_unstable();
    let before = locals.len();
    locals.dedup();
    assert_eq!(
        locals.len(),
        before,
        "distinct foreign entities must map to distinct local eids (collision detected)"
    );
    assert_eq!(eng.eid_translator().len(), n as usize);

    drop(eng);
    cleanup(&path);
}

/// FAILURE: a corrupt `.eidmap` sidecar must not crash open — fall back to an
/// empty translator and a usable DB (re-sync rebuilds the mapping).
#[test]
fn corrupt_eidmap_sidecar_falls_back_gracefully() {
    let path = tmp_path("corrupt");
    {
        let eng = make_engine(&path, 2);
        let himo = eng.himo_id("notes.note").unwrap() as u16;
        eng.resolve_remote_eid(1, make_eid(1, 3), himo).unwrap();
        eng.persist_tables().unwrap();
        assert!(eng.eid_translator().len() >= 1);
        drop(eng);
    }
    // Corrupt the sidecar (bad magic).
    std::fs::write(
        format!("{}.eidmap", path),
        b"GARBAGE-not-a-valid-eidmap-sidecar-xxxxxxxx",
    )
    .unwrap();

    // Reopen must succeed; translator falls back to empty (corrupt sidecar ignored).
    let eng = Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).unwrap();
    assert_eq!(
        eng.eid_translator().len(),
        0,
        "corrupt .eidmap must fall back to an empty translator, not crash"
    );
    // DB is still usable.
    let e = eng.entity_in("notes").unwrap();
    eng.tie_to(e, "notes.note", 1);
    assert_eq!(eng.get(e, "notes.note"), Some(1));

    drop(eng);
    cleanup(&path);
}

/// FAILURE: a truncated `.eidmap` (valid magic, cut-off entries) must also fall
/// back gracefully rather than panicking on a slice out of bounds.
#[test]
fn truncated_eidmap_sidecar_falls_back_gracefully() {
    let path = tmp_path("truncated");
    {
        let eng = make_engine(&path, 2);
        let himo = eng.himo_id("notes.note").unwrap() as u16;
        eng.resolve_remote_eid(1, make_eid(1, 9), himo).unwrap();
        eng.persist_tables().unwrap();
        drop(eng);
    }
    // Read the valid sidecar, then truncate it mid-entry (keep header, cut payload).
    let full = std::fs::read(format!("{}.eidmap", path)).unwrap();
    assert!(full.len() > 12, "sidecar should have a header + at least one entry");
    std::fs::write(format!("{}.eidmap", path), &full[..full.len() - 3]).unwrap();

    let eng = Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).unwrap();
    assert_eq!(
        eng.eid_translator().len(),
        0,
        "truncated .eidmap must fall back to an empty translator, not panic"
    );

    drop(eng);
    cleanup(&path);
}

/// SANITY (uses the sync path end to end under the destructive helpers): two
/// peers, B applies A's record twice concurrently-ish; the translator stays at 1
/// and B's own data is never clobbered.
#[test]
fn double_apply_is_idempotent_on_translator() {
    let path_a = tmp_path("dbl_a");
    let path_b = tmp_path("dbl_b");

    let eng_a = make_engine(&path_a, 1);
    let e_a = eng_a.entity_in("notes").unwrap();
    eng_a.tie_to(e_a, "notes.note", 42);
    eng_a.oplog_commit();
    std::thread::sleep(Duration::from_millis(300));
    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    syncer_a.publish_since(Hlc::ZERO);
    let records = transport.pull(1, Hlc::ZERO);

    let eng_b = make_engine(&path_b, 2);
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());
    syncer_b.apply_records(&records);
    let after_first = eng_b.eid_translator().len();
    syncer_b.apply_records(&records);
    syncer_b.apply_records(&records);
    assert_eq!(
        eng_b.eid_translator().len(),
        after_first,
        "re-applying the same records must not grow the translator"
    );

    drop(syncer_a);
    drop(eng_a);
    drop(syncer_b);
    drop(eng_b);
    cleanup(&path_a);
    cleanup(&path_b);
}

/// FAILURE (C1 regression): syncing a `Content` op must NOT panic. The old
/// anonymous `entity()` fallback panicked on every sync-capable engine (the
/// anonymous table is closed once `enable_sync_tables` runs). Content after a
/// Tie reuses the entity's mapping and applies; the property under test is
/// "apply does not crash".
#[test]
fn content_sync_does_not_panic() {
    let path_a = tmp_path("content_a");
    let path_b = tmp_path("content_b");

    let eng_a = make_engine(&path_a, 1);
    let e_a = eng_a.entity_in("notes").unwrap();
    eng_a.tie_to(e_a, "notes.note", 7);
    eng_a.content_async(e_a, "memo", b"hello");
    eng_a.oplog_commit();
    std::thread::sleep(Duration::from_millis(300));

    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    syncer_a.publish_since(Hlc::ZERO);
    let records = transport.pull(1, Hlc::ZERO);

    // B applies — pre-fix this panicked the applying thread on the Content op.
    let eng_b = make_engine(&path_b, 2);
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());
    let out = syncer_b.apply_records(&records);
    assert!(out.applied > 0, "the Tie (at least) should apply without panicking");

    // The Tie'd entity's content followed via the reused mapping.
    let local = eng_b
        .resolve_remote_eid_existing(1, e_a)
        .expect("entity should be mapped from its Tie");
    assert_eq!(
        eng_b.get_content(local, "memo"),
        Some(b"hello".as_ref()),
        "Content op should apply to the translated entity"
    );

    drop(syncer_a);
    drop(eng_a);
    drop(syncer_b);
    drop(eng_b);
    cleanup(&path_a);
    cleanup(&path_b);
}

/// FAILURE (C-1 regression): a `.eidmap` whose header declares a bogus huge entry
/// count must NOT trigger a multi-GB `Vec::with_capacity` (process abort on open).
/// Valid magic + version, count = u32::MAX, empty body → graceful empty fallback.
/// Without the bounds guard this requests ~51 GB and aborts the whole process.
#[test]
fn huge_count_eidmap_sidecar_does_not_oom() {
    let path = tmp_path("huge_count");
    {
        let eng = make_engine(&path, 2);
        let himo = eng.himo_id("notes.note").unwrap() as u16;
        eng.resolve_remote_eid(1, make_eid(1, 4), himo).unwrap();
        eng.persist_tables().unwrap();
        drop(eng);
    }
    // valid magic "EIDM" + version 1 + count = u32::MAX, but no entries follow.
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"EIDM");
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.extend_from_slice(&u32::MAX.to_le_bytes());
    std::fs::write(format!("{}.eidmap", path), &bytes).unwrap();

    let eng = Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).unwrap();
    assert_eq!(
        eng.eid_translator().len(),
        0,
        "bogus huge-count .eidmap must fall back to an empty translator, not OOM-abort"
    );
    // DB is still usable.
    let e = eng.entity_in("notes").unwrap();
    eng.tie_to(e, "notes.note", 1);
    assert_eq!(eng.get(e, "notes.note"), Some(1));

    drop(eng);
    cleanup(&path);
}

/// REORDER (0.9.0 で書き換え): content は `TieNamed` (himo を名前で運ぶ Tie) と
/// して同期されるようになり、 **自力で entity 写像を作れる** — 旧 `Content` op の
/// 「Tie より先に届くと reorder buffer 退避」という構造問題そのものが消えた。
/// このテストは content record を entity の他の Tie より **先** に配送しても
/// 即 apply される (buffering 不要・ロスなし) ことを固定する。
#[test]
fn content_before_tie_is_buffered_then_applied() {
    let path_a = tmp_path("reorder_a");
    let path_b = tmp_path("reorder_b");

    let eng_a = make_engine(&path_a, 1);
    let e_a = eng_a.entity_in("notes").unwrap();
    eng_a.tie_to(e_a, "notes.note", 7);
    eng_a.content_async(e_a, "memo", b"hello");
    eng_a.oplog_commit();
    std::thread::sleep(Duration::from_millis(300));

    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    syncer_a.publish_since(Hlc::ZERO);
    let records = transport.pull(1, Hlc::ZERO);

    // 0.9.0 regression: 旧 Content op は wire に居ないこと (TieNamed に置換済み)
    assert!(
        !records.iter().any(|r| matches!(r.op, DecodedOp::Content { .. })),
        "0.9.0: content は Op::Content ではなく TieNamed で運ばれるはず"
    );
    // content の TieNamed (+ その直前の Vocab) を先に、 note の Tie を後に配送する。
    // Vocab は vid mapping のため対応する TieNamed より先に必要 (append 順で保証
    // されている連番をそのまま保つ)。
    let content_first: Vec<_> = records
        .iter()
        .filter(|r| matches!(r.op, DecodedOp::TieNamed { .. } | DecodedOp::Vocab { .. }))
        .cloned()
        .collect();
    let rest: Vec<_> = records
        .iter()
        .filter(|r| !matches!(r.op, DecodedOp::TieNamed { .. } | DecodedOp::Vocab { .. }))
        .cloned()
        .collect();
    assert!(
        content_first.iter().any(|r| matches!(r.op, DecodedOp::TieNamed { .. })),
        "A should have a TieNamed record for the content write"
    );

    let eng_b = make_engine(&path_b, 2);
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());

    // 1. content (TieNamed) が先に届く → TieNamed 自身が写像を作って即 apply。
    let out1 = syncer_b.apply_records(&content_first);
    assert!(out1.applied > 0, "TieNamed は写像を自力で作って即 apply されるはず");
    let local = eng_b
        .resolve_remote_eid_existing(1, e_a)
        .expect("TieNamed だけで entity が写像されるはず");
    assert_eq!(
        eng_b.get_content(local, "memo"),
        Some(b"hello".as_ref()),
        "content は Tie を待たずに読めるはず (reorder buffer 不要)"
    );

    // 2. 残りの Tie が届く → 同じ entity に合流する (別 entity を作らない)。
    let out2 = syncer_b.apply_records(&rest);
    assert!(out2.applied > 0, "the Tie should apply");
    let local2 = eng_b
        .resolve_remote_eid_existing(1, e_a)
        .expect("entity mapped");
    assert_eq!(local, local2, "後続の Tie は既存写像に合流するはず");
    assert_eq!(eng_b.get(local2, "notes.note"), Some(7));

    drop(syncer_a);
    drop(eng_a);
    drop(syncer_b);
    drop(eng_b);
    cleanup(&path_a);
    cleanup(&path_b);
}

/// CRITICAL (C regression): a foreign Delete tombstone must survive reopen via
/// `.eidmap` v2, so a stale older Tie (re-delivered WITHOUT its Delete) cannot
/// resurrect the deleted entity. Pre-fix the HlcStore was wiped on reopen (foreign
/// ops aren't in the local oplog under gossip-off) and the entity came back.
#[test]
fn foreign_delete_tombstone_survives_reopen() {
    let path_b = tmp_path("tomb_b");
    let foreign = make_eid(1, 5);
    let himo_id;
    {
        let eng_b = make_engine(&path_b, 2);
        himo_id = eng_b.himo_id("notes.note").unwrap() as u16;
        let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
        let syncer = Syncer::new(eng_b.clone(), transport);

        // peer 1: Tie note=5 @100, then Delete @200 (Delete is the newer write).
        let tie = WireRecord::unsigned(
            Hlc { wall: 100, logical: 0, peer: 1 },
            1,
            DecodedOp::Tie { eid: foreign, himo_id, value: 5 },
        );
        let del = WireRecord::unsigned(
            Hlc { wall: 200, logical: 0, peer: 1 },
            1,
            DecodedOp::Delete { eid: foreign },
        );
        let out = syncer.apply_records(&[tie, del]);
        assert_eq!(out.applied, 2, "Tie + Delete should both apply");
        let local = eng_b.resolve_remote_eid_existing(1, foreign).expect("mapped");
        assert_eq!(eng_b.get(local, "notes.note"), None, "entity deleted before reopen");

        eng_b.oplog_sync().unwrap(); // make the entity removal in the body durable
        eng_b.persist_tables().unwrap(); // persist .eidmap v2 (carries the tombstone)
        drop(syncer);
        drop(eng_b);
    }

    // reopen — the tombstone is restored from .eidmap v2 (peer_id 2 came from the header).
    let eng_b2 = Engine::open_concurrent_with_oplog(&path_b, 16 * 1024 * 1024).unwrap();
    eng_b2.set_peer_id(2);
    let transport2: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer2 = Syncer::new(eng_b2.clone(), transport2);

    // re-deliver ONLY the stale Tie (older than the Delete, which is now gone).
    let stale_tie = WireRecord::unsigned(
        Hlc { wall: 100, logical: 0, peer: 1 },
        1,
        DecodedOp::Tie { eid: foreign, himo_id, value: 5 },
    );
    let out = syncer2.apply_records(&[stale_tie]);
    assert_eq!(out.applied, 0, "stale Tie must be rejected by the restored tombstone");

    let local = eng_b2
        .resolve_remote_eid_existing(1, foreign)
        .expect("mapping restored on reopen");
    assert_eq!(
        eng_b2.get(local, "notes.note"),
        None,
        "deleted foreign entity must NOT resurrect after reopen (tombstone persisted in .eidmap v2)"
    );

    drop(syncer2);
    drop(eng_b2);
    cleanup(&path_b);
}

/// #76 (0.9.0): レプリカ (translated foreign entity) への self-authored write は
/// bridge されない (single-writer guard)。 旧挙動では B のローカル編集が
/// 「B の新規 entity」として全 peer に配信され、 A/C 上で断片化していた。
#[test]
fn replica_writeback_is_not_propagated() {
    let path_a = tmp_path("wb_a");
    let path_b = tmp_path("wb_b");

    // A: entity 作成 → publish
    let eng_a = make_engine(&path_a, 1);
    let e_a = eng_a.entity_in("notes").unwrap();
    eng_a.tie_to(e_a, "notes.note", 7);
    eng_a.oplog_commit();
    std::thread::sleep(Duration::from_millis(300));

    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    syncer_a.publish_since(Hlc::ZERO);

    // B: pull して translated local を得る
    let eng_b = make_engine(&path_b, 2);
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());
    let records = transport.pull(1, Hlc::ZERO);
    syncer_b.apply_records(&records);
    let local_b = eng_b
        .resolve_remote_eid_existing(1, e_a)
        .expect("A の entity が B に写像される");

    // B がレプリカをローカル編集 → guard により bridge されないはず
    eng_b.tie_to(local_b, "notes.note", 99);
    eng_b.oplog_commit();
    std::thread::sleep(Duration::from_millis(300));
    eng_b.transfer_oplog_to_sync_ops();
    let published = syncer_b.publish_since(Hlc::ZERO);
    let leaked: Vec<_> = transport
        .pull(2, Hlc::ZERO)
        .into_iter()
        .filter(|r| r.author_peer == 2)
        .collect();
    assert!(
        leaked.is_empty(),
        "レプリカへの write-back が bridge された ({} records, published={}) — \
         他 peer で entity 断片化する (#76)",
        leaked.len(),
        published,
    );

    // ローカルには反映されている (local-only edit)
    assert_eq!(eng_b.get(local_b, "notes.note"), Some(99));

    drop(syncer_a);
    drop(eng_a);
    drop(syncer_b);
    drop(eng_b);
    cleanup(&path_a);
    cleanup(&path_b);
}
