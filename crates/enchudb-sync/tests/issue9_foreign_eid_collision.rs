//! #9 regression — **FAILING until foreign-eid translation lands.**
//!
//! Cross-peer EntityId collision causes a silent overwrite during sync.
//!
//! Two fresh engines with the same schema both allocate their first entity at
//! the SAME local slot R (`entity_in` returns `make_eid(peer, global)`, so the
//! full eids differ only in the peer-id high bits). When peer B applies peer
//! A's `Tie` record, `Syncer::apply_one` passes the foreign eid through raw and
//! `remote_tie_apply` reduces it via `eid_local` to R — writing over B's OWN
//! entity at slot R. B's data is silently lost.
//!
//! A's write is made the *newer* one on purpose, so it is not LWW-rejected:
//! that isolates the bug to the missing eid translation rather than HLC timing.
//!
//! Correct behavior (post `EidTranslator`): B maps `(peer_a, R)` to a *fresh*
//! local eid, so A's entity and B's entity coexist and B's value stays 200.

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::HimoType;
use enchudb_oplog::{eid_local, Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;
use std::time::Duration;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue9-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
    let _ = std::fs::remove_file(format!("{}.tables", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.db.lock", path));
}

fn make_engine(path: &str, peer: PeerId) -> Arc<Engine> {
    cleanup(path);
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", HimoType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

#[test]
#[ignore = "issue #9: foreign-eid translation (EidTranslator) not yet implemented. \
            Remove #[ignore] to drive the fix red->green. Verified FAILING on 0.8.18."]
fn foreign_eid_collision_must_not_clobber_local_entity() {
    let path_a = tmp_path("a");
    let path_b = tmp_path("b");

    // peer B (id=2): its OWN first entity, note = 200 — the OLDER write.
    let eng_b = make_engine(&path_b, 2);
    let e_b = eng_b.entity_in("notes").unwrap();
    eng_b.tie_to(e_b, "notes.note", 200);
    eng_b.oplog_commit();
    std::thread::sleep(Duration::from_millis(300));

    // peer A (id=1): its first entity, note = 100 — the NEWER write (later wall
    // clock), so it wins LWW and the failure is purely the missing translation.
    let eng_a = make_engine(&path_a, 1);
    let e_a = eng_a.entity_in("notes").unwrap();
    eng_a.tie_to(e_a, "notes.note", 100);
    eng_a.oplog_commit();
    std::thread::sleep(Duration::from_millis(300));

    // Capture A's records into the transport (= B's view of the stream).
    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    let pushed = syncer_a.publish_since(Hlc::ZERO);
    assert!(pushed > 0, "peer A should publish its tie record");
    let records = transport.pull(/*from=*/ 1, Hlc::ZERO);
    assert!(!records.is_empty(), "transport should hold A's records");

    // Preconditions that make this a real collision test:
    // A and B share the same LOCAL slot; the full eids differ only by peer id.
    assert_eq!(
        eid_local(e_a),
        eid_local(e_b),
        "precondition: A and B must share a local slot to exercise the collision \
         (a={:#x} b={:#x})",
        e_a,
        e_b
    );
    assert_ne!(e_a, e_b, "precondition: full eids should differ by peer id");

    // Sanity: B reads its own value before the cross-peer apply.
    assert_eq!(
        eng_b.get(e_b, "notes.note"),
        Some(200),
        "B's own entity should read 200 before sync"
    );

    // B applies A's record. With #9 unfixed the foreign eid is not translated,
    // so this writes over B's own slot R.
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());
    let out = syncer_b.apply_records(&records);
    assert!(out.applied > 0, "A's tie record should be applied by B");

    // THE ASSERTION THAT FAILS UNTIL #9 IS FIXED.
    // A's entity is a DIFFERENT entity that merely shares a local slot, so B's
    // own entity must still hold 200. On buggy code it now reads 100 (A's value
    // overwrote B's at local slot R).
    let b_val = eng_b.get(e_b, "notes.note");
    assert_eq!(
        b_val,
        Some(200),
        "issue #9: applying peer A's record clobbered peer B's own entity at \
         local slot {} (got {:?}); the foreign eid was not translated to a fresh \
         local eid",
        eid_local(e_b),
        b_val
    );

    drop(eng_a);
    drop(eng_b);
    cleanup(&path_a);
    cleanup(&path_b);
}
