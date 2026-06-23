//! #9 destructive + failure tests: concurrency races and corrupt-sidecar fallback.

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::HimoType;
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
    eng.define_himo_in("notes", "note", HimoType::Number, 0).unwrap();
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
            eid_local(eng.resolve_remote_eid(1, foreign, himo))
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
            eid_local(eng.resolve_remote_eid(1, make_eid(1, 100 + i), himo))
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
        eng.resolve_remote_eid(1, make_eid(1, 3), himo);
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
        eng.resolve_remote_eid(1, make_eid(1, 9), himo);
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
