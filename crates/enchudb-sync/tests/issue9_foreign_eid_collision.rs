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
use enchudb_engine::ValueType;
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
    let _ = std::fs::remove_file(format!("{}.eidmap", path));
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

#[test]
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

/// #9 commit 2: 翻訳 mapping が reopen を跨いで永続化される (= 再 sync で
/// 重複 entity を払い出さない)。 `.eidmap` sidecar の往復を検証する。
#[test]
fn translation_mapping_survives_reopen() {
    let path_a = tmp_path("reopen_a");
    let path_b = tmp_path("reopen_b");

    // peer A (id=1): entity + tie、 publish。
    let eng_a = make_engine(&path_a, 1);
    let e_a = eng_a.entity_in("notes").unwrap();
    eng_a.tie_to(e_a, "notes.note", 100);
    eng_a.oplog_commit();
    std::thread::sleep(Duration::from_millis(300));
    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    syncer_a.publish_since(Hlc::ZERO);
    let records = transport.pull(1, Hlc::ZERO);
    assert!(!records.is_empty());

    // peer B (id=2): A の record を apply → foreign entity が local X に mapping。
    let eng_b = make_engine(&path_b, 2);
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());
    let out = syncer_b.apply_records(&records);
    assert!(out.applied > 0, "A's record should apply");
    let local_x = eid_local(
        eng_b
            .resolve_remote_eid_existing(1, e_a)
            .expect("mapping should exist after apply"),
    );
    let n_before = eng_b.eid_translator().len();
    assert_eq!(n_before, 1, "exactly one foreign entity mapped");

    // .eidmap + .tables を persist して全 Arc を drop → reopen。
    eng_b.persist_tables().unwrap();
    drop(syncer_a);
    drop(eng_a);
    drop(syncer_b);
    drop(eng_b);

    // reopen — mapping は `.eidmap` sidecar から復元される。
    let eng_b2 = Engine::open_concurrent_with_oplog(&path_b, 16 * 1024 * 1024).unwrap();
    eng_b2.set_peer_id(2);
    assert_eq!(
        eng_b2.eid_translator().len(),
        n_before,
        "translation mapping must be restored on reopen"
    );
    assert_eq!(
        eng_b2.resolve_remote_eid_existing(1, e_a).map(eid_local),
        Some(local_x),
        "the same foreign entity must resolve to the same local eid after reopen"
    );

    // 再 sync しても新規払い出しせず既存 mapping を再利用 (= 重複しない)。
    let syncer_b2 = Syncer::new(eng_b2.clone(), transport.clone());
    syncer_b2.apply_records(&records);
    assert_eq!(
        eng_b2.eid_translator().len(),
        n_before,
        "re-applying after reopen must reuse the mapping, not allocate a duplicate"
    );

    drop(syncer_b2);
    drop(eng_b2);
    cleanup(&path_a);
    cleanup(&path_b);
}

/// #9 commit 3: cross-peer ref。 Ref himo の value 自体が foreign target eid なので、
/// apply 時に ref の target table 空間の local eid へ翻訳されること。
///
/// B は自分の company を先に作って allocator を 1 つ進めておく。 こうすると A の
/// company の翻訳先が A の raw eid とズレるので、 「翻訳してない」 と ref が B 自身の
/// company を指してしまい assertion で落ちる (= テストが翻訳を本当に検証する)。
#[test]
fn cross_peer_ref_value_is_translated() {
    fn make_ref_engine(path: &str, peer: PeerId) -> Arc<Engine> {
        cleanup(path);
        let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
        eng.define_table("companies", 1000).unwrap();
        eng.define_himo_in("companies", "cid", ValueType::Number, 0).unwrap();
        eng.define_table("users", 1000).unwrap();
        eng.define_himo_in("users", "uid", ValueType::Number, 0).unwrap();
        eng.define_ref_in("users", "company", "companies").unwrap();
        eng.enable_sync_tables().unwrap();
        let eng: Arc<Engine> =
            Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
        eng.set_peer_id(peer);
        eng
    }

    let path_a = tmp_path("ref_a");
    let path_b = tmp_path("ref_b");

    // peer A: company c (cid=7), user u (uid=1) -> ref company = c。
    let eng_a = make_ref_engine(&path_a, 1);
    let c = eng_a.entity_in("companies").unwrap();
    eng_a.tie_to(c, "companies.cid", 7);
    let u = eng_a.entity_in("users").unwrap();
    eng_a.tie_to(u, "users.uid", 1);
    eng_a.tie_ref_to(u, "users.company", c);
    eng_a.oplog_commit();
    std::thread::sleep(Duration::from_millis(300));

    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    syncer_a.publish_since(Hlc::ZERO);
    let records = transport.pull(1, Hlc::ZERO);
    assert!(!records.is_empty());

    // peer B: 自分の company を先に作る (= allocator を 1 進める)。
    let eng_b = make_ref_engine(&path_b, 2);
    let b_own = eng_b.entity_in("companies").unwrap();
    eng_b.tie_to(b_own, "companies.cid", 99);

    // A の records を apply。
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());
    let out = syncer_b.apply_records(&records);
    assert!(out.applied > 0, "A's records should apply");

    // B 側の company / user の翻訳後 local eid。
    let c_local = eid_local(
        eng_b
            .resolve_remote_eid_existing(1, c)
            .expect("company should be mapped"),
    );
    let u_eid = eng_b
        .resolve_remote_eid_existing(1, u)
        .expect("user should be mapped");

    // 翻訳が効いていれば A の company は B 自身の company とは別 slot に置かれる。
    assert_ne!(
        c_local,
        eid_local(b_own),
        "A's company must NOT land on B's own company slot"
    );

    // user の ref value は B の company local (= c_local) を指すこと。 翻訳してないと
    // A の raw eid (= b_own と同じ slot) を指してしまう。
    let ref_val = eng_b
        .get(u_eid, "users.company")
        .expect("user should have a company ref");
    assert_eq!(
        ref_val, c_local,
        "cross-peer ref must be translated to B's local company eid (got {}, want {}); \
         untranslated it would point at B's own company {}",
        ref_val,
        c_local,
        eid_local(b_own)
    );

    drop(syncer_a);
    drop(eng_a);
    drop(syncer_b);
    drop(eng_b);
    cleanup(&path_a);
    cleanup(&path_b);
}
