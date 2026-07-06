//! request10 (#76 逆写像) — write-back の正式サポート。
//!
//! 0.9.0-0.10.x は「translated foreign entity への self-authored write は
//! bridge しない」single-writer guard だった。 0.11 で bridge が逆写像
//! (`EidTranslator::reverse`) により **元 entity の世界番号に宛名を書き戻して
//! 発送**し、 受信側は eid の産みの親 (`eid_peer`) をキーに翻訳するので、
//! どの peer の書き込みも全 peer で同一 entity に収束する。 衝突は HLC LWW。
//!
//! 検証内容:
//! 1. `writeback_converges_on_author_entity` — B が A のレプリカに書いた card が
//!    A では **元 entity** に着弾し (fragment ゼロ)、 第三者 C でも 1 entity に収束
//! 2. `writeback_survives_reopen` — 逆写像が `.eidmap` sidecar から復元され、
//!    reopen 後の write-back も正しく宛名解決される (phase 1 の永続化 e2e)
//! 3. `lww_concurrent_same_card_converges` — 同じ card の双方向編集が
//!    「時間が解決する」 (HLC LWW) で収束する
//! 4. `ref_value_to_replica_stays_local` — Ref 値が translated local を指す write
//!    は発送されない (u32 wire value に世界番号が入らない、 request10 follow-up)

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;
use std::time::Duration;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-req10-{}-{}-{}",
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

/// notes(note, extra) の 2 himo 構成。 note = 作成側が書く card、
/// extra = write-back 側が書く card (LWW 干渉なしで着弾先を検証するため分離)。
fn make_engine(path: &str, peer: PeerId) -> Arc<Engine> {
    cleanup(path);
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.define_himo_in("notes", "extra", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

/// oplog_commit → 背景 consumer が bridge (transfer_oplog_to_sync_ops) するのを待つ。
fn settle(eng: &Arc<Engine>) {
    eng.oplog_commit();
    std::thread::sleep(Duration::from_millis(350));
    eng.transfer_oplog_to_sync_ops();
}

#[test]
fn writeback_converges_on_author_entity() {
    let path_a = tmp_path("wb_a");
    let path_b = tmp_path("wb_b");
    let path_c = tmp_path("wb_c");

    // A (peer 1): entity を産んで note=100。
    let eng_a = make_engine(&path_a, 1);
    let e_a = eng_a.entity_in("notes").unwrap();
    eng_a.tie_to(e_a, "notes.note", 100);
    settle(&eng_a);

    let t_a: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), t_a.clone());
    assert!(syncer_a.publish_since(Hlc::ZERO) > 0);
    let records_a = t_a.pull(1, Hlc::ZERO);
    assert!(!records_a.is_empty());

    // B (peer 2): A の record を apply → レプリカに extra=7 を write-back。
    let eng_b = make_engine(&path_b, 2);
    let syncer_b_in = Syncer::new(eng_b.clone(), t_a.clone());
    assert!(syncer_b_in.apply_records(&records_a).applied > 0);
    let t_world = eng_b
        .resolve_remote_eid_existing(e_a)
        .expect("A's entity must be mapped on B");
    std::thread::sleep(Duration::from_millis(50)); // HLC を確実に前進させる
    eng_b.tie_to(t_world, "notes.extra", 7);
    settle(&eng_b);

    let t_b: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_b_out = Syncer::new(eng_b.clone(), t_b.clone());
    assert!(
        syncer_b_out.publish_since(Hlc::ZERO) > 0,
        "0.11: replica write-back must be bridged & published (guard 撤去)"
    );
    let records_b = t_b.pull(2, Hlc::ZERO);
    assert!(!records_b.is_empty());

    // A が B の stream を apply → B の card が **元 entity** に着弾する。
    let syncer_a_in = Syncer::new(eng_a.clone(), t_b.clone());
    assert!(syncer_a_in.apply_records(&records_b).applied > 0);
    assert_eq!(
        eng_a.get(e_a, "notes.extra"),
        Some(7),
        "B's write-back must land on A's ORIGINAL entity"
    );
    // 断片化ゼロの証明: A 側では自分の entity への識別 (identity) なので
    // 翻訳 entry が 1 つも生えない。
    assert!(
        eng_a.eid_translator().is_empty(),
        "no fragment entity must be allocated on A (translator must stay empty)"
    );

    // 第三者 C: A の stream + B の stream を apply → 1 entity に収束。
    let eng_c = make_engine(&path_c, 3);
    let syncer_c_a = Syncer::new(eng_c.clone(), t_a.clone());
    let syncer_c_b = Syncer::new(eng_c.clone(), t_b.clone());
    assert!(syncer_c_a.apply_records(&records_a).applied > 0);
    assert!(syncer_c_b.apply_records(&records_b).applied > 0);
    let c_eid = eng_c
        .resolve_remote_eid_existing(e_a)
        .expect("A's entity must be mapped on C");
    assert_eq!(eng_c.get(c_eid, "notes.note"), Some(100), "A's card on C");
    assert_eq!(eng_c.get(c_eid, "notes.extra"), Some(7), "B's card on C, same entity");
    assert_eq!(
        eng_c.eid_translator().len(),
        1,
        "C must hold exactly ONE mapped entity (A's + B's writes converged)"
    );

    drop(syncer_a); drop(syncer_a_in); drop(syncer_b_in); drop(syncer_b_out);
    drop(syncer_c_a); drop(syncer_c_b);
    drop(eng_a); drop(eng_b); drop(eng_c);
    cleanup(&path_a); cleanup(&path_b); cleanup(&path_c);
}

#[test]
fn writeback_survives_reopen() {
    let path_a = tmp_path("ro_a");
    let path_b = tmp_path("ro_b");

    let eng_a = make_engine(&path_a, 1);
    let e_a = eng_a.entity_in("notes").unwrap();
    eng_a.tie_to(e_a, "notes.note", 100);
    settle(&eng_a);

    let t_a: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), t_a.clone());
    syncer_a.publish_since(Hlc::ZERO);
    let records_a = t_a.pull(1, Hlc::ZERO);

    // B: apply → persist → 全 drop → reopen。
    let eng_b = make_engine(&path_b, 2);
    let syncer_b = Syncer::new(eng_b.clone(), t_a.clone());
    assert!(syncer_b.apply_records(&records_a).applied > 0);
    eng_b.persist_tables().unwrap();
    drop(syncer_b);
    drop(eng_b);

    // reopen — 逆写像が `.eidmap` sidecar から復元されている。
    let eng_b2 = Engine::open_concurrent_with_oplog(&path_b, 16 * 1024 * 1024).unwrap();
    eng_b2.set_peer_id(2);
    let t_world = eng_b2
        .resolve_remote_eid_existing(e_a)
        .expect("mapping must be restored from .eidmap");
    std::thread::sleep(Duration::from_millis(50));
    eng_b2.tie_to(t_world, "notes.extra", 9);
    settle(&eng_b2);

    let t_b: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_b2 = Syncer::new(eng_b2.clone(), t_b.clone());
    assert!(
        syncer_b2.publish_since(Hlc::ZERO) > 0,
        "write-back after reopen must be published (reverse map restored from sidecar)"
    );
    let records_b = t_b.pull(2, Hlc::ZERO);

    let syncer_a_in = Syncer::new(eng_a.clone(), t_b.clone());
    assert!(syncer_a_in.apply_records(&records_b).applied > 0);
    assert_eq!(
        eng_a.get(e_a, "notes.extra"),
        Some(9),
        "post-reopen write-back must land on A's original entity"
    );

    drop(syncer_a); drop(syncer_a_in); drop(syncer_b2);
    drop(eng_a); drop(eng_b2);
    cleanup(&path_a); cleanup(&path_b);
}

#[test]
fn lww_concurrent_same_card_converges() {
    let path_a = tmp_path("lww_a");
    let path_b = tmp_path("lww_b");

    // A: note=100 を作成 → B に届ける。
    let eng_a = make_engine(&path_a, 1);
    let e_a = eng_a.entity_in("notes").unwrap();
    eng_a.tie_to(e_a, "notes.note", 100);
    settle(&eng_a);

    let t_a: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), t_a.clone());
    syncer_a.publish_since(Hlc::ZERO);
    let records_a = t_a.pull(1, Hlc::ZERO);

    let eng_b = make_engine(&path_b, 2);
    let syncer_b_in = Syncer::new(eng_b.clone(), t_a.clone());
    syncer_b_in.apply_records(&records_a);
    let t_world = eng_b.resolve_remote_eid_existing(e_a).unwrap();

    // B が同じ card (note) を後から 200 に上書き → 時間 (HLC) が B を勝たせる。
    std::thread::sleep(Duration::from_millis(50));
    eng_b.tie_to(t_world, "notes.note", 200);
    settle(&eng_b);

    let t_b: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_b_out = Syncer::new(eng_b.clone(), t_b.clone());
    syncer_b_out.publish_since(Hlc::ZERO);
    let records_b = t_b.pull(2, Hlc::ZERO);

    let syncer_a_in = Syncer::new(eng_a.clone(), t_b.clone());
    assert!(syncer_a_in.apply_records(&records_b).applied > 0);
    assert_eq!(
        eng_a.get(e_a, "notes.note"),
        Some(200),
        "newer write (B) must win LWW on A"
    );
    assert_eq!(eng_b.get(t_world, "notes.note"), Some(200));

    // 逆方向: A がさらに後から 300 → B も 300 に収束 (双方向編集の round-trip)。
    std::thread::sleep(Duration::from_millis(50));
    eng_a.tie_to(e_a, "notes.note", 300);
    settle(&eng_a);
    // InMemoryTransport は一度 publish すると自 peer が known_peers に載り、
    // 2 回目の publish_since が per-peer 経路 (自分以外に peer なし = 0 件) に
    // 乗ってしまうため、 round ごとに fresh transport を使う (既存 sync test の流儀)。
    let t_a2: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_a2 = Syncer::new(eng_a.clone(), t_a2.clone());
    assert!(syncer_a2.publish_since(Hlc::ZERO) > 0);
    let records_a2 = t_a2.pull(1, Hlc::ZERO);
    let syncer_b_in2 = Syncer::new(eng_b.clone(), t_a2.clone());
    assert!(syncer_b_in2.apply_records(&records_a2).applied > 0);
    assert_eq!(
        eng_b.get(t_world, "notes.note"),
        Some(300),
        "even-newer write (A) must win LWW on B — 同時書き込みは時間が解決する"
    );

    drop(syncer_a); drop(syncer_a2); drop(syncer_a_in); drop(syncer_b_in);
    drop(syncer_b_in2); drop(syncer_b_out);
    drop(eng_a); drop(eng_b);
    cleanup(&path_a); cleanup(&path_b);
}

#[test]
fn ref_value_to_replica_stays_local() {
    fn make_ref_engine(path: &str, peer: PeerId) -> Arc<Engine> {
        cleanup(path);
        let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
        eng.define_table("companies", 1000).unwrap();
        eng.define_himo_in("companies", "cid", ValueType::Number, 0).unwrap();
        eng.define_table("users", 1000).unwrap();
        eng.define_himo_in("users", "uid", ValueType::Number, 0).unwrap();
        eng.define_ref_in("users", "company", "companies").unwrap();
        eng.enable_sync_tables().unwrap();
        let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
        eng.set_peer_id(peer);
        eng
    }

    let path_a = tmp_path("ref_a");
    let path_b = tmp_path("ref_b");
    let path_c = tmp_path("ref_c");

    // A: company を産む → B に届ける。
    let eng_a = make_ref_engine(&path_a, 1);
    let c_a = eng_a.entity_in("companies").unwrap();
    eng_a.tie_to(c_a, "companies.cid", 7);
    settle(&eng_a);

    let t_a: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), t_a.clone());
    syncer_a.publish_since(Hlc::ZERO);
    let records_a = t_a.pull(1, Hlc::ZERO);

    // B: 自分の user を作り、 uid (発送されるべき) + A company のレプリカへの
    // ref (発送されては**いけない**) を tie する。
    let eng_b = make_ref_engine(&path_b, 2);
    let syncer_b_in = Syncer::new(eng_b.clone(), t_a.clone());
    assert!(syncer_b_in.apply_records(&records_a).applied > 0);
    let c_replica = eng_b.resolve_remote_eid_existing(c_a).unwrap();

    let u_b = eng_b.entity_in("users").unwrap();
    eng_b.tie_to(u_b, "users.uid", 42);
    eng_b.tie_ref_to(u_b, "users.company", c_replica);
    settle(&eng_b);

    let t_b: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_b_out = Syncer::new(eng_b.clone(), t_b.clone());
    syncer_b_out.publish_since(Hlc::ZERO);
    let records_b = t_b.pull(2, Hlc::ZERO);
    assert!(!records_b.is_empty(), "uid tie must be published");

    // C: B の stream を apply。 uid は届くが、 レプリカ宛 ref は guard されて
    // 届かない (= 誤った宛名で断片を作るくらいなら発送しない)。
    let eng_c = make_ref_engine(&path_c, 3);
    let syncer_c = Syncer::new(eng_c.clone(), t_b.clone());
    assert!(syncer_c.apply_records(&records_b).applied > 0);
    let u_c = eng_c
        .resolve_remote_eid_existing(u_b)
        .expect("B's user must arrive on C");
    assert_eq!(eng_c.get(u_c, "users.uid"), Some(42), "uid card must arrive");
    assert_eq!(
        eng_c.get(u_c, "users.company"),
        None,
        "ref-to-replica must NOT be propagated (request10 follow-up until wire \
         can carry a foreign entity id in the value)"
    );
    // B のローカルでは ref は生きている (local-only)。
    assert_eq!(
        eng_b.get(u_b, "users.company").map(|v| v as u64),
        Some(enchudb_oplog::eid_local(c_replica) as u64),
        "the ref stays usable locally on B"
    );

    drop(syncer_a); drop(syncer_b_in); drop(syncer_b_out); drop(syncer_c);
    drop(eng_a); drop(eng_b); drop(eng_c);
    cleanup(&path_a); cleanup(&path_b); cleanup(&path_c);
}
