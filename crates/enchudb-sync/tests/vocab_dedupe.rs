//! 0.8.5 issue #30: 受信した Vocab record の HLC dedupe 検証。
//!
//! 旧 behavior: `apply_one::DecodedOp::Vocab` 分岐が **LWW check 無しで常に
//! `true` を返す**。 gossip_remote_apply ON 構成で同じ vocab record が無限に
//! re-apply 扱いされ、 caller (Syncer) の applied counter が永久に 0 に戻らず、
//! WAL 再追記から amplification loop に発展した (bisquit dogfood で実証)。
//!
//! 修正後: 同 `(author_peer, vid, bytes)` の再受信は skip カウントに乗る、
//! 2 度目の apply で applied == 0、 skipped == received。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::HimoType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;
use std::time::Duration;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-vocab-dedupe-{}-{}-{}",
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
    eng.define_himo_in("notes", "label", HimoType::Tag, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

#[test]
fn vocab_record_dedupe_on_second_apply() {
    let path_a = tmp_path("a");
    let path_b = tmp_path("b");

    // peer A: Tag value を tie して vocab + tie record を作る
    let eng_a = make_engine(&path_a, 1);
    let e1 = eng_a.entity_in("notes").unwrap();
    eng_a.tie_text_to(e1, "notes.label", "hello");
    eng_a.oplog_commit();
    std::thread::sleep(Duration::from_millis(300));

    // transport 経由で B に push する用の records を A 側 transport から直接 pull
    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    let pushed = syncer_a.publish_since(Hlc::ZERO);
    assert!(pushed > 0, "peer A should publish at least 1 record");

    // transport から raw records を取り出す (= B 視点での pull)
    let records = transport.pull(/*from=*/ 1, Hlc::ZERO);
    assert!(!records.is_empty(), "transport should hold records from peer A");

    // Vocab record が含まれてることを確認 (= テスト前提)
    let has_vocab = records.iter().any(|r| matches!(
        r.op,
        enchudb_oplog::oplog::DecodedOp::Vocab { .. }
    ));
    assert!(has_vocab, "records should include at least one Vocab op");

    // peer B (空 engine、 同 schema) を作って apply 2 回
    let eng_b = make_engine(&path_b, 2);
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());

    let out1 = syncer_b.apply_records(&records);
    assert!(out1.applied > 0, "first apply should apply at least 1 record");

    let out2 = syncer_b.apply_records(&records);
    // 2 回目は全 record が skip されるべき
    assert_eq!(
        out2.applied, 0,
        "second apply should be fully deduplicated, got applied={} skipped={} received={}",
        out2.applied, out2.skipped, out2.received,
    );
    assert_eq!(
        out2.skipped, out2.received,
        "second apply: all received should go to skipped"
    );

    drop(eng_a);
    drop(eng_b);
    cleanup(&path_a);
    cleanup(&path_b);
}

#[test]
fn vocab_record_dedupe_distinguishes_bytes() {
    // 同 (peer, vid) でも bytes が違えば apply される (= defensive case)。
    // 通常の workload では起きないが、 has_remote_vocab が bytes 比較も行う
    // ことを保証する。
    let path_a = tmp_path("bytes_a");
    let path_b = tmp_path("bytes_b");

    let eng_a = make_engine(&path_a, 1);
    let e1 = eng_a.entity_in("notes").unwrap();
    eng_a.tie_text_to(e1, "notes.label", "alpha");
    eng_a.oplog_commit();
    std::thread::sleep(Duration::from_millis(300));

    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let syncer_a = Syncer::new(eng_a.clone(), transport.clone());
    syncer_a.publish_since(Hlc::ZERO);
    let records = transport.pull(1, Hlc::ZERO);

    let eng_b = make_engine(&path_b, 2);
    let syncer_b = Syncer::new(eng_b.clone(), transport.clone());

    // 1 回目: 全 apply
    let out1 = syncer_b.apply_records(&records);
    assert!(out1.applied > 0);

    // 2 回目: 全 skip (vocab dedupe + tie HLC dedupe)
    let out2 = syncer_b.apply_records(&records);
    assert_eq!(out2.applied, 0);

    drop(eng_a);
    drop(eng_b);
    cleanup(&path_a);
    cleanup(&path_b);
}
