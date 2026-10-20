//! #568: 受け手は署名を `signed_bytes` で確かめ、 当てるのは wire の別の欄 (op・HLC・author) だった。 両者が同じ record かを
//! 見ていなかったので、 本物の署名付き record の `signed_bytes` と `signature` を残して欄だけ書き換えた record が、
//! `require_signature` でも通って当たった。
//!
//! 署名を付けた oplog で書いた本物の record を、 欄を 1 つずつ書き換えて当てる。 書き換えた record は `rejected_signature`
//! で、 何も当たらない。 書き換えない record は当たる (対照)。

use enchudb_engine::ValueType;
use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport, WireRecord};
use enchudb_oplog::Hlc;
use enchudb_oplog::oplog::{DecodedOp, Op, OpLog};
use enchudb_sync::Syncer;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue568-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

#[test]
fn a_record_whose_fields_differ_from_its_signed_bytes_is_rejected() {
    let path = tmp_path("db");
    let _ = std::fs::remove_dir_all(&path);
    let mut eng = Engine::create_with_cell_version(&path, 65_536).unwrap();
    eng.define_table("t", 1000).unwrap();
    eng.define_himo_in("t", "n", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 << 20).unwrap();
    eng.set_peer_id(2);
    let n = eng.himo_id("t.n").unwrap() as u16;

    // 署名を付けた author (peer 1) の record: 行 (1, 0) の n = 5
    let kp = Arc::new(enchudb_oplog::keys::Keypair::generate());
    let wal_path = tmp_path("author");
    let author = OpLog::create(std::path::Path::new(&wal_path), 1 << 20).unwrap();
    author.set_peer_id(1);
    author.set_keypair(Some(kp.clone()));
    let row = enchudb_oplog::make_eid(1, 0);
    author.append(Op::Tie { eid: row, himo_id: n, value: 5 }).unwrap();
    author.append(Op::Commit).unwrap();
    let genuine: WireRecord = author.iter_committed().into_iter().map(WireRecord::from).next().unwrap();
    assert_ne!(genuine.signature, [0u8; 64], "前提: 署名付き");
    eng.pubkeys().force_register(1, &kp.public_bytes());
    // author の欄の書き換えが署名を通り抜ける形: 同じ鍵を別の peer にも登録している
    eng.pubkeys().force_register(3, &kp.public_bytes());

    let syncer = Syncer::new(eng.clone(), Arc::new(InMemoryTransport::new()) as Arc<dyn Transport>);
    syncer.set_require_signature(true);
    let forged: [(&str, Box<dyn Fn(&mut WireRecord)>); 4] = [
        ("値", Box::new(|r: &mut WireRecord| r.op = DecodedOp::Tie { eid: row, himo_id: n, value: 6 })),
        ("行", Box::new(|r: &mut WireRecord| r.op = DecodedOp::Tie { eid: enchudb_oplog::make_eid(1, 7), himo_id: n, value: 5 })),
        ("HLC", Box::new(|r: &mut WireRecord| r.hlc = Hlc { wall: r.hlc.wall + 1_000_000, ..r.hlc })),
        ("author", Box::new(|r: &mut WireRecord| r.author_peer = 3)),
    ];
    for (what, edit) in &forged {
        let mut r = genuine.clone();
        edit(&mut r);
        let o = syncer.apply_records(&[r]);
        assert_eq!((o.applied, o.rejected_signature), (0, 1), "{what} を書き換えた record が当たった: {o:?}");
    }
    assert_eq!(eng.pull_raw("t.n", 6).len() + eng.pull_raw("t.n", 5).len(), 0, "書き換えた record が当たった");

    // 対照: 書き換えない record は当たる
    let o = syncer.apply_records(&[genuine]);
    assert_eq!((o.applied, o.rejected_signature), (1, 0), "本物の record が当たらない: {o:?}");
    assert_eq!(eng.pull_raw("t.n", 5).len(), 1);

    drop(syncer);
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_file(&wal_path);
}
