//! 64 bit 列 (`ValueType::Number64`) の値が sync で相手に同じ値のまま届く (u32 に入らない値は
//! oplog / wire の Tie64 で運ばれる)。 名前で運ぶ TieNamed の 64 bit 版は wire の往復だけ確かめる
//! (数値の書き込みは紐の番号で運ばれ、 TieNamed は content の紐への文字列だけが使う)。
use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!("/tmp/enchudb-number64-sync-{}-{}", tag, std::process::id())
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

fn make_engine(path: &str, peer: PeerId) -> Arc<Engine> {
    cleanup(path);
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("t", 1000).unwrap();
    eng.define_himo_in("t", "ts", ValueType::Number64, 0).unwrap();
    eng.define_himo_in("t", "n", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

fn publish_all(eng: &Engine, syncer: &Syncer) {
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
    syncer.publish_since(Hlc::ZERO);
}

#[test]
fn number64_values_sync_whole() {
    let (pa, pb) = (tmp_path("a"), tmp_path("b"));
    let (a, b) = (make_engine(&pa, 1), make_engine(&pb, 2));
    let vals = [3u64, u32::MAX as u64, u32::MAX as u64 + 1, 1 << 40, u64::MAX - 1];
    let mut rows = Vec::new();
    for (i, &v) in vals.iter().enumerate() {
        let e = a.entity_in("t").unwrap();
        a.tie_to(e, "t.n", i as u32);
        a.tie_to(e, "t.ts", v);
        rows.push(e);
    }
    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let (sa, sb) = (Syncer::new(a.clone(), transport.clone()), Syncer::new(b.clone(), transport.clone()));
    publish_all(&a, &sa);
    // wire を通す (encode → decode) — InMemoryTransport が素通しでも形式を確かめる
    for r in transport.pull(1, Hlc::ZERO) {
        let (back, _) = enchudb_engine::transport::WireRecord::decode(&r.encode()).unwrap();
        assert_eq!(format!("{:?}", back.op), format!("{:?}", r.op), "wire の往復で op が変わった");
    }
    // TieNamed の 64 bit 版の wire 形式
    for v in [9u64, u32::MAX as u64 + 1, u64::MAX - 1] {
        let r = enchudb_engine::transport::WireRecord::unsigned(
            Hlc { wall: 1, logical: 0, peer: 1 },
            1,
            enchudb_oplog::oplog::DecodedOp::TieNamed { eid: 5, himo_name: "_c_x".into(), himo_kind: 4, value: v },
        );
        let (back, n) = enchudb_engine::transport::WireRecord::decode(&r.encode()).unwrap();
        assert_eq!(n, r.encode().len());
        assert!(matches!(back.op, enchudb_oplog::oplog::DecodedOp::TieNamed { value, .. } if value == v), "TieNamed {v}");
    }
    let out = sb.pull_once(1);
    assert!(out.applied > 0, "何も適用されていない: {out:?}");
    b.flush_writes();
    for (i, &v) in vals.iter().enumerate() {
        let got: Vec<u64> = b.pull_raw("t.n", i as u32);
        assert_eq!(got.len(), 1, "row {i} が届いていない");
        let e = got[0];
        assert_eq!(b.get64(e, "t.ts"), Some(v), "t.ts row {i}");
        assert_eq!(b.pull_raw("t.ts", v), vec![e], "受け手の索引 row {i}");
    }
    drop((sa, sb, a, b));
    cleanup(&pa);
    cleanup(&pb);
}
