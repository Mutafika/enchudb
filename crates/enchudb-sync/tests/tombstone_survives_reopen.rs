//! **削除済み entity が reopen 後も復活しない** (request17 Phase 1 step 6/7)。
//!
//! # 何が壊れていたか (#140)
//!
//! tombstone (削除の版数) は揮発 `HlcStore` にしか無く、 その再構築は配送バッファ
//! (`_sync_ops`) の walk (`hydrate_hlc_store`) に依存していた。 つまり
//!
//! - プロセスを再起動する
//! - かつ配送バッファが reclaim 済み (= 古い Delete record がもう無い)
//!
//! の 2 つが揃うと **tombstone が消える**。 そこへ Delete より古い Tie が再配送
//! されると、 比較相手が居ないので素通しで適用され、 削除した entity が蘇る。
//! 下流ではこれが「materialize が消す → scan が復活させる」恒久チャーンになった。
//!
//! # どう直したか
//!
//! v9 で tombstone を eid 空間の column に **永続**させ、 判定を engine の
//! `set_cell` / `remote_*_apply` 側に置いた。 配送バッファからの再構築に依存しない
//! ので、 reopen しても reclaim されても tombstone は残る。
//!
//! この test は **新しい transport (= 履歴ゼロ)** で reopen 後の pull を行う。
//! 配送バッファが完全に消えた状態そのものなので、 hydrate では絶対に救えない。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport, WireRecord};
use enchudb_engine::ValueType;
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::Hlc;
use enchudb_sync::Syncer;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-tombreopen-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for s in ["", ".oplog", ".tables", ".crc", ".lock", ".db.lock", ".eidmap", ".schema"] {
        let _ = std::fs::remove_file(format!("{}{}", path, s));
    }
}

fn rec(wall: u64, op: DecodedOp) -> WireRecord {
    WireRecord::unsigned(Hlc { wall, logical: 0, peer: 1 }, 1, op)
}

#[test]
fn deleted_entity_does_not_resurrect_after_reopen_with_empty_transport() {
    let path = tmp_path("B");
    cleanup(&path);
    let foreign_eid = enchudb_oplog::make_eid(1, 0);

    // ── 1. peer A の Tie → Delete を受けて、 tombstone を持った状態を作る ──
    let hid = {
        let mut e = Engine::create_with_capacity(&path, 65_536).unwrap();
        e.define_table("notes", 1000).unwrap();
        e.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
        e.enable_sync_tables().unwrap();
        let b = Engine::concurrentize_with_oplog(e, 16 * 1024 * 1024).unwrap();
        b.set_peer_id(2);
        assert!(b.has_cell_version(), "前提: v9 (版数を永続する) DB であること");
        let hid = b.himo_id("notes.note").unwrap() as u16;

        let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
        let sb = Syncer::new(b.clone(), transport.clone());
        transport.publish(
            1,
            vec![rec(1000, DecodedOp::Tie { eid: foreign_eid, himo_id: hid, value: 111 })],
        );
        assert_eq!(sb.pull_once(1).applied, 1, "A の Tie が apply されていない");
        assert!(!b.pull("notes.note", 111).is_empty(), "行が入っていない");

        transport.publish(1, vec![rec(2000, DecodedOp::Delete { eid: foreign_eid })]);
        assert_eq!(sb.pull_once(1).applied, 1, "A の Delete が apply されていない");
        assert!(b.pull("notes.note", 111).is_empty(), "Delete が効いていない");

        // eidmap / tables sidecar と本体を固めてから閉じる
        b.persist_tables().unwrap();
        b.body_msync().unwrap();
        hid
    };

    // ── 2. reopen。 transport は **新品** (= 配送履歴ゼロ = reclaim 済みと同じ) ──
    let b2 = Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).unwrap();
    b2.set_peer_id(2);
    let transport2: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let sb2 = Syncer::new(b2.clone(), transport2.clone());

    // ── 3. Delete より古い Tie が再配送される ──
    transport2.publish(
        1,
        vec![rec(1500, DecodedOp::Tie { eid: foreign_eid, himo_id: hid, value: 111 })],
    );
    let out = sb2.pull_once(1);
    b2.rebuild();

    assert_eq!(
        out.applied, 0,
        "削除より古い Tie が適用された (received={}, applied={})",
        out.received, out.applied,
    );
    assert!(
        b2.pull("notes.note", 111).is_empty(),
        "削除済み entity が reopen 後に復活した — tombstone が永続していない (#140)",
    );

    drop(b2);
    cleanup(&path);
}
