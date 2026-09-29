//! #160: reopen 後、 配送バッファ (`_sync_ops`) が reclaim 済みでも、 ローカルのより新しい行が古い record で
//! 巻き戻らないこと。
//!
//! 旧版の LWW の記憶 (`HlcStore`) は揮発で、 reopen 後は `_sync_ops` を歩いて作り直していた。 全 peer が ack して
//! reclaim された範囲の版数はどこにも残らず、 cursor を持たない pull (cursor ファイルの喪失 / store のコピー /
//! 新しい peer の ZERO pull) で相手の ring に残る古い record が素通しで当たり、 新しい行が巻き戻った。
//!
//! v9 の DB (`create_with_cell_version`) は cell の版数を列に永続するので、 作り直しに頼らない。 ここは
//! **新品の transport** (= 配送の履歴ゼロ、 reclaim 済みと同じ) で reopen 後に pull する。 削除の方
//! (tombstone) は `tombstone_survives_reopen.rs` (#140)。
//!
//! 版数の列を見ずに揮発の `HlcStore` を引く変異 (`version_of` の分岐を外す) で落ちることは実測。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport, WireRecord};
use enchudb_engine::ValueType;
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::Hlc;
use enchudb_sync::Syncer;
use std::sync::Arc;

const CAP: usize = 16 * 1024 * 1024;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue160-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path); // v10: DB は directory
    for s in ["", ".oplog", ".tables", ".crc", ".lock", ".db.lock", ".eidmap", ".vocabmap", ".schema"] {
        let _ = std::fs::remove_file(format!("{path}{s}"));
    }
}

/// peer A (= 1) の record。
fn rec(wall: u64, op: DecodedOp) -> WireRecord {
    WireRecord::unsigned(Hlc { wall, logical: 0, peer: 1 }, 1, op)
}

#[test]
fn newer_local_row_is_not_rolled_back_after_reclaim_and_reopen() {
    let path = tmp_path("B");
    cleanup(&path);
    let foreign = enchudb_oplog::make_eid(1, 0);

    // ── 1. A の行を受けて、 B がローカルで書き換える。 配送バッファは reclaim まで回す ──
    let (hid, row) = {
        let mut e = Engine::create_with_cell_version(&path, 65_536).unwrap();
        e.define_table("notes", 1000).unwrap();
        e.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
        e.enable_sync_tables().unwrap();
        let b = Engine::concurrentize_with_oplog(e, CAP).unwrap();
        b.set_peer_id(2);
        assert!(b.has_cell_version(), "前提: v9 (版数を永続する) DB であること");
        let hid = b.himo_id("notes.note").unwrap() as u16;

        let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
        let sb = Syncer::new(b.clone(), transport.clone());
        transport.publish(1, vec![rec(1000, DecodedOp::Tie { eid: foreign, himo_id: hid, value: 111 })]);
        assert_eq!(sb.pull_once(1).applied, 1, "A の Tie が apply されていない");
        let row = enchudb_oplog::make_eid(2, *b.pull("notes.note", 111).first().expect("A の行が入っていない"));

        // B のローカルな書き換え (版数は B の今 = A の record よりずっと新しい)
        b.tie_to(row, "notes.note", 222u32);
        b.flush_writes();
        b.oplog_commit();
        b.oplog_sync().unwrap();
        b.transfer_oplog_to_sync_ops();
        assert!(b.min_sync_ops_lsn().is_some(), "前提: B の書き換えが配送バッファに載っていない");
        // 全 peer が読み終えた = 配送バッファから消える (#160 の 「記憶の source が無くなる」 状態)
        b.ack_sync(1, b.current_sync_lsn() + 1).unwrap(); // consumed は 「次に読む lsn」
        b.reclaim_sync_ops();
        assert_eq!(b.min_sync_ops_lsn(), None, "前提: 配送バッファが reclaim されていない");

        b.persist_tables().unwrap();
        b.body_msync().unwrap();
        (hid, row)
    };

    // ── 2. reopen。 transport は新品 (cursor 無しの pull と同じ) ──
    let b2 = Engine::open_concurrent_with_oplog(&path, CAP).unwrap();
    b2.set_peer_id(2);
    assert_eq!(b2.get(row, "notes.note"), Some(222), "前提: reopen で B の書き換えが残っていない");
    let transport2: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let sb2 = Syncer::new(b2.clone(), transport2.clone());

    // ── 3. A の古い record (同じもの + B の書き換えより古い別の値) が届く ──
    transport2.publish(
        1,
        vec![
            rec(1000, DecodedOp::Tie { eid: foreign, himo_id: hid, value: 111 }),
            rec(1500, DecodedOp::Tie { eid: foreign, himo_id: hid, value: 333 }),
        ],
    );
    let out = sb2.pull_once(1);
    assert_eq!(out.applied, 0, "B の書き換えより古い record が当たった (received={})", out.received);
    assert_eq!(
        b2.get(row, "notes.note"),
        Some(222),
        "reclaim + reopen の後、 ローカルのより新しい行が古い record で巻き戻った (#160)"
    );

    drop((sb2, b2));
    cleanup(&path);
}
