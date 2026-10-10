//! #450: WAL が満杯で閉じの Commit が入らなかった group (孤児) を畳む時、 その write の floor を上げる。
//!
//! 孤児の group は Commit で閉じていないので bridge が `_sync_ops` に写さない。 満杯の死区間では `wal_fold_safe` が
//! 「残りが孤児の group だけなら畳んでよい」 として畳む (#268)。 旧: その write は本体に当たっているのに、 WAL に
//! 載らなかった write (#57) として数えなかったので floor を上げず、 畳んだ後は相手に永久に届かなかった。 後に続いた
//! write が落ちれば自分の author は floor = その時の今 で覆われるが、 ここは 1 件も落とさない (閉じの Commit だけが
//! 入らない) 形で見る。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue450-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

fn make_engine(path: &str, peer: PeerId, oplog_capacity: usize) -> Arc<Engine> {
    cleanup(path);
    let mut eng = Engine::create_with_cell_version(path, 65_536).unwrap();
    eng.define_table("notes", 10_000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, oplog_capacity).unwrap();
    eng.set_peer_id(peer);
    eng
}

fn floor_of(eng: &Engine, author: PeerId) -> Option<Hlc> {
    eng.sync_reclaimed_floors().unwrap_or_default().into_iter().find(|(a, _)| *a == author).map(|(_, h)| h)
}

#[test]
fn a_folded_orphan_group_reaches_the_peer() {
    let (pa, pb) = (tmp_path("a"), tmp_path("b"));
    let mem = Arc::new(InMemoryTransport::new());
    mem.register_peer(1);
    mem.register_peer(2);
    let transport: Arc<dyn Transport> = mem.clone();
    let eng_a = make_engine(&pa, 1, 64 * 1024);
    let eng_b = make_engine(&pb, 2, 16 * 1024 * 1024);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());
    sync_a.serve_state();
    let hid = eng_a.himo_id("notes.note").unwrap() as u16;
    let wal = eng_a.oplog().unwrap().clone();

    // B は差分 pull で追従している
    let e = eng_a.entity_in("notes").unwrap();
    eng_a.tie_text_to(e, "notes.body", "first");
    eng_a.oplog_sync().unwrap();
    eng_a.transfer_oplog_to_sync_ops();
    sync_a.publish_since(Hlc::ZERO);
    let out = sync_b.pull_once(1);
    assert!(out.applied > 0 && !out.history_truncated, "{out:?}");

    // 満杯の書き手を待たせず (#388)、 畳ませずに WAL を埋める
    wal.set_room_waiter(None);
    let (orphan, orphan_hlc);
    {
        let _no_fold = eng_a.transfer_lock_for_fold();
        // 閉じた group で WAL を埋め、 残りを 178 B にする。 TieLeaf の record は文字列の長さ + c B
        let e0 = eng_a.entity_in("notes").unwrap();
        let h0 = wal.head();
        eng_a.tie_text_to(e0, "notes.body", "x");
        let c = wal.head() - h0 - 1;
        eng_a.oplog_commit();
        let pad = wal.free_bytes() - 112 - 178 - c;
        let e1 = eng_a.entity_in("notes").unwrap();
        eng_a.tie_text_to(e1, "notes.body", &"p".repeat(pad as usize));
        eng_a.oplog_commit();
        assert_eq!(wal.free_bytes(), 178, "前提: 残り 178 B");
        // Tie (128 B) は載り、 残り 50 B に閉じの Commit (112 B) は入らない
        orphan = eng_a.entity_in("notes").unwrap();
        let head = wal.head();
        eng_a.tie_to(orphan, "notes.note", 7);
        assert_eq!(wal.head(), head + 128, "前提: WAL に載る");
        assert!(wal.append_dead(), "前提: 閉じの Commit が入らない");
        orphan_hlc = eng_a.cell_hlc(orphan, hid);
    }
    // 死区間では Err を返さない
    eng_a.oplog_sync().unwrap();
    // consumer が死区間の例外で畳み、 孤児の group の write を覆う floor を上げる (落ちが止んで 100 ms 後)
    let end = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while floor_of(&eng_a, 1).is_none_or(|f| f < orphan_hlc) {
        assert!(
            std::time::Instant::now() < end,
            "孤児の group を畳んでも floor {:?} が {orphan_hlc:?} を覆わない (head {}、 floor を上げた回数 {})",
            floor_of(&eng_a, 1),
            wal.head(),
            eng_a.wal_drop_floor_bumps()
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(wal.head() < 32 * 1024, "前提: 畳んでいる (head {})", wal.head());

    sync_a.publish_since(Hlc::ZERO);
    let out = sync_b.pull_once(1);
    assert!(out.history_truncated, "孤児の group の穴を知らされない: {out:?}");
    sync_b.bootstrap_pull(1).expect("serve_state 済み");
    assert!(!eng_b.pull_raw("notes.note", 7).is_empty(), "B に孤児の group の note が届いていない");

    drop((sync_a, sync_b, wal, eng_a, eng_b));
    cleanup(&pa);
    cleanup(&pb);
}
