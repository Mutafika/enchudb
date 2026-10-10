//! #449: WAL が満杯で載らなかった write (#57) の floor は、 閉じる時に上げる。
//!
//! floor を上げるのは満杯の episode が終わった周 (落ちなくなって 100 ms) か、 最初に落ちてから 5 秒後で、 それまで
//! 落ちたことはメモリにしか無い。 旧: 閉じる時 (consumer の最後の bridge) もその条件で待ったので、 落ちてすぐ閉じると
//! floor が残らず、 落ちた write は本体にあるのに相手に永久に届かなかった (bootstrap にもならない)。
//!
//! process の死 / 電源断で失われる分は #451 (本体にあって oplog に無い write) で扱う。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue449-{}-{}-{}",
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

fn has_note(eng: &Engine, i: u32) -> bool {
    !eng.pull_raw("notes.note", i).is_empty()
}

#[test]
fn writes_dropped_right_before_close_reach_the_peer() {
    let (pa, pb) = (tmp_path("a"), tmp_path("b"));
    let mem = Arc::new(InMemoryTransport::new());
    mem.register_peer(1);
    mem.register_peer(2);
    let transport: Arc<dyn Transport> = mem.clone();
    let eng_b = make_engine(&pb, 2, 16 * 1024 * 1024);
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());

    let eng_a = make_engine(&pa, 1, 64 * 1024);
    let hid = eng_a.himo_id("notes.note").unwrap() as u16;
    let wal = eng_a.oplog().unwrap().clone();
    // B は差分 pull で追従している
    {
        let sync_a = Syncer::new(eng_a.clone(), transport.clone());
        let e = eng_a.entity_in("notes").unwrap();
        eng_a.tie_text_to(e, "notes.body", "first");
        eng_a.oplog_sync().unwrap();
        eng_a.transfer_oplog_to_sync_ops();
        sync_a.publish_since(Hlc::ZERO);
        let out = sync_b.pull_once(1);
        assert!(out.applied > 0 && !out.history_truncated, "{out:?}");
    }

    // 満杯の書き手を待たせず (#388) 畳ませずに、 WAL に載らない write を作る
    wal.set_room_waiter(None);
    let mut dropped = Vec::new();
    {
        let _no_fold = eng_a.transfer_lock_for_fold();
        // 閉じた group で WAL を埋め、 残りを 100 B (Tie 128 B も Commit 112 B も入らない) にする。 TieLeaf の record は
        // 文字列の長さ + c B
        let e0 = eng_a.entity_in("notes").unwrap();
        let h0 = wal.head();
        eng_a.tie_text_to(e0, "notes.body", "x");
        let c = wal.head() - h0 - 1;
        eng_a.oplog_commit();
        let pad = wal.free_bytes() - 112 - 100 - c;
        let e1 = eng_a.entity_in("notes").unwrap();
        eng_a.tie_text_to(e1, "notes.body", &"p".repeat(pad as usize));
        eng_a.oplog_commit();
        assert_eq!(wal.free_bytes(), 100, "前提: 残り 100 B");
        for i in 0..20u32 {
            let head = wal.head();
            let e = eng_a.entity_in("notes").unwrap();
            eng_a.tie_to(e, "notes.note", i);
            assert_eq!(wal.head(), head, "前提: WAL に載らない");
            dropped.push(eng_a.cell_hlc(e, hid));
        }
    }
    let max_dropped = dropped.into_iter().max().unwrap();
    // 落ちてすぐ閉じる (満杯の episode の終わりを待たない)
    drop((wal, eng_a));

    let eng_a = Engine::open_concurrent_with_oplog(&pa, 64 * 1024).unwrap();
    let floor = eng_a.sync_reclaimed_floors().unwrap_or_default().into_iter().find(|(a, _)| *a == 1).map(|(_, h)| h);
    assert!(floor.is_some_and(|f| f >= max_dropped), "閉じた後の floor {floor:?} が落ちた write {max_dropped:?} を覆わない");
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    sync_a.serve_state();
    eng_a.oplog_sync().unwrap();
    eng_a.transfer_oplog_to_sync_ops();
    sync_a.publish_since(Hlc::ZERO);
    let out = sync_b.pull_once(1);
    assert!(out.history_truncated, "落ちた write の穴を知らされない: {out:?}");
    sync_b.bootstrap_pull(1).expect("serve_state 済み");
    let missing: Vec<u32> = (0..20).filter(|i| !has_note(&eng_b, *i)).collect();
    assert!(missing.is_empty(), "B に届いていない note {missing:?}");

    drop((sync_a, sync_b, eng_a, eng_b));
    cleanup(&pa);
    cleanup(&pb);
}
