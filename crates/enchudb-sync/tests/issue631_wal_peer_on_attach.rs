//! #631: engine の peer id は DB の header に残り、 開き直すと engine は header から戻す。 だが WAL (`OpLog`) に peer id を
//! 渡すのは `Engine::set_peer_id` だけだったので、 次の 2 つの形で自分の書き込みの record が書き手 0 で WAL に載った:
//!
//! 1. WAL を付ける前に `set_peer_id` を呼ぶ (`create*` → `set_peer_id` → `concurrentize_with_oplog`)
//! 2. peer id を決めた DB を開き直し、 `set_peer_id` を呼び直さない
//!
//! 受け手は書き手 0 の record を peer 0 の行として受ける。 同じ行への書き手 1 の書き込みとは別の行になり、 書き手 1 で作った
//! 行の書き換えが相手で新しい行に割れた (書き手は 1 なのに相手は 3 行を持つ)。
//!
//! 直し: WAL を engine に付ける所で engine の peer id を渡す。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::Hlc;
use enchudb_sync::Syncer;
use std::sync::Arc;

const A: u32 = 1;
const B: u32 = 2;

fn tmp(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/enchudb-issue631-{tag}-{}-{nanos}", std::process::id())
}

/// 表 `notes` を持つ sync の DB。 `peer_before_wal` なら WAL を付ける前に peer id を決める (形 1)、 でなければ付けた後。
fn make(path: &str, peer: u32, peer_before_wal: bool) -> Arc<Engine> {
    let mut e = Engine::create_with_cell_version(path, 65_536).unwrap();
    e.define_table("notes", 1000).unwrap();
    e.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    e.enable_sync_tables().unwrap();
    if peer_before_wal {
        e.set_peer_id(peer);
    }
    let e = Engine::concurrentize_with_oplog(e, 16 << 20).unwrap();
    if !peer_before_wal {
        e.set_peer_id(peer);
    }
    e
}

fn write(eng: &Engine, row: Option<u64>, v: u32) -> u64 {
    let e = row.unwrap_or_else(|| eng.entity_in("notes").unwrap());
    eng.tie_to(e, "notes.note", v);
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    e
}

/// B が持つ notes の値ごとの行の数 (10 / 20 / 30)。
fn values(eng: &Engine) -> [usize; 3] {
    [10u32, 20, 30].map(|v| eng.pull_raw("notes.note", v).len())
}

/// B へ配る (`InMemoryTransport` は publish した peer しか知らないので、 宛先を名指す)。
fn publish(eng: &Arc<Engine>, tr: &Arc<dyn Transport>) {
    Syncer::new(eng.clone(), tr.clone()).publish_since_for_peer(B, Hlc::ZERO);
}

/// 最後に WAL に載った record の書き手。
fn last_author(eng: &Engine) -> u32 {
    eng.audit(&enchudb_engine::AuditFilter::default()).iter().rev().find(|r| matches!(r.op, enchudb_oplog::oplog::DecodedOp::Tie { .. })).unwrap().author_peer
}

/// 形 1: WAL を付ける前に peer id を決めた DB の record も、 その peer id で載る。 次の session で `set_peer_id` を呼び直して
/// 書き換えても、 相手で行が割れない。
#[test]
fn a_peer_id_set_before_the_wal_is_attached_reaches_the_wal() {
    let (pa, pb) = (tmp("before-a"), tmp("before-b"));
    let tr: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let a = make(&pa, A, true);
    let b = make(&pb, B, false);
    let row = write(&a, None, 10);
    assert_eq!(last_author(&a), A, "WAL を付ける前に決めた peer id で record が載らない");
    publish(&a, &tr);
    let sb = Syncer::new(b.clone(), tr.clone());
    sb.pull_once(A);
    assert_eq!(values(&b), [1, 0, 0], "前提: 行が届く");
    drop(a);

    let a = Engine::open_concurrent_with_oplog(&pa, 16 << 20).unwrap();
    a.set_peer_id(A);
    write(&a, Some(row), 20);
    publish(&a, &tr);
    let out = sb.pull_once(A);
    assert_eq!(values(&b), [0, 1, 0], "書き換えが相手で別の行に割れた ({out:?})");
    drop((a, b));
    let _ = std::fs::remove_dir_all(&pa);
    let _ = std::fs::remove_dir_all(&pb);
}

/// 形 2: 開き直して `set_peer_id` を呼び直さなくても、 その session の record は header の peer id で載る (書き換えが相手で
/// 別の行に割れない)。
#[test]
fn a_reopened_db_keeps_its_peer_id_in_the_wal() {
    let (pa, pb) = (tmp("reopen-a"), tmp("reopen-b"));
    let tr: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let a = make(&pa, A, false);
    let b = make(&pb, B, false);
    let row = write(&a, None, 10);
    publish(&a, &tr);
    let sb = Syncer::new(b.clone(), tr.clone());
    sb.pull_once(A);
    assert_eq!(values(&b), [1, 0, 0], "前提: 1 回目の session の行は届く");
    drop(a);

    let a = Engine::open_concurrent_with_oplog(&pa, 16 << 20).unwrap();
    assert_eq!(a.peer_id(), A, "前提: engine は header から peer id を戻す");
    write(&a, Some(row), 20);
    write(&a, None, 30);
    assert_eq!(last_author(&a), A, "開き直した session の record が header の peer id で載らない");
    publish(&a, &tr);
    let out = sb.pull_once(A);
    assert_eq!(values(&b), [0, 1, 1], "開き直した session の書き換え (10 → 20) が相手で別の行に割れた / 足した行 (30) が届かない ({out:?})");
    drop((a, b));
    let _ = std::fs::remove_dir_all(&pa);
    let _ = std::fs::remove_dir_all(&pb);
}
