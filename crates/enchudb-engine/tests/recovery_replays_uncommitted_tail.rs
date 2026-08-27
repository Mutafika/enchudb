//! **WAL に durable に届いた write は、 body に反映されないまま checkpoint に
//! 埋められてはいけない。**
//!
//! concurrent write path は queue を 2 本持つ (`engine.rs` の `tie_async_by_id`):
//! producer は op を `write_queue` へ、 record を `oplog_record_queue` へ push し、
//! consumer thread が 1 tick の中で **(1) WAL append → (2) body 適用 → (3) fsync /
//! msync / checkpoint 前進** の順に流す。 つまり **WAL は body より先に書かれる**。
//!
//! (1) と (2) の間で殺されると 「WAL には在るが body には無い record」 が末尾に
//! 残る。 これ自体は crash として正常で、 次の open の recovery が replay して
//! 埋めるのが筋。 ところが旧実装は
//!
//! ```ignore
//! let records = w.recover();          // 未 commit tail は捨てる
//! for rec in &records { eng.apply_oplog_op(..) }
//! w.advance_checkpoint(w.head());     // ← replay していない tail まで越える
//! ```
//!
//! と、 **replay しなかった tail を checkpoint で越えて**いた。 越えられた record は
//! 以後どの scan からも見えず、 body に反映されないまま恒久的に失われる。
//!
//! `advance_checkpoint` を committed_end に留めるだけでは直らない: 走行中の engine は
//! 誰もその record を body に適用しないので、 次の周期 fsync が Commit を打って
//! checkpoint を再び越える。 しかも Commit が付いた時点で `_sync_ops` へ bridge される
//! ので、 **body に無いものを相手に配る**状態が確定する。 だから recovery で
//! 適用しきる (`OpLog::recover_with_tail`)。
//!
//! 実地 (syncretic の chaos soak / SIGKILL 混じり) では、 9 cell を 1 行として書く
//! insert が 「著者側の body には 2 cell、 相手には 3 cell」 で固まり、 以後の scan でも
//! 埋まらない行として残った。 PK cell が欠けた行は PK 引きに掛からないので、 次の scan が
//! 同じ行をもう一度 insert し、 同一 PK の entity が 2 つになる。
//!
//! ここでは crash 相当を **WAL を直接叩いて**決定的に作る (consumer thread の timing に
//! 依存しない)。

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::oplog::{Op, OpLog};
use enchudb_oplog::Hlc;
use std::path::Path;
use std::sync::Arc;

const CAP: usize = 8 * 1024 * 1024;
const PEER: u32 = 42;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-recovery-tail-{}-{}-{}.db",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn fresh(path: &str) {
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap", ".schema"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

/// crash 相当: WAL にだけ record を置く (body 未適用 / 閉じの Commit 無し)。
fn append_orphan_record(path: &str, local_eid: u32, himo_id: u16, value: u32, wall: u64) {
    let wal = OpLog::open(Path::new(&format!("{path}.oplog"))).expect("open wal");
    let oplog_eid = enchudb_oplog::make_eid(wal.peer_id(), local_eid);
    wal.append_at_hlc(
        Op::Tie { eid: oplog_eid, himo_id, value },
        Hlc { wall, logical: 0, peer: PEER },
    )
    .expect("append");
}

#[test]
fn a_record_the_body_never_got_is_replayed_not_buried() {
    let path = tmp_path("replay");
    fresh(&path);

    // 1. baseline を durable にする (checkpoint がここまで前進する)。
    let (eid, himo_id) = {
        let mut eng = Engine::create_with_capacity(&path, 256).unwrap();
        eng.define_himo("v", ValueType::Number, 0);
        let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, CAP).unwrap();
        eng.set_peer_id(PEER);
        let e = eng.entity().unwrap();
        let hid = eng.himo_id("v").expect("himo") as u16;
        eng.tie_async(e, "v", 111);
        eng.flush_writes();
        eng.oplog_sync().expect("durable");
        (e, hid)
    }; // graceful drop

    // 2. crash 相当。 WAL に Tie(222) だけが在り、 body は 111 のまま。
    append_orphan_record(&path, enchudb_oplog::eid_local(eid), himo_id, 222, u64::MAX / 2);

    // 3. reopen = recovery。 WAL に届いていた write が body に入ること。
    {
        let eng = Engine::open_concurrent_with_oplog(&path, CAP).expect("reopen");
        assert_eq!(
            eng.get(eid, "v"),
            Some(222),
            "WAL に durable に届いていた write が body に反映されていない \
             (recovery が未 commit tail を捨て、 checkpoint がそれを越えた)"
        );
    }

    // 4. もう一度開く。 恒久化されていること (3 で見えただけ、 ではない)。
    {
        let eng = Engine::open_concurrent_with_oplog(&path, CAP).expect("reopen 2");
        assert_eq!(
            eng.get(eid, "v"),
            Some(222),
            "recovery で埋めた値が次の open で消えている"
        );
    }

    fresh(&path);
}
