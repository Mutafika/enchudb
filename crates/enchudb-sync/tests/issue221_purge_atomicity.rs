//! #221 — `_sync_ops` の purge (delete + ring free list への slot 返却) が並行
//! 実行に対して atomic であること。
//!
//! `Engine::delete` は冪等で戻り値を持たないので、 lock 無しだと 2 thread が同じ row
//! を purge したとき slot が free list に**二重 push** され、 後続の
//! `entity_in("_sync_ops")` が同一 eid を二回払い出して bridge row が silent に
//! 上書きされる。 並行経路は実在する: `Syncer::absorb_pull_acks`
//! (→ `reclaim_sync_ops`) は複数 peer からの並行 pull で並行実行される。
//!
//! 検証:
//! 1. `concurrent_reclaim_does_not_double_free_slots` — 8 thread が同時に
//!    `reclaim_sync_ops` を回した後、 ring を使い切るまで bridge しても eid が
//!    重複払い出しされない (= 同じ eid に 2 つの lsn が乗らない)。
//! 2. `concurrent_reclaim_purge_count_is_exact` — purge の総数が実際に消えた
//!    row 数と一致する (二重計上しない = delete の冪等性に頼らない)。

use enchudb_engine::engine::Engine;
use enchudb_engine::ValueType;
use enchudb_oplog::PeerId;
use std::collections::HashMap;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue221-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

fn make_engine(path: &str, peer: PeerId) -> Arc<Engine> {
    cleanup(path);
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

/// note を n 件書いて ring まで bridge する。
fn author_notes(eng: &Arc<Engine>, from: u32, n: u32) {
    for i in from..from + n {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
}

/// ring の生存 row: eid → lsn。
fn ring_map(eng: &Arc<Engine>) -> HashMap<u64, u32> {
    let lsn_hid = eng.himo_id("_sync_ops.lsn").unwrap() as u16;
    eng.entities_with_himo(lsn_hid)
        .into_iter()
        .filter_map(|eid| eng.get_by_id(eid, lsn_hid).map(|lsn| (eid, lsn)))
        .collect()
}

/// 全 row を watermark の下に置く (= reclaim 対象にする)。
fn ack_everything(eng: &Arc<Engine>, peer: PeerId) {
    eng.ack_sync(peer, eng.current_sync_lsn() + 1).unwrap();
}

#[test]
fn concurrent_reclaim_does_not_double_free_slots() {
    let p = tmp_path("slots");
    let eng = make_engine(&p, 1);

    author_notes(&eng, 1, 60);
    ack_everything(&eng, 2);
    assert!(!ring_map(&eng).is_empty(), "前提: ring に row がある");

    // 8 thread が同時に reclaim (absorb_pull_acks の並行実行を模す)。
    let mut hs = Vec::new();
    for _ in 0..8 {
        let e = eng.clone();
        hs.push(std::thread::spawn(move || e.reclaim_sync_ops()));
    }
    let purged: usize = hs.into_iter().map(|h| h.join().unwrap()).sum();
    assert!(purged > 0, "reclaim が何も消していない");

    // free list を **purge 数より多く** 使い切るまで bridge する。 二重 push が
    // あると free list の長さが purge 数の 2 倍近くになり、 後半の払い出しが前半と
    // 同じ slot を返す → 同一 eid に 2 つの row が乗って前の row が silent に消える。
    // (purge 数ちょうどしか書かないと、 重複 entry が list の後ろに残るだけで
    // 露出しないので、 意図的に 2 倍以上書く。)
    let before = ring_map(&eng);
    let writes = (purged * 2 + 10) as u32;
    author_notes(&eng, 1000, writes);
    let after = ring_map(&eng);

    assert_eq!(
        after.len(),
        before.len() + writes as usize,
        "#221: slot 二重払い出しで bridge row が消えた \
         (before {} + {writes} → after {}; purged {purged})",
        before.len(),
        after.len()
    );

    cleanup(&p);
}

#[test]
fn concurrent_reclaim_purge_count_is_exact() {
    let p = tmp_path("count");
    let eng = make_engine(&p, 1);

    author_notes(&eng, 1, 40);
    ack_everything(&eng, 2);
    let live_before = ring_map(&eng).len();

    let mut hs = Vec::new();
    for _ in 0..8 {
        let e = eng.clone();
        hs.push(std::thread::spawn(move || e.reclaim_sync_ops()));
    }
    let purged: usize = hs.into_iter().map(|h| h.join().unwrap()).sum();
    let live_after = ring_map(&eng).len();

    assert_eq!(
        purged,
        live_before - live_after,
        "purge 総数が実際に消えた row 数と一致しない (二重計上 = delete の冪等性に \
         頼った purge の証拠): purged {purged}, {live_before} → {live_after}"
    );

    cleanup(&p);
}
