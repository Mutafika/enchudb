//! #970 (PR #977 の読み直し 3 の [1]): 受信の払い出しが、 場所の世代を書く gen.seg を伸ばせない (ディスクの空き不足) 時、 record を
//! 落とさず (`Dropped` = cursor が越えて二度と届かない)、 容量の断り (`RejectedCapacity`) にして再配送させる。 空きが戻れば同じ record
//! が届き直して当たる (#210 と同じ扱い)。
//!
//! 2 つの形:
//!
//! - v15 の受け手で、 新しい行の場所が gen.seg の commit の外の page にある (案 P: 生かす前にその page を commit する)
//! - v14 の受け手で、 満杯の表に v14 の間に消した場所がある (受けた行を置くには使い回す = 先に v15 に移る。 移る手順が gen.seg を
//!   commit できない)。 前は移る手順の断りが確保先なし (`Skip`) に潰れて `Dropped` になった (master では移って当たっていた)
//!
//! 空きの不足は `set_space_margin` で作る (#200 の fault injection)。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;

const NO_SPACE: u64 = u64::MAX / 2;

fn tmp(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/enchudb-issue970-sync-{tag}-{}-{nanos}.db", std::process::id())
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    let _ = std::fs::remove_file(format!("{path}.oplog"));
}

/// 両方の peer の表: `u` (枠 1、 最初の使い回し用)、 `pad` (払い出さない、 `notes` を gen.seg の先の page に置く)、 `notes` (列 `n`)。
/// 紐の番号を揃えるため、 定義の順は両方同じ。 `fill` なら残りの eid 空間を表 `rest` で使い切る (`notes` は枠を足せない)。
fn make(path: &str, peer: PeerId, notes: u32, fill: bool) -> Arc<Engine> {
    let mut e = Engine::create_with_capacity(path, 400_000).unwrap();
    e.define_table("u", 1).unwrap();
    e.define_himo_in("u", "x", ValueType::Number, 0).unwrap();
    e.define_table("pad", 100_000).unwrap();
    e.define_table("notes", notes).unwrap();
    e.define_himo_in("notes", "n", ValueType::Number, 0).unwrap();
    e.enable_sync_tables().unwrap();
    if fill {
        let rest = e.remaining_eid_capacity();
        e.define_table("rest", rest).unwrap();
        assert_eq!(e.remaining_eid_capacity(), 0, "前提: 枠を足せない");
    }
    let e = Engine::concurrentize_with_oplog(e, 16 * 1024 * 1024).unwrap();
    e.set_peer_id(peer);
    e
}

/// A が `notes` に n = 7 の行を作って配る。 B は空きの無い間に受けて断り (再配送させる)、 空きが戻った後に同じ record を受けて当てる。
fn refused_then_redelivered(a: &Arc<Engine>, b: &Arc<Engine>) {
    let mem = Arc::new(InMemoryTransport::new());
    mem.register_peer(1);
    mem.register_peer(2);
    let transport: Arc<dyn Transport> = mem.clone();
    let sa = Syncer::new(a.clone(), transport.clone());
    let sb = Syncer::new(b.clone(), transport.clone());

    let ea = a.entity_in("notes").unwrap();
    a.tie_to(ea, "notes.n", 7u32);
    a.oplog_sync().unwrap();
    assert!(sa.publish_since(Hlc::ZERO) > 0, "前提: A が配れた");

    b.set_space_margin(NO_SPACE);
    let out = sb.pull_once(1);
    assert_eq!(out.dropped_unresolved, 0, "空きの不足で払い出せない record を落とした (二度と届かない): {out:?}");
    assert!(out.rejected_capacity >= 1, "容量の断りとして数えていない: {out:?}");
    assert!(out.min_rejected_hlc.is_some(), "cursor を止めていない = この record は二度と届かない: {out:?}");
    assert!(b.pull_raw("notes.n", 7).is_empty(), "断ったのに行が当たった");

    b.set_space_margin(0);
    sa.publish_since(Hlc::ZERO);
    let out = sb.pull_once(1);
    assert!(out.applied >= 1, "空きが戻っても届き直さない = 失われた: {out:?}");
    let rows = b.pull_raw("notes.n", 7);
    assert_eq!(rows.len(), 1, "届き直した行が当たっていない: {rows:?}");
    assert!(b.is_live_unchecked(rows[0]));
}

/// v15 の受け手: 新しい行の場所 (`notes` の先頭、 場所 100001) は移る時の commit (`u` の場所 0 の page と先回り) の外。
#[test]
fn a_received_row_whose_gen_seg_page_cannot_be_committed_is_redelivered() {
    let (pa, pb) = (tmp("v15-a"), tmp("v15-b"));
    let a = make(&pa, 1, 64, false);
    let b = make(&pb, 2, 64, false);
    let u = b.entity_in("u").unwrap();
    b.delete(u);
    let u2 = b.entity_in("u").unwrap();
    assert!(b.slot_bits() > 0 && u2 != u, "前提: B は v15");
    refused_then_redelivered(&a, &b);
    drop((a, b));
    cleanup(&pa);
    cleanup(&pb);
}

/// v14 の受け手: 満杯の `notes` (枠 2) に v14 の間に消した場所がある。 受けた行は使い回すので、 先に v15 に移る (移る手順は gen.seg を
/// commit できない)。
#[test]
fn a_received_row_that_needs_the_move_to_v15_is_redelivered_when_the_move_cannot_commit() {
    let (pa, pb) = (tmp("v14-a"), tmp("v14-b"));
    let a = make(&pa, 1, 2, false);
    let b = make(&pb, 2, 2, true);
    let mine: Vec<u64> = (0..2u32)
        .map(|i| {
            let e = b.entity_in("notes").unwrap();
            b.tie_to(e, "notes.n", 100 + i);
            e
        })
        .collect();
    b.delete(mine[0]);
    assert_eq!(b.slot_bits(), 0, "前提: B は v14");
    refused_then_redelivered(&a, &b);
    assert!(b.slot_bits() > 0, "前提: 空きが戻った後に移った");
    assert!(b.is_live(mine[1]) && !b.is_live(mine[0]));
    drop((a, b));
    cleanup(&pa);
    cleanup(&pb);
}
