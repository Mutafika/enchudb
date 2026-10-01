//! #368: `_sync_ops` (sync の未配送 record の ring) は、 満杯になったら bridge が止まって ack + reclaim を待つ
//! 作りで、 大きさは `enable_sync_tables` が決める (最大 1 M 行)。 ところが 0.26.0 で入った枠の自動追加
//! (v10 Phase 3) が ring にも効いていて、 ack が来ない間、 どの table にも割り当てていない eid 空間を全部
//! 取るまで止まらなかった。 取られた後は user の table が枠を足せない。
//!
//! 直す前の実測 (entity cap 1024、 `notes` の枠 8、 ring の最初の枠 508、 残りの eid 空間 445):
//!
//! - 滞留なし: `notes` に 453 行入る (8 + 445)
//! - ack なしで `notes` の 1 行を 1000 回書いた後: ring が `[(8, 516), (579, 1024)]` に伸びて残りは 0、
//!   `notes` には 8 行しか入らない
//!
//! bridge は consumer thread も回しているが、 ここでは `transfer_oplog_to_sync_ops` を自分でも呼んで
//! 「もう運べない」 所まで進めてから見る (呼び出しは lock で直列、 返った時点で満杯か読み切りのどちらか)。

use enchudb_engine::{Engine, ValueType};
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue368-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path); // v10: DB は directory
    for suffix in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap", ".schema"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

/// entity cap 1024、 `notes` (枠 8) + sync の table。
fn open(path: &str) -> Arc<Engine> {
    cleanup(path);
    let mut eng = Engine::create_with_capacity(path, 1024).unwrap();
    eng.define_table("notes", 8).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap()
}

/// ack なしで `e` の 1 列を `n` 回書き、 bridge をこれ以上運べない所まで進める。
fn write_unacked(eng: &Engine, e: u64, n: u32) {
    for i in 0..n {
        eng.tie_to(e, "notes.note", i);
        if i % 32 == 31 {
            eng.oplog_commit();
        }
    }
    eng.oplog_commit();
    while eng.transfer_oplog_to_sync_ops() > 0 {}
}

fn bridge_caught_up(eng: &Engine) -> bool {
    eng.sync_ops_bridge_offset() >= eng.oplog_head()
}

/// `notes` に、 `Err` が返るまで行を足す。 足せた数。
fn fill_notes(eng: &Engine) -> u32 {
    let mut n = 0;
    while eng.entity_in("notes").is_ok() {
        n += 1;
        assert!(n <= 2000, "notes が entity cap を超えて入った");
    }
    n
}

/// ack が来ないまま書き続けても、 ring は最初の枠のままで、 空いている eid 空間に手を付けない。
/// user の table はその空間を使って伸びられる。
#[test]
fn unacked_backlog_leaves_free_eid_space_to_user_tables() {
    let path = tmp_path("starve");
    let eng = open(&path);

    let ring = eng.table_eid_extents("_sync_ops").unwrap();
    let ring_cap = eng.table_eid_usage("_sync_ops").unwrap().capacity;
    let remaining = eng.remaining_eid_capacity();
    assert_eq!(ring.len(), 1, "前提: ring の枠は 1 本で始まる");
    assert!(remaining > 0, "前提: 足せる eid 空間が残っている");

    let e = eng.entity_in("notes").unwrap();
    // ring (508) と残り (445) を足した数より多く書く: 自動で足す実装なら残りを全部取る
    write_unacked(&eng, e, ring_cap + remaining + 100);

    assert!(!bridge_caught_up(&eng), "前提: 滞留が ring に入り切っていない");
    let usage = eng.table_eid_usage("_sync_ops").unwrap();
    assert_eq!(usage.free, 0, "前提: ring が満杯で止まっている ({usage:?})");
    assert_eq!(
        eng.table_eid_extents("_sync_ops").unwrap(),
        ring,
        "ack が来ない間に ring が空き eid 空間から枠を足した"
    );
    assert_eq!(usage.capacity, ring_cap);
    assert_eq!(eng.remaining_eid_capacity(), remaining, "空き eid 空間が減った");

    // user の table は残りを全部使える (notes は 8 のうち 1 行を使っている)
    assert_eq!(
        fill_notes(&eng),
        7 + remaining,
        "user の table が空き eid 空間を使えない (notes={:?})",
        eng.table_eid_extents("notes")
    );
    assert_eq!(eng.remaining_eid_capacity(), 0);

    drop(eng);
    cleanup(&path);
}

/// 満杯の ring の error は、 効く手 (ack + reclaim / `grow_table`) を言う。 entity cap を伸ばしても空かない。
#[test]
fn full_ring_error_names_the_way_out() {
    let path = tmp_path("err");
    let eng = open(&path);
    let e = eng.entity_in("notes").unwrap();
    write_unacked(&eng, e, 2000);

    let err = eng.entity_in("_sync_ops").unwrap_err();
    assert!(err.contains("ring is full"), "{err}");
    assert!(err.contains("reclaim_sync_ops") && err.contains("grow_table"), "{err}");
    assert!(!err.contains("grow_entity_cap"), "entity cap を伸ばしても ring は空かない: {err}");

    drop(eng);
    cleanup(&path);
}

/// 明示の `grow_table` は ring にも効く。 足した分だけ、 待っていた record が運ばれる。
#[test]
fn explicit_grow_table_enlarges_the_ring() {
    let path = tmp_path("grow");
    let eng = open(&path);
    let ring_cap = eng.table_eid_usage("_sync_ops").unwrap().capacity;
    let e = eng.entity_in("notes").unwrap();
    write_unacked(&eng, e, 2000);
    assert!(!bridge_caught_up(&eng), "前提: 滞留が ring に入り切っていない");
    let lsn = eng.current_sync_lsn();

    assert_eq!(eng.grow_table("_sync_ops", 100).unwrap(), ring_cap + 100);
    while eng.transfer_oplog_to_sync_ops() > 0 {}

    let usage = eng.table_eid_usage("_sync_ops").unwrap();
    assert_eq!((usage.capacity, usage.live, usage.free), (ring_cap + 100, ring_cap + 100, 0));
    assert_eq!(eng.current_sync_lsn(), lsn + 100, "足した枠の分だけ運ばれていない");
    assert_eq!(eng.table_eid_extents("_sync_ops").unwrap().len(), 2);

    drop(eng);
    cleanup(&path);
}

/// 自動で足さないのは ring だけ。 `_sync_peers` (peer ごとに 1 行) は peer の数だけ伸びる。
#[test]
fn sync_peers_table_still_grows() {
    let path = tmp_path("peers");
    let eng = open(&path);
    let cap = eng.table_eid_usage("_sync_peers").unwrap().capacity;

    for peer in 1..=cap + 5 {
        eng.ack_sync(peer, 0).unwrap_or_else(|e| panic!("peer {peer}: {e}"));
    }
    let usage = eng.table_eid_usage("_sync_peers").unwrap();
    assert_eq!(usage.live, cap + 5, "{usage:?}");
    assert!(usage.capacity > cap, "{usage:?}");

    drop(eng);
    cleanup(&path);
}
