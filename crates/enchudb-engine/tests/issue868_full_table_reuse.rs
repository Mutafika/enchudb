//! #868: 枠を使い切った表で 「消して足す」 を回しても、 足すたびに枠の全部をなめない。
//!
//! 前は、 表が枯渇すると `entity_in` が払い出し位置 (`next_local`) を 1 進めてから、 枠の全部をなめて穴を集めた
//! (`rebuild_free_locals`)。 消した行は 1 つなので穴も 1 つで、 次の 「消して足す」 でまた全部をなめた (100 万行で 1 回 596 µs)。
//! 今は消した行を表ごとの一覧に覚え、 枯渇した時はまずそこから拾う。 使い回す時期は前と同じ (表が枯渇した時、 生きている印が
//! 下りた slot)。
//!
//! 判定は時間でなく数で: 枠をなめた回数 (`free_slot_scans`)、 使った eid 空間 (`remaining_eid_capacity`)、 払い出した eid。

use enchudb_engine::{Engine, ValueType};
use std::sync::Arc;

struct Db {
    path: String,
}

impl Drop for Db {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn scratch(tag: &str) -> Db {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let path = format!("{}/enchudb-issue868-{tag}-{}-{nanos}", std::env::temp_dir().display(), std::process::id());
    let _ = std::fs::remove_dir_all(&path);
    Db { path }
}

/// 枠 `cap` の表 `t` (列 a / b) を持つ engine。 `entity_cap` は DB 全体の eid の上限。
fn open(db: &Db, entity_cap: u32, cap: u32, sync: bool) -> Arc<Engine> {
    let mut eng = Engine::create_growable_with_capacity(&db.path, entity_cap).unwrap();
    eng.define_table("t", cap).unwrap();
    eng.define_himo_in("t", "a", ValueType::Number, 0).unwrap();
    eng.define_himo_in("t", "b", ValueType::Number, 0).unwrap();
    if sync {
        eng.enable_sync_tables().unwrap();
    }
    let eng = Engine::concurrentize_with_oplog(eng, 16 << 20).unwrap();
    eng.set_peer_id(1);
    eng
}

fn insert(eng: &Engine, v: u32) -> u64 {
    let e = eng.entity_in("t").unwrap();
    eng.tie_to(e, "t.a", v);
    eng.tie_to(e, "t.b", v);
    e
}

/// 満杯の表で 「いちばん古い行を消して 1 行足す」 を回しても枠をなめず、 eid 空間も使わない。 sync の DB もしない DB も。
#[test]
fn churning_a_full_table_does_not_scan_the_table() {
    const CAP: u32 = 2_000;
    const CYCLES: u32 = 20_000;
    for sync in [false, true] {
        let db = scratch(if sync { "churn-sync" } else { "churn" });
        let eng = open(&db, 1 << 20, CAP, sync);
        let mut rows: std::collections::VecDeque<u64> = (0..CAP).map(|v| insert(&eng, v)).collect();
        let left = eng.remaining_eid_capacity();
        let scans = eng.free_slot_scans();
        for i in 0..CYCLES {
            let old = rows.pop_front().unwrap();
            eng.delete(old);
            let e = insert(&eng, CAP + i);
            // #731: 使い回した場所の eid は世代が違う (場所で見る)
            assert_eq!(eng.eid_slot(e), eng.eid_slot(old), "sync={sync}: 消した行の slot を使い回していない (表は満杯)");
            assert_ne!(e, old, "sync={sync}: 使い回した場所に消えた行の eid を渡した");
            rows.push_back(e);
        }
        let scanned = eng.free_slot_scans() - scans;
        assert_eq!(scanned, 0, "sync={sync}: {CYCLES} 回の 「消して足す」 で枠を {scanned} 回なめた");
        assert_eq!(eng.remaining_eid_capacity(), left, "sync={sync}: 満杯の表の 「消して足す」 が eid 空間を使った");
        let u = eng.table_eid_usage("t").unwrap();
        assert_eq!((u.capacity, u.live), (CAP, CAP), "sync={sync}: 枠と行の数");
        let a = eng.himo_id("t.a").unwrap() as u16;
        let b = eng.himo_id("t.b").unwrap() as u16;
        for (k, e) in rows.iter().enumerate() {
            let v = u64::from(CYCLES + k as u32);
            assert_eq!((eng.get_by_id(*e, a), eng.get_by_id(*e, b)), (Some(v), Some(v)), "sync={sync}: 行 {k} の中身");
        }
    }
}

/// 「消して足す」 を回した後に穴の無い表へ足すと、 枠を 1 つ分 (先頭の枠と同じ大きさ) 足すだけ。 前は回した回数だけ払い出し位置が
/// 枠の先へ伸びていて、 足す extent をそこまで大きくした (2^32 回で u32 が一周すると、 生きている slot をもう一度払い出す)。
#[test]
fn churning_does_not_push_the_next_slot_past_the_table() {
    const CAP: u32 = 100;
    const CYCLES: u32 = 10_000;
    let db = scratch("next");
    let eng = open(&db, 1 << 20, CAP, false);
    let mut rows: std::collections::VecDeque<u64> = (0..CAP).map(|v| insert(&eng, v)).collect();
    for i in 0..CYCLES {
        eng.delete(rows.pop_front().unwrap());
        rows.push_back(insert(&eng, CAP + i));
    }
    assert_eq!(eng.table_eid_usage("t").unwrap().allocated, CAP);
    let left = eng.remaining_eid_capacity();
    insert(&eng, 0);
    let used = left - eng.remaining_eid_capacity();
    assert_eq!(used, CAP, "穴の無い満杯の表に 1 行足して eid 空間を {used} 使った (枠 1 つ分は {CAP})");
}

/// 足せない (eid 空間が尽きた) 表に足そうとして断られ続けても、 払い出し位置は枠の先へ伸びない: 後で eid 空間を広げて足すと、
/// 枠 1 つ分だけ使う。
#[test]
fn refused_inserts_do_not_push_the_next_slot_past_the_table() {
    const CAP: u32 = 100;
    let db = scratch("refused");
    let eng = open(&db, 1 << 12, CAP, false);
    // t が枠を足しながら eid 空間を使い切るまで足す (残りの数が 0 になっても、 最後に足した枠の空きが残る)
    let mut rows = 0u32;
    while let Ok(e) = eng.entity_in("t") {
        eng.tie_to(e, "t.a", rows);
        rows += 1;
    }
    assert!(rows > CAP && eng.remaining_eid_capacity() == 0, "前提: t が枠を足して eid 空間を使い切った ({rows} 行)");
    for _ in 0..5_000 {
        assert!(eng.entity_in("t").is_err(), "前提: eid 空間が尽きて断られる");
    }
    let before = eng.table_eid_usage("t").unwrap();
    eng.grow_entity_cap(1 << 16).unwrap();
    let left = eng.remaining_eid_capacity();
    insert(&eng, 2);
    let used = left - eng.remaining_eid_capacity();
    assert!(
        used <= CAP,
        "断られた 5000 回の後に 1 行足して eid 空間を {used} 使った (先頭の枠は {CAP}、 足す前の枠 {})",
        before.capacity
    );
}

/// 覚えた一覧に無い穴 (開き直す前に消した行、 一覧の上限を越えた分) も、 一覧を使い切った後に拾う = 満杯の表は枠を足さずに
/// 全部の穴を埋める。
#[test]
fn holes_missing_from_the_list_are_still_found() {
    const CAP: u32 = 80_000; // 開き直した後に消す分 (約 6.9 万) が一覧の上限 (65,536) を越える
    let db = scratch("holes");
    {
        let eng = open(&db, 1 << 20, CAP, false);
        let rows: Vec<u64> = (0..CAP).map(|v| insert(&eng, v)).collect();
        // 開き直す前に消す分 (一覧は開き直すと空)
        for e in rows.iter().step_by(7) {
            eng.delete(*e);
        }
        eng.oplog_sync().unwrap();
    }
    let eng = Engine::open_concurrent_with_oplog(&db.path, 16 << 20).unwrap();
    let all: Vec<u64> = eng.entities_with_himo(eng.himo_id("t.a").unwrap() as u16);
    // 開き直した後に全部消す (一覧の上限を越える)
    for e in &all {
        eng.delete(*e);
    }
    let left = eng.remaining_eid_capacity();
    for v in 0..CAP {
        insert(&eng, v);
    }
    assert_eq!(eng.remaining_eid_capacity(), left, "穴が残っているのに枠を足した");
    let u = eng.table_eid_usage("t").unwrap();
    assert_eq!((u.capacity, u.live), (CAP, CAP));
    assert!(eng.entity_in("t").is_ok(), "満杯になった後は枠を足して払い出す");
}

/// 余地のある表は消した行の slot を使い回さない (今までどおり、 使い回すのは枯渇した時だけ)。 sync の DB もしない DB も。
#[test]
fn a_table_with_room_does_not_reuse_deleted_slots() {
    for sync in [false, true] {
        let db = scratch(if sync { "room-sync" } else { "room" });
        let eng = open(&db, 1 << 20, 1_000, sync);
        let mut seen = std::collections::HashSet::new();
        for v in 0..500 {
            let e = insert(&eng, v);
            assert!(seen.insert(e), "sync={sync}: 余地のある表で eid {e:#x} をもう一度払い出した");
            eng.delete(e);
        }
    }
}
