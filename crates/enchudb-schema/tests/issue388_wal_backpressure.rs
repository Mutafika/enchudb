//! #388: 書き手が休まず書き続けても WAL (oplog) が溢れて append を落とさない。 ring が埋まってきたら consumer が
//! append を止めて書き出し (oplog の fsync + 本体の msync)、 checkpoint を head まで進めて畳む。 その間 WAL に直接
//! 書く書き手 (Commit marker など) は待つ。

use enchudb_schema::Database;
use std::collections::VecDeque;

fn tmp(tag: &str) -> String {
    let p = format!(
        "/tmp/enchudb-issue388-{}-{}-{}.db",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    );
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    for ext in ["oplog", "tables", "lock", "eidmap", "crc"] {
        let _ = std::fs::remove_file(format!("{path}.{ext}"));
    }
}

const WAL: usize = 4 << 20;

/// issue の再現 (WAL を 4 MiB に縮めた): 1 thread で 1 行入れては、 生きている行が 100 を超えた分を古い順に消す。
/// WAL の容量の何倍も書いても、 落ちた record も打てなかった Commit も無い。
///
/// consumer が満杯の瞬間に畳みに入る形 (0.29.2 で consumer 自身の Commit が落ちていた) は確率的にしか踏めないので
/// 5 回繰り返す (1 回あたりの検出率は約 1/3)。
#[test]
fn single_writer_insert_delete_never_drops_wal_records() {
    for round in 0..5 {
        single_writer_round(round);
    }
}

fn single_writer_round(round: u32) {
    let path = tmp(&format!("burst{round}"));
    let rows = 60_000u64;
    let live_ids: Vec<String> = {
        let mut b = Database::create_growable_with_capacity(&path, 1_000_000).unwrap();
        b.table("events")
            .tag("id")
            .tag("owner")
            .leaf("title")
            .number("created_at")
            .primary_key("id")
            .build()
            .unwrap();
        let db = b.finish_with_oplog(WAL).unwrap();
        let t = db.get_table("events").unwrap();
        let e = db.engine();
        let mut live = VecDeque::new();
        for i in 0..rows {
            let id = format!("ev_{i:016x}");
            let eid = t
                .insert()
                .set("id", id.clone())
                .set("owner", format!("u{}", i % 1000))
                .set("title", "something happened in a project")
                .set("created_at", i as i64)
                .commit()
                .unwrap();
            live.push_back((eid, id));
            while live.len() > 100 {
                t.entity(live.pop_front().unwrap().0).delete().unwrap();
            }
        }
        e.oplog_sync().unwrap();
        assert_eq!(e.wal_dropped_records(), 0, "WAL に載らなかった record");
        assert_eq!(e.wal_commit_failures(), 0, "打てなかった Commit");
        assert!(e.wal_room_folds() > 0, "WAL の容量を何度も越えたので畳んでいる");
        live.into_iter().map(|(_, id)| id).collect()
    };
    let db = Database::open_with_oplog(&path, WAL).unwrap();
    let t = db.get_table("events").unwrap();
    assert_eq!(t.all().count().unwrap(), 100);
    for id in &live_ids {
        assert!(t.where_eq("id", id.as_str()).find_one().unwrap().is_some(), "{id}");
    }
    drop(db);
    cleanup(&path);
}

/// 大きな行 (WAL の数 % の Leaf) を書き続けても、 待たされるだけで落ちない (1 つの束が ring の残りに入らない時に、
/// consumer が先に畳む)。
#[test]
fn large_rows_wait_instead_of_dropping() {
    for round in 0..5 {
        large_rows_round(round);
    }
}

fn large_rows_round(round: u32) {
    let path = tmp(&format!("large{round}"));
    let payload = "x".repeat(64 << 10);
    {
        let mut b = Database::create_growable_with_capacity(&path, 100_000).unwrap();
        b.table("blobs").number("id").leaf("body").primary_key("id").build().unwrap();
        let db = b.finish_with_oplog(WAL).unwrap();
        let t = db.get_table("blobs").unwrap();
        let e = db.engine();
        for i in 0..1_000i64 {
            let eid = t.insert().set("id", i).set("body", payload.as_str()).commit().unwrap();
            if i >= 10 {
                let old = t.where_eq("id", i - 10).find_one().unwrap().unwrap();
                t.entity(old).delete().unwrap();
            }
            let _ = eid;
        }
        e.oplog_sync().unwrap();
        assert_eq!(e.wal_dropped_records(), 0);
        assert_eq!(e.wal_commit_failures(), 0);
    }
    cleanup(&path);
}

/// 畳めない時 (sync の bridge が `_sync_ops` の満杯で止まり、 配り終えるまで畳めない) は、 書き手は consumer の 1 回の
/// 試みを待つだけで諦める (今までどおり落ちて #57 の floor に回る)。 上限の 5 秒ずつ待つと、 Commit のたびに止まる。
#[test]
fn writers_do_not_stall_when_the_wal_cannot_be_folded() {
    use enchudb_engine::{Engine, ValueType};
    let path = tmp("stuck");
    let mut eng = Engine::create_growable_with_cell_version(&path, 3_000).unwrap();
    eng.define_table("t", 100).unwrap();
    eng.define_himo_in("t", "k", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, 1 << 20).unwrap();
    let k = eng.himo_id("t.k").unwrap() as u16;
    let started = std::time::Instant::now();
    // ack しない = `_sync_ops` は埋まったまま、 bridge は止まり、 WAL は畳めない
    for i in 0..40_000u32 {
        let e = eng.entity_in("t").unwrap();
        eng.tie_to_by_id(e, k, i);
        eng.commit();
        eng.delete(e);
    }
    eng.oplog_sync().ok();
    let took = started.elapsed();
    assert!(eng.wal_commit_failures() > 0, "畳めないので満杯で落ちている (前提の確認)");
    assert!(took < std::time::Duration::from_secs(20), "書き手が待ちで止まった: {took:?}");
    drop(eng);
    cleanup(&path);
}

/// 1 つの record が ring の残り (1/4 を切る前に畳む目安) より大きい時: consumer は自分の append が満杯にぶつかった
/// その場で畳んで書き直す (待つ相手がいない — 自分が畳む役)。
///
/// consumer が空いている時の試みを 「畳めなかった」 と覚えると、 待っている書き手が諦めて落ちる (#391 で見つけた)。
/// その形は確率的にしか踏めないので 5 回繰り返す。
#[test]
fn a_record_larger_than_the_headroom_is_not_dropped() {
    for round in 0..5 {
        huge_round(round);
    }
}

fn huge_round(round: u32) {
    let path = tmp(&format!("huge{round}"));
    let payload = "y".repeat(1_200 << 10);
    {
        let mut b = Database::create_growable_with_capacity(&path, 100_000).unwrap();
        b.table("blobs").number("id").leaf("body").primary_key("id").build().unwrap();
        let db = b.finish_with_oplog(WAL).unwrap();
        let t = db.get_table("blobs").unwrap();
        let e = db.engine();
        for i in 0..30i64 {
            t.insert().set("id", i).set("body", payload.as_str()).commit().unwrap();
            if i >= 2 {
                let old = t.where_eq("id", i - 2).find_one().unwrap().unwrap();
                t.entity(old).delete().unwrap();
            }
        }
        e.oplog_sync().unwrap();
        assert_eq!(e.wal_dropped_records(), 0);
        assert_eq!(e.wal_commit_failures(), 0);
    }
    cleanup(&path);
}
