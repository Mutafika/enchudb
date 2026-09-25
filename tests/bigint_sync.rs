//! BigInt 列 (engine では紐 2 本) の sync: 相手の peer に同じ値で届き、 2 台が同じ row に書き合っても
//! どちらかが書いた値そのものに収束する (上位と下位が別々の書き手の値に混ざらない)。

use std::sync::Arc;

use enchudb::schema::{Database, Value};
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::Hlc;

fn tmp(tag: &str) -> String {
    let p = format!("/tmp/enchudb-bigint-sync-{}-{}", tag, std::process::id());
    cleanup(&p);
    p
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    for suffix in ["", ".oplog", ".crc", ".tables", ".schema", ".eidmap", ".vocabmap", ".db.lock"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
    }
}

fn make_peer(path: &str, peer: u32) -> Arc<Database> {
    let mut b = Database::create_with_capacity(path, 65_536).unwrap();
    b.table("clocks").tag("k").bigint("at").primary_key("k").build().unwrap();
    b.enable_sync().unwrap();
    let db = b.finish_with_oplog(16 * 1024 * 1024).unwrap();
    db.engine().set_peer_id(peer);
    db
}

fn publish_all(db: &Database, syncer: &Syncer) {
    let eng = db.engine();
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
    syncer.publish_since(Hlc::ZERO);
}

fn at(db: &Database, k: &str) -> Option<Value> {
    let t = db.get_table("clocks").unwrap();
    let e = t.where_eq("k", k).find_one().unwrap()?;
    t.entity(e).get("at")
}

#[test]
fn bigint_syncs_whole_values() {
    let (pa, pb) = (tmp("a"), tmp("b"));
    let (db_a, db_b) = (make_peer(&pa, 1), make_peer(&pb, 2));
    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let sync_a = Syncer::new(db_a.arc_engine(), transport.clone());
    let sync_b = Syncer::new(db_b.arc_engine(), transport.clone());
    let write = |db: &Database, k: &str, v: i64| {
        db.get_table("clocks").unwrap().upsert().set("k", k).set("at", v).commit().unwrap();
    };

    // 片方で書いた値が相手に届く (負の数 / ms の時刻 / 値域の端)
    let vals = [-42i64, 1_790_000_000_123, enchudb::schema::BIGINT_MIN, enchudb::schema::BIGINT_MAX];
    for (i, v) in vals.iter().enumerate() {
        write(&db_a, &format!("k{i}"), *v);
    }
    publish_all(&db_a, &sync_a);
    sync_b.pull_once(1);
    for (i, v) in vals.iter().enumerate() {
        assert_eq!(at(&db_b, &format!("k{i}")), Some(Value::Number(*v)), "k{i} が届いていない");
    }

    // 2 台が同じ row に書き合う: 上位も下位も違う値同士で、 収束した値はどちらかが書いた値そのもの
    for round in 0..50i64 {
        let va = (round << 40) + 7; // 上位も下位も b と違う
        let vb = -(round << 33) - 1_000_000_007;
        write(&db_a, "shared", va);
        write(&db_b, "shared", vb);
        publish_all(&db_a, &sync_a);
        publish_all(&db_b, &sync_b);
        sync_b.pull_once(1);
        sync_a.pull_once(2);
        let (ga, gb) = (at(&db_a, "shared"), at(&db_b, "shared"));
        assert_eq!(ga, gb, "round {round}: 2 台が別の値");
        assert!(
            ga == Some(Value::Number(va)) || ga == Some(Value::Number(vb)),
            "round {round}: どちらも書いていない値 {ga:?} (a {va} / b {vb})"
        );
    }
    drop(sync_a);
    drop(sync_b);
    drop(db_a);
    drop(db_b);
    cleanup(&pa);
    cleanup(&pb);
}
