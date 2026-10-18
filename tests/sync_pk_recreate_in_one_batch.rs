//! #508: 同じ author が 1 回の配り (batch) の中で、 行を消して同じ主キーで作り直す (cube-tester の H8)。
//!
//! 受け手の主キーの照合 (#141 の束ね) を batch を当てる前にまとめてやると、 作り直した行の主キーが、 同じ batch で
//! 消える前の古い行に束ねられる (消す record はまだ当たっていない)。 古い行と新しい行が受け手では 1 つの entity になり、
//! 古い行への別の peer の更新が新しい行に入る。 author では古い行と新しい行は別の entity なので、 恒久に食い違う。
//! 1 件ずつ届けた時 (= batch の切れ目が違うだけ) には起きない。
//!
//! 形: A が作った行 (主キー 7) を B と C に配る。 A が 1 回の書き込みでその行を消して主キー 7 で作り直し、 その後で
//! C が (A の消しを知らずに) 古い行の n を更新する (A の消しより新しい HLC)。 B は A の batch と C の更新を受け取り、
//! A は C の更新を受け取る。 A と B の行の集まりが同じになるか。

use std::sync::Arc;

use enchudb::schema::{Database, Value};
use enchudb::sync::Syncer;
use enchudb::transport::{InMemoryTransport, Transport};
use enchudb_oplog::Hlc;

const TABLE: &str = "items";

struct Cleanup(Vec<String>);

impl Drop for Cleanup {
    fn drop(&mut self) {
        for p in &self.0 {
            let _ = enchudb::db_files::remove_db(p);
        }
    }
}

fn make_peer(path: &str, peer: u32) -> Arc<Database> {
    let _ = enchudb::db_files::remove_db(path);
    let mut b = Database::create_with_capacity(path, 65_536).unwrap();
    b.table(TABLE).number("id").tag("name").number("n").primary_key("id").build().unwrap();
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

fn eid_of(db: &Database, id: i64) -> u64 {
    let found = db.get_table(TABLE).unwrap().where_eq("id", id).find().unwrap();
    assert_eq!(found.len(), 1, "主キー {id} の行が {} 個", found.len());
    found[0]
}

/// 表の生きている行を (id, name, n) で並べる。
fn rows(db: &Database) -> Vec<(Option<Value>, Option<Value>, Option<Value>)> {
    db.engine().rebuild();
    let t = db.get_table(TABLE).unwrap();
    let mut out: Vec<_> = t
        .all()
        .find()
        .unwrap()
        .into_iter()
        .map(|e| {
            let r = t.entity(e);
            (r.get("id"), r.get("name"), r.get("n"))
        })
        .collect();
    out.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    out
}

#[test]
fn recreating_a_primary_key_in_one_batch_converges_with_the_author() {
    let paths: Vec<String> = ["a", "b", "c"]
        .iter()
        .map(|t| format!("/tmp/enchudb-pk-recreate-{t}-{}", std::process::id()))
        .collect();
    let _cleanup = Cleanup(paths.clone());
    let (db_a, db_b, db_c) = (make_peer(&paths[0], 1), make_peer(&paths[1], 2), make_peer(&paths[2], 3));
    let mem = Arc::new(InMemoryTransport::new());
    for peer in 1..=3 {
        mem.register_peer(peer);
    }
    let transport: Arc<dyn Transport> = mem;
    let (sync_a, sync_b, sync_c) = (
        Syncer::new(db_a.arc_engine(), transport.clone()),
        Syncer::new(db_b.arc_engine(), transport.clone()),
        Syncer::new(db_c.arc_engine(), transport.clone()),
    );

    // 1. A が主キー 7 の行を作り、 B と C に配る
    db_a.get_table(TABLE).unwrap().insert().set("id", 7i64).set("name", "x").set("n", 0i64).commit().unwrap();
    publish_all(&db_a, &sync_a);
    sync_b.pull_once(1);
    sync_c.pull_once(1);

    // 2. A が 1 回の配りの中で、 その行を消して主キー 7 で作り直す
    let old = eid_of(&db_a, 7);
    db_a.get_table(TABLE).unwrap().entity(old).delete().unwrap();
    db_a.get_table(TABLE).unwrap().insert().set("id", 7i64).set("name", "y").set("n", 1i64).commit().unwrap();
    publish_all(&db_a, &sync_a);

    // 3. その後で (HLC が A の消しより新しく)、 C が A の消しを知らずに古い行の n を更新する
    std::thread::sleep(std::time::Duration::from_millis(5));
    let old_on_c = eid_of(&db_c, 7);
    db_c.get_table(TABLE).unwrap().entity(old_on_c).update().set("n", 2i64).commit().unwrap();
    publish_all(&db_c, &sync_c);

    // 4. B は A の batch (消す・作り直す) と C の更新を、 A は C の更新を受け取る
    for (got, what) in [(sync_b.pull_once(1), "B ← A"), (sync_b.pull_once(3), "B ← C"), (sync_a.pull_once(3), "A ← C")] {
        assert!(got.received > 0 && got.applied == got.received, "{what}: {got:?}");
    }

    let (on_a, on_b) = (rows(&db_a), rows(&db_b));
    assert_eq!(on_b, on_a, "B の行の集まりが author の A と違う (A: {on_a:?})");
    // C の更新 (古い行の n = 2) が載った行: 表の行として見えなくても、 列の値で引けるものは同じ数
    let n2 = |db: &Database| db.get_table(TABLE).unwrap().where_eq("n", 2i64).find().unwrap().len();
    assert_eq!(n2(&db_b), n2(&db_a), "n = 2 の行の数が違う");
}
