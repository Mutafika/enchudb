//! #878: anonymous の表 (`entity()`) の削除は、 #857 の止め (消した行が `tables` の sidecar より先にある間 checkpoint を進めない) に
//! 数えない。 anonymous の払い出しは `EntitySet` からで、 `next_local` も `tables` も使わない。 前は数えていて、 anonymous だけの DB では
//! sidecar の `next_local` が 0 のまま = `oplog_sync` が checkpoint を止め続けた (`tables` を書けない間は周期も止まり、 WAL が満杯で
//! 書き込みを断った)。

use enchudb_engine::{Engine, ValueType};

#[test]
fn deleting_an_anonymous_row_does_not_hold_oplog_sync() {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let path = std::env::temp_dir().join(format!("enchudb-issue878-{}-{nanos}", std::process::id()));
    let p = path.to_str().unwrap().to_string();
    let eng = Engine::create(&p).unwrap();
    assert!(!eng.has_cell_version(), "前提: 版数の無い DB (版数のある DB の削除は数えない)");
    let a = eng.ensure_himo_dynamic("a", ValueType::Number, 0).unwrap();
    // consumer の周期は `tables` を書いて止めを解くので止める (止めを解くのが `oplog_sync` だけの形)
    let hold = eng.hold_consumer();
    let e = eng.entity().unwrap();
    eng.tie_to_by_id(e, a, 1u32);
    eng.delete(e);
    eng.oplog_sync().unwrap();
    let wal = eng.oplog().unwrap().clone();
    let (cp, head) = (wal.checkpoint(), wal.written_head());
    drop(hold);
    drop((wal, eng));
    let _ = std::fs::remove_dir_all(&path);
    assert_eq!(cp, head, "anonymous の行を消した後の `oplog_sync` が checkpoint を進めなかった");
}
