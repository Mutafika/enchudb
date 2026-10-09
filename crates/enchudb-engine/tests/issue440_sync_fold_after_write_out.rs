//! #440: sync する DB の oplog は、 bridge が `_sync_ops` に写した行と payload を本体の書き出しが届かせた後に畳む。
//!
//! 旧: 畳む条件 (`wal_fold_safe`) は 「bridge が head まで読んだ」 だけで、 写した先が書き出されたかを見なかった。
//! `oplog_sync` も consumer の周期も bridge を本体の書き出しの後に回し、 consumer は bridge した直後に畳んだ。 畳んだ
//! oplog には次の record が上書きされ、 次の oplog の fsync がそれを行の書き出しより先に届かせるので、 その間に電源が
//! 落ちると、 書き出しが返った書き込みの record が oplog にも `_sync_ops` にも無くなった (相手に永久に届かない)。
//! 電源断の模擬は `tests/power_loss.rs` の `power_loss_keeps_sync_records_of_synced_batches` (旧実装では書き込みの途中の
//! 像の約半分で record が消えた)。
//!
//! ここは、 畳むのを書き出し待ちにしても畳まれなくならないこと (書き込みが止んだ後に consumer が書き出して畳む) を見る。

use enchudb_engine::{Engine, ValueType};
use std::time::{Duration, Instant};

#[test]
fn idle_sync_db_folds_the_oplog_after_writing_out_bridged_rows() {
    let path = std::env::temp_dir().join(format!("issue440_idle_fold_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    let mut eng = Engine::create_with_cell_version(path.to_str().unwrap(), 65_536).unwrap();
    eng.define_table("notes", 1_000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, 4 << 20).unwrap();
    for i in 0..100u32 {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
    // oplog_sync は bridge を本体の書き出しの後に回すので、 返った時点で写した行はまだ書き出されていない
    eng.oplog_sync().unwrap();
    assert_eq!(eng.pending_sync_ops(0).len(), 100, "bridge されていない (前提)");
    let wal = eng.oplog().unwrap().clone();
    let deadline = Instant::now() + Duration::from_secs(3);
    while wal.head() != enchudb_oplog::oplog::HEADER_SIZE as u64 {
        assert!(Instant::now() < deadline, "書き込みが止んで 3 秒たっても oplog が畳まれない (head {})", wal.head());
        std::thread::sleep(Duration::from_millis(10));
    }
    // 畳んだ後も、 写した record は配る分に残っている
    assert_eq!(eng.pending_sync_ops(0).len(), 100);
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}

/// 閉じる時も、 bridge した行を書き出してから畳む。 最後の `oplog_sync` の後に書いた分は閉じる時の bridge が写すので、
/// その行を書き出さないと畳めず、 開き直した bridge が oplog を読み直して同じ record をもう一度配る分に積む。
#[test]
fn clean_close_folds_so_reopen_does_not_bridge_again() {
    let path = std::env::temp_dir().join(format!("issue440_close_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    let p = path.to_str().unwrap();
    {
        let mut eng = Engine::create_with_cell_version(p, 65_536).unwrap();
        eng.define_table("notes", 1_000).unwrap();
        eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
        eng.enable_sync_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, 4 << 20).unwrap();
        for i in 0..100u32 {
            let e = eng.entity_in("notes").unwrap();
            eng.tie_to(e, "notes.note", i);
        }
        eng.oplog_sync().unwrap();
        assert_eq!(eng.pending_sync_ops(0).len(), 100, "bridge されていない (前提)");
        // 閉じる時に bridge される分
        for i in 100..150u32 {
            let e = eng.entity_in("notes").unwrap();
            eng.tie_to(e, "notes.note", i);
        }
    }
    let eng = Engine::open_concurrent_with_oplog(p, 4 << 20).unwrap();
    eng.oplog_sync().unwrap();
    while eng.transfer_oplog_to_sync_ops() > 0 {}
    assert_eq!(eng.pending_sync_ops(0).len(), 150, "開き直した後に同じ record をもう一度配る分に積んだ");
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}
