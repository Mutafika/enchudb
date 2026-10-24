//! `Engine::forget_deletes_before`: 線より前の削除の記録を忘れる (ホストが配る忘れてよい線、 `HOST_PLAN.md`)。 消した名前は
//! `pk_tombs` の単体と `enchudb-transport` の `http_host_horizon` が見る。 ここは相手の行の削除 (`foreign_tombs`) と、 その書き出し
//! (`.eidmap` + `eidmap.log`)。

use enchudb_engine::ValueType;
use enchudb_engine::engine::Engine;
use enchudb_oplog::{Hlc, make_eid};
use std::sync::Arc;

fn tmp(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/enchudb-forget-{tag}-{}-{nanos}", std::process::id())
}

fn hlc(w: u64) -> Hlc {
    Hlc { wall: w, logical: 0, peer: 1 }
}

fn open(path: &str, create: bool) -> Arc<Engine> {
    if create {
        let mut eng = Engine::create_with_cell_version(path, 4096).unwrap();
        eng.define_table("t", 1000).unwrap();
        eng.define_himo_in("t", "v", ValueType::Number, 0).unwrap();
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
    }
    let eng = Engine::open_concurrent_with_oplog(path, 4 << 20).unwrap();
    eng.set_peer_id(2);
    eng
}

/// 作成より先に届いた削除 (相手の行の削除の記録) のうち、 線より前のものだけを忘れ、 書き出しも忘れた後の中身になる。
#[test]
fn foreign_deletes_before_the_line_are_forgotten_and_written_out() {
    let path = tmp("foreign");
    let eng = open(&path, true);
    for i in 0..10u32 {
        assert!(eng.remote_delete_apply_foreign(make_eid(1, i), hlc(100 + i as u64)));
    }
    eng.persist_sync_state().unwrap();
    assert_eq!(eng.deleted_before_create(make_eid(1, 3)), Some(hlc(103)), "前提: 覚えている");
    assert_eq!(eng.forget_deletes_before(hlc(105)), 5);
    assert_eq!(eng.forget_deletes_before(hlc(105)), 0, "二度目は何もしない");
    assert_eq!(eng.deleted_before_create(make_eid(1, 3)), None, "線より前を忘れない");
    assert_eq!(eng.deleted_before_create(make_eid(1, 7)), Some(hlc(107)), "線より後まで忘れた");
    eng.persist_sync_state().unwrap();
    assert_eq!(eng.eidmap_disk_diff().unwrap(), None, "書き出しが memory と違う (忘れた記録が残った)");
    drop(eng);
    let back = open(&path, false);
    assert_eq!(back.deleted_before_create(make_eid(1, 3)), None, "開き直すと忘れた記録が戻った");
    assert_eq!(back.deleted_before_create(make_eid(1, 7)), Some(hlc(107)));
    drop(back);
    let _ = std::fs::remove_dir_all(&path);
}
