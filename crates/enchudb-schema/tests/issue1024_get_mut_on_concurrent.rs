//! #1024: `Database::enable_sync` / `prepare_cursors` は、 concurrent の DB (`finish_*` / `open_with_oplog` の後) では断る。
//!
//! 前は `Arc::get_mut(&mut self.eng)` だけで `&mut Engine` を取っていた。 背景の thread (consumer) は engine を生のポインタで指すので
//! `Arc` の数に入らず、 `Arc<Database>` を 1 つだけ持つ利用者が `Arc::get_mut` で `&mut Database` を取ると、 consumer が走っている
//! 横で `&mut Engine` が取れた (表の並びへの push などが consumer の読みと data race)。 `engine_mut` と同じく `is_concurrent` で断る。
//!
//! falsify: 2 つの入口のどちらかの `is_concurrent` の確かめを外すと、 その入口が concurrent の DB で `Ok` を返す (sync の表 /
//! `_cursors` ができる)。
#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;

use enchudb_schema::{Database, SchemaError};

fn tmp(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let p = format!("{}/enchudb-issue1024-{tag}-{}-{nanos}", std::env::temp_dir().display(), std::process::id());
    let _ = std::fs::remove_dir_all(&p);
    p
}

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// `Arc<Database>` の唯一の持ち主から `&mut Database` を取り、 2 つの入口が断ることと、 断った後も DB が使えることを見る。
fn both_are_refused(db: &mut Arc<Database>, when: &str) {
    let db = Arc::get_mut(db).unwrap_or_else(|| panic!("{when}: 前提: Arc<Database> の唯一の持ち主"));
    assert!(db.is_concurrent(), "{when}: 前提: concurrent の DB");
    assert!(db.engine().has_background_threads(), "{when}: 前提: consumer が走っている");
    let sync = db.enable_sync();
    assert!(matches!(sync, Err(SchemaError::Internal(_))), "{when}: concurrent の DB で enable_sync が断らなかった ({sync:?})");
    assert!(!db.sync_enabled(), "{when}: 断ったのに sync の表ができた");
    let cursors = db.prepare_cursors();
    assert!(matches!(cursors, Err(SchemaError::Internal(_))), "{when}: concurrent の DB で prepare_cursors が断らなかった ({cursors:?})");
    assert!(!db.cursors_ready(), "{when}: 断ったのに _cursors ができた");
}

#[test]
fn enable_sync_and_prepare_cursors_refuse_a_concurrent_database() {
    let path = tmp("refuse");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("kv").number("id").number("v").primary_key("id").build().unwrap();
    let mut db = db.finish_with_oplog(1 << 20).unwrap();
    both_are_refused(&mut db, "finish_with_oplog の後");
    let kv = db.get_table("kv").unwrap();
    kv.insert().set("id", 1i64).set("v", 10i64).commit().unwrap();
    assert_eq!(kv.all().count().unwrap(), 1, "断った後も書ける");
    drop(db);

    let mut db = Database::open_with_oplog(&path, 1 << 20).unwrap();
    both_are_refused(&mut db, "open_with_oplog の後");
    assert_eq!(db.get_table("kv").unwrap().all().count().unwrap(), 1, "開き直した後も読める");
}

/// 対照: build phase (concurrent にする前) では今まで通り成功し、 concurrent にした後も sync の表と `_cursors` が残る。
#[test]
fn the_build_phase_still_enables_sync_and_cursors() {
    let path = tmp("build");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("kv").number("id").number("v").primary_key("id").build().unwrap();
    db.enable_sync().unwrap();
    db.prepare_cursors().unwrap();
    let db = db.finish_with_oplog(1 << 20).unwrap();
    assert!(db.sync_enabled() && db.cursors_ready());
}
