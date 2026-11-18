//! #1024: concurrent の DB (`finish_*` / `open_with_oplog` の後) で、 schema が `&mut Engine` を取らない。
//!
//! 前は `Arc::get_mut(&mut self.eng)` だけで `&mut Engine` を取る入口があった。 背景の thread (consumer) は engine を生のポインタで
//! 指すので `Arc` の数に入らず、 `Arc<Database>` を 1 つだけ持つ利用者が `Arc::get_mut` で `&mut Database` を取ると、 consumer が
//! 走っている横で `&mut Engine` が取れた (`Engine::tables` の `Vec` への push などが consumer の読みと data race)。
//!
//! - `enable_sync` / `prepare_cursors` / 新しい表の `build()` / 既にある表に Ref 列を足す `build()` は、 `engine_mut` と同じく断る
//! - `add_column` (Ref でない列) は今まで通り通るが、 engine の `flush(&mut)` をしない (書き出しは consumer)
//! - `open_with_oplog` の schema の読み直し (`load_schema`) は、 engine が既に持つ表・紐・relation に `&mut` を取らない
//!
//! falsify: どれかの入口の `is_concurrent` の確かめを外すと、 その入口が concurrent の DB で `Ok` を返す / `flush` が走る (辞書の
//! clean の印が立つ) / `load_schema` が `&mut` を取った回数が 0 でなくなる。
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

/// 表 `kv` (id, v) と、 `companies` (id, name) / `users` (id, company → companies) を作って `finish_with_oplog` した DB。
fn concurrent_db(path: &str) -> Arc<Database> {
    let mut db = Database::create(path).unwrap();
    db.table("kv").number("id").number("v").primary_key("id").build().unwrap();
    db.table("companies").number("id").tag("name").primary_key("id").build().unwrap();
    db.table("users").number("id").ref_to("company", "companies").primary_key("id").build().unwrap();
    db.finish_with_oplog(1 << 20).unwrap()
}

/// 新しい表の `build()` は断る (表は engine にも schema にもできない)。 既にある表の再宣言 (列を足す superset) は今まで通り通る。
#[test]
fn a_new_table_on_a_concurrent_database_is_refused() {
    let path = tmp("newtable");
    let _c = Cleanup(path.clone());
    let mut arc = concurrent_db(&path);
    let db = Arc::get_mut(&mut arc).expect("前提: Arc<Database> の唯一の持ち主");
    assert!(db.is_concurrent() && db.engine().has_background_threads(), "前提: consumer が走っている");
    let built = db.table("fresh").number("id").number("v").primary_key("id").build().map(|_| ());
    assert!(matches!(built, Err(SchemaError::Internal(_))), "concurrent の DB で新しい表の build() が断らなかった ({built:?})");
    assert!(db.get_table("fresh").is_none(), "断ったのに schema に表ができた");
    assert!(
        !db.engine().list_user_tables().iter().any(|(_, n, _, _)| n == "fresh"),
        "断ったのに engine に表ができた"
    );
    // 対照: 既にある表の superset の再宣言 (列を足す) は通る
    let kv = db.table("kv").number("id").number("v").number("w").primary_key("id").build().expect("既にある表の再宣言");
    let e = kv.insert().set("id", 1i64).set("v", 1i64).set("w", 7i64).commit().unwrap();
    assert_eq!(kv.entity(e).get("w"), Some(enchudb_schema::Value::Number(7)));
}

/// 既にある表に Ref 列を足す `build()` (superset の migrate) は断る (relation は engine にも schema にもできない)。
#[test]
fn a_ref_column_on_a_concurrent_database_is_refused() {
    let path = tmp("refcol");
    let _c = Cleanup(path.clone());
    let mut arc = concurrent_db(&path);
    let db = Arc::get_mut(&mut arc).expect("前提: Arc<Database> の唯一の持ち主");
    assert!(db.engine().has_background_threads(), "前提: consumer が走っている");
    let built = db.table("kv").number("id").number("v").ref_to("owner", "companies").primary_key("id").build().map(|_| ());
    assert!(matches!(built, Err(SchemaError::Internal(_))), "concurrent の DB で Ref 列を足す build() が断らなかった ({built:?})");
    let cols: Vec<String> = db.get_table("kv").unwrap().columns().into_iter().map(|c| c.name).collect();
    assert_eq!(cols, ["id", "v"], "断ったのに schema に列ができた");
    assert!(db.engine().fk_refs_for_table_named("kv").is_empty(), "断ったのに engine に relation ができた");
}

/// `add_column` (Ref でない列) は今まで通り通るが、 engine の `flush(&mut)` はしない。 `flush` は辞書 (vocab) の clean の印を立てる
/// (#709: oplog 付きの engine でも辞書の印は立てる) ので、 印が倒れたままなら `flush` は走っていない。 足した列は開き直しても残る。
#[test]
fn add_column_on_a_concurrent_database_does_not_flush_the_engine() {
    let path = tmp("addcol");
    let _c = Cleanup(path.clone());
    drop(concurrent_db(&path));
    let mut arc = Database::open_with_oplog(&path, 1 << 20).unwrap();
    {
        let db = Arc::get_mut(&mut arc).expect("前提: Arc<Database> の唯一の持ち主");
        assert!(db.engine().has_background_threads(), "前提: consumer が走っている");
        assert!(!db.engine().vocab_index_is_clean(), "前提: 書き手で開いた直後は辞書の印が倒れている");
        db.add_column("kv", "b", enchudb_schema::ColumnType::Number).expect("concurrent の DB の add_column");
        assert!(!db.engine().vocab_index_is_clean(), "concurrent の DB の add_column が engine を flush した (辞書の印が立った)");
        assert_eq!(db.exclusive_engine_takes_while_concurrent(), 0, "concurrent の DB で &mut Engine を取った");
    }
    let kv = arc.get_table("kv").unwrap();
    let e = kv.insert().set("id", 9i64).set("v", 1i64).set("b", 99i64).commit().unwrap();
    assert_eq!(kv.entity(e).get("b"), Some(enchudb_schema::Value::Number(99)));
    arc.engine().oplog_sync().unwrap();
    drop(arc);
    let db = Database::open(&path).unwrap();
    let cols: Vec<String> = db.get_table("kv").unwrap().columns().into_iter().map(|c| c.name).collect();
    assert!(cols.contains(&"b".to_string()), "足した列が開き直した後に無い: {cols:?}");
}

/// `open_with_oplog` の schema の読み直し (`load_schema`) は、 engine が既に持つ表・紐・relation に `&mut Engine` を取らない
/// (consumer は engine を起こした時から走っている)。 開き直した DB は今まで通り読み書きできる。
#[test]
fn reopening_a_known_schema_takes_no_exclusive_engine() {
    let path = tmp("reopen");
    let _c = Cleanup(path.clone());
    {
        let db = concurrent_db(&path);
        let companies = db.get_table("companies").unwrap();
        let c = companies.insert().set("id", 1i64).set("name", "Anthropic").commit().unwrap();
        db.get_table("users").unwrap().insert().set("id", 1i64).set("company", enchudb_schema::Value::Ref(c)).commit().unwrap();
        db.engine().oplog_sync().unwrap();
    }
    for round in 0..2 {
        let db = Database::open_with_oplog(&path, 1 << 20).unwrap();
        assert!(db.engine().has_background_threads(), "前提: consumer が走っている");
        assert_eq!(db.exclusive_engine_takes_while_concurrent(), 0, "{round} 回目の open_with_oplog が &mut Engine を取った");
        let users = db.get_table("users").unwrap();
        assert_eq!(users.all().count().unwrap(), 1);
        assert_eq!(db.engine().fk_refs_for_table_named("users"), [("company".to_string(), "companies".to_string())]);
    }
}
