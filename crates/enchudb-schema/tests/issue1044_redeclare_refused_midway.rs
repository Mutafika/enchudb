//! #1044: 既にある表の再宣言 (後ろに列を足す `build()`) は、 足す列のどれかを断るなら 1 列も足さない。
//!
//! 前は 1 列ずつ足して、 途中の列で断った: 「Ref でない列 → Ref 列」 の順の再宣言を concurrent の DB (`Arc::get_mut` で取った
//! `&mut Database`) で `build()` すると、 Ref でない列は紐と memory の表にでき、 Ref 列で `Internal` が返った。 `.schema` は全部
//! 通った時だけ書くので (concurrent の DB は Drop でも書かない)、 その列はこの session では書けるのに、 開き直すと表に無かった。
//! build phase の DB でも、 Ref 列の先の表が無い再宣言は、 前の列だけを足して `UnknownTable` を返し、 Drop が `.schema` に書いた。
//!
//! falsify: `migrate_existing_table` の 「1 列も足す前に確かめる」 を外すと、 どちらの試験も前の列 `n` が表に残る。
#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;

use enchudb_schema::{Database, SchemaError};

fn tmp(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let p = format!("{}/enchudb-issue1044-{tag}-{}-{nanos}", std::env::temp_dir().display(), std::process::id());
    let _ = std::fs::remove_dir_all(&p);
    p
}

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn columns(db: &Database, table: &str) -> Vec<String> {
    db.get_table(table).unwrap().columns().into_iter().map(|c| c.name).collect()
}

/// concurrent の DB で 「Ref でない列 → Ref 列」 を足す再宣言は、 断って何も足さない (memory の表にも engine にも、 開き直した後も)。
#[test]
fn a_refused_ref_column_on_a_concurrent_database_adds_no_column_before_it() {
    let path = tmp("concurrent");
    let _c = Cleanup(path.clone());
    {
        let mut db = Database::create(&path).unwrap();
        db.table("kv").number("id").number("v").primary_key("id").build().unwrap();
        db.table("companies").number("id").tag("name").primary_key("id").build().unwrap();
        let mut arc = db.finish_with_oplog(1 << 20).unwrap();
        let db = Arc::get_mut(&mut arc).expect("前提: Arc<Database> の唯一の持ち主");
        assert!(db.is_concurrent(), "前提: concurrent の DB");
        let built = db
            .table("kv")
            .number("id")
            .number("v")
            .number("n")
            .ref_to("owner", "companies")
            .primary_key("id")
            .build()
            .map(|_| ());
        assert!(matches!(built, Err(SchemaError::Internal(_))), "前提: Ref 列を足す再宣言は断る ({built:?})");
        assert_eq!(columns(db, "kv"), ["id", "v"], "断った再宣言の前の列が memory の表にできた");
        assert!(db.engine().himo_id("kv.n").is_none(), "断った再宣言の前の列の紐が engine にできた");
        // 断った後も普通に書ける
        let kv = db.get_table("kv").unwrap();
        kv.insert().set("id", 1i64).set("v", 10i64).commit().unwrap();
        arc.engine().oplog_sync().unwrap();
    }
    let db = Database::open_with_oplog(&path, 1 << 20).unwrap();
    assert_eq!(columns(&db, "kv"), ["id", "v"], "開き直した後の列");
    assert_eq!(db.get_table("kv").unwrap().all().count().unwrap(), 1);
}

/// build phase の DB でも、 Ref 列の先の表が無い再宣言は何も足さない (Drop が書く `.schema` にも入らない)。
#[test]
fn a_redeclaration_with_an_unknown_ref_target_adds_no_column_before_it() {
    let path = tmp("unknown");
    let _c = Cleanup(path.clone());
    {
        let mut db = Database::create(&path).unwrap();
        db.table("kv").number("id").number("v").primary_key("id").build().unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        let built =
            db.table("kv").number("id").number("v").number("n").ref_to("owner", "nowhere").primary_key("id").build().map(|_| ());
        assert!(matches!(built, Err(SchemaError::UnknownTable(_))), "前提: 先の表が無い Ref 列は断る ({built:?})");
        assert_eq!(columns(&db, "kv"), ["id", "v"], "断った再宣言の前の列が memory の表にできた");
        assert!(db.engine().himo_id("kv.n").is_none(), "断った再宣言の前の列の紐が engine にできた");
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(columns(&db, "kv"), ["id", "v"], "断った再宣言の前の列が .schema に書かれた");
}

/// 対照: build phase の DB では同じ形の再宣言 (先の表がある) は両方の列を足す。
#[test]
fn the_build_phase_adds_both_columns() {
    let path = tmp("build");
    let _c = Cleanup(path.clone());
    {
        let mut db = Database::create(&path).unwrap();
        db.table("kv").number("id").number("v").primary_key("id").build().unwrap();
        db.table("companies").number("id").tag("name").primary_key("id").build().unwrap();
    }
    {
        let mut db = Database::open(&path).unwrap();
        db.table("kv").number("id").number("v").number("n").ref_to("owner", "companies").primary_key("id").build().unwrap();
        assert_eq!(columns(&db, "kv"), ["id", "v", "n", "owner"]);
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(columns(&db, "kv"), ["id", "v", "n", "owner"]);
}

/// 紐の上限 (`max_himos`) で engine が断る列も、 1 列も足す前に断る (残りの紐の数は前もって数えられる)。 前は残りの本数まで列を足して
/// 次の列で engine の 「too many himos」 を返し、 前の列が半端に残った (build phase の DB では Drop が `.schema` に書いた)。
#[test]
fn a_redeclaration_past_the_himo_limit_adds_no_column() {
    use enchudb_schema::GrowableOptions;
    let path = tmp("himolimit");
    let _c = Cleanup(path.clone());
    {
        let mut db = Database::create_growable_with(&path, GrowableOptions { max_himos: 8, ..Default::default() }).unwrap();
        db.table("kv").number("id").number("v").primary_key("id").build().unwrap();
    }
    let room = {
        let mut db = Database::open(&path).unwrap();
        let room = db.engine().himo_room();
        assert!(room >= 1, "前提: まだ紐を足せる ({room})");
        let mut t = db.table("kv").number("id").number("v");
        for i in 0..=room {
            t = t.number(&format!("n{i}"));
        }
        let built = t.primary_key("id").build().map(|_| ());
        assert!(matches!(built, Err(SchemaError::Internal(_))), "前提: 紐の上限を越える再宣言は断る ({built:?})");
        assert_eq!(columns(&db, "kv"), ["id", "v"], "断った再宣言の前の列が memory の表にできた");
        assert_eq!(db.engine().himo_room(), room, "断った再宣言の前の列の紐が engine にできた");
        room
    };
    let db = Database::open(&path).unwrap();
    assert_eq!(columns(&db, "kv"), ["id", "v"], "断った再宣言の前の列が .schema に書かれた");
    assert_eq!(db.engine().himo_room(), room);
}
