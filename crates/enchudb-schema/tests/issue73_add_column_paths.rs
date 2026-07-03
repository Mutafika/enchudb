//! issue #73: 既存 table への列追加が declarative build() と oplog DB の
//! 両経路から届かない bug の regression test。
//!
//! - G1: 既存 table への `table().build()` 再宣言が declared columns を黙って
//!   捨てていた → on-disk cols が declared の prefix なら trailing 新列を
//!   auto-migrate、 矛盾 (欠落 / 型不一致 / 並べ替え) は `SchemaConflict`。
//! - G2: `add_column` が concurrent (open_with_oplog) Database を拒否していた
//!   → `Engine::ensure_himo_dynamic_in` (`&self`) 経由で両モード対応。
//! - おまけ: `_c_` prefix column は engine content 互換 layer の予約 (0.9.0)
//!   なので schema 層で reject。

use enchudb_schema::{ColumnType, Database, SchemaError, Value};
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue73-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suffix in ["", ".oplog", ".tables", ".crc", ".schema", ".db.lock", ".lock", ".eidmap"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

/// phase 0 相当の共通 setup: (id: Number, a: Number) の table t を作って
/// 1 row (id=1, a=10) を書いて close する。
fn setup_id_a(path: &str) -> enchudb_oplog::EntityId {
    let mut db = Database::create(path).unwrap();
    let t = db.table("t")
        .number("id")
        .number("a")
        .primary_key("id")
        .build()
        .unwrap();
    t.insert().set("id", 1i64).set("a", 10i64).commit().unwrap()
    // drop → .schema / .tables persist
}

/// phase 1: 既存 table を (id, a, b) で再宣言 → b が生えて書ける、
/// 旧 row は intact で b は None。 reopen 後も schema が残る。
#[test]
fn redeclare_superset_auto_migrates_trailing_column() {
    let path = tmp_path("redeclare");
    cleanup(&path);
    let old_row = setup_id_a(&path);

    {
        let mut db = Database::open(&path).unwrap();
        // 再宣言: on-disk (id, a) + trailing b
        let t = db.table("t")
            .number("id")
            .number("a")
            .number("b")
            .primary_key("id")
            .build()
            .expect("superset re-declaration should auto-migrate");

        // b が存在して writable
        assert!(t.himo_id("b").is_some(), "column b should exist after migration");
        let new_row = t.insert()
            .set("id", 2i64)
            .set("a", 20i64)
            .set("b", 200i64)
            .commit()
            .expect("insert with new column b should succeed");
        assert_eq!(t.entity(new_row).get("b"), Some(Value::Number(200)));

        // 旧 row は intact、 b は未 tie = None
        assert_eq!(t.entity(old_row).get("a"), Some(Value::Number(10)));
        assert_eq!(t.entity(old_row).get("b"), None, "old rows read b as None");

        // 旧 row にも b を後付けできる
        t.entity(old_row).set("b", 100i64).commit().unwrap();
        assert_eq!(t.entity(old_row).get("b"), Some(Value::Number(100)));
    }

    // reopen: migrate された schema が sidecar から復元される
    {
        let db = Database::open(&path).unwrap();
        let info = db.list_tables();
        assert_eq!(info.len(), 1);
        let cols: Vec<&str> = info[0].columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(cols, vec!["id", "a", "b"], "b must persist across reopen");
        let t = db.get_table("t").unwrap();
        assert_eq!(t.entity(old_row).get("b"), Some(Value::Number(100)));
    }

    cleanup(&path);
}

/// phase 1 (同一 process 版): create → build → 同 session 内で superset 再宣言。
#[test]
fn redeclare_superset_in_same_session() {
    let path = tmp_path("same-session");
    cleanup(&path);

    let mut db = Database::create(&path).unwrap();
    db.table("t").number("id").number("a").primary_key("id").build().unwrap();

    let t = db.table("t")
        .number("id")
        .number("a")
        .tag("label")
        .primary_key("id")
        .build()
        .expect("in-session superset re-declaration should auto-migrate");
    let e = t.insert()
        .set("id", 1i64)
        .set("a", 1i64)
        .set("label", "x")
        .commit()
        .unwrap();
    assert_eq!(t.entity(e).get("label"), Some(Value::Text("x".into())));

    cleanup(&path);
}

/// phase 1b: 矛盾する再宣言は loud error。
#[test]
fn conflicting_redeclaration_is_loud() {
    let path = tmp_path("conflict");
    cleanup(&path);
    let _ = setup_id_a(&path);

    let mut db = Database::open(&path).unwrap();

    // 1b-1: on-disk 列 'a' の欠落 (declare (id, c))
    let r = db.table("t").number("id").number("c").build();
    match r {
        Err(SchemaError::SchemaConflict(msg)) => {
            assert!(msg.contains("a"), "error should name the missing column, got: {msg}");
        }
        other => panic!("missing on-disk column must be SchemaConflict, got: {:?}",
                        other.err().map(|e| format!("{e:?}"))),
    }

    // 1b-2: 型不一致 (a を Tag で宣言)
    let r = db.table("t").number("id").tag("a").build();
    assert!(
        matches!(r, Err(SchemaError::SchemaConflict(_))),
        "type mismatch must be SchemaConflict, got: {:?}",
        r.err().map(|e| format!("{e:?}"))
    );

    // 1b-3: 並べ替え (declare (a, id))
    let r = db.table("t").number("a").number("id").build();
    assert!(
        matches!(r, Err(SchemaError::SchemaConflict(_))),
        "reordered declaration must be SchemaConflict, got: {:?}",
        r.err().map(|e| format!("{e:?}"))
    );

    // 1b-4: 宣言列数が on-disk より少ない (declare (id) のみ)
    let r = db.table("t").number("id").build();
    assert!(
        matches!(r, Err(SchemaError::SchemaConflict(_))),
        "subset declaration must be SchemaConflict, got: {:?}",
        r.err().map(|e| format!("{e:?}"))
    );

    // 1b-5: PK 不一致 (pk を a に付け替え)
    let r = db.table("t").number("id").number("a").primary_key("a").build();
    assert!(
        matches!(r, Err(SchemaError::SchemaConflict(_))),
        "pk mismatch must be SchemaConflict, got: {:?}",
        r.err().map(|e| format!("{e:?}"))
    );

    // conflict 後も既存 schema は無傷 (= 2 列のまま)
    assert_eq!(db.list_tables()[0].columns.len(), 2);

    // cols 未宣言の handle 取得 idiom は従来通り OK
    assert!(db.table("t").build().is_ok());

    cleanup(&path);
}

/// phase 2: standalone (Database::open) の add_column は従来通り動く。
#[test]
fn add_column_standalone_still_works() {
    let path = tmp_path("standalone");
    cleanup(&path);
    let old_row = setup_id_a(&path);

    {
        let mut db = Database::open(&path).unwrap();
        db.add_column("t", "c", ColumnType::Number).unwrap();
        // idempotent
        db.add_column("t", "c", ColumnType::Number).unwrap();

        let t = db.get_table("t").unwrap();
        let e = t.insert().set("id", 3i64).set("c", 33i64).commit().unwrap();
        assert_eq!(t.entity(e).get("c"), Some(Value::Number(33)));
        assert_eq!(t.entity(old_row).get("c"), None);
    }
    // reopen で c が残る
    {
        let db = Database::open(&path).unwrap();
        let cols: Vec<String> = db.list_tables()[0].columns.iter().map(|c| c.name.clone()).collect();
        assert!(cols.contains(&"c".to_string()), "c must persist, got: {cols:?}");
    }

    cleanup(&path);
}

/// phase 3 (G2 本命): open_with_oplog (concurrent) の Database でも
/// add_column が通り、 新列への row write が round-trip する。
#[test]
fn add_column_on_concurrent_oplog_db_works() {
    let path = tmp_path("concurrent");
    cleanup(&path);
    let old_row = setup_id_a(&path);

    {
        let mut arc: Arc<Database> = Database::open_with_oplog(&path, 64 * 1024).unwrap();
        assert!(arc.is_concurrent());
        // open 直後 = Arc 単一所有の migration window で add_column
        Arc::get_mut(&mut arc)
            .expect("freshly opened Arc<Database> is uniquely owned")
            .add_column("t", "b", ColumnType::Number)
            .expect("add_column must work on concurrent (open_with_oplog) Database");

        let t = arc.get_table("t").unwrap();
        // 新列 round-trip (新規 row + 旧 row への後付け)
        let e = t.insert().set("id", 2i64).set("a", 20i64).set("b", 222i64).commit().unwrap();
        assert_eq!(t.entity(e).get("b"), Some(Value::Number(222)));
        t.entity(old_row).set("b", 111i64).commit().unwrap();
        assert_eq!(t.entity(old_row).get("b"), Some(Value::Number(111)));
        assert_eq!(t.entity(old_row).get("a"), Some(Value::Number(10)));

        arc.engine().oplog_sync().unwrap();
        // drop → consumer thread shutdown sync
    }

    // reopen (standalone) で schema + data が残る
    {
        let db = Database::open(&path).unwrap();
        let cols: Vec<String> = db.list_tables()[0].columns.iter().map(|c| c.name.clone()).collect();
        assert!(cols.contains(&"b".to_string()), "b must persist after concurrent add_column, got: {cols:?}");
        let t = db.get_table("t").unwrap();
        assert_eq!(t.entity(old_row).get("b"), Some(Value::Number(111)));
    }

    cleanup(&path);
}

/// phase 3': concurrent DB への build() superset 再宣言 (= declarative 経路) も
/// Arc 単一所有 window なら通る (両経路が #73 のタイトル)。
#[test]
fn redeclare_superset_on_concurrent_oplog_db_works() {
    let path = tmp_path("concurrent-build");
    cleanup(&path);
    let _ = setup_id_a(&path);

    {
        let mut arc: Arc<Database> = Database::open_with_oplog(&path, 64 * 1024).unwrap();
        let db = Arc::get_mut(&mut arc).expect("uniquely owned");
        let t = db.table("t")
            .number("id")
            .number("a")
            .number("b")
            .primary_key("id")
            .build()
            .expect("superset build() must auto-migrate on concurrent Database");
        let e = t.insert().set("id", 5i64).set("a", 50i64).set("b", 500i64).commit().unwrap();
        assert_eq!(t.entity(e).get("b"), Some(Value::Number(500)));
        arc.engine().oplog_sync().unwrap();
    }

    cleanup(&path);
}

/// 0.9.0: `_c_` prefix は engine content 互換 layer の予約 himo 名前空間なので、
/// user column としては build / add_column 両経路で reject。
#[test]
fn c_prefixed_column_names_are_rejected() {
    let path = tmp_path("c-prefix");
    cleanup(&path);

    let mut db = Database::create(&path).unwrap();

    // build 経路 (新規 table)
    let r = db.table("t").number("id").number("_c_meta").build();
    assert!(
        matches!(r, Err(SchemaError::BadValue(_))),
        "_c_ column must be rejected at build(), got: {:?}",
        r.err().map(|e| format!("{e:?}"))
    );

    // 正常 table を作ってから add_column 経路
    db.table("t2").number("id").primary_key("id").build().unwrap();
    let r = db.add_column("t2", "_c_blob", ColumnType::Leaf);
    assert!(
        matches!(r, Err(SchemaError::BadValue(_))),
        "_c_ column must be rejected at add_column(), got: {:?}",
        r.err().map(|e| format!("{e:?}"))
    );

    // build() migration の trailing 新列経路でも reject
    let r = db.table("t2").number("id").leaf("_c_note").build();
    assert!(
        matches!(r, Err(SchemaError::BadValue(_))),
        "_c_ trailing column must be rejected in migration, got: {:?}",
        r.err().map(|e| format!("{e:?}"))
    );

    cleanup(&path);
}

/// trailing 新列に Ref (relation 付き) を足す migration は standalone で通る。
#[test]
fn redeclare_with_trailing_ref_column_standalone() {
    let path = tmp_path("trailing-ref");
    cleanup(&path);

    let company_eid;
    {
        let mut db = Database::create(&path).unwrap();
        let companies = db.table("companies").number("id").tag("name").primary_key("id").build().unwrap();
        company_eid = companies.insert().set("id", 1i64).set("name", "Acme").commit().unwrap();
        db.table("users").number("id").tag("name").primary_key("id").build().unwrap();
    }

    {
        let mut db = Database::open(&path).unwrap();
        let users = db.table("users")
            .number("id")
            .tag("name")
            .ref_to("company", "companies")
            .primary_key("id")
            .build()
            .expect("trailing ref column should migrate on standalone DB");
        let e = users.insert()
            .set("id", 1i64)
            .set("name", "Alice")
            .set("company", Value::Ref(company_eid))
            .commit()
            .unwrap();
        let staff = users.where_ref("company", company_eid).find().unwrap();
        assert_eq!(staff, vec![e]);
    }

    // reopen で relation も復元される
    {
        let db = Database::open(&path).unwrap();
        let info = db.list_tables();
        let users = info.iter().find(|t| t.name == "users").unwrap();
        let company_col = users.columns.iter().find(|c| c.name == "company").unwrap();
        assert_eq!(company_col.ty, ColumnType::Ref);
        assert_eq!(company_col.ref_to.as_deref(), Some("companies"));
    }

    cleanup(&path);
}
