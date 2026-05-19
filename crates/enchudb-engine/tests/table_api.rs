//! β-light step 3: define_table / entity_in 公開 API のテスト。
//!
//! 検証範囲:
//!   - 旧 API (entity / define_himo / tie) は引き続き anonymous で動作
//!   - define_table が anonymous を close する
//!   - close 後の entity() は panic
//!   - entity_in が table 内 eid range を割り当てる
//!   - 重複 / 容量超過 / 不在 table のエラーケース

use enchudb_engine::{Engine, HimoType};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-table-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suffix in ["", ".wal", ".crc", ".db.lock"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

#[test]
fn legacy_api_works_without_define_table() {
    let path = tmp_path("legacy");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("age", HimoType::Number, 100);
    let e1 = eng.entity();
    let e2 = eng.entity();
    eng.tie(e1, "age", 30);
    eng.tie(e2, "age", 25);
    eng.flush().unwrap();

    let rows = eng.pull_raw("age", 30);
    assert_eq!(rows.len(), 1);

    drop(eng);
    cleanup(&path);
}

#[test]
fn define_table_creates_separate_eid_range() {
    let path = tmp_path("define");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    let tid = eng.define_table("users", 1_000).expect("define users");
    assert_eq!(tid, 1, "first user-defined table should be id 1 (0 is anonymous)");

    let info = eng.list_tables();
    assert_eq!(info.len(), 2, "anonymous + users = 2 tables");
    let users = info.iter().find(|(_, n, _, _)| n == "users").unwrap();
    assert_eq!(users.2, 0, "users.eid_range_lo");
    assert_eq!(users.3, 1_000, "users.eid_range_hi");
    let anon = info.iter().find(|(_, n, _, _)| n.is_empty()).unwrap();
    assert_eq!(anon.3, 0, "anonymous closed at next_eid=0");

    drop(eng);
    cleanup(&path);
}

#[test]
fn entity_in_allocates_within_range() {
    let path = tmp_path("entity_in");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_table("users", 100).unwrap();
    eng.define_himo("age", HimoType::Number, 100);

    let e1 = eng.entity_in("users").unwrap();
    let e2 = eng.entity_in("users").unwrap();
    let local1 = enchudb_wal::eid_local(e1);
    let local2 = enchudb_wal::eid_local(e2);
    assert!(local1 < local2, "monotonic alloc");
    assert!(local1 < 100, "in users range");
    assert!(local2 < 100, "in users range");

    eng.tie(e1, "age", 30);
    let rows = eng.pull_raw("age", 30);
    assert_eq!(rows.len(), 1);

    drop(eng);
    cleanup(&path);
}

#[test]
fn entity_after_define_table_panics() {
    let path = tmp_path("entity_panic");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_table("users", 100).unwrap();

    // anonymous closed なので entity() は panic するはず
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        eng.entity()
    }));
    assert!(result.is_err(), "entity() should panic after define_table");

    cleanup(&path);
}

#[test]
fn duplicate_table_name_errors() {
    let path = tmp_path("dup");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_table("users", 100).unwrap();
    let result = eng.define_table("users", 100);
    assert!(result.is_err(), "duplicate name should error");
    assert!(
        result.unwrap_err().contains("already exists"),
        "error msg should mention existence"
    );

    drop(eng);
    cleanup(&path);
}

#[test]
fn entity_in_unknown_table_errors() {
    let path = tmp_path("unknown");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    let result = eng.entity_in("missing");
    assert!(result.is_err(), "unknown table should error");
    assert!(result.unwrap_err().contains("not found"));

    cleanup(&path);
}

#[test]
fn entity_in_range_exhausted_errors() {
    let path = tmp_path("exhausted");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_table("tiny", 3).unwrap();

    let _ = eng.entity_in("tiny").unwrap();
    let _ = eng.entity_in("tiny").unwrap();
    let _ = eng.entity_in("tiny").unwrap();
    let result = eng.entity_in("tiny");
    assert!(result.is_err(), "should error when range exhausted");
    assert!(result.unwrap_err().contains("exhausted"));

    cleanup(&path);
}

#[test]
fn two_tables_have_disjoint_eid_ranges() {
    let path = tmp_path("disjoint");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_table("a", 100).unwrap();
    eng.define_table("b", 100).unwrap();

    let info = eng.list_tables();
    let a = info.iter().find(|(_, n, _, _)| n == "a").unwrap();
    let b = info.iter().find(|(_, n, _, _)| n == "b").unwrap();
    assert_eq!(a.2, 0);
    assert_eq!(a.3, 100);
    assert_eq!(b.2, 100);
    assert_eq!(b.3, 200);

    let ea = eng.entity_in("a").unwrap();
    let eb = eng.entity_in("b").unwrap();
    let la = enchudb_wal::eid_local(ea);
    let lb = enchudb_wal::eid_local(eb);
    assert!(la < 100, "a in [0, 100)");
    assert!(lb >= 100 && lb < 200, "b in [100, 200)");

    cleanup(&path);
}

#[test]
fn define_himo_in_namespaces_full_name() {
    let path = tmp_path("define_himo_in");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_table("users", 100).unwrap();
    let hid = eng
        .define_himo_in("users", "age", HimoType::Number, 100)
        .expect("define users.age");
    assert_eq!(hid, 0, "first himo should be id 0");

    let e1 = eng.entity_in("users").unwrap();
    eng.tie(e1, "users.age", 30);
    let rows = eng.pull_raw("users.age", 30);
    assert_eq!(rows.len(), 1);

    // bare 名は別 himo として扱われる (storage が full name で分離)
    let bare = eng.pull_raw("age", 30);
    assert!(bare.is_empty(), "bare 'age' is a different himo");

    cleanup(&path);
}

#[test]
fn define_himo_in_separates_two_tables() {
    let path = tmp_path("two_table_himo");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_table("users", 100).unwrap();
    eng.define_table("orders", 100).unwrap();
    eng.define_himo_in("users", "age", HimoType::Number, 100).unwrap();
    eng.define_himo_in("orders", "total", HimoType::Number, 100).unwrap();

    let u = eng.entity_in("users").unwrap();
    let o = eng.entity_in("orders").unwrap();
    eng.tie(u, "users.age", 30);
    eng.tie(o, "orders.total", 999);

    assert_eq!(eng.pull_raw("users.age", 30).len(), 1);
    assert_eq!(eng.pull_raw("orders.total", 999).len(), 1);
    // namespace 衝突なし: users.age に "999" は乗ってない
    assert_eq!(eng.pull_raw("users.age", 999).len(), 0);

    cleanup(&path);
}

#[test]
fn define_himo_in_rejects_dot_in_name() {
    let path = tmp_path("dot_name");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_table("users", 100).unwrap();
    let result = eng.define_himo_in("users", "a.b", HimoType::Number, 100);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("must not contain '.'"));

    cleanup(&path);
}

#[test]
fn define_himo_in_unknown_table_errors() {
    let path = tmp_path("missing_tbl");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    let result = eng.define_himo_in("missing", "age", HimoType::Number, 100);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("not found"));

    cleanup(&path);
}

#[test]
fn define_ref_in_creates_fk_link() {
    let path = tmp_path("ref_link");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_table("users", 100).unwrap();
    eng.define_table("posts", 100).unwrap();
    let hid = eng
        .define_ref_in("posts", "author", "users")
        .expect("define posts.author -> users");
    assert_eq!(hid, 0);

    let alice = eng.entity_in("users").unwrap();
    let post = eng.entity_in("posts").unwrap();
    let alice_local = enchudb_wal::eid_local(alice);
    eng.tie(post, "posts.author", alice_local); // OK: alice in users range
    let posts_by_alice = eng.pull_raw("posts.author", alice_local);
    assert_eq!(posts_by_alice.len(), 1);

    cleanup(&path);
}

#[test]
fn ref_tie_out_of_range_panics() {
    let path = tmp_path("ref_violate");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_table("users", 100).unwrap();
    eng.define_table("posts", 100).unwrap();
    eng.define_ref_in("posts", "author", "users").unwrap();

    let post = eng.entity_in("posts").unwrap();
    // post の eid (>=100) を author に渡すと users range 外 → panic
    let bad_target = enchudb_wal::eid_local(post);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        eng.tie(post, "posts.author", bad_target);
    }));
    assert!(result.is_err(), "Ref to out-of-range eid should panic");

    cleanup(&path);
}

#[test]
fn define_ref_in_unknown_target_errors() {
    let path = tmp_path("ref_unknown");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_table("posts", 100).unwrap();
    let result = eng.define_ref_in("posts", "author", "missing");
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("target table 'missing' not found"));

    cleanup(&path);
}

#[test]
fn ref_without_fk_link_skips_validation() {
    // 旧 API 経路: HimoType::Ref で define_himo_in 経由 (define_ref_in を
    // 呼ばない場合) は fk_refs entry なし → validation skip。 後方互換性。
    let path = tmp_path("ref_no_fk");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_table("posts", 100).unwrap();
    eng.define_himo_in("posts", "author", HimoType::Ref, 0).unwrap();

    let post = eng.entity_in("posts").unwrap();
    // 何でも tie 可能 (validation skip)
    eng.tie(post, "posts.author", 9_999_999);
    assert_eq!(eng.pull_raw("posts.author", 9_999_999).len(), 1);

    cleanup(&path);
}

#[test]
fn anonymous_keeps_open_until_first_define_table() {
    let path = tmp_path("anon_open");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    // entity() を先に呼ぶ
    let e1 = eng.entity();
    let e2 = eng.entity();
    eng.define_himo("foo", HimoType::Number, 10);
    eng.tie(e1, "foo", 1);
    eng.tie(e2, "foo", 2);

    // ここで define_table
    eng.define_table("users", 100).unwrap();

    let info = eng.list_tables();
    let anon = info.iter().find(|(_, n, _, _)| n.is_empty()).unwrap();
    // anonymous は 2 個 entity を持って閉じている
    assert_eq!(anon.3, 2, "anonymous closed at next_eid=2");
    let users = info.iter().find(|(_, n, _, _)| n == "users").unwrap();
    assert_eq!(users.2, 2, "users starts after anonymous");
    assert_eq!(users.3, 102);

    cleanup(&path);
}
