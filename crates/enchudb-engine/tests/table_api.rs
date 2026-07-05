//! β-light step 3: define_table / entity_in 公開 API のテスト。
//!
//! 検証範囲:
//!   - 旧 API (entity / define_himo / tie) は引き続き anonymous で動作
//!   - define_table が anonymous を close する
//!   - close 後の entity() は panic
//!   - entity_in が table 内 eid range を割り当てる
//!   - 重複 / 容量超過 / 不在 table のエラーケース

use enchudb_engine::{Engine, ValueType};

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
    for suffix in ["", ".oplog", ".crc", ".db.lock", ".tables", ".tables.tmp"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

#[test]
fn legacy_api_works_without_define_table() {
    let path = tmp_path("legacy");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("age", ValueType::Number, 100);
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
    eng.define_himo_in("users", "age", ValueType::Number, 100).unwrap();

    let e1 = eng.entity_in("users").unwrap();
    let e2 = eng.entity_in("users").unwrap();
    let local1 = enchudb_oplog::eid_local(e1);
    let local2 = enchudb_oplog::eid_local(e2);
    assert!(local1 < local2, "monotonic alloc");
    assert!(local1 < 100, "in users range");
    assert!(local2 < 100, "in users range");

    eng.tie(e1, "users.age", 30);
    let rows = eng.pull_raw("users.age", 30);
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
    let la = enchudb_oplog::eid_local(ea);
    let lb = enchudb_oplog::eid_local(eb);
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
        .define_himo_in("users", "age", ValueType::Number, 100)
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
    eng.define_himo_in("users", "age", ValueType::Number, 100).unwrap();
    eng.define_himo_in("orders", "total", ValueType::Number, 100).unwrap();

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
    let result = eng.define_himo_in("users", "a.b", ValueType::Number, 100);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("must not contain '.'"));

    cleanup(&path);
}

#[test]
fn define_himo_in_unknown_table_errors() {
    let path = tmp_path("missing_tbl");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    let result = eng.define_himo_in("missing", "age", ValueType::Number, 100);
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
    let alice_local = enchudb_oplog::eid_local(alice);
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
    let bad_target = enchudb_oplog::eid_local(post);
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
    // 旧 API 経路: ValueType::Ref で define_himo_in 経由 (define_ref_in を
    // 呼ばない場合) は fk_refs entry なし → validation skip。 後方互換性。
    let path = tmp_path("ref_no_fk");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_table("posts", 100).unwrap();
    eng.define_himo_in("posts", "author", ValueType::Ref, 0).unwrap();

    let post = eng.entity_in("posts").unwrap();
    // 何でも tie 可能 (validation skip)
    eng.tie(post, "posts.author", 9_999_999);
    assert_eq!(eng.pull_raw("posts.author", 9_999_999).len(), 1);

    cleanup(&path);
}

#[test]
fn tie_out_of_table_range_panics() {
    let path = tmp_path("range_violate");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_table("users", 100).unwrap();
    eng.define_table("posts", 100).unwrap();
    eng.define_himo_in("users", "age", ValueType::Number, 100).unwrap();

    let _ = eng.entity_in("users").unwrap();
    let post = eng.entity_in("posts").unwrap();
    let post_local = enchudb_oplog::eid_local(post);

    // post の eid を users.age に tie しようとする → eid_range 外で panic
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        eng.tie(post, "users.age", 30);
    }));
    assert!(result.is_err(), "tie out-of-table-range should panic");
    let _ = post_local;

    cleanup(&path);
}

#[test]
fn anonymous_open_skips_eid_range_validation() {
    // anonymous (open-ended) は legacy 互換のため validate しない
    let path = tmp_path("anon_no_validate");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("age", ValueType::Number, 100);
    let e = eng.entity();
    eng.tie(e, "age", 30); // OK
    assert_eq!(eng.pull_raw("age", 30).len(), 1);

    cleanup(&path);
}

#[test]
fn anonymous_closed_validates_eid_range() {
    // anonymous が closed なら validate kick-in
    let path = tmp_path("anon_closed");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("age", ValueType::Number, 100);
    let e1 = eng.entity(); // anonymous に 1 個確保 (eid=0)
    eng.tie(e1, "age", 30); // anonymous open なので OK

    eng.define_table("users", 100).unwrap(); // anonymous を [0, 1) で close
    let user = eng.entity_in("users").unwrap();
    let user_local = enchudb_oplog::eid_local(user);

    // anonymous の age に user eid (= 1, 外) を tie → panic
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        eng.tie(user, "age", 99);
    }));
    assert!(result.is_err(), "should panic: anonymous closed at [0,1), user_local={}", user_local);

    cleanup(&path);
}

#[test]
fn positions_isolated_per_table() {
    // β-light の core win 検証: 2 つの table に大きく離れた eid 範囲を
    // 取らせ、 各 himo の query が正しく動くこと。
    let path = tmp_path("isolated");
    cleanup(&path);

    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_table("a", 1000).unwrap();
    eng.define_table("b", 1000).unwrap();
    eng.define_himo_in("a", "v", ValueType::Number, 100).unwrap();
    eng.define_himo_in("b", "v", ValueType::Number, 100).unwrap();

    // a に 10 個、 b に 10 個 entity 作って tie
    let mut a_eids = Vec::new();
    for _ in 0..10 {
        a_eids.push(eng.entity_in("a").unwrap());
    }
    let mut b_eids = Vec::new();
    for _ in 0..10 {
        b_eids.push(eng.entity_in("b").unwrap());
    }
    for (i, &e) in a_eids.iter().enumerate() {
        eng.tie(e, "a.v", (i % 5) as u32);
    }
    for (i, &e) in b_eids.iter().enumerate() {
        eng.tie(e, "b.v", (i % 5) as u32);
    }

    // a.v と b.v は独立: 同じ value=2 で引いてもそれぞれ 2 件ずつ
    assert_eq!(eng.pull_raw("a.v", 2).len(), 2);
    assert_eq!(eng.pull_raw("b.v", 2).len(), 2);
    // 結果 eid が混ざってない
    let a_results = eng.pull_raw("a.v", 2);
    for &e in &a_results {
        let local = enchudb_oplog::eid_local(e);
        assert!(local < 1000, "a.v result eid {} should be in a range", local);
    }
    let b_results = eng.pull_raw("b.v", 2);
    for &e in &b_results {
        let local = enchudb_oplog::eid_local(e);
        assert!(local >= 1000 && local < 2000, "b.v result eid {} should be in b range", local);
    }

    cleanup(&path);
}

#[test]
fn tables_persist_across_reopen() {
    // β-light step 7: schema 定義が sidecar file 経由で reopen に持ち越される
    let path = tmp_path("reopen");
    cleanup(&path);

    // 1. table + himo + entity を作って flush
    {
        let mut eng = Engine::create_standalone(&path).unwrap();
        eng.define_table("users", 100).unwrap();
        eng.define_table("posts", 100).unwrap();
        eng.define_himo_in("users", "age", ValueType::Number, 100).unwrap();
        eng.define_ref_in("posts", "author", "users").unwrap();

        let alice = eng.entity_in("users").unwrap();
        let post = eng.entity_in("posts").unwrap();
        eng.tie(alice, "users.age", 30);
        eng.tie(post, "posts.author", enchudb_oplog::eid_local(alice));
        eng.flush().unwrap();
    }

    // 2. reopen して schema が復元されてること、 query が動くことを確認
    {
        let eng = Engine::open_standalone(&path).unwrap();
        let tables = eng.list_tables();
        let user_table = tables.iter().find(|(_, n, _, _)| n == "users").unwrap();
        let post_table = tables.iter().find(|(_, n, _, _)| n == "posts").unwrap();
        assert_eq!(user_table.2, 0);
        assert_eq!(user_table.3, 100);
        assert_eq!(post_table.2, 100);
        assert_eq!(post_table.3, 200);

        // himo の table 帰属が復元されてる → users.age が users.eid_range で query
        let rows = eng.pull_raw("users.age", 30);
        assert_eq!(rows.len(), 1);
        let posts = eng.pull_raw("posts.author", 0);
        assert_eq!(posts.len(), 1);
    }

    cleanup(&path);
}

#[test]
fn entity_in_continues_after_reopen() {
    // β-light step 7: reopen 後の entity_in が次の eid を正しく割り当てる
    let path = tmp_path("reopen_entity");
    cleanup(&path);

    let alice_eid;
    {
        let mut eng = Engine::create_standalone(&path).unwrap();
        eng.define_table("users", 100).unwrap();
        eng.define_himo_in("users", "name", ValueType::Number, 10).unwrap();
        let alice = eng.entity_in("users").unwrap();
        alice_eid = enchudb_oplog::eid_local(alice);
        eng.tie(alice, "users.name", 1);
        eng.flush().unwrap();
    }

    {
        let mut eng = Engine::open_standalone(&path).unwrap();
        // alice の data は引ける
        assert_eq!(eng.pull_raw("users.name", 1).len(), 1);

        // 次の entity は alice と衝突しない eid
        let bob = eng.entity_in("users").unwrap();
        let bob_local = enchudb_oplog::eid_local(bob);
        assert_ne!(bob_local, alice_eid, "bob shouldn't reuse alice's eid");
        assert!(bob_local < 100, "bob in users range");
    }

    cleanup(&path);
}

#[test]
fn legacy_v4_db_opens_as_anonymous_only() {
    // β-light step 7: tables sidecar 不在 (v4-style) → anonymous fallback
    let path = tmp_path("v4_no_sidecar");
    cleanup(&path);

    {
        let mut eng = Engine::create_standalone(&path).unwrap();
        eng.define_himo("legacy_himo", ValueType::Number, 10);
        let e = eng.entity();
        eng.tie(e, "legacy_himo", 7);
        eng.flush().unwrap();
    }

    // sidecar 削除 = v4 DB 状態を模倣
    let _ = std::fs::remove_file(format!("{}.tables", path));

    {
        let eng = Engine::open_standalone(&path).unwrap();
        let tables = eng.list_tables();
        assert_eq!(tables.len(), 1, "anonymous only");
        assert_eq!(eng.pull_raw("legacy_himo", 7).len(), 1);
    }

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
    eng.define_himo("foo", ValueType::Number, 10);
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
