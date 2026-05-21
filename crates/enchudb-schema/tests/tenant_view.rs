//! Tenant view invariant — issue #12。
//!
//! 「Alice という logical view を開いたら、 物理配置 (server の table cluster か
//! Alice の device 上の DB ファイルか) に関係なく同じ schema / 同じ data / 同じ
//! query が見える」 という不変式を test で担保する。

use enchudb_schema::{Database, Value};
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_path(tag: &str) -> String {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "/tmp/enchudb-tenant-view-{}-{}-{}.db",
        tag,
        std::process::id(),
        n
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
    let _ = std::fs::remove_file(format!("{}.tables", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));
}

/// pattern A (container.tenant("alice")) と pattern B (alice_db.as_view()) が
/// 同じ closure で同じ shape の結果を返す = 不変式そのもの。
#[test]
fn invariant_holds_across_topologies() {
    // pattern A: container DB に alice tenant を切る
    let path_a = tmp_path("container");
    cleanup(&path_a);
    let mut db_a = Database::create(&path_a).unwrap();
    {
        let mut alice = db_a.tenant_mut("alice");
        alice.table("users")
            .number("id")
            .tag("name")
            .number("age")
            .primary_key("id")
            .build()
            .unwrap();
    }
    {
        // tenant("alice") の view 経由で insert
        let alice = db_a.tenant("alice");
        let users = alice.get_table("users").expect("alice.users via view");
        users.insert().set("id", 1).set("name", "Alice").set("age", 30).commit().unwrap();
        users.insert().set("id", 2).set("name", "Aki").set("age", 25).commit().unwrap();
    }

    // pattern B: 単独 DB ファイル
    let path_b = tmp_path("per_user");
    cleanup(&path_b);
    let mut db_b = Database::create(&path_b).unwrap();
    {
        let mut root = db_b.as_view_mut();
        root.table("users")
            .number("id")
            .tag("name")
            .number("age")
            .primary_key("id")
            .build()
            .unwrap();
    }
    {
        let root = db_b.as_view();
        let users = root.get_table("users").expect("root.users via view");
        users.insert().set("id", 1).set("name", "Alice").set("age", 30).commit().unwrap();
        users.insert().set("id", 2).set("name", "Aki").set("age", 25).commit().unwrap();
    }

    // 同一 closure で両 view を query、 同じ shape が返ることを確認
    fn query_view_30(view: &enchudb_schema::TenantView<'_>) -> usize {
        let users = view.get_table("users").expect("users in view");
        users.where_eq("age", 30u32).find().unwrap().len()
    }
    fn query_view_25(view: &enchudb_schema::TenantView<'_>) -> usize {
        let users = view.get_table("users").expect("users in view");
        users.where_eq("age", 25u32).find().unwrap().len()
    }

    let view_a = db_a.tenant("alice");
    let view_b = db_b.as_view();
    assert_eq!(query_view_30(&view_a), 1, "pattern A: 1 user aged 30");
    assert_eq!(query_view_30(&view_b), 1, "pattern B: 1 user aged 30");
    assert_eq!(query_view_25(&view_a), 1, "pattern A: 1 user aged 25");
    assert_eq!(query_view_25(&view_b), 1, "pattern B: 1 user aged 25");

    cleanup(&path_a);
    cleanup(&path_b);
}

/// container DB に alice と bob を両方入れて、 tenant("alice").list_tables() が
/// alice のものだけ返す + prefix が剥がれて short name で返る。
#[test]
fn list_tables_isolation_and_prefix_stripping() {
    let path = tmp_path("isolation");
    cleanup(&path);

    let mut db = Database::create(&path).unwrap();
    {
        db.tenant_mut("alice").table("users").number("id").primary_key("id").build().unwrap();
        db.tenant_mut("alice").table("posts").number("id").primary_key("id").build().unwrap();
        db.tenant_mut("bob").table("users").number("id").primary_key("id").build().unwrap();
    }

    // tenant("alice") は alice のものだけ、 short name で返る
    let alice_tables: Vec<String> = db.tenant("alice").list_tables()
        .into_iter().map(|t| t.name).collect();
    alice_tables.iter().for_each(|n| eprintln!("alice view: {}", n));
    assert!(alice_tables.contains(&"users".to_string()), "alice view sees users");
    assert!(alice_tables.contains(&"posts".to_string()), "alice view sees posts");
    assert_eq!(alice_tables.len(), 2, "alice view sees exactly 2 tables");

    let bob_tables: Vec<String> = db.tenant("bob").list_tables()
        .into_iter().map(|t| t.name).collect();
    assert_eq!(bob_tables, vec!["users".to_string()], "bob view sees only users");

    // root view は全部見える (raw 名で)
    let root_tables: Vec<String> = db.as_view().list_tables()
        .into_iter().map(|t| t.name).collect();
    assert!(root_tables.iter().any(|n| n == "alice.users"));
    assert!(root_tables.iter().any(|n| n == "alice.posts"));
    assert!(root_tables.iter().any(|n| n == "bob.users"));

    cleanup(&path);
}

/// build_via_tenant_mut で建てたものが、 同 process 内で tenant() の view から
/// 引ける + reopen 後にも tenant() で引ける = persistence + 解決の round trip。
#[test]
fn build_via_tenant_mut_round_trip() {
    let path = tmp_path("round_trip");
    cleanup(&path);

    {
        let mut db = Database::create(&path).unwrap();
        db.tenant_mut("alice").table("memos")
            .number("id")
            .leaf("body")
            .primary_key("id")
            .build()
            .unwrap();
        let alice = db.tenant("alice");
        let memos = alice.get_table("memos").expect("alice.memos same session");
        memos.insert().set("id", 1).set("body", "hello").commit().unwrap();
    }

    // reopen
    {
        let db = Database::open(&path).unwrap();
        let alice = db.tenant("alice");
        let memos = alice.get_table("memos").expect("alice.memos after reopen");
        let body = memos.where_eq("id", 1u32).find().unwrap();
        assert_eq!(body.len(), 1, "memo with id=1 found");
        let body_val = memos.entity(body[0]).get("body").unwrap();
        match body_val {
            Value::Text(s) => assert_eq!(s, "hello"),
            other => panic!("expected Text, got {:?}", other),
        }
    }

    cleanup(&path);
}

/// `as_view()` (root view) が単独 DB の全 table を見せる (= pattern B のための
/// fallback、 prefix なし)。
#[test]
fn root_view_sees_all_tables_unprefixed() {
    let path = tmp_path("root_view");
    cleanup(&path);

    let mut db = Database::create(&path).unwrap();
    {
        let mut root = db.as_view_mut();
        root.table("items").number("id").primary_key("id").build().unwrap();
        root.table("notes").number("id").primary_key("id").build().unwrap();
    }

    let names: Vec<String> = db.as_view().list_tables().into_iter().map(|t| t.name).collect();
    assert!(names.contains(&"items".to_string()));
    assert!(names.contains(&"notes".to_string()));
    assert_eq!(names.len(), 2);

    // root view で prefix 無しで table が引ける
    let items = db.as_view().get_table("items").expect("items via root view");
    items.insert().set("id", 42).commit().unwrap();
    assert_eq!(items.where_eq("id", 42u32).find().unwrap().len(), 1);

    cleanup(&path);
}
