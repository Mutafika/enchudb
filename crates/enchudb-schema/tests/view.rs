//! View invariant — issue #12。
//!
//! 「ある scope (例: Alice) の logical view を開いたら、 物理配置 (container 内
//! の table cluster か 単独 DB ファイルか) に関係なく同じ schema / 同じ data /
//! 同じ query が見える」 という不変式を test で担保する。
//!
//! 「tenant」 は caller の解釈名 (= use case のラベル)、 view 自体は scope の
//! 意味を知らない。 DB も view の用途を知らない、 ただ prefix を貼って table を
//! 解決するだけ。

use enchudb_schema::{Database, Value};
use std::sync::atomic::{AtomicU64, Ordering};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn tmp_path(tag: &str) -> String {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "/tmp/enchudb-view-{}-{}-{}.db",
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

/// pattern A (container.view("alice")) と pattern B (alice_db.as_view()) が
/// 同じ closure で同じ shape の結果を返す = 不変式そのもの。
#[test]
fn invariant_holds_across_topologies() {
    // pattern A: container DB に alice scope を切る
    let path_a = tmp_path("container");
    cleanup(&path_a);
    let mut db_a = Database::create(&path_a).unwrap();
    {
        let mut alice = db_a.view_mut("alice");
        alice.table("users")
            .number("id")
            .tag("name")
            .number("age")
            .primary_key("id")
            .build()
            .unwrap();
    }
    {
        // view("alice") の view 経由で insert
        let alice = db_a.view("alice");
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
    fn query_view_30(view: &enchudb_schema::View<'_>) -> usize {
        let users = view.get_table("users").expect("users in view");
        users.where_eq("age", 30u32).find().unwrap().len()
    }
    fn query_view_25(view: &enchudb_schema::View<'_>) -> usize {
        let users = view.get_table("users").expect("users in view");
        users.where_eq("age", 25u32).find().unwrap().len()
    }

    let view_a = db_a.view("alice");
    let view_b = db_b.as_view();
    assert_eq!(query_view_30(&view_a), 1, "pattern A: 1 user aged 30");
    assert_eq!(query_view_30(&view_b), 1, "pattern B: 1 user aged 30");
    assert_eq!(query_view_25(&view_a), 1, "pattern A: 1 user aged 25");
    assert_eq!(query_view_25(&view_b), 1, "pattern B: 1 user aged 25");

    cleanup(&path_a);
    cleanup(&path_b);
}

/// container DB に alice と bob を両方入れて、 view("alice").list_tables() が
/// alice のものだけ返す + prefix が剥がれて short name で返る。
#[test]
fn list_tables_isolation_and_prefix_stripping() {
    let path = tmp_path("isolation");
    cleanup(&path);

    let mut db = Database::create(&path).unwrap();
    {
        db.view_mut("alice").table("users").number("id").primary_key("id").build().unwrap();
        db.view_mut("alice").table("posts").number("id").primary_key("id").build().unwrap();
        db.view_mut("bob").table("users").number("id").primary_key("id").build().unwrap();
    }

    // view("alice") は alice のものだけ、 short name で返る
    let alice_tables: Vec<String> = db.view("alice").list_tables()
        .into_iter().map(|t| t.name).collect();
    alice_tables.iter().for_each(|n| eprintln!("alice view: {}", n));
    assert!(alice_tables.contains(&"users".to_string()), "alice view sees users");
    assert!(alice_tables.contains(&"posts".to_string()), "alice view sees posts");
    assert_eq!(alice_tables.len(), 2, "alice view sees exactly 2 tables");

    let bob_tables: Vec<String> = db.view("bob").list_tables()
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

/// view_mut で建てたものが、 同 process 内で view() から引ける + reopen 後にも
/// view() で引ける = persistence + 解決の round trip。
#[test]
fn build_via_view_mut_round_trip() {
    let path = tmp_path("round_trip");
    cleanup(&path);

    {
        let mut db = Database::create(&path).unwrap();
        db.view_mut("alice").table("memos")
            .number("id")
            .leaf("body")
            .primary_key("id")
            .build()
            .unwrap();
        let alice = db.view("alice");
        let memos = alice.get_table("memos").expect("alice.memos same session");
        memos.insert().set("id", 1).set("body", "hello").commit().unwrap();
    }

    // reopen
    {
        let db = Database::open(&path).unwrap();
        let alice = db.view("alice");
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

/// 現実的な multi-tenant scenario (= view を tenant 用途で使う代表 case):
/// container DB に 3 tenant、 各 tenant に 100 row、 query が view scope に
/// 閉じる + cross-tenant の data 漏れが無いことを確認。
///
/// (API は中立な view、 ここで「tenant」 と呼ぶのは use case のラベル。)
#[test]
fn realistic_multi_tenant_scenario() {
    let path = tmp_path("multi_tenant_realistic");
    cleanup(&path);

    let mut db = Database::create(&path).unwrap();

    let tenants = ["alice", "bob", "carol"];
    for tenant in &tenants {
        db.view_mut(tenant)
            .table("users")
            .number("id")
            .tag("name")
            .number("age")
            .primary_key("id")
            .build()
            .unwrap();
    }

    // 各 tenant に 100 row insert、 age は tenant 内 0..100
    for tenant in &tenants {
        let view = db.view(tenant);
        let users = view.get_table("users").unwrap();
        for i in 0..100u32 {
            users
                .insert()
                .set("id", i)
                .set("name", format!("{}-{}", tenant, i))
                .set("age", 20 + (i % 60))
                .commit()
                .unwrap();
        }
    }

    // 各 tenant 単独で count、 自分の row しか見えない
    for tenant in &tenants {
        let view = db.view(tenant);
        let users = view.get_table("users").unwrap();

        // age == 30 の row 数 (= (30-20)%60 == 10 で割れる id) — それぞれ ceil(100/60) 個
        let count_30 = users.where_eq("age", 30u32).find().unwrap().len();
        assert!(count_30 > 0, "{}: age==30 が居る", tenant);

        // id == 42 の row、 name は tenant prefix で識別可能
        let r = users.where_eq("id", 42u32).find().unwrap();
        assert_eq!(r.len(), 1, "{}: id=42 が 1 件", tenant);
        let name_val = users.entity(r[0]).get("name").unwrap();
        match name_val {
            Value::Text(s) => {
                assert!(
                    s.starts_with(&format!("{}-", tenant)),
                    "{}: name が tenant-prefix で始まる: {}",
                    tenant,
                    s
                );
            }
            other => panic!("expected Text, got {:?}", other),
        }
    }

    // root view (= raw container) で全 table と全 row が見える (cross-tenant aggregator 想定)
    let root_tables: Vec<String> = db
        .as_view()
        .list_tables()
        .into_iter()
        .map(|t| t.name)
        .collect();
    let expected = tenants.iter().map(|t| format!("{}.users", t)).collect::<Vec<_>>();
    for e in &expected {
        assert!(root_tables.contains(e), "root sees {}", e);
    }

    // alice の view から bob のデータが name 経由で漏れないかチェック (= isolation 完全性)
    let alice_view = db.view("alice");
    let alice_users = alice_view.get_table("users").unwrap();
    let bob_42_name = format!("bob-42");
    // tag query は他 tenant の name とは独立な vocab なので、 bob-42 は alice の
    // テーブルには存在しない。 でも vocab は engine global なので、 alice.users で
    // where_eq する場合、 alice のテーブル内に bob-42 がない事を確認する経路で見る
    let bob42_in_alice = alice_users
        .all()
        .find()
        .unwrap()
        .iter()
        .filter_map(|&eid| match alice_users.entity(eid).get("name") {
            Some(Value::Text(s)) if s == bob_42_name => Some(()),
            _ => None,
        })
        .count();
    assert_eq!(bob42_in_alice, 0, "alice view に bob-42 が無い (cross-tenant 漏れ無し)");

    cleanup(&path);
}

/// build view と read view を交互に使うパターン (build → read → build → read) の
/// 正しさ。 実 app では migration や lazy schema 拡張で出てくる。
#[test]
fn interleaved_build_and_read_via_view() {
    let path = tmp_path("interleaved");
    cleanup(&path);

    let mut db = Database::create(&path).unwrap();

    // 1) view_mut で users 定義
    db.view_mut("alice")
        .table("users")
        .number("id")
        .tag("name")
        .primary_key("id")
        .build()
        .unwrap();

    // 2) view() で users に insert
    {
        let alice = db.view("alice");
        alice
            .get_table("users")
            .unwrap()
            .insert()
            .set("id", 1)
            .set("name", "Alice")
            .commit()
            .unwrap();
    }

    // 3) view_mut でさらに posts 定義
    db.view_mut("alice")
        .table("posts")
        .number("id")
        .leaf("body")
        .primary_key("id")
        .build()
        .unwrap();

    // 4) view() から両方見える
    {
        let alice = db.view("alice");
        let tables: Vec<String> = alice.list_tables().into_iter().map(|t| t.name).collect();
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"posts".to_string()));
        assert_eq!(tables.len(), 2);
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
