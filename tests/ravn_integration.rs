use std::sync::Arc;
use enchudb::{Engine, ValueType, Ravn, RavnResult};

fn db_path(tag: &str) -> String {
    let path = format!("/tmp/enchudb_ravn_{tag}.db");
    let _ = std::fs::remove_file(&path);
    path
}

// ════════════════ EC パス辿り ════════════════

#[test]
fn ec_path_navigation() {
    let path = db_path("ec_path");
    let mut eng = Engine::create_standalone(&path).unwrap();

    eng.define_himo("type", ValueType::Number, 10);
    eng.define_himo("price", ValueType::Number, 1000);
    eng.define_himo("status", ValueType::Number, 5);
    eng.define_himo("qty", ValueType::Number, 100);
    eng.define_himo("user_ref", ValueType::Ref, 0);
    eng.define_himo("order_ref", ValueType::Ref, 0);
    eng.define_himo("product_ref", ValueType::Ref, 0);
    eng.define_himo("name", ValueType::Tag, 0);

    // product(0)
    let product = eng.entity();
    eng.tie(product, "type", 1);
    eng.tie_text(product, "name", "Widget");
    eng.tie(product, "price", 500);

    // user(1)
    let user = eng.entity();
    eng.tie(user, "type", 2);
    eng.tie_text(user, "name", "Alice");

    // order(2)
    let order = eng.entity();
    eng.tie(order, "type", 3);
    eng.tie_ref(order, "user_ref", user);
    eng.tie(order, "status", 1);

    // order_item(3)
    let item = eng.entity();
    eng.tie(item, "type", 4);
    eng.tie_ref(item, "order_ref", order);
    eng.tie_ref(item, "product_ref", product);
    eng.tie(item, "qty", 3);

    eng.rebuild();
    let ravn = Ravn::new(Arc::new(eng));

    // item → order_ref → user_ref → name
    let buyer = ravn.path_text(item, &["order_ref", "user_ref", "name"]);
    assert_eq!(buyer, Some(b"Alice".to_vec()));

    // item → product_ref → price
    let price = ravn.path(item, &["product_ref", "price"]);
    assert_eq!(price, Some(500));

    // item → order_ref → status
    let status = ravn.path(item, &["order_ref", "status"]);
    assert_eq!(status, Some(1));
}

// ════════════════ SNS タイムライン ════════════════

#[test]
fn sns_timeline() {
    let path = db_path("sns_timeline");
    let mut eng = Engine::create_standalone(&path).unwrap();

    eng.define_himo("type", ValueType::Number, 10);
    eng.define_himo("author", ValueType::Ref, 0);
    eng.define_himo("follows", ValueType::Ref, 0);
    eng.define_himo("name", ValueType::Tag, 0);
    eng.define_himo("content_id", ValueType::Number, 100);

    // users
    let alice = eng.entity(); // 0
    eng.tie(alice, "type", 1);
    eng.tie_text(alice, "name", "Alice");

    let bob = eng.entity(); // 1
    eng.tie(bob, "type", 1);
    eng.tie_text(bob, "name", "Bob");

    let carol = eng.entity(); // 2
    eng.tie(carol, "type", 1);
    eng.tie_text(carol, "name", "Carol");

    // posts by Bob
    let post1 = eng.entity(); // 3
    eng.tie(post1, "type", 2);
    eng.tie_ref(post1, "author", bob);
    eng.tie(post1, "content_id", 10);

    let post2 = eng.entity(); // 4
    eng.tie(post2, "type", 2);
    eng.tie_ref(post2, "author", bob);
    eng.tie(post2, "content_id", 20);

    // posts by Carol
    let post3 = eng.entity(); // 5
    eng.tie(post3, "type", 2);
    eng.tie_ref(post3, "author", carol);
    eng.tie(post3, "content_id", 30);

    eng.rebuild();
    let ravn = Ravn::new(Arc::new(eng));

    // follow [bob, carol] → get all their posts' content_ids via follow
    // follow gets the value of "author" for each, but we need reverse lookup
    // Instead: find posts by author, then follow content
    // post1(3) → author → Bob(1)
    let author = ravn.path(post1, &["author"]);
    assert_eq!(author, Some(bob as u32));

    // follow from multiple posts to author
    let authors = ravn.follow(&[post1, post2, post3], &["author"]);
    assert_eq!(authors, vec![bob, bob, carol]);
}

// ════════════════ select + テキスト ════════════════

#[test]
fn select_with_text() {
    let path = db_path("select_text");
    let mut eng = Engine::create_standalone(&path).unwrap();

    eng.define_himo("type", ValueType::Number, 10);
    eng.define_himo("age", ValueType::Number, 100);
    eng.define_himo("name", ValueType::Tag, 0);
    eng.define_himo("city", ValueType::Tag, 0);

    let e1 = eng.entity();
    eng.tie(e1, "type", 1);
    eng.tie(e1, "age", 30);
    eng.tie_text(e1, "name", "Tanaka");
    eng.tie_text(e1, "city", "Tokyo");

    let e2 = eng.entity();
    eng.tie(e2, "type", 1);
    eng.tie(e2, "age", 25);
    eng.tie_text(e2, "name", "Suzuki");
    eng.tie_text(e2, "city", "Osaka");

    let e3 = eng.entity();
    eng.tie(e3, "type", 2);
    eng.tie(e3, "age", 40);
    eng.tie_text(e3, "name", "Sato");
    eng.tie_text(e3, "city", "Tokyo");

    eng.rebuild();
    let ravn = Ravn::new(Arc::new(eng));

    // select type=1, get age
    let rows = ravn.select(&[("type", 1)], &["age"]);
    assert_eq!(rows.len(), 2);
    let ages: Vec<u32> = rows.iter().filter_map(|(_, v)| v[0]).collect();
    assert!(ages.contains(&30));
    assert!(ages.contains(&25));

    // select_text type=1, get name and city
    let text_rows = ravn.select_text(&[("type", 1)], &["name", "city"]);
    assert_eq!(text_rows.len(), 2);
    let names: Vec<Vec<u8>> = text_rows.iter().filter_map(|(_, v)| v[0].clone()).collect();
    assert!(names.contains(&b"Tanaka".to_vec()));
    assert!(names.contains(&b"Suzuki".to_vec()));
}

// ════════════════ exec follow パイプ ════════════════

#[test]
fn exec_follow_pipe() {
    let path = db_path("exec_follow");
    let mut eng = Engine::create_standalone(&path).unwrap();

    eng.define_himo("type", ValueType::Number, 10);
    eng.define_himo("dept_ref", ValueType::Ref, 0);
    eng.define_himo("name", ValueType::Tag, 0);
    eng.define_himo("status", ValueType::Number, 5);

    // dept(0)
    let dept = eng.entity();
    eng.tie(dept, "type", 5);
    eng.tie_text(dept, "name", "Engineering");

    // employee(1)
    let emp1 = eng.entity();
    eng.tie(emp1, "type", 2);
    eng.tie(emp1, "status", 0);
    eng.tie_ref(emp1, "dept_ref", dept);

    // employee(2)
    let emp2 = eng.entity();
    eng.tie(emp2, "type", 2);
    eng.tie(emp2, "status", 0);
    eng.tie_ref(emp2, "dept_ref", dept);

    eng.rebuild();
    let ravn = Ravn::new(Arc::new(eng));

    // type:2 status:0 | follow dept_ref | select name
    // → find employees with status=0, follow dept_ref, select name
    match ravn.exec("type:2 status:0 | follow dept_ref") {
        RavnResult::Entities(eids) => {
            assert_eq!(eids.len(), 2);
            assert!(eids.iter().all(|&e| e == dept));
        }
        other => panic!("expected Entities, got {other:?}"),
    }

    // with select pipe
    match ravn.exec("type:2 status:0 | follow dept_ref | select type") {
        RavnResult::Values(rows) => {
            assert_eq!(rows.len(), 2);
            for (eid, vals) in &rows {
                assert_eq!(*eid, dept);
                assert_eq!(vals[0], Some(5));
            }
        }
        other => panic!("expected Values, got {other:?}"),
    }
}

// ════════════════ exec count ════════════════

#[test]
fn exec_count() {
    let path = db_path("exec_count");
    let mut eng = Engine::create_standalone(&path).unwrap();

    eng.define_himo("type", ValueType::Number, 10);
    eng.define_himo("category", ValueType::Number, 10);

    for i in 0..20u32 {
        let e = eng.entity();
        eng.tie(e, "type", 1);
        eng.tie(e, "category", i % 5);
    }
    for i in 0..10u32 {
        let e = eng.entity();
        eng.tie(e, "type", 2);
        eng.tie(e, "category", i % 5);
    }

    eng.rebuild();
    let ravn = Ravn::new(Arc::new(eng));

    // type:1 category:3 | count → 4 (20 entities / 5 categories)
    match ravn.exec("type:1 category:3 | count") {
        RavnResult::Count(4) => {}
        other => panic!("expected Count(4), got {other:?}"),
    }

    // type:2 | count → 10
    match ravn.exec("type:2 | count") {
        RavnResult::Count(10) => {}
        other => panic!("expected Count(10), got {other:?}"),
    }
}

// ════════════════ chained pipes ════════════════

#[test]
fn chained_pipes() {
    let path = db_path("chained");
    let mut eng = Engine::create_standalone(&path).unwrap();

    eng.define_himo("type", ValueType::Number, 10);
    eng.define_himo("user_ref", ValueType::Ref, 0);
    eng.define_himo("region_ref", ValueType::Ref, 0);
    eng.define_himo("name", ValueType::Tag, 0);

    // region(0)
    let region = eng.entity();
    eng.tie(region, "type", 1);
    eng.tie_text(region, "name", "Kanto");

    // user(1)
    let user1 = eng.entity();
    eng.tie(user1, "type", 2);
    eng.tie_ref(user1, "region_ref", region);

    // user(2)
    let user2 = eng.entity();
    eng.tie(user2, "type", 2);
    eng.tie_ref(user2, "region_ref", region);

    // posts by user1
    let post1 = eng.entity(); // 3
    eng.tie(post1, "type", 5);
    eng.tie_ref(post1, "user_ref", user1);

    let post2 = eng.entity(); // 4
    eng.tie(post2, "type", 5);
    eng.tie_ref(post2, "user_ref", user2);

    let post3 = eng.entity(); // 5
    eng.tie(post3, "type", 5);
    eng.tie_ref(post3, "user_ref", user1);

    eng.rebuild();
    let ravn = Ravn::new(Arc::new(eng));

    // type:5 | follow user_ref | follow region_ref | count
    // 3 posts → 3 users (user1, user2, user1) → 3 regions (all same)
    match ravn.exec("type:5 | follow user_ref | follow region_ref | count") {
        RavnResult::Count(3) => {}
        other => panic!("expected Count(3), got {other:?}"),
    }
}

// ════════════════ exec get ════════════════

#[test]
fn exec_get() {
    let path = db_path("exec_get");
    let mut eng = Engine::create_standalone(&path).unwrap();

    eng.define_himo("type", ValueType::Number, 10);
    eng.define_himo("score", ValueType::Number, 100);

    let e1 = eng.entity();
    eng.tie(e1, "type", 1);
    eng.tie(e1, "score", 42);

    let e2 = eng.entity();
    eng.tie(e2, "type", 1);
    eng.tie(e2, "score", 77);

    eng.rebuild();
    let ravn = Ravn::new(Arc::new(eng));

    match ravn.exec("type:1 | get score") {
        RavnResult::Values(rows) => {
            assert_eq!(rows.len(), 2);
            let scores: Vec<u32> = rows.iter().filter_map(|(_, v)| v[0]).collect();
            assert!(scores.contains(&42));
            assert!(scores.contains(&77));
        }
        other => panic!("expected Values, got {other:?}"),
    }
}

// ════════════════ exec エラーケース ════════════════

#[test]
fn exec_errors() {
    let path = db_path("exec_errors");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("type", ValueType::Number, 10);
    let e = eng.entity();
    eng.tie(e, "type", 1);
    eng.rebuild();
    let ravn = Ravn::new(Arc::new(eng));

    assert!(matches!(ravn.exec(""), RavnResult::Error(_)));
    assert!(matches!(ravn.exec("type:1 | follow"), RavnResult::Error(_)));
    assert!(matches!(ravn.exec("type:1 | select"), RavnResult::Error(_)));
    assert!(matches!(ravn.exec("type:1 | get"), RavnResult::Error(_)));
    assert!(matches!(ravn.exec("type:1 | unknown_cmd"), RavnResult::Error(_)));
}

// ════════════════ empty result ════════════════

#[test]
fn exec_empty_result() {
    let path = db_path("exec_empty");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("type", ValueType::Number, 10);
    let e = eng.entity();
    eng.tie(e, "type", 1);
    eng.rebuild();
    let ravn = Ravn::new(Arc::new(eng));

    match ravn.exec("type:9 | count") {
        RavnResult::Count(0) => {}
        other => panic!("expected Count(0), got {other:?}"),
    }

    match ravn.exec("type:9") {
        RavnResult::Entities(v) => assert!(v.is_empty()),
        other => panic!("expected empty Entities, got {other:?}"),
    }
}
