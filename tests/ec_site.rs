//! EnchuDB v27 EC サイト実用テスト。
//!
//! 実行: `cargo test --features v27 --test ec_site -- --nocapture`

#![cfg(feature = "v27")]

use enchudb::{Engine, HimoType};

// ════════════════ ヘルパー ════════════════

fn db_path(tag: &str) -> String {
    let path = format!("/tmp/enchudb_ec_{}.db", tag);
    let _ = std::fs::remove_file(&path);
    path
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
}

/// xorshift32 決定的乱数
struct Xorshift32(u32);

impl Xorshift32 {
    fn new(seed: u32) -> Self { Self(seed) }
    fn next(&mut self) -> u32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 17;
        self.0 ^= self.0 << 5;
        self.0
    }
    fn next_range(&mut self, max: u32) -> u32 {
        self.next() % max
    }
}

// ════════════════ データ生成 ════════════════

const N_PRODUCTS: u32 = 1000;
const N_USERS: u32 = 500;
const N_ORDERS: u32 = 5000;
const N_ORDER_ITEMS: u32 = 15000;
const N_REVIEWS: u32 = 3000;

// Entity type 定数
const TYPE_PRODUCT: u32 = 1;
const TYPE_USER: u32 = 2;
const TYPE_ORDER: u32 = 3;
const TYPE_ORDER_ITEM: u32 = 4;
const TYPE_REVIEW: u32 = 5;

struct EcData {
    products: Vec<u64>,
    users: Vec<u64>,
    orders: Vec<u64>,
    order_items: Vec<u64>,
    reviews: Vec<u64>,
}

fn define_schema(db: &mut Engine) {
    db.define_himo("type", HimoType::Number, 8);
    db.define_himo("category", HimoType::Number, 20);
    db.define_himo("price_band", HimoType::Number, 10);
    db.define_himo("color", HimoType::Number, 12);
    db.define_himo("brand", HimoType::Number, 50);
    db.define_himo("region", HimoType::Number, 10);
    db.define_himo("membership", HimoType::Number, 4);
    db.define_himo("year", HimoType::Number, 5);
    db.define_himo("month", HimoType::Number, 13);
    db.define_himo("order_status", HimoType::Number, 5);
    db.define_himo("rating", HimoType::Number, 6);
    db.define_himo("quantity", HimoType::Number, 100);
    // 参照紐 (max_values=0)
    db.define_himo("user_ref", HimoType::Ref, 0);
    db.define_himo("order_ref", HimoType::Ref, 0);
    db.define_himo("product_ref", HimoType::Ref, 0);
}

fn define_views(_db: &mut Engine) {
    // issue #4 で `Engine::define_view` 削除済み。 NTupleTable は外部 caller
    // ゼロの dead weight だった。 helper は他テストから呼ばれてるので shape
    // だけ残す (内部 no-op)、 query 経路は column_filter で十分速い。
}

fn populate(db: &mut Engine) -> EcData {
    let mut rng = Xorshift32::new(42);

    // Products
    let mut products = Vec::with_capacity(N_PRODUCTS as usize);
    for _ in 0..N_PRODUCTS {
        let e = db.entity();
        db.tie(e, "type", TYPE_PRODUCT);
        db.tie(e, "category", rng.next_range(20));
        db.tie(e, "price_band", rng.next_range(10));
        db.tie(e, "color", rng.next_range(12));
        db.tie(e, "brand", rng.next_range(50));
        products.push(e);
    }

    // Users
    let mut users = Vec::with_capacity(N_USERS as usize);
    for _ in 0..N_USERS {
        let e = db.entity();
        db.tie(e, "type", TYPE_USER);
        db.tie(e, "region", rng.next_range(10));
        db.tie(e, "membership", rng.next_range(4));
        users.push(e);
    }

    // Orders
    let mut orders = Vec::with_capacity(N_ORDERS as usize);
    for _ in 0..N_ORDERS {
        let e = db.entity();
        db.tie(e, "type", TYPE_ORDER);
        db.tie_ref(e, "user_ref", users[rng.next_range(N_USERS) as usize]);
        db.tie(e, "year", rng.next_range(5));       // 0=2022, 1=2023, ..., 4=2026
        db.tie(e, "month", rng.next_range(12) + 1); // 1..12
        db.tie(e, "order_status", rng.next_range(5));
        orders.push(e);
    }

    // OrderItems
    let mut order_items = Vec::with_capacity(N_ORDER_ITEMS as usize);
    for _ in 0..N_ORDER_ITEMS {
        let e = db.entity();
        db.tie(e, "type", TYPE_ORDER_ITEM);
        db.tie_ref(e, "order_ref", orders[rng.next_range(N_ORDERS) as usize]);
        db.tie_ref(e, "product_ref", products[rng.next_range(N_PRODUCTS) as usize]);
        db.tie(e, "quantity", rng.next_range(10) + 1); // 1..10
        order_items.push(e);
    }

    // Reviews
    let mut reviews = Vec::with_capacity(N_REVIEWS as usize);
    for _ in 0..N_REVIEWS {
        let e = db.entity();
        db.tie(e, "type", TYPE_REVIEW);
        db.tie_ref(e, "product_ref", products[rng.next_range(N_PRODUCTS) as usize]);
        db.tie_ref(e, "user_ref", users[rng.next_range(N_USERS) as usize]);
        db.tie(e, "rating", rng.next_range(6)); // 0..5
        reviews.push(e);
    }

    EcData { products, users, orders, order_items, reviews }
}

/// 全紐の cardinality を表示
fn print_cardinalities(db: &Engine) {
    let himo_names = [
        "type", "category", "price_band", "color", "brand",
        "region", "membership", "year", "month", "order_status",
        "rating", "quantity", "user_ref", "order_ref", "product_ref",
    ];
    println!("  === himo_cardinality ===");
    for name in &himo_names {
        if let Some(c) = db.himo_cardinality(name) {
            println!("    {:<16} {}", name, c);
        }
    }
}

/// テスト用 DB を構築して返す。パスと EcData も返す。
fn setup(tag: &str) -> (String, Engine, EcData) {
    let path = db_path(tag);
    let mut db = Engine::create_with_capacity(&path, 100_000).unwrap();
    define_schema(&mut db);
    define_views(&mut db);
    let data = populate(&mut db);
    db.rebuild();
    (path, db, data)
}

// ════════════════ テスト ════════════════

// ──── 1. category_search ────

#[test]
fn category_search() {
    let (path, db, _data) = setup("cat_search");

    // カテゴリ=1(家電) の全商品
    let target_cat: u32 = 1;
    let t = std::time::Instant::now();
    let result = db.query(&[("type", TYPE_PRODUCT), ("category", target_cat)]);
    let elapsed = t.elapsed();

    // 期待値を Column 直読みで算出
    let mut expected = 0u32;
    for eid in 0..db.next_eid() {
        if db.get(eid, "type") == Some(TYPE_PRODUCT)
            && db.get(eid, "category") == Some(target_cat as u32)
        {
            expected += 1;
        }
    }

    println!("  category_search(cat={}): {:?}, {} 件 (expected {})", target_cat, elapsed, result.len(), expected);
    assert_eq!(result.len() as u32, expected, "category search count mismatch");
    assert!(expected > 0, "should have some products in category {}", target_cat);

    cleanup(&path);
}

// ──── 2. category_price_search (3紐 view hit) ────

#[test]
fn category_price_search() {
    let (path, db, _data) = setup("cat_price");

    let target_cat: u32 = 3;
    let target_pb: u32 = 5;
    let t = std::time::Instant::now();
    let result = db.query(&[("type", TYPE_PRODUCT), ("category", target_cat), ("price_band", target_pb)]);
    let elapsed = t.elapsed();

    let mut expected = 0u32;
    for eid in 0..db.next_eid() {
        if db.get(eid, "type") == Some(TYPE_PRODUCT)
            && db.get(eid, "category") == Some(target_cat as u32)
            && db.get(eid, "price_band") == Some(target_pb as u32)
        {
            expected += 1;
        }
    }

    println!("  category_price_search(cat={}, pb={}): {:?}, {} 件", target_cat, target_pb, elapsed, result.len());
    assert_eq!(result.len() as u32, expected, "category+price_band count mismatch");

    cleanup(&path);
}

// ──── 3. color_filter ────

#[test]
fn color_filter() {
    let (path, db, _data) = setup("color_filter");

    let target_cat: u32 = 5;
    let target_color: u32 = 7;
    let t = std::time::Instant::now();
    let result = db.query(&[("type", TYPE_PRODUCT), ("category", target_cat), ("color", target_color)]);
    let elapsed = t.elapsed();

    let mut expected = 0u32;
    for eid in 0..db.next_eid() {
        if db.get(eid, "type") == Some(TYPE_PRODUCT)
            && db.get(eid, "category") == Some(target_cat as u32)
            && db.get(eid, "color") == Some(target_color as u32)
        {
            expected += 1;
        }
    }

    println!("  color_filter(cat={}, color={}): {:?}, {} 件", target_cat, target_color, elapsed, result.len());
    assert_eq!(result.len() as u32, expected, "color filter count mismatch");

    cleanup(&path);
}

// ──── 4. user_orders (参照紐の逆引き) ────

#[test]
fn user_orders() {
    let (path, db, data) = setup("user_orders");

    let target_user = data.users[42]; // 43番目のユーザー
    let t = std::time::Instant::now();
    let result = db.query(&[("type", TYPE_ORDER), ("user_ref", target_user as u32)]);
    let elapsed = t.elapsed();

    // 全注文を走査して user_ref が target_user を指すものを数える
    let mut expected = 0u32;
    for &oid in &data.orders {
        if db.get(oid, "user_ref") == Some(target_user as u32) {
            expected += 1;
        }
    }

    println!("  user_orders(user={}): {:?}, {} 件", target_user, elapsed, result.len());
    assert_eq!(result.len() as u32, expected, "user_orders count mismatch");

    cleanup(&path);
}

// ──── 5. order_items (参照紐連鎖) ────

#[test]
fn order_items() {
    let (path, db, data) = setup("order_items");

    let target_order = data.orders[100];
    let t = std::time::Instant::now();
    let result = db.query(&[("type", TYPE_ORDER_ITEM), ("order_ref", target_order as u32)]);
    let elapsed = t.elapsed();

    let mut expected = 0u32;
    for &item in &data.order_items {
        if db.get(item, "order_ref") == Some(target_order as u32) {
            expected += 1;
        }
    }

    println!("  order_items(order={}): {:?}, {} 件", target_order, elapsed, result.len());
    assert_eq!(result.len() as u32, expected, "order_items count mismatch");

    cleanup(&path);
}

// ──── 6. user_order_products (3段パス辿り) ────

#[test]
fn user_order_products() {
    let (path, db, data) = setup("user_order_products");

    let target_user = data.users[10];

    let t = std::time::Instant::now();

    // Step 1: ユーザー X の全注文
    let user_orders = db.query(&[("type", TYPE_ORDER), ("user_ref", target_user as u32)]);

    // Step 2: 各注文の全明細
    let mut item_eids: Vec<u64> = Vec::new();
    for &order_eid in &user_orders {
        let items = db.query(&[("type", TYPE_ORDER_ITEM), ("order_ref", order_eid as u32)]);
        item_eids.extend_from_slice(&items);
    }

    // Step 3: 各明細の商品を取得
    let mut product_set = std::collections::HashSet::new();
    for &item_eid in &item_eids {
        if let Some(pid) = db.get(item_eid, "product_ref") {
            product_set.insert(pid);
        }
    }

    let elapsed = t.elapsed();

    // 期待値を直接算出
    let mut expected_products = std::collections::HashSet::new();
    for &oid in &data.orders {
        if db.get(oid, "user_ref") == Some(target_user as u32) {
            for &item in &data.order_items {
                if db.get(item, "order_ref") == Some(oid as u32) {
                    if let Some(pid) = db.get(item, "product_ref") {
                        expected_products.insert(pid);
                    }
                }
            }
        }
    }

    println!("  user_order_products(user={}): {:?}, {} 注文 → {} 明細 → {} 商品",
        target_user, elapsed, user_orders.len(), item_eids.len(), product_set.len());
    assert_eq!(product_set, expected_products, "user_order_products mismatch");

    cleanup(&path);
}

// ──── 7. product_reviews ────

#[test]
fn product_reviews() {
    let (path, db, data) = setup("product_reviews");

    let target_product = data.products[50];
    let t = std::time::Instant::now();
    let result = db.query(&[("type", TYPE_REVIEW), ("product_ref", target_product as u32)]);
    let elapsed = t.elapsed();

    let mut expected = 0u32;
    for &rev in &data.reviews {
        if db.get(rev, "product_ref") == Some(target_product as u32) {
            expected += 1;
        }
    }

    println!("  product_reviews(product={}): {:?}, {} 件", target_product, elapsed, result.len());
    assert_eq!(result.len() as u32, expected, "product_reviews count mismatch");

    cleanup(&path);
}

// ──── 8. high_rating_products (逆引き連鎖) ────

#[test]
fn high_rating_products() {
    let (path, db, data) = setup("high_rating");

    let t = std::time::Instant::now();

    // rating=5 の全レビュー
    let top_reviews = db.query(&[("type", TYPE_REVIEW), ("rating", 5)]);

    // そのレビューが指す商品を集める
    let mut product_set = std::collections::HashSet::new();
    for &rev_eid in &top_reviews {
        if let Some(pid) = db.get(rev_eid, "product_ref") {
            product_set.insert(pid);
        }
    }

    let elapsed = t.elapsed();

    // 期待値
    let mut expected_products = std::collections::HashSet::new();
    for &rev in &data.reviews {
        if db.get(rev, "rating") == Some(5) {
            if let Some(pid) = db.get(rev, "product_ref") {
                expected_products.insert(pid);
            }
        }
    }

    println!("  high_rating_products: {:?}, {} レビュー → {} 商品",
        elapsed, top_reviews.len(), product_set.len());
    assert_eq!(product_set, expected_products, "high_rating_products mismatch");

    cleanup(&path);
}

// ──── 9. orders_by_status ────

#[test]
fn orders_by_status() {
    let (path, db, data) = setup("orders_status");

    let status_names = ["pending", "paid", "shipped", "delivered", "cancelled"];
    let mut total = 0usize;

    println!("  orders_by_status:");
    for status in 0..5u32 {
        let t = std::time::Instant::now();
        let result = db.query(&[("type", TYPE_ORDER), ("order_status", status)]);
        let elapsed = t.elapsed();

        let mut expected = 0u32;
        for &oid in &data.orders {
            if db.get(oid, "order_status") == Some(status) {
                expected += 1;
            }
        }

        println!("    {:<12}: {:?}, {} 件", status_names[status as usize], elapsed, result.len());
        assert_eq!(result.len() as u32, expected, "status {} count mismatch", status);
        total += result.len();
    }

    assert_eq!(total, N_ORDERS as usize, "total orders across all statuses must equal N_ORDERS");

    cleanup(&path);
}

// ──── 10. monthly_orders ────

#[test]
fn monthly_orders() {
    let (path, db, _data) = setup("monthly_orders");

    // year=4 (=2026), month=4
    let target_year: u32 = 4;
    let target_month: u32 = 4;
    let t = std::time::Instant::now();
    let result = db.query(&[("type", TYPE_ORDER), ("year", target_year), ("month", target_month)]);
    let elapsed = t.elapsed();

    let mut expected = 0u32;
    for eid in 0..db.next_eid() {
        if db.get(eid, "type") == Some(TYPE_ORDER)
            && db.get(eid, "year") == Some(target_year as u32)
            && db.get(eid, "month") == Some(target_month as u32)
        {
            expected += 1;
        }
    }

    println!("  monthly_orders(year={}, month={}): {:?}, {} 件", target_year, target_month, elapsed, result.len());
    assert_eq!(result.len() as u32, expected, "monthly_orders count mismatch");

    cleanup(&path);
}

// ──── 11. ec_persistence ────

#[test]
fn ec_persistence() {
    let path = db_path("persistence");
    let query_results;

    {
        let mut db = Engine::create_with_capacity(&path, 100_000).unwrap();
        define_schema(&mut db);
        define_views(&mut db);
        let _data = populate(&mut db);
        db.rebuild();

        // 5つのクエリ結果を保存
        query_results = vec![
            db.query(&[("type", TYPE_PRODUCT), ("category", 1)]),
            db.query(&[("type", TYPE_ORDER), ("order_status", 2)]),
            db.query(&[("type", TYPE_REVIEW), ("rating", 5)]),
            db.query(&[("type", TYPE_PRODUCT), ("category", 3), ("price_band", 5)]),
            db.query(&[("type", TYPE_ORDER), ("year", 4), ("month", 4)]),
        ];

        db.flush().unwrap();
        // db drops here
    }

    // reopen
    let db = Engine::open_standalone(&path).unwrap();

    let reopened_results = vec![
        db.query(&[("type", TYPE_PRODUCT), ("category", 1)]),
        db.query(&[("type", TYPE_ORDER), ("order_status", 2)]),
        db.query(&[("type", TYPE_REVIEW), ("rating", 5)]),
        db.query(&[("type", TYPE_PRODUCT), ("category", 3), ("price_band", 5)]),
        db.query(&[("type", TYPE_ORDER), ("year", 4), ("month", 4)]),
    ];

    let labels = [
        "product(cat=1)", "order(status=2)", "review(rating=5)",
        "product(cat=3,pb=5)", "order(y=4,m=4)",
    ];

    println!("  ec_persistence:");
    for i in 0..query_results.len() {
        let mut a = query_results[i].clone();
        let mut b = reopened_results[i].clone();
        a.sort();
        b.sort();
        println!("    {}: before={}, after={}", labels[i], a.len(), b.len());
        assert_eq!(a, b, "persistence mismatch for {}", labels[i]);
    }

    cleanup(&path);
}

// ──── 12. view_persistence ────

#[test]
fn view_persistence() {
    let path = db_path("view_persist");

    // flush + reopen
    {
        let mut db = Engine::create_with_capacity(&path, 100_000).unwrap();
        define_schema(&mut db);
        define_views(&mut db);
        let _data = populate(&mut db);
        db.rebuild();
        db.flush().unwrap();
    }

    // open 後、define_view を呼ばずにクエリ
    let db = Engine::open_standalone(&path).unwrap();

    // 3紐 view (type, category, price_band) でヒットするはず
    let t = std::time::Instant::now();
    let result = db.query(&[("type", TYPE_PRODUCT), ("category", 2), ("price_band", 3)]);
    let elapsed = t.elapsed();

    let mut expected = 0u32;
    for eid in 0..db.next_eid() {
        if db.get(eid, "type") == Some(TYPE_PRODUCT)
            && db.get(eid, "category") == Some(2)
            && db.get(eid, "price_band") == Some(3)
        {
            expected += 1;
        }
    }

    println!("  view_persistence: {:?}, {} 件 (expected {})", elapsed, result.len(), expected);
    assert_eq!(result.len() as u32, expected, "view_persistence count mismatch");

    cleanup(&path);
}

// ──── 13. concurrent_order_creation ────

#[test]
fn concurrent_order_creation() {
    let path = db_path("concurrent");
    let mut db = Engine::create_with_capacity(&path, 100_000).unwrap();
    define_schema(&mut db);

    // 商品とユーザーを先に作る
    let mut products = Vec::new();
    let mut users = Vec::new();
    let mut rng = Xorshift32::new(99);
    for _ in 0..100 {
        let e = db.entity();
        db.tie(e, "type", TYPE_PRODUCT);
        db.tie(e, "category", rng.next_range(20));
        db.tie(e, "price_band", rng.next_range(10));
        products.push(e);
    }
    for _ in 0..50 {
        let e = db.entity();
        db.tie(e, "type", TYPE_USER);
        db.tie(e, "region", rng.next_range(10));
        users.push(e);
    }
    db.rebuild();

    let arc = Engine::concurrentize(db);

    let n_threads = 4;
    let n_orders_per_thread = 100;
    let mut handles = Vec::new();

    for thread_id in 0..n_threads {
        let arc = arc.clone();
        let users = users.clone();
        let _products = products.clone();
        handles.push(std::thread::spawn(move || {
            let mut rng = Xorshift32::new(1000 + thread_id);
            let mut created = Vec::new();
            for _ in 0..n_orders_per_thread {
                let e = arc.entity();
                arc.tie_to(e, "type", TYPE_ORDER);
                let user = users[rng.next_range(users.len() as u32) as usize];
                arc.tie_ref_to(e, "user_ref", user);
                arc.tie_to(e, "year", rng.next_range(5));
                arc.tie_to(e, "month", rng.next_range(12) + 1);
                arc.tie_to(e, "order_status", rng.next_range(5));
                created.push(e);
            }
            created
        }));
    }

    let mut all_orders = Vec::new();
    for h in handles {
        all_orders.extend(h.join().unwrap());
    }

    // rebuild して検索
    arc.rebuild();
    let total_orders = arc.query(&[("type", TYPE_ORDER)]);
    let expected = (n_threads * n_orders_per_thread) as usize;

    println!("  concurrent_order_creation: {} threads x {} = {} orders, query found {}",
        n_threads, n_orders_per_thread, expected, total_orders.len());
    assert_eq!(total_orders.len(), expected, "concurrent order count mismatch");

    // 全 order が実在するか
    let order_set: std::collections::HashSet<u64> = total_orders.into_iter().collect();
    for &oid in &all_orders {
        assert!(order_set.contains(&oid), "order {} not found in query results", oid);
    }

    cleanup(&path);
}

// ──── 14. referential_check ────

#[test]
fn referential_check() {
    let (path, db, data) = setup("ref_check");

    let product_set: std::collections::HashSet<u64> = data.products.iter().copied().collect();
    let user_set: std::collections::HashSet<u64> = data.users.iter().copied().collect();
    let order_set: std::collections::HashSet<u64> = data.orders.iter().copied().collect();

    // 全 OrderItem の product_ref / order_ref が実在するか
    let mut bad_product_refs = 0u32;
    let mut bad_order_refs = 0u32;
    for &item in &data.order_items {
        if let Some(pid) = db.get(item, "product_ref") {
            if !product_set.contains(&(pid as u64)) { bad_product_refs += 1; }
        } else {
            bad_product_refs += 1;
        }
        if let Some(oid) = db.get(item, "order_ref") {
            if !order_set.contains(&(oid as u64)) { bad_order_refs += 1; }
        } else {
            bad_order_refs += 1;
        }
    }

    // 全 Review の product_ref / user_ref が実在するか
    let mut bad_review_prefs = 0u32;
    let mut bad_review_urefs = 0u32;
    for &rev in &data.reviews {
        if let Some(pid) = db.get(rev, "product_ref") {
            if !product_set.contains(&(pid as u64)) { bad_review_prefs += 1; }
        } else {
            bad_review_prefs += 1;
        }
        if let Some(uid) = db.get(rev, "user_ref") {
            if !user_set.contains(&(uid as u64)) { bad_review_urefs += 1; }
        } else {
            bad_review_urefs += 1;
        }
    }

    // 全 Order の user_ref が実在するか
    let mut bad_order_urefs = 0u32;
    for &oid in &data.orders {
        if let Some(uid) = db.get(oid, "user_ref") {
            if !user_set.contains(&(uid as u64)) { bad_order_urefs += 1; }
        } else {
            bad_order_urefs += 1;
        }
    }

    println!("  referential_check:");
    println!("    OrderItem→Product bad refs: {}", bad_product_refs);
    println!("    OrderItem→Order bad refs:   {}", bad_order_refs);
    println!("    Review→Product bad refs:    {}", bad_review_prefs);
    println!("    Review→User bad refs:       {}", bad_review_urefs);
    println!("    Order→User bad refs:        {}", bad_order_urefs);

    assert_eq!(bad_product_refs, 0, "OrderItem has dangling product_ref");
    assert_eq!(bad_order_refs, 0, "OrderItem has dangling order_ref");
    assert_eq!(bad_review_prefs, 0, "Review has dangling product_ref");
    assert_eq!(bad_review_urefs, 0, "Review has dangling user_ref");
    assert_eq!(bad_order_urefs, 0, "Order has dangling user_ref");

    cleanup(&path);
}

// ──── 15. count_consistency ────

#[test]
fn count_consistency() {
    let (path, db, data) = setup("count_consistency");

    print_cardinalities(&db);

    let types = [
        ("Product", TYPE_PRODUCT, data.products.len()),
        ("User", TYPE_USER, data.users.len()),
        ("Order", TYPE_ORDER, data.orders.len()),
        ("OrderItem", TYPE_ORDER_ITEM, data.order_items.len()),
        ("Review", TYPE_REVIEW, data.reviews.len()),
    ];

    println!("  count_consistency:");
    for (name, type_val, expected) in &types {
        let result = db.query(&[("type", *type_val)]);
        println!("    {:<12}: query={}, expected={}", name, result.len(), expected);
        assert_eq!(result.len(), *expected, "{} count mismatch", name);
    }

    let total = N_PRODUCTS + N_USERS + N_ORDERS + N_ORDER_ITEMS + N_REVIEWS;
    assert_eq!(db.entity_count(), total, "total entity count mismatch");

    cleanup(&path);
}
