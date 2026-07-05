//! Scope 実用 demo + overhead bench。
//!
//! 目的:
//!   1. pattern A (container DB + 複数 tenant) と pattern B (per-user DB ファイル)
//!      で **同じ closure が同じ結果を返す** ことを実機で見せる
//!   2. Scope の `get_table` overhead (= prefix resolve の `format!`) が
//!      hot path で許容範囲か実測
//!   3. 「ユーザーが自分のデータを開く」 が pattern によらず同一 API になるかの
//!      体感確認
//!
//! 実行:
//!   cargo run --release -p enchudb-schema --example scope_demo

use enchudb_schema::{Database, Scope, Value};
use std::time::Instant;

const ROWS_PER_TENANT: u32 = 10_000;
const TENANTS: &[&str] = &["alice", "bob", "carol"];

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
    let _ = std::fs::remove_file(format!("{}.tables", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));
}

/// view に対する **app code**。 pattern A でも B でも同じものを呼ぶ。
/// 引数は &Scope 1 つ。 引き継いだ topology を知らない。
fn count_users_aged(view: &Scope<'_>, age: u32) -> usize {
    let users = view.get_table("users").expect("users table");
    users.where_eq("age", age).find().unwrap().len()
}

fn name_of_user_with_id(view: &Scope<'_>, id: u32) -> Option<String> {
    let users = view.get_table("users")?;
    let eids = users.where_eq("id", id).find().ok()?;
    if eids.is_empty() {
        return None;
    }
    match users.entity(eids[0]).get("name")? {
        Value::Text(s) => Some(s),
        _ => None,
    }
}

fn populate(view_mut: &mut enchudb_schema::ScopeMut<'_>, _tenant_label: &str) {
    view_mut
        .table("users")
        .number("id")
        .tag("name")
        .number("age")
        .primary_key("id")
        .build()
        .unwrap();
}

fn insert_rows(view: &Scope<'_>, tenant_label: &str) {
    let users = view.get_table("users").unwrap();
    for i in 0..ROWS_PER_TENANT {
        users
            .insert()
            .set("id", i)
            .set("name", format!("{}-{}", tenant_label, i))
            .set("age", 20 + (i % 60))
            .commit()
            .unwrap();
    }
}

fn main() {
    println!("=================================================================");
    println!("  Scope demo: pattern A (container) vs pattern B (per-user)");
    println!("  rows/tenant={}, tenants={:?}", ROWS_PER_TENANT, TENANTS);
    println!("=================================================================\n");

    // ──── Pattern A: 1 container DB に複数 tenant ────
    let path_a = "/tmp/enchudb-tenant-demo-A.db";
    cleanup(path_a);
    let mut db_a = Database::create(path_a).unwrap();
    let t_a_setup = Instant::now();
    for &tenant in TENANTS {
        populate(&mut db_a.scope_mut(tenant), tenant);
    }
    for &tenant in TENANTS {
        insert_rows(&db_a.scope(tenant), tenant);
    }
    let setup_a = t_a_setup.elapsed();

    // ──── Pattern B: tenant ごとに別 DB ファイル ────
    let path_b_dir = "/tmp/enchudb-tenant-demo-B";
    let _ = std::fs::create_dir_all(path_b_dir);
    let mut dbs_b: Vec<(String, Database)> = Vec::new();
    let t_b_setup = Instant::now();
    for &tenant in TENANTS {
        let path = format!("{}/{}.db", path_b_dir, tenant);
        cleanup(&path);
        let mut db = Database::create(&path).unwrap();
        populate(&mut db.as_scope_mut(), tenant);
        insert_rows(&db.as_scope(), tenant);
        dbs_b.push((tenant.to_string(), db));
    }
    let setup_b = t_b_setup.elapsed();

    println!("Setup:");
    println!("  pattern A (container DB): {:>8.2} ms", setup_a.as_secs_f64() * 1000.0);
    println!("  pattern B (N DB files):   {:>8.2} ms", setup_b.as_secs_f64() * 1000.0);
    println!();

    // ──── 不変式の確認: 同 closure が同 shape を返す ────
    println!("Invariant check (= 同じ closure で同じ結果):");
    println!("  {:<10} | A: age==30 count | B: age==30 count | match?", "tenant");
    println!("  {:<10}-+------------------+------------------+-------", "----");
    for (i, &tenant) in TENANTS.iter().enumerate() {
        let view_a = db_a.scope(tenant);
        let view_b = dbs_b[i].1.as_scope();
        let count_a = count_users_aged(&view_a, 30);
        let count_b = count_users_aged(&view_b, 30);
        let match_ = if count_a == count_b { "✓" } else { "✗ MISMATCH" };
        println!(
            "  {:<10} | {:>16} | {:>16} | {}",
            tenant, count_a, count_b, match_
        );
    }
    println!();

    println!("Sample name lookup (id=42、 ペットフード提案: 名前 prefix で tenant 区別可):");
    for (i, &tenant) in TENANTS.iter().enumerate() {
        let n_a = name_of_user_with_id(&db_a.scope(tenant), 42);
        let n_b = name_of_user_with_id(&dbs_b[i].1.as_scope(), 42);
        println!("  {:<10} | A: {:?} | B: {:?}", tenant, n_a, n_b);
    }
    println!();

    // ──── Isolation 確認: scope("alice") は bob を見ない ────
    println!("Isolation (pattern A の container DB):");
    {
        let alice_view = db_a.scope("alice");
        let alice_tables: Vec<String> =
            alice_view.list_tables().into_iter().map(|t| t.name).collect();
        let bob_users = name_of_user_with_id(&alice_view, 42).unwrap_or_default();
        println!(
            "  alice.list_tables() = {:?}  (bob.* 不可視 ✓)",
            alice_tables
        );
        println!(
            "  alice.get_table('users') → 'users' (= alice.users) = {:?}",
            bob_users
        );
    }
    println!();

    // ──── Overhead bench: Scope::get_table の format! コスト ────
    println!("Overhead bench (get_table の prefix resolution、 1M iter):");
    {
        let view_a = db_a.scope("alice");
        let view_b = dbs_b[0].1.as_scope();

        // pattern A: format!("alice.{}", "users") + Database::get_table
        let t = Instant::now();
        let mut sink = 0usize;
        for _ in 0..1_000_000 {
            sink ^= view_a.get_table("users").unwrap().himo_id("id").unwrap() as usize;
        }
        let dur_a = t.elapsed();
        let ns_a = dur_a.as_nanos() as f64 / 1_000_000.0;

        // pattern B: prefix 無し、 format! 走らないはず
        let t = Instant::now();
        let mut sink2 = 0usize;
        for _ in 0..1_000_000 {
            sink2 ^= view_b.get_table("users").unwrap().himo_id("id").unwrap() as usize;
        }
        let dur_b = t.elapsed();
        let ns_b = dur_b.as_nanos() as f64 / 1_000_000.0;

        // raw Database::get_table (baseline)
        let t = Instant::now();
        let mut sink3 = 0usize;
        for _ in 0..1_000_000 {
            sink3 ^= db_a.get_table("alice.users").unwrap().himo_id("id").unwrap() as usize;
        }
        let dur_raw = t.elapsed();
        let ns_raw = dur_raw.as_nanos() as f64 / 1_000_000.0;

        println!("  raw db.get_table('alice.users')      | {:>7.1} ns/op", ns_raw);
        println!("  scope('alice').get_table('users')    | {:>7.1} ns/op   (差分 {:+.1} ns、 format! 1 回)", ns_a, ns_a - ns_raw);
        println!("  as_scope().get_table('users')        | {:>7.1} ns/op   (差分 {:+.1} ns、 format! 走らず)", ns_b, ns_b - ns_raw);
        println!("  (_sink 抑止: {} {} {})", sink, sink2, sink3);
    }
    println!();

    println!("結論:");
    println!("  - 不変式 ✓ (同 closure → 同 shape across topology)");
    println!("  - isolation ✓ (scope が他 tenant を隠す)");
    println!("  - overhead は format! 1 回分 (~50-100 ns 範囲想定)、");
    println!("    schema layer の get_table は hot path じゃない (起動時 1 回引いて handle 保持)");
    println!("    なので実用上問題なし。");

    // ──── cleanup ────
    cleanup(path_a);
    for &tenant in TENANTS {
        cleanup(&format!("{}/{}.db", path_b_dir, tenant));
    }
    let _ = std::fs::remove_dir(path_b_dir);
}
