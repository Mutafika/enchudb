//! 0.7.0 pattern A: 中央集権 (mail server / SNS / feed) — multi-tenant table
//! cluster を 1 server DB に collapse、 thin client は別途。
//!
//! 1 process で server + client を模擬し、 1 server DB に 2 tenant の table を
//! 持って、 tenant 越えの cross-query が server 側で可能なことを示す。
//!
//! 実運用では:
//! - server: `Engine::create_concurrent_with_oplog` + `HttpRelay::start` で listen
//! - client (web): server に HTTP GET で query、 DB は持たない
//! - tenant isolation は schema 層の `WHERE tenant = ?` で virtual partition
//!
//! 起動: `cargo run --example sync_centralized`
//!
//! このサンプルは perf 計測でも production-ready でもない。 単に「Engine API
//! が中央集権 pattern にどう写像されるか」 を示す reference。

use enchudb::schema::{Database, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = format!("/tmp/enchudb-pattern-a-{}.ecdb", std::process::id());
    let _ = std::fs::remove_file(&path);
    for suf in &[".oplog", ".tables", ".crc", ".db.lock"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suf));
    }

    // server DB を作る (= 全 tenant の table を 1 DB に持つ)。 build phase で
    // schema + sync を整え、 finish_with_oplog で concurrent + WAL モードに遷移。
    let arc_db = {
        let mut db = Database::create(&path)?;
        let _ = db.table("t_alice_posts")
            .number("id")
            .tag("title")
            .number("created_s")
            .primary_key("id")
            .build()?;
        let _ = db.table("t_bob_posts")
            .number("id")
            .tag("title")
            .number("created_s")
            .primary_key("id")
            .build()?;
        db.enable_sync()?;
        db.finish_with_oplog(16 * 1024 * 1024)?
    };

    // tenant ごとに row を insert (= concurrent mode で oplog に流れる)
    let alice_p1 = arc_db.get_table("t_alice_posts").unwrap()
        .insert()
        .set("id", 1i64)
        .set("title", "Hello from Alice")
        .set("created_s", 1716000000i64)
        .commit()?;
    let bob_p1 = arc_db.get_table("t_bob_posts").unwrap()
        .insert()
        .set("id", 1i64)
        .set("title", "Hello from Bob")
        .set("created_s", 1716000005i64)
        .commit()?;

    println!("inserted alice.posts[1]={alice_p1}, bob.posts[1]={bob_p1}");

    // tenant ごと query (= 同 prefix の table を取り出す)
    let alice_posts = arc_db.get_table("t_alice_posts").unwrap();
    let alice_found = alice_posts.where_eq("title", "Hello from Alice").find()?;
    println!("alice posts matching: {} hit(s)", alice_found.len());

    // tenant 越え cross-query は schema 層では直接できないが、 application で
    // 「全 tenant table を走査」 すれば擬似的に実現:
    let mut all_titles = Vec::new();
    for table_name in &["t_alice_posts", "t_bob_posts"] {
        let t = arc_db.get_table(table_name).unwrap();
        for &eid in &t.all().find()? {
            if let Some(Value::Text(s)) = t.entity(eid).get("title") {
                all_titles.push(format!("{table_name}: {s}"));
            }
        }
    }
    println!("cross-tenant feed:");
    for line in &all_titles {
        println!("  {line}");
    }

    // sync table の状況。 transfer_oplog_to_sync_ops は consumer thread 経由が
    // 本来の path だが、 ここでは明示的に 1 度呼んで lsn を進める。
    arc_db.engine().oplog_commit();
    arc_db.engine().flush_writes();
    arc_db.engine().oplog_sync()?;
    let transferred = arc_db.engine().transfer_oplog_to_sync_ops();
    println!("\nsync state:");
    println!("  sync_enabled: {}", arc_db.sync_enabled());
    println!("  transferred: {transferred} record(s)");
    println!("  current_sync_lsn: {}", arc_db.engine().current_sync_lsn());

    let _ = std::fs::remove_file(&path);
    for suf in &[".oplog", ".tables", ".crc", ".db.lock"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suf));
    }
    Ok(())
}
