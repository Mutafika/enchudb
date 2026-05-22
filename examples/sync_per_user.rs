//! 0.7.0 pattern B: per-user DB ファイル (= typical SaaS: memo / todo /
//! password manager)。 user ごとに 1 DB ファイル、 edge と server が
//! **ファイル単位で mirror** する。
//!
//! このパターンでは sync 経路に `_sync_ops` table を使わない (= edge と server
//! の同期は `.ecdb` 全体を file copy するだけで完結)。 OS file boundary が
//! tenant boundary なので op-level の publish は不要。 したがって
//! `enable_sync()` は呼ばず、 schema build → schema mode のまま運用 = OK。
//!
//! 1 process で alice / bob 2 人分の DB を作って、 各々独立に schema を持ち、
//! cross-user 漏れが構造的に不可能なことを示す。
//!
//! 実運用では:
//! - edge (user device): `~/Library/Application Support/myapp/<user>.ecdb`
//! - server (cloud): `/var/lib/myapp/users/<user>.ecdb`
//! - sync: HttpTransport で `.ecdb` を bytewise mirror (= rsync 風)
//! - 削除: `rm <user>.ecdb` で完結 (= GDPR 対応も easy)
//!
//! 起動: `cargo run --example sync_per_user`

use enchudb::schema::Database;

fn open_or_create_user_db(user: &str) -> Result<Database, Box<dyn std::error::Error>> {
    let path = format!("/tmp/enchudb-pattern-b-{}-{}.ecdb", user, std::process::id());
    let _ = std::fs::remove_file(&path);
    for suf in &[".oplog", ".tables", ".crc", ".db.lock"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suf));
    }

    let mut db = Database::create(&path)?;
    let _ = db.table("memos")
        .number("id")
        .tag("title")
        .leaf("body")
        .number("updated_s")
        .primary_key("id")
        .build()?;
    // ← pattern B では enable_sync() は呼ばない (= file mirror で済むため)
    Ok(db)
}

fn cleanup(user: &str) {
    let path = format!("/tmp/enchudb-pattern-b-{}-{}.ecdb", user, std::process::id());
    let _ = std::fs::remove_file(&path);
    for suf in &[".oplog", ".tables", ".crc", ".db.lock"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suf));
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let alice_db = open_or_create_user_db("alice")?;
    let bob_db = open_or_create_user_db("bob")?;

    // 各 user が独立に memo を書く (= cross-user 漏れは構造的に不可)
    {
        let memos = alice_db.get_table("memos").unwrap();
        memos.insert()
            .set("id", 1i64)
            .set("title", "morning routine")
            .set("body", "wake up, coffee, code")
            .set("updated_s", 1716100000i64)
            .commit()?;
        memos.insert()
            .set("id", 2i64)
            .set("title", "groceries")
            .set("body", "eggs, milk, bread")
            .set("updated_s", 1716100100i64)
            .commit()?;
    }
    {
        let memos = bob_db.get_table("memos").unwrap();
        memos.insert()
            .set("id", 1i64)
            .set("title", "weekend plan")
            .set("body", "hiking with friends")
            .set("updated_s", 1716100200i64)
            .commit()?;
    }

    // 各 DB 内で query (= 完全 isolated)
    let alice_memos = alice_db.get_table("memos").unwrap();
    let alice_count = alice_memos.all().count()?;
    println!("alice has {alice_count} memos");

    let bob_memos = bob_db.get_table("memos").unwrap();
    let bob_count = bob_memos.all().count()?;
    println!("bob has {bob_count} memos");

    // sync は file copy で済む。 server 側で:
    //   cp /tmp/enchudb-pattern-b-alice-*.ecdb /var/lib/myapp/users/alice.ecdb
    //   cp /tmp/enchudb-pattern-b-alice-*.ecdb.oplog /var/lib/myapp/users/alice.ecdb.oplog
    //   cp /tmp/enchudb-pattern-b-alice-*.ecdb.tables /var/lib/myapp/users/alice.ecdb.tables
    // と sidecar 込みで mirror すれば OK (= snapshot_export も同じ effect)。
    println!("\nsync policy: per-user file mirror (no op-level publish needed)");

    drop(alice_db);
    drop(bob_db);
    cleanup("alice");
    cleanup("bob");
    Ok(())
}
