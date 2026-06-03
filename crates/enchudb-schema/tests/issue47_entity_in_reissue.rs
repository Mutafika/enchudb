//! issue #47 repro: `entity_in` が live local を re-issue して既存 entity を上書きする
//!
//! 観測: bisquit 0.8.11 で、 22 articles seed 済の DB に対し新 URL を save すると
//! `entity_in("articles")` が **existing live local 20, 21, 22 を返す** → tie_value で
//! seed 過去 row が新 URL に上書きされる silent data loss。
//!
//! ここでは minimal な再現を試みる:
//! - 1 table `articles` (url=Tag PK, title=Tag)
//! - 22 row seed
//! - 3 row 追加 → eid が全部 fresh、 `all().find().len() == 25` を期待
//!
//! reopen を挟む / sync を絡める変種も追加。

use enchudb_schema::{Database, ColumnType};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue47-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
    let _ = std::fs::remove_file(format!("{}.tables", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.db.lock", path));
}

#[test]
fn minimal_no_reopen() {
    let path = tmp_path("minimal");
    cleanup(&path);

    {
        let mut db = Database::create(&path).unwrap();
        db.table("articles")
            .column("url", ColumnType::Tag)
            .column("title", ColumnType::Tag)
            .primary_key("url")
            .build()
            .unwrap();
        let db = db; // immutable borrow for table use

        let articles = db.get_table("articles").unwrap();

        let mut seed_eids = Vec::new();
        for i in 0..22 {
            let eid = articles
                .insert()
                .set("url", format!("https://seed.example/{i}").as_str())
                .set("title", format!("seed {i}").as_str())
                .commit()
                .unwrap();
            seed_eids.push(eid);
        }

        let mut new_eids = Vec::new();
        for u in &["https://new.example/a", "https://new.example/b", "https://new.example/c"] {
            let eid = articles
                .insert()
                .set("url", *u)
                .set("title", "new")
                .commit()
                .unwrap();
            new_eids.push(eid);
        }

        for ne in &new_eids {
            assert!(!seed_eids.contains(ne), "new eid {ne} collides with seed");
        }

        let all = articles.all().find().unwrap();
        assert_eq!(all.len(), 25, "expected 25 articles, got {}", all.len());
    }

    cleanup(&path);
}

#[test]
fn with_reopen() {
    let path = tmp_path("reopen");
    cleanup(&path);

    let seed_eids: Vec<u64>;
    {
        let mut db = Database::create(&path).unwrap();
        db.table("articles")
            .column("url", ColumnType::Tag)
            .column("title", ColumnType::Tag)
            .primary_key("url")
            .build()
            .unwrap();
        let db = db;
        let articles = db.get_table("articles").unwrap();

        let mut eids = Vec::new();
        for i in 0..22 {
            let eid = articles
                .insert()
                .set("url", format!("https://seed.example/{i}").as_str())
                .set("title", format!("seed {i}").as_str())
                .commit()
                .unwrap();
            eids.push(eid);
        }
        seed_eids = eids;
    }

    {
        let db = Database::open(&path).unwrap();
        let articles = db.get_table("articles").unwrap();

        let mut new_eids = Vec::new();
        for u in &["https://new.example/a", "https://new.example/b", "https://new.example/c"] {
            let eid = articles
                .insert()
                .set("url", *u)
                .set("title", "new")
                .commit()
                .unwrap();
            new_eids.push(eid);
        }

        for ne in &new_eids {
            assert!(!seed_eids.contains(ne), "after reopen: new eid {ne} collides with seed");
        }

        let all = articles.all().find().unwrap();
        assert_eq!(all.len(), 25, "after reopen: expected 25 articles, got {}", all.len());
    }

    cleanup(&path);
}
