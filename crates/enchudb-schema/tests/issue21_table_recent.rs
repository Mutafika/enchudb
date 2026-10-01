//! #21: `Table::recent(n)` — table の row を新しい順に最大 n 件、 table の大きさによらずに取る。

use enchudb_schema::Database;

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue21-schema-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path); // v10: DB は directory
    for suf in ["", ".tables", ".oplog", ".crc", ".db.lock", ".eidmap", ".vocabmap", ".positions"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

#[test]
fn recent_is_all_find_reversed_and_survives_reopen() {
    let path = tmp("recent");
    cleanup(&path);
    let mut eids = Vec::new();
    {
        let mut db = Database::create(&path).unwrap();
        db.table("likes").number("id").number("post").primary_key("id").build().unwrap();
        db.table("posts").number("id").tag("body").number("ts").primary_key("id").build().unwrap();
        let posts = db.get_table("posts").unwrap();
        let likes = db.get_table("likes").unwrap();
        assert_eq!(posts.recent(10), Vec::<u64>::new(), "空の table");
        for i in 0..200i64 {
            eids.push(posts.insert().set("id", i).set("body", format!("post {i}")).set("ts", 1000 + i).commit().unwrap());
            // 他の table の row が間に入っても混ざらない
            likes.insert().set("id", i).set("post", i).commit().unwrap();
        }
        let want: Vec<u64> = eids.iter().rev().take(80).copied().collect();
        assert_eq!(posts.recent(80), want);
        // 全件の列挙を逆にしたものと一致する
        let mut all = posts.all().find().unwrap();
        all.reverse();
        assert_eq!(posts.recent(1000), all);
        // 並びの列で引いたものとも一致する (ts は insert の順に増やしてある)
        assert_eq!(posts.recent(80), posts.all().order_by_desc("ts").limit(80).find().unwrap());

        // 削除した row は出ない。 その後に足した row が先頭
        posts.entity(eids[199]).delete().unwrap();
        let newer = posts.insert().set("id", 500i64).set("body", "newer").set("ts", 9999i64).commit().unwrap();
        assert_eq!(posts.recent(3), vec![newer, eids[198], eids[197]]);
        eids.push(newer);
    }
    // 開き直した後も同じ
    let db = Database::open(&path).unwrap();
    let posts = db.get_table("posts").unwrap();
    assert_eq!(posts.recent(3), vec![eids[200], eids[198], eids[197]]);
    assert_eq!(posts.recent(0), Vec::<u64>::new());
    drop(db);
    cleanup(&path);
}

/// 代表列は `all()` と同じ (主キー、 無ければ最初の列)。 最初の列に値の無い row も、 主キーがあれば出る。
#[test]
fn recent_uses_the_primary_key_as_the_representative_column() {
    let path = tmp("pk");
    cleanup(&path);
    let mut db = Database::create(&path).unwrap();
    // 主キーは 2 列目。 1 列目 (note) は張らない row がある
    db.table("events").tag("note").number("id").primary_key("id").build().unwrap();
    let events = db.get_table("events").unwrap();
    let a = events.insert().set("id", 1i64).set("note", "with note").commit().unwrap();
    let b = events.insert().set("id", 2i64).commit().unwrap();
    let c = events.insert().set("id", 3i64).commit().unwrap();
    assert_eq!(events.recent(10), vec![c, b, a]);
    let mut all = events.all().find().unwrap();
    all.reverse();
    assert_eq!(events.recent(10), all);
    drop(db);
    cleanup(&path);
}

