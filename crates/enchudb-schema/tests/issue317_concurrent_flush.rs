//! #317: 「まだ flush していない分」 の合計は、 consumer の定期書き出しと `oplog_sync()` の flush が同じ segment で
//! 同時に走っても引きすぎない (引きすぎると合計が 0 を割って巨大な値になり、 空きがあっても全部の書き込みを断る。
//! 修正前は 100 行ごとの oplog_sync で 10 万行書くと 6 回中 2 回 `WriteRejected(DiskSpace)` になった)。
//! プロセス全体の合計を見るので 1 file 1 テスト。

use enchudb_schema::Database;

/// panic しても DB を消す (変異試験で落とすたびに /tmp に数百 MB 残っていた)。
struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn syncing_while_writing_does_not_reject() {
    for round in 0..4 {
        let path = format!("/tmp/enchudb-issue317-sync-{}-{round}.db", std::process::id());
        let _ = std::fs::remove_dir_all(&path);
        let _cleanup = Cleanup(path.clone());
        {
            let mut db = Database::create(&path).unwrap();
            db.table("t").number("id").tag("name").leaf("memo").number("age").primary_key("id").build().unwrap();
            let db = db.finish_with_oplog(64 << 20).unwrap();
            let t = db.get_table("t").unwrap();
            for i in 0..50_000i64 {
                t.insert()
                    .set("id", i)
                    .set("name", format!("n{}", i % 5000).as_str())
                    .set("memo", format!("m{i}").as_str())
                    .set("age", i % 100)
                    .commit()
                    .unwrap_or_else(|e| panic!("round {round} row {i}: {e}"));
                if i % 50 == 49 {
                    db.engine().oplog_sync().unwrap();
                }
            }
        }
        let _ = std::fs::remove_dir_all(&path);
    }
}
