//! #317: 辞書の索引 (疎なファイル、 slot が全体に散る) は、 新しい値ごとに別々のページを触り、 そのページは
//! 書き出しで本当にディスクを食う (16 KB)。 初めて触るページを数えないと、 使える空きを超えても気付けない。
//!
//! 使える空き 32 MB で新しい Tag の値を書き続ける: 空きの 5 倍を触る前に commit が Err、 開き直すと Ok の row は
//! 全部読める。 プロセス全体の合計を見るので 1 file 1 テスト。

use enchudb_schema::{Database, SchemaError, Value};

/// panic しても DB を消す (変異試験で落とすたびに /tmp に数百 MB 残っていた)。
struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn name(i: i64) -> String {
    format!("tag-{i:08}")
}

#[test]
fn new_index_pages_are_counted() {
    let path = format!("/tmp/enchudb-issue317-pages-{}.db", std::process::id());
    let _ = std::fs::remove_dir_all(&path);
    let _cleanup = Cleanup(path.clone());
    let n;
    {
        let mut db = Database::create(&path).unwrap();
        db.table("tags").number("id").tag("name").primary_key("id").build().unwrap();
        let t = db.get_table("tags").unwrap();
        let eng = db.engine();
        let free = eng.disk_free_bytes().expect("growable backing");
        const USABLE: u64 = 32 << 20;
        eng.set_space_margin(free.saturating_sub(USABLE));
        let page = 16 << 10;
        let mut ok = 0i64;
        n = loop {
            // 1 値 = 高々 1 ページ。 空きの 5 倍ぶんのページを触っても止まらなければ失敗
            assert!((ok as u64) * page < 5 * USABLE, "使える空き {USABLE} の 5 倍のページを触っても断らない");
            match t.insert().set("id", ok).set("name", name(ok).as_str()).commit() {
                Ok(_) => ok += 1,
                Err(SchemaError::WriteRejected(_)) => break ok,
                Err(e) => panic!("row {ok}: {e}"),
            }
        };
        assert!(n > 100, "早すぎる拒否 ({n} 行)");
        eng.set_space_margin(0);
        db.engine_mut().expect("単独所有").flush().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let t = db.get_table("tags").unwrap();
    for i in 0..n {
        let e = t.where_eq("id", i).find_one().unwrap().unwrap_or_else(|| panic!("row {i} が無い"));
        assert_eq!(t.entity(e).get("name"), Some(Value::Text(name(i))), "row {i}");
    }
    drop(t);
    drop(db);
    let _ = std::fs::remove_dir_all(&path);
}
