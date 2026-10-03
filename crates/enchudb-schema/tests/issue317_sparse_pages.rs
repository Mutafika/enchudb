//! #317: 書いたページは書き出しで本当にディスクを食う (16 KB)。 初めて触るページを数えないと、 使える空きを超えても
//! 気付けない。 使える空き 2 MB で新しい Tag の値を書き続ける: 空きを超える前に commit が Err になり、 開き直すと
//! Ok の row は全部読める。 プロセス全体の合計を見るので 1 file 1 テスト。
//!
//! 0.28.5 までは辞書の索引の slot が領域全体に散り、 新しい値ごとに別のページを触ったので、 この試験は 「値 1 つ = 高々
//! 1 ページ」 を上限に断るまでを見ていた。 #374 から索引は語数に合わせて伸び、 ページは値ごとには増えない (先に列や
//! 辞書の data が空きを使い切る、 実測 2 万行前後)。 索引の表を書く時 (伸ばす / 開いた時に作り直す、 同じ
//! `write_gen`) の空きの数え方は `issue327_rebuild_space` が見る。

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
        const USABLE: u64 = 2 << 20;
        eng.set_space_margin(free.saturating_sub(USABLE));
        let mut ok = 0i64;
        n = loop {
            // 1 行は数十 B。 100 万行入っても断らなければ、 書いたページを空きから引いていない
            assert!(ok < 1_000_000, "使える空き {USABLE} B で 100 万行入った (書いたページを空きから引いていない)");
            match t.insert().set("id", ok).set("name", name(ok).as_str()).commit() {
                Ok(_) => ok += 1,
                Err(SchemaError::WriteRejected(_)) => break ok,
                Err(e) => panic!("row {ok}: {e}"),
            }
        };
        eprintln!("{n} 行で拒否");
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
