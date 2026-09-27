//! #327: open 時に辞書の索引を作り直す時 (前回が正常に閉じていない)、 書くページの空きを確かめる。
//! 旧: 伸ばせなくても書きに進み (SIGBUS)、 伸ばせても索引全体に散った slot のページを数えずに書いた (#317 と同じく
//! 空きを超えた分は黙って消えうる)。
//!
//! 索引の 1 ページ目より後ろを穴に戻して 「正常に閉じていない」 印を立て (= 落ちて索引が欠けた状態)、 margin を
//! 巨大にして開く → 索引の空き不足でエラー。 margin を戻せば開けて、 値も値での検索も全部戻る。
//! margin の既定値はプロセス全体なので 1 file 1 テスト。

use enchudb_schema::{Database, Value};
use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;

/// panic しても DB を消す (変異試験で落とすたびに /tmp に数百 MB 残っていた)。
struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn name(i: i64) -> String {
    format!("tag-{i:06}")
}

#[test]
fn rebuild_checks_space_before_writing() {
    let path = format!("/tmp/enchudb-issue327-{}.db", std::process::id());
    let _ = std::fs::remove_dir_all(&path);
    let _cleanup = Cleanup(path.clone());
    const N: i64 = 2000;
    {
        let mut db = Database::create(&path).unwrap();
        db.table("t").number("id").tag("name").primary_key("id").build().unwrap();
        let t = db.get_table("t").unwrap();
        for i in 0..N {
            t.insert().set("id", i).set("name", name(i).as_str()).commit().unwrap();
        }
    }
    // 索引の 1 ページ目 (header 込み) より後ろを穴に戻す + vocab data の clean flag (offset 12) を 0 に
    let index = OpenOptions::new().write(true).open(format!("{path}/vocab.index.seg")).unwrap();
    let len = index.metadata().unwrap().len();
    index.set_len(4096).unwrap();
    index.set_len(len).unwrap();
    drop(index);
    let data = OpenOptions::new().write(true).open(format!("{path}/vocab.data.seg")).unwrap();
    data.write_all_at(&0u32.to_le_bytes(), 12).unwrap();
    drop(data);

    enchudb_engine::segment_map::set_default_space_margin(u64::MAX / 4);
    let err = Database::open(&path).err().expect("空きが無いのに索引を作り直して開けた");
    assert!(err.to_string().contains("vocab.index"), "索引以外の理由で失敗: {err}");

    enchudb_engine::segment_map::set_default_space_margin(32 << 20);
    let db = Database::open(&path).unwrap();
    let t = db.get_table("t").unwrap();
    for i in 0..N {
        let e = t.where_eq("id", i).find_one().unwrap().unwrap();
        assert_eq!(t.entity(e).get("name"), Some(Value::Text(name(i))), "row {i}");
        assert_eq!(t.where_eq("name", name(i).as_str()).find_one().unwrap(), Some(e), "値で引けない row {i}");
    }
    drop(t);
    drop(db);
    let _ = std::fs::remove_dir_all(&path);
}
