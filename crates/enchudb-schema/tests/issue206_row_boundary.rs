//! #206: `commit()` 1 回で書き換えた行の列を、 別 thread が 1 列ずつ読むと 「同時には無かった組」 を掴む
//! (新しい hash と古い mtime)。 `get_many` はその途中を見ない。
//!
//! 書き手は Number / BigInt / Tag / Leaf の 4 列を全部同じ i で書き換え続け、 読み手は `get_many` で読んで
//! 4 列が揃っているかを見る。 修正前 (行の境界なし) は 1 列ずつ読むと数百万回ずれた (実測 360 万 / 1350 万回)。

use enchudb_schema::{Database, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn row(i: i64) -> [Option<Value>; 4] {
    [
        Some(Value::Number(i)),
        Some(Value::Number(-i)),
        Some(Value::Text(format!("tag-{}", i % 50))),
        Some(Value::Text(format!("memo-{i}-{}", "m".repeat((i % 7) as usize * 40)))),
    ]
}

#[test]
fn get_many_never_sees_half_of_a_commit() {
    let path = format!("/tmp/enchudb-issue206-{}.db", std::process::id());
    let _ = std::fs::remove_dir_all(&path);
    let _cleanup = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("files").number("id").number("hash").bigint("mtime").tag("kind").leaf("memo").primary_key("id").build().unwrap();
    let t = db.get_table("files").unwrap();
    let [a, b, c, d] = row(0).map(Option::unwrap);
    let e = t.insert().set("id", 1i64).set("hash", a).set("mtime", b).set("kind", c).set("memo", d).commit().unwrap();
    drop(t);
    let db = db.finish_concurrent().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..2)
        .map(|w| {
            let (db, stop) = (db.clone(), stop.clone());
            std::thread::spawn(move || {
                let t = db.get_table("files").unwrap();
                let mut i = 1 + w;
                while !stop.load(Ordering::Relaxed) {
                    let [a, b, c, d] = row(i).map(Option::unwrap);
                    t.entity(e).update().set("hash", a).set("mtime", b).set("kind", c).set("memo", d).commit().unwrap();
                    i += 2;
                }
            })
        })
        .collect();
    let readers: Vec<_> = (0..2)
        .map(|_| {
            let db = db.clone();
            std::thread::spawn(move || {
                let t = db.get_table("files").unwrap();
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
                let (mut reads, mut torn) = (0u64, Vec::new());
                while std::time::Instant::now() < deadline {
                    let got = t.entity(e).get_many(&["hash", "mtime", "kind", "memo"]);
                    reads += 1;
                    let Some(Value::Number(i)) = got[0] else { panic!("hash が読めない: {got:?}") };
                    if got != row(i) && torn.len() < 3 {
                        torn.push(got);
                    }
                }
                (reads, torn)
            })
        })
        .collect();
    let mut reads = 0;
    for r in readers {
        let (n, torn) = r.join().unwrap();
        reads += n;
        assert!(torn.is_empty(), "commit の途中の組を読んだ: {torn:?}");
    }
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.join().unwrap();
    }
    assert!(reads > 1000, "読めた回数が少なすぎる ({reads})");
}
