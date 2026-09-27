//! #135: 同じ Leaf の cell を複数 thread から `tie_*` で書き直すと、 2 本が同じ旧 slot を読んでどちらも
//! free する (二重 free)。 cell は free 済みの slot を指し、 その hole は後の insert に 2 度払い出されて、
//! 別の cell の中身を上書きする。
//!
//! 8 thread が 1 つの cell を書き直しながら、 それぞれ自分の行の cell (長さを揃えて hole を奪い合わせる)
//! を 1 回ずつ書いていく。 最後に全部の cell を読み返す。

use enchudb_schema::Database;
use std::sync::Arc;

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

const THREADS: u64 = 8;
const ROUNDS: u64 = 20_000;

fn memo(t: u64, i: u64) -> String {
    format!("{t:02}-{i:08}-{}", "x".repeat(100))
}

#[test]
fn concurrent_retie_of_one_cell_keeps_every_cell() {
    let path = format!("/tmp/enchudb-issue135-{}.db", std::process::id());
    let _ = std::fs::remove_dir_all(&path);
    let _cleanup = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("notes").number("id").leaf("memo").primary_key("id").build().unwrap();
    let t = db.get_table("notes").unwrap();
    let hot = t.insert().set("id", 0i64).set("memo", memo(99, 0).as_str()).commit().unwrap();
    let hid = t.himo_id("memo").unwrap();
    let rows: Vec<Vec<u64>> = (0..THREADS)
        .map(|th| (0..ROUNDS).map(|i| t.insert().set("id", (1 + th * ROUNDS + i) as i64).commit().unwrap()).collect())
        .collect();
    drop(t);
    let db = db.finish_concurrent().unwrap();
    let eng = db.arc_engine();
    let rows = Arc::new(rows);
    let hs: Vec<_> = (0..THREADS)
        .map(|th| {
            let (eng, rows) = (eng.clone(), rows.clone());
            std::thread::spawn(move || {
                for i in 0..ROUNDS {
                    eng.try_tie_text_to_by_id(hot, hid, &memo(th, i)).unwrap();
                    eng.try_tie_text_to_by_id(rows[th as usize][i as usize], hid, &memo(th, i)).unwrap();
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    let mut bad = Vec::new();
    for th in 0..THREADS {
        for i in 0..ROUNDS {
            let got = eng.get_text_owned(rows[th as usize][i as usize], "notes.memo");
            if got.as_deref() != Some(memo(th, i).as_bytes()) {
                bad.push((th, i, got.map(|b| String::from_utf8_lossy(&b[..20.min(b.len())]).into_owned())));
            }
        }
    }
    let hot_now = eng.get_text_owned(hot, "notes.memo").map(|b| String::from_utf8_lossy(&b).into_owned());
    assert!(bad.is_empty(), "{} 個の cell の中身が変わった (先頭 {:?})", bad.len(), &bad[..bad.len().min(3)]);
    let hot_now = hot_now.expect("書き直した cell が読めない");
    assert!(hot_now.ends_with(&"x".repeat(100)) && hot_now.len() == memo(0, 0).len(), "書き直した cell が壊れた: {hot_now}");
}
