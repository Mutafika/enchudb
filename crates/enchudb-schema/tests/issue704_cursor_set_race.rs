//! #704: 同じ名前の最初の `cursor_set` が並んでも、 `_cursors` の行は名前ごとに 1 つ。
//!
//! 前は 「在るかの確かめ → 行を作る」 が lock の外で、 並んだ書き手がそれぞれ行を作った (2〜4 行)。 以後の `cursor_set` /
//! `cursor_get` は索引の先頭の行を使い、 先頭は開き直すと変わる (置いた順 → eid の順) ので、 開き直した後に古い位置が返った。
//!
//! 直し:
//! - 行が無い時は 1 本の lock の下で確かめ直してから作る
//! - 0.36〜0.37.4 が作った重複: 読むのは eid のいちばん小さい行 (索引の並びによらない。 その版が開き直した後に読み書きして
//!   いた行)。 その名前の次の `cursor_set` が、 いちばん小さい行に書いて残りを消す
//!
//! 窓を踏んだかは `Engine::cursor_create_races` (lock の下で確かめ直したら他の書き手が先に作っていた回数) で見る。

use enchudb_engine::Engine;
use enchudb_schema::{Cursor, Database, CURSOR_ANSWER};
use std::sync::{Arc, Barrier};

fn tmp_path(tag: &str) -> String {
    format!(
        "{}/enchudb-issue704-{}-{}-{}",
        std::env::temp_dir().display(),
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn rows_of(db: &Database, name: &str) -> Vec<u64> {
    let eng = db.engine();
    let name_h = eng.himo_id("_cursors.name").unwrap() as u16;
    match eng.vocab_id(name) {
        Some(vid) => eng.query_by_id(&[(name_h, vid)]),
        None => vec![],
    }
}

/// 同じ新しい名前を 4 本が同時に置く。 窓 (確かめた時は無く、 作る時には他の書き手が作っていた) を決めた回数踏むまで名前を
/// 変えて回し、 どの名前も 1 行で、 置き直した位置が開き直した後も読めること。
#[test]
fn concurrent_first_cursor_sets_of_one_name_make_one_row() {
    const WRITERS: u64 = 4;
    const WANT: u64 = 20;
    const MAX_NAMES: usize = 900; // `_cursors` の枠は 4096 行 (直しなしでも 4 本 × 900 は入る)
    let path = tmp_path("first");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.prepare_cursors().unwrap();
    db.table("t").number("id").primary_key("id").build().unwrap();
    let db = db.finish_with_oplog(16 << 20).unwrap();
    let before = Engine::cursor_create_races();
    let mut names = 0;
    while Engine::cursor_create_races() - before < WANT && names < MAX_NAMES {
        let name = format!("c{names}");
        names += 1;
        let barrier = Arc::new(Barrier::new(WRITERS as usize));
        let hs: Vec<_> = (0..WRITERS)
            .map(|w| {
                let (db, name, barrier) = (db.clone(), name.clone(), barrier.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    db.cursor_set(&name, Cursor { kind: CURSOR_ANSWER, pos: 100 + w, hlc: None }).unwrap();
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        let rows = rows_of(&db, &name);
        assert_eq!(rows.len(), 1, "名前 {name}: `_cursors` に {} 行 {rows:?}", rows.len());
        let got = db.cursor_get(&name).unwrap();
        assert!((100..100 + WRITERS).contains(&got.pos) && got.kind == CURSOR_ANSWER, "{name}: {got:?}");
        db.cursor_set(&name, Cursor { kind: CURSOR_ANSWER, pos: 999, hlc: None }).unwrap();
        assert_eq!(db.cursor_get(&name).map(|c| c.pos), Some(999));
    }
    let races = Engine::cursor_create_races() - before;
    assert!(races >= WANT, "{names} 個の名前で窓を {races} 回しか踏めなかった (試験の前提)");
    db.engine().oplog_sync().unwrap();
    drop(db);
    let db = Database::open(&path).unwrap();
    for i in 0..names {
        let name = format!("c{i}");
        assert_eq!(db.cursor_get(&name).map(|c| c.pos), Some(999), "開き直した後の {name}");
        assert_eq!(rows_of(&db, &name).len(), 1);
    }
}

/// 0.36〜0.37.4 が残した重複の形を作る: 同じ名前の行を 3 つ、 eid の大きい方から先に名前を書く (索引に置いた順の先頭が
/// eid のいちばん小さい行でない)。 位置は行ごとに違う。
fn plant_duplicates(db: &Database, name: &str, pos_by_row: [u64; 3]) -> Vec<u64> {
    let eng = db.engine();
    let h = |c: &str| eng.himo_id(&format!("_cursors.{c}")).unwrap() as u16;
    let (name_h, kind_h, pos_h) = (h("name"), h("kind"), h("pos"));
    let rows: Vec<u64> = (0..3).map(|_| eng.entity_in("_cursors").unwrap()).collect();
    assert!(rows[0] < rows[1] && rows[1] < rows[2]);
    for i in (0..3).rev() {
        let _row = eng.write_row(rows[i]);
        eng.try_tie_text_to_by_id(rows[i], name_h, name).unwrap();
        eng.try_tie_to_by_id(rows[i], kind_h, CURSOR_ANSWER).unwrap();
        eng.try_tie_to_by_id(rows[i], pos_h, pos_by_row[i]).unwrap();
    }
    rows
}

/// 重複のある DB: 読むのは eid のいちばん小さい行 (開く前も後も同じ答え)。 次の `cursor_set` で 1 行になる。
#[test]
fn duplicate_rows_from_an_older_version_read_the_lowest_row_and_collapse_on_set() {
    let path = tmp_path("dups");
    let _c = Cleanup(path.clone());
    let lowest;
    {
        let mut db = Database::create(&path).unwrap();
        db.prepare_cursors().unwrap();
        db.table("t").number("id").primary_key("id").build().unwrap();
        let db = db.finish_with_oplog(16 << 20).unwrap();
        // 2 回置く: 2 回目が名前の索引を引いて組ませる (組んだ後の書き込みは置いた順に並ぶ。 組む前に書いた行は eid の順)
        db.cursor_set("other", Cursor { kind: CURSOR_ANSWER, pos: 4, hlc: None }).unwrap();
        db.cursor_set("other", Cursor { kind: CURSOR_ANSWER, pos: 5, hlc: None }).unwrap();
        let rows = plant_duplicates(&db, "dup", [101, 102, 103]);
        plant_duplicates(&db, "kept", [201, 202, 203]);
        lowest = rows[0];
        assert_eq!(rows_of(&db, "dup").len(), 3, "試験の前提: 重複を作れていない");
        assert_ne!(rows_of(&db, "dup")[0], lowest, "試験の前提: 索引の先頭が eid のいちばん小さい行でない形を作る");
        // 同じ process の中 (索引は置いた順) でも、 eid のいちばん小さい行を読む
        assert_eq!(db.cursor_get("dup").map(|c| c.pos), Some(101));
        db.engine().oplog_sync().unwrap();
    }
    {
        let db = Database::open_with_oplog(&path, 16 << 20).unwrap();
        // 開き直した後 (索引は eid の順) も同じ行
        assert_eq!(db.cursor_get("dup").map(|c| c.pos), Some(101));
        assert_eq!(db.cursor_get("kept").map(|c| c.pos), Some(201));
        assert_eq!(rows_of(&db, "dup").len(), 3, "読むだけでは片付けない");
        // 置くと、 いちばん小さい行に書いて残りを消す
        db.cursor_set("dup", Cursor { kind: CURSOR_ANSWER, pos: 500, hlc: None }).unwrap();
        assert_eq!(rows_of(&db, "dup"), vec![lowest]);
        assert_eq!(db.cursor_get("dup").map(|c| c.pos), Some(500));
        // 触っていない名前はそのまま
        assert_eq!(rows_of(&db, "kept").len(), 3);
        assert_eq!(db.cursor_get("other").map(|c| c.pos), Some(5));
        db.engine().oplog_sync().unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(rows_of(&db, "dup"), vec![lowest], "片付けた行が開き直して戻った");
    assert_eq!(db.cursor_get("dup").map(|c| c.pos), Some(500));
    assert_eq!(db.cursor_get("kept").map(|c| c.pos), Some(201));
    assert_eq!(db.cursor_get("other").map(|c| c.pos), Some(5));
}

/// 重複のある名前を並んで置いても 1 行になり、 最後に 1 本で置いた位置が残る。
#[test]
fn concurrent_sets_on_a_duplicated_name_collapse_to_one_row() {
    let path = tmp_path("dups-race");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.prepare_cursors().unwrap();
    db.table("t").number("id").primary_key("id").build().unwrap();
    let db = db.finish_with_oplog(16 << 20).unwrap();
    for round in 0..50u64 {
        let name = format!("d{round}");
        let rows = plant_duplicates(&db, &name, [1, 2, 3]);
        let barrier = Arc::new(Barrier::new(4));
        let hs: Vec<_> = (0..4u64)
            .map(|w| {
                let (db, name, barrier) = (db.clone(), name.clone(), barrier.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    for i in 0..20 {
                        db.cursor_set(&name, Cursor { kind: CURSOR_ANSWER, pos: 1000 + w * 100 + i, hlc: None }).unwrap();
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        assert_eq!(rows_of(&db, &name), vec![rows[0]], "{name}");
        let got = db.cursor_get(&name).unwrap().pos;
        assert!((0..4).any(|w| got == 1000 + w * 100 + 19), "{name}: どの書き手の最後の位置でもない {got}");
        db.cursor_set(&name, Cursor { kind: CURSOR_ANSWER, pos: 7, hlc: None }).unwrap();
        assert_eq!(db.cursor_get(&name).map(|c| c.pos), Some(7));
    }
}
