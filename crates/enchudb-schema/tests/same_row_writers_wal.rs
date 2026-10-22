//! 書き手 8 本が WAL 付きの DB で同じ行を書き合う (#206 の test は書き手 2 本・WAL なし)。
//!
//! - 読み手は `get_many` で 4 列 (Number / BigInt / Tag / Leaf) を読み、 1 回の `commit()` の途中の組を見ない (#206)
//! - 書き終えた後、 各列のメモリの値が WAL の最後の record と同じ (`commit()` は行を握ってから WAL に積むので、 同じ行の書き手
//!   どうしで WAL の順と当てる順がそろう。 開き直した後・change listener・相手に届く値が、 今読める値と同じになる。 #503)

use enchudb_oplog::oplog::{DecodedOp, HEADER_SIZE};
use enchudb_oplog::eid_local;
use enchudb_schema::{Database, Value};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

const WRITERS: i64 = 8;
const COMMITS: i64 = 1500;
/// 書き手全体でこの回数の `commit()` ごとに、 書き手が揃って止まり、 読み手が 1 回読み終えるのを待つ (上限 READ_WAIT)。 書き手 8 本が
/// 同じ行を書き続けると、 読み手は行の版が動くたびに読み直して読み終えられない。 CPU が混むと、 書き手が終わるまでに 100 回も読めな
/// かった (#523: 負荷をかけて 30 回中 8 回)。 止まる回数 = 読めた回数の下限 (WRITERS * COMMITS / PAUSE_EVERY = 300)
const PAUSE_EVERY: u64 = 40;
const READ_WAIT: Duration = Duration::from_millis(200);
const COLS: [&str; 4] = ["hash", "mtime", "kind", "memo"];

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = enchudb_engine::db_files::remove_db(&self.0);
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
fn eight_writers_on_one_row_keep_rows_whole_and_end_on_the_last_wal_record() {
    let path = format!("/tmp/enchudb-same-row-writers-wal-{}", std::process::id());
    let _ = enchudb_engine::db_files::remove_db(&path);
    let _cleanup = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("files").number("id").number("hash").bigint("mtime").tag("kind").leaf("memo").primary_key("id").build().unwrap();
    let t = db.get_table("files").unwrap();
    let [a, b, c, d] = row(0).map(Option::unwrap);
    let e = t.insert().set("id", 1i64).set("hash", a).set("mtime", b).set("kind", c).set("memo", d).commit().unwrap();
    let hids: Vec<u16> = COLS.iter().map(|c| t.himo_id(c).unwrap()).collect();
    let db = db.finish_with_oplog(64 << 20).unwrap();
    let eng = db.engine();
    // WAL の最後の record と比べるので、 書き終えるまで畳ませない
    let no_fold = eng.transfer_lock_for_fold();
    let writing = AtomicBool::new(true);
    // 読み手が読み終えた回数と、 書き手が次に書く前に待つ回数
    let (reads_done, wait_for, commits) = (AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0));
    let (reads, torn) = std::thread::scope(|s| {
        let writers: Vec<_> = (0..WRITERS)
            .map(|w| {
                let (db, reads_done, wait_for, commits) = (&db, &reads_done, &wait_for, &commits);
                s.spawn(move || {
                    let t = db.get_table("files").unwrap();
                    for k in 0..COMMITS {
                        let since = Instant::now();
                        while reads_done.load(Ordering::Acquire) < wait_for.load(Ordering::Acquire) && since.elapsed() < READ_WAIT {
                            std::thread::yield_now();
                        }
                        let [a, b, c, d] = row(1 + w + WRITERS * k).map(Option::unwrap);
                        t.entity(e).update().set("hash", a).set("mtime", b).set("kind", c).set("memo", d).commit().unwrap();
                        if commits.fetch_add(1, Ordering::AcqRel) % PAUSE_EVERY == PAUSE_EVERY - 1 {
                            wait_for.store(reads_done.load(Ordering::Acquire) + 1, Ordering::Release);
                        }
                    }
                })
            })
            .collect();
        let readers: Vec<_> = (0..2)
            .map(|_| {
                let (db, writing, reads_done) = (&db, &writing, &reads_done);
                s.spawn(move || {
                    let t = db.get_table("files").unwrap();
                    let (mut reads, mut torn) = (0u64, Vec::new());
                    while writing.load(Ordering::Relaxed) {
                        let got = t.entity(e).get_many(&COLS);
                        reads += 1;
                        reads_done.fetch_add(1, Ordering::AcqRel);
                        let Some(Value::Number(i)) = got[0] else { panic!("hash が読めない: {got:?}") };
                        if got != row(i) && torn.len() < 3 {
                            torn.push(got);
                        }
                    }
                    (reads, torn)
                })
            })
            .collect();
        for w in writers {
            w.join().unwrap();
        }
        writing.store(false, Ordering::Relaxed);
        readers.into_iter().map(|r| r.join().unwrap()).fold((0, Vec::new()), |(n, mut all), (k, torn)| {
            all.extend(torn);
            (n + k, all)
        })
    });
    assert!(torn.is_empty(), "commit の途中の組を読んだ: {torn:?}");
    assert!(reads > 100, "読めた回数が少なすぎる ({reads})");
    assert_eq!(eng.wal_dropped_records(), 0, "WAL から落ちた (比べられない)");

    // 各列を最後に書いた WAL の record (数の列・Tag は Tie の値 = Tag は辞書の番号、 Leaf は TieLeaf の bytes)
    let wal = eng.oplog().unwrap();
    let (recs, _, tail, _) = wal.iter_from_with_tail(HEADER_SIZE as u64);
    let le = eid_local(e);
    let mut last_value = [None; 3];
    let mut last_memo = None;
    for r in recs.iter().chain(tail.iter()) {
        match &r.op {
            DecodedOp::Tie { eid, himo_id, value } if eid_local(*eid) == le => {
                if let Some(k) = hids[..3].iter().position(|h| h == himo_id) {
                    last_value[k] = Some(*value);
                }
            }
            DecodedOp::TieLeaf { eid, himo_name, bytes, .. } if eid_local(*eid) == le && himo_name == "files.memo" => {
                last_memo = Some(bytes.clone())
            }
            _ => {}
        }
    }
    for (k, col) in COLS[..3].iter().enumerate() {
        assert_eq!(eng.get_by_id(e, hids[k]), last_value[k], "{col}: メモリの値が WAL の最後の record と違う");
    }
    let memo = db.get_table("files").unwrap().entity(e).get("memo");
    let Some(Value::Text(memo)) = memo else { panic!("memo が読めない: {memo:?}") };
    assert_eq!(Some(memo.into_bytes()), last_memo, "memo: メモリの値が WAL の最後の record と違う");
    drop(no_fold);
}
