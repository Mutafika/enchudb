//! local-only の表 (`_` で始まる表、 `define_reserved_table`) の書き込みは change listener に渡らない (peer に配らないのと同じ)。
//!
//! listener の中で、 処理した位置を local-only の表に書く (宣言された答えの `_cursors` の `cursor_set` と同じ形)。 前は、 その
//! 書き込みが次の配りで同じ listener に届き、 その中でまた位置を書くので、 書き手が止まっても配りが止まらなかった (配るたびに
//! 1 回分ずつ流れ続け、 changefeed の記録も位置の書き込みで埋まる)。

use enchudb_engine::changefeed::ChangeListener;
use enchudb_oplog::oplog::DecodedOp;
use enchudb_engine::transport::WireRecord;
use enchudb_engine::{db_files, Engine, ValueType};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

const N: u32 = 300;

/// 受けた user の表の値と、 local-only の表の書き込みを受けた数を控え、 1 回ごとに位置を local-only の表に書く。
struct Saver {
    eng: OnceLock<std::sync::Weak<Engine>>,
    pos_row: OnceLock<u64>,
    note: u16,
    pos: u16,
    seen: Mutex<Vec<u64>>,
    calls: AtomicU64,
    local_only_seen: AtomicU64,
}

impl ChangeListener for Saver {
    fn on_changes_at(&self, first: u64, recs: &[WireRecord]) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        for r in recs {
            if let DecodedOp::Tie { himo_id, value, .. } = r.op {
                if himo_id == self.note {
                    self.seen.lock().unwrap().push(value);
                } else if himo_id == self.pos {
                    self.local_only_seen.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        // 処理した位置を local-only の表に書く (同じ commit)
        if let (Some(eng), Some(&row)) = (self.eng.get().and_then(|w| w.upgrade()), self.pos_row.get()) {
            eng.tie_to_by_id(row, self.pos, first + recs.len() as u64);
            eng.oplog_commit();
        }
    }
}

fn path(tag: &str) -> String {
    let p = std::env::temp_dir().join(format!("enchu_cf_local_only_{}_{tag}", std::process::id()));
    let _ = db_files::remove_db(p.to_str().unwrap());
    p.to_str().unwrap().to_string()
}

/// user の表 1 つと local-only の表 1 つ。 listener は位置を local-only の表に書く。
fn run(tag: &str, with_log: bool) {
    let p = path(tag);
    let mut b = Engine::create_with_capacity(&p, 4096).unwrap();
    b.define_table("notes", 1024).unwrap();
    b.define_himo_in("notes", "n", ValueType::Number, 0).unwrap();
    b.define_reserved_table("_pos", 16).unwrap();
    b.define_himo_in("_pos", "at", ValueType::Number, 0).unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(b, 1 << 20).unwrap();
    if with_log {
        eng.enable_change_log(4 << 20).unwrap();
    }
    let note = eng.himo_id("notes.n").unwrap() as u16;
    let pos = eng.himo_id("_pos.at").unwrap() as u16;
    let saver = Arc::new(Saver {
        eng: OnceLock::new(),
        pos_row: OnceLock::new(),
        note,
        pos,
        seen: Mutex::new(Vec::new()),
        calls: AtomicU64::new(0),
        local_only_seen: AtomicU64::new(0),
    });
    let _ = saver.eng.set(Arc::downgrade(&eng));
    let _ = saver.pos_row.set(eng.entity_in("_pos").unwrap());
    eng.add_change_listener(saver.clone());
    for v in 0..N {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to_by_id(e, note, v);
        if v % 10 == 9 {
            eng.oplog_commit();
        }
    }
    eng.oplog_commit();
    eng.oplog_sync().unwrap();
    let until = Instant::now() + Duration::from_secs(30);
    while saver.seen.lock().unwrap().len() < N as usize && Instant::now() < until {
        eng.oplog_sync().unwrap();
        std::thread::sleep(Duration::from_millis(5));
    }
    let got = saver.seen.lock().unwrap().len();
    // 書き手が止まった後: 位置の書き込みが配られ続けるなら、 配りが続く
    let settle = |ms: u64| {
        let until = Instant::now() + Duration::from_millis(ms);
        while Instant::now() < until {
            eng.oplog_sync().unwrap();
            std::thread::sleep(Duration::from_millis(10));
        }
    };
    settle(300);
    let calls = saver.calls.load(Ordering::Relaxed);
    settle(500);
    let more = saver.calls.load(Ordering::Relaxed) - calls;
    let local_only_seen = saver.local_only_seen.load(Ordering::Relaxed);
    let range = eng.change_log_range();
    drop(eng);
    let _ = db_files::remove_db(&p);
    assert_eq!(got, N as usize, "前提: user の表の書き込みが全部届く");
    assert!(calls > 0, "前提: listener が呼ばれて位置を書いた");
    assert_eq!(local_only_seen, 0, "local-only の表の書き込みが listener に届いた");
    assert_eq!(more, 0, "書き手が止まった後も配りが続いた (位置の書き込みを配り直している)");
    if with_log {
        // 記録には user の表の書き込みと、 その Commit だけ (位置の書き込みで埋まらない)
        let (first, next) = range.unwrap();
        assert!(next - first <= (N + N / 10 + 2) as u64, "記録に位置の書き込みが載った ({first}..{next})");
    }
}

#[test]
fn local_only_writes_are_not_delivered() {
    run("plain", false);
}

#[test]
fn local_only_writes_are_not_recorded_in_the_change_log() {
    run("log", true);
}
