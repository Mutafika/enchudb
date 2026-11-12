//! #732: 記録 (`changefeed.seg`) の無い DB で、 並行の形にする前 (`open_standalone` → `add_change_listener` →
//! `concurrentize_with_oplog`、 schema なら build phase) に付けた listener の cursor が 0 のままだった。
//!
//! standalone の間は WAL が無いので、 付けた時に cursor を WAL の head に合わせる所が走らない。 前の session がきれいに
//! 閉じずに WAL に record が残っていると、 「付けた後の commit だけ届く」 に反して、 前の session の record が渡った。
//!
//! 子 process で書いて、 周期の書き出し (100 ms) より前に abort し (Commit で閉じた record が WAL に残る)、 親で
//! standalone で開いて listener を付けてから並行の形にする。
#![cfg(not(target_arch = "wasm32"))]

use enchudb_engine::changefeed::ChangeListener;
use enchudb_engine::transport::WireRecord;
use enchudb_engine::{db_files, Engine, ValueType};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const CHILD: &str = "ENCHU_ISSUE732_CHILD";
/// 前の session (子) が書く値。
const OLD: std::ops::RangeInclusive<u64> = 1..=5;

struct Tmp(String);
impl Tmp {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchudb-issue732-{tag}-{}", std::process::id()));
        let p = p.to_string_lossy().into_owned();
        let _ = db_files::remove_db(&p);
        Tmp(p)
    }
}
impl Drop for Tmp {
    fn drop(&mut self) {
        let _ = db_files::remove_db(&self.0);
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Default)]
struct Rec {
    seen: Mutex<Vec<u64>>,
}
impl Rec {
    fn values(&self) -> Vec<u64> {
        self.seen.lock().unwrap().clone()
    }
}
impl ChangeListener for Rec {
    fn on_changes(&self, recs: &[WireRecord]) {
        let mut s = self.seen.lock().unwrap();
        for r in recs {
            if let enchudb_oplog::oplog::DecodedOp::Tie { value, .. } = r.op {
                s.push(value);
            }
        }
    }
}

fn wait_until(mut f: impl FnMut() -> bool, secs: u64) -> bool {
    let until = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < until {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    f()
}

/// 子: 書いて、 Commit で閉じて、 書き出しの前に落ちる。
#[test]
fn issue732_child() {
    let Ok(path) = std::env::var(CHILD) else { return };
    let eng = Engine::create_concurrent_with_oplog(&path, 4 << 20).unwrap();
    eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
    let ts = eng.himo_id("ts").unwrap() as u16;
    let e = eng.entity().unwrap();
    // 紐の定義と entity は書き出しておく (落ちた後に残るのは下の値の record だけ)
    eng.oplog_sync().unwrap();
    for v in OLD {
        eng.tie_to_by_id(e, ts, v);
    }
    eng.oplog_commit();
    // 周期の書き出し (checkpoint を進めて畳む) より先に落ちる = 値の record が WAL に残る
    std::process::abort();
}

/// 子を走らせ、 落ちた後の DB の path を返す。
fn crashed_db(tag: &str) -> Tmp {
    let tmp = Tmp::new(tag);
    let st = Command::new(std::env::current_exe().unwrap())
        .args(["issue732_child", "--exact", "--nocapture"])
        .env(CHILD, &tmp.0)
        .status()
        .unwrap();
    assert!(!st.success(), "子は abort で終わるはず");
    tmp
}

#[test]
fn listener_added_before_concurrentize_gets_only_later_commits() {
    if std::env::var(CHILD).is_ok() {
        return;
    }
    let tmp = crashed_db("early");
    assert!(!std::path::Path::new(&tmp.0).join("changefeed.seg").exists(), "前提: 記録の無い DB");

    let eng = Engine::open_standalone(&tmp.0).unwrap();
    let rec = Arc::new(Rec::default());
    eng.add_change_listener(rec.clone());
    let eng = Engine::concurrentize_with_oplog(eng, 4 << 20).unwrap();
    let ts = eng.himo_id("ts").unwrap() as u16;
    // 前提: 前の session の値は落ちた後も在る (WAL から再生された。 最後に書いた値が読める)
    assert_eq!(eng.get_by_id(0, ts), Some(*OLD.end()), "前提: 前の session の record が WAL に残っていて再生された");

    let e = eng.entity().unwrap();
    for v in 100..110u64 {
        eng.tie_to_by_id(e, ts, v);
    }
    eng.oplog_sync().unwrap();
    assert!(wait_until(|| rec.values().iter().filter(|v| **v >= 100).count() == 10, 60), "付けた後の commit が届かない: {:?}", rec.values());
    assert_eq!(
        rec.values(),
        (100..110).collect::<Vec<u64>>(),
        "並行の形にする前に付けた listener に、 前の session の record ({OLD:?}) が渡った"
    );
    drop(eng);
    assert_eq!(rec.values(), (100..110).collect::<Vec<u64>>(), "閉じた後に余分に渡っていない");
}

/// 対照: 並行の形で開いてから付けた listener (cursor を head に合わせる道) には、 同じ落ち方の後でも前の session の record は
/// 渡らない。 かつ、 前提の確かめ: 同じ落ち方の WAL には前の session の record が実際に残っている (`audit` で見える)。
#[test]
fn listener_added_after_open_gets_only_later_commits() {
    if std::env::var(CHILD).is_ok() {
        return;
    }
    let tmp = crashed_db("late");
    let eng = Engine::open_concurrent_with_oplog(&tmp.0, 4 << 20).unwrap();
    let rec = Arc::new(Rec::default());
    eng.add_change_listener(rec.clone());
    let ts = eng.himo_id("ts").unwrap() as u16;
    assert_eq!(eng.get_by_id(0, ts), Some(*OLD.end()));
    let e = eng.entity().unwrap();
    for v in 100..110u64 {
        eng.tie_to_by_id(e, ts, v);
    }
    eng.oplog_sync().unwrap();
    assert!(wait_until(|| rec.values().iter().filter(|v| **v >= 100).count() == 10, 60));
    assert_eq!(rec.values(), (100..110).collect::<Vec<u64>>());
}
