//! #702: 記録 (`changefeed.seg`) のある DB で、 並行の形にする前 (`open_standalone` → `add_change_listener` →
//! `concurrentize_with_oplog`、 schema なら build phase) に付けた listener に 1 件も届かなかった。
//!
//! 付けた時は記録がまだ付いていないので `change_listeners` にだけ入り、 後から記録が付くと、 配信は記録の読み手 (`readers`)
//! しか見なかった。 `on_detached` も来ない。 記録の無い DB では届いていた。
#![cfg(not(target_arch = "wasm32"))]

use enchudb_engine::changefeed::ChangeListener;
use enchudb_engine::transport::WireRecord;
use enchudb_engine::{db_files, Engine, ValueType};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct Tmp(String);
impl Tmp {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchudb-issue702-{tag}-{}", std::process::id()));
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
    /// (番号, Tie の値)
    seen: Mutex<Vec<(u64, Option<u64>)>>,
    detached: AtomicUsize,
}
impl Rec {
    fn values(&self) -> Vec<u64> {
        self.seen.lock().unwrap().iter().filter_map(|(_, v)| *v).collect()
    }
    fn numbers(&self) -> Vec<u64> {
        self.seen.lock().unwrap().iter().map(|(n, _)| *n).collect()
    }
}
impl ChangeListener for Rec {
    fn on_changes_at(&self, first: u64, recs: &[WireRecord]) {
        let mut s = self.seen.lock().unwrap();
        for (k, r) in recs.iter().enumerate() {
            let v = match r.op {
                enchudb_oplog::oplog::DecodedOp::Tie { value, .. } => Some(value),
                _ => None,
            };
            s.push((first + k as u64, v));
        }
    }
    fn on_detached(&self, _: enchudb_oplog::Hlc) {
        self.detached.fetch_add(1, Ordering::SeqCst);
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

/// 前の session (値 1 を 1 件書いて閉じる)。 `with_log` なら記録を作る。
fn first_session(path: &str, with_log: bool) {
    let eng = Engine::create_concurrent_with_oplog(path, 4 << 20).unwrap();
    eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
    if with_log {
        eng.enable_change_log(1 << 20).unwrap();
    }
    let e = eng.entity().unwrap();
    eng.tie_to_by_id(e, eng.himo_id("ts").unwrap() as u16, 1u64);
    eng.oplog_sync().unwrap();
}

fn early_listener(with_log: bool) {
    let tmp = Tmp::new(if with_log { "log" } else { "nolog" });
    first_session(&tmp.0, with_log);
    assert_eq!(
        std::path::Path::new(&tmp.0).join("changefeed.seg").exists(),
        with_log,
        "前提: 記録の file が在る / 無い"
    );

    // 並行の形にする前に付ける
    let eng = Engine::open_standalone(&tmp.0).unwrap();
    let rec = Arc::new(Rec::default());
    eng.add_change_listener(rec.clone());
    let eng = Engine::concurrentize_with_oplog(eng, 4 << 20).unwrap();
    assert_eq!(eng.change_listener_count(), 1, "前提: listener は付いている");
    let before = eng.change_log_range();
    assert_eq!(before.is_some(), with_log, "前提: 並行の形にした時に記録が付いた / 付いていない");

    let ts = eng.himo_id("ts").unwrap() as u16;
    let e = eng.entity().unwrap();
    for v in 100..110u64 {
        eng.tie_to_by_id(e, ts, v);
    }
    eng.oplog_sync().unwrap();
    let want: Vec<u64> = (100..110).collect();
    assert!(
        wait_until(|| rec.values().len() >= 10, 30),
        "with_log={with_log}: 並行の形にする前に付けた listener に届かない (受けた値 {:?}、 外れた知らせ {})",
        rec.values(),
        rec.detached.load(Ordering::SeqCst)
    );
    // 付けた後の commit だけ (前の session の 1 は渡らない)、 書いた順に 1 回ずつ
    assert_eq!(rec.values(), want, "with_log={with_log}");
    assert_eq!(rec.detached.load(Ordering::SeqCst), 0);
    assert_eq!(eng.change_listener_count(), 1);
    if let Some((_, next_before)) = before {
        // 番号は記録の続き (付けた時の記録の終わりから 1 つずつ)
        let numbers = rec.numbers();
        assert_eq!(numbers[0], next_before, "最初に渡る番号は、 付けた時の記録の終わり");
        assert!(numbers.windows(2).all(|w| w[1] == w[0] + 1), "番号が続いていない: {numbers:?}");
        assert_eq!(eng.change_log_range().unwrap().1, numbers.last().unwrap() + 1);
        assert_eq!(eng.change_backlog_len(), 0, "読み手が記録の終わりに追いついている");
    }
    drop(eng);
    assert_eq!(rec.values(), want, "閉じた後に余分に渡っていない");
}

#[test]
fn listener_added_before_concurrentize_receives_with_a_record() {
    early_listener(true);
}

/// 対照: 記録の無い DB は前から届いていた。
#[test]
fn listener_added_before_concurrentize_receives_without_a_record() {
    early_listener(false);
}

/// 2 本付けても、 並行の形にした後に付けた listener と同じ番号・同じ中身を受ける。
#[test]
fn early_and_late_listeners_see_the_same_numbers() {
    let tmp = Tmp::new("both");
    first_session(&tmp.0, true);
    let eng = Engine::open_standalone(&tmp.0).unwrap();
    let (a, b) = (Arc::new(Rec::default()), Arc::new(Rec::default()));
    eng.add_change_listener(a.clone());
    eng.add_change_listener(b.clone());
    let eng = Engine::concurrentize_with_oplog(eng, 4 << 20).unwrap();
    assert!(eng.change_log_range().is_some(), "前提: 記録が付いた");
    let late = Arc::new(Rec::default());
    eng.add_change_listener(late.clone());
    assert_eq!(eng.change_listener_count(), 3);

    let ts = eng.himo_id("ts").unwrap() as u16;
    let e = eng.entity().unwrap();
    for v in 200..220u64 {
        eng.tie_to_by_id(e, ts, v);
    }
    eng.oplog_sync().unwrap();
    assert!(wait_until(|| [&a, &b, &late].iter().all(|r| r.values().len() >= 20), 30), "3 本とも受ける");
    let want = late.seen.lock().unwrap().clone();
    assert_eq!(want.iter().filter_map(|(_, v)| *v).collect::<Vec<_>>(), (200..220).collect::<Vec<u64>>());
    assert_eq!(*a.seen.lock().unwrap(), want);
    assert_eq!(*b.seen.lock().unwrap(), want);
}
