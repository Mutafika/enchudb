//! review (0.37.4, changefeed): 疑いの再現。 どれも状態で判定する (時間に頼らない)。
#![cfg(not(target_arch = "wasm32"))]

use enchudb_engine::changefeed::ChangeListener;
use enchudb_engine::transport::WireRecord;
use enchudb_engine::{db_files, Engine, ValueType};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SCRATCH: &str = "/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/eedfc333-d87a-4a47-a3b0-daf4b6d7337a/scratchpad";

struct Tmp(String);
impl Tmp {
    fn new(tag: &str) -> Self {
        let _ = std::fs::create_dir_all(SCRATCH);
        let p = format!("{SCRATCH}/review-cf-{tag}-{}", std::process::id());
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
    seen: Mutex<Vec<(u64, Option<u64>)>>,
    detached: AtomicUsize,
}
impl Rec {
    fn values(&self) -> Vec<u64> {
        self.seen.lock().unwrap().iter().filter_map(|(_, v)| *v).collect()
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
        std::thread::sleep(Duration::from_millis(20));
    }
    f()
}

/// T1: 記録 (changefeed.seg) のある DB を standalone で開き、 listener を付けてから並行の形にする
/// (`spawn_consumer_with_oplog` の 「並行の形にする前に登録した listener があれば、 通知係を起こす」)。
/// 記録の無い DB では届く。 記録のある DB では?
#[test]
fn t1_listener_added_before_concurrentize_with_a_record() {
    for with_log in [false, true] {
        let tmp = Tmp::new(if with_log { "t1-log" } else { "t1-nolog" });
        {
            let eng = Engine::create_concurrent_with_oplog(&tmp.0, 4 << 20).unwrap();
            eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
            if with_log {
                eng.enable_change_log(1 << 20).unwrap();
            }
            let e = eng.entity().unwrap();
            eng.tie_to_by_id(e, eng.himo_id("ts").unwrap() as u16, 1u64);
            eng.oplog_sync().unwrap();
        }
        let eng = Engine::open_standalone(&tmp.0).unwrap();
        let rec = Arc::new(Rec::default());
        eng.add_change_listener(rec.clone());
        let eng = Engine::concurrentize_with_oplog(eng, 4 << 20).unwrap();
        let ts = eng.himo_id("ts").unwrap() as u16;
        let e = eng.entity().unwrap();
        for v in 100..110u64 {
            eng.tie_to_by_id(e, ts, v);
        }
        eng.oplog_sync().unwrap();
        let got = wait_until(|| rec.values().iter().filter(|v| **v >= 100).count() == 10, 5);
        eprintln!(
            "T1 with_log={with_log}: listener_count={} got_all={got} values={:?} log_range={:?} detached={}",
            eng.change_listener_count(),
            rec.values(),
            eng.change_log_range(),
            rec.detached.load(Ordering::SeqCst),
        );
        drop(eng);
        eprintln!("T1 with_log={with_log}: after drop values={:?}", rec.values());
        if with_log {
            assert!(got, "記録のある DB で、 並行の形にする前に付けた listener に 1 件も届かない (知らせも無い)");
        } else {
            assert!(got, "記録の無い DB でも届かない");
        }
    }
}

/// T3: engine A の listener の中で engine B に書いて `B.oplog_sync()` を呼ぶ。 B の listener は `oplog_sync` が返った時点で
/// 受けているはず (「戻った時点で自分の commit を配り終えている」)。 `EMITTING` は thread ごと (engine ごとでない)。
#[test]
fn t3_oplog_sync_on_another_engine_inside_a_listener() {
    let (ta, tb) = (Tmp::new("t3-a"), Tmp::new("t3-b"));
    let a = Engine::create_concurrent_with_oplog(&ta.0, 4 << 20).unwrap();
    let b = Engine::create_concurrent_with_oplog(&tb.0, 4 << 20).unwrap();
    a.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
    b.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
    let b_rec = Arc::new(Rec::default());
    b.add_change_listener(b_rec.clone());

    struct Forward {
        b: Arc<Engine>,
        b_rec: Arc<Rec>,
        /// B.oplog_sync() が返った時点で B の listener が受けていた数 / 書いた数
        at_return: Mutex<Vec<(usize, usize)>>,
        wrote: AtomicUsize,
    }
    impl ChangeListener for Forward {
        fn on_changes(&self, recs: &[WireRecord]) {
            let ts = self.b.himo_id("ts").unwrap() as u16;
            let e = self.b.entity().unwrap();
            for r in recs {
                if let enchudb_oplog::oplog::DecodedOp::Tie { value, .. } = r.op {
                    self.b.tie_to_by_id(e, ts, value);
                    self.wrote.fetch_add(1, Ordering::SeqCst);
                }
            }
            self.b.oplog_sync().unwrap();
            self.at_return.lock().unwrap().push((self.b_rec.values().len(), self.wrote.load(Ordering::SeqCst)));
        }
    }
    let fwd = Arc::new(Forward { b: b.clone(), b_rec: b_rec.clone(), at_return: Mutex::new(Vec::new()), wrote: AtomicUsize::new(0) });
    a.add_change_listener(fwd.clone());

    // 対照: listener の外から B に書いて oplog_sync → 返った時点で届いている
    {
        let ts = b.himo_id("ts").unwrap() as u16;
        let e = b.entity().unwrap();
        b.tie_to_by_id(e, ts, 7u64);
        b.oplog_sync().unwrap();
        assert_eq!(b_rec.values(), vec![7], "対照: listener の外からの oplog_sync は返る前に配る");
    }

    let ts = a.himo_id("ts").unwrap() as u16;
    let e = a.entity().unwrap();
    for v in 100..105u64 {
        a.tie_to_by_id(e, ts, v);
    }
    a.oplog_sync().unwrap();
    assert!(wait_until(|| fwd.wrote.load(Ordering::SeqCst) == 5, 10));
    let at_return = fwd.at_return.lock().unwrap().clone();
    eprintln!("T3: (B の listener が受けていた数 (対照の 1 件込み), B に書いた数) at B.oplog_sync() return = {at_return:?}");
    let eventually = wait_until(|| b_rec.values().len() == 6, 10);
    eprintln!("T3: eventually delivered = {eventually}, values = {:?}", b_rec.values());
    let (got, wrote) = *at_return.last().unwrap();
    drop(fwd);
    assert_eq!(got, wrote + 1, "A の listener の中の B.oplog_sync() が、 B の commit を配らずに返った");
}

/// T4: 記録より大きい量を 1 回の書き出しの間に書くと、 速い (止まっていない) listener も外れるか。
#[test]
fn t4_a_burst_larger_than_the_record_detaches_an_idle_listener() {
    let tmp = Tmp::new("t4");
    let eng = Engine::create_concurrent_with_oplog(&tmp.0, 64 << 20).unwrap();
    eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
    eng.enable_change_log(1 << 20).unwrap();
    let rec = Arc::new(Rec::default());
    eng.add_change_listener(rec.clone());
    let ts = eng.himo_id("ts").unwrap() as u16;
    let e = eng.entity().unwrap();
    // 対照: 小さい量は届く
    eng.tie_to_by_id(e, ts, 1u64);
    eng.oplog_sync().unwrap();
    assert_eq!(rec.values(), vec![1]);
    // 1 回の書き出しの間に 4 万件 (記録 1 MiB より多い)
    let n = 40_000u64;
    for v in 0..n {
        eng.tie_to_by_id(e, ts, 1000 + v);
    }
    eng.oplog_sync().unwrap();
    wait_until(|| rec.values().len() as u64 == n + 1 || rec.detached.load(Ordering::SeqCst) > 0, 20);
    eprintln!(
        "T4: got={} of {} detached={} listener_count={} log_range={:?}",
        rec.values().len(),
        n + 1,
        rec.detached.load(Ordering::SeqCst),
        eng.change_listener_count(),
        eng.change_log_range()
    );
    assert_eq!(rec.detached.load(Ordering::SeqCst), 0, "1 度も遅れていない listener が外された");
}

/// T5: listener が 1 回 panic すると、 通知係の thread は終わる。 その後 別の listener (付け直しも) に consumer の書き出しの分が届くか。
#[test]
fn t5_a_panicking_listener_ends_the_notifier_for_everyone() {
    let tmp = Tmp::new("t5");
    let eng = Engine::create_concurrent_with_oplog(&tmp.0, 4 << 20).unwrap();
    eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
    struct Boom(AtomicBool, AtomicU64);
    impl ChangeListener for Boom {
        fn on_changes(&self, recs: &[WireRecord]) {
            self.1.fetch_add(recs.len() as u64, Ordering::SeqCst);
            if !self.0.swap(true, Ordering::SeqCst) {
                panic!("listener の panic (1 回だけ)");
            }
        }
    }
    let boom = Arc::new(Boom(AtomicBool::new(false), AtomicU64::new(0)));
    let rec = Arc::new(Rec::default());
    eng.add_change_listener(boom.clone());
    eng.add_change_listener(rec.clone());
    let ts = eng.himo_id("ts").unwrap() as u16;
    let e = eng.entity().unwrap();
    // oplog_sync を呼ばない書き手 (consumer の 100 ms の書き出し → 通知係)
    eng.tie_to_by_id(e, ts, 1u64);
    eng.oplog_commit();
    assert!(wait_until(|| boom.0.load(Ordering::SeqCst), 20), "通知係が 1 回目を配らない");
    std::thread::sleep(Duration::from_millis(300));
    let before = rec.values().len();
    for v in 10..20u64 {
        eng.tie_to_by_id(e, ts, v);
        eng.oplog_commit();
    }
    // 付け直した listener も
    let rec2 = Arc::new(Rec::default());
    eng.add_change_listener(rec2.clone());
    eng.tie_to_by_id(e, ts, 99u64);
    eng.oplog_commit();
    let ok = wait_until(|| rec.values().contains(&99) && rec2.values().contains(&99), 8);
    eprintln!(
        "T5: after panic: rec had {before}, now {:?}; rec2 {:?}; delivered_after_panic={ok}; backlog={}",
        rec.values(),
        rec2.values(),
        eng.change_backlog_len()
    );
    assert!(ok, "listener が 1 回 panic した後、 oplog_sync を呼ばない限りどの listener にも届かない");
}
