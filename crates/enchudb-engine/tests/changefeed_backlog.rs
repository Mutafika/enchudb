//! 通知の受け手 (change listener) が遅くても、 WAL を畳むのを待たせない (#496 の後の P3)。
//!
//! 0.34 まで: 受け手に配り終えるまで WAL を畳まず (#337)、 受け手の callback は WAL を書き出す裏の thread (consumer)
//! が呼んでいた。 受け手が遅い・止まると畳めず、 WAL が満杯になって書き手が待たされ (最大 5 秒)、 それでも空かなければ
//! 記録が落ちた。
//!
//! 畳む時に、 まだ受け手に渡していない記録を溜め (backlog) に移す。 受け手には溜めから先に、 順に 1 回ずつ渡す。

use enchudb_engine::changefeed::ChangeListener;
use enchudb_engine::transport::WireRecord;
use enchudb_engine::{db_files, Engine, ValueType};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// 受け取った Tie の値を順に控える受け手。 1 回の呼び出しごとに `delay` 寝る。 `gate` が立つまで最初の呼び出しで止まる。
struct Recorder {
    seen: Mutex<Vec<u64>>,
    calls: AtomicUsize,
    delay: Duration,
    gate: AtomicBool,
    /// `on_detached` で受け取った HLC (#560)
    detached: Mutex<Option<enchudb_oplog::Hlc>>,
}

impl Recorder {
    fn new(delay: Duration, open: bool) -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
            delay,
            gate: AtomicBool::new(open),
            detached: Mutex::new(None),
        })
    }
    fn len(&self) -> usize {
        self.seen.lock().unwrap().len()
    }
}

impl ChangeListener for Recorder {
    fn on_changes(&self, recs: &[WireRecord]) {
        // 止める受け手: test が門を開けるまで (最大 30 秒) 返らない
        let until = Instant::now() + Duration::from_secs(30);
        while !self.gate.load(Ordering::Acquire) && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(1));
        }
        self.calls.fetch_add(1, Ordering::Relaxed);
        std::thread::sleep(self.delay);
        let mut s = self.seen.lock().unwrap();
        for r in recs {
            if let enchudb_oplog::oplog::DecodedOp::Tie { value, .. } = r.op {
                s.push(value);
            }
        }
    }

    fn on_detached(&self, lost_from: enchudb_oplog::Hlc) {
        *self.detached.lock().unwrap() = Some(lost_from);
    }
}

/// 1 本の書き手が entity 1 つに 0, 1, 2, … を順に書き、 100 回ごとに commit する。 書いた後の WAL の head が前より下がった
/// 回数 (= 畳んだ回数) と、 書くのにかかった時間を返す。
fn write_in_order(eng: &Engine, n: u64) -> (usize, Duration) {
    let ts = eng.himo_id("ts").unwrap() as u16;
    let a = eng.entity().unwrap();
    let wal = eng.oplog().unwrap().clone();
    let (mut folds, mut last) = (0usize, wal.head());
    let started = Instant::now();
    for i in 0..n {
        eng.tie_to_by_id(a, ts, i);
        if i % 100 == 99 {
            eng.oplog_commit();
        }
        let h = wal.head();
        if h < last {
            folds += 1;
        }
        last = h;
    }
    eng.oplog_commit();
    (folds, started.elapsed())
}

fn engine(tag: &str, wal_cap: usize) -> (String, Arc<Engine>) {
    let p = format!("/tmp/enchudb-cf-backlog-{tag}-{}", std::process::id());
    let _ = db_files::remove_db(&p);
    let eng: Arc<Engine> = Engine::create_concurrent_with_oplog(&p, wal_cap).unwrap();
    eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
    (p, eng)
}

/// 受け手が `n` 件受け取るまで待ち (最大 `limit`)、 受け取った値の列を返す。
fn wait_for(rec: &Recorder, n: usize, limit: Duration) -> Vec<u64> {
    let until = Instant::now() + limit;
    while rec.len() < n && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(10));
    }
    rec.seen.lock().unwrap().clone()
}

/// 受け取った列が 0..n をちょうど 1 回ずつ順に並べたものか。 違えば最初に食い違った所を返す。
fn first_mismatch(got: &[u64], n: u64) -> Option<String> {
    if let Some(i) = (0..got.len().min(n as usize)).find(|&i| got[i] != i as u64) {
        return Some(format!("{i} 番目が {} (前後 {:?})", got[i], &got[i.saturating_sub(2)..(i + 3).min(got.len())]));
    }
    (got.len() != n as usize).then(|| format!("{} 件 (書いたのは {n} 件)", got.len()))
}

/// 1 回 5 ms の遅い受け手が居ても、 20 回以上畳む間に書いた記録が全部、 順に 1 回ずつ届き、 落ちる記録も無い。
#[test]
fn slow_listener_gets_every_record_in_order_once_across_folds() {
    let (p, eng) = engine("slow", 256 << 10);
    let rec = Recorder::new(Duration::from_millis(5), true);
    eng.add_change_listener(rec.clone());
    const N: u64 = 200_000;
    let (folds, took) = write_in_order(&eng, N);
    let dropped = eng.wal_dropped_records();
    eng.oplog_sync().unwrap();
    let got = wait_for(&rec, N as usize, Duration::from_secs(60));
    drop(eng);
    let _ = db_files::remove_db(&p);
    let seen = format!("畳んだ {folds} 回、 書くのに {took:?}、 落ちた記録 {dropped}、 届いた {} 件", got.len());
    eprintln!("{seen}");
    assert!(folds >= 20, "前提: 書いている間に 20 回以上畳む ({seen})");
    assert_eq!(dropped, 0, "WAL が満杯で落ちた記録 ({seen})");
    if let Some(m) = first_mismatch(&got, N) {
        panic!("受け手に順に 1 回ずつ届いていない: {m} ({seen})");
    }
}

/// 受け手が止まっていても、 書き手は WAL を何回も畳みながら書き終わり (待たされない)、 記録は落ちない。 受け手が戻ると
/// 全部が順に 1 回ずつ届く。
#[test]
fn stalled_listener_does_not_stall_writers() {
    let (p, eng) = engine("stalled", 256 << 10);
    let rec = Recorder::new(Duration::ZERO, false);
    eng.add_change_listener(rec.clone());
    const N: u64 = 50_000;
    let (folds, took) = write_in_order(&eng, N);
    let dropped = eng.wal_dropped_records();
    let calls_while_writing = rec.calls.load(Ordering::Relaxed);
    rec.gate.store(true, Ordering::Release);
    eng.oplog_sync().unwrap();
    let got = wait_for(&rec, N as usize, Duration::from_secs(60));
    drop(eng);
    let _ = db_files::remove_db(&p);
    let seen = format!(
        "畳んだ {folds} 回、 書くのに {took:?}、 落ちた記録 {dropped}、 書いている間の受け手の呼び出し {calls_while_writing} 回、 届いた {} 件",
        got.len()
    );
    eprintln!("{seen}");
    assert_eq!(calls_while_writing, 0, "前提: 書いている間、 受け手は止まっていた ({seen})");
    assert!(folds >= 5, "前提: 書いている間に 5 回以上畳む ({seen})");
    assert_eq!(dropped, 0, "受け手が止まっている間に WAL が満杯で落ちた記録 ({seen})");
    // 満杯の書き手が空きを待つのは最大 5 秒。 1 回でも待たされれば越える
    assert!(took < Duration::from_secs(3), "書き手が止まっている受け手を待った ({seen})");
    if let Some(m) = first_mismatch(&got, N) {
        panic!("受け手に順に 1 回ずつ届いていない: {m} ({seen})");
    }
}

/// #560: 止まった受け手の溜めが上限を越えたら、 溜めを捨てて受け手を外す (書き手は止めない、 記録も落とさない)。 受け手が戻ると
/// `on_detached` で、 渡せなかった最初の記録の HLC を受け取る。 外した後は溜めない。 前は上限が無く、 止まった受け手の間は畳むたびに
/// ring の全部を写して溜め続けた。
#[test]
fn stalled_listener_is_detached_when_the_backlog_overflows() {
    let (p, eng) = engine("overflow", 256 << 10);
    // 1 回の畳みで移す分 (WAL 256 KiB 分の record、 溜めでは 1 つ約 2.5 倍) より大きく、 2 回分より小さい = 溜まって越える
    eng.set_change_backlog_limit(1 << 20);
    let rec = Recorder::new(Duration::ZERO, false);
    eng.add_change_listener(rec.clone());
    const N: u64 = 50_000;
    let (folds, took) = write_in_order(&eng, N);
    let dropped = eng.wal_dropped_records();
    let (detached, listeners, backlog) = (eng.change_listeners_detached(), eng.change_listener_count(), eng.change_backlog_len());
    rec.gate.store(true, Ordering::Release);
    let until = Instant::now() + Duration::from_secs(30);
    while rec.detached.lock().unwrap().is_none() && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(10));
    }
    let lost_from = *rec.detached.lock().unwrap();
    let seen = rec.len();
    drop(eng);
    let _ = db_files::remove_db(&p);
    let what = format!("畳んだ {folds} 回、 書くのに {took:?}、 落ちた記録 {dropped}、 外した {detached}、 受け手 {listeners}、 溜め {backlog}");
    assert!(folds >= 5, "前提: 書いている間に 5 回以上畳む ({what})");
    assert_eq!(detached, 1, "溜めが上限を越えても受け手を外さない ({what})");
    assert_eq!(listeners, 0, "外した受け手が残っている ({what})");
    assert_eq!(backlog, 0, "外した後も溜めている ({what})");
    assert_eq!(dropped, 0, "記録が落ちた ({what})");
    assert!(took < Duration::from_secs(3), "書き手が止まっている受け手を待った ({what})");
    assert!(lost_from.is_some(), "受け手が on_detached を受け取らない ({what})");
    assert!((seen as u64) < N, "外した受け手に全部届いた ({what}、 届いた {seen})");
}

/// #560 の対照: すぐ受け取る受け手は、 上限が WAL の 2 倍でも、 何度畳んでも外されない (溜めの大きさは配った分だけ減る)。
#[test]
fn prompt_listener_is_not_detached_under_a_small_limit() {
    let (p, eng) = engine("prompt", 256 << 10);
    eng.set_change_backlog_limit(512 << 10);
    let rec = Recorder::new(Duration::ZERO, true);
    eng.add_change_listener(rec.clone());
    const N: u64 = 200_000;
    let (folds, _) = write_in_order(&eng, N);
    eng.oplog_sync().unwrap();
    let got = wait_for(&rec, N as usize, Duration::from_secs(60));
    let detached = eng.change_listeners_detached();
    drop(eng);
    let _ = db_files::remove_db(&p);
    assert!(folds >= 20, "前提: 書いている間に 20 回以上畳む (畳んだ {folds} 回)");
    assert_eq!(detached, 0, "すぐ受け取る受け手を外した (畳んだ {folds} 回)");
    if let Some(m) = first_mismatch(&got, N) {
        panic!("受け手に順に 1 回ずつ届いていない: {m}");
    }
}
