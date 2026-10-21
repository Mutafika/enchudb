//! changefeed の記録 (`changefeed.seg`、 `Engine::enable_change_log`)。 listener に渡す record を番号付きで残し、 開き直した後も
//! 番号の続きから受け直せる (`add_change_listener_from`)。
//!
//! 記録が無い時は、 listener の位置はメモリにしか無く、 開き直すと head から始まった。 畳んだ record は `audit` にも出ないので、
//! 落ちた consumer が処理した所の続きから受け直す手が無かった。

use enchudb_engine::change_log::Truncated;
use enchudb_engine::changefeed::ChangeListener;
use enchudb_engine::transport::WireRecord;
use enchudb_engine::{db_files, Engine, ValueType};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// 受け取った record の番号と、 Tie なら値を順に控える。 `gate` が立つまで最初の呼び出しで止まる。
struct Recorder {
    /// (番号, Tie の値)
    seen: Mutex<Vec<(u64, Option<u64>)>>,
    gate: AtomicBool,
    detached: Mutex<Option<enchudb_oplog::Hlc>>,
}

impl Recorder {
    fn new(open: bool) -> Arc<Self> {
        Arc::new(Self { seen: Mutex::new(Vec::new()), gate: AtomicBool::new(open), detached: Mutex::new(None) })
    }
    fn values(&self) -> Vec<u64> {
        self.seen.lock().unwrap().iter().filter_map(|(_, v)| *v).collect()
    }
    /// 値 `v` の Tie の番号。
    fn number_of(&self, v: u64) -> u64 {
        self.seen.lock().unwrap().iter().find(|(_, x)| *x == Some(v)).unwrap().0
    }
}

impl ChangeListener for Recorder {
    fn on_changes_at(&self, first: u64, recs: &[WireRecord]) {
        let until = Instant::now() + Duration::from_secs(30);
        while !self.gate.load(Ordering::Acquire) && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(1));
        }
        let mut s = self.seen.lock().unwrap();
        for (k, r) in recs.iter().enumerate() {
            let v = match r.op {
                enchudb_oplog::oplog::DecodedOp::Tie { value, .. } => Some(value),
                _ => None,
            };
            s.push((first + k as u64, v));
        }
    }

    fn on_detached(&self, lost_from: enchudb_oplog::Hlc) {
        *self.detached.lock().unwrap() = Some(lost_from);
    }
}

fn path(tag: &str) -> String {
    let p = format!("/tmp/enchudb-cf-log-{tag}-{}", std::process::id());
    let _ = db_files::remove_db(&p);
    p
}

fn create(p: &str, wal_cap: usize) -> Arc<Engine> {
    let eng: Arc<Engine> = Engine::create_concurrent_with_oplog(p, wal_cap).unwrap();
    eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
    eng
}

/// entity 1 つに `vals` を順に書き、 100 回ごとに commit する。 WAL を畳んだ回数を返す。
fn write(eng: &Engine, vals: std::ops::Range<u64>) -> usize {
    let ts = eng.himo_id("ts").unwrap() as u16;
    let a = eng.entity().unwrap();
    let wal = eng.oplog().unwrap().clone();
    let (mut folds, mut last) = (0usize, wal.head());
    for v in vals {
        eng.tie_to_by_id(a, ts, v);
        if v % 100 == 99 {
            eng.oplog_commit();
        }
        let h = wal.head();
        if h < last {
            folds += 1;
        }
        last = h;
    }
    eng.oplog_commit();
    folds
}

fn wait_for(rec: &Recorder, n: usize) -> Vec<u64> {
    let until = Instant::now() + Duration::from_secs(60);
    while rec.values().len() < n && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(10));
    }
    rec.values()
}

/// 番号が `from` から 1 つずつ続いているか。
fn assert_dense(rec: &Recorder, from: u64) {
    let s = rec.seen.lock().unwrap();
    for (k, (no, _)) in s.iter().enumerate() {
        assert_eq!(*no, from + k as u64, "{k} 件目の番号が飛んだ / 重なった");
    }
}

/// 処理した所 (値 20000 の番号) の続きから、 開き直した後に受け直す。 間に WAL を何回も畳み、 開き直した後の書き込みもある。
/// 番号は開き直しても同じ record に同じ番号。
#[test]
fn a_listener_resumes_after_reopen_from_the_number_it_stopped_at() {
    let p = path("resume");
    let (resume_at, before) = {
        let eng = create(&p, 256 << 10);
        eng.enable_change_log(8 << 20).unwrap();
        let rec = Recorder::new(true);
        eng.add_change_listener(rec.clone());
        let folds = write(&eng, 0..30_000);
        assert!(folds >= 5, "前提: 書いている間に畳む ({folds} 回)");
        eng.oplog_sync().unwrap();
        assert_eq!(wait_for(&rec, 30_000), (0..30_000).collect::<Vec<_>>());
        assert_dense(&rec, 0);
        let at = rec.number_of(20_000);
        let before: Vec<(u64, Option<u64>)> = rec.seen.lock().unwrap().iter().filter(|(no, _)| *no >= at).cloned().collect();
        (at, before)
    };
    let eng = Engine::open_concurrent_with_oplog(&p, 256 << 10).unwrap();
    write(&eng, 30_000..40_000);
    let rec = Recorder::new(true);
    eng.add_change_listener_from(rec.clone(), resume_at).unwrap();
    eng.oplog_sync().unwrap();
    let got = wait_for(&rec, 20_000);
    drop(eng);
    let _ = db_files::remove_db(&p);
    assert_eq!(got, (20_000..40_000).collect::<Vec<_>>(), "続きから順に 1 回ずつ届いていない");
    assert_dense(&rec, resume_at);
    let after = rec.seen.lock().unwrap().clone();
    assert_eq!(&after[..before.len()], &before[..], "開き直す前と後で、 同じ番号が別の record を指す");
}

/// listener が居ない間に WAL を畳んでも、 記録には全部残り、 0 番から受けられる (畳む前に記録へ写す)。
#[test]
fn records_folded_out_of_the_wal_before_any_listener_are_kept() {
    let p = path("folded");
    let eng = create(&p, 256 << 10);
    eng.enable_change_log(16 << 20).unwrap();
    let folds = write(&eng, 0..30_000);
    eng.oplog_sync().unwrap();
    let rec = Recorder::new(true);
    eng.add_change_listener_from(rec.clone(), 0).unwrap();
    let got = wait_for(&rec, 30_000);
    drop(eng);
    let _ = db_files::remove_db(&p);
    assert!(folds >= 5, "前提: listener の居ない間に畳む ({folds} 回)");
    assert_eq!(got, (0..30_000).collect::<Vec<_>>());
    assert_dense(&rec, 0);
}

/// 記録が一周して捨てた番号からは受け直せない (黙って先から渡さない)。 残っている最古の番号からは受けられる。
#[test]
fn resuming_from_a_dropped_number_is_truncated() {
    let p = path("truncated");
    let eng = create(&p, 256 << 10);
    eng.enable_change_log(1 << 20).unwrap();
    write(&eng, 0..30_000);
    eng.oplog_sync().unwrap();
    let (first, next) = eng.change_log_range().unwrap();
    assert!(first > 0, "前提: 記録が一周して捨てた (range {first}..{next})");
    let rec = Recorder::new(true);
    assert_eq!(eng.add_change_listener_from(rec.clone(), 0), Err(Truncated { from: first }));
    assert_eq!(eng.add_change_listener_from(rec.clone(), next + 1), Err(Truncated { from: next }));
    eng.add_change_listener_from(rec.clone(), first).unwrap();
    eng.oplog_sync().unwrap();
    let until = Instant::now() + Duration::from_secs(30);
    while rec.seen.lock().unwrap().len() < (next - first) as usize && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(10));
    }
    drop(eng);
    let _ = db_files::remove_db(&p);
    assert_dense(&rec, first);
    assert_eq!(rec.values().last(), Some(&29_999));
}

/// 止まった listener の位置を記録が捨てたら、 その listener を外して `on_detached` で知らせる。 書き手は止まらず、 外れた
/// listener は残っている所から付け直せる。
#[test]
fn a_stalled_listener_overrun_by_the_log_is_detached() {
    let p = path("stalled");
    let eng = create(&p, 256 << 10);
    eng.enable_change_log(1 << 20).unwrap();
    let rec = Recorder::new(false);
    eng.add_change_listener(rec.clone());
    write(&eng, 0..100);
    eng.oplog_sync().unwrap(); // 通知係を最初の呼び出しで止める (5 秒で諦めて返る)
    let started = Instant::now();
    write(&eng, 100..40_000);
    let took = started.elapsed();
    rec.gate.store(true, Ordering::Release);
    eng.oplog_sync().unwrap();
    let until = Instant::now() + Duration::from_secs(30);
    while rec.detached.lock().unwrap().is_none() && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(10));
    }
    let detached = *rec.detached.lock().unwrap();
    let count = eng.change_listeners_detached();
    let (first, _) = eng.change_log_range().unwrap();
    let again = Recorder::new(true);
    let resumed = eng.add_change_listener_from(again, first);
    drop(eng);
    let _ = db_files::remove_db(&p);
    assert!(took < Duration::from_secs(3), "書き手が止まった listener を待った ({took:?})");
    let lost_from = detached.expect("外したことを知らせていない");
    // 渡せなかった最初の record の HLC (位置を記録が捨てる時に分かる。 0 は 「分からない」 で、 外す時に控えていない)
    assert!(lost_from.wall > 0, "渡せなかった最初の record の HLC が無い: {lost_from:?}");
    assert_eq!(count, 1);
    assert_eq!(resumed, Ok(()));
}

/// 記録に番号を振るのは書き出した record だけ (WAL の checkpoint まで)。 書き出す前の record に番号を振ると、 電源が落ちた時に
/// 記録にだけ残り (本体には無い)、 開き直した後の番号もずれる (電源断の像の試験は `changefeed_log_power_loss.rs`)。
///
/// 1 回ごとに、 書き出して記録に写し (`oplog_sync`)、 書き出さずに 200 件足してから記録へ写させ、 最後に記録した record が WAL の
/// 中で checkpoint より前に終わるかを見る。 間に WAL が畳まれた回 (記録した record が WAL に無い。 畳むのは全部を書き出した後) は
/// 確かめられないので、 確かめた回が 3 つになるまで回す。 混んだ機械では consumer が書き手に追いつくたびに畳むので、 畳まれる回が
/// 多い (前は 10 回で打ち切り、 12 本並べると 48 回中 26 回が 1 度も確かめられずに落ちた)。
#[test]
fn records_not_yet_written_out_get_no_number() {
    use enchudb_engine::change_log::{fnv1a64, ChangeLog};
    use enchudb_oplog::oplog::HEADER_SIZE;
    let p = path("durable");
    let eng = create(&p, 64 << 20);
    eng.enable_change_log(8 << 20).unwrap();
    let wal = eng.oplog().unwrap().clone();
    let ts = eng.himo_id("ts").unwrap() as u16;
    let a = eng.entity().unwrap();
    let (mut over, mut checked, mut rounds, mut v) = (Vec::new(), 0, 0u64, 0u64);
    let until = Instant::now() + Duration::from_secs(60);
    while checked < 3 && Instant::now() < until {
        rounds += 1;
        // ここまでを書き出して記録に写す
        eng.oplog_sync().unwrap();
        for _ in 0..200 {
            eng.tie_to_by_id(a, ts, v);
            v += 1;
            if v % 50 == 0 {
                eng.oplog_commit();
            }
        }
        // 書き出した分だけ写すはず (書き出す前の 200 件は、 consumer が間に書き出していなければ写さない)
        eng.change_log_range().unwrap();
        // 写した後に読んだ checkpoint (写した時の checkpoint 以上)
        let cp = wal.checkpoint();
        let Some((hlc, id)) = ChangeLog::open(std::path::Path::new(&p), 0, false, true).unwrap().last_written() else { continue };
        let (recs, _) = wal.scan_for_change_log(HEADER_SIZE as u64, u64::MAX, true);
        if let Some((_, end)) = recs.iter().find(|(r, _)| r.hlc == hlc && fnv1a64(&r.signed_bytes) == id) {
            checked += 1;
            if *end > cp {
                over.push((rounds, *end, cp));
            }
        }
    }
    drop(eng);
    let _ = db_files::remove_db(&p);
    assert!(over.is_empty(), "書き出す前の record に番号を振った (回, 最後に記録した record の終端, checkpoint): {over:?}");
    assert!(checked >= 3, "前提: 最後に記録した record を WAL の中で 3 回確かめた ({rounds} 回で {checked} 回)");
}

/// `oplog_sync()` は返る前に、 自分の commit までを listener に渡す (#337。 記録がある時も、 畳むのを待たずに記録へ写して渡す)。
#[test]
fn oplog_sync_hands_its_records_over_before_returning() {
    let p = path("sync");
    let eng = create(&p, 64 << 20);
    eng.enable_change_log(8 << 20).unwrap();
    let rec = Recorder::new(true);
    eng.add_change_listener(rec.clone());
    let ts = eng.himo_id("ts").unwrap() as u16;
    let a = eng.entity().unwrap();
    let mut missing = Vec::new();
    for round in 0..20u64 {
        for v in round * 100..round * 100 + 100 {
            eng.tie_to_by_id(a, ts, v);
        }
        eng.oplog_commit();
        eng.oplog_sync().unwrap();
        let got = rec.values();
        if got.last() != Some(&(round * 100 + 99)) {
            missing.push((round, got.last().copied()));
        }
    }
    drop(eng);
    let _ = db_files::remove_db(&p);
    assert!(missing.is_empty(), "oplog_sync が返った時に渡っていない (round, 受けた最後): {missing:?}");
}

/// listener を付けた後に記録を作ることはできない (付いた listener の位置が記録に無い)。 記録の無い engine の受け直しは Truncated。
#[test]
fn enable_after_a_listener_is_refused_and_resume_without_a_log_is_truncated() {
    let p = path("order");
    let eng = create(&p, 256 << 10);
    let rec = Recorder::new(true);
    assert!(matches!(eng.add_change_listener_from(rec.clone(), 0), Err(Truncated { .. })));
    eng.add_change_listener(rec);
    let e = eng.enable_change_log(1 << 20).unwrap_err();
    drop(eng);
    let _ = db_files::remove_db(&p);
    assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput);
}

