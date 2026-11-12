//! #716: 1 回の書き出しの間に記録 (`changefeed.seg`) の大きさより多く書くと、 追いついている listener も外れた。
//!
//! 記録へ写すのは書き出した分を 1 回で全部、 listener が読むのはその後だったので、 1 回の写しが記録を一周すると listener の
//! 位置ごと捨てた。 直した後: 配る道 (通知係 / `oplog_sync`) は、 読み手を越えない所まで写す → 渡す → 続き、 を繰り返す。
//! consumer の周期の先写しと `change_log_range` も、 残す量が上限 (WAL の 1/32) 以内なら読み手を越えない所で止める。
//! 区画を畳む時の写しは今まで通り全部 (書き手を待たせない) なので、 この試験は畳まない大きさの WAL で回す。
#![cfg(not(target_arch = "wasm32"))]

use enchudb_engine::changefeed::ChangeListener;
use enchudb_engine::transport::WireRecord;
use enchudb_engine::{db_files, Engine, ValueType};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct Tmp(String);
impl Tmp {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchudb-issue716-{tag}-{}", std::process::id()));
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

/// 受けた (番号, Tie の値)。 `gate` が閉じている間は callback の中で待つ (遅い listener)。
struct Rec {
    seen: Mutex<Vec<(u64, u64)>>,
    detached: AtomicUsize,
    gate: AtomicBool,
    entered: AtomicBool,
}
impl Rec {
    fn new(open: bool) -> Arc<Self> {
        Arc::new(Self {
            seen: Mutex::new(Vec::new()),
            detached: AtomicUsize::new(0),
            gate: AtomicBool::new(open),
            entered: AtomicBool::new(false),
        })
    }
    fn values(&self) -> Vec<u64> {
        self.seen.lock().unwrap().iter().map(|(_, v)| *v).collect()
    }
}
impl ChangeListener for Rec {
    fn on_changes_at(&self, first: u64, recs: &[WireRecord]) {
        self.entered.store(true, Ordering::Release);
        let until = Instant::now() + Duration::from_secs(120);
        while !self.gate.load(Ordering::Acquire) && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(2));
        }
        let mut s = self.seen.lock().unwrap();
        for (k, r) in recs.iter().enumerate() {
            if let enchudb_oplog::oplog::DecodedOp::Tie { value, .. } = r.op {
                s.push((first + k as u64, value));
            }
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

/// WAL 256 MiB (畳まない。 急がない写しが残してよい量 = 8 MiB)、 記録 1 MiB (数値の紐の書き込みで約 5000 件)。
const WAL: usize = 256 << 20;
const LOG: usize = 1 << 20;
/// 1 回の burst: 3 万件 (約 6 MiB = 記録の約 6 倍。 残してよい量 8 MiB より小さい)。
const BURST: u64 = 30_000;

fn engine(tag: &str) -> (Tmp, Arc<Engine>) {
    let tmp = Tmp::new(tag);
    let eng = Engine::create_concurrent_with_oplog(&tmp.0, WAL).unwrap();
    eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
    eng.enable_change_log(LOG).unwrap();
    (tmp, eng)
}

/// `poll_range` = 書いている間、 別の thread が `change_log_range()` を呼び続ける (これも記録へ写す道)。
fn burst_keeps_a_caught_up_listener(tag: &str, poll_range: bool) {
    let (_tmp, eng) = engine(tag);
    let rec = Rec::new(true);
    eng.add_change_listener(rec.clone());
    let ts = eng.himo_id("ts").unwrap() as u16;
    let e = eng.entity().unwrap();
    // 対照: 小さい量は届く (= listener は追いついている)
    eng.tie_to_by_id(e, ts, 1u64);
    eng.oplog_sync().unwrap();
    assert_eq!(rec.values(), vec![1]);
    let attached_at = rec.seen.lock().unwrap()[0].0;

    let done = Arc::new(AtomicBool::new(false));
    let poller = poll_range.then(|| {
        let (eng, done) = (eng.clone(), done.clone());
        std::thread::spawn(move || {
            let mut calls = 0u64;
            while !done.load(Ordering::Acquire) {
                let _ = eng.change_log_range();
                calls += 1;
                std::thread::sleep(Duration::from_micros(200));
            }
            calls
        })
    });
    // 1 回の書き出しの間に、 記録より多く書く。 3 回 (記録が何周もする)
    let mut want = vec![1u64];
    for round in 0..3u64 {
        for v in 0..BURST {
            let value = 1_000_000 * (round + 1) + v;
            eng.tie_to_by_id(e, ts, value);
            want.push(value);
        }
        eng.oplog_sync().unwrap();
        // `oplog_sync` は、 通知係が配っている最中ならそれを待ってから残りを配って返る。 返った時点で全部届いている
        assert_eq!(rec.detached.load(Ordering::SeqCst), 0, "{round} 回目: 1 度も遅れていない listener が外された");
        assert!(
            wait_until(|| rec.values().len() == want.len() || rec.detached.load(Ordering::SeqCst) > 0, 120),
            "{round} 回目: 届かない ({} / {})",
            rec.values().len(),
            want.len()
        );
    }
    done.store(true, Ordering::Release);
    if let Some(p) = poller {
        assert!(p.join().unwrap() > 0, "前提: 書いている間に change_log_range を呼んだ");
    }
    assert_eq!(rec.detached.load(Ordering::SeqCst), 0, "1 度も遅れていない listener が外された");
    assert_eq!(eng.change_listener_count(), 1);
    assert_eq!(eng.change_listeners_detached(), 0);
    // 順に 1 回ずつ、 番号は続き
    let seen = rec.seen.lock().unwrap().clone();
    assert_eq!(seen.iter().map(|(_, v)| *v).collect::<Vec<_>>(), want);
    assert!(seen.windows(2).all(|w| w[1].0 == w[0].0 + 1), "番号が続いていない");
    // 前提: 1 回の burst は記録より大きい (記録は一周していて、 残っているのは burst 1 回分より少ない)
    let (first, next) = eng.change_log_range().unwrap();
    assert_eq!(next, seen.last().unwrap().0 + 1);
    eprintln!("記録に残る数 {} (1 件 約 {} B)", next - first, LOG as u64 / (next - first));
    assert!(first > attached_at, "前提: 記録が一周した (最古 {first}、 付けた位置 {attached_at})");
    assert!(next - first < BURST, "前提: 1 回の burst ({BURST} 件) が記録に残る数 ({}) より多い", next - first);
}

#[test]
fn a_burst_larger_than_the_record_keeps_a_caught_up_listener() {
    burst_keeps_a_caught_up_listener("burst", false);
}

#[test]
fn change_log_range_during_a_burst_keeps_a_caught_up_listener() {
    burst_keeps_a_caught_up_listener("range", true);
}

/// 本当に遅い listener (callback から返らない) は今まで通り外れる: 残す量が上限を越えたら、 読み手を待たずに全部写す。
#[test]
fn a_listener_that_does_not_return_is_still_detached() {
    let (_tmp, eng) = engine("stuck");
    let rec = Rec::new(false);
    eng.add_change_listener(rec.clone());
    let ts = eng.himo_id("ts").unwrap() as u16;
    let e = eng.entity().unwrap();
    eng.tie_to_by_id(e, ts, 1u64);
    eng.oplog_commit();
    assert!(wait_until(|| rec.entered.load(Ordering::Acquire), 60), "前提: listener が callback の中で止まっている");
    // 残してよい量 (WAL の 1/32 = 8 MiB) + 記録 (1 MiB) より多く書く。 `oplog_sync` は止まった配信を 5 秒待つので、
    // consumer の周期の書き出しに任せる
    let mut wrote = 0u64;
    let detached = wait_until(
        || {
            for _ in 0..5_000 {
                wrote += 1;
                eng.tie_to_by_id(e, ts, 1000 + wrote);
            }
            eng.oplog_commit();
            eng.change_listeners_detached() > 0 || wrote >= 400_000
        },
        120,
    ) && eng.change_listeners_detached() > 0;
    rec.gate.store(true, Ordering::Release);
    assert!(detached, "返らない listener が、 {wrote} 件 (記録 + 残してよい量より多い) 書いても外れない");
    // 1 件 約 208 B: 残してよい量 8 MiB は約 4 万件。 それより前に外れたなら、 読み手を待たずに写した回がある
    assert!(wrote > 40_000, "返らない listener が、 残してよい量に届く前 ({wrote} 件) に外れた");
    assert_eq!(eng.change_listener_count(), 0);
    assert!(wait_until(|| rec.detached.load(Ordering::SeqCst) == 1, 60), "on_detached が来ない");
}
