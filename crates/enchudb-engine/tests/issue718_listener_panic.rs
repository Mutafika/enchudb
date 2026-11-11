//! #718: listener が callback の中で 1 回 panic すると、 通知係の thread が終わり (handle は残るので起こし直されない)、 以後
//! `oplog_sync()` を呼ばないアプリではどの listener にも届かなかった。 panic した回の後ろの listener にも渡らなかった。
//!
//! 直した後: panic は捕まえ、 その listener だけを外して `on_detached` で知らせる。 同じ回の残りの listener にも、 その後の
//! 回にも、 後から付けた listener にも届く。
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
        let p = std::env::temp_dir().join(format!("enchudb-issue718-{tag}-{}", std::process::id()));
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

fn tie_value(r: &WireRecord) -> Option<u64> {
    match r.op {
        enchudb_oplog::oplog::DecodedOp::Tie { value, .. } => Some(value),
        _ => None,
    }
}

/// 受けた Tie の値と、 呼ばれた thread の名前を控える。
#[derive(Default)]
struct Rec {
    seen: Mutex<Vec<u64>>,
    threads: Mutex<Vec<String>>,
    detached: AtomicUsize,
}
impl Rec {
    fn values(&self) -> Vec<u64> {
        self.seen.lock().unwrap().clone()
    }
}
impl ChangeListener for Rec {
    fn on_changes(&self, recs: &[WireRecord]) {
        self.threads.lock().unwrap().push(std::thread::current().name().unwrap_or("").to_string());
        self.seen.lock().unwrap().extend(recs.iter().filter_map(tie_value));
    }
    fn on_detached(&self, _: enchudb_oplog::Hlc) {
        self.detached.fetch_add(1, Ordering::SeqCst);
    }
}

/// 最初の 1 回だけ panic する。 panic した回の record の HLC と、 `on_detached` の `lost_from` を控える。
#[derive(Default)]
struct Boom {
    fired: AtomicBool,
    /// panic した thread の名前
    fired_on: Mutex<Option<String>>,
    first_hlc: Mutex<Option<enchudb_oplog::Hlc>>,
    calls: AtomicUsize,
    detached: Mutex<Vec<enchudb_oplog::Hlc>>,
    /// `on_detached` の中でも panic する
    boom_in_detached: bool,
}
impl ChangeListener for Boom {
    fn on_changes(&self, recs: &[WireRecord]) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if !self.fired.swap(true, Ordering::SeqCst) {
            *self.first_hlc.lock().unwrap() = recs.first().map(|r| r.hlc);
            *self.fired_on.lock().unwrap() = Some(std::thread::current().name().unwrap_or("").to_string());
            panic!("listener の panic (試験、 1 回だけ)");
        }
    }
    fn on_detached(&self, from: enchudb_oplog::Hlc) {
        self.detached.lock().unwrap().push(from);
        if self.boom_in_detached {
            panic!("on_detached の panic (試験)");
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

/// `oplog_sync` を呼ばない書き手 (consumer の書き出し → 通知係が配る)。 `with_log` = 記録 (changefeed.seg) のある形。
fn notifier_survives(with_log: bool, boom_in_detached: bool) {
    let tmp = Tmp::new(&format!("notifier-{with_log}-{boom_in_detached}"));
    let eng = Engine::create_concurrent_with_oplog(&tmp.0, 4 << 20).unwrap();
    eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
    if with_log {
        eng.enable_change_log(1 << 20).unwrap();
    }
    // panic する listener を先に付ける (同じ回の後ろの listener = rec にも渡ることを見る)
    let boom = Arc::new(Boom { boom_in_detached, ..Default::default() });
    let rec = Arc::new(Rec::default());
    eng.add_change_listener(boom.clone());
    eng.add_change_listener(rec.clone());
    let ts = eng.himo_id("ts").unwrap() as u16;
    let e = eng.entity().unwrap();

    eng.tie_to_by_id(e, ts, 1u64);
    eng.oplog_commit();
    assert!(wait_until(|| boom.fired.load(Ordering::SeqCst), 60), "前提: 通知係が 1 回目を配る");
    // 前提: panic したのは通知係の thread の上 (oplog_sync の呼び手ではない)
    assert_eq!(boom.fired_on.lock().unwrap().as_deref(), Some("enchudb-notifier"), "前提: panic は通知係の thread で起きた");

    // panic した回の後ろの listener にも渡る
    assert!(wait_until(|| rec.values() == vec![1], 60), "panic した回の record が後ろの listener に渡らない: {:?}", rec.values());
    // panic した listener は外れ、 知らせが 1 回来る (lost_from = panic した回の最初の record)
    assert!(wait_until(|| boom.detached.lock().unwrap().len() == 1, 60), "panic した listener に on_detached が来ない");
    assert_eq!(boom.detached.lock().unwrap()[0], boom.first_hlc.lock().unwrap().unwrap());
    assert_eq!(eng.change_listener_count(), 1, "panic した listener だけが外れる");
    assert_eq!(eng.change_listeners_detached(), 1);

    // その後の回も、 後から付けた listener にも、 oplog_sync を呼ばずに届く
    for v in 10..20u64 {
        eng.tie_to_by_id(e, ts, v);
        eng.oplog_commit();
    }
    assert!(wait_until(|| rec.values().len() == 11, 60), "panic の後の record が届かない: {:?}", rec.values());
    let rec2 = Arc::new(Rec::default());
    eng.add_change_listener(rec2.clone());
    eng.tie_to_by_id(e, ts, 99u64);
    eng.oplog_commit();
    assert!(
        wait_until(|| rec.values().contains(&99) && rec2.values().contains(&99), 60),
        "listener が 1 回 panic した後、 どの listener にも届かない (rec {:?}、 後から付けた rec2 {:?}、 溜め {})",
        rec.values(),
        rec2.values(),
        eng.change_backlog_len()
    );
    let mut want: Vec<u64> = vec![1];
    want.extend(10..20);
    want.push(99);
    assert_eq!(rec.values(), want, "順に 1 回ずつ");
    assert_eq!(rec2.values(), vec![99]);
    // 配ったのは通知係 (この試験は oplog_sync を 1 度も呼んでいない)
    assert!(rec.threads.lock().unwrap().iter().all(|t| t == "enchudb-notifier"), "{:?}", rec.threads.lock().unwrap());
    assert_eq!(boom.calls.load(Ordering::SeqCst), 1, "外した listener をもう呼ばない");
    assert_eq!(rec.detached.load(Ordering::SeqCst), 0);

    // 閉じるのもすぐ (通知係は生きていて、 門で終わりを知らせる)
    let started = Instant::now();
    drop(eng);
    assert!(started.elapsed() < Duration::from_secs(4), "Drop に {:?}", started.elapsed());
}

#[test]
fn the_notifier_survives_a_panicking_listener() {
    notifier_survives(false, false);
}

#[test]
fn the_notifier_survives_a_panicking_listener_with_a_record() {
    notifier_survives(true, false);
}

/// `on_detached` の中でまた panic しても同じ。
#[test]
fn the_notifier_survives_a_panic_in_on_detached_too() {
    notifier_survives(false, true);
    notifier_survives(true, true);
}

/// `oplog_sync()` を呼んだ thread の上で panic した時: 呼び手には伝わらず、 同じ回の後ろの listener に渡ってから返る。
#[test]
fn oplog_sync_delivers_to_the_listeners_behind_a_panicking_one() {
    for with_log in [false, true] {
        let tmp = Tmp::new(&format!("sync-{with_log}"));
        let eng = Engine::create_concurrent_with_oplog(&tmp.0, 4 << 20).unwrap();
        eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
        if with_log {
            eng.enable_change_log(1 << 20).unwrap();
        }
        let boom = Arc::new(Boom::default());
        let rec = Arc::new(Rec::default());
        eng.add_change_listener(boom.clone());
        eng.add_change_listener(rec.clone());
        let ts = eng.himo_id("ts").unwrap() as u16;
        let e = eng.entity().unwrap();
        // 通知係より先にこの thread が配るまで繰り返す (どちらが配るかは決まらない。 panic がこの thread で起きた回だけを見る)
        let mut v = 0u64;
        while !boom.fired.load(Ordering::SeqCst) {
            v += 1;
            assert!(v < 1000, "前提: listener に 1 度も渡らない");
            eng.tie_to_by_id(e, ts, v);
            eng.oplog_sync().unwrap();
            wait_until(|| rec.values().len() as u64 == v, 60);
        }
        // どの thread で panic したにせよ、 oplog_sync は返り (ここに来ている)、 後ろの listener は全部を順に受けている
        assert!(wait_until(|| rec.values().len() as u64 == v, 60));
        assert_eq!(rec.values(), (1..=v).collect::<Vec<u64>>(), "with_log={with_log}");
        assert!(wait_until(|| boom.detached.lock().unwrap().len() == 1, 60), "panic した listener に on_detached が来ない");
        assert_eq!(eng.change_listener_count(), 1);
        // 次の回も返る前に届く (配信の lock / 配信中の印が panic で残っていない)
        eng.tie_to_by_id(e, ts, 5000u64);
        eng.oplog_sync().unwrap();
        assert!(wait_until(|| rec.values().last() == Some(&5000), 60), "with_log={with_log}: panic の後の oplog_sync の分が届かない");
        assert_eq!(boom.calls.load(Ordering::SeqCst), 1);
    }
}
