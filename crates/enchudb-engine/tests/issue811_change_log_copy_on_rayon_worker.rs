//! #811: 記録 (changefeed.seg) のある DB で、 rayon の pool の worker の上から `oplog_sync()` を呼んでも、 記録への写しを待つ間に
//! その pool の他の job を同じ stack で走らせないこと。
//!
//! 記録への写し (`ChangeLog::write_entries`) は 4096 件以上の回を専用の pool (#700) に頼む。 前は `pool.install(..)` で頼んでいた:
//! 呼んだ thread が別の pool の worker だと、 rayon は待つ間にその worker の pool の他の job を **同じ stack で** 実行する
//! (`in_worker_cross`)。 写しは `change_emit` (std の Mutex) を握ったまま走るので、 拾った job が `change_log_range()` /
//! `add_change_listener*()` などを呼ぶと、 同じ thread が `change_emit` を取り直して止まる (握ったままなので、 WAL が埋まると
//! consumer も `change_emit` を待って、 書き手は 5 秒で `WalSpace`)。
//!
//! 形: worker 1 本の pool の上の job A が、 1 回に 8192 件書いて、 同じ pool に job B (`change_log_range()` を呼ぶ) を積んでから
//! `oplog_sync()` を呼ぶ。 これを ROUNDS 回。 判定は状態で: B が走った時に A が `oplog_sync()` の中に居たか (居たら、 待つ間に拾って
//! いる) と、 全部が上限の時間の中で返ること。 A の `oplog_sync()` が 4096 件以上を自分で写すかは consumer の周期 (100 ms) との
//! 成り行き (先に consumer が書き出すと、 写すのは通知係) なので ROUNDS 回やる。 検出率: 直しを外して 5/5 で止まる (1 回目の
//! round で止まった、 Mac の手元)。
#![cfg(not(target_arch = "wasm32"))]

use enchudb_engine::{db_files, Engine, ValueType};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::time::Duration;

const ROUNDS: u64 = 10;
const BURST: u64 = 8192;

struct Tmp(String);
impl Tmp {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchudb-issue811-{tag}-{}", std::process::id())).to_string_lossy().into_owned();
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

#[test]
fn oplog_sync_on_a_rayon_worker_does_not_run_that_pools_jobs_under_the_change_lock() {
    let tmp = Tmp::new("sync");
    let eng: Arc<Engine> = Engine::create_concurrent_with_oplog(&tmp.0, 64 << 20).unwrap();
    eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
    eng.enable_change_log(64 << 20).unwrap();
    // アプリの pool (worker 1 本: A が待つ間に B を走らせられるのは A の thread だけ)
    let app = rayon::ThreadPoolBuilder::new().num_threads(1).build().unwrap();
    let in_sync = Arc::new(AtomicBool::new(false));
    let stolen = Arc::new(AtomicU64::new(0));
    let (done_tx, done_rx) = mpsc::channel::<&'static str>();
    {
        let (eng, in_sync, stolen) = (eng.clone(), in_sync.clone(), stolen.clone());
        app.spawn(move || {
            let ts = eng.himo_id("ts").unwrap() as u16;
            let e = eng.entity().unwrap();
            for round in 0..ROUNDS {
                for i in 0..BURST {
                    eng.try_tie_to_by_id(e, ts, round << 32 | i).unwrap();
                    if i % 64 == 63 {
                        eng.oplog_commit();
                    }
                }
                // 同じ pool に積む (この worker の deque に入る)
                let (eng2, in_sync2, stolen2, tx) = (eng.clone(), in_sync.clone(), stolen.clone(), done_tx.clone());
                rayon::spawn(move || {
                    if in_sync2.load(Ordering::Acquire) {
                        stolen2.fetch_add(1, Ordering::Relaxed);
                    }
                    let _ = eng2.change_log_range();
                    let _ = tx.send("range");
                });
                in_sync.store(true, Ordering::Release);
                eng.oplog_sync().unwrap();
                in_sync.store(false, Ordering::Release);
            }
            let _ = done_tx.send("writer");
        });
    }
    // 止まる形を見るので、 上限つきで待つ (止まった thread は process の終わりまで残る)
    let mut got = Vec::new();
    while got.len() < ROUNDS as usize + 1 {
        match done_rx.recv_timeout(Duration::from_secs(30)) {
            Ok(what) => got.push(what),
            Err(_) => panic!(
                "pool の worker の上の oplog_sync() が返らない: 記録への写しを待つ間に同じ pool の job を拾い、 その job が change_emit を \
                 取り直して止まった (返った job {} / {}、 oplog_sync の中で走った job {})",
                got.len(),
                ROUNDS + 1,
                stolen.load(Ordering::Relaxed)
            ),
        }
    }
    assert_eq!(stolen.load(Ordering::Relaxed), 0, "oplog_sync() の中で、 同じ pool の他の job が同じ stack で走った");
    assert_eq!(eng.wal_room_wait_timeouts(), 0, "WAL の空き待ちが時間切れになった");
    // 前提: 書いた分は全部記録に入った (大きな回の写しが走っている)
    let (first, next) = eng.change_log_range().unwrap();
    assert!(next - first >= ROUNDS * BURST, "前提: 書いた {} 件が記録に無い ({first}..{next})", ROUNDS * BURST);
    drop(app);
}
