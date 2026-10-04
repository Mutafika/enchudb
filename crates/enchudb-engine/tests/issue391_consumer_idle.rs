//! #391: 何も書いていない間、 consumer thread は寝ている (旧: 1 ms ごとに起きて見回り)。 書き手が積んだ / WAL に
//! 書いた / 閉じる時に起こされ、 書き出し (100 ms ごと) や畳みが残っている間だけ時間で起きる。

use enchudb_engine::{Engine, ValueType};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn tmp(tag: &str) -> String {
    let p = format!(
        "/tmp/enchudb-issue391-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    );
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    for ext in ["oplog", "tables", "lock", "eidmap", "crc"] {
        let _ = std::fs::remove_file(format!("{path}.{ext}"));
    }
}

fn engine(path: &str, oplog: bool) -> Arc<Engine> {
    let mut eng = Engine::create_growable_with_capacity(path, 10_000).unwrap();
    eng.define_himo("k", ValueType::Number, 0);
    if oplog {
        Engine::concurrentize_with_oplog(eng, 4 << 20).unwrap()
    } else {
        Engine::concurrentize(eng)
    }
}

/// 条件が成り立つまで待つ (最大 `limit`)。
fn wait_until(limit: Duration, mut f: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + limit;
    while Instant::now() < end {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    f()
}

/// 書き終えて書き出し・畳みが済んだ後は、 0.5 秒何もしない間に consumer がほぼ起きない (旧: 約 250 回)。
#[test]
fn idle_consumer_sleeps() {
    for oplog in [true, false] {
        let path = tmp(if oplog { "idle" } else { "idle-nooplog" });
        let eng = engine(&path, oplog);
        let k = eng.himo_id("k").unwrap() as u16;
        let e = eng.entity().unwrap();
        eng.tie_async_by_id(e, k, 1u32);
        eng.tie_to_by_id(e, k, 2u32);
        eng.commit();
        eng.flush_writes();
        // 書き出し → 畳み (100 ms ごとの tick 2 回分) が済むのを待つ
        if oplog {
            let wal = eng.oplog().unwrap().clone();
            assert!(wait_until(Duration::from_secs(3), || wal.head() == wal.checkpoint() && wal.head() <= 4096), "畳まれない");
        }
        std::thread::sleep(Duration::from_millis(150));
        let before = eng.consumer_wakeups();
        std::thread::sleep(Duration::from_millis(500));
        let woke = eng.consumer_wakeups() - before;
        assert!(woke <= 3, "oplog {oplog}: 何もしない 0.5 秒で consumer が {woke} 回起きた");
        drop(eng);
        cleanup(&path);
    }
}

/// 寝ている間に書いても取りこぼさない: async の書き込みは適用され、 同期の書き込み (WAL に直接) も 100 ms ごとの
/// 書き出しで checkpoint が head まで進む。
#[test]
fn writes_after_idle_are_applied_and_flushed() {
    let path = tmp("wake");
    let eng = engine(&path, true);
    let k = eng.himo_id("k").unwrap() as u16;
    let wal = eng.oplog().unwrap().clone();
    for round in 0..3u32 {
        // consumer を寝かせる
        assert!(wait_until(Duration::from_secs(3), || wal.head() == wal.checkpoint()));
        std::thread::sleep(Duration::from_millis(300));
        let a = eng.entity().unwrap();
        eng.tie_async_by_id(a, k, 10 + round);
        let started = Instant::now();
        eng.flush_writes();
        assert!(started.elapsed() < Duration::from_millis(500), "寝ている consumer が async の書き込みで起きない");
        assert_eq!(eng.get(a, "k"), Some(10 + round as u64));

        std::thread::sleep(Duration::from_millis(300));
        let b = eng.entity().unwrap();
        eng.tie_to_by_id(b, k, 20 + round);
        eng.commit();
        let head = wal.head();
        assert!(head > wal.checkpoint());
        assert!(
            wait_until(Duration::from_secs(1), || wal.checkpoint() >= head || wal.head() < head),
            "寝ている consumer が WAL への書き込みで起きず、 書き出されない"
        );
    }
    drop(eng);
    cleanup(&path);
}

/// 寝ている consumer の engine を閉じても止まらない (閉じる時に起こして最終 drain に入らせる)。
#[test]
fn drop_wakes_a_sleeping_consumer() {
    for oplog in [true, false] {
        let path = tmp(if oplog { "drop" } else { "drop-nooplog" });
        let eng = engine(&path, oplog);
        std::thread::sleep(Duration::from_millis(300));
        let started = Instant::now();
        drop(eng);
        assert!(started.elapsed() < Duration::from_secs(2), "oplog {oplog}: 閉じるのに {:?}", started.elapsed());
        cleanup(&path);
    }
}

/// oplog の無い DB でも、 寝ている consumer は async の書き込みで起きる (起こすのは queue に積んだ時だけ)。
#[test]
fn async_write_wakes_without_oplog() {
    let path = tmp("wake-nooplog");
    let eng = engine(&path, false);
    let k = eng.himo_id("k").unwrap() as u16;
    for round in 0..3u32 {
        std::thread::sleep(Duration::from_millis(200));
        let a = eng.entity().unwrap();
        eng.tie_async_by_id(a, k, round);
        let started = Instant::now();
        eng.flush_writes();
        assert!(started.elapsed() < Duration::from_millis(500), "寝ている consumer が起きない");
        assert_eq!(eng.get(a, "k"), Some(round as u64));
    }
    drop(eng);
    cleanup(&path);
}

/// consumer が寝に入る瞬間に書き手が積む形を連打する: async の書き込みを 1 つ積んでは適用を待つ。 起こし損ねると
/// (寝る前に仕事を見直さない / record の queue に積んでも起こさない)、 consumer は次に起こされるまで寝たままで
/// `flush_writes` が返らない。 確率的: 寝る前の見直しを外した変異は 4 回中 3 回検出した。 record の queue に積んだ時の
/// 起こしを外した変異はこのテストでは踏めない (op を積んだ時の起こしで consumer が起き、 窓は 「畳み終えた後に op と
/// record の間で寝に入る」 時だけ)。
#[test]
fn back_to_back_writes_never_strand_the_consumer() {
    for oplog in [false, true] {
        let path = tmp(if oplog { "race" } else { "race-nooplog" });
        let eng = engine(&path, oplog);
        let k = eng.himo_id("k").unwrap() as u16;
        let a = eng.entity().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = {
            let eng = eng.clone();
            std::thread::spawn(move || {
                // oplog の無い DB は 1 回 4 µs ほどなので多めに回す (寝る前の見直しを外した変異の検出率を上げる)
                let n = if oplog { 20_000u32 } else { 200_000 };
                for i in 0..n {
                    // 積むタイミングを少しずつずらして、 consumer が寝に入る窓をなめる
                    for _ in 0..(i % 97) * 8 {
                        std::hint::spin_loop();
                    }
                    eng.tie_async_by_id(a, k, i);
                    eng.flush_writes();
                }
                tx.send(()).unwrap();
            })
        };
        assert!(
            rx.recv_timeout(Duration::from_secs(60)).is_ok(),
            "oplog {oplog}: 積んだ書き込みが適用されないまま止まった (consumer を起こし損ねた)"
        );
        worker.join().unwrap();
        assert_eq!(eng.get(a, "k"), Some(if oplog { 19_999 } else { 199_999 }));
        drop(eng);
        cleanup(&path);
    }
}
