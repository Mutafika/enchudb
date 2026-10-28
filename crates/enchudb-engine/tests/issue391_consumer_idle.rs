//! #391: 何も書いていない間、 consumer thread は寝ている (旧: 1 ms ごとに起きて見回り)。 WAL に書いた / 閉じる時に
//! 起こされ、 書き出し (100 ms ごと) や畳みが残っている間だけ時間で起きる。
//!
//! 0.34 から `*_async` の書き込みは呼んだ thread がその場で書く (consumer に渡さない)。 なので 「consumer が起こされずに
//! 積んだ書き込みが当たらない」 ことは起きなくなり、 その形を見ていた 2 本は 「返った時に見える」 を見る形にした。
//! consumer の仕事は WAL の書き出しだけになったので、 WAL の無い engine は consumer を持たない (起きる回数は 0、 閉じる
//! のもすぐ)。 oplog false の形は、 その対照として残す。

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
            assert!(wait_until(Duration::from_secs(3), || wal.head() == wal.checkpoint() && enchudb_oplog::oplog::wal_pos_offset(wal.head()) <= 4096), "畳まれない");
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

/// 寝ている間に書いても取りこぼさない: async の書き込みは返った時に見え (consumer を待たない)、 同期の書き込み (WAL に
/// 直接) も 100 ms ごとの書き出しで checkpoint が head まで進む。
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
        assert!(started.elapsed() < Duration::from_millis(500), "flush_writes が待った (0.34 から何もしない)");
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

/// 寝ている consumer の engine を閉じても止まらない (閉じる時に起こして最後の書き出しに入らせる)。 WAL の無い engine は
/// consumer を持たないので、 すぐ閉じる。
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

/// consumer が寝ている時に async で書いても、 返った時に見える (`flush_writes` を待たない)。 oplog の有り無しの両方。
/// 前は async の書き込みを consumer が当てていたので、 寝ている consumer を起こし損ねると当たらなかった (0.34 から
/// 呼んだ thread がその場で書く)。
#[test]
fn async_write_after_idle_is_visible_on_return() {
    for oplog in [false, true] {
        let path = tmp(if oplog { "visible-idle" } else { "visible-idle-nooplog" });
        let eng = engine(&path, oplog);
        let k = eng.himo_id("k").unwrap() as u16;
        for round in 0..3u32 {
            // consumer が寝に入るのを待つ
            std::thread::sleep(Duration::from_millis(200));
            let a = eng.entity().unwrap();
            eng.tie_async_by_id(a, k, round);
            assert_eq!(eng.get(a, "k"), Some(round as u64), "oplog {oplog}: 返った時に見えない");
        }
        drop(eng);
        cleanup(&path);
    }
}

/// async の書き込みを連打しても、 1 つずつ返った時に見える (`flush_writes` を待たない)。 前は consumer が寝に入る瞬間に
/// 積むと起こし損ねることがあり (寝る前の見直しを外した変異で 4 回中 3 回)、 2026-10-16 に手元の CI で 2 回 60 秒の打ち切りで
/// 落ちた (oplog の無い形)。 0.34 から async の書き込みは呼んだ thread がその場で書くので、 待つ相手が居ない。
#[test]
fn back_to_back_async_writes_are_visible_on_return() {
    for oplog in [false, true] {
        let path = tmp(if oplog { "race" } else { "race-nooplog" });
        let eng = engine(&path, oplog);
        let k = eng.himo_id("k").unwrap() as u16;
        let a = eng.entity().unwrap();
        let n = if oplog { 20_000u32 } else { 200_000 };
        for i in 0..n {
            // 書くタイミングを少しずつずらす (前の形の窓をなめる並べ方のまま)
            for _ in 0..(i % 97) * 8 {
                std::hint::spin_loop();
            }
            eng.tie_async_by_id(a, k, i);
            assert_eq!(eng.get(a, "k"), Some(i as u64), "oplog {oplog}: {i} 回目が返った時に見えない");
        }
        drop(eng);
        cleanup(&path);
    }
}
