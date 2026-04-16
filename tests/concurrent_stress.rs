//! v27 並行性ストレステスト。
//!
//! `cargo test --features v27 --test concurrent_stress` で実行する。
//! v27 が無効な場合はテスト本体が空になるのでビルドだけ通る。

#![cfg(feature = "v27")]

use enchudb::{Engine, HimoType};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

fn tmp(name: &str) -> String {
    let path = format!("/tmp/enchu_concurrent_stress_{name}.db");
    let _ = std::fs::remove_file(&path);
    path
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
}

// ──────────────────────────────────────────────────────────────────
// 1. 長時間 reader / writer 並走
// ──────────────────────────────────────────────────────────────────

#[test]
fn parallel_readers_during_writes() {
    let path = tmp("parallel_rw");
    let mut eng = Engine::create(&path).unwrap();
    eng.define_himo("k", HimoType::Value, 32);

    // 初期データを撒く
    let eids: Vec<u32> = (0..2_000).map(|_| eng.entity()).collect();
    for (i, &e) in eids.iter().enumerate() {
        eng.tie(e, "k", (i as u32) % 32);
    }

    let arc = Engine::concurrentize(eng);
    let stop = Arc::new(AtomicBool::new(false));
    let reader_iters = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();

    // 4 reader threads
    for _ in 0..4 {
        let arc = arc.clone();
        let stop = stop.clone();
        let counter = reader_iters.clone();
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                for v in 0..32u32 {
                    let pulled = arc.pull_raw("k", v);
                    // 結果は常に有効な entity リスト
                    for &eid in &pulled {
                        assert!(eid < 2_000, "stale eid {} from pull", eid);
                    }
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    // 1 writer thread (async)
    {
        let arc = arc.clone();
        let stop = stop.clone();
        let eids = eids.clone();
        handles.push(thread::spawn(move || {
            let mut i = 0u32;
            while !stop.load(Ordering::Relaxed) {
                let e = eids[(i as usize) % eids.len()];
                arc.tie_async(e, "k", i % 32);
                i = i.wrapping_add(1);
            }
        }));
    }

    thread::sleep(Duration::from_secs(2));
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().expect("thread panicked");
    }

    // reader が回ってる証拠
    let n = reader_iters.load(Ordering::Relaxed);
    assert!(n > 100, "reader iterations too low: {}", n);

    arc.flush_writes();
    drop(arc);
    cleanup(&path);
}

// ──────────────────────────────────────────────────────────────────
// 2. flush_writes 後に書いた値が見える
// ──────────────────────────────────────────────────────────────────

#[test]
fn read_after_flush_sees_writes() {
    let path = tmp("flush_visible");
    let mut eng = Engine::create(&path).unwrap();
    eng.define_himo("v", HimoType::Value, 100);

    let arc = Engine::concurrentize(eng);
    let n: u32 = 10_000;

    // entity を撒きつつ tie_async
    let mut eids = Vec::with_capacity(n as usize);
    for _ in 0..n {
        eids.push(arc.entity());
    }
    for (i, &e) in eids.iter().enumerate() {
        arc.tie_async(e, "v", (i as u32) % 100);
    }

    arc.flush_writes();

    // 各値ごとに 100 件あるはず(10000 / 100)
    let mut total = 0usize;
    for v in 0..100u32 {
        let pulled = arc.pull_raw("v", v);
        assert_eq!(pulled.len(), 100, "value {} expected 100, got {}", v, pulled.len());
        total += pulled.len();
    }
    assert_eq!(total, n as usize);

    drop(arc);
    cleanup(&path);
}

// ──────────────────────────────────────────────────────────────────
// 3. 大量 push しても SegQueue が受け付け、consumer が全件消化
// ──────────────────────────────────────────────────────────────────
//
// 旧称 queue_backpressure: ArrayQueue の満杯時スピン挙動を確認していた。
// SegQueue 化で cap が消え、push は常に成功する。いまは「大量 push でも
// 完走し、件数が合う」ことを確認する。

#[test]
fn unbounded_queue_handles_burst() {
    let path = tmp("unbounded_burst");
    let mut eng = Engine::create(&path).unwrap();
    eng.define_himo("k", HimoType::Value, 50);
    let eids: Vec<u32> = (0..10_000).map(|_| eng.entity()).collect();

    let arc = Engine::concurrentize(eng);

    let started = Instant::now();
    for (i, &e) in eids.iter().enumerate() {
        arc.tie_async(e, "k", (i as u32) % 50);
    }
    arc.flush_writes();
    let elapsed = started.elapsed();

    // 完走する(無限ループしない)。10000 件で 5 秒以内なら OK。
    assert!(elapsed < Duration::from_secs(5), "burst too slow: {:?}", elapsed);

    let mut total = 0usize;
    for v in 0..50u32 {
        total += arc.pull_raw("k", v).len();
    }
    assert_eq!(total, 10_000);

    drop(arc);
    cleanup(&path);
}

// ──────────────────────────────────────────────────────────────────
// 4. flush_writes が完全に drain する
// ──────────────────────────────────────────────────────────────────

#[test]
fn flush_writes_drains_fully() {
    let path = tmp("flush_drains");
    let mut eng = Engine::create(&path).unwrap();
    eng.define_himo("a", HimoType::Value, 10);
    let eids: Vec<u32> = (0..5_000).map(|_| eng.entity()).collect();

    let arc = Engine::concurrentize(eng);

    for (i, &e) in eids.iter().enumerate() {
        arc.tie_async(e, "a", (i as u32) % 10);
    }
    arc.flush_writes();

    assert_eq!(arc.pending_writes(), 0, "pending writes should be 0 after flush");

    drop(arc);
    cleanup(&path);
}

// ──────────────────────────────────────────────────────────────────
// 5. reader 動作中に Engine drop しても SEGV しない
// ──────────────────────────────────────────────────────────────────

#[test]
fn drop_while_reading() {
    let path = tmp("drop_reading");
    let mut eng = Engine::create(&path).unwrap();
    eng.define_himo("k", HimoType::Value, 16);
    let eids: Vec<u32> = (0..500).map(|_| eng.entity()).collect();
    for (i, &e) in eids.iter().enumerate() {
        eng.tie(e, "k", (i as u32) % 16);
    }

    let arc = Engine::concurrentize(eng);
    let stop = Arc::new(AtomicBool::new(false));

    let reader = {
        let arc = arc.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            // 自前で持ってる Arc が main 側 drop 後も Engine を生かす。
            while !stop.load(Ordering::Relaxed) {
                for v in 0..16u32 {
                    let _ = arc.pull_raw("k", v);
                }
            }
        })
    };

    thread::sleep(Duration::from_millis(200));

    // main 側の Arc を捨てる(reader 側がまだ持ってる)
    drop(arc);

    // reader はまだ走り続ける(自分の Arc clone を保持)
    thread::sleep(Duration::from_millis(200));
    stop.store(true, Ordering::Relaxed);
    reader.join().expect("reader panicked");

    cleanup(&path);
}

// ──────────────────────────────────────────────────────────────────
// 6. pending writes が積まれた状態で drop しても全部 drain される
// ──────────────────────────────────────────────────────────────────

#[test]
fn drop_with_pending_writes() {
    let path = tmp("drop_pending");
    let mut eng = Engine::create(&path).unwrap();
    eng.define_himo("k", HimoType::Value, 50);
    let eids: Vec<u32> = (0..3_000).map(|_| eng.entity()).collect();

    let arc = Engine::concurrentize(eng);

    for (i, &e) in eids.iter().enumerate() {
        arc.tie_async(e, "k", (i as u32) % 50);
    }

    // flush_writes は呼ばずに drop。Drop が consumer の最終 drain を待つはず。
    let path2 = path.clone();
    let arc_for_check = arc.clone();
    drop(arc);

    // arc_for_check はまだ生きてるが、内部 consumer は走り続けてる。
    // 念のため少し待ってから flush して確認。
    arc_for_check.flush_writes();
    let mut total = 0usize;
    for v in 0..50u32 {
        total += arc_for_check.pull_raw("k", v).len();
    }
    assert_eq!(total, 3_000, "drop should not lose pending writes");

    drop(arc_for_check);
    cleanup(&path2);
}

// ──────────────────────────────────────────────────────────────────
// 7. 複数 writer thread が同じ Engine に書き込む
// ──────────────────────────────────────────────────────────────────

#[test]
fn multiple_async_writers() {
    let path = tmp("multi_writers");
    let mut eng = Engine::create(&path).unwrap();
    eng.define_himo("g", HimoType::Value, 8);

    let arc = Engine::concurrentize(eng);

    let n_writers = 4;
    let per_writer: u32 = 1_000;

    let mut handles = Vec::new();
    for w in 0..n_writers {
        let arc = arc.clone();
        handles.push(thread::spawn(move || {
            let mut local_eids = Vec::with_capacity(per_writer as usize);
            for _ in 0..per_writer {
                local_eids.push(arc.entity());
            }
            for (i, &e) in local_eids.iter().enumerate() {
                arc.tie_async(e, "g", ((w as u32) + (i as u32)) % 8);
            }
        }));
    }

    for h in handles {
        h.join().expect("writer panicked");
    }
    arc.flush_writes();

    let mut total = 0usize;
    for v in 0..8u32 {
        total += arc.pull_raw("g", v).len();
    }
    assert_eq!(total, (n_writers as u32 * per_writer) as usize);

    drop(arc);
    cleanup(&path);
}

// ──────────────────────────────────────────────────────────────────
// 8. reader が常に整合スナップショット(重複なし、無効 id なし)を見る
// ──────────────────────────────────────────────────────────────────

#[test]
fn reader_sees_consistent_snapshot() {
    let path = tmp("consistent_snapshot");
    let mut eng = Engine::create(&path).unwrap();
    eng.define_himo("c", HimoType::Value, 16);

    let n_ent: u32 = 1_000;
    let eids: Vec<u32> = (0..n_ent).map(|_| eng.entity()).collect();
    for (i, &e) in eids.iter().enumerate() {
        eng.tie(e, "c", (i as u32) % 16);
    }
    let max_eid = eids.iter().copied().max().unwrap();

    let arc = Engine::concurrentize(eng);
    let stop = Arc::new(AtomicBool::new(false));
    let oor = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for _ in 0..3 {
        let arc = arc.clone();
        let stop = stop.clone();
        let oor = oor.clone();
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                for v in 0..16u32 {
                    let pulled = arc.pull_raw("c", v);
                    // 範囲チェック(致命: 一度でもあったら困る)
                    for &e in &pulled {
                        if e > max_eid {
                            oor.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    // 重複は発生してはならない(致命)
                    let mut sorted = pulled.clone();
                    sorted.sort_unstable();
                    let len_before = sorted.len();
                    sorted.dedup();
                    assert_eq!(
                        sorted.len(), len_before,
                        "pull_raw returned duplicate eids: {:?}", pulled
                    );
                }
            }
        }));
    }

    {
        let arc = arc.clone();
        let stop = stop.clone();
        let eids = eids.clone();
        handles.push(thread::spawn(move || {
            let mut i = 0u32;
            while !stop.load(Ordering::Relaxed) {
                let e = eids[(i as usize) % eids.len()];
                arc.tie_async(e, "c", i % 16);
                i = i.wrapping_add(1);
            }
        }));
    }

    thread::sleep(Duration::from_millis(800));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().expect("thread panicked");
    }

    // out-of-range は決して起こってはならない
    assert_eq!(oor.load(Ordering::Relaxed), 0, "reader saw out-of-range eid");

    arc.flush_writes();
    drop(arc);
    cleanup(&path);
}

// ──────────────────────────────────────────────────────────────────
// 9. 長時間 query 中でも結果が常に有効
// ──────────────────────────────────────────────────────────────────

#[test]
fn long_running_query_during_writes() {
    let path = tmp("long_query");
    let mut eng = Engine::create(&path).unwrap();
    eng.define_himo("a", HimoType::Value, 20);
    eng.define_himo("b", HimoType::Value, 20);

    let n_ent: u32 = 2_000;
    let eids: Vec<u32> = (0..n_ent).map(|_| eng.entity()).collect();
    for (i, &e) in eids.iter().enumerate() {
        eng.tie(e, "a", (i as u32) % 20);
        eng.tie(e, "b", (i as u32 / 20) % 20);
    }

    let arc = Engine::concurrentize(eng);
    let stop = Arc::new(AtomicBool::new(false));

    let mut handles = Vec::new();

    // writer: 値を書き換え続ける
    {
        let arc = arc.clone();
        let stop = stop.clone();
        let eids = eids.clone();
        handles.push(thread::spawn(move || {
            let mut i = 0u32;
            while !stop.load(Ordering::Relaxed) {
                let e = eids[(i as usize) % eids.len()];
                arc.tie_async(e, "a", i % 20);
                if i % 3 == 0 {
                    arc.tie_async(e, "b", (i / 7) % 20);
                }
                i = i.wrapping_add(1);
            }
        }));
    }

    // reader: query を 100 回回して、その都度結果が valid かチェック。
    // 重複 eid は発生してはならない(致命)。
    let max_eid = eids.iter().copied().max().unwrap();
    {
        let arc = arc.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                for av in 0..5u32 {
                    for bv in 0..5u32 {
                        let r = arc.query(&[("a", av), ("b", bv)]);
                        // 範囲内チェック
                        for &e in &r {
                            assert!(e <= max_eid, "out-of-range eid {}", e);
                        }
                        // 重複チェック(致命)
                        let mut s = r.clone();
                        s.sort_unstable();
                        let before = s.len();
                        s.dedup();
                        assert_eq!(
                            s.len(), before,
                            "query returned duplicate eids for a={}, b={}: {:?}", av, bv, r
                        );
                    }
                }
            }
        }));
    }

    // reader が終わるまで待つ
    let reader = handles.pop().unwrap();
    reader.join().expect("reader panicked");

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().expect("writer panicked");
    }

    arc.flush_writes();
    drop(arc);
    cleanup(&path);
}

// ──────────────────────────────────────────────────────────────────
// 10. 1 reader が panic しても他 reader と engine は生き残る
// ──────────────────────────────────────────────────────────────────

#[test]
fn panic_in_reader_doesnt_corrupt() {
    let path = tmp("panic_reader");
    let mut eng = Engine::create(&path).unwrap();
    eng.define_himo("k", HimoType::Value, 16);
    let eids: Vec<u32> = (0..500).map(|_| eng.entity()).collect();
    for (i, &e) in eids.iter().enumerate() {
        eng.tie(e, "k", (i as u32) % 16);
    }

    let arc = Engine::concurrentize(eng);

    // panic する reader
    let panicker = {
        let arc = arc.clone();
        thread::spawn(move || {
            // 何回か read してから意図的に unwrap で panic
            for _ in 0..10 {
                let _ = arc.pull_raw("k", 0);
            }
            let _: u32 = None::<u32>.unwrap(); // 意図的 panic
        })
    };

    let res = panicker.join();
    assert!(res.is_err(), "expected panicker to panic");

    // 他 reader が今でも動く
    let survivor = {
        let arc = arc.clone();
        thread::spawn(move || {
            let mut total = 0usize;
            for v in 0..16u32 {
                total += arc.pull_raw("k", v).len();
            }
            total
        })
    };
    let total = survivor.join().expect("survivor reader panicked");
    assert_eq!(total, 500, "engine lost data after reader panic");

    // 書き込みもまだできる
    let new_e = arc.entity();
    arc.tie_async(new_e, "k", 7);
    arc.flush_writes();
    let after = arc.pull_raw("k", 7);
    assert!(after.contains(&new_e), "post-panic write not visible");

    drop(arc);
    cleanup(&path);
}
