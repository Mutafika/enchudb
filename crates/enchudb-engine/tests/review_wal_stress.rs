//! review (0.37.4): 小さい WAL に書き手 8 本 (sync の表の無い DB)。 止まらない / 健全なのに断らない (`WalSpace`) / 読める値 =
//! 最後に通った値 / 開き直しても同じ、 を状態で見る。
//!
//! cargo test -p enchudb-engine --features crashsim --test review_wal_stress -- --nocapture --test-threads=1

use enchudb_engine::{Engine, FaultKind, ValueType};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

fn scratch(tag: &str) -> PathBuf {
    let base = std::env::var("REVIEW_SCRATCH").unwrap_or_else(|_| {
        "/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/eedfc333-d87a-4a47-a3b0-daf4b6d7337a/scratchpad".into()
    });
    let p = PathBuf::from(base).join(format!("review_wal_stress_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn run(tag: &str, cap: usize, writers: usize, per: u32) {
    let root = scratch(tag);
    let db = root.join("db");
    let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 65_536).unwrap();
    eng.define_table("t", 1_000).unwrap();
    eng.define_himo_in("t", "x", ValueType::Number, 0).unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, cap).unwrap();
    let hid = eng.himo_id("t.x").unwrap() as u16;
    let eids: Vec<u64> = (0..writers).map(|_| eng.entity_in("t").unwrap()).collect();
    let progress = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    // 止まりの見張り: 進みが 90 秒動かなければ落とす
    let watchdog = {
        let (progress, done) = (progress.clone(), done.clone());
        let tag = tag.to_string();
        std::thread::spawn(move || {
            let (mut last, mut at) = (0, std::time::Instant::now());
            while !done.load(Ordering::Acquire) {
                std::thread::sleep(std::time::Duration::from_millis(200));
                let p = progress.load(Ordering::Relaxed);
                if p != last {
                    (last, at) = (p, std::time::Instant::now());
                } else if at.elapsed().as_secs() > 90 {
                    eprintln!("[{tag}] 書き手が 90 秒進まない (進み {p}) — 止まった");
                    std::process::abort();
                }
            }
        })
    };
    let results: Vec<(u32, u32)> = std::thread::scope(|s| {
        let hs: Vec<_> = eids
            .iter()
            .map(|&e| {
                let (eng, progress) = (eng.clone(), progress.clone());
                s.spawn(move || {
                    let (mut last_ok, mut rejected) = (0u32, 0u32);
                    for v in 1..=per {
                        match eng.try_tie_to_by_id(e, hid, v) {
                            Ok(()) => last_ok = v,
                            Err(_) => rejected += 1,
                        }
                        progress.fetch_add(1, Ordering::Relaxed);
                    }
                    (last_ok, rejected)
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    done.store(true, Ordering::Release);
    watchdog.join().unwrap();
    let rejected: u32 = results.iter().map(|r| r.1).sum();
    let wal = eng.oplog().unwrap();
    eprintln!(
        "[{tag}] 2 区画 {} / 断った {rejected} (WalSpace fault {}) / 区画の切り替え {} / 空け作りの畳み {} / 待ちの時間切れ {}",
        wal.two_halves(),
        eng.fault_count(FaultKind::WalSpace),
        eng.wal_switches(),
        eng.wal_room_folds(),
        eng.wal_room_wait_timeouts()
    );
    for (i, &e) in eids.iter().enumerate() {
        assert_eq!(eng.get(e, "t.x"), Some(u64::from(results[i].0)), "[{tag}] 書き手 {i}: 読める値が最後に通った値でない");
    }
    eng.oplog_sync().unwrap();
    drop(eng);
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), cap).unwrap();
    for (i, &e) in eids.iter().enumerate() {
        assert_eq!(eng.get(e, "t.x"), Some(u64::from(results[i].0)), "[{tag}] 書き手 {i}: 開き直した値が違う");
    }
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(rejected, 0, "[{tag}] 書き出しは健全なのに書き込みを断った");
}

#[test]
fn eight_writers_on_a_two_half_wal() {
    run("two", 128 * 1024, 8, 30_000);
}

#[test]
fn eight_writers_on_a_one_half_wal() {
    run("one", 24 * 1024, 8, 10_000);
}
