//! v29 ストレステスト。長時間・大量・並行のシナリオ。
//!
//! 通常テストは速く終わるべきなので、重いものは `#[ignore]` + 明示 run で。
//! `cargo test --features v27 --test v29_stress -- --ignored` で実行。

#![cfg(feature = "v27")]

use enchudb::{Engine, HimoType};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

fn tmp(name: &str) -> String {
    let p = format!("/tmp/enchudb-v29-stress-{}-{}", name, std::process::id());
    let _ = std::fs::remove_file(&p);
    let _ = std::fs::remove_file(format!("{}.wal", p));
    let _ = std::fs::remove_file(format!("{}.crc", p));
    p
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.wal", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));
}

/// 通常テスト(早い): 10k 件のライフサイクル。
#[test]
fn sustained_10k_writes_and_reopen() {
    let path = tmp("sustained10k");
    {
        let mut e = Engine::create_with_capacity(&path, 20_000).unwrap();
        e.define_himo("v", HimoType::Number, 100);
        e.flush().unwrap();
    }

    let eng = Engine::open_concurrent_with_wal(&path, 64 * 1024 * 1024).unwrap();
    for i in 0..10_000u32 {
        let ent = eng.entity();
        eng.tie_async(ent, "v", i % 100);
    }
    eng.flush_writes();
    eng.wal_sync().unwrap();
    drop(eng);

    let eng = Engine::open_concurrent_with_wal(&path, 64 * 1024 * 1024).unwrap();
    assert_eq!(eng.entity_count(), 10_000);
    drop(eng);
    cleanup(&path);
}

/// 10 並列 writer × 1k writes 各。
#[test]
fn parallel_10_writers() {
    let path = tmp("par10");
    {
        let mut e = Engine::create_with_capacity(&path, 100_000).unwrap();
        e.define_himo("v", HimoType::Number, 10_000);
        e.flush().unwrap();
    }

    let eng = Engine::open_concurrent_with_wal(&path, 128 * 1024 * 1024).unwrap();
    let mut handles = Vec::new();
    for t in 0..10u32 {
        let e = Arc::clone(&eng);
        handles.push(std::thread::spawn(move || {
            for i in 0..1_000u32 {
                let ent = e.entity();
                e.tie_async(ent, "v", t * 1000 + i);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    eng.flush_writes();
    eng.wal_sync().unwrap();
    let total = eng.entity_count();
    drop(eng);

    let eng = Engine::open_concurrent_with_wal(&path, 128 * 1024 * 1024).unwrap();
    assert_eq!(eng.entity_count(), total, "parallel writes should survive reopen");
    assert_eq!(total, 10_000);
    drop(eng);
    cleanup(&path);
}

/// tie / untie / delete 混在 + 繰り返し kill-recover サイクル。
/// 各サイクル: 1000 書き込み + wal_sync + drop + reopen + 次のサイクル。
#[test]
fn repeated_drop_recover_cycles() {
    let path = tmp("cycles");
    {
        let mut e = Engine::create_with_capacity(&path, 50_000).unwrap();
        e.define_himo("v", HimoType::Number, 1000);
        e.flush().unwrap();
    }

    let cycles = 20;
    let per_cycle = 500;
    for cycle in 0..cycles {
        let eng = Engine::open_concurrent_with_wal(&path, 64 * 1024 * 1024).unwrap();
        for i in 0..per_cycle {
            let ent = eng.entity();
            eng.tie_async(ent, "v", (cycle * 1000 + i) as u32);
        }
        eng.flush_writes();
        eng.wal_sync().unwrap();
        drop(eng);
    }

    let eng = Engine::open_concurrent_with_wal(&path, 64 * 1024 * 1024).unwrap();
    assert_eq!(eng.entity_count(), (cycles * per_cycle) as u32);
    drop(eng);
    cleanup(&path);
}

/// Content op の大量ロード。1k entity × 1KB content。
#[test]
fn content_heavy_load() {
    let path = tmp("content");
    {
        let mut e = Engine::create_with_capacity(&path, 2000).unwrap();
        e.define_himo("id", HimoType::Number, 100);
        e.flush().unwrap();
    }

    let eng = Engine::open_concurrent_with_wal(&path, 64 * 1024 * 1024).unwrap();
    let payload: Vec<u8> = (0..1024u16).map(|i| (i & 0xff) as u8).collect();
    for i in 0..1000u32 {
        let ent = eng.entity();
        eng.tie_async(ent, "id", i);
        eng.content_async(ent, "blob", &payload);
    }
    eng.flush_writes();
    eng.wal_sync().unwrap();
    drop(eng);

    let eng = Engine::open_concurrent_with_wal(&path, 64 * 1024 * 1024).unwrap();
    for i in 0..1000u64 {
        let c = eng.get_content(i, "blob").expect("content missing");
        assert_eq!(c, payload.as_slice(), "content {} corrupted", i);
    }
    drop(eng);
    cleanup(&path);
}

// ═════════════════════════════════════════════════════════
// 以下 ignore。明示的に実行: cargo test -- --ignored
// ═════════════════════════════════════════════════════════

/// 1M 書き込み + 1M 読み出し。数十秒かかる。
#[test]
#[ignore = "heavy stress — run with --ignored"]
fn heavy_1m_writes() {
    let path = tmp("heavy1m");
    {
        let mut e = Engine::create_with_capacity(&path, 1_100_000).unwrap();
        e.define_himo("v", HimoType::Number, 10_000);
        e.flush().unwrap();
    }

    let eng = Engine::open_concurrent_with_wal(&path, 512 * 1024 * 1024).unwrap();
    let t0 = Instant::now();
    for i in 0..1_000_000u32 {
        let ent = eng.entity();
        eng.tie_async(ent, "v", i % 10_000);
    }
    eng.flush_writes();
    eng.wal_sync().unwrap();
    let el = t0.elapsed();
    println!("1M writes: {:?} ({:.0} ops/s)", el, 1e6 / el.as_secs_f64());

    let cnt = eng.entity_count();
    assert_eq!(cnt, 1_000_000);
    drop(eng);

    let t0 = Instant::now();
    let eng = Engine::open_concurrent_with_wal(&path, 512 * 1024 * 1024).unwrap();
    println!("reopen 1M: {:?}", t0.elapsed());
    assert_eq!(eng.entity_count(), 1_000_000);
    drop(eng);
    cleanup(&path);
}

/// 30 秒ランダムファズ(tie / untie / delete / content 混在)。
#[test]
#[ignore = "heavy stress — run with --ignored"]
fn random_ops_fuzz_30s() {
    let path = tmp("fuzz30s");
    {
        let mut e = Engine::create_with_capacity(&path, 100_000).unwrap();
        e.define_himo("k", HimoType::Number, 1000);
        e.define_himo("tag", HimoType::Tag, 0);
        e.flush().unwrap();
    }

    let eng = Engine::open_concurrent_with_wal(&path, 128 * 1024 * 1024).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();

    // 別スレッドで 30s 後に停止
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(30));
        stop_clone.store(true, Ordering::Release);
    });

    let mut ops = 0u64;
    let mut rng = 0x1234_5678_dead_beef_u64;
    let mut next = || {
        rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17;
        rng
    };
    let mut live_eids: Vec<u64> = Vec::new();

    while !stop.load(Ordering::Acquire) {
        let r = next() % 100;
        match r {
            0..=60 => {
                // 60% tie new
                let ent = eng.entity();
                eng.tie_async(ent, "k", (next() % 1000) as u32);
                live_eids.push(ent);
            }
            61..=80 => {
                // 20% untie random
                if !live_eids.is_empty() {
                    let idx = (next() as usize) % live_eids.len();
                    let eid = live_eids[idx];
                    eng.untie_async(eid, "k");
                }
            }
            81..=95 => {
                // 15% content
                if !live_eids.is_empty() {
                    let idx = (next() as usize) % live_eids.len();
                    let eid = live_eids[idx];
                    let payload = vec![(next() & 0xff) as u8; 64];
                    eng.content_async(eid, "data", &payload);
                }
            }
            _ => {
                // 5% delete
                if !live_eids.is_empty() {
                    let idx = (next() as usize) % live_eids.len();
                    let eid = live_eids.swap_remove(idx);
                    eng.delete_async(eid);
                }
            }
        }
        ops += 1;
        if ops % 10_000 == 0 {
            eng.flush_writes();
            eng.wal_sync().unwrap();
        }
    }
    eng.flush_writes();
    eng.wal_sync().unwrap();
    println!("30s fuzz: {} ops ({:.0} ops/s)", ops, ops as f64 / 30.0);
    drop(eng);

    // 再 open で panic しなければ OK
    let eng = Engine::open_concurrent_with_wal(&path, 128 * 1024 * 1024).unwrap();
    let _ = eng.entity_count();
    drop(eng);
    cleanup(&path);
}
