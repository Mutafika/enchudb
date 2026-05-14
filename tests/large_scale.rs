//! 大量データ regression tests。`#[ignore]` 付きで手動実行用。
//!
//! 実行: `cargo test --features v32 --test large_scale -- --ignored --nocapture`
//!
//! 目的:
//! - 1000 万 entity スケールで基本 op がスケールする(O(1)/O(log n) 保持)
//! - 10 万以上の unique 値でも pull_raw が破綻しない
//! - flush + reopen が現実的な時間(数十秒)で終わる
//!
//! 各テストは数分〜十数分かかる。CI の default profile では実行しない。
//!
//! # メトリクス
//!
//! 各テストが終了時に経過時間を println!。手動で diff を取って比較する。
//! 自動 regression gating は benches/core.rs(criterion)側で別途。

use enchudb::{Engine, HimoType};
use std::time::Instant;

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-large-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suffix in ["", ".wal", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

// ─────────────────────────────────────────────────────────────
// 1000 万 entity insert / pull_raw / query
// ─────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn ten_million_insert_and_lookup() {
    let path = tmp("10m");
    let mut eng = Engine::create_with_capacity(&path, 10_000_000).unwrap();
    eng.define_himo("bucket", HimoType::Number, 1000);
    eng.define_himo("flag", HimoType::Number, 4);

    let t0 = Instant::now();
    for i in 0..10_000_000u32 {
        let e = eng.entity();
        eng.tie(e, "bucket", i % 1000);
        eng.tie(e, "flag", i % 4);
    }
    let insert_ms = t0.elapsed().as_millis();
    println!("[10m] insert 10M * 2 ties: {} ms", insert_ms);

    let t1 = Instant::now();
    eng.rebuild();
    println!("[10m] rebuild: {} ms", t1.elapsed().as_millis());

    // pull_raw O(1) per bucket — 全 1000 bucket 回して合計 10M entity に収束
    let t2 = Instant::now();
    let mut total = 0usize;
    for b in 0..1000u32 {
        total += eng.pull_raw("bucket", b).len();
    }
    println!(
        "[10m] 1000 pull_raw scans: {} ms, total entities visited: {}",
        t2.elapsed().as_millis(),
        total
    );
    assert_eq!(total, 10_000_000);

    // query (bucket, flag) 2 条件 AND を 100 回、各 ~10k 件返す想定
    let t3 = Instant::now();
    for i in 0..100u32 {
        let r = eng.query(&[("bucket", i * 10), ("flag", i % 4)]);
        assert!(!r.is_empty());
    }
    println!("[10m] 100 two-cond queries: {} ms", t3.elapsed().as_millis());

    drop(eng);
    cleanup(&path);
}

// ─────────────────────────────────────────────────────────────
// flush + reopen が秒オーダーで終わる
// ─────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn one_million_flush_reopen_cycle() {
    let path = tmp("1m_reopen");
    let t0 = Instant::now();
    {
        let mut eng = Engine::create_with_capacity(&path, 1_000_000).unwrap();
        eng.define_himo("v", HimoType::Number, 100);
        for i in 0..1_000_000u32 {
            let e = eng.entity();
            eng.tie(e, "v", i % 100);
        }
        eng.rebuild();
        eng.flush().unwrap();
    }
    println!("[1m_reopen] create+flush: {} ms", t0.elapsed().as_millis());

    let t1 = Instant::now();
    let eng = Engine::open_standalone(&path).unwrap();
    println!("[1m_reopen] open: {} ms", t1.elapsed().as_millis());
    assert_eq!(eng.entity_count(), 1_000_000);

    let t2 = Instant::now();
    let r = eng.pull_raw("v", 42);
    println!(
        "[1m_reopen] pull_raw after open: {} µs, hits: {}",
        t2.elapsed().as_micros(),
        r.len()
    );
    assert_eq!(r.len(), 10_000);

    drop(eng);
    cleanup(&path);
}

// ─────────────────────────────────────────────────────────────
// 高カーディナリティ unique 値(10 万)
// ─────────────────────────────────────────────────────────────

#[test]
#[ignore]
fn high_cardinality_100k_unique_values() {
    let path = tmp("100k_uniq");
    let mut eng = Engine::create_with_capacity(&path, 200_000).unwrap();
    eng.define_himo("uid", HimoType::Number, 100_000);

    let t0 = Instant::now();
    for i in 0..100_000u32 {
        let e = eng.entity();
        eng.tie(e, "uid", i);
    }
    eng.rebuild();
    println!("[100k_uniq] insert+rebuild: {} ms", t0.elapsed().as_millis());

    // 各 unique value を 1 entity だけ持つ bucket として引く
    let t1 = Instant::now();
    for v in 0..100_000u32 {
        let r = eng.pull_raw("uid", v);
        assert_eq!(r.len(), 1);
    }
    println!(
        "[100k_uniq] 100k pull_raw scans: {} ms",
        t1.elapsed().as_millis()
    );

    drop(eng);
    cleanup(&path);
}
