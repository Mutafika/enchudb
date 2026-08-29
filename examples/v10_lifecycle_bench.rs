//! v10 lifecycle bench — 「新しい page を踏む」 write 経路と file 数に比例する経路を測る。
//!
//! criterion (`benches/core`) の tie は同一 cell 連打なので、 segment の commit 伸長
//! (grow) や page fault のコストが見えない。 0.26.0 で cold write が 0.25.1 比 -25% だった
//! のは、 この bench でしか出なかった (原因: grow のたびの open/close が macOS で dirty page
//! を書き戻す。 `segment_map` の doc 参照)。 v10 の release 前後で必ず併用すること。
//!
//! 使い方:
//! ```text
//! cargo run --release --example v10_lifecycle_bench            # 全 phase
//! cargo run --release --example v10_lifecycle_bench -- x prof  # cold tie を 60 周 (profiler 用)
//! ```
//! 比較対象 (0.25.1 等) は `git archive` した tree に同じ file を置いて走らせる
//! (`Engine` の公開 API しか使わない)。 数字は notes/requests/request21.md と CHANGELOG 0.26.0。

use enchudb::{Engine, ValueType};
use std::time::Instant;

fn sizes(path: &str) -> (u64, u64) {
    let u = enchudb::db_files::disk_usage(path);
    (u.apparent, u.physical)
}
fn rm(path: &str) {
    let _ = enchudb::db_files::remove_db(path);
}
fn ms(t: Instant) -> f64 { t.elapsed().as_secs_f64() * 1e3 }
fn mb(b: u64) -> f64 { b as f64 / 1048576.0 }

fn main() {
    let tag = std::env::args().nth(1).unwrap_or_else(|| "x".into());
    let base = format!("/tmp/enchudb-v10-lifecycle-{}-{}", tag, std::process::id());
    if std::env::args().nth(2).as_deref() == Some("prof") {
        // profiler 用: cold tie phase を繰り返して時間を稼ぐ
        let p = format!("{}-prof.db", base);
        let mut total = 0.0;
        for _ in 0..60 {
            rm(&p);
            let mut eng = Engine::create_with_capacity(&p, 1_000_000).unwrap();
            for i in 0..200 { eng.define_himo(&format!("h{}", i), ValueType::Number, 100); }
            let n = 200_000u32;
            let mut first = 0;
            for i in 0..n { let e = eng.entity().unwrap(); if i == 0 { first = e; } }
            let t = Instant::now();
            for i in 0..n { let e = first + i as u64; eng.tie(e, "h0", i % 100); eng.tie(e, "h1", i % 7); eng.tie(e, "h2", i % 13); }
            total += ms(t);
            drop(eng);
        }
        rm(&p);
        println!("prof: cold tie x3 total {:.1} ms over 60 iters", total);
        return;
    }
    // 1. create_growable (既定 capacity)
    let p = format!("{}-default.db", base); rm(&p);
    let t = Instant::now();
    let eng = Engine::create_growable(&p).unwrap();
    let t_create = ms(t);
    let t = Instant::now(); drop(eng); let t_drop = ms(t);
    let (a, ph) = sizes(&p);
    println!("create_growable(default)  create {:8.1} ms  drop {:7.1} ms  apparent {:9.1} MB  physical {:8.1} MB", t_create, t_drop, mb(a), mb(ph));
    let t = Instant::now(); drop(Engine::open_standalone(&p).unwrap()); println!("open(empty default)       {:8.1} ms", ms(t));
    rm(&p);

    // 2. create_with_capacity(1M) + define_himo x 200 + 200k entities x 3 tie
    let p = format!("{}-1m.db", base); rm(&p);
    let t = Instant::now();
    let mut eng = Engine::create_with_capacity(&p, 1_000_000).unwrap();
    let t_create = ms(t);
    let t = Instant::now();
    for i in 0..200 { eng.define_himo(&format!("h{}", i), ValueType::Number, 100); }
    let t_himo = ms(t);
    let t = Instant::now();
    for i in 0..200_000u32 {
        let e = eng.entity().unwrap();
        eng.tie(e, "h0", i % 100); eng.tie(e, "h1", i % 7); eng.tie(e, "h2", i % 13);
    }
    let t_write = ms(t);
    let t = Instant::now(); eng.flush().unwrap(); let t_flush = ms(t);
    let t = Instant::now(); drop(eng); let t_drop = ms(t);
    let (a, ph) = sizes(&p);
    println!("cap1M: create {:7.1} ms  define_himo x200 {:7.1} ms  write 200k x3 {:8.1} ms ({:.2} M tie/s)  flush {:6.1} ms  drop {:6.1} ms  apparent {:9.1} MB  physical {:8.1} MB",
        t_create, t_himo, t_write, 600_000.0 / (t_write / 1e3) / 1e6, t_flush, t_drop, mb(a), mb(ph));
    let t = Instant::now(); let eng = Engine::open_standalone(&p).unwrap(); let t_open = ms(t);
    let t = Instant::now(); let n = eng.query(&[("h1", 3)]).len(); let t_q = ms(t);
    println!("reopen(clean)             {:8.1} ms  query h1=3 → {} hits {:6.3} ms", t_open, n, t_q);
    let snap = format!("{}-snap.db", base); rm(&snap);
    let t = Instant::now(); eng.snapshot_export(&snap).unwrap(); let t_snap = ms(t);
    let (sa, sph) = sizes(&snap);
    println!("snapshot_export           {:8.1} ms  apparent {:9.1} MB  physical {:8.1} MB", t_snap, mb(sa), mb(sph));
    rm(&snap);
    drop(eng);
    rm(&p);

    // 3. micro: 何が遅いか切り分け (entity のみ / 同一 cell 連打 / 順次 tie / 未定義 cell の読み)
    let p = format!("{}-micro.db", base); rm(&p);
    let mut eng = Engine::create_with_capacity(&p, 2_000_000).unwrap();
    eng.define_himo("a", ValueType::Number, 100);
    eng.define_himo("b", ValueType::Number, 100);
    let n = 1_000_000u32;
    let t = Instant::now();
    let mut first = 0;
    for i in 0..n { let e = eng.entity().unwrap(); if i == 0 { first = e; } }
    let t_ent = ms(t);
    let t = Instant::now();
    for i in 0..n { eng.tie(first, "a", i % 100); }
    let t_same = ms(t);
    let base_e = first;
    let t = Instant::now();
    for i in 0..n { eng.tie(base_e + i as u64, "b", i % 100); }
    let t_seq = ms(t);
    let t = Instant::now();
    let mut acc = 0u64;
    for i in 0..n { acc += eng.get(base_e + i as u64, "b").unwrap_or(0) as u64; }
    let t_get = ms(t);
    println!("micro 1M: entity() {:6.1} ms ({:5.1} ns/op)  tie same cell {:6.1} ms ({:5.1} ns/op)  tie seq {:6.1} ms ({:5.1} ns/op)  get seq {:6.1} ms ({:5.1} ns/op) acc={}",
        t_ent, t_ent * 1e6 / n as f64, t_same, t_same * 1e6 / n as f64, t_seq, t_seq * 1e6 / n as f64, t_get, t_get * 1e6 / n as f64, acc);
    drop(eng);
    rm(&p);

    // 4. lifecycle と同じ形 (1M cap + 200 himo) で phase 分解
    let p = format!("{}-micro2.db", base); rm(&p);
    let mut eng = Engine::create_with_capacity(&p, 1_000_000).unwrap();
    for i in 0..200 { eng.define_himo(&format!("h{}", i), ValueType::Number, 100); }
    let n = 200_000u32;
    let t = Instant::now();
    let mut first = 0;
    for i in 0..n { let e = eng.entity().unwrap(); if i == 0 { first = e; } }
    let t_ent = ms(t);
    let t = Instant::now();
    for i in 0..n { let e = first + i as u64; eng.tie(e, "h0", i % 100); eng.tie(e, "h1", i % 7); eng.tie(e, "h2", i % 13); }
    let t_cold = ms(t);
    let (c, ns) = enchudb::segment_map::grow_stats();
    println!("  segment grows so far: {} calls, {:.2} ms", c, ns as f64 / 1e6);
    let t = Instant::now();
    for i in 0..n { let e = first + i as u64; eng.tie(e, "h0", (i + 1) % 100); eng.tie(e, "h1", (i + 1) % 7); eng.tie(e, "h2", (i + 1) % 13); }
    let t_warm = ms(t);
    let t = Instant::now();
    for i in 0..n { let e = first + i as u64; eng.tie(e, "h3", i % 100); eng.tie(e, "h4", i % 7); eng.tie(e, "h5", i % 13); }
    let t_cold2 = ms(t);
    println!("micro2 200k/200himo: entity() {:5.1} ms  tie x3 cold {:5.1} ms  tie x3 warm(re-tie) {:5.1} ms  tie x3 cold(h3-5) {:5.1} ms", t_ent, t_cold, t_warm, t_cold2);
    drop(eng);
    rm(&p);
}
