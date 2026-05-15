//! 主要 op の性能 regression 検出用 bench。
//!
//! TEST_DESIGN.md P1-4。対象:
//! - tie(writer 1 thread、Column 直書き)
//! - pull_raw(O(1) 点引き)
//! - query(多条件 AND、v26 の pair table パス)
//! - snapshot_export(WAL + body のファイルコピー)
//! - audit(WAL commit 済みレコードの走査)
//!
//! # 使い方
//!
//! 初回(baseline 記録):
//! ```text
//! cargo bench --features v32 --bench core -- --save-baseline main
//! ```
//!
//! 変更後(比較):
//! ```text
//! cargo bench --features v32 --bench core -- --baseline main
//! ```
//! criterion が ±10% 以上の劣化を自動で flag する。
//!
//! # 注意
//! regression 検出が目的なので sample 数は最低限。数値そのものより
//! ΔTime% に注目すること。

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use enchudb::{AuditFilter, Engine, HimoType};
use std::hint::black_box;
use std::sync::Arc;

fn tmp(tag: &str) -> String {
    let p = format!(
        "/tmp/enchudb-bench-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    for suffix in ["", ".wal", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", p, suffix));
    }
    p
}

fn cleanup(path: &str) {
    for suffix in ["", ".wal", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

// ─────────────────────────────────────────────────────────────
// tie (1 thread、plain engine)
// ─────────────────────────────────────────────────────────────

fn bench_tie(c: &mut Criterion) {
    let mut group = c.benchmark_group("tie");
    group.throughput(Throughput::Elements(1));
    group.bench_function("plain_value", |b| {
        b.iter_batched(
            || {
                let path = tmp("tie_plain");
                let mut eng = Engine::create_standalone(&path).unwrap();
                eng.define_himo("v", HimoType::Number, 100);
                let e = eng.entity();
                (path, eng, e, 0u32)
            },
            |(path, mut eng, e, mut counter)| {
                counter = counter.wrapping_add(1);
                eng.tie(black_box(e), "v", black_box(counter % 100));
                drop(eng);
                cleanup(&path);
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

// ─────────────────────────────────────────────────────────────
// pull_raw O(1) 点引き(v26 pair 無しでも O(log n))
// ─────────────────────────────────────────────────────────────

fn bench_pull_raw(c: &mut Criterion) {
    // 事前に 10k entity を書いて rebuild、pull_raw 自体だけ計測
    let path = tmp("pull_raw");
    let mut eng = Engine::create_with_capacity(&path, 100_000).unwrap();
    eng.define_himo("age", HimoType::Number, 100);
    for i in 0..10_000u32 {
        let e = eng.entity();
        eng.tie(e, "age", i % 100);
    }
    eng.rebuild();

    let mut group = c.benchmark_group("pull_raw");
    group.throughput(Throughput::Elements(1));
    group.bench_function("single_value", |b| {
        let mut counter = 0u32;
        b.iter(|| {
            counter = counter.wrapping_add(1);
            let r = eng.pull_raw("age", black_box(counter % 100));
            black_box(r);
        });
    });
    group.finish();

    drop(eng);
    cleanup(&path);
}

// ─────────────────────────────────────────────────────────────
// query 多条件 AND
// ─────────────────────────────────────────────────────────────

fn bench_query(c: &mut Criterion) {
    let path = tmp("query");
    let mut eng = Engine::create_with_capacity(&path, 100_000).unwrap();
    eng.define_himo("age", HimoType::Number, 100);
    eng.define_himo("dept", HimoType::Number, 20);
    for i in 0..10_000u32 {
        let e = eng.entity();
        eng.tie(e, "age", i % 100);
        eng.tie(e, "dept", i % 20);
    }
    eng.rebuild();

    let mut group = c.benchmark_group("query");
    group.throughput(Throughput::Elements(1));
    group.bench_function("two_cond_and", |b| {
        let mut i = 0u32;
        b.iter(|| {
            i = i.wrapping_add(1);
            let r = eng.query(black_box(&[
                ("age", (i % 100) as u32),
                ("dept", (i % 20) as u32),
            ]));
            black_box(r);
        });
    });
    group.finish();

    drop(eng);
    cleanup(&path);
}

// ─────────────────────────────────────────────────────────────
// snapshot_export(WAL + body のファイルコピー)
// ─────────────────────────────────────────────────────────────

fn bench_snapshot_export(c: &mut Criterion) {
    // 小さめ(1k entity)で snapshot_export の純粋な overhead を測る
    let path = tmp("snap_src");
    {
        let mut eng = Engine::create_with_capacity(&path, 2_000).unwrap();
        eng.define_himo("v", HimoType::Number, 100);
        for i in 0..1_000u32 {
            let e = eng.entity();
            eng.tie(e, "v", i % 100);
        }
        eng.flush().unwrap();
    }
    let eng = Engine::open_standalone(&path).unwrap();

    let mut group = c.benchmark_group("snapshot_export");
    group.sample_size(20); // ファイルコピー系は count 減らす
    group.bench_function("small_db", |b| {
        let mut counter = 0u64;
        b.iter_batched(
            || {
                counter += 1;
                format!("/tmp/enchudb-bench-snap-dst-{}-{}", std::process::id(), counter)
            },
            |target| {
                let files = eng.snapshot_export(black_box(&target)).unwrap();
                black_box(&files);
                // cleanup
                for suffix in ["", ".wal", ".crc"] {
                    let _ = std::fs::remove_file(format!("{}{}", target, suffix));
                }
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();

    drop(eng);
    cleanup(&path);
}

// ─────────────────────────────────────────────────────────────
// audit(WAL 走査 + filter)
// ─────────────────────────────────────────────────────────────

fn bench_audit(c: &mut Criterion) {
    // WAL 付き engine に 1k record 入れる
    let path = tmp("audit");
    {
        let mut eng = Engine::create_standalone(&path).unwrap();
        eng.define_himo("v", HimoType::Number, 100);
        eng.flush().unwrap();
    }
    let eng = Engine::open_concurrent_with_wal(&path, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(1);
    for i in 0..1_000u32 {
        let e = eng.entity();
        eng.tie_async(e, "v", i % 100);
    }
    eng.wal_commit();
    eng.flush_writes();
    eng.wal_sync().unwrap();

    let mut group = c.benchmark_group("audit");
    group.throughput(Throughput::Elements(1_000));
    group.bench_function("all_1k_records", |b| {
        b.iter(|| {
            let recs = eng.audit(black_box(&AuditFilter::default()));
            black_box(recs);
        });
    });
    group.finish();

    drop(eng);
    cleanup(&path);
}

// ─────────────────────────────────────────────────────────────
// tie_async (WAL 付き concurrent writer の hot path)
// ─────────────────────────────────────────────────────────────

fn bench_tie_async(c: &mut Criterion) {
    let path = tmp("tie_async");
    {
        let mut eng = Engine::create_standalone(&path).unwrap();
        eng.define_himo("v", HimoType::Number, 100);
        eng.flush().unwrap();
    }
    let eng: Arc<Engine> =
        Engine::open_concurrent_with_wal(&path, 64 * 1024 * 1024).unwrap();
    eng.set_peer_id(1);

    let mut group = c.benchmark_group("tie_async");
    group.throughput(Throughput::Elements(1));
    group.bench_function("wal_signed_off", |b| {
        let mut i = 0u32;
        b.iter(|| {
            i = i.wrapping_add(1);
            let e = eng.entity();
            eng.tie_async(black_box(e), "v", black_box(i % 100));
        });
    });
    group.finish();

    drop(eng);
    cleanup(&path);
}

criterion_group!(
    benches,
    bench_tie,
    bench_tie_async,
    bench_pull_raw,
    bench_query,
    bench_snapshot_export,
    bench_audit,
);
criterion_main!(benches);
