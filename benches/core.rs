//! 主要 op の性能 regression 検出用 bench。
//!
//! TEST_DESIGN.md P1-4 + scale 軸。対象:
//! - tie(writer 1 thread、Column 直書き、 hot loop で reuse)
//! - pull_raw(O(1) 点引き)
//! - query(多条件 AND、v26 の pair table パス)
//! - snapshot_export(WAL + body のファイルコピー)
//! - audit(WAL commit 済みレコードの走査)
//! - scale (10 "table" × 5 himo × 10k entity の global flat workload、
//!   β-light の table-aware 化前後で RSS / query 比較する用)
//!
//! # 使い方
//!
//! 初回(baseline 記録):
//! ```text
//! cargo bench --bench core -- --save-baseline pre_table_aware
//! ```
//!
//! 変更後(比較):
//! ```text
//! cargo bench --bench core -- --baseline pre_table_aware
//! ```
//! criterion が ±10% 以上の劣化を自動で flag する。
//!
//! # noise floor 対策
//! - 各 bench で `iter_batched` の setup を hot loop 外に追い出す (engine
//!   create / cleanup を per-iter で繰り返さない)。 これで tie_plain が
//!   engine init コスト支配で 4x 揺れていた問題を抑える。
//! - sample_size と measurement_time を default より厚めに取り、 system
//!   load の山を均して median を安定化。
//! - bench 中は他重い process を回さないこと。 cargo build や git op が
//!   並行で走ると数字が壊れる。

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use enchudb::{AuditFilter, Engine, ValueType};
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

// 全 group 共通の感度設定。 system noise を均すため default (5s / 100 samples)
// より厚め。 ±10% 程度の真の regression は確実に拾い、 ±2-5% の noise には鈍感に。
const MEASUREMENT_TIME: Duration = Duration::from_secs(10);
const SAMPLE_SIZE: usize = 200;

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
    for suffix in ["", ".oplog", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", p, suffix));
    }
    p
}

fn cleanup(path: &str) {
    for suffix in ["", ".oplog", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

// ─────────────────────────────────────────────────────────────
// tie (1 thread、plain engine)
// ─────────────────────────────────────────────────────────────

fn bench_tie(c: &mut Criterion) {
    // pre-setup を hot loop 外に: 旧版は iter_batched で毎回 engine create + drop
    // + cleanup していたため、 measurement が tie 自体ではなく init コスト支配で
    // ±4x の system 揺れを受けていた。 1 eid を pre-allocate して reuse する。
    let path = tmp("tie_plain");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("v", ValueType::Number, 100);
    let eid = eng.entity();

    let mut group = c.benchmark_group("tie");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(MEASUREMENT_TIME);
    group.sample_size(SAMPLE_SIZE);
    group.bench_function("plain_value", |b| {
        let mut i = 0u32;
        b.iter(|| {
            i = i.wrapping_add(1);
            eng.tie(black_box(eid), "v", black_box(i % 100));
        });
    });
    group.finish();

    drop(eng);
    cleanup(&path);
}

// ─────────────────────────────────────────────────────────────
// pull_raw O(1) 点引き(v26 pair 無しでも O(log n))
// ─────────────────────────────────────────────────────────────

fn bench_pull_raw(c: &mut Criterion) {
    // 事前に 10k entity を書いて rebuild、pull_raw 自体だけ計測
    let path = tmp("pull_raw");
    let mut eng = Engine::create_with_capacity(&path, 100_000).unwrap();
    eng.define_himo("age", ValueType::Number, 100);
    for i in 0..10_000u32 {
        let e = eng.entity();
        eng.tie(e, "age", i % 100);
    }
    eng.rebuild();

    let mut group = c.benchmark_group("pull_raw");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(MEASUREMENT_TIME);
    group.sample_size(SAMPLE_SIZE);
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
    eng.define_himo("age", ValueType::Number, 100);
    eng.define_himo("dept", ValueType::Number, 20);
    for i in 0..10_000u32 {
        let e = eng.entity();
        eng.tie(e, "age", i % 100);
        eng.tie(e, "dept", i % 20);
    }
    eng.rebuild();

    let mut group = c.benchmark_group("query");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(MEASUREMENT_TIME);
    group.sample_size(SAMPLE_SIZE);
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
        eng.define_himo("v", ValueType::Number, 100);
        for i in 0..1_000u32 {
            let e = eng.entity();
            eng.tie(e, "v", i % 100);
        }
        eng.flush().unwrap();
    }
    let eng = Engine::open_standalone(&path).unwrap();

    let mut group = c.benchmark_group("snapshot_export");
    // ファイルコピー系は inherently 重い、 sample_size は 50 まで増やす (旧 20)、
    // measurement_time も 15s に延ばして noise を均す。
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(15));
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
                for suffix in ["", ".oplog", ".crc"] {
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
        eng.define_himo("v", ValueType::Number, 100);
        eng.flush().unwrap();
    }
    let eng = Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(1);
    for i in 0..1_000u32 {
        let e = eng.entity();
        eng.tie_async(e, "v", i % 100);
    }
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();

    let mut group = c.benchmark_group("audit");
    group.throughput(Throughput::Elements(1_000));
    group.measurement_time(MEASUREMENT_TIME);
    group.sample_size(SAMPLE_SIZE);
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
        eng.define_himo("v", ValueType::Number, 100);
        eng.flush().unwrap();
    }
    let eng: Arc<Engine> =
        Engine::open_concurrent_with_oplog(&path, 64 * 1024 * 1024).unwrap();
    eng.set_peer_id(1);

    // 1 entity を事前確保して reuse する。 `b.iter` は warmup + 100 samples で
    // 数千万 iteration になるため、 毎回 `eng.entity()` で slot を取ると
    // `DEFAULT_MAX_ENTITIES = 16M` に当たって panic する (#tie_async bench
    // fragility)。 ここでは tie 自体の cost を測りたいので eid は固定。
    let eid = eng.entity();

    let mut group = c.benchmark_group("tie_async");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(MEASUREMENT_TIME);
    group.sample_size(SAMPLE_SIZE);
    group.bench_function("wal_signed_off", |b| {
        let mut i = 0u32;
        b.iter(|| {
            i = i.wrapping_add(1);
            eng.tie_async(black_box(eid), "v", black_box(i % 100));
        });
    });
    group.finish();

    drop(eng);
    cleanup(&path);
}

// ─────────────────────────────────────────────────────────────
// scale (10 "table" × 5 himo × 10k entity の anonymous flat)
//
// β-light の table-aware 化前後で:
//   - 各 himo の positions size が 100k → 10k 範囲に縮む
//   - RSS / open / table 内 query が改善する想定
// 同じ workload を表現する bench を 0.4.x master 側で先に立てておき、
// 0.5.x で同 bench を再実行して A/B 比較する。
// ─────────────────────────────────────────────────────────────

fn bench_scale(c: &mut Criterion) {
    let path = tmp("scale");
    let mut eng = Engine::create_with_capacity(&path, 200_000).unwrap();

    // 10 table 相当 × 5 himo
    for t in 0..10 {
        for h in 0..5 {
            eng.define_himo(&format!("t{}_h{}", t, h), ValueType::Number, 100);
        }
    }

    // 各 table が 10k entity を確保 + 自分の 5 himo にだけ tie
    let mut table_eids: Vec<Vec<enchudb::EntityId>> = (0..10)
        .map(|_| Vec::with_capacity(10_000))
        .collect();
    for t in 0..10 {
        for _ in 0..10_000 {
            table_eids[t].push(eng.entity());
        }
    }
    for t in 0..10 {
        let himo_names: Vec<String> = (0..5).map(|h| format!("t{}_h{}", t, h)).collect();
        for (i, &e) in table_eids[t].iter().enumerate() {
            let v = (i % 100) as u32;
            for hn in &himo_names {
                eng.tie(e, hn, v);
            }
        }
    }
    eng.rebuild();

    let mut group = c.benchmark_group("scale");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(MEASUREMENT_TIME);
    group.sample_size(SAMPLE_SIZE);

    // 単 himo の dense pull (100 値、 各 100 entity)
    group.bench_function("pull_in_dense_himo", |b| {
        let mut i = 0u32;
        b.iter(|| {
            i = i.wrapping_add(1);
            let r = eng.pull_raw("t5_h0", black_box(i % 100));
            black_box(r);
        });
    });

    // table 内 2-cond AND query (post β-light で table-local positions が効くはず)
    group.bench_function("query_within_table", |b| {
        let mut i = 0u32;
        b.iter(|| {
            i = i.wrapping_add(1);
            let r = eng.query(black_box(&[
                ("t3_h0", (i % 100) as u32),
                ("t3_h1", (i % 100) as u32),
            ]));
            black_box(r);
        });
    });

    group.finish();

    // open + 即 query — 0.4.x lazy rebuild penalty 含む
    drop(eng);
    let mut group2 = c.benchmark_group("scale_open");
    group2.sample_size(30);
    group2.measurement_time(Duration::from_secs(20));
    group2.bench_function("open_then_query", |b| {
        b.iter_batched(
            || (),
            |_| {
                let eng = Engine::open_standalone(&path).unwrap();
                let r = eng.query(&[
                    ("t3_h0", 50),
                    ("t3_h1", 50),
                ]);
                black_box(r);
                drop(eng);
            },
            BatchSize::SmallInput,
        );
    });
    group2.finish();

    cleanup(&path);
}

// ─────────────────────────────────────────────────────────────
// scale_tables (β-light: 10 table × 5 himo × 10k entity を table-aware で)
//
// 同等 workload を `bench_scale` (anonymous flat) と比較する。 β-light で
// positions が table-local (10k 範囲) に収まるため、 同 op が hot path で
// 動く速度自体は変わらないが、 positions の絶対サイズ / reopen 時の
// rebuild 走査範囲は小さくなる。
// ─────────────────────────────────────────────────────────────

fn bench_scale_tables(c: &mut Criterion) {
    let path = tmp("scale_tables");
    let mut eng = Engine::create_with_capacity(&path, 200_000).unwrap();

    // 10 table × 5 himo を table-aware で構築
    for t in 0..10 {
        let tname = format!("t{}", t);
        eng.define_table(&tname, 10_000).unwrap();
        for h in 0..5 {
            let hname = format!("h{}", h);
            eng.define_himo_in(&tname, &hname, ValueType::Number, 100).unwrap();
        }
    }
    for t in 0..10 {
        let tname = format!("t{}", t);
        let himo_full: Vec<String> = (0..5).map(|h| format!("t{}.h{}", t, h)).collect();
        for i in 0..10_000 {
            let e = eng.entity_in(&tname).unwrap();
            let v = (i % 100) as u32;
            for hn in &himo_full {
                eng.tie(e, hn, v);
            }
        }
    }
    eng.rebuild();

    let mut group = c.benchmark_group("scale_tables");
    group.throughput(Throughput::Elements(1));
    group.measurement_time(MEASUREMENT_TIME);
    group.sample_size(SAMPLE_SIZE);

    group.bench_function("pull_in_dense_himo", |b| {
        let mut i = 0u32;
        b.iter(|| {
            i = i.wrapping_add(1);
            let r = eng.pull_raw("t5.h0", black_box(i % 100));
            black_box(r);
        });
    });

    group.bench_function("query_within_table", |b| {
        let mut i = 0u32;
        b.iter(|| {
            i = i.wrapping_add(1);
            let r = eng.query(black_box(&[
                ("t3.h0", (i % 100) as u32),
                ("t3.h1", (i % 100) as u32),
            ]));
            black_box(r);
        });
    });

    group.finish();

    // open + 即 query — β-light で positions sparse 化の効果が出る場所
    drop(eng);
    let mut group2 = c.benchmark_group("scale_tables_open");
    group2.sample_size(30);
    group2.measurement_time(Duration::from_secs(20));
    group2.bench_function("open_then_query", |b| {
        b.iter_batched(
            || (),
            |_| {
                let eng = Engine::open_standalone(&path).unwrap();
                let r = eng.query(&[
                    ("t3.h0", 50),
                    ("t3.h1", 50),
                ]);
                black_box(r);
                drop(eng);
            },
            BatchSize::SmallInput,
        );
    });
    group2.finish();

    cleanup(&path);
    let _ = std::fs::remove_file(format!("{}.tables", path));
}

criterion_group!(
    benches,
    bench_tie,
    bench_tie_async,
    bench_pull_raw,
    bench_query,
    bench_snapshot_export,
    bench_audit,
    bench_scale,
    bench_scale_tables,
);
criterion_main!(benches);
