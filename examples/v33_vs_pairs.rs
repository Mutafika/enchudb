//! v33 (BucketCylinder) vs v33 + v26 (PairTable) — 同一データで side-by-side 比較。
//!
//! 同じプロセス内で:
//!   1. cylinder slice 経路で全クエリを測る (v33 単体)
//!   2. rebuild_pairs() で PairTable を構築
//!   3. 同じクエリを PairTable 経路で測る (v33 + v26)
//!   4. 倍率を併記した表で出す
//!
//! 使い方:
//!   cargo run --release --features "v33 v26" --example v33_vs_pairs
//!
//! v26 feature 無しだと Phase 1 のみ走る (Phase 2 はスキップメッセージ)。

use enchudb::{Engine, HimoType};
use std::time::{Duration, Instant};

const N: u32 = 1_000_000;
const ITERS: u32 = 10_000;

fn fmt_ns(d: Duration) -> String {
    let n = d.as_nanos();
    if n < 1_000 { format!("{} ns", n) }
    else if n < 1_000_000 { format!("{:.2} µs", n as f64 / 1_000.0) }
    else if n < 1_000_000_000 { format!("{:.2} ms", n as f64 / 1_000_000.0) }
    else { format!("{:.2} s", n as f64 / 1_000_000_000.0) }
}

fn bench<F: FnMut() -> usize>(mut f: F) -> (Duration, usize) {
    // warm up
    let _ = f();
    let _ = f();
    let t = Instant::now();
    let mut last = 0;
    for _ in 0..ITERS {
        last = f();
    }
    (t.elapsed() / ITERS, last)
}

fn main() {
    let path = format!("/tmp/enchu_v33_vs_pairs_{}.db", std::process::id());
    let _ = std::fs::remove_file(&path);

    let mut db = Engine::create_with_capacity(&path, N + 100).unwrap();
    // 少ユニーク紐(combo / pair の対象になりやすい)
    db.define_himo("tenant", HimoType::Number, 10);
    db.define_himo("dept",   HimoType::Number, 8);
    db.define_himo("role",   HimoType::Number, 4);
    db.define_himo("status", HimoType::Number, 5);
    db.define_himo("year",   HimoType::Number, 5);
    // 多ユニーク紐
    db.define_himo("salary", HimoType::Number, 1000);
    db.define_himo("age",    HimoType::Number, 60);

    println!("=== v33 vs v33+v26 ペアテーブル ベンチ ===");
    println!("entities: {}, 7 紐(5 少ユニーク + 2 多ユニーク)\n", N);

    // データ投入。 少ユニーク紐は **互いに独立** な値生成 (cross product が
    // 全パターン埋まる) にして、 任意の条件組み合わせが nonzero になるよう
    // にする。 大ユニーク紐(salary/age)は擬似ランダム。
    let t = Instant::now();
    for i in 0..N {
        let e = db.entity();
        db.tie(e, "tenant",  i % 10);            // 10 段階、 周期 10
        db.tie(e, "dept",    (i / 10) % 8);       // 8 段階、 周期 80
        db.tie(e, "role",    (i / 80) % 4);       // 4 段階、 周期 320
        db.tie(e, "status",  (i / 320) % 5);      // 5 段階、 周期 1600
        db.tie(e, "year",    (i / 1600) % 5);     // 5 段階、 周期 8000
        db.tie(e, "salary",  (i * 31) % 1000);    // 1000 段階、 多ユニーク
        db.tie(e, "age",     20 + (i * 17) % 40); // 40 段階、 多ユニーク
    }
    println!("投入:           {}", fmt_ns(t.elapsed()));

    let t = Instant::now();
    db.rebuild();
    println!("rebuild (cyl):  {}", fmt_ns(t.elapsed()));

    let queries: Vec<(&str, Vec<(&str, u32)>)> = vec![
        // ─── decision-time queries (空 or 極小 result、 Vec alloc 影響なし) ───
        ("[空] ミス (salary 該当なし)",             vec![("tenant", 3), ("dept", 2), ("salary", 999)]),
        ("[空] ミス (age 該当なし)",                vec![("tenant", 3), ("age", 999)]),
        ("[極小] 7 条件 全紐",                       vec![("tenant", 3), ("dept", 2), ("role", 1), ("status", 1), ("year", 2), ("salary", 500), ("age", 35)]),

        // ─── full-query queries (Vec materialize 込み) ───
        ("[少] 5 条件 少ユニーク全部 (~125件)",      vec![("tenant", 3), ("dept", 2), ("role", 1), ("status", 1), ("year", 2)]),
        ("[中] 3 条件 (~2500件)",                    vec![("tenant", 3), ("dept", 2), ("status", 1)]),
        ("[中] 2 条件 (~12500件)",                   vec![("tenant", 3), ("dept", 2)]),
        ("[大] 1 条件 (~100000件)",                  vec![("tenant", 3)]),
    ];

    // ─────────────────── Phase 1: v33 単体 (cylinder slice) ───────────────────

    println!("\n── Phase 1: v33 (BucketCylinder のみ) ──");
    println!("| クエリ                              | 速度       | 件数 |");
    println!("|-------------------------------------|------------|------|");
    let mut baselines: Vec<Duration> = Vec::with_capacity(queries.len());
    for (name, q) in &queries {
        let refs: Vec<(&str, u32)> = q.iter().map(|&(s, v)| (s, v)).collect();
        let (elapsed, count) = bench(|| db.query(&refs).len());
        baselines.push(elapsed);
        println!("| {:<35} | {:>10} | {:>4} |", name, fmt_ns(elapsed), count);
    }

    #[cfg(not(feature = "v26"))]
    {
        println!("\nv26 feature が無いので Phase 2 はスキップ。");
        println!("PairTable を測るには: cargo run --release --features \"v33 v26\" --example v33_vs_pairs");
        let _ = std::fs::remove_file(&path);
        return;
    }

    #[cfg(feature = "v26")]
    {
        // ─────────────────── rebuild_pairs ───────────────────
        let t = Instant::now();
        db.rebuild_pairs();
        println!("\nrebuild_pairs:  {}", fmt_ns(t.elapsed()));

        // ─────────── Phase 2: v33 + v26 (PairTable 有効) ───────────
        println!("\n── Phase 2: v33 + v26 (PairTable 有効) ──");
        println!("| クエリ                              | v33 単体   | + Pair     | 倍率  |");
        println!("|-------------------------------------|------------|------------|-------|");
        for ((name, q), baseline) in queries.iter().zip(baselines.iter()) {
            let refs: Vec<(&str, u32)> = q.iter().map(|&(s, v)| (s, v)).collect();
            let (elapsed, _count) = bench(|| db.query(&refs).len());
            let ratio = baseline.as_nanos() as f64 / elapsed.as_nanos().max(1) as f64;
            println!(
                "| {:<35} | {:>10} | {:>10} | {:>4.1}x |",
                name,
                fmt_ns(*baseline),
                fmt_ns(elapsed),
                ratio,
            );
        }

        // ─────────────────── メモリ disclosure ───────────────────
        println!("\n── メモリコスト ──");
        println!("PairTable は heap (anonymous、 evict 不可)、 entity × 紐ペア数に比例。");
        println!("100 万 entity × 7 紐(ペア 21 個)で ~60 MB heap (CLAUDE.md の記録値)。");
        println!("v33 単体は mmap working set のみ (page cache、 OS が evict 可能)。");
    }

    // ─────────────────── 読み方ガイド ───────────────────
    println!("\n── 数字の読み方 ──");
    println!("[空] / [極小]: lookup decision time (Vec alloc 影響なし) — sub-µs / ns 級");
    println!("[少] / [中] / [大]: Vec materialize 込み — 結果サイズに比例 (~ns/entity の memcpy)");
    println!("「ns 売り」 は lookup 部分の話。 1000 件以上 Vec で返す時は memcpy 律速で µs 級。");
    println!("これは原理的限界 (u64 × N の copy)、 enchudb の遅さじゃない。");

    let _ = std::fs::remove_file(&path);
}
