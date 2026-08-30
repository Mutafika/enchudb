//! open cost bench — **consumer の形をした** open 計測 (request23)。
//!
//! v10 は 1 region = 1 file なので、 **DB を開くコストが file 数に比例する定数**になった。
//! 既存の bench (`v10_lifecycle_bench` / criterion) はどれも 「1 回開いて大量に作業する」 形
//! なので、 open が償却されて誤差に見える。 0.26.0 では実際に
//! 「reopen 200 himo 0.96 → 5.5 ms」 と出ていたのに 「1 回きりの経路」 と分類してしまった。
//!
//! 実際の consumer はそうではない:
//! - **kenning / `sf` は 1 コマンド = 1 process** なので open 代を毎回払う (償却先が無い)
//! - **1 process で N 個の DB を開く経路がある** (kenning の `across` は 20〜25 db、
//!   sinfohub は `_router` + `users/*` + `shared/shard_*`)
//!
//! この bench は open が支配する形 (開く → 小さな query 1 本 → 閉じる) で、
//! **himo 数を振って 1 file あたりのコスト**を出し、 **N db を 1 process で開く**逐次 / 並列を
//! 測る。 内訳 (manifest 検証 / segment open) は engine の診断 counter から取る。
//!
//! ```text
//! cargo run --release --example open_cost_bench             # 既定 (himo 0/16/48/200、 20 db)
//! cargo run --release --example open_cost_bench -- 48 25    # himo 48 本、 25 db
//! ```

use enchudb::{Engine, ValueType};
use std::path::Path;
use std::time::Instant;

const ENTS: u32 = 200;
const REPS: usize = 7;

fn ms(d: std::time::Duration) -> f64 {
    d.as_secs_f64() * 1e3
}

fn build_db(path: &str, himos: u32, max_values: u32) {
    let _ = enchudb::db_files::remove_db(path);
    let mut eng = Engine::create_with_capacity(path, 4096).unwrap();
    eng.define_table("t", 1000).unwrap();
    for h in 0..himos {
        eng.define_himo_in("t", &format!("h{h}"), ValueType::Number, max_values).unwrap();
    }
    for i in 0..ENTS {
        let e = eng.entity_in("t").unwrap();
        for h in 0..himos.min(4) {
            eng.tie(e, &format!("t.h{h}"), i + h);
        }
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
}

/// consumer 1 回分: 開く → 小さな query 1 本 → 閉じる。
fn open_query_close(path: &str, himos: u32) -> u64 {
    let eng = Engine::open_readonly(path).unwrap();
    let mut acc = eng.entity_count() as u64;
    if himos > 0 {
        // 触るのは 1 himo だけ (kenning 実測で 48 中 2〜13 本)。
        acc += eng.get(0, "t.h0").map(|v| v.to_string().len() as u64).unwrap_or(0);
    }
    acc
}

fn files_in(path: &str) -> usize {
    fn walk(p: &Path) -> usize {
        let mut n = 0;
        for e in std::fs::read_dir(p).unwrap() {
            let e = e.unwrap();
            if e.path().is_dir() { n += walk(&e.path()) } else { n += 1 }
        }
        n
    }
    walk(Path::new(path))
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let himo_sweep: Vec<u32> = match args.first() {
        Some(a) => a.split(',').map(|s| s.parse().unwrap()).collect(),
        None => vec![0, 16, 48, 200],
    };
    let n_db: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(20);
    // `define_himo` の max_values。 open 時に LockFreeCylinder が
    // min(max_values+1, DENSE_CAP) 個の bucket を **himo ごとに** 確保するので、
    // segment open とは別に himo 数 × max_values に比例するコストが乗る。
    let max_values: u32 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(1000);
    let base = format!("/tmp/enchu_opencost_{}", std::process::id());
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();

    println!("open cost bench — {n_db} db を 1 process で開く (entity {ENTS}、 max_values {max_values}、 REPS {REPS})");
    println!(
        "{:>6} {:>7} {:>11} {:>13} {:>12} {:>11} {:>10} {:>9}",
        "himo", "files", "open 1 db", "うち segment", "うち manifest", "1 file", "N db 逐次", "N db 並列"
    );

    for &himos in &himo_sweep {
        // 1 db で per-open の内訳を取る
        let one = format!("{base}/one_{himos}.db");
        build_db(&one, himos, max_values);
        let files = files_in(&one);
        let mut totals = vec![];
        let (mut seg_ns, mut man_ns, mut seg_n) = (0u64, 0u64, 0u64);
        for r in 0..REPS {
            enchudb_engine::segment_map::reset_stats();
            enchudb_engine::segments::reset_verify_stats();
            let t = Instant::now();
            std::hint::black_box(open_query_close(&one, himos));
            let el = t.elapsed();
            if r > 0 {
                // 1 回目は page cache を温めるだけ
                totals.push(ms(el));
                let (n, ns) = enchudb_engine::segment_map::open_stats();
                let (_, mns) = enchudb_engine::segments::verify_stats();
                seg_n = n;
                seg_ns += ns;
                man_ns += mns;
            }
        }
        let reps = (REPS - 1) as f64;
        let open_ms = median(totals);
        let seg_ms = seg_ns as f64 / reps / 1e6;
        let man_ms = man_ns as f64 / reps / 1e6;
        let per_file_us = if seg_n > 0 { seg_ms * 1e3 / seg_n as f64 } else { 0.0 };

        // N db を 1 process で開く (逐次 / 並列)
        let dbs: Vec<String> = (0..n_db).map(|i| format!("{base}/n{himos}_{i}.db")).collect();
        for d in &dbs {
            build_db(d, himos, max_values);
        }
        let mut seq = vec![];
        let mut par = vec![];
        for r in 0..REPS {
            let t = Instant::now();
            for d in &dbs {
                std::hint::black_box(open_query_close(d, himos));
            }
            let e_seq = ms(t.elapsed());
            let t = Instant::now();
            std::thread::scope(|s| {
                for d in &dbs {
                    s.spawn(move || std::hint::black_box(open_query_close(d, himos)));
                }
            });
            let e_par = ms(t.elapsed());
            if r > 0 {
                seq.push(e_seq);
                par.push(e_par);
            }
        }
        println!(
            "{himos:>6} {files:>7} {:>9.3} ms {:>11.3} ms {:>10.3} ms {:>9.1} µs {:>8.1} ms {:>8.1} ms",
            open_ms,
            seg_ms,
            man_ms,
            per_file_us,
            median(seq),
            median(par),
        );
        for d in dbs.iter().chain(std::iter::once(&one)) {
            let _ = enchudb::db_files::remove_db(d);
        }
    }
    let _ = std::fs::remove_dir_all(&base);
    println!(
        "\n1 file あたり = segment open の合計 / open 回数。 himo 0 の行が固定 segment + manifest の下限。\n\
         段差 (himo 0 → N) が lazy HimoStore (request23 案 D2) で消せる分。"
    );
}
