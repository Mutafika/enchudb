//! ローカル standalone で「ns 級」 操作を分離して測る。
//!
//! 大規模 query (結果 20K eids) は Vec 構築コストで μs に見えるが、
//! per-element に直すと 0.5 ns / eid 帯。 ここでは:
//!  - 単一 lookup (= 真の ns 性能)
//!  - 結果の小さい複合クエリ (= 実用 SaaS/SNS のクエリ規模)
//! を測る。

use enchudb_engine::{Engine, HimoType};
use std::time::Instant;

fn main() {
    let path = "/tmp/enchu_local_ns_bench.db";
    let _ = std::fs::remove_file(path);

    let mut eng = Engine::create_standalone(path).unwrap();
    // 結果集合が「小さく」 なるよう cardinality を高めに取る
    eng.define_himo("user_id", HimoType::Number, 100_000); // 1M / 100K = 10 件/user
    eng.define_himo("year",    HimoType::Number, 10);      // 10 値
    eng.define_himo("city",    HimoType::Tag, 0);

    let n = 1_000_000u32;
    for i in 0..n {
        let e = eng.entity();
        eng.tie(e, "user_id", i % 100_000);
        eng.tie(e, "year",    i % 10);
        eng.tie_text(e, "city", &format!("city_{}", i % 100));
    }
    eng.rebuild();

    let city_0_vid = eng.vocab_id("city_0").unwrap();
    let iters = 1_000_000u32;

    // ──── 1. 単一 eid get ────
    let t = Instant::now();
    let mut sum = 0u64;
    for i in 0..iters {
        let eid = enchudb_wal::make_eid(eng.peer_id(), (i % n) as u32);
        if let Some(v) = eng.get(eid, "user_id") {
            sum = sum.wrapping_add(v as u64);
        }
    }
    let get_ns = t.elapsed().as_nanos() as f64 / iters as f64;

    // ──── 2. 1cond query: user_id=42 → 10 件 ────
    // result set 小さいので per-call も ns 帯であるはず
    let t = Instant::now();
    let mut total = 0u64;
    for _ in 0..iters {
        let v = eng.pull_raw("user_id", 42);
        total = total.wrapping_add(v.len() as u64);
    }
    let q1_ns = t.elapsed().as_nanos() as f64 / iters as f64;

    // ──── 3. 2cond query: user_id=42 AND year=3 → 1 件程度 ────
    let q2_iters = 1_000_000u32;
    let t = Instant::now();
    let mut hits = 0u64;
    for _ in 0..q2_iters {
        let v = eng.query(&[("user_id", 42), ("year", 3)]);
        hits = hits.wrapping_add(v.len() as u64);
    }
    let q2_ns = t.elapsed().as_nanos() as f64 / q2_iters as f64;

    // ──── 4. 3cond query: user_id=42 AND year=3 AND city=city_0 → 0〜1 件 ────
    let t = Instant::now();
    let mut h3 = 0u64;
    for _ in 0..q2_iters {
        let v = eng.query(&[("user_id", 42), ("year", 3), ("city", city_0_vid)]);
        h3 = h3.wrapping_add(v.len() as u64);
    }
    let q3_ns = t.elapsed().as_nanos() as f64 / q2_iters as f64;

    // ──── 5. 巨大 result の 1cond query: year=0 → 100K 件 ────
    let big_iters = 10_000u32;
    let t = Instant::now();
    let mut big = 0u64;
    for _ in 0..big_iters {
        let v = eng.pull_raw("year", 0);
        big = big.wrapping_add(v.len() as u64);
    }
    let big_ns = t.elapsed().as_nanos() as f64 / big_iters as f64;
    let big_per_eid = big_ns / 100_000.0;

    // ──── 6. 実用パターン: 1000 行 × 5 attr 取得 (社員一覧 UI 想定) ────
    // year=0 から先頭 1000 eid を取って、 各行 5 attr を get。
    let eids_for_listing = {
        let mut v = eng.pull_raw("year", 0);
        v.truncate(1000);
        v
    };
    let list_iters = 1_000u32;
    let t = Instant::now();
    let mut acc = 0u64;
    for _ in 0..list_iters {
        for &eid in &eids_for_listing {
            // 5 attr 取得 = 1 行の display 想定
            if let Some(v) = eng.get(eid, "user_id") { acc = acc.wrapping_add(v as u64); }
            if let Some(v) = eng.get(eid, "year")    { acc = acc.wrapping_add(v as u64); }
            if let Some(_) = eng.get_text(eid, "city") { acc = acc.wrapping_add(1); }
            // year を 2 度引いて 5 attr 相当に
            if let Some(v) = eng.get(eid, "year")    { acc = acc.wrapping_add(v as u64); }
            if let Some(v) = eng.get(eid, "user_id") { acc = acc.wrapping_add(v as u64); }
        }
    }
    let list_total = t.elapsed();
    let list_per_call = list_total.as_micros() as f64 / list_iters as f64;
    let list_per_attr = list_total.as_nanos() as f64 / (list_iters as f64 * 1000.0 * 5.0);

    eprintln!("=== local standalone ns-level bench (1M entities) ===");
    eprintln!("");
    eprintln!("--- 真の ns 級 (単一 lookup) ---");
    eprintln!("get(eid, \"user_id\")               : {:>8.1} ns/op     sum={sum}", get_ns);
    eprintln!("");
    eprintln!("--- 結果が小さい実用クエリ ---");
    eprintln!("1cond: user_id=42 (10 件 hit)     : {:>8.1} ns/op     total={total}", q1_ns);
    eprintln!("2cond: user_id=42 AND year=3      : {:>8.1} ns/op     hits={hits}", q2_ns);
    eprintln!("3cond: + city=city_0              : {:>8.1} ns/op     h3={h3}", q3_ns);
    eprintln!("");
    eprintln!("--- 結果が巨大 (100K 件) ---");
    eprintln!("1cond: year=0                     : {:>8.1} ns/op     ≈ {:.2} ns/eid", big_ns, big_per_eid);
    eprintln!("");
    eprintln!("--- 社員一覧 UI 想定 (1000 行 × 5 attr 取得) ---");
    eprintln!("1 list 描画分                     : {:>8.1} μs/call   ≈ {:.1} ns/attr  acc={acc}",
        list_per_call, list_per_attr);
    eprintln!("");
    eprintln!("(= 大規模 result は Vec 構築 0.5 ns/eid 律速、 ns 性能は維持されてる)");

    let _ = std::fs::remove_file(path);
}
