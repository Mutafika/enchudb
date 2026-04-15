/// v27 ベンチマーク — BucketCylinder の write/pull/query 速度。
///
/// 100万 entity × 7 紐。define_himo で max_values 指定済みの紐に対して
/// insert/slice_one/query を計測する。
///
/// 実行:
///   cargo run --release --example v27_bench --features v27

use std::time::Instant;

fn main() {
    let n = 1_000_000u32;

    let path = "/tmp/enchudb_v27_bench.db";
    let _ = std::fs::remove_file(path);

    let mut db = enchudb::Engine::create_with_capacity(path, n + 10).unwrap();

    // 7 紐。tenant/dept/status/grade/city/salary_band/age
    db.define_himo("tenant", enchudb::HimoType::Value, 16);
    db.define_himo("dept", enchudb::HimoType::Value, 8);
    db.define_himo("status", enchudb::HimoType::Value, 4);
    db.define_himo("grade", enchudb::HimoType::Value, 5);
    db.define_himo("city", enchudb::HimoType::Value, 10);
    db.define_himo("salary_band", enchudb::HimoType::Value, 20);
    db.define_himo("age", enchudb::HimoType::Value, 100);

    // ──── write ────
    println!("=== 書き込み ({} entity × 7 紐) ===", n);
    let t = Instant::now();
    for i in 0..n {
        let e = db.entity();
        db.tie(e, "tenant", i % 16);
        db.tie(e, "dept", (i * 7) % 8);
        db.tie(e, "status", (i * 3) % 4);
        db.tie(e, "grade", (i * 11) % 5);
        db.tie(e, "city", (i * 13) % 10);
        db.tie(e, "salary_band", (i * 17) % 20);
        db.tie(e, "age", (i * 19) % 100);
    }
    let elapsed = t.elapsed();
    let per_tie = elapsed.as_nanos() as f64 / (n as f64 * 7.0);
    println!("  総時間: {:?}", elapsed);
    println!("  tie 1件あたり: {:.1} ns", per_tie);

    // ──── rebuild 呼ばずに即クエリが通るか検証 ────
    println!();
    println!("=== rebuild 呼ばず即クエリ ===");
    let probe: &[(&str, u32)] = &[("tenant", 3), ("dept", 5), ("status", 1)];
    let no_rebuild_count = db.query(probe).len();
    let no_rebuild_pull = db.pull_raw("tenant", 3).len();
    println!("  query [tenant=3, dept=5, status=1]: {}件", no_rebuild_count);
    println!("  pull_raw tenant=3: {}件", no_rebuild_pull);

    // v27 では rebuild 不要。呼んでも noop。
    let t = Instant::now();
    db.rebuild();
    println!("  rebuild (noop): {:?}", t.elapsed());

    let after_rebuild_count = db.query(probe).len();
    let after_rebuild_pull = db.pull_raw("tenant", 3).len();
    assert_eq!(no_rebuild_count, after_rebuild_count, "rebuild 前後で query 結果が一致すること");
    assert_eq!(no_rebuild_pull, after_rebuild_pull, "rebuild 前後で pull_raw 結果が一致すること");
    println!("  rebuild 前後で結果一致: OK");

    // ──── pull_raw (1条件) ────
    println!();
    println!("=== pull_raw (1条件) ===");
    let queries_1: &[(&str, u32)] = &[
        ("tenant", 3),
        ("dept", 2),
        ("status", 1),
        ("age", 30),
    ];
    for &(himo, val) in queries_1 {
        // warmup
        let _ = db.pull_raw(himo, val);
        let iters = 10_000;
        let t = Instant::now();
        let mut total = 0usize;
        for _ in 0..iters {
            total += db.pull_raw(himo, val).len();
        }
        let elapsed = t.elapsed() / iters;
        println!("  {}={}: {:?}, {}件", himo, val, elapsed, total / iters as usize);
    }

    // ──── query (複数条件) ────
    println!();
    println!("=== query (複数条件) ===");
    // 実データに合わせて、ヒットする値を狙う。
    // i=3: tenant=3, dept=(21)%8=5, status=(9)%4=1, grade=(33)%5=3, city=(39)%10=9, salary=51%20=11, age=57%100=57
    let queries: Vec<(&str, Vec<(&str, u32)>)> = vec![
        ("2条件", vec![("tenant", 3), ("dept", 5)]),
        ("3条件", vec![("tenant", 3), ("dept", 5), ("status", 1)]),
        ("5条件", vec![("tenant", 3), ("dept", 5), ("status", 1), ("grade", 3), ("city", 9)]),
        ("7条件", vec![("tenant", 3), ("dept", 5), ("status", 1), ("grade", 3), ("city", 9), ("salary_band", 11), ("age", 57)]),
        ("ミス", vec![("tenant", 99), ("dept", 99)]),
    ];
    for (name, q) in &queries {
        let refs: Vec<(&str, u32)> = q.iter().map(|&(s, v)| (s, v)).collect();
        let _ = db.query(&refs);
        let iters = 1000;
        let t = Instant::now();
        let mut count = 0;
        for _ in 0..iters {
            count = db.query(&refs).len();
        }
        let elapsed = t.elapsed() / iters;
        println!("  {}: {:?}, {}件", name, elapsed, count);
    }

    // ──── open 後の読み込み検証 ────
    println!();
    println!("=== open 後の読み込み ===");
    // 書き込み時のクエリ結果を記録
    let pre_counts: Vec<(String, usize)> = queries_1.iter()
        .map(|&(h, v)| (format!("{}={}", h, v), db.pull_raw(h, v).len()))
        .collect();
    let pre_query_counts: Vec<(String, usize)> = queries.iter()
        .map(|(name, q)| {
            let refs: Vec<(&str, u32)> = q.iter().map(|&(s, v)| (s, v)).collect();
            (name.to_string(), db.query(&refs).len())
        })
        .collect();

    db.flush().unwrap();
    drop(db);

    // open
    let t = Instant::now();
    let db2 = enchudb::Engine::open(path).unwrap();
    let open_elapsed = t.elapsed();
    println!("  open + BucketCylinder 再構築: {:?} ({} entity × 7 紐)", open_elapsed, n);

    // rebuild 呼ばず、そのまま pull_raw / query できるか検証
    println!();
    println!("  --- open 直後、rebuild 呼ばずに pull_raw ---");
    for (name, pre_count) in &pre_counts {
        let parts: Vec<&str> = name.split('=').collect();
        let h = parts[0];
        let v: u32 = parts[1].parse().unwrap();
        let post_count = db2.pull_raw(h, v).len();
        let ok = if *pre_count == post_count { "OK" } else { "NG" };
        println!("    {}: {}件 (元: {}件) {}", name, post_count, pre_count, ok);
        assert_eq!(*pre_count, post_count, "open 前後で pull_raw 結果一致");
    }

    println!();
    println!("  --- open 直後、rebuild 呼ばずに query ---");
    for (name, pre_count) in &pre_query_counts {
        let q: &Vec<(&str, u32)> = &queries.iter().find(|(n, _)| n == name).unwrap().1;
        let refs: Vec<(&str, u32)> = q.iter().map(|&(s, v)| (s, v)).collect();
        let post_count = db2.query(&refs).len();
        let ok = if *pre_count == post_count { "OK" } else { "NG" };
        println!("    {}: {}件 (元: {}件) {}", name, post_count, pre_count, ok);
        assert_eq!(*pre_count, post_count, "open 前後で query 結果一致");
    }

    // open 後の読み込み速度
    println!();
    println!("  --- open 後の pull_raw 速度 ---");
    for &(himo, val) in queries_1 {
        let _ = db2.pull_raw(himo, val);
        let iters = 10_000;
        let t = Instant::now();
        let mut total = 0usize;
        for _ in 0..iters {
            total += db2.pull_raw(himo, val).len();
        }
        let elapsed = t.elapsed() / iters;
        println!("    {}={}: {:?}, {}件", himo, val, elapsed, total / iters as usize);
    }

    println!();
    println!("  --- open 後の query 速度 ---");
    for (name, q) in &queries {
        let refs: Vec<(&str, u32)> = q.iter().map(|&(s, v)| (s, v)).collect();
        let _ = db2.query(&refs);
        let iters = 1000;
        let t = Instant::now();
        let mut count = 0;
        for _ in 0..iters {
            count = db2.query(&refs).len();
        }
        let elapsed = t.elapsed() / iters;
        println!("    {}: {:?}, {}件", name, elapsed, count);
    }

    drop(db2);
    let _ = std::fs::remove_file(path);

    // ════════════════════════════════════════════════════════════════════
    // n-tuple 観測窓ベンチ: 宣言あり vs なし
    // ════════════════════════════════════════════════════════════════════
    println!();
    println!("═══════════════════════════════════════════════════════");
    println!("  n-tuple 観測窓(NTupleView)ベンチマーク");
    println!("═══════════════════════════════════════════════════════");

    // ── (1) 宣言なし(ベースライン、PairTable のみ) ──
    println!();
    println!("─── (1) 宣言なし(PairTable のみ) ───");
    let path_a = "/tmp/enchudb_v27_bench_nov.db";
    let _ = std::fs::remove_file(path_a);
    let mut db_a = enchudb::Engine::create_with_capacity(path_a, n + 10).unwrap();
    db_a.define_himo("tenant", enchudb::HimoType::Value, 16);
    db_a.define_himo("dept", enchudb::HimoType::Value, 8);
    db_a.define_himo("status", enchudb::HimoType::Value, 4);
    db_a.define_himo("grade", enchudb::HimoType::Value, 5);
    db_a.define_himo("city", enchudb::HimoType::Value, 10);
    db_a.define_himo("salary_band", enchudb::HimoType::Value, 20);
    db_a.define_himo("age", enchudb::HimoType::Value, 100);

    let t = Instant::now();
    for i in 0..n {
        let e = db_a.entity();
        db_a.tie(e, "tenant", i % 16);
        db_a.tie(e, "dept", (i * 7) % 8);
        db_a.tie(e, "status", (i * 3) % 4);
        db_a.tie(e, "grade", (i * 11) % 5);
        db_a.tie(e, "city", (i * 13) % 10);
        db_a.tie(e, "salary_band", (i * 17) % 20);
        db_a.tie(e, "age", (i * 19) % 100);
    }
    let tie_ns_none = t.elapsed().as_nanos() as f64 / (n as f64 * 7.0);
    println!("  tie: {:.1} ns/件", tie_ns_none);

    let bench_queries: Vec<(&str, Vec<(&str, u32)>)> = vec![
        ("3条件", vec![("tenant", 3), ("dept", 5), ("status", 1)]),
        ("5条件", vec![("tenant", 3), ("dept", 5), ("status", 1), ("grade", 3), ("city", 9)]),
        ("7条件", vec![("tenant", 3), ("dept", 5), ("status", 1), ("grade", 3), ("city", 9), ("salary_band", 11), ("age", 57)]),
    ];

    let mut results_none: Vec<(String, u128)> = Vec::new();
    for (name, q) in &bench_queries {
        let refs: Vec<(&str, u32)> = q.iter().map(|&(s, v)| (s, v)).collect();
        let _ = db_a.query(&refs);
        let iters = 1000u32;
        let t = Instant::now();
        let mut count = 0;
        for _ in 0..iters {
            count = db_a.query(&refs).len();
        }
        let ns = t.elapsed().as_nanos() / iters as u128;
        println!("  {}: {} ns ({}件)", name, ns, count);
        results_none.push((name.to_string(), ns));
    }
    drop(db_a);
    let _ = std::fs::remove_file(path_a);

    // ── (2) 宣言 1 個(3紐観測窓) ──
    println!();
    println!("─── (2) 宣言 1 個: tenant+dept+status ───");
    let path_b = "/tmp/enchudb_v27_bench_v1.db";
    let _ = std::fs::remove_file(path_b);
    let mut db_b = enchudb::Engine::create_with_capacity(path_b, n + 10).unwrap();
    db_b.define_himo("tenant", enchudb::HimoType::Value, 16);
    db_b.define_himo("dept", enchudb::HimoType::Value, 8);
    db_b.define_himo("status", enchudb::HimoType::Value, 4);
    db_b.define_himo("grade", enchudb::HimoType::Value, 5);
    db_b.define_himo("city", enchudb::HimoType::Value, 10);
    db_b.define_himo("salary_band", enchudb::HimoType::Value, 20);
    db_b.define_himo("age", enchudb::HimoType::Value, 100);
    db_b.register_tuple_view(&["tenant", "dept", "status"]).unwrap();

    let t = Instant::now();
    for i in 0..n {
        let e = db_b.entity();
        db_b.tie(e, "tenant", i % 16);
        db_b.tie(e, "dept", (i * 7) % 8);
        db_b.tie(e, "status", (i * 3) % 4);
        db_b.tie(e, "grade", (i * 11) % 5);
        db_b.tie(e, "city", (i * 13) % 10);
        db_b.tie(e, "salary_band", (i * 17) % 20);
        db_b.tie(e, "age", (i * 19) % 100);
    }
    let tie_ns_1 = t.elapsed().as_nanos() as f64 / (n as f64 * 7.0);
    println!("  tie: {:.1} ns/件", tie_ns_1);

    let mut results_1: Vec<(String, u128)> = Vec::new();
    for (name, q) in &bench_queries {
        let refs: Vec<(&str, u32)> = q.iter().map(|&(s, v)| (s, v)).collect();
        let _ = db_b.query(&refs);
        let iters = 1000u32;
        let t = Instant::now();
        let mut count = 0;
        for _ in 0..iters {
            count = db_b.query(&refs).len();
        }
        let ns = t.elapsed().as_nanos() / iters as u128;
        println!("  {}: {} ns ({}件)", name, ns, count);
        results_1.push((name.to_string(), ns));
    }
    drop(db_b);
    let _ = std::fs::remove_file(path_b);

    // ── (3) 宣言 2 個(3紐 + 5紐) ──
    println!();
    println!("─── (3) 宣言 2 個: 3紐 + 5紐観測窓 ───");
    let path_c = "/tmp/enchudb_v27_bench_v2.db";
    let _ = std::fs::remove_file(path_c);
    let mut db_c = enchudb::Engine::create_with_capacity(path_c, n + 10).unwrap();
    db_c.define_himo("tenant", enchudb::HimoType::Value, 16);
    db_c.define_himo("dept", enchudb::HimoType::Value, 8);
    db_c.define_himo("status", enchudb::HimoType::Value, 4);
    db_c.define_himo("grade", enchudb::HimoType::Value, 5);
    db_c.define_himo("city", enchudb::HimoType::Value, 10);
    db_c.define_himo("salary_band", enchudb::HimoType::Value, 20);
    db_c.define_himo("age", enchudb::HimoType::Value, 100);
    // 17*9*5 = 765
    db_c.register_tuple_view(&["tenant", "dept", "status"]).unwrap();
    // 17*9*5*6*11 = 50490
    db_c.register_tuple_view(&["tenant", "dept", "status", "grade", "city"]).unwrap();

    let t = Instant::now();
    for i in 0..n {
        let e = db_c.entity();
        db_c.tie(e, "tenant", i % 16);
        db_c.tie(e, "dept", (i * 7) % 8);
        db_c.tie(e, "status", (i * 3) % 4);
        db_c.tie(e, "grade", (i * 11) % 5);
        db_c.tie(e, "city", (i * 13) % 10);
        db_c.tie(e, "salary_band", (i * 17) % 20);
        db_c.tie(e, "age", (i * 19) % 100);
    }
    let tie_ns_2 = t.elapsed().as_nanos() as f64 / (n as f64 * 7.0);
    println!("  tie: {:.1} ns/件", tie_ns_2);

    let mut results_2: Vec<(String, u128)> = Vec::new();
    for (name, q) in &bench_queries {
        let refs: Vec<(&str, u32)> = q.iter().map(|&(s, v)| (s, v)).collect();
        let _ = db_c.query(&refs);
        let iters = 1000u32;
        let t = Instant::now();
        let mut count = 0;
        for _ in 0..iters {
            count = db_c.query(&refs).len();
        }
        let ns = t.elapsed().as_nanos() / iters as u128;
        println!("  {}: {} ns ({}件)", name, ns, count);
        results_2.push((name.to_string(), ns));
    }
    drop(db_c);
    let _ = std::fs::remove_file(path_c);

    // ── サマリ ──
    println!();
    println!("─── サマリ(query) ───");
    println!("  {:<8} {:>14} {:>14} {:>14}", "", "宣言なし", "宣言1個(3紐)", "宣言2個(3+5紐)");
    for ((n_none, ns_none), ((n1, ns1), (_n2, ns2))) in results_none.iter()
        .zip(results_1.iter().zip(results_2.iter())) {
        let _ = n1;
        let imp1 = *ns_none as f64 / *ns1 as f64;
        let imp2 = *ns_none as f64 / *ns2 as f64;
        println!("  {:<8} {:>10} ns {:>8} ns ({:.1}x) {:>8} ns ({:.1}x)",
            n_none, ns_none, ns1, imp1, ns2, imp2);
    }
    println!();
    println!("─── サマリ(tie) ───");
    println!("  宣言なし:    {:.1} ns/件", tie_ns_none);
    println!("  宣言1個(3紐): {:.1} ns/件", tie_ns_1);
    println!("  宣言2個(3+5紐): {:.1} ns/件", tie_ns_2);

    println!();
    println!("=== 制約 ===");
    println!("  並行性: v27 v1 は単一スレッド前提(BucketCylinder は &mut self)");
    println!("  bitmap/delta: 撤去済み(BucketCylinder で O(1) 即時反映)");
    println!("  range: range_iter は全幅探索のみ、索引最適化なし");
    println!("  n-tuple 観測窓: max_values > 0 必須、セル数 100万超は register エラー");
    println!("  観測窓の永続化: 非対応(実行時のみ、open 後に register_tuple_view を再実行)");
}
