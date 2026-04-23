/// v24 vs v25 vs v26(ハイブリッド) ベンチマーク
///
/// EnchuDB の実 Engine を使って同一データ・同一クエリで比較。
/// v26 は「少ユニーク紐の組み合わせ事前計算 + Column直読み」。

use std::time::Instant;

fn main() {
    let n = 100_000u32;

    // --- v24 (現行 enchudb) ---
    println!("=== データ投入 (v24 Engine) ===");
    let dir24 = "/tmp/enchudb_v26_bench_v24.db";
    let _ = std::fs::remove_file(dir24);

    let mut db = enchudb::Engine::create_standalone(dir24).unwrap();

    // 少ユニーク紐を define_himo で prefix sum 有効化
    db.define_himo("city", enchudb::HimoType::Value, 3);
    db.define_himo("dept", enchudb::HimoType::Value, 4);
    db.define_himo("grade", enchudb::HimoType::Value, 5);
    // 多ユニーク紐は define なし（binary search）
    // price: 0..10000, age: 0..100

    let t = Instant::now();
    for i in 0..n {
        let e = db.entity();
        db.tie(e, "city", i % 3);
        db.tie(e, "dept", (i * 7) % 4);
        db.tie(e, "grade", (i * 13) % 5);
        db.tie(e, "price", (i * 31) % 10000);
        db.tie(e, "age", (i * 17) % 100);
    }
    let insert_time = t.elapsed();
    println!("投入: {}件, {:?}", n, insert_time);

    let t = Instant::now();
    db.rebuild();
    let rebuild_time = t.elapsed();
    println!("rebuild: {:?}", rebuild_time);

    // クエリ定義
    let queries: Vec<(&str, Vec<(&str, u32)>)> = vec![
        ("5条件", vec![("city", 1), ("dept", 2), ("grade", 3), ("price", 500), ("age", 30)]),
        ("3条件(少ユニークのみ)", vec![("city", 1), ("dept", 2), ("grade", 3)]),
        ("2条件(多ユニーク含)", vec![("city", 1), ("price", 500)]),
        ("1条件(price)", vec![("price", 500)]),
        ("2条件(少ユニーク)", vec![("city", 1), ("dept", 2)]),
    ];

    // --- v24 クエリベンチ ---
    println!();
    println!("=== v24 クエリ ===");
    let mut v24_results = vec![];
    for (name, q) in &queries {
        let refs: Vec<(&str, u32)> = q.iter().map(|&(s, v)| (s, v)).collect();
        // warmup
        let _ = db.query(&refs);

        let t = Instant::now();
        let iters = 1000;
        let mut count = 0;
        for _ in 0..iters {
            count = db.query(&refs).len();
        }
        let elapsed = t.elapsed() / iters;
        println!("  {}: {:?}, {}件", name, elapsed, count);
        v24_results.push((name, elapsed, count));
    }

    // --- v26 ハイブリッド ---
    println!();
    println!("=== v26 ハイブリッド構築 ===");

    // 少ユニーク紐の組み合わせテーブル構築
    let city_card = 3u32;
    let dept_card = 4u32;
    let grade_card = 5u32;
    let total_combos = city_card * dept_card * grade_card;

    let t = Instant::now();
    let mut combo_table: Vec<Vec<u64>> = vec![vec![]; total_combos as usize];
    for eid in 0..n as u64 {
        let city = db.get(eid, "city").unwrap_or(u32::MAX);
        let dept = db.get(eid, "dept").unwrap_or(u32::MAX);
        let grade = db.get(eid, "grade").unwrap_or(u32::MAX);
        if city < city_card && dept < dept_card && grade < grade_card {
            let combo_id = city * dept_card * grade_card + dept * grade_card + grade;
            combo_table[combo_id as usize].push(eid);
        }
    }
    let build_combo = t.elapsed();
    let combo_mem: usize = combo_table.iter().map(|v| v.len() * 8 + 24).sum();
    println!("構築: {:?}, 組み合わせ: {}, メモリ: {} KB", build_combo, total_combos, combo_mem / 1024);

    println!();
    println!("=== v26 クエリ ===");
    let mut v26_results = vec![];

    for (name, q) in &queries {
        // 条件を少ユニーク/多ユニークに分離
        let low_card = ["city", "dept", "grade"];
        let mut has_city = None;
        let mut has_dept = None;
        let mut has_grade = None;
        let mut other_conds: Vec<(&str, u32)> = vec![];

        for &(himo, val) in q {
            match himo {
                "city" => has_city = Some(val),
                "dept" => has_dept = Some(val),
                "grade" => has_grade = Some(val),
                _ => other_conds.push((himo, val)),
            }
        }

        // warmup
        let _ = v26_query(&db, &combo_table, dept_card, grade_card,
            has_city, has_dept, has_grade, &other_conds);

        let t = Instant::now();
        let iters = 1000;
        let mut count = 0;
        for _ in 0..iters {
            count = v26_query(&db, &combo_table, dept_card, grade_card,
                has_city, has_dept, has_grade, &other_conds);
        }
        let elapsed = t.elapsed() / iters;
        println!("  {}: {:?}, {}件", name, elapsed, count);
        v26_results.push((name, elapsed, count));
    }

    // --- まとめ ---
    println!();
    println!("=== まとめ ===");
    println!("| クエリ | v24 | v26 | 倍率 |");
    println!("|---|---|---|---|");
    for i in 0..queries.len() {
        let (name, t24, c24) = v24_results[i];
        let (_, t26, c26) = v26_results[i];
        let speedup = t24.as_nanos() as f64 / t26.as_nanos().max(1) as f64;
        println!("| {} | {:?} ({}件) | {:?} ({}件) | {:.1}x |",
            name, t24, c24, t26, c26, speedup);
    }
}

fn v26_query(
    db: &enchudb::Engine,
    combo_table: &[Vec<u64>],
    dept_card: u32,
    grade_card: u32,
    city: Option<u32>,
    dept: Option<u32>,
    grade: Option<u32>,
    other_conds: &[(&str, u32)],
) -> usize {
    // 全少ユニーク条件が揃ってる → combo テーブルから O(1)
    if let (Some(c), Some(d), Some(g)) = (city, dept, grade) {
        let combo_id = c * dept_card * grade_card + d * grade_card + g;
        let candidates = &combo_table[combo_id as usize];

        if other_conds.is_empty() {
            return candidates.len();
        }

        // 候補に対して Column 直読み
        let mut count = 0;
        for &eid in candidates {
            let mut pass = true;
            for &(himo, val) in other_conds {
                if db.get(eid, himo) != Some(val) {
                    pass = false;
                    break;
                }
            }
            if pass { count += 1; }
        }
        return count;
    }

    // 少ユニーク条件が部分的 → 通常の query にフォールバック
    let mut all_conds: Vec<(&str, u32)> = vec![];
    if let Some(v) = city { all_conds.push(("city", v)); }
    if let Some(v) = dept { all_conds.push(("dept", v)); }
    if let Some(v) = grade { all_conds.push(("grade", v)); }
    all_conds.extend_from_slice(other_conds);
    db.query(&all_conds).len()
}
