/// 量子円柱 v26 実験3 — ハイブリッド戦略
///
/// ユニーク数が少ない紐 → 組み合わせ事前計算 O(1)
/// ユニーク数が多い紐 → Column 直読みフィルタ O(k)
/// 従来のビットマップ AND と比較。

use std::time::Instant;

fn main() {
    let n_entities: u32 = 1_000_000;

    // 紐の定義
    // 少ユニーク: city(3), dept(4), grade(5) → 組み合わせ 60
    // 多ユニーク: price(10000), age(100)
    let city_card = 3u32;
    let dept_card = 4u32;
    let grade_card = 5u32;
    let price_card = 10_000u32;
    let age_card = 100u32;

    println!("entities: {}", n_entities);
    println!("少ユニーク: city({}), dept({}), grade({}) → 組み合わせ {}",
        city_card, dept_card, grade_card, city_card * dept_card * grade_card);
    println!("多ユニーク: price({}), age({})", price_card, age_card);
    println!();

    // データ生成
    let cities: Vec<u32> = (0..n_entities).map(|i| i % city_card).collect();
    let depts: Vec<u32> = (0..n_entities).map(|i| (i * 7) % dept_card).collect();
    let grades: Vec<u32> = (0..n_entities).map(|i| (i * 13) % grade_card).collect();
    let prices: Vec<u32> = (0..n_entities).map(|i| (i * 31) % price_card).collect();
    let ages: Vec<u32> = (0..n_entities).map(|i| (i * 17) % age_card).collect();

    // クエリ: city=1 AND dept=2 AND grade=3 AND price=500 AND age=30
    let q_city = 1u32;
    let q_dept = 2u32;
    let q_grade = 3u32;
    let q_price = 500u32;
    let q_age = 30u32;

    // === 方式1: 全条件ビットマップ AND ===
    println!("=== 方式1: 全条件ビットマップ AND ===");
    let bmp_words = (n_entities as usize + 63) / 64;

    let t = Instant::now();
    let mut city_bmps = vec![vec![0u64; bmp_words]; city_card as usize];
    let mut dept_bmps = vec![vec![0u64; bmp_words]; dept_card as usize];
    let mut grade_bmps = vec![vec![0u64; bmp_words]; grade_card as usize];
    let mut price_bmps = vec![vec![0u64; bmp_words]; price_card as usize];
    let mut age_bmps = vec![vec![0u64; bmp_words]; age_card as usize];
    for i in 0..n_entities {
        let w = (i / 64) as usize;
        let b = i % 64;
        city_bmps[cities[i as usize] as usize][w] |= 1u64 << b;
        dept_bmps[depts[i as usize] as usize][w] |= 1u64 << b;
        grade_bmps[grades[i as usize] as usize][w] |= 1u64 << b;
        price_bmps[prices[i as usize] as usize][w] |= 1u64 << b;
        age_bmps[ages[i as usize] as usize][w] |= 1u64 << b;
    }
    let build1 = t.elapsed();

    let t = Instant::now();
    let mut result = vec![!0u64; bmp_words];
    for w in 0..bmp_words {
        result[w] = city_bmps[q_city as usize][w]
            & dept_bmps[q_dept as usize][w]
            & grade_bmps[q_grade as usize][w]
            & price_bmps[q_price as usize][w]
            & age_bmps[q_age as usize][w];
    }
    let count1: u32 = result.iter().map(|w| w.count_ones()).sum();
    let query1 = t.elapsed();
    let mem1 = (city_card + dept_card + grade_card + price_card + age_card) as usize * bmp_words * 8;
    println!("構築: {:?}", build1);
    println!("クエリ: {:?}, 結果: {}件", query1, count1);
    println!("メモリ: {} MB", mem1 / 1024 / 1024);

    // === 方式2: ハイブリッド（組み合わせ + Column直読み）===
    println!();
    println!("=== 方式2: ハイブリッド ===");
    println!("  少ユニーク(city,dept,grade) → フラット配列 O(1)");
    println!("  多ユニーク(price,age) → Column直読みフィルタ");

    let total_combos = city_card * dept_card * grade_card;
    let t = Instant::now();
    // 組み合わせテーブル: entity リスト
    let mut combo_table: Vec<Vec<u32>> = vec![vec![]; total_combos as usize];
    for i in 0..n_entities {
        let combo_id = cities[i as usize] * dept_card * grade_card
            + depts[i as usize] * grade_card
            + grades[i as usize];
        combo_table[combo_id as usize].push(i);
    }
    let build2 = t.elapsed();

    let t = Instant::now();
    // Step 1: 組み合わせテーブルから候補取得 O(1)
    let combo_id = q_city * dept_card * grade_card + q_dept * grade_card + q_grade;
    let candidates = &combo_table[combo_id as usize];

    // Step 2: 候補に対して Column 直読みフィルタ O(k)
    let mut results2: Vec<u32> = Vec::new();
    for &eid in candidates {
        if prices[eid as usize] == q_price && ages[eid as usize] == q_age {
            results2.push(eid);
        }
    }
    let count2 = results2.len();
    let query2 = t.elapsed();
    let mem2: usize = combo_table.iter().map(|v| v.len() * 4 + 24).sum();
    println!("構築: {:?}", build2);
    println!("クエリ: {:?}, 結果: {}件", query2, count2);
    println!("  候補(combo): {}件 → フィルタ後: {}件", candidates.len(), count2);
    println!("メモリ: {} MB", mem2 / 1024 / 1024);

    // === 方式3: 最小スライス + Column直読み（現行EnchuDB相当）===
    println!();
    println!("=== 方式3: 最小スライス + Column直読み（現行相当）===");

    // 各条件のヒット数を調べて最小を選ぶ
    let city_count = cities.iter().filter(|&&v| v == q_city).count();
    let dept_count = depts.iter().filter(|&&v| v == q_dept).count();
    let grade_count = grades.iter().filter(|&&v| v == q_grade).count();
    let price_count = prices.iter().filter(|&&v| v == q_price).count();
    let age_count = ages.iter().filter(|&&v| v == q_age).count();

    println!("  各条件ヒット数: city={}, dept={}, grade={}, price={}, age={}",
        city_count, dept_count, grade_count, price_count, age_count);

    // price が最小スライスになるはず
    let t = Instant::now();
    // price=500 の entity を取得（Cylinder スライス相当）
    let price_slice: Vec<u32> = (0..n_entities)
        .filter(|&i| prices[i as usize] == q_price)
        .collect();

    // Column 直読みフィルタ
    let mut results3: Vec<u32> = Vec::new();
    for &eid in &price_slice {
        if cities[eid as usize] == q_city
            && depts[eid as usize] == q_dept
            && grades[eid as usize] == q_grade
            && ages[eid as usize] == q_age
        {
            results3.push(eid);
        }
    }
    let count3 = results3.len();
    let query3 = t.elapsed();
    println!("クエリ: {:?}, 結果: {}件", query3, count3);
    println!("  候補(price slice): {}件 → フィルタ後: {}件", price_slice.len(), count3);

    // === 検証 ===
    println!();
    println!("=== 検証 ===");
    println!("全方式一致: {}", count1 as usize == count2 && count2 == count3);

    // === まとめ ===
    println!();
    println!("=== まとめ ===");
    println!("| 方式 | 構築 | クエリ | メモリ |");
    println!("|---|---|---|---|");
    println!("| 全ビットマップAND | {:?} | {:?} | {} MB |", build1, query1, mem1 / 1024 / 1024);
    println!("| ハイブリッド | {:?} | {:?} | {} MB |", build2, query2, mem2 / 1024 / 1024);
    println!("| 最小スライス(現行) | - | {:?} | - |", query3);
}
