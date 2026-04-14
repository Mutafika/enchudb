/// 量子円柱 v26 実験2 — 組み合わせ事前計算
///
/// 紐ごとのユニーク数が小さい場合、全組み合わせの entity リストを
/// 事前に持っておけば交差なしで O(1) クエリが可能か検証する。

use std::collections::HashMap;
use std::time::Instant;

fn main() {
    let n_entities: u32 = 1_000_000;

    // 3つの紐: city(3通り), dept(4通り), grade(5通り)
    // 組み合わせ: 3 * 4 * 5 = 60通り
    let city_card = 3u32;
    let dept_card = 4u32;
    let grade_card = 5u32;
    let total_combos = city_card * dept_card * grade_card;

    println!("entities: {}", n_entities);
    println!("city: {}通り, dept: {}通り, grade: {}通り", city_card, dept_card, grade_card);
    println!("組み合わせ: {}通り", total_combos);
    println!();

    // データ生成
    let cities: Vec<u32> = (0..n_entities).map(|i| i % city_card).collect();
    let depts: Vec<u32> = (0..n_entities).map(|i| (i * 7) % dept_card).collect();
    let grades: Vec<u32> = (0..n_entities).map(|i| (i * 13) % grade_card).collect();

    // === 方式1: ビットマップ AND (従来) ===
    println!("=== 方式1: ビットマップ AND ===");

    // ビットマップ構築
    let bmp_words = (n_entities as usize + 63) / 64;

    let t = Instant::now();
    let mut city_bmps: Vec<Vec<u64>> = vec![vec![0u64; bmp_words]; city_card as usize];
    let mut dept_bmps: Vec<Vec<u64>> = vec![vec![0u64; bmp_words]; dept_card as usize];
    let mut grade_bmps: Vec<Vec<u64>> = vec![vec![0u64; bmp_words]; grade_card as usize];

    for i in 0..n_entities {
        let word = (i / 64) as usize;
        let bit = i % 64;
        city_bmps[cities[i as usize] as usize][word] |= 1u64 << bit;
        dept_bmps[depts[i as usize] as usize][word] |= 1u64 << bit;
        grade_bmps[grades[i as usize] as usize][word] |= 1u64 << bit;
    }
    let build_bmp = t.elapsed();
    println!("ビットマップ構築: {:?}", build_bmp);
    println!("メモリ: {} KB", (city_card + dept_card + grade_card) as usize * bmp_words * 8 / 1024);

    // クエリ: city=1 AND dept=2 AND grade=3
    let q_city = 1u32;
    let q_dept = 2u32;
    let q_grade = 3u32;

    let t = Instant::now();
    let mut result_bmp = vec![0u64; bmp_words];
    for w in 0..bmp_words {
        result_bmp[w] = city_bmps[q_city as usize][w]
            & dept_bmps[q_dept as usize][w]
            & grade_bmps[q_grade as usize][w];
    }
    let count_bmp: u32 = result_bmp.iter().map(|w| w.count_ones()).sum();
    let query_bmp = t.elapsed();
    println!("クエリ (AND): {:?}, 結果: {}件", query_bmp, count_bmp);

    // === 方式2: 組み合わせ事前計算 ===
    println!();
    println!("=== 方式2: 組み合わせ事前計算 ===");

    let t = Instant::now();
    let mut combo_map: HashMap<(u32, u32, u32), Vec<u32>> = HashMap::new();
    for i in 0..n_entities {
        let key = (cities[i as usize], depts[i as usize], grades[i as usize]);
        combo_map.entry(key).or_default().push(i);
    }
    let build_combo = t.elapsed();
    let mem_combo: usize = combo_map.values().map(|v| v.len() * 4 + 32).sum();
    println!("構築: {:?}", build_combo);
    println!("組み合わせ数: {} (理論値: {})", combo_map.len(), total_combos);
    println!("メモリ: {} KB", mem_combo / 1024);

    // クエリ
    let t = Instant::now();
    let result_combo = combo_map.get(&(q_city, q_dept, q_grade));
    let count_combo = result_combo.map(|v| v.len()).unwrap_or(0);
    let query_combo = t.elapsed();
    println!("クエリ (lookup): {:?}, 結果: {}件", query_combo, count_combo);

    // === 方式3: 組み合わせビットマップ事前計算 ===
    println!();
    println!("=== 方式3: 組み合わせビットマップ事前計算 ===");

    let t = Instant::now();
    let mut combo_bmps: HashMap<(u32, u32, u32), Vec<u64>> = HashMap::new();
    for i in 0..n_entities {
        let key = (cities[i as usize], depts[i as usize], grades[i as usize]);
        let bmp = combo_bmps.entry(key).or_insert_with(|| vec![0u64; bmp_words]);
        let word = (i / 64) as usize;
        let bit = i % 64;
        bmp[word] |= 1u64 << bit;
    }
    let build_combo_bmp = t.elapsed();
    let mem_combo_bmp: usize = combo_bmps.len() * bmp_words * 8;
    println!("構築: {:?}", build_combo_bmp);
    println!("メモリ: {} KB", mem_combo_bmp / 1024);

    // クエリ
    let t = Instant::now();
    let result_combo_bmp = combo_bmps.get(&(q_city, q_dept, q_grade));
    let count_combo_bmp: u32 = result_combo_bmp
        .map(|b| b.iter().map(|w| w.count_ones()).sum())
        .unwrap_or(0);
    let query_combo_bmp = t.elapsed();
    println!("クエリ (lookup): {:?}, 結果: {}件", query_combo_bmp, count_combo_bmp);

    // === 方式4: フラット配列（HashMap なし）===
    println!();
    println!("=== 方式4: フラット配列 O(1) ===");

    let t = Instant::now();
    // combo_id = city * dept_card * grade_card + dept * grade_card + grade
    let mut flat: Vec<Vec<u32>> = vec![vec![]; total_combos as usize];
    for i in 0..n_entities {
        let combo_id = cities[i as usize] * dept_card * grade_card
            + depts[i as usize] * grade_card
            + grades[i as usize];
        flat[combo_id as usize].push(i);
    }
    let build_flat = t.elapsed();
    let mem_flat: usize = flat.iter().map(|v| v.len() * 4 + 24).sum();
    println!("構築: {:?}", build_flat);
    println!("メモリ: {} KB", mem_flat / 1024);

    // クエリ
    let t = Instant::now();
    let combo_id = q_city * dept_card * grade_card + q_dept * grade_card + q_grade;
    let result_flat = &flat[combo_id as usize];
    let count_flat = result_flat.len();
    let query_flat = t.elapsed();
    println!("クエリ (index): {:?}, 結果: {}件", query_flat, count_flat);

    // === 検証 ===
    println!();
    println!("=== 検証 ===");
    println!("全方式一致: {}", count_bmp as usize == count_combo
        && count_combo == count_combo_bmp as usize
        && count_combo == count_flat);

    println!();
    println!("=== まとめ ===");
    println!("| 方式 | 構築 | クエリ | メモリ |");
    println!("|---|---|---|---|");
    println!("| ビットマップAND | {:?} | {:?} | {} KB |", build_bmp, query_bmp,
        (city_card + dept_card + grade_card) as usize * bmp_words * 8 / 1024);
    println!("| 組み合わせ HashMap | {:?} | {:?} | {} KB |", build_combo, query_combo, mem_combo / 1024);
    println!("| 組み合わせ BMP | {:?} | {:?} | {} KB |", build_combo_bmp, query_combo_bmp, mem_combo_bmp / 1024);
    println!("| フラット配列 | {:?} | {:?} | {} KB |", build_flat, query_flat, mem_flat / 1024);
}
