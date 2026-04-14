/// 量子円柱 v26 実験 — 空間充填曲線でペアテーブルを代替
///
/// 2紐の値を Z-order (Morton code) で1つのキーに変換し、
/// 1次元の sorted array で2条件クエリを実現。
/// ペアテーブルの Vec<u32> entity リストが不要になるか検証。

use std::collections::HashMap;
use std::time::Instant;

/// 2つの u32 値を Z-order で 1つの u64 に変換
#[inline]
fn zorder_encode(a: u32, b: u32) -> u64 {
    let mut z: u64 = 0;
    for i in 0..32 {
        z |= ((a as u64 >> i) & 1) << (i * 2 + 1);
        z |= ((b as u64 >> i) & 1) << (i * 2);
    }
    z
}

/// Z-order キーから (a, b) をデコード
#[inline]
fn zorder_decode(z: u64) -> (u32, u32) {
    let mut a: u32 = 0;
    let mut b: u32 = 0;
    for i in 0..32 {
        a |= ((z >> (i * 2 + 1)) & 1) as u32 >> 0 << i;
        b |= ((z >> (i * 2)) & 1) as u32 >> 0 << i;
    }
    (a, b)
}

/// Z-order Cylinder: (紐A の値, 紐B の値) → entity リスト
/// z-key でソートされた (z_key, entity_id) の配列
struct ZCylinder {
    /// (z_key, entity_id) をz_keyでソート済み
    entries: Vec<(u64, u32)>,
    /// z_key → entries 内の開始位置（HashMap で exact match を高速化）
    index: HashMap<u64, (usize, usize)>, // (start, end) in entries
}

impl ZCylinder {
    fn build(col_a: &[u32], col_b: &[u32], n: u32) -> Self {
        let mut entries: Vec<(u64, u32)> = Vec::with_capacity(n as usize);
        for eid in 0..n {
            let z = zorder_encode(col_a[eid as usize], col_b[eid as usize]);
            entries.push((z, eid));
        }
        entries.sort_unstable_by_key(|&(z, _)| z);

        // インデックス構築: z_key → (start, end)
        let mut index: HashMap<u64, (usize, usize)> = HashMap::new();
        let mut i = 0;
        while i < entries.len() {
            let z = entries[i].0;
            let start = i;
            while i < entries.len() && entries[i].0 == z {
                i += 1;
            }
            index.insert(z, (start, i));
        }

        ZCylinder { entries, index }
    }

    /// exact match: (val_a, val_b) → entity リスト
    fn lookup(&self, val_a: u32, val_b: u32) -> &[(u64, u32)] {
        let z = zorder_encode(val_a, val_b);
        match self.index.get(&z) {
            Some(&(start, end)) => &self.entries[start..end],
            None => &[],
        }
    }

    fn memory_bytes(&self) -> usize {
        self.entries.len() * 12 + self.index.len() * 24
    }
}

/// 方式3: ソート済み配列 + binary search（HashMap なし）
struct ZCylinderBsearch {
    entries: Vec<(u64, u32)>,
}

impl ZCylinderBsearch {
    fn build(col_a: &[u32], col_b: &[u32], n: u32) -> Self {
        let mut entries: Vec<(u64, u32)> = Vec::with_capacity(n as usize);
        for eid in 0..n {
            let z = zorder_encode(col_a[eid as usize], col_b[eid as usize]);
            entries.push((z, eid));
        }
        entries.sort_unstable_by_key(|&(z, _)| z);
        ZCylinderBsearch { entries }
    }

    fn lookup(&self, val_a: u32, val_b: u32) -> &[(u64, u32)] {
        let z = zorder_encode(val_a, val_b);
        let start = self.entries.partition_point(|&(k, _)| k < z);
        let end = self.entries[start..].partition_point(|&(k, _)| k == z) + start;
        &self.entries[start..end]
    }

    fn memory_bytes(&self) -> usize {
        self.entries.len() * 12
    }
}

fn main() {
    let n = 1_000_000u32;

    let himo_defs: Vec<(&str, u32)> = vec![
        ("tenant", 10),
        ("dept", 8),
        ("role", 4),
        ("status", 5),
        ("year", 5),
        ("salary", 1000),
        ("age", 60),
    ];

    let multipliers = [1u32, 3, 7, 11, 13, 31, 17];
    let offsets = [0u32, 0, 0, 0, 0, 0, 20];
    let columns: Vec<Vec<u32>> = (0..himo_defs.len()).map(|h| {
        (0..n).map(|i| offsets[h] + (i * multipliers[h]) % himo_defs[h].1).collect()
    }).collect();

    // === 方式1: ペアテーブル（Vec<u32>、現行 v26）===
    println!("=== 方式1: ペアテーブル（Vec<u32>）===");
    let t = Instant::now();
    // tenant(0) × dept(1) のペア
    let card_b = himo_defs[1].1;
    let table_size = himo_defs[0].1 as usize * card_b as usize;
    let mut pair_table: Vec<Vec<u32>> = vec![vec![]; table_size];
    for eid in 0..n {
        let a = columns[0][eid as usize];
        let b = columns[1][eid as usize];
        pair_table[a as usize * card_b as usize + b as usize].push(eid);
    }
    let build1 = t.elapsed();
    let mem1: usize = pair_table.iter().map(|v| v.len() * 4 + 24).sum();
    println!("構築: {:?}, メモリ: {} KB", build1, mem1 / 1024);

    // === 方式2: ZCylinder（HashMap index）===
    println!("\n=== 方式2: ZCylinder（HashMap）===");
    let t = Instant::now();
    let zcyl = ZCylinder::build(&columns[0], &columns[1], n);
    let build2 = t.elapsed();
    println!("構築: {:?}, メモリ: {} KB", build2, zcyl.memory_bytes() / 1024);

    // === 方式3: ZCylinder（binary search）===
    println!("\n=== 方式3: ZCylinder（bsearch）===");
    let t = Instant::now();
    let zcyl_bs = ZCylinderBsearch::build(&columns[0], &columns[1], n);
    let build3 = t.elapsed();
    println!("構築: {:?}, メモリ: {} KB", build3, zcyl_bs.memory_bytes() / 1024);

    // === クエリベンチ ===
    let test_pairs: Vec<(&str, u32, u32)> = vec![
        ("tenant=3,dept=2", 3, 2),
        ("tenant=0,dept=0", 0, 0),
        ("tenant=9,dept=7", 9, 7),
    ];

    println!("\n=== クエリ比較 ===");
    println!("| クエリ | ペアテーブル | ZCyl(HashMap) | ZCyl(bsearch) | 件数 |");
    println!("|---|---|---|---|---|");

    for (name, va, vb) in &test_pairs {
        let iters = 100_000u32;

        // ペアテーブル
        let t = Instant::now();
        let mut count1 = 0;
        for _ in 0..iters {
            count1 = pair_table[*va as usize * card_b as usize + *vb as usize].len();
        }
        let t1 = t.elapsed() / iters;

        // ZCylinder HashMap
        let t = Instant::now();
        let mut count2 = 0;
        for _ in 0..iters {
            count2 = zcyl.lookup(*va, *vb).len();
        }
        let t2 = t.elapsed() / iters;

        // ZCylinder bsearch
        let t = Instant::now();
        let mut count3 = 0;
        for _ in 0..iters {
            count3 = zcyl_bs.lookup(*va, *vb).len();
        }
        let t3 = t.elapsed() / iters;

        println!("| {} | {:?} ({}) | {:?} ({}) | {:?} ({}) | {} |",
            name, t1, count1, t2, count2, t3, count3, count1);
    }

    // === 多ユニーク紐ペアのメモリ比較 ===
    println!("\n=== 多ユニーク紐ペア: salary(1000) × age(60) ===");

    // ペアテーブル
    // salary: 0..999, age: 20..59 だが max_values は 1000, 60
    // ペアテーブルは (salary * (max_age+1) + age) で引く
    let card_a_sa = himo_defs[5].1 + 1; // salary max+1 = 1001
    let card_b_sa = himo_defs[6].1 + 1; // age max+1 = 61
    let table_size_sa = card_a_sa as usize * card_b_sa as usize;
    let t = Instant::now();
    let mut pair_sa: Vec<Vec<u32>> = vec![vec![]; table_size_sa];
    for eid in 0..n {
        let a = columns[5][eid as usize]; // salary
        let b = columns[6][eid as usize]; // age
        pair_sa[a as usize * card_b_sa as usize + b as usize].push(eid);
    }
    let build_sa1 = t.elapsed();
    let mem_sa1: usize = pair_sa.iter().map(|v| v.len() * 4 + 24).sum();

    // ZCylinder bsearch
    let t = Instant::now();
    let zcyl_sa = ZCylinderBsearch::build(&columns[5], &columns[6], n);
    let build_sa3 = t.elapsed();

    println!("ペアテーブル: 構築 {:?}, メモリ {} MB", build_sa1, mem_sa1 / 1024 / 1024);
    println!("ZCyl(bsearch): 構築 {:?}, メモリ {} MB", build_sa3, zcyl_sa.memory_bytes() / 1024 / 1024);

    // クエリ速度
    let iters = 100_000u32;
    // age は offset 20 がかかってるので実際の値で引く
    let q_salary = 500u32;
    let q_age = 35u32; // columns[6] の実際の値

    let t = Instant::now();
    let mut c1 = 0;
    for _ in 0..iters {
        let idx = q_salary as usize * card_b_sa as usize + q_age as usize;
        c1 = if idx < pair_sa.len() { pair_sa[idx].len() } else { 0 };
    }
    let q1 = t.elapsed() / iters;

    let t = Instant::now();
    let mut c2 = 0;
    for _ in 0..iters {
        c2 = zcyl_sa.lookup(q_salary, q_age).len();
    }
    let q2 = t.elapsed() / iters;

    println!("クエリ salary={},age={}: ペアテーブル {:?} ({}件), ZCyl {:?} ({}件)", q_salary, q_age, q1, c1, q2, c2);

    // === 全ペアの合計メモリ ===
    println!("\n=== 全21ペアの合計メモリ推定 ===");
    let mut total_pair = 0usize;
    let mut total_zcyl = 0usize;
    for a in 0..himo_defs.len() {
        for b in (a+1)..himo_defs.len() {
            let cells = himo_defs[a].1 as usize * himo_defs[b].1 as usize;
            // ペアテーブル: 各セル平均 n/cells 件 × 4bytes + 24 overhead
            let avg_per_cell = n as usize / cells.max(1);
            total_pair += cells * (avg_per_cell * 4 + 24);
            // ZCylinder: n 件 × 12bytes (z_key 8 + eid 4)
            total_zcyl += n as usize * 12;
        }
    }
    println!("ペアテーブル合計: {} MB", total_pair / 1024 / 1024);
    println!("ZCylinder合計: {} MB", total_zcyl / 1024 / 1024);
}
