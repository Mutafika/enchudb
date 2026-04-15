/// 統一デルタシンク実験
///
/// tie → delta記録 → Column + Cylinder + PairTable 全部即時反映 → delta パージ
/// rebuild 不要（初回のみ）

use std::time::Instant;

/// シンプルな Cylinder シミュレーション（ソート済み entity リスト）
struct SimCylinder {
    /// value → entity リスト（ソート済み）
    slices: Vec<Vec<u32>>,
}

impl SimCylinder {
    fn new(max_values: u32) -> Self {
        SimCylinder { slices: vec![vec![]; (max_values + 1) as usize] }
    }

    fn build(column: &[u32], max_values: u32, n: u32) -> Self {
        let mut cyl = Self::new(max_values);
        for eid in 0..n {
            let v = column[eid as usize];
            if v <= max_values {
                cyl.slices[v as usize].push(eid);
            }
        }
        cyl
    }

    #[inline]
    fn add(&mut self, value: u32, eid: u32) {
        let slice = &mut self.slices[value as usize];
        match slice.binary_search(&eid) {
            Ok(_) => {},
            Err(pos) => slice.insert(pos, eid),
        }
    }

    #[inline]
    fn remove(&mut self, value: u32, eid: u32) {
        let slice = &mut self.slices[value as usize];
        if let Ok(pos) = slice.binary_search(&eid) {
            slice.remove(pos);
        }
    }

    fn slice(&self, value: u32) -> &[u32] {
        &self.slices[value as usize]
    }
}

/// シンプルな PairTable シミュレーション（base 直接操作）
struct SimPairTable {
    pairs: Vec<SimPair>,
}

struct SimPair {
    himo_a: usize,
    himo_b: usize,
    card_b: u32,
    cells: Vec<Vec<u32>>,
}

impl SimPairTable {
    fn build(columns: &[Vec<u32>], cardinalities: &[u32], n: u32) -> Self {
        let n_himos = columns.len();
        let mut pairs = Vec::new();

        for a in 0..n_himos {
            let card_a = cardinalities[a];
            for b in (a+1)..n_himos {
                let card_b = cardinalities[b];
                if card_a as u64 * card_b as u64 > 1_000_000 { continue; }

                let table_size = card_a as usize * card_b as usize;
                let mut cells: Vec<Vec<u32>> = vec![vec![]; table_size];
                for eid in 0..n {
                    let va = columns[a][eid as usize];
                    let vb = columns[b][eid as usize];
                    if va < card_a && vb < card_b {
                        cells[va as usize * card_b as usize + vb as usize].push(eid);
                    }
                }
                pairs.push(SimPair { himo_a: a, himo_b: b, card_b, cells });
            }
        }
        SimPairTable { pairs }
    }

    fn update(&mut self, eid: u32, himo_idx: usize, old_val: u32, new_val: u32, other_values: &[(usize, u32)]) {
        for pair in &mut self.pairs {
            let (is_a, other_himo) = if pair.himo_a == himo_idx {
                (true, pair.himo_b)
            } else if pair.himo_b == himo_idx {
                (false, pair.himo_a)
            } else {
                continue;
            };

            let other_val = match other_values.iter().find(|&&(idx, _)| idx == other_himo) {
                Some(&(_, v)) => v,
                None => continue,
            };

            let card_b = pair.card_b;

            // 旧セルから除去
            let old_cid = if is_a { old_val as usize * card_b as usize + other_val as usize }
                          else { other_val as usize * card_b as usize + old_val as usize };
            if old_cid < pair.cells.len() {
                if let Ok(pos) = pair.cells[old_cid].binary_search(&eid) {
                    pair.cells[old_cid].remove(pos);
                }
            }

            // 新セルに追加
            let new_cid = if is_a { new_val as usize * card_b as usize + other_val as usize }
                          else { other_val as usize * card_b as usize + new_val as usize };
            if new_cid < pair.cells.len() {
                match pair.cells[new_cid].binary_search(&eid) {
                    Ok(_) => {},
                    Err(pos) => pair.cells[new_cid].insert(pos, eid),
                }
            }
        }
    }

    fn best_lookup(&self, conds: &[(usize, u32)]) -> Option<(&[u32], Vec<(usize, u32)>)> {
        if conds.len() < 2 { return None; }
        let mut best: Option<(&[u32], Vec<(usize, u32)>)> = None;
        let mut best_len = usize::MAX;

        for pair in &self.pairs {
            let mut va = None;
            let mut vb = None;
            for &(idx, val) in conds {
                if idx == pair.himo_a { va = Some(val); }
                if idx == pair.himo_b { vb = Some(val); }
            }
            if let (Some(a), Some(b)) = (va, vb) {
                let cid = a as usize * pair.card_b as usize + b as usize;
                if cid < pair.cells.len() {
                    let candidates = &pair.cells[cid];
                    if candidates.is_empty() {
                        return Some((&[], conds.to_vec()));
                    }
                    if candidates.len() < best_len {
                        best_len = candidates.len();
                        let remaining: Vec<(usize, u32)> = conds.iter()
                            .filter(|&&(idx, _)| idx != pair.himo_a && idx != pair.himo_b)
                            .copied()
                            .collect();
                        best = Some((candidates, remaining));
                    }
                }
            }
        }
        best
    }
}

/// デルタレコード
struct Delta {
    eid: u32,
    himo_idx: usize,
    old_val: u32,
    new_val: u32,
}

fn main() {
    let n = 100_000u32;
    let himos: Vec<(&str, u32)> = vec![
        ("tenant", 10), ("dept", 8), ("role", 4), ("status", 5),
    ];
    let mults = [1u32, 3, 7, 11];
    let mut columns: Vec<Vec<u32>> = (0..himos.len()).map(|h| {
        (0..n).map(|i| (i * mults[h]) % himos[h].1).collect()
    }).collect();
    let cardinalities: Vec<u32> = himos.iter().map(|&(_, c)| c).collect();

    // 初回構築
    let t = Instant::now();
    let mut cylinders: Vec<SimCylinder> = (0..himos.len())
        .map(|h| SimCylinder::build(&columns[h], cardinalities[h], n))
        .collect();
    let mut pair_table = SimPairTable::build(&columns, &cardinalities, n);
    println!("初回構築: {:?}", t.elapsed());

    // === 統一デルタシンク ===
    println!("\n=== 統一デルタシンク（delta → Column + Cylinder + Pair → purge）===");
    let update_n = 10_000u32;
    let himo_idx = 1; // dept

    let t = Instant::now();
    for i in 0..update_n {
        let eid = i;
        let old_val = columns[himo_idx][eid as usize];
        let new_val = (old_val + 1) % cardinalities[himo_idx];

        // 1. delta 記録（スタック上、ヒープ不要）
        // let _delta = Delta { eid, himo_idx, old_val, new_val };

        // 2. Column 更新
        columns[himo_idx][eid as usize] = new_val;

        // 3. Cylinder 更新
        cylinders[himo_idx].remove(old_val, eid);
        cylinders[himo_idx].add(new_val, eid);

        // 4. PairTable 更新
        let other_values: Vec<(usize, u32)> = (0..himos.len())
            .filter(|&h| h != himo_idx)
            .map(|h| (h, columns[h][eid as usize]))
            .collect();
        pair_table.update(eid, himo_idx, old_val, new_val, &other_values);

        // 5. delta パージ（スタックだから自動）
    }
    let sync_time = t.elapsed();
    println!("統一デルタ {}件: {:?} ({:?}/件)", update_n, sync_time, sync_time / update_n);

    // === クエリ（常にクリーン） ===
    println!("\n=== クエリ（rebuild なし、常にクリーン）===");
    let queries: Vec<(&str, Vec<(usize, u32)>)> = vec![
        ("tenant=3,dept=2", vec![(0, 3), (1, 2)]),
        ("tenant=3,dept=2,role=1", vec![(0, 3), (1, 2), (2, 1)]),
        ("tenant=3,dept=2,role=1,status=1", vec![(0, 3), (1, 2), (2, 1), (3, 1)]),
    ];

    for (name, conds) in &queries {
        let iters = 100_000u32;
        let t = Instant::now();
        let mut count = 0;
        for _ in 0..iters {
            if let Some((candidates, remaining)) = pair_table.best_lookup(conds) {
                if remaining.is_empty() {
                    count = candidates.len();
                } else {
                    let mut c = 0;
                    for &eid in candidates {
                        let mut pass = true;
                        for &(idx, val) in &remaining {
                            if columns[idx][eid as usize] != val { pass = false; break; }
                        }
                        if pass { c += 1; }
                    }
                    count = c;
                }
            }
        }
        let per = t.elapsed() / iters;
        println!("  {}: {:?} ({}件)", name, per, count);
    }

    // Cylinder クエリも確認
    println!("\n=== Cylinder 直接クエリ ===");
    let iters = 100_000u32;
    let t = Instant::now();
    let mut count = 0;
    for _ in 0..iters {
        count = cylinders[0].slice(3).len(); // tenant=3
    }
    println!("  tenant=3: {:?} ({}件)", t.elapsed() / iters, count);

    // === 比較: フル rebuild ===
    println!("\n=== フル rebuild（比較用）===");
    let t = Instant::now();
    for h in 0..himos.len() {
        cylinders[h] = SimCylinder::build(&columns[h], cardinalities[h], n);
    }
    pair_table = SimPairTable::build(&columns, &cardinalities, n);
    println!("  フル rebuild: {:?}", t.elapsed());

    // === 正確性検証 ===
    println!("\n=== 正確性検証（デルタ更新 vs フル rebuild）===");
    // dept を再度全部変えてデルタシンク
    let mut columns2 = columns.clone();
    let mut cyl2: Vec<SimCylinder> = (0..himos.len())
        .map(|h| SimCylinder::build(&columns2[h], cardinalities[h], n))
        .collect();
    let mut pt2 = SimPairTable::build(&columns2, &cardinalities, n);

    for i in 0..1000u32 {
        let eid = i;
        let old_val = columns2[himo_idx][eid as usize];
        let new_val = (old_val + 3) % cardinalities[himo_idx];
        columns2[himo_idx][eid as usize] = new_val;
        cyl2[himo_idx].remove(old_val, eid);
        cyl2[himo_idx].add(new_val, eid);
        let other_values: Vec<(usize, u32)> = (0..himos.len())
            .filter(|&h| h != himo_idx)
            .map(|h| (h, columns2[h][eid as usize]))
            .collect();
        pt2.update(eid, himo_idx, old_val, new_val, &other_values);
    }

    // フル rebuild して比較
    let cyl_rebuild = SimCylinder::build(&columns2[himo_idx], cardinalities[himo_idx], n);
    let pt_rebuild = SimPairTable::build(&columns2, &cardinalities, n);

    let mut ok = true;
    for v in 0..cardinalities[himo_idx] {
        if cyl2[himo_idx].slice(v) != cyl_rebuild.slice(v) {
            println!("  Cylinder 不一致: dept={}", v);
            ok = false;
        }
    }
    for (pi, pair) in pt2.pairs.iter().enumerate() {
        for (ci, cell) in pair.cells.iter().enumerate() {
            if cell != &pt_rebuild.pairs[pi].cells[ci] {
                println!("  PairTable 不一致: pair[{}] cell[{}]", pi, ci);
                ok = false;
                break;
            }
        }
    }
    if ok { println!("  ✓ 全一致"); }
    else { println!("  ✗ 不一致あり"); }
}
