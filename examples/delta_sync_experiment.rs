/// デルタシンク実験 — ブロック全コピーせず差分だけ記録

use std::collections::HashMap;
use std::hash::{Hash, Hasher, DefaultHasher};
use std::time::Instant;

fn hash_list(list: &[u32]) -> u64 {
    let mut h = DefaultHasher::new();
    list.hash(&mut h);
    h.finish()
}

/// デルタ付きセル
struct DeltaCell {
    /// ベースブロック（ソート済み entity リスト）
    base: Vec<u32>,
    /// 追加された entity
    adds: Vec<u32>,
    /// 除去された entity
    removes: Vec<u32>,
}

impl DeltaCell {
    fn new(base: Vec<u32>) -> Self {
        DeltaCell { base, adds: vec![], removes: vec![] }
    }

    fn add(&mut self, eid: u32) {
        // removes にあったら取り消し
        if let Some(pos) = self.removes.iter().position(|&e| e == eid) {
            self.removes.swap_remove(pos);
            return;
        }
        self.adds.push(eid);
    }

    fn remove(&mut self, eid: u32) {
        // adds にあったら取り消し
        if let Some(pos) = self.adds.iter().position(|&e| e == eid) {
            self.adds.swap_remove(pos);
            return;
        }
        self.removes.push(eid);
    }

    /// マージ済みリストを返す（クエリ用）
    fn merged(&self) -> Vec<u32> {
        if self.adds.is_empty() && self.removes.is_empty() {
            return self.base.clone();
        }
        let mut result: Vec<u32> = self.base.iter()
            .filter(|e| !self.removes.contains(e))
            .copied()
            .collect();
        for &eid in &self.adds {
            match result.binary_search(&eid) {
                Ok(_) => {},
                Err(pos) => result.insert(pos, eid),
            }
        }
        result
    }

    /// デルタなしの件数（高速）
    #[inline]
    fn len_approx(&self) -> usize {
        self.base.len() + self.adds.len() - self.removes.len()
    }

    /// デルタが空か
    #[inline]
    fn is_clean(&self) -> bool {
        self.adds.is_empty() && self.removes.is_empty()
    }

    /// compact: デルタをベースに統合
    fn compact(&mut self) {
        if self.is_clean() { return; }
        self.base = self.merged();
        self.adds.clear();
        self.removes.clear();
    }

    /// デルタサイズ（同期データ量）
    fn delta_size_bytes(&self) -> usize {
        (self.adds.len() + self.removes.len()) * 5 // eid(4) + op(1)
    }
}

/// デルタシンク ペアテーブル
struct DeltaPairTable {
    pairs: Vec<DeltaPairEntry>,
}

struct DeltaPairEntry {
    himo_a: usize,
    himo_b: usize,
    card_b: u32,
    cells: Vec<DeltaCell>,
}

impl DeltaPairTable {
    fn build(columns: &[Vec<u32>], cardinalities: &[u32], n: u32) -> Self {
        let n_himos = columns.len();
        let mut pairs = Vec::new();

        for a in 0..n_himos {
            let card_a = cardinalities[a];
            for b in (a+1)..n_himos {
                let card_b = cardinalities[b];
                let cells_count = card_a as u64 * card_b as u64;
                if cells_count > 1_000_000 { continue; }

                let table_size = cells_count as usize;
                let mut raw: Vec<Vec<u32>> = vec![vec![]; table_size];
                for eid in 0..n {
                    let va = columns[a][eid as usize];
                    let vb = columns[b][eid as usize];
                    if va < card_a && vb < card_b {
                        raw[va as usize * card_b as usize + vb as usize].push(eid);
                    }
                }

                let cells: Vec<DeltaCell> = raw.into_iter().map(DeltaCell::new).collect();
                pairs.push(DeltaPairEntry { himo_a: a, himo_b: b, card_b, cells });
            }
        }

        DeltaPairTable { pairs }
    }

    fn update_tie(&mut self, eid: u32, himo_idx: usize, old_val: u32, new_val: u32, other_values: &[(usize, u32)]) {
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
            let old_cell_id = if is_a {
                old_val as usize * card_b as usize + other_val as usize
            } else {
                other_val as usize * card_b as usize + old_val as usize
            };
            if old_cell_id < pair.cells.len() {
                pair.cells[old_cell_id].remove(eid);
            }

            // 新セルに追加
            let new_cell_id = if is_a {
                new_val as usize * card_b as usize + other_val as usize
            } else {
                other_val as usize * card_b as usize + new_val as usize
            };
            if new_cell_id < pair.cells.len() {
                pair.cells[new_cell_id].add(eid);
            }
        }
    }

    fn compact_all(&mut self) {
        for pair in &mut self.pairs {
            for cell in &mut pair.cells {
                cell.compact();
            }
        }
    }

    fn best_lookup(&self, conds: &[(usize, u32)]) -> Option<(Vec<u32>, Vec<(usize, u32)>)> {
        if conds.len() < 2 { return None; }
        let mut best: Option<(Vec<u32>, Vec<(usize, u32)>)> = None;
        let mut best_len = usize::MAX;

        for pair in &self.pairs {
            let mut va = None;
            let mut vb = None;
            for &(idx, val) in conds {
                if idx == pair.himo_a { va = Some(val); }
                if idx == pair.himo_b { vb = Some(val); }
            }
            if let (Some(a), Some(b)) = (va, vb) {
                let cell_id = a as usize * pair.card_b as usize + b as usize;
                if cell_id < pair.cells.len() {
                    let cell = &pair.cells[cell_id];
                    let len = cell.len_approx();
                    if len < best_len {
                        best_len = len;
                        let remaining: Vec<(usize, u32)> = conds.iter()
                            .filter(|&&(idx, _)| idx != pair.himo_a && idx != pair.himo_b)
                            .copied()
                            .collect();
                        if cell.is_clean() {
                            best = Some((cell.base.clone(), remaining));
                        } else {
                            best = Some((cell.merged(), remaining));
                        }
                    }
                }
            }
        }
        best
    }
}

/// CAS 版（比較用）
struct CASCell {
    data: Vec<u32>,
}

fn main() {
    let n = 100_000u32;
    let himos: Vec<(&str, u32)> = vec![
        ("a", 10), ("b", 8), ("c", 5), ("d", 4),
    ];
    let mults = [1u32, 3, 7, 11];
    let mut columns: Vec<Vec<u32>> = (0..himos.len()).map(|h| {
        (0..n).map(|i| (i * mults[h]) % himos[h].1).collect()
    }).collect();
    let cardinalities: Vec<u32> = himos.iter().map(|&(_, c)| c).collect();

    // === 構築 ===
    let t = Instant::now();
    let mut delta_pt = DeltaPairTable::build(&columns, &cardinalities, n);
    println!("デルタ構築: {:?}", t.elapsed());

    // === 差分更新速度 ===
    let update_n = 10_000u32;

    // デルタシンク
    let t = Instant::now();
    for i in 0..update_n {
        let himo_idx = 0;
        let old = columns[himo_idx][i as usize];
        let new_v = (old + 1) % cardinalities[himo_idx];
        let other_values: Vec<(usize, u32)> = (0..himos.len())
            .filter(|&h| h != himo_idx)
            .map(|h| (h, columns[h][i as usize]))
            .collect();
        delta_pt.update_tie(i, himo_idx, old, new_v, &other_values);
        columns[himo_idx][i as usize] = new_v;
    }
    let delta_time = t.elapsed();
    println!("デルタ更新 {}件: {:?} ({:?}/件)", update_n, delta_time, delta_time / update_n);

    // クエリ（デルタ付き）
    let conds = vec![(0, 1u32), (1, 3u32)];
    let iters = 10_000u32;
    let t = Instant::now();
    let mut count = 0;
    for _ in 0..iters {
        if let Some((candidates, _)) = delta_pt.best_lookup(&conds) {
            count = candidates.len();
        }
    }
    let query_dirty = t.elapsed() / iters;
    println!("クエリ(デルタ有): {:?} ({}件)", query_dirty, count);

    // compact
    let t = Instant::now();
    delta_pt.compact_all();
    let compact_time = t.elapsed();
    println!("compact: {:?}", compact_time);

    // クエリ（compact後）
    let t = Instant::now();
    for _ in 0..iters {
        if let Some((candidates, _)) = delta_pt.best_lookup(&conds) {
            count = candidates.len();
        }
    }
    let query_clean = t.elapsed() / iters;
    println!("クエリ(compact後): {:?} ({}件)", query_clean, count);

    // === CAS 版比較 ===
    println!("\n=== 比較 ===");

    // CAS: rebuild してから差分更新
    let mut columns2: Vec<Vec<u32>> = (0..himos.len()).map(|h| {
        (0..n).map(|i| (i * mults[h]) % himos[h].1).collect()
    }).collect();

    let dir = "/tmp/v26_delta_cmp.db";
    let _ = std::fs::remove_file(dir);
    let mut db = enchudb::Engine::create_standalone(dir).unwrap();
    for &(name, max_v) in &himos {
        db.define_himo(name, enchudb::HimoType::Number, max_v);
    }
    for i in 0..n {
        let e = db.entity();
        for (h, &(name, _)) in himos.iter().enumerate() {
            db.tie(e, name, columns2[h][i as usize]);
        }
    }
    db.rebuild();
    db.rebuild_pairs();

    let a_idx = db.himo_id("a").unwrap();
    let t = Instant::now();
    for i in 0..update_n {
        let old = db.get(i as u64, "a").unwrap();
        let new_v = (old + 1) % cardinalities[0];
        db.tie(i as u64, "a", new_v);
        db.apply_pair_delta(i, a_idx, old, new_v);
    }
    let cas_time = t.elapsed();
    println!("CAS更新 {}件: {:?} ({:?}/件)", update_n, cas_time, cas_time / update_n);

    // フル rebuild
    let t = Instant::now();
    db.rebuild_pairs();
    let rebuild_time = t.elapsed();
    println!("フル rebuild: {:?}", rebuild_time);

    println!("\n=== まとめ ===");
    println!("| 方式 | 更新速度 | クエリ(dirty) | クエリ(clean) |");
    println!("|---|---|---|---|");
    println!("| デルタシンク | {:?}/件 | {:?} | {:?} |", delta_time / update_n, query_dirty, query_clean);
    println!("| CAS | {:?}/件 | - | - |", cas_time / update_n);
    println!("| フル rebuild | {:?}/{}件 | - | - |", rebuild_time, update_n);
}
