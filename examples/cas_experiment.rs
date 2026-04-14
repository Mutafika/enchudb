/// 量子円柱 v26 実験 — コンテンツアドレッサブル差分更新
///
/// tie で値が変わった時にペアテーブル全再構築ではなく、
/// 影響するセルだけ差分更新 + ハッシュ更新。
/// 同期に必要な情報量（ハッシュ diff）も計測。

use std::collections::HashMap;
use std::hash::{Hash, Hasher, DefaultHasher};
use std::time::Instant;

fn hash_list(list: &[u32]) -> u64 {
    let mut h = DefaultHasher::new();
    list.hash(&mut h);
    h.finish()
}

/// コンテンツアドレッサブル ペアテーブル（差分更新対応）
struct CASPairTable {
    pairs: Vec<CASPair>,
    /// ハッシュ → entity リスト実体
    store: HashMap<u64, Vec<u32>>,
}

struct CASPair {
    himo_a: usize,
    himo_b: usize,
    card_a: u32,
    card_b: u32,
    /// セル → ハッシュ値（実体は store に）
    cells: Vec<u64>,
}

impl CASPairTable {
    fn build(columns: &[Vec<u32>], cardinalities: &[u32], n: u32) -> Self {
        let n_himos = columns.len();
        let mut store: HashMap<u64, Vec<u32>> = HashMap::new();
        let empty_hash = hash_list(&[]);
        store.insert(empty_hash, vec![]);

        let mut pairs = Vec::new();

        for a in 0..n_himos {
            let card_a = cardinalities[a];
            for b in (a + 1)..n_himos {
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

                let cells: Vec<u64> = raw.into_iter().map(|list| {
                    let h = hash_list(&list);
                    store.entry(h).or_insert(list);
                    h
                }).collect();

                pairs.push(CASPair { himo_a: a, himo_b: b, card_a, card_b, cells });
            }
        }

        CASPairTable { pairs, store }
    }

    /// entity の紐 himo_idx の値が old_val → new_val に変わった時の差分更新
    /// 返り値: 変更されたセルの (pair_index, cell_index, old_hash, new_hash)
    fn update_tie(
        &mut self,
        eid: u32,
        himo_idx: usize,
        old_val: u32,
        new_val: u32,
        columns: &[Vec<u32>], // 他の紐の現在値を読むため
    ) -> Vec<(usize, usize, u64, u64)> {
        let mut changes = Vec::new();

        for (pi, pair) in self.pairs.iter_mut().enumerate() {
            // この紐がペアの一部か
            let (is_a, other_idx) = if pair.himo_a == himo_idx {
                (true, pair.himo_b)
            } else if pair.himo_b == himo_idx {
                (false, pair.himo_a)
            } else {
                continue;
            };

            let other_val = columns[other_idx][eid as usize];

            // 旧セルから entity を除去
            let old_cell_id = if is_a {
                old_val as usize * pair.card_b as usize + other_val as usize
            } else {
                other_val as usize * pair.card_b as usize + old_val as usize
            };

            if old_cell_id < pair.cells.len() {
                let old_hash = pair.cells[old_cell_id];
                let list = self.store.get(&old_hash).cloned().unwrap_or_default();
                let mut new_list: Vec<u32> = list.into_iter().filter(|&e| e != eid).collect();
                let new_hash = hash_list(&new_list);
                self.store.entry(new_hash).or_insert(new_list);
                pair.cells[old_cell_id] = new_hash;
                if old_hash != new_hash {
                    changes.push((pi, old_cell_id, old_hash, new_hash));
                }
            }

            // 新セルに entity を追加
            let new_cell_id = if is_a {
                new_val as usize * pair.card_b as usize + other_val as usize
            } else {
                other_val as usize * pair.card_b as usize + new_val as usize
            };

            if new_cell_id < pair.cells.len() {
                let old_hash = pair.cells[new_cell_id];
                let mut list = self.store.get(&old_hash).cloned().unwrap_or_default();
                // ソート済みで挿入
                match list.binary_search(&eid) {
                    Ok(_) => {}, // 既にある
                    Err(pos) => list.insert(pos, eid),
                }
                let new_hash = hash_list(&list);
                self.store.entry(new_hash).or_insert(list);
                pair.cells[new_cell_id] = new_hash;
                if old_hash != new_hash {
                    changes.push((pi, new_cell_id, old_hash, new_hash));
                }
            }
        }

        changes
    }

    fn lookup(&self, conds: &[(usize, u32)]) -> Option<&[u32]> {
        let mut best: Option<&[u32]> = None;
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
                    let h = pair.cells[cell_id];
                    if let Some(list) = self.store.get(&h) {
                        if list.len() < best_len {
                            best_len = list.len();
                            best = Some(list);
                        }
                    }
                }
            }
        }
        best
    }

    fn unique_lists(&self) -> usize {
        self.store.len()
    }

    fn store_memory(&self) -> usize {
        self.store.values().map(|v| v.len() * 4 + 24).sum::<usize>()
            + self.store.len() * 24
    }

    fn cells_memory(&self) -> usize {
        self.pairs.iter().map(|p| p.cells.len() * 8).sum()
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
    let mut columns: Vec<Vec<u32>> = (0..himo_defs.len()).map(|h| {
        (0..n).map(|i| offsets[h] + (i * multipliers[h]) % himo_defs[h].1).collect()
    }).collect();
    let cardinalities: Vec<u32> = himo_defs.iter().map(|&(_, c)| c).collect();

    // === 構築 ===
    println!("=== CAS ペアテーブル構築 ({} entities) ===", n);
    let t = Instant::now();
    let mut cas = CASPairTable::build(&columns, &cardinalities, n);
    let build_time = t.elapsed();
    println!("構築: {:?}", build_time);
    println!("ユニークリスト: {}", cas.unique_lists());
    println!("メモリ: store {} MB + cells {} KB = {} MB",
        cas.store_memory() / 1024 / 1024,
        cas.cells_memory() / 1024,
        (cas.store_memory() + cas.cells_memory()) / 1024 / 1024);

    // === クエリ ===
    println!("\n=== クエリ ===");
    let conds = vec![(0usize, 3u32), (1, 2)];
    let iters = 100_000u32;
    let t = Instant::now();
    let mut count = 0;
    for _ in 0..iters {
        if let Some(r) = cas.lookup(&conds) {
            count = r.len();
        }
    }
    let q = t.elapsed() / iters;
    println!("tenant=3,dept=2: {:?} ({}件)", q, count);

    // === 差分更新 ===
    println!("\n=== 差分更新 ===");

    // entity 0 の dept を 0 → 3 に変更
    let eid = 0u32;
    let himo_idx = 1; // dept
    let old_val = columns[himo_idx][eid as usize];
    let new_val = 3u32;

    println!("entity {} の dept: {} → {}", eid, old_val, new_val);

    let t = Instant::now();
    let changes = cas.update_tie(eid, himo_idx, old_val, new_val, &columns);
    let update_time = t.elapsed();

    // columns も更新
    columns[himo_idx][eid as usize] = new_val;

    println!("更新時間: {:?}", update_time);
    println!("変更セル数: {}", changes.len());
    println!("同期データ量: {} bytes (セル数 × 24bytes)", changes.len() * 24);

    for (pi, ci, old_h, new_h) in &changes {
        println!("  pair[{}] cell[{}]: {:016x} → {:016x}", pi, ci, old_h, new_h);
    }

    // === 大量差分更新ベンチ ===
    println!("\n=== 大量差分更新ベンチ ===");
    let update_count = 10_000u32;
    let t = Instant::now();
    let mut total_changes = 0usize;
    for i in 0..update_count {
        let eid = i;
        let himo_idx = 1; // dept
        let old_val = columns[himo_idx][eid as usize];
        let new_val = (old_val + 1) % cardinalities[himo_idx];
        let changes = cas.update_tie(eid, himo_idx, old_val, new_val, &columns);
        total_changes += changes.len();
        columns[himo_idx][eid as usize] = new_val;
    }
    let bulk_time = t.elapsed();
    println!("{}件更新: {:?} ({:?}/件)", update_count, bulk_time, bulk_time / update_count);
    println!("変更セル合計: {} ({:.1}セル/件)", total_changes, total_changes as f64 / update_count as f64);
    println!("同期データ量: {} KB", total_changes * 24 / 1024);

    // 更新後のクエリが正しいか
    println!("\n=== 更新後クエリ ===");
    let t = Instant::now();
    let mut count = 0;
    for _ in 0..iters {
        if let Some(r) = cas.lookup(&conds) {
            count = r.len();
        }
    }
    let q = t.elapsed() / iters;
    println!("tenant=3,dept=2: {:?} ({}件)", q, count);
    println!("ユニークリスト: {}", cas.unique_lists());
}
