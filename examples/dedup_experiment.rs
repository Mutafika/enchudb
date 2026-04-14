/// 量子円柱 v26 実験 — コンテンツアドレッサブル重複排除
///
/// ペアテーブルのセルが持つ entity リストを重複排除。
/// 同じリストはハッシュで1箇所だけ保持。

use std::collections::HashMap;
use std::hash::{Hash, Hasher, DefaultHasher};
use std::time::Instant;

/// entity リストのハッシュを計算
fn hash_list(list: &[u32]) -> u64 {
    let mut h = DefaultHasher::new();
    list.hash(&mut h);
    h.finish()
}

/// コンテンツアドレッサブルストア
struct ContentStore {
    /// ハッシュ → entity リスト（実体）
    store: Vec<Vec<u32>>,
    /// ハッシュ → store 内のインデックス
    index: HashMap<u64, usize>,
}

impl ContentStore {
    fn new() -> Self {
        // 空リスト用のエントリを最初に入れておく
        let empty_hash = hash_list(&[]);
        let mut index = HashMap::new();
        index.insert(empty_hash, 0);
        ContentStore { store: vec![vec![]], index }
    }

    /// リストを登録して store 内のインデックスを返す
    fn insert(&mut self, list: Vec<u32>) -> usize {
        let h = hash_list(&list);
        if let Some(&idx) = self.index.get(&h) {
            // 既存（ハッシュ衝突の可能性あるが簡易実装）
            return idx;
        }
        let idx = self.store.len();
        self.index.insert(h, idx);
        self.store.push(list);
        idx
    }

    fn get(&self, idx: usize) -> &[u32] {
        &self.store[idx]
    }

    fn unique_lists(&self) -> usize {
        self.store.len()
    }

    fn total_entities(&self) -> usize {
        self.store.iter().map(|v| v.len()).sum()
    }

    fn memory_bytes(&self) -> usize {
        // store: 各 Vec<u32>
        let store_mem: usize = self.store.iter().map(|v| v.len() * 4 + 24).sum();
        // index: HashMap
        let index_mem = self.index.len() * 24;
        store_mem + index_mem
    }
}

/// 重複排除付きペアテーブル
struct DedupPairTable {
    /// (himo_a, himo_b) → テーブル
    pairs: Vec<DedupPairEntry>,
    /// 共有ストア
    store: ContentStore,
}

struct DedupPairEntry {
    himo_a: usize,
    himo_b: usize,
    card_b: u32,
    /// セル → ContentStore 内のインデックス
    table: Vec<usize>,
}

impl DedupPairTable {
    fn build(columns: &[Vec<u32>], cardinalities: &[u32], n: u32) -> Self {
        let n_himos = columns.len();
        let mut store = ContentStore::new();
        let mut pairs = Vec::new();

        for a in 0..n_himos {
            let card_a = cardinalities[a];
            for b in (a + 1)..n_himos {
                let card_b = cardinalities[b];
                let cells = card_a as u64 * card_b as u64;
                if cells > 1_000_000 { continue; }

                let table_size = cells as usize;
                // まず通常通りリストを構築
                let mut raw: Vec<Vec<u32>> = vec![vec![]; table_size];
                for eid in 0..n {
                    let va = columns[a][eid as usize];
                    let vb = columns[b][eid as usize];
                    if va < card_a && vb < card_b {
                        raw[va as usize * card_b as usize + vb as usize].push(eid);
                    }
                }

                // ContentStore に登録して重複排除
                let table: Vec<usize> = raw.into_iter().map(|list| {
                    if list.is_empty() {
                        0 // 空リストは常にインデックス0
                    } else {
                        store.insert(list)
                    }
                }).collect();

                pairs.push(DedupPairEntry { himo_a: a, himo_b: b, card_b, table });
            }
        }

        DedupPairTable { pairs, store }
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
                if cell_id < pair.table.len() {
                    let store_idx = pair.table[cell_id];
                    let candidates = self.store.get(store_idx);
                    if candidates.len() < best_len {
                        best_len = candidates.len();
                        best = Some(candidates);
                    }
                }
            }
        }
        best
    }

    fn table_memory(&self) -> usize {
        // テーブル: 各セルが usize (8byte)
        self.pairs.iter().map(|p| p.table.len() * 8 + 64).sum::<usize>()
    }
}

/// 通常のペアテーブル（比較用）
struct NormalPairTable {
    pairs: Vec<NormalPairEntry>,
}

struct NormalPairEntry {
    himo_a: usize,
    himo_b: usize,
    card_b: u32,
    table: Vec<Vec<u32>>,
}

impl NormalPairTable {
    fn build(columns: &[Vec<u32>], cardinalities: &[u32], n: u32) -> Self {
        let n_himos = columns.len();
        let mut pairs = Vec::new();

        for a in 0..n_himos {
            let card_a = cardinalities[a];
            for b in (a + 1)..n_himos {
                let card_b = cardinalities[b];
                let cells = card_a as u64 * card_b as u64;
                if cells > 1_000_000 { continue; }

                let table_size = cells as usize;
                let mut table: Vec<Vec<u32>> = vec![vec![]; table_size];
                for eid in 0..n {
                    let va = columns[a][eid as usize];
                    let vb = columns[b][eid as usize];
                    if va < card_a && vb < card_b {
                        table[va as usize * card_b as usize + vb as usize].push(eid);
                    }
                }
                pairs.push(NormalPairEntry { himo_a: a, himo_b: b, card_b, table });
            }
        }
        NormalPairTable { pairs }
    }

    fn memory_bytes(&self) -> usize {
        self.pairs.iter().map(|p| {
            p.table.iter().map(|v| v.len() * 4 + 24).sum::<usize>() + 64
        }).sum()
    }
}

fn main() {
    for n in [100_000u32, 1_000_000, 10_000_000] {
        println!("\n============================================================");
        println!("=== {} entities ===", n);

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
        let cardinalities: Vec<u32> = himo_defs.iter().map(|&(_, c)| c).collect();

        // 通常ペアテーブル
        let t = Instant::now();
        let normal = NormalPairTable::build(&columns, &cardinalities, n);
        let build_normal = t.elapsed();

        // 重複排除ペアテーブル
        let t = Instant::now();
        let dedup = DedupPairTable::build(&columns, &cardinalities, n);
        let build_dedup = t.elapsed();

        let normal_mem = normal.memory_bytes();
        let dedup_store_mem = dedup.store.memory_bytes();
        let dedup_table_mem = dedup.table_memory();
        let dedup_total = dedup_store_mem + dedup_table_mem;

        println!("通常:   構築 {:?}, メモリ {} MB", build_normal, normal_mem / 1024 / 1024);
        println!("重複排除: 構築 {:?}, メモリ {} MB (store {} MB + table {} MB)",
            build_dedup, dedup_total / 1024 / 1024,
            dedup_store_mem / 1024 / 1024, dedup_table_mem / 1024 / 1024);
        println!("ユニークリスト数: {} / セル数: {}",
            dedup.store.unique_lists(), dedup.pairs.iter().map(|p| p.table.len()).sum::<usize>());
        println!("削減率: {:.1}%", (1.0 - dedup_total as f64 / normal_mem as f64) * 100.0);

        // クエリ速度比較
        let conds = vec![(0usize, 3u32), (1, 2), (4, 1)]; // tenant=3, dept=2, year=1
        let iters = 100_000u32;

        let t = Instant::now();
        let mut c = 0;
        for _ in 0..iters {
            if let Some(r) = dedup.lookup(&conds) {
                c = r.len();
            }
        }
        let q_dedup = t.elapsed() / iters;
        println!("クエリ(tenant=3,dept=2,year=1): {:?} ({}件)", q_dedup, c);
    }
}
