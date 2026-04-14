/// 量子円柱 v26 実験 — ペアテーブル（二次元テーブル）
///
/// 全紐ペアの二次元テーブルを事前構築。
/// 任意の2条件が O(1)、3条件以上は最小ペアから Column 直読み。

use std::time::Instant;

/// 紐ペアごとの二次元テーブル
struct PairTable {
    /// (himo_a, himo_b) のペアインデックス → テーブル
    /// テーブルは [val_a * card_b + val_b] = Vec<entity_id>
    pairs: Vec<PairEntry>,
}

struct PairEntry {
    himo_a: usize,
    himo_b: usize,
    card_a: u32,
    card_b: u32,
    table: Vec<Vec<u32>>,
}

impl PairTable {
    fn build(columns: &[Vec<u32>], cardinalities: &[u32], n_entities: u32) -> Self {
        let n_himos = columns.len();
        let mut pairs = Vec::new();

        for a in 0..n_himos {
            for b in (a+1)..n_himos {
                let card_a = cardinalities[a];
                let card_b = cardinalities[b];
                let table_size = card_a as usize * card_b as usize;
                let mut table: Vec<Vec<u32>> = vec![vec![]; table_size];

                for eid in 0..n_entities {
                    let va = columns[a][eid as usize];
                    let vb = columns[b][eid as usize];
                    if va < card_a && vb < card_b {
                        table[va as usize * card_b as usize + vb as usize].push(eid);
                    }
                }

                pairs.push(PairEntry { himo_a: a, himo_b: b, card_a, card_b, table });
            }
        }
        PairTable { pairs }
    }

    /// 条件リストから最小のペアテーブルエントリを引く
    fn best_lookup<'a>(&'a self, conds: &[(usize, u32)]) -> Option<(&'a [u32], Vec<(usize, u32)>)> {
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
                let cell_id = a as usize * pair.card_b as usize + b as usize;
                if cell_id < pair.table.len() {
                    let candidates = &pair.table[cell_id];
                    if candidates.len() < best_len {
                        best_len = candidates.len();
                        // 残り条件
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

    fn memory_bytes(&self) -> usize {
        self.pairs.iter().map(|p| {
            p.table.iter().map(|v| v.len() * 4 + 24).sum::<usize>() + 64
        }).sum()
    }

    fn total_cells(&self) -> usize {
        self.pairs.iter().map(|p| p.table.len()).sum()
    }
}

fn main() {
    let n = 10_000_000u32;

    // 紐定義: 少ユニーク + 多ユニーク混在
    let himo_defs: Vec<(&str, u32)> = vec![
        ("tenant", 10),
        ("dept", 8),
        ("role", 4),
        ("status", 5),
        ("year", 5),
        ("salary", 1000),
        ("age", 60),
    ];

    println!("=== データ生成 ({} entities, {} himos) ===", n, himo_defs.len());

    // Column データ生成
    let multipliers = [1u32, 3, 7, 11, 13, 31, 17];
    let offsets = [0u32, 0, 0, 0, 0, 0, 20];
    let columns: Vec<Vec<u32>> = (0..himo_defs.len()).map(|h| {
        (0..n).map(|i| offsets[h] + (i * multipliers[h]) % himo_defs[h].1).collect()
    }).collect();
    let cardinalities: Vec<u32> = himo_defs.iter().map(|&(_, c)| c).collect();

    // === ペアテーブル構築 ===
    let t = Instant::now();
    let pt = PairTable::build(&columns, &cardinalities, n);
    let build_time = t.elapsed();
    println!("ペアテーブル構築: {:?}", build_time);
    println!("ペア数: {}, セル数: {}, メモリ: {} MB",
        pt.pairs.len(), pt.total_cells(), pt.memory_bytes() / 1024 / 1024);

    // === EnchuDB 構築 ===
    let dir = "/tmp/enchudb_pair.db";
    let _ = std::fs::remove_file(dir);
    let mut db = enchudb::Engine::create_with_capacity(dir, n).unwrap();
    for &(name, max_v) in &himo_defs {
        db.define_himo(name, enchudb::HimoType::Value, max_v);
    }
    for i in 0..n {
        let e = db.entity();
        for (h, &(name, _)) in himo_defs.iter().enumerate() {
            db.tie(e, name, columns[h][i as usize]);
        }
        if i % 1_000_000 == 999_999 {
            db.commit();
        }
    }
    db.commit();
    db.rebuild();

    // === クエリ ===
    let queries: Vec<(&str, Vec<(&str, u32)>)> = vec![
        ("tenant+dept", vec![("tenant", 3), ("dept", 2)]),
        ("tenant+salary", vec![("tenant", 3), ("salary", 500)]),
        ("salary+age", vec![("salary", 500), ("age", 35)]),
        ("tenant+dept+status", vec![("tenant", 3), ("dept", 2), ("status", 1)]),
        ("tenant+dept+salary", vec![("tenant", 3), ("dept", 2), ("salary", 500)]),
        ("5条件", vec![("tenant", 3), ("dept", 2), ("role", 1), ("status", 1), ("year", 2)]),
        ("7条件全部", vec![("tenant", 3), ("dept", 2), ("role", 1), ("status", 1), ("year", 2), ("salary", 500), ("age", 35)]),
    ];

    // ペアテーブルクエリ
    println!("\n=== ペアテーブル + Column直読み ===");
    println!("| クエリ | 速度 | 件数 | 候補数 |");
    println!("|---|---|---|---|");

    for (name, q) in &queries {
        // 条件を himo index に変換
        let conds: Vec<(usize, u32)> = q.iter().map(|&(hname, val)| {
            let idx = himo_defs.iter().position(|&(n, _)| n == hname).unwrap();
            (idx, val)
        }).collect();

        // warmup
        let _ = pt.best_lookup(&conds);

        let iters = 100_000;
        let t = Instant::now();
        let mut count = 0;
        let mut cand_count = 0;
        for _ in 0..iters {
            if let Some((candidates, remaining)) = pt.best_lookup(&conds) {
                cand_count = candidates.len();
                if remaining.is_empty() {
                    count = candidates.len();
                } else {
                    let mut c = 0;
                    for &eid in candidates {
                        let mut pass = true;
                        for &(idx, val) in &remaining {
                            if columns[idx][eid as usize] != val {
                                pass = false;
                                break;
                            }
                        }
                        if pass { c += 1; }
                    }
                    count = c;
                }
            }
        }
        let per = t.elapsed() / iters;
        println!("| {} | {:?} | {} | {} |", name, per, count, cand_count);
    }

    // EnchuDB クエリ
    println!("\n=== EnchuDB v24 (bitmap AND / column filter) ===");
    println!("| クエリ | 速度 | 件数 |");
    println!("|---|---|---|");

    for (name, q) in &queries {
        let refs: Vec<(&str, u32)> = q.iter().map(|&(s, v)| (s, v)).collect();
        let _ = db.query(&refs);

        let iters = 1_000;
        let t = Instant::now();
        let mut count = 0;
        for _ in 0..iters {
            count = db.query(&refs).len();
        }
        let per = t.elapsed() / iters;
        println!("| {} | {:?} | {} |", name, per, count);
    }
}
