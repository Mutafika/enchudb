/// 条件数 1〜10 の速度テスト

#[cfg(not(feature = "v26"))]
fn main() { println!("--features v26"); }

#[cfg(feature = "v26")]
fn main() {
    use std::time::Instant;

    let dir = "/tmp/v26_many_conds.db";
    let _ = std::fs::remove_file(dir);
    let mut db = enchudb::Engine::create_with_capacity(dir, 1_000_000).unwrap();

    // 10紐
    let himos: Vec<(&str, u32)> = vec![
        ("h0", 10), ("h1", 8), ("h2", 5), ("h3", 4), ("h4", 6),
        ("h5", 7), ("h6", 3), ("h7", 9), ("h8", 11), ("h9", 12),
    ];
    for &(name, max_v) in &himos {
        db.define_himo(name, enchudb::HimoType::Number, max_v);
    }

    let n = 500_000u32;
    let mults = [1u32, 3, 7, 11, 13, 17, 19, 23, 29, 31];
    for i in 0..n {
        let e = db.entity();
        for (h, &(name, max_v)) in himos.iter().enumerate() {
            db.tie(e, name, (i * mults[h]) % max_v);
        }
    }
    db.rebuild();
    db.rebuild_pairs();

    println!("=== 条件数 vs 速度 ({} entities, {} himos) ===", n, himos.len());
    println!("| 条件数 | v26 | 件数 |");
    println!("|---|---|---|");

    for num_conds in 1..=10 {
        let q: Vec<(&str, u32)> = himos[..num_conds].iter()
            .map(|&(name, _)| (name, 1u32))
            .collect();

        let _ = db.query(&q);
        let iters = 100_000u32;
        let t = Instant::now();
        let mut count = 0;
        for _ in 0..iters {
            count = db.query(&q).len();
        }
        let per = t.elapsed() / iters;
        println!("| {} | {:?} | {} |", num_conds, per, count);
    }
}
