/// v24 vs v26 統合ベンチマーク
///
/// v26 feature を有効にすると combo テーブルが Engine 内部で動く。
/// 外部の手動 combo テーブルではなく、Engine.query() が自動で使う。

use std::time::Instant;

fn main() {
    let n = 100_000u32;

    let dir = "/tmp/enchudb_v26_integrated.db";
    let _ = std::fs::remove_file(dir);

    let mut db = enchudb::Engine::create(dir).unwrap();

    // 少ユニーク紐
    db.define_himo("city", enchudb::HimoType::Value, 3);
    db.define_himo("dept", enchudb::HimoType::Value, 4);
    db.define_himo("grade", enchudb::HimoType::Value, 5);

    let t = Instant::now();
    for i in 0..n {
        let e = db.entity();
        db.tie(e, "city", i % 3);
        db.tie(e, "dept", (i * 7) % 4);
        db.tie(e, "grade", (i * 13) % 5);
        db.tie(e, "price", (i * 31) % 10000);
        db.tie(e, "age", (i * 17) % 100);
    }
    println!("投入: {}件, {:?}", n, t.elapsed());

    let t = Instant::now();
    db.rebuild();
    println!("rebuild: {:?}", t.elapsed());

    #[cfg(feature = "v26")]
    {
        let t = Instant::now();
        db.rebuild_pairs();
        println!("rebuild_pairs: {:?}", t.elapsed());
    }

    let queries: Vec<(&str, Vec<(&str, u32)>)> = vec![
        ("5条件(混合)", vec![("city", 1), ("dept", 2), ("grade", 3), ("price", 500), ("age", 30)]),
        ("3条件(少ユニークのみ)", vec![("city", 1), ("dept", 2), ("grade", 3)]),
        ("2条件(多ユニーク含)", vec![("city", 1), ("price", 500)]),
        ("1条件(price)", vec![("price", 500)]),
        ("2条件(少ユニーク)", vec![("city", 1), ("dept", 2)]),
        ("4条件(少3+多1)", vec![("city", 1), ("dept", 2), ("grade", 3), ("age", 30)]),
    ];

    #[cfg(feature = "v26")]
    println!("\n=== v26 (combo テーブル有効) ===");
    #[cfg(not(feature = "v26"))]
    println!("\n=== v24 (combo テーブルなし) ===");

    println!("| クエリ | 速度 | 件数 |");
    println!("|---|---|---|");

    for (name, q) in &queries {
        let refs: Vec<(&str, u32)> = q.iter().map(|&(s, v)| (s, v)).collect();
        // warmup
        let _ = db.query(&refs);

        let iters = 10000;
        let t = Instant::now();
        let mut count = 0;
        for _ in 0..iters {
            count = db.query(&refs).len();
        }
        let elapsed = t.elapsed() / iters;
        println!("| {} | {:?} | {} |", name, elapsed, count);
    }
}
