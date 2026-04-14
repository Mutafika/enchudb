/// v26 実用ベンチ — SaaS マルチテナント風データ
///
/// tenant(10社) × dept(8部署) × role(4種) × status(5種) × year(5年)
/// + employee_id(100000通り) + salary(1000段階) + age(60通り)
/// 100万 entity

use std::time::Instant;

fn main() {
    let n = 1_000_000u32;
    let dir = "/tmp/enchudb_v26_real.db";
    let _ = std::fs::remove_file(dir);

    let mut db = enchudb::Engine::create_with_capacity(dir, n).unwrap();

    // 少ユニーク紐（combo 対象）
    db.define_himo("tenant", enchudb::HimoType::Value, 10);
    db.define_himo("dept", enchudb::HimoType::Value, 8);
    db.define_himo("role", enchudb::HimoType::Value, 4);
    db.define_himo("status", enchudb::HimoType::Value, 5);
    db.define_himo("year", enchudb::HimoType::Value, 5);
    // 多ユニーク紐
    db.define_himo("salary", enchudb::HimoType::Value, 1000);
    db.define_himo("age", enchudb::HimoType::Value, 60);

    println!("=== データ投入 ===");
    let t = Instant::now();
    for i in 0..n {
        let e = db.entity();
        db.tie(e, "tenant", i % 10);
        db.tie(e, "dept", (i * 3) % 8);
        db.tie(e, "role", (i * 7) % 4);
        db.tie(e, "status", (i * 11) % 5);
        db.tie(e, "year", (i * 13) % 5);
        db.tie(e, "salary", (i * 31) % 1000);
        db.tie(e, "age", 20 + (i * 17) % 40);
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

    // 実用的なクエリパターン
    let queries: Vec<(&str, Vec<(&str, u32)>)> = vec![
        // ダッシュボード: 自社の部署×ステータス
        ("自社+部署+ステータス", vec![("tenant", 3), ("dept", 2), ("status", 1)]),
        // 人事: 自社の全条件
        ("自社+部署+役職+ステータス+年度", vec![("tenant", 3), ("dept", 2), ("role", 1), ("status", 1), ("year", 2)]),
        // 検索: 自社+給与帯
        ("自社+給与帯", vec![("tenant", 3), ("salary", 500)]),
        // レポート: 自社+年度+年齢
        ("自社+年度+年齢", vec![("tenant", 3), ("year", 2), ("age", 35)]),
        // 全社横断(管理者)
        ("全部署+ステータス", vec![("status", 1)]),
        // 複雑クエリ: 5少ユニーク + 2多ユニーク
        ("7条件全部", vec![("tenant", 3), ("dept", 2), ("role", 1), ("status", 1), ("year", 2), ("salary", 500), ("age", 35)]),
        // テナント一覧
        ("テナント単体", vec![("tenant", 3)]),
        // ミス（該当なし）
        ("ミス(salary=999)", vec![("tenant", 3), ("dept", 2), ("salary", 999)]),
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
