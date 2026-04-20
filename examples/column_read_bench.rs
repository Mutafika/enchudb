/// Column 直読みのコストを実測

use std::time::Instant;

fn main() {
    let n = 1_000_000u32;
    let dir = "/tmp/enchudb_colread.db";
    let _ = std::fs::remove_file(dir);

    let mut db = enchudb::Engine::create_with_capacity(dir, n).unwrap();
    db.define_himo("salary", enchudb::HimoType::Value, 1000);
    db.define_himo("age", enchudb::HimoType::Value, 60);
    db.define_himo("dept", enchudb::HimoType::Value, 8);

    for i in 0..n {
        let e = db.entity();
        db.tie(e, "salary", (i * 31) % 1000);
        db.tie(e, "age", 20 + (i * 17) % 40);
        db.tie(e, "dept", (i * 7) % 8);
    }
    db.rebuild();

    // salary=500 の entity を取得
    let salary_500 = db.pull_raw("salary", 500);
    let count = salary_500.len();
    println!("salary=500: {}件", count);

    // 1件の Column 直読み
    let iters = 100_000;
    let eid = salary_500[0];
    let t = Instant::now();
    for _ in 0..iters {
        std::hint::black_box(db.get(eid, "age"));
    }
    let per = t.elapsed() / iters;
    println!("get 1回: {:?}", per);

    // k件の Column 直読みフィルタ
    for k in [10, 100, 1000, 10000] {
        let candidates: Vec<u64> = salary_500.iter().take(k).copied().collect();
        let actual_k = candidates.len();
        if actual_k == 0 { continue; }

        let iters = 10_000;
        let t = Instant::now();
        let mut hit = 0u32;
        for _ in 0..iters {
            let mut c = 0;
            for &eid in &candidates {
                if db.get(eid, "age") == Some(35) {
                    c += 1;
                }
            }
            hit = c;
        }
        let per = t.elapsed() / iters;
        println!("フィルタ {}件: {:?} ({} hit), {:.0}ns/件",
            actual_k, per, hit, per.as_nanos() as f64 / actual_k as f64);
    }

    // 比較: query (bitmap AND)
    let iters = 10_000;
    let t = Instant::now();
    let mut count = 0;
    for _ in 0..iters {
        count = db.query(&[("salary", 500), ("age", 35)]).len();
    }
    let per = t.elapsed() / iters;
    println!("\nquery(salary=500, age=35): {:?}, {}件", per, count);

    let t = Instant::now();
    for _ in 0..iters {
        count = db.query(&[("salary", 500), ("age", 35), ("dept", 3)]).len();
    }
    let per = t.elapsed() / iters;
    println!("query(salary=500, age=35, dept=3): {:?}, {}件", per, count);
}
