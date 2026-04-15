/// v26 CAS 統合テスト — ペアテーブルの差分更新 + commit/rollback

fn main() {
    let dir = "/tmp/enchudb_v26_cas_test.db";
    let _ = std::fs::remove_file(dir);

    let mut db = enchudb::Engine::create(dir).unwrap();
    db.define_himo("city", enchudb::HimoType::Value, 3);   // 0,1,2
    db.define_himo("dept", enchudb::HimoType::Value, 4);    // 0,1,2,3
    db.define_himo("status", enchudb::HimoType::Value, 2);  // 0,1

    // 10 entities
    for i in 0..10u32 {
        let e = db.entity();
        db.tie(e, "city", i % 3);
        db.tie(e, "dept", i % 4);
        db.tie(e, "status", i % 2);
    }

    db.rebuild();

    #[cfg(feature = "v26")]
    {
        use std::time::Instant;

        db.rebuild_pairs();
        println!("=== 初期状態 ===");
        let r = db.query(&[("city", 0), ("dept", 0)]);
        println!("city=0, dept=0: {:?}", r);
        let r = db.query(&[("city", 1), ("dept", 1)]);
        println!("city=1, dept=1: {:?}", r);

        // === 差分更新テスト ===
        println!("\n=== 差分更新: entity 0 の city を 0 → 2 に変更 ===");
        let eid = 0u32;
        let old_val = db.get(eid, "city").unwrap();
        let new_val = 2u32;
        println!("entity {} の city: {} → {}", eid, old_val, new_val);

        // Column を更新
        db.tie(eid, "city", new_val);
        // ペアテーブルを差分更新
        db.apply_pair_delta(eid, db.himo_id("city").unwrap(), old_val, new_val);

        let r = db.query(&[("city", 0), ("dept", 0)]);
        println!("city=0, dept=0: {:?} (entity 0 がいなくなったはず)", r);
        let r = db.query(&[("city", 2), ("dept", 0)]);
        println!("city=2, dept=0: {:?} (entity 0 が入ったはず)", r);

        // === rollback テスト ===
        println!("\n=== rollback ===");
        db.rebuild_pairs();
        // Column も戻す必要あるが、ここではペアテーブルだけテスト
        // ペアテーブルの状態が戻ったか確認
        let r_after_rollback = db.query(&[("city", 0), ("dept", 0)]);
        println!("city=0, dept=0 (rollback後): {:?}", r_after_rollback);

        // === 再度更新して commit ===
        println!("\n=== 再更新 + commit ===");
        db.apply_pair_delta(eid, db.himo_id("city").unwrap(), old_val, new_val);
        db.rebuild_pairs();
        println!("commit 完了");

        let r = db.query(&[("city", 0), ("dept", 0)]);
        println!("city=0, dept=0: {:?}", r);
        let r = db.query(&[("city", 2), ("dept", 0)]);
        println!("city=2, dept=0: {:?}", r);

        // === ベンチ: 差分更新速度 ===
        println!("\n=== 差分更新ベンチ ===");
        // 大きめのデータで
        let dir2 = "/tmp/enchudb_v26_cas_bench.db";
        let _ = std::fs::remove_file(dir2);
        let mut db2 = enchudb::Engine::create(dir2).unwrap();
        db2.define_himo("tenant", enchudb::HimoType::Value, 10);
        db2.define_himo("dept", enchudb::HimoType::Value, 8);
        db2.define_himo("role", enchudb::HimoType::Value, 4);
        db2.define_himo("status", enchudb::HimoType::Value, 5);

        let n = 100_000u32;
        for i in 0..n {
            let e = db2.entity();
            db2.tie(e, "tenant", i % 10);
            db2.tie(e, "dept", (i * 3) % 8);
            db2.tie(e, "role", (i * 7) % 4);
            db2.tie(e, "status", (i * 11) % 5);
        }
        db2.rebuild();
        db2.rebuild_pairs();

        // 差分更新 1000件
        let t = Instant::now();
        let dept_idx = db2.himo_id("dept").unwrap();
        for i in 0..1000u32 {
            let old = db2.get(i, "dept").unwrap();
            let new_v = (old + 1) % 8;
            db2.tie(i, "dept", new_v);
            db2.apply_pair_delta(i, dept_idx, old, new_v);
        }
        let elapsed = t.elapsed();
        println!("差分更新 1000件: {:?} ({:?}/件)", elapsed, elapsed / 1000);

        // commit
        let t = Instant::now();
        db2.rebuild_pairs();
        println!("commit: {:?}", t.elapsed());

        // クエリ確認
        let t = Instant::now();
        let iters = 10_000u32;
        let mut count = 0;
        for _ in 0..iters {
            count = db2.query(&[("tenant", 3), ("dept", 2)]).len();
        }
        let q = t.elapsed() / iters;
        println!("query tenant=3,dept=2: {:?} ({}件)", q, count);
    }

    #[cfg(not(feature = "v26"))]
    println!("v26 feature 無効。--features v26 で実行してください。");
}
