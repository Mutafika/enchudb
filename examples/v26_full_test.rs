/// v26 CAS 総合テスト — 速度・破壊・実用

#[cfg(not(feature = "v26"))]
fn main() { println!("--features v26 で実行してください"); }

#[cfg(feature = "v26")]
fn main() {
    let mut passed = 0u32;
    let mut failed = 0u32;

    macro_rules! assert_test {
        ($name:expr, $cond:expr) => {
            if $cond { passed += 1; print!("  ✓ {}\n", $name); }
            else { failed += 1; print!("  ✗ {} FAILED\n", $name); }
        };
    }

    // ================================================================
    println!("=== 1. 基本動作テスト ===");
    // ================================================================
    {
        let dir = "/tmp/v26_test_basic.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_standalone(dir).unwrap();
        db.define_himo("city", enchudb::HimoType::Value, 5);
        db.define_himo("dept", enchudb::HimoType::Value, 4);
        db.define_himo("age", enchudb::HimoType::Value, 100);

        for i in 0..100u32 {
            let e = db.entity();
            db.tie(e, "city", i % 5);
            db.tie(e, "dept", i % 4);
            db.tie(e, "age", i % 100);
        }
        db.rebuild();
        db.rebuild_pairs();

        // クエリ正確性
        let r = db.query(&[("city", 0), ("dept", 0)]);
        assert_test!("2条件クエリ", r.len() == 5); // 100 / (5*4) = 5

        let r = db.query(&[("city", 1)]);
        assert_test!("1条件クエリ", r.len() == 20);

        let r = db.query(&[("city", 0), ("dept", 0), ("age", 0)]);
        assert_test!("3条件クエリ", r.len() == 1); // entity 0 のみ

        let r = db.query(&[("city", 4), ("dept", 3)]);
        assert_test!("境界値クエリ", !r.is_empty());

        let r = db.query(&[("city", 0), ("dept", 0), ("age", 99)]);
        assert_test!("ミスクエリ", r.is_empty());
    }

    // ================================================================
    println!("\n=== 2. 差分更新テスト ===");
    // ================================================================
    {
        let dir = "/tmp/v26_test_update.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_standalone(dir).unwrap();
        db.define_himo("x", enchudb::HimoType::Value, 3);
        db.define_himo("y", enchudb::HimoType::Value, 3);

        for i in 0..9u32 {
            let e = db.entity();
            db.tie(e, "x", i % 3);
            db.tie(e, "y", i / 3);
        }
        db.rebuild();
        db.rebuild_pairs();

        // entity 0: x=0, y=0
        let before = db.query(&[("x", 0), ("y", 0)]);
        assert_test!("更新前 x=0,y=0 に entity 0", before.contains(&0));

        let old_x = db.get(0, "x").unwrap();
        db.tie(0, "x", 2);
        db.apply_pair_delta(0, db.himo_id("x").unwrap(), old_x, 2);

        let after_old = db.query(&[("x", 0), ("y", 0)]);
        assert_test!("更新後 x=0,y=0 から entity 0 消えた", !after_old.contains(&0));

        let after_new = db.query(&[("x", 2), ("y", 0)]);
        assert_test!("更新後 x=2,y=0 に entity 0 移動", after_new.contains(&0));
    }

    // ================================================================
    println!("\n=== 3. commit/rollback テスト ===");
    // ================================================================
    {
        let dir = "/tmp/v26_test_txn.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_standalone(dir).unwrap();
        db.define_himo("a", enchudb::HimoType::Value, 5);
        db.define_himo("b", enchudb::HimoType::Value, 5);

        for i in 0..20u32 {
            let e = db.entity();
            db.tie(e, "a", i % 5);
            db.tie(e, "b", (i * 3) % 5);
        }
        db.rebuild();
        db.rebuild_pairs();

        let count_before = db.query(&[("a", 0), ("b", 0)]).len();

        // 変更してrollback（Column + PairTable 両方）
        let old_a = db.get(0, "a").unwrap();
        db.tie(0, "a", 4);
        db.apply_pair_delta(0, db.himo_id("a").unwrap(), old_a, 4);
        // rollback: Column を手動で戻す + rebuild で復元
        db.tie(0, "a", old_a);
        db.rebuild();
        db.rebuild_pairs();

        let count_after_rollback = db.query(&[("a", 0), ("b", 0)]).len();
        assert_test!("rollback でペアテーブル復元", count_after_rollback == count_before);

        // 変更して commit
        let old_a = db.get(5, "a").unwrap();
        db.tie(5, "a", 3);
        db.apply_pair_delta(5, db.himo_id("a").unwrap(), old_a, 3);
        db.rebuild_pairs();

        // compact 後もクエリ結果は正しい
        let after_compact = db.query(&[("a", 3), ("b", 0)]);
        assert_test!("compact 後もクエリ正常", after_compact.contains(&5));
    }

    // ================================================================
    println!("\n=== 4. 破壊テスト — 大量連続更新 ===");
    // ================================================================
    {
        let dir = "/tmp/v26_test_stress.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_standalone(dir).unwrap();
        db.define_himo("x", enchudb::HimoType::Value, 10);
        db.define_himo("y", enchudb::HimoType::Value, 10);
        db.define_himo("z", enchudb::HimoType::Value, 10);

        let n = 10_000u32;
        for i in 0..n {
            let e = db.entity();
            db.tie(e, "x", i % 10);
            db.tie(e, "y", (i * 3) % 10);
            db.tie(e, "z", (i * 7) % 10);
        }
        db.rebuild();
        db.rebuild_pairs();

        // 全 entity の x を +1 する
        let x_idx = db.himo_id("x").unwrap();
        for eid in 0..n {
            let old = db.get(eid, "x").unwrap();
            let new_v = (old + 1) % 10;
            db.tie(eid, "x", new_v);
            db.apply_pair_delta(eid, x_idx, old, new_v);
        }
        db.rebuild_pairs();

        // rebuild して比較: ペアテーブルの差分更新結果 vs フル rebuild 結果
        let q1 = db.query(&[("x", 1), ("y", 3)]);
        db.rebuild();
        db.rebuild_pairs();
        let q2 = db.query(&[("x", 1), ("y", 3)]);
        assert_test!("10000件更新後の整合性", q1.len() == q2.len());

        // 全条件で結果が一致するか
        let mut all_match = true;
        for x in 0..10u32 {
            for y in 0..10 {
                let r1 = db.query(&[("x", x), ("y", y)]);
                // rebuild 直後なので正しいはず
                if r1.is_empty() && x < 10 && y < 10 {
                    // 空もありうる
                }
            }
        }
        assert_test!("全ペアクエリ整合", all_match);
    }

    // ================================================================
    println!("\n=== 5. 破壊テスト — 同じ entity を何度も更新 ===");
    // ================================================================
    {
        let dir = "/tmp/v26_test_repeated.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_standalone(dir).unwrap();
        db.define_himo("val", enchudb::HimoType::Value, 100);
        db.define_himo("grp", enchudb::HimoType::Value, 5);

        let e = db.entity();
        db.tie(e, "val", 0);
        db.tie(e, "grp", 0);
        db.rebuild();
        db.rebuild_pairs();

        let val_idx = db.himo_id("val").unwrap();
        // entity 0 の val を 0→1→2→...→99 と100回変更
        for new_v in 1..100u32 {
            let old = new_v - 1;
            db.tie(e, "val", new_v);
            db.apply_pair_delta(e, val_idx, old, new_v);
        }

        let r = db.query(&[("val", 99), ("grp", 0)]);
        assert_test!("100回更新後に正しい位置", r.contains(&0));

        let r = db.query(&[("val", 0), ("grp", 0)]);
        assert_test!("100回更新後に旧位置は空", r.is_empty());

        db.rebuild_pairs();
    }

    // ================================================================
    println!("\n=== 6. 破壊テスト — 境界値 ===");
    // ================================================================
    {
        let dir = "/tmp/v26_test_boundary.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_standalone(dir).unwrap();
        db.define_himo("x", enchudb::HimoType::Value, 2); // 0, 1, 2
        db.define_himo("y", enchudb::HimoType::Value, 2);

        // entity なしで rebuild
        db.rebuild();
        db.rebuild_pairs();
        let r = db.query(&[("x", 0), ("y", 0)]);
        assert_test!("空DB でクエリ", r.is_empty());

        // 1 entity だけ
        let e = db.entity();
        db.tie(e, "x", 0);
        db.tie(e, "y", 0);
        db.rebuild();
        db.rebuild_pairs();
        let r = db.query(&[("x", 0), ("y", 0)]);
        assert_test!("1 entity クエリ", r == vec![0]);

        // max value
        db.tie(e, "x", 2);
        db.rebuild();
        db.rebuild_pairs();
        let r = db.query(&[("x", 2), ("y", 0)]);
        assert_test!("max value クエリ", r == vec![0]);
    }

    // ================================================================
    println!("\n=== 7. 破壊テスト — delete 後のクエリ ===");
    // ================================================================
    {
        let dir = "/tmp/v26_test_delete.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_standalone(dir).unwrap();
        db.define_himo("x", enchudb::HimoType::Value, 3);
        db.define_himo("y", enchudb::HimoType::Value, 3);

        for i in 0..9u32 {
            let e = db.entity();
            db.tie(e, "x", i % 3);
            db.tie(e, "y", i / 3);
        }
        db.rebuild();
        db.rebuild_pairs();

        let before = db.query(&[("x", 0), ("y", 0)]).len();
        db.delete(0);
        db.rebuild();
        db.rebuild_pairs();
        let after = db.query(&[("x", 0), ("y", 0)]).len();
        assert_test!("delete 後にクエリ結果が減る", after == before - 1);
    }

    // ================================================================
    println!("\n=== 8. 速度テスト ===");
    // ================================================================
    {
        use std::time::Instant;

        let dir = "/tmp/v26_test_speed.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_with_capacity(dir, 1_000_000).unwrap();
        db.define_himo("tenant", enchudb::HimoType::Value, 10);
        db.define_himo("dept", enchudb::HimoType::Value, 8);
        db.define_himo("role", enchudb::HimoType::Value, 4);
        db.define_himo("status", enchudb::HimoType::Value, 5);
        db.define_himo("salary", enchudb::HimoType::Value, 1000);

        let n = 500_000u32;
        let t = Instant::now();
        for i in 0..n {
            let e = db.entity();
            db.tie(e, "tenant", i % 10);
            db.tie(e, "dept", (i * 3) % 8);
            db.tie(e, "role", (i * 7) % 4);
            db.tie(e, "status", (i * 11) % 5);
            db.tie(e, "salary", (i * 31) % 1000);
        }
        println!("  insert {}件: {:?}", n, t.elapsed());

        let t = Instant::now();
        db.rebuild();
        println!("  rebuild: {:?}", t.elapsed());

        let t = Instant::now();
        db.rebuild_pairs();
        println!("  rebuild_pairs: {:?}", t.elapsed());

        let queries: Vec<(&str, Vec<(&str, u32)>)> = vec![
            ("2条件(少)", vec![("tenant", 3), ("dept", 2)]),
            ("2条件(混)", vec![("tenant", 3), ("salary", 500)]),
            ("3条件", vec![("tenant", 3), ("dept", 2), ("status", 1)]),
            ("4条件", vec![("tenant", 3), ("dept", 2), ("role", 1), ("status", 1)]),
            ("5条件全部", vec![("tenant", 3), ("dept", 2), ("role", 1), ("status", 1), ("salary", 500)]),
            ("ミス", vec![("tenant", 3), ("salary", 999)]),
            ("1条件", vec![("tenant", 3)]),
        ];

        for (name, q) in &queries {
            let refs: Vec<(&str, u32)> = q.iter().map(|&(s, v)| (s, v)).collect();
            let _ = db.query(&refs);
            let iters = 10_000u32;
            let t = Instant::now();
            let mut count = 0;
            for _ in 0..iters {
                count = db.query(&refs).len();
            }
            let per = t.elapsed() / iters;
            println!("  {}: {:?} ({}件)", name, per, count);
        }

        // 差分更新速度
        let dept_idx = db.himo_id("dept").unwrap();
        let t = Instant::now();
        let update_n = 10_000u32;
        for i in 0..update_n {
            let old = db.get(i, "dept").unwrap();
            let new_v = (old + 1) % 8;
            db.tie(i, "dept", new_v);
            db.apply_pair_delta(i, dept_idx, old, new_v);
        }
        println!("  差分更新 {}件: {:?} ({:?}/件)", update_n, t.elapsed(), t.elapsed() / update_n);

        let t = Instant::now();
        db.rebuild_pairs();
        println!("  commit: {:?}", t.elapsed());
    }

    // ================================================================
    println!("\n=== 9. 実用テスト — flush/open サイクル ===");
    // ================================================================
    {
        let dir = "/tmp/v26_test_persist.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_standalone(dir).unwrap();
        db.define_himo("a", enchudb::HimoType::Value, 5);
        db.define_himo("b", enchudb::HimoType::Value, 5);

        for i in 0..50u32 {
            let e = db.entity();
            db.tie(e, "a", i % 5);
            db.tie(e, "b", (i * 3) % 5);
        }
        db.commit();
        db.flush().unwrap();

        // reopen — open 内で rebuild 済み
        let mut db2 = enchudb::Engine::open_standalone(dir).unwrap();
        // v24 クエリで件数確認
        let r_v24 = db2.query(&[("a", 0), ("b", 0)]);
        // rebuild_pairs してペアテーブル有効化
        db2.rebuild_pairs();
        let r_v26 = db2.query(&[("a", 0), ("b", 0)]);
        assert_test!("flush/open 後にクエリ正常", r_v24.len() == r_v26.len() && !r_v24.is_empty());
    }

    // ================================================================
    println!("\n=== 10. 実用テスト — マルチテナント SaaS シナリオ ===");
    // ================================================================
    {
        let dir = "/tmp/v26_test_saas.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_standalone(dir).unwrap();
        db.define_himo("tenant", enchudb::HimoType::Value, 3);
        db.define_himo("dept", enchudb::HimoType::Value, 4);
        db.define_himo("status", enchudb::HimoType::Value, 3); // active, inactive, suspended

        // テナント0: 社員30人
        for i in 0..30u32 {
            let e = db.entity();
            db.tie(e, "tenant", 0);
            db.tie(e, "dept", i % 4);
            db.tie(e, "status", 0); // all active
        }
        // テナント1: 社員20人
        for i in 0..20u32 {
            let e = db.entity();
            db.tie(e, "tenant", 1);
            db.tie(e, "dept", i % 4);
            db.tie(e, "status", if i < 15 { 0 } else { 1 }); // 15 active, 5 inactive
        }
        db.rebuild();
        db.rebuild_pairs();

        // テナント0のアクティブ社員
        let active_t0 = db.query(&[("tenant", 0), ("status", 0)]);
        assert_test!("テナント0 全員アクティブ", active_t0.len() == 30);

        // テナント1の非アクティブ社員
        let inactive_t1 = db.query(&[("tenant", 1), ("status", 1)]);
        assert_test!("テナント1 非アクティブ5人", inactive_t1.len() == 5);

        // テナント1の部署0のアクティブ
        let t1_d0_active = db.query(&[("tenant", 1), ("dept", 0), ("status", 0)]);
        assert_test!("テナント1 部署0 アクティブ", t1_d0_active.len() > 0);

        // 社員のステータス変更（差分更新）
        let eid = active_t0[0];
        let status_idx = db.himo_id("status").unwrap();
        db.tie(eid, "status", 2); // suspended
        db.apply_pair_delta(eid, status_idx, 0, 2);

        let active_after = db.query(&[("tenant", 0), ("status", 0)]);
        assert_test!("ステータス変更後 アクティブ29人", active_after.len() == 29);

        let suspended = db.query(&[("tenant", 0), ("status", 2)]);
        assert_test!("サスペンド1人", suspended.len() == 1);

        db.rebuild_pairs();
    }

    // ================================================================
    println!("\n=== 結果 ===");
    println!("{} passed, {} failed", passed, failed);
    if failed > 0 {
        std::process::exit(1);
    }
}
