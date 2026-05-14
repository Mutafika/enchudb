/// v26 破壊テスト — 壊れるまで叩く

#[cfg(not(feature = "v26"))]
fn main() { println!("--features v26"); }

#[cfg(feature = "v26")]
fn main() {
    let mut passed = 0u32;
    let mut failed = 0u32;

    macro_rules! assert_test {
        ($name:expr, $cond:expr) => {
            if $cond { passed += 1; }
            else { failed += 1; print!("  ✗ {} FAILED\n", $name); }
        };
    }
    macro_rules! assert_test_v {
        ($name:expr, $cond:expr) => {
            if $cond { passed += 1; print!("  ✓ {}\n", $name); }
            else { failed += 1; print!("  ✗ {} FAILED\n", $name); }
        };
    }

    // ================================================================
    println!("=== 1. 全 entity 削除して再投入 ===");
    {
        let dir = "/tmp/v26_dest_1.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_standalone(dir).unwrap();
        db.define_himo("x", enchudb::HimoType::Number, 5);
        db.define_himo("y", enchudb::HimoType::Number, 5);

        // 100件投入
        for i in 0..100u32 {
            let e = db.entity();
            db.tie(e, "x", i % 5);
            db.tie(e, "y", i % 5);
        }
        db.rebuild();
        db.rebuild_pairs();
        let before = db.query(&[("x", 0), ("y", 0)]).len();

        // 全削除
        for eid in 0..100u32 { db.delete(eid); }
        db.rebuild();
        db.rebuild_pairs();
        let after_delete = db.query(&[("x", 0), ("y", 0)]).len();
        assert_test_v!("全削除後 0件", after_delete == 0);

        // 再投入
        for i in 0..100u32 {
            let e = db.entity();
            db.tie(e, "x", i % 5);
            db.tie(e, "y", i % 5);
        }
        db.rebuild();
        db.rebuild_pairs();
        let after_reinsert = db.query(&[("x", 0), ("y", 0)]).len();
        assert_test_v!("再投入後の件数一致", after_reinsert == before);
    }

    // ================================================================
    println!("\n=== 2. 全 entity の全紐を untie ===");
    {
        let dir = "/tmp/v26_dest_2.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_standalone(dir).unwrap();
        db.define_himo("a", enchudb::HimoType::Number, 3);
        db.define_himo("b", enchudb::HimoType::Number, 3);

        for i in 0..50u32 {
            let e = db.entity();
            db.tie(e, "a", i % 3);
            db.tie(e, "b", (i * 2) % 3);
        }
        db.rebuild();
        db.rebuild_pairs();

        // 全 untie
        for eid in 0..50u32 {
            db.untie(eid, "a");
            db.untie(eid, "b");
        }
        db.rebuild();
        db.rebuild_pairs();

        let r = db.query(&[("a", 0), ("b", 0)]);
        assert_test_v!("全 untie 後 0件", r.is_empty());

        // get も None
        assert_test_v!("untie 後 get=None", db.get(0, "a").is_none());
    }

    // ================================================================
    println!("\n=== 3. 同じ entity に同じ値を何度も tie ===");
    {
        let dir = "/tmp/v26_dest_3.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_standalone(dir).unwrap();
        db.define_himo("x", enchudb::HimoType::Number, 5);

        let e = db.entity();
        for _ in 0..1000 {
            db.tie(e, "x", 3);
        }
        db.rebuild();
        db.rebuild_pairs();

        assert_test_v!("1000回同値tie後 get=3", db.get(e, "x") == Some(3));
        let r = db.query(&[("x", 3)]);
        assert_test_v!("1000回同値tie後 query=1件", r.len() == 1);
    }

    // ================================================================
    println!("\n=== 4. 大量紐（max_himos に迫る）===");
    {
        let dir = "/tmp/v26_dest_4.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_standalone(dir).unwrap();

        // 50紐定義
        for i in 0..50u32 {
            db.define_himo(&format!("h{}", i), enchudb::HimoType::Number, 3);
        }
        let e = db.entity();
        for i in 0..50u32 {
            db.tie(e, &format!("h{}", i), i % 3);
        }
        db.rebuild();
        db.rebuild_pairs();

        assert_test_v!("50紐 get 正常", db.get(e, "h49") == Some(49 % 3));
        let r = db.query(&[("h0", 0), ("h1", 1)]);
        assert_test_v!("50紐 query 正常", r.contains(&0));
    }

    // ================================================================
    println!("\n=== 5. tie → flush → open → query サイクル 10回 ===");
    {
        let dir = "/tmp/v26_dest_5.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_standalone(dir).unwrap();
        db.define_himo("round", enchudb::HimoType::Number, 10);
        db.define_himo("val", enchudb::HimoType::Number, 100);

        let mut all_ok = true;
        for round in 0..10u32 {
            for i in 0..100u32 {
                let e = db.entity();
                db.tie(e, "round", round);
                db.tie(e, "val", i);
            }
            db.commit();
            db.flush().unwrap();

            let mut db2 = enchudb::Engine::open_standalone(dir).unwrap();
            db2.rebuild_pairs();
            let count = db2.query(&[("round", round)]).len();
            if count != 100 {
                println!("  round {} で不一致: {} != 100", round, count);
                all_ok = false;
            }
            // 元の db に戻す
            db = db2;
        }
        assert_test_v!("10回 flush/open サイクル", all_ok);
    }

    // ================================================================
    println!("\n=== 6. 差分更新の正確性（全ペア検証）===");
    {
        let dir = "/tmp/v26_dest_6.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_standalone(dir).unwrap();
        db.define_himo("a", enchudb::HimoType::Number, 5);
        db.define_himo("b", enchudb::HimoType::Number, 4);
        db.define_himo("c", enchudb::HimoType::Number, 3);

        let n = 10_000u32;
        for i in 0..n {
            let e = db.entity();
            db.tie(e, "a", i % 5);
            db.tie(e, "b", (i * 3) % 4);
            db.tie(e, "c", (i * 7) % 3);
        }
        db.rebuild();
        db.rebuild_pairs();

        // 5000件を差分更新
        let a_idx = db.himo_id("a").unwrap();
        for i in 0..5000u32 {
            let old = db.get(i, "a").unwrap();
            let new_v = (old + 2) % 5;
            db.tie(i, "a", new_v);
            db.apply_pair_delta(i, a_idx, old, new_v);
        }

        // 差分更新の結果
        let mut delta_results: Vec<Vec<u32>> = Vec::new();
        for a in 0..5u32 {
            for b in 0..4u32 {
                delta_results.push(db.query(&[("a", a), ("b", b)]));
            }
        }

        // フル rebuild して比較
        db.rebuild();
        db.rebuild_pairs();

        let mut all_match = true;
        let mut idx = 0;
        for a in 0..5u32 {
            for b in 0..4u32 {
                let rebuild_result = db.query(&[("a", a), ("b", b)]);
                let mut d = delta_results[idx].clone();
                let mut r = rebuild_result.clone();
                d.sort();
                r.sort();
                if d != r {
                    println!("  不一致: a={}, b={}: delta {} != rebuild {}", a, b, d.len(), r.len());
                    all_match = false;
                }
                idx += 1;
            }
        }
        assert_test_v!("5000件差分更新 vs フルrebuild 全一致", all_match);
    }

    // ================================================================
    println!("\n=== 7. 交互に tie/untie ===");
    {
        let dir = "/tmp/v26_dest_7.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_standalone(dir).unwrap();
        db.define_himo("x", enchudb::HimoType::Number, 10);
        db.define_himo("y", enchudb::HimoType::Number, 10);

        let e = db.entity();
        for i in 0..100u32 {
            db.tie(e, "x", i % 10);
            if i % 3 == 0 { db.untie(e, "x"); }
        }
        db.rebuild();
        db.rebuild_pairs();

        // 最終状態: 99 % 3 == 0 なので untie されてるはず
        let val = db.get(e, "x");
        assert_test_v!("交互 tie/untie 最終状態", val.is_none());
    }

    // ================================================================
    println!("\n=== 8. 値 0 の扱い ===");
    {
        let dir = "/tmp/v26_dest_8.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_standalone(dir).unwrap();
        db.define_himo("x", enchudb::HimoType::Number, 10);
        db.define_himo("y", enchudb::HimoType::Number, 10);

        let e = db.entity();
        db.tie(e, "x", 0);
        db.tie(e, "y", 0);
        db.rebuild();
        db.rebuild_pairs();

        assert_test_v!("値0 get", db.get(e, "x") == Some(0));
        let r = db.query(&[("x", 0), ("y", 0)]);
        assert_test_v!("値0 query", r.contains(&e));
    }

    // ================================================================
    println!("\n=== 9. entity ID 飛び番 ===");
    {
        let dir = "/tmp/v26_dest_9.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_standalone(dir).unwrap();
        db.define_himo("x", enchudb::HimoType::Number, 5);
        db.define_himo("y", enchudb::HimoType::Number, 5);

        // 100件作って偶数番を削除
        for i in 0..100u32 {
            let e = db.entity();
            db.tie(e, "x", i % 5);
            db.tie(e, "y", (i * 2) % 5);
        }
        for eid in (0..100u32).step_by(2) {
            db.delete(eid);
        }
        // 50件追加（削除されたIDが再利用される）
        for i in 0..50u32 {
            let e = db.entity();
            db.tie(e, "x", (i + 1) % 5);
            db.tie(e, "y", (i + 2) % 5);
        }
        db.rebuild();
        db.rebuild_pairs();

        let total: usize = (0..5u32).map(|x| db.query(&[("x", x)]).len()).sum();
        assert_test_v!("飛び番後の総数 100件", total == 100);
    }

    // ================================================================
    println!("\n=== 10. tie_text + query 混在 ===");
    {
        let dir = "/tmp/v26_dest_10.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_standalone(dir).unwrap();
        db.define_himo("dept", enchudb::HimoType::Number, 5);

        for i in 0..100u32 {
            let e = db.entity();
            db.tie(e, "dept", i % 5);
            db.tie_text(e, "name", &format!("user_{}", i));
        }
        db.rebuild();
        db.rebuild_pairs();

        let r = db.query(&[("dept", 3)]);
        assert_test_v!("tie_text 混在 query", r.len() == 20);

        // vocab_id で検索
        let vid = db.vocab_id("user_3");
        assert_test_v!("vocab_id 存在", vid.is_some());
    }

    // ================================================================
    println!("\n=== 11. 大量 entity (50万件) flush/open ===");
    {
        let dir = "/tmp/v26_dest_11.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_with_capacity(dir, 500_000).unwrap();
        db.define_himo("x", enchudb::HimoType::Number, 10);
        db.define_himo("y", enchudb::HimoType::Number, 8);

        for i in 0..500_000u32 {
            let e = db.entity();
            db.tie(e, "x", i % 10);
            db.tie(e, "y", (i * 3) % 8);
            if i % 100_000 == 99_999 { db.commit(); }
        }
        db.commit();
        db.flush().unwrap();

        let mut db2 = enchudb::Engine::open_standalone(dir).unwrap();
        db2.rebuild_pairs();
        let r = db2.query(&[("x", 5), ("y", 3)]);
        assert_test_v!("50万件 flush/open query", !r.is_empty());
    }

    // ================================================================
    println!("\n=== 12. rollback 後の再操作 ===");
    {
        let dir = "/tmp/v26_dest_12.db";
        let _ = std::fs::remove_file(dir);
        let mut db = enchudb::Engine::create_standalone(dir).unwrap();
        db.define_himo("x", enchudb::HimoType::Number, 5);

        for i in 0..10u32 {
            let e = db.entity();
            db.tie(e, "x", i % 5);
        }
        db.commit();

        // 追加してrollback
        for _ in 0..10 {
            let e = db.entity();
            db.tie(e, "x", 4);
        }
        db.rollback();

        // rollback 後に再操作
        for i in 0..5u32 {
            let e = db.entity();
            db.tie(e, "x", i);
        }
        db.commit();
        db.rebuild();
        db.rebuild_pairs();

        let total: usize = (0..5u32).map(|v| db.query(&[("x", v)]).len()).sum();
        assert_test_v!("rollback 後の再操作", total == 15);
    }

    // ================================================================
    println!("\n=== 結果 ===");
    println!("{} passed, {} failed", passed, failed);
    if failed > 0 { std::process::exit(1); }
}
