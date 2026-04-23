//! EnchuDB v27 実運用シナリオ統合テスト。
//!
//! 実行: `cargo test --features v27 --test scenarios`
//!
//! - パブリック API のみ (`use enchudb::*`)
//! - v27 前提(`#[cfg(feature = "v27")]` は付けず、cargo の feature 指定で実行する)
//! - std 以外の依存なし

use enchudb::*;
use std::collections::HashSet;

// テストごとに独立した DB ファイルパス
fn db_path(tag: &str) -> String {
    let path = format!("/tmp/enchudb_scen_{}.db", tag);
    let _ = std::fs::remove_file(&path);
    path
}

// ─────────────────────────────────────────────────────────
// 1. tenant_isolation_query
// ─────────────────────────────────────────────────────────
#[test]
fn tenant_isolation_query() {
    let path = db_path("tenant_iso");
    let mut db = Engine::create_with_capacity(&path, 2_000).unwrap();
    db.define_himo("tenant", HimoType::Value, 10);
    db.define_himo("user_no", HimoType::Value, 1_000);

    // 5 tenant × 200 user = 1000 entity
    let mut by_tenant: Vec<Vec<u64>> = vec![Vec::new(); 5];
    for t in 0..5u32 {
        for u in 0..200u32 {
            let e = db.entity();
            db.tie(e, "tenant", t);
            db.tie(e, "user_no", u);
            by_tenant[t as usize].push(e);
        }
    }
    db.rebuild();

    for t in 0..5u32 {
        let result = db.query(&[("tenant", t)]);
        assert_eq!(result.len(), 200, "tenant {} should have 200 users", t);
        let result_set: HashSet<u64> = result.into_iter().collect();
        let expected: HashSet<u64> = by_tenant[t as usize].iter().copied().collect();
        assert_eq!(result_set, expected, "tenant {} entity set mismatch", t);

        // 他テナントの user_no で絞り込んでも自分のテナント内のみ
        let r2 = db.query(&[("tenant", t), ("user_no", 7)]);
        assert_eq!(r2.len(), 1, "tenant {} user_no 7 should be exactly 1", t);
        assert!(by_tenant[t as usize].contains(&r2[0]));
    }
}

// ─────────────────────────────────────────────────────────
// 2. tenant_with_view
// ─────────────────────────────────────────────────────────
#[test]
fn tenant_with_view() {
    let path = db_path("tenant_view");
    let mut db = Engine::create_with_capacity(&path, 2_000).unwrap();
    db.define_himo("tenant", HimoType::Value, 8);
    db.define_himo("status", HimoType::Value, 4);
    db.define_himo("noise", HimoType::Value, 50);

    db.define_view(&["tenant", "status"]).unwrap();

    let n = 800u32;
    for i in 0..n {
        let e = db.entity();
        db.tie(e, "tenant", i % 8);
        db.tie(e, "status", (i / 8) % 4);
        db.tie(e, "noise", i % 50);
    }
    db.rebuild();

    // query(tenant + status) は view 経由
    let r = db.query(&[("tenant", 3), ("status", 2)]);
    let r_set: HashSet<u64> = r.iter().copied().collect();
    let expected: HashSet<u64> = (0..n)
        .filter(|&i| i % 8 == 3 && (i / 8) % 4 == 2)
        .map(|i| i as u64)
        .collect();
    assert_eq!(r_set, expected, "tenant=3 status=2 should match brute force");
    assert!(!r.is_empty());
}

// ─────────────────────────────────────────────────────────
// 3. region_hierarchy_navigation
// ─────────────────────────────────────────────────────────
#[test]
fn region_hierarchy_navigation() {
    let path = db_path("region_hier");
    let mut db = Engine::create_standalone(&path).unwrap();
    db.define_himo("name", HimoType::Symbol, 0);
    db.define_himo("parent", HimoType::Ref, 0);

    // 国
    let japan = db.entity();
    db.tie_text(japan, "name", "Japan");

    // 地方
    let kanto = db.entity();
    db.tie_text(kanto, "name", "Kanto");
    db.tie_ref(kanto, "parent", japan);

    // 都道府県
    let tokyo = db.entity();
    db.tie_text(tokyo, "name", "Tokyo");
    db.tie_ref(tokyo, "parent", kanto);

    // 市区町村
    let shibuya = db.entity();
    db.tie_text(shibuya, "name", "Shibuya");
    db.tie_ref(shibuya, "parent", tokyo);

    // 4段階上に辿る
    let p1 = db.get(shibuya, "parent").unwrap();
    let p2 = db.get(p1 as u64, "parent").unwrap();
    let p3 = db.get(p2 as u64, "parent").unwrap();
    assert_eq!(p1, tokyo as u32);
    assert_eq!(p2, kanto as u32);
    assert_eq!(p3, japan as u32);

    assert_eq!(db.get_text(p3 as u64, "name").unwrap(), b"Japan");
    assert_eq!(db.get_text(p2 as u64, "name").unwrap(), b"Kanto");
    assert_eq!(db.get_text(p1 as u64, "name").unwrap(), b"Tokyo");
}

// ─────────────────────────────────────────────────────────
// 4. reverse_lookup_children
// ─────────────────────────────────────────────────────────
#[test]
fn reverse_lookup_children() {
    let path = db_path("reverse_children");
    let mut db = Engine::create_standalone(&path).unwrap();
    db.define_himo("parent", HimoType::Ref, 0);

    let kanto = db.entity();
    let kansai = db.entity();

    let kanto_pref: Vec<u64> = (0..7)
        .map(|_| {
            let e = db.entity();
            db.tie_ref(e, "parent", kanto);
            e
        })
        .collect();

    let _kansai_pref: Vec<u64> = (0..6)
        .map(|_| {
            let e = db.entity();
            db.tie_ref(e, "parent", kansai);
            e
        })
        .collect();

    db.rebuild();

    let r = db.pull_raw("parent", kanto as u32);
    let r_set: HashSet<u64> = r.iter().copied().collect();
    let expected: HashSet<u64> = kanto_pref.iter().copied().collect();
    assert_eq!(r_set, expected, "kanto children mismatch");

    let r = db.pull_raw("parent", kansai as u32);
    assert_eq!(r.len(), 6);

    // 削除すると逆引きから消える
    db.untie(kanto_pref[0], "parent");
    let r = db.pull_raw("parent", kanto as u32);
    assert_eq!(r.len(), 6);
    assert!(!r.contains(&kanto_pref[0]));

    // 追加で増える
    let new_pref = db.entity();
    db.tie_ref(new_pref, "parent", kanto);
    let r = db.pull_raw("parent", kanto as u32);
    assert_eq!(r.len(), 7);
    assert!(r.contains(&new_pref));
}

// ─────────────────────────────────────────────────────────
// 5. move_entity_between_parents
// ─────────────────────────────────────────────────────────
#[test]
fn move_entity_between_parents() {
    let path = db_path("move_parent");
    let mut db = Engine::create_standalone(&path).unwrap();
    db.define_himo("dept", HimoType::Ref, 0);

    let dept_old = db.entity();
    let dept_new = db.entity();

    let user = db.entity();
    db.tie_ref(user, "dept", dept_old);
    db.rebuild();

    assert!(db.pull_raw("dept", dept_old as u32).contains(&user));
    assert!(!db.pull_raw("dept", dept_new as u32).contains(&user));

    // 移動
    db.tie_ref(user, "dept", dept_new);

    let in_old = db.pull_raw("dept", dept_old as u32);
    let in_new = db.pull_raw("dept", dept_new as u32);
    assert!(!in_old.contains(&user), "user should be removed from old dept");
    assert!(in_new.contains(&user), "user should appear in new dept");
}

// ─────────────────────────────────────────────────────────
// 6. delete_parent_dangling_refs
// ─────────────────────────────────────────────────────────
#[test]
fn delete_parent_dangling_refs() {
    let path = db_path("dangling");
    let mut db = Engine::create_standalone(&path).unwrap();
    db.define_himo("parent", HimoType::Ref, 0);

    let parent = db.entity();
    let child_a = db.entity();
    let child_b = db.entity();
    db.tie_ref(child_a, "parent", parent);
    db.tie_ref(child_b, "parent", parent);
    db.rebuild();

    db.delete(parent);

    // Enchu は参照整合性を強制しない: 子の参照は残ったまま
    assert_eq!(db.get(child_a, "parent"), Some(parent as u32));
    assert_eq!(db.get(child_b, "parent"), Some(parent as u32));

    // 逆引きでも残る(削除された ID 値で引ける)
    let r = db.pull_raw("parent", parent as u32);
    let r_set: HashSet<u64> = r.iter().copied().collect();
    assert!(r_set.contains(&child_a));
    assert!(r_set.contains(&child_b));
}

// ─────────────────────────────────────────────────────────
// 7. bulk_tie_consistency
// ─────────────────────────────────────────────────────────
#[test]
fn bulk_tie_consistency() {
    let path = db_path("bulk");
    let mut db = Engine::create_with_capacity(&path, 110_000).unwrap();
    db.define_himo("a", HimoType::Value, 100);
    db.define_himo("b", HimoType::Value, 50);
    db.define_himo("c", HimoType::Value, 10);

    let n = 100_000u32;
    for i in 0..n {
        let e = db.entity();
        db.tie(e, "a", i % 100);
        db.tie(e, "b", (i / 100) % 50);
        db.tie(e, "c", (i / 5_000) % 10);
    }
    db.rebuild();

    // 各 a 値ちょうど n/100 件
    for v in 0..100u32 {
        let r = db.query(&[("a", v)]);
        assert_eq!(r.len(), (n / 100) as usize, "a={} count", v);
    }
    // 全件合計
    let mut total = 0usize;
    for v in 0..100u32 {
        total += db.query(&[("a", v)]).len();
    }
    assert_eq!(total, n as usize);

    // 2 条件
    let r = db.query(&[("a", 7), ("b", 3)]);
    let expected: usize = (0..n)
        .filter(|&i| i % 100 == 7 && (i / 100) % 50 == 3)
        .count();
    assert_eq!(r.len(), expected);
}

// ─────────────────────────────────────────────────────────
// 8. repeated_tie_untie
// ─────────────────────────────────────────────────────────
#[test]
fn repeated_tie_untie() {
    let path = db_path("repeat");
    let mut db = Engine::create_standalone(&path).unwrap();
    db.define_himo("flag", HimoType::Value, 8);

    let e = db.entity();
    for i in 0..1_000u32 {
        db.tie(e, "flag", i % 8);
        db.untie(e, "flag");
    }
    // 最終: untie で終わってる
    assert_eq!(db.get(e, "flag"), None);
    db.rebuild();
    assert!(db.pull_raw("flag", 0).is_empty());
    assert!(db.pull_raw("flag", 7).is_empty());

    // tie で終わらせる
    db.tie(e, "flag", 5);
    assert_eq!(db.get(e, "flag"), Some(5));
    let r = db.pull_raw("flag", 5);
    assert!(r.contains(&e));

    // 別の値に上書き
    for v in 0..8u32 {
        db.tie(e, "flag", v);
        assert_eq!(db.get(e, "flag"), Some(v));
    }
    let final_v = db.get(e, "flag").unwrap();
    let r = db.pull_raw("flag", final_v);
    assert!(r.contains(&e));
    // それ以外の値からは消えてる
    for v in 0..8u32 {
        if v == final_v { continue; }
        let r = db.pull_raw("flag", v);
        assert!(!r.contains(&e), "stale eid in flag={}", v);
    }
}

// ─────────────────────────────────────────────────────────
// 9. delete_reuse_id
// ─────────────────────────────────────────────────────────
#[test]
fn delete_reuse_id() {
    // 容量を小さくして free stack 経由の再利用を強制する
    let path = db_path("reuse_id");
    let mut db = Engine::create_with_capacity(&path, 4).unwrap();
    db.define_himo("kind", HimoType::Value, 4);
    db.define_himo("val", HimoType::Value, 100);

    let mut eids = Vec::new();
    for i in 0..4u32 {
        let e = db.entity();
        eids.push(e);
        db.tie(e, "kind", i % 4);
        db.tie(e, "val", i + 10);
    }

    // 1 つ削除して再確保 → 容量上限到達なので free stack から同じ ID を返す
    let target = eids[2];
    db.delete(target);
    assert_eq!(db.get(target, "kind"), None);
    assert_eq!(db.get(target, "val"), None);

    let reused = db.entity();
    assert_eq!(reused, target, "free stack should return the freed ID");

    // 再利用 ID には古い紐が残っていない
    assert_eq!(db.get(reused, "kind"), None, "stale kind on reused id");
    assert_eq!(db.get(reused, "val"), None, "stale val on reused id");

    // 新しい紐を張ってクエリで戻ってくる
    db.tie(reused, "kind", 1);
    db.tie(reused, "val", 99);
    db.rebuild();
    let r = db.query(&[("kind", 1), ("val", 99)]);
    assert!(r.contains(&reused));

    // 元の値(古い ID 用) に古い entity が残っていない(古い紐は消えてる)
    let r_old = db.pull_raw("val", 12); // 元 eids[2] の val=12
    assert!(!r_old.contains(&reused), "old val 12 must not include reused id");
}

// ─────────────────────────────────────────────────────────
// 10. cascade_delete_effect
// ─────────────────────────────────────────────────────────
#[test]
fn cascade_delete_effect() {
    let path = db_path("cascade");
    let mut db = Engine::create_standalone(&path).unwrap();
    db.define_himo("owner", HimoType::Ref, 0);

    let owner = db.entity();
    let docs: Vec<u64> = (0..5).map(|_| {
        let e = db.entity();
        db.tie_ref(e, "owner", owner);
        e
    }).collect();

    db.rebuild();
    assert_eq!(db.pull_raw("owner", owner as u32).len(), 5);

    // child を 1 件削除すると逆引きから消える
    db.delete(docs[0]);
    let r = db.pull_raw("owner", owner as u32);
    assert!(!r.contains(&docs[0]), "deleted child must vanish from reverse lookup");
    assert_eq!(r.len(), 4);
}

// ─────────────────────────────────────────────────────────
// 11. views_after_open
// ─────────────────────────────────────────────────────────
#[test]
fn views_after_open() {
    let path = db_path("view_open");

    // 書き込みフェーズ
    let pre_query: HashSet<u64>;
    {
        let mut db = Engine::create_with_capacity(&path, 2_000).unwrap();
        db.define_himo("tenant", HimoType::Value, 8);
        db.define_himo("status", HimoType::Value, 4);
        db.define_himo("grade", HimoType::Value, 5);
        db.define_view(&["tenant", "status"]).unwrap();

        for i in 0..1_000u32 {
            let e = db.entity();
            db.tie(e, "tenant", i % 8);
            db.tie(e, "status", (i / 8) % 4);
            db.tie(e, "grade", (i / 32) % 5);
        }
        db.rebuild();

        let r = db.query(&[("tenant", 2), ("status", 1)]);
        pre_query = r.into_iter().collect();
        assert!(!pre_query.is_empty());

        db.flush().unwrap();
    } // drop

    // open フェーズ — define_view を呼ばない
    let db = Engine::open_standalone(&path).unwrap();
    let r = db.query(&[("tenant", 2), ("status", 1)]);
    let post_query: HashSet<u64> = r.into_iter().collect();
    assert_eq!(post_query, pre_query, "view query must match before/after reopen");
}

// ─────────────────────────────────────────────────────────
// 12. view_hit_vs_miss
// ─────────────────────────────────────────────────────────
#[test]
fn view_hit_vs_miss() {
    let path = db_path("view_hitmiss");
    let mut db = Engine::create_with_capacity(&path, 2_000).unwrap();
    db.define_himo("tenant", HimoType::Value, 8);
    db.define_himo("status", HimoType::Value, 4);
    db.define_himo("grade", HimoType::Value, 5);

    db.define_view(&["tenant", "status"]).unwrap();

    let n = 1_000u32;
    for i in 0..n {
        let e = db.entity();
        db.tie(e, "tenant", i % 8);
        db.tie(e, "status", (i / 8) % 4);
        db.tie(e, "grade", (i / 32) % 5);
    }
    db.rebuild();

    // ヒット: view と同一組合せ
    let r_hit = db.query(&[("tenant", 3), ("status", 2)]);
    let exp_hit: HashSet<u64> = (0..n)
        .filter(|&i| i % 8 == 3 && (i / 8) % 4 == 2)
        .map(|i| i as u64)
        .collect();
    assert_eq!(r_hit.iter().copied().collect::<HashSet<u64>>(), exp_hit);

    // ミス: view が無い組合せ → ペアテーブル or column フィルタ
    let r_miss = db.query(&[("tenant", 3), ("grade", 1)]);
    let exp_miss: HashSet<u64> = (0..n)
        .filter(|&i| i % 8 == 3 && (i / 32) % 5 == 1)
        .map(|i| i as u64)
        .collect();
    assert_eq!(r_miss.iter().copied().collect::<HashSet<u64>>(), exp_miss);

    // view の上に第 3 条件を足す
    let r_three = db.query(&[("tenant", 3), ("status", 2), ("grade", 1)]);
    let exp_three: HashSet<u64> = (0..n)
        .filter(|&i| i % 8 == 3 && (i / 8) % 4 == 2 && (i / 32) % 5 == 1)
        .map(|i| i as u64)
        .collect();
    assert_eq!(r_three.iter().copied().collect::<HashSet<u64>>(), exp_three);
}

// ─────────────────────────────────────────────────────────
// 13. mixed_workload
// ─────────────────────────────────────────────────────────
#[test]
fn mixed_workload() {
    let path = db_path("mixed");
    let mut db = Engine::create_with_capacity(&path, 20_000).unwrap();
    db.define_himo("group", HimoType::Value, 10);
    db.define_himo("score", HimoType::Value, 100);

    // 1万 tie
    let mut all_eids: Vec<u64> = Vec::with_capacity(10_000);
    for i in 0..10_000u32 {
        let e = db.entity();
        db.tie(e, "group", i % 10);
        db.tie(e, "score", i % 100);
        all_eids.push(e);
    }

    // 線形合同で擬似ランダム順
    fn lcg(s: u32) -> u32 { s.wrapping_mul(1_103_515_245).wrapping_add(12345) }
    let mut s = 42u32;

    // untie 1000 件: score 紐を外す
    let mut untied = HashSet::new();
    for _ in 0..1_000 {
        s = lcg(s);
        let idx = (s as usize) % all_eids.len();
        if untied.insert(all_eids[idx]) {
            db.untie(all_eids[idx], "score");
        }
    }

    // delete 500 件: untie 済みも含め可。重複は no-op
    let mut deleted = HashSet::new();
    for _ in 0..500 {
        s = lcg(s);
        let idx = (s as usize) % all_eids.len();
        if deleted.insert(all_eids[idx]) {
            db.delete(all_eids[idx]);
        }
    }

    db.rebuild();

    // 期待値
    let alive_count = 10_000 - deleted.len() as u32;
    assert_eq!(db.entity_count(), alive_count);

    // group 紐: 削除されてないものだけ残る
    let mut group_total = 0usize;
    for g in 0..10u32 {
        group_total += db.query(&[("group", g)]).len();
    }
    assert_eq!(group_total, alive_count as usize);

    // score 紐: untie もしくは delete されたものは消える
    let removed_score: HashSet<u64> = untied.union(&deleted).copied().collect();
    let alive_with_score = 10_000 - removed_score.len();
    let mut score_total = 0usize;
    for v in 0..100u32 {
        score_total += db.pull_raw("score", v).len();
    }
    assert_eq!(score_total, alive_with_score);
}

// ─────────────────────────────────────────────────────────
// 14. many_himos
// ─────────────────────────────────────────────────────────
#[test]
fn many_himos() {
    let path = db_path("many_himos");
    let mut db = Engine::create_with_capacity(&path, 200).unwrap();

    // 50 紐定義
    let names: Vec<String> = (0..50).map(|i| format!("h{:02}", i)).collect();
    for name in &names {
        db.define_himo(name, HimoType::Value, 8);
    }

    // 100 entity に全 50 紐を張る
    let n = 100u32;
    for i in 0..n {
        let e = db.entity();
        for (k, name) in names.iter().enumerate() {
            db.tie(e, name, (i + k as u32) % 8);
        }
    }
    db.rebuild();

    // 各紐の 1 値クエリは n/8 件前後(8で割れない端数あり)
    for name in &names {
        let mut total = 0usize;
        for v in 0..8u32 {
            total += db.pull_raw(name, v).len();
        }
        assert_eq!(total, n as usize, "himo {} total", name);
    }

    // 任意 3 紐の AND が動く
    let r = db.query(&[("h00", 0), ("h01", 1), ("h02", 2)]);
    // 全て (i + k) % 8 = 0/1/2 → i % 8 = 0 を満たす entity
    let exp: HashSet<u64> = (0..n).filter(|&i| i % 8 == 0).map(|i| i as u64).collect();
    assert_eq!(r.iter().copied().collect::<HashSet<u64>>(), exp);
}

// ─────────────────────────────────────────────────────────
// 15. text_and_value_mixed
// ─────────────────────────────────────────────────────────
#[test]
fn text_and_value_mixed() {
    let path = db_path("text_value");
    let mut db = Engine::create_standalone(&path).unwrap();
    db.define_himo("city", HimoType::Symbol, 0);
    db.define_himo("age", HimoType::Value, 100);

    let cities = ["Tokyo", "Osaka", "Kyoto", "Sapporo"];
    let mut by_city: Vec<Vec<u64>> = vec![Vec::new(); cities.len()];
    let n = 200u32;
    for i in 0..n {
        let e = db.entity();
        let ci = (i as usize) % cities.len();
        db.tie_text(e, "city", cities[ci]);
        db.tie(e, "age", i % 100);
        by_city[ci].push(e);
    }
    db.rebuild();

    // find_value で文脈付き ID 引き
    for (ci, &name) in cities.iter().enumerate() {
        let vid = db.find_value("city", name).expect("find_value should hit");
        let r = db.pull_raw("city", vid);
        let r_set: HashSet<u64> = r.into_iter().collect();
        let exp: HashSet<u64> = by_city[ci].iter().copied().collect();
        assert_eq!(r_set, exp, "city {} pull_raw mismatch", name);
    }

    // 値紐は普通に動く
    let r = db.pull_raw("age", 7);
    let exp: HashSet<u64> = (0..n).filter(|&i| i % 100 == 7).map(|i| i as u64).collect();
    assert_eq!(r.iter().copied().collect::<HashSet<u64>>(), exp);

    // get_text と get
    let some_e = by_city[1][0]; // Osaka
    assert_eq!(db.get_text(some_e, "city").unwrap(), b"Osaka");
    let age_val = db.get(some_e, "age").unwrap();
    assert!(age_val < 100);

    // Symbol 紐に対する get_text と Value 紐に対する get の区別
    assert!(db.get_text(some_e, "age").is_none(), "get_text on Value himo returns None");
}

// ─────────────────────────────────────────────────────────
// 15b. pair table 空セルで query が silent 0件を返すバグの再現
// ─────────────────────────────────────────────────────────
#[test]
fn query_pair_empty_cell_fallback() {
    let path = db_path("pair_empty_cell");
    let mut db = Engine::create_standalone(&path).unwrap();
    db.define_himo("type_h", HimoType::Value, 4);
    db.define_himo("board", HimoType::Value, 64);
    db.define_himo("author", HimoType::Value, 1000);

    // pair table を構築（この時点でデータなし → 全セル空）
    db.rebuild();
    db.rebuild_pairs();

    // データ投入（pair table 構築後）
    for i in 0..100u32 {
        let e = db.entity();
        db.tie(e, "type_h", 1);
        db.tie(e, "board", 0);
        db.tie(e, "author", i);
    }
    for i in 0..500u32 {
        let e = db.entity();
        db.tie(e, "type_h", 2);
        db.tie(e, "board", 0);
        db.tie(e, "author", i % 100);
    }
    db.rebuild();

    // pull_raw は正常
    assert_eq!(db.pull_raw("type_h", 1).len(), 100);

    // query が 0 件を返してはいけない
    let result = db.query(&[("type_h", 1), ("board", 0)]);
    assert_eq!(result.len(), 100, "query must not return empty when data exists");

    let result = db.query(&[("type_h", 1), ("board", 0), ("author", 5)]);
    assert_eq!(result.len(), 1);

    let _ = std::fs::remove_file(&path);
}

// ─────────────────────────────────────────────────────────
// 16. sum — 指定 entity 群の紐値を合計
// ─────────────────────────────────────────────────────────
#[test]
fn sum_basic() {
    let path = db_path("sum_basic");
    let mut db = Engine::create_standalone(&path).unwrap();
    db.define_himo("cost", HimoType::Value, 0);

    let mut eids = Vec::new();
    for v in [100, 200, 300, 400] {
        let e = db.entity();
        db.tie(e, "cost", v);
        eids.push(e);
    }

    assert_eq!(db.sum("cost", &eids), 1000);
    assert_eq!(db.sum("cost", &eids[0..2]), 300);
    assert_eq!(db.sum("cost", &[]), 0);
    assert_eq!(db.sum("nonexistent", &eids), 0);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn sum_skips_missing_values() {
    let path = db_path("sum_skip");
    let mut db = Engine::create_standalone(&path).unwrap();
    db.define_himo("price", HimoType::Value, 0);
    db.define_himo("name", HimoType::Symbol, 0);

    let e1 = db.entity(); db.tie(e1, "price", 50);
    let e2 = db.entity(); // price なし
    let e3 = db.entity(); db.tie(e3, "price", 30);

    assert_eq!(db.sum("price", &[e1, e2, e3]), 80);

    let _ = std::fs::remove_file(&path);
}

// ─────────────────────────────────────────────────────────
// 17. group_sum — GROUP BY + SUM
// ─────────────────────────────────────────────────────────
#[test]
fn group_sum_basic() {
    let path = db_path("gsum_basic");
    let mut db = Engine::create_standalone(&path).unwrap();
    db.define_himo("project", HimoType::Value, 10);
    db.define_himo("cost", HimoType::Value, 0);

    // project 0: cost 100, 200
    // project 1: cost 300, 400, 500
    let mut eids = Vec::new();
    for (proj, cost) in [(0, 100), (0, 200), (1, 300), (1, 400), (1, 500)] {
        let e = db.entity();
        db.tie(e, "project", proj);
        db.tie(e, "cost", cost);
        eids.push(e);
    }

    let result = db.group_sum("project", "cost", &eids);
    assert_eq!(result.len(), 2);

    let p0 = result.iter().find(|(k, _)| *k == 0).unwrap().1;
    let p1 = result.iter().find(|(k, _)| *k == 1).unwrap().1;
    assert_eq!(p0, 300);  // 100 + 200
    assert_eq!(p1, 1200); // 300 + 400 + 500

    let _ = std::fs::remove_file(&path);
}

#[test]
fn group_sum_with_query() {
    let path = db_path("gsum_query");
    let mut db = Engine::create_standalone(&path).unwrap();
    db.define_himo("status", HimoType::Value, 5);
    db.define_himo("project", HimoType::Value, 10);
    db.define_himo("material_cost", HimoType::Value, 0);

    // status=1 (施工中): project 0 に 1000, 2000 / project 1 に 3000
    // status=2 (完了): project 0 に 9999
    for (status, proj, cost) in [(1, 0, 1000), (1, 0, 2000), (1, 1, 3000), (2, 0, 9999)] {
        let e = db.entity();
        db.tie(e, "status", status);
        db.tie(e, "project", proj);
        db.tie(e, "material_cost", cost);
    }
    db.rebuild();

    // 施工中だけ取得
    let active = db.pull_raw("status", 1);
    assert_eq!(active.len(), 3);

    // 工事別材料費
    let result = db.group_sum("project", "material_cost", &active);
    let p0 = result.iter().find(|(k, _)| *k == 0).unwrap().1;
    let p1 = result.iter().find(|(k, _)| *k == 1).unwrap().1;
    assert_eq!(p0, 3000);  // 1000 + 2000
    assert_eq!(p1, 3000);

    // 合計
    assert_eq!(db.sum("material_cost", &active), 6000);

    let _ = std::fs::remove_file(&path);
}

// ─────────────────────────────────────────────────────────
// 18. pull_range — 範囲クエリ
// ─────────────────────────────────────────────────────────
#[test]
fn pull_range_basic() {
    let path = db_path("range_basic");
    let mut db = Engine::create_standalone(&path).unwrap();
    db.define_himo("age", HimoType::Value, 100);

    for age in 20..=40 {
        let e = db.entity();
        db.tie(e, "age", age);
    }
    db.rebuild();

    let result = db.pull_range("age", 25, 30);
    assert_eq!(result.len(), 6); // 25,26,27,28,29,30

    let all = db.pull_range("age", 20, 40);
    assert_eq!(all.len(), 21);

    let empty = db.pull_range("age", 50, 60);
    assert_eq!(empty.len(), 0);

    let _ = std::fs::remove_file(&path);
}

// ─────────────────────────────────────────────────────────
// 19. 日付ヘルパー
// ─────────────────────────────────────────────────────────
#[test]
fn date_conversion_roundtrip() {
    // 基準日
    let d = Engine::date_to_days(2000, 1, 1);
    assert_eq!(d, 0);
    assert_eq!(Engine::days_to_date(0), (2000, 1, 1));

    // 今日
    let d = Engine::date_to_days(2026, 4, 20);
    assert_eq!(Engine::days_to_date(d), (2026, 4, 20));

    // うるう年
    let d = Engine::date_to_days(2024, 2, 29);
    assert_eq!(Engine::days_to_date(d), (2024, 2, 29));

    // 年末
    let d = Engine::date_to_days(2025, 12, 31);
    assert_eq!(Engine::days_to_date(d), (2025, 12, 31));
}

#[test]
fn tie_date_and_range() {
    let path = db_path("date_range");
    let mut db = Engine::create_standalone(&path).unwrap();
    db.define_himo("created", HimoType::Value, 0);

    // 2026年4月の10日分
    for day in 1..=10 {
        let e = db.entity();
        db.tie_date(e, "created", 2026, 4, day);
    }
    db.rebuild();

    // 日付で取得
    let e0 = db.pull_date_range("created", (2026, 4, 1), (2026, 4, 1));
    assert_eq!(e0.len(), 1);

    // 4/3〜4/7
    let week = db.pull_date_range("created", (2026, 4, 3), (2026, 4, 7));
    assert_eq!(week.len(), 5);

    // 全部
    let all = db.pull_date_range("created", (2026, 4, 1), (2026, 4, 10));
    assert_eq!(all.len(), 10);

    // get_date で読み返し
    let eid = e0[0];
    assert_eq!(db.get_date(eid, "created"), Some((2026, 4, 1)));

    // 範囲外
    let none = db.pull_date_range("created", (2026, 5, 1), (2026, 5, 31));
    assert_eq!(none.len(), 0);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn date_with_sum() {
    let path = db_path("date_sum");
    let mut db = Engine::create_standalone(&path).unwrap();
    db.define_himo("date", HimoType::Value, 0);
    db.define_himo("sales", HimoType::Value, 0);

    // 4/1: 1000, 4/2: 2000, 4/3: 3000
    for (day, amount) in [(1, 1000), (2, 2000), (3, 3000)] {
        let e = db.entity();
        db.tie_date(e, "date", 2026, 4, day);
        db.tie(e, "sales", amount);
    }
    db.rebuild();

    let april = db.pull_date_range("date", (2026, 4, 1), (2026, 4, 3));
    assert_eq!(db.sum("sales", &april), 6000);

    let first_two = db.pull_date_range("date", (2026, 4, 1), (2026, 4, 2));
    assert_eq!(db.sum("sales", &first_two), 3000);

    let _ = std::fs::remove_file(&path);
}

// ─────────────────────────────────────────────────────────
// 20. min / max / avg / count
// ─────────────────────────────────────────────────────────
#[test]
fn aggregates() {
    let path = db_path("aggregates");
    let mut db = Engine::create_standalone(&path).unwrap();
    db.define_himo("score", HimoType::Value, 0);

    let mut eids = Vec::new();
    for v in [10, 20, 30, 40, 50] {
        let e = db.entity();
        db.tie(e, "score", v);
        eids.push(e);
    }

    assert_eq!(db.min("score", &eids), Some(10));
    assert_eq!(db.max("score", &eids), Some(50));
    assert_eq!(db.avg("score", &eids), Some(30)); // 150 / 5
    assert_eq!(db.sum("score", &eids), 150);
    assert_eq!(db.count("score", &eids), 5);

    // 部分
    assert_eq!(db.min("score", &eids[0..2]), Some(10));
    assert_eq!(db.max("score", &eids[3..5]), Some(50));
    assert_eq!(db.avg("score", &eids[0..2]), Some(15)); // (10+20)/2

    // 空
    assert_eq!(db.min("score", &[]), None);
    assert_eq!(db.max("score", &[]), None);
    assert_eq!(db.avg("score", &[]), None);
    assert_eq!(db.count("score", &[]), 0);

    // 存在しない紐
    assert_eq!(db.min("nope", &eids), None);
    assert_eq!(db.max("nope", &eids), None);
    assert_eq!(db.avg("nope", &eids), None);
    assert_eq!(db.count("nope", &eids), 0);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn aggregates_with_missing_values() {
    let path = db_path("agg_missing");
    let mut db = Engine::create_standalone(&path).unwrap();
    db.define_himo("price", HimoType::Value, 0);

    let e1 = db.entity(); db.tie(e1, "price", 100);
    let e2 = db.entity(); // price なし
    let e3 = db.entity(); db.tie(e3, "price", 300);

    assert_eq!(db.min("price", &[e1, e2, e3]), Some(100));
    assert_eq!(db.max("price", &[e1, e2, e3]), Some(300));
    assert_eq!(db.avg("price", &[e1, e2, e3]), Some(200)); // (100+300)/2, e2 スキップ
    assert_eq!(db.count("price", &[e1, e2, e3]), 2);

    let _ = std::fs::remove_file(&path);
}
