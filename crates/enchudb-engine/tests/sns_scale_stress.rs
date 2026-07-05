//! β-light step 9: SNS scale stress test。
//!
//! 10 table × 5 himo × 10k entity の workload を table-aware で構築し、
//! 各種 op が正常動作することを確認する。 RSS や positions サイズの
//! 数値検証はやらない (実行環境 dependency 高い)、 ここでは構造的整合性のみ。
//!
//! 同 workload を anonymous-only で組むと positions が global eid 範囲
//! (= 100k) を持つ、 table-aware なら各 himo の positions は table 内
//! 10k に収まる。 数値計測は bench 側に任せる。

use enchudb_engine::{Engine, ValueType};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-sns-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suffix in ["", ".oplog", ".crc", ".db.lock", ".tables", ".tables.tmp"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

#[test]
fn sns_workload_10_tables_5_himos_each() {
    let path = tmp_path("sns_basic");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 200_000).unwrap();

    // 10 table × 5 himo
    for t in 0..10 {
        let tname = format!("t{}", t);
        eng.define_table(&tname, 10_000).unwrap();
        for h in 0..5 {
            let hname = format!("h{}", h);
            eng.define_himo_in(&tname, &hname, ValueType::Number, 100).unwrap();
        }
    }

    // 各 table に 1000 entity を確保 + 5 himo に tie
    for t in 0..10 {
        let tname = format!("t{}", t);
        let himo_full: Vec<String> = (0..5).map(|h| format!("t{}.h{}", t, h)).collect();
        for i in 0..1000 {
            let e = eng.entity_in(&tname).unwrap();
            for hn in &himo_full {
                eng.tie(e, hn, (i % 100) as u32);
            }
        }
    }
    eng.flush().unwrap();

    // 各 table の query が正しい件数返す
    for t in 0..10 {
        let himo_name = format!("t{}.h0", t);
        let rows = eng.pull_raw(&himo_name, 50);
        // value=50 で tie した entity 数: i % 100 == 50 → 10 件
        assert_eq!(rows.len(), 10, "table t{}.h0 value=50", t);
    }

    // table 間の隔離: t0.h0 に t3 の eid は混ざってない
    let rows = eng.pull_raw("t0.h0", 50);
    for &e in &rows {
        let local = enchudb_oplog::eid_local(e);
        assert!(local < 10_000, "t0 eid {} should be in [0, 10k)", local);
    }
    let rows = eng.pull_raw("t3.h0", 50);
    for &e in &rows {
        let local = enchudb_oplog::eid_local(e);
        assert!(
            local >= 30_000 && local < 40_000,
            "t3 eid {} should be in [30k, 40k)",
            local
        );
    }

    drop(eng);
    cleanup(&path);
}

#[test]
fn sns_workload_with_fk_refs() {
    // SNS 的: user / post / like の 3 table、 post.author = Ref(user)、
    // like.from = Ref(user)、 like.to = Ref(post)
    let path = tmp_path("sns_fk");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 100_000).unwrap();
    eng.define_table("user", 1_000).unwrap();
    eng.define_table("post", 5_000).unwrap();
    eng.define_table("like", 20_000).unwrap();

    eng.define_himo_in("user", "name", ValueType::Number, 1000).unwrap();
    eng.define_himo_in("post", "body", ValueType::Number, 1000).unwrap();
    eng.define_ref_in("post", "author", "user").unwrap();
    eng.define_ref_in("like", "from", "user").unwrap();
    eng.define_ref_in("like", "to", "post").unwrap();

    // 100 user 作る
    let users: Vec<_> = (0..100).map(|i| {
        let u = eng.entity_in("user").unwrap();
        eng.tie(u, "user.name", i);
        u
    }).collect();

    // 500 post (各 user が ~5 件投稿)
    let posts: Vec<_> = (0..500).map(|i| {
        let p = eng.entity_in("post").unwrap();
        let author = users[i as usize % users.len()];
        eng.tie(p, "post.body", i);
        eng.tie(p, "post.author", enchudb_oplog::eid_local(author));
        p
    }).collect();

    // 1000 like
    for i in 0..1000 {
        let l = eng.entity_in("like").unwrap();
        let from = users[i % users.len()];
        let to = posts[i % posts.len()];
        eng.tie(l, "like.from", enchudb_oplog::eid_local(from));
        eng.tie(l, "like.to", enchudb_oplog::eid_local(to));
    }

    eng.flush().unwrap();

    // FK 経由の query: alice の post 一覧
    let alice = users[0];
    let alice_local = enchudb_oplog::eid_local(alice);
    let alice_posts = eng.pull_raw("post.author", alice_local);
    assert_eq!(alice_posts.len(), 5, "alice should have 5 posts");

    // FK violation: post の eid (= user range 外) を author に渡す → panic
    let some_post = posts[0];
    let some_post_local = enchudb_oplog::eid_local(some_post);
    let new_post = eng.entity_in("post").unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        eng.tie(new_post, "post.author", some_post_local);
    }));
    assert!(result.is_err(), "FK violation should panic");

    drop(eng);
    cleanup(&path);
}

#[test]
fn sns_reopen_preserves_full_schema() {
    let path = tmp_path("sns_reopen");
    cleanup(&path);

    // 1. 構築
    {
        let mut eng = Engine::create_with_capacity(&path, 100_000).unwrap();
        for t in 0..5 {
            let tname = format!("table{}", t);
            eng.define_table(&tname, 10_000).unwrap();
            for h in 0..3 {
                let hname = format!("col{}", h);
                eng.define_himo_in(&tname, &hname, ValueType::Number, 100).unwrap();
            }
        }
        // Ref を 1 個追加
        eng.define_ref_in("table0", "ref_to_t1", "table1").unwrap();

        // 各 table に 100 entity 入れて tie
        for t in 0..5 {
            let tname = format!("table{}", t);
            for i in 0..100 {
                let e = eng.entity_in(&tname).unwrap();
                let hname = format!("{}.col0", tname);
                eng.tie(e, &hname, (i % 100) as u32);
            }
        }
        eng.flush().unwrap();
    }

    // 2. reopen して schema + data 復元確認
    {
        let eng = Engine::open_standalone(&path).unwrap();
        let tables = eng.list_tables();
        assert_eq!(tables.len(), 6, "anonymous + 5 = 6 tables");
        for t in 0..5 {
            let tname = format!("table{}", t);
            let himo = format!("{}.col0", tname);
            let rows = eng.pull_raw(&himo, 50);
            assert_eq!(rows.len(), 1, "{} value=50 should have 1 entity", himo);
        }
    }

    cleanup(&path);
}
