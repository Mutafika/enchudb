//! v31 実証テスト: Agentic Engram 相当の長期記憶ユースケースを
//! **Lance + Kuzu 不使用で、EnchuDB + Ravn だけ**で実装。
//!
//! 参考(元記事): AI エージェント用の記憶システム。
//!   - Lance: 意味類似検索(ベクトル)
//!   - Kuzu:  エンティティ間の関係グラフ(Cypher)
//!
//! このテストでは**ベクトル検索を使わず、紐グラフだけで記憶想起**する構成を実証。
//! セマンティック検索の部分は「LLM による抽出時のタグ付け」に置き換える。
//!
//! # モデル
//!
//! - Session: 発話セッション
//!   - speaker: Ref → Person
//!   - topic: Ref → Topic
//!   - related_file: Ref → File
//!   - decision: Symbol (text)
//!   - date: Value (epoch days)
//!
//! - Person, Topic, File: それぞれ name (Symbol)
//!
//! # 典型クエリ
//!
//! > 「Alice が参加した Next.js トピックのセッションの決定事項」
//!
//! = topic="Next.js" の Topic を起点 → そのセッション群を逆引き →
//!   speaker が Alice で絞り込み → decision を抽出

#![cfg(feature = "v27")]

use enchudb::{Engine, ValueType, Ravn};
use std::sync::Arc;

fn tmp(name: &str) -> String {
    let p = format!("/tmp/enchudb-v31-engram-{}-{}", name, std::process::id());
    let _ = std::fs::remove_file(&p);
    let _ = std::fs::remove_file(format!("{}.oplog", p));
    let _ = std::fs::remove_file(format!("{}.crc", p));
    p
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));
}

/// シナリオ用のデータを仕込む。
/// 戻り値は (engine, person_ids: HashMap<&str, u32>, topic_ids: HashMap<&str, u32>)
fn seed(path: &str) -> Arc<Engine> {
    let mut eng = Engine::create_standalone(path).unwrap();

    // スキーマ
    eng.define_himo("kind", ValueType::Number, 10);     // 1=Person, 2=Topic, 3=File, 4=Session
    eng.define_himo("name", ValueType::Tag, 0);
    eng.define_himo("speaker", ValueType::Ref, 0);
    eng.define_himo("topic", ValueType::Ref, 0);
    eng.define_himo("related_file", ValueType::Ref, 0);
    eng.define_himo("decision", ValueType::Tag, 0);
    eng.define_himo("date", ValueType::Number, 0);

    // 人物
    let alice = eng.entity();
    eng.tie(alice, "kind", 1);
    eng.tie_text(alice, "name", "Alice");

    let bob = eng.entity();
    eng.tie(bob, "kind", 1);
    eng.tie_text(bob, "name", "Bob");

    // トピック
    let nextjs = eng.entity();
    eng.tie(nextjs, "kind", 2);
    eng.tie_text(nextjs, "name", "Next.js");

    let rust_topic = eng.entity();
    eng.tie(rust_topic, "kind", 2);
    eng.tie_text(rust_topic, "name", "Rust");

    // ファイル
    let route_file = eng.entity();
    eng.tie(route_file, "kind", 3);
    eng.tie_text(route_file, "name", "app/api/auth/route.ts");

    // セッション: Alice × Next.js × route_file × "middleware で早期 return"
    let s1 = eng.entity();
    eng.tie(s1, "kind", 4);
    eng.tie_ref(s1, "speaker", alice);
    eng.tie_ref(s1, "topic", nextjs);
    eng.tie_ref(s1, "related_file", route_file);
    eng.tie_text(s1, "decision", "middleware で早期 return に統一");
    eng.tie(s1, "date", 9100);

    // セッション: Alice × Next.js × "SSR を維持"
    let s2 = eng.entity();
    eng.tie(s2, "kind", 4);
    eng.tie_ref(s2, "speaker", alice);
    eng.tie_ref(s2, "topic", nextjs);
    eng.tie_text(s2, "decision", "SSR を維持する");
    eng.tie(s2, "date", 9120);

    // セッション: Bob × Next.js(Alice じゃない → 絞られる対象)
    let s3 = eng.entity();
    eng.tie(s3, "kind", 4);
    eng.tie_ref(s3, "speaker", bob);
    eng.tie_ref(s3, "topic", nextjs);
    eng.tie_text(s3, "decision", "App Router に移行");
    eng.tie(s3, "date", 9110);

    // セッション: Alice × Rust(Next.js じゃない → 絞られる対象)
    let s4 = eng.entity();
    eng.tie(s4, "kind", 4);
    eng.tie_ref(s4, "speaker", alice);
    eng.tie_ref(s4, "topic", rust_topic);
    eng.tie_text(s4, "decision", "Arc<RwLock> を使う");
    eng.tie(s4, "date", 9130);

    eng.rebuild();
    Arc::new(eng)
}

#[test]
fn recall_decisions_by_person_and_topic() {
    let path = tmp("recall");
    let eng = seed(&path);
    let ravn = Ravn::new(Arc::clone(&eng));

    // 「Alice が Next.js で話したセッションの決定事項」
    //
    // 1. topic="Next.js" の Topic entity を取る
    // 2. そこに tie_ref "topic" している Session 群を逆引き
    // 3. speaker="Alice" で filter
    // 4. decision を extract

    let nextjs_id = eng.vocab_id("Next.js").unwrap();
    let topic_entities = eng.query(&[("name", nextjs_id), ("kind", 2)]);
    assert_eq!(topic_entities.len(), 1, "exactly 1 Next.js topic");

    // 逆引きでセッション集合
    let sessions = ravn.reverse_follow(&topic_entities, "topic");
    assert!(sessions.len() >= 3, "should find multiple sessions for Next.js");

    // Alice でフィルタ
    let alice_id = eng.query(&[("name", eng.vocab_id("Alice").unwrap()), ("kind", 1)])[0];
    let alice_sessions = ravn.filter_by(&sessions, "speaker", alice_id as u32);
    assert_eq!(alice_sessions.len(), 2, "Alice has 2 Next.js sessions");

    // decision を抽出
    let decisions = ravn.extract_text(&alice_sessions, "decision");
    let decisions_str: Vec<String> = decisions.iter()
        .map(|b| String::from_utf8_lossy(b).to_string())
        .collect();

    assert!(decisions_str.contains(&"middleware で早期 return に統一".to_string()));
    assert!(decisions_str.contains(&"SSR を維持する".to_string()));
    // Bob の決定 "App Router に移行" は含まれない
    assert!(!decisions_str.iter().any(|s| s.contains("App Router")));
    // Rust の決定は含まれない
    assert!(!decisions_str.iter().any(|s| s.contains("Arc<RwLock>")));

    println!("\n=== Recalled decisions for Alice × Next.js ===");
    for d in &decisions_str {
        println!("  - {}", d);
    }

    cleanup(&path);
}

#[test]
fn recall_sessions_touching_file() {
    let path = tmp("file");
    let eng = seed(&path);
    let ravn = Ravn::new(Arc::clone(&eng));

    // 「ファイル `app/api/auth/route.ts` に関するセッション」
    let file_name = eng.vocab_id("app/api/auth/route.ts").unwrap();
    let files = eng.query(&[("name", file_name), ("kind", 3)]);
    assert_eq!(files.len(), 1);

    let sessions = ravn.reverse_follow(&files, "related_file");
    assert_eq!(sessions.len(), 1, "1 session touching this file");

    let decisions = ravn.extract_text(&sessions, "decision");
    let s: String = String::from_utf8_lossy(&decisions[0]).to_string();
    assert_eq!(s, "middleware で早期 return に統一");

    cleanup(&path);
}

#[test]
fn exec_dsl_alice_nextjs_decisions() {
    let path = tmp("dsl");
    let eng = seed(&path);
    let ravn = Ravn::new(Arc::clone(&eng));

    // Ravn DSL で同じクエリ:
    //   kind:2 name:"Next.js"       — Next.js Topic
    //   | reverse topic              — topic 指しているセッション
    //   | where speaker:_alice       — speaker = Alice(eid を埋め込む)
    //   | get decision                — decision 抽出

    // Alice の eid を事前に取得(DSL では u32 直書きしかできないので)
    let alice_id = eng.query(&[("name", eng.vocab_id("Alice").unwrap()), ("kind", 1)])[0];

    let query = format!(
        r#"kind:2 name:"Next.js" | reverse topic | where speaker:{} | get decision"#,
        alice_id
    );
    match ravn.exec(&query) {
        enchudb::RavnResult::Values(rows) => {
            assert_eq!(rows.len(), 2, "should find 2 decisions, got {}", rows.len());
        }
        other => panic!("expected Values, got {:?}", other),
    }

    cleanup(&path);
}

#[test]
fn graph_traversal_depth_limit() {
    // (親 -> 子) 階層を作って BFS で深さ制限確認
    let path = tmp("bfs");
    let mut eng = Engine::create_standalone(&path).unwrap();
    eng.define_himo("parent", ValueType::Ref, 0);

    // root -> a -> b -> c -> d
    let root = eng.entity();
    let a = eng.entity(); eng.tie_ref(a, "parent", root);
    let b = eng.entity(); eng.tie_ref(b, "parent", a);
    let c = eng.entity(); eng.tie_ref(c, "parent", b);
    let d = eng.entity(); eng.tie_ref(d, "parent", c);

    eng.rebuild();
    let eng = Arc::new(eng);
    let ravn = Ravn::new(Arc::clone(&eng));

    // d から parent を 2 段辿り → [d], [c], [b]
    let levels = ravn.bfs(&[d], "parent", 2);
    assert_eq!(levels.len(), 3);
    assert_eq!(levels[0].1, vec![d]);
    assert_eq!(levels[1].1, vec![c]);
    assert_eq!(levels[2].1, vec![b]);
    // root, a は depth 3, 4 なので出てこない

    cleanup(&path);
}
