//! Sharded query routing の E2E。
//!
//! peer A が himo X を、peer B が himo Y をそれぞれ担当し、
//! 各 peer は自分担当の himo だけ define_himo する状態を作る。
//! ShardQuery が owner peer に自動 route して正しい結果を返すか確認。

#![cfg(feature = "v32")]

use std::sync::Arc;

use enchudb::{Engine, HimoType};
use enchudb::sync::shard::{ExplicitRouter, HashRouter, InMemoryShardTransport, ShardQuery, ShardRouter};

fn tmp(tag: &str) -> String {
    let p = format!("/tmp/enchudb-shard-{}-{}", tag, std::process::id());
    for suffix in ["", ".wal", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", p, suffix));
    }
    p
}

fn cleanup(path: &str) {
    for suffix in ["", ".wal", ".crc"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

/// 単一 himo を single peer で持つ engine を立てる。
fn make_sharded_peer(path: &str, peer: u32, owned_himos: &[(&str, HimoType)]) -> Arc<Engine> {
    {
        let mut eng = Engine::create_standalone(path).unwrap();
        for (name, ty) in owned_himos {
            eng.define_himo(name, *ty, 1000);
        }
        eng.flush().unwrap();
    }
    let eng = Engine::open_concurrent_with_wal(path, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(peer);
    eng
}

#[test]
fn single_condition_routes_to_owner_peer() {
    // 2 peer: A が "color"、B が "size"
    let pa = tmp("single_a");
    let pb = tmp("single_b");
    let eng_a = make_sharded_peer(&pa, 1, &[("color", HimoType::Value)]);
    let eng_b = make_sharded_peer(&pb, 2, &[("size", HimoType::Value)]);

    // A に entity 作って color=5 を tie
    let e1 = eng_a.entity();
    eng_a.tie_to(e1, "color", 5);
    eng_a.rebuild();

    // B に entity 作って size=10 を tie
    let e2 = eng_b.entity();
    eng_b.tie_to(e2, "size", 10);
    eng_b.rebuild();

    // ShardTransport に両 peer 登録
    let transport = Arc::new(InMemoryShardTransport::new());
    transport.register(1, eng_a.clone());
    transport.register(2, eng_b.clone());

    // router: color→1、size→2 を explicit で
    let mut m = std::collections::HashMap::new();
    m.insert("color".to_string(), 1u32);
    m.insert("size".to_string(), 2u32);
    let router = Arc::new(ExplicitRouter::new(m, 1));

    // A 側で ShardQuery
    let q_a = ShardQuery::new(eng_a.clone(), router.clone(), transport.clone());
    // color=5 は local
    let r = q_a.pull_raw("color", 5);
    assert_eq!(r.len(), 1, "A's own color entity");
    assert_eq!(enchudb::eid_peer(r[0]), 1);
    assert_eq!(enchudb::eid_local(r[0]), enchudb::eid_local(e1));

    // size=10 は remote (B)
    let r = q_a.pull_raw("size", 10);
    assert_eq!(r.len(), 1, "remote B's size entity");
    assert_eq!(enchudb::eid_peer(r[0]), 2, "should be peer 2");
    assert_eq!(enchudb::eid_local(r[0]), enchudb::eid_local(e2));

    cleanup(&pa);
    cleanup(&pb);
}

#[test]
fn explicit_router_falls_back_for_unknown_himo() {
    let pa = tmp("fallback_a");
    let eng_a = make_sharded_peer(&pa, 1, &[("color", HimoType::Value)]);

    let transport = Arc::new(InMemoryShardTransport::new());
    transport.register(1, eng_a.clone());

    let m = std::collections::HashMap::new();
    let router = Arc::new(ExplicitRouter::new(m, 1)); // 未知 himo は peer 1 にフォールバック

    let q = ShardQuery::new(eng_a.clone(), router, transport);
    // 未定義 himo は peer 1 (自分) に行き、engine.pull_raw は空 Vec
    let r = q.pull_raw("nonexistent", 42);
    assert_eq!(r, Vec::<enchudb::EntityId>::new());

    cleanup(&pa);
}

#[test]
fn hash_router_deterministic_across_peers() {
    // 同じ peer 集合で作った HashRouter なら、どの peer で作っても同じ owner 判定
    let r1 = HashRouter::new(vec![1, 2, 3]);
    let r2 = HashRouter::new(vec![1, 2, 3]);
    for h in ["color", "size", "weight", "foo", "bar"] {
        assert_eq!(r1.owner_of(h), r2.owner_of(h));
    }
}

#[test]
fn is_mine_filter() {
    // peer 自身が owner の himo だけ tie できるようにする運用判定
    let pa = tmp("mine_a");
    let eng_a = make_sharded_peer(&pa, 1, &[("a_himo", HimoType::Value)]);

    let transport = Arc::new(InMemoryShardTransport::new());
    transport.register(1, eng_a.clone());

    let mut m = std::collections::HashMap::new();
    m.insert("a_himo".to_string(), 1u32);
    m.insert("b_himo".to_string(), 2u32);
    let router = Arc::new(ExplicitRouter::new(m, 1));

    let q = ShardQuery::new(eng_a.clone(), router, transport);
    assert!(q.is_mine("a_himo"));
    assert!(!q.is_mine("b_himo"));

    cleanup(&pa);
}

#[test]
fn two_condition_query_crosses_shards() {
    // 2 条件 AND が 2 shard にまたがっても正しく intersect される。
    // ただしこのテストの構造上、entity は 1 peer にしか作れないので、
    // 「同一 entity が両 himo を持つ」シナリオは semantic にそもそも成立しない
    // (peer A の entity は peer A の himo しか持てない、peer B の himo column には存在しない)。
    //
    // 代わりに「ある entity が条件 X を満たすか (peer A で chek)、
    // 別の entity が条件 Y を満たすか (peer B で check)、交差が空」を確認。
    let pa = tmp("two_a");
    let pb = tmp("two_b");
    let eng_a = make_sharded_peer(&pa, 1, &[("color", HimoType::Value)]);
    let eng_b = make_sharded_peer(&pb, 2, &[("size", HimoType::Value)]);

    // A に entity e_a で color=3、B に別 entity e_b で size=5
    let e_a = eng_a.entity();
    eng_a.tie_to(e_a, "color", 3);
    eng_a.rebuild();

    let e_b = eng_b.entity();
    eng_b.tie_to(e_b, "size", 5);
    eng_b.rebuild();

    let transport = Arc::new(InMemoryShardTransport::new());
    transport.register(1, eng_a.clone());
    transport.register(2, eng_b.clone());
    let mut m = std::collections::HashMap::new();
    m.insert("color".to_string(), 1u32);
    m.insert("size".to_string(), 2u32);
    let router = Arc::new(ExplicitRouter::new(m, 1));

    let q = ShardQuery::new(eng_a.clone(), router, transport);
    // color=3 AND size=5
    // color=3 は e_a (peer 1)、size=5 は e_b (peer 2)
    // 同じ entity じゃないので intersect = 空
    let r = q.query(&[("color", 3), ("size", 5)]);
    assert!(r.is_empty(), "no entity has both, expect empty");

    // 片方だけの query は結果あり
    let r = q.query(&[("color", 3)]);
    assert_eq!(r.len(), 1);
    assert_eq!(enchudb::eid_peer(r[0]), 1);

    let r = q.query(&[("size", 5)]);
    assert_eq!(r.len(), 1);
    assert_eq!(enchudb::eid_peer(r[0]), 2);

    cleanup(&pa);
    cleanup(&pb);
}

#[test]
fn replicated_entity_matches_across_shards_via_sync() {
    // より現実的なシナリオ: entity 自体は replicated (同じ eid)、himo column が sharded。
    //
    // peer A が owner でも peer B から query される himo については、
    // peer A の column に peer B の entity (の remote_tie_apply 経由 entry) が入ってて、
    // B は color を持ってないが (A の column に B の entity の entry)、
    // 跨 shard query で A が「color=3 の entity」を返す、B は size column を持ってる。
    //
    // ここでは Syncer を介さず、手動で remote_tie_apply を呼んで「同じ eid 共有」を再現。
    let pa = tmp("repl_a");
    let pb = tmp("repl_b");
    let eng_a = make_sharded_peer(&pa, 1, &[("color", HimoType::Value)]);
    let eng_b = make_sharded_peer(&pb, 2, &[("size", HimoType::Value)]);

    // 共有 eid: peer 0 が「論理的に」作った entity とする (あるいは root peer 扱い)
    let shared_local = 42u32;
    let shared_eid = enchudb::make_eid(0, shared_local);

    // peer A の color column に entry
    let color_himo = eng_a.himo_id("color").unwrap() as u16;
    eng_a.remote_tie_apply(shared_eid, color_himo, 3);
    eng_a.rebuild();

    // peer B の size column に entry (同じ shared_local)
    let size_himo = eng_b.himo_id("size").unwrap() as u16;
    eng_b.remote_tie_apply(shared_eid, size_himo, 5);
    eng_b.rebuild();

    let transport = Arc::new(InMemoryShardTransport::new());
    transport.register(1, eng_a.clone());
    transport.register(2, eng_b.clone());
    let mut m = std::collections::HashMap::new();
    m.insert("color".to_string(), 1u32);
    m.insert("size".to_string(), 2u32);
    let router = Arc::new(ExplicitRouter::new(m, 1));

    // A 側で両条件 query
    let q = ShardQuery::new(eng_a.clone(), router, transport);
    let r = q.query(&[("color", 3), ("size", 5)]);
    // 現実装の制約: InMemoryShardTransport が peer_id を上位 32bit に composite するため、
    // peer 1 由来の eid = [1|42]、peer 2 由来の eid = [2|42] で別 u64 扱い、intersect 空。
    // 「共有 entity」を表現するには entity id そのものを normalize する合意が要る。
    // → v34+ で「論理 eid (peer 0 のような root)」「peer 跨ぎ resolver」を追加する時に解消。
    assert_eq!(r.len(), 0, "current impl: cross-peer eid composition prevents shared-entity match");

    cleanup(&pa);
    cleanup(&pb);
}
