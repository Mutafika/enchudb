//! issue #47: `remote_tie_apply` (sync 経路) が `advance_table_next_local_for`
//! を呼んでないため、 foreign sync 後に `entity_in` が live local を払出す。
//!
//! repro: 1 process 内で
//!   1) articles 20 seed → next_local = 20
//!   2) foreign Tie (peer != self) を articles.eid_range 内の local 20, 21 に流入
//!      → entities.ensure_live(20/21) で live、 but next_local 据え置きの 20
//!   3) entity_in("articles") → fetch_add(1) で 20 を返す → live local と collision
//!
//! 期待: 修正後は (2) で next_local が 22 まで前進、 (3) は 22 を返す。
//! 修正前: assert で失敗 (entity_in 返り値が live と一致)。

use enchudb_engine::Engine;
use enchudb_oplog::{make_eid, eid_local};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue47-engine-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
    let _ = std::fs::remove_file(format!("{}.tables", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.db.lock", path));
}

#[test]
fn remote_tie_apply_advances_next_local() {
    let path = tmp_path("tie");
    cleanup(&path);

    // create + define + spawn consumer
    let mut eng = Engine::create_growable_with_capacity(&path, 65_536).unwrap();
    eng.define_table("articles", 10_000).unwrap();
    eng.define_himo_in("articles", "url", enchudb_engine::HimoType::Tag, 0).unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(0x10000001); // self
    eng.set_gossip_remote_apply(false);

    let url_hid = eng.himo_id("articles.url").unwrap() as u16;

    // 1) seed 20 articles locally
    let mut seed_eids = Vec::new();
    for i in 0..20 {
        let eid = eng.entity_in("articles").unwrap();
        eng.tie_text_async_by_id(eid, url_hid, &format!("https://seed/{i}"));
        seed_eids.push(eid);
    }
    eng.oplog_commit();
    eng.oplog_sync().ok();

    // 2) simulate foreign sync: 2 Tie ops at foreign locals that land at our local 20, 21
    let foreign_peer: enchudb_oplog::PeerId = 0xABCD0001;
    // articles eid_range_lo を取得
    let articles_lo = eng
        .list_tables()
        .into_iter()
        .find(|(_, name, _, _)| name == "articles")
        .map(|(_, _, lo, _)| lo)
        .unwrap();
    for offset in 20..22 {
        let foreign_eid = make_eid(foreign_peer, articles_lo + offset);
        // remote_tie_apply は value (vid) を渡す形式 — 任意の小さい vid で OK
        eng.remote_tie_apply(foreign_eid, url_hid, 1, None);
        assert_eq!(eid_local(foreign_eid), articles_lo + offset);
    }

    // 3) entity_in should return fresh locals beyond the live foreign ones (= local 22+)
    let new_eid_a = eng.entity_in("articles").unwrap();
    let new_eid_b = eng.entity_in("articles").unwrap();
    let new_eid_c = eng.entity_in("articles").unwrap();

    // 期待: new_eid_a/b/c の local が 22, 23, 24 (= foreign が触った 20, 21 を avoid)
    let new_locals: Vec<u32> = [new_eid_a, new_eid_b, new_eid_c]
        .iter()
        .map(|e| eid_local(*e))
        .collect();
    let foreign_locals: Vec<u32> = (20..22).map(|o| articles_lo + o).collect();

    let collisions: Vec<&u32> = new_locals
        .iter()
        .filter(|l| foreign_locals.contains(l))
        .collect();

    assert!(
        collisions.is_empty(),
        "BUG: entity_in returned live local that collides with foreign sync: \
         new_locals={:?}, foreign_locals={:?}",
        new_locals,
        foreign_locals,
    );

    drop(eng);
    cleanup(&path);
}
