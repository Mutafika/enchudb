//! #255: **writer open が全 leaf himo の cylinder を eager に build していた**。
//!
//! `Engine::open` (rw) は load 末尾で LeafStore の free-list を再構成する (free-list は
//! 非永続、 #88)。 それを `HimoStore::unique_values()` で集めていたため、 leaf を持つ全 himo の
//! in-memory index が open で組まれ、 drop で free されていた (`sf` の実 DB = 117 himo /
//! 9211 entity で open +150 ms / drop +100 ms)。 raw cell の走査に替えた。
//!
//! gate は 2 つ:
//! 1. rw open 直後に cylinder が組まれている himo は 0 本 (readonly と同じ)。 列を触ると 1 本ずつ増える
//! 2. 走査を替えても free-list の中身は同じ: reopen 後に **live slot は配り直されず**、
//!    **空いた slot は再利用される** (footprint が伸びない)

use enchudb_engine::{Engine, ValueType};

fn tmp(tag: &str) -> String {
    let p = format!("{}/enchudb-issue255-{}-{}", std::env::temp_dir().display(), tag, std::process::id());
    let _ = enchudb_engine::db_files::remove_db(&p);
    p
}

const HIMOS: u32 = 8;
const ENTS: u32 = 300;

/// Leaf / Number を交互に持つ table を作り、 全 entity に値を張って閉じる。
fn build_mixed(path: &str) -> Vec<enchudb_oplog::EntityId> {
    let mut eng = Engine::create_with_capacity(path, 4096).unwrap();
    eng.define_table("t", 2048).unwrap();
    for h in 0..HIMOS {
        let vt = if h % 2 == 0 { ValueType::Leaf } else { ValueType::Number };
        eng.define_himo_in("t", &format!("h{h}"), vt, 0).unwrap();
    }
    let mut eids = Vec::new();
    for i in 0..ENTS {
        let e = eng.entity_in("t").unwrap();
        for h in 0..HIMOS {
            if h % 2 == 0 {
                eng.tie_text_to(e, &format!("t.h{h}"), &format!("leaf value {i}/{h}"));
            } else {
                eng.tie(e, &format!("t.h{h}"), i + h);
            }
        }
        eids.push(e);
    }
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    eids
}

#[test]
fn rw_open_builds_no_cylinder_until_touched() {
    let path = tmp("lazy");
    let eids = build_mixed(&path);

    let ro = Engine::open_readonly(&path).unwrap();
    assert_eq!(ro.himos_with_cylinder_built(), 0, "readonly open must not build any cylinder");
    drop(ro);

    let rw = Engine::open(&path).unwrap();
    assert_eq!(
        rw.himos_with_cylinder_built(), 0,
        "rw open must not build any cylinder (leaf free-list rebuild used to build all leaf himos)"
    );
    // 点読み (`get`) は column を直接読むので組まれない。 書き込みは その列だけ 組む
    // (accessor が build を観測できることの確認)
    assert_eq!(rw.get(eids[0], "t.h1"), Some(1));
    assert_eq!(rw.get_text_owned(eids[0], "t.h0").as_deref(), Some(b"leaf value 0/0".as_slice()));
    assert_eq!(rw.himos_with_cylinder_built(), 0);
    rw.tie_to(eids[0], "t.h1", 5);
    assert_eq!(rw.himos_with_cylinder_built(), 1);
    drop(rw);
    let _ = enchudb_engine::db_files::remove_db(&path);
}

/// 走査を替えても free-list が同じ形に再構成されること。 live slot を配り直せば偶数 entity の
/// 値が壊れ、 空き slot を見落とせば footprint が伸びる。
#[test]
fn rebuilt_free_list_keeps_live_slots_and_reuses_holes() {
    let path = tmp("freelist");
    let value = |i: u32| format!("payload {i:05} ------------------------"); // 同じ長さ = 同じ slot size
    let mut eids = Vec::new();
    {
        let mut eng = Engine::create_with_capacity(&path, 4096).unwrap();
        eng.define_table("t", 2048).unwrap();
        eng.define_himo_in("t", "blob", ValueType::Leaf, 0).unwrap();
        for i in 0..200u32 {
            let e = eng.entity_in("t").unwrap();
            eng.tie_text_to(e, "t.blob", &value(i));
            eids.push(e);
        }
        // 奇数を外して穴を空ける。 末尾 (199) は live のまま残す — 末尾の slot を free すると
        // high_water が下がる (= 穴ではなく未使用) ので、 「穴の再利用」 だけを見るため
        for (i, e) in eids.iter().enumerate() {
            if i % 2 == 1 && i < 199 {
                eng.untie(*e, "t.blob");
            }
        }
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
    }

    let eng = Engine::open(&path).unwrap();
    assert_eq!(eng.himos_with_cylinder_built(), 0);
    let footprint = eng.leaf_footprint().expect("leaf region");
    // 穴を埋め直す: 99 個の同サイズ payload は既存の穴に収まり、 footprint は伸びない
    for (i, e) in eids.iter().enumerate() {
        if i % 2 == 1 && i < 199 {
            eng.tie_text_to(*e, "t.blob", &value(1000 + i as u32));
        }
    }
    eng.flush_writes();
    assert_eq!(eng.leaf_footprint(), Some(footprint), "freed slots must be reused after reopen");
    // live slot は配り直されていない
    for (i, e) in eids.iter().enumerate() {
        let want = if i % 2 == 0 || i == 199 { value(i as u32) } else { value(1000 + i as u32) };
        assert_eq!(eng.get_text_owned(*e, "t.blob").as_deref(), Some(want.as_bytes()), "entity {i}");
    }
    drop(eng);

    // 永続化後も同じ
    let ro = Engine::open_readonly(&path).unwrap();
    for (i, e) in eids.iter().enumerate() {
        let want = if i % 2 == 0 || i == 199 { value(i as u32) } else { value(1000 + i as u32) };
        assert_eq!(ro.get_text_owned(*e, "t.blob").as_deref(), Some(want.as_bytes()), "entity {i} after reopen");
    }
    drop(ro);
    let _ = enchudb_engine::db_files::remove_db(&path);
}
