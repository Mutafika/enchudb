//! issue #117 repro: schema `Database` + raw `engine_mut().define_table()` で定義し
//! `finish_*` を呼ばず flush+drop した DB が、reopen で
//!   顔1: table 定義消失 (`.tables` sidecar 未永続 → entity_in が "table not found")
//!   顔2: next_local 巻き戻り → entity_in が生きた eid を再払出 (silent data 破壊)
//! を起こす。#47 (builder 経路) と同じ failure mode が raw-define 経路から漏れたもの。
//!
//! 修正後は両 test が pass する。案2 (open 時 live bitmap から next_local 自己修復) が
//! 入れば経路非依存で塞がる。
//!
//! ※ enchudb テストは固定 /tmp を使うと並行 cargo test で偽 flaky になるため、
//!   path は pid+nanos で unique 化し、逐次前提で書く。

use enchudb_engine::{Engine, ValueType};
use enchudb_schema::Database;

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "enchudb-issue117-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// 顔1: raw-define + flush + drop → reopen で table 定義が消える。
#[test]
fn face1_table_definition_survives_reopen() {
    let dir = tmp_dir("face1");
    let path = dir.join("wiki.ecdb");
    let path_s = path.to_str().unwrap();

    {
        let mut db = Database::create_growable_with_capacity(path_s, 262_144).unwrap();
        let eng = db.engine_mut().unwrap();
        eng.define_table("wiki", 200_000).unwrap();
        eng.define_himo_in("wiki", "kind", ValueType::Tag, 0).unwrap();
        eng.flush().unwrap();
        // finish_* を呼ばず drop (opyula の wiki route と同じ)
    }

    let eng = Engine::open_standalone(path_s).unwrap();
    // 現状 (master): `.tables` sidecar が生成されず table 'wiki' 消失 → Err。
    let r = eng.entity_in("wiki");
    assert!(
        r.is_ok(),
        "reopen 後 table 'wiki' が消えている (#117 顔1): {:?}",
        r.err()
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// 顔2 (致命的): define 時に sidecar を作る (next_local=0) → entity_in で live eid を
/// 進める → flush+drop (drop では sidecar 再永続されない) → reopen で next_local が 0 に
/// 巻き戻り、生きた eid を再払出して既存 entity を無警告で上書きする。
#[test]
fn face2_reopen_does_not_reissue_live_eids() {
    let dir = tmp_dir("face2");
    let path = dir.join("wiki.ecdb");
    let path_s = path.to_str().unwrap();

    let pre: Vec<u64>;
    {
        let mut db = Database::create_growable_with_capacity(path_s, 262_144).unwrap();
        let eng = db.engine_mut().unwrap();
        eng.define_table("wiki", 200_000).unwrap();
        eng.define_himo_in("wiki", "kind", ValueType::Tag, 0).unwrap();
        // define 時点で sidecar を作る (next_local=0 が焼かれる)
        eng.persist_tables().unwrap();

        // 5 entity を alloc (entity_in が global bitset を live マーク、in-mem
        // next_local が 5 まで進む)
        let mut eids = Vec::new();
        for _ in 0..5u32 {
            eids.push(eng.entity_in("wiki").unwrap());
        }
        pre = eids;

        // flush は bitset (5 live) を永続するが、defer により sidecar next_local は
        // 0 のまま。finish_* を呼ばず drop。
        eng.flush().unwrap();
    }

    let eng = Engine::open_standalone(path_s).unwrap();
    // reopen 後、pre の 5 entity は依然 live のはず。
    for &e in &pre {
        assert!(eng.is_live(e), "pre entity {e} が reopen 後 live でない");
    }
    // 新規 alloc は pre と衝突してはならない。master: next_local=0 巻き戻りで pre[0] を再払出。
    let new_eid = eng.entity_in("wiki").unwrap();
    assert!(
        !pre.contains(&new_eid),
        "reopen 後の entity_in が live eid {new_eid} を再払出 (#117 顔2, silent 破壊). pre={pre:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
