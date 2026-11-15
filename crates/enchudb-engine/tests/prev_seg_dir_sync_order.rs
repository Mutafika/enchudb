//! review946c [C]: `prev.seg` の 「書き出した周期」 (header) を進めて書き出す前に、 DB の directory を fsync して sidecar の rename を
//! disk へ送る。 使い回しの退避の載る sidecar のうち `eidalias` と、 `.eidmap` / `pktombs` / `pkformer` のまとめ直しは tmp + rename で、
//! 電源断で rename が戻る file system では、 header だけが 「書き出した」 と言い、 使い回しの前の sidecar のまま開く時に記録を戻さない
//! (`tables` の #884 と同じ形)。
//!
//! rename の戻りは crashsim の像では作れないので、 I/O の点 (`enchudb_oplog::io_point`) を通った順で確かめる: sidecar の書き出し 1 回の
//! 中で `sidecar.rename` (`eidalias`) → `prev.dir_sync` → `segment.msync` (`prev.seg` の header)。 点の記録 (`ENCHU_FAULT_TRACE`) は
//! process に 1 つなので、 この file の試験は 1 本だけ。

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::Hlc;

#[test]
fn the_sidecar_renames_are_synced_before_the_persisted_epoch_is_written_out() {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let root = std::env::temp_dir().join(format!("enchudb-prevseg-dirsync-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let trace = root.join("trace");
    // 点の記録は最初の点で読まれるので、 engine を作る前に置く
    // SAFETY: この test binary の試験は 1 本で、 env を読む thread はまだ無い
    unsafe { std::env::set_var("ENCHU_FAULT_TRACE", &trace) };
    let db = root.join("db");
    let eng = {
        let mut eng = Engine::create_with_capacity(db.to_str().unwrap(), 1024).unwrap();
        eng.define_table("t", 2).unwrap();
        eng.define_himo_in("t", "k", ValueType::Number, 0).unwrap();
        let k = eng.himo_id("t.k").unwrap() as u16;
        eng.set_table_pk("t", k).unwrap();
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, 4 << 20).unwrap();
        eng.set_peer_id(1);
        eng
    };
    let hk = eng.himo_id("t.k").unwrap() as u16;
    let x = eng.entity_in("t").unwrap();
    eng.tie_to(x, "t.k", 7u32);
    // B の行 b を受ける (表は満杯)
    let world_b = enchudb_oplog::make_eid(3, 9);
    let b = eng.resolve_remote_eid(world_b, hk).unwrap();
    assert!(eng.remote_tie_apply_result(b, hk, 8u32, Hlc { wall: 1_000, logical: 0, peer: 3 }).applied());
    eng.oplog_sync().unwrap();
    eng.persist_sync_state().unwrap();
    let read = || -> Vec<String> {
        std::fs::read_to_string(&trace)
            .unwrap_or_default()
            .lines()
            .map(|l| l.split('\t').nth(1).unwrap_or_default().to_string())
            .collect()
    };
    let sites = {
        let _hold = eng.hold_consumer();
        // X を消して場所を使い回し (記録を書く)、 b を Y の別名にする (`eidalias` を書き直す = rename)
        eng.delete(x);
        eng.oplog_commit();
        let y = eng.entity_in("t").unwrap();
        assert_eq!(eng.eid_slot(y), eng.eid_slot(x), "前提: 場所を使い回した");
        eng.tie_to(y, "t.k", 7u32);
        eng.merge_entity_into(b, y).unwrap();
        let start = read().len();
        eng.persist_tables().unwrap();
        read()[start..].to_vec()
    };
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
    let i = sites
        .iter()
        .position(|s| s == "prev.dir_sync")
        .unwrap_or_else(|| panic!("`prev.seg` の周期を進める前に directory を fsync していない: {sites:?}"));
    assert!(sites[..i].iter().any(|s| s == "sidecar.rename"), "前提: sidecar を rename した: {sites:?}");
    assert!(
        sites[i + 1..].iter().any(|s| s == "segment.msync"),
        "前提: 周期を進めた `prev.seg` の header の書き出しが directory の fsync の後にある: {sites:?}"
    );
}
