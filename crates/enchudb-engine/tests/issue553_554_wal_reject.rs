//! #553 / #554: WAL に載らない書き込みは、 Ok を返して本体にだけ書く (再起動の再生・配る分・change listener から抜ける) の
//! ではなく、 `Err` で断って本体にも当てない。
//!
//! - WAL より大きい 1 件 (#553): どの DB でも、 待たずに (前は 5 秒待って consumer を約 2 万回起こした) 断る。 落ちた record
//!   としては数えない (相手に取り直させる floor も上げない)
//! - sync の表の無い DB で満杯の WAL を畳めない時 (#554): 断る (落ちたことを知る相手が居ない)。 畳めないのは書き出しの失敗の
//!   時だけなので、 engine の単体試験 (`wal_reject_tests`) が書き出しの失敗を割り込み点で作って見る。 sync の表のある DB は今まで
//!   通り落とした record として本体に書き、 相手に floor で知らせる (`issue268_commit_failure::records_dropped_from_the_sync_api_are_counted`)

use enchudb_engine::{Engine, FaultKind, TieRejected, ValueType};
use std::time::{Duration, Instant};

fn tmp(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/enchudb-wal-reject-{tag}-{}-{nanos}", std::process::id())
}

fn open(path: &str, sync: bool, oplog: usize) -> std::sync::Arc<Engine> {
    {
        let mut eng = Engine::create_standalone(path).unwrap();
        eng.define_table("rows", 1_000).unwrap();
        eng.define_himo_in("rows", "val", ValueType::Number, 1_000).unwrap();
        eng.define_himo_in("rows", "name", ValueType::Tag, 0).unwrap();
        eng.define_himo_in("rows", "memo", ValueType::Leaf, 0).unwrap();
        if sync {
            eng.enable_sync_tables().unwrap();
        }
        eng.flush().unwrap();
    }
    let eng = Engine::open_concurrent_with_oplog(path, oplog).unwrap();
    eng.set_peer_id(1);
    eng
}

/// #553: WAL (64 KiB) より大きい文字 (100 KiB) を書くと、 待たずに断り、 本体にも当てない。 Leaf (TieLeaf) と Tag (Vocab) の両方、
/// sync の表のある DB と無い DB の両方。
#[test]
fn a_record_larger_than_the_wal_is_rejected_at_once() {
    for sync in [false, true] {
        let path = tmp(if sync { "large-sync" } else { "large" });
        let eng = open(&path, sync, 64 * 1024);
        let memo = eng.himo_id("rows.memo").unwrap() as u16;
        let name = eng.himo_id("rows.name").unwrap() as u16;
        let e = eng.entity_in("rows").unwrap();
        let big = "x".repeat(100 * 1024);
        for (what, himo) in [("Leaf", memo), ("Tag", name)] {
            let started = Instant::now();
            let r = eng.try_tie_text_to_by_id(e, himo, &big);
            let took = started.elapsed();
            assert_eq!(r, Err(TieRejected::Fault(FaultKind::WalSpace)), "sync={sync} {what}: WAL より大きい書き込みを断らない");
            assert!(took < Duration::from_secs(1), "sync={sync} {what}: 断るまで {took:?} 待った");
            assert_eq!(eng.get_text_owned(e, &format!("rows.{}", if himo == memo { "memo" } else { "name" })), None, "sync={sync} {what}: 断った書き込みが本体に当たった");
        }
        assert_eq!(eng.wal_dropped_records(), 0, "sync={sync}: WAL より大きい書き込みを落ちた record に数えた");
        assert_eq!(eng.fault_count(FaultKind::WalSpace), 2);
        // 小さい書き込みは今まで通り
        eng.try_tie_text_to_by_id(e, memo, "small").unwrap();
        assert_eq!(eng.get_text_owned(e, "rows.memo"), Some(b"small".to_vec()));
        drop(eng);
        let _ = std::fs::remove_dir_all(&path);
    }
}
