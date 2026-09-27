//! #316: sync で届いた Tie を、 列を伸ばせない時に 「適用済み」 にしない。
//!
//! 旧: `set_cell_local` が列を伸ばせなかった失敗 (`live_set` の false) を捨て、 版数だけ記録して true を返した。
//! sync 層は `Applied` と数えて cursor を進め、 値の無い cell に版数だけが残るので、 同じ record を再配送しても
//! LWW で負けて入らない = 受け手で黙って恒久的に消えた。
//!
//! 空き不足は `set_space_margin` で作る (#167 と同じ)。

use enchudb_engine::{Engine, FaultKind, RemoteApply, ValueType};
use enchudb_oplog::Hlc;

fn tmp(tag: &str) -> String {
    let p = format!(
        "/tmp/enchudb-issue316-remote-{}-{}-{}.db",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    );
    let _ = std::fs::remove_dir_all(&p);
    p
}

const PEER: u64 = 7;

fn remote_eid(local: u32) -> u64 {
    (PEER << 32) | local as u64
}

fn hlc(wall: u64) -> Hlc {
    Hlc { wall, logical: 0, peer: PEER as u32 }
}

/// 列を伸ばせない時は `RejectedCapacity` で、 値も版数も書かない。 空きが戻れば同じ record が入る。
#[test]
fn remote_tie_without_room_is_rejected_and_redeliverable() {
    let path = tmp("tie");
    let mut eng = Engine::create_growable_with_cell_version(&path, 1 << 20).expect("create");
    eng.define_himo("n", ValueType::Number, 0);
    let hid = eng.himo_id("n").unwrap() as u16;
    assert!(eng.has_cell_version(), "premise: 版数の列があること");

    let wall = 1_790_000_000_000u64;
    // 手前は書ける (前提)
    assert_eq!(eng.remote_tie_apply_result(remote_eid(1), hid, 11u32, hlc(wall)), RemoteApply::Applied);
    assert_eq!(eng.get_by_id(remote_eid(1), hid), Some(11));

    // 以降 「空きが足りない」。 列の commit の外 (末尾寄り) の eid に届いた Tie
    eng.set_space_margin(u64::MAX / 2);
    let far = (1 << 20) - 2;
    let r = eng.remote_tie_apply_result(remote_eid(far), hid, 42u32, hlc(wall + 1));
    assert_eq!(r, RemoteApply::RejectedCapacity, "列を伸ばせないのに {r:?}");
    assert_eq!(eng.get_by_id(remote_eid(far), hid), None, "値が書かれている");
    assert_eq!(eng.cell_hlc(remote_eid(far), hid), Hlc::ZERO, "値が無いのに版数が記録された");
    assert!(eng.fault_count(FaultKind::DiskSpace) > 0);
    // bool 版も適用済みと言わない
    assert!(!eng.remote_tie_apply(remote_eid(far), hid, 42u32, hlc(wall + 1)));

    // 空きが戻れば、 同じ record の再配送が入る (旧: 版数が残っていて LWW で負けた)
    eng.set_space_margin(0);
    assert_eq!(eng.remote_tie_apply_result(remote_eid(far), hid, 42u32, hlc(wall + 1)), RemoteApply::Applied);
    assert_eq!(eng.get_by_id(remote_eid(far), hid), Some(42));
    assert_eq!(eng.cell_hlc(remote_eid(far), hid), hlc(wall + 1));
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}

/// Leaf も同じ: cell の列を伸ばせないなら payload を置かずに `RejectedCapacity`。
#[test]
fn remote_tieleaf_without_room_is_rejected_and_redeliverable() {
    let path = tmp("leaf");
    let mut eng = Engine::create_growable_with_cell_version(&path, 1 << 20).expect("create");
    eng.define_himo("body", ValueType::Leaf, 0);
    let hid = eng.himo_id("body").unwrap() as u16;

    let wall = 1_790_000_000_000u64;
    eng.set_space_margin(u64::MAX / 2);
    let far = (1 << 20) - 2;
    let r = eng.remote_tieleaf_apply(remote_eid(far), hid, b"hello", hlc(wall));
    assert_eq!(r, RemoteApply::RejectedCapacity, "列を伸ばせないのに {r:?}");
    assert_eq!(eng.cell_hlc(remote_eid(far), hid), Hlc::ZERO, "値が無いのに版数が記録された");

    eng.set_space_margin(0);
    assert_eq!(eng.remote_tieleaf_apply(remote_eid(far), hid, b"hello", hlc(wall)), RemoteApply::Applied);
    assert_eq!(eng.get_text(remote_eid(far), "body"), Some(&b"hello"[..]));
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}
