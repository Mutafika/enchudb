//! #442: `_sync_ops` の行は届いたのに payload の ring の entry が届かなかった DB (電源断) を開いても、 bridge が止まらず、
//! 届いていた payload も上書きしない。
//!
//! 旧: 開いた時の ring の位置合わせ (`PayloadRing::restore`) は、 entry が読めるかを見ずに最大 lsn の行から書き込み位置を、
//! 最小 lsn の行から生きている範囲の先頭を決めた。 最大 lsn の行の entry が読めないと書き込み位置を先頭に戻し、 生きて
//! いる範囲の先頭も先頭 (ring を一周していない) なので 「満杯」 になって、 新しい record を配る分に足せなかった。
//!
//! 電源断の後の形は、 entry の header (長さと lsn) を消して作る (電源断の模擬の `Lost` の像では、 書き出していない page
//! はゼロ)。 電源断の模擬そのものは `tests/power_loss.rs` の sync する DB の種類。

use enchudb_engine::{Engine, ValueType};
use std::os::unix::fs::FileExt;

const RING_HEADER: u64 = 64;

fn write_notes(eng: &Engine, from: u32, to: u32) {
    for i in from..to {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i);
    }
}

/// 配る分の Tie の値 (書いた順)。
fn delivered(eng: &Engine) -> Vec<u64> {
    eng.pending_sync_ops(0)
        .iter()
        .filter_map(|p| match enchudb_oplog::oplog::decode_sync_ops_payload(p)?.op {
            enchudb_oplog::oplog::DecodedOp::Tie { value, .. } => Some(value),
            _ => None,
        })
        .collect()
}

#[test]
fn reopen_with_rows_whose_payload_did_not_reach_disk_keeps_bridging() {
    let path = std::env::temp_dir().join(format!("issue442_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    let p = path.to_str().unwrap();
    {
        let mut eng = Engine::create_with_cell_version(p, 65_536).unwrap();
        eng.define_table("notes", 1_000).unwrap();
        eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
        eng.enable_sync_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, 4 << 20).unwrap();
        write_notes(&eng, 0, 300);
        eng.oplog_sync().unwrap();
        assert_eq!(delivered(&eng), (0..300).collect::<Vec<u64>>(), "bridge されていない (前提)");
    }
    // 電源断で最新の 50 行の payload が届かなかった形 (行は届いている)
    {
        let eng = Engine::open_readonly(p).unwrap();
        let lsn_hid = eng.himo_id("_sync_ops.lsn").unwrap() as u16;
        let at_hid = eng.himo_id("_sync_ops.payload_at").unwrap() as u16;
        let mut rows: Vec<(u64, u64)> = eng
            .entities_with_himo(lsn_hid)
            .into_iter()
            .map(|e| (eng.get_by_id(e, lsn_hid).unwrap(), eng.get_by_id(e, at_hid).unwrap()))
            .collect();
        rows.sort();
        assert_eq!(rows.len(), 300);
        assert_eq!(rows[0].1, 1, "最小 lsn の行の entry が ring の先頭に無い (前提)");
        drop(eng);
        let ring = std::fs::OpenOptions::new().write(true).open(path.join("sync.payload.seg")).unwrap();
        for &(_, handle) in &rows[250..] {
            ring.write_all_at(&[0u8; 8], RING_HEADER + (handle - 1) * 8).unwrap();
        }
        ring.sync_all().unwrap();
    }
    let eng = Engine::open_concurrent_with_oplog(p, 4 << 20).unwrap();
    assert_eq!(delivered(&eng), (0..250).collect::<Vec<u64>>(), "payload の届いていない行が配る分に出た (前提)");
    write_notes(&eng, 300, 400);
    eng.oplog_sync().unwrap();
    while eng.transfer_oplog_to_sync_ops() > 0 {}
    let after = delivered(&eng);
    assert_eq!(after.len(), 350, "開いた後に書いた record が配る分に入らない (ring が一杯に見えて bridge が止まった)");
    assert_eq!(after[..250], (0..250).collect::<Vec<u64>>()[..], "届いていた payload が上書きされた");
    assert_eq!(after[250..], (300..400).collect::<Vec<u64>>()[..]);
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}
