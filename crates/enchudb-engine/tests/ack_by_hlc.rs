//! #149: pull cursor (HLC) に基づく ack — `ack_sync_up_to_hlc` の回帰テスト。
//!
//! relay/gateway 経路には明示 ack が無く、 実運用で「リング満杯 → bridge backpressure
//! → 以後の変更が相手に届かない」が発現した。 pull の since カーソルは「適用済み
//! record の max HLC」からしか前進しない = 到達証明なので、 それを consumed_lsn に
//! 写せば reclaim が回る。 本テストはその写像の境界（中間 / 先端 / 証明なし）を固定する。

use enchudb_engine::{Engine, ValueType};
use std::sync::Arc;
use std::time::Duration;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-ackhlc-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suffix in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".schema"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

#[test]
fn cursor_hlc_ack_advances_watermark_and_reclaims() {
    let path = tmp_path("mid");
    cleanup(&path);
    let mut eng = Engine::create_with_capacity(&path, 1024).unwrap();
    eng.define_table("notes", 8).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    let e = eng.entity_in("notes").unwrap();

    // 30 record を書いて bridge を待つ。
    for v in 1..=30u32 {
        eng.tie_to(e, "notes.note", v);
    }
    eng.oplog_commit();
    let mut waited = 0;
    while eng.pending_sync_ops(0).len() < 30 && waited < 100 {
        std::thread::sleep(Duration::from_millis(100));
        waited += 1;
    }
    let payloads = eng.pending_sync_ops(0);
    assert!(payloads.len() >= 30, "bridge されていない（{} 件）", payloads.len());

    // 各 record の HLC（lsn 昇順で返る）。
    let hlcs: Vec<_> = payloads
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .map(|r| r.hlc)
        .collect();
    assert_eq!(hlcs.len(), payloads.len(), "payload が decode できない");

    // ── 証明なし（最古より古い cursor）: ack しない ─────────────────────────
    let too_old = enchudb_oplog::Hlc { wall: 0, logical: 0, peer: 0 };
    assert_eq!(
        eng.ack_sync_up_to_hlc(8, too_old).unwrap(),
        0,
        "消化の証明が無い cursor で ack してはいけない"
    );
    assert_eq!(eng.sync_watermark(), 0, "peer row を作らない（watermark 不変）");

    // ── 中間 cursor: そこまでの lsn に ack が落ち、 reclaim が回る ───────────
    let mid = hlcs[hlcs.len() / 2];
    let acked = eng.ack_sync_up_to_hlc(7, mid).unwrap();
    assert!(acked > 0, "中間 cursor で ack される");
    assert_eq!(eng.sync_watermark(), acked);
    let before = eng.pending_sync_ops(0).len();
    let purged = eng.reclaim_sync_ops();
    assert!(purged > 0, "中間 ack で古い row が回収される");
    let after = eng.pending_sync_ops(0).len();
    assert!(after < before, "生存 row が減っている（{before} → {after}）");
    assert!(after > 0, "cursor 超えの row は残る（過剰 reclaim しない）");

    // ── 先端 cursor（最新 record の HLC）: bridged 済み先端まで ack ──────────
    let head = *hlcs.last().unwrap();
    let acked2 = eng.ack_sync_up_to_hlc(7, head).unwrap();
    assert_eq!(
        acked2,
        eng.current_sync_lsn(),
        "先頭 row まで消化済みなら bridged 先端まで ack"
    );
    eng.reclaim_sync_ops();

    cleanup(&path);
}
