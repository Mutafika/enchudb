//! **ローカル write が LWW に参加すること** (request17 Phase 1 の完了証明)。
//!
//! # 何が壊れていたか
//!
//! `HlcStore` を更新しているのは `sync.rs` の apply と hydrate だけで、 engine の
//! ローカル write (`tie_to` / `tie_to_by_id`) は `Column` を書くが HLC を記録しなかった。
//!
//! したがって「ローカル write の後に、 それより古い remote record を受ける」と、
//! LWW 比較の基準が存在しないまま素通しで適用され、 **ローカルのより新しい値が
//! 古い値へ巻き戻った**。 reopen も reclaim も要らない — プロセスは起動したまま起きる。
//!
//! # なぜ今まで見つからなかったか
//!
//! 既存の LWW テスト (`tests/v32_two_peer_sync.rs` の `lww_concurrent_write_to_same_cell`
//! など) は `transport.publish()` で record を**直接注入**しており、 engine のローカル
//! write を一度も経由していない。 つまり検証されていたのは「remote apply 同士の LWW」
//! だけで、 **ローカル write が絡む経路がテストスイートに存在しなかった**。
//!
//! # どう直したか (request17 Phase 1 step 4 / 5)
//!
//! ローカル write も remote apply も `set_cell(eid, hid, value, hlc)` 一本を通す。
//! 版数の置き場は v9 DB なら per-cell version column、 pre-v9 DB なら従来の揮発
//! `HlcStore` だが、 **判定の入口が 1 本になったので「記録し忘れ」が構造的に起きない**。
//! この test は pre-v9 DB (= 通常 create) で回っているので、 fallback 側の証明でもある。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport, WireRecord};
use enchudb_engine::ValueType;
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::Hlc;
use enchudb_sync::Syncer;
use std::sync::Arc;
use std::time::Duration;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-lwwgap-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    for s in ["", ".oplog", ".tables", ".crc", ".lock", ".db.lock", ".eidmap", ".vocabmap", ".schema"] {
        let _ = std::fs::remove_file(format!("{}{}", path, s));
    }
}

fn open_engine(path: &str, peer: u32) -> Arc<Engine> {
    let mut e = Engine::create_with_capacity(path, 65_536).unwrap();
    e.define_table("notes", 1000).unwrap();
    e.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    e.enable_sync_tables().unwrap();
    let e = Engine::concurrentize_with_oplog(e, 16 * 1024 * 1024).unwrap();
    e.set_peer_id(peer);
    e
}

/// ローカル write は、 それより古い HLC の remote record に**負けてはいけない**。
#[test]
fn local_write_must_not_be_overwritten_by_older_remote_record() {
    let pb = tmp_path("B");
    cleanup(&pb);

    let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
    let b = open_engine(&pb, 2);

    // ── 1. A の古い record (wall=1000) を B が apply する ──
    // これで B の HlcStore には cell → wall=1000 が載る。
    let shared_eid = enchudb_oplog::make_eid(1, 0);
    let hid = b.himo_id("notes.note").unwrap() as u16;
    transport.publish(
        1,
        vec![WireRecord::unsigned(
            Hlc { wall: 1000, logical: 0, peer: 1 },
            1,
            DecodedOp::Tie { eid: shared_eid, himo_id: hid, value: 111 },
        )],
    );
    let sb = Syncer::new(b.clone(), transport.clone());
    assert_eq!(sb.pull_once(1).applied, 1, "A の record が apply されていない");

    let local_eid = *b.pull("notes.note", 111).first().expect("A の行が B に来ていない") as u64;

    // ── 2. B がローカルで上書きする ──
    // engine が採番する HLC は現在時刻ベース (wall は ms 単位で ~1.7e12) なので、
    // 下で送る wall=2000 の record より **圧倒的に新しい**。
    b.tie_to(local_eid, "notes.note", 222);
    b.oplog_commit();
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(b.get(local_eid, "notes.note"), Some(222), "ローカル write が入っていない");

    // ── 3. A から「1 の record より新しいが、 2 のローカル write より遥かに古い」record ──
    transport.publish(
        1,
        vec![WireRecord::unsigned(
            Hlc { wall: 2000, logical: 0, peer: 1 },
            1,
            DecodedOp::Tie { eid: shared_eid, himo_id: hid, value: 333 },
        )],
    );
    let out = sb.pull_once(1);

    // ── 検証: ローカル write のほうが HLC 的に新しいので skip されるべき ──
    assert_eq!(
        out.applied, 0,
        "ローカル write より古い record が適用された (received={}, applied={})",
        out.received, out.applied
    );
    assert_eq!(
        b.get(local_eid, "notes.note"),
        Some(222),
        "ローカルの新しい値 222 が、 より古い remote record 333 に巻き戻った \
         — ローカル write が LWW に参加していない"
    );

    cleanup(&pb);
}
