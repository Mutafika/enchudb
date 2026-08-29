//! #149: pull cursor (HLC) に基づく ack — `ack_sync_up_to_hlc` の回帰テスト。
//!
//! relay/gateway 経路には明示 ack が無く、 実運用で「リング満杯 → bridge backpressure
//! → 以後の変更が相手に届かない」が発現した。 pull の since カーソルは「適用済み
//! record の max HLC」からしか前進しない = 到達証明なので、 それを consumed_lsn に
//! 写せば reclaim が回る。 本テストはその写像の境界（中間 / 先端 / 証明なし）を固定する。

use enchudb_engine::{Engine, ValueType};
use std::sync::atomic::{AtomicBool, Ordering};
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
    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    for suffix in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap", ".schema"] {
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
    eng.set_peer_id(42); // self（watermark の self 除外を検証するのに必要）
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

    // ── 先端 cursor（最新 record の HLC）: 生存 row の最大 lsn まで ack ──────
    // レビュー (#158) 前は「先頭 row まで消化済みなら `current_sync_lsn` まで」だったが、
    // それは snapshot 後に bridge された未 pull record まで ack する経路になる（下の
    // race テスト参照）。 ack は必ず**実在を確認した生存 row の lsn**で止まる。
    let head = *hlcs.last().unwrap();
    let acked2 = eng.ack_sync_up_to_hlc(7, head).unwrap();
    assert!(acked2 > 0, "先端 cursor で ack される");
    let at_ack = eng.pending_sync_ops(acked2 - 1);
    let acked_hlc = at_ack
        .first()
        .and_then(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .map(|r| r.hlc);
    assert!(
        acked_hlc.is_some_and(|h| h <= head),
        "ack した lsn の record は cursor 以下でなければならない (acked={acked2})"
    );
    eng.reclaim_sync_ops();

    // ── 自分自身の peer row は watermark を固定しない ────────────────────────
    // （過去の self-ack 経路の残骸などで self row が古い lsn を持っていても、
    //  他 peer の消化が進めば reclaim は回る — 自著 record を「持っている」のは自明）
    let self_peer = eng.peer_id();
    assert_eq!(self_peer, 42);
    eng.ack_sync(self_peer, 1).unwrap(); // 古い self row を捏造
    assert_eq!(
        eng.sync_watermark(),
        acked2,
        "self row の古い consumed_lsn が watermark を引き下げてはいけない"
    );

    cleanup(&path);
}

/// レビュー (#158) 指摘の race の回帰。
///
/// 生存 row の snapshot を取った後に bridge が `_sync_ops` へ append すると、
/// `current_sync_lsn()` (= `next_sync_lsn - 1`) はその分を含んだ先端を返す。 先端まで
/// ack すると **cursor より新しい = まだ pull されていない record** が「消化済み」と
/// 記録され、 `reclaim_sync_ops` (`lsn < watermark` を delete) が peer に届く前に
/// 回収してしまう (失うと再著者でしか復旧しない)。
///
/// 成立条件は「相手が追いついている状態で ack した最中に append が入る」 = 追いついた
/// 直後に burst が始まる瞬間。 単一スレッドでは `current_sync_lsn > max(生存 row lsn)`
/// を作れない (reclaim は古い順にしか落とさない) ので、 並行 bridge 下で叩いて検証する。
#[test]
fn ack_never_covers_records_newer_than_cursor_under_concurrent_bridge() {
    let path = tmp_path("race");
    cleanup(&path);
    let mut eng = Engine::create_with_capacity(&path, 65536).unwrap();
    eng.define_table("notes", 8).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(42);
    let e = eng.entity_in("notes").unwrap();

    // 「peer が pull 済み」の範囲。 snapshot 走査が長いほど窓が広がるので多めに積む。
    for v in 1..=5000u32 {
        eng.tie_to(e, "notes.note", v);
    }
    eng.oplog_commit();
    let mut waited = 0;
    while eng.pending_sync_ops(0).len() < 5000 && waited < 200 {
        std::thread::sleep(Duration::from_millis(100));
        waited += 1;
    }
    let payloads = eng.pending_sync_ops(0);
    assert!(payloads.len() >= 5000, "bridge されていない ({} 件)", payloads.len());

    // bridge を高頻度で叩くスレッド。 engine の consumer は 1ms 周期 + 数十 ms 遅れで
    // まとめて流すため、 ack の snapshot 走査中に append が入る窓をほぼ踏めない。
    // `transfer_oplog_to_sync_ops` を直接回して窓に当てる (bridge 経路自体は同じ)。
    let stop = Arc::new(AtomicBool::new(false));
    let bridge = {
        let eng = eng.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                eng.transfer_oplog_to_sync_ops();
                std::thread::yield_now();
            }
        })
    };

    // 「追いついた直後に burst が始まる瞬間」を再現する。 毎周回:
    //   1. その時点の生存 row 先端を cursor にする (= peer は追いついている → i==0 経路)
    //   2. 直後に新 record を WAL へ積む (bridge は 1ms 周期の非同期 consumer なので、
    //      ack の snapshot 時点ではまだ `_sync_ops` に入っていない)
    //   3. ack を叩く — snapshot 走査中に bridge が完了すると `next_sync_lsn` が進み、
    //      先端 ack は **未 pull record を巻き込む**
    let mut checked = 0;
    let mut v = 10_000u32;
    for _ in 0..40 {
        let live = eng.pending_sync_ops(0);
        let Some(cur) = live
            .iter()
            .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
            .map(|r| r.hlc)
            .max()
        else {
            continue;
        };

        // burst 開始 (数件まとめて積むと bridge の完了位相が分散して窓に当たりやすい)
        for _ in 0..4 {
            eng.tie_to(e, "notes.note", v);
            v += 1;
        }
        eng.oplog_commit();

        let acked = eng.ack_sync_up_to_hlc(7, cur).unwrap();
        if acked == 0 {
            continue;
        }
        // `pending_sync_ops(acked - 1)` は lsn > acked-1 の生存 row を lsn 昇順で返すので、
        // その先頭が ack 対象 record (ここでは reclaim しないので生存している)。
        if let Some(first) = eng.pending_sync_ops(acked - 1).first() {
            if let Some(rec) = enchudb_oplog::oplog::decode_sync_ops_payload(first) {
                assert!(
                    rec.hlc <= cur,
                    "cursor より新しい (= 未 pull) record まで ack した: acked={acked}"
                );
                checked += 1;
            }
        }
    }
    stop.store(true, Ordering::Relaxed);
    bridge.join().unwrap();
    assert!(checked > 0, "検証が一度も走っていない (ack が常に 0 だった)");

    cleanup(&path);
}
