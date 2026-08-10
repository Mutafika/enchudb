//! #152 回帰: `_sync_ops` 満杯 backpressure が **backlog > ring 容量** でも進行すること。
//!
//! 0.18.2 (#150) の backpressure は cursor を一切進めない retry だったため、 未転送
//! backlog が ring 容量を超えると毎周回「先頭 K 件を挿入 → K+1 件目で満杯 → cursor 据置」
//! を繰り返して**永久に前進しなかった**。 `next_sync_lsn` は挿入のたびに増えるので、
//! 「毎周 K 件配っている」= 正常に見えるのが厄介 (実測 ring 508 / backlog 1281 で
//! 12 周回しても marker は一度も bridge されず)。
//!
//! 本 test は「処理し切った record の終端まで cursor を進める」partial advance が
//! 効いていることを、 **ring 容量を確実に超える backlog の末尾 marker が届くか**で見る。

use enchudb_engine::{Engine, ValueType};
use std::sync::Arc;
use std::time::Duration;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-backlog-drain-{}-{}-{}",
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

/// ring が埋まるまで tie し続ける (1 entity への tie 連打なので user table は消費しない)。
/// pending が 3 回連続で増えなくなったら「満杯」とみなして返す。
fn fill_ring(eng: &Arc<Engine>, e: u64, start: u32) -> (u32, usize) {
    let mut v = start;
    let mut plateau = 0;
    let mut last_pending = 0usize;
    for _ in 0..400 {
        for _ in 0..32 {
            v += 1;
            eng.tie_to(e, "notes.note", v);
        }
        eng.oplog_commit();
        std::thread::sleep(Duration::from_millis(150));
        let p = eng.pending_sync_ops(0).len();
        if p <= last_pending {
            plateau += 1;
            if plateau >= 3 {
                return (v, p);
            }
        } else {
            plateau = 0;
        }
        last_pending = p;
    }
    (v, last_pending)
}

/// payload (oplog record の wire bytes) に marker 値が u32 LE で載っているか。
fn contains_marker(payloads: &[Vec<u8>], marker: u32) -> bool {
    let pat = marker.to_le_bytes();
    payloads.iter().any(|p| p.windows(4).any(|w| w == pat))
}

#[test]
fn backlog_larger_than_ring_drains_instead_of_livelocking() {
    let path = tmp_path("drain");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 1024).unwrap();
    eng.define_table("notes", 8).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();

    let e = eng.entity_in("notes").unwrap();
    let (v, pending_full) = fill_ring(&eng, e, 0);
    assert!(pending_full > 0, "ring に record が入っていない — テスト前提が壊れている");

    // 満杯のまま、 ring 容量を確実に超える backlog を積む (ring ~508 に対し ~1280 件)。
    // ここが本 test の肝: backlog が ring 1 周に収まると 1 回の reclaim で流れ切って
    // しまい、 livelock 領域に入らない (= #150 の test が通っていた理由)。
    let mut vv = v;
    for _ in 0..40 {
        for _ in 0..32 {
            vv += 1;
            eng.tie_to(e, "notes.note", vv);
        }
        eng.oplog_commit();
    }
    assert!(
        (vv - v) as usize > pending_full,
        "backlog ({}) が ring 容量 ({}) を超えていない — テスト前提が壊れている",
        vv - v,
        pending_full,
    );

    let marker = 777_777u32;
    eng.tie_to(e, "notes.note", marker);
    eng.oplog_commit();
    std::thread::sleep(Duration::from_millis(400));

    // ack + reclaim で ring を回し続ければ、 backlog 末尾の marker まで必ず到達する。
    let mut found = false;
    let mut rounds = 0;
    for _ in 0..12 {
        rounds += 1;
        let lsn = eng.current_sync_lsn();
        eng.ack_sync(1, lsn).unwrap();
        eng.reclaim_sync_ops();
        std::thread::sleep(Duration::from_millis(400));
        if contains_marker(&eng.pending_sync_ops(0), marker) {
            found = true;
            break;
        }
    }

    assert!(
        found,
        "backlog 末尾の marker が {} 周回しても bridge されない — 満杯 backpressure が \
         進行不能 (先頭 K 件を再挿入し続ける livelock、 #152)",
        rounds,
    );

    cleanup(&path);
}
