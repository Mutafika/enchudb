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
use std::time::{Duration, Instant};

/// 条件が真になるまで 5ms 間隔で待つ (上限 10 秒)。 固定 sleep だと遅い CI で
/// consumer の自動転送 (100ms tick) が間に合わず落ちるので、 実際の条件を見る (#278 / #336)。
fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "timeout (10s): {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// `_sync_ops` ring が満杯 (bridge が backpressure で止まる状態)。
///
/// 「今の枠に空きが無い」 だけでは満杯でない。 `entity_in` は枠を使い切ると、 どの table にも割り当てていない
/// eid 空間から枠を足す (v10 Phase 3) — この DB では 508 → 953。 足す直前の一瞬 (実測 34 µs) は空きが 0 に
/// 見えるので、 それを満杯と読むと、 その後で枠が伸びて bridge は止まらない (#364: CI で時々落ちた)。
/// 足せる eid 空間が残っていないことを**先に**見る: 残りは減るだけなので、 0 を見た後は枠が伸びない。
fn ring_full(eng: &Engine) -> bool {
    eng.remaining_eid_capacity() == 0
        && eng.table_eid_usage("_sync_ops").expect("_sync_ops が無い").free == 0
}

/// 失敗した時に出す ring の状態 (枠が伸びたのか、 行が減ったのかを 1 回の失敗で決める)。
fn ring_state(eng: &Engine) -> String {
    format!(
        "usage={:?} extents={:?} remaining_eid_capacity={}",
        eng.table_eid_usage("_sync_ops"),
        eng.table_eid_extents("_sync_ops"),
        eng.remaining_eid_capacity(),
    )
}

/// bridge が oplog を読み切った (cursor が head に追いついた)。
fn bridge_caught_up(eng: &Engine) -> bool {
    eng.sync_ops_bridge_offset() >= eng.oplog_head()
}

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
    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    for suffix in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap", ".schema"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

/// ring が埋まるまで tie し続ける (1 entity への tie 連打なので user table は消費しない)。
/// batch ごとに consumer の bridge が 「読み切る」 か 「満杯で止まる」 まで待ち、
/// `_sync_ops` がこれ以上伸びず空きも 0 になったら ([`ring_full`]) 満杯として返す。
/// 旧実装は 150ms sleep 後の pending の頭打ちで満杯と推定していたが、 遅い CI では
/// bridge が遅れただけの頭打ちを満杯と誤認する (#336)。
fn fill_ring(eng: &Arc<Engine>, e: u64, start: u32) -> (u32, usize) {
    let mut v = start;
    for _ in 0..400 {
        for _ in 0..32 {
            v += 1;
            eng.tie_to(e, "notes.note", v);
        }
        eng.oplog_commit();
        wait_until("bridge が batch を読み切るか ring が満杯になる", || {
            ring_full(eng) || bridge_caught_up(eng)
        });
        if ring_full(eng) {
            return (v, eng.pending_sync_ops(0).len());
        }
    }
    panic!(
        "400 batch 書いても _sync_ops ring が満杯にならない — テスト前提が壊れている ({})",
        ring_state(eng)
    );
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

    // 満杯のまま、 ring 容量を確実に超える backlog を積む (ring 953 = 最初の枠 508 + 足した 445 に対し 1280 件)。
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
    // bridge が満杯のまま backlog 末尾 (marker の commit) まで scan したのを待つ
    let head = eng.oplog_head();
    wait_until("bridge が満杯のまま backlog 末尾まで scan する", || {
        eng.bridge_last_committed_end() >= head
    });

    // ack + reclaim で ring を回し続ければ、 backlog 末尾の marker まで必ず到達する。
    let mut found = false;
    let mut rounds = 0;
    for _ in 0..12 {
        rounds += 1;
        let lsn = eng.current_sync_lsn();
        eng.ack_sync(1, lsn).unwrap();
        eng.reclaim_sync_ops();
        // 空いた ring を consumer が埋め直す (= 満杯に戻る) か、 backlog を読み切るまで待つ
        wait_until("reclaim 後に bridge が ring を埋め直すか読み切る", || {
            ring_full(&eng)
                || bridge_caught_up(&eng)
                || contains_marker(&eng.pending_sync_ops(0), marker)
        });
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
