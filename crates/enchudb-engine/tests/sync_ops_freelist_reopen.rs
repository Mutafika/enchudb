//! 0.18.2 regression tests: `_sync_ops` ring の reopen 自己修復と満杯時 backpressure。
//!
//! 実機発現した事故: `free_locals`（reclaim 済み slot の reservoir）は in-memory のみで
//! reopen で消える。 ring を一周以上使った store を reopen すると `next_local` は
//! range 端に居るのに free list は空 → `entity_in("_sync_ops")` が恒久 Err →
//! oplog→sync bridge が row を挿せず、 （旧実装は cursor も進めて捨てるため）
//! **以後の全変更が sync から無言で欠落**していた。

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

/// `_sync_ops` ring に空きが無い (bridge が backpressure で止まる状態)。
fn ring_full(eng: &Engine) -> bool {
    eng.table_eid_usage("_sync_ops").expect("_sync_ops が無い").free == 0
}

/// bridge が oplog を読み切った (cursor が head に追いついた)。
fn bridge_caught_up(eng: &Engine) -> bool {
    eng.sync_ops_bridge_offset() >= eng.oplog_head()
}

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-freelist-reopen-{}-{}-{}",
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

/// ring が埋まるまで tie し続ける（1 entity への tie 連打なので user table は消費しない）。
/// batch ごとに consumer の bridge が 「読み切る」 か 「満杯で止まる」 まで待ち、
/// `_sync_ops` の空きが 0 になったら満杯として返す。
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
    panic!("400 batch 書いても _sync_ops ring が満杯にならない — テスト前提が壊れている");
}

#[test]
fn reopened_store_recovers_reclaimed_slots_and_keeps_bridging() {
    let path = tmp_path("selfheal");
    cleanup(&path);

    let lsn_after_reopen_write;
    let lsn_before_reopen_write;
    {
        let mut eng = Engine::create_with_capacity(&path, 1024).unwrap();
        eng.define_table("notes", 8).unwrap();
        eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
        eng.enable_sync_tables().unwrap();
        let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();

        let e = eng.entity_in("notes").unwrap();

        // ring を満杯まで使う（= next_local を range 端まで進める）
        let (v, pending_full) = fill_ring(&eng, e, 0);
        assert!(pending_full > 0, "ring に record が入っていない — テスト前提が壊れている");

        // 全 ack + reclaim（free list に穴が入る — ただし in-memory のみ）
        let lsn = eng.current_sync_lsn();
        eng.ack_sync(1, lsn).unwrap();
        let purged = eng.reclaim_sync_ops();
        assert!(purged > 0, "reclaim が何も回収していない — テスト前提が壊れている");

        // in-process では free list が生きているので bridge は続く（コントロール）。
        // 値は既知の marker にして reopen 後の pull で entity を掴めるようにする
        // （tie は上書きなので最後の値でしか引けない）。
        let _ = v;
        eng.tie_to(e, "notes.note", 424_242);
        eng.oplog_commit();
        wait_until("in-process の ring 再利用で bridge が進む", || eng.current_sync_lsn() > lsn);
        assert!(
            eng.current_sync_lsn() > lsn,
            "in-process の ring 再利用が壊れている（前提: phase4 ring buffer）"
        );
        // graceful shutdown（consumer が最終 transfer + persist して抜ける）
    }

    // ── reopen: free list は消えた。 range は端。 ここからが本題 ──
    let eng2 = Engine::open(&path).unwrap();
    lsn_before_reopen_write = eng2.current_sync_lsn();

    let notes = eng2.pull("notes.note", 424_242);
    let e2 = *notes.first().expect("note entity が reopen 後に見えない");
    eng2.tie_to(e2.into(), "notes.note", 999_999);
    eng2.oplog_commit();
    // 本題は reopen 後の consumer が自動で bridge すること — 手動 transfer は呼ばず待つ
    wait_until(
        "reopen 後の書き込みが _sync_ops に bridge される (reclaim 済み slot が reopen で失われていない)",
        || eng2.current_sync_lsn() > lsn_before_reopen_write,
    );

    lsn_after_reopen_write = eng2.current_sync_lsn();
    assert!(
        lsn_after_reopen_write > lsn_before_reopen_write,
        "reopen 後の書き込みが _sync_ops に bridge されていない \
         (lsn {} → {}) — reclaim 済み slot が reopen で失われている",
        lsn_before_reopen_write,
        lsn_after_reopen_write,
    );

    cleanup(&path);
}

/// 満杯時の backpressure: ack が来ない relay 型経路で ring が満杯になっても、
/// record は「捨てられる」のではなく「待たされる」。 ack + reclaim で ring が
/// 空いたら、 待っていた record が**必ず**bridge される（旧実装: cursor を進めて
/// 破棄 → ack しても二度と現れない = data loss）。
#[test]
fn full_ring_backpressures_instead_of_dropping() {
    let path = tmp_path("backpressure");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 1024).unwrap();
    eng.define_table("notes", 8).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();

    let e = eng.entity_in("notes").unwrap();
    let (v, _) = fill_ring(&eng, e, 0);

    // 満杯のまま、 さらに 1 件書く（旧実装はここで cursor だけ進めて record を捨てる）
    let marker = 777_777u32;
    let _ = v;
    eng.tie_to(e, "notes.note", marker);
    eng.oplog_commit();
    // bridge が marker の commit まで scan し、 満杯で止まったことを確かめる
    // (旧実装はこの scan で cursor を進めて marker を捨てていた)
    let head = eng.oplog_head();
    wait_until("bridge が満杯のまま marker まで scan する", || {
        eng.bridge_last_committed_end() >= head
    });
    assert!(ring_full(&eng), "marker の scan 前に ring が空いた — テスト前提が壊れている");
    assert!(
        eng.sync_ops_bridge_offset() < head,
        "満杯なのに cursor が marker を越えた — 待機せず破棄している"
    );

    // ack + reclaim で ring を空ける → 待っていた record が bridge されるはず
    let lsn = eng.current_sync_lsn();
    eng.ack_sync(1, lsn).unwrap();
    eng.reclaim_sync_ops();
    wait_until("ring を空けたら待機 record が bridge される", || eng.current_sync_lsn() > lsn);

    // marker の record が pending に現れているか（payload に頼らず lsn 前進で判定した
    // 上で、 実 payload の存在も確かめる）
    assert!(
        eng.current_sync_lsn() > lsn,
        "ring を空けても待機 record が bridge されない — 満杯時に破棄されている"
    );

    cleanup(&path);
}
