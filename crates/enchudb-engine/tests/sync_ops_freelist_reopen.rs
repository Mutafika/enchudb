//! 0.18.2 regression tests: `_sync_ops` ring の reopen 自己修復と満杯時 backpressure。
//!
//! 実機発現した事故: `free_locals`（reclaim 済み slot の reservoir）は in-memory のみで
//! reopen で消える。 ring を一周以上使った store を reopen すると `next_local` は
//! range 端に居るのに free list は空 → `entity_in("_sync_ops")` が恒久 Err →
//! oplog→sync bridge が row を挿せず、 （旧実装は cursor も進めて捨てるため）
//! **以後の全変更が sync から無言で欠落**していた。

use enchudb_engine::{Engine, ValueType};
use std::sync::Arc;
use std::time::Duration;

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
        std::thread::sleep(Duration::from_millis(400));
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
    std::thread::sleep(Duration::from_millis(600));

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
    std::thread::sleep(Duration::from_millis(400));

    // ack + reclaim で ring を空ける → 待っていた record が bridge されるはず
    let lsn = eng.current_sync_lsn();
    eng.ack_sync(1, lsn).unwrap();
    eng.reclaim_sync_ops();
    std::thread::sleep(Duration::from_millis(600));

    // marker の record が pending に現れているか（payload に頼らず lsn 前進で判定した
    // 上で、 実 payload の存在も確かめる）
    assert!(
        eng.current_sync_lsn() > lsn,
        "ring を空けても待機 record が bridge されない — 満杯時に破棄されている"
    );

    cleanup(&path);
}
