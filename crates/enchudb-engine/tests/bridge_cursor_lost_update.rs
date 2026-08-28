//! **`oplog_sync()` が返ったら、 その前に push された record は `_sync_ops` に
//! 乗っている。** — fold (ring reset) と bridge の check-then-act を塞ぐ回帰テスト。
//!
//! consumer tick は
//!
//! ```ignore
//! if engine.wal_fold_safe() && wal.try_reset() { .. }
//! ```
//!
//! と、 「畳んでよいか」 の判定 (lock 外) と fold 本体 (`append_lock` 保持) を
//! 2 操作に分けていた。 その間に caller thread の `oplog_sync` が
//!
//! ```ignore
//! append(Commit) → fsync → body_msync → advance_checkpoint(head)  // head == checkpoint 成立
//! → transfer_oplog_to_sync_ops()                                  // ← ここに来る前に
//! ```
//!
//! と進むと、 consumer は 「判定時は bridge 済みだった」 という stale な根拠で
//! `try_reset` を成功させてしまう。 fold は head/checkpoint を HEADER_SIZE に巻き戻し、
//! `reset_sync_ops_offset()` で bridge cursor も戻すので、 直後の caller 側 transfer は
//! 空振りする。 **`oplog_sync()` は `Ok(())` を返すのに record は `_sync_ops` に無い**
//! (= その変更は sync から無言で欠落、 ring は畳まれているので回復手段も無い)。
//!
//! 既存の `flush_writes_barrier::oplog_sync_bridges_all_records_pushed_before_it` が
//! 同じ不変条件を見ているが、 窓が ns〜ms 級なので単体実行ではほぼ踏めず、 workspace
//! 並列時にだけ ~1/5 で落ちる形で残っていた。 ここでは spinner thread で scheduling
//! 圧を作り、 単体でも踏み抜けるようにする。

use enchudb_engine::{Engine, ValueType};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const ITERS: u32 = 300;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-fold-toctou-{}-{}-{}.db",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn fresh(path: &str) {
    for suffix in ["", ".oplog", ".tables", ".crc", ".lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
    }
}

#[test]
fn oplog_sync_bridges_every_record_under_scheduling_pressure() {
    let path = tmp_path("barrier");
    fresh(&path);

    {
        let mut eng = Engine::create_standalone(&path).expect("create");
        eng.define_table("rows", 100_000).unwrap();
        eng.define_himo_in("rows", "val", ValueType::Number, 1_000_000).unwrap();
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
    }

    let eng: Arc<Engine> =
        Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).expect("open");
    eng.set_peer_id(1);
    let val_hid = eng.himo_id("rows.val").unwrap() as u16;

    // fold 判定 (lock 外) と fold 本体の間で consumer thread を preempt させるための
    // scheduling 圧。 window は ns〜ms 級なので、 runnable thread を core 数より多く
    // 走らせて preemption 確率を上げる。 write を並走させないのは、 bridge された row が
    // peer ack まで reclaim されず `_sync_ops` の row 空間を食い潰して、 テストが別の
    // 理由 (backpressure) で落ちるのを避けるため。
    let stop = Arc::new(AtomicBool::new(false));
    let spinners: Vec<_> = (0..(std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        * 2))
        .map(|_| {
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut x: u64 = 0;
                while !stop.load(Ordering::Relaxed) {
                    x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
                    if x == u64::MAX {
                        std::thread::yield_now();
                    }
                }
                x
            })
        })
        .collect();

    #[derive(Debug)]
    #[allow(dead_code)]
    struct Miss {
        value: u32,
        /// `Some(ms)` = 後から現れた (= barrier の待ち不足)、
        /// `None` = 500ms 待っても現れない (= record が落ちている)
        appeared_after_ms: Option<u32>,
        rows_in_sync_ops: usize,
        oplog_head: u64,
        oplog_checkpoint: u64,
        durable_lsn: u64,
        fold_race_saves: u64,
    }

    let mut missing: Vec<Miss> = Vec::new();
    for i in 0..ITERS {
        let e = eng.entity_in("rows").unwrap();
        eng.tie_async_by_id(e, val_hid, i);
        eng.oplog_sync().expect("oplog_sync");

        let found = eng.pending_sync_ops(0).iter().any(|payload| {
            matches!(
                enchudb_oplog::oplog::decode_sync_ops_payload(payload),
                Some(rec) if matches!(
                    rec.op,
                    enchudb_oplog::oplog::DecodedOp::Tie { value, himo_id, .. }
                        if value == i && himo_id == val_hid
                )
            )
        });
        if !found {
            // **永久に消えた** のか **まだ見えていないだけ** なのかを切り分ける。
            // `oplog_sync()` が返った時点の契約は 「乗っている」 なので、 後から
            // 現れる場合も契約違反だが、 原因の場所が全く違う (前者 = bridge/fold で
            // record が落ちた、 後者 = barrier の待ちが足りない)。
            let mut appeared_after_ms = None;
            for waited in 1..=50u32 {
                std::thread::sleep(std::time::Duration::from_millis(10));
                let now = eng.pending_sync_ops(0).iter().any(|payload| {
                    matches!(
                        enchudb_oplog::oplog::decode_sync_ops_payload(payload),
                        Some(rec) if matches!(
                            rec.op,
                            enchudb_oplog::oplog::DecodedOp::Tie { value, himo_id, .. }
                                if value == i && himo_id == val_hid
                        )
                    )
                });
                if now {
                    appeared_after_ms = Some(waited * 10);
                    break;
                }
            }
            let st = eng.stats();
            missing.push(Miss {
                value: i,
                appeared_after_ms,
                rows_in_sync_ops: eng.pending_sync_ops(0).len(),
                oplog_head: st.oplog_head,
                oplog_checkpoint: st.oplog_checkpoint,
                durable_lsn: st.durable_lsn,
                fold_race_saves: eng.fold_race_saves(),
            });
        }
    }

    stop.store(true, Ordering::Relaxed);
    for s in spinners {
        let _ = s.join();
    }
    fresh(&path);

    // 踏んだ窓の回数は毎回出す (0 なら「この race は起きていない」の証拠になる)。
    eprintln!(
        "[toctou] iters={ITERS} misses={} fold_race_saves={} cursor_repairs={}",
        missing.len(),
        eng.fold_race_saves(),
        eng.sync_ops_cursor_repairs(),
    );
    assert!(
        missing.is_empty(),
        "oplog_sync() returned Ok but {} / {ITERS} record(s) were not in _sync_ops. \
         appeared_after_ms=Some(..) なら barrier の待ち不足、 None なら record 消失:\n{:#?}",
        missing.len(),
        missing,
    );
}
