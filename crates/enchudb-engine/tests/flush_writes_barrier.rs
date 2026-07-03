//! issue5 regression: `flush_writes()` が EntityCreated 込み全 op 完了まで待つ
//! 真の barrier として機能すること。
//!
//! 旧 bug: `entity()` で `push_count++` を呼んでなかったため、 1 iter で
//! 1 EntityCreated + 2 Tie を push しても push_count は += 2。 一方 apply_count
//! は EntityCreated でも += 1 で計上していたので、 `applied >= pushed` が
//! Ties 未 apply の状態で成立 → 早期 return → live query が直前の write を
//! 見落とす。
//!
//! 0.9.0 追補: `flush_writes()` は WAL record queue (`oplog_record_queue`) の
//! barrier でもあること。 op は queue 先行・record 後追い (#77-H4) なので、
//! apply_count だけ待つ旧実装には「op 適用済み・record は queue 内」の窓があり、
//! 直後の `oplog_sync()` が record の入っていない WAL に Commit + fsync して
//! いた (= sync 転送漏れ / crash durability 破れ。 sync lib テストの flaky の
//! 正体)。

use enchudb_engine::{Engine, HimoType};
use std::sync::Arc;

#[test]
fn flush_writes_waits_for_all_ties_including_entity_created_path() {
    let path = "/tmp/test_flush_barrier.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{path}.oplog"));
    let _ = std::fs::remove_file(format!("{path}.lock"));

    // queue_cap を小さめに絞って consumer の drain が writer に追いつかない
    // 状況を作る。
    let eng: Arc<Engine> = Engine::create_concurrent_with_oplog_queue_cap(
        path,
        16 * 1024 * 1024,
        1024,
    ).expect("create");

    {
        let eng_mut = unsafe { &mut *(Arc::as_ptr(&eng) as *mut Engine) };
        eng_mut.define_himo("marker", HimoType::Tag, 100);
        eng_mut.define_himo("value", HimoType::Tag, 1_000_000);
    }
    let marker_hid = eng.himo_id("marker").unwrap() as u16;
    let value_hid = eng.himo_id("value").unwrap() as u16;
    let marker_vid = 1u32;

    let n_writers = 4;
    let per = 5_000u32;
    let total_expected = (n_writers * per) as usize;

    let handles: Vec<_> = (0..n_writers).map(|_| {
        let eng = eng.clone();
        std::thread::spawn(move || {
            for i in 0..per {
                let e = eng.entity();
                eng.tie_async_by_id(e, marker_hid, marker_vid);
                eng.tie_async_by_id(e, value_hid, i);
            }
        })
    }).collect();
    for h in handles { h.join().expect("writer panicked"); }

    // flush_writes が真の barrier なら、 直後の query で全 push 結果が見える。
    eng.flush_writes();
    let live = eng.query_by_id(&[(marker_hid, marker_vid)]);
    assert_eq!(
        live.len(),
        total_expected,
        "flush_writes barrier broken: expected {total_expected} entities visible, got {}",
        live.len()
    );
}

/// 0.9.0 regression: `tie_async` → `oplog_sync` の直後、 その Tie が `_sync_ops`
/// に bridge 済みであること (= sync publish から見えること)。
///
/// 旧 bug は 2 段:
/// 1. flush_writes が apply_count しか待たず、 consumer の drain 境界が
///    「op queue push → WAL record push」の間に挟まると oplog_sync の Commit が
///    record を置き去りにした (WAL queue barrier 欠落)。
/// 2. oplog_sync (caller thread) は checkpoint を進めるが transfer をしないため、
///    consumer tick の try_reset が head==checkpoint の ring を bridge 前に畳み、
///    record が _sync_ops に乗る前に消えた。
/// どちらも sync lib テスト (publish_since 系) の稀な取りこぼしとして観測された。
/// 窓は ns〜ms 級なので、 反復 + 並行 noise writer で踏み抜く。
#[test]
fn oplog_sync_bridges_all_records_pushed_before_it() {
    let path = format!("/tmp/test_walq_barrier_{}.db", std::process::id());
    for suffix in ["", ".oplog", ".tables", ".crc", ".lock"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
    }

    {
        let mut eng = Engine::create_standalone(&path).expect("create");
        eng.define_table("rows", 100_000).unwrap();
        eng.define_himo_in("rows", "val", HimoType::Number, 1_000_000).unwrap();
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
    }
    let eng: Arc<Engine> =
        Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).expect("open");
    eng.set_peer_id(1);
    let val_hid = eng.himo_id("rows.val").unwrap() as u16;

    // noise thread は入れない: bridge された record は peer ack まで reclaim
    // されないため、 高頻度 writer を並走させると `_sync_ops` の row 空間を
    // 食い潰してテスト自体が別の理由で落ちる。 race の摂動は suite 並列実行の
    // scheduling jitter に任せる (元の flaky も sync lib テストが単体では
    // 常に green、 workspace 並列でのみ落ちる形で観測された)。
    // 100 iter ≒ 9 秒 (oplog_sync = 実 fsync)。 これ以上は CI 時間との交換で薄い
    for i in 0..100u32 {
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
        assert!(
            found,
            "Tie{{value:{i}}} pushed before oplog_sync is not in _sync_ops \
             (WAL queue barrier or transfer-before-reset broken)"
        );
    }
    for suffix in ["", ".oplog", ".tables", ".crc", ".lock"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
    }
}
