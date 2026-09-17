//! #268: **Commit marker の append が失敗したときに checkpoint を進めない。**
//!
//! 実機 (syncretic) で bridge が 26.4 時間 0 を返し続け、 配布が完全に沈黙した。
//! 警告が捉えた形は `cursor=32 (ring 先頭) / head=256` で、 その 224 byte は
//! **Commit で閉じられていない group** だった (payload 0 の op は Commit だけなので
//! 224 = record 1〜2 本ぶん)。
//!
//! 旧実装は 5 箇所すべてが `let _ = wal.append(Op::Commit)` で、 失敗しても直後の
//! `advance_checkpoint(head)` を無条件に実行していた。 そうなると
//!
//! - group は閉じられないので **bridge は永久に 0** (`out` に載るのは Commit で
//!   閉じた record だけ)
//! - checkpoint はその group を越えてしまうので **recovery からも見えない**
//! - `head == checkpoint` になるので、 周期 fsync の `head > checkpoint` が
//!   false になり **Commit を打ち直すこともしない**
//!
//! という自己修復不能の状態になる。 counter は当時すべて 0 のままだった。
//!
//! ここで固定するのは 「失敗したら checkpoint を据え置く」 という 1 点だけ。
//! 据え置けば `head > checkpoint` が残るので次の tick が Commit を打ち直し、
//! 一過性の要因なら **再起動せずに自力で復帰する** (実機でも 26.4 時間後に
//! 再起動なしで復帰している)。

use enchudb_engine::{Engine, ValueType};

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-268-{}-{}-{}.db",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

/// 収束待ち (最大 5 秒)。 consumer thread が 100ms 周期で動くので、 一発読みは使わない。
fn until(mut f: impl FnMut() -> bool) -> bool {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if f() {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    f()
}

/// sync tables + number himo 1 本の DB を作って concurrent で開く。
fn open_synced(path: &str, oplog_capacity: usize) -> std::sync::Arc<Engine> {
    let _ = std::fs::remove_dir_all(path);
    {
        let mut eng = Engine::create_standalone(path).unwrap();
        eng.define_table("rows", 1_000).unwrap();
        eng.define_himo_in("rows", "val", ValueType::Number, 1_000)
            .unwrap();
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
    }
    let eng = Engine::open_concurrent_with_oplog(path, oplog_capacity).unwrap();
    eng.set_peer_id(1);
    eng
}

#[test]
fn commit_failure_keeps_checkpoint_behind_and_recovers_without_reopen() {
    let path = tmp("commit-fail");
    let eng = open_synced(&path, 4 * 1024 * 1024);
    let hid = eng.himo_id("rows.val").unwrap() as u16;

    // Commit を打てない状態にする (実機では満杯 / 別 fd の head 先行で起きる)。
    // 残数を大きく取るのは、 周期 fsync と手動 sync のどちらが先でも落ちるようにするため。
    eng.oplog().unwrap().fail_next_commits(u32::MAX);

    let e = eng.entity_in("rows").unwrap();
    eng.tie_async_by_id(e, hid, 7);
    eng.flush_writes(); // WAL には載る (append は成功する)

    // 満杯ではない (= fold では回復しない) Commit 失敗は呼び出し側に返る。
    // ここを `Ok(())` で返していたのが 「oplog_sync() は成功したのに配布されない」
    // の正体。 満杯 (`append_dead`) は fold が畳む設計上の経路なので Ok のまま
    // (`wal_full_fold` が固定している)。
    assert!(!eng.oplog().unwrap().append_dead(), "前提が崩れている: WAL が満杯");
    assert!(
        eng.oplog_sync().is_err(),
        "Commit が打てていないのに oplog_sync() が成功を返している",
    );

    assert!(
        until(|| eng.wal_commit_failures() >= 1),
        "Commit の失敗が数えられていない (握り潰されている)",
    );

    // ★ 本体。 閉じられていない group を checkpoint が越えていない。
    assert!(
        eng.oplog_head() > eng.oplog_checkpoint(),
        "Commit に失敗したのに checkpoint が head まで進んでいる \
         (head={}, checkpoint={}) — この group は bridge からも recovery からも \
         見えなくなり、 head == checkpoint なので Commit の打ち直しも起きない",
        eng.oplog_head(),
        eng.oplog_checkpoint(),
    );

    // 停止の 「形」 が観測できる: record は読めているが Commit が無い。
    eng.transfer_oplog_to_sync_ops();
    assert!(
        until(|| {
            eng.transfer_oplog_to_sync_ops();
            eng.bridge_pending_records() >= 1
        }),
        "未 commit の record 数が観測できない (空 scan としか見えない)",
    );
    assert_eq!(
        eng.bridge_last_scan_stop(),
        "reached-head",
        "打ち切りではなく 「Commit が無い」 形のはず",
    );
    assert_eq!(eng.current_sync_lsn(), 0, "閉じていない record が配布されている");

    // 要因が消えれば **再起動せずに** 流れ出す (実機の 「17 秒で +7,875」 の形)。
    eng.oplog().unwrap().fail_next_commits(0);
    assert!(
        until(|| {
            eng.transfer_oplog_to_sync_ops();
            eng.current_sync_lsn() > 0
        }),
        "Commit が打てるようになっても復帰しない (lsn={}, head={}, cp={}, cursor={}, pending={})",
        eng.current_sync_lsn(),
        eng.oplog_head(),
        eng.oplog_checkpoint(),
        eng.sync_ops_bridge_offset(),
        eng.bridge_pending_records(),
    );
    assert!(
        until(|| eng.bridge_pending_records() == 0),
        "復帰後も未 commit 件数が残っている",
    );

    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}

/// #268: Commit が打てないまま書き続けると WAL は満杯になり、 以降の write は
/// **table 本体には入るが sync 経路から落ちる**。 旧実装はこれを warn-once の
/// 1 行でしか出しておらず、 26 時間の停止でもログに 1 行しか残らなかった
/// (報告者が grep で見つけられなかった)。 累計が数えられていることを固定する。
///
/// 併せて **`flush_writes()` が返ってくること**も固定する。 append 失敗時に
/// `wal_append_count` を進め忘れていたため、 満杯の WAL では barrier
/// (`wal_appended >= wal_pushed`) が永久に成立せず、 writer thread が
/// `yield_now` で spin し続けていた。 sleep も mutex 待ちも panic も無いので、
/// thread dump には 「待っている」 とすら映らない (= このテストが無いと
/// 「hang している」 ことしか分からない)。
#[test]
fn records_dropped_from_the_sync_path_are_counted_not_just_warned_once() {
    let path = tmp("dropped");
    // 小さい WAL + Commit 不能 = 畳めない (head != checkpoint) ので必ず埋まり切る。
    let eng = open_synced(&path, 8 * 1024);
    let hid = eng.himo_id("rows.val").unwrap() as u16;
    eng.oplog().unwrap().fail_next_commits(u32::MAX);

    assert!(
        until(|| {
            for _ in 0..32 {
                let e = eng.entity_in("rows").unwrap();
                eng.tie_async_by_id(e, hid, 1);
            }
            eng.flush_writes();
            eng.wal_dropped_records() > 0
        }),
        "WAL 満杯で落ちた record が数えられていない (free={} bytes, dead={})",
        eng.wal_free_bytes(),
        eng.wal_append_dead(),
    );

    // 落ちた record は table 本体には入っている = ローカルだけ見ていると正常に見える。
    assert!(
        eng.entity_count() > 0,
        "前提が崩れている: 本体にも入っていない",
    );

    eng.oplog().unwrap().fail_next_commits(0);
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}
