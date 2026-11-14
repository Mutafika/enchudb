//! #870: `tables` の sidecar を書けない間 (空き不足など)、 消した行が sidecar の `next_local` より先にあれば、 consumer の周期も
//! 空け作りも checkpoint を進めない。
//!
//! 前は consumer の周期が今まで通り進めた (#857 で止めたのは `oplog_sync` だけ)。 そこで落ちると、 消した行の Tie も Delete も
//! checkpoint より前で再生されず、 開いた `next_local` が消した行の手前に戻って、 sync しない DB は同じ eid をもう一度払い出した。
//! 止めた間は WAL を畳めないので、 満杯になれば書き込みは断る (#553 / #388。 黙って eid を使い回すより断る)。 書ければ次の周期で進む。
//!
//! 書けない状態は、 `tables` の tmp の置き場 (`{db}/tables.tmp`) に directory を置いて作る (`tables` は tmp を作って rename する。
//! 他の書き出し = WAL / 本体 / manifest は書ける)。

#![cfg(unix)]

use enchudb_engine::{Engine, FaultKind, TieRejected, ValueType};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn scratch(tag: &str) -> Scratch {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let p = std::env::temp_dir().join(format!("enchudb-issue870-{tag}-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&p).unwrap();
    Scratch(p)
}

/// `tables` を書けなくする / 書けるように戻す。
fn block_tables(db: &Path, on: bool) {
    let tmp = db.join("tables.tmp");
    if on {
        std::fs::create_dir(&tmp).unwrap();
    } else {
        std::fs::remove_dir(&tmp).unwrap();
    }
}

/// sync しない DB、 表 `t` (列 a)。 consumer は動いている。
fn open(path: &Path, wal: usize) -> (Arc<Engine>, u16) {
    let mut eng = Engine::create_growable_with_capacity(path.to_str().unwrap(), 1 << 16).unwrap();
    eng.define_table("t", 1_000).unwrap();
    eng.define_himo_in("t", "a", ValueType::Number, 0).unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, wal).unwrap();
    let a = eng.himo_id("t.a").unwrap() as u16;
    (eng, a)
}

/// consumer の周期が 1 回まわる (Commit を打って書き出す) のを状態で待つ: 書き出した LSN が、 呼んだ時 (`oplog_sync` の直後) より進む。
fn wait_one_tick(eng: &Engine) {
    let before = eng.stats().durable_lsn;
    let until = Instant::now() + Duration::from_secs(20);
    while eng.stats().durable_lsn <= before {
        assert!(Instant::now() < until, "前提: consumer の周期がまわらない");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// 開いた直後の engine の consumer が 1 回まわるのを待つ。 書き出す物が無ければ (開く時に checkpoint を進めた) 周期は何もしないので、
/// その時は起きた回数で待つ。
fn wait_one_tick_or_idle(eng: &Engine) {
    let wal = eng.oplog().unwrap().clone();
    if wal.checkpoint() == wal.written_head() {
        let w = eng.consumer_wakeups();
        let until = Instant::now() + Duration::from_secs(20);
        while eng.consumer_wakeups() < w + 2 {
            assert!(Instant::now() < until, "前提: consumer がまわらない");
            std::thread::sleep(Duration::from_millis(5));
        }
    } else {
        wait_one_tick(eng);
    }
}

/// 落ちる (閉じる) 瞬間
#[derive(Clone, Copy)]
enum At {
    /// consumer の周期が `tables` を書けずに終わった後
    Tick,
    /// `tables` を書けないまま engine を閉じた後
    Close,
}

/// `tables` を書けない間に行を足して消し、 `at` の瞬間の像 (書き出した物は全部ある、 `tables` は消す前の物) を `image` に写す。
/// 戻り値は消した行の eid。
fn image_with_a_deleted_row(s: &Scratch, at: At, image: &Path) -> u64 {
    let db = s.0.join("db");
    let (eng, a) = open(&db, 4 << 20);
    // 行を足して消すまで consumer を止める (間に `tables` を書かれると、 消した行が sidecar より先にならない)
    let hold = eng.hold_consumer();
    let x = eng.entity_in("t").unwrap();
    eng.tie_to_by_id(x, a, 7u32);
    block_tables(&db, true);
    eng.delete(x);
    eng.oplog_sync().unwrap();
    drop(hold);
    match at {
        At::Tick => {
            // consumer の周期は `tables` を書けずに終わる (前はここで checkpoint を進めた)
            wait_one_tick(&eng);
            assert_eq!(eng.fault_count(FaultKind::DiskSpace), 0, "前提: 本体の書き出しは成功している (書けないのは `tables` だけ)");
            copy(&db, image);
            // 書けるように戻すと、 次の周期で checkpoint が進む (止まり続けない)
            block_tables(&db, false);
            let wal = eng.oplog().unwrap().clone();
            let until = Instant::now() + Duration::from_secs(20);
            while wal.checkpoint() != wal.written_head() {
                assert!(Instant::now() < until, "書けるように戻した後も checkpoint が進まない");
                eng.oplog_commit();
                std::thread::sleep(Duration::from_millis(10));
            }
            drop((wal, eng));
        }
        At::Close => {
            drop(eng);
            copy(&db, image);
            block_tables(&db, false);
        }
    }
    let _ = std::fs::remove_dir(image.join("tables.tmp"));
    x
}

fn copy(from: &Path, to: &Path) {
    assert!(std::process::Command::new("cp").arg("-R").arg(from).arg(to).status().unwrap().success());
}

fn first_new_eid(image: &Path) -> u64 {
    let eng = Engine::open_concurrent_with_oplog(image.to_str().unwrap(), 4 << 20).unwrap();
    eng.entity_in("t").unwrap()
}

/// `tables` を書けない間に消した行の eid は、 consumer の周期の後に落ちた像を開いても、 もう一度払い出さない。
#[test]
fn a_deleted_row_is_not_reissued_after_a_crash_while_tables_cannot_be_written() {
    let s = scratch("tick");
    let image = s.0.join("image");
    let x = image_with_a_deleted_row(&s, At::Tick, &image);
    assert_ne!(first_new_eid(&image), x, "落ちた後に、 消した行の eid {x:#x} をもう一度払い出した");
}

/// 閉じる時も同じ (`tables` を書けないまま閉じた DB を開いても、 消した行の eid を払い出さない)。
#[test]
fn a_deleted_row_is_not_reissued_after_closing_while_tables_cannot_be_written() {
    let s = scratch("close");
    let image = s.0.join("image");
    let x = image_with_a_deleted_row(&s, At::Close, &image);
    assert_ne!(first_new_eid(&image), x, "閉じて開いた後に、 消した行の eid {x:#x} をもう一度払い出した");
}

/// 落ちた像を開いて再生した直後にもう一度落ちても (開いた engine も `tables` を書けない)、 消した行の eid を払い出さない。 前は開く時の
/// 再生の後に checkpoint を進め、 2 度目の像では消した行の record が再生されなかった。 開き方は 2 通り: `open_concurrent_with_oplog` と、
/// `open_standalone` (#880 の後は、 中で WAL 付きの open を通って再生してから開く。 `tables` を書けないと checkpoint を進められないので断る)。
#[test]
fn a_deleted_row_is_not_reissued_after_a_second_crash_right_after_opening() {
    for standalone in [false, true] {
        let s = scratch(if standalone { "reopen-standalone" } else { "reopen" });
        let image = s.0.join("image");
        let x = image_with_a_deleted_row(&s, At::Tick, &image);
        // 開いた engine も `tables` を書けない (= 開いてから最初に書くまでの間に落ちた形を、 時間でなく状態で作る)
        block_tables(&image, true);
        let path = image.to_str().unwrap();
        // #880: 単独の open は、 先に WAL 付きの open で再生して閉じる。 `tables` を書けないとその往復で checkpoint を進められない
        // ので断る (消した行の record は WAL に残る = 2 度目の像も再生する)。 書けるように戻した後の open は消した行の eid を払い出さない
        if standalone {
            match Engine::open_standalone(path) {
                Err(e) => assert!(e.to_string().contains("#880"), "断った理由: {e}"),
                Ok(_) => panic!("`tables` を書けないのに単独で開いた (再生した Delete の record を残せない)"),
            }
            block_tables(&image, false);
            assert_ne!(first_new_eid(&image), x, "standalone: 断った後に開き直して、 消した行の eid {x:#x} をもう一度払い出した");
            continue;
        }
        let eng = Engine::open_concurrent_with_oplog(path, 4 << 20).unwrap();
        wait_one_tick_or_idle(&eng);
        assert_eq!(eng.fault_count(FaultKind::DiskSpace), 0, "前提: 本体の書き出しは成功している (書けないのは `tables` だけ)");
        let again = s.0.join("again");
        copy(&image, &again);
        let _ = std::fs::remove_dir(again.join("tables.tmp"));
        drop(eng);
        assert_ne!(first_new_eid(&again), x, "standalone={standalone}: 2 度落ちた後に、 消した行の eid {x:#x} をもう一度払い出した");
    }
}

/// `tables` を書けず checkpoint を進められない間に WAL が満杯になると、 書き込みは断られる (`WalSpace`、 黙って落とさない)。 書けるように
/// 戻すと、 また書ける。
#[test]
fn writes_are_refused_while_the_held_wal_is_full_and_resume_after() {
    let s = scratch("full");
    let db = s.0.join("db");
    let (eng, a) = open(&db, 256 << 10);
    let hold = eng.hold_consumer();
    let keep = eng.entity_in("t").unwrap();
    eng.tie_to_by_id(keep, a, 1u32);
    let gone = eng.entity_in("t").unwrap();
    eng.tie_to_by_id(gone, a, 2u32);
    block_tables(&db, true);
    eng.delete(gone);
    let folds = eng.wal_room_folds();
    drop(hold);
    let until = Instant::now() + Duration::from_secs(30);
    let mut v = 0u32;
    let refused = loop {
        match eng.try_tie_to_by_id(keep, a, v) {
            Ok(()) => v += 1,
            Err(TieRejected::Fault(FaultKind::WalSpace)) => break true,
            Err(e) => panic!("{e} で断られた"),
        }
        if Instant::now() > until {
            break false;
        }
    };
    let folded = eng.wal_room_folds() - folds;
    assert!(
        refused && folded == 0,
        "`tables` を書けない間に WAL を {folded} 回畳んだ ({v} 回書けた、 断られた {refused}) = 消した行の record を checkpoint の前に置いた"
    );
    block_tables(&db, false);
    let until = Instant::now() + Duration::from_secs(20);
    while eng.try_tie_to_by_id(keep, a, v).is_err() {
        assert!(Instant::now() < until, "書けるように戻した後も書けない");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(eng.get_by_id(keep, a), Some(u64::from(v)));
}
