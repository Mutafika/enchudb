//! #511: sync の DB で WAL が満杯になると畳めず、 その後の record がほぼ全部落ちた (WAL 256 KiB に書き手 1 本で 20 万回書くと、
//! 満杯の後の 197,953 件)。 落ちた record も本体には当たり、 #57 で相手が取り直すが、 満杯の後はほぼ全部がその道に回った。
//!
//! 理由と直し (どれも、 直しを外すとこの file の test が落ちる):
//!
//! 1. 満杯で空きを待つ書き手は `pending_writes` に入ったまま待ち、 sync の DB が畳む道 (`OpLog::try_reset_if`) はそれが
//!    0 でないと畳まなかった。 → 見ない (sync の無い DB の道 `checkpoint_and_reset` と同じ。 全 append は `append_lock` の
//!    下で書き終える)
//! 2. 1 を直しても、 満杯の時に空きが Commit (120 byte、 周の印込み) より小さいと閉じの Commit を打てず、 開いた group を畳む時に
//!    孤児の group として配りから落とした (#450 の死区間の例外。 しかも `wal_dropped_records` に数えていなかった)。
//!    → data の record は閉じの Commit 1 つ分の空きを残してしか場所を取らない (`OpLog::alloc`)。 孤児の group の落ちも数える
//! 3. 書き手が積み続けている間に空き作りが走ると (頼まれてから動き出すまでに空きができていた、 前の満杯の時の頼みが
//!    残っていた)、 閉じの Commit の後に書き手が積んで末尾の group が閉じない、 または書き出しの後に積んで head が checkpoint
//!    より先に進み、 畳めなかった。 その後の 10 ms は待つ書き手がすぐ諦める (`wal_room_failed_recently`) ので、 数千〜2 万件
//!    落ちた。 → 閉じ直して書き出しからやり直す (`make_wal_room_inner`、 回数に上限)
//! 4. 1 を直すと、 相手の ack を取り込んで `_sync_ops` を畳む reclaim が固まる形が出た。 purge は `_sync_ops` の
//!    `free_locals` を握ったまま行を消し (WAL に積む)、 満杯なら空きを待つ。 空きを作る係は bridge で同じ `free_locals` を
//!    取りに行くので互いを待つ (purge は待ちの上限 5 秒で諦め、 次の行で同じことを繰り返す)。 → WAL に積んでから握る
//!    (後に #561 で purge は WAL に積まなくなり、 待ちそのものが無くなった)
//!
//! #552: 書き手が多いと、 consumer は畳めているのに待つ書き手が諦めて落とした (sync の表によらず、 8 本で数件、 64 KiB に
//! 64 本で 2 千件ほど。 どの書き手も待ちは数 ms で、 上限の 5 秒に遠かった)。
//!
//! 5. 待つ書き手は、 頼んだ後に consumer が 2 回試したら諦め、 その数に畳んだ試しも入れていた (畳んだ空きを他の書き手が先に
//!    埋めた書き手を落とす)。 → 畳めない状態だった試しだけ数える
//! 6. 待ち手が空けた後、 append は 1 回しか書き直さなかった (書き直す前に他の書き手が埋めると落とす)。 → 載るか待ち手が
//!    諦めるまで書き直す (oplog の単体 `append_waits_again_when_another_writer_takes_the_room` が道ごとに見る)

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::Hlc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const WAL: usize = 256 * 1024;
const ROWS: u64 = 1000;

/// この file の test は 1 本ずつ回す (書き手 8 本の test が並ぶと CPU を取り合い、 数が揺れる)
static SERIAL: Mutex<()> = Mutex::new(());

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-syncwalfull-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path); // v10: DB は directory
}

/// 表 `rows` (列 `val`) を持つ DB を WAL `wal` byte で開く。 `sync` なら sync の表あり。
fn open(path: &str, sync: bool, wal: usize) -> (Arc<Engine>, u16, Vec<u64>) {
    cleanup(path);
    {
        let mut eng = Engine::create_standalone(path).unwrap();
        eng.define_table("rows", 100_000).unwrap();
        eng.define_himo_in("rows", "val", ValueType::Number, 100_000).unwrap();
        if sync {
            eng.enable_sync_tables().unwrap();
        }
        eng.flush().unwrap();
    }
    let eng = Engine::open_concurrent_with_oplog(path, wal).unwrap();
    eng.set_peer_id(1);
    let hid = eng.himo_id("rows.val").unwrap() as u16;
    let rows = (0..ROWS).map(|_| eng.entity_in("rows").unwrap()).collect();
    (eng, hid, rows)
}

/// 1 回分の結果。
struct Round {
    dropped: u64,
    folds: u64,
    reclaims: u64,
    /// WAL の空きを待つ書き手が、 待ちの上限 (5 秒) まで待った回数 (`Engine::wal_room_wait_timeouts`)
    wait_timeouts: u64,
}

/// 1 回分: 新しい DB に、 書き手 `writers` 本で 1000 行に合わせて `writes` 回書く (書き手どうしで行は重ならない)。 `reclaim`
/// なら書き手 0 が 1000 回ごとに相手の ack を取り込んで reclaim する (相手が追いついている形)。 書き手は切り離した thread で
/// 回し (固まった時に test ごと止まらないよう、 scope で join しない)、 reclaim 1 回が 30 秒を越えたら落とす (固まった時の
/// 番兵)。 reclaim が空きを待って止まったかは時間でなく `Engine::wal_room_wait_timeouts` (待ちの上限 5 秒まで待った回数) で
/// 見る (呼び手の test)。 前は reclaim 1 回が 4 秒を越えたら落としていて、 2 core の CI で 4.00 秒の回が出た (#615 の CI)。
/// WAL は `WAL` byte。
fn round(tag: &str, sync: bool, writers: u64, writes: u64, reclaim: bool) -> Round {
    round_in(WAL, tag, sync, writers, writes, reclaim)
}

/// `round` の WAL を `wal` byte にした形。
fn round_in(wal: usize, tag: &str, sync: bool, writers: u64, writes: u64, reclaim: bool) -> Round {
    let path = tmp_path(tag);
    let (eng, hid, rows) = open(&path, sync, wal);
    let rows = Arc::new(rows);
    let stop = Arc::new(AtomicBool::new(false));
    // 0 = reclaim していない、 それ以外 = 今の reclaim を始めた時刻 (base からの µs + 1)
    let base = Instant::now();
    let reclaim_since = Arc::new(AtomicU64::new(0));
    let reclaims = Arc::new(AtomicU64::new(0));
    let hs: Vec<_> = (0..writers)
        .map(|w| {
            let (eng, rows, stop) = (eng.clone(), rows.clone(), stop.clone());
            let (since, reclaims) = (reclaim_since.clone(), reclaims.clone());
            std::thread::spawn(move || {
                let all = Hlc { wall: u64::MAX / 2, logical: 0, peer: 0 };
                for i in (w..writes).step_by(writers as usize) {
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    eng.tie_to_by_id(rows[(i % ROWS) as usize], hid, i + 1);
                    if reclaim && w == 0 && i % 1000 < writers {
                        since.store(base.elapsed().as_micros() as u64 + 1, Ordering::Release);
                        let _ = eng.ack_sync_up_to_hlc(2, all);
                        eng.reclaim_sync_ops();
                        since.store(0, Ordering::Release);
                        reclaims.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();
    let limit = Duration::from_secs(30);
    while !hs.iter().all(|h| h.is_finished()) {
        if eng.wal_room_wait_timeouts() > 0 {
            stop.store(true, Ordering::Relaxed);
            panic!("{tag}: 空きを待つ書き手が待ちの上限 (5 秒) まで待った (WAL が満杯の間、 空きを作る係が待っている書き手を待っている)");
        }
        let started = reclaim_since.load(Ordering::Acquire);
        if started != 0 {
            let running = base.elapsed().saturating_sub(Duration::from_micros(started - 1));
            if running > limit {
                stop.store(true, Ordering::Relaxed);
                panic!("{tag}: reclaim 1 回が {running:?} 止まった (WAL が満杯の間、 purge が空きを待ったまま bridge の lock を握っている)");
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    for h in hs {
        h.join().unwrap();
    }
    let out = Round {
        dropped: eng.wal_dropped_records(),
        folds: eng.wal_room_folds(),
        reclaims: reclaims.load(Ordering::Relaxed),
        wait_timeouts: eng.wal_room_wait_timeouts(),
    };
    drop(eng);
    cleanup(&path);
    out
}

/// 書き手 1 本が WAL の何倍も書いても record は落ちず、 満杯のたびに畳める。 sync の無い DB (対照) と同じになる。
///
/// falsify: 直し 1 (`try_reset_if` が `pending_writes` を見る) か 2 (data の record が閉じの Commit の分を残さない) を外すと、
/// sync の DB で 5 万中 4.8 万ほど落ちる。
#[test]
fn sync_db_folds_when_the_wal_is_full() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    for sync in [false, true] {
        let r = round(if sync { "fold-sync" } else { "fold-nosync" }, sync, 1, 50_000, false);
        assert_eq!(r.dropped, 0, "sync={sync}: WAL が満杯の間に record が落ちた (満杯で畳んだ {} 回)", r.folds);
        assert!(r.folds > 0, "sync={sync}: 満杯で 1 回も畳んでいない (前提: WAL の何倍も書いている)");
    }
}

/// 書き手 8 本でも落ちない (#552 の前は各回 1000 件未満を許していた。 落ちは直し 5 / 6 の形だった)。
///
/// falsify: 直し 3 (閉じ直してやり直す) を外すと、 2 万件ほど落ちる回が出る。
#[test]
fn many_writers_drop_few_records() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    for n in 0..8 {
        let r = round(&format!("many{n}"), true, 8, 100_000, false);
        assert_eq!(r.dropped, 0, "{n} 回目: 書き手 8 本で {} 件落ちた (満杯で畳んだ {} 回)", r.dropped, r.folds);
        assert!(r.folds > 0, "前提: 満杯で 1 回も畳んでいない");
    }
}

/// 書き手 1 本が自分で相手の ack を取り込んで reclaim する形でも落ちない。
///
/// falsify: 直し 3 のうち、 head が checkpoint より先に進んだ時もやり直す所を外すと、 5 千件ほど落ちる回が出る。
#[test]
fn reclaiming_writer_drops_few_records() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    for n in 0..16 {
        let r = round(&format!("reclaim1-{n}"), true, 1, 100_000, true);
        assert_eq!(r.dropped, 0, "{n} 回目: reclaim しながら {} 件落ちた (満杯で畳んだ {} 回)", r.dropped, r.folds);
        assert_eq!(r.wait_timeouts, 0, "{n} 回目: 空きを待つ書き手が待ちの上限まで待った");
        assert!(r.reclaims > 0 && r.folds > 0, "前提: reclaim {} 回・畳んだ {} 回", r.reclaims, r.folds);
    }
}

/// 書き手 8 本 (書き手 0 が reclaim) を新しい DB で何回もくり返し、 WAL が満杯でも reclaim が止まらない (`round` が見る)。
///
/// falsify: 直し 4 (purge は WAL に積んでから `free_locals` を握る) を外すと、 purge が待ちの上限まで空きを待って落ちた
/// (`wal_room_wait_timeouts` が数える)。 #561 で purge は WAL に積まなくなった (この形の待ちは起きない)。
#[test]
fn reclaim_does_not_stall_while_the_wal_is_full() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    for n in 0..12 {
        let r = round(&format!("stall{n}"), true, 8, 100_000, true);
        assert!(r.reclaims > 0 && r.folds > 0, "前提: reclaim {} 回・畳んだ {} 回", r.reclaims, r.folds);
        assert_eq!(r.wait_timeouts, 0, "{n} 回目: 空きを待つ書き手が待ちの上限まで待った (reclaim {} 回)", r.reclaims);
    }
}

/// 書き手を WAL に比べて多くしても (64 KiB に 32 本 / 64 本) record は落ちない (#552)。 sync の無い DB の空き作りが畳めない
/// のは書き出しの I/O 失敗の時だけなので、 空きを待つ書き手は空きを取れるまで待てる。
///
/// falsify (16 core の Mac、 release): 直し 5 を外すと書き手 32 本の 1 回目で 15〜32 件、 直し 6 (`append` の道) を外すと
/// 324〜376 件落ちる (3 回とも 1 回目で落ちる)。
#[test]
fn crowded_writers_drop_nothing() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    for writers in [32u64, 64] {
        for n in 0..4 {
            let r = round_in(64 * 1024, &format!("crowd{writers}-{n}"), false, writers, 100_000, false);
            assert_eq!(r.dropped, 0, "書き手 {writers} 本の {n} 回目: {} 件落ちた (満杯で畳んだ {} 回)", r.dropped, r.folds);
            assert!(r.folds > 0, "前提: 満杯で 1 回も畳んでいない");
        }
    }
}
