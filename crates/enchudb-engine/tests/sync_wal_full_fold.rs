//! #511: sync の DB で WAL が満杯になると畳めず、 その後の record がほぼ全部落ちた (WAL 256 KiB に書き手 1 本で 20 万回書くと、
//! 満杯の後の 197,953 件)。 落ちた record も本体には当たり、 #57 で相手が取り直すが、 満杯の後はほぼ全部がその道に回った。
//!
//! 理由と直し (どれも、 直しを外すとこの file の test が落ちる):
//!
//! 1. 満杯で空きを待つ書き手は `pending_writes` に入ったまま待ち、 sync の DB が畳む道 (`OpLog::try_reset_if`) はそれが
//!    0 でないと畳まなかった。 → 見ない (sync の無い DB の道 `checkpoint_and_reset` と同じ。 全 append は `append_lock` の
//!    下で書き終える)
//! 2. 1 を直しても、 満杯の時に空きが Commit (112 byte) より小さいと閉じの Commit を打てず、 開いた group を畳む時に
//!    孤児の group として配りから落とした (#450 の死区間の例外。 しかも `wal_dropped_records` に数えていなかった)。
//!    → data の record は閉じの Commit 1 つ分の空きを残してしか場所を取らない (`OpLog::alloc`)。 孤児の group の落ちも数える
//! 3. 書き手が積み続けている間に空き作りが走ると (頼まれてから動き出すまでに空きができていた、 前の満杯の時の頼みが
//!    残っていた)、 閉じの Commit の後に書き手が積んで末尾の group が閉じない、 または書き出しの後に積んで head が checkpoint
//!    より先に進み、 畳めなかった。 その後の 10 ms は待つ書き手がすぐ諦める (`wal_room_failed_recently`) ので、 数千〜2 万件
//!    落ちた。 → 閉じ直して書き出しからやり直す (`make_wal_room_inner`、 回数に上限)
//! 4. 1 を直すと、 相手の ack を取り込んで `_sync_ops` を畳む reclaim が固まる形が出た。 purge は `_sync_ops` の
//!    `free_locals` を握ったまま行を消し (WAL に積む)、 満杯なら空きを待つ。 空きを作る係は bridge で同じ `free_locals` を
//!    取りに行くので互いを待つ (purge は待ちの上限 5 秒で諦め、 次の行で同じことを繰り返す)。 → WAL に積んでから握る

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

/// 表 `rows` (列 `val`) を持つ DB を WAL `WAL` byte で開く。 `sync` なら sync の表あり。
fn open(path: &str, sync: bool) -> (Arc<Engine>, u16, Vec<u64>) {
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
    let eng = Engine::open_concurrent_with_oplog(path, WAL).unwrap();
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
}

/// 1 回分: 新しい DB に、 書き手 `writers` 本で 1000 行に合わせて `writes` 回書く (書き手どうしで行は重ならない)。 `reclaim`
/// なら書き手 0 が 1000 回ごとに相手の ack を取り込んで reclaim する (相手が追いついている形)。 書き手は切り離した thread で
/// 回し (固まった時に test ごと止まらないよう、 scope で join しない)、 reclaim 1 回が 4 秒 (待ちの上限 `WAL_ROOM_WAIT` =
/// 5 秒の手前) を越えたら落とす。
fn round(tag: &str, sync: bool, writers: u64, writes: u64, reclaim: bool) -> Round {
    let path = tmp_path(tag);
    let (eng, hid, rows) = open(&path, sync);
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
    let limit = Duration::from_secs(4);
    while !hs.iter().all(|h| h.is_finished()) {
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

/// 書き手 8 本でも、 落ちはわずか (各回 1000 件未満。 直した後の下調べは 20 万回で 0〜11 件、 sync の無い DB は 0〜180 件)。
///
/// falsify: 直し 3 (閉じ直してやり直す) を外すと、 2 万件ほど落ちる回が出る。
#[test]
fn many_writers_drop_few_records() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    for n in 0..8 {
        let r = round(&format!("many{n}"), true, 8, 100_000, false);
        assert!(r.dropped < 1000, "{n} 回目: 書き手 8 本で {} 件落ちた (満杯で畳んだ {} 回)", r.dropped, r.folds);
        assert!(r.folds > 0, "前提: 満杯で 1 回も畳んでいない");
    }
}

/// 書き手 1 本が自分で相手の ack を取り込んで reclaim する形でも、 落ちはわずか (16 回とも 1000 件未満。 下調べは 0 件)。
///
/// falsify: 直し 3 のうち、 head が checkpoint より先に進んだ時もやり直す所を外すと、 5 千件ほど落ちる回が出る。
#[test]
fn reclaiming_writer_drops_few_records() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    for n in 0..16 {
        let r = round(&format!("reclaim1-{n}"), true, 1, 100_000, true);
        assert!(r.dropped < 1000, "{n} 回目: reclaim しながら {} 件落ちた (満杯で畳んだ {} 回)", r.dropped, r.folds);
        assert!(r.reclaims > 0 && r.folds > 0, "前提: reclaim {} 回・畳んだ {} 回", r.reclaims, r.folds);
    }
}

/// 書き手 8 本 (書き手 0 が reclaim) を新しい DB で何回もくり返し、 WAL が満杯でも reclaim が止まらない (`round` が見る)。
///
/// falsify: 直し 4 (purge は WAL に積んでから `free_locals` を握る) を外すと、 reclaim 1 回が待ちの上限で止まって落ちる。
#[test]
fn reclaim_does_not_stall_while_the_wal_is_full() {
    let _serial = SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    for n in 0..12 {
        let r = round(&format!("stall{n}"), true, 8, 100_000, true);
        assert!(r.reclaims > 0 && r.folds > 0, "前提: reclaim {} 回・畳んだ {} 回", r.reclaims, r.folds);
    }
}
