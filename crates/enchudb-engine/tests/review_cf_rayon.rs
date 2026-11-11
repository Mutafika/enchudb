//! review (0.37.4, changefeed): 記録への写し (`ChangeLog::write_entries`) は 4096 件以上の回を rayon の global pool で並べる
//! (#667 / #683)。 写すのは consumer (pool の外の thread) なので、 pool の worker が空くまで待つ。 アプリの書き手が同じ global pool
//! の上で書いていて (rayon の par_iter で一括投入)、 全員が WAL の空きを待っていると、 空きを作る consumer は worker を待ち、
//! worker は consumer を待つ。
//!
//! 判定は状態で: 書き込みが `WalSpace` で断られた数 (対照 = 同じ書き手を std の thread で回す)。 時間は添えるだけ。
#![cfg(not(target_arch = "wasm32"))]

use enchudb_engine::{db_files, Engine, FaultKind, TieRejected, ValueType};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

const SCRATCH: &str = "/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/eedfc333-d87a-4a47-a3b0-daf4b6d7337a/scratchpad";
const WRITERS: u64 = 2;
const PER_WRITER: u64 = 150_000;

struct Tmp(String);
impl Tmp {
    fn new(tag: &str) -> Self {
        let _ = std::fs::create_dir_all(SCRATCH);
        let p = format!("{SCRATCH}/review-cfr-{tag}-{}", std::process::id());
        let _ = db_files::remove_db(&p);
        Tmp(p)
    }
}
impl Drop for Tmp {
    fn drop(&mut self) {
        let _ = db_files::remove_db(&self.0);
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// 1 本の書き手: 最初に断られた所でやめる (断られるたびに 5 秒待つので)。 (書けた数, 断られた理由)
fn writer(eng: &Engine, w: u64, ok: &AtomicU64) -> Option<TieRejected> {
    let ts = eng.himo_id("ts").unwrap() as u16;
    let e = eng.entity().unwrap();
    for i in 0..PER_WRITER {
        match eng.try_tie_to_by_id(e, ts, w << 40 | i) {
            Ok(()) => {
                ok.fetch_add(1, Ordering::Relaxed);
            }
            Err(r) => return Some(r),
        }
        if i % 64 == 0 {
            eng.oplog_commit();
        }
    }
    None
}

fn run(tag: &str, with_log: bool, on_rayon: bool) -> (u64, Vec<Option<TieRejected>>, f64) {
    let tmp = Tmp::new(tag);
    let eng: Arc<Engine> = Engine::create_concurrent_with_oplog(&tmp.0, 4 << 20).unwrap();
    eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
    if with_log {
        eng.enable_change_log(64 << 20).unwrap();
    }
    let ok = AtomicU64::new(0);
    let t = Instant::now();
    let rejected: Vec<Option<TieRejected>> = if on_rayon {
        use rayon::prelude::*;
        (0..WRITERS as u32).into_par_iter().with_max_len(1).map(|w| writer(&eng, w as u64, &ok)).collect()
    } else {
        std::thread::scope(|s| {
            let hs: Vec<_> = (0..WRITERS).map(|w| { let (eng, ok) = (&eng, &ok); s.spawn(move || writer(eng, w, ok)) }).collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        })
    };
    let secs = t.elapsed().as_secs_f64();
    let n = ok.load(Ordering::Relaxed);
    eprintln!(
        "{tag}: with_log={with_log} on_rayon={on_rayon} wrote={n}/{} rejected={rejected:?} elapsed={secs:.2}s wal_room_folds={} switches={} log_range={:?}",
        WRITERS * PER_WRITER,
        eng.wal_room_folds(),
        eng.oplog().unwrap().switches(),
        eng.change_log_range(),
    );
    (n, rejected, secs)
}

#[test]
fn t8_writers_on_the_rayon_pool_with_a_record() {
    // global pool を書き手の数にする (アプリが pool を全部書き手に使っている形)
    rayon::ThreadPoolBuilder::new().num_threads(WRITERS as usize).build_global().unwrap();
    // 対照 1: 記録なし・rayon の上の書き手
    let (n1, r1, _) = run("t8-nolog-rayon", false, true);
    // 対照 2: 記録あり・std の thread の書き手
    let (n2, r2, _) = run("t8-log-std", true, false);
    // 本体: 記録あり・rayon の上の書き手
    let (n3, r3, _) = run("t8-log-rayon", true, true);
    assert!(r1.iter().all(Option::is_none) && n1 == WRITERS * PER_WRITER, "対照 1 が断られた: {r1:?}");
    assert!(r2.iter().all(Option::is_none) && n2 == WRITERS * PER_WRITER, "対照 2 が断られた: {r2:?}");
    assert!(
        r3.iter().all(Option::is_none) && n3 == WRITERS * PER_WRITER,
        "記録のある DB に rayon の global pool の上から書くと、 WAL の空き待ちが時間切れで断られる: {r3:?} (wrote {n3})"
    );
    let _ = FaultKind::WalSpace;
}
