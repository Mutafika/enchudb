//! #700: 記録 (changefeed.seg) のある DB に、 rayon の global pool の上から書いても止まらない。
//!
//! 記録への写し (`ChangeLog::write_entries`) は 4096 件以上の回を並べる。 前は rayon の global pool に頼んでいた: 写すのは consumer
//! (pool の外の thread) なので worker が空くまで待つ。 アプリの書き手が同じ pool の上で全員 WAL の空きを待っていると、 空きを作る
//! consumer は worker を待ち、 worker は consumer を待つ → 書き手は 5 秒で `WalSpace`。 今は写しだけをする専用の pool に頼む。
//!
//! 判定は状態で: 書き込みが断られた数と、 WAL の区画を実際に移ったこと (対照 = 同じ書き手を std の thread で回す)。
#![cfg(not(target_arch = "wasm32"))]

use enchudb_engine::{db_files, Engine, TieRejected, ValueType};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

const WRITERS: u64 = 2;
const PER_WRITER: u64 = 150_000;

struct Tmp(String);
impl Tmp {
    fn new(tag: &str) -> Self {
        let p = std::env::temp_dir().join(format!("enchudb-issue700-{tag}-{}", std::process::id())).to_string_lossy().into_owned();
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

fn run(tag: &str, with_log: bool, on_rayon: bool) -> (u64, Vec<Option<TieRejected>>, u64) {
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
    (n, rejected, eng.oplog().unwrap().switches())
}

#[test]
fn writers_on_the_rayon_global_pool_with_a_change_log() {
    // global pool を書き手の数にする (アプリが pool を全部書き手に使っている形)
    rayon::ThreadPoolBuilder::new().num_threads(WRITERS as usize).build_global().unwrap();
    // 対照 1: 記録なし・rayon の上の書き手
    let (n1, r1, _) = run("nolog-rayon", false, true);
    // 対照 2: 記録あり・std の thread の書き手
    let (n2, r2, _) = run("log-std", true, false);
    // 本体: 記録あり・rayon の上の書き手
    let (n3, r3, switches) = run("log-rayon", true, true);
    assert!(r1.iter().all(Option::is_none) && n1 == WRITERS * PER_WRITER, "対照 1 が断られた: {r1:?}");
    assert!(r2.iter().all(Option::is_none) && n2 == WRITERS * PER_WRITER, "対照 2 が断られた: {r2:?}");
    assert!(
        r3.iter().all(Option::is_none) && n3 == WRITERS * PER_WRITER,
        "記録のある DB に rayon の global pool の上から書くと、 WAL の空き待ちが時間切れで断られる: {r3:?} (wrote {n3})"
    );
    // 区画を 2 回以上移った = 古い区画 (2 MiB、 数万件) を記録に写してから畳んだ = 並べて写す大きさの回があった
    assert!(switches >= 2, "試験の前提: WAL の区画を移り、 consumer が 1 区画分を記録に写して畳んだ ({switches} 回)");
}
