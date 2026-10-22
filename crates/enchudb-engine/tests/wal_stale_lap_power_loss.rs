//! WAL を畳んだ (ring を頭に戻した) 直後の電源断で、 前の周の record を再生しない (`--features crashsim`)。
//!
//! WAL の header の head は append のたびに mmap に書く。 OS が header の page (新しい周の最初の record も同じ page) を
//! data の page より先に書き出すと、 header は 「checkpoint = 頭、 head = 新しい周の今の位置」、 2 枚目からの page は前の周の
//! ままの像になる。 record の大きさがそろっていると前の周の record の境目が新しい周の続きにそろい、 CRC の合う record として
//! 読めるので、 開く時の再生が前の周の古い値を当て直す (書き出しが返った書き込みが前の値に戻る)。

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::crashsim::{self, Mode};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const WAL_CAP: usize = 256 << 10;
const IMAGES: usize = 60;

#[test]
fn a_fold_then_power_loss_does_not_replay_the_previous_lap() {
    let root = std::env::temp_dir().join(format!("enchu_wal_stale_lap_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    crashsim::start();
    let eng: Arc<Engine> = Engine::create_concurrent_with_oplog(db.to_str().unwrap(), WAL_CAP).unwrap();
    eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
    let acked = Arc::new(AtomicU64::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let writer = {
        let (eng, acked, done) = (eng.clone(), acked.clone(), done.clone());
        std::thread::spawn(move || {
            let ts = eng.himo_id("ts").unwrap() as u16;
            let a = eng.entity().unwrap();
            let mut v = 0u64;
            while !done.load(Ordering::Acquire) && v < 4_000_000 {
                eng.tie_to_by_id(a, ts, v);
                v += 1;
                if v % 50 == 0 {
                    eng.oplog_commit();
                }
                if v % 1000 == 0 {
                    eng.oplog_sync().unwrap();
                    acked.store(v, Ordering::Release);
                }
            }
        })
    };
    let mut images = Vec::new();
    for k in 0..IMAGES {
        std::thread::sleep(Duration::from_millis(4));
        let at = acked.load(Ordering::Acquire);
        let img = root.join(format!("img{k:03}"));
        crashsim::capture(&live, &img, Mode::Mixed { seed: k as u64 * 7919 + 1 }).unwrap();
        images.push((img, at));
    }
    done.store(true, Ordering::Release);
    writer.join().unwrap();
    crashsim::stop();
    drop(eng);
    let mut bad = Vec::new();
    for (img, at) in &images {
        let path = img.join("db");
        let eng = Engine::open_concurrent_with_oplog(path.to_str().unwrap(), WAL_CAP).unwrap();
        let ts = eng.himo_id("ts").unwrap() as u16;
        let got = eng.get_by_id(0, ts);
        drop(eng);
        // 書き出しが返った値 (at - 1) より前に戻っていない
        if *at > 0 && got.is_none_or(|v| v + 1 < *at) {
            bad.push(format!("{}: 開いた後 {got:?}、 書き出しの返った {at}", img.display()));
        }
    }
    let _ = std::fs::remove_dir_all(&root);
    assert!(bad.is_empty(), "{} / {IMAGES} 枚で書き出しの返った値が前に戻った:\n{}", bad.len(), bad.join("\n"));
}
