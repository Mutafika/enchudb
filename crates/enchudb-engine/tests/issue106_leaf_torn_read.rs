//! #106 再現 probe — LeafStore の read-while-write torn read。
//!
//! opyula の `enchudb_upgrade_probe -- leafrace` を engine repo 内に移植した
//! 自己完結版。 writer 1 thread が複数 entity を **size class 混在**の
//! self-describing body（先頭 16 桁 == 末尾 16 桁の世代 stamp）で上書きし続け、
//! reader が毎 read で「先頭 16 桁 == 末尾 16 桁」を照合する。
//!
//!  - 違反 (CORRUPT) = torn read / 世代 mix した bytes が正常 read として返った
//!  - panic (PANIC)  = 破れた len で `LeafStore::get` が OOB slice
//!
//! 期待: **現 tip (0.13.2) では CORRUPT または PANIC が観測される**（= bug 再現）。
//! 修正 (seqlock verify+retry など) 後は violation 0 になるべき。
//!
//! 単一 entity では再現しない（境界が動かない）。 複数 entity + size class 混在で
//! free-list の split / coalesce により slot 境界が動き、 stale offset が別 slot の
//! 中腹を指すようになるのが再現条件。

use enchudb_engine::{Engine, ValueType};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue106-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suf in ["", ".oplog", ".wal", ".tables", ".crc", ".db.lock", ".eidmap", ".positions"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

/// build phase 相当: Arc 単一所有のうちに define。
fn define(eng: &Arc<Engine>, name: &str, vt: ValueType) -> u16 {
    let eng_mut = unsafe { &mut *(Arc::as_ptr(eng) as *mut Engine) };
    eng_mut.define_himo(name, vt, 0);
    eng.himo_id(name).unwrap() as u16
}

const N_ENTITIES: usize = 8;
/// slot size class を散らして free-list の split / coalesce を誘発する。
const SIZE_CLASSES: [usize; 4] = [64, 128, 256, 512];
/// stamp = 世代の 10 進 16 桁。 body 先頭と末尾に同じものを埋める。
const STAMP: usize = 16;

/// gen / entity から self-describing body を作る。
/// `{stamp}-{filler}-{stamp}` 形。 filler 長で size class を変える。
fn body_for(g: u64, e: usize) -> String {
    let stamp = format!("{g:016}");
    let target = SIZE_CLASSES[(g as usize + e) % SIZE_CLASSES.len()];
    // 34 = STAMP(16) + '-' + '-' + STAMP(16)
    let mid_len = target.saturating_sub(STAMP * 2 + 2);
    let mid = "x".repeat(mid_len);
    format!("{stamp}-{mid}-{stamp}")
}

/// read した bytes の先頭 16 桁 == 末尾 16 桁か。 false = torn / 世代 mix。
fn head_matches_tail(b: &[u8]) -> bool {
    b.len() >= STAMP * 2 && b[..STAMP] == b[b.len() - STAMP..]
}

#[test]
fn leaf_read_while_write_torn_read() {
    let path = tmp("leafrace");
    cleanup(&path);

    // oplog なし: torn read は LeafStore 内部の現象で WAL に依存しない。
    // create_concurrent_with_oplog だと WAL-full の警告が出るだけなので使わない。
    let eng: Arc<Engine> = Engine::create_concurrent(&path).expect("create");
    let hid = define(&eng, "body", ValueType::Leaf);

    // entity を確保して gen 0 を張る。
    let eids: Vec<_> = (0..N_ENTITIES).map(|_| eng.entity()).collect();
    for (e, &eid) in eids.iter().enumerate() {
        eng.tie_text_to_by_id(eid, hid, &body_for(0, e));
    }

    // OOB panic の backtrace で stderr が溢れないよう黙らせる（この binary 内だけ）。
    std::panic::set_hook(Box::new(|_| {}));

    let stop = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicU64::new(0));
    let corrupt = Arc::new(AtomicU64::new(0));
    let panicked = Arc::new(AtomicU64::new(0));

    // reader x4: 各 entity を読んで head==tail を照合。 OOB は catch_unwind で捕捉。
    let readers: Vec<_> = (0..4)
        .map(|_| {
            let eng = eng.clone();
            let eids = eids.clone();
            let stop = stop.clone();
            let reads = reads.clone();
            let corrupt = corrupt.clone();
            let panicked = panicked.clone();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    for &eid in &eids {
                        let eng = &eng;
                        // #106 fix: torn read しない安全 API。 catch_unwind は
                        // 「本当に panic しないか」の保険 (fix 後は 0 のはず)。
                        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            eng.get_text_owned(eid, "body")
                        }));
                        reads.fetch_add(1, Ordering::Relaxed);
                        match r {
                            Ok(Some(b)) => {
                                if !head_matches_tail(&b) {
                                    corrupt.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Ok(None) => {} // まだ tie されていない (無い)
                            Err(_) => {
                                panicked.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            })
        })
        .collect();

    // writer: 各 entity を round-robin で世代を上げつつ上書き。
    // size class は body_for が (gen+e) で散らすので slot 境界が動く。
    let deadline_writes = 4_000_000u64;
    let mut g = 1u64;
    let mut written = 0u64;
    while written < deadline_writes {
        for (e, &eid) in eids.iter().enumerate() {
            eng.tie_text_to_by_id(eid, hid, &body_for(g, e));
            written += 1;
        }
        g += 1;
    }

    stop.store(true, Ordering::Relaxed);
    for h in readers {
        h.join().unwrap();
    }
    let _ = std::panic::take_hook();

    let r = reads.load(Ordering::Relaxed);
    let c = corrupt.load(Ordering::Relaxed);
    let p = panicked.load(Ordering::Relaxed);
    eprintln!(
        "== issue106 leafrace: {r} reads vs {written} writes ==\n  CORRUPT {c}  PANIC {p}  (violation rate {:.4}%)",
        (c + p) as f64 / r.max(1) as f64 * 100.0
    );

    cleanup(&path);

    // #106 fix 後の regression assert: writer 稼働中に別 thread が読んでも
    // torn read (CORRUPT) / OOB (PANIC) が 1 件も出ないこと。
    // fix (engine の insert→publish→free 並べ替え + get_text_owned の bounds-clamp
    // copy + column 再読 retry) を無効化すると、 この assert が落ちる (falsify 済)。
    assert_eq!(
        c + p, 0,
        "read-while-write で torn read を観測 ({r} reads, CORRUPT {c} / PANIC {p})"
    );
}
