//! #106 cross-process 検証 — writer が leaf を churn 中に、 **別 Engine ハンドル
//! (`open_readonly`)** が `get_text_owned` で読んでも torn read しない。
//!
//! `open_readonly` は writer flock を取らず同じ file を `MAP_SHARED` で map する
//! (= 別プロセスの readonly reader = oboro / tail / studio パターン、 CLAUDE.md の
//! peer 検証方針)。 別ハンドルは **別 `gen_seq` カウンタ** を持つが、 reader は
//! gen を書かず共有 mmap から読むだけなので、 writer が焼いた gen で seqlock が成立
//! する。 これで in-process epoch では守れない cross-process 経路を検証する。
//!
//! 同一プロセス内の 2 ハンドルだが、 触る state は完全に共有 mmap (column + leaf
//! region) 経由なので、 別プロセスと同じ read 経路を通る。

use enchudb_engine::{Engine, ValueType};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue106xp-{}-{}-{}",
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

const N_ENTITIES: usize = 8;
const SIZE_CLASSES: [usize; 4] = [64, 128, 256, 512];
const STAMP: usize = 16;

fn body_for(g: u64, e: usize) -> String {
    let stamp = format!("{g:016}");
    let target = SIZE_CLASSES[(g as usize + e) % SIZE_CLASSES.len()];
    let mid_len = target.saturating_sub(STAMP * 2 + 2);
    format!("{stamp}-{}-{stamp}", "x".repeat(mid_len))
}

fn head_matches_tail(b: &[u8]) -> bool {
    b.len() >= STAMP * 2 && b[..STAMP] == b[b.len() - STAMP..]
}

#[test]
fn leaf_read_while_write_cross_process() {
    let path = tmp("xp");
    cleanup(&path);

    // ── writer ハンドル (書き込み) ──
    let writer: Arc<Engine> = Engine::create_concurrent(&path).expect("create");
    let hid = {
        let w = unsafe { &mut *(Arc::as_ptr(&writer) as *mut Engine) };
        w.define_himo("body", ValueType::Leaf, 0);
        writer.himo_id("body").unwrap() as u16
    };
    let eids: Vec<_> = (0..N_ENTITIES).map(|_| writer.entity()).collect();
    for (e, &eid) in eids.iter().enumerate() {
        writer.tie_text_to_by_id(eid, hid, &body_for(0, e));
    }
    writer.flush_writes();

    // ── reader ハンドル (別 Engine、 readonly、 flock 非取得 = 別プロセス相当) ──
    let reader: Arc<Engine> = Arc::new(Engine::open_readonly(&path).expect("open_readonly"));
    assert!(reader.is_readonly());
    // reader は writer が焼いた既存 gen を共有 mmap から読める。
    assert!(
        reader.get_text_owned(eids[0], "body").is_some(),
        "readonly reader が leaf を読めない"
    );

    std::panic::set_hook(Box::new(|_| {}));

    let stop = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicU64::new(0));
    let corrupt = Arc::new(AtomicU64::new(0));
    let panicked = Arc::new(AtomicU64::new(0));

    // reader x3: 別ハンドル経由で読む。
    let readers: Vec<_> = (0..3)
        .map(|_| {
            let reader = reader.clone();
            let eids = eids.clone();
            let stop = stop.clone();
            let reads = reads.clone();
            let corrupt = corrupt.clone();
            let panicked = panicked.clone();
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    for &eid in &eids {
                        let reader = &reader;
                        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            reader.get_text_owned(eid, "body")
                        }));
                        reads.fetch_add(1, Ordering::Relaxed);
                        match r {
                            Ok(Some(b)) => {
                                if !head_matches_tail(&b) {
                                    corrupt.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Ok(None) => {}
                            Err(_) => {
                                panicked.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            })
        })
        .collect();

    // writer: churn。
    let deadline_writes = 2_000_000u64;
    let mut g = 1u64;
    let mut written = 0u64;
    while written < deadline_writes {
        for (e, &eid) in eids.iter().enumerate() {
            writer.tie_text_to_by_id(eid, hid, &body_for(g, e));
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
    eprintln!("== issue106 cross-process: {r} reads vs {written} writes ==\n  CORRUPT {c}  PANIC {p}");

    drop(reader);
    drop(writer);
    cleanup(&path);

    assert_eq!(
        c + p, 0,
        "cross-process (open_readonly) read-while-write で torn read を観測 (CORRUPT {c} / PANIC {p})"
    );
}
