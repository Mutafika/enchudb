//! ② fault-injection — 破損した oplog を食わせても reopen が graceful か。
//!
//! `crash_recovery_compacts`（issue95_stress）は「正常な durable state を drop→reopen」
//! だけを見る。こっちは **oplog を truncate / byte-flip して壊した状態**で reopen し、
//!   - SIGBUS / panic せず、Ok（body から復旧）か clean Err のどちらか
//!   - Ok なら pull 結果が **書いた集合の subset**（phantom eid なし・値正しい）
//! を要求する。oplog の線形 parse（CRC32 per record）が破損 tail を安全に扱えるかの網羅。
//!
//! 注: `oplog_sync` で body は msync 済みなので、reopen は body から全 state を復元できる
//! はず。よって truncation は「壊れた oplog tail を parse しても壊れない」ことの検証。
//! body sync 前の crash（未 checkpoint tail の replay）は subprocess crash が要るので別 follow-up。

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::eid_local;
use std::collections::HashMap;
use std::sync::Arc;

const VMAX: u32 = 50;
const OPLOG_CAP: usize = 64 * 1024 * 1024;

fn fresh(path: &str) {
    for suf in ["", ".oplog", ".lock", ".tables", ".crc"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

fn define(eng: &Arc<Engine>, name: &str) -> u16 {
    let eng_mut = unsafe { &mut *(Arc::as_ptr(eng) as *mut Engine) };
    eng_mut.define_himo(name, ValueType::Number, 0);
    eng.himo_id(name).unwrap() as u16
}

/// durable な DB を作り、書いた (eid_local -> value) を返す。
fn make_durable(path: &str, n: u32) -> HashMap<u32, u32> {
    fresh(path);
    let eng: Arc<Engine> = Engine::create_concurrent_with_oplog(path, OPLOG_CAP).expect("create");
    let vhid = define(&eng, "v");
    let mut written = HashMap::new();
    for i in 0..n {
        let e = eng.entity();
        let v = i % VMAX;
        eng.tie_async_by_id(e, vhid, v);
        written.insert(eid_local(e), v);
    }
    eng.flush_writes();
    eng.oplog_sync().expect("durable");
    drop(eng);
    written
}

fn copy_db(src: &str, dst: &str) {
    fresh(dst);
    std::fs::copy(src, dst).expect("copy body");
    std::fs::copy(format!("{src}.oplog"), format!("{dst}.oplog")).expect("copy oplog");
}

fn oplog_len(path: &str) -> u64 {
    std::fs::metadata(format!("{path}.oplog")).map(|m| m.len()).unwrap_or(0)
}

/// reopen が graceful（Ok なら subset 一貫）であることを確認。
fn assert_graceful_reopen(path: &str, written: &HashMap<u32, u32>, ctx: &str) {
    match Engine::open_concurrent_with_oplog(path, OPLOG_CAP) {
        Ok(eng) => {
            // pull した eid は全て「書いた eid」かつ値が一致（phantom / 破損値なし）。
            for v in 0..VMAX {
                for e in eng.pull_raw("v", v) {
                    let lid = eid_local(e);
                    assert_eq!(
                        written.get(&lid),
                        Some(&v),
                        "{ctx}: phantom/破損 eid {lid} が bucket {v} に出た"
                    );
                }
            }
            drop(eng);
        }
        Err(_) => { /* clean Err も graceful（crash していない）。許容 */ }
    }
}

/// oplog を様々な長さに truncate → reopen が壊れない。
#[test]
fn reopen_survives_oplog_truncation() {
    let base = "/tmp/test_oplog_fault_trunc_base.db";
    let written = make_durable(base, 5_000);
    let full = oplog_len(base);
    assert!(full > 0, "oplog が空");

    for pct in [0u64, 10, 33, 50, 75, 90, 99] {
        let path = format!("/tmp/test_oplog_fault_trunc_{pct}.db");
        copy_db(base, &path);
        let new_len = full * pct / 100;
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(format!("{path}.oplog"))
            .unwrap();
        f.set_len(new_len).unwrap();
        f.sync_all().unwrap();
        drop(f);
        assert_graceful_reopen(&path, &written, &format!("truncate {pct}%"));
        fresh(&path);
    }
    fresh(base);
}

/// oplog の各所を byte-flip → CRC32 検知で reopen が壊れない。
#[test]
fn reopen_survives_oplog_byte_flips() {
    let base = "/tmp/test_oplog_fault_flip_base.db";
    let written = make_durable(base, 5_000);
    let full = oplog_len(base);
    assert!(full > 0, "oplog が空");

    // header 近傍・中盤・終盤の代表点を反転（deterministic）。
    for &off in &[0u64, 4, 16, 64, full / 4, full / 2, full * 3 / 4, full.saturating_sub(8)] {
        if off >= full {
            continue;
        }
        let path = format!("/tmp/test_oplog_fault_flip_{off}.db");
        copy_db(base, &path);
        // 1 byte を XOR 0xFF で反転。
        use std::io::{Read, Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(format!("{path}.oplog"))
            .unwrap();
        f.seek(SeekFrom::Start(off)).unwrap();
        let mut b = [0u8; 1];
        f.read_exact(&mut b).unwrap();
        b[0] ^= 0xFF;
        f.seek(SeekFrom::Start(off)).unwrap();
        f.write_all(&b).unwrap();
        f.sync_all().unwrap();
        drop(f);
        assert_graceful_reopen(&path, &written, &format!("flip@{off}"));
        fresh(&path);
    }
    fresh(base);
}
