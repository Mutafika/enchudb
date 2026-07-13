//! ② fault-injection — 破損した oplog を食わせても reopen が graceful か。
//!
//! `crash_recovery_compacts`（issue95_stress）は「正常な durable state を drop→reopen」
//! だけを見る。こっちは **oplog を 縮小 / tail zero 化 / byte-flip して壊した状態**で reopen し、
//!   - SIGBUS / panic せず、Ok（body から復旧）か clean Err のどちらか
//!   - Ok なら pull 結果が **書いた集合と完全一致**（phantom なし + 1 件も欠けない。
//!     body は msync 済みなので、開けた以上ロスは許されない）
//!   - zero-tail / flip は **最低 1 case は Ok** であること（全 case Err の vacuous pass を
//!     弾く。file 縮小は capacity guard の clean Err が仕様なので guard 対象外）
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

/// reopen が graceful であることを確認し、 Ok だったかを返す（caller の vacuity guard 用）。
/// Ok なら subset でなく**完全一致**を要求する: body は msync 済み = 破損は oplog tail
/// のみなので、 開けた以上 1 件も欠けてはいけない。
fn assert_graceful_reopen(path: &str, written: &HashMap<u32, u32>, ctx: &str) -> bool {
    match Engine::open_concurrent_with_oplog(path, OPLOG_CAP) {
        Ok(eng) => {
            // pull した eid は全て「書いた eid」かつ値が一致（phantom / 破損値なし）。
            let mut seen = 0usize;
            for v in 0..VMAX {
                for e in eng.pull_raw("v", v) {
                    let lid = eid_local(e);
                    assert_eq!(
                        written.get(&lid),
                        Some(&v),
                        "{ctx}: phantom/破損 eid {lid} が bucket {v} に出た"
                    );
                    seen += 1;
                }
            }
            // phantom なし + 件数一致 = 集合として完全一致（eid ごとに値は一意）。
            assert_eq!(
                seen,
                written.len(),
                "{ctx}: 復旧が不完全 — body msync 済みなのに {seen}/{} 件しか引けない",
                written.len()
            );
            drop(eng);
            true
        }
        Err(e) => {
            // clean Err も graceful（crash していない）。ただし全滅は caller で弾く。
            eprintln!("{ctx}: clean Err: {e:?}");
            false
        }
    }
}

/// oplog file を物理的に縮小 → reopen が壊れない。
///
/// WAL は固定容量 pre-allocate なので、 file の縮小は header の capacity 検査で
/// **clean Err になるのが仕様**（"WAL truncated: header capacity … exceeds file size"）。
/// ここでは「guard が全長で発火し crash しない」ことだけを見る。 復旧経路
/// （torn tail → body から復元）は `reopen_recovers_with_zeroed_wal_tail` が踏む。
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
        // 縮小 file は capacity guard の clean Err が期待値（Ok でも完全一致なら可）。
        assert_graceful_reopen(&path, &written, &format!("truncate {pct}%"));
        fresh(&path);
    }
    fresh(base);
}

/// WAL の tail を zero 化（file size 不変 = pre-allocate 済み WAL の torn write 模擬）→
/// reopen は record 検査（CRC32）で壊れた tail を捨て、 **body から完全復旧**する。
/// こちらが「復旧経路」の本体検証。
#[test]
fn reopen_recovers_with_zeroed_wal_tail() {
    let base = "/tmp/test_oplog_fault_zerotail_base.db";
    let written = make_durable(base, 5_000);
    let full = oplog_len(base);
    assert!(full > 0, "oplog が空");

    let mut ok_cases = 0usize;
    // header 直後〜record 領域の複数点から EOF まで 0 fill。 header 内に食い込む点は
    // clean Err で graceful、 header より後ろの点は Ok + 完全一致になるはず。
    for &from in &[64u64, 512, 4 * 1024, 64 * 1024, 256 * 1024] {
        if from >= full {
            continue;
        }
        let path = format!("/tmp/test_oplog_fault_zerotail_{from}.db");
        copy_db(base, &path);
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(format!("{path}.oplog"))
            .unwrap();
        f.seek(SeekFrom::Start(from)).unwrap();
        f.write_all(&vec![0u8; (full - from) as usize]).unwrap();
        f.sync_all().unwrap();
        drop(f);
        if assert_graceful_reopen(&path, &written, &format!("zerotail@{from}")) {
            ok_cases += 1;
        }
        fresh(&path);
    }
    // vacuity guard: 全 case Err だと「body から復旧」を一度も踏まずに green になる。
    assert!(ok_cases >= 1, "全 zero-tail case が Err — body 復旧経路が未検証 (vacuous pass)");
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
    let mut ok_cases = 0usize;
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
        if assert_graceful_reopen(&path, &written, &format!("flip@{off}")) {
            ok_cases += 1;
        }
        fresh(&path);
    }
    // vacuity guard: 少なくとも 1 case は Ok で「CRC 検知 → body から復旧」を実地に踏むこと。
    assert!(ok_cases >= 1, "全 flip case が Err — 復旧経路が未検証 (vacuous pass)");
    fresh(base);
}
