//! 0.9.0 (L1) regression: truncated / header 破損 .oplog は open 時に
//! InvalidData で弾く。 旧実装は header の capacity/head/checkpoint を
//! 実 file 長と突き合わせず、 truncated WAL が正常 open → append で
//! mmap OOB panic した。

use enchudb_oplog::oplog::{Op, OpLog, HEADER_SIZE};
use std::io::ErrorKind;
use std::path::PathBuf;

const CAP: usize = 64 * 1024;

fn tmp(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "enchudb-truncated-oplog-{}-{}.oplog",
        name,
        std::process::id()
    ));
    let _ = std::fs::remove_file(&p);
    p
}

fn create_with_records(path: &PathBuf) {
    let wal = OpLog::create(path, CAP).unwrap();
    wal.append(Op::Tie { eid: 1, himo_id: 0, value: 42 }).unwrap();
    wal.append(Op::Commit).unwrap();
    wal.fsync().unwrap();
}

fn set_header_u64(path: &PathBuf, offset: u64, value: u64) {
    use std::io::{Seek, SeekFrom, Write};
    let mut f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.seek(SeekFrom::Start(offset)).unwrap();
    f.write_all(&value.to_le_bytes()).unwrap();
    f.sync_all().unwrap();
}

/// OpLog は Debug ではないので unwrap_err が使えない — Err だけ取り出す helper。
fn expect_err<T>(r: std::io::Result<T>, ctx: &str) -> std::io::Error {
    match r {
        Err(e) => e,
        Ok(_) => panic!("{ctx}: expected error, got Ok"),
    }
}

#[test]
fn reopen_intact_oplog_is_ok() {
    // control: 無傷の WAL は開けて record も読める
    let path = tmp("intact");
    create_with_records(&path);
    let wal = OpLog::open(&path).unwrap();
    let recs = wal.recover();
    assert_eq!(recs.len(), 1);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn truncated_oplog_open_is_invalid_data() {
    let path = tmp("truncated");
    create_with_records(&path);
    // crash / 不完全 copy simulate: capacity の半分に切り詰める
    let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    f.set_len((CAP / 2) as u64).unwrap();
    drop(f);

    let err = expect_err(OpLog::open(&path), "truncated open");
    assert_eq!(err.kind(), ErrorKind::InvalidData, "got: {err}");
    assert!(err.to_string().contains("truncated"), "got: {err}");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn tiny_truncated_oplog_open_is_invalid_data() {
    // header 未満まで truncate (16 bytes)
    let path = tmp("tiny");
    create_with_records(&path);
    let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    f.set_len((HEADER_SIZE / 2) as u64).unwrap();
    drop(f);

    let err = expect_err(OpLog::open(&path), "corrupt open");
    assert_eq!(err.kind(), ErrorKind::InvalidData, "got: {err}");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn corrupt_head_beyond_capacity_is_invalid_data() {
    let path = tmp("head-oob");
    create_with_records(&path);
    // head を capacity 超えに書き換え (append 位置が OOB になる値)
    set_header_u64(&path, 8, (CAP + 4096) as u64);

    let err = expect_err(OpLog::open(&path), "corrupt open");
    assert_eq!(err.kind(), ErrorKind::InvalidData, "got: {err}");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn corrupt_checkpoint_below_header_is_invalid_data() {
    let path = tmp("cp-low");
    create_with_records(&path);
    // checkpoint を header 内 (= 不正) に書き換え
    set_header_u64(&path, 16, 4);

    let err = expect_err(OpLog::open(&path), "corrupt open");
    assert_eq!(err.kind(), ErrorKind::InvalidData, "got: {err}");
    let _ = std::fs::remove_file(&path);
}
