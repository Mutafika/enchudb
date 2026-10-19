//! 壊れた / 途中で切れた `.oplog` を開いて読み直す (`OpLog::open` → `recover` / `recover_with_tail`)。 panic しない。
//! 入力の先頭 32 byte が header (magic・版・head・checkpoint・capacity)、 残りが record の並び。 magic と版は正しい値に
//! 置き換える (header の検査で毎回はじかれないように)。 capacity は file の長さ以下に丸めて渡すので、 head / checkpoint
//! の範囲の検査と record の scan まで届く。
#![no_main]
use enchudb_oplog::oplog::{OpLog, HEADER_SIZE, OPLOG_FILE_VERSION};
use libfuzzer_sys::fuzz_target;
use std::sync::atomic::{AtomicU64, Ordering};

static N: AtomicU64 = AtomicU64::new(0);

fuzz_target!(|data: &[u8]| {
    if data.len() < HEADER_SIZE {
        return;
    }
    let mut buf = data.to_vec();
    buf[0..4].copy_from_slice(b"EWAL");
    buf[4..8].copy_from_slice(&OPLOG_FILE_VERSION.to_le_bytes());
    let len = buf.len() as u64;
    let cap = u64::from_le_bytes(buf[24..32].try_into().unwrap());
    // capacity は file の長さ以下 (大きいと open が 「切れている」 と言って返るだけ)
    let cap = if cap > len { len } else { cap.max(HEADER_SIZE as u64) };
    buf[24..32].copy_from_slice(&cap.to_le_bytes());
    let dir = std::env::temp_dir().join(format!("enchudb-fuzz-oplog-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join(format!("{}.oplog", N.fetch_add(1, Ordering::Relaxed)));
    std::fs::write(&path, &buf).unwrap();
    if let Ok(log) = OpLog::open(&path) {
        let _ = log.recover();
        let _ = log.recover_with_tail();
    }
    let _ = std::fs::remove_file(&path);
});
