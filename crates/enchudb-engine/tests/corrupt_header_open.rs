//! 0.9.0 (L1) regression: header CRC == 0 の legacy 経路 (v27 以前 DB 互換で
//! `verify_header_crc` が素通し) でも、 破損した himo_count / *_size / *_cap は
//! open 時に InvalidData で弾く (旧実装は panic / OOB region map まで進んだ)。

use enchudb_engine::Engine;
use std::io::ErrorKind;

// engine.rs のヘッダレイアウト定数 (private なので test 側に複製)
const H_MAX_HIMOS: u64 = 12;
const H_HIMO_COUNT: u64 = 16;
const H_VOCAB_INDEX_CAP: u64 = 24;
const H_VOCAB_DATA_SIZE: u64 = 28;
const H_HEADER_CRC: u64 = 64;

fn tmp(name: &str) -> String {
    let p = std::env::temp_dir().join(format!(
        "enchu-corrupt-header-{}-{}.db",
        name,
        std::process::id()
    ));
    let s = p.to_str().unwrap().to_string();
    for suffix in ["", ".oplog", ".crc", ".tables", ".eidmap", ".lock"] {
        let _ = std::fs::remove_file(format!("{s}{suffix}"));
    }
    s
}

fn make_db(path: &str) {
    let mut eng = Engine::create_compact(path).unwrap();
    let e = eng.entity();
    eng.tie(e, "age", 30);
}

fn patch(path: &str, offset: u64, bytes: &[u8]) {
    use std::io::{Seek, SeekFrom, Write};
    let mut f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.seek(SeekFrom::Start(offset)).unwrap();
    f.write_all(bytes).unwrap();
    f.sync_all().unwrap();
}

/// header CRC を 0 にして legacy (v27 以前) 経路に落とす。
fn zero_header_crc(path: &str) {
    patch(path, H_HEADER_CRC, &0u32.to_le_bytes());
}

fn expect_open_err(path: &str, ctx: &str) -> std::io::Error {
    match Engine::open_standalone(path) {
        Err(e) => e,
        Ok(_) => panic!("{ctx}: expected open error, got Ok"),
    }
}

#[test]
fn legacy_crc_zero_with_intact_fields_still_opens() {
    // 過剰検査で正常な legacy DB を弾いていないことの control
    let path = tmp("legacy-ok");
    make_db(&path);
    zero_header_crc(&path);
    let eng = Engine::open_standalone(&path).unwrap();
    assert_eq!(eng.entity_count(), 1);
    drop(eng);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn corrupt_himo_count_is_invalid_data() {
    let path = tmp("himo-count");
    make_db(&path);
    zero_header_crc(&path);
    // himo_count を max_himos (create_compact は 64) 超えに
    patch(&path, H_HIMO_COUNT, &1_000_000u32.to_le_bytes());
    let err = expect_open_err(&path, "himo_count > max_himos");
    assert_eq!(err.kind(), ErrorKind::InvalidData, "got: {err}");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn corrupt_vocab_index_cap_zero_is_invalid_data() {
    let path = tmp("index-cap");
    make_db(&path);
    zero_header_crc(&path);
    patch(&path, H_VOCAB_INDEX_CAP, &0u32.to_le_bytes());
    let err = expect_open_err(&path, "vocab_index_cap == 0");
    assert_eq!(err.kind(), ErrorKind::InvalidData, "got: {err}");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn corrupt_vocab_data_size_huge_is_invalid_data() {
    let path = tmp("vocab-size");
    make_db(&path);
    zero_header_crc(&path);
    // u32::MAX 超の data size (u32 data_end format limit 違反 + usize wrap 誘発値)
    patch(&path, H_VOCAB_DATA_SIZE, &u64::MAX.to_le_bytes());
    let err = expect_open_err(&path, "vocab_data_size huge");
    assert_eq!(err.kind(), ErrorKind::InvalidData, "got: {err}");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn corrupt_max_himos_huge_is_invalid_data() {
    let path = tmp("max-himos");
    make_db(&path);
    zero_header_crc(&path);
    // layout の himo cluster が file size を大きく超える値 → truncation 検出で弾く
    patch(&path, H_MAX_HIMOS, &u32::MAX.to_le_bytes());
    let err = expect_open_err(&path, "max_himos huge");
    assert_eq!(err.kind(), ErrorKind::InvalidData, "got: {err}");
    let _ = std::fs::remove_file(&path);
}
