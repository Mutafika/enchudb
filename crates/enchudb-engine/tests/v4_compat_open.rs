//! v4 ファイルフォーマットを v5 engine で open できる後方互換テスト。
//!
//! step 1: v5 への version bump 時の compat assert。 v4 DB は anonymous table
//! 1 個に migrate されて open される (= 旧 flat 空間と同じ動作)。
//!
//! テスト戦略:
//!   1. 通常通り v5 DB を作って data を 入れて flush
//!   2. close 後に header の version bytes を 5 → 4 に書き換え + header CRC 再計算
//!   3. 再 open
//!   4. data がそのまま読めることを assert
//!
//! 注: 現時点 (step 1) では engine 内部に table 概念は無いので、 v4 と v5 の
//! 中身は実質同一。 step 2 以降で table data が v5 のみ存在する形になっても、
//! v4 DB は "tables なし = anonymous のみ" として open される必要がある。

use enchudb_engine::{Engine, HimoType};
use std::io::{Read, Seek, SeekFrom, Write};

const H_VERSION_OFFSET: u64 = 4;
const H_HEADER_CRC_OFFSET: u64 = 64;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-v4-compat-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suffix in ["", ".oplog", ".crc", ".db.lock"] {
        let _ = std::fs::remove_file(format!("{}{}", path, suffix));
    }
}

/// header [0..64) を FNV-1a 32bit で hash。 engine.rs の compute_header_crc と
/// 同じアルゴリズム (test 用に再実装)。
fn compute_header_crc(header: &[u8]) -> u32 {
    let mut h: u32 = 0x811c9dc5;
    for &b in &header[0..64] {
        h ^= b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    h
}

/// v5 DB の header を v4 に書き換える。 H_VERSION (offset 4) を 4 に、 header
/// CRC (offset 64) を再計算して overwrite。
fn rewrite_header_as_v4(path: &str) {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open DB");

    // header 4096 bytes を読む
    let mut header = vec![0u8; 4096];
    file.seek(SeekFrom::Start(0)).unwrap();
    file.read_exact(&mut header).unwrap();

    // version field を 4 に
    header[H_VERSION_OFFSET as usize..H_VERSION_OFFSET as usize + 4]
        .copy_from_slice(&4u32.to_le_bytes());

    // CRC 再計算 (range [0..64), version 含む)
    let new_crc = compute_header_crc(&header);
    header[H_HEADER_CRC_OFFSET as usize..H_HEADER_CRC_OFFSET as usize + 4]
        .copy_from_slice(&new_crc.to_le_bytes());

    // 書き戻す
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&header).unwrap();
    file.sync_all().unwrap();
}

#[test]
fn v4_db_opens_via_legacy_path() {
    let path = tmp_path("legacy_open");
    cleanup(&path);

    // 1. v5 で作る
    {
        let mut eng = Engine::create_standalone(&path).unwrap();
        eng.define_himo("age", HimoType::Number, 100);
        let e1 = eng.entity();
        let e2 = eng.entity();
        eng.tie(e1, "age", 30);
        eng.tie(e2, "age", 25);
        eng.flush().unwrap();
    }

    // 2. version を v4 に書き換える
    rewrite_header_as_v4(&path);

    // 3. v4 compat path で open できることを assert
    let eng = Engine::open_standalone(&path).expect("v4 compat open should succeed");
    let rows = eng.pull_raw("age", 30);
    assert_eq!(rows.len(), 1, "age=30 で 1 entity 引けるはず");
    let rows2 = eng.pull_raw("age", 25);
    assert_eq!(rows2.len(), 1, "age=25 で 1 entity 引けるはず");

    drop(eng);
    cleanup(&path);
}

#[test]
fn v4_db_gradual_migration_to_tables() {
    // β-light step 8: v4 DB を open → 旧 data 読める + 新 table を追加して
    // reopen → 旧 legacy + 新 tables が共存する。 段階移行のシナリオ。
    let path = tmp_path("v4_migrate");
    cleanup(&path);

    // 1. v5 で作って data 入れる
    {
        let mut eng = Engine::create_standalone(&path).unwrap();
        eng.define_himo("legacy_age", HimoType::Number, 100);
        let e = eng.entity();
        eng.tie(e, "legacy_age", 42);
        eng.flush().unwrap();
    }

    // 2. header を v4 に書き換え (= v4 DB simulation) + sidecar 削除
    rewrite_header_as_v4(&path);
    let _ = std::fs::remove_file(format!("{}.tables", path));

    // 3. v0.5.x で open → legacy data 読める + tables 追加
    {
        let mut eng = Engine::open_standalone(&path).expect("v4 compat open");
        // 旧 data
        assert_eq!(eng.pull_raw("legacy_age", 42).len(), 1);

        // 新 table を追加
        eng.define_table("users", 100).unwrap();
        eng.define_himo_in("users", "age", HimoType::Number, 100).unwrap();
        let alice = eng.entity_in("users").unwrap();
        eng.tie(alice, "users.age", 30);
        eng.flush().unwrap();
    }

    // 4. reopen → legacy + 新 table 共存
    {
        let eng = Engine::open_standalone(&path).unwrap();
        // 旧 himo 残ってる
        assert_eq!(eng.pull_raw("legacy_age", 42).len(), 1);
        // 新 himo も復元
        assert_eq!(eng.pull_raw("users.age", 30).len(), 1);
        // tables 復元
        let tables = eng.list_tables();
        assert!(tables.iter().any(|(_, n, _, _)| n == "users"));
    }

    cleanup(&path);
}

#[test]
fn unknown_version_is_rejected() {
    let path = tmp_path("unknown_version");
    cleanup(&path);

    // v5 で作る
    {
        let mut eng = Engine::create_standalone(&path).unwrap();
        eng.define_himo("v", HimoType::Number, 10);
        eng.flush().unwrap();
    }

    // version を 99 (未来 / 未知) に書き換え
    {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut header = vec![0u8; 4096];
        file.seek(SeekFrom::Start(0)).unwrap();
        file.read_exact(&mut header).unwrap();
        header[H_VERSION_OFFSET as usize..H_VERSION_OFFSET as usize + 4]
            .copy_from_slice(&99u32.to_le_bytes());
        let new_crc = compute_header_crc(&header);
        header[H_HEADER_CRC_OFFSET as usize..H_HEADER_CRC_OFFSET as usize + 4]
            .copy_from_slice(&new_crc.to_le_bytes());
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&header).unwrap();
        file.sync_all().unwrap();
    }

    // 未知 version は弾かれる
    let result = Engine::open_standalone(&path);
    let err_msg = match result {
        Ok(_) => panic!("unknown version は reject されるべき"),
        Err(e) => e.to_string(),
    };
    assert!(
        err_msg.contains("unsupported"),
        "error message に unsupported が含まれるべき: got {}",
        err_msg
    );

    cleanup(&path);
}
