//! sync 参加 DB の版数列 (per-cell version column + tombstone column) の存在条件。
//!
//! 歴史: 0.20.0 は writer open で **無条件に** v9 化 (apparent ×3.6)。 request18 (#173 /
//! 0.25.0) で sync tables を持つ DB だけに絞り、 `enable_sync_tables()` は file を伸ばして
//! flag を立てるだけ (column は次の open から = #243 の窓) だった。
//!
//! v10 (request21): 版数列は独立 segment (`ver/*.seg` / `tomb.seg`) なので
//! `enable_sync_tables()` が **その場で** 生やす。 窓は無い。 writer open の回収路は
//! 「sync tables があるのに版数列が無い DB」 (segment を作った後・flag を立てる前の crash) を
//! 拾う。 この file はその 3 点を固定する:
//!
//! 1. sync しない DB は open しても版数列を持たない (request18 の主眼)
//! 2. `enable_sync_tables()` で即座に版数列が生え、 reopen 後も残る
//! 3. flag を取りこぼした sync DB は writer open で回収される (既存 cell の版数は ZERO のまま = A-1)

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::Hlc;

fn tmp(tag: &str) -> String {
    format!("/tmp/enchudb-v9auto-{}-{}", tag, std::process::id())
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    for s in [".oplog", ".tables", ".tables.tmp", ".crc", ".lock", ".eidmap", ".vocabmap", ".schema"] {
        let _ = std::fs::remove_file(format!("{path}{s}"));
    }
}

fn hlc(w: u64, p: u32) -> Hlc {
    Hlc { wall: w, logical: 0, peer: p }
}

/// header の `H_CELL_VERSION` (offset 88) を落とす = 「segment は作ったが flag を立てる前に
/// 落ちた sync DB」 の再現。 v10 では header は `{path}/header.seg`。
fn clear_cell_version_flag(path: &str) {
    use std::io::{Read, Seek, SeekFrom, Write};
    const HEADER_SIZE: usize = 4096;
    const H_CELL_VERSION: usize = 88;
    const H_HEADER_CRC: usize = 64;
    let hp = format!("{path}/header.seg");
    let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&hp).unwrap();
    let mut buf = vec![0u8; HEADER_SIZE];
    f.read_exact(&mut buf).unwrap();
    buf[H_CELL_VERSION..H_CELL_VERSION + 4].copy_from_slice(&0u32.to_le_bytes());
    // header CRC は FNV-1a 32bit over [0, H_HEADER_CRC)
    let mut h: u32 = 2166136261;
    for b in &buf[..H_HEADER_CRC] {
        h ^= *b as u32;
        h = h.wrapping_mul(16777619);
    }
    buf[H_HEADER_CRC..H_HEADER_CRC + 4].copy_from_slice(&h.to_le_bytes());
    f.seek(SeekFrom::Start(0)).unwrap();
    f.write_all(&buf).unwrap();
    f.sync_all().unwrap();
}

fn ver_segment_count(path: &str) -> usize {
    std::fs::read_dir(format!("{path}/ver")).map(|d| d.count()).unwrap_or(0)
}

/// 1. sync しない DB は writer open しても版数列を持たない。
#[test]
fn plain_db_is_not_migrated_on_open() {
    let path = tmp("plain");
    cleanup(&path);
    {
        let mut eng = Engine::create_without_cell_version(&path, 4096).unwrap();
        eng.define_himo("age", ValueType::Number, 0);
        let e = eng.entity().unwrap();
        eng.tie_to(e, "age", 7);
        eng.flush().unwrap();
    }
    let eng = Engine::open_standalone(&path).unwrap();
    assert!(!eng.has_cell_version(), "sync しない DB が v9 化された");
    assert_eq!(eng.get(enchudb_oplog::make_eid(0, 0), "age"), Some(7));
    drop(eng);
    assert_eq!(ver_segment_count(&path), 0, "sync しない DB に版数 segment ができた");
    assert!(!std::path::Path::new(&format!("{path}/tomb.seg")).exists());
    cleanup(&path);
}

/// 2. `enable_sync_tables()` はその場で版数列を生やし、 そのセッションの版数が reopen 後も残る。
#[test]
fn enable_sync_tables_grows_version_columns_immediately() {
    let path = tmp("enable");
    cleanup(&path);
    let (eid, hid) = {
        let mut eng = Engine::create_without_cell_version(&path, 4096).unwrap();
        eng.define_himo("age", ValueType::Number, 0);
        let hid = eng.himo_id("age").unwrap() as u16;
        let e = eng.entity().unwrap();
        eng.tie_to(e, "age", 7);
        assert!(!eng.has_cell_version());
        eng.enable_sync_tables().unwrap();
        assert!(eng.has_cell_version(), "enable_sync_tables が版数列をその場で生やしていない");
        // sync tables 自身の himo も含めて、 定義済み himo 全部に版数列が付く
        assert_eq!(ver_segment_count(&path), eng.himo_count(), "定義済み himo ぶんの ver segment が無い");
        assert!(std::path::Path::new(&format!("{path}/tomb.seg")).exists());
        assert!(eng.set_cell(e, hid, 8, hlc(200, 1)));
        assert_eq!(eng.cell_hlc(e, hid), hlc(200, 1));
        eng.flush().unwrap();
        (e, hid)
    };
    let eng = Engine::open_standalone(&path).unwrap();
    assert!(eng.has_cell_version(), "reopen で版数列を見失った");
    assert!(eng.sync_tables_enabled());
    assert_eq!(eng.cell_hlc(eid, hid), hlc(200, 1), "有効化セッションの版数が reopen で消えた (#243)");
    assert_eq!(eng.get(eid, "age"), Some(8));
    // 有効化後に足した himo にも版数列が付く
    drop(eng);
    let mut eng = Engine::open_standalone(&path).unwrap();
    let before = ver_segment_count(&path);
    eng.define_himo("name", ValueType::Number, 0);
    assert_eq!(ver_segment_count(&path), before + 1, "後から足した himo の ver segment が無い");
    drop(eng);
    cleanup(&path);
}

/// 3. flag を取りこぼした sync DB は writer open で回収される。 既存 cell の版数は ZERO の
/// まま (A-1: 版数不明 = 現状維持)、 新しい write からは版数が載る。
#[test]
fn sync_db_missing_the_flag_is_repaired_on_writer_open() {
    let path = tmp("repair");
    cleanup(&path);
    let (eid, hid) = {
        let mut eng = Engine::create_without_cell_version(&path, 4096).unwrap();
        eng.define_himo("age", ValueType::Number, 0);
        let hid = eng.himo_id("age").unwrap() as u16;
        let e = eng.entity().unwrap();
        eng.tie_to(e, "age", 7);
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
        (e, hid)
    };
    clear_cell_version_flag(&path);

    // readonly は回収しない (共有 mapping を書かない契約)
    {
        let ro = Engine::open_readonly(&path).unwrap();
        assert!(!ro.has_cell_version(), "readonly open が版数列を生やした");
    }
    let eng = Engine::open_standalone(&path).unwrap();
    assert!(eng.has_cell_version(), "sync DB の writer open で版数列が回収されていない");
    assert_eq!(eng.get(eid, "age"), Some(7), "回収で既存データが壊れた");
    assert_eq!(eng.cell_hlc(eid, hid), Hlc::ZERO, "既存 cell に版数が付いてしまった (A-1 違反)");
    assert!(eng.set_cell(eid, hid, 9, hlc(100, 1)), "版数不明 cell への write が弾かれた");
    assert_eq!(eng.cell_hlc(eid, hid), hlc(100, 1));
    drop(eng);
    // 二度目の open は no-op (flag が立っているので)
    let eng = Engine::open_standalone(&path).unwrap();
    assert!(eng.has_cell_version());
    assert_eq!(eng.cell_hlc(eid, hid), hlc(100, 1));
    drop(eng);
    cleanup(&path);
}
