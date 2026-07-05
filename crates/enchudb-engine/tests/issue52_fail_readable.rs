//! issue #52: ENOSPC retry spam + sidecar 破損で全 DB 読み取り不能 problem の
//! engine 側 fail-readable / self-heal の regression test。
//!
//! 検証:
//! - `.tables` sidecar が破損していても `Engine::open_standalone` が成功する
//! - 破損 sidecar が `.tables.corrupt-<ts>` に rename される
//! - `.tables.tmp` 残骸が open 時に削除される (self-heal)

use enchudb_engine::Engine;
use std::fs;
use std::path::PathBuf;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue52-engine-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup_all(path: &str) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(format!("{}.oplog", path));
    let _ = fs::remove_file(format!("{}.tables", path));
    let _ = fs::remove_file(format!("{}.tables.tmp", path));
    let _ = fs::remove_file(format!("{}.crc", path));
    let _ = fs::remove_file(format!("{}.db.lock", path));
    let parent = std::path::Path::new(path).parent().unwrap_or(std::path::Path::new("/tmp"));
    let stem = std::path::Path::new(path).file_name().unwrap().to_string_lossy().to_string();
    if let Ok(entries) = fs::read_dir(parent) {
        for ent in entries.flatten() {
            let n = ent.file_name().to_string_lossy().to_string();
            if n.starts_with(&stem) && n.contains(".corrupt-") {
                let _ = fs::remove_file(ent.path());
            }
        }
    }
}

#[test]
fn open_recovers_from_corrupt_tables_sidecar() {
    let path = tmp_path("corrupt-tables");
    cleanup_all(&path);

    // 初期化: table 定義あり DB
    {
        let mut eng = Engine::create_growable_with_capacity(&path, 1000).unwrap();
        eng.define_table("widgets", 100).unwrap();
        eng.define_himo_in("widgets", "name", enchudb_engine::ValueType::Tag, 0).unwrap();
        eng.persist_tables().unwrap();
    }

    // sidecar を意図的に破損させる
    let tables_path = format!("{}.tables", path);
    fs::write(&tables_path, b"GARBAGE-NOT-VALID-TABLES-SIDECAR").unwrap();

    // open: fix なし → InvalidData silently ignored、 widgets table 失われる
    //       fix あり → warn + rename + anonymous fallback で open 成功、
    //                  widgets は失われる (engine 直 fallback) が DB 自体は open 可能
    let result = Engine::open_standalone(&path);
    assert!(
        result.is_ok(),
        "Engine::open_standalone should succeed even with corrupt tables sidecar, got: {:?}",
        result.err()
    );

    // .tables.corrupt-<ts> が作られたこと
    let parent = std::path::Path::new(&path).parent().unwrap_or(std::path::Path::new("/tmp"));
    let stem = std::path::Path::new(&path).file_name().unwrap().to_string_lossy().to_string();
    let backup_found = fs::read_dir(parent)
        .unwrap()
        .flatten()
        .any(|ent| {
            let n = ent.file_name().to_string_lossy().to_string();
            n.starts_with(&stem) && n.contains(".tables.corrupt-")
        });
    assert!(backup_found, "expected .tables.corrupt-<ts> backup file");

    // 元の `.tables` は今は anonymous tables の persist で上書きされてるはずなので OK
    cleanup_all(&path);
}

#[test]
fn open_self_heals_stale_tables_tmp() {
    let path = tmp_path("self-heal-tmp");
    cleanup_all(&path);

    // 初期化
    {
        let mut eng = Engine::create_growable_with_capacity(&path, 1000).unwrap();
        eng.define_table("widgets", 100).unwrap();
        eng.persist_tables().unwrap();
    }

    // `.tables.tmp` 残骸を意図的に置く (= persist 途中で crash した shape)
    let tmp_path: PathBuf = PathBuf::from(format!("{}.tables.tmp", path));
    fs::write(&tmp_path, b"residual garbage from a crashed persist").unwrap();
    assert!(tmp_path.exists());

    // open
    let _eng = Engine::open_standalone(&path).unwrap();

    // .tmp は削除されてる
    assert!(
        !tmp_path.exists(),
        "stale .tables.tmp should be removed at open, but it still exists"
    );

    cleanup_all(&path);
}
