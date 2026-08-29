//! `snapshot_export` が DB を **肥大化させずに** 写すこと。
//!
//! v9 まで: 本体は sparse な巨大 1 ファイルで、 素の `std::fs::copy` は Linux で穴を 0 埋め
//! して apparent 全量を物理化した (この test はそれを固定していた)。
//!
//! v10 (request21): 本体は directory + segment file 群で、 各 segment は書いた分しか
//! 伸びない (unix は見かけも物理も)。 「穴を保つ」 という問題自体が消えたので、 この test は
//! **snapshot の総サイズが source と同じで、 かつ予約 (旧 apparent) より桁違いに小さい**
//! ことと、 中身が読めることを固定する。
#![cfg(unix)]

use enchudb_engine::{Engine, ValueType};

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-snapholes-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    for s in [".oplog", ".tables", ".tables.tmp", ".crc", ".lock", ".db.lock", ".eidmap", ".vocabmap", ".schema"] {
        let _ = std::fs::remove_file(format!("{}{}", path, s));
    }
}

/// directory 配下の file 長の合計 (= v10 の apparent)。
fn dir_bytes(path: &str) -> u64 {
    fn walk(p: &std::path::Path, acc: &mut u64) {
        for e in std::fs::read_dir(p).unwrap() {
            let e = e.unwrap();
            if e.file_type().unwrap().is_dir() {
                walk(&e.path(), acc);
            } else {
                *acc += e.metadata().unwrap().len();
            }
        }
    }
    let mut acc = 0;
    walk(std::path::Path::new(path), &mut acc);
    acc
}

#[test]
fn snapshot_export_does_not_bloat_the_copy() {
    let src = tmp("src");
    let dst = tmp("dst");
    cleanup(&src);
    cleanup(&dst);

    let cap = 512 * 1024u32;
    let mut eng = Engine::create_with_capacity(&src, cap).unwrap();
    eng.define_himo("age", ValueType::Number, 0);
    let mut eids = Vec::new();
    for i in 0..1000u32 {
        let e = eng.entity().unwrap();
        eng.tie_to(e, "age", i);
        eids.push(e);
    }
    eng.flush().unwrap();

    let src_bytes = dir_bytes(&src);
    // 旧 layout の apparent (cap 512K で数百 MB) より桁違いに小さいこと = segment 化の効果
    assert!(
        src_bytes < 32 * 1024 * 1024,
        "v10 の source が旧 apparent 級に膨らんでいる ({} bytes)",
        src_bytes,
    );

    eng.snapshot_export(&dst).expect("snapshot_export");
    let dst_bytes = dir_bytes(&dst);
    assert_eq!(dst_bytes, src_bytes, "snapshot の総サイズが source と違う (肥大化 or 欠落)");
    drop(eng);

    let restored = Engine::open_standalone(&dst).expect("snapshot が open できない");
    for (i, &e) in eids.iter().enumerate() {
        assert_eq!(restored.get(e, "age"), Some(i as u32), "snapshot の中身が壊れている (eid #{})", i);
    }
    drop(restored);
    cleanup(&src);
    cleanup(&dst);
}
