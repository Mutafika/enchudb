//! `snapshot_export` が DB body の **穴を潰さない**こと。
//!
//! # 何が壊れていたか
//!
//! DB body は 「apparent は巨大、 実データはごく一部」 な sparse ファイル。
//! 置いてあるだけなら穴は物理を消費しないが、 `std::fs::copy` は platform に
//! よって挙動が違う — macOS は clonefile で穴を維持するのに対し、 **Linux は
//! 穴を 0 で埋めて実際に書き出す**。
//!
//! そのため `snapshot_export` は Linux で apparent 全量を物理化していた。
//! 既定 capacity の DB なら 1 回の snapshot で 24 GB を書く。 CI (ubuntu) が
//! `No space left on device` で runner ごと落ちたのはこれが直接の原因。
//!
//! # この test について
//!
//! capacity は **わざと控えめ** (= apparent 1 GB 級) にしてある。 回帰したときに
//! 「assert が落ちる」で済ませるためで、 実 DB 相当の capacity にすると回帰時に
//! ディスクを食い潰して test runner ごと死ぬ (それが元の症状)。
//!
//! falsify: `snapshot_export` の `copy_sparse` を `std::fs::copy` に戻すと、
//! Linux でこの test が落ちる。 macOS は `fs::copy` が元々穴を維持するので
//! **落ちない — falsify は Linux で行うこと**。

#![cfg(unix)]

use enchudb_engine::{Engine, ValueType};
use std::os::unix::fs::MetadataExt;

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
    for s in ["", ".oplog", ".tables", ".tables.tmp", ".crc", ".lock", ".db.lock", ".eidmap", ".vocabmap", ".schema"] {
        let _ = std::fs::remove_file(format!("{}{}", path, s));
    }
}

fn apparent(path: &str) -> u64 {
    std::fs::metadata(path).unwrap().len()
}

fn physical(path: &str) -> u64 {
    std::fs::metadata(path).unwrap().blocks() * 512
}

#[test]
fn snapshot_export_does_not_materialize_the_holes() {
    let src = tmp("src");
    let dst = tmp("dst");
    cleanup(&src);
    cleanup(&dst);

    let mut eng = Engine::create_with_capacity(&src, 512 * 1024).unwrap();
    eng.define_himo("age", ValueType::Number, 0);
    let mut eids = Vec::new();
    for i in 0..1000u32 {
        let e = eng.entity();
        eng.tie_to(e, "age", i);
        eids.push(e);
    }
    eng.flush().unwrap();

    let src_apparent = apparent(&src);
    assert!(
        src_apparent > 256 * 1024 * 1024,
        "前提が崩れた: source が sparse な巨大ファイルでない ({} bytes)",
        src_apparent,
    );
    assert!(
        physical(&src) < src_apparent / 10,
        "前提が崩れた: source 自体が既に密 (physical {} / apparent {})",
        physical(&src),
        src_apparent,
    );

    eng.snapshot_export(&dst).expect("snapshot_export");

    assert_eq!(apparent(&dst), src_apparent, "snapshot の apparent size が違う");

    let phys = physical(&dst);
    assert!(
        phys < src_apparent / 10,
        "snapshot が穴を潰している: physical {} bytes / apparent {} bytes \
         (std::fs::copy は Linux で穴を 0 埋めする — copy_sparse を使うこと)",
        phys,
        src_apparent,
    );

    // 穴を飛ばしても中身は壊れていないこと。
    drop(eng);
    let restored = Engine::open_standalone(&dst).expect("snapshot が open できない");
    for (i, &e) in eids.iter().enumerate() {
        assert_eq!(
            restored.get(e, "age"),
            Some(i as u32),
            "snapshot の中身が壊れている (eid #{})",
            i,
        );
    }

    drop(restored);
    cleanup(&src);
    cleanup(&dst);
}
