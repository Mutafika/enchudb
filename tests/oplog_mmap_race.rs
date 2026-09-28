//! WAL-vs-mmap durability race の demo test。
//!
//! enchudb の write 順序: mmap write → wal.append (buffer only) → 後で consumer
//! が周期 fsync + msync。 この「buffer 段階」で crash + kernel が mmap dirty
//! page を先に disk に出した場合、 mmap 新値・ WAL 欠落で再起動 → 一見復元
//! されるが peer sync に乗らない silent loss が起きる。
//!
//! 通常運用では確率低い (consumer tick 100ms 内に kernel が dirty を出すのは
//! メモリ圧迫時くらい)。 ただし `crash_writer` の `mmap_ahead` scenario で
//! body_msync を明示的に呼ぶことで **deterministic に再現できる**。
//!
//! 用途: 既知 race を文書化 + 将来 fix した時の regression check。
//!
//! 走らせ方:
//!   cargo test --test oplog_mmap_race -- --ignored   (crash_writer は test が build する)
//!
//! `#[ignore]` で default skip。 fix 未着手の間は CI に流さない。

use std::path::PathBuf;
use std::process::{Command, Stdio};

fn tmp(name: &str) -> String {
    format!(
        "/tmp/enchudb-race-{}-{}-{}",
        name,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
    let _ = std::fs::remove_file(format!("{}.crc", path));
}

/// `crash_writer` (enchudb-engine の bin) の path。 root package の test からは cargo が
/// 他 crate の bin を作ってくれないので、 この test と同じ target dir / profile に 1 回だけ
/// `cargo build` する (事前 build 不要、 古い binary も作り直される、 #272)。
fn crash_writer_bin() -> PathBuf {
    static BIN: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    BIN.get_or_init(|| {
        // test binary は <target>/<profile>/deps/<name>、 bin は <target>/<profile>/<bin>
        let exe = std::env::current_exe().unwrap();
        let profile_dir = exe.parent().and_then(|d| d.parent()).unwrap().to_path_buf();
        let target_dir = profile_dir.parent().unwrap();
        let profile = match profile_dir.file_name().and_then(|s| s.to_str()) {
            Some("debug") | None => "dev",
            Some(p) => p,
        };
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
        let status = Command::new(cargo)
            .args(["build", "-p", "enchudb-engine", "--bin", "crash_writer", "--profile", profile])
            .arg("--target-dir")
            .arg(target_dir)
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .status()
            .expect("cargo build crash_writer");
        assert!(status.success(), "crash_writer の build に失敗: {status:?}");
        let bin = profile_dir.join("crash_writer");
        assert!(bin.exists(), "build したのに crash_writer が無い: {}", bin.display());
        bin
    })
    .clone()
}

/// **既知 race の demo**: mmap が WAL より disk 到達が先になるパターン。
///
/// 期待結果:
/// - mmap には value 42 が残る (body_msync 済)
/// - WAL には何も無い (fsync 前に abort)
/// - 結果: 単独 DB は復元できるが、 peer sync に乗らない silent loss
#[test]
#[ignore]
fn mmap_ahead_of_wal_silent_sync_loss() {
    let path = tmp("mmap_ahead");

    // Phase 1: schema を準備して flush (mmap も WAL も clean baseline)
    {
        let mut eng = enchudb::Engine::create_with_capacity(&path, 1024).unwrap();
        eng.define_himo("n", enchudb::ValueType::Number, 100);
        eng.flush().unwrap();
    }

    // Phase 2: subprocess で race を仕込んで abort
    let status = Command::new(crash_writer_bin())
        .args([&path, "mmap_ahead"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    // abort は exit code 134 (= 128 + SIGABRT 6)、 .success() は false
    assert!(
        !status.success(),
        "crash_writer should have aborted, got: {:?}",
        status
    );

    // Phase 3: 親 process で re-open し race の症状を確認
    let eng = enchudb::Engine::open_standalone(&path).unwrap();

    // mmap には value 42 が残ってる (body_msync 済 → kernel disk flush 済)
    let eids = eng.pull_raw("n", 42);
    assert!(
        !eids.is_empty(),
        "mmap should retain value 42 from msync, got {} eids",
        eids.len()
    );

    // WAL audit: 当該 op (Tie) は記録されてない (fsync 前に abort、 buffer のみ)
    // open_standalone は WAL を見ないので、 raw fs で WAL ファイル size 確認
    let oplog_path = format!("{}/oplog", path);
    if let Ok(meta) = std::fs::metadata(&oplog_path) {
        // WAL ヘッダ分 + checkpoint marker (前 phase の flush 由来) は載っているが、
        // mmap_ahead での Tie op は載ってないことを確認するため、 WAL を open して
        // recover() で record 数を見る。
        let wal = enchudb_oplog::oplog::OpLog::open(std::path::Path::new(&oplog_path)).unwrap();
        let records = wal.recover();
        // Tie op が含まれているか (=race が「閉じてる」= 期待外)
        let has_tie = records.iter().any(|r| {
            matches!(&r.op, enchudb_oplog::oplog::DecodedOp::Tie { .. })
        });
        assert!(
            !has_tie,
            "WAL should NOT have the Tie op (race demo failed: consumer fsync'd too fast). \
             wal_size={}, records={}",
            meta.len(),
            records.len()
        );
    }

    eprintln!(
        "race reproduced: mmap retains value 42 ({} eid(s)), WAL missing the op",
        eids.len()
    );

    cleanup(&path);
}

/// 比較対象: `oplog_sync()` を呼んだ後の abort なら mmap も WAL も整合的に残る。
/// (race は閉じる、 = explicit sync が機能している証明)
#[test]
#[ignore]
fn oplog_sync_closes_race_window() {
    let path = tmp("oplog_sync");

    {
        let mut eng = enchudb::Engine::create_with_capacity(&path, 1024).unwrap();
        eng.define_himo("n", enchudb::ValueType::Number, 100);
        eng.flush().unwrap();
    }

    // crash_writer の "normal" scenario は oplog_sync を呼ぶ
    let status = Command::new(crash_writer_bin())
        .args([&path, "normal", "1"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(status.success(), "normal exit should succeed");

    let eng = enchudb::Engine::open_standalone(&path).unwrap();
    let eids = eng.pull_raw("n", 0);
    assert!(!eids.is_empty(), "mmap should have value");

    let oplog_path = format!("{}/oplog", path);
    let wal = enchudb_oplog::oplog::OpLog::open(std::path::Path::new(&oplog_path)).unwrap();
    let records = wal.recover();
    let has_tie = records
        .iter()
        .any(|r| matches!(&r.op, enchudb_oplog::oplog::DecodedOp::Tie { .. }));
    // oplog_sync 経由なら durable に Tie が記録される
    assert!(
        has_tie || records.is_empty(),
        "oplog_sync path should be consistent (Tie in WAL or checkpoint already advanced)"
    );

    cleanup(&path);
}
