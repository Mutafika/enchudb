//! `Engine::probe` / `Engine::exists`: **open せずに** path の素性を言う。
//!
//! v10 は DB が directory なので、 consumer の `if path.exists() { open } else { create }` が
//! 「create の途中で落ちた半端な directory」 を既存 DB と誤認する。 sidecar 名を決め打ちで
//! 見に行かせないための入口 (消費側 sinfo からの要望)。

use enchudb_engine::{DbState, Engine, ValueType};
use std::path::Path;

fn base(tag: &str) -> String {
    let p = format!("/tmp/enchu_probe_{}_{tag}", std::process::id());
    let _ = std::fs::remove_dir_all(&p);
    let _ = std::fs::remove_file(&p);
    p
}

fn make_db(path: &str) {
    let mut eng = Engine::create_with_capacity(path, 1024).unwrap();
    eng.define_table("t", 100).unwrap();
    eng.define_himo_in("t", "n", ValueType::Number, 100).unwrap();
    let e = eng.entity_in("t").unwrap();
    eng.tie(e, "t.n", 42);
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
}

#[test]
fn probe_tells_missing_ready_incomplete_damaged_and_legacy_apart() {
    // Missing
    let missing = base("missing");
    assert_eq!(Engine::probe(&missing), DbState::Missing);
    assert!(!Engine::exists(&missing));

    // Ready
    let ready = base("ready");
    make_db(&ready);
    assert_eq!(Engine::probe(&ready), DbState::Ready);
    assert!(Engine::exists(&ready));

    // Incomplete: directory はあるが header.seg が無い (create の中断)
    let incomplete = base("incomplete");
    std::fs::create_dir_all(&incomplete).unwrap();
    assert_eq!(Engine::probe(&incomplete), DbState::Incomplete);
    assert!(!Engine::exists(&incomplete));

    // Damaged: segment を切り詰める
    let damaged = base("damaged");
    make_db(&damaged);
    let seg = Path::new(&damaged).join("himo/0000.seg");
    std::fs::OpenOptions::new().write(true).open(&seg).unwrap().set_len(0).unwrap();
    match Engine::probe(&damaged) {
        DbState::Damaged(why) => assert!(why.contains("truncated"), "理由が不親切: {why}"),
        other => panic!("Damaged を期待したが {other:?}"),
    }
    assert!(!Engine::exists(&damaged));
    // open も同じ判断をすること (probe と open がずれない)
    assert!(Engine::open(&damaged).is_err(), "probe が Damaged と言った DB が open できた");

    // 書きかけの manifest (end 行が無い / 行数が合わない) は 「無い」 扱い = 検証を飛ばす。
    // manifest は fsync していないので、 crash 後に中途半端な内容が残りうる。 そこで
    // 健全な DB を Damaged と誤判定する方が害が大きい。
    let torn = base("torn_manifest");
    make_db(&torn);
    let manifest = Path::new(&torn).join("segments");
    let full = std::fs::read_to_string(&manifest).unwrap();
    assert!(full.lines().last().unwrap().starts_with("end "), "end 行が無い: {full}");
    let cut: String =
        full.lines().take(3).map(|l| format!("{l}\n")).collect();
    std::fs::write(&manifest, &cut).unwrap();
    assert_eq!(Engine::probe(&torn), DbState::Ready, "書きかけ manifest で Damaged にした");
    assert!(Engine::open(&torn).is_ok(), "書きかけ manifest で open できなくなった");
    // 行数が合わない end 行も同じ扱い
    std::fs::write(&manifest, format!("{cut}end 99\n")).unwrap();
    assert_eq!(Engine::probe(&torn), DbState::Ready, "行数不一致の manifest で Damaged にした");
    let _ = std::fs::remove_dir_all(&torn);

    // SingleFileLegacy: v9 互換の 1 ファイル
    let src = base("legacy_src");
    make_db(&src);
    let packed = base("legacy.db");
    Engine::pack_dir(&src, Path::new(&packed)).unwrap();
    assert_eq!(Engine::probe(&packed), DbState::SingleFileLegacy);
    assert!(!Engine::exists(&packed));

    for p in [&ready, &incomplete, &damaged, &src] {
        let _ = std::fs::remove_dir_all(p);
    }
    let _ = std::fs::remove_file(&packed);
}
