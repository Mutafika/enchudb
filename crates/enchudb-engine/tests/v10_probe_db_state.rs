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

    // header が指す segment が消えている: manifest があるなら 「後から消された」 = Damaged
    let gone = base("segment_gone");
    make_db(&gone);
    std::fs::remove_file(Path::new(&gone).join("himo/0000.seg")).unwrap();
    match Engine::probe(&gone) {
        DbState::Damaged(why) => assert!(why.contains("missing segment"), "理由が不親切: {why}"),
        other => panic!("Damaged を期待したが {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&gone);

    // segment は揃っていて manifest だけ無い = 開けるし中身もある。 ここで Incomplete と
    // 言うと 「消して作り直してよい」 と読まれてデータを消しかねないので Ready。
    let no_manifest = base("no_manifest");
    make_db(&no_manifest);
    std::fs::remove_file(Path::new(&no_manifest).join("segments")).unwrap();
    assert_eq!(Engine::probe(&no_manifest), DbState::Ready, "中身のある DB を Incomplete と言った");
    let _ = std::fs::remove_dir_all(&no_manifest);

    // segment が欠けていて manifest も無い = create の途中 (作り直してよい)
    let half = base("half_created");
    make_db(&half);
    std::fs::remove_file(Path::new(&half).join("segments")).unwrap();
    std::fs::remove_file(Path::new(&half).join("himo/0000.seg")).unwrap();
    assert_eq!(Engine::probe(&half), DbState::Incomplete, "create 途中を Incomplete と言わない");
    let _ = std::fs::remove_dir_all(&half);

    // writer が lock を握っている最中でも probe でき、 file を 1 つも増やさない
    // (sinfo の local peer が readonly で覗く経路と競合しないこと)
    let held = base("lock_held");
    let mut eng = Engine::create_with_capacity(&held, 1024).unwrap();
    eng.define_table("t", 100).unwrap();
    eng.flush().unwrap();
    let before: Vec<_> = std::fs::read_dir(&held).unwrap().map(|e| e.unwrap().file_name()).collect();
    assert_eq!(Engine::probe(&held), DbState::Ready, "writer が lock 中に probe できない");
    let after: Vec<_> = std::fs::read_dir(&held).unwrap().map(|e| e.unwrap().file_name()).collect();
    assert_eq!(before.len(), after.len(), "probe が file を作った");
    drop(eng);
    let _ = std::fs::remove_dir_all(&held);

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
