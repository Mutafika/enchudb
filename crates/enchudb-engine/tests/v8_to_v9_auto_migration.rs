//! v8 以前の DB を writer open したとき、 **自動で v9 領域が生える**こと。
//!
//! # なぜ自動移行が要るか
//!
//! 版数 (= その cell がいつ書かれたか) を持たない DB は LWW の判定材料が無いので、
//! #154 / #160 の巻き戻りを抱えたままになり、 anti-entropy (Phase 2) も効かない。
//! 「新機能の恩恵を受けられない DB が永久に残る」 のは DB として筋が悪い。
//!
//! v9 領域は variable cluster の **末尾**にあり、 それより手前の region offset は
//! `cell_version` の真偽で 1 byte も変わらない (request17 step 1 の設計)。 つまり
//! 移行は 「ファイルを伸ばして header の flag を立てる」 だけで、 データの移動が
//! 一切要らない。 それなら手動 migration を強いる理由が無いので writer open で
//! 自動的に行う (#123 の vocab index migration と同じ方針)。
//!
//! # このテストが固定すること
//!
//! **やること**: 領域が生える / 既存データが無傷 / 以後の書き込みに版数が付く /
//! 冪等 / readonly では起きない
//!
//! **やらないこと**: 移行しただけでは過去の巻き戻りは直らない (版数不明のまま)。
//! これは仕様なので、 誤って 「直った」 と読まないよう明示的に固定する。

use enchudb_engine::engine::Engine;
use enchudb_engine::ValueType;
use enchudb_oplog::Hlc;

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-v9mig-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(p: &str) {
    for s in ["", ".oplog", ".tables", ".tables.tmp", ".crc", ".lock", ".db.lock", ".eidmap", ".vocabmap", ".schema"] {
        let _ = std::fs::remove_file(format!("{}{}", p, s));
    }
}

fn hlc(w: u64, p: u32) -> Hlc {
    Hlc { wall: w, logical: 0, peer: p }
}

/// v9 領域を持たない DB を作り、 値を 3 つ入れて閉じる。
fn make_pre_v9(path: &str) -> u16 {
    cleanup(path);
    let mut eng = Engine::create_without_cell_version(path, 4096).unwrap();
    eng.define_himo("age", ValueType::Number, 0);
    let hid = eng.himo_id("age").unwrap() as u16;
    assert!(!eng.has_cell_version(), "前提が崩れた: v9 領域を持っている");
    for i in 0..3u32 {
        let e = eng.entity().unwrap();
        eng.tie_to(e, "age", 100 + i);
    }
    eng.flush().unwrap();
    hid
}

/// 本題: writer open で v9 領域が生え、 **既存データは無傷**。
#[test]
fn writer_open_grows_v9_regions_and_keeps_the_data() {
    let path = tmp("grow");
    make_pre_v9(&path);
    let before_size = std::fs::metadata(&path).unwrap().len();

    let eng = Engine::open_standalone(&path).unwrap();
    assert!(eng.has_cell_version(), "writer open で v9 領域が生えていない");

    let after_size = std::fs::metadata(&path).unwrap().len();
    assert!(
        after_size > before_size,
        "ファイルが伸びていない ({} -> {})",
        before_size,
        after_size,
    );

    // 既存データが読める (= 手前の region が 1 byte もずれていない)
    for i in 0..3u32 {
        let eid = enchudb_oplog::make_eid(0, i);
        assert_eq!(eng.get(eid, "age"), Some(100 + i), "移行で既存データが壊れた (eid {})", i);
    }
    drop(eng);
    cleanup(&path);
}

/// 移行後の write には版数が付く (= 移行が「有効化」として機能している)。
#[test]
fn writes_after_migration_record_versions() {
    let path = tmp("record");
    let hid = make_pre_v9(&path);

    let eng = Engine::open_standalone(&path).unwrap();
    let eid = enchudb_oplog::make_eid(0, 0);
    assert_eq!(eng.cell_hlc(eid, hid), Hlc::ZERO, "移行直後は版数不明のはず");

    assert!(eng.set_cell(eid, hid, 777, hlc(5000, 1)), "書き込めていない");
    assert_eq!(eng.cell_hlc(eid, hid), hlc(5000, 1), "版数が記録されていない");
    assert_eq!(eng.get(eid, "age"), Some(777));

    // 版数が入った後は、 古い record が弾かれる (= LWW が効き始めた)
    assert!(!eng.set_cell(eid, hid, 111, hlc(4000, 1)), "古い write を受け入れた");
    assert_eq!(eng.get(eid, "age"), Some(777));
    drop(eng);
    cleanup(&path);
}

/// **移行しただけでは過去の巻き戻りは直らない。** 仕様なので明示的に固定する。
///
/// 移行直後の cell は版数不明 = 比較材料が無い = 何でも受け入れる (A-1)。
/// 「migration したから直った」 と誤読されないための杭。
#[test]
fn migration_alone_does_not_retroactively_protect_existing_cells() {
    let path = tmp("noretro");
    let hid = make_pre_v9(&path);

    let eng = Engine::open_standalone(&path).unwrap();
    let eid = enchudb_oplog::make_eid(0, 0);

    // 一度も書き直していない cell は版数不明なので、 どんなに古い record も通る
    assert_eq!(eng.cell_hlc(eid, hid), Hlc::ZERO);
    assert!(
        eng.set_cell(eid, hid, 1, hlc(1, 1)),
        "版数不明の cell が古い write を弾いた — A-1 (版数不明 = 現状維持) が崩れている",
    );
    assert_eq!(eng.get(eid, "age"), Some(1));
    drop(eng);
    cleanup(&path);
}

/// 冪等。 2 回開いても壊れず、 ファイルも二重に伸びない。
#[test]
fn migration_is_idempotent() {
    let path = tmp("idem");
    make_pre_v9(&path);

    let eng = Engine::open_standalone(&path).unwrap();
    assert!(eng.has_cell_version());
    let size_after_first = std::fs::metadata(&path).unwrap().len();
    drop(eng);

    let eng2 = Engine::open_standalone(&path).unwrap();
    assert!(eng2.has_cell_version());
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        size_after_first,
        "2 回目の open でファイルが更に伸びた (冪等でない)",
    );
    assert_eq!(eng2.get(enchudb_oplog::make_eid(0, 1), "age"), Some(101));
    drop(eng2);
    cleanup(&path);
}

/// readonly open では移行しない (共有 mmap を書かない契約)。
#[test]
fn readonly_open_does_not_migrate() {
    let path = tmp("ro");
    make_pre_v9(&path);
    let before = std::fs::metadata(&path).unwrap().len();

    let eng = Engine::open_readonly(&path).unwrap();
    assert!(!eng.has_cell_version(), "readonly open が移行してしまった");
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        before,
        "readonly open がファイルを伸ばした",
    );
    assert_eq!(eng.get(enchudb_oplog::make_eid(0, 2), "age"), Some(102));
    drop(eng);
    cleanup(&path);
}

/// 既に v9 の DB を開いても何も起きない (no-op)。
#[test]
fn already_v9_is_untouched() {
    let path = tmp("already");
    cleanup(&path);
    {
        let mut eng = Engine::create_with_capacity(&path, 4096).unwrap();
        eng.define_himo("age", ValueType::Number, 0);
        assert!(eng.has_cell_version());
        let e = eng.entity().unwrap();
        eng.tie_to(e, "age", 7);
        eng.flush().unwrap();
    }
    let before = std::fs::metadata(&path).unwrap().len();

    let eng = Engine::open_standalone(&path).unwrap();
    assert!(eng.has_cell_version());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), before, "v9 DB のサイズが変わった");
    assert_eq!(eng.get(enchudb_oplog::make_eid(0, 0), "age"), Some(7));
    drop(eng);
    cleanup(&path);
}

/// **削除の記録だけは移行直後から効く。**
///
/// `.eidmap` sidecar は foreign entity の tombstone HLC を既に永続化している。
/// その読み込み (`open_internal` 内、 migration より後) は `set_tombstone_local` を
/// 通るので、 生えたばかりの tombstone column に自動で載る。 版数と違って
/// 「移行前の情報が残っている」 唯一の軸なので、 ここだけは遡って効く。
#[test]
fn foreign_delete_records_land_in_the_new_tombstone_column() {
    let path = tmp("tombseed");
    cleanup(&path);
    let foreign = enchudb_oplog::make_eid(1, 555);

    // v9 領域を持たない DB で、 foreign entity を受けて削除する
    let hid = {
        let mut eng = Engine::create_without_cell_version(&path, 4096).unwrap();
        eng.define_table("t", 8).unwrap();
        eng.define_himo_in("t", "age", ValueType::Number, 0).unwrap();
        let hid = eng.himo_id("t.age").unwrap() as u16;
        eng.set_peer_id(2);
        assert!(!eng.has_cell_version(), "前提が崩れた");

        let t = eng.resolve_remote_eid(foreign, hid).expect("翻訳できない");
        assert!(eng.remote_tie_apply(t, hid, 111, hlc(1000, 1), None));
        assert!(eng.remote_delete_apply(t, hlc(2000, 1), None));

        eng.persist_tables().unwrap(); // .eidmap に tombstone が載る
        eng.flush().unwrap();
        hid
    };

    // writer open → v9 領域が生え、 .eidmap の削除記録がその column に載る
    let eng = Engine::open_standalone(&path).unwrap();
    assert!(eng.has_cell_version(), "移行していない");

    let t = eng.resolve_remote_eid(foreign, hid).expect("翻訳が復元されていない");
    assert_eq!(
        eng.tombstone_hlc(t),
        hlc(2000, 1),
        "移行後の tombstone column に .eidmap の削除記録が載っていない",
    );
    // 削除より古い Tie は弾かれる = 移行直後から効いている
    assert!(
        !eng.remote_tie_apply(t, hid, 111, hlc(1500, 1), None),
        "削除より古い Tie が通った",
    );
    drop(eng);
    cleanup(&path);
}
