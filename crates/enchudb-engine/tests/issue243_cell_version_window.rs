//! #243: `enable_sync_tables()` の窓で書いた cell 版数が reopen 後に ZERO で固定される。
//!
//! report は syncretic 側から。 「新規 store の最初のセッション」 = ユーザーがフォルダを
//! 丸ごと入れる瞬間がまるごとこの窓に当たっていた。
//!
//! # この file が固定すること
//!
//! 1. **窓は実在し、 版数は実際に消える** — 現仕様の pin。 塞いだら (issue の A 案:
//!    v9 tail を別 mmap する) この test を反転させる
//! 2. **窓に入ったことが consumer 側から判る** — `has_cell_version()` が false、
//!    `volatile_cell_versions()` が非 0。 「気づいた consumer だけが助かる」 を
//!    避けるには、 気づく手段が assert できる必要がある
//! 3. **窓を回避する経路が本当に窓を回避している** — create 時点で v9 を確保する
//!    3 経路 (eager / growable / `GrowableOptions`)
//! 4. **hydrate の判定材料が この session の write で汚れない** —
//!    `cell_versions_were_empty_at_open()` は open 直後の file の状態を凍らせる
//!
//! 2 は 1 の裏返しではない点に注意。 版数が消えること自体は 「後から sync 化する DB」
//! では避けられない (0.20.0 以来の 「移行済み cell は版数 ZERO」 と同じ意味論) ので、
//! 消えたことが **黙って**起きないことが実質的な保証になる。

use enchudb_engine::engine::Engine;
use enchudb_engine::{GrowableOptions, ValueType};
use enchudb_oplog::Hlc;

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-i243-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(p: &str) {
    for s in [
        "", ".oplog", ".tables", ".tables.tmp", ".crc", ".lock", ".db.lock", ".eidmap",
        ".vocabmap", ".schema",
    ] {
        let _ = std::fs::remove_file(format!("{}{}", p, s));
    }
}

fn define_rows(eng: &mut Engine) {
    eng.define_table("files", 64).unwrap();
    eng.define_himo_in("files", "n", ValueType::Number, 0).unwrap();
}

/// 窓の最小再現 — `Syncer` も ack / reclaim も要らず、 engine crate 単体で決定的に出る。
///
/// 「配送バッファが reclaim 済みだと hydrate で戻らない」 は窓の **帰結の 1 つ**で
/// あって原因ではない。 原因は 「版数の置き場がこの session に存在しない」 こと。
#[test]
fn versions_written_in_the_enable_window_are_zero_after_reopen() {
    let path = tmp("window");
    cleanup(&path);

    let (eid, himo) = {
        let mut eng = Engine::create_with_capacity(&path, 1024).unwrap();
        define_rows(&mut eng);
        eng.enable_sync_tables().unwrap();
        assert!(
            !eng.has_cell_version(),
            "enable 直後に v9 column が生えている — 窓が塞がったなら本 test を反転させる",
        );

        let eng = Engine::concurrentize_with_oplog(eng, 1 << 20).unwrap();
        eng.set_peer_id(7);
        let e = eng.entity_in("files").unwrap();
        eng.tie_to(e, "files.n", 111);
        eng.flush_writes();
        eng.oplog_sync().unwrap();

        // 窓に居ることは 2 つの観測 API で判る。
        assert!(!eng.has_cell_version());
        assert!(
            eng.volatile_cell_versions() > 0,
            "窓の中で版数付き write をしたのに数えられていない",
        );

        // `cell_hlc` は **永続している版数**を返すので、 窓の中では既に ZERO。
        // LWW 自体は揮発 `HlcStore` の版数で正しく判定している (この session の間は)。
        // 「今 ZERO」 と 「reopen 後も ZERO」 の違いは、 後者だけが取り返しがつかない点。
        let himo = eng.himo_id("files.n").expect("himo") as u16;
        assert_eq!(eng.cell_hlc(e, himo), Hlc::ZERO);
        (e, himo)
    };

    let eng = Engine::open_concurrent_with_oplog(&path, 1 << 20).unwrap();
    assert!(eng.has_cell_version(), "次の open で v9 column が生えていない");
    assert_eq!(eng.get_by_id(eid, himo), Some(111), "値まで消えている (これは別の壊れ)");
    assert_eq!(
        eng.cell_hlc(eid, himo),
        Hlc::ZERO,
        "窓が塞がった = A 案が入った。 本 test を 「版数が残る」 側へ反転させる",
    );
    // 揮発した版数は復元されていないので、 この session の counter は 0。
    assert_eq!(eng.volatile_cell_versions(), 0);
    drop(eng);
    cleanup(&path);
}

/// 窓を回避する経路 (eager) — **これが #243 の fix の実体**。
#[test]
fn create_with_cell_version_has_no_window() {
    let path = tmp("eager");
    cleanup(&path);

    let (eid, himo, wrote) = {
        let mut eng = Engine::create_with_cell_version(&path, 1024).unwrap();
        define_rows(&mut eng);
        eng.enable_sync_tables().unwrap();
        assert!(eng.has_cell_version(), "create 時点で v9 を確保していない");

        let eng = Engine::concurrentize_with_oplog(eng, 1 << 20).unwrap();
        eng.set_peer_id(7);
        let e = eng.entity_in("files").unwrap();
        eng.tie_to(e, "files.n", 111);
        eng.flush_writes();
        eng.oplog_sync().unwrap();

        assert_eq!(
            eng.volatile_cell_versions(),
            0,
            "v9 column があるのに揮発 store へ落ちている",
        );
        let himo = eng.himo_id("files.n").expect("himo") as u16;
        let wrote = eng.cell_hlc(e, himo);
        assert_ne!(wrote, Hlc::ZERO, "版数が載っていない");
        (e, himo, wrote)
    };

    let eng = Engine::open_concurrent_with_oplog(&path, 1 << 20).unwrap();
    assert_eq!(eng.get_by_id(eid, himo), Some(111));
    assert_eq!(eng.cell_hlc(eid, himo), wrote, "reopen で版数が失われた");
    drop(eng);
    cleanup(&path);
}

/// 窓を回避する経路 (growable)。 growable は `enable_sync_tables()` が file すら
/// 伸ばさない (address 予約が v8 total で取られている) ので、 eager より確実に踏む。
#[test]
fn create_growable_with_cell_version_has_no_window() {
    let path = tmp("growable");
    cleanup(&path);

    let (eid, himo, wrote) = {
        let mut eng = Engine::create_growable_with_cell_version(&path, 1024).unwrap();
        define_rows(&mut eng);
        eng.enable_sync_tables().unwrap();
        assert!(eng.has_cell_version());

        let eng = Engine::concurrentize_with_oplog(eng, 1 << 20).unwrap();
        eng.set_peer_id(7);
        let e = eng.entity_in("files").unwrap();
        eng.tie_to(e, "files.n", 222);
        eng.flush_writes();
        eng.oplog_sync().unwrap();

        assert_eq!(eng.volatile_cell_versions(), 0);
        let himo = eng.himo_id("files.n").expect("himo") as u16;
        let wrote = eng.cell_hlc(e, himo);
        assert_ne!(wrote, Hlc::ZERO);
        (e, himo, wrote)
    };

    let eng = Engine::open_concurrent_with_oplog(&path, 1 << 20).unwrap();
    assert_eq!(eng.get_by_id(eid, himo), Some(222));
    assert_eq!(eng.cell_hlc(eid, himo), wrote, "reopen で版数が失われた");
    drop(eng);
    cleanup(&path);
}

/// `GrowableOptions.cell_version` — layout knob を指定する consumer (schema 層の
/// `Database::create_growable_with`) が通る経路。 ここが `false` 固定だったので、
/// 「constructor を 2 本公開する」 だけでは実 consumer に届かなかった。
#[test]
fn growable_options_can_reserve_v9() {
    let path = tmp("opts");
    cleanup(&path);
    {
        let eng = Engine::create_growable_opts(
            &path,
            GrowableOptions { max_entities: 1024, cell_version: true, ..Default::default() },
        )
        .unwrap();
        assert!(eng.has_cell_version(), "GrowableOptions.cell_version が効いていない");
    }
    cleanup(&path);

    // default は false のまま — 単独 DB に v9 の apparent size を払わせない。
    let path2 = tmp("opts-default");
    cleanup(&path2);
    {
        let eng = Engine::create_growable_opts(
            &path2,
            GrowableOptions { max_entities: 1024, ..Default::default() },
        )
        .unwrap();
        assert!(!eng.has_cell_version(), "default が v9 を確保している");
    }
    cleanup(&path2);
}

/// hydrate の判定材料は 「**開いた瞬間の** file に版数が載っていたか」。
///
/// `Syncer::new` の時点で `cell_versions_are_empty()` を呼ぶと、 実アプリの通常順序
/// (open → initial scan で書く → sync 開始) で必ず false になり、 窓を経た DB の
/// 復元路が塞がっていた。 snapshot はこの session の write では動かない。
#[test]
fn empty_at_open_snapshot_is_not_disturbed_by_this_sessions_writes() {
    let path = tmp("snapshot");
    cleanup(&path);
    {
        let mut eng = Engine::create_with_cell_version(&path, 1024).unwrap();
        define_rows(&mut eng);
        eng.enable_sync_tables().unwrap();
        eng.flush().unwrap();
    }

    let eng = Engine::open_concurrent_with_oplog(&path, 1 << 20).unwrap();
    assert!(eng.cell_versions_are_empty(), "版数を 1 つも書いていないのに空でない");
    assert!(eng.cell_versions_were_empty_at_open());

    eng.set_peer_id(7);
    let e = eng.entity_in("files").unwrap();
    eng.tie_to(e, "files.n", 333);
    eng.flush_writes();

    assert!(!eng.cell_versions_are_empty(), "書いたのに版数が載っていない");
    assert!(
        eng.cell_versions_were_empty_at_open(),
        "この session の write が open 時点の snapshot を汚した",
    );
    drop(eng);
    cleanup(&path);
}
