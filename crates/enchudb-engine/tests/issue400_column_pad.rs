//! #400: 列の cell を header から離して置く (`column::CELLS_PAD`)。 APFS は書いた所に隣り合う 16 MiB 未満の穴を 0 で
//! 埋めて実体化するので、 後ろの table の列は 「header と自分の行の間」 が実ディスクになっていた。
//!
//! 実ディスクの assert は APFS (macOS) の時だけ (Linux / Windows は元から穴のまま)。 中身の読み書き・packed の往復・
//! 移行は全 OS で見る。

use enchudb_engine::column::CELLS_PAD;
use enchudb_engine::{Engine, GrowableOptions, ValueType};

fn tmp(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue400-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    for ext in ["oplog", "tables", "lock", "eidmap", "crc", "packed"] {
        let _ = std::fs::remove_file(format!("{path}.{ext}"));
    }
}

#[cfg(unix)]
fn physical(path: &std::path::Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).unwrap().blocks() * 512
}

const FRONT: u32 = 600_000;

/// 前の table に `FRONT` 行、 後ろの table に 3 行 (後ろの table の列は eid `FRONT` 以降だけ)。
fn build(path: &str, column_pad: Option<bool>) -> (u16, u16, Vec<u64>) {
    let mut eng = Engine::create_growable_opts(
        path,
        GrowableOptions { max_entities: 2_000_000, column_pad, ..Default::default() },
    )
    .unwrap();
    eng.define_table("big", FRONT + 16).unwrap();
    eng.define_himo_in("big", "k", ValueType::Number, 0).unwrap();
    eng.define_table("small", 16).unwrap();
    eng.define_himo_in("small", "v", ValueType::Number, 0).unwrap();
    eng.define_himo_in("small", "name", ValueType::Tag, 0).unwrap();
    let k = eng.himo_id("big.k").unwrap() as u16;
    let v = eng.himo_id("small.v").unwrap() as u16;
    let name = eng.himo_id("small.name").unwrap() as u16;
    for i in 0..FRONT {
        let e = eng.entity_in("big").unwrap();
        eng.tie_to_by_id(e, k, i);
    }
    let mut small = Vec::new();
    for i in 0..3u32 {
        let e = eng.entity_in("small").unwrap();
        eng.tie_to_by_id(e, v, 7 + i);
        eng.tie_text_to_by_id(e, name, &format!("n{i}"));
        small.push(e);
    }
    eng.flush().unwrap();
    (k, v, small)
}

fn check(eng: &Engine, small: &[u64]) {
    for (i, &e) in small.iter().enumerate() {
        assert_eq!(eng.get(e, "small.v"), Some(7 + i as u64));
        assert_eq!(eng.get_text_owned(e, "small.name").as_deref(), Some(format!("n{i}").as_bytes()));
    }
    assert_eq!(eng.pull_raw("small.v", 8u32).len(), 1);
    assert_eq!(eng.pull_raw("big.k", 123_456u32).len(), 1);
    assert_eq!(eng.pull_raw("big.k", FRONT - 1).len(), 1);
}

fn file_version(path: &str) -> u32 {
    let b = std::fs::read(std::path::Path::new(path).join("header.seg")).unwrap();
    u32::from_le_bytes(b[4..8].try_into().unwrap())
}

/// この OS で新しく作る DB が列の cell を離すか (macOS / Linux / Android)。
const PAD_BY_DEFAULT: bool = cfg!(any(target_os = "macos", target_os = "linux", target_os = "android"));

fn himo_file(path: &str, eng_hid: u16) -> std::path::PathBuf {
    std::path::Path::new(path).join(format!("himo/{eng_hid:04}.seg"))
}

/// 新しく作る DB (cell を離す): 後ろの table の列は、 header と自分の行の 2 か所だけ (APFS で数十 KB)。 file の見かけは
/// 「離した分 + 自分の行まで」 で、 伸ばす歩幅は cell の量で決まる (離した分で 16 MB ずつ先回りしない)。
#[test]
fn a_late_table_column_does_not_materialize_the_gap() {
    let path = tmp("new");
    let (_, v, small) = build(&path, Some(true));
    let eng = Engine::open_readonly(&path).unwrap();
    check(&eng, &small);
    let f = himo_file(&path, v);
    let len = std::fs::metadata(&f).unwrap().len();
    let last_cell = CELLS_PAD as u64 + (FRONT as u64 + 16 + 3) * 4;
    assert!(len >= last_cell && len < last_cell + (1 << 20), "見かけ {len} (行の末尾 {last_cell})");
    #[cfg(target_os = "macos")]
    assert!(physical(&f) < 256 << 10, "後ろの table の列の実ディスク {} B", physical(&f));
    drop(eng);
    cleanup(&path);
}

/// 前提の確認: 離さない DB (旧形式) では、 APFS は後ろの table の列の 「先頭から自分の行まで」 を実ディスクにする。
#[cfg(target_os = "macos")]
#[test]
fn premise_the_old_layout_materializes_the_gap_on_apfs() {
    let path = tmp("old");
    let (_, v, _) = build(&path, Some(false));
    let f = himo_file(&path, v);
    assert!(physical(&f) > 2 << 20, "旧形式でも実ディスクが小さい: {} B", physical(&f));
    cleanup(&path);
}

/// 既存 DB (離さない形) を移す: 値はそのまま、 後ろの table の列の実ディスクが縮む。 2 回目は何もしない。
#[test]
fn migrate_moves_existing_columns() {
    let path = tmp("mig");
    let (_, v, small) = build(&path, Some(false));
    assert_eq!(file_version(&path), 10);
    let moved = Engine::migrate_column_pad(&path).unwrap();
    assert_eq!(moved, 3, "列 3 本を移す");
    assert_eq!(file_version(&path), 13, "移した DB は旧 binary が開けない version に");
    assert_eq!(Engine::migrate_column_pad(&path).unwrap(), 0, "移し済みは飛ばす");
    #[cfg(target_os = "macos")]
    assert!(physical(&himo_file(&path, v)) < 256 << 10, "移した後の実ディスク {} B", physical(&himo_file(&path, v)));
    let _ = v;
    {
        let mut eng = Engine::open_standalone(&path).unwrap();
        check(&eng, &small);
        // 移した後も書ける (列を伸ばす / 新しい列も離して作る)
        let e = small[0];
        eng.tie_to_by_id(e, eng.himo_id("small.v").unwrap() as u16, 99u32);
        eng.flush().unwrap();
    }
    let eng = Engine::open_readonly(&path).unwrap();
    assert_eq!(eng.get(small[0], "small.v"), Some(99));
    drop(eng);
    cleanup(&path);
}

/// 移行の途中で落ちた形 (一部の列だけ離した形のまま): 列は自分の header で形を名乗るので、 混ざっていても読める。
#[test]
fn a_half_migrated_db_reads_both_layouts() {
    let path = tmp("half");
    let (k, v, small) = build(&path, Some(false));
    let manifest = std::path::Path::new(&path).join("segments");
    let old_big = std::fs::read(himo_file(&path, k)).unwrap();
    let old_manifest = std::fs::read(&manifest).unwrap();
    Engine::migrate_column_pad(&path).unwrap();
    // big.k だけ旧形式に戻す (= その列を移す前に落ちた。 manifest は移行前の flush のまま)
    std::fs::write(himo_file(&path, k), &old_big).unwrap();
    std::fs::write(&manifest, &old_manifest).unwrap();
    {
        let eng = Engine::open_readonly(&path).unwrap();
        check(&eng, &small);
    }
    assert_eq!(Engine::migrate_column_pad(&path).unwrap(), 1, "残りの 1 本を移す");
    let eng = Engine::open_readonly(&path).unwrap();
    check(&eng, &small);
    let _ = v;
    drop(eng);
    cleanup(&path);
}

/// packed (relay の bootstrap / wasm) は cell を離さない形で運ぶ: `from_bytes` で読め、 `unpack_to_dir` で展開した
/// directory も読める。
#[test]
fn packed_roundtrip_of_a_padded_db() {
    let path = tmp("pack");
    let (_, v, small) = build(&path, Some(true));
    let packed = std::path::PathBuf::from(format!("{path}.packed"));
    let size = Engine::pack_dir(&path, &packed).unwrap();
    assert!(size < 1 << 40);
    let bytes = std::fs::read(&packed).unwrap();
    // packed は離さない形 = 旧 binary も読める version (v12 以下) で、 印も 0
    assert!(u32::from_le_bytes(bytes[4..8].try_into().unwrap()) <= 12, "packed の version");
    assert_eq!(&bytes[100..104], &[0, 0, 0, 0], "packed の header に列を離す印が残る");
    let mem = Engine::from_bytes(bytes).unwrap();
    check(&mem, &small);
    drop(mem);
    let dst = tmp("unpacked");
    Engine::unpack_to_dir(&packed, &dst).unwrap();
    let eng = Engine::open_readonly(&dst).unwrap();
    check(&eng, &small);
    // 展開した directory は、 この OS で新しく作る DB と同じ形
    let len = std::fs::metadata(himo_file(&dst, v)).unwrap().len();
    assert_eq!(len >= CELLS_PAD as u64, PAD_BY_DEFAULT, "展開した列の形 ({len} B)");
    assert_eq!(file_version(&dst) == 13, PAD_BY_DEFAULT);
    drop(eng);
    cleanup(&path);
    cleanup(&dst);
}

/// sync の DB の版数列 / 削除の印の列も離す (cell は entity の番号で引くので同じ穴を持つ)。 書いた版数・削除の印は
/// 開き直しても残る。
#[test]
fn version_columns_are_padded_too() {
    use enchudb_oplog::Hlc;
    let path = tmp("ver");
    let at = |wall| Hlc { wall, logical: 0, peer: 2 };
    let (hid, e, gone);
    {
        let mut eng = Engine::create_growable_opts(
            &path,
            GrowableOptions { max_entities: 100_000, column_pad: Some(true), ..Default::default() },
        )
        .unwrap();
        eng.define_table("t", 1_000).unwrap();
        eng.define_himo_in("t", "x", ValueType::Number, 0).unwrap();
        eng.enable_sync_tables().unwrap();
        hid = eng.himo_id("t.x").unwrap() as u16;
        e = eng.entity_in("t").unwrap();
        gone = eng.entity_in("t").unwrap();
        assert!(eng.remote_tie_apply(e, hid, 5, at(100)));
        assert!(eng.set_tombstone(gone, at(200)));
        eng.flush().unwrap();
    }
    for rel in [format!("ver/{hid:04}.seg"), "tomb.seg".to_string()] {
        let len = std::fs::metadata(std::path::Path::new(&path).join(&rel)).unwrap().len();
        assert!(len >= CELLS_PAD as u64, "{rel} が離されていない ({len} B)");
    }
    let eng = Engine::open_standalone(&path).unwrap();
    assert_eq!(eng.get(e, "t.x"), Some(5));
    assert_eq!(eng.cell_hlc(e, hid), at(100), "版数が開き直しで消えた");
    assert_eq!(eng.tombstone_hlc(gone), at(200), "削除の印が開き直しで消えた");
    drop(eng);
    cleanup(&path);
}

/// 予約を cap と同値にした DB (iOS / Windows の既定と同じ) でも、 離した分を予約に足すので cap の端の eid まで書ける。
#[test]
fn the_reservation_covers_the_pad() {
    let path = tmp("reserve");
    let cap = 50_000u32;
    let last;
    {
        let mut eng = Engine::create_growable_opts(
            &path,
            GrowableOptions { max_entities: cap, reserve_entities: Some(cap), column_pad: Some(true), ..Default::default() },
        )
        .unwrap();
        eng.define_table("t", cap - 100).unwrap();
        eng.define_himo_in("t", "x", ValueType::Number, 0).unwrap();
        let x = eng.himo_id("t.x").unwrap() as u16;
        let mut e = 0;
        while let Ok(n) = eng.entity_in("t") {
            e = n;
            if enchudb_oplog::eid_local(n) + 200 > cap {
                break;
            }
        }
        eng.tie_to_by_id(e, x, 42u32);
        eng.flush().unwrap();
        last = e;
    }
    let eng = Engine::open_readonly(&path).unwrap();
    assert_eq!(eng.get(last, "t.x"), Some(42));
    drop(eng);
    cleanup(&path);
}

/// 後ろの table の列を伸ばす歩幅は、 その列の行の量で決まる (最大 1 MiB)。 空けた分や表の手前の行の分で先回りすると、
/// APFS が先回りした末尾の穴を実ディスクにする。 行 20 万 (800 KB) と 3 万 (120 KB、 歩幅の上限より小さい) の 2 通り。
#[test]
fn a_late_column_grows_by_its_own_rows() {
    for rows in [200_000u32, 30_000] {
        late_column_round(rows);
    }
}

fn late_column_round(rows: u32) {
    let path = tmp("grow");
    let mut eng = Engine::create_growable_opts(
        &path,
        GrowableOptions { max_entities: 4_000_000, column_pad: Some(true), ..Default::default() },
    )
    .unwrap();
    eng.define_table("big", 3_000_016).unwrap();
    eng.define_himo_in("big", "k", ValueType::Number, 0).unwrap();
    eng.define_table("late", rows + 16).unwrap();
    eng.define_himo_in("late", "x", ValueType::Number, 0).unwrap();
    let k = eng.himo_id("big.k").unwrap() as u16;
    let x = eng.himo_id("late.x").unwrap() as u16;
    let e0 = eng.entity_in("big").unwrap();
    eng.tie_to_by_id(e0, k, 1u32);
    for _ in 1..3_000_000 {
        eng.entity_in("big").unwrap();
    }
    let mut last = 0;
    for i in 0..rows {
        let e = eng.entity_in("late").unwrap();
        eng.tie_to_by_id(e, x, i);
        last = enchudb_oplog::eid_local(e) as u64;
    }
    eng.flush().unwrap();
    let len = std::fs::metadata(himo_file(&path, x)).unwrap().len();
    let end = CELLS_PAD as u64 + (last + 1) * 4;
    // 先回りは 「その列の行の量」 (と 1 MiB の小さい方) + 1 回の最小歩幅まで
    let data = rows as u64 * 4;
    assert!(len >= end && len - end <= data.min(1 << 20) + (64 << 10), "行 {rows}: 末尾の先回り {} B", len - end);
    drop(eng);
    cleanup(&path);
}
