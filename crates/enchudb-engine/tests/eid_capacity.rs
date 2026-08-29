//! **eid 枠は create 時に固定される。 満杯にする前に気付く手段が要る。**
//!
//! `max_entities` は header に焼かれ、 各 table の range はその中を先着順に
//! 切り出す。 溢れると `entity_in` が `Err` を返し、 アプリの掃引がそこで止まる。
//! 掃引が止まると **削除も流れなくなる** — 削除は枠を空ける唯一の手段なので、
//! 一度この形に入ると自力では戻れない。
//!
//! 実地 (syncretic の実機 store): live 15,953 / 枠 16,384 まで詰まっており、
//! 追加の table を `with_capacity` で切ろうとして
//! `eid range [41984, 107520) exceeds max_entities 65536` で失敗した。 残り
//! eid 空間を問い合わせる手段が無く、 既知の table 名の range から手で引き算して
//! 回避していた。 ここで固定するのは 「**公式に問い合わせられる**」 の 1 点。

use enchudb_engine::{Engine, ValueType};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-eid-capacity-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap", ".schema"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

#[test]
fn remaining_eid_capacity_tells_how_much_is_left_to_carve() {
    let path = tmp_path("remaining");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 1024).unwrap();
    let before = eng.remaining_eid_capacity();
    eng.define_table("files", 256).unwrap();
    assert_eq!(
        eng.remaining_eid_capacity(),
        before - 256,
        "切り出した分だけ残量が減っていない"
    );

    // 残量を超える table は **黙って縮まず** Err になり、 残量を名指しする。
    let rest = eng.remaining_eid_capacity();
    let err = eng.define_table("too_big", rest + 1).unwrap_err();
    assert!(
        err.contains(&format!("remaining {rest}")),
        "残量が error に載っていない: {err}"
    );
    // ちょうど残量分なら通る (off-by-one で 1 枠取り逃がさないこと)。
    eng.define_table("exact", rest).unwrap();
    assert_eq!(eng.remaining_eid_capacity(), 0);

    cleanup(&path);
}

#[test]
fn table_eid_usage_counts_live_rows_and_free_slots() {
    let path = tmp_path("usage");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 1024).unwrap();
    eng.define_table("files", 64).unwrap();
    eng.define_himo_in("files", "n", ValueType::Number, 0).unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, 1 << 20).unwrap();

    let u = eng.table_eid_usage("files").expect("usage");
    assert_eq!((u.capacity, u.live, u.free), (64, 0, 64));

    let mut rows = Vec::new();
    for i in 0..10u32 {
        let e = eng.entity_in("files").expect("row");
        eng.tie_to(e, "files.n", i);
        rows.push(e);
    }
    let u = eng.table_eid_usage("files").expect("usage");
    assert_eq!((u.live, u.free, u.allocated), (10, 54, 10));

    // 削除は枠を返す (= free が戻る)。 これがアプリの唯一の出口。
    eng.delete(rows[0]);
    eng.delete(rows[1]);
    let u = eng.table_eid_usage("files").expect("usage");
    assert_eq!((u.live, u.free), (8, 56), "削除しても枠が戻っていない");
    assert_eq!(u.allocated, 10, "allocated は払出の最大なので減らない");

    assert!(eng.table_eid_usage("nope").is_none());

    cleanup(&path);
}

/// 枠を使い切ったとき、 `entity_in` は落ちるが **削除は通る** こと。
///
/// 満杯で削除まで止まると回復不能になる。 アプリは Err を握って掃引を
/// 続け、 削除を流し切れば枠が戻る。
#[test]
fn a_full_table_still_accepts_deletes() {
    let path = tmp_path("full");
    cleanup(&path);

    let mut eng = Engine::create_with_capacity(&path, 1024).unwrap();
    eng.define_table("files", 4).unwrap();
    eng.define_himo_in("files", "n", ValueType::Number, 0).unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, 1 << 20).unwrap();

    let mut rows = Vec::new();
    for i in 0..4u32 {
        let e = eng.entity_in("files").expect("row");
        eng.tie_to(e, "files.n", i);
        rows.push(e);
    }
    let err = eng.entity_in("files").unwrap_err();
    assert!(err.contains("exhausted"), "満杯の error が変わっている: {err}");
    assert_eq!(eng.table_eid_usage("files").unwrap().free, 0);

    eng.delete(rows[0]);
    assert_eq!(
        eng.table_eid_usage("files").unwrap().free,
        1,
        "満杯の table で削除が枠を返していない (詰んだら二度と戻れない)"
    );
    let reused = eng.entity_in("files").expect("枠が戻っているのに払い出せない");
    assert_eq!(
        enchudb_oplog::eid_local(reused),
        enchudb_oplog::eid_local(rows[0]),
        "返った slot が再利用されていない"
    );

    cleanup(&path);
}
