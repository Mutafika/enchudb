//! #970 (PR #977 の読み直し 2 の [A]): 場所を使い回したことのある DB (v15) で、 ディスクの空きが足りずに新しい行の列を書けなかった
//! insert は、 書きかけの行を残さない (#316: `commit()` が `WriteRejected` を返す時、 書けた列の行を消して片付ける)。
//!
//! PR #977 の 1 回目の作り直しは、 v15 の DB の削除を 「場所の世代を書く gen.seg の page を commit できなければ断る」 にしていた。 空きの
//! 無い時は、 この片付けの削除も断られ、 名前の列だけ書いた行が生きて残った (後の upsert がその行を名前で引く)。 今の形 (案 P) は
//! 生かす場所の gen.seg の page を生かす前に commit する (払い出す時。 v15 に移る時は払い出し済みの場所まで) ので、 生きている行の削除は
//! 断らない。 空きの不足は `set_space_margin` で作る。
//!
//! PR #977 の読み直し 4 の [9]: 払い出す場所の page を commit できない insert (gen.seg を伸ばせない) は、 列を伸ばせない時と同じ
//! `WriteRejected(Fault(DiskSpace))` (前は `Internal("entity_in(..) failed: ..")`)。

use enchudb_engine::{FaultKind, TieRejected};
use enchudb_schema::{Database, SchemaError};

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn tmp(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("{}/enchudb-issue970-{tag}-{}-{nanos}", std::env::temp_dir().display(), std::process::id())
}

#[test]
fn an_insert_rejected_for_space_leaves_no_half_written_row_in_a_v15_db() {
    let path = tmp("cleanup");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("u").number("id").primary_key("id").with_capacity(1).build().unwrap();
    // t の場所を gen.seg を作った時の commit の外へ送る
    db.table("pad").number("id").primary_key("id").with_capacity(40_000).build().unwrap();
    db.table("t").number("id").number("b").primary_key("id").build().unwrap();
    let u = db.get_table("u").unwrap();
    let t = db.get_table("t").unwrap();
    // 最初の使い回し (v15 に移る)
    let a = u.insert().set("id", 1i64).commit().unwrap();
    u.entity(a).delete().unwrap();
    let b = u.insert().set("id", 2i64).commit().unwrap();
    assert_ne!(a, b, "前提: v15 (使い回した場所は世代の違う eid)");
    assert_eq!(db.engine().slot_bits(), 24, "前提: v15");
    // 列 b を書いていない行で、 列 id の cell だけを commit しておく (新しい行の名前は書けて、 b は伸ばせない形)
    for i in 0..3i64 {
        t.insert().set("id", i).commit().unwrap();
    }
    let slot = (t.where_eq("id", 0i64).find_one().unwrap().unwrap() as u32) & ((1 << 24) - 1);
    assert!(slot >= 32_768, "前提: t の場所は gen.seg を作った時の commit の外 (前の形は消す時に gen.seg を伸ばした)");
    let n = t.all().count().unwrap();
    let eng = db.engine();
    eng.set_space_margin(u64::MAX / 2);
    let got = t.insert().set("id", 99i64).set("b", 1i64).commit();
    assert!(
        matches!(got, Err(SchemaError::WriteRejected(TieRejected::Fault(FaultKind::DiskSpace)))),
        "前提: 列 b を伸ばせずに断られた: {got:?}"
    );
    let left = t.where_eq("id", 99i64).find().unwrap();
    assert!(left.is_empty(), "断った insert の書きかけの行が残った: {left:?}");
    assert_eq!(t.all().count().unwrap(), n, "断った insert が行の数を変えた");
    eng.set_space_margin(0);
    let e = t.insert().set("id", 99i64).set("b", 1i64).commit().expect("空きが戻っても足せない");
    assert_eq!(t.where_eq("id", 99i64).find().unwrap(), vec![e]);
}

/// 読み直し 4 の [9]: 払い出す場所の gen.seg の page を commit できない insert (v15 の DB で、 commit の外の page の場所。 gen.seg だけ
/// 空きが無い = 列は伸ばせる) は `WriteRejected(Fault(DiskSpace))`。 空きが戻れば通る。
#[test]
fn an_insert_refused_for_gen_seg_space_is_a_write_rejection() {
    let path = tmp("genspace");
    let _c = Cleanup(path.clone());
    let mut db = Database::create(&path).unwrap();
    db.table("u").number("id").primary_key("id").with_capacity(1).build().unwrap();
    db.table("pad").number("id").primary_key("id").with_capacity(100_000).build().unwrap();
    db.table("t").number("id").primary_key("id").build().unwrap();
    let u = db.get_table("u").unwrap();
    let t = db.get_table("t").unwrap();
    let a = u.insert().set("id", 1i64).commit().unwrap();
    u.entity(a).delete().unwrap();
    u.insert().set("id", 2i64).commit().unwrap();
    assert_eq!(db.engine().slot_bits(), 24, "前提: v15");
    let eng = db.engine();
    eng.set_gen_space_margin(u64::MAX / 2);
    let got = t.insert().set("id", 7i64).commit();
    assert!(
        matches!(got, Err(SchemaError::WriteRejected(TieRejected::Fault(FaultKind::DiskSpace)))),
        "gen.seg を伸ばせない insert が空きの不足の断りでない: {got:?}"
    );
    assert!(t.where_eq("id", 7i64).find().unwrap().is_empty());
    eng.set_gen_space_margin(0);
    let e = t.insert().set("id", 7i64).commit().expect("空きが戻っても足せない");
    assert_eq!(t.where_eq("id", 7i64).find().unwrap(), vec![e]);
}
