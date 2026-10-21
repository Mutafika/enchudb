//! 位置の印の宣言 (`TableBuilder::marks`): build での検査 (無い列 / Leaf)、 engine への宣言、 開き直しで戻る、 書かなければ
//! 外れる、 答えは宣言の有無で同じ (手で数えた答えと比べる)。

use enchudb_schema::{ColumnType, Database, SchemaError};

fn tmp_path(name: &str) -> String {
    format!("/tmp/enchudb-marks-decl-{}-{}", name, std::process::id())
}

fn cleanup(path: &str) {
    let _ = enchudb_engine::db_files::remove_db(path);
}

/// 行 i の score: 3 の倍数の行は書き換えた後の値。
fn score_of(i: i64, churned: bool) -> i64 {
    if churned && i % 3 == 0 { (i * 7) % 50 } else { i % 50 }
}

#[test]
fn marks_are_declared_persisted_and_dropped() {
    let path = tmp_path("decl");
    cleanup(&path);
    const N: i64 = 2_000;
    {
        let mut db = Database::create(&path).unwrap();
        let t = db
            .table("t")
            .number("id")
            .number("score")
            .column("memo", ColumnType::Leaf)
            .primary_key("id")
            .marks("score")
            .build()
            .unwrap();
        for i in 0..N {
            t.insert().set("id", i).set("score", score_of(i, false)).commit().unwrap();
        }
        assert_eq!(db.engine().marks_declarations(), vec!["t.score".to_string()], "build で宣言が engine に届く");
        let t = db.get_table("t").unwrap();
        // 読んで索引を組む → 書き換える → 範囲の読みが手で数えた答えと同じ
        let before = t.where_range("score", 10i64, 19i64).count().unwrap();
        assert_eq!(before, (0..N).filter(|&i| (10..=19).contains(&score_of(i, false))).count());
        for i in (0..N).step_by(3) {
            t.upsert().set("id", i).set("score", score_of(i, true)).commit().unwrap();
        }
        let want = (0..N).filter(|&i| (10..=19).contains(&score_of(i, true))).count();
        let mut got = t.where_range("score", 10i64, 19i64).find().unwrap();
        got.sort_unstable();
        got.dedup();
        assert_eq!(got.len(), want, "書き換えの後の範囲の find");
        assert_eq!(t.where_range("score", 10i64, 19i64).count().unwrap(), want, "書き換えの後の範囲の count");
    }
    {
        let db = Database::open(&path).unwrap();
        assert_eq!(db.engine().marks_declarations(), vec!["t.score".to_string()], "開き直すと戻る");
        let t = db.get_table("t").unwrap();
        let want = (0..N).filter(|&i| (10..=19).contains(&score_of(i, true))).count();
        assert_eq!(t.where_range("score", 10i64, 19i64).count().unwrap(), want, "開き直した後の答え");
    }
    {
        // 列だけ書いて印を書かなければ外れる
        let mut db = Database::open(&path).unwrap();
        db.table("t").number("id").number("score").column("memo", ColumnType::Leaf).primary_key("id").build().unwrap();
        assert!(db.engine().marks_declarations().is_empty(), "書かなければ外れる");
        let t = db.get_table("t").unwrap();
        let want = (0..N).filter(|&i| (10..=19).contains(&score_of(i, true))).count();
        assert_eq!(t.where_range("score", 10i64, 19i64).count().unwrap(), want, "外した後も答えは同じ");
    }
    {
        // handle を取るだけの build は何も変えない。 印だけ書く build は印を足す
        let mut db = Database::open(&path).unwrap();
        db.table("t").build().unwrap();
        assert!(db.engine().marks_declarations().is_empty());
        db.table("t").marks("score").build().unwrap();
        assert_eq!(db.engine().marks_declarations(), vec!["t.score".to_string()]);
    }
    {
        // Leaf には付けられない / 無い列は断る
        let mut db = Database::open(&path).unwrap();
        let r = db.table("t").number("id").number("score").column("memo", ColumnType::Leaf).primary_key("id").marks("memo").build();
        assert!(matches!(r.as_ref().err(), Some(SchemaError::BadValue(_))), "Leaf: {:?}", r.as_ref().err());
        let r = db.table("t").number("id").number("score").column("memo", ColumnType::Leaf).primary_key("id").marks("nope").build();
        assert!(matches!(r.as_ref().err(), Some(SchemaError::BadValue(_))), "無い列: {:?}", r.as_ref().err());
    }
    cleanup(&path);
}
