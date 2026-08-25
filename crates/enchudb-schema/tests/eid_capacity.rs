//! schema 層から **残り eid 空間**を問い合わせられること。
//!
//! `max_entities` は create 時に header へ焼かれる。 後から table を足すアプリは
//! `with_capacity` を決める前に残量を知る必要があるが、 手段が無かったため
//! 「既知の table 名の range を全部引いて自分で引き算する」 しかなかった
//! (実地: syncretic が `_local_seen` を足そうとして
//!  `eid range [41984, 107520) exceeds max_entities 65536` で失敗)。

use enchudb_schema::Database;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-schema-eid-capacity-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    for suf in ["", ".oplog", ".tables", ".schema", ".crc", ".db.lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

#[test]
fn the_builder_can_ask_how_much_eid_space_is_left() {
    let path = tmp_path("builder");
    cleanup(&path);

    let mut db = Database::create_with_capacity(&path, 4096).unwrap();
    db.table("files").tag("path").with_capacity(1024).build().unwrap();
    db.enable_sync().unwrap();

    // sync 用の予約 table (`_sync_ops` / `_sync_peers`) も枠を食う。 その分も
    // 引いた残量が返ること (= 「files の分だけ引けばいい」 ではない)。
    let rest = db.remaining_eid_capacity();
    assert!(rest > 0 && rest < 4096 - 1024, "残量が予約 table 分を数えていない: {rest}");

    // 残量を超える要求は黙って縮まず Err。 メッセージに残量が載る。
    let err = match db.table("_seen").local_only().tag("path").with_capacity(rest + 1).build() {
        Ok(_) => panic!("残量を超える with_capacity が通ってしまった"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains(&format!("remaining {rest}")), "残量が error に載っていない: {err}");

    // 残量ちょうどなら通る。
    db.table("_seen").local_only().tag("path").with_capacity(rest).build().unwrap();
    assert_eq!(db.remaining_eid_capacity(), 0);

    let db = db.finish_with_oplog(8 * 1024 * 1024).unwrap();
    let seen = db.get_table("_seen").expect("_seen");
    seen.insert().set("path", "a.txt").commit().unwrap();
    let u = db.table_eid_usage("_seen").expect("usage");
    assert_eq!((u.capacity, u.live), (rest, 1));
    assert_eq!(u.free, rest - 1, "残り行数が引けない");

    cleanup(&path);
}
