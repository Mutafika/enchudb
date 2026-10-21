//! #551: Leaf の列を名前 (主キー) にした表を作れた。 Leaf は語を束ねない (値で引けない) ので名前にならず、 upsert が毎回
//! 新しい行を作り、 sync で束ねず、 消した名前も覚えなかった (どれも黙って)。 新しく作る表の build で断る。

use enchudb_schema::{Database, SchemaError};

#[test]
fn a_leaf_column_cannot_be_the_name() {
    let path = format!(
        "/tmp/enchudb-issue551-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    );
    let mut db = Database::create(&path).unwrap();
    let r = db.table("notes").leaf("key").leaf("body").primary_key("key").build().map(|_| ());
    assert!(matches!(r, Err(SchemaError::BadValue(_))), "Leaf の名前の表を作った: {r:?}");
    assert!(db.get_table("notes").is_none(), "断った表が残った");
    // tag / number / bigint は名前にできる。 名前でない Leaf の列は今まで通り
    db.table("a").tag("key").leaf("body").primary_key("key").build().unwrap();
    db.table("b").number("id").leaf("body").primary_key("id").build().unwrap();
    db.table("c").bigint("id").primary_key("id").build().unwrap();
    drop(db);
    let _ = std::fs::remove_dir_all(&path);
}
