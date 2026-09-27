//! #317: macOS (APFS) は mmap の page を書き出す時にブロックを確保するので、 書き出し前の page は空きから
//! 引かれない。 伸ばす時に 「まだ flush していない分」 も空きから引いて見ないと、 伸ばすたびに通って、 空きを
//! 超えた分が flush Ok のまま消える。
//!
//! 使える空きを `set_space_margin` で 64 MB に絞り、 flush せずに Leaf を書き続ける:
//! - 使える空きを大きく超える前に commit が Err になる (旧: 書き出し前は空きが減らないので通り続けた)
//! - 断られた後に開き直しても壊れず、 Ok の row は全部読める (旧: Leaf の high_water だけ進んで中身の無い範囲が
//!   残り、 open の走査が panic した)
//! - flush した後はまた書ける (flush で書き出した分は 「まだ flush していない分」 から引く)
//! - table の eid の範囲の手前 (列の先頭の穴) は書かないので数えない: 前に 12 table 作って穴を 48 MB にしても
//!   最初から断らない
//!
//! プロセス全体の合計 (`UNFLUSHED`) を見るので、 他のテストと混ぜないよう 1 file 1 テスト。

use enchudb_schema::{Database, SchemaError, Value};

fn memo(i: i64) -> String {
    format!("{i:08}-{}", "m".repeat(1000))
}

#[test]
fn stops_before_the_space_it_was_given() {
    let path = format!("/tmp/enchudb-issue317-{}.db", std::process::id());
    let _ = std::fs::remove_dir_all(&path);
    let n;
    {
        let mut db = Database::create(&path).unwrap();
        // notes の eid は 12 × 100 万から = 列の先頭に 48 MB の穴
        for k in 0..12 {
            db.table(&format!("filler{k}")).number("x").build().unwrap();
        }
        db.table("notes").number("id").leaf("memo").primary_key("id").build().unwrap();
        let t = db.get_table("notes").unwrap();
        let eng = db.engine();
        let free = eng.disk_free_bytes().expect("growable backing");
        const USABLE: u64 = 64 << 20;
        eng.set_space_margin(free.saturating_sub(USABLE));
        let mut ok = 0i64;
        let rejected = loop {
            // 使える空きの 5 倍まで書いて止まらなければ失敗
            assert!((ok as u64) * 1000 < 5 * USABLE, "使える空き {USABLE} の 5 倍書いても断らない");
            match t.insert().set("id", ok).set("memo", memo(ok).as_str()).commit() {
                Ok(_) => ok += 1,
                Err(SchemaError::WriteRejected(_)) => break ok,
                Err(e) => panic!("row {ok}: {e}"),
            }
        };
        // 書けた分は使える空き (+ 伸ばす 1 回分の余り) を超えない
        assert!((rejected as u64) * 1000 <= USABLE + (16 << 20), "{rejected} 行 = 使える空きを超えて書けた");
        assert!(rejected > 1000, "早すぎる拒否 ({rejected} 行)");
        // flush すると書き出した分は空きから引かれ、 「まだ flush していない分」 から外れる = 測り直せばまた書ける
        db.engine_mut().expect("単独所有").flush().unwrap();
        let t = db.get_table("notes").unwrap();
        let eng = db.engine();
        let free = eng.disk_free_bytes().unwrap();
        eng.set_space_margin(free.saturating_sub(USABLE));
        for i in rejected..rejected + 1000 {
            t.insert().set("id", i).set("memo", memo(i).as_str()).commit().unwrap_or_else(|e| panic!("flush の後の row {i}: {e}"));
        }
        n = rejected + 1000;
        eng.set_space_margin(0);
        db.engine_mut().expect("単独所有").flush().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let t = db.get_table("notes").unwrap();
    for i in 0..n {
        let e = t.where_eq("id", i).find_one().unwrap().unwrap_or_else(|| panic!("row {i} が無い"));
        assert_eq!(t.entity(e).get("memo"), Some(Value::Text(memo(i))), "row {i}");
    }
    assert_eq!(t.where_eq("id", n).find_one().unwrap(), None, "拒否された row {n} が残っている");
    drop(t);
    drop(db);
    let _ = std::fs::remove_dir_all(&path);
}
