//! #881: 満杯の表が枠を足す時、 足す枠を `tables` の sidecar に置いてから見せる。 置けなければ足さずに断る。
//!
//! 前は枠を見せてから `tables` を書き、 書けなかった失敗を捨てていた。 足した枠に行 A を書いて落ちると、 開いた時にその枠が
//! 無く (再生はどの表にも当たらない)、 次に枠を足す時に同じ範囲を切って、 A の eid をもう一度払い出した (A は表からも見えなくなる)。
//!
//! `tables` を書けない状態は #870 と同じく、 tmp の置き場 (`{db}/tables.tmp`) に directory を置いて作る。

#![cfg(unix)]

use enchudb_engine::{Engine, ValueType};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const CAP: u32 = 4;

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn scratch(tag: &str) -> Scratch {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let p = std::env::temp_dir().join(format!("enchudb-issue881-{tag}-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&p).unwrap();
    Scratch(p)
}

fn block_tables(db: &Path, on: bool) {
    let tmp = db.join("tables.tmp");
    if on {
        std::fs::create_dir(&tmp).unwrap();
    } else {
        std::fs::remove_dir(&tmp).unwrap();
    }
}

/// sync しない DB、 枠 `CAP` の表 `t` (列 a) を満杯にした engine。
fn full_table(db: &Path) -> (Arc<Engine>, u16) {
    let mut eng = Engine::create_growable_with_capacity(db.to_str().unwrap(), 1 << 16).unwrap();
    eng.define_table("t", CAP).unwrap();
    eng.define_himo_in("t", "a", ValueType::Number, 0).unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, 4 << 20).unwrap();
    let a = eng.himo_id("t.a").unwrap() as u16;
    for v in 0..CAP {
        let e = eng.entity_in("t").unwrap();
        eng.tie_to_by_id(e, a, v);
    }
    (eng, a)
}

/// `tables` を書けない間に満杯の表へ足すと、 枠を足さずに断る (前は黙って足し、 落ちた後に足した枠の行の eid をもう一度払い出した)。
/// 書けるように戻せば足せる。
#[test]
fn a_full_table_does_not_grow_while_tables_cannot_be_written() {
    let s = scratch("refuse");
    let db = s.0.join("db");
    let (eng, a) = full_table(&db);
    block_tables(&db, true);
    match eng.entity_in("t") {
        Err(e) => {
            assert!(e.contains("#881"), "断った理由: {e}");
            assert_eq!(eng.table_eid_usage("t").unwrap().capacity, CAP, "断ったのに枠を足した");
        }
        Ok(x) => {
            // 前の形: 黙って枠を足した。 その行を書いて落ちた像を開くと、 x をもう一度払い出す
            eng.tie_to_by_id(x, a, 100u32);
            eng.oplog_sync().unwrap();
            let image = s.0.join("image");
            assert!(std::process::Command::new("cp").arg("-R").arg(&db).arg(&image).status().unwrap().success());
            std::fs::remove_dir(image.join("tables.tmp")).unwrap();
            drop(eng);
            let reopened = Engine::open_concurrent_with_oplog(image.to_str().unwrap(), 4 << 20).unwrap();
            let y = reopened.entity_in("t").unwrap();
            panic!(
                "`tables` を書けないまま枠を足した (足した枠の行 {x:#x}、 落ちた後に払い出した {y:#x}、 行の値 {:?})",
                reopened.get_by_id(x, a)
            );
        }
    }
    block_tables(&db, false);
    let x = eng.entity_in("t").expect("書けるように戻したのに足せない");
    eng.tie_to_by_id(x, a, 100u32);
    assert_eq!(eng.table_eid_usage("t").unwrap().capacity, CAP * 2);
}

/// 足した枠は `tables` にある: 足した枠に行を書いて落ちた像を開いても、 その行は表に居て、 次に足す行は別の eid。
#[test]
fn a_row_in_a_grown_extent_survives_a_crash() {
    let s = scratch("crash");
    let db = s.0.join("db");
    let (eng, a) = full_table(&db);
    let x = eng.entity_in("t").unwrap();
    eng.tie_to_by_id(x, a, 100u32);
    eng.oplog_sync().unwrap();
    let image = s.0.join("image");
    assert!(std::process::Command::new("cp").arg("-R").arg(&db).arg(&image).status().unwrap().success());
    drop(eng);
    let reopened = Engine::open_concurrent_with_oplog(image.to_str().unwrap(), 4 << 20).unwrap();
    assert_eq!(reopened.table_eid_usage("t").unwrap().capacity, CAP * 2, "足した枠が無い");
    assert_eq!(reopened.get_by_id(x, a), Some(100));
    let y = reopened.entity_in("t").unwrap();
    assert_ne!(y, x, "落ちた後に、 足した枠の行の eid {x:#x} をもう一度払い出した");
}
