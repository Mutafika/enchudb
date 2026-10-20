//! #534: 列の segment (`himo/NNNN.seg`) は開いた後に初めて触る時に開く。 その I/O が失敗したら:
//!
//! - 書き込み (`try_tie_*` / `untie` / `delete`) は panic せず、 何も変えずに断る (fault `SegmentIo` を数える。 結果を返す
//!   API は `Err`)。 次に触った時にまた開いてみる
//! - 読みは今まで通り panic する (読みの API は `Result` を持たないので、 握ると 「値が無い」 という間違った答えになる)。
//!   ただし lock を離してから panic する (旧: lock を持ったまま panic して毒し、 同じ表を読む別の thread も panic した)
//!
//! 開いた後に列の file を消すと、 初めて開く `open` が失敗する (root でも)。 開く途中で列を読む所 (Leaf の空きの組み直し)
//! の失敗は `tests/fault_matrix.rs` (crashsim) が見る。

use enchudb_engine::{Engine, FaultKind, TieRejected, ValueType};
use std::path::PathBuf;

fn tmp(tag: &str) -> String {
    let p = std::env::temp_dir().join(format!("enchu_issue534_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p.to_str().unwrap().to_string()
}

fn segment_file(db: &str, eng: &Engine, col: &str) -> PathBuf {
    let h = eng.himo_id(col).unwrap();
    PathBuf::from(db).join(format!("himo/{h:04}.seg"))
}

#[test]
fn writes_to_a_column_that_cannot_be_opened_are_refused_not_panicked() {
    let db = tmp("write");
    let (e, f);
    {
        let mut eng = Engine::create_with_capacity(&db, 4096).unwrap();
        eng.define_table("t", 100).unwrap();
        eng.define_himo_in("t", "n", ValueType::Number, 0).unwrap();
        eng.define_himo_in("t", "m", ValueType::Number, 0).unwrap();
        e = eng.entity_in("t").unwrap();
        f = eng.entity_in("t").unwrap();
        eng.tie(e, "t.n", 1u32);
        eng.tie(e, "t.m", 2u32);
        eng.tie(f, "t.m", 3u32);
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
    }
    let eng = Engine::open_standalone(&db).unwrap();
    let n = eng.himo_id("t.n").unwrap() as u16;
    // 開いた後に t.n の file を消す (まだ誰も t.n を触っていない = まだ開いていない)
    let path = segment_file(&db, &eng, "t.n");
    let bytes = std::fs::read(&path).unwrap();
    std::fs::remove_file(&path).unwrap();

    // 書き込みは断る (panic しない)
    let r = eng.try_tie_to_by_id(e, n, 9u32);
    assert_eq!(r, Err(TieRejected::Fault(FaultKind::SegmentIo)), "開けない列への書き込みが断られない");
    assert_eq!(eng.fault_count(FaultKind::SegmentIo), 1);
    eng.untie(e, "t.n");
    assert_eq!(eng.fault_count(FaultKind::SegmentIo), 2, "untie が断られない");
    // delete は全部の列の古い値を読むので断る: 途中まで消さない (他の列の値は残る)
    eng.delete(e);
    assert_eq!(eng.fault_count(FaultKind::SegmentIo), 3, "delete が断られない");
    assert_eq!(eng.get(e, "t.m"), Some(2), "断った delete が途中まで消した");
    // 開ける列は今まで通り
    eng.try_tie_to_by_id(f, eng.himo_id("t.m").unwrap() as u16, 4u32).unwrap();

    // file が戻れば、 次に触った時に開いて書ける
    std::fs::write(&path, &bytes).unwrap();
    eng.try_tie_to_by_id(e, n, 9u32).unwrap();
    assert_eq!(eng.get(e, "t.n"), Some(9));
    drop(eng);
    let _ = std::fs::remove_dir_all(&db);
}

/// 読みで開けない時は panic するが、 lock を毒さない: panic の後も別の列を読み書きでき、 file が戻れば同じ列も読める。
#[test]
fn a_read_that_cannot_open_the_column_panics_without_poisoning() {
    let db = tmp("read");
    let e;
    {
        let mut eng = Engine::create_with_capacity(&db, 4096).unwrap();
        eng.define_table("t", 100).unwrap();
        eng.define_himo_in("t", "n", ValueType::Number, 0).unwrap();
        eng.define_himo_in("t", "m", ValueType::Number, 0).unwrap();
        e = eng.entity_in("t").unwrap();
        eng.tie(e, "t.n", 1u32);
        eng.tie(e, "t.m", 2u32);
        eng.flush().unwrap();
        eng.persist_tables().unwrap();
    }
    let eng = std::sync::Arc::new(Engine::open_standalone(&db).unwrap());
    let path = segment_file(&db, &eng, "t.n");
    let bytes = std::fs::read(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    let r = {
        let eng = eng.clone();
        std::thread::spawn(move || eng.get(e, "t.n")).join()
    };
    assert!(r.is_err(), "前提: 開けない列の読みは panic する");
    // 別の列 (同じ segment の表の lock を通る) は読み書きできる
    assert_eq!(eng.get(e, "t.m"), Some(2), "panic の後に別の列が読めない (lock が毒された)");
    eng.try_tie_to_by_id(e, eng.himo_id("t.m").unwrap() as u16, 5u32).unwrap();
    std::fs::write(&path, &bytes).unwrap();
    assert_eq!(eng.get(e, "t.n"), Some(1), "file が戻っても読めない");
    drop(eng);
    let _ = std::fs::remove_dir_all(&db);
}
