//! #415: 作った直後に落ちても、 開けない + 作り直せない状態に詰まらない。
//!
//! - create は mkdir の直後に作成中の印 (`creating`) を置き、 作った中身を全部書き出してから消す。 印の残った directory
//!   は開くと 「作成中」 で断り、 create が片付けて作り直す。 旧: header だけを書き出して返し、 辞書 / entity / Leaf 領域
//!   の header は最初の flush まで書き出さなかった。 その間に電源が落ちると 「壊れている」 で開けず、 directory はある
//!   ので create し直しも断られた
//! - oplog は作った時に header を書き出す。 header が一度も書き出されていない oplog は開く側が作り直す。 旧: consumer の
//!   最初の fsync (100 ms) より前に落ちると `bad WAL magic` で開けなかった
//!
//! 電源断の像で確かめるのは `tests/power_loss.rs` (最初の書き出しが返る前の像は、 開けるか、 「作成中」 と言って作り
//! 直せること)。 ここは印と oplog の扱いを直接見る。

use enchudb_engine::{DbState, Engine, ValueType};

fn tmp(tag: &str) -> String {
    let p = std::env::temp_dir().join(format!("issue415_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p.to_str().unwrap().to_string()
}

#[test]
fn create_removes_its_marker() {
    let path = tmp("marker_removed");
    let eng = Engine::create_growable(&path).unwrap();
    assert!(!std::path::Path::new(&path).join("creating").exists(), "create が作成中の印を残した");
    drop(eng);
    assert!(matches!(Engine::probe(&path), DbState::Ready));
    let _ = std::fs::remove_dir_all(&path);
}

/// 作成中の印が残った directory (作る途中で落ちた) は、 開くと 「作成中」 で断り、 create が片付けて作り直す。
#[test]
fn interrupted_create_is_reported_and_can_be_created_again() {
    let path = tmp("interrupted");
    {
        let mut eng = Engine::create_growable(&path).unwrap();
        eng.define_himo("n", ValueType::Number, 0);
        eng.flush().unwrap();
    }
    std::fs::File::create(std::path::Path::new(&path).join("creating")).unwrap();
    let err = Engine::open_standalone(&path).err().expect("作成中の印があるのに開けた");
    assert!(err.to_string().contains("incomplete"), "作成中と言わない: {err}");
    assert!(matches!(Engine::probe(&path), DbState::Incomplete));
    let mut eng = Engine::create_growable(&path).expect("作成中の印が残った directory に作り直せない");
    eng.define_himo("n", ValueType::Number, 0);
    let e = eng.entity().unwrap();
    eng.tie(e, "n", 7u32);
    eng.flush().unwrap();
    drop(eng);
    let eng = Engine::open_standalone(&path).unwrap();
    assert_eq!(eng.get(e, "n"), Some(7));
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}

/// mkdir の直後 (作成中の印を置く前) に落ちた directory (lock 以外に何も無い) も、 create が片付けて作り直す。
#[test]
fn directory_left_right_after_mkdir_can_be_created_again() {
    for with_lock in [false, true] {
        let path = tmp(&format!("mkdir_only_{with_lock}"));
        std::fs::create_dir_all(&path).unwrap();
        if with_lock {
            std::fs::File::create(std::path::Path::new(&path).join("lock")).unwrap();
        }
        assert!(matches!(Engine::probe(&path), DbState::Incomplete));
        let eng = Engine::create_growable(&path).expect("mkdir の直後に落ちた directory に作り直せない");
        drop(eng);
        assert!(matches!(Engine::probe(&path), DbState::Ready));
        let _ = std::fs::remove_dir_all(&path);
    }
}

/// header が一度も書き出されていない oplog (作った直後に落ちた) は、 開く側が作り直す。 本体の値はそのまま読める。
#[test]
fn never_written_oplog_is_created_again() {
    let path = tmp("oplog");
    let e = {
        let mut eng = Engine::create_growable(&path).unwrap();
        eng.define_himo("n", ValueType::Number, 0);
        let e = eng.entity().unwrap();
        eng.tie(e, "n", 9u32);
        eng.flush().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, 1 << 20).unwrap();
        eng.oplog_sync().unwrap();
        e
    };
    // oplog の header を 0 に戻す (作った直後に落ちた像と同じ)
    let oplog = std::path::Path::new(&path).join("oplog");
    let mut bytes = std::fs::read(&oplog).unwrap();
    bytes[..32].fill(0);
    std::fs::write(&oplog, &bytes).unwrap();
    let eng = Engine::open_concurrent_with_oplog(&path, 1 << 20).expect("header が 0 の oplog で開けない");
    assert_eq!(eng.get(e, "n"), Some(9));
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}
