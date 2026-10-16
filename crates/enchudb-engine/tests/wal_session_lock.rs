//! 書き手の engine を WAL 付きで開くと、 WAL の flock を開いている間ずっと持つ (2026-10-16)。 前は record を 1 件追記するたびに
//! flock を取って外していた (system call 2 回)。 書き手の process は `.db.lock` で 1 つなので、 開いている間に同じ WAL へ追記する
//! 別の process は居ない。 対照: engine を閉じると、 別に開いた file から lock が取れる。
#![cfg(not(target_arch = "wasm32"))]

use enchudb_engine::{db_files, Engine};

#[test]
fn writer_engine_holds_the_wal_lock_while_open() {
    let path = format!("/tmp/enchudb-wal-session-{}", std::process::id());
    let _ = db_files::remove_db(&path);
    let wal_path = db_files::path_for(&path, db_files::OPLOG);
    {
        let eng = Engine::create_concurrent_with_oplog(&path, 1 << 20).unwrap();
        assert!(eng.oplog().expect("WAL 付き").session_locked());
        let other = std::fs::OpenOptions::new().read(true).write(true).open(&wal_path).unwrap();
        assert!(!enchudb_oplog::filelock::try_lock_exclusive(&other).unwrap(), "開いている間は別の file から取れない");
        drop(eng);
        assert!(enchudb_oplog::filelock::try_lock_exclusive(&other).unwrap(), "閉じると取れる");
        let _ = enchudb_oplog::filelock::unlock(&other);
    }
    let _ = db_files::remove_db(&path);
}
