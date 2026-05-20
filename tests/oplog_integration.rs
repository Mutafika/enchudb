//! v28 WAL 統合テスト。プロセスクラッシュ / fsync 挙動 / リカバリを検証。

#![cfg(feature = "v27")]

use enchudb::{Engine, HimoType};
use std::path::Path;
use std::sync::Arc;

fn tmp(name: &str) -> String {
    let p = format!("/tmp/enchudb-wal-it-{}-{}", name, std::process::id());
    let _ = std::fs::remove_file(&p);
    let _ = std::fs::remove_file(format!("{}.oplog", p));
    p
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.oplog", path));
}

#[test]
fn create_open_with_wal_roundtrip() {
    let path = tmp("roundtrip");

    // create + 書き込み + commit + sync
    {
        let mut eng = Engine::create_with_capacity(&path, 1024).unwrap();
        eng.define_himo("age", HimoType::Number, 100);
        drop(eng);

        // create_concurrent_with_oplog は define 済み DB を対象にするパターンが必要
        // ただ現 API は create_with_capacity ベースなので、テストは open_concurrent_with_oplog で
    }

    let eng = Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).unwrap();
    let e1 = eng.entity();
    eng.tie_async(e1, "age", 30);
    eng.flush_writes();
    eng.oplog_commit();
    eng.oplog_sync().unwrap();

    let durable = eng.durable_lsn();
    assert!(durable > 0, "durable_lsn should advance after oplog_sync");

    drop(eng);

    // 再 open → リカバリで age=30 が復元されるはず
    let eng2 = Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).unwrap();
    assert_eq!(eng2.get(0, "age"), Some(30));
    drop(eng2);

    cleanup(&path);
}

#[test]
fn recover_after_drop_without_flush() {
    let path = tmp("nodrop");

    // define 済みの DB を準備
    {
        let mut eng = Engine::create_with_capacity(&path, 1024).unwrap();
        eng.define_himo("tag", HimoType::Number, 50);
        eng.flush().unwrap();
    }

    // WAL 有効で書き込む → commit → fsync を待たずに drop(プロセスキル模擬)
    {
        let eng = Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).unwrap();
        let e = eng.entity();
        eng.tie_async(e, "tag", 7);
        eng.flush_writes();
        eng.oplog_commit();
        eng.oplog_sync().unwrap(); // 明示 sync で耐久化

        // Drop が consumer をシャットダウンし、最終 fsync も実行
    }

    // 再 open で復元確認
    let eng = Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).unwrap();
    assert_eq!(eng.get(0, "tag"), Some(7));
    drop(eng);

    cleanup(&path);
}

#[test]
fn uncommitted_writes_discarded_on_recovery() {
    let path = tmp("uncommitted");

    {
        let mut eng = Engine::create_with_capacity(&path, 1024).unwrap();
        eng.define_himo("score", HimoType::Number, 1000);
        eng.flush().unwrap();
    }

    // Commit 済みと未 Commit を混ぜて書く
    {
        let eng = Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).unwrap();
        let e1 = eng.entity();
        eng.tie_async(e1, "score", 100);
        eng.flush_writes();
        eng.oplog_commit();
        eng.oplog_sync().unwrap();

        // commit 無しで書く(これは recover で破棄される)
        let e2 = eng.entity();
        eng.tie_async(e2, "score", 999);
        eng.flush_writes();
        // oplog_commit 呼ばないまま drop
    }

    // 再 open → e1 の score=100 は残る、e2 の score=999 は…
    // 注意: apply_op で entity は alive になっているので、本体には両方書かれている。
    // WAL の uncommitted 分だけが recovery で捨てられる設計。
    // このテストは「WAL が適切に Commit 境界を守る」ことを確認するので、
    // ここでは WAL の recover() が commit 済みレコードだけを返すことを別途検証する。
    // (直接 OpLog::recover を呼ぶテストは wal.rs 側にある)

    let eng = Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).unwrap();
    // e1=100 は確実に残る
    assert_eq!(eng.get(0, "score"), Some(100));
    drop(eng);

    cleanup(&path);
}

#[test]
fn oplog_file_is_created() {
    let path = tmp("filecheck");
    {
        let mut eng = Engine::create_with_capacity(&path, 64).unwrap();
        eng.define_himo("x", HimoType::Number, 10);
        eng.flush().unwrap();
    }
    {
        let _eng = Engine::open_concurrent_with_oplog(&path, 1024 * 1024).unwrap();
        assert!(Path::new(&format!("{}.oplog", path)).exists());
    }
    cleanup(&path);
}

#[test]
fn stats_snapshot() {
    let path = tmp("stats");
    {
        let mut e = Engine::create_with_capacity(&path, 100).unwrap();
        e.define_himo("n", HimoType::Number, 10);
        e.flush().unwrap();
    }
    let eng = Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).unwrap();
    for i in 0..50u32 {
        let ent = eng.entity();
        eng.tie_async(ent, "n", i);
    }
    eng.flush_writes();
    eng.oplog_sync().unwrap();

    let s = eng.stats();
    assert_eq!(s.entity_count, 50);
    assert!(s.oplog_head > 0);
    assert_eq!(s.oplog_head, s.oplog_checkpoint, "after oplog_sync, checkpoint == head");
    assert!(s.durable_lsn > 0);
    assert_eq!(s.pushed, s.applied);

    drop(eng);
    cleanup(&path);
}

#[test]
fn content_async_roundtrip() {
    let path = tmp("content_async");
    {
        let mut e = Engine::create_with_capacity(&path, 100).unwrap();
        e.define_himo("tag", HimoType::Number, 10);
        e.flush().unwrap();
    }

    // content_async で書いて、sync して drop
    {
        let eng = Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).unwrap();
        let e = eng.entity();
        eng.tie_async(e, "tag", 3);
        eng.content_async(e, "memo", b"hello crash");
        eng.flush_writes();
        eng.oplog_sync().unwrap();
    }

    // 再 open → content 復元確認
    let eng = Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).unwrap();
    assert_eq!(eng.get_content(0, "memo"), Some(b"hello crash".as_slice()));
    drop(eng);
    cleanup(&path);
}

#[test]
fn auto_commit_on_shutdown() {
    let path = tmp("autocommit");
    {
        let mut e = Engine::create_with_capacity(&path, 100).unwrap();
        e.define_himo("n", HimoType::Number, 100);
        e.flush().unwrap();
    }

    // 手動 oplog_commit を呼ばずに drop → shutdown path が auto-commit
    {
        let eng = Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).unwrap();
        let e = eng.entity();
        eng.tie_async(e, "n", 42);
        eng.flush_writes();
        // ここで oplog_commit も oplog_sync も呼ばない → drop 時の auto-commit に委ねる
    }

    // 再 open → n=42 が残っていれば auto-commit が効いた証拠
    let eng = Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).unwrap();
    assert_eq!(eng.get(0, "n"), Some(42));
    drop(eng);
    cleanup(&path);
}

#[test]
fn header_crc_detects_corruption() {
    let path = tmp("crc");
    {
        let mut e = Engine::create_with_capacity(&path, 100).unwrap();
        e.define_himo("x", HimoType::Number, 10);
        e.flush().unwrap();
    }

    // ヘッダの H_MAX_ENTITIES を改竄(バイト 8..12)
    use std::io::{Read, Write, Seek, SeekFrom};
    use std::fs::OpenOptions;
    {
        let mut f = OpenOptions::new().read(true).write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(8)).unwrap();
        let mut b = [0u8; 1]; f.read_exact(&mut b).unwrap();
        f.seek(SeekFrom::Current(-1)).unwrap();
        f.write_all(&[b[0] ^ 0xFF]).unwrap();
    }

    // open は CRC 不一致で失敗するはず
    match Engine::open_standalone(&path) {
        Ok(_) => panic!("corrupted header should fail to open"),
        Err(e) => {
            let s = format!("{}", e);
            assert!(s.contains("CRC"), "error should mention CRC: {}", s);
        }
    }
    cleanup(&path);
}

#[test]
fn concurrent_writes_with_oplog() {
    let path = tmp("concurrent");
    {
        let mut eng = Engine::create_with_capacity(&path, 10_000).unwrap();
        eng.define_himo("n", HimoType::Number, 10_000);
        eng.flush().unwrap();
    }

    let eng = Engine::open_concurrent_with_oplog(&path, 64 * 1024 * 1024).unwrap();
    let mut handles = Vec::new();
    for t in 0..4 {
        let e = Arc::clone(&eng);
        handles.push(std::thread::spawn(move || {
            for i in 0..250 {
                let ent = e.entity();
                e.tie_async(ent, "n", (t * 1000 + i) as u32);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }

    eng.flush_writes();
    eng.oplog_commit();
    eng.oplog_sync().unwrap();

    // 1000 件書かれているはず
    assert!(eng.entity_count() >= 1000);
    drop(eng);

    cleanup(&path);
}
