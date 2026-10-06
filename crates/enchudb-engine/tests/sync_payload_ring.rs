//! `_sync_ops` の payload が辞書を食わないこと (`sync.payload.seg`)。
//!
//! `_sync_ops.payload` は engine 内部 table の Leaf なので LeafStore に載らず、 辞書 (vocab) に置いていた。
//! 辞書は値の byte を回収しないので、 bridge した record の数だけ辞書が伸び続け、 ack / reclaim しても戻らな
//! かった。 下流の実機では生きている行が数千なのに辞書の data が 512 MiB に着き、 全 table の Tag 書き込みが
//! 止まった。 payload は専用の循環バッファに置き、 reclaim した分はその場所を使い回す。

use std::sync::Arc;

use enchudb_engine::{Engine, ValueType};

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-payload-ring-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    for suffix in [".oplog", ".tables", ".crc", ".db.lock"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
    }
}

fn open_synced(path: &str) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(path, 65_536).unwrap();
    eng.define_table("notes", 64).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap()
}

/// 書いて bridge し切るまで待つ。
fn write_and_bridge(eng: &Engine, e: u64, v: u32) {
    eng.tie_to(e, "notes.note", v);
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
}

/// 書き込みの回数。 辞書に置いていた時は 1 回ごとに payload 1 つ分 (100 B 前後) 辞書が伸びた。
const WRITES: u32 = 3_000;
/// 何回の書き込みごとに ack + reclaim するか。
const RECLAIM_EVERY: u32 = 100;
/// 書き込みの間に増えてよい辞書の語数 (payload 以外の一回限りの値の分)。
const VOCAB_SLACK: u32 = 8;

#[test]
fn bridged_payloads_do_not_grow_the_vocabulary() {
    let path = tmp_path("vocab");
    cleanup(&path);
    let eng = open_synced(&path);
    let e = eng.entity_in("notes").unwrap();

    write_and_bridge(&eng, e, 0);
    let before = eng.vocab_usage();
    let mut reclaims = 0;
    for v in 1..=WRITES {
        write_and_bridge(&eng, e, v);
        // 相手が消化したことにして ring を回す (reclaim した場所が使い回されること)
        if v % RECLAIM_EVERY == 0 {
            eng.ack_sync(1, eng.current_sync_lsn()).unwrap();
            eng.reclaim_sync_ops();
            reclaims += 1;
        }
    }
    let after = eng.vocab_usage();
    // reclaim は 1 回ごとに `_sync_peers.reclaimed_floor` (24 B) を辞書に書く (これは別件、 回数は ack の頻度
    // 次第で record の数ではない)。 payload が辞書に居ると WRITES 語以上伸びる。
    assert!(
        after.entries - before.entries <= reclaims + VOCAB_SLACK,
        "{WRITES} 回の bridge で辞書が {} 語 / {} B 伸びた (payload が辞書に置かれている)",
        after.entries - before.entries,
        after.data_bytes - before.data_bytes,
    );

    // まだ消化されていない record は、 ring から完全な wire bytes で読める
    let pending = eng.pending_sync_ops(0);
    assert!(!pending.is_empty(), "未消化の record が無い — テスト前提が壊れている");
    for p in &pending {
        assert!(
            enchudb_oplog::oplog::decode_sync_ops_payload(p).is_some(),
            "ring から読んだ payload が decode できない"
        );
    }
    drop(eng);
    cleanup(&path);
}

#[test]
fn ring_payloads_survive_a_reopen() {
    let path = tmp_path("reopen");
    cleanup(&path);
    let before: Vec<Vec<u8>>;
    {
        let eng = open_synced(&path);
        let e = eng.entity_in("notes").unwrap();
        for v in 1..=50 {
            write_and_bridge(&eng, e, v);
        }
        before = eng.pending_sync_ops(0);
        assert!(before.len() >= 50, "bridge されていない ({})", before.len());
        eng.flush_writes();
    }
    let eng = Engine::open(&path).unwrap();
    assert_eq!(eng.pending_sync_ops(0), before, "reopen 後に ring の payload が読めない");

    // reopen 後の書き込みも ring に続けて置ける (head が復元されている)
    let notes = eng.pull("notes.note", 50);
    let e = (*notes.first().expect("note が見えない")).into();
    let lsn = eng.current_sync_lsn();
    eng.tie_to(e, "notes.note", 51);
    eng.oplog_commit();
    eng.flush_writes();
    eng.oplog_sync().unwrap();
    eng.transfer_oplog_to_sync_ops();
    let after = eng.pending_sync_ops(lsn);
    assert_eq!(after.len(), 1, "reopen 後の書き込みが bridge されていない");
    assert!(enchudb_oplog::oplog::decode_sync_ops_payload(&after[0]).is_some());
    // 前の record も壊れていない (新しい entry が古い entry を上書きしていない)
    assert_eq!(eng.pending_sync_ops(0)[..before.len()], before[..]);
    drop(eng);
    cleanup(&path);
}

/// ring の file を開けない (壊れた / 消えた) 時、 ring に payload を置いた row を dead row として消さない。
/// 壊れているのは置き場の方で、 消すと未配送の record を失う。 file を戻せば読める。
#[test]
fn rows_are_kept_while_the_ring_cannot_be_opened() {
    let path = tmp_path("unreachable");
    cleanup(&path);
    let before: Vec<Vec<u8>>;
    {
        let eng = open_synced(&path);
        let e = eng.entity_in("notes").unwrap();
        for v in 1..=20 {
            write_and_bridge(&eng, e, v);
        }
        before = eng.pending_sync_ops(0);
        assert!(before.len() >= 20);
        eng.flush_writes();
    }
    let ring = format!("{path}/sync.payload.seg");
    let saved = format!("{path}.ring-saved");
    std::fs::rename(&ring, &saved).unwrap();
    std::fs::write(&ring, b"not a ring").unwrap();
    {
        let eng = Engine::open(&path).unwrap();
        // 相手が全部消化した後と、 先頭からの prefix ack の両方を走らせる
        eng.ack_sync_up_to_hlc(1, enchudb_oplog::Hlc { wall: u64::MAX, logical: 0, peer: u32::MAX }).ok();
        eng.reclaim_sync_ops();
        eng.flush_writes();
    }
    std::fs::remove_file(&ring).unwrap();
    std::fs::rename(&saved, &ring).unwrap();
    let eng = Engine::open(&path).unwrap();
    assert_eq!(eng.pending_sync_ops(0), before, "ring を開けない間に row が消された");
    drop(eng);
    cleanup(&path);
}

fn file_version(path: &str) -> u32 {
    let b = std::fs::read(format!("{path}/header.seg")).unwrap();
    u32::from_le_bytes(b[4..8].try_into().unwrap())
}

/// 列を離していない DB に ring ができた後で `migrate_column_pad` を流しても、 version を v14 から下げない。
/// 下げると 0.30 の binary が開けてしまい、 `payload_at` を知らないので ring の row を dead row として消す。
#[test]
fn column_pad_migration_keeps_the_ring_version() {
    let path = tmp_path("colpad");
    cleanup(&path);
    let before: Vec<Vec<u8>>;
    {
        let opts = enchudb_engine::GrowableOptions { column_pad: Some(false), ..Default::default() };
        let mut eng = Engine::create_growable_opts(&path, opts).unwrap();
        eng.define_table("notes", 64).unwrap();
        eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
        eng.enable_sync_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
        let e = eng.entity_in("notes").unwrap();
        for v in 1..=5 {
            write_and_bridge(&eng, e, v);
        }
        before = eng.pending_sync_ops(0);
        assert!(before.len() >= 5);
        eng.flush_writes();
    }
    assert_eq!(file_version(&path), 14, "ring を作った DB が v14 になっていない — テスト前提");
    assert!(Engine::migrate_column_pad(&path).unwrap() > 0, "列が移っていない — テスト前提");
    assert_eq!(file_version(&path), 14, "migrate_column_pad が ring の DB の version を下げた");
    let eng = Engine::open(&path).unwrap();
    assert_eq!(eng.pending_sync_ops(0), before, "移行後に ring の payload が読めない");
    drop(eng);
    cleanup(&path);
}
