//! #561 の 2: `_sync_ops` の行を消す purge は WAL に Delete を積まない。
//!
//! `_sync_ops` の行は WAL の record から bridge が作る本体だけの書き込みで、 行を書く Tie も積まない。 消す purge だけが
//! Delete (128 B) を積んでいたので、 落ち着いた sync の DB では自分の record 1 つにつき後から Delete が 1 つ載り、 WAL の量が
//! ほぼ倍だった。 満杯で落ちると配らない record なのに自分の floor を上げ (相手全員が bootstrap)、 再生は slot を使い回した後の
//! 新しい行を消した。
//!
//! 積まない時は、 汚れた終了の後に開く時の作り直し (#451、 載っていない自分の書き込みを探す) が、 purge の削除の版数を
//! 「載らなかった自分の削除」 と読まないこと (内部の表の slot は見ない)。 読むと、 後に書いた record より古いので floor を
//! 上げて相手全員を bootstrap に回す。
//!
//! 落ちた像は、 開いたままの DB の directory を写して作る (写しは閉じる処理を通らない)。

use enchudb_engine::{Engine, ValueType};
use std::sync::Arc;

const ME: u32 = 1;

fn tmp(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("/tmp/enchudb-issue561-{tag}-{}-{nanos}", std::process::id())
}

/// sync の DB (peer 1) に行を `rows` 個書き、 bridge まで済ませる。
fn writer(path: &str, rows: u32) -> Arc<Engine> {
    let mut eng = Engine::create_with_cell_version(path, 65_536).unwrap();
    eng.define_table("notes", 1000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, 16 << 20).unwrap();
    eng.set_peer_id(ME);
    for i in 0..rows {
        let e = eng.entity_in("notes").unwrap();
        eng.tie_to(e, "notes.note", i + 1);
        eng.oplog_commit();
    }
    eng.oplog_sync().unwrap();
    eng
}

/// 相手 (peer 2) が全部を ack した後に reclaim する。 消した行の数。
fn ack_all_and_reclaim(eng: &Engine) -> usize {
    let top = eng.pending_sync_ops(0).len() as u32;
    assert!(top > 0, "前提: bridge した行がある");
    eng.ack_sync(2, eng.stats().oplog_next_lsn as u32 + top).unwrap();
    eng.reclaim_sync_ops()
}

fn copy_dir(from: &str, to: &str) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        let dst = std::path::Path::new(to).join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(e.path().to_str().unwrap(), dst.to_str().unwrap());
        } else if e.file_name() != "lock" {
            std::fs::copy(e.path(), &dst).unwrap();
        }
    }
}

/// purge は WAL に何も積まない (前は消した行 1 つにつき Delete 1 つ)。
#[test]
fn a_purge_puts_nothing_in_the_wal() {
    let path = tmp("wal");
    let eng = writer(&path, 200);
    let before = eng.stats().oplog_next_lsn;
    let purged = ack_all_and_reclaim(&eng);
    assert!(purged >= 200, "前提: 書いた行を消した ({purged})");
    let added = eng.stats().oplog_next_lsn - before;
    assert_eq!(added, 0, "{purged} 行の purge で WAL に {added} record 積んだ");
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}

/// purge の後に書いて汚れた終了をしても、 開く時に floor を上げない (相手を bootstrap に回さない)。 purge の削除の版数は
/// WAL にも `_sync_ops` の行にも無いが、 配らない内部の行なので作り直しも floor も要らない。
#[test]
fn an_unclean_close_after_purges_keeps_the_floor() {
    let (path, image) = (tmp("floor"), tmp("floor-image"));
    let eng = writer(&path, 50);
    let purged = ack_all_and_reclaim(&eng);
    assert!(purged >= 50, "前提: 書いた行を消した ({purged})");
    // purge より後の自分の record (作り直しが 「配った版数」 に数える)
    let e = eng.entity_in("notes").unwrap();
    eng.tie_to(e, "notes.note", 999);
    eng.oplog_commit();
    let floor_before = eng.sync_reclaimed_floors().unwrap_or_default();
    copy_dir(&path, &image);
    drop(eng);

    let back = Engine::open_concurrent_with_oplog(&image, 16 << 20).unwrap();
    assert_eq!(back.unlogged_writes_relogged(), 0, "purge の削除を載らなかった自分の削除として作り直した");
    let mine = |fs: &[(u32, enchudb_oplog::Hlc)]| fs.iter().filter(|(a, _)| *a == ME || *a == u32::MAX).map(|(_, h)| *h).max();
    assert_eq!(
        mine(&back.sync_reclaimed_floors().unwrap_or_default()),
        mine(&floor_before),
        "汚れた終了の後に開いて自分の floor を上げた (相手全員が bootstrap に回る)"
    );
    assert_eq!(back.get(e, "notes.note"), Some(999));
    drop(back);
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(&image);
}
