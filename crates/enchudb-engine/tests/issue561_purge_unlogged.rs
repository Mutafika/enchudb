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

/// 開いたままの DB の directory を写す (閉じる処理を通らない像)。 WAL を最初に写す: 裏の書き出し係は写している間も bridge・
/// 書き出し・畳みを進めるので、 本体を先に写すと 「本体には cell があるが、 行はまだ無く、 record は WAL から畳まれた」 像が
/// できうる (その像では作り直しが正しくその 1 件を作り直す)
fn copy_dir(from: &str, to: &str) {
    std::fs::create_dir_all(to).unwrap();
    let mut entries: Vec<_> = std::fs::read_dir(from).unwrap().map(|e| e.unwrap()).collect();
    entries.sort_by_key(|e| e.file_name() != "oplog");
    for e in entries {
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
/// WAL にも `_sync_ops` の行にも無く、 後に書いた record (作り直しが 「配った版数」 に数える) より古い。 内部の行の削除を
/// 見ると、 載らなかった自分の削除が配った版数より古い = 作り直せないので floor を上げる道に入る。
///
/// 作り直しの数は見ない: 写している間に裏の書き出し係が後の record を畳むと、 その 1 件は正しく作り直される (CI で 1 回踏んだ。
/// 手元の Mac / Linux の Docker では 60 回ずつ再現しなかった)。
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
    assert_eq!(
        my_floor(&back.sync_reclaimed_floors().unwrap_or_default()),
        my_floor(&floor_before),
        "汚れた終了の後に開いて自分の floor を上げた (相手全員が bootstrap に回る)"
    );
    assert_eq!(back.get(e, "notes.note"), Some(999));
    drop(back);
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(&image);
}

/// purge だけをして (後に何も書かず) 汚れた終了をすると、 開く時に何も作り直さない。 内部の行の削除を見ると、 purge の削除を
/// 「載らなかった自分の削除」 として WAL に作り直す (配らない内部の行の Delete を積む)。
#[test]
fn an_unclean_close_right_after_purges_relogs_nothing() {
    let (path, image) = (tmp("relog"), tmp("relog-image"));
    let eng = writer(&path, 50);
    let purged = ack_all_and_reclaim(&eng);
    assert!(purged >= 50, "前提: 書いた行を消した ({purged})");
    let floor_before = eng.sync_reclaimed_floors().unwrap_or_default();
    copy_dir(&path, &image);
    drop(eng);

    let back = Engine::open_concurrent_with_oplog(&image, 16 << 20).unwrap();
    assert_eq!(back.unlogged_writes_relogged(), 0, "purge の削除を載らなかった自分の削除として作り直した");
    assert_eq!(my_floor(&back.sync_reclaimed_floors().unwrap_or_default()), my_floor(&floor_before));
    drop(back);
    let _ = std::fs::remove_dir_all(&path);
    let _ = std::fs::remove_dir_all(&image);
}

/// 自分 (と author を問わない) の floor。
fn my_floor(floors: &[(u32, enchudb_oplog::Hlc)]) -> Option<enchudb_oplog::Hlc> {
    floors.iter().filter(|(a, _)| *a == ME || *a == u32::MAX).map(|(_, h)| *h).max()
}
