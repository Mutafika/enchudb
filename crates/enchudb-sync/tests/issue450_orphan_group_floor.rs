//! #450: WAL が満杯で閉じの Commit が入らなかった group (孤児) を畳む時、 その write の floor を上げる。
//!
//! 孤児の group は Commit で閉じていないので bridge が `_sync_ops` に写さない。 満杯の死区間では `wal_fold_safe` が
//! 「残りが孤児の group だけなら畳んでよい」 として畳む (#268)。 旧: その write は本体に当たっているのに、 WAL に
//! 載らなかった write (#57) として数えなかったので floor を上げず、 畳んだ後は相手に永久に届かなかった。 後に続いた
//! write が落ちれば自分の author は floor = その時の今 で覆われるが、 ここは 1 件も落とさない (閉じの Commit だけが
//! 入らない) 形で見る。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue450-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
}

fn make_engine(path: &str, peer: PeerId, oplog_capacity: usize) -> Arc<Engine> {
    cleanup(path);
    let mut eng = Engine::create_with_cell_version(path, 65_536).unwrap();
    eng.define_table("notes", 10_000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, oplog_capacity).unwrap();
    eng.set_peer_id(peer);
    eng
}

fn floor_of(eng: &Engine, author: PeerId) -> Option<Hlc> {
    eng.sync_reclaimed_floors().unwrap_or_default().into_iter().find(|(a, _)| *a == author).map(|(_, h)| h)
}

/// record の大きさ (oplog file v3、 #608: 周の印 8 B 込み)。 Tie = 112 + 16 + 8、 Commit = 112 + 8。
const TIE: u64 = 136;
const COMMIT: u64 = 120;
/// 準備の残り。 Tie は載り、 その後の閉じの Commit は入らない (残り 50 B)
const ROOM: u64 = TIE + 50;

/// 準備 (WAL の残りを [`ROOM`] にして孤児の group を作る) の途中に、 裏の consumer が WAL に Commit を 1 つ打つことが
/// ある。 満杯に近い WAL を空けに来る段 (#388) は、 畳む lock (準備の間この test が持つ) を試す前に Commit を打つ (100 ms
/// ごとの Commit も同じ)。 入ると残りがちょうど Commit 1 つ分ずれて前提を作れないので、 その時だけ作り直す (他のずれは前提の
/// 誤りとして落とす)。
///
/// 1 区画 (v3) の形の WAL で見る (2 区画にできない大きさ)。 2 区画の形では孤児にならない (↓ の test)。
#[test]
fn a_folded_orphan_group_reaches_the_peer() {
    for attempt in 0..5 {
        if folded_orphan_group_reaches_the_peer(attempt, false) {
            return;
        }
    }
    panic!("5 回とも準備の途中で consumer の Commit が WAL に入り、 前提を作れなかった");
}

/// 0.37 の 2 区画の形: 今の区画に閉じの Commit が入らなくても、 次の Commit がもう一方の区画に入って group を閉じる (切り替えは
/// 閉じの Commit が入らなければ閉じずに移り、 読み手は group を区画をまたいで読む)。 孤児にならず、 相手には普通の差分で届く
/// (floor は上げない)。
#[test]
fn an_unclosed_group_is_closed_in_the_other_half_and_reaches_the_peer() {
    for attempt in 0..5 {
        if folded_orphan_group_reaches_the_peer(attempt, true) {
            return;
        }
    }
    panic!("5 回とも準備の途中で consumer の Commit が WAL に入り、 前提を作れなかった");
}

/// 1 回分。 準備の途中に consumer の Commit が入ったら false (作り直す)。 `two` = 2 区画の形の WAL。
fn folded_orphan_group_reaches_the_peer(attempt: u32, two: bool) -> bool {
    let (pa, pb) = (tmp_path("a"), tmp_path("b"));
    let mem = Arc::new(InMemoryTransport::new());
    mem.register_peer(1);
    mem.register_peer(2);
    let transport: Arc<dyn Transport> = mem.clone();
    let eng_a = make_engine(&pa, 1, if two { 64 * 1024 } else { 24 * 1024 });
    let eng_b = make_engine(&pb, 2, 16 * 1024 * 1024);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());
    sync_a.serve_state();
    let hid = eng_a.himo_id("notes.note").unwrap() as u16;
    let wal = eng_a.oplog().unwrap().clone();
    // #667: 区画を移った時に consumer を起こすと、 移った直後 (下の準備の途中) に必ず Commit が入る。 この試験は満杯の死区間の
    // 状態を手で作るので切る (100 ms ごとの Commit が入った時は今まで通り作り直す)
    eng_a.set_wal_fold_on_switch(false);

    // B は差分 pull で追従している
    let e = eng_a.entity_in("notes").unwrap();
    eng_a.tie_text_to(e, "notes.body", "first");
    eng_a.oplog_sync().unwrap();
    eng_a.transfer_oplog_to_sync_ops();
    sync_a.publish_since(Hlc::ZERO);
    let out = sync_b.pull_once(1);
    assert!(out.applied > 0 && !out.history_truncated, "{out:?}");

    // 満杯の書き手を待たせず (#388)、 畳ませずに WAL を埋める。 今の append は閉じの Commit の分の空きを残す (#511) ので、 孤児の
    // group (閉じの Commit が入らない満杯) は普通の書き込みでは作れない。 前の版の binary が書いた WAL と同じく、 残さない形にして作る
    wal.set_room_waiter(None);
    wal.keep_commit_room_for_test(false);
    // consumer の Commit が 1 つ入ったか: 残りがちょうど Commit 1 つ分ずれる (c を測る間に入れば ROOM + COMMIT、 埋めた後なら
    // ROOM − COMMIT、 Tie の前なら Tie が載らずに ROOM − COMMIT)
    assert_eq!(wal.record_size_of(0) as u64, COMMIT, "前提: Commit の大きさ");
    assert_eq!(wal.record_size_of(16) as u64, TIE, "前提: Tie の大きさ");
    let raced = |free: u64| free.abs_diff(ROOM) == COMMIT;
    let orphan_hlc = {
        let _no_fold = eng_a.transfer_lock_for_fold();
        assert_eq!(wal.two_halves(), two, "前提: WAL の形");
        if two {
            // A を埋めて B へ移る (A は畳めないまま残るので、 空きは B の残りだけ)
            let filler = eng_a.entity_in("notes").unwrap();
            let switched = wal.switches();
            let mut i = 0u32;
            while wal.switches() == switched {
                eng_a.tie_to(filler, "notes.note", i);
                eng_a.oplog_commit();
                i += 1;
            }
        }
        // 閉じた group で WAL を埋め、 残りを ROOM にする。 TieLeaf の record は文字列の長さ + c B
        let e0 = eng_a.entity_in("notes").unwrap();
        let h0 = wal.head();
        eng_a.tie_text_to(e0, "notes.body", "x");
        let c = wal.head() - h0 - 1;
        eng_a.oplog_commit();
        let pad = wal.free_bytes() - COMMIT - ROOM - c;
        let e1 = eng_a.entity_in("notes").unwrap();
        eng_a.tie_text_to(e1, "notes.body", &"p".repeat(pad as usize));
        eng_a.oplog_commit();
        let free = wal.free_bytes();
        if raced(free) {
            None
        } else {
            assert_eq!(free, ROOM, "前提: 残り {ROOM} B");
            // Tie は載り、 残り 50 B に閉じの Commit は入らない
            let orphan = eng_a.entity_in("notes").unwrap();
            let head = wal.head();
            eng_a.tie_to(orphan, "notes.note", 7);
            if raced(wal.free_bytes()) {
                None
            } else {
                assert_eq!(wal.head(), head + TIE, "前提: WAL に載る");
                assert!(wal.append_dead(), "前提: 閉じの Commit が入らない");
                Some(eng_a.cell_hlc(orphan, hid))
            }
        }
    };
    let Some(orphan_hlc) = orphan_hlc else {
        eprintln!("[issue450] 作り直す ({attempt} 回目): 準備の途中で consumer の Commit が WAL に入った");
        drop((sync_a, sync_b, wal, eng_a, eng_b));
        cleanup(&pa);
        cleanup(&pb);
        return false;
    };
    // 死区間では Err を返さない
    eng_a.oplog_sync().unwrap();
    if two {
        // 次の Commit がもう一方の区画に入って group を閉じる: 普通の差分で届き、 floor は上げない
        let end = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            eng_a.oplog_sync().unwrap();
            eng_a.transfer_oplog_to_sync_ops();
            sync_a.publish_since(Hlc::ZERO);
            let out = sync_b.pull_once(1);
            assert!(!out.history_truncated, "孤児にならないはずの group で穴を知らされた: {out:?}");
            if !eng_b.pull_raw("notes.note", 7).is_empty() {
                break;
            }
            assert!(std::time::Instant::now() < end, "閉じていなかった group の note が 5 秒で届かない (head {})", wal.head());
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert_eq!(eng_a.wal_drop_floor_bumps(), 0, "孤児にならないので floor は上げない");
        assert!(floor_of(&eng_a, 1).is_none_or(|f| f < orphan_hlc));
        drop((sync_a, sync_b, wal, eng_a, eng_b));
        cleanup(&pa);
        cleanup(&pb);
        return true;
    }
    // consumer が死区間の例外で畳み、 孤児の group の write を覆う floor を上げる (落ちが止んで 100 ms 後)
    let end = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while floor_of(&eng_a, 1).is_none_or(|f| f < orphan_hlc) {
        assert!(
            std::time::Instant::now() < end,
            "孤児の group を畳んでも floor {:?} が {orphan_hlc:?} を覆わない (head {}、 floor を上げた回数 {})",
            floor_of(&eng_a, 1),
            wal.head(),
            eng_a.wal_drop_floor_bumps()
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(enchudb_oplog::oplog::wal_pos_offset(wal.head()) < 32 * 1024, "前提: 畳んでいる (head {})", wal.head());

    sync_a.publish_since(Hlc::ZERO);
    let out = sync_b.pull_once(1);
    assert!(out.history_truncated, "孤児の group の穴を知らされない: {out:?}");
    sync_b.bootstrap_pull(1).expect("serve_state 済み");
    assert!(!eng_b.pull_raw("notes.note", 7).is_empty(), "B に孤児の group の note が届いていない");

    drop((sync_a, sync_b, wal, eng_a, eng_b));
    cleanup(&pa);
    cleanup(&pb);
    true
}
