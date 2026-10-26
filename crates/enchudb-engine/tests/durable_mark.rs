//! #451 (段階 1、 OPEN_TIME_PLAN): 配り済みの印。 書き出しが返るたびに payload の ring の header に (W, S) を置き、 開く時は
//! 印より後 (HLC > W の cell、 lsn > S の行) だけを照合する。 ここは電源断なしで見える約束: 印の進み方 (切れ目は Commit の前、
//! 置くのは書き出しの後)、 落ちた write (#57) が floor を持つまで印が止まること、 開き直した時に印の S 以下の lsn を出さないこと。
//! 電源断の像は `power_loss.rs` (crashsim)。

use enchudb_engine::sync_payload_ring::{DurableMark, PayloadRing};
use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::oplog::{decode_sync_ops_payload, DecodedOp};
use enchudb_oplog::Hlc;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

const OPLOG_CAP: usize = 4 * 1024 * 1024;

fn scratch(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("enchu_durable_mark_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// sync する DB (cell の版数あり)、 note (Number) / body (Leaf)、 peer 1。
fn notes_engine(db: &Path, oplog_cap: usize) -> Arc<Engine> {
    let mut eng = Engine::create_with_cell_version(db.to_str().unwrap(), 65_536).unwrap();
    eng.define_table("notes", 1_000).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    eng.flush().unwrap();
    eng.persist_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, oplog_cap).unwrap();
    eng.set_peer_id(1);
    eng
}

fn floor_of(eng: &Engine, author: u32) -> Option<Hlc> {
    eng.sync_reclaimed_floors().unwrap_or_default().into_iter().find(|(a, _)| *a == author).map(|(_, h)| h)
}

/// 畳む lock (`transfer_oplog_to_sync_ops` も取る) を持って呼ぶ。 裏の consumer は 100 ms ごとの書き出しで Commit を 1 つ打ってから
/// bridge でこの lock を取りに行って止まる。 WAL を埋める準備の途中に入ると残りが Commit 1 つ分ずれる (#648) ので、 ここで Commit を
/// 1 つ打って consumer に書き出させ、 consumer の Commit が入るのを待つ = その後は lock を離すまで consumer は WAL に書かない。 lock を
/// 取った時に consumer がもう lock の前に居る (書き出しを終えていた / 畳みに来ていた) なら Commit は来ないので、 5 秒で見切る。
fn let_the_consumer_commit_and_wait_on_the_lock(eng: &Engine) {
    let wal = eng.oplog().unwrap();
    eng.oplog_commit();
    let ours = wal.head();
    let end = Instant::now() + Duration::from_secs(5);
    while wal.head() == ours && Instant::now() < end {
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// 印は書き出し (`oplog_sync`) のたびに進み、 切れ目 (Commit の前) より後に書いた write は覆わない。 置くのは bridge の後なので、
/// 最初の書き出し (ring はその bridge が作る) でも置ける。
#[test]
fn the_mark_follows_each_durable_point_and_never_covers_later_writes() {
    let root = scratch("follow");
    let eng = notes_engine(&root.join("db"), OPLOG_CAP);
    let note = eng.himo_id("notes.note").unwrap() as u16;
    assert_eq!(eng.durable_mark(), None, "ring が無いうちは印が無い");
    let a = eng.entity_in("notes").unwrap();
    eng.tie_to(a, "notes.note", 1);
    let ha = eng.cell_hlc(a, note);
    eng.oplog_sync().unwrap();
    let m0 = eng.durable_mark().expect("最初の書き出し (bridge の後) で印が置かれる");
    assert!(m0.hlc >= ha && m0.lsn >= 1, "最初の印: 切れ目の後の bridge が作った行 (record は切れ目以下) も S に入る: {m0:?} {ha:?}");
    let b = eng.entity_in("notes").unwrap();
    eng.tie_to(b, "notes.note", 2);
    let hb = eng.cell_hlc(b, note);
    eng.oplog_sync().unwrap();
    let m = eng.durable_mark().expect("書き出しのたびに印が置かれる");
    assert!(m.hlc >= hb, "書き出した write は印に入る: {m:?} {hb:?}");
    assert_eq!(m.hlc.peer, 1);
    assert!(m.lsn >= 1, "1 回目の bridge の行は印の S に入る: {m:?}");
    let c = eng.entity_in("notes").unwrap();
    eng.tie_to(c, "notes.note", 3);
    let hc = eng.cell_hlc(c, note);
    assert!(eng.durable_mark().unwrap().hlc < hc, "書き出す前の write を印は覆わない");
    eng.oplog_sync().unwrap();
    let m2 = eng.durable_mark().unwrap();
    assert!(m2.hlc >= hc && m2.lsn > m.lsn, "印は進む: {m2:?} (前 {m:?})");
    assert!(eng.durable_mark_publishes() >= 2);
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
}

/// WAL が満杯で落ちた write (#57) の floor がまだ無い間は印を進めない。 bridge を止めたまま落とし、 bridge に落ちた record を
/// 持ち越させてから (floor は静かになるまで上げない) 書き出し (`oplog_sync`、 Commit は入る大きさを残す) を走らせる: 印が落ちた
/// write を越えるのは floor が記録された後だけ。 floor が上がった後の書き出しで、 印は落ちた write を越える。
/// (WAL が覚えている段階で止まることは `power_loss.rs` の `a_mark_never_covers_a_dropped_write_before_its_floor_is_on_disk`)
#[test]
fn dropped_writes_hold_the_mark_until_their_floor_is_recorded() {
    let root = scratch("dropped");
    let eng = notes_engine(&root.join("db"), 64 * 1024);
    let note = eng.himo_id("notes.note").unwrap() as u16;
    let wal = eng.oplog().unwrap().clone();
    wal.set_room_waiter(None);
    let e0 = eng.entity_in("notes").unwrap();
    eng.tie_to(e0, "notes.note", 100);
    eng.oplog_sync().unwrap();
    eng.oplog_sync().unwrap();
    let before = eng.durable_mark().expect("前提: 印が置かれている");
    let mut dropped = Vec::new();
    {
        let _no_fold = eng.transfer_lock_for_fold();
        let_the_consumer_commit_and_wait_on_the_lock(&eng);
        // 2 区画: A を埋めて B へ移る (A は畳めないまま残るので、 空きは B の残りだけ)
        let filler = eng.entity_in("notes").unwrap();
        let switched = wal.switches();
        let mut i = 0u32;
        while wal.switches() == switched {
            eng.tie_to(filler, "notes.note", i);
            eng.oplog_commit();
            i += 1;
        }
        let e1 = eng.entity_in("notes").unwrap();
        let h0 = wal.head();
        eng.tie_text_to(e1, "notes.body", "x");
        let c = wal.head() - h0 - 1;
        eng.oplog_commit();
        let commit = wal.record_size_of(0) as u64;
        let pad = wal.free_bytes() - commit - (commit + 60) - c;
        let e2 = eng.entity_in("notes").unwrap();
        eng.tie_text_to(e2, "notes.body", &"p".repeat(pad as usize));
        eng.oplog_commit();
        assert_eq!(wal.free_bytes(), commit + 60, "前提: Commit は入るが Tie は入らない残り");
        for i in 0..5u32 {
            let head = wal.head();
            let e = eng.entity_in("notes").unwrap();
            eng.tie_to(e, "notes.note", i);
            assert_eq!(wal.head(), head, "前提: WAL に載らない");
            dropped.push(eng.cell_hlc(e, note));
        }
    }
    let max_dropped = dropped.iter().copied().max().unwrap();
    // bridge に落ちた record を持ち越させる (floor は静かになってからなので、 まだ上がらない)。 その上で書き出す
    eng.transfer_oplog_to_sync_ops();
    eng.oplog_sync().unwrap();
    let mark = eng.durable_mark().unwrap();
    let floor = floor_of(&eng, 1);
    assert!(
        mark == before || floor.is_some_and(|f| f >= max_dropped),
        "floor {floor:?} が無いのに印が落ちた write を越えた: {mark:?} (前 {before:?}、 落ちた {max_dropped:?})"
    );
    // 静かになってから floor が上がる
    let deadline = Instant::now() + Duration::from_secs(10);
    while floor_of(&eng, 1).is_none_or(|f| f < max_dropped) {
        assert!(Instant::now() < deadline, "前提: 落ちた write の floor が 10 秒で上がらない");
        std::thread::sleep(Duration::from_millis(20));
    }
    // consumer は floor を上げた周の続きで書き出してから WAL を畳む。 畳まれる前に書くと e3 も WAL に載らずに落ちて、 その floor が
    // 無いので印は止まったまま (#652)。 畳まれるのも待つ
    while wal.free_bytes() < wal.capacity() / 2 {
        assert!(Instant::now() < deadline, "前提: floor が上がった後、 WAL が畳まれない (head {})", wal.head());
        std::thread::sleep(Duration::from_millis(20));
    }
    let e3 = eng.entity_in("notes").unwrap();
    let head = wal.head();
    eng.tie_to(e3, "notes.note", 7);
    assert!(wal.head() > head, "前提: e3 は WAL に載る");
    eng.oplog_sync().unwrap();
    let after = eng.durable_mark().unwrap();
    assert!(after.hlc >= max_dropped, "floor が上がった後の書き出しで印は進む: {after:?} {max_dropped:?}");
    drop((wal, eng));
    let _ = std::fs::remove_dir_all(&root);
}

/// 開き直した後に出す lsn は印の S より大きい (行を全部回収した後でも。 印は 「lsn ≤ S の行は切れ目より前」 を言うので、 S 以下を
/// 出し直すと相手の cursor と食い違う)。 行から戻せる lsn が小さいままの形を、 印の S だけを先に進めて作る。
#[test]
fn a_reopen_does_not_hand_out_lsns_at_or_below_the_mark() {
    let root = scratch("reopen_lsn");
    let db = root.join("db");
    let m = {
        let eng = notes_engine(&db, OPLOG_CAP);
        let a = eng.entity_in("notes").unwrap();
        eng.tie_to(a, "notes.note", 1);
        eng.oplog_sync().unwrap();
        eng.oplog_sync().unwrap();
        eng.durable_mark().expect("前提: 印")
    };
    assert!(m.lsn < 1000);
    {
        // 閉じるまでに印がもう 1 歩進むことがある (consumer の周期の書き出し / 閉じる時の書き出し、 #649) ので、 ring の今の印から作る
        let ring = PayloadRing::open_or_create(&db, 16 << 20, false).unwrap();
        let now = ring.durable_mark().expect("前提: 印");
        assert!(ring.set_durable_mark(DurableMark { lsn: 1000, ..now }));
        ring.flush().unwrap();
    }
    let eng = Engine::open_concurrent_with_oplog(db.to_str().unwrap(), OPLOG_CAP).unwrap();
    let y = eng.entity_in("notes").unwrap();
    eng.tie_to(y, "notes.note", 9);
    eng.oplog_sync().unwrap();
    let after_mark: Vec<_> = eng.pending_sync_ops(1000).iter().filter_map(|p| decode_sync_ops_payload(p)).collect();
    assert!(
        after_mark.iter().any(|r| matches!(r.op, DecodedOp::Tie { value: 9, .. })),
        "開き直した後の行の lsn が印の S (1000) 以下に戻った: {} 行",
        after_mark.len()
    );
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
}

/// 切れ目の後に載った自分の record (HLC > C) の行は、 その切れ目の印の S に入らない (入れると、 開く時にその行を読まず、 その record が
/// oplog から畳まれていれば作り直す = 重ねて配る)。 bridge を止めたまま書き出しを始め (切れ目 C)、 その後に x を書いてから bridge を
/// 放す: 書き出しの bridge は a (≤ C) と x (> C) の行を作るが、 S は x の行の手前まで。
#[test]
fn rows_from_records_after_the_cut_are_not_covered_by_its_mark() {
    let root = scratch("after_cut");
    let eng = notes_engine(&root.join("db"), OPLOG_CAP);
    let a = eng.entity_in("notes").unwrap();
    eng.tie_to(a, "notes.note", 1);
    let syncer = {
        let _no_bridge = eng.transfer_lock_for_fold();
        let syncer = {
            let eng = eng.clone();
            std::thread::spawn(move || eng.oplog_sync())
        };
        // 切れ目 (と Commit、 書き出し) の後、 bridge で止まっている間に x を書く
        std::thread::sleep(Duration::from_millis(300));
        let x = eng.entity_in("notes").unwrap();
        eng.tie_to(x, "notes.note", 7);
        eng.oplog_commit();
        syncer
    };
    syncer.join().unwrap().unwrap();
    let m = eng.durable_mark().expect("印");
    let after: Vec<_> = eng.pending_sync_ops(m.lsn).iter().filter_map(|p| decode_sync_ops_payload(p)).collect();
    assert!(
        after.iter().any(|r| matches!(r.op, DecodedOp::Tie { value: 7, .. })),
        "切れ目の後の record (x) の行が印の S {} に覆われた ({} 行が S より後)",
        m.lsn,
        after.len()
    );
    assert!(
        !after.iter().any(|r| matches!(r.op, DecodedOp::Tie { value: 1, .. })),
        "切れ目の前の record (a) の行は S に入る: S = {}",
        m.lsn
    );
    drop(eng);
    let _ = std::fs::remove_dir_all(&root);
}

/// 切れ目の後に打つ Commit が満杯で畳みを起こすと (閉じの Commit も入らない死区間、 #450)、 切れ目以下の閉じていない record が孤児として
/// 落ちる (floor はまだ無い、 HLC は切れ目より前)。 その書き出しでは印を置かない (置くと孤児が印に覆われ、 開く時に見ず相手に届かない)。
/// 普通の書き込みは閉じの Commit の分の空きを残すので、 この形は試験の API (`keep_commit_room_for_test(false)`) で作る。
#[test]
fn an_orphaned_group_dropped_by_the_commit_after_the_cut_is_not_covered() {
    let root = scratch("orphan");
    // 満杯の死区間 (閉じの Commit も入らない) で consumer が畳む形は 1 区画 (v3) の形の道なので、 2 区画にできない大きさ
    // (`2 * HALF_ALIGN` 未満)。 2 区画は区画を移る時に閉じの Commit で group を閉じる
    let eng = notes_engine(&root.join("db"), 24 * 1024);
    let note = eng.himo_id("notes.note").unwrap() as u16;
    let wal = eng.oplog().unwrap().clone();
    wal.keep_commit_room_for_test(false);
    let e0 = eng.entity_in("notes").unwrap();
    eng.tie_to(e0, "notes.note", 100);
    eng.oplog_sync().unwrap();
    let before = eng.durable_mark().expect("前提: 印");
    // 閉じた group で埋め、 残りを Commit も入らない大きさにする。 最後の record (e2) は閉じていない group = 畳む時の孤児
    let e1 = eng.entity_in("notes").unwrap();
    let h0 = wal.head();
    eng.tie_text_to(e1, "notes.body", "x");
    let c = wal.head() - h0 - 1;
    eng.oplog_commit();
    let commit = wal.record_size_of(0) as u64;
    let pad = wal.free_bytes() - 50 - c;
    let e2 = eng.entity_in("notes").unwrap();
    eng.tie_text_to(e2, "notes.body", &"p".repeat(pad as usize));
    let h2 = eng.cell_hlc(e2, eng.himo_id("notes.body").unwrap() as u16);
    assert!(wal.free_bytes() < commit, "前提: Commit も入らない残り ({} < {commit})", wal.free_bytes());
    let _ = note;
    // 切れ目 → Commit が満杯 → consumer が死区間で畳む (e2 を孤児として落とす) → 新しい周で Commit が通る → 書き出し → 置く?
    eng.oplog_sync().unwrap();
    assert!(eng.wal_dropped_records() >= 1, "前提: 畳みで孤児が落ちた");
    let mark = eng.durable_mark().unwrap();
    let floor = floor_of(&eng, 1);
    assert!(
        mark == before || floor.is_some_and(|f| f >= h2) || mark.hlc < h2,
        "floor {floor:?} の無い孤児 {h2:?} を印 {mark:?} が覆った (前 {before:?})"
    );
    drop((wal, eng));
    let _ = std::fs::remove_dir_all(&root);
}
