//! #846: `_sync_ops` の ring が満杯で止まった bridge は、 止まっている間 WAL を読み直さない。
//!
//! 止まった bridge は cursor の record を ring に置けず、 ring が空く (ack → reclaim) まで何周しても同じ所で止まる。 前は周の
//! たびに cursor から先の commit 済みの record を全部読んで decode していた: ack の来ない相手の DB (ring も WAL も満杯) は、
//! 何も書かなくても consumer が周期ごとに WAL の全 record を読み直し続けた (64 MiB の WAL で 5 秒に CPU 2.5 秒)。
//!
//! 直し: 止まった所 (cursor、 ring の行の数と枠、 読み終えた位置、 残っている record の鍵の要約) を覚え、 cursor と ring が
//! そのままの間は WAL に増えた分だけを読む。 consumer は、 止まったままで見直す物が無い間、 空け作りを回さず 1 秒ごとに起きる
//! (行が消えた / WAL に record が載った・落ちた時は起こされる)。
//!
//! 「読み直さない」 は bridge が WAL から decode した record の数 (`Engine::bridge_wal_records_read`) で見る。

use enchudb_engine::transport::WireRecord;
use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ME: PeerId = 1;
const AUTHOR: PeerId = 9;
const PULLER: PeerId = 77;
const WAL: usize = 16 * 1024 * 1024;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!("enchudb-issue846-{tag}-{}-{nanos}", std::process::id())).to_str().unwrap().to_string()
}

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn create_with_wal(path: &str, wal: usize) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(path, 1024).unwrap();
    eng.define_table("notes", 8).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.define_himo_in("notes", "body", ValueType::Leaf, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, wal).unwrap();
    eng.set_peer_id(ME);
    eng
}

fn create(path: &str) -> Arc<Engine> {
    create_with_wal(path, WAL)
}

fn drain(eng: &Engine) {
    while eng.transfer_oplog_to_sync_ops() > 0 {}
}

fn decoded(payloads: Vec<Vec<u8>>) -> Vec<enchudb_oplog::oplog::Record> {
    payloads.iter().filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p)).collect()
}

/// ring の行 (lsn の順) の、 `notes.note` に書いた値。
fn notes(payloads: Vec<Vec<u8>>) -> Vec<u64> {
    decoded(payloads)
        .into_iter()
        .filter_map(|r| match r.op {
            DecodedOp::Tie { value, .. } => Some(value),
            _ => None,
        })
        .collect()
}

/// ring の行のうち鍵 (書き手, 版数) が `key` の物の数 (Commit は除く)。
fn with_key(payloads: Vec<Vec<u8>>, key: (PeerId, Hlc)) -> usize {
    decoded(payloads).iter().filter(|r| !matches!(r.op, DecodedOp::Commit) && (r.author_peer, r.hlc) == key).count()
}

fn ring_full(eng: &Engine) -> bool {
    let u = eng.table_eid_usage("_sync_ops").unwrap();
    u.live == u.capacity
}

/// ring を満杯にして止める (書いている間は bridge も畳みも止める)。 `from` から `ring + extra` 個の値を書く。 返すのは ring の枠の数。
fn fill(eng: &Engine, e: u64, from: u32, extra: u32) -> u32 {
    let ring = eng.table_eid_usage("_sync_ops").unwrap().capacity;
    {
        let _no_bridge = eng.transfer_lock_for_fold();
        for i in 0..ring + extra {
            eng.tie_to(e, "notes.note", from + i);
        }
        eng.oplog_commit();
    }
    drain(eng);
    assert!(ring_full(eng), "前提: ring が満杯");
    assert!(eng.sync_ops_bridge_offset() < eng.stats().oplog_head, "前提: bridge は WAL を読み切っていない (止まっている)");
    ring
}

/// ring の行を受け手が全部受けた事にして回収する。
fn ack_all(eng: &Engine) -> usize {
    let top = decoded(eng.pending_sync_ops(0)).iter().filter(|r| r.author_peer == ME).map(|r| r.hlc).max().unwrap();
    eng.ack_sync_up_to_cursors(PULLER, &[(ME, top)]).unwrap();
    eng.reclaim_sync_ops()
}

fn until(what: &str, limit: Duration, mut done: impl FnMut() -> bool) {
    let t0 = Instant::now();
    while !done() {
        assert!(t0.elapsed() < limit, "{what} ({limit:?} 待った)");
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// 止まった bridge は、 何周しても WAL を読み直さない。 開き直した後 (cursor は 「頭から」) も、 止まった最初の周が読んだ後は読まない。
///
/// falsify: 覚えを使わない (`resume_stalled_bridge` が常に None) と、 周のたびに残りの record を全部読む。
#[test]
fn a_stalled_bridge_does_not_reread_the_wal() {
    let path = tmp_path("idle");
    let _cleanup = Cleanup(path.clone());
    {
        let eng = create(&path);
        let e = eng.entity_in("notes").unwrap();
        fill(&eng, e, 0, 300);
        let read = eng.bridge_wal_records_read();
        for _ in 0..50 {
            assert_eq!(eng.transfer_oplog_to_sync_ops(), 0, "前提: ring が満杯のまま (1 行も写せない)");
        }
        assert_eq!(eng.bridge_wal_records_read(), read, "止まった bridge が、 何も書いていないのに WAL を読み直した");
        // 配ってよい所は、 止まっている間も置いてある (割れた組は無い)
        assert_eq!(eng.settled_sync_ops().len(), eng.pending_sync_ops(0).len());
        assert_eq!(eng.bridge_wal_records_read(), read, "配る側の 1 周が WAL を読み直した");
        eng.oplog_sync().unwrap();
        assert_eq!(Arc::strong_count(&eng), 1);
    }
    let eng = Engine::open_concurrent_with_oplog(&path, WAL).unwrap();
    eng.set_peer_id(ME);
    drain(&eng);
    assert!(ring_full(&eng), "前提: 開き直しても ring は満杯");
    assert!(eng.sync_ops_bridge_offset() < eng.stats().oplog_head, "前提: 開き直した後も bridge は止まっている");
    let read = eng.bridge_wal_records_read();
    assert!(read >= 300, "前提: 開き直した後の最初の周は WAL を読む (読んだ {read} 件)");
    for _ in 0..50 {
        assert_eq!(eng.transfer_oplog_to_sync_ops(), 0);
    }
    assert_eq!(eng.bridge_wal_records_read(), read, "開き直した後、 止まった bridge が周のたびに WAL を読み直した");
}

/// 止まっている間に書いた record は、 増えた分だけ読む (cursor から読み直さない)。 ring が空いたら、 止まる前の record も
/// 止まっている間の record も、 書いた順に 1 回ずつ ring に載る。
///
/// falsify: 続きを読む位置を進めない (`scanned_end` を置かない) と増えた分を周のたびに読み直す。 ring の印を見ない
/// (行が消えても覚えを使う) と、 ack の後も 1 行も写さない。
#[test]
fn only_new_records_are_read_while_stalled_and_bridging_resumes_after_an_ack() {
    const EXTRA: u32 = 300;
    const ROUNDS: u32 = 40;
    let path = tmp_path("resume");
    let _cleanup = Cleanup(path.clone());
    let eng = create(&path);
    let e = eng.entity_in("notes").unwrap();
    let ring = fill(&eng, e, 0, EXTRA);
    let mut seen = notes(eng.pending_sync_ops(0));
    let read = eng.bridge_wal_records_read();
    for i in 0..ROUNDS {
        eng.tie_to(e, "notes.note", 1_000_000 + i);
        eng.oplog_commit();
        assert_eq!(eng.transfer_oplog_to_sync_ops(), 0, "前提: ring が満杯のまま");
    }
    let grew = eng.bridge_wal_records_read() - read;
    // 1 周に増えるのは Tie 1 つ + Commit 1 つ (consumer の tick の Commit が混ざっても数個)
    assert!(grew >= u64::from(ROUNDS), "前提: 止まっている間に載った record は読む (読んだ {grew} 件)");
    assert!(
        grew <= u64::from(4 * ROUNDS),
        "止まっている間、 WAL に増えた分 ({} 件ほど) でなく残りの record を読み直した ({grew} 件、 残り {EXTRA} 件)",
        2 * ROUNDS
    );

    // ring が空いたら続きから写す。 全部で、 書いた順に 1 回ずつ
    let mut reclaimed = 0;
    for _ in 0..8 {
        reclaimed += ack_all(&eng);
        drain(&eng);
        let last = seen.last().copied();
        seen.extend(notes(eng.pending_sync_ops(0)).into_iter().filter(|v| last.is_none_or(|last| *v > last)));
        if eng.sync_ops_bridge_offset() >= eng.stats().oplog_head {
            break;
        }
    }
    assert!(reclaimed + 8 >= ring as usize, "前提: ring の行を回収した (回収 {reclaimed} 行、 ring {ring} 行)");
    let want: Vec<u64> = (0..ring + EXTRA).map(u64::from).chain((0..ROUNDS).map(|i| u64::from(1_000_000 + i))).collect();
    assert_eq!(seen.len(), want.len(), "ring に載った record の数が、 書いた数と違う");
    assert_eq!(seen, want, "ring が空いた後、 書いた順に 1 回ずつ載らなかった");
}

/// 止まっている間に、 ring にもう載っている行と同じ (書き手, 版数) の record が WAL に増えたら (中継した state の組の続き)、
/// その組の行は配らない (#828)。 増えた分だけを読む道でも、 cursor から読み直した時と同じ答え。
///
/// falsify: 増えた record の鍵を覚えに足さないと、 組の前半を配る。
#[test]
fn a_group_continued_while_stalled_is_held_back() {
    const HALF: usize = 20;
    let path = tmp_path("group");
    let _cleanup = Cleanup(path.clone());
    let eng = create(&path);
    let e = eng.entity_in("notes").unwrap();
    fill(&eng, e, 0, 10);
    ack_all(&eng);
    drain(&eng);
    let left = eng.table_eid_usage("_sync_ops").unwrap().free as usize;
    assert!(left > 2 * HALF, "前提: ring が空いた (空き {left})");

    // 空きを HALF だけ残して埋め、 組の前半 HALF 本を載せる (ring がちょうど埋まる)。 その後ろの自分の record が置けずに止まる
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    let key = (AUTHOR, Hlc { wall: now + 60_000, logical: 0, peer: AUTHOR });
    let relay = |vids: std::ops::Range<usize>| {
        for vid in vids {
            let rec = WireRecord::unsigned(key.1, AUTHOR, DecodedOp::Vocab { vid: vid as u32, bytes: format!("w{vid}").into_bytes() });
            assert!(eng.relay_record(&rec), "前提: 組の record を WAL に載せた");
        }
    };
    {
        let _no_bridge = eng.transfer_lock_for_fold();
        for i in 0..(left - HALF) as u32 {
            eng.tie_to(e, "notes.note", 3_000_000 + i);
        }
        relay(0..HALF);
        for i in 0..5u32 {
            eng.tie_to(e, "notes.note", 4_000_000 + i);
        }
        eng.oplog_commit();
    }
    drain(&eng);
    assert!(ring_full(&eng), "前提: ring が満杯");
    assert_eq!(with_key(eng.pending_sync_ops(0), key), HALF, "前提: 組の前半が全部 ring に載っている");
    assert_eq!(with_key(eng.settled_sync_ops(), key), HALF, "前提: 続きが WAL に無い間は、 載っている組の行は配ってよい");

    // 止まったまま、 組の続きが WAL に載る
    let read = eng.bridge_wal_records_read();
    relay(HALF..2 * HALF);
    eng.oplog_commit();
    assert_eq!(eng.transfer_oplog_to_sync_ops(), 0, "前提: ring が満杯のまま");
    let grew = eng.bridge_wal_records_read() - read;
    assert!(grew >= HALF as u64 && grew <= 2 * HALF as u64 + 8, "前提: 増えた分だけ読んだ (読んだ {grew} 件)");
    assert_eq!(with_key(eng.settled_sync_ops(), key), 0, "止まっている間に続きが WAL に載った組の、 前半を配った");
    for _ in 0..5 {
        eng.transfer_oplog_to_sync_ops();
        assert_eq!(with_key(eng.settled_sync_ops(), key), 0, "割れた組の前半を配った");
    }

    // 組の前の行を受け手が受けて ack → 続きが載って、 組がそろう
    ack_all(&eng);
    drain(&eng);
    let handed = with_key(eng.settled_sync_ops(), key);
    let floor = eng.sync_reclaimed_floors().unwrap_or_default().into_iter().find(|(a, _)| *a == AUTHOR).map(|(_, h)| h);
    assert!(
        handed == 2 * HALF || (handed == 0 && floor >= Some(key.1)),
        "組は、 全部載ってから配るか、 配らずに書き手の floor を組の版数に上げる (配ってよい行に {handed} 本、 floor {floor:?})"
    );
}

/// ring も WAL も満杯で落ちた書き込み (#57) の floor は、 止まった bridge が WAL を読み直さなくても上がる。 閉じる時は待たずに
/// 上げる (#449)。 ring が空けば WAL が畳めて、 また書ける。
///
/// falsify: 止まった周が落ちた record を見ない (`wal_drop_floors_due` を呼ばない) と floor が上がらない。
#[test]
fn dropped_writes_raise_the_floor_while_the_bridge_is_stalled() {
    const SMALL_WAL: usize = 256 * 1024;
    let path = tmp_path("drop");
    let _cleanup = Cleanup(path.clone());
    let own_floor = |eng: &Engine| eng.sync_reclaimed_floors().unwrap_or_default().into_iter().find(|(a, _)| *a == ME).map(|(_, h)| h);
    let dropped_at;
    {
        let eng = create_with_wal(&path, SMALL_WAL);
        let e = eng.entity_in("notes").unwrap();
        // ring を埋める (WAL は ring より小さいので、 少しずつ書いて書き出す)
        let mut v = 0u32;
        until("ring が満杯にならない", Duration::from_secs(60), || {
            for _ in 0..50 {
                eng.tie_to(e, "notes.note", v);
                v += 1;
            }
            eng.oplog_sync().unwrap();
            ring_full(&eng)
        });
        // WAL も埋める (bridge が読み切れないので畳めない)
        until("WAL が満杯にならない", Duration::from_secs(60), || {
            eng.tie_to(e, "notes.note", v);
            v += 1;
            eng.wal_dropped_records() > 0
        });
        assert_eq!(eng.wal_room_wait_timeouts(), 0, "空きを待つ書き手が上限まで待った (畳めない状態はすぐ分かるはず)");
        let read = eng.bridge_wal_records_read();
        let bumps = eng.wal_drop_floor_bumps();
        // 落ちが止んで静かになったら (consumer の周期が) floor を上げる
        until("落ちた書き込みの floor が上がらない", Duration::from_secs(30), || eng.wal_drop_floor_bumps() > bumps);
        let floor = own_floor(&eng).expect("自分の floor");
        assert!(eng.bridge_wal_records_read() - read < 64, "止まった bridge が、 落ちた record のたびに WAL を読み直した");

        // もう 1 回落として、 静かになるのを待たずに閉じる
        let dropped = eng.wal_dropped_records();
        eng.tie_to(e, "notes.note", v);
        assert!(eng.wal_dropped_records() > dropped, "前提: WAL は満杯のまま (書き込みが落ちる)");
        dropped_at = floor;
        assert_eq!(Arc::strong_count(&eng), 1);
    }
    let eng = Engine::open_concurrent_with_oplog(&path, SMALL_WAL).unwrap();
    eng.set_peer_id(ME);
    let floor = own_floor(&eng).expect("閉じた後の自分の floor");
    assert!(floor > dropped_at, "閉じる直前に落ちた書き込みの floor が、 閉じた後に残っていない ({floor:?} <= {dropped_at:?})");

    // ring が空けば bridge が進み、 WAL が畳めて、 また書ける
    let e = eng.entities_with_himo(eng.himo_id("notes.note").unwrap() as u16).into_iter().next().expect("前提: 行が残っている");
    until("ring を空けても書けるようにならない", Duration::from_secs(60), || {
        drain(&eng);
        ack_all(&eng);
        eng.oplog_sync().unwrap();
        let dropped = eng.wal_dropped_records();
        eng.tie_to(e, "notes.note", 9_999_999u32);
        eng.wal_dropped_records() == dropped
    });
}

/// 止まっている間に落ちた record より後の record が WAL に残っていたら、 floor が上がるまで 「止めた」 と数える (#783。 cursor
/// から読み直す道と同じ)。 大きい record だけが載らない満杯で作る: 小さい record はその後も載る。
///
/// falsify: 止まった周が、 残っている record の書き手ごとのいちばん新しい版数を見ないと、 数えない。
#[test]
fn a_record_after_a_dropped_one_is_counted_as_held_while_stalled() {
    const SMALL_WAL: usize = 256 * 1024;
    const BIG: usize = 16 * 1024;
    let path = tmp_path("held");
    let _cleanup = Cleanup(path.clone());
    let eng = create_with_wal(&path, SMALL_WAL);
    let e = eng.entity_in("notes").unwrap();
    let mut v = 0u32;
    until("ring が満杯にならない", Duration::from_secs(60), || {
        for _ in 0..50 {
            eng.tie_to(e, "notes.note", v);
            v += 1;
        }
        eng.oplog_sync().unwrap();
        ring_full(&eng)
    });
    // 大きい record が載らない所まで小さい record で埋める (小さい record はまだ載る)
    until("WAL が埋まらない", Duration::from_secs(60), || {
        eng.tie_to(e, "notes.note", v);
        v += 1;
        eng.wal_free_bytes() < BIG as u64
    });
    assert_eq!(eng.wal_dropped_records(), 0, "前提: 小さい record は落ちていない");
    let big = "x".repeat(BIG);
    let mut held = false;
    // floor を上げた直後 (次に上げるまで 1 秒空ける間) に、 もう 1 回落として、 その後ろに小さい record を載せる。 遅い環境で
    // 1 秒を過ぎると止めずに上げるので、 何回か試す
    for _ in 0..5 {
        let (dropped, bumps) = (eng.wal_dropped_records(), eng.wal_drop_floor_bumps());
        eng.tie_text_to(e, "notes.body", &big);
        assert!(eng.wal_dropped_records() > dropped, "前提: 大きい record が載らずに落ちた");
        until("落ちた record の floor が上がらない", Duration::from_secs(30), || eng.wal_drop_floor_bumps() > bumps);
        let (dropped, holds, read) = (eng.wal_dropped_records(), eng.wal_drop_bridge_holds(), eng.bridge_wal_records_read());
        eng.tie_text_to(e, "notes.body", &big);
        assert!(eng.wal_dropped_records() > dropped, "前提: 大きい record がまた落ちた");
        eng.tie_to(e, "notes.note", v);
        v += 1;
        assert_eq!(eng.wal_dropped_records(), dropped + 1, "前提: 落ちた record の後ろの小さい record は載った");
        eng.oplog_commit();
        eng.transfer_oplog_to_sync_ops();
        assert!(ring_full(&eng), "前提: ring は満杯のまま");
        assert!(eng.bridge_wal_records_read() - read < 64, "止まった bridge が WAL を読み直した");
        if eng.wal_drop_bridge_holds() > holds {
            held = true;
            break;
        }
    }
    assert!(held, "落ちた record より後の record が WAL に残っているのに、 止めたと数えなかった (5 回とも)");
}

/// 止まったままの DB の consumer は、 何も書かなければ 1 秒ごとにしか起きない (前は 100 ms ごとに起きて空け作りと bridge を
/// 回した)。 ack で行が消えたら、 次の 1 秒を待たずに起きて続きを写す。
///
/// falsify: 行を消した時に consumer を起こさないと、 続きを写すのが次の 1 秒の周期になる (下の 3 回とも遅れる)。 止まった
/// ままの印 (`bridge_parked`) を見ないと、 100 ms ごとに起きる。
#[test]
fn the_consumer_sleeps_while_stalled_and_wakes_when_a_row_is_reclaimed() {
    let path = tmp_path("park");
    let _cleanup = Cleanup(path.clone());
    let eng = create(&path);
    let e = eng.entity_in("notes").unwrap();
    let ring = fill(&eng, e, 0, 4 * 1024) as usize;
    eng.oplog_sync().unwrap();
    // 落ち着くまで (書き出し、 印の書き出し) 待ってから数える
    std::thread::sleep(Duration::from_millis(1500));
    let (w0, read) = (eng.consumer_wakeups(), eng.bridge_wal_records_read());
    std::thread::sleep(Duration::from_millis(2500));
    let woke = eng.consumer_wakeups() - w0;
    assert!(woke <= 6, "止まったままの DB の consumer が、 何も書いていない 2.5 秒に {woke} 回起きた (1 秒ごとのはず)");
    assert_eq!(eng.bridge_wal_records_read(), read, "何も書いていない間に、 止まった bridge が WAL を読み直した");

    // consumer が起きた直後 (次の周期は 1 秒先) に ring を空ける。 起こされれば、 すぐ続きを写す
    let mut quick = false;
    let mut slowest = Duration::ZERO;
    for _ in 0..3 {
        until("止まった bridge が続きを写し切らない", Duration::from_secs(30), || ring_full(&eng));
        std::thread::sleep(Duration::from_millis(300));
        let w = eng.consumer_wakeups();
        until("consumer が起きない", Duration::from_secs(30), || eng.consumer_wakeups() > w);
        std::thread::sleep(Duration::from_millis(50));
        let lsn = eng.current_sync_lsn();
        let t0 = Instant::now();
        assert!(ack_all(&eng) >= ring / 2, "前提: ring の行を回収した");
        until("ring が空いたのに、 bridge が続きを写さない", Duration::from_secs(30), || eng.current_sync_lsn() > lsn);
        slowest = slowest.max(t0.elapsed());
        quick |= t0.elapsed() < Duration::from_millis(500);
    }
    assert!(quick, "行を消しても consumer が起きず、 続きを写すのが次の周期まで遅れた (3 回とも 500 ms 以上、 最長 {slowest:?})");
}
