//! #57: WAL が満杯で載らなかった write も、 相手に届くこと。
//!
//! 載らなかった record は `_sync_ops` にも入らないので、 差分 pull では二度と届かない (ローカルには
//! 書けているので、 相手とだけ永久にずれる)。 engine の bridge は、 載らなかった record の author の
//! history floor を 「今」 に上げる。 それより前の cursor の puller は差分 pull で `history_truncated`
//! になり、 bootstrap (#140) で live state (載らなかった write を含む) を受け取る。
//!
//! WAL は 64 KiB にして、 consumer の 1 周 (100 ms) より速く書いて溢れさせる (注入なし)。

use enchudb_engine::engine::Engine;
use enchudb_engine::transport::{InMemoryTransport, Transport};
use enchudb_engine::ValueType;
use enchudb_oplog::{Hlc, PeerId};
use enchudb_sync::Syncer;
use std::sync::Arc;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue57-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

fn cleanup(path: &str) {
    let _ = std::fs::remove_dir_all(path);
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
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

/// `async_` なら consumer が WAL にまとめて載せる経路 (`tie_async`)、 でなければ書く thread が載せる経路。
fn write_notes(eng: &Arc<Engine>, from: u32, n: u32, async_: bool) -> Vec<u64> {
    (from..from + n)
        .map(|i| {
            let e = eng.entity_in("notes").unwrap();
            if async_ {
                eng.tie_async(e, "notes.note", i);
            } else {
                eng.tie_to(e, "notes.note", i);
                eng.tie_text_to(e, "notes.body", &format!("body {i}"));
            }
            e
        })
        .collect()
}

fn settle(eng: &Arc<Engine>) {
    eng.oplog_commit();
    eng.flush_writes();
    // WAL が満杯なら Commit も載らず Err (#268、 group は空いた後の Commit で閉じる)。 ここでは見ない
    let _ = eng.oplog_sync();
    eng.transfer_oplog_to_sync_ops();
}

/// bridge が WAL に載らなかった record のために floor を上げるまで待つ (上げた回数が `before` を越えるまで)。
/// 落ちるのが止まった後は次の bridge で上がるので、 2 秒 (満杯の間に待つ上限 5 秒より短い) で見切る。
fn wait_floor_bump(eng: &Arc<Engine>, before: u64) {
    let end = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while eng.wal_drop_floor_bumps() <= before {
        assert!(std::time::Instant::now() < end, "落ちるのが止まって 2 秒で floor が上がらない");
        eng.transfer_oplog_to_sync_ops();
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

fn has_note(eng: &Arc<Engine>, i: u32, async_: bool) -> bool {
    let Some(e) = eng.pull_raw("notes.note", i).first().copied() else { return false };
    async_ || eng.get_text_owned(e, "notes.body").is_some_and(|b| b == format!("body {i}").as_bytes())
}

#[test]
fn writes_dropped_from_a_full_wal_reach_the_peer_through_bootstrap() {
    dropped_writes_reach_the_peer(false, false);
}

#[test]
fn async_writes_dropped_from_a_full_wal_reach_the_peer_through_bootstrap() {
    dropped_writes_reach_the_peer(true, false);
}

/// `publish_since_for_peer` を直に呼ぶ publisher (`publish_since` の先頭の広告を通らない) にも
/// floor が届く: 集めた record と一緒に広告する。 旧: この経路は広告せず、 落ちた後の record を
/// 古い floor のまま配って、 puller の cursor が floor を越えた。
#[test]
fn a_per_peer_publish_carries_the_raised_floor() {
    dropped_writes_reach_the_peer(false, true);
}

fn dropped_writes_reach_the_peer(async_: bool, per_peer: bool) {
    let tag = format!("{}{}", if async_ { "async" } else { "sync" }, if per_peer { "-pp" } else { "" });
    let (pa, pb) = (tmp_path(&format!("a-{tag}")), tmp_path(&format!("b-{tag}")));
    let mem = Arc::new(InMemoryTransport::new());
    mem.register_peer(1);
    mem.register_peer(2);
    let transport: Arc<dyn Transport> = mem.clone();
    let eng_a = make_engine(&pa, 1, 64 * 1024);
    let eng_b = make_engine(&pb, 2, 16 * 1024 * 1024);
    let sync_a = Syncer::new(eng_a.clone(), transport.clone());
    let sync_b = Syncer::new(eng_b.clone(), transport.clone());
    sync_a.serve_state();

    // B は差分 pull で追従している
    write_notes(&eng_a, 0, 10, async_);
    settle(&eng_a);
    sync_a.publish_since(Hlc::ZERO);
    let out = sync_b.pull_once(1);
    assert!(out.applied > 0 && !out.history_truncated, "{out:?}");

    // 64 KiB の WAL を 1 周より速く溢れさせる
    let burst = write_notes(&eng_a, 10, 5_000, async_);
    // 載らなかった write にも版数が付いている (ZERO = 版数不明だと、 後から届く古い write に負ける)
    let hid = eng_a.himo_id("notes.note").unwrap() as u16;
    settle(&eng_a);
    assert!(burst.iter().all(|e| eng_a.cell_hlc(*e, hid) != Hlc::ZERO), "版数の無い cell がある");
    // floor は満杯の episode が終わった周に上がる (WAL が畳まれて空いた後の bridge)
    wait_floor_bump(&eng_a, 0);
    // 落ちた write の版数は floor 以下 (floor を越えた cursor の puller が取りこぼさない)
    let floor = eng_a.sync_reclaimed_floors().unwrap().into_iter().find(|(a, _)| *a == 1).unwrap().1;
    let max_cell = burst.iter().map(|e| eng_a.cell_hlc(*e, hid)).max().unwrap();
    assert!(max_cell <= floor, "落ちた write の版数 {max_cell:?} が floor {floor:?} を越える");
    if per_peer {
        sync_a.publish_since_for_peer(2, Hlc::ZERO);
    } else {
        sync_a.publish_since(Hlc::ZERO);
    }

    let out = sync_b.pull_once(1);
    assert!(out.history_truncated, "落ちた write の穴を知らされない: {out:?}");
    let boot = sync_b.bootstrap_pull(1).expect("serve_state 済み");
    assert_eq!(boot.outcome.dropped_vocab, 0, "{boot:?}");
    let missing: Vec<u32> = (0..5_010).filter(|i| !has_note(&eng_b, *i, async_)).collect();
    assert!(missing.is_empty(), "B に届いていない note {} 件 (先頭 {:?})", missing.len(), &missing[..missing.len().min(5)]);

    // bootstrap の後は差分 pull に戻る。 WAL が畳まれて空くまで待つ (空く前に書くと、 また落ちて
    // floor が上がり、 もう一度 bootstrap に回るのが正しい動き)
    let wal = eng_a.oplog().unwrap().clone();
    let end = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while wal.head() > 16 * 1024 {
        assert!(std::time::Instant::now() < end, "WAL が 10 秒で畳まれない (head {})", wal.head());
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let bumps = eng_a.wal_drop_floor_bumps();
    write_notes(&eng_a, 5_010, 3, async_);
    settle(&eng_a);
    assert_eq!(eng_a.wal_drop_floor_bumps(), bumps, "前提: 空いた WAL には載ること");
    sync_a.publish_since(Hlc::ZERO);
    let out = sync_b.pull_once(1);
    assert!(!out.history_truncated, "{out:?}");
    assert!((5_010..5_013).all(|i| has_note(&eng_b, i, async_)));

    drop((sync_a, sync_b, eng_a, eng_b));
    cleanup(&pa);
    cleanup(&pb);
}

/// 中継した他人の record が落ちたら、 その author の floor は **落ちた record の HLC** (中継役の今ではない)。
/// 他人の author の bootstrap は as_of を 「その author の HLC の max」 で決めるので、 中継役の今を入れると
/// bootstrap しても cursor が floor に届かず、 pull のたびに truncated になり続ける (#345 レビュー)。
#[test]
fn a_dropped_relayed_record_raises_its_authors_floor_to_its_own_hlc() {
    let pa = tmp_path("relay");
    let eng = make_engine(&pa, 1, 64 * 1024);
    let src_p = format!("{pa}-src.oplog");
    let src = enchudb_oplog::oplog::OpLog::create(std::path::Path::new(&src_p), 1024 * 1024).unwrap();
    src.set_peer_id(7);
    src.append(enchudb_oplog::oplog::Op::Tie { eid: 1, himo_id: 0, value: 1 }).unwrap();
    src.append(enchudb_oplog::oplog::Op::Commit).unwrap();
    let rec = src.iter_committed().into_iter().next().unwrap();
    // 中継役の clock を author 7 より先に進めておく
    for _ in 0..1000 {
        eng.oplog().unwrap().mint_hlc();
    }

    // WAL を溢れさせてから中継する
    let wal = eng.oplog().unwrap().clone();
    while wal.append(enchudb_oplog::oplog::Op::Tie { eid: 1, himo_id: 0, value: 1 }).is_ok() {}
    assert!(wal.append_relayed_verbatim(&rec.signed_bytes, &rec.signature, &rec.pubkey_fp).is_err(), "前提: 満杯");
    wait_floor_bump(&eng, 0);
    let floors = eng.sync_reclaimed_floors().unwrap();
    let f7 = floors.iter().find(|(a, _)| *a == 7).map(|(_, h)| *h);
    assert_eq!(f7, Some(rec.hlc), "author 7 の floor は落ちた record の HLC: {floors:?}");
    drop((wal, eng));
    let _ = std::fs::remove_file(&src_p);
    cleanup(&pa);
}

/// 書き続けて WAL が満杯のままの間は floor を上げない (上げるたびに全 peer が全状態を bootstrap する)。
/// 書くのを止めて落ちなくなった周に上げ、 その後は上げない。
///
/// 負荷が高いと書き手が止まった周が挟まり、 満杯の間にも上がりうる (それは episode の終わりとして正しい)。
/// なので 「上げない」 は 「毎周は上げない」 で見る: consumer の 15 周のうち 5 回未満 (毎周上げると 15 回前後)。
#[test]
fn the_floor_is_raised_once_when_the_full_episode_ends() {
    let pa = tmp_path("episode");
    let eng = make_engine(&pa, 1, 64 * 1024);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let w = {
        let (eng, stop) = (eng.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut i = 0u32;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let e = eng.entity_in("notes").unwrap();
                eng.tie_to(e, "notes.note", i % 1000);
                eng.untie(e, "notes.note");
                eng.delete(e);
                i += 1;
            }
        })
    };
    // 1.5 秒 (consumer の 15 周) 書き続ける。 bridge は consumer の周だけ
    std::thread::sleep(std::time::Duration::from_millis(1500));
    let during = eng.wal_drop_floor_bumps();
    assert!(during < 5, "満杯の間に floor を {during} 回上げた");
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    w.join().unwrap();
    wait_floor_bump(&eng, during);
    let after = eng.wal_drop_floor_bumps();
    std::thread::sleep(std::time::Duration::from_millis(300));
    eng.transfer_oplog_to_sync_ops();
    assert_eq!(eng.wal_drop_floor_bumps(), after, "落ちなくなった後にも上げた");
    drop(eng);
    cleanup(&pa);
}
