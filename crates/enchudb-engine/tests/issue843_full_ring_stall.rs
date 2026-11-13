//! #843: `_sync_ops` の ring が満杯のままの DB (ack の来ない相手) で、 bridge が止まった周のたびに ring の全行を読まない。
//!
//! bridge は止まった周 (ring の満杯 / #783 の待ち) に、 まだ写していない record と同じ (書き手, 版数) の行が ring にあるかを
//! 見る (#828 / #830、 同じ鍵の組を割って配らないため)。 #835 からは ring の行そのものをなめていて、 ring が満杯の間は
//! 写す record のあるたびに ring の行の数だけ読んだ (100 万行で 1 回 28〜32 ms、 bridge の lock を持ったまま)。
//!
//! 直し: ring に載った事のある行の版数のいちばん新しい所を覚え、 それより新しい鍵 (自分の時計で後から書いた record はいつも
//! そう) は読まずに 「無い」 と答える。 残った鍵が前と同じで ring が変わっていなければ前の答えを返す。 組の頭を見失わない事は
//! `issue828_settled_after_reopen.rs` と transport の `issue828_*` / `issue830_*`、 engine の単体 `ring_newest_tests` が見る。

use enchudb_engine::transport::WireRecord;
use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId};
use std::sync::Arc;

const ME: PeerId = 1;
const AUTHOR: PeerId = 9;
const PULLER: PeerId = 77;
const WAL: usize = 16 * 1024 * 1024;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!("enchudb-issue843-{tag}-{}-{nanos}", std::process::id())).to_str().unwrap().to_string()
}

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn create(path: &str) -> Arc<Engine> {
    let mut eng = Engine::create_with_capacity(path, 1024).unwrap();
    eng.define_table("notes", 8).unwrap();
    eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
    eng.enable_sync_tables().unwrap();
    let eng = Engine::concurrentize_with_oplog(eng, WAL).unwrap();
    eng.set_peer_id(ME);
    eng
}

fn drain(eng: &Engine) {
    while eng.transfer_oplog_to_sync_ops() > 0 {}
}

/// ring の行 (lsn の順): (書き手, 版数)。 Commit は除く。
fn rows(payloads: Vec<Vec<u8>>) -> Vec<(PeerId, Hlc)> {
    payloads
        .iter()
        .filter_map(|p| enchudb_oplog::oplog::decode_sync_ops_payload(p))
        .filter(|r| !matches!(r.op, DecodedOp::Commit))
        .map(|r| (r.author_peer, r.hlc))
        .collect()
}

/// ring を満杯にして止める (書いている間は bridge も畳みも止める)。 返すのは ring の枠の数。
fn fill(eng: &Engine, e: u64, extra: u32) -> u32 {
    let ring = eng.table_eid_usage("_sync_ops").unwrap().capacity;
    {
        let _no_bridge = eng.transfer_lock_for_fold();
        for i in 0..ring + extra {
            eng.tie_to(e, "notes.note", i);
        }
        eng.oplog_commit();
    }
    drain(eng);
    let u = eng.table_eid_usage("_sync_ops").unwrap();
    assert_eq!(u.live, u.capacity, "前提: ring が満杯");
    assert!(eng.sync_ops_bridge_offset() < eng.stats().oplog_head, "前提: bridge は WAL を読み切っていない (止まっている)");
    ring
}

/// ring が満杯のまま書き続けても (止まった周が続いても)、 ring の行をなめるのは最初の 1 回だけ。 開き直した後も 1 回だけ。
///
/// falsify: 近道 (`RingNewest::is_newer_than_every_row`) を外すと、 止まった周のたびに (この試験では 40 回以上) なめる。
#[test]
fn a_full_ring_is_read_once_not_on_every_stalled_round() {
    let path = tmp_path("once");
    let _cleanup = Cleanup(path.clone());
    {
        let eng = create(&path);
        let e = eng.entity_in("notes").unwrap();
        fill(&eng, e, 50);
        let after_fill = eng.sync_ring_scans();
        assert!(after_fill >= 1, "前提: 止まった周が ring を読んだ");
        for i in 0..40u32 {
            eng.tie_to(e, "notes.note", 1_000_000 + i);
            eng.oplog_commit();
            eng.transfer_oplog_to_sync_ops();
        }
        eng.oplog_sync().unwrap();
        assert_eq!(eng.sync_ring_scans(), after_fill, "ring が満杯のまま書くたびに、 止まった周が ring の全行を読んだ");
        // 割れた組は無いので、 ring の行は全部配ってよい
        assert_eq!(eng.settled_sync_ops().len(), eng.pending_sync_ops(0).len(), "割れた組が無いのに、 配らない行がある");
        assert_eq!(Arc::strong_count(&eng), 1);
    }
    let eng = Engine::open_concurrent_with_oplog(&path, WAL).unwrap();
    eng.set_peer_id(ME);
    let e = eng.pull_raw("notes.note", 1_000_039u32).first().copied().expect("前提: 行が残っている");
    drain(&eng);
    let u = eng.table_eid_usage("_sync_ops").unwrap();
    assert_eq!(u.live, u.capacity, "前提: 開き直しても ring は満杯");
    for i in 0..40u32 {
        eng.tie_to(e, "notes.note", 2_000_000 + i);
        eng.oplog_commit();
        eng.transfer_oplog_to_sync_ops();
    }
    eng.oplog_sync().unwrap();
    // 開き直した後は、 畳んでいない WAL を頭から写し直す (もう ring にある record = ring の版数以前の鍵) ので、 最初の止まった周が
    // 読む。 その後は、 後から書いた record の鍵を除いた問いが同じで ring も変わらないので読まない
    assert_eq!(eng.sync_ring_scans(), 1, "開き直した後、 ring が満杯のまま書くたびに ring の全行を読んだ");
}

/// 近道が効いている (ring を 1 回読んで覚えた後) でも、 後から来た同じ (書き手, 版数) の組が ring の端で割れたら配らない。
/// ack で ring が空いて組が載り切ったら配る (または、 載り切らないなら書き手の floor を組の版数に上げる)。
///
/// falsify: bridge が載せた行を覚えない (`RingNewest::note` を外す) と、 組の鍵が 「ring のどの行よりも新しい」 のままになり、
/// 割れた組の前半を配る。
#[test]
fn a_group_cut_apart_after_the_ring_was_read_is_still_held_back() {
    const GROUP: usize = 100;
    let path = tmp_path("group");
    let _cleanup = Cleanup(path.clone());
    let eng = create(&path);
    let e = eng.entity_in("notes").unwrap();
    let ring = fill(&eng, e, 10) as usize;
    assert!(eng.sync_ring_scans() >= 1, "前提: ring を読んで覚えた");

    // 受け手が ring の行を全部受けて ack → 回収 → 残りを写す
    let top = rows(eng.pending_sync_ops(0)).iter().filter(|(a, _)| *a == ME).map(|(_, h)| *h).max().unwrap();
    eng.ack_sync_up_to_cursors(PULLER, &[(ME, top)]).unwrap();
    assert!(eng.reclaim_sync_ops() + 1 >= ring, "前提: ring の行を回収した");
    drain(&eng);
    let left = eng.table_eid_usage("_sync_ops").unwrap().free as usize;
    assert!(left > GROUP, "前提: ring が空いた (空き {left})");

    // 空きを GROUP / 2 だけ残して埋め、 その後に同じ (書き手, 版数) の record GROUP 本 (中継した state の形、 版数は今より先 =
    // 覚えた 「いちばん新しい行」 より新しい)
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
    let group_key = (AUTHOR, Hlc { wall: now + 60_000, logical: 0, peer: AUTHOR });
    let on = |payloads: Vec<Vec<u8>>| rows(payloads).iter().filter(|k| **k == group_key).count();
    {
        let _no_bridge = eng.transfer_lock_for_fold();
        // 閉じの Commit も 1 行使う
        for i in 0..(left - GROUP / 2) as u32 {
            eng.tie_to(e, "notes.note", 3_000_000 + i);
        }
        for vid in 0..GROUP as u32 {
            let rec = WireRecord::unsigned(group_key.1, AUTHOR, DecodedOp::Vocab { vid, bytes: format!("w{vid}").into_bytes() });
            assert!(eng.relay_record(&rec), "前提: 組の record を WAL に載せた");
        }
        eng.oplog_commit();
    }
    drain(&eng);
    let first_half = on(eng.pending_sync_ops(0));
    assert!(first_half > 0 && first_half < GROUP, "前提: 組が ring の端で割れている (載った {first_half} / {GROUP} 本)");
    assert_eq!(on(eng.settled_sync_ops()), 0, "割れた組の前半を配った");

    // 止まったまま書き続ける (新しい鍵が増える) 間も配らず、 ring も読み直さない
    let scans = eng.sync_ring_scans();
    for i in 0..20u32 {
        eng.tie_to(e, "notes.note", 4_000_000 + i);
        eng.oplog_commit();
        eng.transfer_oplog_to_sync_ops();
        assert_eq!(on(eng.settled_sync_ops()), 0, "割れた組の前半を配った ({i} 回目)");
    }
    assert_eq!(eng.sync_ring_scans(), scans, "組が割れたまま止まっている間、 周のたびに ring を読んだ");

    // 組の前の行を受け手が受けて ack → 回収 → 組の続きが載る
    let top = rows(eng.pending_sync_ops(0)).iter().filter(|(a, _)| *a == ME).map(|(_, h)| *h).max().unwrap();
    eng.ack_sync_up_to_cursors(PULLER, &[(ME, top)]).unwrap();
    eng.reclaim_sync_ops();
    drain(&eng);
    let handed = on(eng.settled_sync_ops());
    let floor = eng.sync_reclaimed_floors().unwrap_or_default().into_iter().find(|(a, _)| *a == AUTHOR).map(|(_, h)| h);
    assert!(
        handed == GROUP || (handed == 0 && floor >= Some(group_key.1)),
        "組は、 全部載ってから配るか、 配らずに書き手の floor を組の版数に上げる (配ってよい行に {handed} 本、 floor {floor:?})"
    );
}
