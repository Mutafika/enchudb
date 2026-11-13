//! #828 の続き (#832 の後の master の CI で落ちた窓): 同じ (書き手, 版数) の組が ring の端で割れたまま閉じて開き直した後、
//! bridge が WAL を頭から写し直す間に、 ring に残っている組の前半を 「配ってよい」 にしない。
//!
//! bridge の位置は memory なので、 畳んでいない WAL は開き直すと頭から写し直される (もう ring にある record の行がもう 1 つ
//! できる)。 #832 は 「直近に写した鍵を 64 個まで覚え、 止まった所から後ろの record と同じ鍵の行があれば組の途中」 と見ていた。
//! 組の続きより前に、 もう配った record が 65 個以上 WAL にあると、 写し直しの間に組の鍵が押し出され、 次に満杯で止まった所で
//! 「途中の組は無い」 になって、 ring に残っている組の前半が配られた (受け手の cursor が組の版数に進み、 後から載る組は
//! `hlc > cursor` を通らない)。
//!
//! 窓を必ず踏む: WAL を畳ませずに (bridge が追いつかない = 畳めない) 閉じ、 開き直して、 前の record だけ ack して回収する。
//! 空いた分だけ写し直しが進み、 組の続きの手前でまた満杯になる。

use enchudb_engine::transport::WireRecord;
use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::oplog::DecodedOp;
use enchudb_oplog::{Hlc, PeerId};
use std::sync::Arc;

const ME: PeerId = 1;
/// 組の record の書き手 (中継した state の書き手) と、 ack する受け手。
const AUTHOR: PeerId = 9;
const PULLER: PeerId = 77;
const GROUP: usize = 100;

fn tmp_path(tag: &str) -> String {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!("enchudb-issue828-eng-{tag}-{}-{nanos}", std::process::id())).to_str().unwrap().to_string()
}

struct Cleanup(String);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
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

fn drain(eng: &Engine) {
    while eng.transfer_oplog_to_sync_ops() > 0 {}
}

#[test]
fn a_group_cut_apart_is_not_handed_out_while_the_bridge_recopies_the_wal_after_reopen() {
    let path = tmp_path("recopy");
    let _cleanup = Cleanup(path.clone());
    let group_key = (AUTHOR, Hlc { wall: 5, logical: 0, peer: AUTHOR });
    let on = |payloads: Vec<Vec<u8>>| rows(payloads).iter().filter(|k| **k == group_key).count();
    let (first_half, fillers) = {
        let mut eng = Engine::create_with_capacity(&path, 1024).unwrap();
        eng.define_table("notes", 8).unwrap();
        eng.define_himo_in("notes", "note", ValueType::Number, 0).unwrap();
        eng.enable_sync_tables().unwrap();
        let eng = Engine::concurrentize_with_oplog(eng, 16 * 1024 * 1024).unwrap();
        eng.set_peer_id(ME);
        let ring = eng.table_eid_usage("_sync_ops").unwrap().capacity as usize;
        let fillers = ring - GROUP / 2;
        assert!(fillers > 2 * 64, "前提: 組の前に、 覚える鍵の数 (64) よりずっと多い record がある (ring {ring} 行)");
        {
            // 書いている間は bridge も畳みも止める (consumer が途中まで写して WAL を畳むと、 開き直した後の写し直しが無い)
            let _no_bridge = eng.transfer_lock_for_fold();
            let e = eng.entity_in("notes").unwrap();
            for i in 0..fillers as u32 {
                eng.tie_to(e, "notes.note", i);
            }
            // 同じ (書き手, 版数) の record GROUP 本 (中継した state の形)
            for vid in 0..GROUP as u32 {
                let rec = WireRecord::unsigned(group_key.1, AUTHOR, DecodedOp::Vocab { vid, bytes: format!("w{vid}").into_bytes() });
                assert!(eng.relay_record(&rec), "前提: 組の record を WAL に載せた");
            }
            eng.oplog_commit();
        }
        drain(&eng);
        let all = rows(eng.pending_sync_ops(0));
        let first_half = all.iter().filter(|k| **k == group_key).count();
        assert!(first_half > 0 && first_half < GROUP, "前提: 組が ring の端で割れている (載った {first_half} / {GROUP} 本)");
        assert_eq!(all.iter().filter(|(a, _)| *a == ME).count(), fillers, "前提: 組の前の record は全部 ring にある");
        assert_eq!(on(eng.settled_sync_ops()), 0, "閉じる前: 割れた組は配らない");
        assert_eq!(Arc::strong_count(&eng), 1);
        (first_half, fillers)
    };

    let eng = Engine::open_concurrent_with_oplog(&path, 16 * 1024 * 1024).unwrap();
    eng.set_peer_id(ME);
    drain(&eng);
    assert_eq!(on(eng.pending_sync_ops(0)), first_half, "前提: 開き直した直後も、 組は ring の上で割れたまま");
    assert_eq!(on(eng.settled_sync_ops()), 0, "開き直した直後: 割れた組は配らない");

    // 受け手が、 組の前の record を全部受けて ack する → 回収 → 空いた分だけ bridge が WAL を頭から写し直す
    let before: Vec<(PeerId, Hlc)> = rows(eng.pending_sync_ops(0)).into_iter().filter(|(a, _)| *a == ME).collect();
    let top = before.iter().map(|(_, h)| *h).max().unwrap();
    let acked = eng.ack_sync_up_to_cursors(PULLER, &[(ME, top)]);
    let reclaimed = eng.reclaim_sync_ops();
    // (回収は ack の位置の行を 1 つ残す)
    assert!(reclaimed + 1 >= fillers, "前提: 組の前の record の行を回収した (ack {acked:?}、 回収 {reclaimed} 行、 組の前 {fillers} 行)");
    drain(&eng);
    let all = rows(eng.pending_sync_ops(0));
    let recopied = all.iter().filter(|(a, _)| *a == ME).count();
    assert!(recopied > 64, "前提: bridge は、 もう配った record を 65 個以上写し直した ({recopied} 行)");
    let now = all.iter().filter(|k| **k == group_key).count();
    assert!(now >= first_half && now < first_half + GROUP, "前提: 組の続きはまだ載り切っていない (ring に {now} 本、 前半 {first_half} 本)");
    assert_eq!(on(eng.settled_sync_ops()), 0, "写し直しの途中: ring に残っている組の前半 ({first_half} 本) を配った");

    // 写し直した分も ack する。 組の前半の行 (ack していない) が回収の列の頭を塞ぐので、 写し直した行は回収されず、 ring は
    // 空かない = 組は載り切らない。 その時は書き手の floor が組の版数に上がっていて、 受け手は state で取り直す
    eng.ack_sync_up_to_cursors(PULLER, &[(ME, top)]).unwrap();
    eng.reclaim_sync_ops();
    drain(&eng);
    let handed = on(eng.settled_sync_ops());
    let floor = eng.sync_reclaimed_floors().unwrap_or_default().into_iter().find(|(a, _)| *a == AUTHOR).map(|(_, h)| h);
    assert!(
        handed >= GROUP || (handed == 0 && floor >= Some(group_key.1)),
        "組は、 全部載ってから配るか、 配らずに書き手の floor を組の版数に上げる (配ってよい行に {handed} 本、 floor {floor:?})"
    );
}
