//! #506 まとめ書き: 置き場 (`COMBINE_SLOTS` = 64) より多い書き手の thread が同時に積む。 置き場の番号が 64 を越えた thread は頼まずに
//! lock を待って自分で積み、 置き場に頼みを置いた thread と混ざる。 どの書き手も自分の record の (LSN, HLC) を受け取り、 WAL には全部の
//! record が 1 回ずつ、 LSN と HLC の増える順に載る。 置き場の番号は process 全体で配るので、 他の試験の thread と混ざらないように
//! 別の test binary に置く。
use enchudb_oplog::oplog::{DecodedOp, Op, OpLog};
use std::collections::HashMap;
use std::sync::Barrier;

#[test]
fn more_writers_than_slots_get_back_their_own_record() {
    const THREADS: u64 = 72;
    const PER: u64 = 2_000;
    let p = std::env::temp_dir().join(format!("enchudb-wal-combine-many-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    let _ = std::fs::remove_file(&p);
    let wal = OpLog::create(&p, 64 << 20).unwrap();
    wal.set_peer_id(1);
    let b = Barrier::new(THREADS as usize);
    let got: Vec<(u64, enchudb_oplog::Hlc, u64)> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..THREADS)
            .map(|t| {
                let (wal, b) = (&wal, &b);
                s.spawn(move || {
                    b.wait();
                    (0..PER)
                        .map(|i| {
                            let eid = t << 32 | i;
                            let (lsn, hlc) = wal.append_with_hlc(Op::Tie { eid, himo_id: 0, value: i }).unwrap();
                            (lsn, hlc, eid)
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        hs.into_iter().flat_map(|h| h.join().unwrap()).collect()
    });
    assert_eq!(wal.pending_writes(), 0, "append 中の writer の数が 0 に戻る");
    wal.append(Op::Commit).unwrap();
    let recs: Vec<_> = wal.iter_committed().into_iter().filter(|r| !matches!(r.op, DecodedOp::Commit)).collect();
    assert_eq!(recs.len(), (THREADS * PER) as usize, "どの record も 1 回ずつ載る");
    for w in recs.windows(2) {
        assert!(w[0].lsn < w[1].lsn && w[0].hlc < w[1].hlc, "WAL の順と LSN / HLC の順が違う");
    }
    let by_lsn: HashMap<u64, (enchudb_oplog::Hlc, u64)> = recs
        .iter()
        .map(|r| match r.op {
            DecodedOp::Tie { eid, .. } => (r.lsn, (r.hlc, eid)),
            ref other => panic!("unexpected op: {other:?}"),
        })
        .collect();
    for (lsn, hlc, eid) in got {
        assert_eq!(by_lsn.get(&lsn), Some(&(hlc, eid)), "書き手が受け取った LSN {lsn} の record が、 その書き手の値と HLC でない");
    }
    eprintln!("置き場の頼みとして積んだ record: {} / {}", wal.combined_appends(), THREADS * PER);
    drop(wal);
    let _ = std::fs::remove_dir_all(&p);
    let _ = std::fs::remove_file(&p);
}
