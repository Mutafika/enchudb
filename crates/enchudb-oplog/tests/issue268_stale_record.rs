//! #268: 畳んだ (fold) 後の ring には前の周の record が消されずに残る。 append は head を先に進めてから
//! record を書くので、 書いている最中の領域には前の周の record が見える。 並行した scan (sync の bridge) が
//! それを読むと、 前の周の Commit (payload 0 = CRC が必ず合う) で group を閉じたつもりになり、 cursor を
//! 今の周の record の境目でない位置へ進める。 以後その位置から読むと毎回 bad-magic で空になり、 bridge は
//! 開き直すまで 1 件も運ばない (実機: cursor 256 = 32 + Commit 2 個のまま 71 時間)。
//!
//! 毎周: 前の周に Commit を 4 個書いて畳み、 bridge と同じ形の scan を回しながら署名付きの record + Commit を
//! 書く (record の長さは周ごとに変える)。 最後に cursor から読み切って、 運ばれたのがその周の record 1 件だけで、
//! cursor が head に着いたかを見る。 修正前は 3000 周中 2976〜2998 周で cursor が 144 (= 32 + 前の周の Commit 1 個)
//! に止まった (署名しないと payload を書いてから header を書くまでの窓が狭く、 初めて触る page の周だけ当たる)。

use enchudb_oplog::oplog::{DecodedOp, Op, OpLog, Record, HEADER_SIZE};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

struct Cleanup(std::path::PathBuf);
impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn vids(recs: &[(Record, u64)]) -> Vec<u32> {
    recs.iter()
        .map(|(r, _)| match &r.op {
            DecodedOp::Vocab { vid, .. } => *vid,
            other => panic!("書いていない op: {other:?}"),
        })
        .collect()
}

#[test]
fn scan_never_reads_the_previous_cycle() {
    let path = std::env::temp_dir().join(format!("enchudb-issue268-{}.oplog", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let _cleanup = Cleanup(path.clone());
    let wal = Arc::new(OpLog::create(&path, 1 << 20).unwrap());
    // 実機と同じく署名する (payload を書いてから header を書くまでの窓が署名の分だけ開く)
    wal.set_keypair(Some(Arc::new(enchudb_oplog::keys::Keypair::generate())));
    let cursor = Arc::new(AtomicU64::new(HEADER_SIZE as u64));
    let carried: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
    let active = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let lock = Arc::new(Mutex::new(()));
    // bridge: cursor から読み、 読み切った commit 済み group の終端まで cursor を進める
    let scanner = {
        let (wal, cursor, carried, active, stop, lock) =
            (wal.clone(), cursor.clone(), carried.clone(), active.clone(), stop.clone(), lock.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                if !active.load(Ordering::Acquire) {
                    std::hint::spin_loop();
                    continue;
                }
                let _g = lock.lock().unwrap();
                if !active.load(Ordering::Acquire) {
                    continue;
                }
                let from = cursor.load(Ordering::Acquire);
                let (recs, end) = wal.iter_committed_from_with_offsets(from);
                carried.lock().unwrap().extend(vids(&recs));
                cursor.store(end, Ordering::Release);
            }
        })
    };
    let mut stuck = Vec::new();
    for round in 0..3000u32 {
        {
            let _g = lock.lock().unwrap();
            // 前の周: ring の先頭から長さ 0 の Commit を並べて畳む (実機の consumer は空の group でも Commit を打つ)
            wal.advance_checkpoint(wal.head());
            if wal.head() > HEADER_SIZE as u64 {
                assert!(wal.try_reset(), "round {round}: 畳めない");
            }
            for _ in 0..4 {
                wal.append(Op::Commit).unwrap();
            }
            wal.advance_checkpoint(wal.head());
            assert!(wal.try_reset(), "round {round}: 畳めない");
            cursor.store(HEADER_SIZE as u64, Ordering::Release);
            carried.lock().unwrap().clear();
            active.store(true, Ordering::Release);
        }
        let big = vec![round as u8; 4096 + (round as usize % 5) * 200];
        wal.append(Op::Vocab { vid: round, bytes: &big }).unwrap();
        wal.append(Op::Commit).unwrap();
        let _g = lock.lock().unwrap();
        active.store(false, Ordering::Release);
        let from = cursor.load(Ordering::Acquire);
        let (recs, end) = wal.iter_committed_from_with_offsets(from);
        let mut got = std::mem::take(&mut *carried.lock().unwrap());
        got.extend(vids(&recs));
        if got != [round] || end != wal.head() {
            stuck.push((round, from, end, got));
        }
        wal.advance_checkpoint(wal.head());
    }
    stop.store(true, Ordering::Relaxed);
    scanner.join().unwrap();
    assert!(stuck.is_empty(), "{} / 3000 周で bridge が止まった (round, cursor, 読めた終端, 運んだ vid) 先頭 {:?}", stuck.len(), &stuck[..stuck.len().min(5)]);
}
