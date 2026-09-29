//! #349: `clear_local_only_tables` と `repair_interrupted_deletes` (→ `remove_entity_body`) は、 行の cell を
//! 1 列ずつ消すのに row lock (#135) を取っていなかった。 他の書き手は全部 row lock の中なので、
//! `read_row` (#206) で 1 行の列をそろって読む相手に、 消している途中の行 (a は在るが b は無い) が見えた。
//!
//! 行は `write_row` の中で a と b を両方書くので、 そろって読めば (在る, 在る) か (無い, 無い) のどちらか。
//! 片方だけの組が 1 回でも見えたら落とす。
//!
//! 実測 (M 系 mac、 読み手 7 本、 1.5 秒): lock 無しだと片方だけの組が clear で 14,485、 repair で 15,313〜28,275
//! (2 本とも毎回落ちる)。 lock ありは 0。

use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::Hlc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const CAP: usize = 8 * 1024 * 1024;
const PEER: u32 = 7;
const ROWS: usize = 2000;

fn tmp_path(tag: &str) -> String {
    format!(
        "/tmp/enchudb-issue349-{}-{}-{}.db",
        tag,
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    )
}

fn fresh(path: &str) {
    let _ = std::fs::remove_dir_all(path); // v10: DB は directory
    for suf in ["", ".oplog", ".tables", ".crc", ".db.lock", ".eidmap", ".vocabmap", ".schema"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

fn hlc(wall: u64) -> Hlc {
    Hlc { wall, logical: 0, peer: PEER }
}

/// 行 `eid` の a と b を 1 回の書き込みとして書く。
fn write_both(eng: &Engine, eid: u64, ha: u16, hb: u16, at: u64) {
    let _row = eng.write_row(eid);
    assert!(eng.remote_tie_apply(eid, ha, 1, hlc(at)));
    assert!(eng.remote_tie_apply(eid, hb, 2, hlc(at)));
}

/// `eids` の行を `read_row` で読み続け、 片方の列だけ在る組を数える読み手を `n` 本。
fn spawn_readers(
    eng: &Arc<Engine>,
    eids: &Arc<Vec<u64>>,
    stop: &Arc<AtomicBool>,
    torn: &Arc<AtomicUsize>,
    (ha, hb): (u16, u16),
    n: usize,
) -> Vec<std::thread::JoinHandle<()>> {
    (0..n)
        .map(|k| {
            let (eng, eids, stop, torn) = (eng.clone(), eids.clone(), stop.clone(), torn.clone());
            std::thread::spawn(move || {
                let mut i = k * 997;
                while !stop.load(Ordering::Relaxed) {
                    let eid = eids[i % eids.len()];
                    let (a, b) = eng.read_row(eid, || (eng.get_by_id(eid, ha), eng.get_by_id(eid, hb)));
                    if a.is_some() != b.is_some() {
                        torn.fetch_add(1, Ordering::Relaxed);
                    }
                    i += 1;
                }
            })
        })
        .collect()
}

fn readers() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4).clamp(2, 8) - 1
}

#[test]
fn clear_local_only_tables_does_not_show_a_half_cleared_row() {
    let path = tmp_path("clear");
    fresh(&path);
    let mut eng = Engine::create_with_cell_version(&path, 4 * ROWS as u32).unwrap();
    eng.define_reserved_table("_seen", ROWS as u32).unwrap();
    eng.define_himo_in("_seen", "a", ValueType::Number, 0).unwrap();
    eng.define_himo_in("_seen", "b", ValueType::Number, 0).unwrap();
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, CAP).unwrap();
    eng.set_peer_id(PEER);
    let ha = eng.himo_id("_seen.a").expect("himo _seen.a") as u16;
    let hb = eng.himo_id("_seen.b").expect("himo _seen.b") as u16;

    // clear は払い出し位置を 0 に戻すので、 毎回同じ eid が払い出される
    let fill = |at: u64| -> Vec<u64> {
        (0..ROWS)
            .map(|_| {
                let eid = eng.entity_in("_seen").expect("_seen の entity");
                write_both(&eng, eid, ha, hb, at);
                eid
            })
            .collect()
    };
    let eids = Arc::new(fill(100));
    let (stop, torn) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicUsize::new(0)));
    let rs = spawn_readers(&eng, &eids, &stop, &torn, (ha, hb), readers());

    let end = Instant::now() + Duration::from_millis(1500);
    let mut rounds = 0u64;
    while Instant::now() < end {
        assert_eq!(eng.clear_local_only_tables(), ROWS, "全部の行を消していない");
        rounds += 1;
        assert_eq!(fill(100 + rounds), *eids, "払い出しが戻っていない (test の前提)");
    }
    stop.store(true, Ordering::Relaxed);
    for r in rs {
        r.join().unwrap();
    }
    let torn = torn.load(Ordering::Relaxed);
    eprintln!("clear: {rounds} 回 × {ROWS} 行 / 片方だけの組 {torn}");
    assert!(rounds > 0);
    assert_eq!(torn, 0, "消している途中の行 (片方の列だけ在る) が read_row に見えた");
    drop(eng);
    fresh(&path);
}

#[test]
fn repair_interrupted_deletes_does_not_show_a_half_removed_row() {
    let path = tmp_path("repair");
    fresh(&path);
    let mut eng = Engine::create_with_cell_version(&path, 4 * ROWS as u32).unwrap();
    eng.define_himo("a", ValueType::Number, 0);
    eng.define_himo("b", ValueType::Number, 0);
    let eng: Arc<Engine> = Engine::concurrentize_with_oplog(eng, CAP).unwrap();
    eng.set_peer_id(PEER);
    let ha = eng.himo_id("a").expect("himo a") as u16;
    let hb = eng.himo_id("b").expect("himo b") as u16;
    let eids: Arc<Vec<u64>> = Arc::new((0..ROWS).map(|_| eng.entity().unwrap()).collect());

    let (stop, torn) = (Arc::new(AtomicBool::new(false)), Arc::new(AtomicUsize::new(0)));
    let rs = spawn_readers(&eng, &eids, &stop, &torn, (ha, hb), readers());

    // 回ごとに版を上げる: 本体を書き直す (tombstone より新しい版) → tombstone をさらに新しい版で置く
    // (= 途中で切れた delete の形) → repair が本体を消す
    let end = Instant::now() + Duration::from_millis(1500);
    let mut at = 100u64;
    let mut rounds = 0u64;
    while Instant::now() < end {
        at += 10;
        for &eid in eids.iter() {
            write_both(&eng, eid, ha, hb, at);
            assert!(eng.set_tombstone(eid, hlc(at + 5)), "tombstone を記録できていない");
        }
        assert_eq!(eng.repair_interrupted_deletes(), ROWS, "途中で切れた delete を全部直していない");
        rounds += 1;
    }
    stop.store(true, Ordering::Relaxed);
    for r in rs {
        r.join().unwrap();
    }
    let torn = torn.load(Ordering::Relaxed);
    eprintln!("repair: {rounds} 回 × {ROWS} 行 / 片方だけの組 {torn}");
    assert!(rounds > 0);
    assert_eq!(torn, 0, "消している途中の行 (片方の列だけ在る) が read_row に見えた");
    drop(eng);
    fresh(&path);
}
