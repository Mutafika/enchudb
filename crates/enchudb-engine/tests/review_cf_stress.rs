//! review (0.37.4, changefeed): (1) 記録の header が壊れた後の 「空の記録として開く」 道、 (2) 書き手 4 本 + oplog_sync + 小さい WAL
//! (区画の移りと畳みが続く) の下で、 listener に 1 回ずつ・書き手ごとの順で渡るか。 状態で判定。
#![cfg(not(target_arch = "wasm32"))]

use enchudb_engine::change_log::{ChangeLog, NewEntry};
use enchudb_engine::changefeed::ChangeListener;
use enchudb_engine::transport::WireRecord;
use enchudb_engine::{db_files, Engine, ValueType};
use enchudb_oplog::Hlc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const SCRATCH: &str = "/private/tmp/claude-501/-Users-kubo-Desktop-mutafika-enchudb/eedfc333-d87a-4a47-a3b0-daf4b6d7337a/scratchpad";

struct Tmp(String);
impl Tmp {
    fn new(tag: &str) -> Self {
        let _ = std::fs::create_dir_all(SCRATCH);
        let p = format!("{SCRATCH}/review-cfs-{tag}-{}", std::process::id());
        let _ = db_files::remove_db(&p);
        let _ = std::fs::remove_dir_all(&p);
        Tmp(p)
    }
}
impl Drop for Tmp {
    fn drop(&mut self) {
        let _ = db_files::remove_db(&self.0);
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn rec(i: u64, tag: u8) -> NewEntry {
    let mut b = vec![tag; 64];
    b[..8].copy_from_slice(&i.to_le_bytes());
    NewEntry { hlc: Hlc { wall: 1000 + i, logical: tag as u32, peer: 1 }, wal_id: (tag as u64) << 32 | i, bytes: b }
}

/// T2: header の crc が合わない → 「空の記録として開く (番号を 0 から、 古い番号の読み手は全員 Truncated)」。
/// その後に数件足して開き直すと、 前の世代の entry (番号が続き crc も合う) を拾い足さないか。
#[test]
fn t2_damaged_header_then_reopen_resurrects_old_entries() {
    let tmp = Tmp::new("t2");
    std::fs::create_dir_all(&tmp.0).unwrap();
    let dir = std::path::Path::new(&tmp.0);
    {
        let log = ChangeLog::open(dir, 1 << 20, true, false).unwrap();
        let old: Vec<NewEntry> = (0..100).map(|i| rec(i, b'O')).collect();
        log.append(&old, &[], &mut Vec::new()).unwrap();
        assert_eq!(log.range(), (0, 100));
    }
    // header の crc を壊す (電源断で header の page がちぎれた像の代わり)
    {
        use std::io::{Read, Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new().read(true).write(true).open(dir.join("changefeed.seg")).unwrap();
        let mut b = [0u8; 4];
        f.seek(SeekFrom::Start(76)).unwrap();
        f.read_exact(&mut b).unwrap();
        b[0] ^= 0xff;
        f.seek(SeekFrom::Start(76)).unwrap();
        f.write_all(&b).unwrap();
        f.sync_all().unwrap();
    }
    {
        let log = ChangeLog::open(dir, 0, false, false).unwrap();
        assert_eq!(log.range(), (0, 0), "壊れた header は空の記録");
        // 古い読み手 (位置 50) は Truncated
        assert!(log.read(50, 1 << 20).is_err());
        let new: Vec<NewEntry> = (0..3).map(|i| rec(i, b'N')).collect();
        log.append(&new, &[], &mut Vec::new()).unwrap();
        assert_eq!(log.range(), (0, 3));
    }
    let log = ChangeLog::open(dir, 0, false, false).unwrap();
    let range = log.range();
    let got = log.read(0, 1 << 20).unwrap();
    let tags: Vec<(u64, char)> = got.iter().map(|e| (e.no, e.bytes[8] as char)).collect();
    eprintln!("T2: range after reopen = {range:?}; first 6 entries (no, 世代) = {:?}; last_written = {:?}", &tags[..tags.len().min(6)], log.last_written());
    let stale_reader = log.read(50, 1 << 20).map(|es| (es[0].no, es[0].bytes[8] as char));
    eprintln!("T2: 古い読み手 (位置 50) の読み = {stale_reader:?}");
    assert_eq!(range, (0, 3), "開き直したら、 壊れる前の世代の entry が新しい番号の続きとして戻った");
}

struct Rec {
    seen: Mutex<Vec<(u64, u64)>>,
    detached: AtomicUsize,
}
impl ChangeListener for Rec {
    fn on_changes_at(&self, first: u64, recs: &[WireRecord]) {
        let mut s = self.seen.lock().unwrap();
        for (k, r) in recs.iter().enumerate() {
            if let enchudb_oplog::oplog::DecodedOp::Tie { value, .. } = r.op {
                s.push((first + k as u64, value));
            }
        }
    }
    fn on_detached(&self, _: Hlc) {
        self.detached.fetch_add(1, Ordering::SeqCst);
    }
}

const WRITERS: u64 = 4;

/// 1 session: 書き手 4 本が `from..to` を自分の entity に書く (値 = 書き手 << 40 | 連番)。 1 本は時々 oplog_sync。 受かった書き込みを返す。
fn session(eng: &Arc<Engine>, from: u64, to: u64) -> Vec<u64> {
    let ts = eng.himo_id("ts").unwrap() as u16;
    let stop = Arc::new(AtomicBool::new(false));
    let syncer = {
        let (eng, stop) = (eng.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                let _ = eng.oplog_sync();
                std::thread::sleep(Duration::from_millis(3));
            }
        })
    };
    let hs: Vec<_> = (0..WRITERS)
        .map(|w| {
            let eng = eng.clone();
            std::thread::spawn(move || {
                let e = eng.entity().unwrap();
                let mut ok = Vec::new();
                for i in from..to {
                    let v = w << 40 | i;
                    if eng.try_tie_to_by_id(e, ts, v).is_ok() {
                        ok.push(v);
                    }
                    if i % 37 == 0 {
                        eng.oplog_commit();
                    }
                }
                eng.oplog_commit();
                ok
            })
        })
        .collect();
    let mut all = Vec::new();
    for h in hs {
        all.extend(h.join().unwrap());
    }
    stop.store(true, Ordering::Release);
    syncer.join().unwrap();
    all
}

fn check(tag: &str, wrote: &[u64], seen: &[(u64, u64)], dense_numbers: bool) {
    let mut w = wrote.to_vec();
    w.sort_unstable();
    let mut s: Vec<u64> = seen.iter().map(|x| x.1).collect();
    let n_seen = s.len();
    s.sort_unstable();
    let mut dups = 0;
    for p in s.windows(2) {
        if p[0] == p[1] {
            dups += 1;
        }
    }
    s.dedup();
    let missing = w.iter().filter(|v| s.binary_search(v).is_err()).count();
    let extra = s.iter().filter(|v| w.binary_search(v).is_err()).count();
    // 書き手ごとの順
    let mut last = [None::<u64>; WRITERS as usize];
    let mut disorder = 0;
    for (_, v) in seen {
        let wi = (v >> 40) as usize;
        if last[wi].is_some_and(|l| l >= *v) {
            disorder += 1;
        }
        last[wi] = Some(*v);
    }
    let mut num_bad = 0;
    for p in seen.windows(2) {
        if p[1].0 <= p[0].0 {
            num_bad += 1;
        }
    }
    eprintln!(
        "{tag}: wrote={} seen={n_seen} dups={dups} missing={missing} extra={extra} disorder={disorder} number_not_increasing={num_bad}",
        w.len()
    );
    assert_eq!((dups, missing, extra, disorder), (0, 0, 0, 0), "{tag}");
    if dense_numbers {
        assert_eq!(num_bad, 0, "{tag}: 番号が増えていない");
    }
}

fn wait_seen(rec: &Rec, n: usize, eng: &Engine) -> bool {
    let until = Instant::now() + Duration::from_secs(120);
    while Instant::now() < until {
        if rec.seen.lock().unwrap().len() >= n || rec.detached.load(Ordering::SeqCst) > 0 {
            return true;
        }
        let _ = eng.oplog_sync();
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

/// 記録なし: 1 session。 WAL 256 KiB (2 区画)。
#[test]
fn t6a_no_record_exactly_once_in_order() {
    let tmp = Tmp::new("t6a");
    let eng = Engine::create_concurrent_with_oplog(&tmp.0, 256 << 10).unwrap();
    eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
    let rec = Arc::new(Rec { seen: Mutex::new(Vec::new()), detached: AtomicUsize::new(0) });
    eng.add_change_listener(rec.clone());
    let wrote = session(&eng, 0, 30_000);
    assert!(wait_seen(&rec, wrote.len(), &eng), "届き切らない: seen={} wrote={}", rec.seen.lock().unwrap().len(), wrote.len());
    eprintln!("t6a: detached={} folds={} switches={}", rec.detached.load(Ordering::SeqCst), eng.wal_room_folds(), eng.oplog().unwrap().switches());
    assert_eq!(rec.detached.load(Ordering::SeqCst), 0);
    let seen = rec.seen.lock().unwrap().clone();
    check("t6a", &wrote, &seen, true);
}

/// 記録あり: 3 session、 開き直すたびに 「受けた最後の番号 + 1」 から付け直す。 session の間、 listener の居ない間の書き込みもある。
#[test]
fn t6b_record_resume_across_reopens_exactly_once_in_order() {
    let tmp = Tmp::new("t6b");
    let mut wrote = Vec::new();
    let mut seen: Vec<(u64, u64)> = Vec::new();
    let mut next_pos = 0u64;
    for round in 0..3u64 {
        let eng = if round == 0 {
            let eng = Engine::create_concurrent_with_oplog(&tmp.0, 256 << 10).unwrap();
            eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
            eng.enable_change_log(64 << 20).unwrap();
            eng
        } else {
            Engine::open_concurrent_with_oplog(&tmp.0, 256 << 10).unwrap()
        };
        // listener の居ない間の書き込み
        wrote.extend(session(&eng, round * 100_000, round * 100_000 + 5_000));
        let rec = Arc::new(Rec { seen: Mutex::new(Vec::new()), detached: AtomicUsize::new(0) });
        eng.add_change_listener_from(rec.clone(), next_pos).unwrap_or_else(|t| panic!("round {round}: {t:?} (pos {next_pos}, range {:?})", eng.change_log_range()));
        wrote.extend(session(&eng, round * 100_000 + 5_000, round * 100_000 + 25_000));
        if round < 2 {
            // 途中で閉じる: 受けた所まで控える (届き切るのを待たない)
            std::thread::sleep(Duration::from_millis(30));
        } else {
            let want = wrote.len() - seen.len();
            assert!(wait_seen(&rec, want, &eng), "届き切らない");
        }
        assert_eq!(rec.detached.load(Ordering::SeqCst), 0, "round {round}: 外れた");
        eprintln!("t6b round {round}: range={:?} switches={}", eng.change_log_range(), eng.oplog().unwrap().switches());
        drop(eng);
        let s = rec.seen.lock().unwrap().clone();
        // 番号は Tie 以外 (Vocab など) も使うので、 受けた record 全部の最後の番号は Tie だけからは分からない → Tie の最後 + 1 から
        // 付け直す (その後ろの Tie でない record を重ねて受けても Tie は重ならない)
        if let Some(last) = s.last() {
            next_pos = last.0 + 1;
        }
        seen.extend(s);
    }
    check("t6b", &wrote, &seen, true);
}
