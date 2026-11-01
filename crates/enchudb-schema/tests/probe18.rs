//! 18 回目の測り (試作、 `#[ignore]`): T7 (`ball_lazy.rs` の `a_build_drops_what_it_already_applied_from_the_side_notes`) と同じ形
//! (WAL 64 MiB、 記録 1 MiB、 球を宣言、 changefeed の listener、 書き手 4 本が全力で書いて 1 件ごとに commit) で、 球を組まずに測る:
//! (1) 書き手の速さ、 (2) 書き手が区画を移ってから consumer が古い区画を畳むまで、 (3) 区画の頭から WAL を読み終える時間と、 その間に
//! 読み始めた区画か読んだ区画が畳まれたか (組む側の追いつきが読み直す形と同じ見方)。 merge の前と後の tree に同じ file を置いて交互に回す。
//! 結果は 1 行の JSON (`PROBE18 {..}`)。 回す時間は `PROBE_SECS` (既定 8 秒)。

use enchudb_oplog::oplog::{wal_pos, wal_pos_lap, DecodedOp, HEADER_SIZE};
use enchudb_schema::Database;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

const TH: usize = 4;
const PER: usize = 250;
const VALS: [u64; 6] = [3, 5, 8, 13, 21, 2_000_000];
const WAL: usize = 64 << 20;

fn median(v: &mut [f64]) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

#[test]
#[ignore]
fn probe18() {
    let secs: u64 = std::env::var("PROBE_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(8);
    let path = format!("/tmp/enchudb-probe18-{}", std::process::id());
    let _ = enchudb_engine::db_files::remove_db(&path);
    // 作る (ball_lazy の create と同じ表・記録 1 MiB・球の宣言・宣言の後の 1000 件)
    let eids: Vec<u64> = {
        let mut db = Database::create(&path).unwrap();
        db.table("t").number("id").number("a").primary_key("id").build().unwrap();
        let eids = {
            let t = db.get_table("t").unwrap();
            (0..TH * PER).map(|i| t.insert().set("id", i as i64).set("a", VALS[i % VALS.len()] as i64).commit().unwrap()).collect::<Vec<_>>()
        };
        let db = db.finish_with_oplog(WAL).unwrap();
        let eng = db.engine();
        eng.enable_change_log(1 << 20).unwrap();
        assert!(eng.declare_ball("t.a").unwrap());
        let ha = eng.himo_id("t.a").unwrap() as u16;
        for i in 0..1000usize {
            eng.tie_to_by_id(eids[(i * 7919) % eids.len()], ha, VALS[i % VALS.len()] as u32);
            eng.commit();
        }
        eids
    };
    // 開き直す (T7 の session と同じ)。 球は読まない = 組まない
    let db = Database::open_with_oplog(&path, WAL).unwrap();
    let eng = db.engine();
    struct Quiet;
    impl enchudb_engine::changefeed::ChangeListener for Quiet {}
    eng.add_change_listener(std::sync::Arc::new(Quiet));
    let ha = eng.himo_id("t.a").unwrap() as u16;
    let wal = eng.oplog().unwrap();
    let stop = AtomicBool::new(false);
    let writes = AtomicU64::new(0);
    let (mut lags, mut scans_old, mut scans_new) = (Vec::new(), Vec::new(), Vec::new());
    let mut old_missing = 0u64;
    let t_all = Instant::now();
    std::thread::scope(|s| {
        for th in 0..TH {
            let (stop, writes, eids) = (&stop, &writes, &eids);
            s.spawn(move || {
                let mut r = 0x9E37_79B9_7F4A_7C15u64.wrapping_mul(th as u64 + 1);
                while !stop.load(Ordering::Relaxed) {
                    r ^= r << 13;
                    r ^= r >> 7;
                    r ^= r << 17;
                    let e = eids[th * PER + (r % PER as u64) as usize];
                    let x = ((r >> 20) % (VALS.len() as u64 + 1)) as usize;
                    if x < VALS.len() {
                        eng.tie_to_by_id(e, ha, VALS[x] as u32);
                    } else {
                        eng.untie_by_id(e, ha);
                    }
                    eng.commit();
                    writes.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        // 区画を移った時刻と畳んだ時刻 (100 µs ごとに数を見る)。 畳んだ時の遅れ = その前の最後の移りからの時間
        let watch = s.spawn(|| {
            let (mut sw, mut fo) = (wal.switches(), wal.folds());
            let mut last_switch: Option<Instant> = None;
            let mut lags = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                let (sw2, fo2) = (wal.switches(), wal.folds());
                let now = Instant::now();
                if sw2 != sw {
                    last_switch = Some(now);
                    sw = sw2;
                }
                if fo2 != fo {
                    if let Some(t) = last_switch {
                        lags.push((now - t).as_secs_f64() * 1e3);
                    }
                    fo = fo2;
                }
                std::thread::sleep(Duration::from_micros(100));
            }
            lags
        });
        // 区画の頭から読む (組む側の scan と同じ closure の手間: 球の紐の書き込みだけを溜める)。 古い区画 (今の周 - 1) と今の区画を交互に
        let deadline = Instant::now() + Duration::from_secs(secs);
        let mut k = 0u64;
        while Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(30));
            k += 1;
            let lap = wal_pos_lap(wal.head());
            let old = k % 2 == 0;
            let start = if old { wal_pos(lap.wrapping_sub(1), HEADER_SIZE as u64) } else { wal_pos(lap, HEADER_SIZE as u64) };
            if !wal.holds(start) {
                old_missing += u64::from(old);
                continue;
            }
            let t = Instant::now();
            let mut ops = Vec::new();
            let (_, seen, laps) = wal.scan_ops(start, |hlc, _, op| {
                if matches!(op, DecodedOp::Tie { himo_id, .. } | DecodedOp::Untie { himo_id, .. } if himo_id == ha) {
                    ops.push(hlc);
                }
            });
            let ms = t.elapsed().as_secs_f64() * 1e3;
            let held = |l: u32| wal.holds(wal_pos(l, HEADER_SIZE as u64));
            let gone = !wal.holds(start) || !laps.iter().flatten().all(|&(l, _)| held(l));
            if old { scans_old.push((ms, seen, gone)) } else { scans_new.push((ms, seen, gone)) }
        }
        stop.store(true, Ordering::Relaxed);
        lags = watch.join().unwrap();
    });
    let el = t_all.elapsed().as_secs_f64();
    let sum = |v: &[(f64, u64, bool)]| {
        let mut ms: Vec<f64> = v.iter().map(|x| x.0).collect();
        let mut seen: Vec<f64> = v.iter().map(|x| x.1 as f64).collect();
        let gone = v.iter().filter(|x| x.2).count();
        format!("{{\"n\":{},\"gone\":{},\"ms_med\":{:.1},\"seen_med\":{:.0}}}", v.len(), gone, median(&mut ms), median(&mut seen))
    };
    let mut l2 = lags.clone();
    let lag_max = lags.iter().cloned().fold(0.0, f64::max);
    println!(
        "PROBE18 {{\"writes_per_s\":{:.0},\"switches\":{},\"folds\":{},\"half_records\":{:.0},\"lag_n\":{},\"lag_med_ms\":{:.1},\"lag_max_ms\":{:.1},\"old\":{},\"old_missing\":{},\"new\":{}}}",
        writes.load(Ordering::Relaxed) as f64 / el,
        wal.switches(),
        wal.folds(),
        writes.load(Ordering::Relaxed) as f64 / wal.switches().max(1) as f64,
        lags.len(),
        median(&mut l2),
        lag_max,
        sum(&scans_old),
        old_missing,
        sum(&scans_new)
    );
    drop(db);
    let _ = enchudb_engine::db_files::remove_db(&path);
}
