//! changefeed の記録 (`changefeed.seg`) を電源断の像で確かめる (`--features crashsim`)。
//!
//! 書き手が値 0, 1, 2, … を順に書き、 WAL を何回も畳みながら `oplog_sync` を挟む。 書いている最中の任意の瞬間に、 電源が落ちた
//! 姿の像 (`Lost` = 書き出しの済んだ page だけ / `Mixed` = page ごとに今の中身か書き出した中身) を撮る。 像を開いて記録を最古から
//! 受け直すと:
//!
//! 1. 番号は 1 つずつ続く
//! 2. 値は飛ばず・重ならずに続く (記録した record を開き直した後にもう一度記録しない、 間の record を落とさない)
//! 3. 本体に無い値は無い (記録の最後 ≤ 開いた後の列の値)
//! 4. `oplog_sync` が返った値までは入っている
//!
//! 書き出していない (WAL の fsync の前の) record に番号を振ると、 落ちた像では記録にだけ残る (3 で落ちる。 開き直した後に WAL の
//! 中で続きの位置が見つからず WAL の頭から写し直すと、 値が重なって 2 でも落ちる)。
//!
//! 像は確率で当たる。 1 回の run (60 枚) で落ちる割合の実測 (2026-10-22、 変異を入れて 10 回):
//! - 畳んだ印 (頭に戻した header) を書き出さない: 畳み続ける形 10/10、 まれな形 2/10
//! - 書き出す前の record も記録に写す: 畳み続ける形 3/10、 まれな形 0/10 (決まって見るのは `changefeed_log.rs` の
//!   `records_not_yet_written_out_get_no_number`)
//! - 開き直した時に続きの位置を探さない: 5 回中 5 回
//! - 畳んだ後の WAL に残る前の周の record を再生する (#608、 oplog の周の印を見ない): 5 回中 4 回 (畳み続ける形 2/5、 まれな形 3/5)

#![cfg(all(feature = "crashsim", unix))]

use enchudb_engine::changefeed::ChangeListener;
use enchudb_engine::transport::WireRecord;
use enchudb_engine::{Engine, ValueType};
use enchudb_oplog::crashsim::{self, Mode};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// 開く時の WAL の大きさ (作った時の大きさで開くので、 開く側の値は使われない)
const WAL_CAP: usize = 256 << 10;
const LOG_CAP: usize = 4 << 20;
const IMAGES: usize = 60;

#[derive(Default)]
struct Recorder {
    /// (番号, Tie の値)
    seen: Mutex<Vec<(u64, Option<u64>)>>,
}

impl ChangeListener for Recorder {
    fn on_changes_at(&self, first: u64, recs: &[WireRecord]) {
        let mut s = self.seen.lock().unwrap();
        for (k, r) in recs.iter().enumerate() {
            let v = match r.op {
                enchudb_oplog::oplog::DecodedOp::Tie { value, .. } => Some(value),
                _ => None,
            };
            s.push((first + k as u64, v));
        }
    }
}

fn scratch(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("enchu_cflog_power_loss_{}_{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// (番号の始まり, 受けた (番号, Tie の値), 開いた後の列の値)
type Replayed = (u64, Vec<(u64, Option<u64>)>, Option<u64>);

/// 像を開いて記録を最古から受け直す。
fn replay(db: &str) -> Result<Replayed, String> {
    let eng = Engine::open_concurrent_with_oplog(db, WAL_CAP).map_err(|e| format!("開けない: {e}"))?;
    let value = eng.himo_id("ts").and_then(|ts| eng.get_by_id(0, ts as u16));
    let (first, _) = eng.change_log_range().ok_or("記録が無い")?;
    let rec = Arc::new(Recorder::default());
    eng.add_change_listener_from(rec.clone(), first).map_err(|e| format!("受け直せない: {e}"))?;
    eng.oplog_sync().map_err(|e| e.to_string())?;
    let next = eng.change_log_range().unwrap().1;
    let until = Instant::now() + Duration::from_secs(30);
    while (rec.seen.lock().unwrap().len() as u64) < next - first && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(5));
    }
    drop(eng);
    let seen = rec.seen.lock().unwrap().clone();
    Ok((first, seen, value))
}

/// WAL が小さく、 1 ms ほどで畳み続ける形 (畳む時の写し・畳んだ印・続きの位置)。
#[test]
fn the_change_log_survives_power_loss_while_folding() {
    run("fold", 256 << 10);
}

/// WAL が大きく、 畳むのがまれな形 (書いている最中の写しが、 書き出した所までしか写さない)。
#[test]
fn the_change_log_survives_power_loss_between_folds() {
    run("nofold", 64 << 20);
}

/// crashsim の控えは process に 1 つなので、 試験を 1 本ずつ回す。
static SERIAL: Mutex<()> = Mutex::new(());

fn run(tag: &str, wal_cap: usize) {
    let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let root = scratch(tag);
    let live = root.join("live");
    std::fs::create_dir_all(&live).unwrap();
    let db = live.join("db");
    crashsim::start();
    let eng: Arc<Engine> = Engine::create_concurrent_with_oplog(db.to_str().unwrap(), wal_cap).unwrap();
    eng.ensure_himo_dynamic("ts", ValueType::Number64, 0).unwrap();
    eng.enable_change_log(LOG_CAP).unwrap();
    let acked = Arc::new(AtomicU64::new(0)); // oplog_sync が返った値の数
    let done = Arc::new(AtomicBool::new(false));
    let writer = {
        let (eng, acked, done) = (eng.clone(), acked.clone(), done.clone());
        std::thread::spawn(move || {
            let ts = eng.himo_id("ts").unwrap() as u16;
            let a = eng.entity().unwrap();
            let mut v = 0u64;
            while !done.load(Ordering::Acquire) && v < 4_000_000 {
                eng.tie_to_by_id(a, ts, v);
                v += 1;
                if v.is_multiple_of(50) {
                    eng.oplog_commit();
                }
                if v.is_multiple_of(10_000) {
                    eng.oplog_sync().unwrap();
                    acked.store(v, Ordering::Release);
                }
            }
            eng.oplog_sync().unwrap();
        })
    };
    // 書いている最中にも記録へ写す (listener の通知係と同じ道)。 書き出す前の record を写すと、 落ちた像では記録にだけ残る
    let poller = {
        let (eng, done) = (eng.clone(), done.clone());
        std::thread::spawn(move || {
            while !done.load(Ordering::Acquire) {
                let _ = eng.change_log_range();
                std::thread::sleep(Duration::from_micros(500));
            }
        })
    };
    let mut images = Vec::new();
    for k in 0..IMAGES {
        std::thread::sleep(Duration::from_millis(4));
        let at = acked.load(Ordering::Acquire);
        let img = root.join(format!("img{k:03}"));
        let mode = if k % 2 == 0 { Mode::Lost } else { Mode::Mixed { seed: k as u64 * 7919 + 1 } };
        crashsim::capture(&live, &img, mode).unwrap();
        images.push((img, mode, at));
    }
    done.store(true, Ordering::Release);
    writer.join().unwrap();
    poller.join().unwrap();
    let folds = eng.wal_room_folds();
    crashsim::stop();
    drop(eng);

    let mut failures = Vec::new();
    // (受けた数, 最古の番号, 最後の値, 書き出しの返った数) — 前提の確かめ用
    let mut stats: Vec<(usize, u64, Option<u64>, u64)> = Vec::new();
    let keep = std::env::var("ENCHU_POWER_LOSS_KEEP").is_ok();
    for (img, mode, at) in &images {
        if keep {
            // 検証は像を開いて再生するので、 撮ったままの像を隣に写しておく
            let _ = std::process::Command::new("cp").arg("-R").arg(img).arg(img.with_extension("raw")).status();
        }
        let path = img.join("db");
        let (first, seen, value) = match replay(path.to_str().unwrap()) {
            Ok(r) => r,
            Err(e) => {
                failures.push(format!("{} ({mode:?}): {e}", img.display()));
                continue;
            }
        };
        if let Some(k) = seen.iter().enumerate().position(|(k, (no, _))| *no != first + k as u64) {
            failures.push(format!("{} ({mode:?}): {k} 件目で番号が飛んだ / 重なった", img.display()));
            continue;
        }
        let vals: Vec<u64> = seen.iter().filter_map(|(_, v)| *v).collect();
        stats.push((seen.len(), first, vals.last().copied(), *at));
        if let Some(w) = vals.windows(2).position(|w| w[1] != w[0] + 1) {
            failures.push(format!(
                "{} ({mode:?}): 値が続いていない: {:?}",
                img.display(),
                &vals[w.saturating_sub(2)..(w + 3).min(vals.len())]
            ));
            continue;
        }
        // 記録は本体に届いた record だけ (書き出す前の record を記録に載せると、 落ちた像で本体に無い値を渡す)
        if let (Some(&last), Some(v)) = (vals.last(), value)
            && last > v
        {
            failures.push(format!("{} ({mode:?}): 記録が本体に無い値まで渡した (記録の最後 {last}、 開いた後の値 {v})", img.display()));
            continue;
        }
        if *at > 0 && vals.last().is_none_or(|&last| last + 1 < *at) {
            failures.push(format!("{} ({mode:?}): 書き出しの返った {at} 件まで無い (最後 {:?})", img.display(), vals.last()));
        }
    }
    let wrapped = stats.iter().filter(|s| s.1 > 0).count();
    let with_acked = stats.iter().filter(|s| s.3 > 0).count();
    eprintln!(
        "[{tag}] 像 {} 枚、 WAL を畳んだ回数 {folds}、 記録が一周していた像 {wrapped} 枚、 書き出しの返った後の像 {with_acked} 枚",
        images.len()
    );
    for s in &stats {
        eprintln!("  受けた {} 件、 最古 {}、 最後の値 {:?}、 書き出しの返った {}", s.0, s.1, s.2, s.3);
    }
    assert!(wrapped > 0 && with_acked >= IMAGES / 2, "前提: 記録が一周した像と、 書き出しの返った後の像がある");
    if !keep {
        let _ = std::fs::remove_dir_all(&root);
    }
    assert!(failures.is_empty(), "{} 枚が合わない:\n{}", failures.len(), failures.join("\n"));
}

/// 撮ったままの像 (`ENCHU_CFLOG_IMAGE` = `imgNNN.raw/db`) の記録と WAL を、 開かずに読んで並べる (調べる用)。
#[test]
#[ignore]
fn dump_image() {
    use enchudb_engine::change_log::ChangeLog;
    use enchudb_oplog::oplog::{DecodedOp, OpLog, HEADER_SIZE};
    let Ok(db) = std::env::var("ENCHU_CFLOG_IMAGE") else { return };
    let db = PathBuf::from(db);
    let log = ChangeLog::open(&db, 0, false, true).unwrap();
    let (first, next) = log.range();
    eprintln!("記録: {first}..{next}、 最後の印 {:?}", log.last_written());
    let tail = log.read(next.saturating_sub(3), 1 << 20).unwrap();
    for e in &tail {
        let (w, _) = WireRecord::decode(&e.bytes).unwrap();
        eprintln!("  記録 {} hlc {:?} op {:?} id {}", e.no, w.hlc, w.op, enchudb_engine::change_log::fnv1a64(&w.signed_bytes));
    }
    let wal = OpLog::open(&db.join("oplog")).unwrap();
    eprintln!("WAL: head {} checkpoint {}", wal.head(), wal.checkpoint());
    let (recs, end) = wal.scan_for_change_log(HEADER_SIZE as u64, u64::MAX, true);
    eprintln!("WAL の record {} 件、 読み終え {end}", recs.len());
    let v = |r: &enchudb_oplog::oplog::Record| match r.op { DecodedOp::Tie { value, .. } => Some(value), _ => None };
    if let (Some(a), Some(b)) = (recs.first(), recs.last()) {
        eprintln!("  最初 {:?} hlc {:?} / 最後 {:?} hlc {:?} (終端 {})", v(&a.0), a.0.hlc, v(&b.0), b.0.hlc, b.1);
    }
    let found = log.last_written().and_then(|(h, id)| recs.iter().position(|(r, _)| r.hlc == h && enchudb_engine::change_log::fnv1a64(&r.signed_bytes) == id));
    eprintln!("最後の印は WAL の {found:?} 件目");
}

