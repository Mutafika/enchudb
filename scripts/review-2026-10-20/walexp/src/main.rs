use enchudb_engine::{db_files, Engine, ValueType};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

fn path(tag: &str) -> String {
    std::env::temp_dir()
        .join(format!("enchudb-walexp-{tag}-{}", std::process::id()))
        .to_str()
        .unwrap()
        .to_string()
}

/// exp1: a single record larger than the whole WAL. Hypothesis: writer stalls WAL_ROOM_WAIT (5 s), then drops.
fn oversized(sync: bool) {
    let p = path(if sync { "over-sync" } else { "over" });
    let _ = db_files::remove_db(&p);
    let eng: Arc<Engine> = if sync {
        {
            let mut e = Engine::create_standalone(&p).unwrap();
            e.define_table("rows", 1000).unwrap();
            e.define_himo_in("rows", "body", ValueType::Leaf, 0).unwrap();
            e.define_himo_in("rows", "val", ValueType::Number, 0).unwrap();
            e.enable_sync_tables().unwrap();
            e.flush().unwrap();
        }
        let e = Engine::open_concurrent_with_oplog(&p, 64 << 10).unwrap();
        e.set_peer_id(1);
        e
    } else {
        let e: Arc<Engine> = Engine::create_concurrent_with_oplog(&p, 64 << 10).unwrap();
        e.ensure_himo_dynamic("body", ValueType::Leaf, 0).unwrap();
        e.ensure_himo_dynamic("val", ValueType::Number, 0).unwrap();
        e
    };
    let e = if sync { eng.entity_in("rows").unwrap() } else { eng.entity().unwrap() };
    let (body, val) = if sync { ("rows.body", "rows.val") } else { ("body", "val") };
    // warm: a few small writes first
    for i in 0..10u32 { eng.tie_to(e, val, i); }
    let big = "x".repeat(100 * 1024);
    let wk0 = eng.consumer_wakeups();
    let t = Instant::now();
    eng.tie_text_to(e, body, &big);
    let took = t.elapsed();
    let wk1 = eng.consumer_wakeups();
    println!("[oversized sync={sync}] tie_text_to(100 KiB) into 64 KiB WAL took {took:?}; dropped={} folds={} consumer_wakeups_delta={} value_readable={}",
        eng.wal_dropped_records(), eng.wal_room_folds(), wk1 - wk0, eng.get_text_owned(e, body).map(|v| v.len()).unwrap_or(0));
    // a second small write afterwards: does it proceed normally?
    let t = Instant::now();
    eng.tie_to(e, val, 99);
    println!("[oversized sync={sync}] next small write took {:?}; dropped={}", t.elapsed(), eng.wal_dropped_records());
    drop(eng);
    let _ = db_files::remove_db(&p);
}

/// exp2: non-sync DB, N writers, 256 KiB WAL: drops/folds + max single-write latency.
fn many_writers(writers: u64, writes: u64, wal: usize) {
    let p = path(&format!("many{writers}"));
    let _ = db_files::remove_db(&p);
    let eng: Arc<Engine> = Engine::create_concurrent_with_oplog(&p, wal).unwrap();
    let hid = eng.ensure_himo_dynamic("val", ValueType::Number, 0).unwrap();
    let rows: Arc<Vec<u64>> = Arc::new((0..1000).map(|_| eng.entity().unwrap()).collect());
    let max_ns = Arc::new(AtomicU64::new(0));
    let slow = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let hs: Vec<_> = (0..writers).map(|w| {
        let (eng, rows, max_ns, slow) = (eng.clone(), rows.clone(), max_ns.clone(), slow.clone());
        std::thread::spawn(move || {
            for i in (w..writes).step_by(writers as usize) {
                let t = Instant::now();
                eng.tie_to_by_id(rows[(i % 1000) as usize], hid, i + 1);
                let ns = t.elapsed().as_nanos() as u64;
                max_ns.fetch_max(ns, Ordering::Relaxed);
                if ns > 1_000_000_000 { slow.fetch_add(1, Ordering::Relaxed); }
            }
        })
    }).collect();
    for h in hs { h.join().unwrap(); }
    println!("[many writers={writers} wal={wal}] {writes} writes in {:?}: dropped={} folds={} max_write={:?} writes_over_1s={}",
        t0.elapsed(), eng.wal_dropped_records(), eng.wal_room_folds(), Duration::from_nanos(max_ns.load(Ordering::Relaxed)), slow.load(Ordering::Relaxed));
    drop(eng);
    let _ = db_files::remove_db(&p);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(|s| s.as_str()) {
        Some("over") => { oversized(false); oversized(true); }
        Some("many") => {
            let w: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(8);
            let n: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(200_000);
            let wal: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(256 << 10);
            many_writers(w, n, wal);
        }
        Some("many-sync") => {
            let w: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(8);
            let n: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(200_000);
            let wal: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(256 << 10);
            many_sync(w, n, wal);
        }
        Some("stalled") => stalled_listener(),
        _ => eprintln!("usage: walexp over | many [writers] [writes] [wal_bytes] | many-sync ... | stalled"),
    }
}

// ---- added modes ----
use enchudb_engine::changefeed::ChangeListener;
use enchudb_engine::transport::WireRecord;

struct Blocker { block_for: Duration, calls: AtomicU64 }
impl ChangeListener for Blocker {
    fn on_changes(&self, _: &[WireRecord]) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        std::thread::sleep(self.block_for);
    }
}

/// sync DB, N writers, small WAL: drops + max write latency (same as `many` but with sync tables).
pub fn many_sync(writers: u64, writes: u64, wal: usize) {
    let p = path(&format!("manysync{writers}"));
    let _ = db_files::remove_db(&p);
    {
        let mut e = Engine::create_standalone(&p).unwrap();
        e.define_table("rows", 100_000).unwrap();
        e.define_himo_in("rows", "val", ValueType::Number, 100_000).unwrap();
        e.enable_sync_tables().unwrap();
        e.flush().unwrap();
    }
    let eng = Engine::open_concurrent_with_oplog(&p, wal).unwrap();
    eng.set_peer_id(1);
    let hid = eng.himo_id("rows.val").unwrap() as u16;
    let rows: Arc<Vec<u64>> = Arc::new((0..1000).map(|_| eng.entity_in("rows").unwrap()).collect());
    let max_ns = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let hs: Vec<_> = (0..writers).map(|w| {
        let (eng, rows, max_ns) = (eng.clone(), rows.clone(), max_ns.clone());
        std::thread::spawn(move || {
            for i in (w..writes).step_by(writers as usize) {
                let t = Instant::now();
                eng.tie_to_by_id(rows[(i % 1000) as usize], hid, i + 1);
                max_ns.fetch_max(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
        })
    }).collect();
    for h in hs { h.join().unwrap(); }
    println!("[many-sync writers={writers} wal={wal}] {writes} writes in {:?}: dropped={} folds={} max_write={:?} floor_bumps={}",
        t0.elapsed(), eng.wal_dropped_records(), eng.wal_room_folds(), Duration::from_nanos(max_ns.load(Ordering::Relaxed)), eng.wal_drop_floor_bumps());
    drop(eng);
    let _ = db_files::remove_db(&p);
}

/// a listener that blocks 2 s per call: does oplog_sync() on another thread block behind it? does drop() hang?
pub fn stalled_listener() {
    let p = path("stalled");
    let _ = db_files::remove_db(&p);
    let eng: Arc<Engine> = Engine::create_concurrent_with_oplog(&p, 1 << 20).unwrap();
    let hid = eng.ensure_himo_dynamic("val", ValueType::Number, 0).unwrap();
    let l = Arc::new(Blocker { block_for: Duration::from_secs(2), calls: AtomicU64::new(0) });
    eng.add_change_listener(l.clone());
    let e = eng.entity().unwrap();
    eng.tie_to_by_id(e, hid, 1);
    eng.oplog_commit();
    // let the notifier pick it up (consumer tick 100 ms) and get stuck in the 2 s callback
    std::thread::sleep(Duration::from_millis(300));
    let calls0 = l.calls.load(Ordering::Relaxed);
    eng.tie_to_by_id(e, hid, 2);
    eng.oplog_commit();
    let t = Instant::now();
    eng.oplog_sync().unwrap();
    println!("[stalled] listener calls before oplog_sync={calls0}; oplog_sync() on the caller thread took {:?} (listener blocks 2 s per call)", t.elapsed());
    let t = Instant::now();
    eng.tie_to_by_id(e, hid, 3);
    println!("[stalled] plain tie_to during the stall took {:?}", t.elapsed());
    // drop: the notifier delivers remaining records (each call blocks 2 s) before Drop returns
    let t = Instant::now();
    drop(eng);
    println!("[stalled] drop(engine) took {:?}", t.elapsed());
    let _ = db_files::remove_db(&p);
}
