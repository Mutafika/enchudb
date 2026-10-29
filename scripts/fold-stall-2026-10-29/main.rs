//! 0.37 の WAL 2 区画で、 区画を畳む時に書き手が止まるか。
//! args: <none|listen|log|loglisten> <秒> <seed>
//! 表 t (id, a) に 10 万行。 WAL 64 MiB (FOLD_WAL_MIB)、 記録 64 MiB (FOLD_LOG_MIB)。 書き手 1 本が upsert し続け、 1 回ごとの時間を取る。
use enchudb::schema::Database;
use std::time::{Duration, Instant};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

fn env_usize(k: &str, d: usize) -> usize {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

struct Quiet;
impl enchudb::changefeed::ChangeListener for Quiet {}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let mode = a[1].clone();
    let secs: f64 = a[2].parse().unwrap();
    let seed: u64 = a[3].parse().unwrap();
    let rows = env_usize("FOLD_ROWS", 100_000);
    // 書く速さ (件/秒、 0 = 全力)。 1000 件ごとに予定の時刻まで寝る
    let rate = env_usize("FOLD_RATE", 0);
    let wal = env_usize("FOLD_WAL_MIB", 64) << 20;
    let log = env_usize("FOLD_LOG_MIB", 64) << 20;
    let dir = std::env::var("FOLD_DIR").expect("FOLD_DIR");
    let path = format!("{dir}/fold-{mode}-{}", std::process::id());
    {
        let mut db = Database::create(&path).unwrap();
        db.table("t").number("id").number("a").primary_key("id").build().unwrap();
        let t = db.get_table("t").unwrap();
        for i in 0..rows {
            t.insert().set("id", i as i64).set("a", (i % 100) as i64).commit().unwrap();
        }
    }
    let db = Database::open_with_oplog(&path, wal).unwrap();
    let eng = db.engine();
    if mode.starts_with("log") {
        eng.enable_change_log(log).unwrap();
    }
    if let Some(mib) = std::env::var("FOLD_BACKLOG_MIB").ok().and_then(|v| v.parse::<usize>().ok()) {
        eng.set_change_backlog_limit(mib << 20);
    }
    if mode.ends_with("listen") {
        eng.add_change_listener(std::sync::Arc::new(Quiet));
    }
    let t = db.get_table("t").unwrap();
    let mut r = Rng(seed | 1);
    let mut lats: Vec<u64> = Vec::with_capacity(8 << 20);
    let mut slow: Vec<(u64, u64)> = Vec::new();
    let (f0, s0) = (eng.wal_room_folds(), eng.wal_switches());
    let origin = Instant::now();
    let end = origin + Duration::from_secs_f64(secs);
    loop {
        let t0 = Instant::now();
        if t0 >= end {
            break;
        }
        let (i, v) = ((r.next() % rows as u64) as i64, (r.next() % 100) as i64);
        t.upsert().set("id", i).set("a", v).commit().unwrap();
        let ns = t0.elapsed().as_nanos() as u64;
        lats.push(ns);
        if rate > 0 && lats.len() % 1000 == 0 {
            let due = origin + Duration::from_secs_f64(lats.len() as f64 / rate as f64);
            let now = Instant::now();
            if due > now {
                std::thread::sleep(due - now);
            }
        }
        if ns > 10_000_000 {
            slow.push(((t0 - origin).as_millis() as u64, ns / 1_000_000));
        }
    }
    let (f1, s1) = (eng.wal_room_folds(), eng.wal_switches());
    let detached = eng.change_listeners_detached();
    let records = eng.change_log_range().map_or(0, |(f, x)| x - f);
    drop(t);
    drop(db);
    let _ = std::fs::remove_dir_all(&path);
    let n = lats.len();
    let stalled: u64 = slow.iter().map(|&(_, ms)| ms).sum();
    let slow_at: Vec<String> = slow.iter().map(|&(at, ms)| format!("[{at},{ms}]")).collect();
    lats.sort_unstable();
    println!(
        "{{\"mode\":\"{mode}\",\"rate\":{rate},\"seed\":{seed},\"secs\":{secs},\"wal_mib\":{},\"n\":{n},\"p50_ns\":{},\"p99_ns\":{},\"max_ms\":{:.1},\"slow\":{},\"slow_ms\":{stalled},\"room_folds\":{},\"switches\":{},\"log_records\":{records},\"detached\":{detached},\"slow_at\":[{}]}}",
        wal >> 20,
        lats[n / 2],
        lats[n * 99 / 100],
        lats[n - 1] as f64 / 1e6,
        slow.len(),
        f1 - f0,
        s1 - s0,
        slow_at.join(",")
    );
}
