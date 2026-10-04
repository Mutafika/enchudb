//! #391: 何も書いていない DB を N 個開いた時の、 process の CPU 時間。 issue の再現 (engine の API で同じ形)。
//!
//! `cargo run --release -p enchudb-engine --example idle_wake_bench -- 32 [nooplog]`

use enchudb_engine::{Engine, ValueType};
use std::time::{Duration, Instant};

fn cpu_secs() -> f64 {
    let mut ru: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut ru) };
    let t = |v: libc::timeval| v.tv_sec as f64 + v.tv_usec as f64 / 1e6;
    t(ru.ru_utime) + t(ru.ru_stime)
}

fn main() {
    let n: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(1);
    let oplog = std::env::args().nth(2).as_deref() != Some("nooplog");
    let dir = std::env::temp_dir().join(format!("idlewake_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut dbs = Vec::new();
    for i in 0..n {
        let path = dir.join(format!("db{i}.enchu"));
        let mut eng = Engine::create_growable_with_capacity(path.to_str().unwrap(), 100_000).unwrap();
        eng.define_himo("k", ValueType::Number, 0);
        let eng = if oplog { Engine::concurrentize_with_oplog(eng, 64 << 20).unwrap() } else { Engine::concurrentize(eng) };
        let e = eng.entity().unwrap();
        eng.tie_to_by_id(e, 0, 1u32);
        eng.commit();
        dbs.push(eng);
    }
    std::thread::sleep(Duration::from_secs(2));
    let (c0, t0) = (cpu_secs(), Instant::now());
    std::thread::sleep(Duration::from_secs(10));
    let (c, t) = (cpu_secs() - c0, t0.elapsed().as_secs_f64());
    println!(
        "{n:>3} DB{}  idle {t:.1} s  CPU {:.2} ms/s ({:.3}% of a core)",
        if oplog { "" } else { " (oplog なし)" },
        c / t * 1e3,
        c / t * 100.0
    );
    drop(dbs);
    let _ = std::fs::remove_dir_all(&dir);
}
