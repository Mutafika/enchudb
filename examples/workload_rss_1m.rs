//! 1M entity workload の RSS / VSZ 実測。
//!
//! Usage:
//!   cargo run --release --example workload_rss_1m
//!   /usr/bin/time -l cargo run --release --example workload_rss_1m
//!
//! 計測するフェーズ:
//!   1. baseline (起動直後)
//!   2. after create_growable
//!   3. after define_himo x 7
//!   4. after 1M entity insert (1M * 7 tie)
//!   5. after rebuild
//!   6. after 10k mixed query
//!   7. after drop
//!
//! 各フェーズで in-program RSS / VSZ を出す。 peak RSS は /usr/bin/time -l 併用推奨。

use enchudb::{Engine, HimoType};
use std::time::Instant;

fn parse_n() -> u32 {
    std::env::args().nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000)
}

fn parse_q() -> u32 {
    std::env::args().nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000)
}

fn rss_mb() -> u64 {
    #[cfg(target_os = "macos")]
    {
        use std::mem::MaybeUninit;
        unsafe {
            let mut info: MaybeUninit<libc::mach_task_basic_info> = MaybeUninit::uninit();
            let mut count = (std::mem::size_of::<libc::mach_task_basic_info>()
                / std::mem::size_of::<libc::natural_t>()) as libc::mach_msg_type_number_t;
            let ret = libc::task_info(
                libc::mach_task_self(),
                libc::MACH_TASK_BASIC_INFO as libc::task_flavor_t,
                info.as_mut_ptr() as libc::task_info_t,
                &mut count,
            );
            if ret != libc::KERN_SUCCESS {
                return 0;
            }
            info.assume_init().resident_size / 1024 / 1024
        }
    }
    #[cfg(target_os = "linux")]
    {
        let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        for line in s.lines() {
            if let Some(v) = line.strip_prefix("VmRSS:") {
                let kb: u64 = v.trim().split_whitespace().next().unwrap_or("0").parse().unwrap_or(0);
                return kb / 1024;
            }
        }
        0
    }
}

fn vsz_mb() -> u64 {
    #[cfg(target_os = "macos")]
    {
        use std::mem::MaybeUninit;
        unsafe {
            let mut info: MaybeUninit<libc::mach_task_basic_info> = MaybeUninit::uninit();
            let mut count = (std::mem::size_of::<libc::mach_task_basic_info>()
                / std::mem::size_of::<libc::natural_t>()) as libc::mach_msg_type_number_t;
            let ret = libc::task_info(
                libc::mach_task_self(),
                libc::MACH_TASK_BASIC_INFO as libc::task_flavor_t,
                info.as_mut_ptr() as libc::task_info_t,
                &mut count,
            );
            if ret != libc::KERN_SUCCESS {
                return 0;
            }
            info.assume_init().virtual_size / 1024 / 1024
        }
    }
    #[cfg(target_os = "linux")]
    {
        let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        for line in s.lines() {
            if let Some(v) = line.strip_prefix("VmSize:") {
                let kb: u64 = v.trim().split_whitespace().next().unwrap_or("0").parse().unwrap_or(0);
                return kb / 1024;
            }
        }
        0
    }
}

fn snap(label: &str, t0: Instant) {
    use std::io::Write;
    println!(
        "[{:>7.3}s] RSS={:>6} MB  VSZ={:>7} MB  {}",
        t0.elapsed().as_secs_f64(),
        rss_mb(),
        vsz_mb(),
        label
    );
    let _ = std::io::stdout().flush();
}

fn main() {
    let n: u32 = parse_n();
    let q_iters: u32 = parse_q();
    println!("N = {}, Q_ITERS = {}", n, q_iters);
    use std::io::Write;
    let _ = std::io::stdout().flush();
    let path = "/tmp/enchudb_workload_rss_1m.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.wal", path));

    let t0 = Instant::now();
    snap("baseline", t0);

    let mut eng = Engine::create_growable_with_capacity(path, n + 100).unwrap();
    snap("after create_growable", t0);

    // v33_vs_pairs と同じ schema: 5 low-card + 2 high-card himos
    eng.define_himo("tenant", HimoType::Number, 10);
    eng.define_himo("dept",   HimoType::Number, 8);
    eng.define_himo("role",   HimoType::Number, 4);
    eng.define_himo("status", HimoType::Number, 5);
    eng.define_himo("year",   HimoType::Number, 5);
    eng.define_himo("salary", HimoType::Number, 1000);
    eng.define_himo("age",    HimoType::Number, 60);
    snap("after define_himo x 7", t0);

    let t_ins = Instant::now();
    for i in 0..n {
        let e = eng.entity();
        eng.tie(e, "tenant", i % 10);
        eng.tie(e, "dept",   (i / 10) % 8);
        eng.tie(e, "role",   (i / 80) % 4);
        eng.tie(e, "status", (i / 320) % 5);
        eng.tie(e, "year",   (i / 1600) % 5);
        eng.tie(e, "salary", (i * 31) % 1000);
        eng.tie(e, "age",    20 + (i * 17) % 40);
    }
    let ins_ms = t_ins.elapsed().as_millis();
    snap(&format!("after insert {} * 7 ties ({} ms)", n, ins_ms), t0);

    let t_r = Instant::now();
    eng.rebuild();
    let r_ms = t_r.elapsed().as_millis();
    snap(&format!("after rebuild ({} ms)", r_ms), t0);

    // mixed query: 多条件 AND を rotate
    let t_q = Instant::now();
    let mut total_hits = 0usize;
    for i in 0..q_iters {
        let tenant = i % 10;
        let dept = (i / 10) % 8;
        let status = (i / 80) % 5;
        let r = eng.query(&[("tenant", tenant), ("dept", dept), ("status", status)]);
        total_hits += r.len();
    }
    let q_ms = t_q.elapsed().as_millis();
    snap(&format!("after {} mixed query ({} ms, {} hits total)", q_iters, q_ms, total_hits), t0);

    let meta = std::fs::metadata(path).unwrap();
    println!(
        "\nfile apparent size: {:.1} MB  ({} bytes)",
        meta.len() as f64 / 1024.0 / 1024.0,
        meta.len(),
    );

    let t_drop = Instant::now();
    drop(eng);
    let drop_ms = t_drop.elapsed().as_millis();
    snap(&format!("after drop ({} ms)", drop_ms), t0);

    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.wal", path));
}
