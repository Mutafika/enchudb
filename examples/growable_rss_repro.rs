//! create_growable の起動 RSS を実測する最小 repro。
//!
//! Usage:
//!   cargo run --release --example growable_rss_repro

use std::time::Instant;

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
    println!(
        "[{:>8.3}s] RSS={:>6} MB  VSZ={:>7} MB  {}",
        t0.elapsed().as_secs_f64(),
        rss_mb(),
        vsz_mb(),
        label
    );
}

fn main() {
    let path = "/tmp/enchudb_growable_rss_repro.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.wal", path));

    let t0 = Instant::now();
    snap("baseline", t0);

    let mut eng = enchudb::Engine::create_growable(path).expect("create_growable failed");
    snap("after create_growable", t0);

    // sinfo 相当の負荷: 200 himos を define する。
    // 修正前: 200 × 128 MB = 25 GB heap (BucketCylinder::positions)
    // 修正後: 各 himo の positions = Vec::new() → 数十 KB
    for i in 0..200 {
        eng.define_himo(&format!("h{}", i), enchudb::HimoType::Number, 100);
    }
    snap("after define_himo × 200", t0);

    let meta = std::fs::metadata(path).unwrap();
    println!("file on-disk size: {} bytes ({:.1} MB apparent)", meta.len(), meta.len() as f64 / 1024.0 / 1024.0);
}
