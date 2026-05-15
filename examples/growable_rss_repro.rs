//! create_growable の起動 RSS / VSZ と process teardown 時間を実測する repro。
//!
//! Usage:
//!   cargo run --release --example growable_rss_repro [-- (default|cap16k|cap65k|cap1M|tiny)]
//!
//! issue2 の主訴は process exit 時の munmap teardown が 1.2 sec 食う件。 これは
//! GrowableMap が `reserve = layout.total_size` の anonymous PROT_NONE 範囲を
//! 取って、 exit でその範囲全部の VM map teardown が走るため。
//!
//! このスクリプトは:
//! 1. baseline / after-create / after-define の RSS / VSZ を出す
//! 2. drop(eng) の wall-clock を計測 (= munmap teardown のコスト)
//! 3. layout.total_size との比較で 「VSZ 増分 ≒ layout」 が成立してるか確認

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
    let mode = std::env::args().nth(1).unwrap_or_else(|| "default".to_string());

    let path = "/tmp/enchudb_growable_rss_repro.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.wal", path));

    let t0 = Instant::now();
    let baseline_vsz = vsz_mb();
    snap("baseline", t0);

    let eng = match mode.as_str() {
        "default" => enchudb::Engine::create_growable(path).unwrap(),
        "cap16k" => enchudb::Engine::create_growable_with_capacity(path, 16_384).unwrap(),
        "cap65k" => enchudb::Engine::create_growable_with_capacity(path, 65_536).unwrap(),
        "cap1M" => enchudb::Engine::create_growable_with_capacity(path, 1_048_576).unwrap(),
        "tiny" => enchudb::Engine::create_growable_tiny(path).unwrap(),
        other => {
            eprintln!("unknown mode: {}", other);
            std::process::exit(2);
        }
    };
    let after_create_vsz = vsz_mb();
    snap(&format!("after create_growable [mode={}]", mode), t0);

    {
        let mut eng = eng;
        for i in 0..50 {
            eng.define_himo(&format!("h{}", i), enchudb::HimoType::Number, 100);
        }
        snap("after define_himo × 50", t0);

        let meta = std::fs::metadata(path).unwrap();
        println!(
            "file on-disk apparent size: {} bytes ({:.1} MB)",
            meta.len(),
            meta.len() as f64 / 1024.0 / 1024.0
        );
        println!(
            "VSZ delta from baseline (= GrowableMap reserve): {} MB",
            after_create_vsz.saturating_sub(baseline_vsz),
        );

        // teardown 計測: ここで drop が走る (Engine → GrowableMap → munmap)
        let t_drop = Instant::now();
        drop(eng);
        let drop_ms = t_drop.elapsed().as_millis();
        println!(
            "[teardown] drop(Engine) wall-clock: {} ms (= GrowableMap munmap)",
            drop_ms
        );
    }

    // file 削除
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.wal", path));
}
