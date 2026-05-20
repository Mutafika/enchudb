//! Sparse-schema RSS test — モデル検証。
//!
//! 仮説:
//!   storage ≈ 12 byte × Σ ties  ← N × max_himo じゃない
//!
//! 検証方法:
//!   同じ N entities、 同じ 7 himos 宣言、 ただし各 entity が tie するのは
//!   **3 himos のみ** (eid % 7 を起点に round-robin で 3 個選択)。
//!   total ties = N × 3、 dense 版の N × 7 の 43%。
//!
//! 期待:
//!   - dense 版 5M × 7 ties = 35M tuples → RSS 595 MB (insert 後、 measure 済)
//!   - sparse 版 5M × 3 ties = 15M tuples → 期待 RSS ~255 MB (43%)
//!   - もし positions の dense allocation が漏れてるなら sparse でも同じくらい
//!     RSS が出てしまう (= モデル違反確認)
//!
//! Usage:
//!   cargo run --release --example workload_sparse_rss [N=5000000]

use enchudb::{Engine, HimoType};
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
            if ret != libc::KERN_SUCCESS { return 0; }
            info.assume_init().resident_size / 1024 / 1024
        }
    }
    #[cfg(target_os = "linux")] { 0 }
}

fn snap(label: &str, t0: Instant) {
    use std::io::Write;
    println!("[{:>7.3}s] RSS={:>5} MB  {}", t0.elapsed().as_secs_f64(), rss_mb(), label);
    let _ = std::io::stdout().flush();
}

fn main() {
    let n: u32 = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(5_000_000);
    let path = "/tmp/enchudb_workload_sparse.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.oplog", path));

    let t0 = Instant::now();
    snap("baseline", t0);

    let mut eng = Engine::create_growable_with_capacity(path, n + 100).unwrap();
    eng.define_himo("tenant", HimoType::Number, 10);
    eng.define_himo("dept",   HimoType::Number, 8);
    eng.define_himo("role",   HimoType::Number, 4);
    eng.define_himo("status", HimoType::Number, 5);
    eng.define_himo("year",   HimoType::Number, 5);
    eng.define_himo("salary", HimoType::Number, 1000);
    eng.define_himo("age",    HimoType::Number, 60);
    snap("after define_himo x 7", t0);

    // sparse pattern: 各 entity は (eid % 7) を起点に 3 個の himo を tie。
    // 例: eid=0 は himo[0,1,2]、 eid=1 は himo[1,2,3]、 ... round-robin
    let himo_names = ["tenant", "dept", "role", "status", "year", "salary", "age"];
    let mut total_ties: u64 = 0;
    let t_ins = Instant::now();
    for i in 0..n {
        let e = eng.entity();
        let start = (i % 7) as usize;
        for k in 0..3 {
            let h = (start + k) % 7;
            let val = match h {
                0 => i % 10,
                1 => (i / 10) % 8,
                2 => (i / 80) % 4,
                3 => (i / 320) % 5,
                4 => (i / 1600) % 5,
                5 => (i * 31) % 1000,
                6 => 20 + (i * 17) % 40,
                _ => unreachable!(),
            };
            eng.tie(e, himo_names[h], val);
            total_ties += 1;
        }
    }
    let ins_ms = t_ins.elapsed().as_millis();
    snap(&format!(
        "after insert {} entities × 3 ties = {} tuples ({} ms)",
        n, total_ties, ins_ms
    ), t0);

    // 各 himo の実 tie count を print (モデル検証用)
    println!("\n── 各 himo の実 tie count (positions size 推定) ──");
    for &name in &himo_names {
        let len = eng.himo_cardinality(name).map(|c| c.to_string()).unwrap_or_else(|| "n/a".into());
        println!("  {:<8}: cardinality = {}", name, len);
    }

    let meta = std::fs::metadata(path).unwrap();
    println!("\nfile apparent size: {:.1} MB", meta.len() as f64 / 1024.0 / 1024.0);
    snap("end", t0);

    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.oplog", path));
}
