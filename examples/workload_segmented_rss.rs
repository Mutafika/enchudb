//! Table-segmented schema RSS test — モデル検証 (varied column counts)。
//!
//! ビジュアライザーの 6-gon 構造を再現:
//!   messages (5 cols) / tasks (3) / costs (2) / metadata (4) / tools (3) / settings (6)
//!   = 6 仮想 table × 23 himos 合計
//!
//! 各 table の entity は contiguous eid range に配置:
//!   messages eid 0..N、 tasks N..2N、 costs 2N..3N、 ...
//!
//! 仮説検証:
//!   - 現コードの positions は max_eid_tied 比例 (仮説 a)
//!     → 後ろの table の himo ほど positions が大きくなる
//!     → 計算: 5×N + 3×2N + 2×3N + 4×4N + 3×5N + 6×6N = 5+6+6+16+15+36 = 84N entries
//!     → 8 byte × 84 × 1M = 672 MB anonymous heap
//!   - ideal (eid_min..eid_max range のみ) なら:
//!     → 23 × N entries = 184 MB
//!   - round-robin pattern では 23 × 6N = 138 himo×N = **1104 MB**
//!
//! 比較:
//!   round-robin sparse (前回): 同じ total tuples で RSS 498 MB / 5M
//!   table-segmented (この test): 同じ total tuples なら RSS ?? / 6M
//!
//! Usage:
//!   cargo run --release --example workload_segmented_rss [N_PER_TABLE=1000000]

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

// 各 「table」 の仕様: (table 名、 himo 名のリスト、 各 himo の max_values)
fn tables() -> Vec<(&'static str, Vec<(&'static str, u32)>)> {
    vec![
        ("messages", vec![
            ("msg_author",  100),
            ("msg_room",    50),
            ("msg_time",    1000),
            ("msg_emoji",   200),
            ("msg_replies", 50),
        ]),
        ("tasks", vec![
            ("tsk_status",   5),
            ("tsk_owner",    100),
            ("tsk_priority", 4),
        ]),
        ("costs", vec![
            ("cost_amount",   1000),
            ("cost_category", 20),
        ]),
        ("metadata", vec![
            ("meta_version", 100),
            ("meta_author",  50),
            ("meta_created", 1000),
            ("meta_tags",    200),
        ]),
        ("tools", vec![
            ("tool_lang",     30),
            ("tool_runtime",  20),
            ("tool_size",     500),
        ]),
        ("settings", vec![
            ("set_theme",     10),
            ("set_lang",      50),
            ("set_notif",     5),
            ("set_privacy",   8),
            ("set_audio",     30),
            ("set_layout",    15),
        ]),
    ]
}

fn main() {
    let n: u32 = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(1_000_000);
    let path = "/tmp/enchudb_segmented_rss.db";
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.oplog", path));

    let tbls = tables();
    let total_himos: usize = tbls.iter().map(|(_, h)| h.len()).sum();
    let total_entities = n * tbls.len() as u32;
    let total_ties: u64 = tbls.iter().map(|(_, h)| h.len() as u64 * n as u64).sum();

    println!("=== table-segmented schema ===");
    for (name, himos) in &tbls {
        println!("  {:<10} ({} cols): {:?}", name, himos.len(), himos.iter().map(|(n, _)| *n).collect::<Vec<_>>());
    }
    println!("Total: {} tables, {} himos, {} entities, {} ties\n", tbls.len(), total_himos, total_entities, total_ties);

    let t0 = Instant::now();
    snap("baseline", t0);

    let mut eng = Engine::create_growable_with_capacity(path, total_entities + 100).unwrap();
    for (_, himos) in &tbls {
        for &(name, max_v) in himos {
            eng.define_himo(name, HimoType::Number, max_v);
        }
    }
    snap(&format!("after define_himo x {}", total_himos), t0);

    // table-segmented insert: 各 table が連続 eid range を占める
    let t_ins = Instant::now();
    for (table_idx, (_, himos)) in tbls.iter().enumerate() {
        for i in 0..n {
            let e = eng.entity();
            // この entity は自 table の himo だけを tie
            for (h_idx, &(name, _)) in himos.iter().enumerate() {
                let val = (i.wrapping_mul(31).wrapping_add(h_idx as u32 * 17 + table_idx as u32 * 13)) % 1000;
                eng.tie(e, name, val);
            }
        }
    }
    let ins_ms = t_ins.elapsed().as_millis();
    snap(&format!("after insert {} entities × per-table cols = {} ties ({} ms)",
        total_entities, total_ties, ins_ms), t0);

    // positions の理論計算
    println!("\n── positions size 理論値 ──");
    let mut current_eid_end: u64 = 0;
    let mut total_positions_dense: u64 = 0;
    let mut total_positions_ideal: u64 = 0;
    for (i, (table_name, himos)) in tbls.iter().enumerate() {
        let eid_start = current_eid_end;
        let eid_end = current_eid_end + n as u64;
        current_eid_end = eid_end;
        let table_size = n as u64;
        let himos_per_table = himos.len() as u64;
        let dense = himos_per_table * eid_end * 8;
        let ideal = himos_per_table * table_size * 8;
        total_positions_dense += dense;
        total_positions_ideal += ideal;
        println!(
            "  {:<10} eid {:>9}..{:<9} | {} himos × max_eid_tied={:>9} → {:>5} MB | ideal {:>5} MB",
            table_name, eid_start, eid_end, himos_per_table, eid_end,
            dense / 1_048_576, ideal / 1_048_576,
        );
    }
    println!(
        "  ─────\n  positions (current code, max_eid_tied):  {} MB",
        total_positions_dense / 1_048_576
    );
    println!(
        "  positions (ideal, table-local range):    {} MB",
        total_positions_ideal / 1_048_576
    );
    println!(
        "  実測 RSS との差分から実態を推定する\n"
    );

    let meta = std::fs::metadata(path).unwrap();
    println!("file apparent size: {:.1} MB", meta.len() as f64 / 1024.0 / 1024.0);
    snap("end", t0);

    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}.crc", path));
    let _ = std::fs::remove_file(format!("{}.oplog", path));
}
