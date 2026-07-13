//! #95 — 出荷経路（実 `Engine` + `LockFreeCylinder`）で制約3つを実測する。
//!
//! `lockfree_bucket_probe` は手書きプロトタイプ（RwLock vs Epoch）だった。 こっちは
//! **本物の `tie_async → oplog → consumer → LockFreeCylinder` と `pull_raw` 経路**で、
//! 修正の主張を裏取りする:
//!
//!   A. write は read に stall しない … 巨大 bucket を reader が clone し続けても
//!      consumer の drain throughput がほぼ落ちない（0 reader vs 4 reader）。
//!   B. read は write に stall しない … writer が同 bucket を叩き続けても
//!      pull_raw の latency 分布が安定（idle vs hammer で p99 が跳ねない）。
//!   C. メモリ ~1x … N 本 tie 後の RSS 増分が raw column bytes に近い
//!      （ダブルバッファなら 2x になる）。
//!
//! Usage: cargo run --release --example lockfree_engine_bench [N]   (default 2,000,000)

use enchudb::{Engine, ValueType};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

fn rss_mb() -> u64 {
    #[cfg(target_os = "macos")]
    {
        use std::mem::MaybeUninit;
        unsafe {
            let mut info: MaybeUninit<libc::mach_task_basic_info> = MaybeUninit::uninit();
            let mut count = (std::mem::size_of::<libc::mach_task_basic_info>()
                / std::mem::size_of::<libc::natural_t>())
                as libc::mach_msg_type_number_t;
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
                let kb: u64 = v
                    .trim()
                    .split_whitespace()
                    .next()
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or(0);
                return kb / 1024;
            }
        }
        0
    }
}

fn fresh(path: &str) {
    for suf in ["", ".oplog", ".lock"] {
        let _ = std::fs::remove_file(format!("{path}{suf}"));
    }
}

/// entity cap は固定（growable は file 拡張のみ）。 scenario が作る総 entity 数を渡す。
fn build(path: &str, max_entities: u32) -> Arc<Engine> {
    fresh(path);
    let mut eng = Engine::create_growable_with_capacity(path, max_entities + 1000).unwrap();
    eng.define_himo("v", ValueType::Number, 0);
    Engine::concurrentize_with_oplog(eng, 256 * 1024 * 1024).unwrap()
}

fn pct(sorted_ns: &[u64], p: f64) -> f64 {
    if sorted_ns.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_ns.len() as f64 - 1.0) * p).round() as usize;
    sorted_ns[idx] as f64 / 1e6 // ms
}

/// A: drain throughput を 0 reader / 4 reader で比べる。
/// reader は巨大 bucket（value 7）を clone し続ける「long read」。
fn scenario_a(n: u32) {
    println!("── A. write は read に stall しない（drain M/s、 高いほど良い）──");
    let prefill = (n / 2).min(1_000_000);
    for readers in [0u32, 4] {
        let path = format!("/tmp/enchu_lf_a_{readers}.db");
        // prefill + 計測用 n 本の entity を作る。
        let eng = build(&path, prefill + n);
        let hid = eng.himo_id("v").unwrap() as u16;

        // value 7 に巨大 bucket を仕込む（reader の clone を重くする）。
        for _ in 0..prefill {
            let e = eng.entity();
            eng.tie_async_by_id(e, hid, 7);
        }
        eng.flush_writes();

        // 計測対象 entity を timing の外で先に確保（entity() の allocation noise を除外、
        // 測るのは純粋に tie_async → consumer drain の経路）。
        let eids: Vec<_> = (0..n).map(|_| eng.entity()).collect();

        let stop = Arc::new(AtomicBool::new(false));
        let reads = Arc::new(AtomicU64::new(0));
        let handles: Vec<_> = (0..readers)
            .map(|_| {
                let eng = eng.clone();
                let stop = stop.clone();
                let reads = reads.clone();
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        let v = eng.pull_raw("v", 7); // 巨大 clone = long read
                        std::hint::black_box(&v);
                        reads.fetch_add(1, Ordering::Relaxed);
                    }
                })
            })
            .collect();

        // 計測対象の write を投げて drain までの時間を測る。
        let t0 = Instant::now();
        for &e in &eids {
            eng.tie_async_by_id(e, hid, 7);
        }
        eng.flush_writes();
        let drain_s = t0.elapsed().as_secs_f64();

        stop.store(true, Ordering::Relaxed);
        for h in handles {
            h.join().unwrap();
        }
        println!(
            "  readers={readers} | drain {:>7.2} M/s | {:>6.1} ns/tie | long-reads done: {}",
            n as f64 / drain_s / 1e6,
            drain_s * 1e9 / n as f64,
            reads.load(Ordering::Relaxed),
        );
        drop(eng);
        fresh(&path);
    }
    println!(
        "  → drain が同オーダーを保てば「write は long read に stall しない」。"
    );
    println!(
        "    減少分は 4 reader の CPU/帯域 contention（lock 待ちではない）。"
    );
    println!(
        "    RwLock なら long read 1 本ごとに consumer の insert が丸ごと待つ（probe で 214ms）。\n"
    );
}

/// B: pull_raw の latency 分布を writer idle / hammer で比べる。
fn scenario_b(n: u32) {
    println!("── B. read は write に stall しない（pull_raw latency、 低いほど良い）──");
    let path = "/tmp/enchu_lf_b.db";
    let prefill = (n / 2).min(1_000_000);
    // hammer writer の entity 上限。 sampling 中に食い尽くさないよう cap に含める。
    let hammer_budget = n;
    let eng = build(path, prefill + hammer_budget);
    let hid = eng.himo_id("v").unwrap() as u16;

    // value 7 に bucket を仕込む。
    for _ in 0..prefill {
        let e = eng.entity();
        eng.tie_async_by_id(e, hid, 7);
    }
    eng.flush_writes();

    let sample = |eng: &Arc<Engine>| -> Vec<u64> {
        let mut lat = Vec::with_capacity(300);
        for _ in 0..300 {
            let t = Instant::now();
            let v = eng.pull_raw("v", 7);
            std::hint::black_box(&v);
            lat.push(t.elapsed().as_nanos() as u64);
        }
        lat.sort_unstable();
        lat
    };

    // idle: writer なし。
    let idle = sample(&eng);

    // hammer: writer が value 7 を叩き続ける最中に read latency を測る。
    let stop = Arc::new(AtomicBool::new(false));
    let applied = Arc::new(AtomicU64::new(0));
    let w = {
        let eng = eng.clone();
        let stop = stop.clone();
        let applied = applied.clone();
        std::thread::spawn(move || {
            // entity budget を使い切ったら stop まで idle（cap 超過を防ぐ）。
            while !stop.load(Ordering::Relaxed)
                && applied.load(Ordering::Relaxed) < hammer_budget as u64
            {
                let e = eng.entity();
                eng.tie_async_by_id(e, hid, 7);
                applied.fetch_add(1, Ordering::Relaxed);
            }
        })
    };
    let hammer = sample(&eng);
    stop.store(true, Ordering::Relaxed);
    w.join().unwrap();

    println!("  {:>7} | {:>9} | {:>9} | {:>9}", "state", "p50 ms", "p99 ms", "max ms");
    println!(
        "  {:>7} | {:>9.3} | {:>9.3} | {:>9.3}",
        "idle",
        pct(&idle, 0.50),
        pct(&idle, 0.99),
        pct(&idle, 1.0)
    );
    println!(
        "  {:>7} | {:>9.3} | {:>9.3} | {:>9.3}   (writer applied {} 本)",
        "hammer",
        pct(&hammer, 0.50),
        pct(&hammer, 0.99),
        pct(&hammer, 1.0),
        applied.load(Ordering::Relaxed),
    );
    println!("  → hammer の p99 が idle と同オーダーなら「read は write に stall しない」\n");
    drop(eng);
    fresh(path);
}

/// C: index（column + cylinder）の RSS 増分を isolate する。
/// oplog を挟むと 256MB mmap が RSS を支配して cylinder 分が埋もれるので、
/// **standalone（oplog なし）** で column + cylinder だけを載せて測る。
/// N entity は column 4B + cylinder 4B = N×8B が理論下限。 append-only は ~1x、
/// double-buffer なら cylinder が 2 倍で全体が押し上がる。
fn scenario_c(n: u32) {
    println!("── C. index メモリ ~1x（cylinder が double-buffer なら押し上がる）──");
    let path = "/tmp/enchu_lf_c.db";
    fresh(path);
    // standalone: oplog を張らない（concurrentize しない）→ RSS は column+cylinder 主体。
    let mut eng = Engine::create_growable_with_capacity(path, n + 1000).unwrap();
    eng.define_himo("v", ValueType::Number, 0);

    let before = rss_mb();
    // 10k cardinality に散らす（bucket 平均 ~200、 現実的な分布）。&mut tie は cylinder も live 更新。
    let eids: Vec<_> = (0..n).map(|_| eng.entity()).collect();
    for (i, &e) in eids.iter().enumerate() {
        eng.tie(e, "v", (i as u32) % 10_000);
    }
    // cylinder を完全に実体化（append-only なので既に live、 念のため touch）。
    std::hint::black_box(eng.pull_raw("v", 0));
    let after = rss_mb();

    // 厳密指標: cylinder が確保している eid backing の総 bytes（RSS ノイズなし）。
    // append-only なら各 eid は 1 度だけ載る → N×4 に pow2 slack を足しただけ。
    // double-buffer なら 2 コピーで >= 2x になる（この比が < 2 なら double-buffer なし）。
    let cyl_bytes = eng.himo_cylinder_backing_bytes("v").unwrap();
    let eid_min = n as usize * 4; // 各 eid 1 度・slack ゼロの下限
    let delta = after.saturating_sub(before);
    println!("  N={n} ties, cardinality 10k, standalone（oplog なし）");
    println!(
        "  cylinder backing: {:.1} MB = {:.2}x（eid×4 下限 {:.1} MB）← 厳密指標",
        cyl_bytes as f64 / 1024.0 / 1024.0,
        cyl_bytes as f64 / eid_min as f64,
        eid_min as f64 / 1024.0 / 1024.0,
    );
    println!("  参考: RSS {before} → {after} MB（+{delta} MB、 column+allocator 込みの粗い値）");
    println!("  → cylinder backing が < 2x なら append-only 単一保持（double-buffer していない）\n");
    drop(eng);
    fresh(path);
}

fn main() {
    let n: u32 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000);
    println!("#95 lock-free engine bench: N={n}（実 Engine 経路）\n");
    scenario_a(n);
    scenario_b(n);
    scenario_c(n);
}
