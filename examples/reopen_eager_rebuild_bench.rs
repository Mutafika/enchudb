//! issue #3 bench: open 時の eager cylinder rebuild の cost を測る。
//!
//! N himos × M entities を tie した DB を作って drop、 再 open の wall-clock と
//! peak RSS を計測する。 master vs lazy-rebuild branch の比較に使う。
//!
//! Usage:
//!   cargo run --release --example reopen_eager_rebuild_bench
//!   cargo run --release --example reopen_eager_rebuild_bench -- 200 1000000
//!                                                              ^^^ ^^^^^^^
//!                                                              himos entities
//!
//! 出力:
//!   create  -- N entities × N himos の populate 時間
//!   drop    -- 明示 drop 時間 (flush 含む)
//!   reopen  -- open_standalone() の wall-clock
//!   queries -- reopen 後の 1000 random query の合計時間 (lazy rebuild の効果が
//!              query 側に出るので、 reopen + query を合計して比較する)

use enchudb::{Engine, HimoType};
use std::time::Instant;

fn rss_mb() -> u64 {
    #[cfg(target_os = "macos")]
    unsafe {
        use std::mem::MaybeUninit;
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
    #[cfg(not(target_os = "macos"))] { 0 }
}

fn main() {
    let himos: u32 = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(100);
    let entities: u32 = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(100_000);

    let path = format!("/tmp/enchudb_reopen_bench_{}_{}.db", himos, entities);
    for ext in &["", ".oplog", ".crc", ".lock"] {
        let _ = std::fs::remove_file(format!("{path}{ext}"));
    }

    println!("config: {} himos × {} entities", himos, entities);
    println!("rss(baseline) = {} MB", rss_mb());

    // ─── create + populate ───
    let himo_names: Vec<String> = (0..himos).map(|i| format!("h{i:03}")).collect();
    let t0 = Instant::now();
    {
        let mut eng = Engine::create_growable_with_capacity(&path, entities + 100).unwrap();
        for name in &himo_names {
            eng.define_himo(name, HimoType::Number, 100);
        }
        for i in 0..entities {
            let e = eng.entity();
            for (h_idx, name) in himo_names.iter().enumerate() {
                let v = (i.wrapping_mul(31).wrapping_add(h_idx as u32 * 17)) % 100;
                eng.tie(e, name, v);
            }
        }
        eng.flush().unwrap();
    }
    println!("create+populate: {:>7.2} ms  rss={} MB", t0.elapsed().as_secs_f64() * 1000.0, rss_mb());

    // ─── reopen (cold from disk) ───
    // 数回繰り返してウォーム化された OS page cache の影響を均す
    let mut reopen_times: Vec<f64> = Vec::new();
    for _ in 0..3 {
        let t = Instant::now();
        let eng = Engine::open_standalone(&path).unwrap();
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        reopen_times.push(ms);
        drop(eng);
    }
    let reopen_med = {
        let mut s = reopen_times.clone(); s.sort_by(|a, b| a.partial_cmp(b).unwrap());
        s[s.len() / 2]
    };
    println!("reopen (median of 3): {:>7.2} ms  rss={} MB  [samples: {:?}]",
        reopen_med, rss_mb(), reopen_times.iter().map(|x| format!("{:.1}", x)).collect::<Vec<_>>());

    // ─── reopen + 1000 query ───
    // lazy rebuild の場合は最初の query で per-himo rebuild が走る (= reopen time の cost が
    // ここに移動する)。 reopen + query 合計の比較が意味ある指標。
    let t = Instant::now();
    let eng = Engine::open_standalone(&path).unwrap();
    let t_reopen = t.elapsed().as_secs_f64() * 1000.0;

    let t = Instant::now();
    let mut total_hits = 0usize;
    for i in 0..1000u32 {
        let h_a = &himo_names[(i as usize) % himos as usize];
        let h_b = &himo_names[(i as usize + 7) % himos as usize];
        let v_a = (i.wrapping_mul(31)) % 100;
        let v_b = (i.wrapping_mul(13)) % 100;
        let r = eng.query(&[(h_a.as_str(), v_a), (h_b.as_str(), v_b)]);
        total_hits += r.len();
    }
    let t_q = t.elapsed().as_secs_f64() * 1000.0;
    println!("reopen + 1000 query: open={:>7.2} ms  query={:>7.2} ms  total={:>7.2} ms  hits={}",
        t_reopen, t_q, t_reopen + t_q, total_hits);
    drop(eng);

    // ─── cleanup ───
    for ext in &["", ".oplog", ".crc", ".lock"] {
        let _ = std::fs::remove_file(format!("{path}{ext}"));
    }
}
