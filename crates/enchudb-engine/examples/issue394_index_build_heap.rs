//! #394: open 後に Number 列の索引を初めて組む時のヒープ / footprint。
//!
//! 行 15,057,124、 値は `0..max` に等間隔の `distinct` 種 (issue の合成データと同じ形)。 readonly で開き直し、
//! `pull_raw` を 1 回呼んで索引を組ませ、 その前後のヒープ (counting allocator の生存量と峰) と phys_footprint を出す。
//!
//! 実行: cargo run --release -p enchudb-engine --example issue394_index_build_heap [distinct]
//!   (DB は初回だけ作る。 `ENCHU_394_ROWS` で行数を変えられる)

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(l) };
        if !p.is_null() {
            let n = LIVE.fetch_add(l.size(), Ordering::Relaxed) + l.size();
            PEAK.fetch_max(n, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
        unsafe { System.dealloc(p, l) };
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        let np = unsafe { System.realloc(p, l, new) };
        if !np.is_null() {
            if new >= l.size() {
                let n = LIVE.fetch_add(new - l.size(), Ordering::Relaxed) + (new - l.size());
                PEAK.fetch_max(n, Ordering::Relaxed);
            } else {
                LIVE.fetch_sub(l.size() - new, Ordering::Relaxed);
            }
        }
        np
    }
}

#[global_allocator]
static A: Counting = Counting;

fn mb(n: usize) -> f64 {
    n as f64 / (1024.0 * 1024.0)
}

#[cfg(target_os = "macos")]
fn footprint() -> usize {
    let mut info: libc::rusage_info_v2 = unsafe { std::mem::zeroed() };
    let r = unsafe {
        libc::proc_pid_rusage(
            std::process::id() as i32,
            libc::RUSAGE_INFO_V2,
            &mut info as *mut _ as *mut libc::rusage_info_t,
        )
    };
    if r == 0 { info.ri_phys_footprint as usize } else { 0 }
}

#[cfg(not(target_os = "macos"))]
fn footprint() -> usize {
    0
}

use enchudb_engine::{Engine, GrowableOptions, ValueType};

fn main() {
    let distinct: u64 = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(238_503);
    let rows: u64 = std::env::var("ENCHU_394_ROWS").ok().and_then(|s| s.parse().ok()).unwrap_or(15_057_124);
    let max: u64 = 989_554;
    let path = format!("/tmp/enchudb-issue394-{rows}-{distinct}");
    if !std::path::Path::new(&path).exists() {
        let mut eng = Engine::create_growable_opts(
            &path,
            GrowableOptions { max_entities: rows as u32 + 1024, ..Default::default() },
        )
        .unwrap();
        eng.define_himo("to_id", ValueType::Number, 0);
        for i in 0..rows {
            let e = eng.entity().unwrap();
            eng.tie(e, "to_id", ((i % distinct) * max / distinct) as u32);
        }
        eng.flush().unwrap();
        drop(eng);
    }
    let eng = Engine::open_readonly(&path).unwrap();
    let (live0, fp0) = (LIVE.load(Ordering::Relaxed), footprint());
    PEAK.store(live0, Ordering::Relaxed);
    let t = std::time::Instant::now();
    let n = eng.pull_raw("to_id", 1000u32).len();
    let took = t.elapsed();
    let (live1, peak, fp1) = (LIVE.load(Ordering::Relaxed), PEAK.load(Ordering::Relaxed), footprint());
    println!(
        "rows {rows} distinct {distinct}: pull_raw {took:?} (hit {n})  heap +{:.1} MB (peak +{:.1} MB)  footprint {:.1} -> {:.1} MB",
        mb(live1 - live0),
        mb(peak - live0),
        mb(fp0),
        mb(fp1)
    );
    // 少し後 (他の thread が epoch を進める機会は無い = readonly で何もしない server と同じ)
    let _ = eng.pull_raw("to_id", 2000u32);
    println!(
        "  after 2nd pull_raw: heap +{:.1} MB  footprint {:.1} MB",
        mb(LIVE.load(Ordering::Relaxed) - live0),
        mb(footprint())
    );
}
