//! #394: open 後に索引を初めて組む時、 組み立て中の一時確保 (並びを Vec に集める) も、 bucket を倍々で伸ばした
//! 古い backing (組む間ずっと pin を持つので epoch を過ぎず、 readonly で読むだけの process では残り続けた) も残さない。
//!
//! 生存ヒープを数える allocator で、 readonly で開いて `pull_raw` を 1 回呼んだ前後を測る。 この binary の test は
//! この 1 本だけ (他の test の確保を数えないため)。

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

use enchudb_engine::{Engine, GrowableOptions, ValueType};

/// issue の形を縮めた列 (行 200 万、 値 3 万種を 0..100 万に等間隔、 1 値あたり約 67 行) を readonly で開いて
/// 1 回引く。 索引の常駐は 1 行 4 B + 値ごとの bucket + 配列 (8 B × 最大値) 程度で、 組む間の峰もそれを大きく
/// 超えない。 直す前は常駐が約 2.5 倍 (伸ばした古い backing が残る)、 峰が常駐の約 3 倍 (並びの Vec と添字の Vec)。
#[test]
fn building_the_index_leaves_no_transient_heap() {
    let rows: u64 = 2_000_000;
    let distinct: u64 = 30_000;
    let max: u64 = 989_554;
    let path = format!(
        "/tmp/enchudb-issue394-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    );
    {
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
    }
    let eng = Engine::open_readonly(&path).unwrap();
    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let want = (0..rows).filter(|i| (i % distinct) * max / distinct == 33 * max / distinct).count();
    assert_eq!(eng.pull_raw("to_id", (33 * max / distinct) as u32).len(), want);
    let live = LIVE.load(Ordering::Relaxed) - base;
    let peak = PEAK.load(Ordering::Relaxed) - base;
    // 中身: 行 4 B × 200 万 = 8 MB、 bucket 3 万 × 100 B 前後 = 3 MB、 配列 8 B × 100 万 = 8 MB
    let mib = |n: usize| n as f64 / (1 << 20) as f64;
    eprintln!("index heap +{:.1} MiB (peak +{:.1} MiB)", mib(live), mib(peak));
    assert!(live < 24 << 20, "索引の常駐が大きい (伸ばした古い backing が残る?): +{:.1} MiB", mib(live));
    assert!(
        peak < live + (live >> 1),
        "組む間の峰が常駐を大きく超える (一時確保): 峰 +{:.1} MiB / 常駐 +{:.1} MiB",
        mib(peak),
        mib(live)
    );
    drop(eng);
    let _ = std::fs::remove_dir_all(&path);
}
