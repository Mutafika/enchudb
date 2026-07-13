//! ② prototype (#95) — lock-free append-only bucket の proof-of-concept。
//!
//! Option B の核（単一 backing + `AtomicUsize` published len + crossbeam-epoch で
//! realloc 遅延解放）を実装し、 現状の `RwLock<Vec>` 版と並べて
//! **「read は write にブロックされない / write は long read にブロックされない」**
//! を実測する。
//!
//! 仕掛け: bucket を大きめ (2M) に pre-fill → read_to_vec が ms かかる「long read」に
//! なる。 その long read を 2 reader が回してる最中に、 1 writer が push し続ける。
//!   - RwLock 版: writer の push は write lock を取れず、 long read が終わるまで待つ
//!     → **writer max latency が ms に跳ねる**（= 現状 enchu の 13ms stall と同型）。
//!   - Epoch 版: writer は reader を一切待たない → **writer max latency は µs**。
//!
//! Usage: cargo run --release --example lockfree_bucket_probe

use crossbeam_epoch::{self as epoch, Atomic, Owned};
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

const PREFILL: usize = 2_000_000;
const DUR_SECS: u64 = 3;

// ───────────────── 現状相当: RwLock<Vec> ─────────────────
struct RwLockBucket {
    inner: RwLock<Vec<u32>>,
}
impl RwLockBucket {
    fn new() -> Self {
        Self {
            inner: RwLock::new(Vec::new()),
        }
    }
    fn push(&self, eid: u32) {
        self.inner.write().unwrap().push(eid);
    }
    fn read_to_vec(&self) -> Vec<u32> {
        self.inner.read().unwrap().clone() // read lock を保持したままクローン
    }
}

// ───────────────── Option B: append-only + epoch ─────────────────
struct Buf {
    data: Box<[UnsafeCell<u32>]>, // cap slots
    len: AtomicUsize,             // publish 済み件数
}
unsafe impl Sync for Buf {}
unsafe impl Send for Buf {}
impl Buf {
    fn with_cap(cap: usize) -> Self {
        let v: Vec<UnsafeCell<u32>> = (0..cap).map(|_| UnsafeCell::new(0)).collect();
        Self {
            data: v.into_boxed_slice(),
            len: AtomicUsize::new(0),
        }
    }
}

struct EpochBucket {
    backing: Atomic<Buf>,
}
impl EpochBucket {
    fn new() -> Self {
        Self {
            backing: Atomic::new(Buf::with_cap(4)),
        }
    }

    /// 単一 writer 前提。
    fn push(&self, eid: u32) {
        let guard = epoch::pin();
        let cur = self.backing.load(Ordering::Acquire, &guard);
        let b = unsafe { cur.deref() };
        let l = b.len.load(Ordering::Relaxed);
        if l < b.data.len() {
            // spare slot（未 publish の index l）に書いてから len を publish
            unsafe {
                *b.data[l].get() = eid;
            }
            b.len.store(l + 1, Ordering::Release);
        } else {
            // 倍化: 新 backing を作って publish、 旧 backing は epoch で遅延解放
            let ncap = (b.data.len() * 2).max(4);
            let nb = Buf::with_cap(ncap);
            for i in 0..l {
                unsafe {
                    *nb.data[i].get() = *b.data[i].get();
                }
            }
            unsafe {
                *nb.data[l].get() = eid;
            }
            nb.len.store(l + 1, Ordering::Relaxed); // まだ誰も見ていない
            self.backing.store(Owned::new(nb), Ordering::Release);
            unsafe {
                guard.defer_destroy(cur);
            }
        }
    }

    /// 多 reader、 lock-free。
    fn read_to_vec(&self) -> Vec<u32> {
        let guard = epoch::pin();
        let cur = self.backing.load(Ordering::Acquire, &guard);
        let b = unsafe { cur.deref() };
        let n = b.len.load(Ordering::Acquire);
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            out.push(unsafe { *b.data[i].get() });
        }
        out
    }
}

struct Stats {
    reads: AtomicU64,
    writes: AtomicU64,
    reader_max_ns: AtomicU64,
    writer_max_ns: AtomicU64,
    corruption: AtomicU64,
}
impl Stats {
    fn new() -> Self {
        Self {
            reads: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            reader_max_ns: AtomicU64::new(0),
            writer_max_ns: AtomicU64::new(0),
            corruption: AtomicU64::new(0),
        }
    }
}
fn bump_max(a: &AtomicU64, v: u64) {
    let mut cur = a.load(Ordering::Relaxed);
    while v > cur {
        match a.compare_exchange_weak(cur, v, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(x) => cur = x,
        }
    }
}

fn run<B, R, W>(name: &str, bucket: Arc<B>, read: R, write: W)
where
    B: Send + Sync + 'static,
    R: Fn(&B) -> Vec<u32> + Send + Sync + Copy + 'static,
    W: Fn(&B, u32) + Send + Sync + Copy + 'static,
{
    let stats = Arc::new(Stats::new());
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = vec![];

    // reader x2: long read (2M clone) を回し続ける
    for _ in 0..2 {
        let b = bucket.clone();
        let s = stats.clone();
        let st = stop.clone();
        handles.push(thread::spawn(move || {
            let mut prev = 0usize;
            while !st.load(Ordering::Relaxed) {
                let t = Instant::now();
                let v = read(&b);
                let ns = t.elapsed().as_nanos() as u64;
                if v.len() + 100 < prev {
                    s.corruption.fetch_add(1, Ordering::Relaxed); // 単調減少 = 破損疑い
                }
                prev = v.len();
                s.reads.fetch_add(1, Ordering::Relaxed);
                bump_max(&s.reader_max_ns, ns);
            }
        }));
    }

    // writer x1: push し続けて、 各 push の latency を測る
    {
        let b = bucket.clone();
        let s = stats.clone();
        let st = stop.clone();
        handles.push(thread::spawn(move || {
            let mut i = PREFILL as u32;
            while !st.load(Ordering::Relaxed) {
                let t = Instant::now();
                write(&b, i);
                let ns = t.elapsed().as_nanos() as u64;
                bump_max(&s.writer_max_ns, ns);
                s.writes.fetch_add(1, Ordering::Relaxed);
                i = i.wrapping_add(1);
            }
        }));
    }

    thread::sleep(Duration::from_secs(DUR_SECS));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    println!(
        "{name:>10} | reads {:>7} | writes {:>9} | reader_max {:>8.2} ms | writer_max {:>8.3} ms | corruption {}",
        stats.reads.load(Ordering::Relaxed),
        stats.writes.load(Ordering::Relaxed),
        stats.reader_max_ns.load(Ordering::Relaxed) as f64 / 1e6,
        stats.writer_max_ns.load(Ordering::Relaxed) as f64 / 1e6,
        stats.corruption.load(Ordering::Relaxed),
    );
}

fn main() {
    println!(
        "prototype: bucket pre-fill {PREFILL} → long read (~ms)。 2 reader + 1 writer、 {DUR_SECS}s\n"
    );
    println!("狙い: writer_max が RwLock で ms に跳ね（long read 待ち）、 Epoch で µs に留まれば成功\n");

    // RwLock 版
    let rw = Arc::new(RwLockBucket::new());
    for i in 0..PREFILL as u32 {
        rw.push(i);
    }
    run(
        "RwLock",
        rw,
        |b| b.read_to_vec(),
        |b, e| b.push(e),
    );

    // Epoch 版
    let ep = Arc::new(EpochBucket::new());
    for i in 0..PREFILL as u32 {
        ep.push(i);
    }
    run(
        "Epoch",
        ep,
        |b| b.read_to_vec(),
        |b, e| b.push(e),
    );

    println!("\n読み: writer_max — RwLock は long read に待たされ ms、 Epoch は待たず µs〜サブ ms が期待。");
}
