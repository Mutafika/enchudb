//! loom model — segment の **書いた範囲 (dirty) の取り出し** の契約を全 interleaving で検証する。#421。
//!
//! ## 何を守っているのか
//! 書き手は page に書いた後 `SegmentMap::mark_dirty` で範囲 [lo, hi) に足す。 `flush_dirty` は範囲を取り出して
//! msync する。 呼び側 (`oplog_sync` / consumer の周期 sync) は flush が返ったら 「呼ぶ前に印を付けた page は
//! 書き出し済み」 として checkpoint を進めるので、 守る性質は 2 つ:
//!
//! 1. **印が消えない**: `mark_dirty` と同時に flush が走っても、 その印はその flush か次の flush が書き出す。
//!    旧実装は lo / hi を別々の atomic に置き、 flush が別々に swap していた。 間に入った `mark_dirty` の lo だけが
//!    残って hi を flush が持っていくと、 残りは 「hi <= lo = 空」 に化け、 次の flush が飛ばす。
//! 2. **返った flush の前の印は書き出し済み**: flush が 2 本同時に走ると、 後の方は前の方が取り出した後の
//!    「空」 を見る。 前の方の msync が終わる前に返ると、 呼び側はまだ届いていない page で checkpoint を進める
//!    (電源断の模擬で `oplog_sync` の返った値が消えた)。
//!
//! 今の実装: 範囲は page 番号の [first, last] を 1 語 (`AtomicU64`) に詰め、 印は CAS、 取り出しは 1 回の swap。
//! flush は segment ごとの mutex で 1 本ずつ。 model はこの 2 つを写し、 旧い形 (lo / hi の 2 語 / lock なし) では
//! loom が窓を見つけることも確かめる (model が窓を表現できている確認)。 msync は 「範囲の page に書き出した印を
//! 立てる」 で置く。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_flush_dirty --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};

/// 空 (first = u32::MAX、 last = 0 = last < first)
const NONE: u64 = (u32::MAX as u64) << 32;
const LAST_MASK: u64 = u32::MAX as u64;
const PAGES: usize = 4;

#[derive(Clone, Copy)]
enum Range {
    /// 今の形: 1 語に詰めて CAS / swap
    Packed,
    /// 旧い形: lo / hi を別々の atomic に置き、 別々に swap
    Split,
}

struct Model {
    range: Range,
    one_at_a_time: bool,
    packed: AtomicU64,
    lo: AtomicUsize,
    hi: AtomicUsize,
    flush_lock: Mutex<()>,
    /// page ごとの 「書き出した」 (msync が終わった)
    synced: [AtomicBool; PAGES],
}

impl Model {
    fn new(range: Range, one_at_a_time: bool) -> Self {
        Self {
            range,
            one_at_a_time,
            packed: AtomicU64::new(NONE),
            lo: AtomicUsize::new(usize::MAX),
            hi: AtomicUsize::new(0),
            flush_lock: Mutex::new(()),
            synced: std::array::from_fn(|_| AtomicBool::new(false)),
        }
    }

    /// `SegmentMap::mark_dirty` (page 1 枚)。
    fn mark(&self, page: usize) {
        match self.range {
            Range::Packed => {
                // `mark_dirty` (判定は 1 語の読み 1 回) + `widen_dirty` (CAS)
                let (first, last) = (page as u64, page as u64);
                let mut cur = self.packed.load(Ordering::Relaxed);
                if cur >> 32 <= first && cur & LAST_MASK >= last {
                    return;
                }
                loop {
                    let (cfirst, clast) = (cur >> 32, cur & LAST_MASK);
                    let next = if clast < cfirst { (first << 32) | last } else { (cfirst.min(first) << 32) | clast.max(last) };
                    if next == cur {
                        return;
                    }
                    match self.packed.compare_exchange_weak(cur, next, Ordering::Release, Ordering::Relaxed) {
                        Ok(_) => return,
                        Err(now) => cur = now,
                    }
                }
            }
            Range::Split => {
                let (lo, hi) = (page, page + 1);
                if self.lo.load(Ordering::Relaxed) <= lo && self.hi.load(Ordering::Relaxed) >= hi {
                    return;
                }
                self.lo.fetch_min(lo, Ordering::Release);
                self.hi.fetch_max(hi, Ordering::Release);
            }
        }
    }

    /// 取り出した範囲を [lo, hi) で返す (空は hi <= lo)。
    fn take(&self) -> (usize, usize) {
        match self.range {
            Range::Packed => {
                let d = self.packed.swap(NONE, Ordering::AcqRel);
                let (first, last) = (d >> 32, d & LAST_MASK);
                if last < first { (1, 0) } else { (first as usize, last as usize + 1) }
            }
            Range::Split => {
                let lo = self.lo.swap(usize::MAX, Ordering::AcqRel);
                let hi = self.hi.swap(0, Ordering::AcqRel);
                (lo, hi)
            }
        }
    }

    /// `SegmentMap::flush_dirty`。
    fn flush(&self) {
        let _g = self.one_at_a_time.then(|| self.flush_lock.lock().unwrap());
        let (lo, hi) = self.take();
        if hi <= lo {
            return;
        }
        for p in lo..hi.min(PAGES) {
            self.synced[p].store(true, Ordering::Release);
        }
    }

    fn synced(&self, page: usize) -> bool {
        self.synced[page].load(Ordering::Acquire)
    }
}

/// 性質 1: page 3 に印がある所で、 page 1 への印と flush が同時に走る。 その後の flush の後は両方書き出し済み。
fn mark_during_flush(range: Range) {
    loom::model(move || {
        let m = Arc::new(Model::new(range, true));
        m.mark(3);
        let w = {
            let m = m.clone();
            loom::thread::spawn(move || m.mark(1))
        };
        m.flush();
        w.join().unwrap();
        m.flush();
        assert!(m.synced(3), "印が消えた (page 3)");
        assert!(m.synced(1), "印が消えた (page 1)");
    });
}

/// 性質 2: page 0 に印がある所で flush が 2 本同時に走る。 どちらも返った時には page 0 が書き出し済み。
fn concurrent_flushes(one_at_a_time: bool) {
    loom::model(move || {
        let m = Arc::new(Model::new(Range::Packed, one_at_a_time));
        m.mark(0);
        let f = {
            let m = m.clone();
            loom::thread::spawn(move || {
                m.flush();
                assert!(m.synced(0), "返った flush の前の印が書き出されていない");
            })
        };
        m.flush();
        assert!(m.synced(0), "返った flush の前の印が書き出されていない");
        f.join().unwrap();
    });
}

/// 1 語に詰めて 1 回の swap で取り出す = 同時の印が消えない。
#[test]
fn packed_range_keeps_a_concurrent_mark() {
    mark_during_flush(Range::Packed);
}

/// lo / hi を別々に swap すると loom が印の消える interleaving を見つける (旧実装)。
#[test]
#[should_panic(expected = "印が消えた")]
fn split_range_loses_a_concurrent_mark() {
    mark_during_flush(Range::Split);
}

/// flush を 1 本ずつにする = 後から来た flush は前の flush の書き出しを待ってから返る。
#[test]
fn one_flush_at_a_time_returns_after_the_write() {
    concurrent_flushes(true);
}

/// lock なしだと、 後から来た flush が前の flush の書き出しより先に返る interleaving を loom が見つける (旧実装)。
#[test]
#[should_panic(expected = "書き出されていない")]
fn unlocked_flush_returns_before_the_write() {
    concurrent_flushes(false);
}
