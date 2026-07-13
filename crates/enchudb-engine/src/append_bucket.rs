//! `AppendBucket` — 単一 writer / 多 reader の lock-free append-only 列。 #95。
//!
//! `BucketCylinder` の各 bucket（value → eid 列）の格納をこれに置き換える土台。
//! 現状の `RwLock<BucketCylinder>` は read が write に相互排他になり、 長い read が
//! write を stall させる（#95）。 この型は:
//!
//! - **read は完全 lock-free**（writer を一切待たない）。 crossbeam-epoch で pin。
//! - **write は単一 consumer 前提**で直列（append O(1) amortized）。
//! - 一度 publish した要素は二度と動かさない（append-only）ので、 read は published
//!   範囲 `[0..len]` を安全に `&[u32]` として読める。
//! - 容量超過時のみ backing を倍化し、 旧 backing は epoch で全 reader 通過後に解放。
//!
//! ## 並行契約
//! - `push` は **同時に 1 thread のみ**（consumer）。 複数 writer は未対応（race する）。
//! - `read_to_vec` / `with_read` / `len` は **多 reader 並行可**、 writer と並行可。

use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned};
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};

const INITIAL_CAP: usize = 4;

/// 単一の連続 backing。 `data[0..len]` が publish 済み（不変）、 `data[len..cap]` は spare。
struct Buf {
    data: Box<[UnsafeCell<u32>]>,
    len: AtomicUsize,
}

// SAFETY: published 範囲 [0..len] は書き換わらない。 writer は spare slot [len] のみ書く。
// reader は published 範囲のみ読む。 disjoint index への同時アクセスで、 可視化は len の
// Release/Acquire で順序付けられる。
unsafe impl Sync for Buf {}

impl Buf {
    fn with_cap(cap: usize) -> Self {
        let v: Vec<UnsafeCell<u32>> = (0..cap).map(|_| UnsafeCell::new(0)).collect();
        Self {
            data: v.into_boxed_slice(),
            len: AtomicUsize::new(0),
        }
    }
    #[inline]
    fn cap(&self) -> usize {
        self.data.len()
    }
    /// published 範囲を `&[u32]` として borrow。 caller が [0..n] の不変性を保証すること。
    #[inline]
    unsafe fn published(&self, n: usize) -> &[u32] {
        // SAFETY: caller が n <= published len かつ [0..n] 不変を保証。
        unsafe { std::slice::from_raw_parts(self.data.as_ptr() as *const u32, n) }
    }
}

pub struct AppendBucket {
    backing: Atomic<Buf>,
}

// SAFETY: 単一 writer / 多 reader。 全アクセスは atomic len + epoch で同期される。
unsafe impl Sync for AppendBucket {}
unsafe impl Send for AppendBucket {}

impl AppendBucket {
    /// 空の bucket（初回 push まで heap 確保しない）。
    pub fn new() -> Self {
        Self {
            backing: Atomic::new(Buf::with_cap(0)),
        }
    }

    /// 単一 writer append。 spare があれば in-place、 満杯なら倍化 + epoch 遅延解放。
    pub fn push(&self, eid: u32) {
        let guard = epoch::pin();
        let cur = self.backing.load(Ordering::Acquire, &guard);
        // SAFETY: backing は常に非 null（new/push で維持）。
        let b = unsafe { cur.deref() };
        let l = b.len.load(Ordering::Relaxed); // 単一 writer なので自身の len は Relaxed で可
        if l < b.cap() {
            // spare slot（未 publish の index l）に書いてから len を publish
            unsafe {
                *b.data[l].get() = eid;
            }
            b.len.store(l + 1, Ordering::Release);
        } else {
            let ncap = (b.cap() * 2).max(INITIAL_CAP);
            let nb = Buf::with_cap(ncap);
            // 既存 published 要素をコピー（[0..l] は不変なので安全に読める）
            let src = unsafe { b.published(l) };
            for (i, &v) in src.iter().enumerate() {
                unsafe {
                    *nb.data[i].get() = v;
                }
            }
            unsafe {
                *nb.data[l].get() = eid;
            }
            nb.len.store(l + 1, Ordering::Relaxed); // まだ誰も見ていない
            self.backing.store(Owned::new(nb), Ordering::Release);
            // 旧 backing は全 reader が epoch を通過してから解放
            unsafe {
                guard.defer_destroy(cur);
            }
        }
    }

    /// 外部 guard 下で published slice を borrow（query 単位 pin 用、 zero-copy）。
    #[inline]
    pub fn with_read<R>(&self, guard: &Guard, f: impl FnOnce(&[u32]) -> R) -> R {
        let cur = self.backing.load(Ordering::Acquire, guard);
        let b = unsafe { cur.deref() };
        let n = b.len.load(Ordering::Acquire);
        // SAFETY: [0..n] は publish 済み = 不変。 guard が backing を生存させる。
        let slice = unsafe { b.published(n) };
        f(slice)
    }

    /// snapshot コピー（standalone 用、 内部で pin）。
    pub fn read_to_vec(&self) -> Vec<u32> {
        let guard = epoch::pin();
        self.with_read(&guard, |s| s.to_vec())
    }

    /// 現在の publish 済み件数（lock-free）。
    pub fn len(&self) -> usize {
        let guard = epoch::pin();
        let cur = self.backing.load(Ordering::Acquire, &guard);
        unsafe { cur.deref() }.len.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 現在の backing 容量（slot 数、 pow2 で切り上がる）。 メモリ会計用。
    /// append-only なので eid は 1 度だけ載る = capacity は published len の pow2 上界。
    pub fn capacity(&self) -> usize {
        let guard = epoch::pin();
        let cur = self.backing.load(Ordering::Acquire, &guard);
        unsafe { cur.deref() }.cap()
    }
}

impl Default for AppendBucket {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for AppendBucket {
    fn drop(&mut self) {
        // 排他アクセス（drop）なので unprotected で current backing を直接解放。
        // 過去 realloc の defer_destroy 分は crossbeam collector が回収する。
        let cur = self.backing.load(Ordering::Relaxed, unsafe { epoch::unprotected() });
        if !cur.is_null() {
            // SAFETY: 他 thread からの参照なし（drop = 単独所有）。
            unsafe {
                drop(cur.into_owned());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn basic_push_read() {
        let b = AppendBucket::new();
        assert!(b.is_empty());
        for i in 0..100u32 {
            b.push(i);
        }
        assert_eq!(b.len(), 100);
        let v = b.read_to_vec();
        assert_eq!(v, (0..100).collect::<Vec<_>>());
    }

    #[test]
    fn realloc_growth() {
        let b = AppendBucket::new();
        for i in 0..10_000u32 {
            b.push(i);
        }
        assert_eq!(b.len(), 10_000);
        assert_eq!(b.read_to_vec(), (0..10_000).collect::<Vec<_>>());
    }

    #[test]
    fn with_read_zero_copy() {
        let b = AppendBucket::new();
        for i in 0..50u32 {
            b.push(i * 2);
        }
        let guard = epoch::pin();
        let sum: u64 = b.with_read(&guard, |s| s.iter().map(|&x| x as u64).sum());
        assert_eq!(sum, (0..50).map(|i| (i * 2) as u64).sum());
    }

    /// 1 writer + N reader 並行: 破損なし、 len 単調非減少、 全要素 valid。
    #[test]
    fn concurrent_writer_readers() {
        let b = Arc::new(AppendBucket::new());
        let stop = Arc::new(AtomicBool::new(false));
        let n = 200_000u32;

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let b = b.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let mut prev = 0usize;
                    let mut iters = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        let v = b.read_to_vec();
                        // append-only なので len は単調非減少
                        assert!(v.len() >= prev, "len regressed {} -> {}", prev, v.len());
                        prev = v.len();
                        // 内容は push した値そのもの（i 番目 == i）
                        for (i, &x) in v.iter().enumerate() {
                            assert_eq!(x, i as u32, "corruption at idx {i}");
                        }
                        iters += 1;
                    }
                    iters
                })
            })
            .collect();

        // 単一 writer
        for i in 0..n {
            b.push(i);
        }
        stop.store(true, Ordering::Relaxed);
        for r in readers {
            r.join().unwrap();
        }

        assert_eq!(b.len(), n as usize);
        let v = b.read_to_vec();
        assert_eq!(v, (0..n).collect::<Vec<_>>());
    }
}
