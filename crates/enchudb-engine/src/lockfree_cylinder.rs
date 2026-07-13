//! `LockFreeCylinder` — value → eid の lock-free concurrent bucket store。 #95。
//!
//! 現状の `RwLock<BucketCylinder>` を置き換える。 単一 writer（consumer）/ 多 reader。
//!
//! ## 構造
//! - **dense**（value < `DENSE_CAP`）: `Atomic<Vec<Arc<AppendBucket>>>`。 外側 Vec を
//!   epoch-swap で成長させ、 `AppendBucket` 本体は `Arc` で stable（成長で動かない）。
//!   read は完全 lock-free。
//! - **sparse**（value ≥ `DENSE_CAP`、 稀）: `Mutex<HashMap>`。 common path（dense）を
//!   lock-free に保ち、 稀な高 value だけ lock を許容する pragmatic split。
//!
//! ## append-only + lazy verify（#95 設計）
//! - `insert` は該当 value の bucket に **append するだけ**。 旧 value からの削除はしない
//!   （swap_remove / positions は廃止）。 値更新・削除で生じる stale entry は、
//!   **read 側が Column を verify して filter** する（`HimoStore` 層の責務）。
//! - よって `total()` / `unique_count()` は churn した himo では **append 数ベースの
//!   over-count**（compaction するまで）。 append-only himo（削除なし）では正確。
//!   compaction は後付け最適化（#95）。

use crate::append_bucket::AppendBucket;
use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub const DENSE_CAP: u32 = 1 << 20;

type DenseArr = Vec<Arc<AppendBucket>>;

pub struct LockFreeCylinder {
    dense: Atomic<DenseArr>,
    sparse: Mutex<HashMap<u32, Arc<AppendBucket>>>,
    /// value < DENSE_CAP の初期成長ヒント（0 = 空 start）。
    dense_hint: u32,
    total: AtomicUsize,
    unique_count: AtomicU32,
}

// SAFETY: 単一 writer / 多 reader。 dense は epoch、 sparse は Mutex で同期。
unsafe impl Sync for LockFreeCylinder {}
unsafe impl Send for LockFreeCylinder {}

impl LockFreeCylinder {
    pub fn new(max_values: u32) -> Self {
        let hint = if max_values == 0 {
            0
        } else {
            ((max_values as usize + 1).min(DENSE_CAP as usize)) as u32
        };
        let init: DenseArr = (0..hint).map(|_| Arc::new(AppendBucket::new())).collect();
        Self {
            dense: Atomic::new(init),
            sparse: Mutex::new(HashMap::new()),
            dense_hint: hint,
            total: AtomicUsize::new(0),
            unique_count: AtomicU32::new(0),
        }
    }

    /// 単一 writer append。 value の bucket に eid を足す（旧 value は放置＝lazy verify）。
    pub fn insert(&self, eid: u32, value: u32) {
        debug_assert!(value != u32::MAX, "value == u32::MAX is sentinel");
        if value < DENSE_CAP {
            let guard = epoch::pin();
            let arr = self.dense.load(Ordering::Acquire, &guard);
            // SAFETY: dense は常に非 null。
            let vec = unsafe { arr.deref() };
            if (value as usize) < vec.len() {
                let b = &vec[value as usize];
                let was_empty = b.is_empty();
                b.push(eid);
                self.bump_stats(was_empty);
            } else {
                // 成長: doubling で amortize、 既存 Arc は clone（refcount）、 新規は空 bucket
                let mut nv: DenseArr = vec.clone();
                let new_len = ((value as usize) + 1).max(vec.len() * 2).min(DENSE_CAP as usize);
                nv.resize_with(new_len, || Arc::new(AppendBucket::new()));
                nv[value as usize].push(eid);
                self.bump_stats(true); // 新 bucket は必ず空だった
                self.dense.store(Owned::new(nv), Ordering::Release);
                // SAFETY: 旧 array は全 reader が epoch 通過後に解放。
                unsafe {
                    guard.defer_destroy(arr);
                }
            }
        } else {
            let mut sp = self.sparse.lock().unwrap();
            let b = sp
                .entry(value)
                .or_insert_with(|| Arc::new(AppendBucket::new()));
            let was_empty = b.is_empty();
            b.push(eid);
            drop(sp);
            self.bump_stats(was_empty);
        }
    }

    #[inline]
    fn bump_stats(&self, was_empty: bool) {
        self.total.fetch_add(1, Ordering::Relaxed);
        if was_empty {
            self.unique_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// dense value の bucket を外部 guard 下で read（lock-free、 zero-copy）。
    /// sparse は None を返す（sparse は `read_to_vec` 経由で）。
    #[inline]
    pub fn with_dense_read<R>(
        &self,
        guard: &Guard,
        value: u32,
        f: impl FnOnce(&[u32]) -> R,
    ) -> Option<R> {
        if value >= DENSE_CAP {
            return None;
        }
        let arr = self.dense.load(Ordering::Acquire, guard);
        let vec = unsafe { arr.deref() };
        if (value as usize) < vec.len() {
            Some(vec[value as usize].with_read(guard, f))
        } else {
            Some(f(&[]))
        }
    }

    /// value の全 eid を Vec で返す（dense / sparse 両対応、 内部で pin）。
    /// 注: stale filter はしない（caller = HimoStore が Column verify する）。
    pub fn read_to_vec(&self, value: u32) -> Vec<u32> {
        if value < DENSE_CAP {
            let guard = epoch::pin();
            self.with_dense_read(&guard, value, |s| s.to_vec())
                .unwrap_or_default()
        } else {
            let sp = self.sparse.lock().unwrap();
            match sp.get(&value) {
                Some(b) => b.read_to_vec(),
                None => Vec::new(),
            }
        }
    }

    /// value の bucket 長（raw、 stale 込み）。planner の pivot 選択用。
    pub fn slice_len(&self, value: u32) -> usize {
        if value < DENSE_CAP {
            let guard = epoch::pin();
            let arr = self.dense.load(Ordering::Acquire, &guard);
            let vec = unsafe { arr.deref() };
            if (value as usize) < vec.len() {
                vec[value as usize].len()
            } else {
                0
            }
        } else {
            let sp = self.sparse.lock().unwrap();
            sp.get(&value).map(|b| b.len()).unwrap_or(0)
        }
    }

    pub fn total(&self) -> usize {
        self.total.load(Ordering::Relaxed)
    }

    pub fn unique_count(&self) -> u32 {
        self.unique_count.load(Ordering::Relaxed)
    }

    /// 非空 bucket の value を列挙（順序保証なし、 stale 込みの近似）。
    pub fn unique_values(&self) -> Vec<u32> {
        let guard = epoch::pin();
        let arr = self.dense.load(Ordering::Acquire, &guard);
        let vec = unsafe { arr.deref() };
        let mut out: Vec<u32> = vec
            .iter()
            .enumerate()
            .filter_map(|(v, b)| if b.is_empty() { None } else { Some(v as u32) })
            .collect();
        let sp = self.sparse.lock().unwrap();
        out.extend(sp.keys().copied());
        out
    }
}

impl Drop for LockFreeCylinder {
    fn drop(&mut self) {
        // 現在の dense array を解放（過去 grow の defer 分は collector が回収）。
        let cur = self.dense.load(Ordering::Relaxed, unsafe { epoch::unprotected() });
        if !cur.is_null() {
            // SAFETY: drop = 単独所有。
            unsafe {
                drop(cur.into_owned());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn dense_insert_read() {
        let c = LockFreeCylinder::new(0);
        for e in 0..100u32 {
            c.insert(e, e % 10); // value 0..9
        }
        assert_eq!(c.total(), 100);
        assert_eq!(c.unique_count(), 10);
        let v0 = c.read_to_vec(0);
        assert_eq!(v0, (0..100).filter(|e| e % 10 == 0).collect::<Vec<_>>());
    }

    #[test]
    fn dense_grow() {
        let c = LockFreeCylinder::new(0);
        // value を疎に増やして外側 Vec の成長を誘発
        c.insert(1, 5);
        c.insert(2, 500);
        c.insert(3, 50_000);
        c.insert(4, 999_999);
        // insert(eid, value): value 500→eid2, value 50000→eid3, value 999999→eid4
        assert_eq!(c.read_to_vec(500), vec![2]);
        assert_eq!(c.read_to_vec(50_000), vec![3]);
        assert_eq!(c.read_to_vec(999_999), vec![4]);
        assert_eq!(c.unique_count(), 4);
    }

    #[test]
    fn sparse_path() {
        let c = LockFreeCylinder::new(0);
        let big = DENSE_CAP + 42;
        c.insert(7, big);
        c.insert(8, big);
        assert_eq!(c.read_to_vec(big), vec![7, 8]);
        assert_eq!(c.unique_count(), 1);
        assert_eq!(c.slice_len(big), 2);
    }

    #[test]
    fn concurrent_writer_readers() {
        let c = Arc::new(LockFreeCylinder::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let n = 100_000u32;
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let c = c.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        // value 3 の bucket を読み続ける（成長する dense 配列越し）
                        let v = c.read_to_vec(3);
                        // 全要素は value 3 に insert した eid（= e where e%7==3）
                        for &e in &v {
                            assert_eq!(e % 7, 3, "corruption: {e}");
                        }
                    }
                })
            })
            .collect();
        for e in 0..n {
            c.insert(e, e % 7);
        }
        stop.store(true, Ordering::Relaxed);
        for r in readers {
            r.join().unwrap();
        }
        assert_eq!(c.total(), n as usize);
        assert_eq!(c.unique_count(), 7);
    }
}
