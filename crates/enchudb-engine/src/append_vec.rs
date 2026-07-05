//! `AppendVec<T>`: 固定 capacity・append-only・lock-free read の Vec 代替。
//!
//! `Engine` の himo 並列配列 (`himos` / `himo_names` / `value_types` /
//! `himo_max_values` / `himo_to_table`) を `&self` から定義追加できるようにする
//! ための内部構造 (himo dynamic definition, 0.9.0)。
//!
//! # 設計 (design choice "a")
//!
//! - slots は構築時に capacity 分だけ確保して以後 **realloc しない**。
//!   したがって公開済み要素への `&T` は AppendVec が生きてる限り不変アドレス。
//! - `push` は内部 Mutex で直列化 (定義系 = 低頻度 path のみが呼ぶ)。
//!   要素を slot に書いた **後** に `len` を Release store で publish する。
//! - reader は `len` を Acquire load して 0..len を lock-free に読む。
//!   Acquire/Release ペアで slot 書き込みが len より先に可視化されるので
//!   data race なし (= `RwLock` 案 "b" と違い hot path read に lock 不要)。
//! - 公開済み slot (0..len) は以後書き換えない (append-only 不変条件)。
//!   要素単位の後書きが必要なら `T = AtomicU16` 等の atomic 型を入れる。
//!
//! この 3 点で `unsafe` の健全性が閉じる: realloc なし + publish 前の slot は
//! reader から不可視 + 公開後は immutable。

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

pub struct AppendVec<T> {
    /// 固定長 slot 領域。 `len` 未満のみ初期化済み。
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    /// 初期化済み要素数。 push が Release で前進、 reader は Acquire で観測。
    len: AtomicUsize,
    /// push の直列化 (reader は取らない)。 Engine 側の himo_def_lock でも
    /// 直列化されるが、 構造単体でも安全になるよう defense in depth で持つ。
    push_lock: Mutex<()>,
}

// SAFETY: slots への並行アクセスは上記の publish 規約 (len の Acquire/Release +
// append-only + push の Mutex 直列化) で data race を排除している。
unsafe impl<T: Send> Send for AppendVec<T> {}
unsafe impl<T: Send + Sync> Sync for AppendVec<T> {}

impl<T> AppendVec<T> {
    pub fn with_capacity(cap: usize) -> Self {
        let mut v = Vec::with_capacity(cap);
        v.resize_with(cap, || UnsafeCell::new(MaybeUninit::uninit()));
        Self {
            slots: v.into_boxed_slice(),
            len: AtomicUsize::new(0),
            push_lock: Mutex::new(()),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Acquire)
    }

    #[inline]
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    #[allow(dead_code)]
    pub fn capacity(&self) -> usize {
        self.slots.len()
    }

    /// 末尾に append し、 index を返す。 capacity 超過なら `Err(v)` で値を返す。
    /// `&self` で呼べる (内部 Mutex で直列化)。
    pub fn push(&self, v: T) -> Result<usize, T> {
        let _g = self.push_lock.lock().unwrap();
        // push_lock 下なので Relaxed で十分 (writer は自分だけ)。
        let n = self.len.load(Ordering::Relaxed);
        if n >= self.slots.len() {
            return Err(v);
        }
        // SAFETY: slot n は len 未満に一度も入っていない = 未初期化領域への
        // 単独 write。 reader は len(Acquire) < n+1 の間この slot を見ない。
        unsafe { (*self.slots[n].get()).write(v) };
        self.len.store(n + 1, Ordering::Release);
        Ok(n)
    }

    #[inline]
    pub fn get(&self, i: usize) -> Option<&T> {
        if i < self.len() {
            // SAFETY: i < len(Acquire) なので slot i は初期化済みかつ以後不変。
            Some(unsafe { &*(self.slots[i].get() as *const T) })
        } else {
            None
        }
    }

    /// 初期化済み prefix を `&[T]` で返す。 slice 長は呼び出し時点の len で
    /// 固定される (並行 push は slice 外の slot にしか触れないので安全)。
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        let n = self.len();
        // SAFETY: UnsafeCell<MaybeUninit<T>> は T と layout 互換
        // (どちらも repr(transparent))。 先頭 n 要素は初期化済みで不変。
        unsafe { std::slice::from_raw_parts(self.slots.as_ptr() as *const T, n) }
    }

    #[inline]
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.as_slice().iter()
    }
}

impl<T> std::ops::Index<usize> for AppendVec<T> {
    type Output = T;
    #[inline]
    fn index(&self, i: usize) -> &T {
        match self.get(i) {
            Some(v) => v,
            None => panic!(
                "index out of bounds: the len is {} but the index is {}",
                self.len(),
                i
            ),
        }
    }
}

impl<'a, T> IntoIterator for &'a AppendVec<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<T> Drop for AppendVec<T> {
    fn drop(&mut self) {
        let n = *self.len.get_mut();
        for i in 0..n {
            // SAFETY: 先頭 n 要素は初期化済み。 &mut self なので単独アクセス。
            unsafe { (*self.slots[i].get()).assume_init_drop() };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn push_get_index_iter() {
        let v: AppendVec<String> = AppendVec::with_capacity(4);
        assert!(v.is_empty());
        assert_eq!(v.push("a".to_string()), Ok(0));
        assert_eq!(v.push("b".to_string()), Ok(1));
        assert_eq!(v.len(), 2);
        assert_eq!(v[0], "a");
        assert_eq!(v.get(1).map(|s| s.as_str()), Some("b"));
        assert_eq!(v.get(2), None);
        assert_eq!(v.iter().count(), 2);
        assert_eq!(v.as_slice(), &["a".to_string(), "b".to_string()][..]);
    }

    #[test]
    fn capacity_overflow_returns_err() {
        let v: AppendVec<u32> = AppendVec::with_capacity(1);
        assert_eq!(v.push(7), Ok(0));
        assert_eq!(v.push(8), Err(8));
        assert_eq!(v.len(), 1);
    }

    #[test]
    fn concurrent_readers_see_prefix() {
        let v: Arc<AppendVec<u64>> = Arc::new(AppendVec::with_capacity(1000));
        let writer = {
            let v = v.clone();
            std::thread::spawn(move || {
                for i in 0..1000u64 {
                    v.push(i).unwrap();
                }
            })
        };
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let v = v.clone();
                std::thread::spawn(move || {
                    for _ in 0..10_000 {
                        let n = v.len();
                        for (i, &x) in v.as_slice().iter().enumerate() {
                            assert_eq!(x, i as u64);
                        }
                        assert!(v.len() >= n);
                    }
                })
            })
            .collect();
        writer.join().unwrap();
        for r in readers {
            r.join().unwrap();
        }
        assert_eq!(v.len(), 1000);
    }
}
