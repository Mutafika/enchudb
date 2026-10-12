//! 円柱を組んだ紐の書き手の、 値ごとの鍵 (2026-10-11)。
//!
//! [`crate::write_gate`] で、 円柱を組んでいない紐の書き手は `write_lock` を取らなくなった。 円柱を組んだ紐 (値から引いた
//! ことのある紐、 購読を張った紐) の書き手は `write_lock` で 1 本ずつ並んだままで、 書き手を増やすと合計の書き込みが遅く
//! なった (w1、 宣言なし・社員単位の購読: 書き手 1 本 14.8 M 回/秒 → 8 本 5.8〜6.1 M 回/秒。 書き手の時間の 67% が lock の
//! 取り合いで、 lock の下の仕事は 5% 未満)。
//!
//! 円柱の bucket ([`crate::append_bucket::AppendBucket`]) は 1 本ごとに書き手 1 本の約束で、 違う値の bucket に触る書き手
//! どうしは並べなくてよい ([`crate::order_index`] の置き場の鍵と同じ考え)。 並べるのは:
//!
//! - 同じ値の bucket に触る書き手 (足す・古い印を付ける・詰め直す): 値ごとの鍵 ([`ValueLocks::lock_pair`]、 古い値と新しい
//!   値の 2 本)
//! - sparse (`SparseRuns`、 書き手 1 本の約束) に触る書き手: `write_lock`
//! - 配列を伸ばす書き手 (配列を写して差し替える。 写す間に他の書き手が bucket を置くと、 新しい配列から消える):
//!   `write_lock` と全部の鍵 ([`ValueLocks::lock_all`])
//! - 書き手を止める側 (`HimoStore::write_guard`: 並びの索引の構築と購読の登録): `write_lock`、 門、 全部の鍵
//!
//! 取る順は `write_lock` → 鍵 (番号の小さい方から)。 鍵だけで書く書き手は `write_lock` を取らない。
//!
//! 鍵は spin lock (取るのは 1 回の RMW、 放すのは store)。 下で持つ仕事はたいてい数十〜数百 ns なので、 寝かせずに回って
//! 待つ ([`crate::row_lock`] と同じ待ち方: 64 回回ったら yield)。 bucket を詰め直す書き手 (stale が半分を超えた bucket の
//! 全部の cell を読む) は数十 µs 持つ。 値の下位 bit で選ぶので、 続いた値 (年齢、 同じ表の eid) は別の鍵になる。 鍵は
//! 128 B にそろえて cache line を分ける (1024 本 = 128 KB、 円柱を組む時に作る)。

use std::sync::atomic::{AtomicBool, Ordering};

/// 鍵の本数 (値の下位 bit で選ぶ)。
///
/// 2026-10-12: 64 本だと、 鍵 1 本に会社 1000 社のうち 16 社ほどが乗り、 書き手 8 本・購読ありで 1 本 300 万回書き続ける
/// (w1) と、 書き手の時間の 13〜16% が値の鍵の待ちだった (待ち 58〜67 万回のうち、 1 µs 以下の衝突が 55〜64 万回、 bucket の
/// 詰め直し (1 回 39〜44 µs、 4000 回ほど) の間の 16〜64 µs の待ちが 1.8〜2.0 万回)。 1024 本で待ちは 4〜4.5 万回、 書き手の
/// 時間の 1.7〜3.4% に減り、 8 本で 1.22〜1.26 倍。 書き手 1 本では差が出ない (順番をずらした交互 30 回)。
pub(crate) const STRIPES: usize = 1024;

/// 128 B に揃えて鍵ごとに cache line を分ける (Apple の M 系は 128 B)。
#[derive(Default)]
#[repr(align(128))]
struct Stripe(AtomicBool);

pub(crate) struct ValueLocks {
    stripes: Box<[Stripe]>,
}

/// 値 `v` の鍵の番号。
#[inline]
fn stripe_of(v: u64) -> usize {
    v as usize & (STRIPES - 1)
}

fn backoff(spins: &mut u32) {
    if *spins < 64 {
        std::hint::spin_loop();
        *spins += 1;
    } else {
        std::thread::yield_now();
    }
}

impl Default for ValueLocks {
    fn default() -> Self {
        Self::new()
    }
}

impl ValueLocks {
    pub(crate) fn new() -> Self {
        Self { stripes: (0..STRIPES).map(|_| Stripe::default()).collect() }
    }

    #[inline]
    fn lock_at(&self, i: usize) {
        let s = &self.stripes[i].0;
        if s.compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed).is_err() {
            Self::lock_slow(s);
        }
    }

    #[cold]
    fn lock_slow(s: &AtomicBool) {
        let mut spins = 0u32;
        loop {
            while s.load(Ordering::Relaxed) {
                backoff(&mut spins);
            }
            if s.compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                return;
            }
        }
    }

    #[inline]
    fn unlock_at(&self, i: usize) {
        self.stripes[i].0.store(false, Ordering::Release);
    }

    /// 値 `a` と `b` の鍵を取る (番号の小さい方から。 同じ鍵なら 1 本、 `None` は取らない)。
    #[inline]
    pub(crate) fn lock_pair(&self, a: Option<u64>, b: Option<u64>) -> Held<'_> {
        let (x, y) = match (a.map(stripe_of), b.map(stripe_of)) {
            (Some(x), Some(y)) if x == y => (Some(x), None),
            (Some(x), Some(y)) => (Some(x.min(y)), Some(x.max(y))),
            (x, None) => (x, None),
            (None, y) => (y, None),
        };
        if let Some(x) = x {
            self.lock_at(x);
        }
        if let Some(y) = y {
            self.lock_at(y);
        }
        Held { locks: self, x, y }
    }

    /// 全部の鍵を取る (番号の順)。 持っている間、 鍵で書く書き手は 1 本も中に居ない。
    pub(crate) fn lock_all(&self) -> HeldAll<'_> {
        for i in 0..STRIPES {
            self.lock_at(i);
        }
        HeldAll { locks: self }
    }

    /// 値 `v` の鍵を誰かが持っているか (test 用)。
    #[cfg(test)]
    pub(crate) fn is_held(&self, v: u64) -> bool {
        self.stripes[stripe_of(v)].0.load(Ordering::Relaxed)
    }
}

/// [`ValueLocks::lock_pair`] で取った鍵 (drop で放す)。
pub(crate) struct Held<'a> {
    locks: &'a ValueLocks,
    x: Option<usize>,
    y: Option<usize>,
}

impl Drop for Held<'_> {
    #[inline]
    fn drop(&mut self) {
        if let Some(y) = self.y {
            self.locks.unlock_at(y);
        }
        if let Some(x) = self.x {
            self.locks.unlock_at(x);
        }
    }
}

/// [`ValueLocks::lock_all`] で取った全部の鍵 (drop で放す)。
pub(crate) struct HeldAll<'a> {
    locks: &'a ValueLocks,
}

impl Drop for HeldAll<'_> {
    fn drop(&mut self) {
        for i in (0..STRIPES).rev() {
            self.locks.unlock_at(i);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn pair_takes_one_lock_for_values_on_the_same_stripe() {
        let l = ValueLocks::new();
        {
            let _h = l.lock_pair(Some(3), Some(3 + STRIPES as u64));
            assert!(l.is_held(3));
            assert!(!l.is_held(4));
        }
        assert!(!l.is_held(3), "drop で放す");
        {
            let _h = l.lock_pair(Some(9), Some(2));
            assert!(l.is_held(9) && l.is_held(2));
        }
        assert!(!l.is_held(9) && !l.is_held(2));
        {
            let _h = l.lock_pair(None, Some(5));
            assert!(l.is_held(5));
        }
        assert!(!l.is_held(5));
    }

    /// 鍵ごとの数を、 その鍵の下で 「読んでから書く」 で足す。 同じ鍵の書き手が重なれば足し算が失われる。 全部の鍵を
    /// 持つ間は、 鍵の書き手が 1 本も中に居ない (中の数を数える)。
    #[test]
    fn writers_on_a_stripe_and_lock_all_never_overlap() {
        let l = Arc::new(ValueLocks::new());
        let inside = Arc::new(AtomicUsize::new(0));
        let counts: Arc<Vec<AtomicUsize>> = Arc::new((0..STRIPES).map(|_| AtomicUsize::new(0)).collect());
        let rounds = if cfg!(miri) { 50 } else { 20_000 };
        let hs: Vec<_> = (0..4u64)
            .map(|t| {
                let (l, inside, counts) = (l.clone(), inside.clone(), counts.clone());
                std::thread::spawn(move || {
                    let mut touched = 0;
                    for i in 0..rounds as u64 {
                        let (a, b) = (i % 7, t + i % 5);
                        let _h = l.lock_pair(Some(a), Some(b));
                        inside.fetch_add(1, Ordering::Relaxed);
                        let mut add = |s: usize| {
                            let v = counts[s].load(Ordering::Relaxed);
                            counts[s].store(v + 1, Ordering::Relaxed);
                            touched += 1;
                        };
                        add(stripe_of(a));
                        if stripe_of(b) != stripe_of(a) {
                            add(stripe_of(b));
                        }
                        inside.fetch_sub(1, Ordering::Relaxed);
                    }
                    touched
                })
            })
            .collect();
        let mut overlaps = 0;
        for _ in 0..if cfg!(miri) { 5 } else { 2_000 } {
            let _all = l.lock_all();
            overlaps += usize::from(inside.load(Ordering::Relaxed) != 0);
        }
        let touched: usize = hs.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(overlaps, 0, "全部の鍵を持つ間に鍵の書き手が中に居た");
        let sum: usize = counts.iter().map(|c| c.load(Ordering::Relaxed)).sum();
        assert_eq!(sum, touched, "鍵の下の足し算が失われた (同じ鍵の書き手が重なった)");
    }
}
