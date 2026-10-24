//! WAL に積む lock (`OpLog::append_lock`、 #506)。
//!
//! 手順は parking_lot の `Mutex` と同じ: 状態は 1 byte の 2 bit (持っている / 寝ている書き手がいる)、 取れなければ少し回ってから
//! `parking_lot_core` で寝て、 離す側は寝ている書き手がいる時だけ起こす。 寝た書き手が飢えないように、 `parking_lot_core` が公平に
//! するよう言った時 (0.5 ms ごと) は、 lock を外さずに起こした書き手へそのまま渡す。
//!
//! 違うのは `try_lock` だけで、 CAS 1 回で取る (取れなければ CAS が返した状態を見る)。 parking_lot の `try_lock` は状態を普通に読んで
//! から CAS する。 Apple の core では、 atomic の命令で書く line を普通に読むと CAS だけの時より 1 回 4〜8 ns 重く、 書き手 1 本の
//! append (36 ns) が try_lock で 1 割遅くなった。 まとめ書き (#506) は lock を取れるかを try_lock で見る (取れなければ置き場に頼む)
//! ので、 書き手 1 本の道もこれを通る。
//!
//! 状態:
//!
//! | PARKED | LOCKED | 意味 |
//! |---|---|---|
//! | 0 | 0 | 誰も持たず、 誰も寝ていない |
//! | 0 | 1 | 1 本が持ち、 寝ている書き手はいない |
//! | 1 | 0 | 誰も持たず、 寝ている (寝ようとしている) 書き手がいる。 起こされた書き手がこれから取りに来る |
//! | 1 | 1 | 1 本が持ち、 寝ている書き手がいる。 PARKED を外すのは `parking_lot_core` の bucket の lock の下 (callback) だけ |

use lock_api::GuardNoSend;
use parking_lot_core::{DEFAULT_PARK_TOKEN, ParkResult, SpinWait, UnparkResult, UnparkToken};
use std::sync::atomic::{AtomicU8, Ordering};

const LOCKED: u8 = 0b01;
const PARKED: u8 = 0b10;
/// 起こした書き手は自分で取りに行く
const TOKEN_NORMAL: UnparkToken = UnparkToken(0);
/// 起こした書き手に lock を外さずに渡した
const TOKEN_HANDOFF: UnparkToken = UnparkToken(1);

pub(crate) type WalLock = lock_api::Mutex<WalRawLock, ()>;
pub(crate) type WalGuard<'a> = lock_api::MutexGuard<'a, WalRawLock, ()>;

pub(crate) struct WalRawLock {
    state: AtomicU8,
}

// SAFETY: 同時に 1 本だけが持つ (LOCKED を立てた CAS に勝った書き手、 または外さずに渡された書き手)。 Acquire で取り Release で離す。
unsafe impl lock_api::RawMutex for WalRawLock {
    #[allow(clippy::declare_interior_mutable_const)]
    const INIT: Self = Self { state: AtomicU8::new(0) };
    type GuardMarker = GuardNoSend;

    #[inline]
    fn lock(&self) {
        if self.state.compare_exchange_weak(0, LOCKED, Ordering::Acquire, Ordering::Relaxed).is_err() {
            self.lock_slow();
        }
    }

    #[inline]
    fn try_lock(&self) -> bool {
        match self.state.compare_exchange(0, LOCKED, Ordering::Acquire, Ordering::Relaxed) {
            Ok(_) => true,
            // 寝ている書き手だけがいる (PARKED のみ) 時も取る。 状態は CAS が返した値を使い、 読み直さない
            Err(mut s) => loop {
                if s & LOCKED != 0 {
                    return false;
                }
                match self.state.compare_exchange_weak(s, s | LOCKED, Ordering::Acquire, Ordering::Relaxed) {
                    Ok(_) => return true,
                    Err(x) => s = x,
                }
            },
        }
    }

    #[inline]
    unsafe fn unlock(&self) {
        if self.state.compare_exchange(LOCKED, 0, Ordering::Release, Ordering::Relaxed).is_ok() {
            return;
        }
        self.unlock_slow();
    }

    #[inline]
    fn is_locked(&self) -> bool {
        self.state.load(Ordering::Relaxed) & LOCKED != 0
    }
}

impl WalRawLock {
    #[cold]
    fn lock_slow(&self) {
        let mut spin = SpinWait::new();
        let mut state = self.state.load(Ordering::Relaxed);
        loop {
            // 持ち手がいなければ取る (寝ている書き手がいても)
            if state & LOCKED == 0 {
                match self.state.compare_exchange_weak(state, state | LOCKED, Ordering::Acquire, Ordering::Relaxed) {
                    Ok(_) => return,
                    Err(x) => state = x,
                }
                continue;
            }
            // 寝ている書き手がいなければ、 少し回って待つ
            if state & PARKED == 0 && spin.spin() {
                state = self.state.load(Ordering::Relaxed);
                continue;
            }
            // 寝る印を立てる (離す側が起こしに来る)
            if state & PARKED == 0
                && let Err(x) = self.state.compare_exchange_weak(state, state | PARKED, Ordering::Relaxed, Ordering::Relaxed)
            {
                state = x;
                continue;
            }
            let addr = self as *const Self as usize;
            // SAFETY: `addr` はこの lock の番地 (他の用途で park しない)。 callback は panic せず、 parking_lot の lock を取らない。
            // 時間切れは無い (timeout = None) ので timed_out は呼ばれない
            let r = unsafe {
                parking_lot_core::park(
                    addr,
                    || self.state.load(Ordering::Relaxed) == LOCKED | PARKED,
                    || {},
                    |_, _| {},
                    DEFAULT_PARK_TOKEN,
                    None,
                )
            };
            match r {
                // 離した書き手が lock を外さずに渡した
                ParkResult::Unparked(TOKEN_HANDOFF) => return,
                // 起こされた / 寝る前に状態が変わった: 取り直す
                ParkResult::Unparked(_) | ParkResult::Invalid | ParkResult::TimedOut => {}
            }
            spin.reset();
            state = self.state.load(Ordering::Relaxed);
        }
    }

    #[cold]
    fn unlock_slow(&self) {
        let addr = self as *const Self as usize;
        let callback = |result: UnparkResult| {
            // 公平にする番 (寝ている書き手が 0.5 ms 以上待った): lock を外さずに起こした書き手へ渡す
            if result.unparked_threads != 0 && result.be_fair {
                if !result.have_more_threads {
                    self.state.store(LOCKED, Ordering::Relaxed);
                }
                return TOKEN_HANDOFF;
            }
            // 外す。 まだ寝ている書き手がいれば PARKED を残す
            self.state.store(if result.have_more_threads { PARKED } else { 0 }, Ordering::Release);
            TOKEN_NORMAL
        };
        // SAFETY: `addr` はこの lock の番地。 callback は panic せず、 parking_lot の lock を取らない
        unsafe {
            parking_lot_core::unpark_one(addr, callback);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::UnsafeCell;

    struct Counter(UnsafeCell<u64>);
    // SAFETY: 試験の中で lock の下でだけ触る
    unsafe impl Sync for Counter {}

    impl Counter {
        fn ptr(&self) -> *mut u64 {
            self.0.get()
        }
    }

    /// 書き手 8 本が lock の下で数を足すと、 足した数が全部残る (同時に 2 本が持たない)。 寝る・起こすの道も通る (取り合いが多い)。
    #[test]
    fn lock_excludes_and_wakes_sleepers() {
        let lock = WalLock::new(());
        let n = Counter(UnsafeCell::new(0));
        std::thread::scope(|s| {
            for _ in 0..8 {
                s.spawn(|| {
                    for i in 0..50_000u32 {
                        let _g = lock.lock();
                        // SAFETY: lock の下
                        unsafe { *n.ptr() += 1 };
                        if i % 4096 == 0 {
                            // 持ったまま少し止まって、 他の書き手を寝かせる
                            std::thread::sleep(std::time::Duration::from_micros(50));
                        }
                    }
                });
            }
        });
        assert_eq!(unsafe { *n.ptr() }, 8 * 50_000);
        assert_eq!(lock_state(&lock), 0, "全員が離した後は誰も持たず、 誰も寝ていない");
    }

    /// try_lock: 持たれている間は取れず、 離した後は取れる。 寝ている書き手だけがいる (PARKED のみ) 時も取れる (CAS が返した状態を見る)。
    #[test]
    fn try_lock_takes_a_free_lock_even_with_sleepers() {
        let lock = WalLock::new(());
        let g = lock.try_lock().expect("空いている lock を取れない");
        assert!(lock.try_lock().is_none(), "持たれている lock を取った");
        assert!(lock.is_locked());
        drop(g);
        assert!(!lock.is_locked());
        // SAFETY: 試験: 寝ている書き手だけがいる状態を作る
        unsafe { lock.raw() }.state.store(PARKED, Ordering::Relaxed);
        let g = lock.try_lock().expect("PARKED だけの lock を取れない");
        assert_eq!(lock_state(&lock), LOCKED | PARKED);
        // 戻す (寝ている書き手は本当はいないので、 離す道の unpark は誰も起こさず状態を 0 にする)
        drop(g);
        assert_eq!(lock_state(&lock), 0);
    }

    fn lock_state(lock: &WalLock) -> u8 {
        // SAFETY: 状態を読むだけ
        unsafe { lock.raw() }.state.load(Ordering::Relaxed)
    }
}
