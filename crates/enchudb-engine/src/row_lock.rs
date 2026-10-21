//! 行の書き込み境界 (#206) と、 同じ cell への並行書き込みの直列化 (#135)。
//!
//! entity の local id の下位 bit で選んだ **stripe** ごとに 1 本の seqlock を持つ (偶数 = 書き手なし、
//! 奇数 = 書いている最中)。
//!
//! - 書き手は stripe を奇数にしてから cell を書き、 書き終えたら次の偶数にする。 同じ stripe の書き手は
//!   待つ = 同じ行 (同じ cell) の書き込みは重ならない。 旧: 2 本の書き手が同じ旧 Leaf slot を読んで
//!   どちらも free し、 cell が free 済みの slot を指した (#135)
//! - 読み手は偶数の版を見てから読み、 読み終えて版が変わっていなければ採る (変わっていたら読み直す)。
//!   行を 1 回の書き込みで書き換える書き手 (schema の `commit()`) の途中の組を掴まない (#206)
//!
//! stripe は行より少ないので、 別の行が同じ stripe に当たると書き手は待ち、 読み手は読み直す (結果は
//! 正しい、 遅くなるだけ)。 版は process の中だけ (mmap に置かない = 形式の変更も、 crash で奇数のまま
//! 残る心配も無い。 書き手は `.db.lock` で 1 process)。
//!
//! 読み手が揃った版を掴めずに握る時 ([`RowLocks::read`] の終わり、 Leaf の読み (#131)) は、 書き手より先に
//! 握る: stripe ごとに 「握りたい読み手の数」 を置き、 それが 0 でない間は書き手は新しく握らない。 旧:
//! 書き手と同じ早い者勝ちの取り合いで、 書き手が大勢いると読み手が負け続けた (書き手 24 本で読み 1 回に 2 秒)。
//!
//! ただし別の stripe を握っている thread は読みのために握らない (揃うまで読み直し続ける)。 行 A を握って行 B を
//! 読む thread と、 行 B を握って行 A を読む thread が互いを待って止まるので。
//!
//! 同じ thread が握っている stripe はもう一度握れる (schema の行の書き込みの中の 1 cell ずつの書き込み、
//! 行を握ったまま読む時)。 stripe ごとに握っている thread の印を置き、 奇数で印が自分なら入れ子。
//!
//! engine の内部の表 (`_sync_ops` / `_sync_peers`) の行は、 ユーザの行と別の stripe を使う
//! ([`RowLocks::add_internal_range`])。 内部の表は WAL を書き出す裏の thread (sync の bridge) が書く。 同じ stripe を
//! 使うと、 ユーザの行を握ったまま WAL の空きを待つ書き手 (schema の `commit()` など) と、 空きを作るために
//! `_sync_ops` の行を書く bridge が互いを待ち、 書き手は待ちの上限 (5 秒) で諦めて record を落とした (#502)。 握る順は
//! ユーザの行 → 内部の表の行だけ (内部の表の行を握ったまま、 ユーザの行も WAL の空きも待たない: 内部の表は WAL に
//! 積まない)。

// `--cfg enchu_loom` の時は loom の atomic で組み、 `tests/loom_row_lock.rs` がこの code そのものを全 interleaving で走らせる (#563)
#[cfg(enchu_loom)]
use loom::sync::atomic::{fence, AtomicU32, AtomicU64, AtomicUsize, Ordering};
#[cfg(not(enchu_loom))]
use std::sync::atomic::{fence, AtomicU32, AtomicU64, AtomicUsize, Ordering};

#[cfg(not(enchu_loom))]
const STRIPES: usize = 1 << 12;
/// loom は atomic を 1 つずつ追うので、 stripe を少なくする (行 0 と 2 が同じ stripe、 1 は別)。
#[cfg(enchu_loom)]
const STRIPES: usize = 2;
/// engine の内部の表の行の stripe の数 (ユーザの行の stripe の後ろに置く)。
#[cfg(not(enchu_loom))]
const INTERNAL_STRIPES: usize = 1 << 8;
#[cfg(enchu_loom)]
const INTERNAL_STRIPES: usize = 2;
/// 内部の表の範囲を lock を取らずに引ける数。 越えた分 (表を何度も伸ばした時) は `more_internal` に置く。
const INTERNAL_RANGES: usize = 16;
/// 揃った版を掴めなかったら握って読むまでの試行回数。
#[cfg(not(enchu_loom))]
const READ_TRIES: u32 = 128;
/// loom は試行の数だけ状態が増えるので少なくする (握って読む道も通る)。
#[cfg(enchu_loom)]
const READ_TRIES: u32 = 2;

pub struct RowLocks {
    /// 1 stripe の版・握っている thread の印・先に握りたい読み手の数を 1 つに並べる (書き手 1 回で触る
    /// cache line を 1 本にする)。 ユーザの行の `STRIPES` 本の後ろに、 内部の表の行の `INTERNAL_STRIPES` 本。
    stripes: Box<[Stripe]>,
    /// 内部の表の行の範囲 `[lo, hi)` (`lo << 32 | hi`)。 足すだけ (減らさない)。
    internal: [AtomicU64; INTERNAL_RANGES],
    /// 足した範囲の数 (`INTERNAL_RANGES` を越えた分は `more_internal` にある)。
    internal_len: AtomicUsize,
    more_internal: std::sync::RwLock<Vec<(u32, u32)>>,
}

/// 32 B に揃えて cache line をまたがせない (24 B のままだと 3 つに 1 つがまたぐ)。
#[repr(align(32))]
struct Stripe {
    /// 偶数 = 書き手なし、 奇数 = 書いている最中。
    ver: AtomicU64,
    /// stripe を握っている thread の印 (0 = 誰も)。 握った thread だけが書く = 自分の印が見えたら自分が握っている。
    owner: AtomicUsize,
    /// 先に握りたい読み手の数。 0 でない間、 書き手は新しく握らない。
    urgent: AtomicU32,
}

impl Default for Stripe {
    fn default() -> Self {
        Stripe { ver: AtomicU64::new(0), owner: AtomicUsize::new(0), urgent: AtomicU32::new(0) }
    }
}

#[cfg(not(enchu_loom))]
thread_local! {
    static TOKEN: u8 = const { 0 };
}

/// thread ごとに違う 0 でない値 (thread local の番地)。
#[cfg(not(enchu_loom))]
fn me() -> usize {
    TOKEN.with(|t| t as *const u8 as usize)
}

/// loom の thread は thread local の置き場を使い回す (同時に生きている 2 本が同じ番地になる) ので、 番号を数えて配る。
#[cfg(enchu_loom)]
fn me() -> usize {
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);
    loom::thread_local! {
        static TOKEN: usize = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    TOKEN.with(|t| *t)
}

fn backoff(spins: &mut u32) {
    // loom は回り続ける thread を先に進めないので、 毎回ほかの thread に譲る
    #[cfg(enchu_loom)]
    {
        let _ = spins;
        loom::thread::yield_now();
    }
    #[cfg(not(enchu_loom))]
    if *spins < 64 {
        std::hint::spin_loop();
        *spins += 1;
    } else {
        std::thread::yield_now();
    }
}

impl Default for RowLocks {
    fn default() -> Self {
        Self::new()
    }
}

impl RowLocks {
    pub fn new() -> Self {
        Self {
            stripes: (0..STRIPES + INTERNAL_STRIPES).map(|_| Stripe::default()).collect(),
            internal: std::array::from_fn(|_| AtomicU64::new(0)),
            internal_len: AtomicUsize::new(0),
            more_internal: std::sync::RwLock::new(Vec::new()),
        }
    }

    /// engine の内部の表の行の範囲 `[lo, hi)` を足す (同じ範囲は 1 回だけ)。 その範囲の行を誰かが握る前に呼ぶこと
    /// (開いた時・表を作った時・表を伸ばして eid を払い出す前): 足す前と後で同じ行が別の stripe を使うと、 書き手どうしが
    /// 重なる。 並行に呼ばないこと (呼び手が直列にする: `&mut` か表を伸ばす lock)。
    pub fn add_internal_range(&self, lo: u32, hi: u32) {
        let n = self.internal_len.load(Ordering::Relaxed);
        let fixed = self.internal[..n.min(INTERNAL_RANGES)].iter().map(|r| unpack(r.load(Ordering::Relaxed)));
        let more = self.more_internal.read().unwrap_or_else(|p| p.into_inner());
        if lo >= hi || fixed.chain(more.iter().copied()).any(|r| r == (lo, hi)) {
            return;
        }
        drop(more);
        if n < INTERNAL_RANGES {
            self.internal[n].store(u64::from(lo) << 32 | u64::from(hi), Ordering::Relaxed);
        } else {
            self.more_internal.write().unwrap_or_else(|p| p.into_inner()).push((lo, hi));
        }
        // 範囲を置いてから数を増やす (読む側の理由は `is_internal`)
        self.internal_len.store(n + 1, Ordering::Release);
    }

    /// 行 `local` の stripe。 内部の表の行は後ろの stripe。
    #[inline]
    fn stripe(&self, local: u32) -> usize {
        if self.is_internal(local) {
            STRIPES + (local as usize & (INTERNAL_STRIPES - 1))
        } else {
            local as usize & (STRIPES - 1)
        }
    }

    #[inline]
    fn is_internal(&self, local: u32) -> bool {
        // Relaxed で足りる: 範囲の中の行を握る thread は、 その eid を受け取った時点で、 範囲を足したこと (eid を払い出す前) が
        // 見えている (happens-before)。 範囲の外の行は、 範囲が見えても見えなくても同じ stripe になる
        let n = self.internal_len.load(Ordering::Relaxed);
        if n == 0 {
            return false;
        }
        let inside = |(lo, hi): (u32, u32)| local >= lo && local < hi;
        self.internal[..n.min(INTERNAL_RANGES)].iter().any(|r| inside(unpack(r.load(Ordering::Relaxed))))
            || (n > INTERNAL_RANGES
                && self.more_internal.read().unwrap_or_else(|p| p.into_inner()).iter().any(|&r| inside(r)))
    }

    /// この thread が行 `local` の stripe を握っているか (握った証を受け取る関数の debug の確かめ用)。
    pub fn held_by_me(&self, local: u32) -> bool {
        self.held(self.stripe(local), me())
    }

    /// この thread が stripe `i` を握っているか。
    fn held(&self, i: usize, me: usize) -> bool {
        self.stripes[i].ver.load(Ordering::Relaxed) & 1 == 1 && self.stripes[i].owner.load(Ordering::Relaxed) == me
    }

    /// 行 `local` を書く間握る。 drop で離す。
    pub fn write(&self, local: u32) -> RowWrite<'_> {
        self.lock(local, false)
    }

    /// 読み手が揃った版を掴めなかった時に、 書き手より先に握る。 drop で離す。
    /// この thread が別の stripe を握っている時は握らずに None (互いを待って止まらないように)。
    pub fn read_locked(&self, local: u32) -> Option<RowWrite<'_>> {
        let i = self.stripe(local);
        let me = me();
        // 握っている stripe は数えずに探す (握りに来るのは稀。 書き手の度に thread local を数えると tie 1 回 +0.5 ns)
        if !self.held(i, me) && (0..self.stripes.len()).any(|j| self.held(j, me)) {
            return None;
        }
        Some(self.lock(local, true))
    }

    fn lock(&self, local: u32, urgent: bool) -> RowWrite<'_> {
        let i = self.stripe(local);
        let me = me();
        if self.held(i, me) {
            return RowWrite { locks: self, i: usize::MAX, _not_send: std::marker::PhantomData };
        }
        let s = &self.stripes[i].ver;
        if urgent {
            self.stripes[i].urgent.fetch_add(1, Ordering::Relaxed);
        }
        let mut spins = 0;
        loop {
            // Acquire: 前の書き手が離した後に見る 「先に握りたい読み手の数」 は、 その書き手が離す前に見た数より古くない
            let v = s.load(Ordering::Acquire);
            let wait = v & 1 == 1 || (!urgent && self.stripes[i].urgent.load(Ordering::Relaxed) != 0);
            if !wait && s.compare_exchange_weak(v, v + 1, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                break;
            }
            backoff(&mut spins);
        }
        if urgent {
            self.stripes[i].urgent.fetch_sub(1, Ordering::Relaxed);
        }
        // 奇数を見せてから cell を書く (読み手が cell の新しい値を見たなら、 後の版の読みは奇数か次の版)
        fence(Ordering::Release);
        self.stripes[i].owner.store(me, Ordering::Relaxed);
        RowWrite { locks: self, i, _not_send: std::marker::PhantomData }
    }

    /// 行 `local` を、 同じ行への書き込みと重ならずに読む。 `f` は途中の書き込みを見たら捨てられ、
    /// 呼び直される (副作用を持たないこと)。 書き手が休まず同じ stripe を書き続けて揃わない時は、 握って読む。
    pub fn read<R>(&self, local: u32, mut f: impl FnMut() -> R) -> R {
        let i = self.stripe(local);
        // 自分が書いている最中の行 = 他の書き手はいない
        if self.held(i, me()) {
            return f();
        }
        let s = &self.stripes[i].ver;
        let mut spins = 0;
        let mut tries = 0;
        loop {
            // 書き手が休まず書き続けて揃った版を掴めない: 書き手より先に握って読む (読みは今の書き手 1 本を待つだけ)
            if tries == READ_TRIES
                && let Some(_w) = self.read_locked(local)
            {
                return f();
            }
            tries += 1;
            let v = s.load(Ordering::Acquire);
            if v & 1 == 1 {
                backoff(&mut spins);
                continue;
            }
            let r = f();
            fence(Ordering::Acquire);
            if s.load(Ordering::Relaxed) == v {
                return r;
            }
            backoff(&mut spins);
        }
    }
}

fn unpack(r: u64) -> (u32, u32) {
    ((r >> 32) as u32, r as u32)
}

/// 握った thread で離すこと (握っている thread の印で入れ子を見分ける) — `Send` にしない。
pub struct RowWrite<'a> {
    locks: &'a RowLocks,
    /// 握った stripe。 `usize::MAX` = 入れ子 (外側が離す)。
    i: usize,
    _not_send: std::marker::PhantomData<*const ()>,
}

impl Drop for RowWrite<'_> {
    fn drop(&mut self) {
        if self.i != usize::MAX {
            self.locks.stripes[self.i].owner.store(0, Ordering::Relaxed);
            self.locks.stripes[self.i].ver.fetch_add(1, Ordering::Release);
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    fn wait_until(what: &str, f: impl Fn() -> bool) {
        let end = Instant::now() + Duration::from_secs(10);
        while !f() {
            assert!(Instant::now() < end, "{what} が 10 秒で成り立たない");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// 握られた stripe を書き手 8 本と読み手 1 本が待つ。 離した後に最初に握るのは読み手。
    #[test]
    fn a_reader_that_has_to_lock_goes_before_waiting_writers() {
        let locks = Arc::new(RowLocks::new());
        for round in 0..50 {
            let order = Arc::new(std::sync::Mutex::new(Vec::new()));
            let held = locks.write(7);
            let writers: Vec<_> = (0..8)
                .map(|_| {
                    let (locks, order) = (locks.clone(), order.clone());
                    std::thread::spawn(move || {
                        let _w = locks.write(7);
                        order.lock().unwrap().push("writer");
                    })
                })
                .collect();
            let reader = {
                let (locks, order) = (locks.clone(), order.clone());
                std::thread::spawn(move || {
                    let _r = locks.read_locked(7).unwrap();
                    order.lock().unwrap().push("reader");
                })
            };
            wait_until("読み手が待ちに入る", || locks.stripes[7].urgent.load(Ordering::Relaxed) != 0);
            drop(held);
            reader.join().unwrap();
            for w in writers {
                w.join().unwrap();
            }
            assert_eq!(order.lock().unwrap()[0], "reader", "round {round}: 待っていた書き手が読み手より先に握った");
        }
    }

    /// 別の stripe を握っている thread は読みのために握らない (握ると、 互いの行を待つ 2 本が止まる)。
    #[test]
    fn a_thread_holding_a_row_does_not_lock_another_row_to_read() {
        let locks = Arc::new(RowLocks::new());
        let (tx, rx) = std::sync::mpsc::channel();
        let other = {
            let locks = locks.clone();
            std::thread::spawn(move || {
                let _b = locks.write(2);
                rx.recv().unwrap(); // 相手が終わるまで離さない
            })
        };
        wait_until("別の thread が行 2 を握る", || locks.stripes[2].ver.load(Ordering::Relaxed) & 1 == 1);
        let _a = locks.write(1);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let me = {
            let locks = locks.clone();
            std::thread::spawn(move || {
                let _a = locks.write(3);
                // 行 3 を握ったまま行 2 を読むために握ろうとする: 握らずに None
                let got = locks.read_locked(2).is_some();
                // 同じ stripe の入れ子は握れる
                let nested = locks.read_locked(3).is_some();
                done_tx.send((got, nested)).unwrap();
            })
        };
        let (got, nested) = done_rx.recv_timeout(Duration::from_secs(10)).expect("行を握ったまま別の行を握りに行って止まった");
        assert!(!got);
        assert!(nested);
        tx.send(()).unwrap();
        me.join().unwrap();
        other.join().unwrap();
        // 何も握っていない thread は握れる
        drop(_a);
        assert!(locks.read_locked(2).is_some());
    }

    /// 内部の表の行は、 下位 bit が同じユーザの行を握られていても待たない (別の stripe)。 範囲の外の行は今まで通り
    /// 同じ stripe で待つ (対照)。
    #[test]
    fn internal_rows_do_not_wait_for_user_rows() {
        let locks = Arc::new(RowLocks::new());
        locks.add_internal_range(10_000, 20_000);
        let user = 7u32;
        let internal = 3 * STRIPES as u32 + 7; // 12295: 範囲の中、 下位 12 bit が `user` と同じ
        let outside = 5 * STRIPES as u32 + 7; // 20487: 範囲の外、 下位 12 bit が同じ
        assert_eq!(locks.stripe(internal), STRIPES + 7);
        assert_eq!(locks.stripe(outside), 7);
        let held = locks.write(user);
        let take = |row: u32| {
            let (locks, (tx, rx)) = (locks.clone(), std::sync::mpsc::channel());
            let t = std::thread::spawn(move || {
                let _w = locks.write(row);
                tx.send(()).unwrap();
            });
            (t, rx)
        };
        let (t, rx) = take(internal);
        rx.recv_timeout(Duration::from_secs(10)).expect("内部の表の行がユーザの行の lock を待った");
        t.join().unwrap();
        let (t, rx) = take(outside);
        assert!(rx.recv_timeout(Duration::from_millis(200)).is_err(), "範囲の外の行が、 同じ stripe の lock を待たなかった");
        drop(held);
        rx.recv_timeout(Duration::from_secs(10)).expect("離した後も範囲の外の行が握れない");
        t.join().unwrap();
    }

    /// 範囲は同じものを 1 回だけ数え、 lock を取らずに引ける数を越えても引ける。
    #[test]
    fn internal_ranges_are_counted_once_and_found_beyond_the_fixed_slots() {
        let locks = RowLocks::new();
        locks.add_internal_range(100, 200);
        locks.add_internal_range(100, 200);
        locks.add_internal_range(5, 5); // 空の範囲は足さない
        assert_eq!(locks.internal_len.load(Ordering::Relaxed), 1);
        for k in 0..2 * INTERNAL_RANGES as u32 {
            locks.add_internal_range(1000 + 10 * k, 1005 + 10 * k);
        }
        assert_eq!(locks.internal_len.load(Ordering::Relaxed), 1 + 2 * INTERNAL_RANGES);
        for k in 0..2 * INTERNAL_RANGES as u32 {
            assert!(locks.is_internal(1000 + 10 * k + 4), "{k} 番目の範囲が引けない");
            assert!(!locks.is_internal(1005 + 10 * k), "{k} 番目の範囲の外 (hi) を内部と見た");
        }
        assert!(locks.is_internal(150));
        assert!(!locks.is_internal(99));
    }
}
