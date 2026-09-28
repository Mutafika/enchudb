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

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};

const STRIPES: usize = 1 << 12;
/// 揃った版を掴めなかったら握って読むまでの試行回数。
const READ_TRIES: u32 = 128;

pub struct RowLocks {
    /// 1 stripe の版・握っている thread の印・先に握りたい読み手の数を 1 つに並べる (書き手 1 回で触る
    /// cache line を 1 本にする)。
    stripes: Box<[Stripe]>,
}

/// 32 B に揃えて cache line をまたがせない (24 B のままだと 3 つに 1 つがまたぐ)。
#[derive(Default)]
#[repr(align(32))]
struct Stripe {
    /// 偶数 = 書き手なし、 奇数 = 書いている最中。
    ver: AtomicU64,
    /// stripe を握っている thread の印 (0 = 誰も)。 握った thread だけが書く = 自分の印が見えたら自分が握っている。
    owner: AtomicUsize,
    /// 先に握りたい読み手の数。 0 でない間、 書き手は新しく握らない。
    urgent: AtomicU32,
}

thread_local! {
    static TOKEN: u8 = const { 0 };
}

/// thread ごとに違う 0 でない値 (thread local の番地)。
fn me() -> usize {
    TOKEN.with(|t| t as *const u8 as usize)
}

fn backoff(spins: &mut u32) {
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
            stripes: (0..STRIPES).map(|_| Stripe::default()).collect(),
        }
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
        let i = local as usize & (STRIPES - 1);
        let me = me();
        // 握っている stripe は数えずに探す (握りに来るのは稀。 書き手の度に thread local を数えると tie 1 回 +0.5 ns)
        if !self.held(i, me) && (0..STRIPES).any(|j| self.held(j, me)) {
            return None;
        }
        Some(self.lock(local, true))
    }

    fn lock(&self, local: u32, urgent: bool) -> RowWrite<'_> {
        let i = local as usize & (STRIPES - 1);
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
        std::sync::atomic::fence(Ordering::Release);
        self.stripes[i].owner.store(me, Ordering::Relaxed);
        RowWrite { locks: self, i, _not_send: std::marker::PhantomData }
    }

    /// 行 `local` を、 同じ行への書き込みと重ならずに読む。 `f` は途中の書き込みを見たら捨てられ、
    /// 呼び直される (副作用を持たないこと)。 書き手が休まず同じ stripe を書き続けて揃わない時は、 握って読む。
    pub fn read<R>(&self, local: u32, mut f: impl FnMut() -> R) -> R {
        let i = local as usize & (STRIPES - 1);
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
            std::sync::atomic::fence(Ordering::Acquire);
            if s.load(Ordering::Relaxed) == v {
                return r;
            }
            backoff(&mut spins);
        }
    }
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

#[cfg(test)]
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
}
