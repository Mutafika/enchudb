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
//! 同じ thread が握っている stripe はもう一度握れる (schema の行の書き込みの中の 1 cell ずつの書き込み、
//! 行を握ったまま読む時)。 stripe ごとに握っている thread の印を置き、 奇数で印が自分なら入れ子。

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

const STRIPES: usize = 1 << 12;
/// 揃った版を掴めなかったら握って読むまでの試行回数。
const READ_TRIES: u32 = 128;

pub struct RowLocks {
    stripes: Box<[AtomicU64]>,
    /// stripe を握っている thread の印 (0 = 誰も)。 握った thread だけが書く = 自分の印が見えたら自分が握っている。
    owners: Box<[AtomicUsize]>,
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
            stripes: (0..STRIPES).map(|_| AtomicU64::new(0)).collect(),
            owners: (0..STRIPES).map(|_| AtomicUsize::new(0)).collect(),
        }
    }

    /// この thread が stripe `i` を握っているか。
    fn held(&self, i: usize, me: usize) -> bool {
        self.stripes[i].load(Ordering::Relaxed) & 1 == 1 && self.owners[i].load(Ordering::Relaxed) == me
    }

    /// 行 `local` を書く間握る。 drop で離す。
    pub fn write(&self, local: u32) -> RowWrite<'_> {
        let i = local as usize & (STRIPES - 1);
        let me = me();
        if self.held(i, me) {
            return RowWrite { locks: self, i: usize::MAX, _not_send: std::marker::PhantomData };
        }
        let s = &self.stripes[i];
        let mut spins = 0;
        loop {
            let v = s.load(Ordering::Relaxed);
            if v & 1 == 0 && s.compare_exchange_weak(v, v + 1, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                break;
            }
            backoff(&mut spins);
        }
        // 奇数を見せてから cell を書く (読み手が cell の新しい値を見たなら、 後の版の読みは奇数か次の版)
        std::sync::atomic::fence(Ordering::Release);
        self.owners[i].store(me, Ordering::Relaxed);
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
        let s = &self.stripes[i];
        let mut spins = 0;
        for _ in 0..READ_TRIES {
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
        // 書き手が休まず書き続けて揃った版を掴めない: 書き手と同じく握って読む (書き手は待つ、 読みは必ず終わる)
        let _w = self.write(local);
        f()
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
            self.locks.owners[self.i].store(0, Ordering::Relaxed);
            self.locks.stripes[self.i].fetch_add(1, Ordering::Release);
        }
    }
}
