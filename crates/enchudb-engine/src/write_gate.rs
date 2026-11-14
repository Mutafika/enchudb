//! 円柱を組んでいない紐の書き手の門 (2026-10-11)。
//!
//! 紐の書き手は `HimoStore` の `write_lock` で 1 本ずつ並んでいた。 同じ紐の書き手を増やすと、 合計の書き込みが遅く
//! なった (w1、 宣言も購読も無し: 書き手 1 本 16.4 M 回/秒 → 8 本 4.7 M 回/秒。 書き手の時間の 82% が lock の取り合い)。
//! 円柱を組んでいない紐の書き込みは Column の cell に書くだけ (cell は atomic、 count は fetch_max) なので、 書き手どうしを
//! 並べる理由は無い (同じ cell の書き手は engine の行の lock で並ぶ)。 並べていたのは、 `write_lock` を barrier に使う
//! 3 つの約束のため:
//!
//! - 円柱の遅延構築 (#270、 `HimoStore::ensure_cylinder_built`): 組む前に書いた cell は走査に読まれ、 組んだ後に書く
//!   書き手は自分で円柱に入れる
//! - 並びの索引の構築 (`Engine::order_ready`): 同じ形
//! - 購読の登録の barrier (`crate::live`): barrier より前に書いた cell は初期候補の走査に読まれ、 後に書く書き手は
//!   購読を見る
//!
//! そこで円柱を組んでいない紐の書き手は、 `write_lock` の代わりに門を通る ([`WriteGate::try_enter`])。 3 つの約束の側は
//! `write_lock` を取ってから門を閉じ、 中に居る書き手が出るのを待つ ([`WriteGate::close`])。 閉じた後に来た書き手は
//! `write_lock` の道に回るので、 閉じる側が lock を離すまで待つ (今までと同じ受け渡し)。 円柱を組んだら門は閉じたまま
//! (組んだ円柱の bucket は書き手 1 本の約束なので、 値ごとの鍵の下で手入れする、 [`crate::value_locks`])。 barrier と並びの
//! 索引の構築は、 終わったら開ける ([`WriteGate::open`])。 開けた後に入った書き手は開けた RMW と同期する (開ける前に
//! 置いた購読や索引が見える)。
//!
//! 書き手は shard の数を足してから閉じているかを見る。 閉じる側は印を立ててから数が 0 になるのを待つ。 どちらも同じ
//! word への RMW なので、 word ごとの変更の順でどちらかが必ず先になる (SeqCst の fence は要らない): 書き手が先なら
//! 閉じる側はその書き手が出るまで待ち、 閉じる側が先なら書き手は印を見て `write_lock` の道に回る。
//! [`crate::publish_gate`] と同じ形で、 違いは閉じている時に待たずに `write_lock` の道へ回ることと、 shard を書き手の
//! thread ごとに選ぶこと (書き手どうしが 1 本の cache line を取り合わない)。 loom model:
//! `tests/loom_lazy_cylinder_build.rs` / `tests/loom_order_index_build.rs` / `tests/loom_live_subscribe.rs`。

use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

/// shard の数 (書き手の thread ごとに 1 つ。 これより多い thread は shard を分け合う)。
const SHARDS: usize = 16;
/// shard の最上位 bit = 閉じている。 残りの bit = 中に居る書き手の数。
const CLOSED: u32 = 1 << 31;

/// 128 B に揃えて shard ごとに cache line を分ける。
#[derive(Default)]
#[repr(align(128))]
struct Shard(AtomicU32);

pub(crate) struct WriteGate {
    shards: Box<[Shard]>,
}

/// この thread が使う shard (全部の門で同じ番号)。
#[inline]
fn shard_index() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static IDX: usize = NEXT.fetch_add(1, Ordering::Relaxed) % SHARDS;
    }
    IDX.with(|i| *i)
}

impl Default for WriteGate {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteGate {
    /// 開いた門。
    pub(crate) fn new() -> Self {
        Self { shards: (0..SHARDS).map(|_| Shard::default()).collect() }
    }

    /// 開いていれば入り、 出るまで持つ印を返す。 閉じていれば None (呼び手は `write_lock` の道へ)。
    #[inline]
    pub(crate) fn try_enter(&self) -> Option<Pass<'_>> {
        let s = &self.shards[shard_index()].0;
        // Acquire: 開けた側の Release と対 (開ける前に置いた購読や索引が見える)
        if s.fetch_add(1, Ordering::Acquire) & CLOSED == 0 {
            Some(Pass { shard: s })
        } else {
            // 中では何も書いていない
            s.fetch_sub(1, Ordering::Relaxed);
            None
        }
    }

    /// 門を閉じ、 中に居る書き手が出るのを待つ。 `write_lock` の下で呼ぶ (閉じる側は 1 本)。 返った後は、 門の中で書いた
    /// cell が全部見え、 新しく来る書き手は `write_lock` の道に回る。
    pub(crate) fn close(&self) {
        for s in self.shards.iter() {
            s.0.fetch_or(CLOSED, Ordering::AcqRel);
        }
        for s in self.shards.iter() {
            let mut spins = 0u32;
            // Acquire: 出た書き手の Release と対 (その書き手が書いた cell が見える)
            while s.0.load(Ordering::Acquire) & !CLOSED != 0 {
                if spins < 64 {
                    std::hint::spin_loop();
                    spins += 1;
                } else {
                    std::thread::yield_now();
                }
            }
        }
    }

    /// 門を開ける。 `write_lock` の下で、 円柱を組んでいない時だけ呼ぶ。
    pub(crate) fn open(&self) {
        for s in self.shards.iter() {
            // Release: この後に入った書き手が、 開ける前に置いたもの (購読・索引) を見る
            s.0.fetch_and(!CLOSED, Ordering::Release);
        }
    }

    /// 閉じているか (test 用)。
    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        self.shards.iter().all(|s| s.0.load(Ordering::Relaxed) & CLOSED != 0)
    }
}

/// 門の中に居る間持つ。 drop で出る。
pub(crate) struct Pass<'a> {
    shard: &'a AtomicU32,
}

impl Drop for Pass<'_> {
    #[inline]
    fn drop(&mut self) {
        // Release: 閉じる側の Acquire と対 (門の中で書いた cell が見える)
        self.shard.fetch_sub(1, Ordering::Release);
    }
}

/// #850 の後 (#873 のレビュー): WAL に積んで本体に当てる自分の書き込み (engine の `LocalApply`) が、 積む前に通る門。
///
/// 書き手は WAL に積んでから cell に当て終えるまで印 (`OpLog::applying`) を持ち、 書き出しは checkpoint をその手前で切る。 その間に
/// 書き手が長く待つと (同じ紐の円柱を組む・並びの索引を作る・列を数える側が `write_lock` を持って列をなめる間)、 WAL のその区画を
/// 畳めず、 満杯になると全部の書き手が空きを待って、 待ち切れずに record を落とす / 書き込みを断る。 そこで、 紐を長く止める側
/// ([`IntentGate::hold`]) は `write_lock` を取る **前に** この門を閉じ、 中に居る書き手 (積む前から当て終えるまで) が出るのを待つ。
/// 閉じている間に来た書き手は、 積む前に (印を持たずに) 止める側が終わるのを待つ ([`IntentGate::enter`])。 中に居る書き手は、
/// 当てる間に止める側を待たない (止める側はまだ `write_lock` も門も鍵も取っていない)。
///
/// 止める側は閉じてから中が空くのを待つので、 中の書き手が WAL の空きを待っていれば、 その間待つ (積む前の書き手は印を持たない
/// ので、 空きは作られる)。 短い barrier (購読の登録の `write_barrier`) は閉じない (今まで通り、 当てている書き手が少し待つ)。
pub(crate) struct IntentGate {
    gate: WriteGate,
    /// 止める側が持つ間持つ (止める側は 1 本ずつ、 閉じられた書き手はこれを待つ)
    closer: parking_lot::Mutex<()>,
}

impl Default for IntentGate {
    fn default() -> Self {
        Self::new()
    }
}

impl IntentGate {
    pub(crate) fn new() -> Self {
        Self { gate: WriteGate::new(), closer: parking_lot::Mutex::new(()) }
    }

    /// 入る。 閉じていれば、 止める側が終わるのを待ってから入り直す。
    #[inline]
    pub(crate) fn enter(&self) -> IntentPass<'_> {
        loop {
            if let Some(p) = self.try_enter() {
                return p;
            }
            self.wait();
        }
    }

    /// 開いていれば入る (待たない)。 いくつもの門に入る書き手 (delete) は、 1 つでも閉じていれば入った分を出てから待つ
    /// ([`Self::wait`]、 入ったまま待つと、 止める側が 2 本の紐を順に止める間に互いに待つ)。
    #[inline]
    pub(crate) fn try_enter(&self) -> Option<IntentPass<'_>> {
        let pass = self.gate.try_enter()?;
        #[cfg(debug_assertions)]
        held::push(self);
        Some(IntentPass { _pass: pass, #[cfg(debug_assertions)] gate: self })
    }

    /// 止める側が終わるのを待つ (止める側は閉じている間ずっと `closer` を持つ)。
    pub(crate) fn wait(&self) {
        drop(self.closer.lock());
    }

    /// 閉じて、 中の書き手が出るのを待つ。 drop で開ける。 `write_lock` を取る前に呼ぶ (中の書き手が当てる間に `write_lock` を
    /// 待つことがある)。 この門に入ったままの thread が呼ぶと止まる (debug build は panic)。
    pub(crate) fn hold(&self) -> IntentHold<'_> {
        #[cfg(debug_assertions)]
        assert!(!held::contains(self), "IntentGate::hold: この thread はこの紐の書き込みの途中 (自分を待って止まる)");
        let closer = self.closer.lock();
        self.gate.close();
        IntentHold { gate: self, _closer: closer }
    }
}

/// [`IntentGate`] の中に居る間持つ。
pub(crate) struct IntentPass<'a> {
    _pass: Pass<'a>,
    #[cfg(debug_assertions)]
    gate: &'a IntentGate,
}

#[cfg(debug_assertions)]
impl Drop for IntentPass<'_> {
    fn drop(&mut self) {
        held::remove(self.gate);
    }
}

/// [`IntentGate::hold`] の間持つ。 drop で開ける (その後 `closer` を放す = 待っている書き手が入る)。
pub(crate) struct IntentHold<'a> {
    gate: &'a IntentGate,
    _closer: parking_lot::MutexGuard<'a, ()>,
}

impl Drop for IntentHold<'_> {
    fn drop(&mut self) {
        self.gate.gate.open();
    }
}

/// debug build: この thread が入っている門 (止める側を自分で待つ形を panic にする)。
#[cfg(debug_assertions)]
mod held {
    use super::IntentGate;
    use std::cell::RefCell;

    thread_local! {
        static HELD: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
    }

    pub(super) fn push(g: &IntentGate) {
        let _ = HELD.try_with(|h| h.borrow_mut().push(g as *const _ as usize));
    }

    pub(super) fn remove(g: &IntentGate) {
        let _ = HELD.try_with(|h| {
            let mut h = h.borrow_mut();
            if let Some(i) = h.iter().rposition(|&p| p == g as *const _ as usize) {
                h.swap_remove(i);
            }
        });
    }

    pub(super) fn contains(g: &IntentGate) -> bool {
        HELD.try_with(|h| h.borrow().contains(&(g as *const _ as usize))).unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::time::Duration;

    /// 閉じる側は、 中に居る書き手が出るまで返らない。
    #[test]
    fn close_waits_for_writers_inside() {
        let gate = Arc::new(WriteGate::new());
        let inside = gate.try_enter().expect("開いた門に入れない");
        let closed = Arc::new(AtomicBool::new(false));
        let h = {
            let (gate, closed) = (gate.clone(), closed.clone());
            std::thread::spawn(move || {
                gate.close();
                closed.store(true, Ordering::SeqCst);
            })
        };
        std::thread::sleep(Duration::from_millis(50));
        assert!(!closed.load(Ordering::SeqCst), "中に書き手が居るのに閉じ終えた");
        drop(inside);
        h.join().unwrap();
        assert!(closed.load(Ordering::SeqCst));
    }

    /// 閉じている間は入れず (待たずに None)、 開けたらまた入れる。 入れなかった書き手は数に残らない。
    #[test]
    fn closed_gate_turns_writers_away_until_opened() {
        let gate = WriteGate::new();
        assert!(!gate.is_closed());
        gate.close();
        assert!(gate.is_closed());
        assert!(gate.try_enter().is_none(), "閉じた門に入れた");
        // 断られた書き手が数を残していれば、 次の close が待ち続ける
        gate.close();
        gate.open();
        assert!(!gate.is_closed());
        let p = gate.try_enter();
        assert!(p.is_some(), "開けた門に入れない");
    }

    /// 止める側 (`hold`) は中の書き手が出るまで返らず、 持っている間に来た書き手は待ち、 放すと入る。
    #[test]
    fn intent_hold_waits_for_writers_inside_and_holds_new_ones() {
        let gate = Arc::new(IntentGate::new());
        let inside = gate.enter();
        let held = Arc::new(AtomicBool::new(false));
        let h = {
            let (gate, held) = (gate.clone(), held.clone());
            std::thread::spawn(move || {
                let hold = gate.hold();
                held.store(true, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(100));
                drop(hold);
            })
        };
        std::thread::sleep(Duration::from_millis(50));
        assert!(!held.load(Ordering::SeqCst), "中に書き手が居るのに止め始めた");
        drop(inside);
        while !held.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        assert!(gate.try_enter().is_none(), "止めている間に入れた");
        let t = std::time::Instant::now();
        let p = gate.enter();
        assert!(t.elapsed() >= Duration::from_millis(30), "止めている間に待たずに入った");
        drop(p);
        h.join().unwrap();
    }
}
