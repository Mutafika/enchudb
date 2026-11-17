//! #978: 削除が落とした cell と、 その削除の tombstone (tomb.seg) の書き出しの順。
//!
//! sync する DB の削除は 「tombstone を置く」 と 「行の cell を落とす」 の 2 つ。 本体の書き出し (`Engine::write_out_body`) が列を
//! tomb.seg より先に msync すると、 落とした cell だけが disk に届いた電源断の像がある。 削除の record は WAL に無いことがある (本体は
//! WAL の書き出しの後に書き出す、 #451) ので、 開くと WAL の再生がその行の checkpoint の後ろの record で行を生かし直し、 落とした cell
//! (checkpoint の手前に書いた名前など) は戻らない = 名前の無い行が生きる。 tombstone も無いので #451 の作り直しも開く時の sweep も
//! 削除を知らず、 相手と永久に食い違った。
//!
//! そこで書き出しは、 列を msync する段の前ごとにこの門を閉じ、 tomb.seg を書き出してから列を msync して開く。 削除は tombstone を
//! 書いてから門に入り、 門の中で cell を落とす ([`DeleteGate::enter`])。 閉じている間に来た削除は開くまで待つ。 = 列の msync が拾う
//! 落とした cell の tombstone は、 必ずその msync の前に disk に届いている。 逆の像 (tombstone は届き cell は残る) は、 開く時の sweep
//! (`finish_interrupted_deletes`、 死が勝つ) が削除を完了させる。
//!
//! `PublishGate` (#419) と同じ形だが、 閉じる側が並んでよい (本体の書き出しは並んで走る、 #801。 1 本ずつにすると、 前は誰も閉じずに
//! 並んで msync していた中身を指さない列の段も書き出しが 1 本ずつになる)。 shard の語の上位 32 bit が閉じている数、 下位が中に居る削除の数。
//!
//! 読み直し [2] (phase-fair): 閉じる側が並ぶ形 (ホストが client ごとの publish で `oplog_sync` する、 Sync mode の書き手が複数、 pull の
//! たびの書き出し) では、 閉じている数が 0 に戻らない間が続き、 削除が門に入れない (飢える)。 そこで閉じた門で待ち始めた削除を数え
//! (`wanting`)、 0 でない間は新しく閉じる側が閉じる前に待つ: 今閉じている側が開くと、 待っていた削除が先に入る。 閉じる側が待つのは、
//! 待っている削除が入るまで (今閉じている側の残りの msync と、 開いた時に起こされた削除が入ってくるまで)。 閉じた後に来た削除は
//! 数えず、 開いている門にはそのまま入るので、 閉じる側も飢えない。
//!
//! 削除の待ちの上限 (読み直し 2 [G]): 待ち始めた時に閉じていた窓 1 つとは限らない。 削除が閉じた門を読んでから待ちの数を足すまでの
//! 間に、 新しく閉じる側が 0 を見て閉じうる (その窓も待つ = 窓 2 つ。 3 つ目の閉じる側は待ちの数を見て譲る)。 窓 1 つの長さは、 その段の
//! msync の他に、 中身を指す列の段では書き出しの門 (`PublishGate::close` の Mutex) を待つ間を含む (削除の門を先に閉じるので、 書き出しが
//! 重なると先に並んだ書き出しの列の msync の間も閉じたまま)。
//!
//! 待ち方 (読み直し 2 [K] の後): 待つ側は短く spin してから寝て、 待ちを解く側が起こす。 閉じた門で待つ削除は、 開いた閉じる側が
//! 起こす (`opened`)。 閉じる側の待ち (待っている削除が入る / 中の削除が出る) は、 待っている削除が入って待ちの数を 0 にした時と、
//! 閉じた門の中から最後の削除が出た時に、 その削除が起こす (`progressed`)。 閉じる側は寝る前に [`CLOSER_SPIN`] の間 spin する (待つ
//! 相手は起こされて入ってくる削除か門の中の短い仕事なので、 たいていその間に終わる = 閉じる側は寝ない)。 前は待つ側が 50 µs の眠りで
//! 見に行き、 閉じる側が 1 回譲ると削除の眠り (OS の眠りの粒度で約 130 µs) が明けて入るまで閉じなかった: 中身を指す列を 1 本ずつ
//! 書き出す段は列ごとに閉じるので、 削除が続く間の書き出しが 「書く範囲のある中身を指す列の数 × 約 130 µs」 伸びた (Mac の測り: Tag 16
//! 列の表に 150 µs ごとの削除で、 `oplog_sync` の p50 1.4 → 2.6 ms)。
//!
//! 起こし損ねない約束 (`tests/loom_delete_gate.rs` の model): 寝る側は寝床の lock (`sleep`) の下で条件を読み直し、 寝ている数を足して
//! から寝る。 起こす側は atomic を書き換えた後に寝床の lock を取り、 寝ている数が 0 でなければ起こす。 = 起こす側が lock を取る前に寝る
//! 側が lock の下で読めば書き換えが見え、 後なら寝る側はもう数えられて寝ている。 削除の速い道 (開いている門に入る / 出る) は今まで
//! 通り atomic の CAS 1 回と fetch_sub 1 回 (出る時は返った値を比べるだけ) で、 寝床の lock を取るのは、 閉じている門に当たった削除・
//! 閉じた門の中から最後に出る削除・閉じる側だけ。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

/// shard の数 (削除は slot で選ぶ)。 閉じる側は全部を閉じる。
const SHARDS: usize = 8;
/// shard の語の、 閉じている数の 1。
const CLOSER: u64 = 1 << 32;
/// shard の語の、 中に居る削除の数の bit。
const INSIDE: u64 = CLOSER - 1;
/// 閉じた門に当たった削除が、 寝る前に読み直す回数 (門は列の msync の間 = 数十 µs 以上閉じるので、 すぐ寝る)。
const DELETE_SPINS: u32 = 64;
/// 閉じる側が寝る前に spin する長さ。 待つ相手は、 起こされて入ってくる削除 (起こす遅れ) か、 門の中で cell を落としている削除
/// (短い) なので、 たいていこの間に終わる (閉じる側が寝て起こされる遅れを、 書き出しの段ごとの譲りに足さない)。
const CLOSER_SPIN: Duration = Duration::from_micros(100);

/// 64 B に揃えて shard ごとに cache line を分ける。
#[derive(Default)]
#[repr(align(64))]
struct Shard(AtomicU64);

pub struct DeleteGate {
    shards: [Shard; SHARDS],
    /// 閉じている門に当たった削除の数 (観測用)
    waits: AtomicU64,
    /// 閉じた門で今待っている削除の数 (読み直し [2]、 module doc の phase-fair)。 0 でない間、 新しく閉じる側は閉じる前に待つ
    wanting: Shard,
    /// 閉じる側が、 待っている削除を先に通すために閉じる前に待った回数 (観測用)
    yields: AtomicU64,
    /// 閉じている門に当たった削除が入れるまで待った時間の最大 (ns、 観測用)
    wait_max_ns: AtomicU64,
    /// 門を閉じていた時間の合計 (ns、 観測用。 閉じる側が並んだ区間は、 それぞれの区間を足す)
    closed_ns: AtomicU64,
    /// 寝床 (遅い道だけ): 寝ている削除と閉じる側の数。 中では atomic を読む / 数を足し引きするだけ (他の lock を取らない)
    sleep: Mutex<Sleepers>,
    /// 閉じた門で寝ている削除 (閉じる側が開いた時に起こす)
    opened: Condvar,
    /// 寝ている閉じる側 (待っている削除が入って待ちの数を 0 にした時、 閉じた門の中から最後の削除が出た時に起こす)
    progressed: Condvar,
}

/// 寝床で寝ている数 (`DeleteGate::sleep`)。 起こす側は、 0 なら起こさない (起こすたびの syscall を省く)。
#[derive(Default)]
struct Sleepers {
    deletes: u32,
    closers: u32,
    /// 寝た回数 (試験の前提: 寝る道を通ったか)
    #[cfg(test)]
    slept: (u64, u64),
}

// 読み直し [4]: 門の中で門を閉じる (門の中の削除が本体の書き出しを呼ぶ) と、 閉じる側が自分の退出を待って止まり続ける。 閉じた門に同じ
// thread が入る / 閉じたままもう一度閉じる (phase-fair: 待っている削除が入るのを待つ) も同じ。 debug build は thread ごとに 「門の中」 と
// 「閉じている」 の数を持ち、 止まる代わりに panic する。 数は門 (engine) ごと (読み直し 2 [I]: 前は thread で 1 つで、 engine A の門を
// 閉じたまま engine B で消すと、 止まらない形でも panic した)。
#[cfg(debug_assertions)]
thread_local! {
    /// (門の番地, 中に居る数, 閉じている数)。 どちらも 0 になった門は外す (`DeletePass` / `DeleteClosed` は門を借りるので、 門が
    /// drop される時は 0 = 番地の使い回しで取り違えない)
    static HERE: std::cell::RefCell<Vec<(usize, u32, u32)>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// この thread の、 門 `key` の (中に居る数, 閉じている数)。
#[cfg(debug_assertions)]
fn here(key: usize) -> (u32, u32) {
    HERE.with(|h| h.borrow().iter().find(|e| e.0 == key).map_or((0, 0), |e| (e.1, e.2)))
}

/// この thread の、 門 `key` の数を足す / 引く。
#[cfg(debug_assertions)]
fn count_here(key: usize, inside: i32, closing: i32) {
    HERE.with(|h| {
        let mut h = h.borrow_mut();
        let i = match h.iter().position(|e| e.0 == key) {
            Some(i) => i,
            None => {
                h.push((key, 0, 0));
                h.len() - 1
            }
        };
        h[i].1 = h[i].1.checked_add_signed(inside).expect("削除の門の 「中に居る」 数が負");
        h[i].2 = h[i].2.checked_add_signed(closing).expect("削除の門の 「閉じている」 数が負");
        if h[i].1 == 0 && h[i].2 == 0 {
            h.swap_remove(i);
        }
    });
}

/// この thread がどれかの削除の門の中に居るか (debug build だけ数える。 release は常に false。 試験用)。
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn inside_on_this_thread() -> bool {
    #[cfg(debug_assertions)]
    {
        HERE.with(|h| h.borrow().iter().any(|e| e.1 > 0))
    }
    #[cfg(not(debug_assertions))]
    {
        false
    }
}

impl Default for DeleteGate {
    fn default() -> Self {
        Self::new()
    }
}

impl DeleteGate {
    pub fn new() -> Self {
        Self {
            shards: Default::default(),
            waits: AtomicU64::new(0),
            wanting: Shard::default(),
            yields: AtomicU64::new(0),
            wait_max_ns: AtomicU64::new(0),
            closed_ns: AtomicU64::new(0),
            sleep: Mutex::new(Sleepers::default()),
            opened: Condvar::new(),
            progressed: Condvar::new(),
        }
    }

    /// 寝床の lock (中で panic する所は無いが、 毒は無視する)。
    fn sleepers(&self) -> MutexGuard<'_, Sleepers> {
        self.sleep.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// 寝ている閉じる側を起こす。 待ちを解く atomic (待ちの数 / 中に居る数) を書き換えた後に呼ぶ (寝床の lock の下で数を見るので、
    /// この lock より前に寝床で読み直した閉じる側は書き換えを見ていて、 後の閉じる側は数えられて寝ている)。
    fn wake_closers(&self) {
        let g = self.sleepers();
        if g.closers > 0 {
            self.progressed.notify_all();
        }
    }

    /// 閉じる側が `done` になるまで待つ: [`CLOSER_SPIN`] の間 spin し、 まだなら寝床で寝る (待っている削除が入って待ちの数を 0 にした
    /// 時 / 閉じた門の中から最後の削除が出た時に、 その削除が起こす)。
    fn closer_wait(&self, done: impl Fn() -> bool) {
        let mut spins = 0u32;
        let mut since: Option<Instant> = None;
        while !done() {
            std::hint::spin_loop();
            spins = spins.wrapping_add(1);
            if !spins.is_multiple_of(64) || since.get_or_insert_with(Instant::now).elapsed() < CLOSER_SPIN {
                continue;
            }
            let mut g = self.sleepers();
            while !done() {
                #[cfg(test)]
                {
                    g.slept.1 += 1;
                }
                g.closers += 1;
                g = self.progressed.wait(g).unwrap_or_else(PoisonError::into_inner);
                g.closers -= 1;
            }
            return;
        }
    }

    /// 削除が行 `key` の cell を落とす間持つ。 閉じていれば開くまで待つ。 tombstone はこの前に書くこと (閉じる側は中の削除が出るのを
    /// 待ってから tomb.seg を書き出す)。 drop で出る。 中に居る間に本体の書き出しを呼ばないこと (自分を待つ)。
    pub fn enter(&self, key: u32) -> DeletePass<'_> {
        #[cfg(debug_assertions)]
        debug_assert_eq!(
            here(self.debug_key()).1,
            0,
            "#978: 削除の門を閉じている thread が門に入ろうとした (自分が開くのを待って止まる)"
        );
        let shard = key as usize % SHARDS;
        let s = &self.shards[shard].0;
        let mut spins = 0;
        let mut waited: Option<Instant> = None;
        loop {
            let v = s.load(Ordering::Acquire);
            if v < CLOSER {
                // 語全体で CAS するので、 読んだ後に閉じられたら失敗して読み直す (閉じた後には入れない)
                if s.compare_exchange_weak(v, v + 1, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                    break;
                }
                continue;
            }
            if waited.is_none() {
                waited = Some(Instant::now());
                self.waits.fetch_add(1, Ordering::Relaxed);
                // 新しく閉じる側に、 先に通してもらう (入ったら外す)
                self.wanting.0.fetch_add(1, Ordering::AcqRel);
                #[cfg(feature = "crashsim")]
                crate::engine::write_out_hook::call(crate::engine::write_out_hook::Phase::DeleteGateWait);
            }
            if spins < DELETE_SPINS {
                spins += 1;
                std::hint::spin_loop();
                continue;
            }
            // 開くまで寝る。 開く側は閉じた数を下ろしてから寝床の lock を取って起こすので、 lock の下で閉じているのを見てから寝れば
            // 起こし損ねない (module doc)
            let mut g = self.sleepers();
            while s.load(Ordering::Acquire) >= CLOSER {
                #[cfg(test)]
                {
                    g.slept.0 += 1;
                }
                g.deletes += 1;
                g = self.opened.wait(g).unwrap_or_else(PoisonError::into_inner);
                g.deletes -= 1;
            }
        }
        if let Some(t) = waited {
            // 待ちの数を 0 にしたら、 待っている削除を先に通そうと待つ閉じる側を起こす
            if self.wanting.0.fetch_sub(1, Ordering::AcqRel) == 1 {
                self.wake_closers();
            }
            self.wait_max_ns.fetch_max(u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX), Ordering::Relaxed);
        }
        #[cfg(debug_assertions)]
        count_here(self.debug_key(), 1, 0);
        DeletePass {
            gate: self,
            shard,
            #[cfg(debug_assertions)]
            key: self.debug_key(),
        }
    }

    /// debug の数の鍵 (門の番地)。
    #[cfg(debug_assertions)]
    fn debug_key(&self) -> usize {
        self as *const Self as usize
    }

    /// 門を閉じ、 中に居る削除が出るのを待つ。 返った後は、 門の中で落とした cell とその前に書いた tombstone が全部見える。 閉じる側は
    /// 並んでよい (全部が開くまで削除は入れない)。 drop で開く。 閉じた門で待っている削除が居れば、 それが入るまで閉じずに待つ
    /// (phase-fair、 module doc)。 閉じている物を持ったまま呼ばないこと (待っている削除が入れない)。
    pub fn close(&self) -> DeleteClosed<'_> {
        #[cfg(debug_assertions)]
        {
            let (inside, closing) = here(self.debug_key());
            debug_assert_eq!(
                inside,
                0,
                "#978: 削除の門の中で門を閉じた (自分の退出を待って止まる。 門の中で本体の書き出しを呼ばないこと、 `LocalApply::deferring_settles`)"
            );
            debug_assert_eq!(
                closing,
                0,
                "#978: 削除の門を閉じたまま、 もう一度閉じた (待っている削除が入れず、 それを待って止まる。 段ごとに開いてから閉じること)"
            );
        }
        let start = Instant::now();
        if self.wanting.0.load(Ordering::Acquire) != 0 {
            self.yields.fetch_add(1, Ordering::Relaxed);
            // 待っている削除が入って待ちの数を 0 にすると、 その削除が起こす
            self.closer_wait(|| self.wanting.0.load(Ordering::Acquire) == 0);
        }
        for s in &self.shards {
            s.0.fetch_add(CLOSER, Ordering::AcqRel);
        }
        for s in &self.shards {
            // Acquire: 出た削除の Release と対 (その削除が書いた tombstone と落とした cell が見える)。 閉じた門の中から最後に出た削除が
            // 起こす
            self.closer_wait(|| s.0.load(Ordering::Acquire) & INSIDE == 0);
        }
        #[cfg(debug_assertions)]
        count_here(self.debug_key(), 0, 1);
        DeleteClosed { gate: self, start }
    }

    /// 閉じている門に当たった削除の数 (#978、 観測用)。
    pub fn waits(&self) -> u64 {
        self.waits.load(Ordering::Relaxed)
    }

    /// 閉じる側が、 待っている削除を先に通すために閉じる前に待った回数 (#978 の読み直し [2]、 観測用)。
    pub fn yields(&self) -> u64 {
        self.yields.load(Ordering::Relaxed)
    }

    /// 閉じている門に当たった削除が入れるまで待った時間の最大 (ns、 #978、 観測用)。
    pub fn wait_max_ns(&self) -> u64 {
        self.wait_max_ns.load(Ordering::Relaxed)
    }

    /// 門を閉じていた時間の合計 (ns、 #978、 観測用)。
    pub fn closed_ns(&self) -> u64 {
        self.closed_ns.load(Ordering::Relaxed)
    }
}

/// 門の中に居る間持つ。
pub struct DeletePass<'a> {
    gate: &'a DeleteGate,
    shard: usize,
    /// debug の数の鍵 (門の番地、 `DeleteGate::debug_key`)
    #[cfg(debug_assertions)]
    key: usize,
}

impl Drop for DeletePass<'_> {
    fn drop(&mut self) {
        let prev = self.gate.shards[self.shard].0.fetch_sub(1, Ordering::Release);
        // 閉じた門の中から最後に出た: 中が出るのを待っている閉じる側を起こす (開いている門から出る速い道は、 この比べだけ)
        if prev >= CLOSER && prev & INSIDE == 1 {
            self.gate.wake_closers();
        }
        #[cfg(debug_assertions)]
        count_here(self.key, -1, 0);
    }
}

/// 閉じている間持つ。
pub struct DeleteClosed<'a> {
    gate: &'a DeleteGate,
    start: Instant,
}

impl Drop for DeleteClosed<'_> {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        count_here(self.gate.debug_key(), 0, -1);
        for s in &self.gate.shards {
            s.0.fetch_sub(CLOSER, Ordering::Release);
        }
        let ns = u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.gate.closed_ns.fetch_add(ns, Ordering::Relaxed);
        // 閉じた門で寝ている削除を起こす (閉じた数を下ろした後に寝床の lock を取る = この lock より前に寝床で読み直した削除は開いた
        // のを見ていて、 後の削除は数えられて寝ている。 閉じる側がまだ居れば、 起きた削除は読み直してまた寝る)
        let g = self.gate.sleepers();
        if g.deletes > 0 {
            self.gate.opened.notify_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    /// 閉じる側は、 中に居る削除が出るまで返らない。
    #[test]
    fn close_waits_for_deletes_inside() {
        let gate = Arc::new(DeleteGate::new());
        let inside = gate.enter(5);
        let closed = Arc::new(AtomicBool::new(false));
        let h = {
            let (gate, closed) = (gate.clone(), closed.clone());
            std::thread::spawn(move || {
                let _c = gate.close();
                closed.store(true, Ordering::SeqCst);
            })
        };
        std::thread::sleep(Duration::from_millis(50));
        assert!(!closed.load(Ordering::SeqCst), "中に削除が居るのに閉じ終えた");
        drop(inside);
        h.join().unwrap();
        assert!(closed.load(Ordering::SeqCst));
    }

    /// 閉じている間に来た削除は、 開くまで入れない (どの shard の削除も)。
    #[test]
    fn deletes_wait_while_closed() {
        let gate = Arc::new(DeleteGate::new());
        let c = gate.close();
        let entered = Arc::new(AtomicBool::new(false));
        let hs: Vec<_> = (0..SHARDS as u32)
            .map(|key| {
                let (gate, entered) = (gate.clone(), entered.clone());
                std::thread::spawn(move || {
                    let _p = gate.enter(key);
                    entered.store(true, Ordering::SeqCst);
                })
            })
            .collect();
        std::thread::sleep(Duration::from_millis(50));
        assert!(!entered.load(Ordering::SeqCst), "閉じている間に入れた");
        assert_eq!(gate.waits(), SHARDS as u64, "閉じた門に当たった削除を数えていない");
        drop(c);
        for h in hs {
            h.join().unwrap();
        }
        assert!(entered.load(Ordering::SeqCst));
    }

    /// 読み直し [2] (phase-fair): 閉じた門で削除が待っている間、 新しく閉じる側は閉じる前に待ち、 今閉じている側が開くと待っていた削除が
    /// 先に入る (閉じる側が重なり続けても削除が飢えない)。 前は新しく閉じる側がすぐ閉じ、 1 本目が開いても削除は 2 本目が開くまで
    /// 入れなかった。
    #[test]
    fn a_waiting_delete_goes_before_a_new_closer() {
        let gate = Arc::new(DeleteGate::new());
        let first = gate.close();
        let order: Arc<std::sync::Mutex<Vec<&'static str>>> = Arc::default();
        let d = {
            let (gate, order) = (gate.clone(), order.clone());
            std::thread::spawn(move || {
                let _p = gate.enter(1);
                order.lock().unwrap().push("delete");
            })
        };
        let end = Instant::now() + Duration::from_secs(5);
        while gate.wanting.0.load(Ordering::Acquire) == 0 {
            assert!(Instant::now() < end, "前提: 削除が閉じた門で待ち始めない");
            std::thread::sleep(Duration::from_millis(1));
        }
        let b = {
            let (gate, order) = (gate.clone(), order.clone());
            std::thread::spawn(move || {
                let _c = gate.close();
                order.lock().unwrap().push("close");
            })
        };
        // 2 本目が閉じる前の待ちに入るまで待つ (読み直し 2 [C]: 決まった時間だけ眠ってから数を見ると、 2 本目の thread が `close` に
        // 入る前に見て落ちうる)
        let end = Instant::now() + Duration::from_secs(5);
        while gate.yields() == 0 {
            assert!(order.lock().unwrap().is_empty(), "待っている削除が居るのに、 新しく閉じる側が閉じた ({:?})", order.lock().unwrap());
            assert!(Instant::now() < end, "新しく閉じる側が、 待っている削除を先に通す待ちに入らない (待った回数が 0 のまま)");
            std::thread::sleep(Duration::from_millis(1));
        }
        // 待ちに入った後も、 1 本目が開くまで 2 本目は閉じない
        std::thread::sleep(Duration::from_millis(30));
        assert!(order.lock().unwrap().is_empty(), "待っている削除が居るのに、 新しく閉じる側が閉じた ({:?})", order.lock().unwrap());
        drop(first);
        d.join().unwrap();
        b.join().unwrap();
        assert_eq!(*order.lock().unwrap(), vec!["delete", "close"], "1 本目が開いた後、 待っていた削除より先に 2 本目が閉じた");
    }

    /// 読み直し 2 [I]: debug build の 「門の中」 「閉じている」 の数は門ごと。 1 本の thread が門 a を閉じたまま別の門 b に入る、 b の中に
    /// 居て a を閉じるのは止まらない (panic しない。 前は thread で 1 つの数で、 engine をまたいで panic した)。
    #[test]
    fn the_debug_marks_are_per_gate() {
        let (a, b) = (DeleteGate::new(), DeleteGate::new());
        let closed = a.close();
        drop(b.enter(1));
        drop(closed);
        let inside = b.enter(1);
        drop(a.close());
        assert_eq!(inside_on_this_thread(), cfg!(debug_assertions));
        drop(inside);
        assert!(!inside_on_this_thread());
    }

    /// 同じ門の中で閉じるのは、 debug build では止まる代わりに panic する。
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "削除の門の中で門を閉じた")]
    fn closing_the_gate_one_is_inside_panics_in_debug() {
        let g = DeleteGate::new();
        let _inside = g.enter(1);
        let _closed = g.close();
    }

    /// 読み直し 2 [K] の後: 閉じた門で待つ削除は寝て、 開いた閉じる側が起こす (50 µs の眠りで見に行かない)。 寝たこと (寝床の数) を
    /// 見てから開く。
    #[test]
    fn a_sleeping_delete_is_woken_by_the_opener() {
        let gate = Arc::new(DeleteGate::new());
        let c = gate.close();
        let h = {
            let gate = gate.clone();
            std::thread::spawn(move || drop(gate.enter(2)))
        };
        let end = Instant::now() + Duration::from_secs(5);
        while gate.sleepers().deletes == 0 {
            assert!(Instant::now() < end, "閉じた門で待つ削除が寝ない");
            std::thread::sleep(Duration::from_millis(1));
        }
        drop(c);
        let end = Instant::now() + Duration::from_secs(60);
        while !h.is_finished() {
            assert!(Instant::now() < end, "開いても寝た削除が起きない (起こし損ね。 join せずに落とす)");
            std::thread::sleep(Duration::from_millis(1));
        }
        h.join().unwrap();
        assert_eq!(gate.sleepers().deletes, 0, "寝ている削除の数が戻らない");
    }

    /// 閉じる側も寝て、 待ちを解いた削除が起こす: 中の削除が出る (閉じた門の中から最後に出る) / 待っている削除が入る (待ちの数を 0 に
    /// する)。 どちらも閉じる側が寝たこと (寝床の数) を見てから解く。
    #[test]
    fn a_sleeping_closer_is_woken_by_the_delete() {
        // 中の削除が出るのを待つ閉じる側
        let gate = Arc::new(DeleteGate::new());
        let inside = gate.enter(1);
        let h = {
            let gate = gate.clone();
            std::thread::spawn(move || drop(gate.close()))
        };
        let end = Instant::now() + Duration::from_secs(5);
        while gate.sleepers().closers == 0 {
            assert!(Instant::now() < end, "中の削除を待つ閉じる側が寝ない");
            std::thread::sleep(Duration::from_millis(1));
        }
        drop(inside);
        let end = Instant::now() + Duration::from_secs(60);
        while !h.is_finished() {
            assert!(Instant::now() < end, "中の削除が出ても閉じる側が起きない (起こし損ね。 join せずに落とす)");
            std::thread::sleep(Duration::from_millis(1));
        }
        h.join().unwrap();

        // 待っている削除が入るのを待つ閉じる側 (phase-fair)。 1 本目が閉じた門で削除が待ち、 2 本目が寝た後に 1 本目が開く
        let gate = Arc::new(DeleteGate::new());
        let first = gate.close();
        let (in_tx, in_rx) = std::sync::mpsc::channel::<()>();
        let (out_tx, out_rx) = std::sync::mpsc::channel::<()>();
        let d = {
            let gate = gate.clone();
            std::thread::spawn(move || {
                let _p = gate.enter(1);
                in_tx.send(()).unwrap();
                out_rx.recv().unwrap();
            })
        };
        let end = Instant::now() + Duration::from_secs(5);
        while gate.wanting.0.load(Ordering::SeqCst) == 0 {
            assert!(Instant::now() < end, "前提: 削除が閉じた門で待ち始めない");
            std::thread::sleep(Duration::from_millis(1));
        }
        let second = {
            let gate = gate.clone();
            std::thread::spawn(move || drop(gate.close()))
        };
        let end = Instant::now() + Duration::from_secs(5);
        while gate.sleepers().closers == 0 {
            assert!(Instant::now() < end, "待っている削除を先に通す閉じる側が寝ない");
            std::thread::sleep(Duration::from_millis(1));
        }
        drop(first);
        in_rx
            .recv_timeout(Duration::from_secs(60))
            .expect("1 本目が開いても、 待っていた削除が入らない (寝た削除を起こし損ねた。 join せずに落とす)");
        out_tx.send(()).unwrap();
        let end = Instant::now() + Duration::from_secs(60);
        while !second.is_finished() {
            assert!(Instant::now() < end, "待っていた削除が入っても閉じる側が起きない (起こし損ね。 join せずに落とす)");
            std::thread::sleep(Duration::from_millis(1));
        }
        second.join().unwrap();
        d.join().unwrap();
        assert_eq!(gate.sleepers().closers, 0, "寝ている閉じる側の数が戻らない");
    }

    /// 起こし損ねない (実物の門で): 削除 4 本と閉じる側 2 本が、 入る / 閉じるを繰り返しても止まらない。 削除は時々門の中に長く居て
    /// (閉じる側が spin を越えて寝る)、 閉じる側は時々閉じたまま待つ (削除が寝る)。 起こし損ねると、 寝た削除 / 閉じる側が起きずに
    /// 止まる (60 秒で落とす。 止まった thread は join しない)。 model (`tests/loom_delete_gate.rs`) は写しなので、 本体の起こし方も
    /// ここで回す。
    #[test]
    fn no_lost_wakeups_under_churn() {
        let gate = Arc::new(DeleteGate::new());
        let done = Arc::new(AtomicU64::new(0));
        let mut hs = Vec::new();
        for t in 0..4u32 {
            let (gate, done) = (gate.clone(), done.clone());
            hs.push(std::thread::spawn(move || {
                for i in 0..1600u32 {
                    let _p = gate.enter(t * 3 + i);
                    if i % 8 == 0 {
                        std::thread::sleep(Duration::from_micros(300));
                    }
                }
                done.fetch_add(1, Ordering::SeqCst);
            }));
        }
        for _ in 0..2 {
            let (gate, done) = (gate.clone(), done.clone());
            hs.push(std::thread::spawn(move || {
                for i in 0..800u32 {
                    let _c = gate.close();
                    if i % 2 == 0 {
                        std::thread::sleep(Duration::from_micros(200));
                    }
                }
                done.fetch_add(1, Ordering::SeqCst);
            }));
        }
        let end = Instant::now() + Duration::from_secs(60);
        while done.load(Ordering::SeqCst) < 6 {
            if Instant::now() >= end {
                let s = gate.sleepers();
                panic!(
                    "60 秒で終わらない (起こし損ねて寝たまま: 寝ている削除 {} / 閉じる側 {}、 待ちの数 {})",
                    s.deletes,
                    s.closers,
                    gate.wanting.0.load(Ordering::SeqCst)
                );
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        for h in hs {
            h.join().unwrap();
        }
        let (deletes, closers) = gate.sleepers().slept;
        assert!(deletes > 0 && closers > 0, "前提: 削除も閉じる側も寝た (寝た回数 削除 {deletes} / 閉じる側 {closers})");
        eprintln!("寝た回数: 削除 {deletes} / 閉じる側 {closers}、 譲り {}", gate.yields());
    }

    /// 閉じる側が 2 本なら、 両方が開くまで入れない (1 本目が開いても 2 本目の閉じた印は消えない)。
    #[test]
    fn closers_overlap() {
        let gate = Arc::new(DeleteGate::new());
        let first = gate.close();
        // 2 本目は別の thread で閉じる (同じ thread で閉じたまま閉じるのは debug build の `debug_assert`)
        let (closed_tx, closed_rx) = std::sync::mpsc::channel::<()>();
        let (open_tx, open_rx) = std::sync::mpsc::channel::<()>();
        let second = {
            let gate = gate.clone();
            std::thread::spawn(move || {
                let _c = gate.close();
                closed_tx.send(()).unwrap();
                open_rx.recv().unwrap();
            })
        };
        closed_rx.recv().unwrap();
        drop(first);
        let entered = Arc::new(AtomicBool::new(false));
        let h = {
            let (gate, entered) = (gate.clone(), entered.clone());
            std::thread::spawn(move || {
                let _p = gate.enter(3);
                entered.store(true, Ordering::SeqCst);
            })
        };
        std::thread::sleep(Duration::from_millis(30));
        assert!(!entered.load(Ordering::SeqCst), "2 本目が閉じている間に入れた");
        open_tx.send(()).unwrap();
        second.join().unwrap();
        let end = Instant::now() + Duration::from_secs(5);
        while !entered.load(Ordering::SeqCst) {
            assert!(Instant::now() < end, "両方が開いた後も入れない");
            std::thread::sleep(Duration::from_millis(1));
        }
        h.join().unwrap();
    }
}
