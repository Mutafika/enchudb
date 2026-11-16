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
//! 待っている削除が入るまで (今閉じている側の残りの msync と、 削除の待ちの眠り 1 回分)。 閉じた後に来た削除は数えず、 開いている
//! 門にはそのまま入るので、 閉じる側も飢えない。

use std::sync::atomic::{AtomicU64, Ordering};

/// shard の数 (削除は slot で選ぶ)。 閉じる側は全部を閉じる。
const SHARDS: usize = 8;
/// shard の語の、 閉じている数の 1。
const CLOSER: u64 = 1 << 32;
/// shard の語の、 中に居る削除の数の bit。
const INSIDE: u64 = CLOSER - 1;

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
}

// 読み直し [4]: 門の中で門を閉じる (門の中の削除が本体の書き出しを呼ぶ) と、 閉じる側が自分の退出を待って止まり続ける。 閉じた門に同じ
// thread が入る / 閉じたままもう一度閉じる (phase-fair: 待っている削除が入るのを待つ) も同じ。 debug build は thread ごとに 「門の中」 と
// 「閉じている」 の数を持ち、 止まる代わりに panic する。
#[cfg(debug_assertions)]
thread_local! {
    static INSIDE_HERE: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    static CLOSING_HERE: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// この thread が削除の門の中に居るか (debug build だけ数える。 release は常に false。 試験用)。
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn inside_on_this_thread() -> bool {
    #[cfg(debug_assertions)]
    {
        INSIDE_HERE.with(|c| c.get() > 0)
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

/// 待つ: 最初は spin、 次に yield、 長くなったら短く眠る (門は列の msync の間 = ms 単位で閉じる)。
fn backoff(spins: &mut u32) {
    if *spins < 64 {
        std::hint::spin_loop();
    } else if *spins < 128 {
        std::thread::yield_now();
    } else {
        std::thread::sleep(std::time::Duration::from_micros(50));
    }
    *spins = spins.saturating_add(1);
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
        }
    }

    /// 削除が行 `key` の cell を落とす間持つ。 閉じていれば開くまで待つ。 tombstone はこの前に書くこと (閉じる側は中の削除が出るのを
    /// 待ってから tomb.seg を書き出す)。 drop で出る。 中に居る間に本体の書き出しを呼ばないこと (自分を待つ)。
    pub fn enter(&self, key: u32) -> DeletePass<'_> {
        #[cfg(debug_assertions)]
        CLOSING_HERE.with(|c| {
            debug_assert_eq!(c.get(), 0, "#978: 削除の門を閉じている thread が門に入ろうとした (自分が開くのを待って止まる)")
        });
        let s = &self.shards[key as usize % SHARDS].0;
        let mut spins = 0;
        let mut waited: Option<std::time::Instant> = None;
        loop {
            let v = s.load(Ordering::Acquire);
            // 語全体で CAS するので、 読んだ後に閉じられたら失敗して読み直す (閉じた後には入れない)
            if v < CLOSER && s.compare_exchange_weak(v, v + 1, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                break;
            }
            if v >= CLOSER && waited.is_none() {
                waited = Some(std::time::Instant::now());
                self.waits.fetch_add(1, Ordering::Relaxed);
                // 新しく閉じる側に、 先に通してもらう (入ったら外す)
                self.wanting.0.fetch_add(1, Ordering::AcqRel);
                #[cfg(feature = "crashsim")]
                crate::engine::write_out_hook::call(crate::engine::write_out_hook::Phase::DeleteGateWait);
            }
            backoff(&mut spins);
        }
        if let Some(t) = waited {
            self.wanting.0.fetch_sub(1, Ordering::AcqRel);
            self.wait_max_ns.fetch_max(u64::try_from(t.elapsed().as_nanos()).unwrap_or(u64::MAX), Ordering::Relaxed);
        }
        #[cfg(debug_assertions)]
        INSIDE_HERE.with(|c| c.set(c.get() + 1));
        DeletePass { shard: s }
    }

    /// 門を閉じ、 中に居る削除が出るのを待つ。 返った後は、 門の中で落とした cell とその前に書いた tombstone が全部見える。 閉じる側は
    /// 並んでよい (全部が開くまで削除は入れない)。 drop で開く。 閉じた門で待っている削除が居れば、 それが入るまで閉じずに待つ
    /// (phase-fair、 module doc)。 閉じている物を持ったまま呼ばないこと (待っている削除が入れない)。
    pub fn close(&self) -> DeleteClosed<'_> {
        #[cfg(debug_assertions)]
        INSIDE_HERE.with(|c| {
            debug_assert_eq!(
                c.get(),
                0,
                "#978: 削除の門の中で門を閉じた (自分の退出を待って止まる。 門の中で本体の書き出しを呼ばないこと、 `LocalApply::deferring_settles`)"
            )
        });
        #[cfg(debug_assertions)]
        CLOSING_HERE.with(|c| {
            debug_assert_eq!(
                c.get(),
                0,
                "#978: 削除の門を閉じたまま、 もう一度閉じた (待っている削除が入れず、 それを待って止まる。 段ごとに開いてから閉じること)"
            )
        });
        let start = std::time::Instant::now();
        let mut spins = 0;
        while self.wanting.0.load(Ordering::Acquire) != 0 {
            if spins == 0 {
                self.yields.fetch_add(1, Ordering::Relaxed);
            }
            backoff(&mut spins);
        }
        for s in &self.shards {
            s.0.fetch_add(CLOSER, Ordering::AcqRel);
        }
        for s in &self.shards {
            let mut spins = 0;
            // Acquire: 出た削除の Release と対 (その削除が書いた tombstone と落とした cell が見える)
            while s.0.load(Ordering::Acquire) & INSIDE != 0 {
                backoff(&mut spins);
            }
        }
        #[cfg(debug_assertions)]
        CLOSING_HERE.with(|c| c.set(c.get() + 1));
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
    shard: &'a AtomicU64,
}

impl Drop for DeletePass<'_> {
    fn drop(&mut self) {
        self.shard.fetch_sub(1, Ordering::Release);
        #[cfg(debug_assertions)]
        INSIDE_HERE.with(|c| c.set(c.get() - 1));
    }
}

/// 閉じている間持つ。
pub struct DeleteClosed<'a> {
    gate: &'a DeleteGate,
    start: std::time::Instant,
}

impl Drop for DeleteClosed<'_> {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        CLOSING_HERE.with(|c| c.set(c.get() - 1));
        for s in &self.gate.shards {
            s.0.fetch_sub(CLOSER, Ordering::Release);
        }
        let ns = u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.gate.closed_ns.fetch_add(ns, Ordering::Relaxed);
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
        std::thread::sleep(Duration::from_millis(30));
        assert!(order.lock().unwrap().is_empty(), "待っている削除が居るのに、 新しく閉じる側が閉じた ({:?})", order.lock().unwrap());
        assert!(gate.yields() >= 1, "新しく閉じる側が待った回数を数えていない");
        drop(first);
        d.join().unwrap();
        b.join().unwrap();
        assert_eq!(*order.lock().unwrap(), vec!["delete", "close"], "1 本目が開いた後、 待っていた削除より先に 2 本目が閉じた");
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
