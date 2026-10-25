//! #419: 中身 (Leaf の slot / 辞書の語) を指す cell の書き込みと、 本体の書き出し (msync) の順序。
//!
//! Tag / Leaf の cell は中身の場所を指す。 書き出しは中身の segment を列より先に msync するが、 並行の書き手は
//! その間も書く。 中身の msync より **後に** 書いた中身を指す cell を、 列の msync より **前に** 書くと、 cell だけが
//! ディスクに届く。 電源断の後、 cell の指す先に中身が無い (旧い値も新しい値も読めない)。
//!
//! そこで書き出しは、 中身を指す列を msync する間だけその列の門を閉じる ([`PublishGate::close`]): 閉じたら中に居る
//! 書き手が出るのを待ち、 (門を通った書き手が居た列なら) 中身をもう一度 msync してから、 列を msync する。 閉じている
//! 間に来た書き手は開くまで待つ。 書き手は cell の書き込みだけを門の中で行う ([`PublishGate::enter`])。 中身はその前に
//! 書くので (program order)、 門を閉じた後の中身の msync に入る。
//!
//! #656: 門は列ごとに閉じる (前は 1 つの門を中身を指す列の全部の msync の間閉じていて、 Tag 列が 16 本の表の書き手は
//! 書き出しのたびに 16 回の msync を待った)。 列は番号で [`GROUPS`] 組に分け、 組ごとに shard を持つ (同じ組の列は
//! 一緒に閉じる = 閉じすぎるだけで、 守りは変わらない)。 門を通って cell を書いた書き手は、 列の印 ([`PublishGate::take_written`])
//! を立てる。 書き出しは印のある列だけ中身をもう一度 msync する。 中身がもうディスクにある cell (書き出しの済んだ語を
//! 指す Tag) は門を通らない (`Engine::live_set`)。
//!
//! 書き手の数は shard ごとの atomic で数える (並行の書き手が 1 本の cache line を取り合わない)。 閉じる側は 1 本ずつ。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

/// 列の組の数 (列は番号で組に分ける)。 1 組ずつ閉じられる。 番号の続いた 64 本までの列は別々の組になる (表の列は
/// 続いた番号を取る)。
const GROUPS: usize = 64;
/// 組ごとの shard の数 (書き手は key で選ぶ)。
const PER_GROUP: usize = 4;
/// shard の最上位 bit = 閉じている。 残りの bit = 中に居る書き手の数。
const CLOSED: u64 = 1 << 63;
/// 列の番号は u16 (紐の番号)。 印は列ごとに 1 bit。
const COLUMNS: usize = 1 << 16;

/// 64 B に揃えて shard ごとに cache line を分ける。
#[derive(Default)]
#[repr(align(64))]
struct Shard(AtomicU64);

pub struct PublishGate {
    shards: Box<[Shard]>,
    /// 列ごとの印: 前に書き出してから、 門を通って cell を書いた書き手が居た (中身がまだディスクに無いかもしれない cell)
    written: Box<[AtomicU64]>,
    /// 閉じる側を 1 本にする (2 本目は 1 本目が開くまで待つ)
    closer: Mutex<()>,
    /// 閉じている門に当たった書き手の数 (観測用)
    waits: AtomicU64,
    /// 門を閉じていた時間の合計 (ns、 観測用)
    closed_ns: AtomicU64,
}

impl Default for PublishGate {
    fn default() -> Self {
        Self::new()
    }
}

/// 待つ: 最初は spin、 次に yield、 長くなったら短く眠る (門は書き出しの間 = ms 単位で閉じる)。
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

#[inline]
fn group(column: u16) -> usize {
    column as usize % GROUPS
}

impl PublishGate {
    pub fn new() -> Self {
        Self {
            shards: (0..GROUPS * PER_GROUP).map(|_| Shard::default()).collect(),
            written: (0..COLUMNS / 64).map(|_| AtomicU64::new(0)).collect(),
            closer: Mutex::new(()),
            waits: AtomicU64::new(0),
            closed_ns: AtomicU64::new(0),
        }
    }

    fn group_shards(&self, g: usize) -> &[Shard] {
        &self.shards[g * PER_GROUP..(g + 1) * PER_GROUP]
    }

    /// 列 `column` の中身を指す cell を書く間持つ。 その列の門が閉じていれば開くまで待つ。 入ったら列の印を立てる。
    /// drop で出る。
    pub fn enter(&self, column: u16, key: u32) -> Pass<'_> {
        let s = &self.group_shards(group(column))[key as usize % PER_GROUP].0;
        let mut spins = 0;
        loop {
            let v = s.load(Ordering::Acquire);
            // 語全体で CAS するので、 読んだ後に閉じられたら失敗して読み直す (閉じた後には入れない)
            if v & CLOSED == 0 && s.compare_exchange_weak(v, v + 1, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                break;
            }
            if v & CLOSED != 0 && spins == 0 {
                self.waits.fetch_add(1, Ordering::Relaxed);
            }
            backoff(&mut spins);
        }
        // 印は中に居る間に立てる (閉じる側は書き手が出るのを待ってから読む = 出る時の Release と対)
        let (w, bit) = (&self.written[column as usize / 64], 1u64 << (column % 64));
        if w.load(Ordering::Relaxed) & bit == 0 {
            w.fetch_or(bit, Ordering::Relaxed);
        }
        Pass { shard: s }
    }

    /// 列 `column` の印が立っているか (閉じずに読む。 どの列を一緒に閉じるかを決める目安)。
    pub fn written(&self, column: u16) -> bool {
        self.written[column as usize / 64].load(Ordering::Acquire) & 1 << (column % 64) != 0
    }

    /// 列 `column` の印を下ろし、 立っていたかを返す。 その列の門を閉じている間に呼ぶこと (閉じる前に下ろすと、 その後に
    /// 門を通った書き手の印を消す)。
    pub fn take_written(&self, column: u16) -> bool {
        let bit = 1u64 << (column % 64);
        self.written[column as usize / 64].fetch_and(!bit, Ordering::AcqRel) & bit != 0
    }

    /// `columns` の門を閉じ、 中に居る書き手が出るのを待つ。 返った後は、 その列に門の中で書いた cell (とその前に書いた
    /// 中身) が全部見える。 drop で開く。
    pub fn close(&self, columns: impl IntoIterator<Item = u16>) -> Closed<'_> {
        let one = self.closer.lock().unwrap_or_else(|p| p.into_inner());
        let start = std::time::Instant::now();
        let mut groups = 0u64;
        for c in columns {
            groups |= 1 << group(c);
        }
        let closing = || (0..GROUPS).filter(move |g| groups & 1 << g != 0);
        for g in closing() {
            for s in self.group_shards(g) {
                s.0.fetch_or(CLOSED, Ordering::AcqRel);
            }
        }
        for g in closing() {
            for s in self.group_shards(g) {
                let mut spins = 0;
                // Acquire: 出た書き手の Release と対 (その書き手が書いた cell と中身、 立てた印が見える)
                while s.0.load(Ordering::Acquire) & !CLOSED != 0 {
                    backoff(&mut spins);
                }
            }
        }
        Closed { gate: self, groups, start, _one: one }
    }

    /// 閉じている門に当たった書き手の数 (#656、 観測用)。
    pub fn waits(&self) -> u64 {
        self.waits.load(Ordering::Relaxed)
    }

    /// 門を閉じていた時間の合計 (ns、 #656、 観測用)。 列の組を閉じた区間を足す (同じ区間に閉じた組は 1 回)。
    pub fn closed_ns(&self) -> u64 {
        self.closed_ns.load(Ordering::Relaxed)
    }
}

/// 門の中に居る間持つ。
pub struct Pass<'a> {
    shard: &'a AtomicU64,
}

impl Drop for Pass<'_> {
    fn drop(&mut self) {
        self.shard.fetch_sub(1, Ordering::Release);
    }
}

/// 閉じている間持つ。
pub struct Closed<'a> {
    gate: &'a PublishGate,
    /// 閉じた組 (bit)
    groups: u64,
    start: std::time::Instant,
    _one: MutexGuard<'a, ()>,
}

impl Drop for Closed<'_> {
    fn drop(&mut self) {
        for g in (0..GROUPS).filter(|g| self.groups & 1 << g != 0) {
            for s in self.gate.group_shards(g) {
                s.0.fetch_and(!CLOSED, Ordering::Release);
            }
        }
        if self.groups != 0 {
            let ns = u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX);
            self.gate.closed_ns.fetch_add(ns, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    /// 閉じる側は、 中に居る書き手が出るまで返らない。
    #[test]
    fn close_waits_for_writers_inside() {
        let gate = Arc::new(PublishGate::new());
        let inside = gate.enter(5, 7);
        let closed = Arc::new(AtomicBool::new(false));
        let h = {
            let (gate, closed) = (gate.clone(), closed.clone());
            std::thread::spawn(move || {
                let _c = gate.close([5]);
                closed.store(true, Ordering::SeqCst);
            })
        };
        std::thread::sleep(Duration::from_millis(50));
        assert!(!closed.load(Ordering::SeqCst), "中に書き手が居るのに閉じ終えた");
        drop(inside);
        h.join().unwrap();
        assert!(closed.load(Ordering::SeqCst));
    }

    /// 閉じている間に来た書き手は、 開くまで入れない。
    #[test]
    fn writers_wait_while_closed() {
        let gate = Arc::new(PublishGate::new());
        let c = gate.close([2]);
        let entered = Arc::new(AtomicBool::new(false));
        let h = {
            let (gate, entered) = (gate.clone(), entered.clone());
            std::thread::spawn(move || {
                let _p = gate.enter(2, 3);
                entered.store(true, Ordering::SeqCst);
            })
        };
        std::thread::sleep(Duration::from_millis(50));
        assert!(!entered.load(Ordering::SeqCst), "閉じている間に入れた");
        assert_eq!(gate.waits(), 1, "閉じた門に当たった書き手を数えていない");
        drop(c);
        let end = Instant::now() + Duration::from_secs(5);
        while !entered.load(Ordering::SeqCst) {
            assert!(Instant::now() < end, "開いた後も入れない");
            std::thread::sleep(Duration::from_millis(1));
        }
        h.join().unwrap();
        assert!(gate.closed_ns() >= 50_000_000, "閉じていた時間を数えていない ({} ns)", gate.closed_ns());
    }

    /// #656: 閉じたのと別の組の列の書き手は待たない。 同じ組の列は一緒に閉じる。
    #[test]
    fn other_columns_pass_while_one_is_closed() {
        let gate = Arc::new(PublishGate::new());
        let closed = gate.close([3]);
        // 別の組: すぐ入れる (待てばこの thread が止まる)
        drop(gate.enter(4, 0));
        drop(gate.enter(3 + GROUPS as u16 + 1, 9));
        assert_eq!(gate.waits(), 0);
        let entered = Arc::new(AtomicBool::new(false));
        let h = {
            let (gate, entered) = (gate.clone(), entered.clone());
            std::thread::spawn(move || {
                // 同じ組の別の列
                let _p = gate.enter(3 + GROUPS as u16, 1);
                entered.store(true, Ordering::SeqCst);
            })
        };
        std::thread::sleep(Duration::from_millis(30));
        assert!(!entered.load(Ordering::SeqCst), "閉じた組の列に入れた");
        drop(closed);
        h.join().unwrap();
    }

    /// 門を通った列だけ印が立ち、 下ろすと次に通るまで立たない。
    #[test]
    fn written_marks_follow_the_columns_entered() {
        let gate = PublishGate::new();
        assert!(!gate.written(70));
        drop(gate.enter(70, 1));
        drop(gate.enter(70, 2));
        assert!(gate.written(70) && !gate.written(71) && !gate.written(6), "通った列だけに印");
        let _c = gate.close([70]);
        assert!(gate.take_written(70));
        assert!(!gate.take_written(70), "下ろした印が残る");
        assert!(!gate.written(70));
    }

    /// 閉じる側が 2 本でも、 2 本目は 1 本目が開くまで閉じ終えない (開いた時に 2 本目の閉じた印を消さない)。
    #[test]
    fn closers_take_turns() {
        let gate = Arc::new(PublishGate::new());
        let first = gate.close([1]);
        let second_done = Arc::new(AtomicBool::new(false));
        let h = {
            let (gate, done) = (gate.clone(), second_done.clone());
            std::thread::spawn(move || {
                let c = gate.close([1]);
                done.store(true, Ordering::SeqCst);
                // 2 本目が閉じている間は入れない
                let entered = Arc::new(AtomicBool::new(false));
                let w = {
                    let (gate, entered) = (gate.clone(), entered.clone());
                    std::thread::spawn(move || {
                        let _p = gate.enter(1, 1);
                        entered.store(true, Ordering::SeqCst);
                    })
                };
                std::thread::sleep(Duration::from_millis(30));
                assert!(!entered.load(Ordering::SeqCst), "2 本目が閉じている間に入れた");
                drop(c);
                w.join().unwrap();
            })
        };
        std::thread::sleep(Duration::from_millis(30));
        assert!(!second_done.load(Ordering::SeqCst), "1 本目が閉じている間に 2 本目が閉じ終えた");
        drop(first);
        h.join().unwrap();
        assert!(second_done.load(Ordering::SeqCst));
    }
}
