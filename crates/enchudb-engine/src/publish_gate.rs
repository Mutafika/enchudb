//! #419: 中身 (Leaf の slot / 辞書の語) を指す cell の書き込みと、 本体の書き出し (msync) の順序。
//!
//! Tag / Leaf の cell は中身の場所を指す。 書き出しは中身の segment を列より先に msync するが、 並行の書き手は
//! その間も書く。 中身の msync より **後に** 書いた中身を指す cell を、 列の msync より **前に** 書くと、 cell だけが
//! ディスクに届く。 電源断の後、 cell の指す先に中身が無い (旧い値も新しい値も読めない)。
//!
//! そこで書き出しは、 中身を指す列を msync する間だけ門を閉じる ([`PublishGate::close`]): 閉じたら中に居る書き手が
//! 出るのを待ち、 中身をもう一度 msync してから、 中身を指す列を msync する。 閉じている間に来た書き手は開くまで
//! 待つ。 書き手は cell の書き込みだけを門の中で行う ([`PublishGate::enter`])。 中身はその前に書くので (program
//! order)、 門を閉じた後の中身の msync に入る。
//!
//! 書き手の数は shard ごとの atomic で数える (並行の書き手が 1 本の cache line を取り合わない)。 閉じる側は 1 本ずつ。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

/// shard の数 (書き手は key で選ぶ)。
const SHARDS: usize = 64;
/// shard の最上位 bit = 閉じている。 残りの bit = 中に居る書き手の数。
const CLOSED: u64 = 1 << 63;

/// 64 B に揃えて shard ごとに cache line を分ける。
#[derive(Default)]
#[repr(align(64))]
struct Shard(AtomicU64);

pub struct PublishGate {
    shards: Box<[Shard]>,
    /// 閉じる側を 1 本にする (2 本目は 1 本目が開くまで待つ)
    closer: Mutex<()>,
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

impl PublishGate {
    pub fn new() -> Self {
        Self { shards: (0..SHARDS).map(|_| Shard::default()).collect(), closer: Mutex::new(()) }
    }

    /// 中身を指す cell を書く間持つ。 閉じていれば開くまで待つ。 drop で出る。
    pub fn enter(&self, key: u32) -> Pass<'_> {
        let s = &self.shards[key as usize % SHARDS].0;
        let mut spins = 0;
        loop {
            let v = s.load(Ordering::Acquire);
            // 語全体で CAS するので、 読んだ後に閉じられたら失敗して読み直す (閉じた後には入れない)
            if v & CLOSED == 0 && s.compare_exchange_weak(v, v + 1, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                return Pass { shard: s };
            }
            backoff(&mut spins);
        }
    }

    /// 門を閉じ、 中に居る書き手が出るのを待つ。 返った後は、 門の中で書いた cell (とその前に書いた中身) が
    /// 全部見える。 drop で開く。
    pub fn close(&self) -> Closed<'_> {
        let one = self.closer.lock().unwrap_or_else(|p| p.into_inner());
        for s in self.shards.iter() {
            s.0.fetch_or(CLOSED, Ordering::AcqRel);
        }
        for s in self.shards.iter() {
            let mut spins = 0;
            // Acquire: 出た書き手の Release と対 (その書き手が書いた cell と中身が見える)
            while s.0.load(Ordering::Acquire) & !CLOSED != 0 {
                backoff(&mut spins);
            }
        }
        Closed { gate: self, _one: one }
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
    _one: MutexGuard<'a, ()>,
}

impl Drop for Closed<'_> {
    fn drop(&mut self) {
        for s in self.gate.shards.iter() {
            s.0.fetch_and(!CLOSED, Ordering::Release);
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
        let inside = gate.enter(7);
        let closed = Arc::new(AtomicBool::new(false));
        let h = {
            let (gate, closed) = (gate.clone(), closed.clone());
            std::thread::spawn(move || {
                let _c = gate.close();
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
        let c = gate.close();
        let entered = Arc::new(AtomicBool::new(false));
        let h = {
            let (gate, entered) = (gate.clone(), entered.clone());
            std::thread::spawn(move || {
                let _p = gate.enter(3);
                entered.store(true, Ordering::SeqCst);
            })
        };
        std::thread::sleep(Duration::from_millis(50));
        assert!(!entered.load(Ordering::SeqCst), "閉じている間に入れた");
        drop(c);
        let end = Instant::now() + Duration::from_secs(5);
        while !entered.load(Ordering::SeqCst) {
            assert!(Instant::now() < end, "開いた後も入れない");
            std::thread::sleep(Duration::from_millis(1));
        }
        h.join().unwrap();
    }

    /// 閉じる側が 2 本でも、 2 本目は 1 本目が開くまで閉じ終えない (開いた時に 2 本目の閉じた印を消さない)。
    #[test]
    fn closers_take_turns() {
        let gate = Arc::new(PublishGate::new());
        let first = gate.close();
        let second_done = Arc::new(AtomicBool::new(false));
        let h = {
            let (gate, done) = (gate.clone(), second_done.clone());
            std::thread::spawn(move || {
                let c = gate.close();
                done.store(true, Ordering::SeqCst);
                // 2 本目が閉じている間は入れない
                let entered = Arc::new(AtomicBool::new(false));
                let w = {
                    let (gate, entered) = (gate.clone(), entered.clone());
                    std::thread::spawn(move || {
                        let _p = gate.enter(1);
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
