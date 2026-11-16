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
        Self { shards: Default::default(), waits: AtomicU64::new(0) }
    }

    /// 削除が行 `key` の cell を落とす間持つ。 閉じていれば開くまで待つ。 tombstone はこの前に書くこと (閉じる側は中の削除が出るのを
    /// 待ってから tomb.seg を書き出す)。 drop で出る。 中に居る間に本体の書き出しを呼ばないこと (自分を待つ)。
    pub fn enter(&self, key: u32) -> DeletePass<'_> {
        let s = &self.shards[key as usize % SHARDS].0;
        let mut spins = 0;
        loop {
            let v = s.load(Ordering::Acquire);
            // 語全体で CAS するので、 読んだ後に閉じられたら失敗して読み直す (閉じた後には入れない)
            if v < CLOSER && s.compare_exchange_weak(v, v + 1, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                break;
            }
            if v >= CLOSER && spins == 0 {
                self.waits.fetch_add(1, Ordering::Relaxed);
            }
            backoff(&mut spins);
        }
        DeletePass { shard: s }
    }

    /// 門を閉じ、 中に居る削除が出るのを待つ。 返った後は、 門の中で落とした cell とその前に書いた tombstone が全部見える。 閉じる側は
    /// 並んでよい (全部が開くまで削除は入れない)。 drop で開く。
    pub fn close(&self) -> DeleteClosed<'_> {
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
        DeleteClosed { gate: self }
    }

    /// 閉じている門に当たった削除の数 (#978、 観測用)。
    pub fn waits(&self) -> u64 {
        self.waits.load(Ordering::Relaxed)
    }
}

/// 門の中に居る間持つ。
pub struct DeletePass<'a> {
    shard: &'a AtomicU64,
}

impl Drop for DeletePass<'_> {
    fn drop(&mut self) {
        self.shard.fetch_sub(1, Ordering::Release);
    }
}

/// 閉じている間持つ。
pub struct DeleteClosed<'a> {
    gate: &'a DeleteGate,
}

impl Drop for DeleteClosed<'_> {
    fn drop(&mut self) {
        for s in &self.gate.shards {
            s.0.fetch_sub(CLOSER, Ordering::Release);
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

    /// 閉じる側が 2 本なら、 両方が開くまで入れない (1 本目が開いても 2 本目の閉じた印は消えない)。
    #[test]
    fn closers_overlap() {
        let gate = Arc::new(DeleteGate::new());
        let first = gate.close();
        let second = gate.close();
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
        drop(second);
        let end = Instant::now() + Duration::from_secs(5);
        while !entered.load(Ordering::SeqCst) {
            assert!(Instant::now() < end, "両方が開いた後も入れない");
            std::thread::sleep(Duration::from_millis(1));
        }
        h.join().unwrap();
    }
}
