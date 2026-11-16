//! loom model — #978 の削除の門 (`crate::delete_gate::DeleteGate`) の約束を全 interleaving で検証する。
//!
//! 削除は tombstone を置いてから門に入り、 門の中で行の cell を落として出る (`Engine::remove_entity_body_of`)。 本体の書き出しは
//! 列を msync する段の前に門を閉じ (shard ごとに閉じている数を 1 足す)、 中に居る削除が出るのを待ってから tomb.seg を書き出し、
//! 列を msync して開く (`Engine::close_deletes`)。 守る性質: **列の msync が拾う落とした cell の tombstone は、 その前の tomb.seg の
//! 書き出しに入っている** (入っていないと、 落とした cell だけが disk に届いた電源断の像で、 再生が行を名前の無い行として生かし
//! 直す)。 閉じる側は並んでよい (片方が開いても、 もう片方が閉じている間は入れない)。
//!
//! 書き出し (msync) は 「tombstone / cell を読む」 で置く。 tombstone と cell は削除が門の外 / 中で書き、 書き出しは並んで読む
//! (msync は書きかけの page も拾う) ので atomic の Relaxed。 順序は門の atomic だけが作る。
//!
//! 対照 (loom が窓を見つける): 門の前で cell を落とす形、 門を閉じる前に tomb.seg を書き出す形。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_delete_gate --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use loom::sync::Arc;
use loom::thread;

/// shard の語の、 閉じている数の 1 (`delete_gate::CLOSER`)。
const CLOSER: u64 = 1 << 32;
/// 中に居る削除の数の bit。
const INSIDE: u64 = CLOSER - 1;
/// 置く tombstone (0 = 無い)。
const TOMB: u32 = 5;
/// 落とす前の cell の値 (0 = 落とした)。
const NAME: u32 = 7;

struct Gate {
    shards: [AtomicU64; 2],
    tomb: AtomicU32,
    cell: AtomicU32,
}

impl Gate {
    fn new() -> Self {
        Self { shards: [AtomicU64::new(0), AtomicU64::new(0)], tomb: AtomicU32::new(0), cell: AtomicU32::new(NAME) }
    }

    /// `DeleteGate::enter` + `remove_entity_body_of`: tombstone を置く → 入る → cell を落とす → 出る。 `drop_first` は門の前で
    /// cell を落とす (壊した形)。
    fn delete(&self, key: usize, drop_first: bool) {
        self.tomb.store(TOMB, Ordering::Relaxed);
        if drop_first {
            self.cell.store(0, Ordering::Relaxed);
        }
        let s = &self.shards[key % 2];
        loop {
            let v = s.load(Ordering::Acquire);
            if v < CLOSER && s.compare_exchange_weak(v, v + 1, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                break;
            }
            thread::yield_now();
        }
        self.cell.store(0, Ordering::Relaxed);
        s.fetch_sub(1, Ordering::Release);
    }

    /// `write_out_body` の列の段 1 つ: 閉じる → 中が出るのを待つ → tomb.seg (tombstone を読む) → 列 (cell を読む) → 開く。
    /// `tombs_first` は閉じる前に tomb.seg を書き出す (壊した形)。 `one_bit` は閉じた印を数でなく 1 bit にする (壊した形: 先に
    /// 開いた側が、 もう片方の閉じた印も消す)。
    fn write_out(&self, tombs_first: bool, one_bit: bool) {
        let mut durable_tomb = if tombs_first { self.tomb.load(Ordering::Relaxed) } else { 0 };
        for s in &self.shards {
            if one_bit {
                s.fetch_or(CLOSER, Ordering::AcqRel);
            } else {
                s.fetch_add(CLOSER, Ordering::AcqRel);
            }
        }
        for s in &self.shards {
            while s.load(Ordering::Acquire) & INSIDE != 0 {
                thread::yield_now();
            }
        }
        if !tombs_first {
            durable_tomb = self.tomb.load(Ordering::Relaxed);
        }
        if self.cell.load(Ordering::Relaxed) == 0 {
            assert_eq!(durable_tomb, TOMB, "落とした cell を、 その tombstone より先に書き出した");
        }
        for s in &self.shards {
            if one_bit {
                s.fetch_and(!CLOSER, Ordering::Release);
            } else {
                s.fetch_sub(CLOSER, Ordering::Release);
            }
        }
    }
}

fn model(drop_first: bool, tombs_first: bool) {
    loom::model(move || {
        let g = Arc::new(Gate::new());
        let d = {
            let g = g.clone();
            thread::spawn(move || g.delete(1, drop_first))
        };
        g.write_out(tombs_first, false);
        d.join().unwrap();
    });
}

/// 門の中で cell を落とし、 閉じてから tomb.seg を書き出せば、 書き出す落とした cell の tombstone は先に書き出してある。
#[test]
fn a_dropped_cell_is_never_written_out_before_its_tombstone() {
    model(false, false);
}

/// 閉じる側 2 本と削除 1 本。 thread が 3 本で待ちの loop が 2 つあるので、 割り込みの回数を 2 までにして枝の上限を上げる
/// (無制限だと終わらない)。
fn overlapping_model(one_bit: bool) {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(2);
    b.max_branches = 100_000;
    b.check(move || {
        let g = Arc::new(Gate::new());
        let d = {
            let g = g.clone();
            thread::spawn(move || g.delete(0, false))
        };
        let other = {
            let g = g.clone();
            thread::spawn(move || g.write_out(false, one_bit))
        };
        g.write_out(false, one_bit);
        d.join().unwrap();
        other.join().unwrap();
    });
}

/// 閉じる側が 2 本並んでも同じ (先に開いた方が、 もう片方の閉じている間に削除を通さない)。
#[test]
fn overlapping_closers_keep_the_order() {
    overlapping_model(false);
}

/// 対照: 閉じた印が 1 bit だと、 先に開いた側がもう片方の閉じた印も消し、 閉じている間に削除が入る。
#[test]
#[should_panic]
fn a_one_bit_close_lets_a_delete_in_while_the_other_closer_writes_out() {
    overlapping_model(true);
}

/// 対照: 門の前で cell を落とすと、 tombstone を書き出す前の列の書き出しが落とした cell を拾う (loom が窓を見つける)。
#[test]
#[should_panic]
fn dropping_before_the_gate_writes_out_a_cell_without_its_tombstone() {
    model(true, false);
}

/// 対照: 門を閉じる前に tomb.seg を書き出すと、 その後・閉じる前に入った削除の cell を tombstone なしで書き出す。
#[test]
#[should_panic]
fn writing_the_tombstones_before_closing_misses_a_delete() {
    model(false, true);
}
