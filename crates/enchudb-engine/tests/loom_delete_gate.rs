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
//! 読み直し [1]: 削除は門を出た後に生きている印を下ろす (`live_free`、 門の外)。 書き出しは生きている印 (entities) も門を閉じている間に
//! 書き出す。 守る性質: **書き出す生きている印が 「死んでいる」 なら、 その削除の tombstone は先に書き出してある** (入っていないと、
//! 生きている印だけが届いた像で、 WAL に record の無い行の削除を誰も知らない)。 対照: 生きている印を門を閉じる前に書き出す形 (前の順)。
//!
//! 読み直し [2] の phase-fair (閉じた門で待っている削除が居る間、 新しく閉じる側は閉じる前に待つ): 守る性質 (tombstone が先) は、 閉じる
//! 側 2 本の model に待ちを入れても崩れない (待ちは yield 1 回の軽い形、 `Wait::Once`。 loop の待ちを入れると 3 本の thread の
//! interleaving が増えて 10 分で終わらなかった)。 待ちの順そのもの (**新しく閉じる側が待っている削除を見たら、 閉じる前にその削除が
//! 入っている**) は、 1 本目が閉じた後に削除と 2 本目が来る形 (`fair_model`、 待ちは loop) で見る。 対照: 待たない形で loom が順の
//! 破れを見つける (読み直し 2 [E]: 前は閉じる側 1 本の model にだけ待ちがあり、 削除が待ち始めるのはその唯一の閉じる側が閉じた後
//! = 待ちは毎回 0 で、 何も検査していなかった)。
//!
//! この model は `DeleteGate` の写し (shard 2 本、 待ちは yield)。 `delete_gate.rs` の ordering を変えた時は、 ここも同じに変えること
//! (読み直し [10]: 写しなので、 本体だけ変えても model は落ちない)。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_delete_gate --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
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
    /// 閉じた門で待っている削除の数 (`DeleteGate::wanting`、 phase-fair: 0 でない間、 新しく閉じる側は閉じる前に待つ)
    wanting: AtomicU64,
    tomb: AtomicU32,
    cell: AtomicU32,
    /// 生きている印 (1 = 生きている、 0 = 下ろした)
    live: AtomicU32,
    /// 削除が門に入った (`fair_model` の順を見る印)
    entered: AtomicBool,
}

/// 閉じる側の phase-fair の待ち (`DeleteGate::close` の、 閉じた門で待っている削除が入るまで閉じない)。
#[derive(Clone, Copy, PartialEq)]
enum Wait {
    /// 待たない
    No,
    /// 待っている削除が居なくなるまで待つ (本体と同じ)
    Loop,
    /// 待っている削除が居れば yield を 1 回だけ (閉じる側 2 本の model 用の軽い形: 待ちの数を読む所と、 その後に閉じる所の
    /// interleaving は本体と同じに出る)
    Once,
}

impl Gate {
    fn new() -> Self {
        Self {
            shards: [AtomicU64::new(0), AtomicU64::new(0)],
            wanting: AtomicU64::new(0),
            tomb: AtomicU32::new(0),
            cell: AtomicU32::new(NAME),
            live: AtomicU32::new(1),
            entered: AtomicBool::new(false),
        }
    }

    /// `DeleteGate::enter` + `remove_entity_body_of`: tombstone を置く → 入る → cell を落とす → 出る。 `drop_first` は門の前で
    /// cell を落とす (壊した形)。
    fn delete(&self, key: usize, drop_first: bool) {
        self.tomb.store(TOMB, Ordering::Relaxed);
        if drop_first {
            self.cell.store(0, Ordering::Relaxed);
        }
        let s = &self.shards[key % 2];
        let mut wanted = false;
        loop {
            let v = s.load(Ordering::Acquire);
            if v < CLOSER && s.compare_exchange_weak(v, v + 1, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                self.entered.store(true, Ordering::Release);
                break;
            }
            if v >= CLOSER && !wanted {
                wanted = true;
                self.wanting.fetch_add(1, Ordering::AcqRel);
            }
            thread::yield_now();
        }
        if wanted {
            self.wanting.fetch_sub(1, Ordering::AcqRel);
        }
        self.cell.store(0, Ordering::Relaxed);
        s.fetch_sub(1, Ordering::Release);
        // `live_free`: 門を出た後に生きている印を下ろす
        self.live.store(0, Ordering::Relaxed);
    }

    /// `write_out_body` の最後の段: 閉じる → 中が出るのを待つ → tomb.seg (tombstone を読む) → 生きている印 → 列 (cell を読む) → 開く。
    /// `tombs_first` は閉じる前に tomb.seg を書き出す (壊した形)。 `one_bit` は閉じた印を数でなく 1 bit にする (壊した形: 先に
    /// 開いた側が、 もう片方の閉じた印も消す)。 `live_first` は生きている印を閉じる前 (tomb.seg の前) に書き出す (壊した形 = 読み直し
    /// [1] の前の順)。
    ///
    /// `wait` は phase-fair の待ち ([`Wait`])。 待ちは削除を飢えさせない仕組みで、 守る性質 (tombstone が先) を崩さないこと。
    fn write_out(&self, tombs_first: bool, one_bit: bool, live_first: bool, wait: Wait) {
        let mut durable_tomb = if tombs_first { self.tomb.load(Ordering::Relaxed) } else { 0 };
        // 生きている印を書き出す時、 下ろした印の tombstone はもう書き出してあること
        let write_live = |durable_tomb: u32| {
            if self.live.load(Ordering::Relaxed) == 0 {
                assert_eq!(durable_tomb, TOMB, "下ろした生きている印を、 その tombstone より先に書き出した");
            }
        };
        if live_first {
            write_live(durable_tomb);
        }
        match wait {
            Wait::No => {}
            Wait::Loop => {
                while self.wanting.load(Ordering::Acquire) != 0 {
                    thread::yield_now();
                }
            }
            Wait::Once => {
                if self.wanting.load(Ordering::Acquire) != 0 {
                    thread::yield_now();
                }
            }
        }
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
        if !live_first {
            write_live(durable_tomb);
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

fn model(drop_first: bool, tombs_first: bool, live_first: bool) {
    loom::model(move || {
        let g = Arc::new(Gate::new());
        let d = {
            let g = g.clone();
            thread::spawn(move || g.delete(1, drop_first))
        };
        g.write_out(tombs_first, false, live_first, Wait::Loop);
        d.join().unwrap();
    });
}

/// 門の中で cell を落とし、 閉じてから tomb.seg を書き出せば、 書き出す落とした cell の tombstone は先に書き出してある。
#[test]
fn a_dropped_cell_is_never_written_out_before_its_tombstone() {
    model(false, false, false);
}

/// 閉じる側 2 本と削除 1 本。 thread が 3 本で待ちの loop が 2 つあるので、 割り込みの回数を 2 までにして枝の上限を上げる
/// (無制限だと終わらない)。 閉じる側の phase-fair の待ちは yield 1 回 (`Wait::Once`): 片方が閉じた門で削除が待ち始めてから、
/// もう片方が待ちの数を読む interleaving が出る。
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
            thread::spawn(move || g.write_out(false, one_bit, false, Wait::Once))
        };
        g.write_out(false, one_bit, false, Wait::Once);
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
    model(true, false, false);
}

/// 対照: 門を閉じる前に tomb.seg を書き出すと、 その後・閉じる前に入った削除の cell を tombstone なしで書き出す。
#[test]
#[should_panic]
fn writing_the_tombstones_before_closing_misses_a_delete() {
    model(false, true, false);
}

/// 対照 (読み直し [1]): 生きている印を門を閉じる前 (tomb.seg の前) に書き出すと、 門を出て生きている印を下ろした削除の tombstone より
/// 先に、 下ろした印を書き出す (loom が窓を見つける)。
#[test]
#[should_panic]
fn writing_the_live_bits_before_closing_misses_a_tombstone() {
    model(false, false, true);
}

/// phase-fair の順 (読み直し [2] / 読み直し 2 [E]): 1 本目 (main) が閉じた後に、 削除と 2 本目の閉じる側が来る。 2 本目は閉じる前に待ちの数を
/// 読み、 待っている削除が居たら、 2 本目が閉じた時にはその削除は門に入っている (1 本目が開いた後、 削除が 2 本目より先に入る)。
/// `wait` = `Wait::No` は対照 (2 本目が待たずに閉じ、 待っていた削除は 2 本目が開くまで入れない = 閉じる側が重なり続けると飢える)。
fn fair_model(wait: Wait) {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(3);
    b.check(move || {
        let g = Arc::new(Gate::new());
        // 1 本目が閉じる (中に削除は居ない)
        for s in &g.shards {
            s.fetch_add(CLOSER, Ordering::AcqRel);
        }
        let d = {
            let g = g.clone();
            thread::spawn(move || g.delete(1, false))
        };
        let second = {
            let g = g.clone();
            thread::spawn(move || {
                let saw = g.wanting.load(Ordering::Acquire) != 0;
                if wait == Wait::Loop {
                    while g.wanting.load(Ordering::Acquire) != 0 {
                        thread::yield_now();
                    }
                }
                for s in &g.shards {
                    s.fetch_add(CLOSER, Ordering::AcqRel);
                }
                for s in &g.shards {
                    while s.load(Ordering::Acquire) & INSIDE != 0 {
                        thread::yield_now();
                    }
                }
                if saw {
                    assert!(g.entered.load(Ordering::Acquire), "待っている削除を見たのに、 その削除より先に閉じた");
                }
                for s in &g.shards {
                    s.fetch_sub(CLOSER, Ordering::Release);
                }
            })
        };
        // 1 本目が開く
        for s in &g.shards {
            s.fetch_sub(CLOSER, Ordering::Release);
        }
        d.join().unwrap();
        second.join().unwrap();
    });
}

/// 待っている削除を見た新しく閉じる側は、 その削除が入るまで閉じない。
#[test]
fn a_new_closer_lets_a_waiting_delete_in_first() {
    fair_model(Wait::Loop);
}

/// 対照: 待たずに閉じると、 待っていた削除より先に閉じる (loom が順の破れを見つける)。
#[test]
#[should_panic(expected = "その削除より先に閉じた")]
fn a_closer_that_does_not_wait_goes_before_a_waiting_delete() {
    fair_model(Wait::No);
}
