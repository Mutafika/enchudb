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
//! 側 2 本の model に待ちを入れても崩れない。 待ちの順そのもの (**新しく閉じる側が待っている削除を見たら、 閉じる前にその削除が
//! 入っている**) は、 1 本目が閉じた後に削除と 2 本目が来る形 (`fair_model`) で見る。 対照: 待たない形で loom が順の破れを見つける。
//!
//! 読み直し 2 [K] の後の待ち方 (寝て、 待ちを解く側が起こす): 待つ側は寝床の lock の下で条件を読み直し、 寝ている数を足してから寝る
//! (`Condvar::wait`)。 起こす側は atomic を書き換えた後に寝床の lock を取り、 寝ている数が 0 でなければ起こす。 起こす所は 3 つ:
//! 閉じる側が開いた時 (閉じた門で寝ている削除)、 待っている削除が入って待ちの数を 0 にした時 (先に通すのを待つ閉じる側)、 閉じた門の
//! 中から最後の削除が出た時 (中が出るのを待つ閉じる側)。 守る性質: **起こし損ねない** = 寝た thread が起きずに全員が止まる
//! interleaving が無い (loom は全員が止まると `deadlock` で panic する)。 上の model は全部この待ち方で回る (寝る前の spin は、 寝る前の
//! 読み直し 1 回で置く)。 対照 (loom が起こし損ねを見つける): 開いた数を下ろす前に起こす形、 寝る側が lock の外で読んでから寝る形、
//! 出る削除が閉じた印を出る前に読む形、 待ちの数を 0 にした削除が起こさない形。
//!
//! この model は `DeleteGate` の写し (shard 2 本)。 `delete_gate.rs` の ordering や待ち方を変えた時は、 ここも同じに変えること
//! (読み直し [10]: 写しなので、 本体だけ変えても model は落ちない。 本体の起こし方は `delete_gate::tests::no_lost_wakeups_under_churn`
//! でも回す)。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_delete_gate --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use loom::sync::{Condvar, Mutex};
use loom::thread;
// loom の `Arc` でなく std の `Arc`: 起こし損ねの対照は loom が全員の止まり (deadlock) を見つけて panic し、 止まった thread を片付ける
// 時に loom の `Arc` の drop が片付け済みの scheduler に触ってもう一度 panic する (abort して `should_panic` で受けられない)。 共有は
// `Gate` の中の loom の atomic / Mutex / Condvar が model に載せるので、 入れ物の参照の数は model に要らない
use std::sync::Arc;

/// shard の語の、 閉じている数の 1 (`delete_gate::CLOSER`)。
const CLOSER: u64 = 1 << 32;
/// 中に居る削除の数の bit。
const INSIDE: u64 = CLOSER - 1;
/// 置く tombstone (0 = 無い)。
const TOMB: u32 = 5;
/// 落とす前の cell の値 (0 = 落とした)。
const NAME: u32 = 7;

/// 起こし方の壊した形 (対照)。
#[derive(Clone, Copy, PartialEq, Debug)]
enum Broken {
    /// 壊していない (本体と同じ)
    None,
    /// 開く側が、 閉じた数を下ろす前に起こす (起きた削除は閉じたままを見てまた寝る)
    WakeBeforeOpen,
    /// 寝る削除が、 閉じているのを寝床の lock の外で読んでから lock を取って寝る (読み直さない)
    CheckOutsideLock,
    /// 出る削除が、 閉じた印と中の数を出る (`fetch_sub`) 前に読む (その間に閉じた側を起こさない)
    ExitReadsBeforeLeaving,
    /// 待っていた削除が、 待ちの数を 0 にしても閉じる側を起こさない
    NoWakeOnWantingZero,
}

/// 寝床で寝ている数 (`delete_gate::Sleepers`)。
#[derive(Default)]
struct Sleepers {
    deletes: u32,
    closers: u32,
}

struct Gate {
    shards: [AtomicU64; 2],
    /// 閉じた門で待っている削除の数 (`DeleteGate::wanting`、 phase-fair: 0 でない間、 新しく閉じる側は閉じる前に待つ)
    wanting: AtomicU64,
    sleep: Mutex<Sleepers>,
    /// 閉じた門で寝ている削除 (開いた側が起こす)
    opened: Condvar,
    /// 寝ている閉じる側 (待ちの数を 0 にした削除 / 閉じた門の中から最後に出た削除が起こす)
    progressed: Condvar,
    broken: Broken,
    tomb: AtomicU32,
    cell: AtomicU32,
    /// 生きている印 (1 = 生きている、 0 = 下ろした)
    live: AtomicU32,
    /// 削除が門に入った (`fair_model` の順を見る印)
    entered: AtomicBool,
}

impl Gate {
    fn new(broken: Broken) -> Self {
        Self {
            shards: [AtomicU64::new(0), AtomicU64::new(0)],
            wanting: AtomicU64::new(0),
            sleep: Mutex::new(Sleepers::default()),
            opened: Condvar::new(),
            progressed: Condvar::new(),
            broken,
            tomb: AtomicU32::new(0),
            cell: AtomicU32::new(NAME),
            live: AtomicU32::new(1),
            entered: AtomicBool::new(false),
        }
    }

    /// `DeleteGate::wake_closers`: 寝床の lock を取り、 寝ている閉じる側が居れば起こす。
    fn wake_closers(&self) {
        let g = self.sleep.lock().unwrap();
        if g.closers > 0 {
            self.progressed.notify_all();
        }
    }

    /// `DeleteGate::closer_wait`: 寝る前に 1 回読み直し (spin の代わり)、 まだなら寝床の lock の下で読み直して寝る。
    fn closer_wait(&self, done: impl Fn() -> bool) {
        if done() {
            return;
        }
        let mut g = self.sleep.lock().unwrap();
        while !done() {
            g.closers += 1;
            g = self.progressed.wait(g).unwrap();
            g.closers -= 1;
        }
    }

    /// `DeleteGate::enter` + `remove_entity_body_of` + `DeletePass::drop`: tombstone を置く → 入る (閉じていれば寝て、 開いた側が
    /// 起こす) → cell を落とす → 出る (閉じた門の中から最後に出たら閉じる側を起こす)。 `drop_first` は門の前で cell を落とす (壊した形)。
    fn delete(&self, key: usize, drop_first: bool) {
        self.tomb.store(TOMB, Ordering::Relaxed);
        if drop_first {
            self.cell.store(0, Ordering::Relaxed);
        }
        let s = &self.shards[key % 2];
        let mut wanted = false;
        loop {
            let v = s.load(Ordering::Acquire);
            if v < CLOSER {
                if s.compare_exchange_weak(v, v + 1, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                    self.entered.store(true, Ordering::Release);
                    break;
                }
                continue;
            }
            if !wanted {
                wanted = true;
                self.wanting.fetch_add(1, Ordering::AcqRel);
            }
            // 寝る (本体は spin の後。 spin は上の読み直しで置く)
            if self.broken == Broken::CheckOutsideLock {
                if s.load(Ordering::Acquire) >= CLOSER {
                    let mut g = self.sleep.lock().unwrap();
                    g.deletes += 1;
                    g = self.opened.wait(g).unwrap();
                    g.deletes -= 1;
                }
                continue;
            }
            let mut g = self.sleep.lock().unwrap();
            while s.load(Ordering::Acquire) >= CLOSER {
                g.deletes += 1;
                g = self.opened.wait(g).unwrap();
                g.deletes -= 1;
            }
        }
        if wanted && self.wanting.fetch_sub(1, Ordering::AcqRel) == 1 && self.broken != Broken::NoWakeOnWantingZero {
            self.wake_closers();
        }
        self.cell.store(0, Ordering::Relaxed);
        // 出る
        let prev = if self.broken == Broken::ExitReadsBeforeLeaving {
            let v = s.load(Ordering::Acquire);
            s.fetch_sub(1, Ordering::Release);
            v
        } else {
            s.fetch_sub(1, Ordering::Release)
        };
        if prev >= CLOSER && prev & INSIDE == 1 {
            self.wake_closers();
        }
        // `live_free`: 門を出た後に生きている印を下ろす
        self.live.store(0, Ordering::Relaxed);
    }

    /// `DeleteGate::close` の閉じるまで: (phase-fair なら) 待っている削除が入るまで待つ → 閉じる → 中が出るのを待つ。 `one_bit` は閉じた
    /// 印を数でなく 1 bit にする (壊した形: 先に開いた側が、 もう片方の閉じた印も消す)。
    fn close(&self, fair: bool, one_bit: bool) {
        if fair && self.wanting.load(Ordering::Acquire) != 0 {
            self.closer_wait(|| self.wanting.load(Ordering::Acquire) == 0);
        }
        for s in &self.shards {
            if one_bit {
                s.fetch_or(CLOSER, Ordering::AcqRel);
            } else {
                s.fetch_add(CLOSER, Ordering::AcqRel);
            }
        }
        for s in &self.shards {
            self.closer_wait(|| s.load(Ordering::Acquire) & INSIDE == 0);
        }
    }

    /// `DeleteClosed::drop`: 開く → 寝床の lock を取り、 閉じた門で寝ている削除が居れば起こす。
    fn open(&self, one_bit: bool) {
        if self.broken == Broken::WakeBeforeOpen {
            self.wake_deletes();
        }
        for s in &self.shards {
            if one_bit {
                s.fetch_and(!CLOSER, Ordering::Release);
            } else {
                s.fetch_sub(CLOSER, Ordering::Release);
            }
        }
        if self.broken != Broken::WakeBeforeOpen {
            self.wake_deletes();
        }
    }

    fn wake_deletes(&self) {
        let g = self.sleep.lock().unwrap();
        if g.deletes > 0 {
            self.opened.notify_all();
        }
    }

    /// `write_out_body` の最後の段: 閉じる → 中が出るのを待つ → tomb.seg (tombstone を読む) → 生きている印 → 列 (cell を読む) → 開く。
    /// `tombs_first` は閉じる前に tomb.seg を書き出す (壊した形)。 `live_first` は生きている印を閉じる前 (tomb.seg の前) に書き出す
    /// (壊した形 = 読み直し [1] の前の順)。 `fair` は phase-fair の待ち。 待ちは削除を飢えさせない仕組みで、 守る性質 (tombstone が先) を
    /// 崩さないこと。
    fn write_out(&self, tombs_first: bool, one_bit: bool, live_first: bool, fair: bool) {
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
        self.close(fair, one_bit);
        if !tombs_first {
            durable_tomb = self.tomb.load(Ordering::Relaxed);
        }
        if !live_first {
            write_live(durable_tomb);
        }
        if self.cell.load(Ordering::Relaxed) == 0 {
            assert_eq!(durable_tomb, TOMB, "落とした cell を、 その tombstone より先に書き出した");
        }
        self.open(one_bit);
    }
}

fn model(drop_first: bool, tombs_first: bool, live_first: bool) {
    loom::model(move || {
        let g = Arc::new(Gate::new(Broken::None));
        let d = {
            let g = g.clone();
            thread::spawn(move || g.delete(1, drop_first))
        };
        g.write_out(tombs_first, false, live_first, true);
        d.join().unwrap();
    });
}

/// 門の中で cell を落とし、 閉じてから tomb.seg を書き出せば、 書き出す落とした cell の tombstone は先に書き出してある (寝る削除・寝る
/// 閉じる側も起こし損ねない)。
#[test]
fn a_dropped_cell_is_never_written_out_before_its_tombstone() {
    model(false, false, false);
}

/// 閉じる側 2 本と削除 1 本。 thread が 3 本なので、 割り込みの回数を 2 までにして枝の上限を上げる (無制限だと終わらない)。 閉じる側は
/// 2 本とも phase-fair の待ちをする (片方が閉じた門で削除が待ち始めてから、 もう片方が待ちの数を読んで寝る interleaving も出る)。
fn overlapping_model(one_bit: bool) {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(2);
    b.max_branches = 100_000;
    b.check(move || {
        let g = Arc::new(Gate::new(Broken::None));
        let d = {
            let g = g.clone();
            thread::spawn(move || g.delete(0, false))
        };
        let other = {
            let g = g.clone();
            thread::spawn(move || g.write_out(false, one_bit, false, true))
        };
        g.write_out(false, one_bit, false, true);
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
#[should_panic(expected = "tombstone より先に書き出した")]
fn a_one_bit_close_lets_a_delete_in_while_the_other_closer_writes_out() {
    overlapping_model(true);
}

/// 対照: 門の前で cell を落とすと、 tombstone を書き出す前の列の書き出しが落とした cell を拾う (loom が窓を見つける)。
#[test]
#[should_panic(expected = "落とした cell を、 その tombstone より先に書き出した")]
fn dropping_before_the_gate_writes_out_a_cell_without_its_tombstone() {
    model(true, false, false);
}

/// 対照: 門を閉じる前に tomb.seg を書き出すと、 その後・閉じる前に入った削除の cell を tombstone なしで書き出す。
#[test]
#[should_panic(expected = "tombstone より先に書き出した")]
fn writing_the_tombstones_before_closing_misses_a_delete() {
    model(false, true, false);
}

/// 対照 (読み直し [1]): 生きている印を門を閉じる前 (tomb.seg の前) に書き出すと、 門を出て生きている印を下ろした削除の tombstone より
/// 先に、 下ろした印を書き出す (loom が窓を見つける)。
#[test]
#[should_panic(expected = "下ろした生きている印を、 その tombstone より先に書き出した")]
fn writing_the_live_bits_before_closing_misses_a_tombstone() {
    model(false, false, true);
}

/// phase-fair の順 (読み直し [2] / 読み直し 2 [E]): 1 本目 (main) が閉じた後に、 削除と 2 本目の閉じる側が来る。 2 本目は閉じる前に待ちの数を
/// 読み、 待っている削除が居たら、 2 本目が閉じた時にはその削除は門に入っている (1 本目が開いた後、 削除が 2 本目より先に入る)。
/// `fair` = false は対照 (2 本目が待たずに閉じ、 待っていた削除は 2 本目が開くまで入れない = 閉じる側が重なり続けると飢える)。
/// `broken` は起こし方の壊した形 (対照、 loom が起こし損ねを見つける)。 削除・2 本目が寝て、 1 本目の開く・削除が入る・削除が出るが
/// それぞれを起こす形 (起こす所 3 つを全部通る)。
fn fair_model(fair: bool, broken: Broken) {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(3);
    b.check(move || {
        let g = Arc::new(Gate::new(broken));
        // 1 本目が閉じる (中に削除は居ない)
        g.close(true, false);
        let d = {
            let g = g.clone();
            thread::spawn(move || g.delete(1, false))
        };
        let second = {
            let g = g.clone();
            thread::spawn(move || {
                let saw = g.wanting.load(Ordering::Acquire) != 0;
                g.close(fair, false);
                if saw {
                    assert!(g.entered.load(Ordering::Acquire), "待っている削除を見たのに、 その削除より先に閉じた");
                }
                g.open(false);
            })
        };
        // 1 本目が開く
        g.open(false);
        d.join().unwrap();
        second.join().unwrap();
    });
}

/// 待っている削除を見た新しく閉じる側は、 その削除が入るまで閉じない (寝た削除・閉じる側は起こされる)。
#[test]
fn a_new_closer_lets_a_waiting_delete_in_first() {
    fair_model(true, Broken::None);
}

/// 対照: 待たずに閉じると、 待っていた削除より先に閉じる (loom が順の破れを見つける)。
#[test]
#[should_panic(expected = "その削除より先に閉じた")]
fn a_closer_that_does_not_wait_goes_before_a_waiting_delete() {
    fair_model(false, Broken::None);
}

/// 対照 (起こし損ね): 開く側が閉じた数を下ろす前に起こすと、 起きた削除は閉じたままを見てまた寝て、 誰も起こさない。
#[test]
#[should_panic(expected = "deadlock")]
fn waking_before_opening_loses_a_sleeping_delete() {
    fair_model(true, Broken::WakeBeforeOpen);
}

/// 対照 (起こし損ね): 寝る削除が lock の外で閉じているのを読んでから寝ると、 読んだ後・寝る前に開いた側の起こしを逃す。
#[test]
#[should_panic(expected = "deadlock")]
fn checking_outside_the_lock_loses_a_wakeup() {
    fair_model(true, Broken::CheckOutsideLock);
}

/// 対照 (起こし損ね): 出る削除が閉じた印を出る前に読むと、 読んだ後・出る前に閉じた側は中が出るのを待って寝たまま。
#[test]
#[should_panic(expected = "deadlock")]
fn reading_before_leaving_loses_a_sleeping_closer() {
    model_broken(Broken::ExitReadsBeforeLeaving);
}

/// 対照 (起こし損ね): 待ちの数を 0 にした削除が起こさないと、 先に通すのを待って寝た閉じる側は寝たまま。
#[test]
#[should_panic(expected = "deadlock")]
fn not_waking_on_wanting_zero_loses_a_sleeping_closer() {
    fair_model(true, Broken::NoWakeOnWantingZero);
}

/// 削除 1 本と閉じる側 1 本の model を、 起こし方を壊して回す。
fn model_broken(broken: Broken) {
    loom::model(move || {
        let g = Arc::new(Gate::new(broken));
        let d = {
            let g = g.clone();
            thread::spawn(move || g.delete(1, false))
        };
        g.write_out(false, false, false, true);
        d.join().unwrap();
    });
}
