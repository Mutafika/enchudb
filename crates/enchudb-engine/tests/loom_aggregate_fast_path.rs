//! loom model — #1064 の残り F6: eid の並びを受ける集計の速い道 (`Engine::fold_eids` / `par_fold_eids`) の、 照合せずに読む根拠の順。
//!
//! 1 つの場所 (表 T の、 世代 0 の行 = 前の住人、 cell = OLD): 表の印 (`Generations::table_has` / `mark_table`)、 場所の bit (0 = 世代を
//! 持たない DB = v14)、 場所の 2 B (下の byte = 世代、 「死んでいる」 印)、 生きている印、 列の cell。
//!
//! 書き手は code の順に、 その行を消して場所を使い回し、 次の住人の値 (NEW) を書く:
//! - 世代を持つ DB (v15): 消す道 = cell を落とす (Release) → 印 (`mark_table`: Acquire の load で見て、 無ければ AcqRel の fetch_or) →
//!   世代 + 1 と 「死んでいる」 印の RMW (`kill_generation` → `Generations::kill`) → 生きている印を下ろす RMW。 使い回す道 = 読んだ 2 B の
//!   ままなら 「死んでいる」 印を落とす CAS (`revive_slot`) → 生きている印を立てる RMW → 次の住人の値を Release で書く
//! - 世代を持たない DB (v14): 消す道は世代を上げない (cell を落とす → 生きている印を下ろす)。 使い回す道が先に v15 に移る: 場所の bit を
//!   Release で置き、 v14 の間に消した場所を移る手順の 4. で kill (印 → 世代の RMW) してから使い回す
//! - 使い回す側が別の thread の形 (`Split`): 消す側の最後の RMW (生きている印を下ろす) を Acquire で見た thread が使い回す (払い出しの
//!   空きの列 / 受信の `ensure_live_slot` の 「生きていない」 の読み)。 受信の道は値を書いてから生かす (`Receive`)
//!
//! 読み手 (集計の速い道、 code と同じ):
//! - 始め (`fast_eid_reads`): 表の印を Acquire で読み、 立っていれば照合つきの道 (この model の外、 `loom_read_app_generation`)。 場所の
//!   bit を Acquire で読み、 0 なら v14 の形 (どの local も場所)
//! - 読む: `ACQ` の道 (`get_value` / `get_value32`) は cell を Acquire、 でない道 (列の生の並び `stored_slice`) は Relaxed
//! - 終わり (`fast_eid_reads_held`): fence(Acquire) の後に、 v14 の形は場所の bit (Acquire) が 0 のままか、 v15 の形は表の印 (Acquire) が
//!   立っていないか。 だめなら照合つきの道でやり直す (速い道の答えは捨てる)
//! - 並列の版 (`Par`): worker が読み、 `ACQ` でなければ chunk の終わりに fence(Acquire)、 rayon の合流 (join) の後に呼び手が終わりの確かめ
//!
//! 守る性質: **速い道が答えを返したなら、 その読みは使い回した後の住人の値 (NEW) を読んでいない** (読んでいれば終わりの確かめが印 / 場所の
//! bit を見てやり直す)。 前の住人の値 (OLD) や落とした後 (値なし) を読むのは、 集計と並んだ削除 = どちらの答えでもよい。 新しい世代の
//! eid (local が 2^S 以上 = どの枠の外) は 1 つずつ照合つき (この model の外)。
//!
//! 対照 (should_panic): 順を外した形は loom が窓を見つける — cell を Relaxed で読んで終わりの fence も無い (`ACQ` の道で cell を Relaxed に
//! する / fence を外す。 v15 と v14 の形) / 終わりの印を fence の前に読む / 書き手が次の住人の値を Relaxed で書く / 印を値の後に立てる /
//! v14 の形で終わりに場所の bit を見ない。
//!
//! 入れていない対照: 並列の版で worker の chunk の終わりの fence を外す形。 C++ の model では、 呼び手の fence は別の thread (worker) の
//! Relaxed の読みと書き手を同期させない (fence と同期するのは同じ thread の前の読み) ので窓があるが、 loom 0.7 は join を通した後の
//! 呼び手の印の読みに古い値を選ばせず、 窓を見つけない (2026-11-23 に試した)。 code は worker の fence を残す。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_aggregate_fast_path --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, AtomicU8, AtomicU16, AtomicU32, Ordering, fence};
use loom::thread;

/// 場所の 2 B の 「死んでいる」 印 (`generation.rs` と同じ形: 下の byte = 世代)。
const DEAD: u16 = 1 << 8;
const GEN: u16 = 0xFF;
/// 前の住人の値 / 次の住人の値 (cell の 0 = 値なし)。
const OLD: u32 = 10;
const NEW: u32 = 20;
/// 世代を持つ DB の場所の bit。
const BITS: u32 = 24;

struct Slot {
    flag: AtomicU8,
    bits: AtomicU32,
    slot_gen: AtomicU16,
    live: AtomicBool,
    cell: AtomicU32,
}

/// 書き手の形。
#[derive(Clone, Copy, PartialEq)]
enum Writer {
    /// code: 1 つの thread が消して使い回す (払い出しの道: 生かしてから値)
    Code,
    /// code: 消す thread と使い回す thread が別 (払い出しの道)
    Split,
    /// code: 消す thread と、 値を書いてから生かす受信の道が別
    Receive,
    /// 対照: 次の住人の値を Relaxed で書く
    RelaxedValue,
    /// 対照: 表の印を、 次の住人の値を書いた後に立てる
    MarkAfterValue,
}

/// 読み手の形。
#[derive(Clone, Copy, PartialEq)]
enum Reader {
    /// code の `ACQ` の道: cell を Acquire、 終わりの fence → 印
    Acquired,
    /// code の `ACQ` でない道: cell を Relaxed、 終わりの fence → 印
    Fenced,
    /// code の並列の版 (`ACQ` でない道): worker が cell を Relaxed で読み chunk の終わりに fence、 join の後に呼び手が fence → 印
    Par,
    /// 対照: cell を Relaxed で読み、 終わりの fence も無い (`ACQ` の道を Relaxed にして fence を外した形 = `ACQ` でない道の fence を外した形)
    RelaxedNoFence,
    /// 対照: 終わりの印 (Acquire) を fence の前に読む (Acquire の load は前の cell の Relaxed の load を縛らない)
    FlagBeforeFence,
    /// 対照: v14 の形で終わりに場所の bit を見ない (印だけを見る)
    NoBitsRecheck,
}

impl Slot {
    fn new(bits: u32) -> Self {
        Slot {
            flag: AtomicU8::new(0),
            bits: AtomicU32::new(bits),
            slot_gen: AtomicU16::new(0),
            live: AtomicBool::new(true),
            cell: AtomicU32::new(OLD),
        }
    }

    /// `Generations::mark_table`: Acquire の load で見て、 無ければ AcqRel の fetch_or。
    fn mark_table(&self) {
        if self.flag.load(Ordering::Acquire) == 0 {
            self.flag.fetch_or(1, Ordering::AcqRel);
        }
    }

    /// 世代 + 1 と 「死んでいる」 印の RMW (`Generations::kill`)。
    fn raise(&self) {
        let _ = self.slot_gen.fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| Some(((v & GEN) + 1) | DEAD));
    }

    /// `kill_generation`: 印 → 世代の RMW。
    fn kill(&self) {
        self.mark_table();
        self.raise();
    }

    /// 消す道 (`live_free`)。 v14 の DB は世代を上げない。
    fn delete(&self, w: Writer) {
        self.cell.store(0, Ordering::Release);
        if self.bits.load(Ordering::Acquire) != 0 {
            if w == Writer::MarkAfterValue {
                // 対照: 世代だけ上げ、 印は次の住人の値の後に
                self.raise();
            } else {
                self.kill();
            }
        }
        self.live.fetch_and(false, Ordering::AcqRel);
    }

    /// 使い回す道。 v14 の DB は先に v15 に移る (場所の bit → 移る手順の 4. の kill)。
    fn reuse(&self, w: Writer) {
        if self.bits.load(Ordering::Acquire) == 0 {
            self.bits.store(BITS, Ordering::Release);
            self.kill();
        }
        let revive = || {
            let seen = self.slot_gen.load(Ordering::Acquire);
            if seen & DEAD != 0 {
                let _ = self.slot_gen.compare_exchange(seen, seen & !DEAD, Ordering::AcqRel, Ordering::Acquire);
            }
            self.live.fetch_or(true, Ordering::AcqRel);
        };
        match w {
            Writer::Receive => {
                self.cell.store(NEW, Ordering::Release);
                revive();
            }
            Writer::RelaxedValue => {
                revive();
                self.cell.store(NEW, Ordering::Relaxed);
            }
            Writer::MarkAfterValue => {
                revive();
                self.cell.store(NEW, Ordering::Release);
                self.mark_table();
            }
            Writer::Code | Writer::Split => {
                revive();
                self.cell.store(NEW, Ordering::Release);
            }
        }
    }

    /// 速い道の始め (`fast_eid_reads`): 印が立っていれば照合つき (`None`)。 `Some(v14 の形か)`。
    fn start(&self) -> Option<bool> {
        if self.flag.load(Ordering::Acquire) != 0 {
            return None;
        }
        Some(self.bits.load(Ordering::Acquire) == 0)
    }

    /// 速い道の終わり (`fast_eid_reads_held`) の、 fence の後の読み。
    fn held_after_fence(&self, any_local: bool, rd: Reader) -> bool {
        if any_local {
            // 対照 (`NoBitsRecheck`): v14 の形は表の枠を見ずに読むので、 読んだ場所は読む紐の表 T の物とは限らない。 別の表の場所の世代を
            // 上げる書きは T の印を立てない = T の印だけを見る形は、 誰も立てない印を見るのと同じ
            return rd == Reader::NoBitsRecheck || self.bits.load(Ordering::Acquire) == 0;
        }
        self.flag.load(Ordering::Acquire) == 0
    }
}

/// 速い道の集計 (eid 1 つ = 前の住人の世代 0 の eid)。 返す: 速い道の答え (`Some(読んだ cell)`)、 照合つきの道に行った / やり直した
/// なら `None`。
fn aggregate(slot: &Arc<Slot>, rd: Reader) -> Option<u32> {
    let any_local = slot.start()?;
    let v = match rd {
        Reader::Par => {
            let worker = {
                let slot = slot.clone();
                thread::spawn(move || {
                    let v = slot.cell.load(Ordering::Relaxed);
                    fence(Ordering::Acquire);
                    v
                })
            };
            worker.join().unwrap()
        }
        Reader::Acquired => slot.cell.load(Ordering::Acquire),
        _ => slot.cell.load(Ordering::Relaxed),
    };
    let held = match rd {
        Reader::RelaxedNoFence => slot.held_after_fence(any_local, rd),
        Reader::FlagBeforeFence => {
            let h = slot.held_after_fence(any_local, rd);
            fence(Ordering::Acquire);
            h
        }
        _ => {
            fence(Ordering::Acquire);
            slot.held_after_fence(any_local, rd)
        }
    };
    held.then_some(v)
}

fn check(bits: u32, w: Writer, rd: Reader) {
    loom::model(move || {
        let slot = Arc::new(Slot::new(bits));
        let writers = match w {
            Writer::Split | Writer::Receive => {
                let d = {
                    let slot = slot.clone();
                    thread::spawn(move || slot.delete(w))
                };
                let r = {
                    let slot = slot.clone();
                    // 生きていない (消す側の最後の RMW) を Acquire で見た thread だけが使い回す
                    thread::spawn(move || {
                        if !slot.live.load(Ordering::Acquire) {
                            slot.reuse(w);
                        }
                    })
                };
                vec![d, r]
            }
            _ => {
                let slot = slot.clone();
                vec![thread::spawn(move || {
                    slot.delete(w);
                    slot.reuse(w);
                })]
            }
        };
        let got = aggregate(&slot, rd);
        for h in writers {
            h.join().unwrap();
        }
        assert_ne!(got, Some(NEW), "速い道が、 使い回した後の住人の値を読んだまま答えを返した");
    });
}

#[test]
fn the_acquired_path_never_returns_the_next_residents_value() {
    check(BITS, Writer::Code, Reader::Acquired);
    check(0, Writer::Code, Reader::Acquired);
}

#[test]
fn the_fenced_path_never_returns_the_next_residents_value() {
    check(BITS, Writer::Code, Reader::Fenced);
    check(0, Writer::Code, Reader::Fenced);
}

#[test]
fn the_parallel_path_never_returns_the_next_residents_value() {
    check(BITS, Writer::Code, Reader::Par);
    check(0, Writer::Code, Reader::Par);
}

/// 使い回す側が別の thread (払い出しの道 / 値を書いてから生かす受信の道)。
#[test]
fn reuse_on_another_thread_never_reaches_the_answer() {
    for w in [Writer::Split, Writer::Receive] {
        check(BITS, w, Reader::Acquired);
        check(BITS, w, Reader::Fenced);
        check(0, w, Reader::Fenced);
    }
}

/// 対照: cell を Relaxed で読んで終わりの fence も無いと、 終わりの印の読みが書き手の印を見ない。
#[test]
#[should_panic(expected = "使い回した後の住人の値")]
fn a_relaxed_read_without_the_fence_returns_the_next_residents_value() {
    check(BITS, Writer::Code, Reader::RelaxedNoFence);
}

/// 対照: 同じく v14 の形 (終わりの場所の bit の読みが移ったことを見ない)。
#[test]
#[should_panic(expected = "使い回した後の住人の値")]
fn a_relaxed_read_without_the_fence_across_the_move_returns_the_next_residents_value() {
    check(0, Writer::Code, Reader::RelaxedNoFence);
}

/// 対照: 終わりの印を fence の前に読むと、 fence は前の印の load を縛らない。
#[test]
#[should_panic(expected = "使い回した後の住人の値")]
fn reading_the_flag_before_the_fence_returns_the_next_residents_value() {
    check(BITS, Writer::Code, Reader::FlagBeforeFence);
}

/// 対照: 書き手が次の住人の値を Relaxed で書くと、 cell の Acquire の読みも fence も書き手と同期しない。
#[test]
#[should_panic(expected = "使い回した後の住人の値")]
fn a_relaxed_store_of_the_next_value_reaches_the_answer() {
    check(BITS, Writer::RelaxedValue, Reader::Acquired);
}

/// 対照: 表の印を次の住人の値の後に立てると、 値を読んだ読みの終わりの確かめが印を見ないことがある。
#[test]
#[should_panic(expected = "使い回した後の住人の値")]
fn marking_the_table_after_the_value_reaches_the_answer() {
    check(BITS, Writer::MarkAfterValue, Reader::Acquired);
}

/// 対照: v14 の形で終わりに場所の bit を見ないと (読む紐の表の印だけ)、 移る手順の前に始めた読みが、 移って別の表の場所を使い回した後の
/// 値を返す (移る手順の 4. はその場所の表の印を立てるが、 v14 の形は表の枠を見ずに別の表の場所も読む)。
#[test]
#[should_panic(expected = "使い回した後の住人の値")]
fn the_v14_shape_without_the_bits_recheck_returns_the_next_residents_value() {
    check(0, Writer::Code, Reader::NoBitsRecheck);
}
