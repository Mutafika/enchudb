//! loom model — #1015 / #1020: lock を取らない読みの入口 (`Engine::read_app_in`) と出口 (`GenReader::eids`) の照合の順。
//!
//! 1 つの場所: page の印、 場所の 2 B (下の byte = 世代、 「死んでいる」 印)、 列の cell、 場所の bit (0 = 世代を持たない DB)。
//!
//! 書き手は code の順に、 その場所の行 (前の住人、 cell = OLD) を消して場所を使い回し、 次の住人の値を書く:
//! 1. cell を落とす (`remove_entity_body_of` → `live_remove`、 Release)
//! 2. (世代を持たない DB なら) v15 に移る: 場所の bit を置く (`Generations::start`、 Release。 v14 の間に消した場所は移る手順の 4. で
//!    下の kill を受ける)
//! 3. kill: page の印が無ければ立てる (`cell_for_write` → `mark_page`)、 世代 + 1 と 「死んでいる」 印を 1 回の RMW (`Generations::kill`)
//! 4. 使い回す: 読んだ 2 B のままなら 「死んでいる」 印を落とす CAS (`revive_slot` → `Generations::revive_from`)
//! 5. 次の住人の値 (NEW) を cell に Release で書く
//! 6. 次の住人の eid を **同期の無い渡し方** (Relaxed の store) で読み手に渡す
//!
//! 読み手の形 (code と同じ):
//! - 読む前の照合: 場所の bit (Relaxed) → 世代 0 の eid は印と 2 B を Relaxed、 世代 ≠ 0 の eid は Acquire (`GenReader::current`)。
//!   場所の bit を 0 と読んだ世代 ≠ 0 の eid は、 世代込みの local を場所として読む = 列の数の外 (`value_at` が None)
//! - 読む (`f`): get の道は cell を Acquire、 fence のある道 (`get_text_owned`) は Relaxed の後に fence(Acquire)
//! - 読んだ後の照合 (全部 `f` と fence の後): 世代を持つ DB は印と 2 B を Relaxed で読み直す (`ReadAppPost::Gen`)。 世代を持たない DB
//!   は場所の bit を Relaxed で読み直し、 0 でなければ fence の後に全部を Acquire で引き直す (`ReadAppPost::Recheck` → `app_slot_still`)
//!
//! 守る性質:
//! - **古い eid の読みは、 使い回した後の住人の値 (NEW) を返さない** (読んだ後の照合と、 cell の Acquire / fence の順)
//! - **新しい世代の eid の読みは、 前の住人の値 (OLD) を返さない** (#1020 の読み直し A: 読む前の照合の世代 ≠ 0 の Acquire が、
//!   2 B を書いた kill / revive の RMW と同期し、 その前に落とした cell を見せる)
//! - **出口が新しい世代を付けて返した eid の後の読みは、 前の住人の値を見ない** (出口は 2 B を Relaxed で読み、 返す前に fence(Acquire))
//!
//! 対照 (should_panic): 順を外した形は loom が窓を見つける — cell を Relaxed で読んで fence も無い / 読んだ後の照合を外す / 世代を
//! 持たない道で読んだ後に場所の bit を見ない / 世代 ≠ 0 の eid の読む前の照合を Relaxed にする (#1020 の前の形) / 出口の fence を外す。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_read_app_generation --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicU8, AtomicU16, AtomicU32, Ordering, fence};
use loom::thread;

/// 場所の 2 B の 「死んでいる」 印 (`generation.rs` と同じ形: 下の byte = 世代)。
const DEAD: u16 = 1 << 8;
const GEN: u16 = 0xFF;
/// 前の住人の値 / 次の住人の値 (cell の 0 = 値なし)。
const OLD: u32 = 10;
const NEW: u32 = 20;

struct Slot {
    mark: AtomicU8,
    slot_gen: AtomicU16,
    cell: AtomicU32,
    bits: AtomicU32,
    /// 次の住人の eid を渡したか (Relaxed)
    handoff: AtomicU32,
}

/// 列の cell の読み (`f`)。
#[derive(Clone, Copy, PartialEq)]
enum Read {
    /// get の道 (`read_app_acquired`): cell を Acquire
    Acquired,
    /// fence のある道 (`read_app`): cell を Relaxed、 `f` の後に fence(Acquire)
    Fenced,
    /// 対照: cell を Relaxed、 fence なし
    RelaxedNoFence,
}

/// 読んだ後の照合。
#[derive(Clone, Copy, PartialEq)]
enum Post {
    /// code の形
    Full,
    /// 対照: 読んだ後の照合を全部外す
    Skip,
    /// 対照: 世代を持たない道 (`Recheck`) で場所の bit を読み直さない (移ったことを見落とす)
    NoBitsRecheck,
}

#[derive(Clone, Copy)]
struct Reader {
    /// 世代 ≠ 0 の eid の読む前の照合の順 (code: Acquire。 対照: Relaxed)
    pre: Ordering,
    how: Read,
    post: Post,
}

const CODE: Reader = Reader { pre: Ordering::Acquire, how: Read::Acquired, post: Post::Full };
const CODE_FENCED: Reader = Reader { pre: Ordering::Acquire, how: Read::Fenced, post: Post::Full };

/// 始めの形: 場所の bit・page の印・場所の世代 (前の住人の eid の世代)。
#[derive(Clone, Copy)]
struct Start {
    bits: u32,
    mark: u8,
    slot_gen: u16,
}

/// 世代を持つ DB、 印の無い page (その page で最初の使い回し)。
const FRESH: Start = Start { bits: 24, mark: 0, slot_gen: 0 };
/// 世代を持つ DB、 印のある page の 1 回使い回した場所 (書き手は印を立て直さない)。
const MARKED: Start = Start { bits: 24, mark: 1, slot_gen: 1 };
/// 世代を持たない DB (使い回しが v15 に移る)。
const V14: Start = Start { bits: 0, mark: 0, slot_gen: 0 };

impl Slot {
    fn new(s: Start) -> Self {
        Slot {
            mark: AtomicU8::new(s.mark),
            slot_gen: AtomicU16::new(s.slot_gen),
            cell: AtomicU32::new(OLD),
            bits: AtomicU32::new(s.bits),
            handoff: AtomicU32::new(0),
        }
    }

    /// `GenReader::current` / `current_relaxed`: 印の無い page は世代 0 (2 B を読まない)。
    fn current(&self, order: Ordering) -> u16 {
        if self.mark.load(order) == 0 {
            return 0;
        }
        self.slot_gen.load(order) & GEN
    }

    /// `Engine::read_app_in` (eid の世代 `g`)。
    fn read(&self, g: u16, rd: Reader) -> Option<u32> {
        let gen_path = self.bits.load(Ordering::Relaxed) != 0;
        if gen_path {
            let cur = if g == 0 { self.current(Ordering::Relaxed) } else { self.current(rd.pre) };
            if cur != g {
                return None;
            }
        } else if g != 0 {
            // 世代込みの local を場所として読む = 列の数の外
            return None;
        }
        let v = match rd.how {
            Read::Acquired => self.cell.load(Ordering::Acquire),
            Read::Fenced | Read::RelaxedNoFence => self.cell.load(Ordering::Relaxed),
        };
        if rd.how == Read::Fenced {
            fence(Ordering::Acquire);
        }
        match rd.post {
            Post::Skip => {}
            _ if gen_path => {
                // ReadAppPost::Gen
                if self.current(Ordering::Relaxed) != g {
                    return None;
                }
            }
            post => {
                // ReadAppPost::Recheck
                let now = if post == Post::NoBitsRecheck { 0 } else { self.bits.load(Ordering::Relaxed) };
                if now != 0 {
                    // `app_slot_still`: fence → `app_slot` (場所の bit・印・2 B を Acquire で)
                    fence(Ordering::Acquire);
                    if self.bits.load(Ordering::Acquire) != 0 && self.current(Ordering::Acquire) != g {
                        return None;
                    }
                }
            }
        }
        (v != 0).then_some(v)
    }

    /// 出口 (`GenReader::eids`): 場所の bit (Acquire、 `Generations::reader`)、 page の印 (Acquire)、 2 B (Relaxed) で今の世代を付ける。
    /// `fence_after`: 2 B を読んだら返す前に fence(Acquire) (code)。
    fn exit(&self, fence_after: bool) -> u16 {
        if self.bits.load(Ordering::Acquire) == 0 || self.mark.load(Ordering::Acquire) == 0 {
            return 0;
        }
        let g = self.slot_gen.load(Ordering::Relaxed) & GEN;
        if fence_after {
            fence(Ordering::Acquire);
        }
        g
    }

    /// 書き手 (code の順、 module doc の 1.〜6.)。
    fn delete_and_reuse(&self) {
        self.cell.store(0, Ordering::Release);
        if self.bits.load(Ordering::Relaxed) == 0 {
            self.bits.store(24, Ordering::Release);
        }
        if self.mark.load(Ordering::Acquire) == 0 {
            self.mark.fetch_or(1, Ordering::AcqRel);
        }
        let _ = self.slot_gen.fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| Some(((v & GEN) + 1) | DEAD));
        let seen = self.slot_gen.load(Ordering::Acquire);
        if seen & DEAD != 0 {
            let _ = self.slot_gen.compare_exchange(seen, seen & !DEAD, Ordering::AcqRel, Ordering::Acquire);
        }
        self.cell.store(NEW, Ordering::Release);
        self.handoff.store(1, Ordering::Relaxed);
    }
}

/// 古い eid (前の住人、 世代 = 始めの場所の世代) の読みが、 次の住人の値を返さない。
fn old_eid(s: Start, rd: Reader) {
    loom::model(move || {
        let slot = Arc::new(Slot::new(s));
        let w = {
            let slot = slot.clone();
            thread::spawn(move || slot.delete_and_reuse())
        };
        let got = slot.read(s.slot_gen, rd);
        w.join().unwrap();
        assert_ne!(got, Some(NEW), "古い eid の読みが、 使い回した後の住人の値を返した");
    });
}

/// 新しい eid (次の住人、 世代 + 1) を同期の無い渡し方で受けた読みが、 前の住人の値を返さない。
fn new_eid(s: Start, rd: Reader) {
    loom::model(move || {
        let slot = Arc::new(Slot::new(s));
        let w = {
            let slot = slot.clone();
            thread::spawn(move || slot.delete_and_reuse())
        };
        let got = (slot.handoff.load(Ordering::Relaxed) == 1).then(|| slot.read(s.slot_gen + 1, rd)).flatten();
        w.join().unwrap();
        assert_ne!(got, Some(OLD), "新しい eid の読みが、 前の住人の値を返した");
    });
}

/// 出口が新しい世代を付けて返した eid の後の読み (cell を Relaxed で読む = 照合を通らない読みでも) が、 前の住人の値を見ない。
///
/// 読み手は出口の前に `handoff` を Relaxed で読む (同期は作らない)。 loom 0.7 は、 読み手の最初の load が書き手の RMW の値を読む形
/// では、 その後の Relaxed の load に古い store を選ばせない (fence の無い対照が落ちない。 2026-11-19 に試した)。 先に書き手の最後の
/// store を読ませると選ばせる。
fn exit_then_read(fence_after: bool) {
    loom::model(move || {
        let slot = Arc::new(Slot::new(MARKED));
        let w = {
            let slot = slot.clone();
            thread::spawn(move || slot.delete_and_reuse())
        };
        let got = (slot.handoff.load(Ordering::Relaxed) == 1 && slot.exit(fence_after) == MARKED.slot_gen + 1)
            .then(|| slot.cell.load(Ordering::Relaxed));
        w.join().unwrap();
        assert_ne!(got, Some(OLD), "出口が返した新しい eid の後の読みが、 前の住人の値を見た");
    });
}

#[test]
fn get_path_never_returns_the_next_residents_value_to_an_old_eid() {
    old_eid(FRESH, CODE);
    old_eid(MARKED, CODE);
}

#[test]
fn fenced_path_never_returns_the_next_residents_value_to_an_old_eid() {
    old_eid(FRESH, CODE_FENCED);
    old_eid(MARKED, CODE_FENCED);
}

#[test]
fn reads_across_the_move_to_v15_never_return_the_next_residents_value() {
    old_eid(V14, CODE);
    old_eid(V14, CODE_FENCED);
}

#[test]
fn get_path_never_returns_the_previous_residents_value_to_a_new_eid() {
    new_eid(FRESH, CODE);
    new_eid(MARKED, CODE);
    new_eid(V14, CODE);
}

#[test]
fn fenced_path_never_returns_the_previous_residents_value_to_a_new_eid() {
    new_eid(FRESH, CODE_FENCED);
    new_eid(MARKED, CODE_FENCED);
    new_eid(V14, CODE_FENCED);
}

#[test]
fn a_read_after_the_exit_never_sees_the_previous_residents_value() {
    exit_then_read(true);
}

/// 対照: cell を Relaxed で読んで fence も無いと、 読んだ後の照合の Relaxed の load が書き手の世代の書きを見ない。
#[test]
#[should_panic(expected = "使い回した後の住人の値")]
fn a_relaxed_cell_without_the_fence_returns_the_next_residents_value() {
    old_eid(FRESH, Reader { how: Read::RelaxedNoFence, ..CODE });
}

/// 対照: 読んだ後の照合を外すと、 読む前の照合の後に使い回した住人の値を返す。
#[test]
#[should_panic(expected = "使い回した後の住人の値")]
fn without_the_check_after_the_read_returns_the_next_residents_value() {
    old_eid(FRESH, Reader { post: Post::Skip, ..CODE });
}

/// 対照: 世代を持たない DB の読みで、 読んだ後に場所の bit だけを見ない (照合の他の所は code のまま)。
#[test]
#[should_panic(expected = "使い回した後の住人の値")]
fn without_the_bits_recheck_a_read_across_the_move_returns_the_next_residents_value() {
    old_eid(V14, Reader { post: Post::NoBitsRecheck, ..CODE });
}

/// 対照 (#1020 の読み直し A、 #1020 の前の形): 世代 ≠ 0 の eid の読む前の照合を Relaxed で読むと、 cell の Acquire の load は前の住人の
/// 値を読んでよく、 読んだ後の照合も通る (印の無い page)。
#[test]
#[should_panic(expected = "前の住人の値")]
fn a_relaxed_check_before_the_read_returns_the_previous_residents_value_to_a_new_eid() {
    new_eid(FRESH, Reader { pre: Ordering::Relaxed, ..CODE });
}

/// 対照: 同じく、 印のある page (書き手は印を書かない)。
#[test]
#[should_panic(expected = "前の住人の値")]
fn a_relaxed_check_before_the_read_on_a_marked_page_returns_the_previous_residents_value() {
    new_eid(MARKED, Reader { pre: Ordering::Relaxed, ..CODE });
}

/// 対照: 出口が 2 B を Relaxed で読んで fence を置かないと、 返した新しい eid の後の読みが前の住人の値を見る。
#[test]
#[should_panic(expected = "前の住人の値")]
fn without_the_fence_the_exit_lets_a_later_read_see_the_previous_residents_value() {
    exit_then_read(false);
}
