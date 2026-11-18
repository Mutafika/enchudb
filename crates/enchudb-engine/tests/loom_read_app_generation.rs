//! loom model — #1015: lock を取らない読みの入口 (`Engine::read_app_in`) の照合の順。
//!
//! 読み手はアプリの eid (世代 g | 場所) で 1 つの cell を読む: 前の照合 (page の印 → 印があれば場所の 2 B) → cell を読む → 読んだ
//! 後の照合 (同じ)。 書き手は同じ場所の行を消して使い回し、 次の住人の値を cell に書く (`Generations::kill` = page の印 → 世代を上げて
//! 「死んでいる」 印 → 生きている印を下ろす、 使い回し = 印を落とす → 生きている印を立てる → cell に Release で書く)。 守る性質:
//! **読み手は、 使い回した後の住人の値を古い eid に返さない** (返すのは前の住人の値か None)。
//!
//! #1015 で照合の読みを全部 Relaxed にし (page の印・場所の 2 B・場所の bit)、 get の道 (`read_app_acquired`) は読んだ後の fence を
//! 省いた。 順は 2 つのどちらかが付ける:
//!
//! - get の道: cell の Acquire の load (`HimoStore::get_value_inline`、 #106)。 次の住人の値を読んだ読みは、 その値を Release で書いた
//!   書き手と同期するので、 後の照合の Relaxed の load も書き手の世代の書きを見る
//! - fence のある道 (`read_app`、 `get_text_owned` など): cell を Relaxed で読んでも、 読んだ後の `fence(Acquire)` が同じ順を付ける
//!
//! 対照 (should_panic): cell を Relaxed で読んで fence も無い形と、 読んだ後の照合を外した形は、 loom が次の住人の値を返す道を
//! 見つける。 世代を持たない DB (場所の bit 0) で読みの間に v15 に移る形 (`ReadAppPost::Bits`: 読んだ後に場所の bit を Relaxed で
//! 読み直す) も同じ。
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

/// 1 つの場所: page の印、 場所の 2 B、 列の cell、 場所の bit (0 = 世代を持たない DB)。
struct Slot {
    mark: AtomicU8,
    slot_gen: AtomicU16,
    cell: AtomicU32,
    bits: AtomicU32,
}

#[derive(Clone, Copy, PartialEq)]
enum Read {
    /// get の道: cell を Acquire で読み、 fence なし
    Acquired,
    /// fence のある道: cell を Relaxed で読み、 読んだ後に fence(Acquire)
    Fenced,
    /// 対照: cell を Relaxed で読み、 fence なし
    RelaxedNoFence,
}

impl Slot {
    fn new(bits: u32) -> Self {
        Slot { mark: AtomicU8::new(0), slot_gen: AtomicU16::new(0), cell: AtomicU32::new(OLD), bits: AtomicU32::new(bits) }
    }

    /// `GenReader::current_relaxed`: 印の無い page は世代 0 (2 B を読まない)。
    fn current_relaxed(&self) -> u16 {
        if self.mark.load(Ordering::Relaxed) == 0 {
            return 0;
        }
        self.slot_gen.load(Ordering::Relaxed) & GEN
    }

    /// 世代 0 の eid の読み (`read_app_in`)。 `post` = 読んだ後の照合をする。
    fn read(&self, how: Read, post: bool) -> Option<u32> {
        let v15 = self.bits.load(Ordering::Relaxed) != 0;
        if v15 && self.current_relaxed() != 0 {
            return None;
        }
        let v = match how {
            Read::Acquired => self.cell.load(Ordering::Acquire),
            Read::Fenced => {
                let v = self.cell.load(Ordering::Relaxed);
                fence(Ordering::Acquire);
                v
            }
            Read::RelaxedNoFence => self.cell.load(Ordering::Relaxed),
        };
        if post {
            if v15 {
                // ReadAppPost::Gen
                if self.current_relaxed() != 0 {
                    return None;
                }
            } else {
                // ReadAppPost::Bits: 移っていたら全部を引き直す (`app_slot_still` = fence の後に `app_slot` = 印と世代を Acquire で)
                let now = if how == Read::Fenced { self.bits.load(Ordering::Acquire) } else { self.bits.load(Ordering::Relaxed) };
                if now != 0 {
                    fence(Ordering::Acquire);
                    if self.mark.load(Ordering::Acquire) != 0 && self.slot_gen.load(Ordering::Acquire) & GEN != 0 {
                        return None;
                    }
                }
            }
        }
        (v != 0).then_some(v)
    }

    /// 書き手: (世代を持たない DB なら先に v15 に移り) 行を消して同じ場所を使い回し、 次の住人の値を書く。
    fn delete_and_reuse(&self) {
        if self.bits.load(Ordering::Relaxed) == 0 {
            // `Generations::start` (region は先に置いてある)
            self.bits.store(24, Ordering::Release);
        }
        // `Generations::kill`: page の印を先に (`cell_for_write` → `mark_page`)、 世代 + 1 と 「死んでいる」 印を 1 回の RMW
        self.mark.fetch_or(1, Ordering::AcqRel);
        let _ = self.slot_gen.fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| Some(((v & GEN) + 1) | DEAD));
        // 消した行の cell を落とす
        self.cell.store(0, Ordering::Release);
        // 使い回し: 印を落とす (`revive`) → 生きている印 (この model では省く) → 次の住人の値
        self.slot_gen.fetch_and(!DEAD, Ordering::AcqRel);
        self.cell.store(NEW, Ordering::Release);
    }
}

fn run(bits: u32, how: Read, post: bool) {
    loom::model(move || {
        let s = Arc::new(Slot::new(bits));
        let w = {
            let s = s.clone();
            thread::spawn(move || s.delete_and_reuse())
        };
        let got = s.read(how, post);
        w.join().unwrap();
        assert_ne!(got, Some(NEW), "古い eid の読みが、 使い回した後の住人の値を返した");
    });
}

#[test]
fn get_path_never_returns_the_next_residents_value() {
    run(24, Read::Acquired, true);
}

#[test]
fn fenced_path_never_returns_the_next_residents_value() {
    run(24, Read::Fenced, true);
}

#[test]
fn reads_across_the_move_to_v15_never_return_the_next_residents_value() {
    run(0, Read::Acquired, true);
    run(0, Read::Fenced, true);
}

/// 対照: cell を Relaxed で読んで fence も無いと、 後の照合の Relaxed の load が書き手の世代の書きを見ない。
#[test]
#[should_panic(expected = "使い回した後の住人の値")]
fn a_relaxed_cell_without_the_fence_returns_the_next_residents_value() {
    run(24, Read::RelaxedNoFence, true);
}

/// 対照: 読んだ後の照合を外すと、 前の照合の後に使い回した住人の値を返す。
#[test]
#[should_panic(expected = "使い回した後の住人の値")]
fn without_the_check_after_the_read_returns_the_next_residents_value() {
    run(24, Read::Acquired, false);
}

/// 対照: 世代を持たない DB の読みで、 読んだ後に場所の bit を見ない (移ったことを見落とす)。
#[test]
#[should_panic(expected = "使い回した後の住人の値")]
fn without_the_bits_check_a_read_across_the_move_returns_the_next_residents_value() {
    run(0, Read::Acquired, false);
}
