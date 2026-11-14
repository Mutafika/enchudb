//! loom model — #731: 場所の世代 (`gen.seg`) と生きている印 (entities) の書き込みの順。
//!
//! ## 1. file version 15 に移る手順 (`Engine::move_to_generations`) と、 行を消す道 (`Engine::live_free`) の揃え
//!
//! v14 の間は行を消しても場所の世代を上げない。 最初の使い回しで v15 に移る時、 移る側は 「消す時に上げる」 の印を立ててから
//! 死んだ場所をなめて世代を 1 にする (v14 の間に消した場所)。 守る性質: **移った後、 死んだ場所の世代は 0 でない** (0 のまま
//! だと、 その場所を使い回した行が消えた行と同じ eid になる = #731 が戻る)。
//!
//! 窓: 消す書き手が 「印なし」 と読んで (上げない)、 生きている印を下ろす前に、 移る側が印を立てて生きている印を読む (生きて
//! いるのでなめない) と、 誰も上げない。
//!
//! 揃え (`Generations::gate`): 印と 「印を見ずに入った書き手の数」 を 1 語に置く。 消す書き手は入る時の RMW で印を読み、 印が
//! 無ければ生きている印を下ろしてから出る (数を戻す)。 移る側は印を立てる RMW の後、 数が 0 になるのを待ってからなめる。 同じ語の
//! RMW の順なので、 印を見ずに入った書き手は 「移る側の待ち」 に数えられ、 なめる時には死んでいる。 別の atomic を SeqCst の
//! fence で読み直す形 (初めの実装) は、 loom が SeqCst を AcqRel として扱うので確かめられない (#656 の門と同じ理由で 1 語に寄せた)。
//! `move_without_the_gate_leaves_a_freed_slot_at_generation_zero` は揃えを外した形 (印を load で読むだけ、 待たない) で、 loom が
//! 窓を見つける (should_panic)。
//!
//! ## 2. 電源断の像と開く時の突き合わせ (PR #885 の読み直し F1、 案 (a))
//!
//! 生きている印と世代は別の segment で、 本体の書き出しは消す書き手を止めずに 2 つを書き出す (OS も page を勝手な時に先に書き出す)。
//! 像 = 生きている印と場所の 2 B を、 書き手と並んで勝手な時に 1 つずつ読んだ物 (読む順も 2 通り)。 開く時の突き合わせ
//! (`Engine::reconcile_generations`) は **死が勝つ**: どちらかが死んでいれば削除を完了させる (gen.seg だけ死 = 世代をもう 1 つ上げて
//! 生きている印を下ろす、 entities だけ死 = 世代を上げて印を立てる)。 像に当てた結果について:
//!
//! - 消す (`kill` = 世代 + 1 と 「死んでいる」 印を 1 回の store → 生きている印を下ろす): **結果は 「消す前 (生きている, g)」 か
//!   「消した後 (死んでいる, g + 1 以上)」**。 印を持たない形 (前の 1 B、 `heal_without_the_dead_mark_tears_a_delete`) は
//!   「生きている, g + 1」 を残す (should_panic)
//! - 使い回す (`revive` = 印を落とす → 生きている印を立てる)、 消えた行 (世代 g) の場所、 次の住人は g + 1: **生きている結果の世代は
//!   g + 1、 死んでいる結果の世代は g + 1 以上** (消えた行の eid が生き返らない)。 「gen.seg だけ死 = 削除が届いていない → 世代を
//!   戻す」 規則 (`reverting_heal_brings_the_deleted_rows_generation_back`) は、 使い回しの生きている印だけ届いた像を削除の途中と
//!   読み違えて g に戻す (should_panic)
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_eid_generation_move --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use loom::thread;

const ON: u64 = 1 << 63;
const COUNT: u64 = u32::MAX as u64;
/// 場所の 2 B の 「死んでいる」 印 (`generation.rs` と同じ形: 下の byte = 世代)。
const DEAD: u16 = 1 << 8;
const GEN: u16 = 0xFF;

/// 1 つの場所 (生きている印 + 世代と 「死んでいる」 印の 2 B) と、 移る手順の印。
struct Model {
    live: AtomicBool,
    cell: AtomicU16,
    gate: AtomicU64,
}

impl Model {
    fn new(live: bool, cell: u16) -> Self {
        Model { live: AtomicBool::new(live), cell: AtomicU16::new(cell), gate: AtomicU64::new(0) }
    }

    fn generation(&self) -> u16 {
        self.cell.load(Ordering::Acquire) & GEN
    }

    /// `Generations::kill`: 世代を `g` 以上にして 「死んでいる」 印を立てる (1 回の RMW)。
    fn kill(&self, g: u16) {
        let _ = self.cell.fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| Some((v & GEN).max(g) | DEAD));
    }

    /// 消す道 (`live_free`)。 `gated` = 1 語の揃えを使う。
    fn delete(&self, gated: bool) {
        if gated {
            let was_live = self.live.load(Ordering::Acquire);
            let on = self.gate.fetch_add(1, Ordering::AcqRel) & ON != 0;
            if on && was_live {
                self.kill(self.generation() + 1);
            }
            self.live.store(false, Ordering::Release);
            self.gate.fetch_sub(1, Ordering::Release);
        } else {
            let on = self.gate.load(Ordering::SeqCst) & ON != 0;
            if on {
                self.kill(1);
            }
            self.live.store(false, Ordering::SeqCst);
        }
    }

    /// 移る手順 (`move_to_generations`): 印を立て、 (揃えるなら) 待ってから、 死んだ場所を 「世代 1、 死んでいる」 にする。
    fn move_to_generations(&self, gated: bool) {
        self.gate.fetch_or(ON, Ordering::AcqRel);
        if gated {
            while self.gate.load(Ordering::Acquire) & COUNT != 0 {
                thread::yield_now();
            }
        }
        if !self.live.load(Ordering::Acquire) && self.cell.load(Ordering::Acquire) == 0 {
            self.kill(1);
        }
    }

    /// 使い回す (`entity_in` の取り出し): `revive_first` = 印を落としてから生きている印を立てる (正しい順)。
    fn reuse(&self, revive_first: bool) {
        if revive_first {
            self.cell.fetch_and(!DEAD, Ordering::AcqRel);
            self.live.store(true, Ordering::Release);
        } else {
            self.live.store(true, Ordering::Release);
            self.cell.fetch_and(!DEAD, Ordering::AcqRel);
        }
    }

    /// 電源断の像: 2 つの segment を、 書き手と並んで 1 つずつ読む (`live_first` = 生きている印を先に)。 間で書き手が動ける (yield)。
    fn snapshot(&self, live_first: bool) -> (bool, u16) {
        if live_first {
            let live = self.live.load(Ordering::Acquire);
            thread::yield_now();
            (live, self.cell.load(Ordering::Acquire))
        } else {
            let cell = self.cell.load(Ordering::Acquire);
            thread::yield_now();
            (self.live.load(Ordering::Acquire), cell)
        }
    }
}

/// 開く時の突き合わせ (`reconcile_generations`) を像に当てた (生きているか, 世代)。 `rule`: 突き合わせの規則。
#[derive(Clone, Copy)]
enum Rule {
    /// 死が勝つ (今の形)
    DeadWins,
    /// 「死んでいる」 印を持たない (前の 1 B の形): 像のまま
    NoMark,
    /// gen.seg だけ死 = 削除が届いていない → 世代を戻す (main の初めの案)
    Revert,
}

fn reconcile((live, cell): (bool, u16), rule: Rule) -> (bool, u16) {
    let g = cell & GEN;
    let dead = cell & DEAD != 0;
    match (rule, live, dead) {
        (Rule::NoMark, ..) => (live, g),
        (Rule::DeadWins, true, true) => (false, g + 1), // 削除を完了 (世代をもう 1 つ)
        (Rule::Revert, true, true) => (true, g - 1),    // 削除が届いていない → 戻す
        (_, false, false) => (false, g + 1),            // 生きている印だけ届いた削除 → 完了させる
        _ => (live, g),
    }
}

fn run_move(gated: bool) {
    loom::model(move || {
        let m = Arc::new(Model::new(true, 0));
        let d = {
            let m = m.clone();
            thread::spawn(move || m.delete(gated))
        };
        m.move_to_generations(gated);
        d.join().unwrap();
        assert!(!m.live.load(Ordering::Acquire), "前提: 行は消えた");
        assert_ne!(m.generation(), 0, "v15 に移った後、 v14 の間に消した場所の世代が 0 のまま");
    });
}

#[test]
fn a_slot_freed_while_moving_to_generations_never_stays_at_generation_zero() {
    run_move(true);
}

#[test]
#[should_panic(expected = "世代が 0 のまま")]
fn move_without_the_gate_leaves_a_freed_slot_at_generation_zero() {
    run_move(false);
}

/// v15 (印 ON)、 世代 1 の生きている行を消す書き手と、 並んで撮る像。
fn run_torn_delete(rule: Rule) {
    for live_first in [true, false] {
        loom::model(move || {
            let m = Arc::new(Model::new(true, 1));
            m.gate.store(ON, Ordering::Release);
            let d = {
                let m = m.clone();
                thread::spawn(move || m.delete(true))
            };
            let image = m.snapshot(live_first);
            d.join().unwrap();
            let healed = reconcile(image, rule);
            assert!(
                matches!(healed, (true, 1) | (false, 2..)),
                "突き合わせた像が消す途中: {healed:?} (像 {image:?}、 生きている印を先に読む {live_first})"
            );
        });
    }
}

#[test]
fn a_torn_delete_heals_to_before_or_after() {
    run_torn_delete(Rule::DeadWins);
}

#[test]
#[should_panic(expected = "突き合わせた像が消す途中")]
fn heal_without_the_dead_mark_tears_a_delete() {
    run_torn_delete(Rule::NoMark);
}

/// 世代 1 の行が消えた場所 (「世代 2、 死んでいる」、 削除は書き出し済み) を使い回す書き手と、 並んで撮る像。
fn run_torn_reuse(rule: Rule) {
    for live_first in [true, false] {
        loom::model(move || {
            let m = Arc::new(Model::new(false, 2 | DEAD));
            m.gate.store(ON, Ordering::Release);
            let r = {
                let m = m.clone();
                thread::spawn(move || m.reuse(true))
            };
            let image = m.snapshot(live_first);
            r.join().unwrap();
            let (live, g) = reconcile(image, rule);
            assert!(
                if live { g == 2 } else { g >= 2 },
                "使い回しの像で消えた行の世代が生き返った: 生きている {live}、 世代 {g} (像 {image:?})"
            );
        });
    }
}

#[test]
fn a_torn_reuse_never_heals_to_the_deleted_rows_generation() {
    run_torn_reuse(Rule::DeadWins);
}

#[test]
#[should_panic(expected = "消えた行の世代が生き返った")]
fn reverting_heal_brings_the_deleted_rows_generation_back() {
    run_torn_reuse(Rule::Revert);
}
