//! loom model — #731: file version 15 に移る手順 (`Engine::move_to_generations`) と、 行を消す道 (`Engine::live_free`) の揃え。
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
//!
//! `move_without_the_gate_leaves_a_freed_slot_at_generation_zero` は揃えを外した形 (印を load で読むだけ、 待たない) で、 loom が
//! 窓を見つける (should_panic)。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_eid_generation_move --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use loom::thread;

const ON: u64 = 1 << 63;
const COUNT: u64 = u32::MAX as u64;

/// 1 つの場所と、 移る手順の印。
struct Model {
    live: AtomicBool,
    generation: AtomicU32,
    gate: AtomicU64,
}

impl Model {
    fn new() -> Self {
        Model { live: AtomicBool::new(true), generation: AtomicU32::new(0), gate: AtomicU64::new(0) }
    }

    /// 消す道 (`live_free`)。 `gated` = 1 語の揃えを使う。
    fn delete(&self, gated: bool) {
        if gated {
            let (on, entered) = if self.gate.load(Ordering::Acquire) & ON != 0 {
                (true, false)
            } else {
                let v = self.gate.fetch_add(1, Ordering::AcqRel);
                (v & ON != 0, true)
            };
            if on {
                self.generation.fetch_max(1, Ordering::AcqRel);
            }
            self.live.store(false, Ordering::Release);
            if entered {
                self.gate.fetch_sub(1, Ordering::Release);
            }
        } else {
            let on = self.gate.load(Ordering::SeqCst) & ON != 0;
            if on {
                self.generation.fetch_max(1, Ordering::AcqRel);
            }
            self.live.store(false, Ordering::SeqCst);
        }
    }

    /// 移る手順 (`move_to_generations`): 印を立て、 (揃えるなら) 待ってから、 死んだ場所の世代を 1 にする。
    fn move_to_generations(&self, gated: bool) {
        self.gate.fetch_or(ON, Ordering::AcqRel);
        if gated {
            while self.gate.load(Ordering::Acquire) & COUNT != 0 {
                thread::yield_now();
            }
        }
        if !self.live.load(Ordering::Acquire) && self.generation.load(Ordering::Acquire) == 0 {
            self.generation.fetch_max(1, Ordering::AcqRel);
        }
    }
}

fn run(gated: bool) {
    loom::model(move || {
        let m = Arc::new(Model::new());
        let d = {
            let m = m.clone();
            thread::spawn(move || m.delete(gated))
        };
        m.move_to_generations(gated);
        d.join().unwrap();
        assert!(!m.live.load(Ordering::Acquire), "前提: 行は消えた");
        assert_ne!(m.generation.load(Ordering::Acquire), 0, "v15 に移った後、 v14 の間に消した場所の世代が 0 のまま");
    });
}

#[test]
fn a_slot_freed_while_moving_to_generations_never_stays_at_generation_zero() {
    run(true);
}

#[test]
#[should_panic(expected = "世代が 0 のまま")]
fn move_without_the_gate_leaves_a_freed_slot_at_generation_zero() {
    run(false);
}
