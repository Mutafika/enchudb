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
//! 同じ語の 2 つ目の約束 (PR #885 の読み直し F1): 本体の書き出しは、 生きている印と場所の世代を、 消す書き手を止めて (`freeze`、
//! 印 FREEZE を立てて中の書き手が出るのを待つ) 続けて書き出す。 守る性質: **書き出した 2 つの像は 「消す前の (生きている, g)」 か
//! 「消した後の (死んでいる, g + 1)」**。 止めない形 (`snapshot_without_the_freeze_tears_a_delete`) は loom が 「生きている, g + 1」 を
//! 見つける。 消す道の 「生きていた行だけ上げる」 (`was_live`、 入る前に読む) も model に入れた。
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
const FREEZE: u64 = 1 << 62;
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

    /// 消す道の入口 (`Generations::enter_free`): 数えて入る。 v15 で書き出しが止めている間は数を戻して待つ。
    fn enter(&self) -> bool {
        loop {
            let v = self.gate.fetch_add(1, Ordering::AcqRel);
            if v & FREEZE == 0 || v & ON == 0 {
                return v & ON != 0;
            }
            self.gate.fetch_sub(1, Ordering::AcqRel);
            while self.gate.load(Ordering::Acquire) & FREEZE != 0 {
                thread::yield_now();
            }
        }
    }

    /// 消す道 (`live_free`)。 `gated` = 1 語の揃えを使う。
    fn delete(&self, gated: bool) {
        if gated {
            let was_live = self.live.load(Ordering::Acquire);
            let on = self.enter();
            if on && was_live {
                let g = self.generation.load(Ordering::Acquire);
                self.generation.fetch_max(g + 1, Ordering::AcqRel);
            }
            self.live.store(false, Ordering::Release);
            self.gate.fetch_sub(1, Ordering::Release);
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

/// 書き出し (`Generations::freeze` の中の entities → gen.seg) と消す道。 v15 (印 ON)、 世代 1 の生きている行。
fn run_snapshot(frozen: bool) {
    loom::model(move || {
        let m = Arc::new(Model::new());
        m.generation.store(1, Ordering::Release);
        m.gate.store(ON, Ordering::Release);
        let d = {
            let m = m.clone();
            thread::spawn(move || m.delete(true))
        };
        if frozen {
            m.gate.fetch_or(FREEZE, Ordering::AcqRel);
            while m.gate.load(Ordering::Acquire) & COUNT != 0 {
                thread::yield_now();
            }
        }
        let live = m.live.load(Ordering::Acquire);
        // 2 つの書き出しの間 (entities の msync の後、 gen.seg の msync の前)。 消す書き手が動ける所 (yield が無いと loom はこの間で
        // 切り替えず、 止めない形の窓も見つけない)
        thread::yield_now();
        let g = m.generation.load(Ordering::Acquire);
        if frozen {
            m.gate.fetch_and(!FREEZE, Ordering::AcqRel);
        }
        d.join().unwrap();
        assert!(matches!((live, g), (true, 1) | (false, 2)), "書き出した像が消す途中: 生きている {live}, 世代 {g}");
    });
}

#[test]
fn a_frozen_write_out_never_tears_a_delete() {
    run_snapshot(true);
}

#[test]
#[should_panic(expected = "書き出した像が消す途中")]
fn snapshot_without_the_freeze_tears_a_delete() {
    run_snapshot(false);
}
