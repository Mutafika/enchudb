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
//! ## 3. 払い出しと移る手順 (PR #885 の読み直し 3a) は `run_claim` の doc
//!
//! ## 4. 古い読みで印を落とす生かす道 (#901)
//!
//! 生かす道が 「死んでいる・生きていない」 と読んだ後に、 別の書き手が使い回してその行を消し始めた (kill の後・生きている印を下ろす
//! 前) 場所の印を落とさない (落とすと消す途中の像が F1 の元の形になる)。 落とすのは読んだ 2 B のままの時だけ (CAS)、 引退した場所は
//! 落とさない。 読み直して落とす形と、 引退した場所でも落とす形は、 loom が窓を見つける (should_panic)。 詳しくは 4 の model の doc。
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

/// ## 3. 払い出しと移る手順 (PR #885 の読み直し 3a)
///
/// 払い出しの道 A: (死の印を落とす = 印の無い page では何もしない) → 生きている印を立てる → `settle_claim` (移り終える前は gate を
/// RMW で読み、 「消す時に上げる」 が立っていれば移り終えるのを待つ → 死の印があれば落とす) → 場所の今の世代で eid を作る。 移る側 M:
/// 印を立てる → 死んだ場所のうち世代 0 を 「世代 1、 死」 にする → 移り終えた印。 守る性質: **A の場所は死の印を持たず、 A が作った
/// eid の世代が場所の世代と同じ**。 gate を読まない形 (`a_claim_without_the_gate_...`) は、 M が A の生きている印を読む前・A が死の印を
/// 見た後に殺す窓を loom が見つける。
fn run_claim(fenced: bool) {
    loom::model(move || {
        let m = Arc::new(Model::new(false, 0));
        let moved = Arc::new(AtomicBool::new(false));
        let mv = {
            let (m, moved) = (m.clone(), moved.clone());
            thread::spawn(move || {
                m.gate.fetch_or(ON, Ordering::AcqRel);
                if !m.live.load(Ordering::Acquire) && m.cell.load(Ordering::Acquire) == 0 {
                    m.kill(1);
                }
                moved.store(true, Ordering::Release);
            })
        };
        m.live.store(true, Ordering::Release); // allocate_at (RMW の代わり)
        if fenced && !moved.load(Ordering::Acquire) && m.gate.fetch_add(0, Ordering::AcqRel) & ON != 0 {
            while !moved.load(Ordering::Acquire) {
                thread::yield_now();
            }
        }
        if m.cell.load(Ordering::Acquire) & DEAD != 0 {
            m.cell.fetch_and(!DEAD, Ordering::AcqRel);
        }
        let handed = m.generation();
        mv.join().unwrap();
        let cell = m.cell.load(Ordering::Acquire);
        assert!(
            cell & DEAD == 0 && cell & GEN == handed,
            "払い出した場所が死の印を持つ / 渡した世代 {handed} が場所 {cell:#x} と違う"
        );
    });
}

#[test]
fn a_claim_overlapping_the_move_keeps_its_slot_and_eid() {
    run_claim(true);
}

#[test]
#[should_panic(expected = "払い出した場所が死の印を持つ")]
fn a_claim_without_the_gate_can_be_killed_by_the_move() {
    run_claim(false);
}

/// ## 4. 古い読みで印を落とす生かす道と、 使い回して消す書き手 (#901)
///
/// 生かす道 A (`Engine::revive_slot`: 受信の当て込み / 束ね / 空きの一覧の古い項目 / 払い出し位置): 場所の 2 B を読み、 生きている
/// 印を読み、 「死んでいる・生きていない」 なら印を落とす。 その間に B が同じ場所を使い回し (印を落とす → 生きている印を立てる)、
/// その行を消し始める (kill = 世代を上げて印 → 生きている印を下ろす)。 守る性質: **消す途中 (kill の後・生きている印を下ろす前) の
/// 場所は死の印を持つ** (持たないと、 その間の像が F1 の元の形 「次の世代、 印なし、 生きている」 になり、 開く時に消えた行が次の
/// 世代の eid で生きる)。
///
/// - 今の形 (`Revive::Cas`): 落とすのは読んだ 2 B のままの時だけ (CAS)、 引退した場所は落とさない
/// - 読み直して落とす形 (`Revive::FetchAnd`、 #901 の前): 読んだ後に B が使い回して kill した印を落とす (should_panic)
/// - 引退した場所でも CAS で落とす形 (`Revive::CasEvenRetired`): 引退の印は kill が上限で止める値なので、 使い回して (`entity()` の
///   空きの山は引退した場所を取ってから引退を見る) 消すと同じ 2 B に戻り、 CAS が変化を見分けられない (should_panic)
#[derive(Clone, Copy)]
enum Revive {
    Cas,
    FetchAnd,
    CasEvenRetired,
}

/// 使える世代の最大と引退の印 (場所の bit 24 の DB)。
const MAX_GEN: u16 = 254;
const RETIRED: u16 = 255;

impl Model {
    /// `Engine::revive_slot`。
    fn revive_slot(&self, how: Revive) {
        loop {
            let seen = self.cell.load(Ordering::Acquire);
            if seen & DEAD == 0 || (matches!(how, Revive::Cas) && seen & GEN > MAX_GEN) {
                return;
            }
            if self.live.load(Ordering::Acquire) {
                return;
            }
            match how {
                Revive::FetchAnd => {
                    self.cell.fetch_and(!DEAD, Ordering::AcqRel);
                    return;
                }
                Revive::Cas | Revive::CasEvenRetired => {
                    if self.cell.compare_exchange(seen, seen & !DEAD, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                        return;
                    }
                }
            }
        }
    }

    /// 払い出し: 生かす道 → 生きている印を立てる (`allocate_at` の RMW、 取れたか) → 自分の場所の死の印を落とす (`settle_claim`)。
    fn claim(&self, how: Revive) -> bool {
        self.revive_slot(how);
        if self.live.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_err() {
            return false;
        }
        if self.cell.load(Ordering::Acquire) & DEAD != 0 {
            self.cell.fetch_and(!DEAD, Ordering::AcqRel);
        }
        true
    }

    /// 使い回した行を消す (`live_free`): kill (世代 + 1、 上限は引退の印) → 消す途中に死の印があるか → 生きている印を下ろす。
    fn delete_claimed(&self) -> bool {
        let g = self.generation();
        self.kill((g + 1).min(RETIRED));
        let marked = self.cell.load(Ordering::Acquire) & DEAD != 0;
        self.live.store(false, Ordering::Release);
        marked
    }
}

/// 世代 1 の行が消えた場所 (「世代 2、 死んでいる」)。 B が使い回してその行を消し、 A が並んで生かす道を通る (取れたら自分の行)。
fn run_stale_revive(how: Revive) {
    loom::model(move || {
        let m = Arc::new(Model::new(false, 2 | DEAD));
        m.gate.store(ON, Ordering::Release);
        let b = {
            let m = m.clone();
            thread::spawn(move || !m.claim(how) || m.delete_claimed())
        };
        m.claim(how);
        assert!(b.join().unwrap(), "消す途中の場所の死の印を、 前に読んだ 「生きていない」 で落とした");
    });
}

#[test]
fn a_stale_revive_keeps_the_dead_mark_of_a_row_being_deleted() {
    run_stale_revive(Revive::Cas);
}

#[test]
#[should_panic(expected = "消す途中の場所の死の印")]
fn stale_revive_by_fetch_and_drops_the_dead_mark_of_a_row_being_deleted() {
    run_stale_revive(Revive::FetchAnd);
}

/// 引退した場所 (「引退の印、 死んでいる」) を `entity()` の空きの山が取り (生かす道 → 生きている印 → 引退を見て kill → 下ろす)、
/// A が並んで生かす道を通る。
fn run_retired_revive(how: Revive) {
    loom::model(move || {
        let m = Arc::new(Model::new(false, RETIRED | DEAD));
        m.gate.store(ON, Ordering::Release);
        let b = {
            let m = m.clone();
            thread::spawn(move || {
                m.revive_slot(how);
                m.live.store(true, Ordering::Release); // 空きの山から取った場所に印を立てる (取り合いは free_lock の下)
                m.delete_claimed()
            })
        };
        m.revive_slot(how);
        assert!(b.join().unwrap(), "消す途中の場所の死の印を、 前に読んだ 「生きていない」 で落とした");
    });
}

#[test]
fn a_stale_revive_keeps_the_dead_mark_of_a_retired_slot() {
    run_retired_revive(Revive::Cas);
}

#[test]
#[should_panic(expected = "消す途中の場所の死の印")]
fn a_stale_cas_revive_on_a_retired_slot_drops_the_dead_mark() {
    run_retired_revive(Revive::CasEvenRetired);
}
