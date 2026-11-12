//! loom model — #708 の、 辞書の回収が 「書き出しを待つ場所」 を取らない約束を全 interleaving で検証する。
//!
//! 最後の参照を返す側 (`Vocabulary::release_slot`) は、 `retired` の lock の下で 「書き出しを待つ印」 (場所ごとの stamp) を
//! 立ててから、 参照を 1 → 0 にする。 場所を取る側 (`Vocabulary::claim`) は、 待ち行列から出した場所の参照を 0 → CLAIMED に
//! CAS してから印を読み、 印があれば参照を 0 に戻して取らない。 待ち行列には、 前に参照 0 になった時の古い entry が残って
//! いることがある (その後に同じ語が使い直された場所)。
//!
//! 守る性質: **参照を返した付け替えが書き出される前に、 その場所を取らない** (取ると offsets をその場で次の語に書き換え、
//! それが列より先にディスクに届いて、 ディスクの cell の指す語が潰れる)。 模型には書き出し (`durable_end`) が無いので、
//! 取る側は決して取れてはいけない。
//!
//! 印を参照の後に立てる形 (壊した形) は、 0 を見た取る側がまだ立っていない印を読む窓がある
//! (`stamp_after_the_release_lets_a_stale_entry_claim_the_slot` で loom が見つける)。 決まった順の試験では踏めない窓なので
//! ここで見る。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_vocab_retire_stamp --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

const CLAIMED: u32 = u32::MAX;

/// 場所 1 つ。 参照 1 (cell が指している) から始まり、 待ち行列にはこの場所の古い entry がある。
struct Slot {
    refs: AtomicU32,
    stamp: AtomicU32,
    /// `Reclaim::retired` の lock (中身 = 待っている entry の数)
    retired: Mutex<u32>,
    /// 取る側が場所を取った (offsets を書き換える所まで進んだ)
    taken: AtomicBool,
}

impl Slot {
    fn new() -> Self {
        Self { refs: AtomicU32::new(1), stamp: AtomicU32::new(0), retired: Mutex::new(0), taken: AtomicBool::new(false) }
    }

    /// `Vocabulary::release_slot` の最後の参照を返す道。 `stamp_first = false` は印を参照の後に立てる (壊した形)。
    fn release(&self, stamp_first: bool) {
        let mut pending = self.retired.lock().unwrap();
        let epoch = 1;
        if stamp_first {
            self.stamp.swap(epoch, Ordering::AcqRel);
        }
        let released = self.refs.compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire).is_ok();
        assert!(released, "模型では参照を足す書き手は居ない");
        if !stamp_first {
            self.stamp.store(epoch, Ordering::Release);
        }
        *pending += 1;
    }

    /// `Vocabulary::claim` の、 待ち行列から出した 1 つ分。
    fn claim(&self) {
        if self.refs.compare_exchange(0, CLAIMED, Ordering::AcqRel, Ordering::Relaxed).is_err() {
            return;
        }
        if self.stamp.load(Ordering::Acquire) != 0 {
            self.refs.store(0, Ordering::Release);
            return;
        }
        self.taken.store(true, Ordering::Release);
    }
}

fn model(stamp_first: bool) {
    loom::model(move || {
        let s = Arc::new(Slot::new());
        let releaser = {
            let s = s.clone();
            thread::spawn(move || s.release(stamp_first))
        };
        s.claim();
        releaser.join().unwrap();
        assert!(!s.taken.load(Ordering::Acquire), "書き出しを待つ場所を、 待ち行列の古い entry で取った");
        // 取りかけて戻した後も、 場所は 「参照 0・待っている」 のまま
        assert_eq!(s.refs.load(Ordering::Acquire), 0);
        assert_eq!(s.stamp.load(Ordering::Acquire), 1);
    });
}

/// 印を参照の前に立てれば、 0 を見た取る側は必ず印を見る。
#[test]
fn a_stale_entry_never_claims_a_slot_waiting_for_the_write_out() {
    model(true);
}

/// 対照: 印を参照の後に立てると、 その間に取る側が場所を取る (loom が窓を見つける)。
#[test]
#[should_panic]
fn stamp_after_the_release_lets_a_stale_entry_claim_the_slot() {
    model(false);
}
