//! loom model — live query の **印の置き場 (shard) と dirty flag** の契約を全 interleaving で検証する。
//!
//! ## 何を守っているのか
//! 書き込み W は 「自分の shard の lock の下で印を置く → lock を離す → dirty の自分の bit が落ちて
//! いれば立てる」。 poll P は 「dirty の全 bit を落とす (swap) → 立っていた bit の shard だけを lock
//! して印を取り出す」。
//!
//! 守る性質: **どの interleaving でも、 印が shard に残ったまま その shard の bit が落ちている状態で終わらない**
//! (そうなると次の poll は 「印なし」 と見て飛ばし、 その印は次の書き込みまで評価されない)。
//!
//! 順序が根拠: P の swap は P の shard lock より前。 W の印が P の取り出しより後なら、 W の lock は
//! P の unlock に同期するので、 W の dirty の load は P の swap を見る (= false を見て立て直す)。
//! W が dirty を **印より先に** 立てると、 P が swap → 取り出し (空) → W が印を置く、 で印が残る。
//!
//! 立てる操作は `store` でなく `fetch_or` (RMW)。 loom 0.7 は 「別 thread の `store` の後の RMW」 が
//! 古い値を読む実行を作る (メモリモデル上ありえない。 `store` で書いた最小の model でも
//! swap → join → load が両方 false になった) ので、 実コードも model も RMW にしている。
//! load で立っているのを見たら RMW はしないので、 書き込みのたびに cache line を取り合うことはない。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_live_dirty --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::atomic::{AtomicU32, Ordering};
use loom::sync::{Arc, Mutex};

struct Model {
    /// shard 2 個 (書き手ごとに別)。 中身は印の数。
    shards: [Mutex<usize>; 2],
    /// bit i = shard i に印がある。
    dirty: AtomicU32,
    /// poll が取り出した印の数。
    taken: Mutex<usize>,
}

impl Model {
    /// `Family::mark`。 `dirty_first` = 誤った順序 (dirty を印より先に立てる)。
    fn write(&self, shard: usize, dirty_first: bool) {
        let bit = 1u32 << shard;
        let set_dirty = || {
            if self.dirty.load(Ordering::Acquire) & bit == 0 {
                self.dirty.fetch_or(bit, Ordering::AcqRel);
            }
        };
        if dirty_first {
            set_dirty();
        }
        *self.shards[shard].lock().unwrap() += 1;
        if !dirty_first {
            set_dirty();
        }
    }

    /// `Family::settle` の取り出し部分。
    fn poll(&self) {
        let mask = self.dirty.swap(0, Ordering::AcqRel);
        for (i, sh) in self.shards.iter().enumerate() {
            if mask & (1 << i) == 0 {
                continue;
            }
            let mut n = sh.lock().unwrap();
            *self.taken.lock().unwrap() += *n;
            *n = 0;
        }
    }
}

fn run(dirty_first: bool) {
    loom::model(move || {
        let m = Arc::new(Model {
            shards: [Mutex::new(0), Mutex::new(0)],
            dirty: AtomicU32::new(0),
            taken: Mutex::new(0),
        });
        let ws: Vec<_> = (0..2)
            .map(|i| {
                let m = m.clone();
                loom::thread::spawn(move || m.write(i, dirty_first))
            })
            .collect();
        m.poll();
        for w in ws {
            w.join().unwrap();
        }
        let dirty = m.dirty.load(Ordering::Acquire);
        for (i, sh) in m.shards.iter().enumerate() {
            let left = *sh.lock().unwrap();
            assert!(left == 0 || dirty & (1 << i) != 0, "印が残っているのに dirty が落ちている (印の取りこぼし)");
        }
        // もう 1 回 poll すれば全部取り出せる
        m.poll();
        assert_eq!(*m.taken.lock().unwrap(), 2, "印の取りこぼし");
    });
}

/// 印を置いて lock を離した後に dirty を立てる = 取りこぼし無し。
#[test]
fn mark_then_dirty_never_strands() {
    run(false);
}

/// dirty を先に立てると loom が取りこぼしの interleaving を見つける (model が窓を表現できている確認)。
#[test]
#[should_panic(expected = "取りこぼし")]
fn dirty_then_mark_strands() {
    run(true);
}


