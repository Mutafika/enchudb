//! loom model — live query の **登録と並行書き込み** の契約を全 interleaving で検証する。
//!
//! ## 何を守っているのか
//! 書き込み W は 「himo の write_lock 下で Column を書く → lock を離す → `active` を見て、
//! 購読があれば eid を評価し直す」。 登録 R は 「route に載せて `active` を上げる →
//! 条件の himo の write_lock を 1 度取って離す (barrier) → 初期集合を Column から数える」。
//!
//! barrier が無いと、 W の `active` load と R の Column load が **両方とも相手の store を
//! 見ない** 窓がある (store → load の入れ替え、 store buffering)。 W は購読が無いと思って
//! 評価をスキップし、 R は書き込み前の Column を数える → その eid は購読から永久に漏れる
//! (次にその eid が書かれるまで)。
//!
//! barrier が塞ぐ理由: W の lock が barrier より先なら W の Column store は barrier の
//! lock 取得に happens-before (R の走査に見える)。 後なら barrier の unlock が W の lock に
//! 同期するので、 W の `active` load は R の `fetch_add` を見る。
//!
//! **実コードの並行テストはこの窓を踏めない** — `tests/live_query.rs` の
//! `subscribe_while_writing_loses_nothing` は barrier を外しても 10 run 中 0 回しか
//! 落ちなかった (実測)。 よって **これが barrier の唯一の gate**。 barrier を外すと下の
//! test は落ちる (実測は module 末尾の `#[test]` の doc)。
//!
//! ## model の範囲
//! 購読 1 本 × 条件 1 個 (`値 == 7`) × eid 1 個。 Column cell / write_lock / `active` /
//! 購読 mutex を写す。 route (RwLock<Vec>) は 「`active` > 0 を見たら必ず見える」 ことを
//! registry 側で `active` の Release の前に push することで保証しているので、 model では
//! `active` に畳む。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_live_subscribe --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};

struct Model {
    /// Column cell。 0 = 未設定、 それ以外は `value + 1`。
    col: AtomicUsize,
    write_lock: Mutex<()>,
    /// 登録中の購読数。
    active: AtomicUsize,
    /// 購読の membership (条件を満たすか)。 `LiveState::m` に相当。
    member: Mutex<bool>,
}

impl Model {
    fn eval(&self) -> bool {
        self.col.load(Ordering::Acquire) == 7 + 1
    }

    /// `Engine::live_set` → `HimoStore::set` → `LiveRegistry::touch`。
    fn write(&self, value: usize) {
        {
            let _g = self.write_lock.lock().unwrap();
            self.col.store(value + 1, Ordering::Release);
        }
        if self.active.load(Ordering::Acquire) == 0 {
            return;
        }
        let mut m = self.member.lock().unwrap();
        *m = self.eval();
    }

    /// `Engine::subscribe`。
    fn subscribe(&self, barrier: bool) {
        self.active.fetch_add(1, Ordering::Release);
        if barrier {
            drop(self.write_lock.lock().unwrap());
        }
        // seed: 候補 (= Column 走査) を評価し直す
        let mut m = self.member.lock().unwrap();
        *m = self.eval();
    }
}

fn run(barrier: bool) {
    loom::model(move || {
        let m = Arc::new(Model {
            col: AtomicUsize::new(0),
            write_lock: Mutex::new(()),
            active: AtomicUsize::new(0),
            member: Mutex::new(false),
        });
        let w = {
            let m = m.clone();
            loom::thread::spawn(move || m.write(7))
        };
        m.subscribe(barrier);
        w.join().unwrap();
        let got = *m.member.lock().unwrap();
        assert_eq!(got, m.eval(), "購読の membership が Column と食い違う (登録と並行した書き込みの取りこぼし)");
    });
}

/// barrier あり = 全 interleaving で取りこぼし無し。
#[test]
fn subscribe_with_barrier_never_misses() {
    run(true);
}

/// barrier を外すと loom が取りこぼしの interleaving を見つける = この model が窓を
/// 表現できていることの確認 (model が甘いと上の test は何を外しても通ってしまう)。
#[test]
#[should_panic(expected = "取りこぼし")]
fn subscribe_without_barrier_misses() {
    run(false);
}
