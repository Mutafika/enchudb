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
//! ## 門 (2026-10-11、 `write_gate`)
//! cylinder を組んでいない紐の W は write_lock を取らずに門を通って Column を書く (shard の
//! 数を足し、 閉じていなければ書いて数を引く。 閉じていれば lock の道へ)。 barrier
//! (`HimoStore::write_barrier`) は lock を取った後で門を閉じ、 中の W が出るのを待ってから
//! 門を開けて lock を離す:
//!
//! - W の門に入るのが閉じるより先 → barrier は W が出るのを待つ → W の Column store は R の走査に見える
//! - 閉じている間に来た W → lock の道で barrier の unlock を待つ (上と同じ)
//! - 開けた後に入った W → W の門に入る RMW が barrier の開ける RMW に同期する → W は `active` を見る
//!
//! **lock を取って離すだけの barrier (門を閉じない、 前の形) では、 門を通る W を取りこぼす**
//! (`subscribe_with_lock_only_barrier_misses_gate_writers`)。
//!
//! ## 値の鍵 (2026-10-11、 `value_locks`)
//! cylinder を組んだ紐 (門は閉じたまま) の W は、 write_lock を取らずに値ごとの鍵の下で Column を書く
//! (`HimoStore::set_indexed`)。 barrier は lock を取って門を閉じた後、 全部の鍵も取って離す (`HimoStore::write_guard`):
//! W の鍵が barrier より先なら W の Column store は R の走査に見え、 後なら barrier の鍵の unlock が W の鍵に同期して W は
//! `active` を見る (`barrier_on_a_closed_gate_never_misses`)。 **鍵を取らない barrier は、 鍵で書く W を取りこぼす**
//! (`subscribe_with_barrier_that_skips_value_locks_misses_indexed_writers`)。
//!
//! ## model の範囲
//! 購読 1 本 × 条件 1 個 (`値 == 7`) × eid 1 個。 Column cell / write_lock / 門 (1 shard) / 値の鍵 (1 本) /
//! `active` / 購読 mutex を写す。 route (RwLock<Vec>) は 「`active` > 0 を見たら必ず見える」
//! ことを registry 側で `active` の Release の前に push することで保証しているので、 model
//! では `active` に畳む。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_live_subscribe --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};

/// 門の shard の最上位 bit = 閉じている (`write_gate::CLOSED` と同じ形)。
const CLOSED: usize = 1 << 31;

/// 登録の barrier の形。
#[derive(Clone, Copy, PartialEq)]
enum Barrier {
    /// barrier なし (対照)。
    None,
    /// write_lock を取って離すだけ (門を閉じない、 前の形。 門を通る W に対しては対照)。
    LockOnly,
    /// `HimoStore::write_barrier`: lock → 門を閉じて中の W を待つ → (組んだ紐なら) 全部の鍵を取って離す → (組んで
    /// いなければ) 門を開ける → unlock。
    Gate,
    /// 対照: `Gate` から値の鍵を取る所を外した形 (組んだ紐の、 鍵で書く W を取りこぼす)。
    GateWithoutValueLocks,
}

struct Model {
    /// Column cell。 0 = 未設定、 それ以外は `value + 1`。
    col: AtomicUsize,
    write_lock: Mutex<()>,
    /// 門 (`WriteGate`、 1 shard)。 閉じた印 | 中に居る W の数。
    gate: AtomicUsize,
    /// cylinder を組んでいない紐 (門が開く)。 false = 組んだ紐 (門は閉じたまま、 W は値の鍵で書く)。
    gate_open: bool,
    /// 組んだ紐の値の鍵 (`ValueLocks`、 値 1 つなので 1 本)。
    value_lock: Mutex<()>,
    /// 登録中の購読数。
    active: AtomicUsize,
    /// 購読の membership (条件を満たすか)。 `LiveState::m` に相当。
    member: Mutex<bool>,
}

impl Model {
    fn new(gate_open: bool) -> Self {
        Model {
            col: AtomicUsize::new(0),
            write_lock: Mutex::new(()),
            gate: AtomicUsize::new(if gate_open { 0 } else { CLOSED }),
            gate_open,
            value_lock: Mutex::new(()),
            active: AtomicUsize::new(0),
            member: Mutex::new(false),
        }
    }

    fn eval(&self) -> bool {
        self.col.load(Ordering::Acquire) == 7 + 1
    }

    /// `Engine::live_set` → `HimoStore::set` (門か write_lock か値の鍵の下) → `LiveRegistry::touch`。
    fn write(&self, value: usize) {
        if !self.gate_open {
            // 組んだ紐: 値の鍵の下で書く (`set_indexed`)
            let _k = self.value_lock.lock().unwrap();
            self.col.store(value + 1, Ordering::Release);
        } else if self.gate.fetch_add(1, Ordering::Acquire) & CLOSED == 0 {
            self.col.store(value + 1, Ordering::Release);
            self.gate.fetch_sub(1, Ordering::Release);
        } else {
            self.gate.fetch_sub(1, Ordering::Relaxed);
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
    fn subscribe(&self, barrier: Barrier) {
        self.active.fetch_add(1, Ordering::Release);
        match barrier {
            Barrier::None => {}
            Barrier::LockOnly => drop(self.write_lock.lock().unwrap()),
            Barrier::Gate | Barrier::GateWithoutValueLocks => {
                let g = self.write_lock.lock().unwrap();
                self.gate.fetch_or(CLOSED, Ordering::AcqRel);
                while self.gate.load(Ordering::Acquire) & !CLOSED != 0 {
                    loom::thread::yield_now();
                }
                if !self.gate_open && barrier == Barrier::Gate {
                    // 組んだ紐: 全部の鍵を取って離す (鍵の中の W が出るのを待つ)
                    drop(self.value_lock.lock().unwrap());
                }
                if self.gate_open {
                    self.gate.fetch_and(!CLOSED, Ordering::Release);
                }
                drop(g);
            }
        }
        // seed: 候補 (= Column 走査) を評価し直す
        let mut m = self.member.lock().unwrap();
        *m = self.eval();
    }
}

fn run(barrier: Barrier, gate_open: bool) {
    loom::model(move || {
        let m = Arc::new(Model::new(gate_open));
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

/// barrier あり (門を通る W) = 全 interleaving で取りこぼし無し。
#[test]
fn subscribe_with_barrier_never_misses() {
    run(Barrier::Gate, true);
}

/// cylinder を組んだ紐 (門は閉じたまま、 W は値の鍵の道) でも取りこぼし無し。
#[test]
fn barrier_on_a_closed_gate_never_misses() {
    run(Barrier::Gate, false);
}

/// 対照: 組んだ紐で値の鍵を取らない barrier は、 鍵で書く W を取りこぼす。
#[test]
#[should_panic(expected = "取りこぼし")]
fn subscribe_with_barrier_that_skips_value_locks_misses_indexed_writers() {
    run(Barrier::GateWithoutValueLocks, false);
}

/// barrier を外すと loom が取りこぼしの interleaving を見つける = この model が窓を
/// 表現できていることの確認 (model が甘いと上の test は何を外しても通ってしまう)。
#[test]
#[should_panic(expected = "取りこぼし")]
fn subscribe_without_barrier_misses() {
    run(Barrier::None, true);
}

/// 対照: lock を取って離すだけの barrier (門を閉じない、 前の形) は、 門を通る W を取りこぼす。
#[test]
#[should_panic(expected = "取りこぼし")]
fn subscribe_with_lock_only_barrier_misses_gate_writers() {
    run(Barrier::LockOnly, true);
}
