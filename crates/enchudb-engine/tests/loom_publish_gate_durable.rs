//! loom model — #656 の書き出しの門の 2 つの約束を全 interleaving で検証する。
//!
//! ## 1. 辞書の境目 (`Vocabulary::durable_mark`)
//! 語を置く書き手 (`Vocabulary::alloc_entry`) は、 今の版の書き手の数に入ってから (`Durable::begin_write`、 版と数は
//! 1 語なので、 版を読むのと入るのは 1 回の CAS) data の場所を取り、 data と offsets を書いて出る。 境目を取る側は data の
//! 終わりを読み、 版を 1 つ進め (同じ語の RMW)、 前の版の書き手が出るのを待ってから書き出す。 守る性質: **終わりより前の場所を取った書き手の書いた中身は、 待ち終えた後の書き出しから見える** (見えずに
//! 書き出すと、 その語を 「済んだ」 とし、 その語を指す cell が門を通らずに中身より先に届く)。
//!
//! 終わりを読むのは版を進める **前**。 逆にすると、 進めた後に新しい版に入った書き手が場所を取り、 その後に読んだ終わりに
//! 入るのに待たない (`mark_that_reads_the_end_after_the_flip_misses_a_writer` で loom が窓を見つける)。 版と数を別の atomic に
//! 置いて SeqCst で読み直す形 (初めの実装) は、 loom が SeqCst を AcqRel として扱うので正しい順でも窓を報告し、 確かめられ
//! なかった (1 語にしたのはそのため)。
//!
//! ## 2. 列の印 (`PublishGate::enter` / `take_written`)
//! 門を通る書き手は、 中身を書いてから入り、 列に印を立ててから cell を書いて出る。 書き出しは列の門を閉じ、 中に居る書き手が
//! 出るのを待ってから印を下ろし、 印があれば中身を書き出し直してから列を書き出す。 守る性質: **書き出しが見る cell を書いた
//! 書き手の中身は、 書き出し直しが見る** (印を見落とすと、 中身を書き出し直さずに cell を書き出す)。 印を閉じる前に下ろすと
//! 窓がある (`taking_the_mark_before_closing_misses_a_writer`)。
//!
//! 書き出し (msync) は 「中身 / cell を読む」 で置く。 中身は `loom::cell::UnsafeCell` なので、 同期の無い読みは loom が
//! 見つける。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_publish_gate_durable --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::cell::UnsafeCell;
use loom::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use loom::sync::Arc;
use loom::thread;

/// 辞書の境目の模型。 data の場所は 1 語ずつ (長さ 1)。 版と書き手の数は 1 語 (`Durable::state`)。
struct Vocab {
    data_end: AtomicU32,
    /// 最上位 bit = 版の偶奇、 下位 32 bit = 偶の版の書き手の数、 その上 = 奇の版の数
    state: AtomicU64,
    /// 語の中身 (data と offsets) — 書き手が書き、 書き出しが読む
    word: UnsafeCell<u32>,
}

const ODD: u64 = 1 << 63;

fn one(odd: bool) -> u64 {
    if odd { 1 << 32 } else { 1 }
}

fn count(s: u64, odd: bool) -> u64 {
    if odd { (s & !ODD) >> 32 } else { s & u32::MAX as u64 }
}

impl Vocab {
    fn new() -> Self {
        Self { data_end: AtomicU32::new(0), state: AtomicU64::new(0), word: UnsafeCell::new(0) }
    }

    /// `Vocabulary::alloc_entry` (`Durable::begin_write` → 場所を取る → 書く → 出る)。
    fn insert(&self) {
        let mut s = self.state.load(Ordering::Relaxed);
        let odd = loop {
            let odd = s & ODD != 0;
            match self.state.compare_exchange_weak(s, s + one(odd), Ordering::Acquire, Ordering::Relaxed) {
                Ok(_) => break odd,
                Err(now) => s = now,
            }
        };
        // 実装の data の場所は Relaxed の fetch_update
        self.data_end.fetch_add(1, Ordering::Relaxed);
        self.word.with_mut(|p| unsafe { *p = 7 });
        self.state.fetch_sub(one(odd), Ordering::Release);
    }

    /// `Vocabulary::durable_mark`。 `end_first = false` は終わりを版を進めた後に読む (壊した形)。
    fn mark(&self, end_first: bool) -> u32 {
        let read_end = || self.data_end.load(Ordering::Acquire);
        let mut end = if end_first { read_end() } else { 0 };
        let odd = self.state.fetch_xor(ODD, Ordering::AcqRel) & ODD != 0;
        if !end_first {
            end = read_end();
        }
        while count(self.state.load(Ordering::Acquire), odd) != 0 {
            thread::yield_now();
        }
        end
    }
}

/// 境目を取って 「書き出す」 (終わりより前の場所の中身を読む)。
fn mark_and_write_out(v: &Vocab, end_first: bool) {
    let end = v.mark(end_first);
    // 書き手の場所は 0。 終わりがそれより先なら、 その中身は書き出しに入る (読める) はず
    if end > 0 {
        let got = v.word.with(|p| unsafe { *p });
        assert_eq!(got, 7, "境目に入る語の中身が書き出しから見えない");
    }
}

fn vocab_model(end_first: bool, writers: usize) {
    loom::model(move || {
        let v = Arc::new(Vocab::new());
        let ws: Vec<_> = (0..writers)
            .map(|_| {
                let v = v.clone();
                thread::spawn(move || v.insert())
            })
            .collect();
        mark_and_write_out(&v, end_first);
        for w in ws {
            w.join().unwrap();
        }
    });
}

/// 境目を取る側は、 終わりより前の場所を取った書き手を待つ。
#[test]
fn mark_waits_for_a_writer_before_the_end() {
    vocab_model(true, 1);
}

/// 対照: 終わりを版を進めた後に読むと、 新しい版の書き手の場所が終わりに入り、 待たずに書き出す (loom が窓を見つける)。
#[test]
#[should_panic]
fn mark_that_reads_the_end_after_the_flip_misses_a_writer() {
    vocab_model(false, 1);
}

/// 境目を 2 回続けて取っても (2 回目は 1 回目の版の後)、 書き手を待つ。
#[test]
fn second_mark_waits_for_a_writer_of_the_next_epoch() {
    loom::model(|| {
        let v = Arc::new(Vocab::new());
        let w = {
            let v = v.clone();
            thread::spawn(move || v.insert())
        };
        let _ = v.mark(true);
        mark_and_write_out(&v, true);
        w.join().unwrap();
    });
}

/// 列の印と門の模型 (1 列、 shard 1 つ)。
struct Gate {
    shard: AtomicU64,
    written: AtomicBool,
    /// 書き手の中身 (辞書の語 / Leaf の slot)
    content: UnsafeCell<u32>,
    /// 中身を指す cell
    cell: AtomicU32,
}

const CLOSED: u64 = 1 << 63;

impl Gate {
    fn new() -> Self {
        Self { shard: AtomicU64::new(0), written: AtomicBool::new(false), content: UnsafeCell::new(0), cell: AtomicU32::new(0) }
    }

    /// `Engine::live_set` の門を通る道: 中身を書く → 入る → 印 → cell → 出る。
    fn write(&self) {
        self.content.with_mut(|p| unsafe { *p = 9 });
        loop {
            let v = self.shard.load(Ordering::Acquire);
            if v & CLOSED == 0 && self.shard.compare_exchange_weak(v, v + 1, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                break;
            }
            thread::yield_now();
        }
        if !self.written.load(Ordering::Relaxed) {
            self.written.store(true, Ordering::Relaxed);
        }
        self.cell.store(1, Ordering::Relaxed);
        self.shard.fetch_sub(1, Ordering::Release);
    }

    /// `write_out_body` の 1 列分。 `take_first` は印を閉じる前に下ろす (壊した形)。
    fn write_out(&self, take_first: bool) {
        let mut entered = take_first && self.written.swap(false, Ordering::AcqRel);
        self.shard.fetch_or(CLOSED, Ordering::AcqRel);
        while self.shard.load(Ordering::Acquire) & !CLOSED != 0 {
            thread::yield_now();
        }
        if !take_first {
            entered = self.written.swap(false, Ordering::AcqRel);
        }
        // 列を書き出す: 届く cell が門を通った書き手のものなら、 その中身を書き出し直しているはず
        let cell = self.cell.load(Ordering::Relaxed);
        if cell == 1 {
            assert!(entered, "門を通った書き手の cell を、 中身を書き出し直さずに書き出した");
            let got = self.content.with(|p| unsafe { *p });
            assert_eq!(got, 9);
        }
        self.shard.fetch_and(!CLOSED, Ordering::Release);
    }
}

fn gate_model(take_first: bool) {
    loom::model(move || {
        let g = Arc::new(Gate::new());
        let w = {
            let g = g.clone();
            thread::spawn(move || g.write())
        };
        g.write_out(take_first);
        w.join().unwrap();
    });
}

/// 閉じて書き手が出るのを待ってから印を下ろせば、 書き出す cell の書き手の印を見落とさない。
#[test]
fn closing_before_taking_the_mark_sees_every_writer() {
    gate_model(false);
}

/// 対照: 印を閉じる前に下ろすと、 その後に門を通った書き手の cell を中身なしで書き出す (loom が窓を見つける)。
#[test]
#[should_panic]
fn taking_the_mark_before_closing_misses_a_writer() {
    gate_model(true);
}
