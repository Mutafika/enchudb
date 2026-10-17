//! loom model — 行の seqlock (`row_lock::RowLocks`) で、 読み手が書き込みの途中の組を取らない (#206)。
//!
//! ## 何を守っているのか
//! 書き手は stripe の版を奇数にしてから cell を書き、 書き終えたら次の偶数にする。 読み手は偶数の版を見てから cell を読み、
//! 読み終えて版が変わっていなければ採る。 版の順は 2 つの fence に頼る:
//!
//! - 書き手: 版を奇数にする CAS (Acquire) の後の `fence(Release)` — 読み手が cell の新しい値を見たなら、 後の版の読みは
//!   奇数か次の版 (読み直す)
//! - 読み手: cell を読んだ後、 版を読み直す前の `fence(Acquire)`
//!
//! model は 1 本の書き手が 2 つの cell を 0 → 1 に書き、 1 本の読み手が 1 回読む。 採った組は (0, 0) か (1, 1)。 fence を
//! 片方ずつ外すと loom が途中の組 ((1, 0) など) を採る interleaving を見つけることも確かめる (model が窓を表現できている確認)。
//! cell は relaxed の atomic で置く (本物は mmap の列を読む。 model が見るのは版の順だけ)。 入れ子・先に握りたい読み手
//! (`urgent`)・stripe の振り分けは model に入れない (`row_lock` の unit test)。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_row_seqlock --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::atomic::{fence, AtomicU32, AtomicU64, Ordering};
use loom::sync::Arc;

#[derive(Clone, Copy)]
struct Fences {
    writer: bool,
    reader: bool,
}

struct Stripe {
    ver: AtomicU64,
    a: AtomicU32,
    b: AtomicU32,
}

impl Stripe {
    fn write(&self, v: u32, f: Fences) {
        loop {
            let cur = self.ver.load(Ordering::Acquire);
            if cur & 1 == 0 && self.ver.compare_exchange_weak(cur, cur + 1, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                break;
            }
            loom::thread::yield_now();
        }
        if f.writer {
            fence(Ordering::Release);
        }
        self.a.store(v, Ordering::Relaxed);
        self.b.store(v, Ordering::Relaxed);
        self.ver.fetch_add(1, Ordering::Release);
    }

    fn read(&self, f: Fences) -> (u32, u32) {
        loop {
            let v = self.ver.load(Ordering::Acquire);
            if v & 1 == 1 {
                loom::thread::yield_now();
                continue;
            }
            let got = (self.a.load(Ordering::Relaxed), self.b.load(Ordering::Relaxed));
            if f.reader {
                fence(Ordering::Acquire);
            }
            if self.ver.load(Ordering::Relaxed) == v {
                return got;
            }
            loom::thread::yield_now();
        }
    }
}

fn check(f: Fences) {
    loom::model(move || {
        let s = Arc::new(Stripe { ver: AtomicU64::new(0), a: AtomicU32::new(0), b: AtomicU32::new(0) });
        let writer = {
            let s = s.clone();
            loom::thread::spawn(move || s.write(1, f))
        };
        let got = s.read(f);
        assert!(got == (0, 0) || got == (1, 1), "書き込みの途中の組を採った: {got:?}");
        writer.join().unwrap();
    });
}

#[test]
fn a_reader_never_takes_half_a_write() {
    check(Fences { writer: true, reader: true });
}

/// 対照: 読み手の `fence(Acquire)` が無いと、 途中の組を採る。
#[test]
#[should_panic(expected = "途中の組")]
fn without_the_readers_fence_a_reader_takes_half_a_write() {
    check(Fences { writer: true, reader: false });
}

/// 対照: 書き手の `fence(Release)` が無いと、 途中の組を採る。
#[test]
#[should_panic(expected = "途中の組")]
fn without_the_writers_fence_a_reader_takes_half_a_write() {
    check(Fences { writer: false, reader: true });
}
