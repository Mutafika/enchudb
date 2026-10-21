//! loom model — 行の lock と seqlock (`row_lock.rs`) の **code そのもの** を全 interleaving で走らせる (#563)。
//!
//! `loom_row_seqlock.rs` / `loom_row_wal_order.rs` は考え方を自前の lock / fence で模した model で、 `row_lock.rs` を変えても落ちない
//! (例: 握った後の `fence(Release)` を消しても通った)。 ここは `--cfg loom` で loom の atomic に差し替えた `RowLocks` を使う
//! (stripe は 2 本 = 行 0 と 2 が同じ stripe、 読み直しは 2 回で握って読む道に入る)。
//!
//! - 同じ行の書き手は重ならない (書いている間の中身を loom の `UnsafeCell` に置き、 重なれば data race として落ちる)
//! - 読み手は書き込みの途中の組を採らない (書き手が 2 つの値を書く。 揃わない組を返せば落ちる)
//! - 同じ thread は同じ stripe をもう一度握れる (行 0 を握ったまま行 2 を書く)
//!
//! 実行: `RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_row_lock --release`

#![cfg(loom)]

use enchudb_engine::row_lock::RowLocks;
use loom::cell::UnsafeCell;
use loom::sync::atomic::{AtomicU64, Ordering};
use loom::sync::Arc;

/// 同じ行の書き手 2 本は重ならない。 握っている間だけ触る中身を `UnsafeCell` に置くので、 lock が排他と happens-before を
/// 作らなければ loom が data race を見つける。
#[test]
fn writers_of_one_row_do_not_overlap() {
    loom::model(|| {
        let locks = Arc::new(RowLocks::new());
        let cell = Arc::new(UnsafeCell::new(0u32));
        let hs: Vec<_> = (0..2)
            .map(|_| {
                let (locks, cell) = (locks.clone(), cell.clone());
                loom::thread::spawn(move || {
                    let _w = locks.write(0);
                    cell.with_mut(|p| unsafe { *p += 1 });
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        let _w = locks.write(0);
        assert_eq!(cell.with(|p| unsafe { *p }), 2);
    });
}

/// 読み手は書き込みの途中の組を採らない: 書き手は行 0 を握って 2 つの値を順に書く。 読み手 (`read`) が返す組は (0, 0) か (1, 1)。
#[test]
fn a_reader_never_takes_a_half_written_row() {
    loom::model(|| {
        let locks = Arc::new(RowLocks::new());
        let a = Arc::new(AtomicU64::new(0));
        let b = Arc::new(AtomicU64::new(0));
        let writer = {
            let (locks, a, b) = (locks.clone(), a.clone(), b.clone());
            loom::thread::spawn(move || {
                let _w = locks.write(0);
                a.store(1, Ordering::Relaxed);
                b.store(1, Ordering::Relaxed);
            })
        };
        let got = locks.read(0, || (a.load(Ordering::Relaxed), b.load(Ordering::Relaxed)));
        assert!(got == (0, 0) || got == (1, 1), "書き込みの途中の組を読んだ: {got:?}");
        writer.join().unwrap();
    });
}

/// 同じ thread は同じ stripe をもう一度握れる (行 0 と 2 は同じ stripe)。 別の thread の書き手とも重ならない。
#[test]
fn the_same_thread_can_take_a_stripe_it_holds() {
    loom::model(|| {
        let locks = Arc::new(RowLocks::new());
        let cell = Arc::new(UnsafeCell::new(0u32));
        let other = {
            let (locks, cell) = (locks.clone(), cell.clone());
            loom::thread::spawn(move || {
                let _w = locks.write(2);
                cell.with_mut(|p| unsafe { *p += 1 });
            })
        };
        {
            let _outer = locks.write(0);
            assert!(locks.held_by_me(2), "同じ stripe の行を握っていると見えない");
            let _inner = locks.write(2);
            cell.with_mut(|p| unsafe { *p += 10 });
        }
        other.join().unwrap();
        let _w = locks.write(0);
        assert_eq!(cell.with(|p| unsafe { *p }), 11);
    });
}
