//! loom model — 0.37 の WAL の 2 区画: lock を取らない読み手 (sync の bridge / changefeed) が 2 つの区画の 「書き終えた終端」 を
//! 読む順 (`OpLog::read_plan`)。
//!
//! 書き手 (`append_lock` の下、 1 本) は今の区画に record を書いて終端を置き (Release)、 載らなくなったらもう一方の区画へ移り
//! (今の区画の番号を置く、 Release)、 移った先に書いて終端を置く (Release)。 移る先の周は畳んだ時に置いてあることがある (その時は
//! 移る時に周を書かない = 読み手は周の読みで書き手と同期できない)。
//!
//! 読み手は 2 つの区画の周を読んで古い順に並べ、 **新しい区画の終端を先に** 読んでから古い区画の終端を読み、 古い区画 → 新しい
//! 区画の順に読む。 守る性質: **新しい区画に record が見えたら、 古い区画の終端は最後の値** (古い区画の残りを飛ばして新しい区画を
//! 読み、 位置がその先へ進むと、 その残りは二度と読まれない: changefeed の受け手に区画 1 つ分が届かなかった)。 古い区画の終端を
//! 先に読む形 (`older_end_first_skips_records`) は、 loom が窓を見つける。 新しい区画の終端の読みを Relaxed にした形
//! (`relaxed_newer_end_skips_records`) も同じ。
//!
//! 模型は考え方の写し (実装の行を変えても落ちない)。 実装の窓は `oplog.rs` の
//! `a_reader_does_not_skip_records_added_while_it_reads_the_half_ends` が hook で決まった順に踏む。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_wal_halves --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use loom::sync::Arc;
use loom::thread;

/// 2 つの区画の周と書き終えた終端 (record の数で数える)、 今書き足している区画の番号。
struct Wal {
    lap: [AtomicU32; 2],
    written: [AtomicU64; 2],
    active: AtomicUsize,
}

impl Wal {
    /// A (周 1) が今の区画で record 1 件。 B は畳んだ後 (空、 周 2 = A より新しい)。
    fn new() -> Self {
        Self {
            lap: [AtomicU32::new(1), AtomicU32::new(2)],
            written: [AtomicU64::new(1), AtomicU64::new(0)],
            active: AtomicUsize::new(0),
        }
    }

    /// 書き手: A にもう 1 件足して、 B へ移り (B の周は畳んだ時に置いてあるので書かない)、 B に 1 件。
    fn write(&self) {
        self.written[0].store(2, Ordering::Release);
        self.active.store(1, Ordering::Release);
        self.written[1].store(1, Ordering::Release);
    }

    /// 読み手: (古い区画の番号, 古い区画の終端, 新しい区画の終端)。 `newer_first` = 新しい区画の終端を先に読む (正しい順)。
    fn read(&self, newer_first: bool, newer_order: Ordering) -> (usize, u64, u64) {
        let (la, lb) = (self.lap[0].load(Ordering::Acquire), self.lap[1].load(Ordering::Acquire));
        let (older, newer) = if lb > la { (0, 1) } else { (1, 0) };
        if newer_first {
            let n = self.written[newer].load(newer_order);
            let o = self.written[older].load(Ordering::Acquire);
            (older, o, n)
        } else {
            let o = self.written[older].load(Ordering::Acquire);
            let n = self.written[newer].load(newer_order);
            (older, o, n)
        }
    }
}

fn model(newer_first: bool, newer_order: Ordering) {
    loom::model(move || {
        let wal = Arc::new(Wal::new());
        let w = {
            let wal = wal.clone();
            thread::spawn(move || wal.write())
        };
        let (older, older_end, newer_end) = wal.read(newer_first, newer_order);
        assert_eq!(older, 0, "前提: A が古い区画");
        if newer_end > 0 {
            assert_eq!(older_end, 2, "新しい区画に record が見えたのに、 古い区画の最後の record を飛ばした");
        }
        w.join().unwrap();
    });
}

/// 新しい区画の終端を先に (Acquire) 読めば、 新しい区画に record が見えた時、 古い区画の終端は最後の値。
#[test]
fn newer_end_first_sees_every_record_of_the_older_half() {
    model(true, Ordering::Acquire);
}

/// 対照: 古い区画の終端を先に読むと、 その後に足して移った書き手の、 古い区画の残りを飛ばす (loom が窓を見つける)。
#[test]
#[should_panic]
fn older_end_first_skips_records() {
    model(false, Ordering::Acquire);
}

/// 対照: 新しい区画の終端を先に読んでも Relaxed では、 古い区画の終端の書き込みと同期しない (loom が窓を見つける)。
#[test]
#[should_panic]
fn relaxed_newer_end_skips_records() {
    model(true, Ordering::Relaxed);
}
