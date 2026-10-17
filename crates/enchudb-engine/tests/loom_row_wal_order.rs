//! loom model — 同じ cell の書き手の 「WAL に積む順」 と 「本体に当てる順」 (#503)。
//!
//! ## 何を守っているのか
//! ローカルの書き込みは WAL に積んで HLC を振り (append の lock の下 = WAL の順 = HLC の順)、 行の lock の下で本体に当てる。
//! 版数を持たない DB (sync の表なし) は後から当てた方が残るので、 同じ cell の書き手どうしで積む順と当てる順がそろって
//! いないと、 メモリの値が WAL の最後の record と食い違う (開き直した後・change listener・相手が見る値と違う)。
//!
//! 今の形: 行の lock を握ってから積み、 当て終えてから離す (`Engine::append_local_op` の doc)。 旧い形 (積んでから行の lock を
//! 握る) では loom が窓を見つけることも確かめる (model が窓を表現できている確認)。 版数を持つ DB は、 旧い形でも版数の比べで
//! 古い方を弾いて合わせていた — その代わり、 ローカルの書き込みが弾かれた (`Err(OlderThanCell)` と 「clock went backwards?」 の
//! 誤報)。 本体の cell は `UnsafeCell` で置き、 行の lock の外で触ると loom が data race として落とす。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_row_wal_order --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::cell::UnsafeCell;
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::sync::{Arc, Mutex};

#[derive(Clone, Copy, PartialEq)]
enum Order {
    /// 今の形: 行の lock → WAL に積む → 当てる → 離す
    RowFirst,
    /// 旧い形: WAL に積む → 行の lock → 当てる → 離す
    WalFirst,
}

struct Model {
    order: Order,
    /// 版数を持つ DB (sync の表あり) か
    versioned: bool,
    row: Mutex<()>,
    /// WAL の record の値。 位置 + 1 が HLC (append の lock の下で振る)
    wal: Mutex<Vec<u32>>,
    /// 本体の cell (値, 版数)。 行の lock の下でだけ触る
    cell: UnsafeCell<(u32, usize)>,
    rejected: AtomicUsize,
}

// SAFETY: `cell` は行の lock の下でだけ触る (loom が UnsafeCell の触り方を追って race を検査する)。
unsafe impl Sync for Model {}
unsafe impl Send for Model {}

impl Model {
    fn new(order: Order, versioned: bool) -> Self {
        Self {
            order,
            versioned,
            row: Mutex::new(()),
            wal: Mutex::new(Vec::new()),
            cell: UnsafeCell::new((0, 0)),
            rejected: AtomicUsize::new(0),
        }
    }

    /// WAL に積んで HLC を返す。
    fn append(&self, v: u32) -> usize {
        let mut wal = self.wal.lock().unwrap();
        wal.push(v);
        wal.len()
    }

    /// 行の lock の下で本体に当てる。 版数を持つ DB は、 cell の版数より古い HLC を弾く。
    fn apply(&self, v: u32, hlc: usize) {
        self.cell.with_mut(|c| unsafe {
            if self.versioned && hlc < (*c).1 {
                self.rejected.fetch_add(1, Ordering::Relaxed);
                return;
            }
            *c = (v, hlc);
        });
    }

    fn write(&self, v: u32) {
        match self.order {
            Order::RowFirst => {
                let _row = self.row.lock().unwrap();
                let hlc = self.append(v);
                self.apply(v, hlc);
            }
            Order::WalFirst => {
                let hlc = self.append(v);
                let _row = self.row.lock().unwrap();
                self.apply(v, hlc);
            }
        }
    }
}

/// 書き手 2 本が同じ cell に 1 回ずつ書く。 書き終えた後、 メモリの値が WAL の最後の record と同じ。 `no_rejects` なら、
/// ローカルの書き込みが弾かれていないことも見る。
fn check(order: Order, versioned: bool, no_rejects: bool) {
    loom::model(move || {
        let m = Arc::new(Model::new(order, versioned));
        let writers: Vec<_> = (1..=2u32)
            .map(|v| {
                let m = m.clone();
                loom::thread::spawn(move || m.write(v))
            })
            .collect();
        for w in writers {
            w.join().unwrap();
        }
        let last = *m.wal.lock().unwrap().last().unwrap();
        let _row = m.row.lock().unwrap();
        let (value, _) = m.cell.with(|c| unsafe { *c });
        assert_eq!(value, last, "メモリの値が WAL の最後の record と違う");
        if no_rejects {
            assert_eq!(m.rejected.load(Ordering::Relaxed), 0, "ローカルの書き込みを版数で弾いた");
        }
    });
}

/// 今の形: 版数を持たない DB でも、 最後の値は WAL の最後の record。
#[test]
fn row_first_ends_on_the_last_wal_record() {
    check(Order::RowFirst, false, true);
}

/// 今の形: 版数を持つ DB で、 ローカルの書き込みを弾かない。
#[test]
fn row_first_with_versions_rejects_no_local_write() {
    check(Order::RowFirst, true, true);
}

/// 対照: 旧い形は、 版数を持たない DB で古い方を残す。
#[test]
#[should_panic(expected = "WAL の最後の record と違う")]
fn wal_first_leaves_an_older_value() {
    check(Order::WalFirst, false, false);
}

/// 旧い形でも、 版数を持つ DB は古い方を弾いて合わせる (sync の表のある DB でずれなかった理由)。
#[test]
fn wal_first_with_versions_still_ends_on_the_last_wal_record() {
    check(Order::WalFirst, true, false);
}

/// 対照: その代わり、 旧い形はローカルの書き込みを弾く (誤報の 「clock went backwards?」)。
#[test]
#[should_panic(expected = "版数で弾いた")]
fn wal_first_with_versions_rejects_a_local_write() {
    check(Order::WalFirst, true, true);
}
