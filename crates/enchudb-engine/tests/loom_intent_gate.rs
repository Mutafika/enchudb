//! loom model — #873 のレビュー / #1016: WAL に積んで本体に当てる自分の書き込みの、 紐ごとの門 (`write_gate::IntentGate`)。
//!
//! #1016 の形: 門は WAL に積む時に確かめる。 書き手は WAL の門の世代を (Acquire で) 読み、 書く紐の門が開いているのを見て、
//! `append_lock` の下で世代が変わっていなければ積む (積んだ位置を印に置く)。 当てる間に紐の `write_lock` を少し取ることがある (sparse に
//! 触る / 配列を伸ばす書き手)。 当て終えたら印を外す。 紐を長く止める側 (円柱を組む / 並びの索引を作る / 列を数える) は `closer` を
//! 取り、 門の印を立て、 `append_lock` の下で世代を進めてその時の終端 P を読み、 P より前に積まれた書き込みの印が全部外れるのを待って
//! から `write_lock` を取って列をなめる。 並びの索引を作る側は 2 本の紐 (via → key) を順に止める。 行の削除は 2 本の紐を書く (両方の
//! 門を見る)。 前の形 (紐ごと・shard ごとの数を書き手が足して引く) は書き込み 1 回に RMW 2 回だった。
//!
//! 守る性質:
//! - **止める側が列をなめている間、 その紐を書く書き手は印に位置を持たない** (持っていると、 その書き手は `write_lock` を待ち、
//!   checkpoint がその record の手前で止まって WAL を畳めない = #873 のレビューの止まり方)
//! - **どの順に走っても全部の thread が終わる** (互いに待って止まらない)
//!
//! 対照は loom が窓 / 止まりを見つける: 書き手が積む時に世代を確かめない形、 止める側が `append_lock` の外で世代を進める形、 書き手が
//! 世代を Relaxed で読む形 (門の印が見えない)、 止める側が印を待たない形、 止める側が待つ前に `write_lock` を取る形 (止まる。 loom は
//! branch の上限で落ちる)。
//!
//! 模型は考え方の写し (実装の行を変えても落ちない)。 実装は engine の `apply_liveness_tests` (止める側が持つ間に書き手 A が積む前で
//! 待ち、 別の書き手 B が待たされない) と `write_gate` の単体試験が踏む。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_intent_gate --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

const IDLE: u64 = u64::MAX;

#[derive(Clone, Copy)]
struct Shape {
    /// 書き手が `append_lock` の下で世代を確かめる (正しい形)
    epoch_check: bool,
    /// 止める側が `append_lock` の下で世代を進める (正しい形)
    bump_under_lock: bool,
    /// 書き手が世代を読む順序 (Acquire が正しい)
    epoch_read: Ordering,
    /// 止める側が、 世代を進めた時の終端より前の印が外れるのを待つ (正しい形)
    wait_marks: bool,
    /// 止める側が待つ前に `write_lock` を取る (対照)
    write_lock_first: bool,
}

const RIGHT: Shape =
    Shape { epoch_check: true, bump_under_lock: true, epoch_read: Ordering::Acquire, wait_marks: true, write_lock_first: false };

/// WAL: `append_lock` の下で世代を確かめて積む。
struct Wal {
    append_lock: Mutex<()>,
    /// 門の世代
    gate: AtomicU32,
    /// 書き終えた終端 (= 次に積む位置)
    written: AtomicU64,
    /// 書き手ごとの印 (0 = 書き手 W、 1 = 行の削除 D)
    marks: [AtomicU64; 2],
}

/// 紐 1 本: 門の印、 止める側の `closer`、 紐の `write_lock`、 止める側が列をなめている印。
struct Column {
    closed: AtomicU32,
    closer: Mutex<()>,
    write_lock: Mutex<()>,
    scanning: AtomicBool,
}

impl Column {
    fn new() -> Self {
        Self { closed: AtomicU32::new(0), closer: Mutex::new(()), write_lock: Mutex::new(()), scanning: AtomicBool::new(false) }
    }
}

impl Wal {
    fn new() -> Self {
        Self {
            append_lock: Mutex::new(()),
            gate: AtomicU32::new(0),
            written: AtomicU64::new(0),
            marks: [AtomicU64::new(IDLE), AtomicU64::new(IDLE)],
        }
    }

    /// 書き手 `me` が `cols` を書く 1 件を積む (積めたら印に位置がある)。
    fn append(&self, s: Shape, me: usize, cols: &[&Column]) {
        loop {
            let e = self.gate.load(s.epoch_read);
            if let Some(c) = cols.iter().find(|c| c.closed.load(Ordering::Relaxed) != 0) {
                drop(c.closer.lock().unwrap());
                continue;
            }
            let g = self.append_lock.lock().unwrap();
            if s.epoch_check && self.gate.load(Ordering::Relaxed) != e {
                drop(g);
                continue;
            }
            let pos = self.written.load(Ordering::Relaxed);
            self.marks[me].store(pos, Ordering::Relaxed);
            self.written.store(pos + 1, Ordering::Release);
            drop(g);
            return;
        }
    }

    /// 止める側: 門の印を立てて世代を進め、 その時の終端を返す。
    fn close(&self, s: Shape, c: &Column) -> u64 {
        c.closed.store(1, Ordering::Relaxed);
        let g = s.bump_under_lock.then(|| self.append_lock.lock().unwrap());
        let e = self.gate.load(Ordering::Relaxed);
        self.gate.store(e + 1, Ordering::Release);
        let p = self.written.load(Ordering::Acquire);
        drop(g);
        p
    }

    /// 終端 `p` より前の印が外れる (か `p` 以降の位置になる) まで待つ。
    fn wait_before(&self, p: u64) {
        while self.marks.iter().any(|m| m.load(Ordering::Acquire) < p) {
            thread::yield_now();
        }
    }
}

/// 止める側が持つ間。
struct Held<'a> {
    col: &'a Column,
    _lock: Option<loom::sync::MutexGuard<'a, ()>>,
    _closer: Option<loom::sync::MutexGuard<'a, ()>>,
}

impl Drop for Held<'_> {
    fn drop(&mut self) {
        self.col.scanning.store(false, Ordering::SeqCst);
        drop(self._lock.take());
        self.col.closed.store(0, Ordering::Release);
        drop(self._closer.take());
    }
}

/// 止める側: `closer` を取り、 門を閉じて中を待ってから `write_lock` を取って列をなめる。 なめている間、 その紐を書く書き手 (`writers`)
/// の印に位置が無いことを確かめる。
fn hold<'a>(wal: &Wal, s: Shape, c: &'a Column, writers: &[usize]) -> Held<'a> {
    let closer = c.closer.lock().unwrap();
    let early = s.write_lock_first.then(|| c.write_lock.lock().unwrap());
    let p = wal.close(s, c);
    if s.wait_marks {
        wal.wait_before(p);
    }
    let lock = early.unwrap_or_else(|| c.write_lock.lock().unwrap());
    c.scanning.store(true, Ordering::SeqCst);
    for &w in writers {
        assert_eq!(wal.marks[w].load(Ordering::SeqCst), IDLE, "止める側が列をなめている間に、 その紐の書き手が積んで当てていない");
    }
    Held { col: c, _lock: Some(lock), _closer: Some(closer) }
}

/// 当てる: 紐の `write_lock` を少し取る (sparse に触る書き手) → 印を外す。
fn apply(wal: &Wal, me: usize, cols: &[&Column]) {
    for c in cols {
        drop(c.write_lock.lock().unwrap());
    }
    wal.marks[me].store(IDLE, Ordering::Release);
}

const W: usize = 0;
const D: usize = 1;

fn model(s: Shape) {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(2);
    b.check(move || {
        let wal = Arc::new(Wal::new());
        let (via, key) = (Arc::new(Column::new()), Arc::new(Column::new()));
        // 並びの索引を作る側: via → key の順に止める (via を書くのは W と D、 key を書くのは D)
        let builder = {
            let (wal, via, key) = (wal.clone(), via.clone(), key.clone());
            thread::spawn(move || {
                let _v = hold(&wal, s, &via, &[W, D]);
                let _k = hold(&wal, s, &key, &[D]);
            })
        };
        // 行の削除: key と via の両方を書く
        let delete = {
            let (wal, via, key) = (wal.clone(), via.clone(), key.clone());
            thread::spawn(move || {
                wal.append(s, D, &[&key, &via]);
                apply(&wal, D, &[&key, &via]);
            })
        };
        // via 1 本の書き手
        let writer = {
            let (wal, via) = (wal.clone(), via.clone());
            thread::spawn(move || {
                wal.append(s, W, &[&via]);
                apply(&wal, W, &[&via]);
            })
        };
        builder.join().unwrap();
        delete.join().unwrap();
        writer.join().unwrap();
    });
}

/// 正しい形: 止める側が列をなめている間、 その紐の書き手は印に位置を持たず、 どの順でも止まらない。
#[test]
fn writers_and_long_holders_never_wait_on_each_other() {
    model(RIGHT);
}

/// 対照: 書き手が積む時に世代を確かめないと、 門を見た後・積む前に止める側が始め、 書き手が積んで当てる間に列をなめる側と重なる。
#[test]
#[should_panic]
fn control_appending_without_the_epoch_check_overlaps_the_scan() {
    model(Shape { epoch_check: false, ..RIGHT });
}

/// 対照: 止める側が `append_lock` の外で世代を進めると、 書き手が確かめた後・積む前に進めて終端を読み、 その後に積まれる。
#[test]
#[should_panic]
fn control_bumping_outside_the_append_lock_overlaps_the_scan() {
    model(Shape { bump_under_lock: false, ..RIGHT });
}

/// 対照: 書き手が世代を Relaxed で読むと、 進んだ世代を読んでも門の印が見えない。
#[test]
#[should_panic]
fn control_a_relaxed_epoch_read_misses_the_closed_gate() {
    model(Shape { epoch_read: Ordering::Relaxed, ..RIGHT });
}

/// 対照: 止める側が、 閉じる前に積まれた書き込みが当て終えるのを待たないと、 その書き手が当てる間に列をなめる。
#[test]
#[should_panic]
fn control_a_holder_that_does_not_wait_for_applying_writers_overlaps_the_scan() {
    model(Shape { wait_marks: false, ..RIGHT });
}

/// 対照: 止める側が待つ前に `write_lock` を取ると止まる (当てている書き手が `write_lock` を待ち、 止める側はその書き手の印を待つ)。
#[test]
#[should_panic]
fn control_a_holder_that_takes_the_write_lock_first_deadlocks() {
    model(Shape { write_lock_first: true, ..RIGHT });
}
