//! loom model — #873 のレビュー: WAL に積んで本体に当てる自分の書き込みが積む前に通る門 (`write_gate::IntentGate`) で、 互いに
//! 待って止まらない。
//!
//! 書き手は積む前に、 書く紐の門に入る (閉じていれば、 止める側が終わるのを `closer` で待って入り直す)。 入っている間に当てる
//! (当てる間に、 その紐の `write_lock` を少し取ることがある = sparse に触る / 配列を伸ばす書き手)。 紐を長く止める側 (円柱を組む /
//! 並びの索引を作る / 列を数える) は `closer` を取り、 門を閉じて中の書き手が出るのを待ってから `write_lock` を取る。 並びの索引を
//! 作る側は 2 本の紐 (via → key) を順に止める。
//!
//! 守る性質: **どの順に走っても全部の thread が終わる** (互いに待って止まらない)。 いくつもの紐に入る書き手 (行の削除) は、
//! 1 つでも閉じていれば入った分を出てから待つ (`Engine::local_apply_row`)。
//!
//! 対照は loom が止まりを見つける: 削除が入ったまま次の紐を待つ形 (止める側が via を止めて key を待つ間、 削除は key に入って
//! via を待つ)、 止める側が門を閉じる前に `write_lock` を取る形 (中の書き手が当てる間に `write_lock` を待つ)。 止まった形では、
//! 止める側が中の書き手を待って回り続ける (他の thread は lock を待つ) ので、 loom は branch の上限 ("Model exceeded maximum number of
//! branches") で落ちる (手元で、 落ちた所が `closer` / `write_lock` の lock であることを見た)。
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

use loom::sync::atomic::{AtomicU32, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

const CLOSED: u32 = 1 << 31;

/// 紐 1 本: 書き手の門 (shard 1 つ) と、 止める側の `closer`、 紐の `write_lock`。
struct Column {
    gate: AtomicU32,
    closer: Mutex<()>,
    write_lock: Mutex<()>,
}

impl Column {
    fn new() -> Self {
        Self { gate: AtomicU32::new(0), closer: Mutex::new(()), write_lock: Mutex::new(()) }
    }

    fn try_enter(&self) -> bool {
        if self.gate.fetch_add(1, Ordering::Acquire) & CLOSED == 0 {
            true
        } else {
            self.gate.fetch_sub(1, Ordering::Relaxed);
            false
        }
    }

    fn exit(&self) {
        self.gate.fetch_sub(1, Ordering::Release);
    }

    fn wait(&self) {
        drop(self.closer.lock().unwrap());
    }

    fn enter(&self) {
        while !self.try_enter() {
            self.wait();
        }
    }

    /// 止める側: `closer` を取り、 門を閉じて中が出るのを待つ。 `write_lock_first` は対照 (閉じる前に `write_lock` を取る)。
    fn hold(&self, write_lock_first: bool) -> Held<'_> {
        let closer = self.closer.lock().unwrap();
        let early = write_lock_first.then(|| self.write_lock.lock().unwrap());
        self.gate.fetch_or(CLOSED, Ordering::AcqRel);
        while self.gate.load(Ordering::Acquire) & !CLOSED != 0 {
            thread::yield_now();
        }
        let lock = early.unwrap_or_else(|| self.write_lock.lock().unwrap());
        Held { col: self, _lock: Some(lock), _closer: Some(closer) }
    }
}

struct Held<'a> {
    col: &'a Column,
    _lock: Option<loom::sync::MutexGuard<'a, ()>>,
    _closer: Option<loom::sync::MutexGuard<'a, ()>>,
}

impl Drop for Held<'_> {
    fn drop(&mut self) {
        drop(self._lock.take());
        self.col.gate.fetch_and(!CLOSED, Ordering::Release);
        drop(self._closer.take());
    }
}

/// 当てる: 紐の `write_lock` を少し取る (sparse に触る書き手)。
fn apply(c: &Column) {
    drop(c.write_lock.lock().unwrap());
}

#[derive(Clone, Copy)]
struct Shape {
    /// 削除が、 1 つでも閉じていれば入った分を出てから待つ (正しい形)
    delete_backs_off: bool,
    /// 止める側が門を閉じる前に `write_lock` を取る (対照)
    write_lock_first: bool,
}

fn model(shape: Shape) {
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(2);
    b.check(move || {
        let (via, key) = (Arc::new(Column::new()), Arc::new(Column::new()));
        // 並びの索引を作る側: via → key の順に止める
        let builder = {
            let (via, key) = (via.clone(), key.clone());
            thread::spawn(move || {
                let _v = via.hold(shape.write_lock_first);
                let _k = key.hold(shape.write_lock_first);
            })
        };
        // 行の削除: key と via の両方に入る (紐の番号の順 = key → via。 止める側と逆の順)
        let delete = {
            let (via, key) = (via.clone(), key.clone());
            thread::spawn(move || {
                if shape.delete_backs_off {
                    loop {
                        if !key.try_enter() {
                            key.wait();
                            continue;
                        }
                        if !via.try_enter() {
                            key.exit();
                            via.wait();
                            continue;
                        }
                        break;
                    }
                } else {
                    key.enter();
                    via.enter();
                }
                apply(&key);
                apply(&via);
                key.exit();
                via.exit();
            })
        };
        // via 1 本の書き手
        let writer = {
            let via = via.clone();
            thread::spawn(move || {
                via.enter();
                apply(&via);
                via.exit();
            })
        };
        builder.join().unwrap();
        delete.join().unwrap();
        writer.join().unwrap();
    });
}

/// 正しい形: どの順でも止まらない。
#[test]
fn writers_and_long_holders_never_wait_on_each_other() {
    model(Shape { delete_backs_off: true, write_lock_first: false });
}

/// 対照: 削除が入ったまま次の紐を待つと止まる (止める側が via を止めて key の中を待つ間、 削除は key に居て via を待つ)。
#[test]
#[should_panic]
fn control_a_delete_that_waits_while_inside_deadlocks() {
    model(Shape { delete_backs_off: false, write_lock_first: false });
}

/// 対照: 止める側が門を閉じる前に `write_lock` を取ると止まる (中の書き手が当てる間に `write_lock` を待つ)。
#[test]
#[should_panic]
fn control_a_holder_that_takes_the_write_lock_first_deadlocks() {
    model(Shape { delete_backs_off: true, write_lock_first: true });
}
