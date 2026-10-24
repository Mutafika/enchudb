//! loom model — WAL のまとめ書きの置き場 (`OpLog::lock_or_request` / `append_by_request` / `serve_slots`、 #506)。
//!
//! ## 何を守っているのか
//! `append_lock` を取れなかった書き手は、 自分の置き場 (thread ごとに 1 つ、 持ち主は自分だけ) に頼みを書いて印を PENDING にし
//! (Release)、 「頼みが置かれているかもしれない」 印 (`wanted`) が倒れていれば立てる (Release)。 lock を持つ書き手は自分の分を積んだ後、
//! `wanted` が立っていれば (Acquire) 全部の置き場の印を Relaxed で読んでから fence(Acquire) を 1 回打ち、 PENDING だった置き場の頼みを
//! 読んで積み、 結果を書いてから印を DONE にする (Release)。 何も無ければ `wanted` を倒す。 頼んだ書き手は DONE を見て (Acquire) 結果を
//! 取り、 印を EMPTY に戻して次の頼みに使い回す。 待つ間に lock を取れたら、 積まれていれば結果を取り、 まだなら頼みを下げて自分で積む。
//!
//! 確かめること: どの頼みも 1 回だけ積まれ、 頼んだ書き手は自分の record の場所を受け取る。 頼みと結果の受け渡しに data race が
//! 無い (置き場の使い回しを含む)。 対照 4 本: 結果より先に DONE にする形、 PENDING を Relaxed で置く形、 印を読んだ後の fence を
//! 打たない形、 lock を取った頼み手が積まれたかを見ずに頼みを下げる形を loom が見つける。
//!
//! model の lock は loom の atomic の spin lock (try_lock = CAS 1 回、 lock = 取れるまで yield)。 loom の `Mutex::try_lock` は他の
//! thread の操作と依存しない分岐として扱われ、 try_lock が失敗する並び (= 頼みを置く道) を loom が辿らない (最初の形は 11 通りしか
//! 辿らず、 頼みを 1 度も置かなかった)。 待つ書き手の回り待ち (DONE を見る・時々 lock を試す) は、 「1 回見る → DONE でなければ lock を
//! 待つ」 に置き換える (回り待ちのままだと loom の枝が増えすぎて終わらない)。
//!
//! ## 実行
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test -p enchudb-engine --test loom_wal_combine_slots --release
//! ```
//! 通常の `cargo test` では `#![cfg(loom)]` で空 build。

#![cfg(loom)]

use loom::cell::UnsafeCell;
use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering, fence};

const EMPTY: u32 = 0;
const PENDING: u32 = 1;
const DONE: u32 = 2;

#[derive(Clone, Copy, PartialEq)]
enum Bug {
    None,
    /// 結果を書く前に DONE にする
    DoneBeforeResult,
    /// 頼みを書いた後の PENDING を Relaxed で置く
    RelaxedPublish,
    /// 印を Relaxed で読んだ後の fence(Acquire) を打たない
    NoFence,
    /// lock を取った頼み手が、 積まれたかを見ずに頼みを下げて自分で積む
    WithdrawWithoutCheck,
}

struct Slot {
    state: AtomicU32,
    req: UnsafeCell<u32>,
    result: UnsafeCell<Option<usize>>,
}

struct Wal {
    locked: AtomicBool,
    wanted: AtomicBool,
    slots: Vec<Slot>,
    /// 積んだ record (lock の下でだけ触る)
    log: UnsafeCell<Vec<u32>>,
    /// 頼みとして積まれた数 (model が頼みの道を本当に辿ったかの確かめ)
    served: AtomicUsize,
    bug: Bug,
}

// SAFETY: `log` は lock の下で、 置き場の `req` / `result` は印の受け渡しの順でだけ触る (loom が UnsafeCell の触り方を追って
// race を検査する)。
unsafe impl Sync for Wal {}
unsafe impl Send for Wal {}

impl Wal {
    fn new(writers: usize, bug: Bug) -> Self {
        Self {
            locked: AtomicBool::new(false),
            wanted: AtomicBool::new(false),
            slots: (0..writers)
                .map(|_| Slot { state: AtomicU32::new(EMPTY), req: UnsafeCell::new(0), result: UnsafeCell::new(None) })
                .collect(),
            log: UnsafeCell::new(Vec::new()),
            served: AtomicUsize::new(0),
            bug,
        }
    }

    fn try_lock(&self) -> bool {
        self.locked.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok()
    }

    fn lock(&self) {
        while !self.try_lock() {
            loom::thread::yield_now();
        }
    }

    fn unlock(&self) {
        self.locked.store(false, Ordering::Release);
    }

    /// lock を持って呼ぶ
    fn append_locked(&self, v: u32) -> usize {
        self.log.with_mut(|l| unsafe {
            (*l).push(v);
            (*l).len() - 1
        })
    }

    /// lock を持って呼ぶ。 本物と同じ: 印が立っていれば置き場の印を全部読んでから fence を 1 回打ち、 PENDING だった置き場を積む
    fn serve(&self) {
        if !self.wanted.load(Ordering::Acquire) {
            return;
        }
        let pending: Vec<usize> =
            self.slots.iter().enumerate().filter(|(_, s)| s.state.load(Ordering::Relaxed) == PENDING).map(|(i, _)| i).collect();
        if pending.is_empty() {
            self.wanted.store(false, Ordering::Relaxed);
            return;
        }
        if self.bug != Bug::NoFence {
            fence(Ordering::Acquire);
        }
        for i in pending {
            let s = &self.slots[i];
            let v = s.req.with(|r| unsafe { *r });
            let at = self.append_locked(v);
            self.served.fetch_add(1, Ordering::Relaxed);
            if self.bug == Bug::DoneBeforeResult {
                s.state.store(DONE, Ordering::Release);
                s.result.with_mut(|r| unsafe { *r = Some(at) });
            } else {
                s.result.with_mut(|r| unsafe { *r = Some(at) });
                s.state.store(DONE, Ordering::Release);
            }
        }
    }

    /// 置き場の結果を取って空に戻す
    fn take(&self, s: &Slot) -> Option<usize> {
        (s.state.load(Ordering::Acquire) == DONE).then(|| {
            let at = s.result.with_mut(|r| unsafe { (*r).take() }).expect("結果が無い");
            s.state.store(EMPTY, Ordering::Relaxed);
            at
        })
    }

    /// 書き手 `me` (置き場の持ち主) が `v` を積み、 record の位置を返す
    fn append(&self, me: usize, v: u32) -> usize {
        if self.try_lock() {
            let at = self.append_locked(v);
            self.serve();
            self.unlock();
            return at;
        }
        let s = &self.slots[me];
        s.req.with_mut(|r| unsafe { *r = v });
        s.state.store(PENDING, if self.bug == Bug::RelaxedPublish { Ordering::Relaxed } else { Ordering::Release });
        if !self.wanted.load(Ordering::Relaxed) {
            self.wanted.store(true, Ordering::Release);
        }
        if let Some(at) = self.take(s) {
            return at;
        }
        self.lock();
        if self.bug != Bug::WithdrawWithoutCheck
            && let Some(at) = self.take(s)
        {
            self.serve();
            self.unlock();
            return at;
        }
        // 頼みを下げて自分で積む (lock を持つのは自分だけ)
        s.state.store(EMPTY, Ordering::Relaxed);
        let at = self.append_locked(v);
        self.serve();
        self.unlock();
        at
    }
}

/// 書き手 2 本が 2 回ずつ積む (2 回目は置き場の使い回し)。 書き手 0 は main thread。 頼みとして積まれた数の最大を返す。
fn check(bug: Bug) -> usize {
    check_with(bug, 4)
}

/// `check` の preemption の上限を変える形
fn check_with(bug: Bug, preemptions: usize) -> usize {
    let most = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let m = most.clone();
    let mut b = loom::model::Builder::new();
    b.preemption_bound = Some(preemptions);
    b.check(move || {
        let w = Arc::new(Wal::new(2, bug));
        let other = {
            let w = w.clone();
            loom::thread::spawn(move || [w.append(1, 10), w.append(1, 11)])
        };
        let mine = [w.append(0, 20), w.append(0, 21)];
        let theirs = other.join().unwrap();
        w.lock();
        let log = w.log.with(|l| unsafe { (*l).clone() });
        let mut sorted = log.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, vec![10, 11, 20, 21], "どの頼みも 1 回だけ積む: {log:?}");
        for (at, v) in mine.iter().zip([20, 21]).chain(theirs.iter().zip([10, 11])) {
            assert_eq!(log.get(*at), Some(&v), "書き手が受け取った場所の record が、 その書き手の値でない");
        }
        assert!(w.slots.iter().all(|s| s.state.load(Ordering::Relaxed) == EMPTY), "置き場が空に戻る");
        m.fetch_max(w.served.load(Ordering::Relaxed), std::sync::atomic::Ordering::Relaxed);
        w.unlock();
    });
    most.load(std::sync::atomic::Ordering::Relaxed)
}

/// panic の文
fn panic_message(f: impl FnOnce() + std::panic::UnwindSafe) -> String {
    let e = std::panic::catch_unwind(f).expect_err("loom が見つけなかった");
    e.downcast_ref::<String>().cloned().or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string())).unwrap_or_default()
}

/// 今の形: どの頼みも 1 回だけ積まれ、 頼んだ書き手は自分の場所を受け取る。 受け渡しに race が無い。 model が他の書き手に積まれる道
/// (頼みの道) を本当に辿ったことも見る。
#[test]
fn slots_hand_over_requests_and_results() {
    assert!(check(Bug::None) > 0, "頼みとして積まれる並びを 1 つも辿っていない");
}

/// 対照: 結果より先に DONE にすると、 頼んだ書き手が結果の無いうちに読む (か、 結果の受け渡しが data race になる)。 窓に入るには
/// 持ち手が lock の中で 1 回、 頼み手が頼んだ直後に 1 回、 持ち手が DONE と結果の間で 1 回止まる (上限 3 では見つからなかった)
#[test]
fn done_before_result_is_caught() {
    let msg = panic_message(|| {
        check(Bug::DoneBeforeResult);
    });
    assert!(msg.contains("結果が無い") || msg.contains("Causality violation"), "別の理由で落ちた: {msg}");
}

/// 対照: PENDING を Relaxed で置くと、 積む書き手が頼みの中身を書き終わる前に読みうる。
#[test]
#[should_panic(expected = "Causality violation")]
fn relaxed_publish_is_caught() {
    check(Bug::RelaxedPublish);
}

/// 対照: 印を Relaxed で読んだ後の fence を打たないと、 頼みの中身を読む順が保証されない。
#[test]
#[should_panic(expected = "Causality violation")]
fn missing_fence_is_caught() {
    check(Bug::NoFence);
}

/// 対照: lock を取った頼み手が積まれたかを見ずに頼みを下げると、 前の持ち手が積んだ頼みを自分でも積む (2 回載る)。
#[test]
#[should_panic(expected = "1 回だけ積む")]
fn withdrawing_a_served_request_is_caught() {
    check(Bug::WithdrawWithoutCheck);
}
