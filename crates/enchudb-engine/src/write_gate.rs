//! 円柱を組んでいない紐の書き手の門 (2026-10-11)。
//!
//! 紐の書き手は `HimoStore` の `write_lock` で 1 本ずつ並んでいた。 同じ紐の書き手を増やすと、 合計の書き込みが遅く
//! なった (w1、 宣言も購読も無し: 書き手 1 本 16.4 M 回/秒 → 8 本 4.7 M 回/秒。 書き手の時間の 82% が lock の取り合い)。
//! 円柱を組んでいない紐の書き込みは Column の cell に書くだけ (cell は atomic、 count は fetch_max) なので、 書き手どうしを
//! 並べる理由は無い (同じ cell の書き手は engine の行の lock で並ぶ)。 並べていたのは、 `write_lock` を barrier に使う
//! 3 つの約束のため:
//!
//! - 円柱の遅延構築 (#270、 `HimoStore::ensure_cylinder_built`): 組む前に書いた cell は走査に読まれ、 組んだ後に書く
//!   書き手は自分で円柱に入れる
//! - 並びの索引の構築 (`Engine::order_ready`): 同じ形
//! - 購読の登録の barrier (`crate::live`): barrier より前に書いた cell は初期候補の走査に読まれ、 後に書く書き手は
//!   購読を見る
//!
//! そこで円柱を組んでいない紐の書き手は、 `write_lock` の代わりに門を通る ([`WriteGate::try_enter`])。 3 つの約束の側は
//! `write_lock` を取ってから門を閉じ、 中に居る書き手が出るのを待つ ([`WriteGate::close`])。 閉じた後に来た書き手は
//! `write_lock` の道に回るので、 閉じる側が lock を離すまで待つ (今までと同じ受け渡し)。 円柱を組んだら門は閉じたまま
//! (組んだ円柱の bucket は書き手 1 本の約束なので、 値ごとの鍵の下で手入れする、 [`crate::value_locks`])。 barrier と並びの
//! 索引の構築は、 終わったら開ける ([`WriteGate::open`])。 開けた後に入った書き手は開けた RMW と同期する (開ける前に
//! 置いた購読や索引が見える)。
//!
//! 書き手は shard の数を足してから閉じているかを見る。 閉じる側は印を立ててから数が 0 になるのを待つ。 どちらも同じ
//! word への RMW なので、 word ごとの変更の順でどちらかが必ず先になる (SeqCst の fence は要らない): 書き手が先なら
//! 閉じる側はその書き手が出るまで待ち、 閉じる側が先なら書き手は印を見て `write_lock` の道に回る。
//! [`crate::publish_gate`] と同じ形で、 違いは閉じている時に待たずに `write_lock` の道へ回ることと、 shard を書き手の
//! thread ごとに選ぶこと (書き手どうしが 1 本の cache line を取り合わない)。 loom model:
//! `tests/loom_lazy_cylinder_build.rs` / `tests/loom_order_index_build.rs` / `tests/loom_live_subscribe.rs`。

use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};

/// shard の数 (書き手の thread ごとに 1 つ。 これより多い thread は shard を分け合う)。
const SHARDS: usize = 16;
/// shard の最上位 bit = 閉じている。 残りの bit = 中に居る書き手の数。
const CLOSED: u32 = 1 << 31;

/// 128 B に揃えて shard ごとに cache line を分ける。
#[derive(Default)]
#[repr(align(128))]
struct Shard(AtomicU32);

pub(crate) struct WriteGate {
    shards: Box<[Shard]>,
}

/// この thread が使う shard (全部の門で同じ番号)。 #1016: 中身が定数で始まる thread-local (初めの 1 回だけ番号を引く)。 前の形
/// (初期化の closure を持つ thread-local) は、 読むたびに 「初期化したか」 を見た。
#[inline]
fn shard_index() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static IDX: std::cell::Cell<u32> = const { std::cell::Cell::new(u32::MAX) };
    }
    IDX.with(|c| {
        let i = c.get();
        if i != u32::MAX {
            return i as usize;
        }
        let i = (NEXT.fetch_add(1, Ordering::Relaxed) % SHARDS) as u32;
        c.set(i);
        i as usize
    })
}

impl Default for WriteGate {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteGate {
    /// 開いた門。
    pub(crate) fn new() -> Self {
        Self { shards: (0..SHARDS).map(|_| Shard::default()).collect() }
    }

    /// 開いていれば入り、 出るまで持つ印を返す。 閉じていれば None (呼び手は `write_lock` の道へ)。
    #[inline]
    pub(crate) fn try_enter(&self) -> Option<Pass<'_>> {
        let s = &self.shards[shard_index()].0;
        // Acquire: 開けた側の Release と対 (開ける前に置いた購読や索引が見える)
        if s.fetch_add(1, Ordering::Acquire) & CLOSED == 0 {
            Some(Pass { shard: s })
        } else {
            // 中では何も書いていない
            s.fetch_sub(1, Ordering::Relaxed);
            None
        }
    }

    /// 門を閉じ、 中に居る書き手が出るのを待つ。 `write_lock` の下で呼ぶ (閉じる側は 1 本)。 返った後は、 門の中で書いた
    /// cell が全部見え、 新しく来る書き手は `write_lock` の道に回る。
    pub(crate) fn close(&self) {
        for s in self.shards.iter() {
            s.0.fetch_or(CLOSED, Ordering::AcqRel);
        }
        for s in self.shards.iter() {
            let mut spins = 0u32;
            // Acquire: 出た書き手の Release と対 (その書き手が書いた cell が見える)
            while s.0.load(Ordering::Acquire) & !CLOSED != 0 {
                if spins < 64 {
                    std::hint::spin_loop();
                } else {
                    std::thread::yield_now();
                }
                spins = spins.saturating_add(1);
            }
        }
    }

    /// 門を開ける。 `write_lock` の下で、 円柱を組んでいない時だけ呼ぶ。
    pub(crate) fn open(&self) {
        for s in self.shards.iter() {
            // Release: この後に入った書き手が、 開ける前に置いたもの (購読・索引) を見る
            s.0.fetch_and(!CLOSED, Ordering::Release);
        }
    }

    /// 閉じているか (test 用)。
    #[cfg(test)]
    pub(crate) fn is_closed(&self) -> bool {
        self.shards.iter().all(|s| s.0.load(Ordering::Relaxed) & CLOSED != 0)
    }
}

/// 門の中に居る間持つ。 drop で出る。
pub(crate) struct Pass<'a> {
    shard: &'a AtomicU32,
}

impl Drop for Pass<'_> {
    #[inline]
    fn drop(&mut self) {
        // Release: 閉じる側の Acquire と対 (門の中で書いた cell が見える)
        self.shard.fetch_sub(1, Ordering::Release);
    }
}

/// #850 の後 (#873 のレビュー): WAL に積んで本体に当てる自分の書き込み (engine の `LocalApply`) の、 紐ごとの門。
///
/// 書き手は WAL に積んでから cell に当て終えるまで印 (`OpLog::applying`) を持ち、 書き出しは checkpoint をその手前で切る。 その間に
/// 書き手が長く待つと (同じ紐の円柱を組む・並びの索引を作る・列を数える側が `write_lock` を持って列をなめる間)、 WAL のその区画を
/// 畳めず、 満杯になると全部の書き手が空きを待って、 待ち切れずに record を落とす / 書き込みを断る。 そこで、 紐を長く止める側
/// ([`IntentGate::hold`]) は `write_lock` を取る **前に** この門を閉じ、 中に居る書き手 (積んでから当て終えるまで) が出るのを待つ。
/// 閉じた後に来た書き手は、 積む前に (印に位置を持たずに) 止める側が終わるのを待つ。 中に居る書き手は、 当てる間に止める側を
/// 待たない (止める側はまだ `write_lock` も門も鍵も取っていない)。
///
/// #1016: 門は WAL に積む時に確かめる (前は紐ごと・thread の shard ごとの数を書き込みのたびに足して引いた = RMW 2 回、 列の多い
/// 行の insert で 1 列 6 ns ほど)。
///
/// - 書き手 (engine の `LocalApply::arm`): WAL の門の世代を読み (`OpLog::gate_epoch`、 Acquire)、 書く紐の門が開いているのを見て
///   ([`Self::is_closed`])、 その世代で印を作って積む (`OpLog::applying_gated`)。 積む時 (`append_lock` の下) に世代が変わって
///   いれば積まずに返る (`oplog::GateMoved`) ので、 世代を読み直して門を確かめ直す。 閉じていれば [`Self::wait`] で止める側が
///   終わるのを待つ (印に位置は無い)
/// - 止める側 ([`Self::hold`]): `closer` を取り、 門の印を立ててから、 `append_lock` の下で世代を進め (`OpLog::close_gate`)、 その時
///   までに積まれた当てる書き込みが当て終えるのを待つ (`OpLog::wait_applied_before`)。 世代を進めた後に積もうとする書き手は、 世代の
///   違いで断られるか、 進んだ世代を読んで門の印を見る。 待つのは閉じた紐の書き手に限らない (積まれた当てる書き込み全部、 当てる
///   間は止める側を待たないので必ず終わる)
/// - WAL の空きを待つ書き手は積む前 (印に位置が無い) なので、 止める側はその書き手を待たない (#896 で門を一時出ていた所)
/// - 鍵の順: 行の lock → 門 (止める側は行の lock を取らない)。 止める側は自分の書き込みを当てている途中に呼ばないこと (自分の
///   印を待って止まる。 debug build は panic)
///
/// WAL の無い engine は書き手が門を見ない (印も無い) ので、 止める側は `closer` を取るだけ。 短い barrier (購読の登録の
/// `write_barrier`) はこの門を閉じない (今まで通り、 当てている書き手が少し待つ)。
pub(crate) struct IntentGate {
    /// 閉じている (止める側が持つ間 1)。 書き手は WAL の門の世代を Acquire で読んだ後に読む
    closed: AtomicU32,
    /// 止める側が持つ間持つ (止める側は 1 本ずつ、 閉じられた書き手はこれを待つ)
    closer: parking_lot::Mutex<()>,
    /// この紐を持つ engine の WAL (WAL 付きの engine が付ける、 [`Self::attach_wal`])。 止める側が門の世代を進め、 当てている書き手を
    /// 待つ。 engine より長く生かさない (`Weak`)
    #[cfg(not(target_arch = "wasm32"))]
    wal: std::sync::OnceLock<(std::sync::Weak<enchudb_oplog::oplog::OpLog>, u64)>,
}

impl Default for IntentGate {
    fn default() -> Self {
        Self::new()
    }
}

impl IntentGate {
    pub(crate) fn new() -> Self {
        Self {
            closed: AtomicU32::new(0),
            closer: parking_lot::Mutex::new(()),
            #[cfg(not(target_arch = "wasm32"))]
            wal: std::sync::OnceLock::new(),
        }
    }

    /// この紐の書き手が積む WAL を付ける (engine が WAL を付ける時と、 WAL 付きの engine に紐を足す時。 書き手が来る前に)。 `tag` は
    /// この紐の書き手が印に置く札 (紐の番号、 `OpLog::applying_gated`)。 止める側は、 札がこれか `TAG_ANY` の書き込みだけを待つ。
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn attach_wal(&self, wal: &std::sync::Arc<enchudb_oplog::oplog::OpLog>, tag: u64) {
        let _ = self.wal.set((std::sync::Arc::downgrade(wal), tag));
    }

    /// 閉じているか (書き手: WAL の門の世代を Acquire で読んだ後に見る)。
    #[inline(always)]
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed) != 0
    }

    /// 止める側が終わるのを待つ (止める側は閉じている間ずっと `closer` を持つ)。
    pub(crate) fn wait(&self) {
        drop(self.closer.lock());
    }

    /// 閉じて、 中の書き手 (積んでから当て終えるまでの書き手) が出るのを待つ。 drop で開ける。 `write_lock` を取る前に呼ぶ (中の
    /// 書き手が当てる間に `write_lock` を待つことがある)。 自分の書き込みを当てている途中の thread が呼ぶと止まる (debug build は
    /// panic)。
    pub(crate) fn hold(&self) -> IntentHold<'_> {
        #[cfg(debug_assertions)]
        assert!(
            !applying_here::get(),
            "IntentGate::hold: この thread は自分の書き込みを WAL に積んで当てている途中 (自分の印を待って止まる)"
        );
        let closer = self.closer.lock();
        // 世代を進める (`close_gate` の Release) より前: 進んだ世代を読んだ書き手が見る
        self.closed.store(1, Ordering::Relaxed);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some((wal, tag)) = self.wal.get().and_then(|(w, t)| w.upgrade().map(|w| (w, *t))) {
            let p = wal.close_gate();
            // この紐を書く書き込み (札がこの紐か、 いくつもの紐を書く行の削除) だけを待つ。 前の形 (紐ごとの門の数) と同じく、 別の紐の
            // 書き手が当てる途中で長く待っても (書き出しの門など) 止める側は待たない
            wal.wait_applied_before(p, |t| t == tag || t == enchudb_oplog::oplog::TAG_ANY);
        }
        IntentHold { gate: self, _closer: closer }
    }
}

/// [`IntentGate::hold`] の間持つ。 drop で開ける (その後 `closer` を放す = 待っている書き手が入る)。
pub(crate) struct IntentHold<'a> {
    gate: &'a IntentGate,
    _closer: parking_lot::MutexGuard<'a, ()>,
}

impl Drop for IntentHold<'_> {
    fn drop(&mut self) {
        // Release: 開いたのを見た書き手に、 止めていた間に置いたもの (組んだ円柱・索引) を見せる (書き手はそれぞれの flag でも見る)
        self.gate.closed.store(0, Ordering::Release);
    }
}

/// debug build: この thread が自分の書き込みを WAL に積んで当てている途中か (engine の `LocalApply`。 止める側を自分で待つ形を
/// panic にする)。
#[cfg(debug_assertions)]
pub(crate) mod applying_here {
    use std::cell::Cell;

    thread_local! {
        static N: Cell<u32> = const { Cell::new(0) };
    }

    pub(crate) fn enter() {
        let _ = N.try_with(|n| n.set(n.get() + 1));
    }

    pub(crate) fn exit() {
        let _ = N.try_with(|n| n.set(n.get().saturating_sub(1)));
    }

    pub(super) fn get() -> bool {
        N.try_with(|n| n.get() != 0).unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;
    use std::time::Duration;

    /// 閉じる側は、 中に居る書き手が出るまで返らない。
    #[test]
    fn close_waits_for_writers_inside() {
        let gate = Arc::new(WriteGate::new());
        let inside = gate.try_enter().expect("開いた門に入れない");
        let closed = Arc::new(AtomicBool::new(false));
        let h = {
            let (gate, closed) = (gate.clone(), closed.clone());
            std::thread::spawn(move || {
                gate.close();
                closed.store(true, Ordering::SeqCst);
            })
        };
        std::thread::sleep(Duration::from_millis(50));
        assert!(!closed.load(Ordering::SeqCst), "中に書き手が居るのに閉じ終えた");
        drop(inside);
        h.join().unwrap();
        assert!(closed.load(Ordering::SeqCst));
    }

    /// 閉じている間は入れず (待たずに None)、 開けたらまた入れる。 入れなかった書き手は数に残らない。
    #[test]
    fn closed_gate_turns_writers_away_until_opened() {
        let gate = WriteGate::new();
        assert!(!gate.is_closed());
        gate.close();
        assert!(gate.is_closed());
        assert!(gate.try_enter().is_none(), "閉じた門に入れた");
        // 断られた書き手が数を残していれば、 次の close が待ち続ける
        gate.close();
        gate.open();
        assert!(!gate.is_closed());
        let p = gate.try_enter();
        assert!(p.is_some(), "開けた門に入れない");
    }

    fn tmp_wal(name: &str) -> (String, Arc<enchudb_oplog::oplog::OpLog>) {
        let p = std::env::temp_dir().join(format!("enchu-write-gate-{name}-{}", std::process::id())).to_string_lossy().into_owned();
        let _ = std::fs::remove_file(&p);
        let wal = Arc::new(enchudb_oplog::oplog::OpLog::create(std::path::Path::new(&p), 64 * 1024).unwrap());
        (p, wal)
    }

    /// #1016: 止める側 (`hold`) は、 閉じる前に積まれて当てている途中の書き込み (印に位置がある) が当て終えるまで返らない。 閉じた後に
    /// 積もうとする書き手は、 閉じる前に読んだ世代では積めない (`GateMoved`)。 開けた後は積める。
    #[test]
    fn intent_hold_waits_for_an_applying_writer_and_turns_new_ones_away() {
        use enchudb_oplog::oplog::{is_gate_moved, Op};
        let (p, wal) = tmp_wal("hold");
        let gate = Arc::new(IntentGate::new());
        gate.attach_wal(&wal, 7);
        // 当てている途中の書き手: 積んだ (印に位置がある)、 まだ外していない
        let e0 = wal.gate_epoch();
        let a = wal.applying_gated(e0, 7);
        wal.append_with_hlc_applying(Op::Tie { eid: 1, himo_id: 0, value: 1 }, &a).unwrap();
        let held = Arc::new(AtomicBool::new(false));
        let h = {
            let (gate, held) = (gate.clone(), held.clone());
            std::thread::spawn(move || {
                let hold = gate.hold();
                held.store(true, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(100));
                drop(hold);
            })
        };
        std::thread::sleep(Duration::from_millis(50));
        assert!(!held.load(Ordering::SeqCst), "当てている途中の書き手が居るのに止め始めた");
        // 閉じる前に読んだ世代の書き手は積めない (積む前に門を確かめ直す)
        let b = wal.applying_gated(e0, 7);
        let r = wal.append_with_hlc_applying(Op::Tie { eid: 2, himo_id: 0, value: 1 }, &b);
        assert!(r.as_ref().is_err_and(is_gate_moved), "閉じる前の世代で積めた ({r:?})");
        assert!(gate.is_closed(), "閉じている間に門が開いて見える");
        drop(b);
        drop(a);
        while !held.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        assert!(gate.is_closed());
        let t = std::time::Instant::now();
        gate.wait();
        assert!(t.elapsed() >= Duration::from_millis(30), "止めている間に待たずに入った");
        h.join().unwrap();
        assert!(!gate.is_closed(), "放した後も閉じている");
        let c = wal.applying_gated(wal.gate_epoch(), 7);
        wal.append_with_hlc_applying(Op::Tie { eid: 3, himo_id: 0, value: 1 }, &c).unwrap();
        drop(c);
        drop(wal);
        let _ = std::fs::remove_file(&p);
    }

    /// #1016: 止める側は、 積む前の書き手 (WAL の空きを待つ書き手など、 印に位置が無い) や、 閉じた後に積まれた書き込みは待たない。
    /// 世代を進めた時の位置より前に積まれた書き込みだけを待つ。
    #[test]
    fn intent_hold_does_not_wait_for_writers_that_appended_after_it_closed() {
        use enchudb_oplog::oplog::Op;
        let (p, wal) = tmp_wal("noappend");
        let gate = Arc::new(IntentGate::new());
        gate.attach_wal(&wal, 7);
        // 印を作っただけ (積む前): 止める側は待たない
        let early = wal.applying_gated(wal.gate_epoch(), 7);
        drop(gate.hold());
        drop(early);
        // 閉じる前に積んだ A を待つ間に、 閉じた後の世代で B が積む。 A を外すと、 B が当てている途中でも止める側は返る
        let a = wal.applying_gated(wal.gate_epoch(), 7);
        wal.append_with_hlc_applying(Op::Tie { eid: 1, himo_id: 0, value: 1 }, &a).unwrap();
        let e0 = wal.gate_epoch();
        let held = Arc::new(AtomicBool::new(false));
        let h = {
            let (gate, held) = (gate.clone(), held.clone());
            std::thread::spawn(move || {
                let hold = gate.hold();
                held.store(true, Ordering::SeqCst);
                drop(hold);
            })
        };
        while wal.gate_epoch() == e0 {
            std::thread::yield_now();
        }
        let b = wal.applying_gated(wal.gate_epoch(), 7);
        wal.append_with_hlc_applying(Op::Tie { eid: 2, himo_id: 0, value: 1 }, &b).unwrap();
        std::thread::sleep(Duration::from_millis(30));
        assert!(!held.load(Ordering::SeqCst), "閉じる前に積んだ書き手が居るのに止め始めた");
        drop(a);
        h.join().unwrap();
        assert!(held.load(Ordering::SeqCst), "閉じた後に積んだ書き手を待った");
        drop(b);
        drop(wal);
        let _ = std::fs::remove_file(&p);
    }
}
