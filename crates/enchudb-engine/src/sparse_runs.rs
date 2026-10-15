//! `SparseRuns` — `LockFreeCylinder` の dense の配列の外の値 (配列の窓に入らない大きな値と、 窓の中でも配列を
//! そこまで伸ばしていない値、 #373) の索引。
//!
//! 値の順に並べた `(値, eid)` の **run** の組 (LSM)。 ms の時刻や 64 bit の ID のように値の種類が多い列は、
//! 値ごとの bucket (`AppendBucket`、 1 値 ~100 B + 値 → bucket の map) だと索引が本体の 10 倍を超える。
//! run なら 1 件 12 B (値 8 + eid 4) で、 等値も範囲も二分探索で引ける。
//!
//! ## 構造
//! - `delta`: 小さな sorted run (書き込みはここに入る。 満杯で run に昇格)
//! - `runs`: 大きい順の sorted run の列。 昇格した run は、 末尾の run が自分の `FANOUT` 倍より小さければ
//!   併合を繰り返す (run の大きさが `FANOUT` 倍ずつ離れる = 本数 O(log_FANOUT N))。 1 件あたりの併合コストは
//!   償却 O(FANOUT · log_FANOUT N)。 読みは run の本数だけ二分探索するので、 本数を抑える方を取る
//! - 全体を `RunSet` として epoch で publish する。 **読み手は lock を取らない** (pin → load → 二分探索)。
//!   書き手は 1 本 (`HimoStore` の write_lock が直列化) で、 書くたびに新しい `RunSet` を作って差し替える
//!   (run 本体は `Arc` で共有、 写すのは delta と run の Arc の並びだけ)
//!
//! ## append-only + 読む側の verify
//! `LockFreeCylinder` と同じ: 値の書き換え / 削除で古い entry は消さず、 読む側 (`HimoStore`) が Column の
//! 現在値と突き合わせて落とす。 古い entry が半分を超えたら `rebuild` が Column 基準で組み直す。
use crossbeam_epoch::{self as epoch, Atomic, Owned};
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// delta の上限。 書き込み 1 回で delta を写すので小さく保つ (64 件 = 768 B)。
const DELTA_CAP: usize = 64;

/// run の大きさの比。 100 万件で run は ~7 本 (2 倍ずつなら ~14 本)。
const FANOUT: usize = 4;

/// 値の順 (同じ値は eid の順) に並べた run。 値と eid は別の並び (12 B / 件、 padding なし)。
#[derive(Default)]
struct Run {
    vals: Vec<u64>,
    eids: Vec<u32>,
}

impl Run {
    fn len(&self) -> usize {
        self.vals.len()
    }

    /// `lo..=hi` の値を持つ entry の添字の範囲。
    fn span(&self, lo: u64, hi: u64) -> std::ops::Range<usize> {
        let a = self.vals.partition_point(|&v| v < lo);
        let b = self.vals.partition_point(|&v| v <= hi);
        a..b.max(a)
    }

    /// 2 本の run を併合する (どちらも sorted)。
    fn merge(a: &Run, b: &Run) -> Run {
        let n = a.len() + b.len();
        let mut out = Run { vals: Vec::with_capacity(n), eids: Vec::with_capacity(n) };
        let (mut i, mut j) = (0, 0);
        while i < a.len() || j < b.len() {
            let take_a = j >= b.len() || (i < a.len() && (a.vals[i], a.eids[i]) <= (b.vals[j], b.eids[j]));
            if take_a {
                out.vals.push(a.vals[i]);
                out.eids.push(a.eids[i]);
                i += 1;
            } else {
                out.vals.push(b.vals[j]);
                out.eids.push(b.eids[j]);
                j += 1;
            }
        }
        out
    }
}

/// 読み手が見る全体。 `runs` は大きい順。
#[derive(Default)]
struct RunSet {
    runs: Vec<Arc<Run>>,
    delta: Run,
}

impl RunSet {
    fn all(&self) -> impl Iterator<Item = &Run> {
        self.runs.iter().map(|r| &**r).chain(std::iter::once(&self.delta))
    }
}

/// 控えた 1 つの版 ([`SparseRuns::snapshot`])。 続けて読む間に書き手が差し替えても、 同じ版を読む。
pub(crate) struct SparseSnap<'a> {
    set: &'a RunSet,
}

impl SparseSnap<'_> {
    /// 値が `lo..=hi` の entry を値の順 (`desc` なら大きい順) に、 値ごとにまとめて `f(値, eid の列, 頭の昇順の長さ)` に渡す
    /// (古い entry 込み)。 eid の列は run ごとの区間をつないだもの: run は (値, eid) の順なので区間は昇順で、 崩れはつなぎ目にしか
    /// 無い。 頭の昇順の長さは、 つなぎ目で前の区間の最後と次の区間の最初を比べて出す (区間の数だけの比べ。 中は読まない)。
    /// `f` が `Break` を返したら止めて `Break`。 値 1 つごとに run の本数ぶんの比べ (run の中の位置は持ち越すので、 二分探索は
    /// 最初の 1 回だけ)。
    pub(crate) fn walk(&self, lo: u64, hi: u64, desc: bool, mut f: impl FnMut(u64, &[u32], usize) -> ControlFlow<()>) -> ControlFlow<()> {
        if lo > hi {
            return ControlFlow::Continue(());
        }
        let runs: Vec<&Run> = self.set.all().filter(|r| r.len() > 0).collect();
        // run ごとの、 まだ渡していない添字の範囲
        let mut left: Vec<std::ops::Range<usize>> = runs.iter().map(|r| r.span(lo, hi)).collect();
        let mut buf = Vec::new();
        loop {
            // 次の値: 昇順なら残りの先頭の最小、 降順なら残りの末尾の最大
            let ends = runs.iter().zip(&left).filter(|(_, s)| !s.is_empty());
            let next = if desc { ends.map(|(r, s)| r.vals[s.end - 1]).max() } else { ends.map(|(r, s)| r.vals[s.start]).min() };
            let Some(v) = next else { return ControlFlow::Continue(()) };
            buf.clear();
            // 頭の昇順の長さ: 頭が全部 (= buf の長さ) の間だけ、 次の区間が前の最後以上なら区間ごと伸ばす
            let mut head = 0;
            for (r, s) in runs.iter().zip(left.iter_mut()) {
                let seg = if desc {
                    let mut a = s.end;
                    while a > s.start && r.vals[a - 1] == v {
                        a -= 1;
                    }
                    let seg = &r.eids[a..s.end];
                    s.end = a;
                    seg
                } else {
                    let mut b = s.start;
                    while b < s.end && r.vals[b] == v {
                        b += 1;
                    }
                    let seg = &r.eids[s.start..b];
                    s.start = b;
                    seg
                };
                if let Some(&first) = seg.first()
                    && head == buf.len()
                    && buf.last().is_none_or(|&last| last <= first)
                {
                    head += seg.len();
                }
                buf.extend_from_slice(seg);
            }
            f(v, &buf, head)?;
        }
    }

    /// 値が `lo..=hi` の entry の数 (古い entry 込み、 確保なし)。
    pub(crate) fn count_in(&self, lo: u64, hi: u64) -> usize {
        if lo > hi {
            return 0;
        }
        self.set.all().map(|r| r.span(lo, hi).len()).sum()
    }

    /// 値が `lo..=hi` の entry の eid を、 run ごとの区間で `f` に渡す (古い entry 込み、 空の区間は渡さない)。 run をまたいだ
    /// 混ぜ合わせはしない (順の要らない読み用)。
    pub(crate) fn for_each_span(&self, lo: u64, hi: u64, mut f: impl FnMut(&[u32])) {
        if lo > hi {
            return;
        }
        for r in self.set.all() {
            let s = r.span(lo, hi);
            if !s.is_empty() {
                f(&r.eids[s]);
            }
        }
    }

    /// 値が `lo..=hi` の entry を `f(値, eid)` に (古い entry 込み、 run ごとに値の順)。
    pub(crate) fn for_each_in(&self, lo: u64, hi: u64, mut f: impl FnMut(u64, u32)) {
        if lo > hi {
            return;
        }
        for r in self.set.all() {
            let s = r.span(lo, hi);
            for (&v, &e) in r.vals[s.clone()].iter().zip(&r.eids[s]) {
                f(v, e);
            }
        }
    }
}

pub struct SparseRuns {
    set: Atomic<RunSet>,
}

// SAFETY: 書き手 1 本 (呼び側の lock) / 多読み手。 RunSet は publish 後に変更しない (差し替えのみ)。
unsafe impl Sync for SparseRuns {}
unsafe impl Send for SparseRuns {}

impl Default for SparseRuns {
    fn default() -> Self {
        Self { set: Atomic::new(RunSet::default()) }
    }
}

impl SparseRuns {
    /// 今の RunSet を読む (lock-free)。
    fn with<R>(&self, f: impl FnOnce(&RunSet) -> R) -> R {
        let guard = epoch::pin();
        let cur = self.set.load(Ordering::Acquire, &guard);
        // SAFETY: set は常に非 null、 古い RunSet は epoch を過ぎるまで解放されない。
        f(unsafe { cur.deref() })
    }

    /// 今の版を 1 つ控えて `f` に渡す (lock-free。 `f` の間は同じ版を読む)。
    pub(crate) fn snapshot<R>(&self, f: impl FnOnce(SparseSnap<'_>) -> R) -> R {
        self.with(|set| f(SparseSnap { set }))
    }

    /// 新しい RunSet に差し替える (書き手のみ)。
    fn publish(&self, next: RunSet) {
        let guard = epoch::pin();
        let old = self.set.swap(Owned::new(next), Ordering::AcqRel, &guard);
        // SAFETY: 旧 RunSet は全読み手が epoch を通過した後に解放。
        unsafe { guard.defer_destroy(old) };
    }

    /// 空の索引に `(value, eid)` の組をまとめて入れる (書き手のみ、 索引を組み直す時)。 並べ直して 1 本の run に
    /// するので、 1 件ずつ `insert` するより速い (1 件ごとの差し替えが無い)。
    pub fn build_from(&self, mut pairs: Vec<(u64, u32)>) {
        debug_assert!(self.is_empty(), "build_from は空の索引に");
        if pairs.is_empty() {
            return;
        }
        pairs.sort_unstable();
        let run = Run { vals: pairs.iter().map(|p| p.0).collect(), eids: pairs.iter().map(|p| p.1).collect() };
        self.publish(RunSet { runs: vec![Arc::new(run)], delta: Run::default() });
    }

    /// `(value, eid)` を足す (書き手のみ)。
    pub fn insert(&self, value: u64, eid: u32) {
        let next = self.with(|cur| {
            let mut delta = Run { vals: cur.delta.vals.clone(), eids: cur.delta.eids.clone() };
            let at = delta.vals.partition_point(|&v| v < value);
            let at = at + delta.eids[at..].iter().zip(&delta.vals[at..]).take_while(|&(&e, &v)| v == value && e < eid).count();
            delta.vals.insert(at, value);
            delta.eids.insert(at, eid);
            let mut runs = cur.runs.clone();
            if delta.len() >= DELTA_CAP {
                // 昇格: 末尾の run が同じ大きさ以下なら併合を繰り返す
                let mut r = delta;
                while runs.last().is_some_and(|last| last.len() < FANOUT * r.len()) {
                    let last = runs.pop().expect("checked");
                    r = Run::merge(&last, &r);
                }
                runs.push(Arc::new(r));
                delta = Run::default();
            }
            RunSet { runs, delta }
        });
        self.publish(next);
    }

    /// 値 `value` の entry の eid (古い entry 込み、 呼び側が verify する)。 run ごとに二分探索 1 回 +
    /// 同じ値の間だけ進む。
    pub fn lookup(&self, value: u64) -> Vec<u32> {
        let mut out = Vec::new();
        self.lookup_into(value, &mut out);
        out
    }

    /// [`lookup`](Self::lookup) の、 呼び手の buffer の後ろに足す版。
    pub fn lookup_into(&self, value: u64, out: &mut Vec<u32>) {
        self.with(|set| {
            for r in set.all() {
                let mut i = r.vals.partition_point(|&v| v < value);
                while i < r.vals.len() && r.vals[i] == value {
                    out.push(r.eids[i]);
                    i += 1;
                }
            }
        })
    }

    /// 値が `lo..=hi` の entry の eid (古い entry 込み、 run ごとに値の順)。
    pub fn range(&self, lo: u64, hi: u64) -> Vec<u32> {
        self.with(|set| {
            let mut out = Vec::new();
            for r in set.all() {
                out.extend_from_slice(&r.eids[r.span(lo, hi)]);
            }
            out
        })
    }

    /// 値が `lo..=hi` の entry を `(値, eid)` で (古い entry 込み)。
    pub fn range_pairs(&self, lo: u64, hi: u64) -> Vec<(u64, u32)> {
        self.with(|set| {
            let mut out = Vec::new();
            for r in set.all() {
                let s = r.span(lo, hi);
                out.extend(r.vals[s.clone()].iter().copied().zip(r.eids[s].iter().copied()));
            }
            out
        })
    }

    /// 値が `lo..=hi` の entry の数 (古い entry 込み、 確保なし)。
    pub fn count_in(&self, lo: u64, hi: u64) -> usize {
        self.with(|set| set.all().map(|r| r.span(lo, hi).len()).sum())
    }

    /// entry の数 (古い entry 込み)。
    pub fn len(&self) -> usize {
        self.with(|set| set.all().map(|r| r.len()).sum())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 入っている値 (古い entry 込み、 昇順・重複なし)。
    pub fn values(&self) -> Vec<u64> {
        let mut out: Vec<u64> = self.with(|set| set.all().flat_map(|r| r.vals.iter().copied()).collect());
        out.sort_unstable();
        out.dedup();
        out
    }

    /// 全 entry を `keep(値, eid)` で選り分けて 1 本の run に組み直す (書き手のみ)。 落とした数を返す。
    /// 同じ `(値, eid)` の重複 (書き換えで戻った値) もここで 1 つにする。
    pub fn rebuild(&self, mut keep: impl FnMut(u64, u32) -> bool) -> usize {
        let (next, removed) = self.with(|set| {
            let mut all = Run::default();
            for r in set.all() {
                all = Run::merge(&all, r);
            }
            let before = all.len();
            let mut out = Run { vals: Vec::with_capacity(before), eids: Vec::with_capacity(before) };
            for (i, (&v, &e)) in all.vals.iter().zip(&all.eids).enumerate() {
                let dup = i > 0 && all.vals[i - 1] == v && all.eids[i - 1] == e;
                if !dup && keep(v, e) {
                    out.vals.push(v);
                    out.eids.push(e);
                }
            }
            let removed = before - out.len();
            let runs = if out.len() == 0 { Vec::new() } else { vec![Arc::new(out)] };
            (RunSet { runs, delta: Run::default() }, removed)
        });
        self.publish(next);
        removed
    }

    /// 確保している bytes (観測用)。
    pub fn backing_bytes(&self) -> usize {
        self.with(|set| set.all().map(|r| r.vals.capacity() * 8 + r.eids.capacity() * 4).sum())
    }

    /// run の本数 (delta を除く、 診断用)。
    pub fn run_count(&self) -> usize {
        self.with(|set| set.runs.len())
    }
}

impl Drop for SparseRuns {
    fn drop(&mut self) {
        let cur = self.set.load(Ordering::Relaxed, unsafe { epoch::unprotected() });
        if !cur.is_null() {
            // SAFETY: drop = 単独所有。
            unsafe { drop(cur.into_owned()) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    /// lookup / range / values / rebuild が、 全 entry を持つ素朴な Vec と一致する (run の昇格・併合を
    /// 何段もまたぐ件数で)。
    #[test]
    fn matches_a_plain_vec() {
        let s = SparseRuns::default();
        let mut all: Vec<(u64, u32)> = Vec::new();
        let mut rng = Rng(0x5a55_0001);
        for eid in 0..5_000u32 {
            // 値は 4 通りの大きさ (同じ値が何度も来る / ばらばら / u32 を超える)
            let v = match rng.next() % 4 {
                0 => (1 << 20) + rng.next() % 16,
                1 => rng.next() >> 1,
                2 => (1u64 << 40) + rng.next() % 1000,
                _ => u64::MAX - 1 - rng.next() % 8,
            };
            s.insert(v, eid);
            all.push((v, eid));
        }
        assert!(s.run_count() >= 2, "delta が run に昇格していない / 大きさの違う run が残っていない");
        assert!(s.run_count() < 10, "run が併合されていない: {}", s.run_count());
        assert_eq!(s.len(), all.len());
        let sorted = |mut v: Vec<u32>| {
            v.sort_unstable();
            v
        };
        for _ in 0..500 {
            let (v, _) = all[(rng.next() % all.len() as u64) as usize];
            let want: Vec<u32> = all.iter().filter(|p| p.0 == v).map(|p| p.1).collect();
            assert_eq!(sorted(s.lookup(v)), sorted(want), "lookup {v}");
            let (a, b) = (rng.next() >> (rng.next() % 64), rng.next() >> (rng.next() % 64));
            let (lo, hi) = (a.min(b), a.max(b));
            let want: Vec<u32> = all.iter().filter(|p| lo <= p.0 && p.0 <= hi).map(|p| p.1).collect();
            assert_eq!(sorted(s.range(lo, hi)), sorted(want), "range {lo}..={hi}");
        }
        let mut vals: Vec<u64> = all.iter().map(|p| p.0).collect();
        vals.sort_unstable();
        vals.dedup();
        assert_eq!(s.values(), vals);
        // rebuild: 奇数 eid を落とす
        let removed = s.rebuild(|_, e| e % 2 == 0);
        assert_eq!(removed, 2_500);
        assert_eq!(s.run_count(), 1);
        let want: Vec<u32> = all.iter().filter(|p| p.1 % 2 == 0 && p.0 == all[0].0).map(|p| p.1).collect();
        assert_eq!(sorted(s.lookup(all[0].0)), sorted(want));
    }

    /// `walk` が値ごとに渡す頭の昇順の長さ (run ごとの区間のつなぎ目だけを比べた数) が、 渡した eid の列を探した長さ
    /// (`sorted_prefix_len`) とちょうど同じ: 値の順・逆順、 同じ値の eid がいくつもの run にまたがって、 つなぎ目で崩れる値・
    /// 崩れない値・同じ eid が続く値を混ぜる。 対照: 頭が途中で切れる値も、 run をまたいで頭が続く値もある。
    #[test]
    fn walk_reports_the_sorted_head_of_each_value() {
        let s = SparseRuns::default();
        let mut rng = Rng(0x5a55_0002);
        let n = if cfg!(miri) { 600u32 } else { 20_000 };
        for i in 0..n {
            // 値は 32 + 8 通り。 前の 32 通りは eid に 「大きくなる」 と 「ばらばら (同じ eid も)」 を混ぜて run の間で順が崩れる
            // ようにし、 後の 8 通りは eid が大きくなる一方 (古い run ほど小さい eid なので、 つなぎ目をまたいでも頭が続く)
            let mono = rng.next() % 4 == 0;
            let v = if mono { (1 << 41) + rng.next() % 8 } else { (1 << 40) + rng.next() % 32 };
            let e = if !mono && rng.next() % 3 == 0 { (rng.next() % u64::from(n)) as u32 } else { i };
            s.insert(v, e);
        }
        assert!(s.run_count() >= 2, "run が 1 本だけ (つなぎ目が無い)");
        let (mut cut, mut across) = (0, 0);
        for desc in [false, true] {
            s.snapshot(|sp| {
                sp.walk(0, u64::MAX, desc, |v, eids, head| {
                    assert_eq!(head, crate::append_bucket::sorted_prefix_len(eids), "値 {v} (desc {desc})");
                    cut += usize::from(head < eids.len());
                    across += usize::from(head == eids.len() && eids.len() > 1);
                    ControlFlow::Continue(())
                })
            });
        }
        assert!(cut > 0, "対照: 頭が途中で切れる値が無い");
        assert!(across > 0, "対照: 頭が全部の値が無い");
    }

    /// 同じ `(値, eid)` の重複は rebuild で 1 つになる。
    #[test]
    fn rebuild_drops_duplicates() {
        let s = SparseRuns::default();
        for _ in 0..3 {
            s.insert(1 << 30, 7);
        }
        s.insert(1 << 30, 8);
        assert_eq!(s.lookup(1 << 30).len(), 4);
        assert_eq!(s.rebuild(|_, _| true), 2);
        let mut got = s.lookup(1 << 30);
        got.sort_unstable();
        assert_eq!(got, vec![7, 8]);
    }

    /// 読み手は書き手と並行に読める (差し替え中も壊れた run を見ない)。
    #[test]
    fn concurrent_readers_see_consistent_sets() {
        let s = Arc::new(SparseRuns::default());
        let w = s.clone();
        let writer = std::thread::spawn(move || {
            for i in 0..20_000u32 {
                w.insert((1u64 << 33) + (i % 97) as u64, i);
            }
        });
        let mut seen = 0;
        while !writer.is_finished() {
            let got = s.lookup((1u64 << 33) + 5);
            assert!(got.iter().all(|&e| e % 97 == 5), "別の値の entry が混ざった");
            seen = seen.max(got.len());
        }
        writer.join().unwrap();
        assert_eq!(s.lookup((1u64 << 33) + 5).len(), (0..20_000u32).filter(|i| i % 97 == 5).count());
        let _ = seen;
    }
}
