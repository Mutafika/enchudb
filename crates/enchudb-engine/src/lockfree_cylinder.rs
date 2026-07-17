//! `LockFreeCylinder` — value → eid の lock-free concurrent bucket store。 #95。
//!
//! 現状の `RwLock<BucketCylinder>` を置き換える。 **同時に 1 writer** / 多 reader。
//! writer は 1 thread とは限らない（consumer + 同期 tie + schema commit）ので、
//! 呼び出し側 = `HimoStore` が per-himo `write_lock` で直列化して契約を満たす。
//!
//! ## 構造
//! - **dense**（value < `DENSE_CAP`）: `Atomic<Vec<Arc<AppendBucket>>>`。 外側 Vec を
//!   epoch-swap で成長させ、 `AppendBucket` 本体は `Arc` で stable（成長で動かない）。
//!   read は完全 lock-free。
//! - **sparse**（value ≥ `DENSE_CAP`、 稀）: `Mutex<HashMap>`。 common path（dense）を
//!   lock-free に保ち、 稀な高 value だけ lock を許容する pragmatic split。
//!
//! ## append-only + lazy verify（#95 設計）
//! - `insert` は該当 value の bucket に **append するだけ**。 旧 value からの削除はしない
//!   （swap_remove / positions は廃止）。 値更新・削除で生じる stale entry は、
//!   **read 側が Column を verify して filter** する（`HimoStore` 層の責務）。
//! - よって `total()` / `unique_count()` は churn した himo では **append 数ベースの
//!   over-count**（compaction #99 まで）。 append-only himo（削除なし）では正確。
//!   compaction は後付け最適化（#99）。

use crate::append_bucket::AppendBucket;
use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub const DENSE_CAP: u32 = 1 << 20;

type DenseArr = Vec<Arc<AppendBucket>>;

pub struct LockFreeCylinder {
    dense: Atomic<DenseArr>,
    sparse: Mutex<HashMap<u32, Arc<AppendBucket>>>,
    /// backing に現存する slot 総数 (stale 込み。compaction の除去分は反映)。
    /// メモリ会計・診断用、かつ total_live 導出の被減数 (request12.1)。
    total: AtomicUsize,
    /// 一度でも要素が入った bucket 数 (raw)。診断用。
    unique_count: AtomicU32,
    /// backing に現存する stale slot 総数 (= note_stale 累計 − compaction 除去分)。
    /// live な tie 総数は `total − stale_total` で導出する (request12.1)。
    /// per-insert の RMW を避け、churn 経路 (note_stale / compaction) でのみ更新
    /// — 純 insert workload の hot path を master と同一コストに保つ。
    stale_total: AtomicUsize,
    /// live > 0 の bucket 数 (= 正確な cardinality)。request12。
    unique_live: AtomicU32,
    /// delete / 値更新が一度でも起きたか (himo 全体、診断用)。
    /// read の verify 判定は request12 で bucket 単位 (`AppendBucket::needs_verify`)
    /// に局所化された — churn していない bucket は fast path のまま。
    any_removed: AtomicBool,
}

// SAFETY: 単一 writer / 多 reader。 dense は epoch、 sparse は Mutex で同期。
unsafe impl Sync for LockFreeCylinder {}
unsafe impl Send for LockFreeCylinder {}

impl LockFreeCylinder {
    pub fn new(max_values: u32) -> Self {
        let hint = if max_values == 0 {
            0
        } else {
            ((max_values as usize + 1).min(DENSE_CAP as usize)) as u32
        };
        let init: DenseArr = (0..hint).map(|_| Arc::new(AppendBucket::new())).collect();
        Self {
            dense: Atomic::new(init),
            sparse: Mutex::new(HashMap::new()),
            total: AtomicUsize::new(0),
            unique_count: AtomicU32::new(0),
            stale_total: AtomicUsize::new(0),
            unique_live: AtomicU32::new(0),
            any_removed: AtomicBool::new(false),
        }
    }

    /// これまでに delete / 値更新があったか（himo 全体、診断用）。
    /// verify の要否判定には使わない (bucket 単位 flag に局所化、request12)。
    #[allow(dead_code)]
    #[inline]
    pub fn any_removed(&self) -> bool {
        self.any_removed.load(Ordering::Relaxed)
    }

    /// 値更新 / 削除で `value` の bucket に stale が残ることを記録する (request12)。
    /// 該当 bucket の removed flag を立て、live を -1。write_lock 下の単一 writer 前提。
    /// **Column を書き換える前に呼ぶこと** (reader が「flag 未設定なのに Column は
    /// 新値」を観測する窓を作らない — 旧 mark_removed と同じ順序契約)。
    ///
    /// 戻り値は `(bucket の raw len, 減分後の live)` — 呼び出し側 (HimoStore) が
    /// incremental compaction の trigger 判定 (stale 率) に使う (P2)。
    pub fn note_stale(&self, value: u32) -> Option<(usize, u32)> {
        self.any_removed.store(true, Ordering::Relaxed);
        let stats = if value < DENSE_CAP {
            let guard = epoch::pin();
            let arr = self.dense.load(Ordering::Acquire, &guard);
            // SAFETY: dense は常に非 null。
            let vec = unsafe { arr.deref() };
            if (value as usize) < vec.len() {
                let b = &vec[value as usize];
                b.note_stale().map(|prev| (b.len(), prev - 1))
            } else {
                debug_assert!(false, "note_stale: 未確保 bucket (value={value})");
                None
            }
        } else {
            let sp = self.sparse.lock().unwrap();
            match sp.get(&value) {
                Some(b) => b.note_stale().map(|prev| (b.len(), prev - 1)),
                None => {
                    debug_assert!(false, "note_stale: 未確保 sparse bucket (value={value})");
                    None
                }
            }
        };
        match stats {
            Some((_, live_after)) => {
                self.stale_total.fetch_add(1, Ordering::Relaxed);
                if live_after == 0 {
                    // この bucket の live が 0 になった = cardinality 減
                    self.unique_live.fetch_sub(1, Ordering::Relaxed);
                }
            }
            None => debug_assert!(false, "note_stale: live 0 の bucket への stale 通知"),
        }
        stats
    }

    /// `value` の bucket を `keep` 判定で組み直す (request12 P2 = incremental
    /// compaction)。write_lock 下の単一 writer 前提。reader は停止しない
    /// (epoch swap)。**Column が最新になってから呼ぶこと** — keep は Column の
    /// 現在値照合 (`value_eq`) であり、更新前に呼ぶと移動中の eid を live と
    /// 誤認して残し、removed=false の fast path が stale を返すようになる。
    ///
    /// live/removed は bucket 内で確定し直すため、cylinder 側の total_live /
    /// unique_live は不変 (live な eid の集合は compaction で変わらない)。
    pub fn compact_bucket(&self, value: u32, keep: impl FnMut(u32) -> bool) -> usize {
        if value < DENSE_CAP {
            let guard = epoch::pin();
            let arr = self.dense.load(Ordering::Acquire, &guard);
            // SAFETY: dense は常に非 null。
            let vec = unsafe { arr.deref() };
            if (value as usize) < vec.len() {
                let b = &vec[value as usize];
                let before = b.len();
                let kept = b.compact_in(&guard, keep);
                self.discount_compacted(before - kept);
                kept
            } else {
                0
            }
        } else {
            let b = self.sparse.lock().unwrap().get(&value).cloned();
            match b {
                Some(b) => {
                    let guard = epoch::pin();
                    let before = b.len();
                    let kept = b.compact_in(&guard, keep);
                    self.discount_compacted(before - kept);
                    kept
                }
                None => 0,
            }
        }
    }

    /// compaction で除去された slot 数 (= その bucket に居た stale 全量) を
    /// total / stale_total から同数引く — `total_live` (= 差分) は不変のまま、
    /// raw `total` は「backing に現存する slot 総数」として正確さを保つ。
    fn discount_compacted(&self, removed: usize) {
        if removed > 0 {
            self.total.fetch_sub(removed, Ordering::Relaxed);
            self.stale_total.fetch_sub(removed, Ordering::Relaxed);
        }
    }

    /// 単一 writer append。 value の bucket に eid を足す（旧 value は放置＝lazy verify）。
    /// pin は insert 全体で 1 回（push / unique 判定に guard を回す。 hot path の
    /// pin 3 回 → 1 回、 write 天井対策）。
    pub fn insert(&self, eid: u32, value: u32) {
        debug_assert!(value != u32::MAX, "value == u32::MAX is sentinel");
        let guard = epoch::pin();
        if value < DENSE_CAP {
            let arr = self.dense.load(Ordering::Acquire, &guard);
            // SAFETY: dense は常に非 null。
            let vec = unsafe { arr.deref() };
            if (value as usize) < vec.len() {
                let b = &vec[value as usize];
                let prev_len = b.push_in(eid, &guard);
                let prev_live = b.live_inc();
                self.bump_stats(prev_len == 0, prev_live == 0);
            } else {
                // 成長: doubling で amortize、 既存 Arc は clone（refcount）、 新規は空 bucket
                let mut nv: DenseArr = vec.clone();
                let new_len = ((value as usize) + 1).max(vec.len() * 2).min(DENSE_CAP as usize);
                nv.resize_with(new_len, || Arc::new(AppendBucket::new()));
                nv[value as usize].push_in(eid, &guard);
                nv[value as usize].live_inc();
                self.bump_stats(true, true); // 新 bucket は必ず空だった
                self.dense.store(Owned::new(nv), Ordering::Release);
                // SAFETY: 旧 array は全 reader が epoch 通過後に解放。
                unsafe {
                    guard.defer_destroy(arr);
                }
            }
        } else {
            let mut sp = self.sparse.lock().unwrap();
            let b = sp
                .entry(value)
                .or_insert_with(|| Arc::new(AppendBucket::new()));
            let prev_len = b.push_in(eid, &guard);
            let prev_live = b.live_inc();
            drop(sp);
            self.bump_stats(prev_len == 0, prev_live == 0);
        }
    }

    #[inline]
    fn bump_stats(&self, was_empty: bool, was_dead: bool) {
        self.total.fetch_add(1, Ordering::Relaxed);
        // total_live は total − stale_total で導出 (request12.1) — ここで RMW を
        // 増やさない (raw tie_async + oplog の consumer apply が per-insert コストに
        // 直結する。sunsu matrix で +33% の実測退行が出た)。
        if was_empty {
            self.unique_count.fetch_add(1, Ordering::Relaxed);
        }
        if was_dead {
            // live 0 → 1 遷移 = cardinality 増 (初 insert または全滅からの復活)
            self.unique_live.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// dense value の bucket を外部 guard 下で read（lock-free、 zero-copy）。
    /// sparse は None を返す（sparse は `read_to_vec` 経由で）。
    #[allow(dead_code)]
    #[inline]
    pub fn with_dense_read<R>(
        &self,
        guard: &Guard,
        value: u32,
        f: impl FnOnce(&[u32]) -> R,
    ) -> Option<R> {
        if value >= DENSE_CAP {
            return None;
        }
        let arr = self.dense.load(Ordering::Acquire, guard);
        let vec = unsafe { arr.deref() };
        if (value as usize) < vec.len() {
            Some(vec[value as usize].with_read(guard, f))
        } else {
            Some(f(&[]))
        }
    }

    /// value の全 eid を Vec で返す（dense / sparse 両対応、 内部で pin）。
    /// 注: stale filter はしない（caller = HimoStore が Column verify する）。
    #[allow(dead_code)]
    pub fn read_to_vec(&self, value: u32) -> Vec<u32> {
        self.read_to_vec_verify(value).0
    }

    /// `read_to_vec` + この bucket の read が Column verify を要するか (request12)。
    /// 判定は `AppendBucket::read_snapshot_verify` の 3 段プロトコル
    /// (slice → flag → backing ptr 再検証) — 順序の健全性論証はそちらの doc 参照。
    pub fn read_to_vec_verify(&self, value: u32) -> (Vec<u32>, bool) {
        if value < DENSE_CAP {
            let guard = epoch::pin();
            let arr = self.dense.load(Ordering::Acquire, &guard);
            // SAFETY: dense は常に非 null。
            let vec = unsafe { arr.deref() };
            if (value as usize) < vec.len() {
                vec[value as usize].read_snapshot_verify(&guard)
            } else {
                (Vec::new(), false)
            }
        } else {
            // Arc を取ったら lock を即座に落とす。 bucket コピーを lock 下でやると
            // 巨大 sparse bucket の read が writer の insert(sparse) を stall させる
            // （#95 の相互排他を sparse 側で再発させない）。
            let b = self.sparse.lock().unwrap().get(&value).cloned();
            match b {
                Some(b) => {
                    let guard = epoch::pin();
                    b.read_snapshot_verify(&guard)
                }
                None => (Vec::new(), false),
            }
        }
    }

    /// value の bucket 長（raw、 stale 込み）。診断用 (planner は `slice_len_live` へ移行)。
    #[allow(dead_code)]
    pub fn slice_len(&self, value: u32) -> usize {
        if value < DENSE_CAP {
            let guard = epoch::pin();
            let arr = self.dense.load(Ordering::Acquire, &guard);
            let vec = unsafe { arr.deref() };
            if (value as usize) < vec.len() {
                vec[value as usize].len()
            } else {
                0
            }
        } else {
            let sp = self.sparse.lock().unwrap();
            sp.get(&value).map(|b| b.len()).unwrap_or(0)
        }
    }

    /// backing に現存する slot 総数 (raw、stale 込み、compaction 除去は反映)。診断用。
    #[allow(dead_code)]
    pub fn total(&self) -> usize {
        self.total.load(Ordering::Relaxed)
    }

    /// 一度でも要素が入った bucket 数 (raw)。診断用。
    #[allow(dead_code)]
    pub fn unique_count(&self) -> u32 {
        self.unique_count.load(Ordering::Relaxed)
    }

    /// live な tie 総数 (正確、churn の影響なし)。request12。
    /// `total − stale_total` で導出。stale → total の順で load するので
    /// 並行 insert があっても負に振れない (並行 compaction 中のみ一時的に
    /// 過少に見えうる — Relaxed 統計として許容、saturating で防御)。
    pub fn total_live(&self) -> usize {
        let stale = self.stale_total.load(Ordering::Relaxed);
        self.total.load(Ordering::Relaxed).saturating_sub(stale)
    }

    /// live > 0 の bucket 数 (正確な cardinality)。request12。
    pub fn unique_live(&self) -> u32 {
        self.unique_live.load(Ordering::Relaxed)
    }

    /// value の live 件数 (= verify 後の pull 結果の件数、正確)。
    /// planner の pivot 選択用 (raw の `slice_len` は stale 込みで over-count する)。
    pub fn slice_len_live(&self, value: u32) -> usize {
        if value < DENSE_CAP {
            let guard = epoch::pin();
            let arr = self.dense.load(Ordering::Acquire, &guard);
            let vec = unsafe { arr.deref() };
            if (value as usize) < vec.len() {
                vec[value as usize].live() as usize
            } else {
                0
            }
        } else {
            let sp = self.sparse.lock().unwrap();
            sp.get(&value).map(|b| b.live() as usize).unwrap_or(0)
        }
    }

    /// value の bucket に churn 痕があるか。write_lock 下 (単一 writer) では正確 —
    /// `compact_now` の clean-bucket skip 判定用。
    pub fn bucket_needs_verify(&self, value: u32) -> bool {
        if value < DENSE_CAP {
            let guard = epoch::pin();
            let arr = self.dense.load(Ordering::Acquire, &guard);
            // SAFETY: dense は常に非 null。
            let vec = unsafe { arr.deref() };
            (value as usize) < vec.len() && vec[value as usize].needs_verify()
        } else {
            let sp = self.sparse.lock().unwrap();
            sp.get(&value).map(|b| b.needs_verify()).unwrap_or(false)
        }
    }

    /// cylinder が現在確保している eid backing の総 bytes（pow2 slack 込み、 メモリ観測用）。
    /// append-only なので各 eid は 1 度だけ載る → `total()*4 * (pow2 slack)` に収まる。
    /// double-buffer（2 コピー保持）なら `>= 2x` になるので、 それとの区別に使える。
    pub fn backing_bytes(&self) -> usize {
        let guard = epoch::pin();
        let arr = self.dense.load(Ordering::Acquire, &guard);
        let vec = unsafe { arr.deref() };
        let mut slots: usize = vec.iter().map(|b| b.capacity()).sum();
        let sp = self.sparse.lock().unwrap();
        slots += sp.values().map(|b| b.capacity()).sum::<usize>();
        drop(sp);
        slots * std::mem::size_of::<u32>()
    }

    /// 非空 bucket の value を列挙（順序保証なし、 stale 込みの近似）。
    pub fn unique_values(&self) -> Vec<u32> {
        let guard = epoch::pin();
        let arr = self.dense.load(Ordering::Acquire, &guard);
        let vec = unsafe { arr.deref() };
        let mut out: Vec<u32> = vec
            .iter()
            .enumerate()
            .filter_map(|(v, b)| if b.is_empty() { None } else { Some(v as u32) })
            .collect();
        let sp = self.sparse.lock().unwrap();
        out.extend(sp.keys().copied());
        out
    }
}

impl Drop for LockFreeCylinder {
    fn drop(&mut self) {
        // 現在の dense array を解放（過去 grow の defer 分は collector が回収）。
        let cur = self.dense.load(Ordering::Relaxed, unsafe { epoch::unprotected() });
        if !cur.is_null() {
            // SAFETY: drop = 単独所有。
            unsafe {
                drop(cur.into_owned());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn dense_insert_read() {
        let c = LockFreeCylinder::new(0);
        for e in 0..100u32 {
            c.insert(e, e % 10); // value 0..9
        }
        assert_eq!(c.total(), 100);
        assert_eq!(c.unique_count(), 10);
        let v0 = c.read_to_vec(0);
        assert_eq!(v0, (0..100).filter(|e| e % 10 == 0).collect::<Vec<_>>());
    }

    #[test]
    fn dense_grow() {
        let c = LockFreeCylinder::new(0);
        // value を疎に増やして外側 Vec の成長（store + defer_destroy）を誘発。
        c.insert(1, 5);
        c.insert(2, 500); // 0→~501 bucket に grow、epoch swap を叩く
        assert_eq!(c.read_to_vec(500), vec![2]);
        // 大 value（value+1 個の bucket 確保）は miri で alloc 爆発するので skip。
        // grow + defer の unsafe は value 500 の時点で網羅済み。
        if cfg!(miri) {
            assert_eq!(c.unique_count(), 2);
        } else {
            c.insert(3, 50_000);
            c.insert(4, 999_999);
            assert_eq!(c.read_to_vec(50_000), vec![3]);
            assert_eq!(c.read_to_vec(999_999), vec![4]);
            assert_eq!(c.unique_count(), 4);
        }
    }

    #[test]
    fn sparse_path() {
        let c = LockFreeCylinder::new(0);
        let big = DENSE_CAP + 42;
        c.insert(7, big);
        c.insert(8, big);
        assert_eq!(c.read_to_vec(big), vec![7, 8]);
        assert_eq!(c.unique_count(), 1);
        assert_eq!(c.slice_len(big), 2);
        // request12: sparse 側も live / bucket-local flag が効く
        assert_eq!(c.slice_len_live(big), 2);
        c.note_stale(big);
        assert_eq!(c.slice_len_live(big), 1);
        assert!(c.read_to_vec_verify(big).1);
    }

    /// request12: verify 判定が bucket 局所であること + live counter の正確性。
    #[test]
    fn bucket_local_verify_and_live_counters() {
        let c = LockFreeCylinder::new(0);
        // v0 に 5 eid、v1 に 5 eid
        for e in 0..10u32 {
            c.insert(e, e % 2);
        }
        assert_eq!(c.total_live(), 10);
        assert_eq!(c.unique_live(), 2);
        assert!(!c.read_to_vec_verify(0).1);
        assert!(!c.read_to_vec_verify(1).1);

        // e0 を v0 → v1 へ churn (呼び出し側の順序で note_stale → insert)
        c.note_stale(0);
        c.insert(0, 1);
        // v0 だけ verify 要、v1 は fast path のまま (himo 全体には波及しない)
        assert!(c.read_to_vec_verify(0).1, "churn した bucket は verify 要");
        assert!(!c.read_to_vec_verify(1).1, "無傷の bucket が verify に落ちている (局所化の破れ)");
        assert_eq!(c.total_live(), 10, "live 総数は churn で不変");
        assert_eq!(c.unique_live(), 2);
        assert_eq!(c.slice_len_live(0), 4);
        assert_eq!(c.slice_len_live(1), 6);
        // raw は stale 込みで over-count のまま (診断用)
        assert_eq!(c.total(), 11);

        // v0 の残り 4 eid も全部 v1 へ → v0 の live が 0 になり cardinality 減
        for e in [2u32, 4, 6, 8] {
            c.note_stale(0);
            c.insert(e, 1);
        }
        assert_eq!(c.unique_live(), 1, "live 0 の bucket は cardinality から消える");
        assert_eq!(c.total_live(), 10);
        assert_eq!(c.slice_len_live(0), 0);
    }

    /// request12 P2: compact_bucket が stale を除去して flag を戻し、
    /// live 系カウンタは不変のまま。
    #[test]
    fn compact_bucket_shrinks_and_resets_flag() {
        let c = LockFreeCylinder::new(0);
        for e in 0..100u32 {
            c.insert(e, 0);
        }
        // e0..e59 が v1 へ移動 (呼び出し側の順序で note_stale → insert)
        for e in 0..60u32 {
            c.note_stale(0);
            c.insert(e, 1);
        }
        assert!(c.read_to_vec_verify(0).1);
        assert_eq!(c.slice_len(0), 100, "raw は stale 込みのまま");
        assert_eq!(c.slice_len_live(0), 40);

        // Column 相当の keep: e60 以上が今も v0
        let n = c.compact_bucket(0, |e| e >= 60);
        assert_eq!(n, 40);
        let (v, needs_verify) = c.read_to_vec_verify(0);
        assert!(!needs_verify, "compact 後は fast path");
        assert_eq!(v, (60..100).collect::<Vec<_>>());
        assert_eq!(c.slice_len(0), 40, "raw len も縮む");
        // live 集合は不変なので cylinder 側カウンタは動かない
        assert_eq!(c.total_live(), 100);
        assert_eq!(c.unique_live(), 2);
        // request12.1: raw total も compaction 除去分を反映 (Σ len と一致)
        assert_eq!(c.total(), 100, "total = 現存 slot 総数 (v0:40 + v1:60)");
    }

    /// #95 メモリ制約: append-only なので各 eid は backing に 1 度だけ載る。
    /// pow2 doubling の slack 込みでも `< 2x`。 double-buffer（2 コピー）なら `>= 2x`
    /// になるので、 この上界が「double-buffer していない」ことの厳密証明になる。
    #[test]
    fn no_double_buffer_backing_bound() {
        let c = LockFreeCylinder::new(0);
        // per-bucket 密度 ~100 を維持（pow2 fill 良好 → slack ~1.28x）。miri は総数だけ縮小。
        let (n, card) = if cfg!(miri) {
            (3_000u32, 30u32)
        } else {
            (1_000_000u32, 10_000u32)
        };
        for e in 0..n {
            c.insert(e, e % card);
        }
        assert_eq!(c.total(), n as usize);
        let bytes = c.backing_bytes();
        let min = n as usize * 4; // 各 eid 1 度・slack ゼロの理論下限
        assert!(
            bytes < 2 * min,
            "backing {bytes} >= 2x min {min}: double-buffer の疑い（append-only 破れ）"
        );
        // 参考: 実比率（pow2 slack）を可視化。
        eprintln!(
            "backing {} bytes = {:.2}x min（pow2 slack のみ）",
            bytes,
            bytes as f64 / min as f64
        );
    }

    /// request12 レビュー指摘 (PR #103): fast path は「verify 判定の後に積まれた
    /// churn 痕」を返してはならない。flag 先読みのみの実装では、reader の
    /// flag=false load → writer の往復 churn (B→A→B で bucket に [e,e]) →
    /// slice copy の interleaving で fast path が重複 eid を返す。
    /// slice → flag → backing ptr 再検証の 3 段プロトコルで防ぐ。
    #[test]
    fn fast_path_never_returns_duplicates_under_churn() {
        let c = Arc::new(LockFreeCylinder::new(0));
        c.insert(7, 1); // e=7 が v1 に live
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let (c, stop) = (c.clone(), stop.clone());
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    // e を v1 → v0 → v1 と往復 (himo_store の順序契約: flag → 移動)
                    c.note_stale(1);
                    c.insert(7, 0);
                    c.note_stale(0);
                    c.insert(7, 1);
                    // Column 相当: e は今 v1 に居る。compact で flag が false に戻り、
                    // reader が fast path を踏めるようになる (= 検証対象の窓が開く)
                    c.compact_bucket(0, |_| false);
                    c.compact_bucket(1, |e| e == 7);
                }
            })
        };
        let iters = if cfg!(miri) { 300u64 } else { 3_000_000 };
        let mut fast_reads = 0u64;
        for _ in 0..iters {
            let (v, needs_verify) = c.read_to_vec_verify(1);
            if !needs_verify {
                fast_reads += 1;
                // fast path の slice は dup-free でなければならない (verify を通らない)
                if v.len() > 1 {
                    let mut s = v.clone();
                    s.sort_unstable();
                    s.dedup();
                    assert_eq!(s.len(), v.len(), "fast path が重複 eid を返した: {v:?}");
                }
            }
        }
        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
        assert!(fast_reads > 0, "fast path を一度も踏まなかった (テストが無効化している)");
    }

    #[test]
    fn concurrent_writer_readers() {
        let c = Arc::new(LockFreeCylinder::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let n = if cfg!(miri) { 150u32 } else { 100_000u32 };
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let c = c.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        // value 3 の bucket を読み続ける（成長する dense 配列越し）
                        let v = c.read_to_vec(3);
                        // 全要素は value 3 に insert した eid（= e where e%7==3）
                        for &e in &v {
                            assert_eq!(e % 7, 3, "corruption: {e}");
                        }
                    }
                })
            })
            .collect();
        for e in 0..n {
            c.insert(e, e % 7);
        }
        stop.store(true, Ordering::Relaxed);
        for r in readers {
            r.join().unwrap();
        }
        assert_eq!(c.total(), n as usize);
        assert_eq!(c.unique_count(), 7);
    }
}
