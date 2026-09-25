//! `LockFreeCylinder` — value → eid の lock-free concurrent bucket store。 #95。
//!
//! 現状の `RwLock<BucketCylinder>` を置き換える。 **同時に 1 writer** / 多 reader。
//! writer は 1 thread とは限らない（consumer + 同期 tie + schema commit）ので、
//! 呼び出し側 = `HimoStore` が per-himo `write_lock` で直列化して契約を満たす。
//!
//! ## 構造
//! - **dense**（value < `DENSE_CAP`、 または `MID` ± `DENSE_CAP / 2`）: `Atomic<Vec<Arc<AppendBucket>>>`。 外側 Vec を
//!   epoch-swap で成長させ、 `AppendBucket` 本体は `Arc` で stable（成長で動かない）。
//!   read は完全 lock-free。 残り 2 本の配列は `MID` = 2^63 の上 (`dense_pos`、 `MID + i`) と下 (`dense_neg`、
//!   `MID - 1 - i`) を受け持つ — schema / SQL の符号付き 64 bit の列は大小の順を保つ `v ^ 2^63` で置くので、
//!   0 に近い符号付きの値 (年齢・件数・負の小さな数) は全部ここに来る。 0 以上だけの列は `dense_pos` だけを
//!   u32 の列の `dense` と同じ大きさで使う
//! - **sparse**（どちらの dense にも入らない値）: `SparseRuns` (値の順に並べた `(値, eid)` の run の組、 1 件 12 B、
//!   読みは lock-free)。 ms の時刻や 64 bit の ID のように値の種類が多い列はほぼ全部ここに入る。 値ごとの
//!   件数・種類数は持たない (`slice_len_live` / `unique_live` は dense の分だけ — 正確な値は Column と
//!   突き合わせる `HimoStore` が出す)。 古い entry は bucket ごとでなく sparse 全体で `compact_sparse`。
//!
//! ## append-only + lazy verify（#95 設計）
//! - `insert` は該当 value の bucket に **append するだけ**。 旧 value からの削除はしない
//!   （swap_remove / positions は廃止）。 値更新・削除で生じる stale entry は、
//!   **read 側が Column を verify して filter** する（`HimoStore` 層の責務）。
//! - よって `total()` / `unique_count()` は churn した himo では **append 数ベースの
//!   over-count**（compaction #99 まで）。 append-only himo（削除なし）では正確。
//!   compaction は後付け最適化（#99）。

use crate::append_bucket::AppendBucket;
use crate::sparse_runs::SparseRuns;
use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;

pub const DENSE_CAP: u32 = 1 << 20;

/// 符号付き 64 bit の 0 の符号化 (`0 ^ 2^63`)。 `dense_pos` / `dense_neg` はこの上下を受け持つ。
pub const MID: u64 = 1 << 63;
const MID_HALF: u64 = (DENSE_CAP / 2) as u64;

/// dense の 3 本の配列: 0 から (`LOW`)、 `MID` から上 (`POS`)、 `MID - 1` から下 (`NEG`)。
const LOW: usize = 0;
const POS: usize = 1;
const NEG: usize = 2;

/// 値の dense での置き場所: `(配列, 添字)`。 None は sparse。
#[inline]
fn dense_slot(value: u64) -> Option<(usize, usize)> {
    if value < DENSE_CAP as u64 {
        Some((LOW, value as usize))
    } else if value >= MID && value - MID < MID_HALF {
        Some((POS, (value - MID) as usize))
    } else if value < MID && MID - 1 - value < MID_HALF {
        Some((NEG, (MID - 1 - value) as usize))
    } else {
        None
    }
}

/// `dense_slot` の逆 (配列と添字 → 値)。
#[inline]
fn slot_value(k: usize, i: usize) -> u64 {
    match k {
        LOW => i as u64,
        POS => MID + i as u64,
        _ => MID - 1 - i as u64,
    }
}

/// 値が dense に入るか (値ごとの件数・種類数を持つか)。
#[inline]
pub fn is_dense(value: u64) -> bool {
    dense_slot(value).is_some()
}

/// `new(max_values)` が事前確保する bucket 数の上限 (request23 案 F)。
/// これを超える分は `insert` の成長経路 (doubling) が必要になった時だけ確保する。
const PREALLOC_CAP: usize = 64;

type DenseArr = Vec<Arc<AppendBucket>>;

static DENSE_GROWS: AtomicUsize = AtomicUsize::new(0);

/// dense 配列を伸ばした回数 (診断用)。 事前確保をどこで打ち切るかの判断材料
/// (request23 案 F): 事前確保が十分なら 0 のまま、 打ち切ると通常経路になる。
pub fn dense_grow_count() -> usize {
    DENSE_GROWS.load(Ordering::Relaxed)
}

pub struct LockFreeCylinder {
    dense: Atomic<DenseArr>,
    /// `MID` から上 / `MID - 1` から下の値の dense (添字は `dense_slot`)。 使われるまで空。
    dense_pos: Atomic<DenseArr>,
    dense_neg: Atomic<DenseArr>,
    sparse: SparseRuns,
    /// sparse の古い entry の数 (note_stale の累計 − compact_sparse の除去分)。 0 なら sparse の read は
    /// verify 不要 (重複も古い entry も無い)。
    sparse_stale: AtomicUsize,
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
        // 事前確保は `PREALLOC_CAP` 個までで打ち切る (request23 案 F)。
        //
        // 昔は `min(max_values+1, DENSE_CAP)` = 宣言した分を全部確保していたが、 これは
        // **himo ごと・open のたび**に効くので、 大きく宣言した DB の open が
        // `max_values` に比例して伸びていた (himo 117 / max_values 20,000 で open 103.7 ms)。
        // dense は `insert` に成長経路 (doubling) を持っているので、 事前確保は純粋な
        // 最適化であって無くても動く。 打ち切っても:
        // - open は `max_values` 非依存になる (同条件で 103.7 → 5.5 ms)
        // - write は変わらない (200k tie の実測で差なし、 実 consumer の sunsu2 でも同一)
        // 低 cardinality 列 (`cardinality()` の想定用途) は `PREALLOC_CAP` 以内に収まるので、
        // hint としての意味は残る。
        let hint = if max_values == 0 {
            0
        } else {
            ((max_values as usize + 1).min(PREALLOC_CAP)) as u32
        };
        let init: DenseArr = (0..hint).map(|_| Arc::new(AppendBucket::new())).collect();
        Self {
            dense: Atomic::new(init),
            dense_pos: Atomic::new(Vec::new()),
            dense_neg: Atomic::new(Vec::new()),
            sparse: SparseRuns::default(),
            sparse_stale: AtomicUsize::new(0),
            total: AtomicUsize::new(0),
            unique_count: AtomicU32::new(0),
            stale_total: AtomicUsize::new(0),
            unique_live: AtomicU32::new(0),
            any_removed: AtomicBool::new(false),
        }
    }

    #[inline]
    fn arr(&self, k: usize) -> &Atomic<DenseArr> {
        match k {
            LOW => &self.dense,
            POS => &self.dense_pos,
            _ => &self.dense_neg,
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
    pub fn note_stale(&self, value: u64) -> Option<(usize, u32)> {
        self.any_removed.store(true, Ordering::Relaxed);
        let stats = if let Some((k, i)) = dense_slot(value) {
            let guard = epoch::pin();
            let arr = self.arr(k).load(Ordering::Acquire, &guard);
            // SAFETY: dense は常に非 null。
            let vec = unsafe { arr.deref() };
            if i < vec.len() {
                let b = &vec[i];
                b.note_stale().map(|prev| (b.len(), prev - 1))
            } else {
                debug_assert!(false, "note_stale: 未確保 bucket (value={value})");
                None
            }
        } else {
            // sparse は値ごとの件数を持たない: 古い entry の数だけ数え、 掃除は sparse 全体
            // (`sparse_needs_compact` / `compact_sparse`)
            self.stale_total.fetch_add(1, Ordering::Relaxed);
            self.sparse_stale.fetch_add(1, Ordering::Relaxed);
            return None;
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
    pub fn compact_bucket(&self, value: u64, keep: impl FnMut(u32) -> bool) -> usize {
        if let Some((k, i)) = dense_slot(value) {
            let guard = epoch::pin();
            let arr = self.arr(k).load(Ordering::Acquire, &guard);
            // SAFETY: dense は常に非 null。
            let vec = unsafe { arr.deref() };
            if i < vec.len() {
                let b = &vec[i];
                let before = b.len();
                let kept = b.compact_in(&guard, keep);
                self.discount_compacted(before - kept);
                kept
            } else {
                0
            }
        } else {
            // sparse は値ごとに組み直さない (`compact_sparse` が全体で)
            let _ = keep;
            0
        }
    }

    /// sparse の古い entry が半分を超えたか (掃除の合図)。 write_lock 下で。
    pub fn sparse_needs_compact(&self) -> bool {
        let stale = self.sparse_stale.load(Ordering::Relaxed);
        stale > 0 && stale * 2 >= self.sparse.len().max(64)
    }

    /// sparse の古い entry があるか。
    pub fn sparse_churned(&self) -> bool {
        self.sparse_stale.load(Ordering::Relaxed) > 0
    }

    /// sparse を `keep(値, eid)` (Column の現在値との照合) で組み直す。 write_lock 下・Column 更新後に。
    /// 重複 (書き換えで戻った値) も 1 つにする。 落とした数を返す。
    pub fn compact_sparse(&self, keep: impl FnMut(u64, u32) -> bool) -> usize {
        let removed = self.sparse.rebuild(keep);
        if removed > 0 {
            self.total.fetch_sub(removed, Ordering::Relaxed);
            self.stale_total.fetch_sub(removed.min(self.stale_total.load(Ordering::Relaxed)), Ordering::Relaxed);
        }
        self.sparse_stale.store(0, Ordering::Relaxed);
        removed
    }

    /// 値が `lo..=hi` の entry の eid (古い entry・重複込み、 呼び側が Column で verify する)。 dense は
    /// 範囲の bucket を順に、 sparse は run を二分探索で。
    pub fn range_raw(&self, lo: u64, hi: u64) -> Vec<u32> {
        let mut out = Vec::new();
        if lo > hi {
            return out;
        }
        if lo < DENSE_CAP as u64 {
            let guard = epoch::pin();
            let arr = self.dense.load(Ordering::Acquire, &guard);
            // SAFETY: dense は常に非 null。
            let vec = unsafe { arr.deref() };
            if !vec.is_empty() {
                let end = hi.min(vec.len() as u64 - 1);
                for v in lo..=end {
                    vec[v as usize].with_read(&guard, |s| out.extend_from_slice(s));
                }
            }
        }
        // MID の上下: どちらも添字が値の順 (上は昇順、 下は降順) なので、 範囲に当たる添字だけ
        for (k, a, b) in [
            (POS, lo.max(MID), hi.min(MID + MID_HALF - 1)),
            (NEG, lo.max(MID - MID_HALF), hi.min(MID - 1)),
        ] {
            if a > b {
                continue;
            }
            let (i0, i1) = if k == POS { (a - MID, b - MID) } else { (MID - 1 - b, MID - 1 - a) };
            let guard = epoch::pin();
            let arr = self.arr(k).load(Ordering::Acquire, &guard);
            // SAFETY: dense は常に非 null。
            let vec = unsafe { arr.deref() };
            if (i0 as usize) < vec.len() {
                for i in i0..=i1.min(vec.len() as u64 - 1) {
                    vec[i as usize].with_read(&guard, |s| out.extend_from_slice(s));
                }
            }
        }
        if hi >= DENSE_CAP as u64 {
            // sparse に dense の値は入っていないので、 範囲をそのまま渡してよい
            out.extend(self.sparse.range(lo.max(DENSE_CAP as u64), hi));
        }
        out
    }

    /// sparse の entry を `lo..=hi` で `(値, eid)` (古い entry 込み)。
    pub fn sparse_range(&self, lo: u64, hi: u64) -> Vec<(u64, u32)> {
        self.sparse.range_pairs(lo.max(DENSE_CAP as u64), hi)
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
    pub fn insert(&self, eid: u32, value: u64) {
        debug_assert!(value != u64::MAX, "value == u64::MAX is sentinel");
        let guard = epoch::pin();
        if let Some((k, i)) = dense_slot(value) {
            let dense = self.arr(k);
            let arr = dense.load(Ordering::Acquire, &guard);
            // SAFETY: dense は常に非 null。
            let vec = unsafe { arr.deref() };
            if i < vec.len() {
                let b = &vec[i];
                let prev_len = b.push_in(eid, &guard);
                let prev_live = b.live_inc();
                self.bump_stats(prev_len == 0, prev_live == 0);
            } else {
                // 成長: doubling で amortize、 既存 Arc は clone（refcount）、 新規は空 bucket
                let mut nv: DenseArr = vec.clone();
                let new_len = (i + 1).max(vec.len() * 2).min(DENSE_CAP as usize);
                nv.resize_with(new_len, || Arc::new(AppendBucket::new()));
                nv[i].push_in(eid, &guard);
                nv[i].live_inc();
                self.bump_stats(true, true); // 新 bucket は必ず空だった
                DENSE_GROWS.fetch_add(1, Ordering::Relaxed);
                dense.store(Owned::new(nv), Ordering::Release);
                // SAFETY: 旧 array は全 reader が epoch 通過後に解放。
                unsafe {
                    guard.defer_destroy(arr);
                }
            }
        } else {
            // 値の種類数 (unique_count / unique_live) は dense の分だけ数える
            self.sparse.insert(value, eid);
            self.total.fetch_add(1, Ordering::Relaxed);
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
        value: u64,
        f: impl FnOnce(&[u32]) -> R,
    ) -> Option<R> {
        let (k, i) = dense_slot(value)?;
        let arr = self.arr(k).load(Ordering::Acquire, guard);
        let vec = unsafe { arr.deref() };
        if i < vec.len() {
            Some(vec[i].with_read(guard, f))
        } else {
            Some(f(&[]))
        }
    }

    /// value の全 eid を Vec で返す（dense / sparse 両対応、 内部で pin）。
    /// 注: stale filter はしない（caller = HimoStore が Column verify する）。
    #[allow(dead_code)]
    pub fn read_to_vec(&self, value: u64) -> Vec<u32> {
        self.read_to_vec_verify(value).0
    }

    /// `read_to_vec` + この bucket の read が Column verify を要するか (request12)。
    /// 判定は `AppendBucket::read_snapshot_verify` の 3 段プロトコル
    /// (slice → flag → backing ptr 再検証) — 順序の健全性論証はそちらの doc 参照。
    pub fn read_to_vec_verify(&self, value: u64) -> (Vec<u32>, bool) {
        if let Some((k, i)) = dense_slot(value) {
            let guard = epoch::pin();
            let arr = self.arr(k).load(Ordering::Acquire, &guard);
            // SAFETY: dense は常に非 null。
            let vec = unsafe { arr.deref() };
            if i < vec.len() {
                vec[i].read_snapshot_verify(&guard)
            } else {
                (Vec::new(), false)
            }
        } else {
            // run ごとに eid 順なので並べ直す (dense の bucket と同じく eid の昇順で返す)
            let mut out = self.sparse.lookup(value);
            if out.len() > 1 {
                out.sort_unstable();
            }
            (out, self.sparse_churned())
        }
    }

    /// value の bucket 長（raw、 stale 込み）。診断用 (planner は `slice_len_live` へ移行)。
    #[allow(dead_code)]
    pub fn slice_len(&self, value: u64) -> usize {
        if let Some((k, i)) = dense_slot(value) {
            let guard = epoch::pin();
            let arr = self.arr(k).load(Ordering::Acquire, &guard);
            let vec = unsafe { arr.deref() };
            if i < vec.len() {
                vec[i].len()
            } else {
                0
            }
        } else {
            self.sparse.lookup(value).len()
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
    pub fn slice_len_live(&self, value: u64) -> usize {
        if let Some((k, i)) = dense_slot(value) {
            let guard = epoch::pin();
            let arr = self.arr(k).load(Ordering::Acquire, &guard);
            let vec = unsafe { arr.deref() };
            if i < vec.len() {
                vec[i].live() as usize
            } else {
                0
            }
        } else {
            // 古い entry 込みの上限 (正確な件数は HimoStore が Column と突き合わせる)
            self.sparse.lookup(value).len()
        }
    }

    /// value の bucket に churn 痕があるか。write_lock 下 (単一 writer) では正確 —
    /// `compact_now` の clean-bucket skip 判定用。
    pub fn bucket_needs_verify(&self, value: u64) -> bool {
        if let Some((k, i)) = dense_slot(value) {
            let guard = epoch::pin();
            let arr = self.arr(k).load(Ordering::Acquire, &guard);
            // SAFETY: dense は常に非 null。
            let vec = unsafe { arr.deref() };
            i < vec.len() && vec[i].needs_verify()
        } else {
            self.sparse_churned()
        }
    }

    /// cylinder が現在確保している eid backing の総 bytes（pow2 slack 込み、 メモリ観測用）。
    /// append-only なので各 eid は 1 度だけ載る → `total()*4 * (pow2 slack)` に収まる。
    /// double-buffer（2 コピー保持）なら `>= 2x` になるので、 それとの区別に使える。
    pub fn backing_bytes(&self) -> usize {
        let guard = epoch::pin();
        let mut slots = 0usize;
        for k in [LOW, POS, NEG] {
            let arr = self.arr(k).load(Ordering::Acquire, &guard);
            let vec = unsafe { arr.deref() };
            slots += vec.iter().map(|b| b.capacity()).sum::<usize>();
        }
        slots * std::mem::size_of::<u32>() + self.sparse.backing_bytes()
    }

    /// 非空 bucket の value を列挙（順序保証なし、 stale 込みの近似）。
    pub fn unique_values(&self) -> Vec<u64> {
        let guard = epoch::pin();
        let arr = self.dense.load(Ordering::Acquire, &guard);
        let vec = unsafe { arr.deref() };
        let mut out: Vec<u64> = vec
            .iter()
            .enumerate()
            .filter_map(|(v, b)| if b.is_empty() { None } else { Some(v as u64) })
            .collect();
        for k in [POS, NEG] {
            let arr = self.arr(k).load(Ordering::Acquire, &guard);
            let vec = unsafe { arr.deref() };
            out.extend(vec.iter().enumerate().filter(|(_, b)| !b.is_empty()).map(|(i, _)| slot_value(k, i)));
        }
        out.extend(self.sparse.values());
        out
    }
}

impl Drop for LockFreeCylinder {
    fn drop(&mut self) {
        // 現在の dense array を解放（過去 grow の defer 分は collector が回収）。
        for dense in [&self.dense, &self.dense_pos, &self.dense_neg] {
            let cur = dense.load(Ordering::Relaxed, unsafe { epoch::unprotected() });
            if !cur.is_null() {
                // SAFETY: drop = 単独所有。
                unsafe {
                    drop(cur.into_owned());
                }
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
            c.insert(e, (e % 10) as u64); // value 0..9
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
        c.insert(7, big as u64);
        c.insert(8, big as u64);
        assert_eq!(c.read_to_vec(big as u64), vec![7, 8]);
        // sparse は値ごとの件数・種類数を持たない (種類数は dense の分だけ、 件数は古い entry 込みの上限。
        // 正確な値は HimoStore が Column と突き合わせる)
        assert_eq!(c.unique_count(), 0);
        assert_eq!(c.slice_len(big as u64), 2);
        assert_eq!(c.slice_len_live(big as u64), 2);
        assert!(!c.read_to_vec_verify(big as u64).1, "古い entry が無いうちは verify 不要");
        c.note_stale(big as u64);
        assert!(c.read_to_vec_verify(big as u64).1, "古い entry があれば verify");
        assert_eq!(c.total_live(), 1);
        // 掃除: eid 7 は別の値に移った (Column では big でない) とする
        assert_eq!(c.compact_sparse(|_, eid| eid != 7), 1);
        assert_eq!(c.read_to_vec(big as u64), vec![8]);
        assert!(!c.read_to_vec_verify(big as u64).1);
        assert_eq!(c.total_live(), 1);
        assert_eq!(c.total(), 1);
    }

    /// 3 本の dense 配列の bucket 数の和。
    fn dense_len(c: &LockFreeCylinder) -> usize {
        let guard = epoch::pin();
        [LOW, POS, NEG].iter().map(|&k| unsafe { c.arr(k).load(Ordering::Acquire, &guard).deref() }.len()).sum()
    }

    /// 符号付き 64 bit の符号化 (`v ^ 2^63`) で 0 に近い値は 2 本目の dense に入る: 値ごとの件数・種類数を
    /// O(1) で持ち、 範囲も引ける。 窓の外 (±2^19 より遠い) は sparse。
    #[test]
    fn values_near_mid_are_dense() {
        let enc = |v: i64| (v as u64) ^ MID;
        let c = LockFreeCylinder::new(0);
        let h = MID_HALF as i64;
        let near = [0i64, -1, 1, 30, -30, h - 1, -h];
        for (e, &v) in near.iter().enumerate() {
            assert!(is_dense(enc(v)), "{v} は dense");
            c.insert(e as u32, enc(v));
            c.insert(100 + e as u32, enc(v));
        }
        for v in [h, -h - 1, 1 << 40, -(1 << 40)] {
            assert!(!is_dense(enc(v)), "{v} は sparse");
        }
        c.insert(50, enc(h));
        // 種類数は dense の分だけ数える = 窓の中の値は全部数えられている
        assert_eq!(c.unique_live(), near.len() as u32);
        for (e, &v) in near.iter().enumerate() {
            assert_eq!(c.read_to_vec(enc(v)), vec![e as u32, 100 + e as u32], "{v}");
            assert_eq!(c.slice_len_live(enc(v)), 2, "{v}");
        }
        let mut vals = c.unique_values();
        vals.sort_unstable();
        let mut want: Vec<u64> = near.iter().map(|&v| enc(v)).chain([enc(h)]).collect();
        want.sort_unstable();
        assert_eq!(vals, want, "添字 → 値の戻し");
        // 範囲: 窓の中と外 (sparse) をまたいで
        let mut got = c.range_raw(enc(-30), enc(h));
        got.sort_unstable();
        assert_eq!(got, vec![0, 1, 2, 3, 4, 5, 50, 100, 101, 102, 103, 104, 105], "-30..=2^19 (-2^19 の 6 だけ外)");
        let mut got = c.range_raw(enc(-h), enc(-1));
        got.sort_unstable();
        assert_eq!(got, vec![1, 4, 6, 101, 104, 106]);
        // 0 以上だけの列は u32 の列と同じ数の bucket (上の配列だけ、 値の数ぶん)。 負をまたいでも値の数ぶん
        let n = 1000u32;
        let (as_u32, as_i64, both) = (LockFreeCylinder::new(0), LockFreeCylinder::new(0), LockFreeCylinder::new(0));
        for v in 0..n {
            as_u32.insert(v, v as u64);
            as_i64.insert(v, enc(v as i64));
            both.insert(v, enc(v as i64 - (n / 2) as i64));
        }
        assert_eq!(dense_len(&as_i64), dense_len(&as_u32), "0 以上の i64 は u32 と同じ数の bucket");
        assert!(dense_len(&both) <= dense_len(&as_u32), "{} / {}", dense_len(&both), dense_len(&as_u32));
        // 掃除・古い entry の印も 2 本目で
        c.note_stale(enc(-1));
        assert!(c.read_to_vec_verify(enc(-1)).1);
        assert_eq!(c.compact_bucket(enc(-1), |eid| eid != 1), 1);
        assert_eq!(c.read_to_vec(enc(-1)), vec![101]);
    }

    /// request12: verify 判定が bucket 局所であること + live counter の正確性。
    #[test]
    fn bucket_local_verify_and_live_counters() {
        let c = LockFreeCylinder::new(0);
        // v0 に 5 eid、v1 に 5 eid
        for e in 0..10u32 {
            c.insert(e, (e % 2) as u64);
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
            c.insert(e, (e % card) as u64);
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

    /// dense の **成長中に reader が読む** 形。 事前確保 (`new(max_values)`) がある間は
    /// 成長は 「宣言を超えた時」 しか起きないので、 この経路は実質踏まれていなかった。
    /// 事前確保を打ち切る (request23 案 F) と **主経路になる**ので、 先に押さえる。
    ///
    /// writer が value を増やしながら insert → `dense.store(Owned::new(nv))` +
    /// `defer_destroy(旧配列)` が繰り返し走る。 その間 reader は既に入れた value を読み続け、
    /// **読めた eid が正しいこと** (別 value の eid が混ざらない / 壊れた値が出ない) を見る。
    ///
    /// 名前を `dense` で始めているのは CI の miri が
    /// `lockfree_cylinder::tests::dense` で絞っているため (= この test も UB 検証に乗る)。
    #[test]
    fn dense_grows_while_readers_read() {
        let c = Arc::new(LockFreeCylinder::new(0));
        let n: u32 = if cfg!(miri) { 64 } else { 20_000 };
        let ready = Arc::new(AtomicU32::new(0));
        let stop = Arc::new(AtomicBool::new(false));

        // writer: value を 0..n と増やしながら入れる (eid == value にしておく)。
        let writer = {
            let (c, ready) = (c.clone(), ready.clone());
            std::thread::spawn(move || {
                for v in 0..n {
                    c.insert(v, v as u64);
                    ready.store(v + 1, Ordering::Release);
                }
            })
        };

        // reader: 既に入った範囲を読み続ける。 eid == value が守られていること。
        let readers: Vec<_> = (0..3)
            .map(|k| {
                let (c, ready, stop) = (c.clone(), ready.clone(), stop.clone());
                std::thread::spawn(move || {
                    let mut seen = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        let hi = ready.load(Ordering::Acquire);
                        if hi == 0 {
                            std::hint::spin_loop();
                            continue;
                        }
                        let v = (seen.wrapping_mul(2654435761).wrapping_add(k) as u32) % hi;
                        let got = c.read_to_vec(v as u64);
                        assert!(
                            got.iter().all(|&e| e == v),
                            "value {v} に別の eid が混ざった: {got:?}"
                        );
                        seen += 1;
                    }
                    seen
                })
            })
            .collect();

        writer.join().unwrap();
        stop.store(true, Ordering::Relaxed);
        let reads: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();

        // 成長が終わった後、 全部が読めること (取りこぼしが無い)。
        for v in 0..n {
            assert_eq!(c.read_to_vec(v as u64), vec![v], "value {v} が消えた");
        }
        assert_eq!(c.unique_count(), n, "unique_count が合わない");
        assert!(reads > 0, "reader が 1 回も読めていない");
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
            c.insert(e, (e % 7) as u64);
        }
        stop.store(true, Ordering::Relaxed);
        for r in readers {
            r.join().unwrap();
        }
        assert_eq!(c.total(), n as usize);
        assert_eq!(c.unique_count(), 7);
    }
}
