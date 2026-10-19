//! `LockFreeCylinder` — value → eid の lock-free concurrent bucket store。 #95。
//!
//! 現状の `RwLock<BucketCylinder>` を置き換える。 **bucket ごとに同時に 1 writer** / 多 reader。
//! writer は 1 thread とは限らない（consumer + 同期 tie + schema commit）ので、
//! 呼び出し側 = `HimoStore` が契約を満たす: bucket の書き手は値ごとの鍵で 1 本、 sparse と配列の差し替えの書き手は
//! per-himo `write_lock` で 1 本 (差し替える書き手は全部の鍵も持つ、 `crate::value_locks`、 2026-10-11)。 前は
//! `write_lock` で全部の書き手を 1 本に並べていた。
//!
//! ## 構造
//! - **dense**: 値ごとの bucket の配列 3 本 (`Atomic<Vec<Slot>>`)。 0 から (`dense`、 値 < `DENSE_CAP`)、 `MID` = 2^63
//!   から上 (`dense_pos`、 `MID + i`)、 `MID - 1` から下 (`dense_neg`、 `MID - 1 - i`) — schema / SQL の符号付き 64 bit
//!   の列は大小の順を保つ `v ^ 2^63` で置くので、 0 に近い符号付きの値 (年齢・件数・負の小さな数) は後の 2 本に来る。
//!   外側 Vec を epoch-swap で成長させ、 `AppendBucket` 本体は `Arc` で stable（成長で動かない）。 read は完全
//!   lock-free。 要素 ([`Slot`]) は、 その値が 1 件でも入るまで空 (bucket を作らない、 #358)
//! - **sparse**: `SparseRuns` (値の順に並べた `(値, eid)` の run の組、 1 件 12 B、 読みは lock-free)。 dense の
//!   配列の**今の長さの外**の値は全部ここ。 ms の時刻や 64 bit の ID のように値の種類が多い列はほぼ全部ここに入る。
//!   値ごとの件数・種類数は持たない (`slice_len_live` / `unique_live` は dense の分だけ — 正確な値は Column と
//!   突き合わせる `HimoStore` が出す)。 古い entry は bucket ごとでなく sparse 全体で `compact_sparse`。
//!
//! ## 値の置き場所 = 配列の今の長さ (#373)
//! 値は、 その配列の添字が**今の配列の長さ未満なら dense、 それ以外は sparse**。 配列を伸ばすのは、 伸ばした後も
//! 中身が十分に詰まっている時だけ (`DENSE_FREE_LEN` / `DENSE_MIN_FILL`)。 Tag の値は DB で 1 つの辞書の ID なので、
//! 他の表の行が辞書 ID を進めると、 1 つの列の値は辞書 ID の範囲に**まばらに**散る。 昔は値の入った位置まで配列を
//! 必ず伸ばしたので、 配列 (空の要素 1 つ 8 B) が 「その列の値の数」 でなく 「辞書全体の大きさ」 に比例した
//! (表 16 個で 1 列あたり辞書の 1/16 しか埋まらない)。 今はまばらな値は sparse に置き、 dense の配列の大きさは
//! 中身の数に比例する。
//!
//! 配列を伸ばす時は、 新しく覆う範囲の値を sparse から dense の bucket へ移す (`insert` の成長経路)。 順序:
//! bucket に入れた配列を差し替えて (= 長さが伸びる) から、 sparse から消す。 読み手は配列の長さで置き場所を
//! 決めるので、 差し替え前に読んだ読み手は sparse に、 後に読んだ読み手は dense に、 どちらも値を見つける。
//! 古い長さで sparse を読みに行った間に消された時のために、 sparse を読んだ後で配列の長さを読み直す
//! ([`LockFreeCylinder::with_value`])。
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
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

pub const DENSE_CAP: u32 = 1 << 20;

/// 符号付き 64 bit の 0 の符号化 (`0 ^ 2^63`)。 `dense_pos` / `dense_neg` はこの上下を受け持つ。
pub const MID: u64 = 1 << 63;
const MID_HALF: u64 = (DENSE_CAP / 2) as u64;

/// dense の 3 本の配列: 0 から (`LOW`)、 `MID` から上 (`POS`)、 `MID - 1` から下 (`NEG`)。
const LOW: usize = 0;
const POS: usize = 1;
const NEG: usize = 2;

/// 配列をこの長さまでは、 中身の数を問わず伸ばす (1 本 8 KB)。 値の小さな列 (年齢・件数・宣言の小さな列) は
/// 今までどおり全部 dense。
const DENSE_FREE_LEN: usize = 1024;

/// 配列を `DENSE_FREE_LEN` より長く伸ばすのは、 伸ばした後の長さが 「bucket のある要素 + sparse で待っている
/// entry」 のこの倍以下の時だけ (#373)。 空の要素 1 つ 8 B なので、 中身 1 つあたりの配列は最大 32 B
/// (倍々で伸ばすので、 伸ばした直後は最大 64 B)。
const DENSE_MIN_FILL: usize = 4;

/// 値の区間 `(最小, 最大, 置き場)`。 置き場は `Some(配列)` か `None` (sparse) ([`LockFreeCylinder::segments`])。
type Segment = (u64, u64, Option<usize>);

/// [`LockFreeCylinder::for_each_part`] が渡す置き場の 1 単位。 `verify` = 古い entry がありうる (Column で今の値を確かめる。
/// 無ければ全部 live で重複も無い)。
pub(crate) enum Part<'a> {
    /// dense の値 1 つの bucket: eid の列 (置いてある順) と、 その頭の昇順 (広義) の長さ。
    Bucket { value: u64, eids: &'a [u32], head: usize, verify: bool },
    /// sparse の run 1 本の、 値が `lo..=hi` の区間の eid の列 ((値, eid) の順)。 `lo..=hi` は sparse の区間 (`segments`) で
    /// 切ってある: 区間どうしは重ならず、 dense の bucket の値とも重ならない。
    Run { lo: u64, hi: u64, eids: &'a [u32], verify: bool },
}

/// 配列 `k` の添字の上限 (配列はここまでしか伸ばさない)。
#[inline]
fn region_cap(k: usize) -> usize {
    if k == LOW { DENSE_CAP as usize } else { MID_HALF as usize }
}

/// test 用の割り込み点: 名前の一致した 1 回だけ、 登録した処理をこの thread で走らせる。 読み手と書き手の
/// 競合の窓 (配列を伸ばして sparse から消す間) を、 thread の運に頼らず踏むため。 test 以外では何もしない。
#[cfg(test)]
mod hook {
    use std::cell::RefCell;

    type Hook = (&'static str, Box<dyn FnOnce()>);

    thread_local! {
        static HOOK: RefCell<Option<Hook>> = const { RefCell::new(None) };
    }

    pub fn set(at: &'static str, f: impl FnOnce() + 'static) {
        HOOK.with(|h| *h.borrow_mut() = Some((at, Box::new(f))));
    }

    pub fn at(name: &'static str) {
        let hit = HOOK.with(|h| {
            let mut h = h.borrow_mut();
            if h.as_ref().is_some_and(|(at, _)| *at == name) { h.take() } else { None }
        });
        if let Some((_, f)) = hit {
            f();
        }
    }
}

#[cfg(not(test))]
mod hook {
    #[inline(always)]
    pub fn at(_: &'static str) {}
}

/// `new(max_values)` が事前確保する bucket 数の上限 (request23 案 F)。
/// これを超える分は `insert` の成長経路 (doubling) が必要になった時だけ確保する。
const PREALLOC_CAP: usize = 64;

/// dense の 1 要素: その値の bucket を指すか、 **空** (その値がまだ 1 件も入っていない)。
///
/// #358: 昔は全要素が `Arc<AppendBucket>` で、 配列を伸ばすたびに間の全要素へ空の bucket を作っていた
/// (1 要素あたり `Arc` と `Buf` の確保で約 90 B)。 今は空の要素は null の pointer 1 つ (8 B) だけ。
///
/// - 指す先は **空 → bucket の 1 回だけ**変わる (置くのは書き手、 [`Slot::get_or_create`])。 1 度置いたら
///   その `Slot` が drop するまで同じ bucket を指す
/// - 配列を伸ばす時の写し ([`Clone`]) は同じ bucket を指す (`Arc` の参照を 1 つ増やす)。 bucket は配列が
///   代わっても動かない
/// - 古い配列を掴んだ読み手は、 その後に置かれた bucket を見ない (空と読む) — 配列を伸ばす前の値の読みと同じで、
///   読みが書き込みより前に並ぶだけ
pub(crate) struct Slot(AtomicPtr<AppendBucket>);

impl Slot {
    #[inline]
    pub(crate) fn empty() -> Self {
        Slot(AtomicPtr::new(std::ptr::null_mut()))
    }

    /// bucket (空なら `None`)。 読み手・書き手のどちらからでも。
    #[inline]
    pub(crate) fn get(&self) -> Option<&AppendBucket> {
        let p = self.0.load(Ordering::Acquire);
        // SAFETY: 非 null なら `Arc::into_raw` で置いた pointer で、 この Slot が参照を 1 つ持っている。
        // 1 度置いたら変わらないので、 Slot が生きている間 (`&self` の間) は有効。
        unsafe { p.as_ref() }
    }

    /// bucket (空なら作って置く) と、 今作ったか。 **その値の書き手だけ** — `HimoStore` の値の鍵の下で同時に 1 本。
    #[inline]
    pub(crate) fn get_or_create(&self) -> (&AppendBucket, bool) {
        self.get_or_create_with(0)
    }

    /// `get_or_create` の、 作る時に `cap` 件分を先に確保する版 (索引をまとめて組む時、 #394)。
    #[inline]
    fn get_or_create_with(&self, cap: usize) -> (&AppendBucket, bool) {
        if let Some(b) = self.get() {
            return (b, false);
        }
        let p = Arc::into_raw(Arc::new(AppendBucket::with_capacity(cap))) as *mut AppendBucket;
        self.0.store(p, Ordering::Release);
        // SAFETY: 今置いた pointer (参照はこの Slot が持つ)。
        (unsafe { &*p }, true)
    }
}

impl Clone for Slot {
    fn clone(&self) -> Self {
        let p = self.0.load(Ordering::Acquire);
        if !p.is_null() {
            // SAFETY: `Arc::into_raw` 由来で、 `self` が参照を持っている間は生きている。 写しの分を 1 つ足す。
            unsafe { Arc::increment_strong_count(p as *const AppendBucket) };
        }
        Slot(AtomicPtr::new(p))
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        let p = *self.0.get_mut();
        if !p.is_null() {
            // SAFETY: この Slot が持っていた参照を 1 つ返す。
            unsafe { drop(Arc::from_raw(p as *const AppendBucket)) };
        }
    }
}

type DenseArr = Vec<Slot>;

static DENSE_GROWS: AtomicUsize = AtomicUsize::new(0);

/// dense 配列を伸ばした回数 (診断用)。 事前確保をどこで打ち切るかの判断材料
/// (request23 案 F): 事前確保が十分なら 0 のまま、 打ち切ると通常経路になる。
pub fn dense_grow_count() -> usize {
    DENSE_GROWS.load(Ordering::Relaxed)
}

pub struct LockFreeCylinder {
    /// `dense` の 0 番に当たる値 ([`set_low_base`](Self::set_low_base)、 既定 0)。 組んだ後は変わらない。
    low_base: AtomicU64,
    dense: Atomic<DenseArr>,
    /// `MID` から上 / `MID - 1` から下の値の dense (添字は `dense_slot`)。 使われるまで空。
    dense_pos: Atomic<DenseArr>,
    dense_neg: Atomic<DenseArr>,
    sparse: SparseRuns,
    /// 配列ごとの、 bucket のある要素の数 (書き手だけが触る)。 配列を伸ばすかの判断に使う (#373)。
    used: [AtomicUsize; 3],
    /// 配列ごとの、 その配列の窓に入る値で、 今の長さの外にあるので sparse に置いた entry の数 (古い entry 込み、
    /// 書き手だけが触る)。 配列を伸ばせば dense に移る候補 (#373)。
    waiting: [AtomicUsize; 3],
    /// sparse の古い entry の数 (note_stale の累計 − compact_sparse の除去分)。 0 なら sparse の read は
    /// verify 不要 (重複も古い entry も無い)。
    sparse_stale: AtomicUsize,
    /// 一度でも要素が入った bucket 数 (raw)。診断用。
    ///
    /// 2026-10-11: backing に現存する slot 総数 (`total`) と stale slot 総数 (`stale_total`) は持たない。 書き込みのたびに
    /// 全部の書き手が同じ数を足し引きすると、 値の鍵で並んで書く書き手どうしが 1 本の cache line を取り合う。 読むのは
    /// test と診断だけなので、 読む時に bucket ごとの数と sparse の数から出す ([`total`](Self::total) /
    /// [`total_live`](Self::total_live))。
    unique_count: AtomicU32,
    /// live > 0 の bucket 数 (= 正確な cardinality)。request12。
    unique_live: AtomicU32,
    /// delete / 値更新が一度でも起きたか (himo 全体、診断用)。
    /// read の verify 判定は request12 で bucket 単位 (`AppendBucket::needs_verify`)
    /// に局所化された — churn していない bucket は fast path のまま。
    any_removed: AtomicBool,
}

// SAFETY: bucket ごとに単一 writer (sparse と配列の差し替えは全体で単一 writer) / 多 reader。 dense / sparse とも epoch で
// publish。
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
        // - write は変わらない (200k tie の実測で差なし、 実 consumer の負荷 harness でも同一)
        // 低 cardinality 列 (`cardinality()` の想定用途) は `PREALLOC_CAP` 以内に収まるので、
        // hint としての意味は残る。
        let hint = if max_values == 0 {
            0
        } else {
            ((max_values as usize + 1).min(PREALLOC_CAP)) as u32
        };
        // 確保するのは要素 (空の Slot) だけ。 bucket は値が入った時に作る (#358)
        let init: DenseArr = (0..hint).map(|_| Slot::empty()).collect();
        Self {
            low_base: AtomicU64::new(0),
            dense: Atomic::new(init),
            dense_pos: Atomic::new(Vec::new()),
            dense_neg: Atomic::new(Vec::new()),
            sparse: SparseRuns::default(),
            used: Default::default(),
            waiting: Default::default(),
            sparse_stale: AtomicUsize::new(0),
            unique_count: AtomicU32::new(0),
            unique_live: AtomicU32::new(0),
            any_removed: AtomicBool::new(false),
        }
    }

    /// `dense` (0 から上の値の配列) の 0 番に当たる値を `base` にする (既定 0)。 ref の列は指す先の表の eid の
    /// 始まりにすると、 2^20 より後ろの表を指す ref も dense に置ける (2026-10-11。 表を 3 つ作ると後の表の eid は
    /// 2^20 の先になり、 その表を指す ref の値は全部 sparse に入って、 会社ごとの読みが遅かった)。 `base` より前の値は
    /// sparse に置く (答えは同じ)。
    ///
    /// **空の索引にだけ** 置ける: `HimoStore` が組む前に write_lock の下で呼ぶ (読み手は組み終わりの flag の
    /// Release / Acquire の後にしか索引を読まないので、 この値を見る)。 `MID` の上下の窓と重なる `base` は 0 にする。
    pub fn set_low_base(&self, base: u64) {
        debug_assert_eq!(self.total(), 0, "set_low_base は空の索引に");
        let base = if base <= MID - MID_HALF - DENSE_CAP as u64 { base } else { 0 };
        self.low_base.store(base, Ordering::Relaxed);
    }

    /// `dense` の 0 番に当たる値 ([`set_low_base`](Self::set_low_base))。
    pub fn low_base(&self) -> u64 {
        self.low_base.load(Ordering::Relaxed)
    }

    /// 値の dense での位置: `(配列, 添字)`。 None は dense の窓の外 (常に sparse)。 添字が今の配列の長さ以上なら
    /// その値も sparse にある (module doc)。 `LOW` の窓は `low_base` から `DENSE_CAP` 個。
    #[inline]
    fn dense_slot(&self, value: u64) -> Option<(usize, usize)> {
        // value < low_base なら wrapping_sub は大きな数になって窓の外
        let off = value.wrapping_sub(self.low_base.load(Ordering::Relaxed));
        if off < DENSE_CAP as u64 {
            Some((LOW, off as usize))
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
    fn slot_value(&self, k: usize, i: usize) -> u64 {
        match k {
            LOW => self.low_base.load(Ordering::Relaxed) + i as u64,
            POS => MID + i as u64,
            _ => MID - 1 - i as u64,
        }
    }

    /// 配列 `k` の添字 `from..to` に当たる値の範囲 (`lo..=hi`、 `from < to`)。
    #[inline]
    fn region_values(&self, k: usize, from: usize, to: usize) -> (u64, u64) {
        let (a, b) = (self.slot_value(k, from), self.slot_value(k, to - 1));
        (a.min(b), a.max(b))
    }

    #[inline]
    fn arr(&self, k: usize) -> &Atomic<DenseArr> {
        match k {
            LOW => &self.dense,
            POS => &self.dense_pos,
            _ => &self.dense_neg,
        }
    }

    /// 配列 `k` の今の中身。
    #[inline]
    fn vec_of<'g>(&self, k: usize, guard: &'g Guard) -> &'g DenseArr {
        // SAFETY: dense は常に非 null、 古い配列は epoch を過ぎるまで解放されない。
        unsafe { self.arr(k).load(Ordering::Acquire, guard).deref() }
    }

    /// 配列 `k` の添字 `i` が今の配列の中なら、 その要素の bucket (`Some(None)` = 中だが空の要素)。 外なら `None`
    /// (= その値は sparse に置いている)。
    #[inline]
    fn dense_at<'g>(&self, k: usize, i: usize, guard: &'g Guard) -> Option<Option<&'g AppendBucket>> {
        self.vec_of(k, guard).get(i).map(Slot::get)
    }

    /// 3 本の配列の今の長さ。
    fn lens(&self, guard: &Guard) -> [usize; 3] {
        [LOW, POS, NEG].map(|k| self.vec_of(k, guard).len())
    }

    /// 値を読む: 今 dense に置いているなら `on_dense` (bucket、 空の要素なら `None`)、 そうでなければ `on_sparse`。
    ///
    /// 配列を伸ばす書き手は、 新しく覆う値を sparse から dense へ移し、 配列を差し替えてから sparse から消す
    /// (module doc)。 古い長さを見て sparse を読む間に消されることがあるので、 sparse を読んだ後で配列の長さを
    /// 読み直し、 値が dense に移っていたら dense を読む。 sparse の消した後の版を見たなら、 それより前に
    /// 差し替えた配列も見える (sparse の publish は配列の store の後、 どちらも Acquire / Release)。
    fn with_value<R>(
        &self,
        value: u64,
        mut on_dense: impl FnMut(Option<&AppendBucket>, &Guard) -> R,
        on_sparse: impl FnOnce(&SparseRuns) -> R,
    ) -> R {
        let guard = epoch::pin();
        let Some((k, i)) = self.dense_slot(value) else {
            return on_sparse(&self.sparse);
        };
        if let Some(b) = self.dense_at(k, i, &guard) {
            return on_dense(b, &guard);
        }
        hook::at("reader_missed_dense");
        let r = on_sparse(&self.sparse);
        match self.dense_at(k, i, &guard) {
            Some(b) => on_dense(b, &guard),
            None => r,
        }
    }

    /// 書き手用: 今 dense に置いている値なら、 その bucket (`Some(None)` = 空の要素)。 書き手は配列を差し替える
    /// 本人なので読み直しは要らない。
    fn writer_dense_at<'g>(&self, value: u64, guard: &'g Guard) -> Option<Option<&'g AppendBucket>> {
        let (k, i) = self.dense_slot(value)?;
        self.dense_at(k, i, guard)
    }

    /// 値を今 dense に置いているか (値ごとの件数・種類数を O(1) で持つか)。 配列が伸びると false → true に
    /// 変わりうる (逆は無い)。
    pub fn is_dense(&self, value: u64) -> bool {
        let guard = epoch::pin();
        self.writer_dense_at(value, &guard).is_some()
    }

    /// これまでに delete / 値更新があったか（himo 全体、診断用）。
    /// verify の要否判定には使わない (bucket 単位 flag に局所化、request12)。
    #[allow(dead_code)]
    #[inline]
    pub fn any_removed(&self) -> bool {
        self.any_removed.load(Ordering::Relaxed)
    }

    /// 値を今の配列の中 (bucket で持つ所) に置くか。 配列は伸びるだけなので、 一度 true なら以後も true。
    #[inline]
    pub(crate) fn in_array(&self, value: u64, guard: &Guard) -> bool {
        self.dense_slot(value).is_some_and(|(k, i)| i < self.vec_of(k, guard).len())
    }

    /// 値更新 / 削除で `value` の bucket に stale が残ることを記録する (request12)。
    /// 該当 bucket の removed flag を立て、live を -1。 その bucket の書き手 1 本 (`HimoStore` の値の鍵の下、 sparse の
    /// 値は write_lock の下) で呼ぶ。
    /// **Column を書き換える前に呼ぶこと** (reader が「flag 未設定なのに Column は
    /// 新値」を観測する窓を作らない — 旧 mark_removed と同じ順序契約)。
    ///
    /// 戻り値は `(bucket の raw len, 減分後の live)` — 呼び出し側 (HimoStore) が
    /// incremental compaction の trigger 判定 (stale 率) に使う (P2)。
    pub fn note_stale(&self, value: u64) -> Option<(usize, u32)> {
        self.note_stale_in(value, &epoch::pin())
    }

    /// [`note_stale`](Self::note_stale) の外部 guard 版 (書き手が 1 回の書き込みで pin を 1 回にする)。
    pub(crate) fn note_stale_in(&self, value: u64, guard: &Guard) -> Option<(usize, u32)> {
        // 立っていれば書かない (書き手が並ぶと、 同じ値の store でも cache line を取り合う)
        if !self.any_removed.load(Ordering::Relaxed) {
            self.any_removed.store(true, Ordering::Relaxed);
        }
        let stats = match self.writer_dense_at(value, guard) {
            Some(Some(b)) => b.note_stale().map(|prev| (b.len_in(guard), prev - 1)),
            Some(None) => {
                debug_assert!(false, "note_stale: 未確保 bucket (value={value})");
                None
            }
            None => {
                // sparse は値ごとの件数を持たない: 古い entry の数だけ数え、 掃除は sparse 全体
                // (`sparse_needs_compact` / `compact_sparse`)
                self.sparse_stale.fetch_add(1, Ordering::Release);
                return None;
            }
        };
        match stats {
            Some((_, live_after)) => {
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
        self.compact_bucket_in(value, &epoch::pin(), keep)
    }

    /// [`compact_bucket`](Self::compact_bucket) の外部 guard 版。
    pub(crate) fn compact_bucket_in(&self, value: u64, guard: &Guard, keep: impl FnMut(u32) -> bool) -> usize {
        match self.writer_dense_at(value, guard) {
            Some(Some(b)) => {
                b.compact_in(guard, keep)
            }
            // 空の要素 / sparse は値ごとに組み直さない (`compact_sparse` が全体で)
            _ => 0,
        }
    }

    /// 値 `value` の bucket を照らして読むと決めた時に呼ぶ: dense の bucket なら今の (len, live) を控え、 (bucket の長さ, 間に書き込みの
    /// 無い照らしを続けた数) を返す ([`AppendBucket::mark_verified`])。 sparse の値・空の要素は None。
    pub fn mark_verified(&self, value: u64) -> Option<(usize, u32)> {
        let guard = epoch::pin();
        match self.writer_dense_at(value, &guard) {
            Some(Some(b)) => Some((b.len(), b.mark_verified(&guard))),
            _ => None,
        }
    }

    /// sparse の古い entry が半分を超えたか (掃除の合図)。 write_lock 下で。
    pub fn sparse_needs_compact(&self) -> bool {
        let stale = self.sparse_stale.load(Ordering::Relaxed);
        stale > 0 && stale * 2 >= self.sparse.len().max(64)
    }

    /// sparse の古い entry があるか。 書き手 (write_lock の下) 用。
    pub fn sparse_churned(&self) -> bool {
        self.sparse_stale.load(Ordering::Relaxed) > 0
    }

    /// 読み手用: sparse を **読む前** に、 古い entry がありうるかを見る。 読んだ後に見ると、 組み直す前の版 (古い entry
    /// 入り) を読んだまま、 組み直した後の 「古い entry なし」 を見て、 古い entry を確かめずに live として返す
    /// (`compact_sparse` は組み直した版を出してから数を 0 に戻す。 ここで 0 を Acquire で見たなら、 この後に読む版は
    /// 組み直した後のもの)。 並行の書き込みの test で、 会社単位の購読が、 もう別の会社に移った社員を返して見つかった。
    #[inline]
    fn sparse_churned_before_read(&self) -> bool {
        self.sparse_stale.load(Ordering::Acquire) > 0
    }

    /// sparse を `keep(値, eid)` (Column の現在値との照合) で組み直す。 write_lock 下・Column 更新後に。
    /// 重複 (書き換えで戻った値) も 1 つにする。 落とした数を返す。
    pub fn compact_sparse(&self, keep: impl FnMut(u64, u32) -> bool) -> usize {
        let removed = self.sparse.rebuild(keep);
        // 組み直した版を出した **後** に 0 へ戻す (Release、 読み手は `sparse_churned_before_read` で Acquire)
        self.sparse_stale.store(0, Ordering::Release);
        self.recount_waiting();
        removed
    }

    /// `waiting` を sparse の中身から数え直す (書き手、 sparse を組み直した後に)。
    fn recount_waiting(&self) {
        let guard = epoch::pin();
        let lens = self.lens(&guard);
        for k in [LOW, POS, NEG] {
            let n = if lens[k] < region_cap(k) {
                let (lo, hi) = self.region_values(k, lens[k], region_cap(k));
                self.sparse.count_in(lo, hi)
            } else {
                0
            };
            self.waiting[k].store(n, Ordering::Relaxed);
        }
    }

    /// 値が `lo..=hi` の entry の eid (古い entry・重複込み、 呼び側が Column で verify して重複を除く)。 dense は
    /// 範囲の bucket を順に、 sparse は run を二分探索で。
    pub fn range_raw(&self, lo: u64, hi: u64) -> Vec<u32> {
        let mut out = Vec::new();
        if lo > hi {
            return out;
        }
        let guard = epoch::pin();
        let before = self.lens(&guard);
        self.dense_range_into(lo, hi, [0; 3], before, &guard, &mut out);
        hook::at("range_read_dense");
        // sparse には今の長さの外の値がある。 配列を伸ばした直後は、 移した値がまだ残っていることもある (重複は
        // 呼び側が除く)
        out.extend(self.sparse.range(lo, hi));
        // sparse を読む間に配列が伸びて値が dense に移っていたら、 増えた分を dense から読む (`with_value` と同じ)
        let after = self.lens(&guard);
        if after != before {
            self.dense_range_into(lo, hi, before, after, &guard, &mut out);
        }
        out
    }

    /// 3 本の配列の添字 `from[k]..to[k]` のうち、 値が `lo..=hi` の bucket の中身を `out` に足す。
    fn dense_range_into(&self, lo: u64, hi: u64, from: [usize; 3], to: [usize; 3], guard: &Guard, out: &mut Vec<u32>) {
        for k in [LOW, POS, NEG] {
            if from[k] >= to[k] {
                continue;
            }
            // 範囲 lo..=hi に当たる添字 (配列 k の窓の中だけ)
            let (wlo, whi) = self.region_values(k, 0, region_cap(k));
            let (a, b) = (lo.max(wlo), hi.min(whi));
            if a > b {
                continue;
            }
            let (i0, i1) = match k {
                LOW => ((a - wlo) as usize, (b - wlo) as usize),
                POS => ((a - MID) as usize, (b - MID) as usize),
                _ => ((MID - 1 - b) as usize, (MID - 1 - a) as usize),
            };
            let (i0, i1) = (i0.max(from[k]), i1.min(to[k] - 1));
            if i0 > i1 {
                continue;
            }
            let vec = self.vec_of(k, guard);
            let i1 = i1.min(vec.len().saturating_sub(1));
            if vec.is_empty() || i0 > i1 {
                continue;
            }
            // 空の要素は飛ばす (添字の順 = 上の配列は値の昇順、 下の配列は値の降順)
            for b in vec[i0..=i1].iter().filter_map(Slot::get) {
                b.with_read(guard, |s| out.extend_from_slice(s));
            }
        }
    }

    /// sparse の entry を `lo..=hi` で `(値, eid)` (古い entry 込み)。 dense に移した値 (配列を伸ばした直後に
    /// sparse に残っているもの) は除く。
    pub fn sparse_range(&self, lo: u64, hi: u64) -> Vec<(u64, u32)> {
        let mut out = self.sparse.range_pairs(lo, hi);
        let guard = epoch::pin();
        let lens = self.lens(&guard);
        out.retain(|&(v, _)| self.dense_slot(v).is_none_or(|(k, i)| i >= lens[k]));
        out
    }

    /// 配列 `k` の窓の中の値の添字 ([`slot_value`](Self::slot_value) の逆)。
    #[inline]
    fn slot_index(&self, k: usize, value: u64) -> usize {
        match k {
            LOW => (value - self.low_base.load(Ordering::Relaxed)) as usize,
            POS => (value - MID) as usize,
            _ => (MID - 1 - value) as usize,
        }
    }

    /// 値の空間を、 配列の今の長さ `lens` で区間に分けて値の昇順に並べる: `(区間の最小, 最大, 置き場)`。 置き場は
    /// `Some(配列)` か `None` (sparse)。 区間ごとに置き場が 1 つに決まる (配列が覆う値は bucket、 それ以外は sparse、
    /// module doc): sparse `[0, base)`、 `LOW`、 sparse (間)、 `NEG`、 `POS`、 sparse (上)。 `set_low_base` が `LOW` の窓を
    /// `NEG` の窓より下に置くので、 区間は重ならない。 返り値は区間の配列と、 その中の使う数 (区間は 6 つまで。 読み 1 回ごとに
    /// 呼ぶので heap を使わない)。
    fn segments(&self, lens: [usize; 3]) -> ([Segment; 6], usize) {
        let base = self.low_base.load(Ordering::Relaxed);
        let (nl, np, nn) = (lens[LOW] as u64, lens[POS] as u64, lens[NEG] as u64);
        let mut segs = [(0u64, 0u64, None::<usize>); 6];
        let mut len = 0;
        let mut push = |seg: Segment| {
            segs[len] = seg;
            len += 1;
        };
        // 次の区間の始まり
        let mut at = 0u64;
        if nl > 0 {
            if base > 0 {
                push((0, base - 1, None));
            }
            push((base, base + nl - 1, Some(LOW)));
            at = base + nl;
        }
        let neg_lo = MID - nn;
        if at < neg_lo {
            push((at, neg_lo - 1, None));
        }
        if nn > 0 {
            push((neg_lo, MID - 1, Some(NEG)));
        }
        if np > 0 {
            push((MID, MID + np - 1, Some(POS)));
        }
        push((MID + np, u64::MAX, None));
        (segs, len)
    }

    /// 値が `lo..=hi` の entry を値の順 (`desc` なら大きい順) に、 値ごとに `f(値, eid の列, 頭の昇順の長さ, Column で確かめるか)`
    /// に渡す。 eid の列は古い entry・重複込みで並べていない (`HimoStore::walk_values` が確かめて並べる)。 頭の昇順の長さは
    /// 置き場が渡す (読む側は探さない): dense の bucket は書き手が足すたびに持っている数 ([`AppendBucket::snapshot_sorted_in`])、
    /// sparse の値は run ごとの区間のつなぎ目を比べた数 ([`SparseSnap::walk`](crate::sparse_runs::SparseSnap::walk))。
    /// dense の空の要素と live が 0 の bucket は渡さない。 `f` が `Break` を返したら止める。
    ///
    /// 区間 ([`segments`](Self::segments)) を値の順に読むので、 混ぜ合わせずに値の順になる。 sparse の版を **配列より
    /// 先に** 控える: 配列を伸ばす書き手は bucket に移して配列を差し替えてから sparse から消すので、 先に控えた版に無い
    /// 値は、 後で読む配列が覆っている。 配列が覆う区間にある sparse の entry (移した直後の残り) は読まない。 古い entry の
    /// 有無は sparse を控える前に見る (`sparse_churned_before_read`)。
    pub fn walk(&self, lo: u64, hi: u64, desc: bool, mut f: impl FnMut(u64, &[u32], usize, bool) -> ControlFlow<()>) {
        if lo > hi {
            return;
        }
        let churned = self.sparse_churned_before_read();
        let _ = self.sparse.snapshot(|sp| {
            let guard = epoch::pin();
            let vecs = [LOW, POS, NEG].map(|k| self.vec_of(k, &guard));
            let (mut segs, len) = self.segments(vecs.map(|v| v.len()));
            let segs = &mut segs[..len];
            if desc {
                segs.reverse();
            }
            for &mut (a, b, at) in segs {
                let (a, b) = (a.max(lo), b.min(hi));
                if a > b {
                    continue;
                }
                let Some(k) = at else {
                    sp.walk(a, b, desc, |v, eids, head| f(v, eids, head, churned))?;
                    continue;
                };
                let (i, j) = (self.slot_index(k, a), self.slot_index(k, b));
                let i0 = i.min(j);
                let slots = &vecs[k][i0..=i.max(j)];
                let mut visit = |i: usize, s: &Slot| match s.get() {
                    Some(bk) if bk.live() > 0 => {
                        let (raw, head, verify) = bk.snapshot_sorted_in(&guard);
                        if raw.is_empty() { ControlFlow::Continue(()) } else { f(self.slot_value(k, i0 + i), raw, head, verify) }
                    }
                    _ => ControlFlow::Continue(()),
                };
                // 値の昇順は、 LOW / POS は添字の昇順、 NEG は降順
                if (k != NEG) != desc {
                    for (i, s) in slots.iter().enumerate() {
                        visit(i, s)?;
                    }
                } else {
                    for (i, s) in slots.iter().enumerate().rev() {
                        visit(i, s)?;
                    }
                }
            }
            ControlFlow::Continue(())
        });
    }

    /// 値が `lo..=hi` の entry を、 置き場の単位 ([`Part`]) で順を決めずに `f` に渡す: dense は値 1 つの bucket ごと、 sparse は
    /// run ごとの区間 (値の順の混ぜ合わせをしない)。 dense の空の要素と live が 0 の bucket は渡さない。 区間と版の控え方は
    /// [`walk`](Self::walk) と同じ (sparse の版を配列より先に控え、 配列が覆う区間の sparse は読まない。 古い entry の有無は
    /// sparse を控える前に見る)。
    pub(crate) fn for_each_part(&self, lo: u64, hi: u64, mut f: impl FnMut(Part<'_>)) {
        if lo > hi {
            return;
        }
        let churned = self.sparse_churned_before_read();
        self.sparse.snapshot(|sp| {
            let guard = epoch::pin();
            let vecs = [LOW, POS, NEG].map(|k| self.vec_of(k, &guard));
            let (segs, len) = self.segments(vecs.map(|v| v.len()));
            for &(a, b, at) in &segs[..len] {
                let (a, b) = (a.max(lo), b.min(hi));
                if a > b {
                    continue;
                }
                let Some(k) = at else {
                    sp.for_each_span(a, b, |eids| f(Part::Run { lo: a, hi: b, eids, verify: churned }));
                    continue;
                };
                let (i, j) = (self.slot_index(k, a), self.slot_index(k, b));
                let i0 = i.min(j);
                for (d, s) in vecs[k][i0..=i.max(j)].iter().enumerate() {
                    if let Some(bk) = s.get()
                        && bk.live() > 0
                    {
                        let (eids, head, verify) = bk.snapshot_sorted_in(&guard);
                        if !eids.is_empty() {
                            f(Part::Bucket { value: self.slot_value(k, i0 + d), eids, head, verify });
                        }
                    }
                }
            }
        });
    }

    /// 値が `lo..=hi` の live な entity の数。 dense の値は bucket の live の数を足すだけ (区間の中の要素の数に比例、
    /// entity は読まない)。 sparse の値は、 古い entry が無ければ run の二分探索で数え、 あれば entry を `live(値, eid)`
    /// (Column の現在値との照合、 `HimoStore` が渡す) で確かめ、 同じ eid を 1 つにして数える。 区間と版の控え方は
    /// [`walk`](Self::walk) と同じ。
    pub fn count_range(&self, lo: u64, hi: u64, mut live: impl FnMut(u64, u32) -> bool) -> usize {
        if lo > hi {
            return 0;
        }
        let churned = self.sparse_churned_before_read();
        self.sparse.snapshot(|sp| {
            let guard = epoch::pin();
            let vecs = [LOW, POS, NEG].map(|k| self.vec_of(k, &guard));
            let mut n = 0usize;
            let mut checked: Vec<u32> = Vec::new();
            let (segs, len) = self.segments(vecs.map(|v| v.len()));
            for &(a, b, at) in &segs[..len] {
                let (a, b) = (a.max(lo), b.min(hi));
                if a > b {
                    continue;
                }
                match at {
                    None if !churned => n += sp.count_in(a, b),
                    None => sp.for_each_in(a, b, |v, e| {
                        if live(v, e) {
                            checked.push(e);
                        }
                    }),
                    Some(k) => {
                        let (i, j) = (self.slot_index(k, a), self.slot_index(k, b));
                        n += vecs[k][i.min(j)..=i.max(j)].iter().filter_map(Slot::get).map(|b| b.live() as usize).sum::<usize>();
                    }
                }
            }
            // 書き換えで戻った値の重複 (同じ eid の entry 2 つ) を 1 つに
            checked.sort_unstable();
            checked.dedup();
            n + checked.len()
        })
    }

    /// 単一 writer append。 value の bucket に eid を足す（旧 value は放置＝lazy verify）。
    /// pin は insert 全体で 1 回（push / unique 判定に guard を回す。 hot path の
    /// pin 3 回 → 1 回、 write 天井対策）。
    ///
    /// `keep(値, eid)` は Column の現在値との照合 (`HimoStore` が渡す)。 配列を伸ばして sparse の entry を dense の
    /// bucket へ移す時に、 古い entry を移さないために使う (bucket の live の数を正しく保つ)。 呼ぶのは
    /// **Column を書き換えた後** (`set` の順序)。
    ///
    /// 配列を伸ばすことがあるので、 他の書き手が 1 本も居ない時に (`HimoStore` では write_lock と全部の鍵の下で)。
    pub fn insert(&self, eid: u32, value: u64, keep: impl FnMut(u64, u32) -> bool) {
        self.insert_with(eid, value, true, keep);
    }

    /// 配列の中の値 (`in_array` が true) の bucket に足す。 配列は伸ばさない。 その値の bucket の書き手 1 本で
    /// (`HimoStore` の値の鍵の下)。 配列を伸ばす書き手は全部の鍵を持つので、 この間に配列は差し替わらない。
    #[inline]
    pub(crate) fn push_in_array(&self, eid: u32, value: u64, guard: &Guard) {
        debug_assert!(self.in_array(value, guard), "push_in_array: 配列の外の値 {value}");
        let (k, i) = self.dense_slot(value).expect("push_in_array: 窓の外の値");
        self.push_slot(k, &self.vec_of(k, guard)[i], eid, guard);
    }

    /// 要素 `slot` (配列 `k`) の bucket に足す (空なら、 ここで初めて bucket を作る、 #358)。
    #[inline]
    fn push_slot(&self, k: usize, slot: &Slot, eid: u32, guard: &Guard) {
        let (b, created) = slot.get_or_create();
        if created {
            self.used[k].fetch_add(1, Ordering::Relaxed);
        }
        let prev_len = b.push_in(eid, guard);
        let prev_live = b.live_inc();
        self.bump_stats(prev_len == 0, prev_live == 0);
    }

    /// 値を足す時に配列を伸ばすか (`insert` と同じ判定)。 配列の中の値と窓の外の値は false。 判定に使う数は、
    /// 鍵で書く書き手が並んで増やす (bucket を作った数) ので、 呼んだ後で変わりうる — 決めたら `insert_with` に渡す。
    pub(crate) fn would_grow(&self, value: u64, guard: &Guard) -> bool {
        let Some((k, i)) = self.dense_slot(value) else {
            return false;
        };
        let len = self.vec_of(k, guard).len();
        i >= len && !self.grow_too_sparse(k, i, len)
    }

    /// 配列 `k` (今の長さ `len`) を添字 `i` まで伸ばすと、 空の要素ばかりになるか (#373)。
    #[inline]
    fn grow_too_sparse(&self, k: usize, i: usize, len: usize) -> bool {
        let new_len = (i + 1).max(len * 2).min(region_cap(k));
        let filled = self.used[k].load(Ordering::Relaxed) + self.waiting[k].load(Ordering::Relaxed) + 1;
        new_len > DENSE_FREE_LEN && new_len > DENSE_MIN_FILL * filled
    }

    /// `insert` の、 配列を伸ばすかを選べる版。 `may_grow = false` なら配列の外の値は伸ばさずに sparse へ (sparse の
    /// 書き手 1 本 = `HimoStore` の write_lock の下で)。 `true` なら他の書き手が 1 本も居ない時に。
    pub(crate) fn insert_with(&self, eid: u32, value: u64, may_grow: bool, keep: impl FnMut(u64, u32) -> bool) {
        debug_assert!(value != u64::MAX, "value == u64::MAX is sentinel");
        let guard = epoch::pin();
        let Some((k, i)) = self.dense_slot(value) else {
            // 値の種類数 (unique_count / unique_live) は dense の分だけ数える
            self.sparse.insert(value, eid);
            return;
        };
        let dense = self.arr(k);
        let arr = dense.load(Ordering::Acquire, &guard);
        // SAFETY: dense は常に非 null。
        let vec = unsafe { arr.deref() };
        if i < vec.len() {
            self.push_slot(k, &vec[i], eid, &guard);
            return;
        }
        // 配列の外: 伸ばすか、 sparse に置くか (#373)
        if !may_grow || self.grow_too_sparse(k, i, vec.len()) {
            // 伸ばさない (伸ばすと空の要素ばかりになる): sparse に置く。 entry が溜まれば、 後で伸ばす時に dense へ移る
            self.sparse.insert(value, eid);
            self.waiting[k].fetch_add(1, Ordering::Relaxed);
            return;
        }
        // 成長: doubling で amortize、 既存の bucket は写しが同じものを指す (refcount)。 足した要素は
        // 空のまま — bucket を作るのは値が入る要素だけ (#358)
        let new_len = (i + 1).max(vec.len() * 2).min(region_cap(k));
        let old_len = vec.len();
        let mut nv: DenseArr = vec.clone();
        nv.resize_with(new_len, Slot::empty);
        let waited = self.waiting[k].load(Ordering::Relaxed) > 0;
        let moved = if waited { self.move_from_sparse(k, old_len, new_len, &nv, (value, eid), keep, &guard) } else { 0 };
        self.push_slot(k, &nv[i], eid, &guard);
        DENSE_GROWS.fetch_add(1, Ordering::Relaxed);
        dense.store(Owned::new(nv), Ordering::Release);
        // SAFETY: 旧 array は全 reader が epoch 通過後に解放。
        unsafe {
            guard.defer_destroy(arr);
        }
        hook::at("grown_before_cleanup");
        if waited {
            // 移した値を sparse から消す — 配列を差し替えた**後**で (読み手は配列の長さで置き場所を決める、
            // `with_value`)。 消すのは、 今 dense が覆う値の entry 全部 (移したもの + 古い entry・重複)
            let lens = self.lens(&guard);
            let removed = self.sparse.rebuild(|v, _| self.dense_slot(v).is_none_or(|(k2, j)| j >= lens[k2]));
            // 移した entry は dense に数え直しただけ。 残りは古い entry・重複 (sparse の古い entry に数えてある)
            let dropped = removed.saturating_sub(moved);
            self.sparse_stale.fetch_sub(dropped.min(self.sparse_stale.load(Ordering::Relaxed)), Ordering::Relaxed);
            self.recount_waiting();
        }
    }

    /// 配列 `k` を `old_len` → `new_len` に伸ばす時、 新しく覆う値の sparse の entry を、 差し替える前の新しい
    /// 配列 `nv` の bucket へ移す。 移すのは `keep` (Column の現在値) を通った entry を 1 回ずつ。 `skip` (これから
    /// 入れる entry) は移さない — 古い重複が残っていると、 入れる分と合わせて 2 回数えるので。 移した数を返す。
    #[allow(clippy::too_many_arguments)]
    fn move_from_sparse(
        &self,
        k: usize,
        old_len: usize,
        new_len: usize,
        nv: &DenseArr,
        skip: (u64, u32),
        mut keep: impl FnMut(u64, u32) -> bool,
        guard: &Guard,
    ) -> usize {
        let (lo, hi) = self.region_values(k, old_len, new_len);
        let mut pairs = self.sparse.range_pairs(lo, hi);
        pairs.sort_unstable();
        pairs.dedup();
        let mut moved = 0;
        for (v, e) in pairs {
            if (v, e) == skip || !keep(v, e) {
                continue;
            }
            let (_, j) = self.dense_slot(v).expect("配列 k の窓の値");
            let (b, created) = nv[j].get_or_create();
            if created {
                self.used[k].fetch_add(1, Ordering::Relaxed);
                self.unique_count.fetch_add(1, Ordering::Relaxed);
            }
            b.push_in(e, guard);
            if b.live_inc() == 0 {
                self.unique_live.fetch_add(1, Ordering::Relaxed);
            }
            moved += 1;
        }
        moved
    }

    /// 空の索引を `(eid, 値)` の並び (Column の中身、 eid の順) からまとめて組む。 書き手のみ (`HimoStore` が
    /// write_lock の下で、 読み手が索引を使い始める前に呼ぶ)。 `entries` は同じ並びを 2 回出す (1 回目で数え、
    /// 2 回目で入れる)。
    ///
    /// 1 件ずつ `insert` すると、 sparse に置く値は 1 件ごとに run の組を差し替えるので遅い (#373 で Tag の値の多くが
    /// sparse に来るようになった)。 ここでは全部を見てから決める: 配列ごとに、 長さ L (2 の冪、 `DENSE_FREE_LEN` 以上)
    /// のうち 「添字 L 未満の entry の数の `DENSE_MIN_FILL` 倍 ≥ L」 を満たす最大のものまで dense、 残りは並べ直して
    /// 1 本の run にする。 `insert` と同じ規則 (配列の長さ未満 = dense) なので、 後の `insert` はそのまま続けられる。
    ///
    /// #394: 並びを Vec に集めず (1,500 万行で 240 MB + 添字 120 MB の一時確保)、 1 回目は値の添字ごとの件数だけを
    /// 数える。 bucket は 2 回目に入る件数ちょうどで作る — 倍々で伸ばすと、 伸ばすたびの古い backing が、 この関数の
    /// 間ずっと持つ pin のせいで epoch を過ぎず、 後で誰も pin しない (readonly で読むだけの) process では残り続けた。
    pub fn build<I: Iterator<Item = (u32, u64)>>(&self, entries: impl Fn() -> I) {
        debug_assert_eq!(self.total(), 0, "build は空の索引に");
        let guard = epoch::pin();
        // 1 回目: 配列ごとに、 添字ごとの件数 (出てきた最大の添字まで) と sparse の件数
        let mut counts: [Vec<u32>; 3] = Default::default();
        let mut n_sparse = 0usize;
        for (_, v) in entries() {
            match self.dense_slot(v) {
                Some((k, i)) => {
                    let c = &mut counts[k];
                    if c.len() <= i {
                        c.resize((i + 1).next_power_of_two().min(region_cap(k)), 0);
                    }
                    c[i] += 1;
                }
                None => n_sparse += 1,
            }
        }
        // 配列ごとの長さを決める
        let mut lens = self.lens(&guard);
        for k in [LOW, POS, NEG] {
            let c = &counts[k];
            if c.is_empty() {
                continue;
            }
            // below(l) = 添字 l 未満の entry の数
            let below = |l: usize| c[..l.min(c.len())].iter().map(|&n| n as usize).sum::<usize>();
            let cap = region_cap(k);
            let mut best = DENSE_FREE_LEN.min(cap);
            let mut l = best;
            let mut acc = below(l);
            while l < cap {
                let next = (l * 2).min(cap);
                acc += c[l.min(c.len())..next.min(c.len())].iter().map(|&n| n as usize).sum::<usize>();
                l = next;
                if acc * DENSE_MIN_FILL >= l {
                    best = l;
                }
            }
            // 要るのは入る値の最大の添字まで (今の長さより短くはしない)
            let top = c[..best.min(c.len())].iter().rposition(|&n| n > 0).map_or(0, |i| i + 1);
            lens[k] = lens[k].max(top);
            n_sparse += c[best.min(c.len())..].iter().map(|&n| n as usize).sum::<usize>();
        }
        // dense の配列を作って、 eid の順に bucket へ。 残りは sparse へまとめて
        let mut arrs: [DenseArr; 3] = [LOW, POS, NEG].map(|k| {
            let mut v = self.vec_of(k, &guard).clone();
            v.resize_with(lens[k], Slot::empty);
            v
        });
        let mut sparse = Vec::with_capacity(n_sparse);
        for (eid, v) in entries() {
            match self.dense_slot(v) {
                Some((k, i)) if i < lens[k] => {
                    let (b, created) = arrs[k][i].get_or_create_with(counts[k][i] as usize);
                    if created {
                        self.used[k].fetch_add(1, Ordering::Relaxed);
                    }
                    let prev_len = b.push_in(eid, &guard);
                    let prev_live = b.live_inc();
                    self.bump_stats(prev_len == 0, prev_live == 0);
                }
                _ => sparse.push((v, eid)),
            }
        }
        for (k, arr) in arrs.iter_mut().enumerate() {
            let old = self.arr(k).swap(Owned::new(std::mem::take(arr)), Ordering::AcqRel, &guard);
            // SAFETY: 旧 array は全 reader が epoch 通過後に解放。
            unsafe { guard.defer_destroy(old) };
        }
        self.sparse.build_from(sparse);
        self.recount_waiting();
    }

    #[inline]
    fn bump_stats(&self, was_empty: bool, was_dead: bool) {
        // 総数は持たない (読む時に数える、 `total` / `total_live`) — ここで RMW を増やさない (raw tie_async + oplog の
        // consumer apply が per-insert コストに直結する。SNS 型の負荷試験 matrix で +33% の実測退行が出た)。
        if was_empty {
            self.unique_count.fetch_add(1, Ordering::Relaxed);
        }
        if was_dead {
            // live 0 → 1 遷移 = cardinality 増 (初 insert または全滅からの復活)
            self.unique_live.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 今 dense に置いている value の bucket を外部 guard 下で read（lock-free、 zero-copy）。
    /// sparse は None を返す（sparse は `read_to_vec` 経由で）。
    #[allow(dead_code)]
    #[inline]
    pub fn with_dense_read<R>(
        &self,
        guard: &Guard,
        value: u64,
        f: impl FnOnce(&[u32]) -> R,
    ) -> Option<R> {
        match self.writer_dense_at(value, guard)? {
            Some(b) => Some(b.with_read(guard, f)),
            None => Some(f(&[])),
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
        self.with_value(
            value,
            |b, guard| b.map_or((Vec::new(), false), |b| b.read_snapshot_verify(guard)),
            |sparse| {
                // 古い entry の有無は **読む前** に見る (`sparse_churned_before_read`)
                let churned = self.sparse_churned_before_read();
                // run ごとに eid 順なので並べ直す (dense の bucket と同じく eid の昇順で返す)
                let mut out = sparse.lookup(value);
                hook::at("sparse_after_lookup");
                if out.len() > 1 {
                    out.sort_unstable();
                }
                (out, churned)
            },
        )
    }

    /// [`read_to_vec_verify`](Self::read_to_vec_verify) の、 呼び手の buffer の後ろに足す版 (多くの値を続けて
    /// 読む時に、 値ごとの Vec を作らない)。 読み方は [`with_value`](Self::with_value) と同じで、 sparse を読んだ後に
    /// 値が dense に移っていたら、 足した分を捨てて dense を読み直す。 返り値 = 足した分が Column verify を要するか。
    pub fn read_into_verify(&self, value: u64, out: &mut Vec<u32>) -> bool {
        let start = out.len();
        let sparse_into = |out: &mut Vec<u32>| {
            // 古い entry の有無は **読む前** に見る (`sparse_churned_before_read`)
            let churned = self.sparse_churned_before_read();
            self.sparse.lookup_into(value, out);
            hook::at("sparse_after_lookup");
            // run ごとに eid 順なので並べ直す (dense の bucket と同じく eid の昇順で返す)
            if out.len() - start > 1 {
                out[start..].sort_unstable();
            }
            churned
        };
        let guard = epoch::pin();
        let Some((k, i)) = self.dense_slot(value) else {
            return sparse_into(out);
        };
        if let Some(b) = self.dense_at(k, i, &guard) {
            return b.is_some_and(|b| b.read_snapshot_verify_into(&guard, out));
        }
        hook::at("reader_missed_dense");
        let r = sparse_into(out);
        hook::at("reader_read_sparse");
        match self.dense_at(k, i, &guard) {
            Some(b) => {
                out.truncate(start);
                b.is_some_and(|b| b.read_snapshot_verify_into(&guard, out))
            }
            None => r,
        }
    }

    /// value の bucket 長（raw、 stale 込み）。診断用 (planner は `slice_len_live` へ移行)。
    #[allow(dead_code)]
    pub fn slice_len(&self, value: u64) -> usize {
        self.with_value(value, |b, _| b.map_or(0, AppendBucket::len), |sparse| sparse.lookup(value).len())
    }

    /// backing に現存する slot 総数 (raw、stale 込み、compaction 除去は反映)。診断用。 読む時に bucket ごとの長さと sparse の
    /// entry の数を足す (O(配列の長さ + sparse の run の数))。
    #[allow(dead_code)]
    pub fn total(&self) -> usize {
        self.fold_buckets(AppendBucket::len) + self.sparse.len()
    }

    /// 3 本の配列の全部の bucket について `f` の和。
    fn fold_buckets(&self, f: impl Fn(&AppendBucket) -> usize) -> usize {
        let guard = epoch::pin();
        [LOW, POS, NEG].iter().map(|&k| self.vec_of(k, &guard).iter().filter_map(Slot::get).map(&f).sum::<usize>()).sum()
    }

    /// 一度でも要素が入った bucket 数 (raw)。診断用。
    #[allow(dead_code)]
    pub fn unique_count(&self) -> u32 {
        self.unique_count.load(Ordering::Relaxed)
    }

    /// live な tie 総数 (正確、churn の影響なし)。request12。 読む時に数える: dense は bucket ごとの live の和、 sparse は
    /// entry の数 − 古い entry の数 (重複は古い entry に数えてある)。 書き込みと並ぶと一時的にずれうる (Relaxed 統計、
    /// saturating で防御)。
    pub fn total_live(&self) -> usize {
        let stale = self.sparse_stale.load(Ordering::Relaxed);
        self.fold_buckets(|b| b.live() as usize) + self.sparse.len().saturating_sub(stale)
    }

    /// live > 0 の bucket 数 (dense に置いている値の正確な cardinality)。request12。
    pub fn unique_live(&self) -> u32 {
        self.unique_live.load(Ordering::Relaxed)
    }

    /// value の live 件数 (= verify 後の pull 結果の件数、正確)。 単体 test 用 (planner の pivot 選択などは
    /// [`dense_live_len`](Self::dense_live_len) を 1 回の pin で使う)。
    #[cfg(test)]
    pub fn slice_len_live(&self, value: u64) -> usize {
        // sparse は古い entry 込みの上限 (正確な件数は HimoStore が Column と突き合わせる)
        self.with_value(value, |b, _| b.map_or(0, |b| b.live() as usize), |sparse| sparse.lookup(value).len())
    }

    /// 今 dense に置いている値なら、 その live な件数 (空の要素は 0)。 sparse の値は None (件数を持たない、 呼び手が引いて
    /// 数える)。 [`is_dense`](Self::is_dense) と [`slice_len_live`](Self::slice_len_live) を 1 回の pin で
    /// (2026-10-11: 件数を数える所は値ごとに 2 回 pin し、 dense の位置も 2 回求めていた)。
    pub fn dense_live_len(&self, value: u64) -> Option<usize> {
        let guard = epoch::pin();
        let (k, i) = self.dense_slot(value)?;
        self.dense_at(k, i, &guard).map(|b| b.map_or(0, |b| b.live() as usize))
    }

    /// value の bucket に churn 痕があるか。write_lock 下 (単一 writer) では正確 —
    /// `compact_now` の clean-bucket skip 判定用。
    pub fn bucket_needs_verify(&self, value: u64) -> bool {
        let guard = epoch::pin();
        match self.writer_dense_at(value, &guard) {
            Some(b) => b.is_some_and(AppendBucket::needs_verify),
            None => self.sparse_churned(),
        }
    }

    /// cylinder が現在確保している eid backing の総 bytes（pow2 slack 込み、 メモリ観測用）。
    /// append-only なので各 eid は 1 度だけ載る → `total()*4 * (pow2 slack)` に収まる。
    /// double-buffer（2 コピー保持）なら `>= 2x` になるので、 それとの区別に使える。
    pub fn backing_bytes(&self) -> usize {
        let guard = epoch::pin();
        let mut slots = 0usize;
        for k in [LOW, POS, NEG] {
            slots += self.vec_of(k, &guard).iter().filter_map(Slot::get).map(|b| b.capacity()).sum::<usize>();
        }
        slots * std::mem::size_of::<u32>() + self.sparse.backing_bytes()
    }

    /// 3 本の dense 配列の要素の数 (空の要素込み、 観測用)。 1 要素 8 B。
    #[cfg(test)]
    pub fn dense_slots(&self) -> usize {
        let guard = epoch::pin();
        self.lens(&guard).iter().sum()
    }

    /// 非空 bucket の value を列挙（順序保証なし、 stale 込みの近似）。
    pub fn unique_values(&self) -> Vec<u64> {
        let guard = epoch::pin();
        let mut out = Vec::new();
        for k in [LOW, POS, NEG] {
            out.extend(
                self.vec_of(k, &guard)
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| s.get().is_some_and(|b| !b.is_empty()))
                    .map(|(i, _)| self.slot_value(k, i)),
            );
        }
        // dense に移した値 (配列を伸ばした直後に sparse に残っているもの) は 2 度出さない
        let lens = self.lens(&guard);
        out.extend(self.sparse.values().into_iter().filter(|&v| self.dense_slot(v).is_none_or(|(k, i)| i >= lens[k])));
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

    /// 古い entry の無い test 用の `keep` (全部生きている)。
    fn all(_: u64, _: u32) -> bool {
        true
    }

    #[test]
    fn dense_insert_read() {
        let c = LockFreeCylinder::new(0);
        for e in 0..100u32 {
            c.insert(e, (e % 10) as u64, all); // value 0..9
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
        c.insert(1, 5, all);
        c.insert(2, 500, all); // 0→~501 bucket に grow、epoch swap を叩く
        assert_eq!(c.read_to_vec(500), vec![2]);
        assert_eq!(c.unique_count(), 2);
        // 遠い値 1 件ずつでは配列を伸ばさない (#373): sparse に置く (miri でも配列の確保は小さいまま)。
        // 種類数は dense の分だけ
        c.insert(3, 50_000, all);
        c.insert(4, 999_999, all);
        assert_eq!(c.read_to_vec(50_000), vec![3]);
        assert_eq!(c.read_to_vec(999_999), vec![4]);
        assert!(!c.is_dense(50_000) && !c.is_dense(999_999));
        assert_eq!(c.unique_count(), 2);
        assert!(dense_len(&c) <= DENSE_FREE_LEN, "{}", dense_len(&c));
    }

    /// #358: 配列を伸ばしても、 間の要素に bucket は作らない。 空の要素は、 どの読みでも 「何も無い」。
    /// (名前が `dense` で始まる test は CI の Miri でも回る — `Slot` の unsafe を見る)
    #[test]
    fn dense_grown_slots_stay_empty_until_first_insert() {
        let c = LockFreeCylinder::new(0);
        c.insert(1, 5, all);
        c.insert(2, 500, all);
        {
            let guard = epoch::pin();
            let arr = c.dense.load(Ordering::Acquire, &guard);
            let vec = unsafe { arr.deref() };
            assert!(vec.len() > 500);
            assert_eq!(vec.iter().filter(|s| s.get().is_some()).count(), 2, "bucket を持つのは値の入った要素だけ");
        }
        // 空の要素 (値 100) への読み
        assert_eq!(c.read_to_vec(100), Vec::<u32>::new());
        assert_eq!(c.read_to_vec_verify(100), (Vec::new(), false));
        assert_eq!(c.slice_len(100), 0);
        assert_eq!(c.slice_len_live(100), 0);
        assert!(!c.bucket_needs_verify(100));
        assert_eq!(c.compact_bucket(100, |_| true), 0);
        {
            let guard = epoch::pin();
            assert_eq!(c.with_dense_read(&guard, 100, |s| s.len()), Some(0));
        }
        // 空の要素をまたぐ読み
        assert_eq!(c.range_raw(0, 600), vec![1, 2]);
        assert_eq!(c.range_raw(6, 499), Vec::<u32>::new());
        let mut values = c.unique_values();
        values.sort_unstable();
        assert_eq!(values, vec![5, 500]);
        assert_eq!((c.unique_count(), c.unique_live(), c.total()), (2, 2, 2));
        assert_eq!(c.backing_bytes(), 2 * 4 * std::mem::size_of::<u32>(), "eid の置き場は bucket 2 つ分だけ");

        // 空だった要素に後から入れる (配列は伸びない)
        c.insert(3, 100, all);
        c.insert(4, 100, all);
        assert_eq!(c.read_to_vec(100), vec![3, 4]);
        assert_eq!(c.slice_len_live(100), 2);
        assert_eq!(c.range_raw(0, 600), vec![1, 3, 4, 2]);
        assert_eq!((c.unique_count(), c.unique_live(), c.total()), (3, 3, 4));
        let guard = epoch::pin();
        let vec = unsafe { c.dense.load(Ordering::Acquire, &guard).deref() };
        assert_eq!(vec.iter().filter(|s| s.get().is_some()).count(), 3);
    }

    /// 事前確保 (`new(max_values)`) も要素だけで、 bucket は作らない。
    #[test]
    fn dense_prealloc_makes_no_buckets() {
        let c = LockFreeCylinder::new(1000);
        let guard = epoch::pin();
        let vec = unsafe { c.dense.load(Ordering::Acquire, &guard).deref() };
        assert_eq!(vec.len(), PREALLOC_CAP);
        assert_eq!(vec.iter().filter(|s| s.get().is_some()).count(), 0);
        drop(guard);
        c.insert(1, 3, all);
        assert_eq!(c.read_to_vec(3), vec![1]);
        assert_eq!(c.read_to_vec(4), Vec::<u32>::new());
    }

    /// 掃除で空になった bucket (要素は bucket を持ったまま) の値は `unique_values` に出ない。 3 本の配列とも。
    #[test]
    fn dense_unique_values_skips_emptied_buckets() {
        let c = LockFreeCylinder::new(0);
        for (eid, v) in [(1u32, 7u64), (2, 9), (3, MID + 3), (4, MID + 5), (5, MID - 4), (6, MID - 6)] {
            c.insert(eid, v, all);
        }
        // 7 / MID + 3 / MID - 4 の entity が別の値へ移った: 古い entry にして掃除で落とす
        for v in [7, MID + 3, MID - 4] {
            c.note_stale(v);
            assert_eq!(c.compact_bucket(v, |_| false), 0);
        }
        let mut values = c.unique_values();
        values.sort_unstable();
        assert_eq!(values, vec![9, MID - 6, MID + 5]);
        assert_eq!(c.unique_live(), 3);
    }

    /// 範囲の下端が配列の外でも落ちない (空の要素を飛ばす読みは配列を slice で切るので、 外の下端は先に弾く)。
    #[test]
    fn dense_range_beyond_array_is_empty() {
        let c = LockFreeCylinder::new(0);
        c.insert(1, 5, all); // 配列は 6 要素
        assert_eq!(c.range_raw(6, 9), Vec::<u32>::new());
        assert_eq!(c.range_raw(7, 9), Vec::<u32>::new());
        assert_eq!(c.range_raw(100, 200), Vec::<u32>::new());
        assert_eq!(c.range_raw(5, 200), vec![1]);
        // MID の上下の配列も同じ
        c.insert(2, MID + 3, all);
        c.insert(3, MID - 4, all);
        assert_eq!(c.range_raw(MID + 10, MID + 20), Vec::<u32>::new());
        assert_eq!(c.range_raw(MID - 20, MID - 10), Vec::<u32>::new());
        assert_eq!(c.range_raw(MID - 20, MID + 20), vec![2, 3]);
    }

    /// `Slot`: 写しは同じ bucket を指して参照を 1 つ足し、 drop で返す (足さなければ解放済みを読み、
    /// 返さなければ bucket が残り続ける)。
    #[test]
    fn dense_slot_clone_shares_bucket_and_drop_releases() {
        /// bucket の参照の数 (数えるための 1 つは除く)。
        fn refs(p: *const AppendBucket) -> usize {
            // SAFETY: 呼ぶ側が、 p を指す Slot を 1 つ以上持っている間に呼ぶ。
            unsafe {
                Arc::increment_strong_count(p);
                let a = Arc::from_raw(p);
                Arc::strong_count(&a) - 1
            }
        }
        let s = Slot::empty();
        assert!(s.get().is_none());
        let early = s.clone();
        assert!(early.get().is_none(), "空の写しは空");

        let p = s.get_or_create().0 as *const AppendBucket;
        let (again, created) = s.get_or_create();
        assert_eq!(again as *const AppendBucket, p, "2 度目は同じ bucket");
        assert!(!created, "2 度目は作らない");
        assert!(early.get().is_none(), "写した後に置いた bucket は、 先に取った写しからは見えない");
        assert_eq!(refs(p), 1);

        let c1 = s.clone();
        let c2 = c1.clone();
        assert_eq!(c1.get().unwrap() as *const AppendBucket, p);
        assert_eq!(refs(p), 3);
        drop(c1);
        drop(s);
        assert_eq!(refs(p), 1, "残りは c2 の 1 つ");
        assert!(c2.get().unwrap().is_empty());
    }

    #[test]
    fn sparse_path() {
        let c = LockFreeCylinder::new(0);
        let big = DENSE_CAP + 42;
        c.insert(7, big as u64, all);
        c.insert(8, big as u64, all);
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

    /// Column の代わり: eid → 今の値。 `keep` と 「今の値の eid」 を出す。
    struct Cells(std::cell::RefCell<std::collections::HashMap<u32, u64>>);

    impl Cells {
        fn new() -> Self {
            Cells(Default::default())
        }
        /// `HimoStore::set` と同じ順: 古い値の印 → Column → insert。
        fn set(&self, c: &LockFreeCylinder, eid: u32, v: u64) {
            let old = self.0.borrow().get(&eid).copied();
            if old == Some(v) {
                return;
            }
            if let Some(o) = old {
                c.note_stale(o);
            }
            self.0.borrow_mut().insert(eid, v);
            c.insert(eid, v, |v, e| self.0.borrow().get(&e) == Some(&v));
        }
        fn keep(&self) -> impl FnMut(u64, u32) -> bool + '_ {
            |v, e| self.0.borrow().get(&e) == Some(&v)
        }
        fn eids_of(&self, v: u64) -> Vec<u32> {
            let mut out: Vec<u32> = self.0.borrow().iter().filter(|p| *p.1 == v).map(|p| *p.0).collect();
            out.sort_unstable();
            out
        }
    }

    /// 値を verify して読む (`HimoStore::pull` と同じ)。
    fn pull(c: &LockFreeCylinder, cells: &Cells, v: u64) -> Vec<u32> {
        let (mut raw, verify) = c.read_to_vec_verify(v);
        if verify {
            raw.retain(|&e| cells.0.borrow().get(&e) == Some(&v));
        }
        raw.sort_unstable();
        raw.dedup();
        raw
    }

    /// `walk` で値の順に読んだ (値, eid の列) を、 `HimoStore::walk_values` と同じく確かめて並べて集める。 `limit` 件の
    /// entity を集めたら止める。
    fn walked(c: &LockFreeCylinder, cells: &Cells, lo: u64, hi: u64, desc: bool, limit: usize) -> Vec<(u64, Vec<u32>)> {
        let mut out: Vec<(u64, Vec<u32>)> = Vec::new();
        let mut n = 0;
        c.walk(lo, hi, desc, |v, raw, head, verify| {
            // 置き場が渡す頭の長さは、 探した長さとちょうど同じ (短いと遅く、 長いと答えが壊れる)
            assert_eq!(head, crate::append_bucket::sorted_prefix_len(raw), "値 {v}: 頭の昇順の長さが合わない");
            let mut got: Vec<u32> = raw.iter().copied().filter(|&e| !verify || cells.0.borrow().get(&e) == Some(&v)).collect();
            got.sort_unstable();
            got.dedup();
            if got.is_empty() {
                return ControlFlow::Continue(());
            }
            n += got.len();
            out.push((v, got));
            if n >= limit { ControlFlow::Break(()) } else { ControlFlow::Continue(()) }
        });
        out
    }

    /// `for_each_part` で置き場の単位に読んだ entry を、 `HimoStore::pull_range_unordered` と同じく確かめて集める (重複を落として
    /// eid の昇順で返す)。 置き場の形も見る: bucket の頭の長さは探した長さと同じ、 bucket の値と run の区間は問い合わせの範囲の中で
    /// 重ならない、 古い entry の無い所 (`verify` が false) の eid に重複が無い。
    fn parted(c: &LockFreeCylinder, cells: &Cells, lo: u64, hi: u64) -> Vec<u32> {
        let (mut clean, mut checked, mut values, mut spans) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        c.for_each_part(lo, hi, |part| match part {
            Part::Bucket { value, eids, head, verify } => {
                assert_eq!(head, crate::append_bucket::sorted_prefix_len(eids), "値 {value}: 頭の昇順の長さが合わない");
                assert!(lo <= value && value <= hi, "範囲の外の bucket {value}");
                values.push(value);
                if verify {
                    checked.extend(eids.iter().copied().filter(|&e| cells.0.borrow().get(&e) == Some(&value)));
                } else {
                    clean.extend_from_slice(eids);
                }
            }
            Part::Run { lo: a, hi: b, eids, verify } => {
                assert!(lo <= a && a <= b && b <= hi, "範囲の外の run の区間 {a}..={b}");
                spans.push((a, b));
                if verify {
                    checked.extend(eids.iter().copied().filter(|&e| cells.0.borrow().get(&e).is_some_and(|&v| a <= v && v <= b)));
                } else {
                    clean.extend_from_slice(eids);
                }
            }
        });
        assert!(values.iter().all(|&v| spans.iter().all(|&(a, b)| v < a || b < v)), "bucket の値が run の区間に重なる");
        clean.sort_unstable();
        assert!(clean.windows(2).all(|w| w[0] < w[1]), "古い entry の無い所に同じ eid が 2 つある");
        clean.extend(checked);
        clean.sort_unstable();
        clean.dedup();
        clean
    }

    /// `expected` の entity を eid の昇順で。
    fn expected_eids(cells: &Cells, lo: u64, hi: u64) -> Vec<u32> {
        let mut out: Vec<u32> = expected(cells, lo, hi, false, usize::MAX).into_iter().flat_map(|x| x.1).collect();
        out.sort_unstable();
        out
    }

    /// Column の代わり (`Cells`) から出した答え: 値が `lo..=hi` の (値, eid の昇順) を値の順に、 `limit` 件の entity まで
    /// (最後の値は全部入れる — `walked` と同じ切り方)。
    fn expected(cells: &Cells, lo: u64, hi: u64, desc: bool, limit: usize) -> Vec<(u64, Vec<u32>)> {
        let mut vals: Vec<u64> = cells.0.borrow().values().copied().filter(|v| lo <= *v && *v <= hi).collect();
        vals.sort_unstable();
        vals.dedup();
        if desc {
            vals.reverse();
        }
        let mut out = Vec::new();
        let mut n = 0;
        for v in vals {
            if n >= limit {
                break;
            }
            let e = cells.eids_of(v);
            n += e.len();
            out.push((v, e));
        }
        out
    }

    /// 値の順にたどる `walk` と範囲の件数 `count_range` を、 値が 6 つの区間 (sparse の下の窓の外は無いので、 LOW・
    /// LOW の窓の中で配列の外 (後で伸びて dense に移る)・LOW の窓の外の sparse・NEG・POS・POS の窓の外の sparse) に
    /// 散る列で、 書き換えと消しの後に、 Column の代わりから出した答えと比べる。 昇順・降順・範囲の端・途中で止める。
    #[test]
    fn walk_and_count_range_match_cells_across_regions() {
        let c = LockFreeCylinder::new(0);
        let cells = Cells::new();
        let n = if cfg!(miri) { 60u32 } else { 3_000 };
        let mut s = 0x2545_F491_4F6C_DD1Du64;
        let mut rnd = move || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        let pick = |r: u64| -> u64 {
            match r % 6 {
                0 => r % 50,
                1 => 1_500 + (r >> 8) % 20,
                2 => 10_000_000 + (r >> 8) % 1_000_000,
                3 => MID - 1 - (r >> 8) % 40,
                4 => MID + (r >> 8) % 40,
                _ => MID + (1 << 40) + (r >> 8) % 1_000,
            }
        };
        for e in 0..n {
            let v = pick(rnd());
            cells.set(&c, e, v);
        }
        // 書き換え (古い entry と、 戻った値の重複を残す) と消し (印 → Column の順)
        for _ in 0..n / 3 {
            let e = (rnd() % n as u64) as u32;
            let v = pick(rnd());
            cells.set(&c, e, v);
        }
        // sparse の値へ戻る往復 (x → 別の値 → x): sparse に同じ (x, e) の entry が 2 つ残る (件数で 2 回数えない)
        let far: Vec<u32> = (0..n).filter(|e| cells.0.borrow().get(e).is_some_and(|&v| (10_000_000..MID - MID_HALF).contains(&v))).take(20).collect();
        assert!(!far.is_empty(), "test の前提: sparse の値の entity がある");
        for &e in &far {
            let x = cells.0.borrow()[&e];
            cells.set(&c, e, 7);
            cells.set(&c, e, x);
        }
        for _ in 0..n / 10 {
            let e = (rnd() % n as u64) as u32;
            if far.contains(&e) {
                continue;
            }
            let old = cells.0.borrow().get(&e).copied();
            if let Some(o) = old {
                c.note_stale(o);
                cells.0.borrow_mut().remove(&e);
            }
        }
        if !cfg!(miri) {
            assert!(c.is_dense(1_500), "LOW の窓の中の値が配列に移っていない (test の前提)");
        }
        // 往復した entity の値だけの範囲: 重複を 2 回数えない
        for &e in &far {
            let x = cells.0.borrow()[&e];
            assert_eq!(c.count_range(x, x, cells.keep()), cells.eids_of(x).len(), "往復した値 {x}");
        }
        assert!(c.sparse_churned(), "sparse に古い entry が無い (test の前提)");
        let edges = [0, 49, 50, 1_499, 1_519, 9_999_999, 10_500_000, MID - 41, MID - 1, MID, MID + 39, MID + 40, MID + (1 << 40), u64::MAX];
        let mut ranges: Vec<(u64, u64)> = vec![(0, u64::MAX), (MID - 20, MID + 20), (5, 5), (MID, MID), (20, 10)];
        for &a in &edges {
            for &b in &edges {
                if a <= b {
                    ranges.push((a, b));
                }
            }
        }
        for &(lo, hi) in &ranges {
            for desc in [false, true] {
                assert_eq!(walked(&c, &cells, lo, hi, desc, usize::MAX), expected(&cells, lo, hi, desc, usize::MAX), "{lo}..={hi} desc={desc}");
            }
            let want: usize = expected(&cells, lo, hi, false, usize::MAX).iter().map(|x| x.1.len()).sum();
            assert_eq!(c.count_range(lo, hi, cells.keep()), want, "count_range {lo}..={hi}");
            assert_eq!(parted(&c, &cells, lo, hi), expected_eids(&cells, lo, hi), "for_each_part {lo}..={hi}");
        }
        // 途中で止める (上位 k): 止めた所までが同じ
        for k in [1, 7, 100] {
            for desc in [false, true] {
                assert_eq!(walked(&c, &cells, 0, u64::MAX, desc, k), expected(&cells, 0, u64::MAX, desc, k), "k={k} desc={desc}");
            }
        }
        // 掃除しても同じ (sparse は古い entry が無くなり、 run の区間を写すだけの道になる)
        c.compact_sparse(cells.keep());
        assert!(!c.sparse_churned());
        for desc in [false, true] {
            assert_eq!(walked(&c, &cells, 0, u64::MAX, desc, usize::MAX), expected(&cells, 0, u64::MAX, desc, usize::MAX));
        }
        for &(lo, hi) in &ranges {
            assert_eq!(parted(&c, &cells, lo, hi), expected_eids(&cells, lo, hi), "掃除の後の for_each_part {lo}..={hi}");
        }
        assert_eq!(c.count_range(0, u64::MAX, cells.keep()), cells.0.borrow().len());
    }

    /// #373: 辞書 ID のように値が範囲にまばらに散る列 (表 16 個の DB の 1 列 = 16 個に 1 つ) は、 配列を値の範囲まで
    /// 伸ばさず sparse に置く。 配列は中身の数に比例する。 読みは全部引ける。
    #[test]
    fn dense_scattered_values_stay_in_sparse() {
        let c = LockFreeCylinder::new(0);
        let n = if cfg!(miri) { 200u32 } else { 20_000 };
        for e in 0..n {
            c.insert(e, e as u64 * 16, all);
        }
        let slots = c.dense_slots();
        assert!(slots <= 2 * DENSE_FREE_LEN, "まばらな値で配列を伸ばした: {slots} 要素 (値 {n} 個)");
        for e in (0..n).step_by(97) {
            assert_eq!(c.read_to_vec(e as u64 * 16), vec![e]);
            assert_eq!(c.read_to_vec(e as u64 * 16 + 1), Vec::<u32>::new());
        }
        let mut got = c.range_raw(16, 16 * 40);
        got.sort_unstable();
        assert_eq!(got, (1..=40).collect::<Vec<u32>>());
        assert_eq!(c.total_live(), n as usize);
        // 詰まった列 (全部の値) は今までどおり全部 dense
        let d = LockFreeCylinder::new(0);
        for e in 0..n {
            d.insert(e, e as u64, all);
        }
        assert!((0..n).all(|v| d.is_dense(v as u64)));
        assert_eq!(d.unique_live(), n);
    }

    /// #373: 遠い 1 つの値に行が溜まると (辞書 ID の大きい Tag の値を多くの行が持つ)、 配列を伸ばして sparse の
    /// entry を bucket へ移す。 移した後は値ごとの件数を O(1) で持つ。 古い entry は移さない (live の数が正しい)。
    #[test]
    fn dense_crowded_far_value_moves_from_sparse() {
        let c = LockFreeCylinder::new(0);
        let cells = Cells::new();
        let far = 5_000u64; // DENSE_FREE_LEN より先
        let n = if cfg!(miri) { 40u32 } else { 4_000 };
        // 何行かは 1 度 far に入れてから別の値へ移す (sparse に古い entry を残す)
        for e in 0..n {
            cells.set(&c, e, far);
            if e % 10 == 0 {
                cells.set(&c, e, 3);
            }
            if e % 10 == 1 {
                // far → 3 → far: far の古い entry (重複) を残す
                cells.set(&c, e, 3);
                cells.set(&c, e, far);
            }
        }
        if cfg!(miri) {
            // miri の件数では配列を伸ばすほど溜まらない: sparse のまま全部引ける
            assert!(!c.is_dense(far));
        } else {
            assert!(c.is_dense(far), "行が溜まっても配列を伸ばさない");
            assert_eq!(c.sparse.count_in(far, far), 0, "移した entry (と古い entry) が sparse から消えていない");
            // 値ごとの件数が O(1) で正しい (古い entry・重複を移していない)
            assert_eq!(c.slice_len_live(far), cells.eids_of(far).len());
            assert_eq!(c.unique_live(), 2, "far と 3");
        }
        assert_used_matches(&c);
        assert_eq!(pull(&c, &cells, far), cells.eids_of(far));
        assert_eq!(pull(&c, &cells, 3), cells.eids_of(3));
        assert_eq!(c.total_live(), n as usize, "live な entry の総数");
        // 掃除しても集合は変わらない
        c.compact_sparse(cells.keep());
        assert_eq!(pull(&c, &cells, far), cells.eids_of(far));
        assert_eq!(c.total_live(), n as usize);
    }

    /// #373: 配列を伸ばす insert の entry と同じ `(値, eid)` の古い重複が sparse に残っていたら (far → 別の値 →
    /// far)、 移す側では数えない — 数えると、 これから入れる分と合わせて live を 2 回数える。
    #[test]
    fn dense_move_does_not_count_the_inserted_entry_twice() {
        let c = LockFreeCylinder::new(0);
        let cells = Cells::new();
        let far = 2_000u64; // 配列を 2_001 に伸ばすには 「bucket + 待ち + 1」 が 501 以上
        cells.set(&c, 0, far);
        for e in 1..=498 {
            cells.set(&c, e, far);
        }
        cells.set(&c, 0, 3); // sparse に (far, 0) の古い entry が残る。 3 は dense
        assert!(!c.is_dense(far), "前提: まだ sparse");
        cells.set(&c, 0, far); // 待ち 499 + bucket 1 + 1 = 501: この insert で伸ばす
        assert!(c.is_dense(far), "前提: この insert で配列を伸ばした");
        assert_eq!(c.slice_len_live(far), 499, "far の live");
        assert_eq!(pull(&c, &cells, far), cells.eids_of(far));
        let (raw, verify) = c.read_to_vec_verify(far);
        assert!(!verify, "移した bucket は古い entry を持たない");
        assert_eq!(raw.len(), 499, "bucket に重複がある: {}", raw.len());
        assert_eq!(c.total_live(), 499);
        assert_eq!(c.unique_live(), 1, "3 は空になった");
        assert_used_matches(&c);
    }

    /// 値 2_000 に 500 行を sparse で待たせる: 次の 1 行 (`trigger`) で配列が伸びて全部 dense へ移る
    /// (伸ばすには 「bucket + 待ち + 1」 が 501 以上)。
    fn ready_to_grow() -> (Arc<LockFreeCylinder>, u64) {
        let c = Arc::new(LockFreeCylinder::new(0));
        let far = 2_000u64;
        for e in 0..500 {
            c.insert(e, far, all);
        }
        assert!(!c.is_dense(far), "前提: まだ sparse");
        (c, far)
    }

    fn trigger(c: &LockFreeCylinder, far: u64) {
        c.insert(500, far, all);
        assert!(c.is_dense(far), "前提: この insert で配列が伸びた");
    }

    /// #373: 読み手が 「配列の外 = sparse」 と決めた直後に、 書き手が配列を伸ばして値を dense へ移し、 sparse から
    /// 消す。 読み手は sparse を読んだ後で配列の長さを読み直すので、 値を見落とさない。
    #[test]
    fn dense_reader_rechecks_after_values_move_out_of_sparse() {
        let (c, far) = ready_to_grow();
        let w = c.clone();
        hook::set("reader_missed_dense", move || trigger(&w, far));
        let got = c.read_to_vec(far);
        assert_eq!(got.len(), 501, "読む間に dense へ移った値を見落とした: {} 件", got.len());

        // 範囲の読みも同じ (dense を読んだ後、 sparse を読む前に移る)
        let (c, far) = ready_to_grow();
        let w = c.clone();
        hook::set("range_read_dense", move || trigger(&w, far));
        let mut got = c.range_raw(far, far);
        got.sort_unstable();
        got.dedup();
        assert_eq!(got.len(), 501, "範囲の読みで dense へ移った値を見落とした: {} 件", got.len());
    }

    /// sparse の読みは古い entry の有無を **読む前** に見る: 読んだ直後 (割り込み点 `sparse_after_lookup`) に書き手が
    /// sparse を組み直して数を 0 に戻しても、 読んだ版にあった古い entry を確かめずに返さない。 読んだ後に見る形だと、
    /// 組み直した後の 0 を見て、 もう別の値に移った entity を live として返していた。 `read_into_verify` も同じ。
    #[test]
    fn sparse_reader_checks_stale_before_reading() {
        for read_into in [false, true] {
            let c = std::rc::Rc::new(LockFreeCylinder::new(0));
            let cells = std::rc::Rc::new(Cells::new());
            let far = 5_000_000u64; // DENSE_CAP 以上 = sparse
            cells.set(&c, 0, far);
            cells.set(&c, 1, far);
            cells.set(&c, 0, far + 1); // 0 は far を出た: far に古い entry が残る
            assert!(c.sparse_churned(), "前提: 古い entry がある");
            let (c2, k2) = (c.clone(), cells.clone());
            hook::set("sparse_after_lookup", move || {
                c2.compact_sparse(k2.keep());
            });
            let (raw, verify) = if read_into {
                let mut out = Vec::new();
                let v = c.read_into_verify(far, &mut out);
                (out, v)
            } else {
                c.read_to_vec_verify(far)
            };
            assert!(!c.sparse_churned(), "前提: 読む間に組み直した (read_into={read_into})");
            assert!(verify || !raw.contains(&0), "古い entry (0) を確かめずに返した (read_into={read_into}): {raw:?}");
            assert_eq!(pull(&c, &cells, far), vec![1]);
        }
    }

    /// `read_into_verify` も同じ: sparse を読んだ後で dense へ移った値を見落とさず、 sparse で足した分は捨てて
    /// 読み直す (2 度足さない)。 呼び手が先に積んでいた分には触らない。
    #[test]
    fn dense_reader_into_rechecks_after_values_move_out_of_sparse() {
        let (c, far) = ready_to_grow();
        let w = c.clone();
        hook::set("reader_missed_dense", move || trigger(&w, far));
        let mut out = vec![7, 7, 7];
        c.read_into_verify(far, &mut out);
        assert_eq!(&out[..3], &[7, 7, 7], "先に積んであった分を変えた");
        assert_eq!(out.len() - 3, 501, "dense へ移った値を見落とした / sparse の分と 2 度足した: {} 件", out.len() - 3);
        let mut got = out[3..].to_vec();
        got.sort_unstable();
        got.dedup();
        assert_eq!(got.len(), 501, "重複がある");

        // sparse を読んだ **後** に dense へ移る (読み直す前): sparse で足した 500 件を捨ててから dense を読む
        let (c, far) = ready_to_grow();
        let w = c.clone();
        hook::set("reader_read_sparse", move || trigger(&w, far));
        let mut out = vec![7, 7, 7];
        c.read_into_verify(far, &mut out);
        assert_eq!(&out[..3], &[7, 7, 7], "先に積んであった分を変えた");
        assert_eq!(out.len() - 3, 501, "sparse で足した分を捨てずに dense も足した: {} 件", out.len() - 3);
    }

    /// #373: 配列を伸ばした直後 (sparse から消す前) は、 移した値が dense と sparse の両方にある。 その間に値を
    /// 数える読み (`sparse_range` / `unique_values`) は 2 度数えない。
    #[test]
    fn dense_moved_values_are_not_counted_twice_before_cleanup() {
        let (c, far) = ready_to_grow();
        let r = c.clone();
        let seen = Arc::new(std::sync::Mutex::new(None));
        let out = seen.clone();
        hook::set("grown_before_cleanup", move || {
            assert!(r.sparse.count_in(far, far) > 0, "前提: まだ sparse に残っている");
            *out.lock().unwrap() = Some((r.sparse_range(far, far).len(), r.unique_values()));
        });
        trigger(&c, far);
        let (sparse_len, values) = seen.lock().unwrap().take().expect("割り込み点を通っていない");
        assert_eq!(sparse_len, 0, "dense に移した値を sparse_range にも出した");
        assert_eq!(values, vec![far], "dense に移した値を unique_values に 2 度出した");
    }

    /// #373: sparse を掃除した後は、 待ちの数 (配列を伸ばす判断に使う) を数え直す。 古い entry を数えたままだと、
    /// 溜まっていない値で配列を伸ばす。
    #[test]
    fn dense_waiting_is_recounted_after_compaction() {
        let c = LockFreeCylinder::new(0);
        let cells = Cells::new();
        let far = 2_000u64; // 伸ばすには 「bucket + 待ち + 1」 が 501 以上
        for e in 0..400 {
            cells.set(&c, e, far);
        }
        for e in 0..400 {
            cells.set(&c, e, 1); // 全部移った: sparse の 400 件は古い entry
        }
        c.compact_sparse(cells.keep());
        for e in 400..600 {
            cells.set(&c, e, far);
        }
        // 生きている待ちは 200 + bucket 1 (値 1)。 古い 400 件を数えたままなら 601 で伸びる
        assert!(!c.is_dense(far), "掃除で消えた古い entry を待ちに数えて、 配列を伸ばした");
        assert_eq!(pull(&c, &cells, far), cells.eids_of(far));
    }

    /// #373: 配列を伸ばして sparse から dense へ移している最中も、 読み手は入れ終えた値を必ず見つける (移した後で
    /// sparse から消すので、 古い長さで sparse を読んだ読み手は長さを読み直す)。 範囲の読みも同じ。
    #[test]
    fn dense_readers_never_miss_values_moved_from_sparse() {
        let c = Arc::new(LockFreeCylinder::new(0));
        // 値 v = 2000 + (e % 64) に行を溜める: 64 個の値が sparse に溜まり、 ある所で配列が伸びて全部移る
        let n: u32 = if cfg!(miri) { 300 } else { 60_000 };
        let ready = Arc::new(AtomicU32::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let val = |e: u32| 2_000 + (e % 64) as u64;
        let writer = {
            let (c, ready) = (c.clone(), ready.clone());
            std::thread::spawn(move || {
                for e in 0..n {
                    c.insert(e, val(e), all);
                    ready.store(e + 1, Ordering::Release);
                }
            })
        };
        let readers: Vec<_> = (0..3u32)
            .map(|k| {
                let (c, ready, stop) = (c.clone(), ready.clone(), stop.clone());
                std::thread::spawn(move || {
                    let mut reads = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        let done = ready.load(Ordering::Acquire);
                        if done == 0 {
                            std::hint::spin_loop();
                            continue;
                        }
                        // 入れ終えた eid の 1 つ: その値の読みに必ず居る
                        let e = (reads as u32).wrapping_mul(2_654_435_761).wrapping_add(k) % done;
                        let got = c.read_to_vec(val(e));
                        assert!(got.contains(&e), "値 {} の読みに入れ終えた eid {e} が居ない", val(e));
                        assert!(got.iter().all(|&x| val(x) == val(e)), "別の値の eid が混ざった");
                        let r = c.range_raw(val(e), val(e));
                        assert!(r.contains(&e), "範囲の読みに入れ終えた eid {e} が居ない");
                        reads += 1;
                    }
                    reads
                })
            })
            .collect();
        writer.join().unwrap();
        stop.store(true, Ordering::Relaxed);
        let reads: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();
        assert!(reads > 0);
        if !cfg!(miri) {
            assert!(c.is_dense(2_000), "前提: 途中で配列が伸びて dense に移っている");
        }
        for v in 2_000..2_064u64 {
            let mut got = c.read_to_vec(v);
            got.sort_unstable();
            assert_eq!(got, (0..n).filter(|&e| val(e) == v).collect::<Vec<_>>());
        }
    }

    /// `used` (配列を伸ばす判断に使う、 bucket のある要素の数) が配列の中身と一致する。
    fn assert_used_matches(c: &LockFreeCylinder) {
        let guard = epoch::pin();
        for k in [LOW, POS, NEG] {
            let n = c.vec_of(k, &guard).iter().filter(|s| s.get().is_some()).count();
            assert_eq!(c.used[k].load(Ordering::Relaxed), n, "配列 {k} の used");
        }
    }

    /// まとめて組む (`build`) と 1 件ずつ入れる (`insert`) で、 読みが同じ。 まばらな値は sparse、 詰まった値と
    /// 行の溜まった遠い値は dense に振り分ける。 組んだ後の `insert` (配列を伸ばす・sparse から移す) も続けられる。
    #[test]
    fn dense_build_matches_inserting_one_by_one() {
        let enc = |v: i64| (v as u64) ^ MID;
        let n: u32 = if cfg!(miri) { 300 } else { 30_000 };
        let mut x = 0x1234_5678_9abc_def1u64;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        // 小さい値 / まばらな値 (16 個に 1 つ) / 1 つの遠い値に溜まる行 / 0 に近い符号付き / 窓の外
        let entries: Vec<(u32, u64)> = (0..n)
            .map(|e| {
                let v = match next() % 5 {
                    0 => next() % 50,
                    1 => 3_000 + (next() % 20_000) * 16,
                    2 => 9_000,
                    3 => enc((next() % 200) as i64 - 100),
                    _ => (1 << 40) + next() % 1_000,
                };
                (e, v)
            })
            .collect();
        let built = LockFreeCylinder::new(0);
        built.build(|| entries.iter().copied());
        let one = LockFreeCylinder::new(0);
        for &(e, v) in &entries {
            one.insert(e, v, all);
        }
        let sorted = |mut v: Vec<u32>| {
            v.sort_unstable();
            v
        };
        let mut vals: Vec<u64> = entries.iter().map(|p| p.1).collect();
        vals.sort_unstable();
        vals.dedup();
        for &v in &vals {
            let want: Vec<u32> = entries.iter().filter(|p| p.1 == v).map(|p| p.0).collect();
            assert_eq!(sorted(built.read_to_vec(v)), want, "build: {v}");
            assert_eq!(sorted(one.read_to_vec(v)), want, "insert: {v}");
        }
        for (lo, hi) in [(0, 100), (2_000, 400_000), (enc(-50), enc(50)), (0, u64::MAX - 1)] {
            let want: Vec<u32> = entries.iter().filter(|p| lo <= p.1 && p.1 <= hi).map(|p| p.0).collect();
            assert_eq!(sorted(built.range_raw(lo, hi)), sorted(want), "build range {lo}..={hi}");
        }
        assert_eq!(sorted_u64(built.unique_values()), vals);
        assert_eq!(built.total_live(), n as usize);
        assert!(built.is_dense(10) && built.is_dense(enc(-3)), "小さい値は dense");
        // 9_000 の行で 3 万前後までは配列が詰まって見えるので、 まばらな値はそれより先で見る
        assert!(!built.is_dense(3_000 + 16 * 4_200), "まばらな値は sparse");
        if !cfg!(miri) {
            assert!(built.is_dense(9_000), "行の溜まった遠い値は dense");
        }
        // 配列の長さ L は 「添字 L 未満の entry の数 × DENSE_MIN_FILL」 以下
        assert!(built.dense_slots() <= DENSE_MIN_FILL * n as usize, "配列が大きすぎる: {}", built.dense_slots());
        assert_used_matches(&built);
        // 配列は入る値の最大の添字まで (小さい値だけなら 2 の冪まで伸ばさない)
        let small = LockFreeCylinder::new(0);
        small.build(|| (0..200u32).map(|e| (e, (e % 50) as u64)));
        assert_eq!(small.dense_slots(), 50);
        // 組んだ後も 1 件ずつ入れられる: まばらな値に行を溜めて配列を伸ばす
        let (far, more) = (3_000 + 16 * 4_200, if cfg!(miri) { 50 } else { 12_000 });
        for e in n..n + more {
            built.insert(e, far, all);
        }
        assert!(built.is_dense(far) || cfg!(miri), "行が溜まっても配列を伸ばさない");
        let want: Vec<u32> = entries.iter().filter(|p| p.1 == far).map(|p| p.0).chain(n..n + more).collect();
        assert_eq!(sorted(built.read_to_vec(far)), want);
        assert_used_matches(&built);
    }

    fn sorted_u64(mut v: Vec<u64>) -> Vec<u64> {
        v.sort_unstable();
        v
    }

    /// 3 本の dense 配列の bucket 数の和。
    fn dense_len(c: &LockFreeCylinder) -> usize {
        let guard = epoch::pin();
        [LOW, POS, NEG].iter().map(|&k| unsafe { c.arr(k).load(Ordering::Acquire, &guard).deref() }.len()).sum()
    }

    /// 2026-10-11: dense の窓の始まり (`set_low_base`)。 2^20 より先の値 (後ろの表を指す ref) も、 窓の始まりを
    /// そこにすれば dense に置ける。 窓の前の値 (別の表を指す ref など) は sparse で、 答えは同じ。 1 件ずつの insert
    /// (配列を伸ばす・異動で古い entry を残す) と、 まとめて組む `build` の両方。 値の一覧・範囲の読み・件数も。
    /// 対照: 窓の始まりが 0 なら、 同じ値は全部 sparse。
    #[test]
    fn dense_low_base_puts_values_far_beyond_2_20() {
        let base = 2_000_000u64;
        let n = if cfg!(miri) { 400u32 } else { 30_000 };
        // 会社 200 社 (base から) を指す社員。 50 人に 1 人は窓の前の値 (base − 5) を指す
        let value_of = |e: u32| if e.is_multiple_of(50) { base - 5 } else { base + u64::from(e % 200) };
        let c = LockFreeCylinder::new(0);
        c.set_low_base(base);
        assert_eq!(c.low_base(), base);
        let cells = Cells::new();
        for e in 0..n {
            cells.set(&c, e, value_of(e));
        }
        // 異動 (古い entry を残す)
        for e in (0..n).step_by(7) {
            cells.set(&c, e, base + 150);
        }
        assert!(c.is_dense(base) && c.is_dense(base + 199), "窓の中の値が dense でない");
        assert!(!c.is_dense(base - 5), "窓の前の値は sparse");
        let probes = [base - 5, base, base + 1, base + 150, base + 199, base + 200];
        for v in probes {
            assert_eq!(pull(&c, &cells, v), cells.eids_of(v), "値 {v}");
        }
        assert_eq!(c.slice_len_live(base + 150), cells.eids_of(base + 150).len(), "dense の値ごとの件数");
        // 1 回の pin で件数 (dense の値) / None (sparse の値): `is_dense` + `slice_len_live` と同じ答え
        for v in probes {
            let two = c.is_dense(v).then(|| c.slice_len_live(v));
            assert_eq!(c.dense_live_len(v), two, "dense_live_len({v})");
        }
        assert_eq!(c.dense_live_len(base), Some(0), "窓の中で誰も指さない値は空の要素 = 0");
        assert_eq!(c.dense_live_len(base - 5), None, "窓の前の値は sparse = None");
        // 今の値の種類 (base + 0 / 50 / 100 は誰も指さない: 50 人に 1 人の窓の前の値と重なるので)
        let mut want: Vec<u64> = cells.0.borrow().values().copied().collect();
        want.sort_unstable();
        want.dedup();
        assert!(want.contains(&(base - 5)) && want.contains(&(base + 199)) && !want.contains(&base), "前提");
        let mut got = c.unique_values();
        got.sort_unstable();
        assert_eq!(got, want, "値の一覧 (添字 → 値に窓の始まりを足す)");
        // 範囲: 窓の前から窓の中へまたぐ / 窓の中だけ
        for (lo, hi) in [(base - 10, base + 3), (base + 100, base + 120)] {
            let mut r = c.range_raw(lo, hi);
            r.retain(|&e| cells.0.borrow().get(&e).is_some_and(|&v| (lo..=hi).contains(&v)));
            r.sort_unstable();
            r.dedup();
            let mut w: Vec<u32> = (lo..=hi).flat_map(|v| cells.eids_of(v)).collect();
            w.sort_unstable();
            assert_eq!(r, w, "範囲 {lo}..={hi}");
        }
        assert_used_matches(&c);
        // まとめて組む形も同じ
        let entries: Vec<(u32, u64)> = (0..n).filter_map(|e| cells.0.borrow().get(&e).map(|&v| (e, v))).collect();
        let b = LockFreeCylinder::new(0);
        b.set_low_base(base);
        b.build(|| entries.iter().copied());
        assert!(b.is_dense(base) && b.is_dense(base + 199) && !b.is_dense(base - 5));
        for v in probes {
            let mut r = b.read_to_vec(v);
            r.sort_unstable();
            assert_eq!(r, cells.eids_of(v), "組んだ形の値 {v}");
        }
        // 対照: 窓の始まりが 0 なら、 同じ値は全部 sparse
        let z = LockFreeCylinder::new(0);
        z.build(|| entries.iter().copied());
        assert!(!z.is_dense(base) && !z.is_dense(base + 199), "対照: 窓の始まり 0 で 2^20 より先の値が dense");
        let mut r = z.read_to_vec(base + 1);
        r.sort_unstable();
        assert_eq!(r, cells.eids_of(base + 1), "対照の答え");
    }

    /// `set_low_base` は `MID` の上下の窓 (符号付きの値) と重なる始まりを 0 にする。
    #[test]
    fn dense_low_base_never_overlaps_the_signed_windows() {
        let c = LockFreeCylinder::new(0);
        c.set_low_base(MID - 10);
        assert_eq!(c.low_base(), 0, "符号付きの窓と重なる始まりを置いた");
        let d = LockFreeCylinder::new(0);
        let ok = MID - MID_HALF - DENSE_CAP as u64;
        d.set_low_base(ok);
        assert_eq!(d.low_base(), ok);
        d.insert(1, ok + 3, |_, _| true);
        d.insert(2, MID - 1, |_, _| true); // 符号付きの -1 = NEG の窓
        assert!(d.is_dense(ok + 3) && d.is_dense(MID - 1));
        assert_eq!(d.read_to_vec(ok + 3), vec![1]);
        assert_eq!(d.read_to_vec(MID - 1), vec![2]);
    }

    /// 符号付き 64 bit の符号化 (`v ^ 2^63`) で 0 に近い値は 2 本目・3 本目の dense に入る: 値ごとの件数・種類数を
    /// O(1) で持ち、 範囲も引ける。 窓の中でも配列を伸ばしていない遠い値 (±2^19 近く) と窓の外は sparse。
    #[test]
    fn values_near_mid_are_dense() {
        let enc = |v: i64| (v as u64) ^ MID;
        let c = LockFreeCylinder::new(0);
        let h = MID_HALF as i64;
        let near = [0i64, -1, 1, 30, -30, 500, -500];
        for (e, &v) in near.iter().enumerate() {
            c.insert(e as u32, enc(v), all);
            c.insert(100 + e as u32, enc(v), all);
            assert!(c.is_dense(enc(v)), "{v} は dense");
        }
        // 窓の中だが遠い値 (1 件ずつ) は配列を伸ばさない。 窓の外は常に sparse
        c.insert(50, enc(h - 1), all);
        c.insert(51, enc(-h), all);
        for v in [h - 1, -h, h, -h - 1, 1 << 40, -(1 << 40)] {
            assert!(!c.is_dense(enc(v)), "{v} は sparse");
        }
        assert!(dense_len(&c) <= 2 * DENSE_FREE_LEN, "遠い値 1 件で配列を窓の端まで伸ばした: {}", dense_len(&c));
        // 種類数は dense の分だけ数える = 近い値は全部数えられている
        assert_eq!(c.unique_live(), near.len() as u32);
        for (e, &v) in near.iter().enumerate() {
            assert_eq!(c.read_to_vec(enc(v)), vec![e as u32, 100 + e as u32], "{v}");
            assert_eq!(c.slice_len_live(enc(v)), 2, "{v}");
        }
        assert_eq!(c.read_to_vec(enc(h - 1)), vec![50]);
        assert_eq!(c.read_to_vec(enc(-h)), vec![51]);
        let mut vals = c.unique_values();
        vals.sort_unstable();
        let mut want: Vec<u64> = near.iter().map(|&v| enc(v)).chain([enc(h - 1), enc(-h)]).collect();
        want.sort_unstable();
        assert_eq!(vals, want, "添字 → 値の戻し + sparse の値");
        // 範囲: dense と sparse をまたいで
        let mut got = c.range_raw(enc(-30), enc(h - 1));
        got.sort_unstable();
        assert_eq!(got, vec![0, 1, 2, 3, 4, 5, 50, 100, 101, 102, 103, 104, 105], "-30..=2^19-1 (-500 の 6 だけ外)");
        let mut got = c.range_raw(enc(-h), enc(-1));
        got.sort_unstable();
        assert_eq!(got, vec![1, 4, 6, 51, 101, 104, 106]);
        // 0 以上だけの列は u32 の列と同じ数の bucket (上の配列だけ、 値の数ぶん)。 負をまたいでも値の数ぶん
        let n = 1000u32;
        let (as_u32, as_i64, both) = (LockFreeCylinder::new(0), LockFreeCylinder::new(0), LockFreeCylinder::new(0));
        for v in 0..n {
            as_u32.insert(v, v as u64, all);
            as_i64.insert(v, enc(v as i64), all);
            both.insert(v, enc(v as i64 - (n / 2) as i64), all);
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
            c.insert(e, (e % 2) as u64, all);
        }
        assert_eq!(c.total_live(), 10);
        assert_eq!(c.unique_live(), 2);
        assert!(!c.read_to_vec_verify(0).1);
        assert!(!c.read_to_vec_verify(1).1);

        // e0 を v0 → v1 へ churn (呼び出し側の順序で note_stale → insert)
        c.note_stale(0);
        c.insert(0, 1, all);
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
            c.insert(e, 1, all);
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
            c.insert(e, 0, all);
        }
        // e0..e59 が v1 へ移動 (呼び出し側の順序で note_stale → insert)
        for e in 0..60u32 {
            c.note_stale(0);
            c.insert(e, 1, all);
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
            c.insert(e, (e % card) as u64, all);
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
                    c.insert(v, v as u64, all);
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
        c.insert(7, 1, all); // e=7 が v1 に live
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let (c, stop) = (c.clone(), stop.clone());
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    // e を v1 → v0 → v1 と往復 (himo_store の順序契約: flag → 移動)
                    c.note_stale(1);
                    c.insert(7, 0, all);
                    c.note_stale(0);
                    c.insert(7, 1, all);
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
            c.insert(e, (e % 7) as u64, all);
        }
        stop.store(true, Ordering::Relaxed);
        for r in readers {
            r.join().unwrap();
        }
        assert_eq!(c.total(), n as usize);
        assert_eq!(c.unique_count(), 7);
    }
}
