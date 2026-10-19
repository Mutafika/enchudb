//! `AppendBucket` — 単一 writer / 多 reader の lock-free append-only 列。 #95。
//!
//! `BucketCylinder` の各 bucket（value → eid 列）の格納をこれに置き換える土台。
//! 現状の `RwLock<BucketCylinder>` は read が write に相互排他になり、 長い read が
//! write を stall させる（#95）。 この型は:
//!
//! - **read は完全 lock-free**（writer を一切待たない）。 crossbeam-epoch で pin。
//! - **write は単一 consumer 前提**で直列（append O(1) amortized）。
//! - 一度 publish した要素は二度と動かさない（append-only）ので、 read は published
//!   範囲 `[0..len]` を安全に `&[u32]` として読める。
//! - 容量超過時のみ backing を倍化し、 旧 backing は epoch で全 reader 通過後に解放。
//!
//! ## 並行契約
//! - `push` / `compact_in` は **同時に 1 thread のみ**（HimoStore write_lock 下）。
//!   複数 writer は未対応（race する）。
//! - `read_to_vec` / `with_read` / `len` は **多 reader 並行可**、 writer と並行可。
//! - request12 P2: `compact_in` は backing を組み直して swap するため、
//!   **len は単調非減少ではない** (compaction で縮む)。reader は len の減少を
//!   「並行 compaction があった」として扱うこと (スナップショット自体は常に整合)。
//!
//! ## Miri での UB 検証（#95）
//! この型の `unsafe`（`from_raw_parts` over `UnsafeCell`、epoch `defer_destroy`）は
//! Miri で aliasing UB がないことを確認済み。crossbeam-epoch 0.9 は **Stacked Borrows**
//! に非互換（内部の intrusive list で違反、既知）なので **Tree Borrows** で回す。
//! epoch は遅延解放なので exit 時の未回収 garbage を leak 扱いしない `-Zmiri-ignore-leaks`
//! も要る。concurrent/大量 test は `cfg!(miri)` で縮小。
//!
//! ```sh
//! MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-tree-borrows -Zmiri-ignore-leaks" \
//!   cargo +nightly miri test -p enchudb-engine --lib append_bucket
//! ```

use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned};
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};

const INITIAL_CAP: usize = 4;

/// 単一の連続 backing。 `data[0..len]` が publish 済み（不変）、 `data[len..cap]` は spare。
struct Buf {
    data: Box<[UnsafeCell<u32>]>,
    len: AtomicUsize,
    /// `data[0..len]` の頭の昇順 (同じ値が続くのも昇順に数える) の長さ。 書き手が足すたびに持つ (頭が全部昇順で、 前の値
    /// 以上なら 1 つ伸ばす)。 読み手は `len` と小さい方を使う (読んだ `len` より大きい値は、 その後に足した書き手の分)。
    sorted: AtomicUsize,
}

// SAFETY: published 範囲 [0..len] は書き換わらない。 writer は spare slot [len] のみ書く。
// reader は published 範囲のみ読む。 disjoint index への同時アクセスで、 可視化は len の
// Release/Acquire で順序付けられる。
unsafe impl Sync for Buf {}

impl Buf {
    fn with_cap(cap: usize) -> Self {
        let v: Vec<UnsafeCell<u32>> = (0..cap).map(|_| UnsafeCell::new(0)).collect();
        Self {
            data: v.into_boxed_slice(),
            len: AtomicUsize::new(0),
            sorted: AtomicUsize::new(0),
        }
    }
    #[inline]
    fn cap(&self) -> usize {
        self.data.len()
    }
    /// published 範囲を `&[u32]` として borrow。 caller が [0..n] の不変性を保証すること。
    #[inline]
    unsafe fn published(&self, n: usize) -> &[u32] {
        // SAFETY: caller が n <= published len かつ [0..n] 不変を保証。
        unsafe { std::slice::from_raw_parts(self.data.as_ptr() as *const u32, n) }
    }
}

pub struct AppendBucket {
    backing: Atomic<Buf>,
    /// この bucket の値を Column が現在指している eid 数 (= verify 後の正確な件数)。
    /// request12: 書き込み側 (HimoStore、write_lock 下) が旧値の decrement / 新値の
    /// increment を行う。read の fast-path 判定と統計・planner の pivot 選択に使う。
    live: AtomicU32,
    /// この bucket に stale (値更新/削除の残骸) が居るか。true なら read は
    /// Column verify + dedup を通す。himo 全体 flag (旧 any_removed) の bucket 局所版。
    removed: AtomicBool,
    /// 最後に照らした読みの時の (len, live) (`len << 32 | live`、 `u64::MAX` = まだ)。 [`mark_verified`](Self::mark_verified)。
    verified: AtomicU64,
    /// 間に書き込みの無い照らしを繰り返した回数 (1 回目の読みの後の読み直しの数)。
    rereads: AtomicU32,
}

// SAFETY: 単一 writer / 多 reader。 全アクセスは atomic len + epoch で同期される。
unsafe impl Sync for AppendBucket {}
unsafe impl Send for AppendBucket {}

/// 同じ eid の 2 本目以降を落とし、 eid の昇順にして返す (重複は稀 = re-tie 往復痕。 hash を使わない)。
/// 前は重複を探すためだけに (eid, 位置) の写しを並べ、 並びは捨てて元の順 (最初に現れた順) で返していた。 今は並べた順で
/// 返すので、 組み直した bucket は eid の昇順になる (組んだ時の bucket と同じ形)。 bucket は 「前の昇順 + 後ろに足した順番外」
/// なので、 読みと同じ [`sort_appended`] で後ろだけ並べて混ぜる。
fn dedup_sorted(mut v: Vec<u32>) -> Vec<u32> {
    sort_appended(&mut v);
    v.dedup();
    v
}

/// 列の頭の昇順の長さ (最初に崩れた所の次の位置。 全部昇順なら列の長さ)。 昇順かは先に `is_sorted` で見て、 崩れた所は
/// 256 個の塊ごとに `is_sorted` で飛ばしてから探す ([`sort_appended`] と `HimoStore` の 1 件ずつ出す読みが使う)。
pub(crate) fn sorted_prefix_len(v: &[u32]) -> usize {
    if v.is_sorted() {
        return v.len();
    }
    const C: usize = 256;
    let mut s = 0;
    while s + C < v.len() && v[s..=s + C].is_sorted() {
        s += C;
    }
    s + v[s..].windows(2).position(|w| w[0] > w[1]).map_or(v.len() - s, |i| i + 1)
}

/// bucket から写した eid の列を昇順にする (verify した読み・組み直し)。 bucket は組んだ時と組み直した時の eid の昇順の上に、
/// 書き換えで入ってきた entity が後ろに足されるので、 列は 「長い昇順 + 後ろの順番外」。 最初に崩れた所から後ろだけを写して
/// 並べ、 列の後ろから混ぜる (前の昇順の部分は並べ直さない)。 昇順かは先に `is_sorted` (塊ごとにまとめて比べるので速い) で見て、
/// 崩れた所も 256 個の塊ごとに `is_sorted` で飛ばしてから探す (1 つずつ探すと、 昇順の時に 1 行 0.07 → 0.30 ns になった)。
/// 全部ばらばらでも `sort_unstable` と同じ並びを返す (後ろの写しの分だけ余計に手間がかかる)。
pub(crate) fn sort_appended(v: &mut [u32]) {
    let k = sorted_prefix_len(v);
    if k == v.len() {
        return;
    }
    let mut b = v[k..].to_vec();
    b.sort_unstable();
    // 前 (v[..k]、 昇順) と b (昇順) を、 v の後ろから大きい順に置く。 置く先 w は読む前 i より前に来ない (w = i + j)
    let (mut i, mut j) = (k, b.len());
    while j > 0 {
        let w = i + j;
        if i > 0 && v[i - 1] > b[j - 1] {
            v[w - 1] = v[i - 1];
            i -= 1;
        } else {
            v[w - 1] = b[j - 1];
            j -= 1;
        }
    }
}

impl AppendBucket {
    /// 空の bucket（初回 push まで heap 確保しない）。
    pub fn new() -> Self {
        Self {
            backing: Atomic::new(Buf::with_cap(0)),
            live: AtomicU32::new(0),
            removed: AtomicBool::new(false),
            verified: AtomicU64::new(u64::MAX),
            rereads: AtomicU32::new(0),
        }
    }

    /// `cap` 件分の backing を先に確保した空の bucket。 入る件数が分かっている時 (索引をまとめて組む時、 #394)
    /// に使う — 倍々で伸ばすと、 伸ばすたびの古い backing が epoch を過ぎるまで残る。
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            backing: Atomic::new(Buf::with_cap(cap)),
            live: AtomicU32::new(0),
            removed: AtomicBool::new(false),
            verified: AtomicU64::new(u64::MAX),
            rereads: AtomicU32::new(0),
        }
    }

    // ──── request12: bucket ローカルメタデータ ────
    // live/removed は write_lock 下の単一 writer だけが更新する。read 側の
    // verify 要否は `read_snapshot_verify` の 3 段プロトコル (slice → flag →
    // backing ptr 再検証) で判定する。flag 単独の前読み/後読みはどちらも不健全:
    // - 前読み: flag=false load 後に積まれた churn 痕 (往復 churn の dup) を
    //   fast path が素通しする (PR #103 レビューで実証、再現テストあり)
    // - 後読み: compaction の flag リセットと交差すると旧 backing の stale を
    //   flag=false で素通しする
    // Column 側の無同期読みは既知の別 issue #100。

    /// live +1。戻り値は増分前 (0 なら「live な値が初めて入った」= unique 判定)。
    #[inline]
    pub fn live_inc(&self) -> u32 {
        self.live.fetch_add(1, Ordering::Relaxed)
    }

    /// stale 化: removed flag を立てて live -1。戻り値は減分前の live。
    /// live == 0 のとき (契約違反: Column に無い値の stale 通知) は何もせず None。
    #[inline]
    pub fn note_stale(&self) -> Option<u32> {
        self.removed.store(true, Ordering::Release);
        self.live
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_sub(1))
            .ok()
    }

    /// 現在の live 数 (= verify 後の正確な件数)。
    #[inline]
    pub fn live(&self) -> u32 {
        self.live.load(Ordering::Relaxed)
    }

    /// この bucket の read が Column verify を要するか。
    #[inline]
    pub fn needs_verify(&self) -> bool {
        self.removed.load(Ordering::Acquire)
    }

    /// 今の (len, live)。 書き手は足すと len を、 古い印を付けると live を動かす (詰め直すまで len は減らず、 足さない間の live は
    /// 減るだけ) ので、 同じなら間に書き込みは無い。
    fn snap(&self, guard: &Guard) -> u64 {
        let cur = self.backing.load(Ordering::Acquire, guard);
        // SAFETY: backing は常に非 null。
        let len = unsafe { cur.deref() }.len.load(Ordering::Acquire) as u64;
        (len << 32) | u64::from(self.live.load(Ordering::Relaxed))
    }

    /// 照らして読む (Column verify を通す) と決めた時に呼ぶ: 今の (len, live) を控え、 間に書き手の触っていない照らしを何回続けて
    /// いるかを返す (0 = 前に照らした時から書き込みがあった / 初めて、 1 = 2 回目、 2 = 3 回目 …)。 詰め直すかは呼び手が決める
    /// (`HimoStore::compact_if_reread`)。 読み手どうしが並ぶと同じ数を見うる (詰め直しは値の鍵を取れた 1 本だけ)。
    pub fn mark_verified(&self, guard: &Guard) -> u32 {
        let snap = self.snap(guard);
        if self.verified.swap(snap, Ordering::Relaxed) == snap {
            self.rereads.fetch_add(1, Ordering::Relaxed).saturating_add(1)
        } else {
            self.rereads.store(0, Ordering::Relaxed);
            0
        }
    }

    /// 単一 writer append。 spare があれば in-place、 満杯なら倍化 + epoch 遅延解放。
    /// (standalone 用。 hot path は `push_in` — lib 内 caller が居なくても API として維持)
    #[allow(dead_code)]
    pub fn push(&self, eid: u32) {
        let guard = epoch::pin();
        self.push_in(eid, &guard);
    }

    /// `push` の外部 guard 版。 呼び出し元が 1 query/1 insert 単位で pin 済みのとき
    /// pin の重複を避ける（#95: insert 経路の pin 3 回 → 1 回）。
    /// 戻り値は push 前の published len（0 なら「空 bucket への初 push」= unique 判定用）。
    pub fn push_in(&self, eid: u32, guard: &Guard) -> usize {
        let cur = self.backing.load(Ordering::Acquire, guard);
        // SAFETY: backing は常に非 null（new/push で維持）。
        let b = unsafe { cur.deref() };
        let l = b.len.load(Ordering::Relaxed); // 単一 writer なので自身の len は Relaxed で可
        if l < b.cap() {
            // spare slot（未 publish の index l）に書いてから len を publish
            unsafe {
                *b.data[l].get() = eid;
            }
            // 頭の昇順が全部 (= l) で前の値以上なら伸ばす (読み手は len と小さい方を使うので、 len より先に置いてよい)
            if b.sorted.load(Ordering::Relaxed) == l && (l == 0 || unsafe { *b.data[l - 1].get() } <= eid) {
                b.sorted.store(l + 1, Ordering::Relaxed);
            }
            b.len.store(l + 1, Ordering::Release);
        } else {
            let ncap = (b.cap() * 2).max(INITIAL_CAP);
            let nb = Buf::with_cap(ncap);
            // 既存 published 要素をコピー（[0..l] は不変なので安全に読める）
            let src = unsafe { b.published(l) };
            for (i, &v) in src.iter().enumerate() {
                unsafe {
                    *nb.data[i].get() = v;
                }
            }
            unsafe {
                *nb.data[l].get() = eid;
            }
            let sorted = b.sorted.load(Ordering::Relaxed);
            let sorted = if sorted == l && (l == 0 || src[l - 1] <= eid) { l + 1 } else { sorted };
            nb.sorted.store(sorted, Ordering::Relaxed);
            nb.len.store(l + 1, Ordering::Relaxed); // まだ誰も見ていない
            self.backing.store(Owned::new(nb), Ordering::Release);
            // 旧 backing は全 reader が epoch を通過してから解放
            unsafe {
                guard.defer_destroy(cur);
            }
        }
        l
    }

    /// published prefix を `keep` 判定で組み直して backing を atomic swap する
    /// (request12 P2 = incremental compaction)。単一 writer (write_lock 下) 前提。
    ///
    /// - 同一 eid の重複 slot (re-tie 往復痕) は 1 本だけ残し、 残した entry は eid の昇順に並べ直す (dedup)
    /// - 旧 backing は epoch で全 reader 通過後に解放 — reader は停止しない
    /// - swap 後に live を確定し、removed flag を **backing swap の後で** false に
    ///   戻す (Release)。reader は flag を Acquire で slice より先に読むので、
    ///   flag=false を見た reader は必ず新 backing を見る。
    ///
    /// 戻り値は compaction 後の live 数。
    pub fn compact_in(&self, guard: &Guard, mut keep: impl FnMut(u32) -> bool) -> usize {
        let cur = self.backing.load(Ordering::Acquire, guard);
        // SAFETY: backing は常に非 null。
        let b = unsafe { cur.deref() };
        let l = b.len.load(Ordering::Relaxed); // 単一 writer なので自身の len は Relaxed で可
        let src = unsafe { b.published(l) };
        let kept = dedup_sorted(src.iter().copied().filter(|&e| keep(e)).collect());

        let ncap = kept.len().next_power_of_two().max(INITIAL_CAP);
        let nb = Buf::with_cap(ncap);
        for (i, &v) in kept.iter().enumerate() {
            unsafe {
                *nb.data[i].get() = v;
            }
        }
        nb.sorted.store(kept.len(), Ordering::Relaxed); // dedup_sorted で昇順
        nb.len.store(kept.len(), Ordering::Relaxed); // まだ誰も見ていない
        self.backing.store(Owned::new(nb), Ordering::Release);
        // SAFETY: 旧 backing は全 reader が epoch を通過してから解放。
        unsafe {
            guard.defer_destroy(cur);
        }
        self.live.store(kept.len() as u32, Ordering::Relaxed);
        self.verified.store(u64::MAX, Ordering::Relaxed);
        self.rereads.store(0, Ordering::Relaxed);
        self.removed.store(false, Ordering::Release); // swap 後 — 順序契約はメソッド doc 参照
        kept.len()
    }

    /// 外部 guard 下で published slice を borrow（query 単位 pin 用、 zero-copy）。
    #[inline]
    pub fn with_read<R>(&self, guard: &Guard, f: impl FnOnce(&[u32]) -> R) -> R {
        let cur = self.backing.load(Ordering::Acquire, guard);
        let b = unsafe { cur.deref() };
        let n = b.len.load(Ordering::Acquire);
        // SAFETY: [0..n] は publish 済み = 不変。 guard が backing を生存させる。
        let slice = unsafe { b.published(n) };
        f(slice)
    }

    /// snapshot コピー（test 用、 内部で pin）。
    #[cfg(test)]
    pub fn read_to_vec(&self) -> Vec<u32> {
        let guard = epoch::pin();
        self.with_read(&guard, |s| s.to_vec())
    }

    /// snapshot コピー + verify 要否 (request12)。順序が本質 (PR #103 レビュー):
    ///
    /// 1. backing p1 の published slice を copy
    /// 2. removed flag を load — slice より **後**
    /// 3. backing p2 を再 load、p1 != p2 なら compaction と交差 → verify
    ///
    /// 健全性: churn 痕 (dup / 離脱後 slot と新 slot の混在) を slice で観測した
    /// なら、その push の len Release-store を Acquire で読んでいる ⇒ writer が
    /// push より前に立てた flag=true が (2) で見える。flag を false に戻すのは
    /// compaction だけで、その場合 backing が必ず swap されるので (3) が落とす。
    /// flag=false && p1==p2 で残る観測遅延は「stale をまだ live と見る」方向のみ
    /// = 読み開始時点への線形化で正当 (旧 any_removed と同等の許容)。
    /// guard を (1)-(3) で保持し続けるため旧 backing は解放されず ptr ABA なし。
    pub fn read_snapshot_verify(&self, guard: &Guard) -> (Vec<u32>, bool) {
        let p1 = self.backing.load(Ordering::Acquire, guard);
        // SAFETY: backing は常に非 null。
        let b = unsafe { p1.deref() };
        let n = b.len.load(Ordering::Acquire);
        // SAFETY: [0..n] は publish 済み = 不変。 guard が backing を生存させる。
        let v = unsafe { b.published(n) }.to_vec();
        let f = self.removed.load(Ordering::Acquire);
        let p2 = self.backing.load(Ordering::Acquire, guard);
        (v, f || p2 != p1)
    }

    /// [`read_snapshot_verify`](Self::read_snapshot_verify) の、 呼び手の buffer の後ろに足す版 (多くの bucket を
    /// 続けて読む時に、 bucket ごとの Vec を作らない)。 3 段の順序は同じ。
    pub fn read_snapshot_verify_into(&self, guard: &Guard, out: &mut Vec<u32>) -> bool {
        let (s, verify) = self.snapshot_in(guard);
        out.extend_from_slice(s);
        verify
    }

    /// 中身を写す前で止める版: (publish 済みの slice, verify が要るか)。 3 段の順序は同じ (1 の 「観測」 は len の
    /// Acquire load で、 [0..n] は publish 済み = 不変なので、 中身を後で写しても (1) で写したのと同じ)。 slice は guard の
    /// 間は生きている。 いくつかの bucket を揃えて控えてから中身を読む時に使う (並びの索引の帯、 控える間だけを seqlock で
    /// 揃える)。
    #[inline]
    pub fn snapshot_in<'g>(&self, guard: &'g Guard) -> (&'g [u32], bool) {
        let p1 = self.backing.load(Ordering::Acquire, guard);
        // SAFETY: backing は常に非 null。
        let b = unsafe { p1.deref() };
        let n = b.len.load(Ordering::Acquire);
        // SAFETY: [0..n] は publish 済み = 不変。 guard が backing を生存させる。
        let s = unsafe { b.published(n) };
        let f = self.removed.load(Ordering::Acquire);
        let p2 = self.backing.load(Ordering::Acquire, guard);
        (s, f || p2 != p1)
    }

    /// [`snapshot_in`](Self::snapshot_in) に、 slice の頭の昇順の長さ (書き手が足すたびに持っている数、 slice の長さ以下) を
    /// 足したもの。 読み手は頭を探さずに、 頭はそのまま・後ろだけを並べて混ぜられる (`HimoStore` の 1 件ずつ出す読み)。
    #[inline]
    pub fn snapshot_sorted_in<'g>(&self, guard: &'g Guard) -> (&'g [u32], usize, bool) {
        let p1 = self.backing.load(Ordering::Acquire, guard);
        // SAFETY: backing は常に非 null。
        let b = unsafe { p1.deref() };
        let n = b.len.load(Ordering::Acquire);
        let head = b.sorted.load(Ordering::Relaxed).min(n);
        // SAFETY: [0..n] は publish 済み = 不変。 guard が backing を生存させる。
        let s = unsafe { b.published(n) };
        let f = self.removed.load(Ordering::Acquire);
        let p2 = self.backing.load(Ordering::Acquire, guard);
        (s, head, f || p2 != p1)
    }

    /// 現在の publish 済み件数（lock-free）。
    pub fn len(&self) -> usize {
        let guard = epoch::pin();
        let cur = self.backing.load(Ordering::Acquire, &guard);
        unsafe { cur.deref() }.len.load(Ordering::Acquire)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 現在の backing 容量（slot 数、 pow2 で切り上がる）。 メモリ会計用。
    /// append-only なので eid は 1 度だけ載る = capacity は published len の pow2 上界。
    pub fn capacity(&self) -> usize {
        let guard = epoch::pin();
        let cur = self.backing.load(Ordering::Acquire, &guard);
        unsafe { cur.deref() }.cap()
    }
}

impl Default for AppendBucket {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for AppendBucket {
    fn drop(&mut self) {
        // 排他アクセス（drop）なので unprotected で current backing を直接解放。
        // 過去 realloc の defer_destroy 分は crossbeam collector が回収する。
        let cur = self.backing.load(Ordering::Relaxed, unsafe { epoch::unprotected() });
        if !cur.is_null() {
            // SAFETY: 他 thread からの参照なし（drop = 単独所有）。
            unsafe {
                drop(cur.into_owned());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn basic_push_read() {
        let b = AppendBucket::new();
        assert!(b.is_empty());
        for i in 0..100u32 {
            b.push(i);
        }
        assert_eq!(b.len(), 100);
        let v = b.read_to_vec();
        assert_eq!(v, (0..100).collect::<Vec<_>>());
    }

    #[test]
    fn realloc_growth() {
        let b = AppendBucket::new();
        // miri は ~1000x 遅い。 1,000 push でも doubling (cap 4 起点) を 8 回踏むので
        // 成長 path の UB 検証には十分。
        let n = if cfg!(miri) { 1_000u32 } else { 10_000u32 };
        for i in 0..n {
            b.push(i);
        }
        assert_eq!(b.len(), n as usize);
        assert_eq!(b.read_to_vec(), (0..n).collect::<Vec<_>>());
    }

    #[test]
    fn with_read_zero_copy() {
        let b = AppendBucket::new();
        for i in 0..50u32 {
            b.push(i * 2);
        }
        let guard = epoch::pin();
        let sum: u64 = b.with_read(&guard, |s| s.iter().map(|&x| x as u64).sum());
        assert_eq!(sum, (0..50).map(|i| (i * 2) as u64).sum());
    }

    /// request12 P2: compact_in が stale を落とし、dup を 1 本化し、flag を戻す。
    /// (miri 対象 — compact_in の unsafe を Tree Borrows で検証する入口)
    #[test]
    fn compact_filters_and_dedups() {
        let b = AppendBucket::new();
        for i in 0..100u32 {
            b.push(i);
            b.live_inc();
        }
        // 前半 50 個が stale になった想定
        for _ in 0..50 {
            b.note_stale();
        }
        assert!(b.needs_verify());
        assert_eq!(b.live(), 50);

        let guard = epoch::pin();
        let n = b.compact_in(&guard, |e| e >= 50);
        assert_eq!(n, 50);
        assert!(!b.needs_verify(), "compact 後は fast path に戻る");
        assert_eq!(b.live(), 50);
        assert_eq!(b.read_to_vec(), (50..100).collect::<Vec<_>>());
        assert!(b.capacity() <= 64, "backing が縮んでいない: cap {}", b.capacity());

        // dup (re-tie 往復痕) は 1 本だけ残る
        let d = AppendBucket::new();
        for e in [5u32, 5, 7, 5] {
            d.push(e);
        }
        let n = d.compact_in(&guard, |_| true);
        assert_eq!(n, 2);
        assert_eq!(d.read_to_vec(), vec![5, 7]);
        // 組み直すと eid の昇順になる (前は最初に現れた順のまま = [9, 5, 3, 7])
        let o = AppendBucket::new();
        for e in [9u32, 5, 9, 3, 5, 7] {
            o.push(e);
        }
        assert_eq!(o.compact_in(&guard, |_| true), 4);
        assert_eq!(o.read_to_vec(), vec![3, 5, 7, 9]);
    }

    /// 組み直した bucket は eid の昇順で、 中身は前と同じ (keep の通る eid を 1 本ずつ)。 bucket の形は、 組んだ時の昇順の後ろに
    /// 書き換えで入ってきた順番外の entry と re-tie の重複が続く形。 負の対照: 組み直す前の bucket は昇順でない。
    #[test]
    fn compact_sorts_by_eid() {
        let mut rng = 0x2545_f491_4f6c_dd1du64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let guard = epoch::pin();
        let mut unsorted_before = 0;
        let n_cases = if cfg!(miri) { 14 } else { 700 };
        for case in 0..n_cases {
            let n = [1usize, 2, 63, 64, 65, 300, 1000][case % 7];
            let b = AppendBucket::new();
            let mut pushed = Vec::with_capacity(n);
            for i in 0..n {
                let e = if i < n / 2 { i as u32 * 3 } else { (next() % (3 * n as u64 + 7)) as u32 };
                b.push(e);
                b.live_inc();
                pushed.push(e);
            }
            unsorted_before += usize::from(!b.read_to_vec().is_sorted());
            let keep = |e: u32| e % 7 != 3;
            let want: std::collections::BTreeSet<u32> = pushed.iter().copied().filter(|&e| keep(e)).collect();
            let got_n = b.compact_in(&guard, keep);
            assert_eq!(got_n, want.len(), "case {case}");
            assert_eq!(b.read_to_vec(), want.iter().copied().collect::<Vec<_>>(), "case {case}: 中身か並びが違う");
            assert_eq!(b.live() as usize, want.len());
            assert!(!b.needs_verify());
        }
        assert!(unsorted_before >= n_cases / 2, "負の対照: 組み直す前の bucket が昇順でない回が少ない ({unsorted_before})");
    }

    /// `sort_appended` は `sort_unstable` と同じ並びを返す (昇順・後ろだけ順番外・全部ばらばら・重複・空・1 つ・塊 (256 個) を
    /// またぐ長さ)。 負の対照: 混ぜずに後ろだけ並べた形は昇順にならない
    #[test]
    fn sort_appended_matches_sort_unstable() {
        let mut r = 0x2545_F491_4F6C_DD1Du64;
        let mut next = move || {
            r ^= r << 13;
            r ^= r >> 7;
            r ^= r << 17;
            r
        };
        let mut caught = false;
        let n_cases = if cfg!(miri) { 40 } else { 3000 };
        for case in 0..n_cases {
            let n = (next() % if cfg!(miri) { 300 } else { 2000 }) as usize;
            let mut v: Vec<u32> = (0..n).map(|_| (next() % 500) as u32).collect();
            match case % 4 {
                0 => v.sort_unstable(),
                1 => {
                    let k = (next() as usize) % (n + 1);
                    v[..k].sort_unstable();
                }
                2 => {
                    // 前の昇順の後ろに、 少しだけ順番外 (書き換えの形)
                    let k = n - n / 10;
                    v[..k].sort_unstable();
                }
                _ => {}
            }
            let mut want = v.clone();
            want.sort_unstable();
            let mut got = v.clone();
            sort_appended(&mut got);
            assert_eq!(got, want, "case {case}");
            if let Some(k) = v.windows(2).position(|w| w[0] > w[1]).map(|i| i + 1) {
                let mut bad = v.clone();
                bad[k..].sort_unstable();
                caught |= !bad.is_sorted();
            }
        }
        assert!(caught, "負の対照: 混ぜない形がいつも昇順になった (比べ方が弱い)");
    }

    /// 書き手が持つ頭の昇順の長さ (`snapshot_sorted_in` の 2 つ目) が、 探した長さ (`sorted_prefix_len`) といつも同じ:
    /// 昇順に足す・同じ値を続けて足す・前より小さい値を足す (その後は伸びない)・backing を倍にする境目 (4, 8, 16, ..)・
    /// 詰め直し (`compact_in`、 昇順に並べ直すので全部) の後。 空の bucket は 0。
    #[test]
    fn sorted_head_is_kept_by_the_writer() {
        let mut r = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            r ^= r << 13;
            r ^= r >> 7;
            r ^= r << 17;
            r
        };
        let guard = epoch::pin();
        let head = |b: &AppendBucket| {
            let (s, h, _) = b.snapshot_sorted_in(&guard);
            (h, sorted_prefix_len(s), s.len())
        };
        let mut broke = 0;
        let n_cases = if cfg!(miri) { 12 } else { 400 };
        for case in 0..n_cases {
            let b = if case % 2 == 0 { AppendBucket::new() } else { AppendBucket::with_capacity(5) };
            let (h, want, _) = head(&b);
            assert_eq!((h, want), (0, 0), "case {case}: 空");
            let n = 1 + (next() % if cfg!(miri) { 40 } else { 300 }) as usize;
            let mut last = 0u32;
            for i in 0..n {
                // 多くは昇順 (同じ値も)、 時々前より小さい値
                let e = match next() % 10 {
                    0 => (next() % 1000) as u32,
                    1 => last,
                    _ => last + (next() % 5) as u32,
                };
                last = last.max(e);
                b.push(e);
                let (h, want, len) = head(&b);
                assert_eq!(h, want, "case {case} の {i} 件目 (len {len})");
                broke += usize::from(want < len);
            }
            // 詰め直し: 偶数だけ残す。 並べ直すので頭は全部
            b.compact_in(&guard, |e| e % 2 == 0);
            let (h, want, len) = head(&b);
            assert_eq!((h, want), (len, len), "case {case}: 詰め直しの後");
            // 詰め直しの後に足しても同じ
            for _ in 0..8 {
                b.push((next() % 2000) as u32);
                let (h, want, _) = head(&b);
                assert_eq!(h, want, "case {case}: 詰め直しの後に足した");
            }
        }
        assert!(broke > 0, "対照: 頭が崩れる場合が無かった (順番外を足していない)");
    }

    /// 書き手 1 本が順番外も混ぜて足す間、 読み手 4 本が控えた slice の頭 (書き手が持つ長さ) がいつも昇順。
    #[test]
    fn concurrent_readers_see_a_sorted_head() {
        let b = Arc::new(AppendBucket::new());
        let stop = Arc::new(AtomicBool::new(false));
        let n = if cfg!(miri) { 200u32 } else { 100_000u32 };
        let readers: Vec<_> = (0..4)
            .map(|_| {
                let b = b.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let mut seen = 0usize;
                    while !stop.load(Ordering::Relaxed) {
                        let guard = epoch::pin();
                        let (s, h, _) = b.snapshot_sorted_in(&guard);
                        assert!(h <= s.len() && s[..h].is_sorted(), "頭 {h} が昇順でない (len {})", s.len());
                        seen = seen.max(h);
                    }
                    seen
                })
            })
            .collect();
        // 頭は n * 3 / 4 まで伸び、 その後に小さい値を 1 つ足して止まる (後ろは昇順でも頭は伸びない)
        for i in 0..n {
            b.push(if i == n * 3 / 4 { 0 } else { i + 1 });
        }
        stop.store(true, Ordering::Relaxed);
        for r in readers {
            r.join().unwrap();
        }
        let guard = epoch::pin();
        let (s, h, _) = b.snapshot_sorted_in(&guard);
        assert_eq!((h, s.len()), ((n * 3 / 4) as usize, n as usize));
    }

    /// 1 writer + N reader 並行: 破損なし、 len 単調非減少、 全要素 valid。
    #[test]
    fn concurrent_writer_readers() {
        let b = Arc::new(AppendBucket::new());
        let stop = Arc::new(AtomicBool::new(false));
        // miri は ~1000x 遅い + UB 検出が目的なので少回数で interleaving を突く。
        let n = if cfg!(miri) { 300u32 } else { 200_000u32 };

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let b = b.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let mut prev = 0usize;
                    let mut iters = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        let v = b.read_to_vec();
                        // append-only なので len は単調非減少
                        assert!(v.len() >= prev, "len regressed {} -> {}", prev, v.len());
                        prev = v.len();
                        // 内容は push した値そのもの（i 番目 == i）
                        for (i, &x) in v.iter().enumerate() {
                            assert_eq!(x, i as u32, "corruption at idx {i}");
                        }
                        iters += 1;
                    }
                    iters
                })
            })
            .collect();

        // 単一 writer
        for i in 0..n {
            b.push(i);
        }
        stop.store(true, Ordering::Relaxed);
        for r in readers {
            r.join().unwrap();
        }

        assert_eq!(b.len(), n as usize);
        let v = b.read_to_vec();
        assert_eq!(v, (0..n).collect::<Vec<_>>());
    }
}
