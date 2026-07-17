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
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};

const INITIAL_CAP: usize = 4;

/// 単一の連続 backing。 `data[0..len]` が publish 済み（不変）、 `data[len..cap]` は spare。
struct Buf {
    data: Box<[UnsafeCell<u32>]>,
    len: AtomicUsize,
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
}

// SAFETY: 単一 writer / 多 reader。 全アクセスは atomic len + epoch で同期される。
unsafe impl Sync for AppendBucket {}
unsafe impl Send for AppendBucket {}

impl AppendBucket {
    /// 空の bucket（初回 push まで heap 確保しない）。
    pub fn new() -> Self {
        Self {
            backing: Atomic::new(Buf::with_cap(0)),
            live: AtomicU32::new(0),
            removed: AtomicBool::new(false),
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
    /// - 同一 eid の重複 slot (re-tie 往復痕) は最初の 1 本だけ残す (dedup)
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
        let mut seen = std::collections::HashSet::with_capacity(self.live() as usize + 1);
        let kept: Vec<u32> = src
            .iter()
            .copied()
            .filter(|&e| keep(e) && seen.insert(e))
            .collect();

        let ncap = kept.len().next_power_of_two().max(INITIAL_CAP);
        let nb = Buf::with_cap(ncap);
        for (i, &v) in kept.iter().enumerate() {
            unsafe {
                *nb.data[i].get() = v;
            }
        }
        nb.len.store(kept.len(), Ordering::Relaxed); // まだ誰も見ていない
        self.backing.store(Owned::new(nb), Ordering::Release);
        // SAFETY: 旧 backing は全 reader が epoch を通過してから解放。
        unsafe {
            guard.defer_destroy(cur);
        }
        self.live.store(kept.len() as u32, Ordering::Relaxed);
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

    /// snapshot コピー（standalone 用、 内部で pin）。
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

        // dup (re-tie 往復痕) は最初の 1 本だけ残る
        let d = AppendBucket::new();
        for e in [5u32, 5, 7, 5] {
            d.push(e);
        }
        let n = d.compact_in(&guard, |_| true);
        assert_eq!(n, 2);
        assert_eq!(d.read_to_vec(), vec![5, 7]);
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
