//! `AppendBucket` — 書き手 1 本 (値の鍵で守る) / 多 reader の lock-free append-only 列。 #95。
//!
//! `BucketCylinder` の各 bucket（value → eid 列）の格納をこれに置き換える土台。
//! 現状の `RwLock<BucketCylinder>` は read が write に相互排他になり、 長い read が
//! write を stall させる（#95）。 この型は:
//!
//! - **read は完全 lock-free**（writer を一切待たない）。 crossbeam-epoch で pin。
//! - **write は値の鍵で bucket ごとに直列**（append O(1) amortized。 前は consumer 1 本の前提だった）。
//! - 一度 publish した要素は二度と動かさない（append-only）ので、 read は published
//!   範囲 `[0..len]` を安全に `&[u32]` として読める。
//! - 容量超過時のみ backing を倍化し、 旧 backing は epoch で全 reader 通過後に解放。
//!
//! ## 並行契約
//! - `push` / `compact_in` は **同時に 1 thread のみ**。 守るのは値ごとの鍵 (`HimoStore` の `value_locks`、 0.32.1 の #467。
//!   円柱を組む前の書き手・sparse・配列の伸長は `write_lock`、 組む時は `write_guard`)。 鍵を持てば読み手でもよい:
//!   読み直しの詰め直し (#520、 `HimoStore::compact_if_reread`) は読み手が鍵を取って `compact_in` を呼ぶ。
//!   鍵なしの複数 writer は未対応（race する）。
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

/// 空の bucket に初めて足す時の backing の大きさ。
pub(crate) const INITIAL_CAP: usize = 4;

/// 単一の連続 backing。 `data[0..len]` が publish 済み（不変）、 `data[len..cap]` は spare。
struct Buf {
    data: Box<[UnsafeCell<u32>]>,
    len: AtomicUsize,
    /// `data[i]` の entity がこの値を出ていったら bit `i` を立てる (書き手が置いた位置を覚えていて、 書き換え・消しの時に
    /// 立てる)。 読み手は bit の立っていない entry を写すだけ (Column を読まない)。
    stale: Box<[AtomicU64]>,
    /// この backing の古い entry が全部印されているか。 書き手が位置を知らずに古くすると false (次に足す前に下げるので、
    /// 足した分の見える読み手には下がったのも見える)。 組み直した backing は true。 bucket ごとでなく backing ごとに持つ (読み手は
    /// 控えた backing の flag を読む: 組み直しの前の backing を控えた読み手が、 組み直した後の true を見ないように)。
    exact: AtomicBool,
}

// SAFETY: published 範囲 [0..len] は書き換わらない。 writer は spare slot [len] のみ書く。
// reader は published 範囲のみ読む。 disjoint index への同時アクセスで、 可視化は len の
// Release/Acquire で順序付けられる。
unsafe impl Sync for Buf {}

impl Buf {
    fn with_cap(cap: usize) -> Self {
        Self::from_vec(Vec::new(), cap)
    }

    /// `v` を頭に置いた容量 `cap` (`v.len()` 以上) の backing。 `v` は昇順であること (詰め直しの出力。 読み手は昇順を前提に
    /// しないが、 詰め直した直後の bucket が eid の順で読めることを `compact_sorts_by_eid` が確かめる)。
    /// 空きは 0 で埋める (`vec![0; cap]` は確保の時に 0 の page をもらうだけ)。 古い印は無し、 印は全部そろっている (true)。
    fn from_vec(v: Vec<u32>, cap: usize) -> Self {
        debug_assert!(v.len() <= cap && v.is_sorted());
        let len = v.len();
        let data: Box<[u32]> = if len == 0 {
            vec![0u32; cap].into_boxed_slice()
        } else {
            let mut v = v;
            v.resize(cap, 0);
            v.into_boxed_slice()
        };
        // SAFETY: UnsafeCell<u32> は u32 と同じ表現 (repr(transparent))。 長さはそのまま移る
        let data = unsafe { Box::from_raw(Box::into_raw(data) as *mut [UnsafeCell<u32>]) };
        Self {
            data,
            len: AtomicUsize::new(len),
            stale: (0..cap.div_ceil(64)).map(|_| AtomicU64::new(0)).collect(),
            exact: AtomicBool::new(true),
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
    /// request12: 値の鍵を持つ側 (HimoStore の書き手、 `value_locks` 下) が旧値の decrement / 新値の
    /// increment を行う (詰め直しは残った数で置き直す)。read の fast-path 判定と統計・planner の pivot 選択に使う。
    live: AtomicU32,
    /// この bucket に stale (値更新/削除の残骸) が居るか。true なら read は
    /// Column verify + dedup を通す。himo 全体 flag (旧 any_removed) の bucket 局所版。
    removed: AtomicBool,
    /// 最後に照らした読みの時の (len, live) (`len << 32 | live`、 `u64::MAX` = まだ)。 [`mark_verified`](Self::mark_verified)。
    verified: AtomicU64,
    /// 間に書き込みの無い照らしを繰り返した回数 (1 回目の読みの後の読み直しの数)。
    rereads: AtomicU32,
}

// SAFETY: 書き手は値の鍵で 1 本 / 多 reader。 全アクセスは atomic len + epoch で同期される。
unsafe impl Sync for AppendBucket {}
unsafe impl Send for AppendBucket {}

/// `s` のうち、 `marks` の bit (位置 i が bit i) が立っていない entry を `out` に足す (古い印の付いた entry を飛ばす)。
/// 64 個ずつ印の語を読み、 印の無い塊はそのまま写す。
pub(crate) fn copy_unmarked(s: &[u32], marks: &[AtomicU64], out: &mut Vec<u32>) {
    let start = out.len();
    out.reserve(s.len());
    let mut w = start;
    // SAFETY: 書く先は reserve した中 (w は start + 読んだ数を超えない)。 最後に w に長さを合わせる
    unsafe {
        let dst = out.as_mut_ptr();
        for (k, chunk) in s.chunks(64).enumerate() {
            let m = marks[k].load(Ordering::Acquire);
            if m == 0 {
                std::ptr::copy_nonoverlapping(chunk.as_ptr(), dst.add(w), chunk.len());
                w += chunk.len();
                continue;
            }
            // 分かれ道なしで詰める: 毎回書いて、 印が無ければ先へ進む
            for (i, &e) in chunk.iter().enumerate() {
                *dst.add(w) = e;
                w += ((m >> i) & 1 == 0) as usize;
            }
        }
        out.set_len(w);
    }
}

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
/// 256 個の塊ごとに `is_sorted` で飛ばしてから探す ([`sort_appended`] と、 `HimoStore` の古い entry のある bucket の範囲の読み
/// (`push_live_once`) が使う)。 書き手は頭の長さを持たない (#558。 0.33.0〜0.35.0 は足すたびに持っていたが、 #481 の後は値の
/// 順にたどる読みが使わず、 範囲の読みだけが使うので、 読む時に探す: 昇順の塊は 256 個ごとに飛ぶので確かめの読みより軽い)。
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
    sort_eids(&mut b);
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

/// これより短い列は比較 sort ([`sort_eids`])。 512 件で基数 sort が並び、 それより長いと 2〜4 倍速い
/// (1 件あたり: 1024 件 4.3 → 1.9 ns、 2.5 万件 6.9 → 1.7 ns、 eid が 17 bit の時)。
const RADIX_MIN: usize = 512;

/// eid の列を昇順にする。 長い列は基数 sort: 最小値からの差を 11 bit 以下の同じ幅の桁に分け、 下の桁から安定に振り分ける
/// (桁の数は差の bit 数で決まる。 eid は表の中で近い所に集まるので、 2 桁で済むことが多い)。 比較 sort は列が長いほど
/// 1 件の段数 (log n) が増え、 書き換えの多い列の詰め直しで整列が書き手の時間の 15% を占めていた。
fn sort_eids(v: &mut Vec<u32>) {
    if v.len() < RADIX_MIN {
        v.sort_unstable();
        return;
    }
    let (min, max) = v.iter().fold((u32::MAX, 0), |(lo, hi), &x| (lo.min(x), hi.max(x)));
    let bits = 32 - (max - min).leading_zeros();
    if bits == 0 {
        return; // 全部同じ値
    }
    let passes = bits.div_ceil(11);
    let w = bits.div_ceil(passes);
    let mask = (1u32 << w) - 1;
    let mut buf = vec![0u32; v.len()];
    let mut count = [0u32; 1 << 11];
    for p in 0..passes {
        let shift = p * w;
        let count = &mut count[..1 << w];
        count.fill(0);
        for &x in v.iter() {
            count[(((x - min) >> shift) & mask) as usize] += 1;
        }
        let mut at = 0;
        for c in count.iter_mut() {
            let n = *c;
            *c = at;
            at += n;
        }
        for &x in v.iter() {
            let d = &mut count[(((x - min) >> shift) & mask) as usize];
            buf[*d as usize] = x;
            *d += 1;
        }
        std::mem::swap(v, &mut buf);
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
    // live/removed を更新するのは値の鍵を持つ 1 thread だけ (書き手の set / remove と、 #520 の後は読み手の読み直しの
    // 詰め直し `compact_in` も。 排他は `value_locks`、 write_lock ではない)。read 側の
    // verify 要否は `read_marked_verify` の 3 段プロトコル (slice → flag →
    // backing ptr 再検証) で判定する。flag 単独の前読み/後読みはどちらも不健全:
    // - 前読み: flag=false load 後に積まれた churn 痕 (往復 churn の dup) を
    //   fast path が素通しする (PR #103 レビューで実証、再現テストあり)
    // - 後読み: compaction の flag リセットと交差すると旧 backing の stale を
    //   flag=false で素通しする
    // Column 側の無同期読みは既知の別 issue #100。

    /// live +1。戻り値は増分前 (0 なら「live な値が初めて入った」= unique 判定)。 live を動かすのは bucket の書き手 1 本
    /// (`push_in` と同じ) なので、 読んで書くだけ (不可分の足し算は要らない)。
    #[inline]
    pub fn live_inc(&self) -> u32 {
        let v = self.live.load(Ordering::Relaxed);
        self.live.store(v + 1, Ordering::Relaxed);
        v
    }

    /// stale 化: removed flag を立てて live -1。戻り値は減分前の live。
    /// live == 0 のとき (契約違反: Column に無い値の stale 通知) は何もせず None。 書き手 1 本 ([`live_inc`](Self::live_inc))。
    #[inline]
    pub fn note_stale(&self) -> Option<u32> {
        self.removed.store(true, Ordering::Release);
        let v = self.live.load(Ordering::Relaxed);
        let after = v.checked_sub(1)?;
        self.live.store(after, Ordering::Relaxed);
        Some(v)
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

    /// 値の鍵を持つ 1 thread の append。 spare があれば in-place、 満杯なら倍化 + epoch 遅延解放。
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
        let l = b.len.load(Ordering::Relaxed); // 鍵を持つ 1 thread だけが len を進めるので自身の len は Relaxed で可
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
            // 古い印も写す (印を立てるのはこの bucket の書き手 1 本 = 自分なので、 写す間に増えない)
            for (w, x) in b.stale.iter().enumerate() {
                nb.stale[w].store(x.load(Ordering::Relaxed), Ordering::Relaxed);
            }
            nb.exact.store(b.exact.load(Ordering::Relaxed), Ordering::Relaxed);
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
    /// (request12 P2 = incremental compaction)。値の鍵を持つ 1 thread (書き手か、 #520 の読み直しの詰め直しをする読み手) 前提。
    ///
    /// - 同一 eid の重複 slot (re-tie 往復痕) は 1 本だけ残し、 残した entry は eid の昇順に並べ直す (dedup)
    /// - 旧 backing は epoch で全 reader 通過後に解放 — reader は停止しない
    /// - swap 後に live を確定し、removed flag を **backing swap の後で** false に
    ///   戻す (Release)。reader は flag を Acquire で slice より先に読むので、
    ///   flag=false を見た reader は必ず新 backing を見る。
    ///
    /// 戻り値は compaction 後の live 数。
    pub fn compact_in(&self, guard: &Guard, keep: impl FnMut(u32) -> bool) -> usize {
        self.compact_in_placed(guard, keep, |_, _| {})
    }

    /// [`compact_in`](Self::compact_in) の、 残した entry の新しい位置を `placed(eid, 位置)` で知らせる版 (書き手が位置の表を
    /// 書き直す)。
    pub fn compact_in_placed(&self, guard: &Guard, mut keep: impl FnMut(u32) -> bool, mut placed: impl FnMut(u32, usize)) -> usize {
        let cur = self.backing.load(Ordering::Acquire, guard);
        // SAFETY: backing は常に非 null。
        let b = unsafe { cur.deref() };
        let l = b.len.load(Ordering::Relaxed); // 鍵を持つ 1 thread だけが len を進めるので自身の len は Relaxed で可
        let src = unsafe { b.published(l) };
        // 分岐なしで照らす: 先に置いてから、 残すなら次へ進む (残す / 落とすは書き換えの順に入り混じるので、 分岐は半分外れる)
        let mut kept = vec![0u32; src.len()];
        let mut n = 0;
        for &e in src {
            kept[n] = e;
            n += usize::from(keep(e));
        }
        kept.truncate(n);
        let kept = dedup_sorted(kept);
        let live = kept.len();
        // 残した entry の新しい位置を書き手に知らせる (位置の表を書き直す)
        for (i, &e) in kept.iter().enumerate() {
            placed(e, i);
        }

        // 昇順の列をそのまま backing にする (まだ誰も見ていない。 古い印は無し、 印はそろっている)
        let nb = Buf::from_vec(kept, live.next_power_of_two().max(INITIAL_CAP));
        self.backing.store(Owned::new(nb), Ordering::Release);
        // SAFETY: 旧 backing は全 reader が epoch を通過してから解放。
        unsafe {
            guard.defer_destroy(cur);
        }
        self.live.store(live as u32, Ordering::Relaxed);
        self.verified.store(u64::MAX, Ordering::Relaxed);
        self.rereads.store(0, Ordering::Relaxed);
        self.removed.store(false, Ordering::Release); // swap 後 — 順序契約はメソッド doc 参照
        live
    }

    /// 位置 `p` の entry (`eid` のはず) を古いと印す (この bucket の書き手 1 本で)。 位置を知らない (`None`) か、 そこに
    /// `eid` が無ければ印せないので、 この bucket の印は不完全になる (読みは Column で確かめる)。
    pub fn mark_stale(&self, at: Option<(usize, u32)>, guard: &Guard) {
        let b = unsafe { self.backing.load(Ordering::Acquire, guard).deref() };
        match at {
            // 位置の表は書き手が足す時と組み直す時に書くので、 その位置には eid がある (中身は読まない: 古い bucket の中身は
            // 乱れた位置への読みになる)
            Some((p, eid)) if p < b.len.load(Ordering::Relaxed) => {
                debug_assert_eq!(unsafe { *b.data[p].get() }, eid, "位置の表が bucket と合わない");
                let _ = eid;
                // 印を立てるのはこの bucket の書き手 1 本 (値の鍵の下) なので、 atomic の RMW でなく読んでから書く
                let w = &b.stale[p / 64];
                w.store(w.load(Ordering::Relaxed) | (1u64 << (p % 64)), Ordering::Release);
            }
            _ => b.exact.store(false, Ordering::Release),
        }
    }

    /// [`snapshot_in`](Self::snapshot_in) に、 古い印の列と 「印が全部か」 を足した版。
    pub fn snapshot_marked_in<'g>(&self, guard: &'g Guard) -> (&'g [u32], &'g [AtomicU64], bool, bool) {
        let p1 = self.backing.load(Ordering::Acquire, guard);
        // SAFETY: backing は常に非 null。
        let b = unsafe { p1.deref() };
        let n = b.len.load(Ordering::Acquire);
        // len の後に読む: 印の付かない古い entry の後に足した分が [0..n] に見えるなら、 その前に下げた flag も見える
        let exact = b.exact.load(Ordering::Acquire);
        // SAFETY: [0..n] は publish 済み = 不変。 guard が backing を生存させる。
        let s = unsafe { b.published(n) };
        let f = self.removed.load(Ordering::Acquire);
        let p2 = self.backing.load(Ordering::Acquire, guard);
        (s, &b.stale, f || p2 != p1, exact)
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

    /// 控えた slice を写す読み ([`snapshot_marked_in`](Self::snapshot_marked_in) の 3 段: slice → flag → backing の再検証) で、 古い印が全部そろっていれば印の立った entry を飛ばして
    /// 写す版。 返すのは `(写し, Column の確かめが要るか, 古い entry があるか)`: 印がそろっていれば確かめは要らない (false) が
    /// 古い entry はある (true。 読み直しの詰め直しはこちらで数える)。 印が欠けていれば前と同じ (全部写して両方 true)。
    /// `use_marks` が false (印を持たない列) なら印を見ない (古い entry があれば全部写して確かめを求める)。
    pub fn read_marked_verify(&self, guard: &Guard, use_marks: bool) -> (Vec<u32>, bool, bool) {
        let (s, marks, stale, exact) = self.snapshot_marked_in(guard);
        if stale && exact && use_marks {
            let mut v = Vec::with_capacity(s.len());
            copy_unmarked(s, marks, &mut v);
            (v, false, true)
        } else {
            (s.to_vec(), stale, stale)
        }
    }

    /// [`read_marked_verify`](Self::read_marked_verify) の、 呼び手の buffer の後ろに足す版。 返すのは
    /// `(Column の確かめが要るか, 古い entry があるか)`。
    pub fn read_marked_verify_into(&self, guard: &Guard, out: &mut Vec<u32>, use_marks: bool) -> (bool, bool) {
        let (s, marks, stale, exact) = self.snapshot_marked_in(guard);
        if stale && exact && use_marks {
            copy_unmarked(s, marks, out);
            (false, true)
        } else {
            out.extend_from_slice(s);
            (stale, stale)
        }
    }

    /// [`read_marked_verify`](Self::read_marked_verify) と同じ 3 段の、 印を見ない版 (呼び手の buffer の後ろに足す。 多くの bucket を
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

    /// 現在の publish 済み件数（lock-free）。
    pub fn len(&self) -> usize {
        self.len_in(&epoch::pin())
    }

    /// [`len`](Self::len) の外部 guard 版。
    #[inline]
    pub fn len_in(&self, guard: &Guard) -> usize {
        let cur = self.backing.load(Ordering::Acquire, guard);
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
        // 3000 は後ろに足した分 (1500) が基数 sort になる長さ (Miri では重いので外す)
        let ns: &[usize] = if cfg!(miri) { &[1, 2, 63, 64, 65, 300, 1000] } else { &[1, 2, 63, 64, 65, 300, 1000, 3000] };
        let n_cases = if cfg!(miri) { 14 } else { 800 };
        for case in 0..n_cases {
            let n = ns[case % ns.len()];
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
            let mut want: Vec<u32> = want.into_iter().collect();
            assert_eq!(b.read_to_vec(), want, "case {case}: 中身か並びが違う");
            assert_eq!(b.live() as usize, want.len());
            assert!(!b.needs_verify());
            // 組み直した backing の空き (容量まで) と、 その先の倍化を通って足す: 足した分は後ろに足した順で読める
            let cap = b.capacity();
            assert!(cap >= want.len() && cap.is_power_of_two(), "case {case}: 容量 {cap} / 中身 {}", want.len());
            for _ in 0..cap - want.len() + 3 {
                let e = (next() % (3 * n as u64 + 7)) as u32;
                b.push(e);
                want.push(e);
            }
            assert_eq!(b.read_to_vec(), want, "case {case}: 組み直した後に足した分");
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

    /// `sort_eids` は `sort_unstable` と同じ並びを返す: 基数 sort に切り替わる長さの前後 (511 / 512 / 513) と長い列、
    /// 値の幅 (最小値からの差の bit 数) が 1 桁・2 桁・3 桁になる所 (11 / 12 / 22 / 23 / 32 bit)、 最小値が大きく差が狭い列
    /// (上の bit が揃う)、 全部同じ値、 重複の多い列、 昇順・逆順。
    #[test]
    fn sort_eids_matches_sort_unstable() {
        let mut r = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            r ^= r << 13;
            r ^= r >> 7;
            r ^= r << 17;
            r
        };
        // Miri では基数 sort の長さ 1 つと、 全部同じ値・1 / 2 / 3 桁の幅・ばらばらと重複の多い列だけ
        let lens: &[usize] = if cfg!(miri) { &[513] } else { &[0, 1, 2, 100, 511, 512, 513, 1000, 4097, 30_000] };
        let shapes: &[u32] = if cfg!(miri) { &[0, 3] } else { &[0, 1, 2, 3] };
        let spans: &[u64] = if cfg!(miri) {
            &[0, (1 << 11) + 1, (1 << 22) + 5, u32::MAX as u64]
        } else {
            &[0, 1, 7, 1 << 11, (1 << 11) + 1, 1 << 22, (1 << 22) + 5, 1 << 31, u32::MAX as u64]
        };
        for &n in lens {
            for (si, &span) in spans.iter().enumerate() {
                for base in [0u32, 3_000_000_000] {
                    // 差が span 以下 (span + base が u32 に入る所まで)
                    let span = span.min(u32::MAX as u64 - base as u64);
                    for &shape in shapes {
                        let mut v: Vec<u32> = (0..n).map(|_| base + (next() % (span + 1)) as u32).collect();
                        match shape {
                            0 => {}
                            1 => v.sort_unstable(),
                            2 => v.sort_unstable_by(|a, b| b.cmp(a)),
                            // 重複の多い列 (同じ値を 8 回ずつ)
                            _ => {
                                let w = v.clone();
                                for (i, x) in v.iter_mut().enumerate() {
                                    *x = w[i / 8 * 8];
                                }
                            }
                        }
                        let mut want = v.clone();
                        want.sort_unstable();
                        sort_eids(&mut v);
                        assert_eq!(v, want, "n {n} span[{si}] {span} base {base} shape {shape}");
                    }
                }
            }
        }
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
