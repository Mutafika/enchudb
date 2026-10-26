//! 球 (試作、 branch `proto/ball-history`): 宣言した紐の値の履歴。
//!
//! 球は entity ごとに 1 つ。 書き換えると回って別の面が上を向く (球はどこにも動かない)。 デッキ = その値の面を上にしている
//! 球の集まり (見え方)。 円柱の bucket は、 その球を見つけるための名簿 (master のまま: 回って外れた行は印か Column で飛ばし、
//! 清書 = 詰め直しで捨てる)。 履歴は 2 か所に書く:
//!
//! - **球の面** (`Face`): (eid, 値, 上を向いた時刻 `from`, 外れた時刻 `to`)。 同じ球の 1 つ前の面へ糸 (`prev`) を張る。
//!   「時点 t の値」 ([`Ball::value_at`]) は今の面から糸をたどる
//! - **デッキの差分の帳面**: 値ごとに 「時刻 t に球 X が入った / 出た」 を書き足す。 時刻は帳面の書き手の鍵の中で取るので、
//!   値ごとの差分の並びはそのまま時刻の順。 「時点 t にこの値だった球」 ([`Ball::pull_at`]) は、 今のデッキから t より後の差分を
//!   新しい方から逆に当てて作る (入った → 抜く、 出た → 足す。 最後に当たる = t より後の最初の差分が t の時の向きを決め、
//!   差分の無い球は今と同じ)。 手間は 「t より後の差分の数」 に比例
//!   - `LOG_DENSE` 未満の値は値ごとの帳面 (`DiffLog`)。 差分は時刻 / eid / 入った bit の列 3 本に分けて詰める: 戻す時に
//!     読むのは eid と bit (1 差分 4 B + 1 bit)、 時刻は t の二分探索だけが読む (1 つの構造体 16 B に並べていた時は、 真ん中の
//!     時点の読みの大半が cache の外から差分を読む分だった)
//!   - それより大きい値 (時刻 / ID の列: 1 つの値の差分が 1〜2 個) は値ごとに帳面を作らず、 値の下位 bit で分けた
//!     `WIDE` 本の列 (`WideStripe`) に足し、 同じ値の 1 つ前の差分へ糸を張る。 列への書き足し (値 → 糸の頭の表を引く) は
//!     名簿の鍵の**外**で済ませる: 名簿の鍵を取る前に列の lock を取って場所を取り ([`Ball::reserve`]、 時刻は未定)、
//!     名簿の鍵の中では時刻を書き込むだけ。 列の lock は名簿を書き換え終わるまで持つ (読み手は列の lock の下で読むので、
//!     時刻の未定の差分は見えない)。 sparse の値の名簿の鍵は紐 1 本の write_lock なので、 その中で表を引くと書き手が並ぶ
//!     (書き手 4 本で書き換え 1 回 +950 ns だった)
//!
//! 同じ値の書き直しは球を回さない (面も差分も足さない)。 外す (untie / delete) は今の面を閉じ、 「出た」 を書く。
//!
//! ## 順序
//! 値ごとの帳面の書き手 1 本の約束は、 名簿 (円柱) の書き手の鍵がそのまま守る: engine は行の lock の下で
//! `HimoStore::set_noting` / `remove_noting` を呼び、 HimoStore が古い値と新しい値の名簿の鍵 (値の鍵か write_lock) を
//! 取った中で、 名簿と列を書き換える**前に** [`Ball::note`] が時刻を取って帳面に書き足す (len を Release)。 鍵を離した後、
//! 行の lock の下で [`Ball::turn`] が球を回す。 同じ値に触る書き手は、 その値が配列の中ならその値の鍵、 sparse なら write_lock
//! を必ず持ち、 配列を伸ばす書き手は write_lock と全部の鍵を持つので、 同じ値の帳面の書き手は 1 本ずつ並ぶ (帳面の並び = 時刻の順)。
//! 大きい値の列は、 同じ値の書き手が列の lock を場所を取ってから時刻を書くまで持つので、 糸の並び = 時刻の順。 鍵の順は
//! 列の lock (番号の小さい方から) → 名簿の鍵。
//! 宣言 ([`Balls::declare`]) は円柱を先に組む (組んでいない紐の書き手は鍵を取らない。 組んだ印は戻らない)。
//!
//! 読み手は今のデッキを先に読み、 その後で帳面を読む (Acquire)。 読み手が名簿の変化を見たなら、 その前に書いた差分も見える。
//! 名簿の変化をまだ見ていない時に差分だけ見えても、 入った / 出たを集合として当てるので答えは同じ。
//!
//! ## 試作の範囲
//! - メモリの上だけ (開き直すと消える)。 宣言も保存しない
//! - 時刻は engine の中の通し番号 ([`Balls::now`])。 HLC ではない。 宣言の前からある値の面は `from = 0` (「前から」、 差分は書かない)
//! - 宣言 ([`Balls::declare`]) は書き手の居ない時に
//! - 球は eid ごと: delete は今の面を閉じるだけで、 同じ eid を使い回した次の entity の面は前の entity の面の続きに付く
//! - 読みが時刻 t = 今 の時、 同時に書いている entity は前の向きに見えうる。 過去の t は、 その時刻までの書き込みが返った後なら揺れない
//! - 大きい値の列は lock で読む (読む間、 同じ列に足す書き手は待つ)

use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned};
use std::cell::{Cell, UnsafeCell};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock};

/// 糸の終わり。
const NONE: u32 = u32::MAX;
/// まだ上を向いている面の `to`。
pub const OPEN: u64 = u64::MAX;

const CHUNK_BITS: u32 = 16;
const CHUNK: usize = 1 << CHUNK_BITS;
/// 帳面を配列で引く値の上限 (2^20、 円柱の dense と同じ)。 上は `WIDE` 本の列。
const LOG_DENSE: u64 = 1 << 20;
const WIDE: usize = 64;
/// 書き手ごとに面の場所をまとめて取る数。
const BLOCK: u64 = 64;
/// 控えの間隔の下限 (差分の数)。 間隔は max(これ, 前の控えの大きさ) = 控えは 1 差分あたり 4 B 以下。
const SNAP_MIN: usize = 1024;
/// 時点 t の読みで、 t より後の差分がこの数以上なら eid の bit の表で戻す (少なければ並べて合わせる方が速い)。
const BITS_MIN_AFTER: usize = 1024;
/// bit の表の語の数が (今のデッキ + 差分) のこの倍までなら表を使う (eid が広く散る時は並べて合わせる)。
const BITS_MAX_SPREAD: usize = 8;

/// 球に付ける通し番号 (書き手ごとの面の場所の塊がどの球の物かを見分ける。 番地は使い回されるので使わない)。
static BALL_IDS: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// この thread が取った面の場所の塊: (球の番号, 次, 終わり)。 球 8 つまで。 追い出した塊の残りは使わない場所になる。
    static BLOCKS: Cell<[(u64, u32, u32); 8]> = const { Cell::new([(0, 0, 0); 8]) };
    /// 時点 t の読みの bit の表 (使い回す)。
    static BITS: Cell<Vec<u64>> = const { Cell::new(Vec::new()) };
}

/// u32 の添字で引く、 塊ごとに初めて触った時に取る配列。 読み手は取らない (無い塊は空として読む)。
struct Chunked<T> {
    chunks: Box<[OnceLock<Box<[T]>>]>,
}

impl<T: Default> Chunked<T> {
    /// 添字 `n` 未満を覆う。
    fn new(n: u64) -> Self {
        Self { chunks: (0..n.div_ceil(CHUNK as u64)).map(|_| OnceLock::new()).collect() }
    }

    #[inline]
    fn get(&self, i: u32) -> Option<&T> {
        let c = self.chunks.get((i >> CHUNK_BITS) as usize)?.get()?;
        Some(&c[i as usize & (CHUNK - 1)])
    }

    #[inline]
    fn get_or_alloc(&self, i: u32) -> &T {
        let c = self.chunks[(i >> CHUNK_BITS) as usize].get_or_init(|| (0..CHUNK).map(|_| T::default()).collect());
        &c[i as usize & (CHUNK - 1)]
    }

    fn allocated(&self) -> usize {
        self.chunks.iter().filter(|c| c.get().is_some()).count()
    }

    fn for_each(&self, mut f: impl FnMut(&T)) {
        for c in self.chunks.iter().filter_map(|c| c.get()) {
            c.iter().for_each(&mut f);
        }
    }
}

/// 球の面 1 つ。 中身は面を出す前に 1 回書き、 後から変わるのは `to` (閉じる時) だけ。
#[derive(Default)]
struct Face {
    eid: AtomicU32,
    /// 同じ球の 1 つ前の面 (`NONE` = 最初の面)。
    prev: AtomicU32,
    value: AtomicU64,
    from: AtomicU64,
    to: AtomicU64,
}

/// 球から読み出した面 1 つ。 `to == OPEN` は今も上を向いている面。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FaceView {
    pub value: u64,
    pub from: u64,
    pub to: u64,
}

/// デッキの差分 1 つ: 時刻 `t` に球 `eid` が入った (`into` = 1) / 出た (0)。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Diff {
    t: u64,
    eid: u32,
    into: u32,
}

/// 帳面の backing。 差分を列 3 本に分けて持つ: 時刻 (時点 t の二分探索だけが読む) / eid / 入った bit (差分 64 個で 1 語、
/// 出していない bit は 0)。 戻す時に読むのは eid と bit だけ (1 差分 4 B + 1 bit)。 `[0..len]` が出した差分 (以後変わらない)。
struct LogBuf {
    t: Box<[UnsafeCell<u64>]>,
    eid: Box<[UnsafeCell<u32>]>,
    into: Box<[AtomicU64]>,
    len: AtomicUsize,
}

// SAFETY: 出した範囲 [0..len] は書き換わらない。 書き手 (名簿の鍵の下で 1 本) は [len] だけに書いてから len を Release で進め、
// 読み手は len を Acquire で読んで [0..len] だけを読む。 入った bit の語は読み手の読む bit と書き手の立てる bit が同じ語に
// 乗るので atomic (書き手は 1 本、 立てるだけ)。
unsafe impl Sync for LogBuf {}

impl LogBuf {
    fn with_cap(cap: usize) -> Self {
        Self {
            t: (0..cap).map(|_| UnsafeCell::new(0)).collect(),
            eid: (0..cap).map(|_| UnsafeCell::new(0)).collect(),
            into: (0..cap.div_ceil(64)).map(|_| AtomicU64::new(0)).collect(),
            len: AtomicUsize::new(0),
        }
    }
}

/// 控え 1 つ: 帳面の `pos` より前の差分を全部当てたデッキ (eid の昇順)。 位置は帳面の書き手が登録の時に決め、 中身は後で作る
/// (作り終えるまで `eids` は空)。
#[derive(Default)]
struct Snap {
    pos: AtomicUsize,
    eids: OnceLock<Box<[u32]>>,
}

/// 控えの並び (位置の順)。 登録は帳面の書き手 1 本 (名簿の鍵の下)、 読み手は `n` を Acquire で読んで [0..n) を見る。
/// 塊 c は 2^c 個 (登録した控えの塊は登録の前に取る)。
struct SnapList {
    chunks: [OnceLock<Box<[Snap]>>; 32],
    n: AtomicUsize,
}

impl SnapList {
    fn new() -> Self {
        Self { chunks: std::array::from_fn(|_| OnceLock::new()), n: AtomicUsize::new(0) }
    }

    /// j 番目の (塊, 塊の中の場所)。
    fn at(j: usize) -> (usize, usize) {
        let c = (usize::BITS - 1 - (j + 1).leading_zeros()) as usize;
        (c, j + 1 - (1 << c))
    }

    fn get(&self, j: usize) -> &Snap {
        let (c, o) = Self::at(j);
        &self.chunks[c].get().expect("ball: 登録した控えの塊は取ってある")[o]
    }

    /// 位置 `pos` の控えを登録する (中身はまだ)。 帳面の書き手 1 本だけ。
    fn register(&self, pos: usize) -> usize {
        let j = self.n.load(Ordering::Relaxed);
        let (c, o) = Self::at(j);
        self.chunks[c].get_or_init(|| (0..1usize << c).map(|_| Snap::default()).collect())[o].pos.store(pos, Ordering::Relaxed);
        self.n.store(j + 1, Ordering::Release);
        j
    }

    fn len(&self) -> usize {
        self.n.load(Ordering::Acquire)
    }
}

/// 値 1 つのデッキの差分の帳面。 書き手 1 本 (名簿の鍵の下) / 読み手は lock を取らない (epoch)。 足すのは後ろだけ、 満杯なら
/// 倍の backing に写して差し替える (古い backing は読み手が抜けてから捨てる)。
///
/// 時刻ごとのデッキの控え: 帳面の頭 (位置 0) のデッキは `seed` (宣言の時の値、 無ければ空)。 その後は、 前の控えから差分が
/// max(控えの間隔の下限, 前の控えの大きさ) 個たまるたびに (大きさは書き手が帳面に足すたびに数える)、 帳面の書き手が位置を登録し、 名簿の鍵を離した後で同じ書き手が
/// 中身を作る ([`DiffLog::build`]: 作り終えた一番近い前の控えから差分を前向きに当てる。 帳面の [0..位置) は変わらないので鍵は要らない)。
/// 控えは 1 差分あたり 4 B 以下。
struct DiffLog {
    buf: Atomic<LogBuf>,
    seed: Box<[u32]>,
    /// 最初の控えを登録する時に取る。
    snaps: OnceLock<Box<SnapList>>,
    /// 次の控えを登録する帳面の位置 (書き手だけが読み書き)。
    snap_due: AtomicUsize,
    /// 帳面の終わりのデッキの大きさ (書き手だけが読み書き。 入った +1 / 出た −1、 控えの間隔に使う)。
    size: AtomicUsize,
}

impl DiffLog {
    /// `seed` = 帳面の頭のデッキ (eid の昇順、 重なりなし)。 `snap_min` = 控えの間隔の下限。
    fn new(seed: Box<[u32]>, snap_min: usize) -> Self {
        let n = seed.len();
        Self {
            buf: Atomic::new(LogBuf::with_cap(0)),
            seed,
            snaps: OnceLock::new(),
            snap_due: AtomicUsize::new(snap_min.max(n)),
            size: AtomicUsize::new(n),
        }
    }

    /// 後ろに足す。 名簿の鍵を持つ 1 thread だけ。 控えを登録したらその番号 (名簿の鍵を離した後で [`DiffLog::build`] に渡す)。
    fn push(&self, d: Diff, snap_min: usize, guard: &Guard) -> Option<usize> {
        let cur = self.buf.load(Ordering::Acquire, guard);
        // SAFETY: buf は常に非 null。
        let mut b = unsafe { cur.deref() };
        let l = b.len.load(Ordering::Relaxed);
        if l == b.t.len() {
            let n = LogBuf::with_cap((l * 2).max(4));
            for i in 0..l {
                // SAFETY: 古い backing の [0..l] は出した範囲で変わらない。 新しい backing はまだ誰も見ていない
                unsafe {
                    *n.t[i].get() = *b.t[i].get();
                    *n.eid[i].get() = *b.eid[i].get();
                }
            }
            for (w, x) in n.into.iter().zip(&b.into[..l.div_ceil(64)]) {
                w.store(x.load(Ordering::Relaxed), Ordering::Relaxed);
            }
            n.len.store(l, Ordering::Relaxed);
            let n = Owned::new(n).into_shared(guard);
            self.buf.store(n, Ordering::Release);
            // SAFETY: 古い backing は読み手が epoch を抜けてから捨てる。 新しい backing は今 store した (非 null)
            unsafe {
                guard.defer_destroy(cur);
                b = n.deref();
            }
        }
        // SAFETY: [l] はまだ出していない (読み手は読まない)。 書き手は 1 本
        unsafe {
            *b.t[l].get() = d.t;
            *b.eid[l].get() = d.eid;
        }
        if d.into == 1 {
            let w = &b.into[l >> 6];
            w.store(w.load(Ordering::Relaxed) | 1 << (l & 63), Ordering::Relaxed);
        }
        b.len.store(l + 1, Ordering::Release);
        let size = self.size.load(Ordering::Relaxed);
        debug_assert!(d.into == 1 || size > 0, "ball: 空のデッキから出た");
        let size = if d.into == 1 { size + 1 } else { size.wrapping_sub(1) };
        self.size.store(size, Ordering::Relaxed);
        if l + 1 < self.snap_due.load(Ordering::Relaxed) {
            return None;
        }
        let j = self.snaps.get_or_init(|| Box::new(SnapList::new())).register(l + 1);
        // 間隔 = max(下限, この控えの大きさ): 控えは 1 差分あたり 4 B 以下
        self.snap_due.store(l + 1 + snap_min.max(size), Ordering::Relaxed);
        Some(j)
    }

    /// 控え `j` の中身を作る (登録した書き手が名簿の鍵を離した後で)。 作り終えた一番近い前の控え (無ければ頭) から差分を前向きに当てる。
    fn build(&self, j: usize, guard: &Guard) {
        let list = self.snaps.get().expect("ball: 登録した控えの並びは取ってある");
        let target = list.get(j).pos.load(Ordering::Relaxed);
        let (from, base) = (0..j)
            .rev()
            .find_map(|i| {
                let s = list.get(i);
                s.eids.get().map(|e| (s.pos.load(Ordering::Relaxed), &**e))
            })
            .unwrap_or((0, &*self.seed));
        let out = self.read(guard, |_, eids, into| apply(base, Tail { eids: &eids[from..target], into, skip: from }, false));
        let _ = list.get(j).eids.set(out.into_boxed_slice());
    }

    /// 時点 t の読みをどこから当てるか: 控えからなら (t の位置 k, 控えの位置, デッキ)、 今から戻すなら None。 差分の数で一番近い
    /// もの (同じなら控え)。
    fn choose(&self, t: u64, guard: &Guard) -> Option<(usize, usize, &[u32])> {
        let (k, len) = self.read(guard, |ts, _, _| (ts.partition_point(|&x| x <= t), ts.len()));
        let (pos, base) = self.nearest(k);
        (pos.abs_diff(k) <= len - k).then_some((k, pos, base))
    }

    /// 帳面の位置 `k` に一番近い作り終えた控え (頭を含む): (位置, デッキ)。 同じ近さなら前の方。
    fn nearest(&self, k: usize) -> (usize, &[u32]) {
        let mut best = (0, &*self.seed);
        let Some(list) = self.snaps.get() else { return best };
        let n = list.len();
        // 位置は登録の順に増える: k 以下の最後と k より後の最初
        let (mut lo, mut hi) = (0, n);
        while lo < hi {
            let m = (lo + hi) / 2;
            if list.get(m).pos.load(Ordering::Relaxed) <= k {
                lo = m + 1;
            } else {
                hi = m;
            }
        }
        let built = |i: usize| {
            let s = list.get(i);
            s.eids.get().map(|e| (s.pos.load(Ordering::Relaxed), &**e))
        };
        if let Some(b) = (0..lo).rev().find_map(built) {
            best = b;
        }
        if let Some(a) = (lo..n).find_map(built)
            && a.0 - k < k - best.0
        {
            best = a;
        }
        best
    }

    /// (登録した控えの数, 作り終えた数)。
    fn snap_counts(&self) -> (usize, usize) {
        self.snaps.get().map_or((0, 0), |l| (l.len(), (0..l.len()).filter(|&j| l.get(j).eids.get().is_some()).count()))
    }

    /// 頭と控えのデッキの byte。
    fn snap_bytes(&self) -> usize {
        let list = self.snaps.get().map_or(0, |l| (0..l.len()).map(|j| l.get(j).eids.get().map_or(0, |e| e.len())).sum::<usize>());
        (self.seed.len() + list) * 4
    }

    /// 出した差分を読む: f(時刻, eid, 入った bit)。
    fn read<R>(&self, guard: &Guard, f: impl FnOnce(&[u64], &[u32], &[AtomicU64]) -> R) -> R {
        // SAFETY: buf は常に非 null、 guard が backing を生かす。
        let b = unsafe { self.buf.load(Ordering::Acquire, guard).deref() };
        let n = b.len.load(Ordering::Acquire);
        // SAFETY: [0..n] は出した範囲 (変わらない)。 UnsafeCell<T> は T と同じ表現
        let (t, eid) = unsafe {
            (std::slice::from_raw_parts(b.t.as_ptr() as *const u64, n), std::slice::from_raw_parts(b.eid.as_ptr() as *const u32, n))
        };
        f(t, eid, &b.into)
    }

    fn len(&self, guard: &Guard) -> usize {
        self.read(guard, |t, _, _| t.len())
    }

    /// 取った大きさ (byte)。
    fn bytes(&self, guard: &Guard) -> usize {
        // SAFETY: buf は常に非 null。
        let b = unsafe { self.buf.load(Ordering::Acquire, guard).deref() };
        b.t.len() * 12 + b.into.len() * 8
    }
}

/// 時点 t より後の差分 (時刻の順) の eid と入った bit。 差分 k (`eids[k]`) の bit は `into` の `skip + k` 番目。
#[derive(Clone, Copy)]
struct Tail<'a> {
    eids: &'a [u32],
    into: &'a [AtomicU64],
    skip: usize,
}

impl Tail<'_> {
    fn len(&self) -> usize {
        self.eids.len()
    }

    #[inline]
    fn entered(&self, k: usize) -> bool {
        let i = self.skip + k;
        self.into[i >> 6].load(Ordering::Relaxed) >> (i & 63) & 1 == 1
    }

    /// 新しい方から (eid, 入った) を渡す (bit の語は 64 個に 1 回読む)。
    #[inline]
    fn for_each_rev(&self, mut f: impl FnMut(u32, bool)) {
        let mut k = self.eids.len();
        while k > 0 {
            let i = self.skip + k - 1;
            let w = self.into[i >> 6].load(Ordering::Relaxed);
            let start = ((i >> 6) << 6).saturating_sub(self.skip);
            for j in (start..k).rev() {
                f(self.eids[j], w >> ((self.skip + j) & 63) & 1 == 1);
            }
            k = start;
        }
    }

    /// 古い方から (eid, 入った) を渡す (bit の語は 64 個に 1 回読む)。
    #[inline]
    fn for_each(&self, mut f: impl FnMut(u32, bool)) {
        let mut k = 0;
        while k < self.eids.len() {
            let i = self.skip + k;
            let w = self.into[i >> 6].load(Ordering::Relaxed);
            let end = (((i >> 6) + 1) << 6).saturating_sub(self.skip).min(self.eids.len());
            for j in k..end {
                f(self.eids[j], w >> ((self.skip + j) & 63) & 1 == 1);
            }
            k = end;
        }
    }
}

/// 集めた差分の写し (大きい値の糸から集めた差分、 試験)。
struct TailBuf {
    eids: Vec<u32>,
    into: Vec<AtomicU64>,
}

impl TailBuf {
    /// 時刻の順の差分から。
    fn new(ds: impl IntoIterator<Item = (u32, bool)>) -> Self {
        let mut b = Self { eids: Vec::new(), into: Vec::new() };
        for (k, (e, into)) in ds.into_iter().enumerate() {
            b.eids.push(e);
            if k & 63 == 0 {
                b.into.push(AtomicU64::new(0));
            }
            if into {
                *b.into[k >> 6].get_mut() |= 1 << (k & 63);
            }
        }
        b
    }

    /// `from` 番目から後。
    fn tail(&self, from: usize) -> Tail<'_> {
        self.range(from, self.eids.len())
    }

    /// [from..to)。
    fn range(&self, from: usize, to: usize) -> Tail<'_> {
        Tail { eids: &self.eids[from..to], into: &self.into, skip: from }
    }
}

impl Drop for DiffLog {
    fn drop(&mut self) {
        // SAFETY: drop は帳面を持つ最後の 1 つ (誰も読んでいない)。
        unsafe { drop(self.buf.load(Ordering::Relaxed, epoch::unprotected()).into_owned()) };
    }
}

/// 大きい値の差分の、 時刻の未定の印 (列の lock を持つ書き手の外には見えない)。
const PENDING: u64 = u64::MAX;

/// 大きい値の差分 1 つ (16 B)。 `link` = 同じ値の 1 つ前の差分の場所 + 1 (0 = 最初) << 1 | 入った。
#[derive(Clone, Copy)]
struct WideDiff {
    t: u64,
    eid: u32,
    link: u32,
}

/// 大きい値の差分の列 1 本 (値の下位 bit で選ぶ)。 値ごとの帳面の代わりに、 全部の値の差分を 1 本に足して値ごとの糸でつなぐ。
#[derive(Default)]
struct WideStripe {
    /// 値 → その値の最後の差分の場所 + 1。
    heads: BTreeMap<u64, u32>,
    diffs: Vec<WideDiff>,
}

impl WideStripe {
    /// 値 `v` の糸の先に足す。 返り値は足す前の糸の頭 (取り消す時に戻す)。
    fn push(&mut self, v: u64, d: Diff) -> u32 {
        let i = self.diffs.len();
        assert!(i < (1 << 31) - 1, "ball: 大きい値の差分の列が一杯 (試作)");
        let head = self.heads.entry(v).or_insert(0);
        let prev = *head;
        self.diffs.push(WideDiff { t: d.t, eid: d.eid, link: prev << 1 | d.into });
        *head = i as u32 + 1;
        prev
    }

    /// 最後に足した値 `v` の差分を取り消す (`push` の返り値を `prev` に)。
    fn pop(&mut self, v: u64, prev: u32) {
        self.diffs.pop();
        if prev == 0 {
            self.heads.remove(&v);
        } else {
            self.heads.insert(v, prev);
        }
    }

    /// 値 `v` の t より後の差分を時刻の順に。
    fn after(&self, v: u64, t: u64) -> TailBuf {
        let mut out = Vec::new();
        let mut at = self.heads.get(&v).copied().unwrap_or(0);
        while at != 0 {
            let d = self.diffs[at as usize - 1];
            if d.t <= t {
                break;
            }
            out.push((d.eid, d.link & 1 == 1));
            at = d.link >> 1;
        }
        TailBuf::new(out.into_iter().rev())
    }
}

/// [`Ball::reserve`] で取った場所 1 つ: (`held` の添字, 列の中の場所, 値, 足す前の糸の頭)。
struct Slot {
    held: usize,
    at: usize,
    v: u64,
    prev: u32,
}

/// 書き換え 1 回の帳面の仕事: 大きい値の差分の、 取った場所と列の lock (`slots` は [古い値 (出た), 新しい値 (入った)])、 と
/// 登録した控え (値, 番号)。 控えは名簿の鍵を離した後で [`Ball::build_snaps`] に渡す ([`Reserved::take_snaps`])。
pub(crate) struct Reserved<'a> {
    held: [Option<(usize, MutexGuard<'a, WideStripe>)>; 2],
    slots: [Option<Slot>; 2],
    filled: bool,
    snaps: [Option<(u32, usize)>; 2],
}

impl Reserved<'_> {
    /// 登録した控え (値, 番号)。
    pub(crate) fn take_snaps(&mut self) -> [Option<(u32, usize)>; 2] {
        std::mem::take(&mut self.snaps)
    }

    /// `i` (0 = 古い値、 1 = 新しい値) の場所に時刻を書く。
    fn fill(&mut self, i: usize, v: u64, ts: u64) {
        let s = self.slots[i].as_ref().expect("ball: 大きい値の場所は名簿の鍵の前に取ってある");
        debug_assert_eq!(s.v, v);
        self.held[s.held].as_mut().unwrap().1.diffs[s.at].t = ts;
    }
}

impl Drop for Reserved<'_> {
    fn drop(&mut self) {
        if self.filled {
            return;
        }
        // 書かなかった (同じ値 / 書けなかった): 足した順の逆に取り消す
        for s in self.slots.iter().rev().flatten() {
            let g = &mut self.held[s.held].as_mut().unwrap().1;
            debug_assert_eq!(g.diffs.len(), s.at + 1);
            g.pop(s.v, s.prev);
        }
    }
}

/// 紐 1 本の球の全部 (entity ごとの球 + 値ごとの差分の帳面)。
pub(crate) struct Ball {
    id: u64,
    faces: Chunked<Face>,
    /// 書き手に渡した面の場所の数 (書き手ごとの塊の、 まだ使っていない分を含む)。
    next: AtomicU64,
    /// entity → 今の面 + 1 (0 = 球が無い)。
    heads: Chunked<AtomicU32>,
    /// 値 (`LOG_DENSE` 未満) → 帳面。
    logs: Chunked<OnceLock<Box<DiffLog>>>,
    /// `LOG_DENSE` 以上の値の差分。 値の下位 bit で `WIDE` 本に分ける (違う値の書き手は名簿の鍵では並ばないので lock で守る)。
    wide: Box<[Mutex<WideStripe>]>,
    /// 控えの間隔の下限 (差分の数)。
    snap_min: usize,
}

impl Ball {
    fn new(snap_min: usize) -> Self {
        Self {
            id: BALL_IDS.fetch_add(1, Ordering::Relaxed),
            faces: Chunked::new(1 << 32),
            next: AtomicU64::new(0),
            heads: Chunked::new(1 << 32),
            logs: Chunked::new(LOG_DENSE),
            wide: (0..WIDE).map(|_| Mutex::new(WideStripe::default())).collect(),
            snap_min,
        }
    }

    /// [`Ball::note`] が登録した控えを作る。 名簿の鍵を離した後で、 登録した書き手が呼ぶ。
    pub(crate) fn build_snaps(&self, snaps: [Option<(u32, usize)>; 2]) {
        if snaps == [None, None] {
            return;
        }
        let guard = epoch::pin();
        for (v, j) in snaps.into_iter().flatten() {
            self.logs.get(v).and_then(|l| l.get()).expect("ball: 控えを登録した帳面はある").build(j, &guard);
        }
    }

    /// 宣言の時のデッキを帳面の頭に置く (`LOG_DENSE` 未満の値、 eid の昇順)。 書き手の居ない時に。
    fn seed_logs(&self, decks: BTreeMap<u32, Vec<u32>>) {
        for (v, mut eids) in decks {
            eids.sort_unstable();
            debug_assert!(eids.windows(2).all(|w| w[0] < w[1]), "ball: 宣言のデッキに重なり");
            let _ = self.logs.get_or_alloc(v).set(Box::new(DiffLog::new(eids.into_boxed_slice(), self.snap_min)));
        }
    }

    #[inline]
    fn face(&self, i: u32) -> &Face {
        self.faces.get(i).expect("ball: 出した面の塊は取ってある")
    }

    fn stripe(&self, v: u64) -> MutexGuard<'_, WideStripe> {
        self.wide[v as usize % WIDE].lock().unwrap_or_else(|e| e.into_inner())
    }

    /// 書き換え `old` → `new` の大きい値の側の差分の場所を、 **名簿の鍵を取る前に** 取る (時刻は未定)。 返り値は列の lock を
    /// 持ったまま、 [`Ball::note`] が時刻を書き込む。 `note` を呼ばずに落とすと取り消す。 どちらも小さい値なら何も取らない。
    pub(crate) fn reserve(&self, eid: u32, old: Option<u64>, new: Option<u64>) -> Reserved<'_> {
        let mut r = Reserved { held: [None, None], slots: [None, None], filled: false, snaps: [None, None] };
        let wide = |v: Option<u64>| v.filter(|&v| v >= LOG_DENSE);
        let vs = [wide(old), wide(new)];
        if old == new || vs == [None, None] {
            return r;
        }
        // 列の lock は番号の小さい方から (同じ列なら 1 つ)
        let mut ks: Vec<usize> = vs.iter().flatten().map(|&v| v as usize % WIDE).collect();
        ks.sort_unstable();
        ks.dedup();
        for (h, &k) in ks.iter().enumerate() {
            r.held[h] = Some((k, self.wide[k].lock().unwrap_or_else(|e| e.into_inner())));
        }
        for (i, v) in vs.into_iter().enumerate() {
            let Some(v) = v else { continue };
            let h = ks.iter().position(|&k| k == v as usize % WIDE).unwrap();
            let g = &mut r.held[h].as_mut().unwrap().1;
            let at = g.diffs.len();
            let prev = g.push(v, Diff { t: PENDING, eid, into: i as u32 });
            r.slots[i] = Some(Slot { held: h, at, v, prev });
        }
        r
    }

    /// 書き換え `old` → `new` を帳面に記す。 **名簿の鍵の中、 名簿を書き換える前に** 呼ぶ (`HimoStore::set_noting` の `note`):
    /// 時刻を取り、 古い値の帳面に 「出た」、 新しい値の帳面に 「入った」 を足す (大きい値は `wide` に取った場所に時刻を書く)。
    /// 返り値は時刻 (同じ値なら何もせず None)。 鍵を離した後、 `wide` から控えを取り出して ([`Reserved::take_snaps`]) 落とし、
    /// 行の lock の下で [`Ball::turn`] にこの時刻を渡し、 控えを [`Ball::build_snaps`] で作ること。
    pub(crate) fn note(&self, eid: u32, old: Option<u64>, new: Option<u64>, clock: &AtomicU64, guard: &Guard, wide: &mut Reserved<'_>) -> Option<u64> {
        if old == new {
            return None;
        }
        let ts = clock.fetch_add(1, Ordering::Relaxed) + 1;
        for (i, v) in [old, new].into_iter().enumerate() {
            debug_assert_eq!(wide.slots[i].is_some(), v.is_some_and(|v| v >= LOG_DENSE), "ball: 取った場所と書き換えが違う");
            match v {
                Some(v) if v < LOG_DENSE => {
                    let log = self.logs.get_or_alloc(v as u32).get_or_init(|| Box::new(DiffLog::new(Box::new([]), self.snap_min)));
                    wide.snaps[i] = log.push(Diff { t: ts, eid, into: i as u32 }, self.snap_min, guard).map(|j| (v as u32, j));
                }
                Some(v) => wide.fill(i, v, ts),
                None => {}
            }
        }
        wide.filled = true;
        Some(ts)
    }

    /// 面の場所を 1 つ。 この thread の塊から渡し、 尽きたら `BLOCK` 個まとめて取る。 NONE は糸の終わりに使うので、
    /// 場所を使い切ったら記録をやめる (試作)。
    fn alloc(&self) -> Option<u32> {
        BLOCKS.with(|c| {
            let mut b = c.get();
            let k = match b.iter().position(|e| e.0 == self.id) {
                Some(k) => k,
                None => {
                    b.rotate_right(1);
                    b[0] = (self.id, 0, 0);
                    0
                }
            };
            if b[k].1 == b[k].2 {
                let start = self.next.fetch_add(BLOCK, Ordering::Relaxed);
                if start + BLOCK >= NONE as u64 {
                    return None;
                }
                b[k].1 = start as u32;
                b[k].2 = (start + BLOCK) as u32;
            }
            let i = b[k].1;
            b[k].1 += 1;
            c.set(b);
            Some(i)
        })
    }

    /// 値 `v` の面を `i` として出す。
    fn push_face(&self, i: u32, eid: u32, prev: u32, v: u64, from: u64) {
        let f = self.faces.get_or_alloc(i);
        f.eid.store(eid, Ordering::Relaxed);
        f.prev.store(prev, Ordering::Relaxed);
        f.value.store(v, Ordering::Relaxed);
        f.from.store(from, Ordering::Relaxed);
        f.to.store(OPEN, Ordering::Relaxed);
    }

    /// 球を回す: `new` を上に向ける (None = 今の面を閉じるだけ)、 時刻 `ts` ([`Ball::note`] の返り値)。 行の lock の下で呼ぶ。
    pub(crate) fn turn(&self, eid: u32, new: Option<u64>, ts: u64) {
        let head = self.heads.get_or_alloc(eid);
        let cur = head.load(Ordering::Relaxed);
        if let Some(f) = (cur != 0).then(|| self.face(cur - 1)).filter(|f| f.to.load(Ordering::Relaxed) == OPEN) {
            f.to.store(ts, Ordering::Release);
        }
        if let Some(v) = new {
            let Some(i) = self.alloc() else { return };
            self.push_face(i, eid, cur.wrapping_sub(1), v, ts);
            head.store(i + 1, Ordering::Release);
        }
    }

    /// 宣言の前からある値を 「前から」 (from = 0) の面にする (差分は書かない: t より後の差分の無い球は今と同じ)。
    fn seed(&self, eid: u32, v: u64) {
        let head = self.heads.get_or_alloc(eid);
        if head.load(Ordering::Relaxed) != 0 {
            return;
        }
        let Some(i) = self.alloc() else { return };
        self.push_face(i, eid, NONE, v, 0);
        head.store(i + 1, Ordering::Release);
    }

    /// 時点 `t` に上を向いていた値。 今の面から前の面へたどり、 `from <= t` の最初の面が `t` でまだ開いていれば値。
    pub fn value_at(&self, eid: u32, t: u64) -> Option<u64> {
        let mut i = self.heads.get(eid)?.load(Ordering::Acquire).checked_sub(1)?;
        loop {
            let f = self.face(i);
            if f.from.load(Ordering::Relaxed) <= t {
                return (f.to.load(Ordering::Acquire) > t).then(|| f.value.load(Ordering::Relaxed));
            }
            i = f.prev.load(Ordering::Relaxed);
            if i == NONE {
                return None;
            }
        }
    }

    /// 球の面を古い順に。
    pub fn faces_of(&self, eid: u32) -> Vec<FaceView> {
        let mut out = Vec::new();
        let Some(mut i) = self.heads.get(eid).and_then(|h| h.load(Ordering::Acquire).checked_sub(1)) else {
            return out;
        };
        loop {
            let f = self.face(i);
            out.push(FaceView { value: f.value.load(Ordering::Relaxed), from: f.from.load(Ordering::Relaxed), to: f.to.load(Ordering::Acquire) });
            i = f.prev.load(Ordering::Relaxed);
            if i == NONE {
                break;
            }
        }
        out.reverse();
        out
    }

    /// 時点 `t` に値 `v` の面を上にしていた球 (eid の昇順)。 `now` は今のデッキを名簿から読む (並びは問わない、 重なりは無いこと
    /// = `HimoStore::pull` の返り値)。 帳面の t の位置 k に、 今 (帳面の終わり) と作り終えた控えのうち差分の数で一番近いものから
    /// 当てる: 今か k より後の控えからは逆に戻し、 k より前の控え (帳面の頭を含む) からは前向きに進める。 同じ近さなら控え
    /// (名簿を読まない)。 今から戻す時は名簿を読んでから帳面を読み直す (名簿の変化を見たなら、 その前に書いた差分も見える)。
    pub fn pull_at(&self, v: u64, t: u64, now: impl FnOnce() -> Vec<u32>) -> Vec<u32> {
        if v >= LOG_DENSE {
            let now = now();
            let after = self.stripe(v).after(v, t);
            return from_now(now, after.tail(0));
        }
        let Some(log) = self.logs.get(v as u32).and_then(|l| l.get()) else { return from_now(now(), TailBuf::new([]).tail(0)) };
        let guard = epoch::pin();
        if let Some((k, pos, base)) = log.choose(t, &guard) {
            return log.read(&guard, |_, eids, into| {
                if pos <= k {
                    apply(base, Tail { eids: &eids[pos..k], into, skip: pos }, false)
                } else {
                    apply(base, Tail { eids: &eids[k..pos], into, skip: k }, true)
                }
            });
        }
        let now = now();
        log.read(&guard, |ts, eids, into| {
            let k = ts.partition_point(|&x| x <= t);
            from_now(now, Tail { eids: &eids[k..], into, skip: k })
        })
    }

    /// 時点 `t` の答えを、 今と作り終えた全部の控え (帳面の頭を含む) のそれぞれから作る (試験用: どの道も同じ答えか)。
    #[cfg(test)]
    fn pull_at_every(&self, v: u64, t: u64, now: Vec<u32>) -> Vec<(String, Vec<u32>)> {
        let mut out = vec![("今".to_string(), self.pull_at(v, t, || now.clone()))];
        let Some(log) = self.logs.get(v as u32).and_then(|l| l.get()) else { return out };
        let guard = epoch::pin();
        log.read(&guard, |ts, eids, into| {
            let k = ts.partition_point(|&x| x <= t);
            let mut bases = vec![(0, &*log.seed)];
            if let Some(list) = log.snaps.get() {
                bases.extend((0..list.len()).filter_map(|j| list.get(j).eids.get().map(|e| (list.get(j).pos.load(Ordering::Relaxed), &**e))));
            }
            for (pos, base) in bases {
                let got = if pos <= k {
                    apply(base, Tail { eids: &eids[pos..k], into, skip: pos }, false)
                } else {
                    apply(base, Tail { eids: &eids[k..pos], into, skip: k }, true)
                };
                out.push((format!("控え@{pos}"), got));
            }
        });
        out
    }

    /// 帳面の控えの数: (登録した数, 作り終えた数)。 書き手が返った後は同じ (登録した書き手が鍵を離した後で作る)。
    pub fn snap_counts(&self) -> (usize, usize) {
        let mut n = (0, 0);
        self.logs.for_each(|l| {
            let (r, b) = l.get().map_or((0, 0), |l| l.snap_counts());
            n = (n.0 + r, n.1 + b);
        });
        n
    }

    /// 書き手に渡した面の場所の数 (書き手ごとの塊の、 まだ使っていない分 = 高々 `BLOCK` × 書き手の数を含む)。
    pub fn face_count(&self) -> u64 {
        self.next.load(Ordering::Relaxed).min(NONE as u64)
    }

    /// 帳面の差分の数。
    pub fn diff_count(&self) -> u64 {
        let guard = epoch::pin();
        let mut n = 0u64;
        self.logs.for_each(|l| n += l.get().map_or(0, |l| l.len(&guard) as u64));
        n + self.wide.iter().map(|w| w.lock().unwrap_or_else(|e| e.into_inner()).diffs.len() as u64).sum::<u64>()
    }

    /// 取った大きさ (byte、 塊の表は除く。 大きい値の表は 1 つ 24 B と見積もる)。
    pub fn bytes(&self) -> usize {
        let guard = epoch::pin();
        let mut logs = 0usize;
        self.logs.for_each(|l| logs += l.get().map_or(0, |l| l.bytes(&guard) + l.snap_bytes()));
        let wide: usize = self
            .wide
            .iter()
            .map(|w| {
                let w = w.lock().unwrap_or_else(|e| e.into_inner());
                w.diffs.capacity() * std::mem::size_of::<WideDiff>() + w.heads.len() * 24
            })
            .sum();
        self.faces.allocated() * CHUNK * std::mem::size_of::<Face>()
            + self.heads.allocated() * CHUNK * 4
            + self.logs.allocated() * CHUNK * std::mem::size_of::<OnceLock<Box<DiffLog>>>()
            + logs
            + wide
    }
}

/// 今のデッキ `now` (並びは問わない、 重なりは無い) に、 t より後の差分 `after` (時刻の順) を新しい方から逆に当てた、
/// 時点 t のデッキ (eid の昇順)。
fn from_now(mut now: Vec<u32>, after: Tail<'_>) -> Vec<u32> {
    // 名簿の読みはほぼ並んでいる (詰め直した bucket + 後ろに足した分) ので比較 sort (並んだ所を一気に通る)。 基数 sort は
    // 並びによらず桁の数だけ写すので、 今を指した読みが 5 → 22 µs になった。 重なりは無い約束 (落とす走査 1 回で +2.8 µs)
    now.sort_unstable();
    debug_assert!(now.windows(2).all(|w| w[0] < w[1]), "ball: 今のデッキに重なり");
    if after.len() == 0 {
        return now;
    }
    apply(&now, after, true)
}

/// デッキ `base` (eid の昇順、 重なりなし) に差分 `tail` (時刻の順) を当てたデッキ (eid の昇順)。 `back` = base が tail の後の
/// デッキで、 新しい方から逆に当てる (入った → 抜く、 出た → 足す。 球ごとに最初の差分が決める)。 でなければ base が tail の前の
/// デッキで、 古い方から当てる (入った → 足す、 出た → 抜く。 球ごとに最後の差分が決める)。 差分の無い球は base のまま。
/// 差分が多く eid が狭く集まっていれば bit の表で、 そうでなければ並べて合わせる。
fn apply(base: &[u32], tail: Tail<'_>, back: bool) -> Vec<u32> {
    match bits_window(base, tail) {
        Some((lo, words)) => apply_bits(base, tail, lo, words, back),
        None => apply_merge(base, tail, back),
    }
}

/// bit の表を使うなら (表の始まりの eid, 語の数)。
fn bits_window(base: &[u32], tail: Tail<'_>) -> Option<(u32, usize)> {
    if tail.len() < BITS_MIN_AFTER {
        return None;
    }
    let (lo, hi) = base.iter().chain(tail.eids).fold((u32::MAX, 0), |(lo, hi), &e| (lo.min(e), hi.max(e)));
    let words = ((hi - lo) as usize >> 6) + 1;
    (words <= BITS_MAX_SPREAD * (base.len() + tail.len())).then_some((lo, words))
}

/// eid の bit の表に base を立て、 差分を当てる。 表を頭から読めば昇順。
fn apply_bits(base: &[u32], tail: Tail<'_>, lo: u32, words: usize, back: bool) -> Vec<u32> {
    // 表は thread ごとに使い回す (毎回取ると 100 KB を超える表は新しい page を触るたびに page fault)
    BITS.with(|b| {
        let mut bits = b.take();
        bits.clear();
        bits.resize(words, 0);
        let out = apply_on(&mut bits, base, tail, lo, back);
        b.set(bits);
        out
    })
}

fn apply_on(bits: &mut [u64], base: &[u32], tail: Tail<'_>, lo: u32, back: bool) -> Vec<u32> {
    for &e in base {
        let i = (e - lo) as usize;
        bits[i >> 6] |= 1 << (i & 63);
    }
    // 立てる / 落とすを枝なしで (入った / 出たは揃わずに並ぶので、 枝にすると読み違いが続く: 差分 9.4k で 30 µs、 枝なし 6 µs。
    // 値 1 つを繰り返し読む時だけ枝の予測が覚えて 10 µs)。 逆に当てる時は入った → 落とす、 前向きは入った → 立てる
    let mut put = |e: u32, into: bool| {
        let i = (e - lo) as usize;
        let m = 1u64 << (i & 63);
        let w = &mut bits[i >> 6];
        *w = (*w & !m) | (((into != back) as u64).wrapping_neg() & m);
    };
    if back {
        tail.for_each_rev(&mut put);
    } else {
        tail.for_each(&mut put);
    }
    collect_bits(bits, lo, base.len() + tail.len() / 2)
}

/// 表の立った bit の eid を昇順に。 語ごとに 4 個を枝なしで書き (空いた bit の分は捨てる場所に書く)、 立っていた数だけ進める。
/// 語ごとに立った数だけ回すと、 回る数が語ごとに揺れて読み違いが続く (語 15k で 64 µs、 これで 18 µs)。
fn collect_bits(bits: &[u64], lo: u32, hint: usize) -> Vec<u32> {
    let mut out: Vec<u32> = Vec::with_capacity(hint + 64);
    for (w, &x) in bits.iter().enumerate() {
        if out.capacity() - out.len() < 64 {
            out.reserve(64);
        }
        let base = lo as u64 + ((w as u64) << 6);
        let c = x.count_ones() as usize;
        let spare = &mut out.spare_capacity_mut()[..64];
        let mut y = x;
        for s in &mut spare[..4] {
            s.write((base + y.trailing_zeros() as u64) as u32);
            y &= y.wrapping_sub(1);
        }
        if c > 4 {
            for s in &mut spare[4..c] {
                s.write((base + y.trailing_zeros() as u64) as u32);
                y &= y - 1;
            }
        }
        // SAFETY: [len..len + c] は今書いた (c ≤ 64 ≤ 空き)
        unsafe { out.set_len(out.len() + c) };
    }
    out
}

/// base と、 球ごとに決める差分 (逆なら最初 = t の直後、 前向きなら最後) を eid の順に並べて合わせる。 決める差分のある球は
/// その差分で (逆: 入った = 居なかった、 前向き: 入った = 居る)、 無い球は base のまま。
fn apply_merge(base: &[u32], tail: Tail<'_>, back: bool) -> Vec<u32> {
    if tail.len() == 0 {
        return base.to_vec();
    }
    let mut pick: Vec<(u32, u32, bool)> = tail.eids.iter().enumerate().map(|(i, &e)| (e, i as u32, tail.entered(i) != back)).collect();
    // 球ごとに決める差分を先頭に: 逆は古い方、 前向きは新しい方
    pick.sort_unstable_by_key(|&(e, i, _)| (e, if back { i } else { !i }));
    pick.dedup_by_key(|x| x.0);
    let mut out = Vec::with_capacity(base.len() + pick.len());
    let mut j = 0;
    for &e in base {
        while j < pick.len() && pick[j].0 < e {
            if pick[j].2 {
                out.push(pick[j].0);
            }
            j += 1;
        }
        if j < pick.len() && pick[j].0 == e {
            if pick[j].2 {
                out.push(e);
            }
            j += 1;
        } else {
            out.push(e);
        }
    }
    out.extend(pick[j..].iter().filter(|x| x.2).map(|x| x.0));
    out
}

/// engine 1 つの球の全部。 宣言が 1 つも無ければ書き手は `any` の load 1 回で素通り。
pub(crate) struct Balls {
    any: AtomicBool,
    clock: AtomicU64,
    /// これから宣言する球の控えの間隔の下限 (差分の数)。
    snap_min: AtomicUsize,
    /// 紐の番号 → 球。 最初の宣言で取る。
    slots: OnceLock<Box<[OnceLock<Box<Ball>>]>>,
}

impl Balls {
    pub fn new() -> Self {
        Self { any: AtomicBool::new(false), clock: AtomicU64::new(0), snap_min: AtomicUsize::new(SNAP_MIN), slots: OnceLock::new() }
    }

    #[inline]
    pub fn any(&self) -> bool {
        self.any.load(Ordering::Acquire)
    }

    /// 今の時刻 (最後に記した書き換えの時刻)。 この後の書き換えは、 これより大きい時刻になる。
    pub fn now(&self) -> u64 {
        self.clock.load(Ordering::Relaxed)
    }

    /// 時刻の数え口 ([`Ball::note`] に渡す)。
    #[inline]
    pub fn clock(&self) -> &AtomicU64 {
        &self.clock
    }

    #[inline]
    pub fn get(&self, hid: u16) -> Option<&Ball> {
        self.slots.get()?[hid as usize].get().map(|b| &**b)
    }

    /// これから宣言する球の控えの間隔の下限 (差分の数、 既定 `SNAP_MIN`)。 試験が小さくして控えの道を通す。
    pub fn set_snap_min(&self, n: usize) {
        self.snap_min.store(n.max(1), Ordering::Relaxed);
    }

    /// 紐 `hid` を球にする。 `seed` に今の値 (eid, 値) を全部渡す。 宣言済みなら false。 名簿 (円柱) は先に組んでおくこと。
    /// 今の値は球の 「前から」 の面と、 帳面の頭のデッキ (時刻 0 のデッキ、 `LOG_DENSE` 未満の値) になる。
    pub fn declare(&self, hid: u16, seed: impl FnOnce(&mut dyn FnMut(u32, u64))) -> bool {
        let slots = self.slots.get_or_init(|| (0..=u16::MAX as usize).map(|_| OnceLock::new()).collect());
        let mut fresh = false;
        let ball = slots[hid as usize].get_or_init(|| {
            fresh = true;
            Box::new(Ball::new(self.snap_min.load(Ordering::Relaxed)))
        });
        if fresh {
            let mut decks: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
            seed(&mut |eid, v| {
                ball.seed(eid, v);
                if v < LOG_DENSE {
                    decks.entry(v as u32).or_default().push(eid);
                }
            });
            ball.seed_logs(decks);
            self.any.store(true, Ordering::Release);
        }
        fresh
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 名簿の鍵の代わり (値の下位 bit で選ぶ)。 書き手は古い値と新しい値の鍵を取った中で `note`、 離してから `turn`。
    struct Locks(Vec<parking_lot::Mutex<()>>);

    impl Locks {
        fn new(n: usize) -> Self {
            Self((0..n).map(|_| parking_lot::Mutex::new(())).collect())
        }
        fn record(&self, b: &Balls, hid: u16, eid: u32, old: Option<u64>, new: Option<u64>) -> Option<u64> {
            let ball = b.get(hid)?;
            let mut wide = ball.reserve(eid, old, new);
            let mut ks: Vec<usize> = old.into_iter().chain(new).map(|v| v as usize % self.0.len()).collect();
            ks.sort_unstable();
            ks.dedup();
            let held: Vec<_> = ks.iter().map(|&k| self.0[k].lock()).collect();
            let ts = ball.note(eid, old, new, b.clock(), &epoch::pin(), &mut wide);
            drop(held);
            let snaps = wide.take_snaps();
            drop(wide);
            if let Some(ts) = ts {
                ball.turn(eid, new, ts);
            }
            ball.build_snaps(snaps);
            ts
        }
    }

    /// 試験の手元: 球と、 名簿の代わりの今の値の写し ((紐, eid) → 値)。
    struct T {
        b: Balls,
        locks: Locks,
        cur: BTreeMap<(u16, u32), u64>,
    }

    impl T {
        fn new() -> Self {
            Self { b: Balls::new(), locks: Locks::new(16), cur: BTreeMap::new() }
        }
        /// 控えの間隔の下限を `n` にして (宣言の前に)。
        fn with_snap_min(n: usize) -> Self {
            let x = Self::new();
            x.b.set_snap_min(n);
            x
        }
        fn set(&mut self, hid: u16, eid: u32, v: u64) {
            let old = self.cur.insert((hid, eid), v);
            self.locks.record(&self.b, hid, eid, old, Some(v));
        }
        fn rm(&mut self, hid: u16, eid: u32) {
            let old = self.cur.remove(&(hid, eid));
            self.locks.record(&self.b, hid, eid, old, None);
        }
        /// 今のデッキ (名簿から読んだつもり)。
        fn now(&self, hid: u16, v: u64) -> Vec<u32> {
            self.cur.iter().filter(|&(&(h, _), &x)| h == hid && x == v).map(|(&(_, e), _)| e).collect()
        }
        fn pull_at(&self, hid: u16, v: u64, t: u64) -> Vec<u32> {
            self.b.get(hid).unwrap().pull_at(v, t, || self.now(hid, v))
        }
    }

    #[test]
    fn faces_and_diffs_keep_the_old_values() {
        let mut x = T::new();
        assert!(x.b.declare(3, |_| {}));
        x.set(3, 7, 10);
        let t1 = x.b.now();
        x.set(3, 7, 10); // 同じ値は回さない (時刻も進まない)
        assert_eq!(x.b.now(), t1);
        x.set(3, 7, 20);
        let t2 = x.b.now();
        x.rm(3, 7);
        let t3 = x.b.now();
        let ball = x.b.get(3).unwrap();
        assert_eq!(ball.value_at(7, 0), None, "書く前");
        assert_eq!(ball.value_at(7, t1), Some(10));
        assert_eq!(ball.value_at(7, t2), Some(20));
        assert_eq!(ball.value_at(7, t3), None, "外した後");
        assert_eq!(
            ball.faces_of(7),
            vec![FaceView { value: 10, from: t1, to: t2 }, FaceView { value: 20, from: t2, to: t3 }]
        );
        assert_eq!(x.pull_at(3, 10, 0), Vec::<u32>::new());
        assert_eq!(x.pull_at(3, 10, t1), vec![7]);
        assert_eq!(x.pull_at(3, 10, t2), Vec::<u32>::new());
        assert_eq!(x.pull_at(3, 20, t2), vec![7]);
        assert_eq!(x.pull_at(3, 20, t3), Vec::<u32>::new());
        assert_eq!(ball.diff_count(), 4, "入った 10 / 出た 10 + 入った 20 / 出た 20");
    }

    #[test]
    fn seeded_faces_are_from_the_start() {
        let mut x = T::new();
        x.cur.insert((0, 1), 5);
        x.cur.insert((0, 2), 5);
        x.b.declare(0, |f| {
            f(1, 5);
            f(2, 5);
        });
        x.set(0, 1, 6);
        let t = x.b.now();
        assert_eq!(x.pull_at(0, 5, 0), vec![1, 2]);
        assert_eq!(x.pull_at(0, 5, t), vec![2]);
        assert_eq!(x.pull_at(0, 6, 0), Vec::<u32>::new());
        assert_eq!(x.b.get(0).unwrap().value_at(1, t - 1), Some(5));
    }

    #[test]
    fn wide_values_and_reused_eids() {
        let mut x = T::new();
        x.b.declare(1, |_| {});
        let big = 1u64 << 40;
        x.set(1, 4, big);
        let t = x.b.now();
        x.rm(1, 4); // delete も外すだけ
        x.set(1, 4, 9); // 使い回した eid の次の entity
        let ball = x.b.get(1).unwrap();
        assert_eq!(x.pull_at(1, big, t), vec![4]);
        assert_eq!(x.pull_at(1, big, t + 1), Vec::<u32>::new());
        assert_eq!(ball.value_at(4, t), Some(big), "球の糸もデッキの帳面と同じく前の面を答える");
        assert_eq!(ball.value_at(4, t + 1), None);
        assert_eq!(
            ball.faces_of(4),
            vec![FaceView { value: big, from: t, to: t + 1 }, FaceView { value: 9, from: t + 2, to: OPEN }]
        );
    }

    /// 大きい値の差分は値ごとの帳面でなく列 1 本に足し、 同じ値の糸でつなぐ。 同じ列に落ちる値 (下位 bit が同じ) が
    /// 交互に書かれても、 値ごとの答えは混ざらない。
    #[test]
    fn wide_values_sharing_a_stripe_keep_their_own_threads() {
        let mut x = T::new();
        x.b.declare(0, |_| {});
        let vals: Vec<u64> = (0..4).map(|k| (1u64 << 30) + k * WIDE as u64).collect(); // 全部同じ列
        let mut log: Vec<(u64, BTreeMap<u32, u64>)> = vec![(0, BTreeMap::new())];
        let mut r = 0x2545_F491_4F6C_DD1Du64;
        for _ in 0..400 {
            r ^= r << 13;
            r ^= r >> 7;
            r ^= r << 17;
            let e = (r % 6) as u32;
            match (r >> 8) % 5 {
                4 => x.rm(0, e),
                k => x.set(0, e, vals[k as usize]),
            }
            let snap: BTreeMap<u32, u64> = x.cur.iter().map(|(&(_, e), &v)| (e, v)).collect();
            log.push((x.b.now(), snap));
        }
        for (t, snap) in &log {
            for &v in &vals {
                let want: Vec<u32> = snap.iter().filter(|&(_, &x)| x == v).map(|(&e, _)| e).collect();
                assert_eq!(x.pull_at(0, v, *t), want, "t={t} v={v}");
            }
        }
    }

    /// 大きい値の場所を取って書かなかった (同じ値 / 名簿に書けなかった) 時は取り消す: 差分も糸の頭も残らず、 その後の
    /// 書き換えの糸は取る前の続きにつながる (古い値と新しい値が同じ列 / 違う列 / 片方だけ大きい値、 の 3 通り)。
    #[test]
    fn unused_wide_reservations_are_taken_back() {
        let mut x = T::new();
        x.b.declare(0, |_| {});
        let (a, b, c) = (1u64 << 30, (1u64 << 30) + WIDE as u64, (1u64 << 30) + 1); // a と b は同じ列
        x.set(0, 1, a);
        let t1 = x.b.now();
        for (old, new) in [(Some(a), Some(b)), (Some(a), Some(c)), (Some(a), Some(5)), (Some(a), None), (None, Some(b))] {
            let r = x.b.get(0).unwrap().reserve(1, old, new);
            drop(r);
        }
        assert_eq!(x.b.get(0).unwrap().diff_count(), 1, "取り消した場所は残らない");
        x.set(0, 1, b);
        let t2 = x.b.now();
        x.set(0, 2, a);
        x.set(0, 1, a);
        assert_eq!(x.b.get(0).unwrap().diff_count(), 6);
        assert_eq!(x.pull_at(0, a, t1), vec![1]);
        assert_eq!(x.pull_at(0, a, t2), Vec::<u32>::new());
        assert_eq!(x.pull_at(0, b, t2), vec![1]);
        assert_eq!(x.pull_at(0, a, x.b.now()), vec![1, 2]);
        assert_eq!(x.pull_at(0, b, 0), Vec::<u32>::new());
        assert_eq!(x.pull_at(0, c, 0), Vec::<u32>::new());
    }

    /// 1 つの球が何度も出入りする: t より後に最初に入った / 出た、 で t の時の向きが決まる。
    #[test]
    fn many_round_trips_of_one_ball() {
        let mut x = T::new();
        x.b.declare(0, |_| {});
        let mut log: Vec<(u64, bool)> = Vec::new(); // (時刻, 7 が値 1 か)
        for i in 0..50u64 {
            let v = if i % 3 == 0 { 1 } else { 2 };
            x.set(0, 7, v);
            log.push((x.b.now(), v == 1));
            if i % 7 == 0 {
                x.rm(0, 7);
                log.push((x.b.now(), false));
            }
        }
        for t in 0..=x.b.now() + 1 {
            let k = log.partition_point(|&(ts, _)| ts <= t);
            let want = if k > 0 && log[k - 1].1 { vec![7] } else { vec![] };
            assert_eq!(x.pull_at(0, 1, t), want, "t={t}");
        }
    }

    /// 当て方 2 通り (bit の表 / 並べて合わせる) × 向き 2 つ (今から逆に / 始まりから前向きに) を、 手で決めた答え (始まりの写しに
    /// 頭から cut 個の差分を当てたもの) と比べる。
    /// 差分は手元の写しから作るので、 入った / 出たは球ごとに交互、 今のデッキは差分の後の写し (並びを崩して渡す)。 eid の狭い /
    /// 広い散らばりを混ぜる。
    #[test]
    fn both_roll_backs_agree_with_the_first_diff_after_t() {
        let mut r = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            r ^= r << 13;
            r ^= r >> 7;
            r ^= r << 17;
            r
        };
        // Miri は小さく (bit の表の読み出しの unsafe を通すため。 表の道は 1024 未満でも直に呼ぶ)
        let (cases, big) = if cfg!(miri) { (6, 200) } else { (60, 3000) };
        for case in 0..cases {
            let spread: u64 = [64, 5000, 1 << 31][case % 3];
            let base = (next() % (u32::MAX as u64 - spread)) as u32;
            let n = 50 + (next() % big) as usize;
            let mut cur: BTreeMap<u32, bool> = BTreeMap::new();
            for _ in 0..n {
                cur.insert(base + (next() % spread) as u32, true);
            }
            let start = cur.clone();
            let mut after = Vec::new();
            for t in 1..=(next() % (big as u64 * 4 / 3)) {
                let e = if next() % 2 == 0 && !cur.is_empty() {
                    *cur.keys().nth((next() % cur.len() as u64) as usize).unwrap()
                } else {
                    base + (next() % spread) as u32
                };
                let inside = cur.get(&e).copied().unwrap_or(false);
                cur.insert(e, !inside);
                after.push(Diff { t, eid: e, into: !inside as u32 });
            }
            let mut now: Vec<u32> = cur.iter().filter(|x| *x.1).map(|x| *x.0).collect();
            if case % 2 == 0 {
                now.reverse();
            } else {
                let k = now.len() / 3;
                now.rotate_left(k);
            }
            // 時点 t = cut 番目の差分の時刻: t より後の差分だけを渡し (帳面と同じく、 入った bit は頭から数える)、 答えは写しに
            // 頭から cut 個当てたもの。 cut は bit の語の境目の前後も
            let buf = TailBuf::new(after.iter().map(|d| (d.eid, d.into == 1)));
            let n = after.len();
            for cut in [0, 63.min(n), 64.min(n), 65.min(n), n / 3, n / 2, n] {
                let tail = buf.tail(cut);
                let mut mid: BTreeMap<u32, bool> = start.clone();
                for d in &after[..cut] {
                    mid.insert(d.eid, d.into == 1);
                }
                let want: Vec<u32> = mid.iter().filter(|x| *x.1).map(|x| *x.0).collect();
                let mut sorted = now.clone();
                sorted.sort_unstable();
                assert_eq!(apply_merge(&sorted, tail, true), want, "merge case {case} cut {cut}");
                let (lo, hi) = now.iter().chain(tail.eids).fold((u32::MAX, 0), |(lo, hi), &e| (lo.min(e), hi.max(e)));
                if lo <= hi && hi - lo < 1 << 24 {
                    let words = ((hi - lo) as usize >> 6) + 1;
                    assert_eq!(apply_bits(&now, tail, lo, words, true), want, "bits case {case} cut {cut}");
                }
                assert_eq!(from_now(now.clone(), tail), want, "from_now case {case} cut {cut}");
                // 前向き: 始まりのデッキから頭の cut 個を当てる (控えから進める道)
                let start_deck: Vec<u32> = start.iter().filter(|x| *x.1).map(|x| *x.0).collect();
                let head = buf.range(0, cut);
                assert_eq!(apply_merge(&start_deck, head, false), want, "merge fwd case {case} cut {cut}");
                let (lo, hi) = start_deck.iter().chain(head.eids).fold((u32::MAX, 0), |(lo, hi), &e| (lo.min(e), hi.max(e)));
                if lo <= hi && hi - lo < 1 << 24 {
                    let words = ((hi - lo) as usize >> 6) + 1;
                    assert_eq!(apply_bits(&start_deck, head, lo, words, false), want, "bits fwd case {case} cut {cut}");
                }
                assert_eq!(apply(&start_deck, head, false), want, "apply fwd case {case} cut {cut}");
                // 前向きを途中から: cut の時のデッキから [cut..n) を当てると最後のデッキ (入った bit は頭から数える = skip > 0)
                let end_deck: Vec<u32> = cur.iter().filter(|x| *x.1).map(|x| *x.0).collect();
                assert_eq!(apply(&want, buf.range(cut, n), false), end_deck, "apply fwd from cut case {case} cut {cut}");
                assert_eq!(apply_merge(&want, buf.range(cut, n), false), end_deck, "merge fwd from cut case {case} cut {cut}");
            }
        }
    }

    /// 時刻ごとのデッキの控え: 宣言の時のデッキ (帳面の頭) と、 差分がたまるたびに作る控えの、 どれから当てても (控えの前の
    /// 時点へは逆に、 後の時点へは前向きに) 手元の写しと同じ答え。 宣言の前の値も、 大きい値も混ぜる。
    #[test]
    fn every_snapshot_agrees_with_a_log() {
        let mut x = T::with_snap_min(8);
        let vals: [u64; 6] = [0, 1, 2, 3, 7, 1 << 30];
        for e in 0..20u32 {
            x.cur.insert((0, e), vals[e as usize % 4]);
        }
        let seed: Vec<(u32, u64)> = x.cur.iter().map(|(&(_, e), &v)| (e, v)).collect();
        x.b.declare(0, |f| seed.iter().for_each(|&(e, v)| f(e, v)));
        let mut log: Vec<(u64, BTreeMap<u32, u64>)> = vec![(0, x.cur.iter().map(|(&(_, e), &v)| (e, v)).collect())];
        let mut r = 0x2545_F491_4F6C_DD1Du64;
        for _ in 0..3000 {
            r ^= r << 13;
            r ^= r >> 7;
            r ^= r << 17;
            let e = (r % 40) as u32;
            match (r >> 8) % 8 {
                6 | 7 => x.rm(0, e),
                k => x.set(0, e, vals[k as usize]),
            }
            log.push((x.b.now(), x.cur.iter().map(|(&(_, e), &v)| (e, v)).collect()));
        }
        let ball = x.b.get(0).unwrap();
        let (registered, built) = ball.snap_counts();
        assert!(built > 50, "控えの道を通っていない: {built}");
        assert_eq!(registered, built, "登録した控えは全部作る");
        for (t, snap) in log.iter().step_by(29) {
            for &v in &vals {
                let want: Vec<u32> = snap.iter().filter(|&(_, &x)| x == v).map(|(&e, _)| e).collect();
                for (way, got) in ball.pull_at_every(v, *t, x.now(0, v)) {
                    assert_eq!(got, want, "t={t} v={v} {way}");
                }
            }
        }
    }

    /// 時点 t の読みは、 今と控えのうち差分の数で一番近いところから当てる (同じ近さなら控え)。 答えはどこからでも同じなので、
    /// 選び方は答えでなく選んだ場所で見る。
    #[test]
    fn reads_start_from_the_nearest_snapshot() {
        let mut x = T::with_snap_min(10);
        x.b.declare(0, |_| {});
        // 値 1 の帳面: 入る / 出るを繰り返す (1 回の書き換えで 1 差分)。 控えは 10, 20, ... (デッキは小さいので間隔は 10)
        let mut ts = vec![0u64];
        for i in 0..46u32 {
            if i % 2 == 0 {
                x.set(0, i / 2, 1);
            } else {
                x.rm(0, i / 2);
            }
            ts.push(x.b.now());
        }
        let ball = x.b.get(0).unwrap();
        let log = ball.logs.get(1).and_then(|l| l.get()).unwrap();
        let guard = epoch::pin();
        let pos: Vec<usize> = (0..log.snaps.get().unwrap().len()).map(|j| log.snaps.get().unwrap().get(j).pos.load(Ordering::Relaxed)).collect();
        assert_eq!(pos, vec![10, 20, 30, 40]);
        // 位置 k (= k 個目の差分の時刻) → 選ぶ場所 (None = 今 = 46)。 43 は控え 40 と今が同じ近さ
        for (k, want) in [(0, Some(0)), (4, Some(0)), (5, Some(0)), (6, Some(10)), (15, Some(10)), (16, Some(20)), (42, Some(40)), (43, Some(40)), (44, None), (46, None)] {
            assert_eq!(log.choose(ts[k], &guard).map(|c| c.1), want, "k={k}");
            if let Some((kk, _, _)) = log.choose(ts[k], &guard) {
                assert_eq!(kk, k);
            }
        }
    }

    /// 控えの間隔は max(下限, 前の控えの大きさ): デッキが大きい値は控えの間を広げる (控えは 1 差分あたり 4 B 以下)。
    #[test]
    fn snapshot_interval_follows_the_deck_size() {
        let mut x = T::with_snap_min(4);
        for e in 0..30u32 {
            x.cur.insert((0, e), 1);
        }
        x.b.declare(0, |f| (0..30).for_each(|e| f(e, 1)));
        // 値 1 から 30..40 を出し入れする (デッキは 30〜40)
        for i in 0..200u32 {
            let e = 30 + i % 10;
            if (i / 10) % 2 == 0 {
                x.set(0, e, 1);
            } else {
                x.rm(0, e);
            }
        }
        let log = x.b.get(0).unwrap().logs.get(1).and_then(|l| l.get()).unwrap();
        let list = log.snaps.get().unwrap();
        let mut prev = (0, 30);
        for j in 0..list.len() {
            let s = list.get(j);
            let (pos, size) = (s.pos.load(Ordering::Relaxed), s.eids.get().unwrap().len());
            assert_eq!(pos - prev.0, 4usize.max(prev.1), "控え {j}");
            prev = (pos, size);
        }
        assert!(list.len() >= 4);
    }

    /// 書き手 4 本が自分の entity 8 個に、 同じ値の組 (0..5) を書いたり外したりする。 回ごとに Barrier でそろえ、 そろった時刻を
    /// 外から控える (= その時刻の答えは手元の写しで決まる、 球の付けた時刻を使わない)。 最後に、 全部の回の時刻でデッキの答えと
    /// 球の答えを写しと比べる (今のデッキは最後の写しから渡す)。 回の途中の時刻は、 デッキの答えを球の面と突き合わせる。
    /// 名簿の鍵は試験が持つ (`Locks`、 値ごと)。
    #[test]
    fn concurrent_writers_agree_with_a_log() {
        const TH: u32 = 4;
        const PER: u32 = 8;
        const VALS: u64 = 5;
        let b = Balls::new();
        b.set_snap_min(16); // 控えを作る書き手と、 帳面に足す書き手 / 読み手が重なる
        b.declare(0, |_| {});
        let locks = Locks::new(VALS as usize);
        let rounds = if cfg!(miri) { 4 } else { 300 };
        let bar = std::sync::Barrier::new(TH as usize + 1);
        let log = std::sync::Mutex::new(vec![None::<u64>; (TH * PER) as usize]);
        let mut checks: Vec<(u64, Vec<Option<u64>>)> = Vec::new();
        std::thread::scope(|s| {
            for th in 0..TH {
                let (b, bar, log, locks) = (&b, &bar, &log, &locks);
                s.spawn(move || {
                    let mut r = 0x9E37_79B9_7F4A_7C15u64 ^ th as u64;
                    let mut mine = vec![None::<u64>; PER as usize];
                    for _ in 0..rounds {
                        for _ in 0..PER * 3 {
                            r ^= r << 13;
                            r ^= r >> 7;
                            r ^= r << 17;
                            let k = (r % PER as u64) as usize;
                            let e = th * PER + k as u32;
                            let new = match (r >> 8) % (VALS + 1) {
                                VALS => None,
                                v => Some(v),
                            };
                            locks.record(b, 0, e, mine[k], new);
                            mine[k] = new;
                        }
                        log.lock().unwrap()[(th * PER) as usize..((th + 1) * PER) as usize].copy_from_slice(&mine);
                        bar.wait();
                        bar.wait();
                    }
                });
            }
            for _ in 0..rounds {
                bar.wait();
                checks.push((b.now(), log.lock().unwrap().clone()));
                bar.wait();
            }
        });
        let ball = b.get(0).unwrap();
        let last = checks.last().unwrap().1.clone();
        for (r, (t, want)) in checks.iter().enumerate() {
            for v in 0..VALS {
                let deck: Vec<u32> = (0..TH * PER).filter(|&e| want[e as usize] == Some(v)).collect();
                let now: Vec<u32> = (0..TH * PER).filter(|&e| last[e as usize] == Some(v)).collect();
                // 全部の控えから当てるのは 15 回に 1 回 (控えの数 × 帳面の長さかかる)、 他は選んだところから
                if r % 15 == 0 {
                    for (way, got) in ball.pull_at_every(v, *t, now) {
                        assert_eq!(got, deck, "回 {r} t={t} v={v} {way}");
                    }
                } else {
                    assert_eq!(ball.pull_at(v, *t, || now.clone()), deck, "回 {r} t={t} v={v}");
                }
            }
            for e in 0..TH * PER {
                assert_eq!(ball.value_at(e, *t), want[e as usize], "回 {r} t={t} e={e}");
            }
        }
        let (registered, built) = ball.snap_counts();
        assert!(built > 100, "控えの道を通っていない: {built}");
        assert_eq!(registered, built, "登録した控えは全部作る");
        // 回の途中の時刻も: デッキの帳面の答え = 球の面から集めた答え (球の面は行ごとに時刻の順。 帳面の並びが時刻の順から
        // 崩れると、 同じ回の中の 2 つの書き込みの間の時刻で食い違う)。 最後の 40 回の時刻を全部
        let end = b.now();
        let from = checks[checks.len().saturating_sub(41)].0;
        for t in from..=end {
            for v in 0..VALS {
                let now: Vec<u32> = (0..TH * PER).filter(|&e| last[e as usize] == Some(v)).collect();
                let faces: Vec<u32> = (0..TH * PER).filter(|&e| ball.value_at(e, t) == Some(v)).collect();
                assert_eq!(ball.pull_at(v, t, || now.clone()), faces, "t={t} v={v}");
            }
        }
    }
}
