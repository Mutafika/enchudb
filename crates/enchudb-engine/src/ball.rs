//! 球 (試作、 branch `proto/ball-history`): 宣言した紐の値の履歴。
//!
//! 球は entity ごとに 1 つ。 書き換えると回って別の面が上を向く (球はどこにも動かない)。 デッキ = その値の面を上にしている
//! 球の集まり (見え方)。 円柱の bucket は、 その球を見つけるための名簿 (master のまま: 回って外れた行は印か Column で飛ばし、
//! 清書 = 詰め直しで捨てる)。 履歴は 2 か所に書く:
//!
//! - **球の面** (`Face`): (値, 上を向いた時刻 `from`, 外れた時刻 `to`)。 同じ球の 1 つ前の面へ糸 (`prev`) と、 もっと前の面へ
//!   飛び糸 (`jump`) を張る。 「時点 t の値」 ([`Ball::value_at`]) は今の面から飛び糸と糸をたどる (面の数 n に O(log n) 歩、
//!   [`skip_gap`])
//! - **デッキの差分の帳面**: 値ごとに 「時刻 t に球 X が入った / 出た」 を書き足す。 時刻は帳面の書き手の鍵の中で取るので、
//!   値ごとの差分の並びはそのまま時刻の順。 「時点 t にこの値だった球」 ([`Ball::pull_at`]) は、 今のデッキから t より後の差分を
//!   新しい方から逆に当てて作る (入った → 抜く、 出た → 足す。 最後に当たる = t より後の最初の差分が t の時の向きを決め、
//!   差分の無い球は今と同じ)。 手間は 「t より後の差分の数」 に比例
//!   - `LOG_DENSE` 未満の値は値ごとの帳面 (`DiffLog`)。 差分は時刻 / eid / 入った bit の列 3 本に分けて詰める: 戻す時に
//!     読むのは eid と bit (1 差分 4 B + 1 bit)、 時刻は t の二分探索だけが読む (1 つの構造体 16 B に並べていた時は、 真ん中の
//!     時点の読みの大半が cache の外から差分を読む分だった)
//!   - それより大きい値 (時刻 / ID の列: 1 つの値の差分が 1〜2 個) は値ごとに帳面を作らず、 値の下位 bit で分けた
//!     `WIDE` 本の列 (`WideStripe`) の末尾に足し、 溜まったら値の順の run に並べる (`SparseRuns` と同じ LSM の形。 読みは
//!     run ごとの二分探索と末尾)。 列への書き足しは名簿の鍵の**外**で済ませる: 名簿の鍵を取る前に列の lock を取って場所を取り
//!     ([`Ball::reserve`]、 時刻は未定)、 名簿の鍵の中では時刻を書き込むだけ。 列の lock は名簿を書き換え終わるまで持つ
//!     (読み手は列の lock の下で読むので、 時刻の未定の差分は見えない)。 sparse の値の名簿の鍵は紐 1 本の write_lock なので、
//!     その中で列を触ると書き手が並ぶ (書き手 4 本で書き換え 1 回 +950 ns だった)
//!
//! 同じ値の書き直しは球を回さない (面も差分も足さない)。 外す (untie / delete) は今の面を閉じ、 「出た」 を書く。
//!
//! ## 順序
//! 値ごとの帳面の書き手 1 本の約束は、 名簿 (円柱) の書き手の鍵がそのまま守る: engine は行の lock の下で
//! `HimoStore::set_noting` / `remove_noting` を呼び、 HimoStore が古い値と新しい値の名簿の鍵 (値の鍵か write_lock) を
//! 取った中で、 名簿と列を書き換える**前に** [`Ball::note`] が時刻を取って帳面に書き足す (len を Release)。 鍵を離した後、
//! 行の lock の下で [`Ball::turn`] が球を回す。 同じ値に触る書き手は、 その値が配列の中ならその値の鍵、 sparse なら write_lock
//! を必ず持ち、 配列を伸ばす書き手は write_lock と全部の鍵を持つので、 同じ値の帳面の書き手は 1 本ずつ並ぶ (帳面の並び = 時刻の順)。
//! 大きい値の列は、 同じ球の書き手が行の lock で並び、 列の lock を場所を取ってから時刻を書くまで持つので、 足した順 (run の中の
//! 同じ値の並び) は同じ球の中では時刻の順 (違う球の間は問わない: 当てる時に決めるのは球ごとの最初 / 最後の差分)。 鍵の順は
//! 列の lock (番号の小さい方から) → 名簿の鍵。
//! 宣言 ([`Balls::declare`]) は円柱を先に組む (組んでいない紐の書き手は鍵を取らない。 組んだ印は戻らない)。
//!
//! 読み手は今のデッキを先に読み、 その後で帳面を読む (Acquire)。 読み手が名簿の変化を見たなら、 その前に書いた差分も見える。
//! 名簿の変化をまだ見ていない時に差分だけ見えても、 入った / 出たを集合として当てるので答えは同じ。
//!
//! ## 試作の範囲
//! - 球はメモリの上。 changefeed の記録 (`enable_change_log`) のある DB は、 球にした紐ごとに土台の列 (`crate::ball_base`、 記録の
//!   始まりの時点の列) を disk に置く = 宣言を保存する。 開き直した後は、 最初に昔を読む時に土台 + 記録の残り + WAL の残りから
//!   書き手を止めずに組む ([`Balls::begin_publish`] の約束)。 記録の無い DB は開き直すと消える (宣言し直す)
//! - 地平 ([`Ball::horizon`]: 宣言の時刻、 組み直した球は土台の時点) より前の時点は [`TooOld`]
//! - 時刻は書き込みの WAL の HLC を詰めたもの ([`pack_hlc`]、 帳面と球の順を保つ分だけ上げる)。 WAL の無い書き込みは engine の中の
//!   通し番号 ([`Clock`])。 宣言の前からある値の面は `from = 0` (「前から」、 差分は書かない)
//! - 宣言 ([`Balls::declare`]) は書き手の居ない時に
//! - 球は eid ごと: delete は今の面を閉じるだけで、 同じ eid を使い回した次の entity の面は前の entity の面の続きに付く
//! - 読みが時刻 t = 今 の時、 同時に書いている entity は前の向きに見えうる。 過去の t は、 その時刻までの書き込みが返った後なら揺れない
//! - 大きい値の列は lock で読む (読む間、 同じ列に足す書き手は待つ)

use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned};
use std::cell::{Cell, UnsafeCell};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};
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
    /// この thread の、 付けた時刻の置き場の番号 (`usize::MAX` = まだ)。
    static CLOCK_SLOT: Cell<usize> = const { Cell::new(usize::MAX) };
    /// 今の書き込みで HLC より上げた時刻 (0 = 上げていない)。 engine が WAL の時計に渡す ([`take_raised`])。
    static RAISED: Cell<u64> = const { Cell::new(0) };
}

/// HLC (wall ms, logical) を球の時刻 (u64) に詰める: wall << 23 | logical (logical は 2^23 未満に切る。 1 ms に 800 万回は書かない)。
/// wall 41 bit で 2039 年 + 約 70 年。 HLC::ZERO は 0 (= 時刻なし、 数え口を使う)。
pub(crate) fn pack_hlc(wall_ms: u64, logical: u32) -> u64 {
    wall_ms << 23 | (logical as u64).min((1 << 23) - 1)
}

/// [`pack_hlc`] の逆: (wall ms, logical)。
pub(crate) fn unpack_hlc(t: u64) -> (u64, u32) {
    (t >> 23, (t & ((1 << 23) - 1)) as u32)
}

/// 深さ `d` (最初の面 = 0) の面の飛び幅: 飛び先は深さ d − 幅。 幅 = d を 2^k − 1 の和に大きい方から分けた時のいちばん小さい項
/// (skew binary の飛び糸、 Myers 1983 "An applicative random-access stack")。 幅 1 なら飛び先は親、 そうでなければ親の飛び先の飛び先
/// (書く時に余分に読む面は高々 1 つ、 幅は深さだけで決まるので面に深さを置かない)。 前へたどる時に 「飛び先も t より後なら飛ぶ、
/// そうでなければ 1 つ戻る」 で、 面の数 n に O(log n) 歩。
#[inline]
fn skip_gap(mut d: u32) -> u32 {
    let mut w = 0;
    while d > 0 {
        w = ((1u64 << (63 - (d as u64 + 1).leading_zeros())) - 1) as u32;
        d -= w;
    }
    w
}

/// 時点が球の地平より前 (宣言の前 / 記録が捨てた所より前): その時点の値はもう分からない。 `horizon` 以降なら答える。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TooOld {
    pub horizon: u64,
}

impl std::fmt::Display for TooOld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "the ball keeps no history before {}", self.horizon)
    }
}

impl std::error::Error for TooOld {}

/// 今の書き込みで HLC より上げた時刻 (0 = 上げていない) を取り出す。 上げたら、 書き込みが返る前に (行の lock の中で) WAL の時計を
/// そこまで進めること: でないと、 この後に払い出す HLC が上げた時刻以下になり、 「今」 より後の書き込みが過去の時点に見える
/// (過去の答えが後から変わる)。
pub(crate) fn take_raised() -> u64 {
    RAISED.with(|c| c.replace(0))
}

/// 置き場 1 つ (cache line ごと、 書き手どうしで取り合わない)。
#[repr(align(128))]
#[derive(Default)]
struct Padded(AtomicU64);

/// 置き場の数。
const CLOCK_SLOTS: usize = 64;
/// 置き場の番号を thread に配る数え口 (thread を作る時に 1 回だけ)。
static NEXT_CLOCK_SLOT: AtomicUsize = AtomicUsize::new(0);

/// 球の時刻。 HLC の無い書き込み (standalone) は数え口 `counter` (書き手全員で 1 つ、 名簿の鍵の中で取るので帳面の順 = 時刻の順)。
/// HLC のある書き込みは HLC を使い、 帳面と球の時刻の順を保つ分だけ上げる ([`Ball::note`])。 HLC から付けた時刻と、 上げた時刻は
/// 書き手ごとの置き場に置く (今の時刻 = 数え口と置き場の最大、 書き手は自分の置き場に書くだけ)。
pub(crate) struct Clock {
    counter: AtomicU64,
    last: Box<[Padded]>,
}

impl Clock {
    fn new() -> Self {
        Self { counter: AtomicU64::new(0), last: (0..CLOCK_SLOTS).map(|_| Padded::default()).collect() }
    }

    /// 書き込みの時刻の元: HLC (`hlc`、 [`pack_hlc`]) があればそれ、 無ければ (0) 数え口を 1 進めた値。 返り値の 2 つ目は HLC から取ったか。
    #[inline]
    fn base(&self, hlc: u64) -> (u64, bool) {
        match hlc {
            0 => (self.counter.fetch_add(1, Ordering::Relaxed) + 1, false),
            h => (h, true),
        }
    }

    /// 付けた時刻を自分の置き場に置く。
    #[inline]
    fn publish(&self, ts: u64) {
        let i = CLOCK_SLOT.with(|c| {
            if c.get() == usize::MAX {
                c.set(NEXT_CLOCK_SLOT.fetch_add(1, Ordering::Relaxed) % CLOCK_SLOTS);
            }
            c.get()
        });
        // 置き場を共有する thread (65 本目から) と取り合っても最大は残る
        self.last[i].0.fetch_max(ts, Ordering::Relaxed);
    }

    /// 今の時刻 (付けた時刻の最大)。 この後の書き換えは、 これより大きい時刻になる (HLC の書き込みは HLC が進む限り)。
    fn now(&self) -> u64 {
        self.last.iter().map(|p| p.0.load(Ordering::Relaxed)).fold(self.counter.load(Ordering::Relaxed), u64::max)
    }
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
        // 並びは `<[_]>::get` で引かない: 並び全体 (OnceLock 65536 個) の参照を作り、 Miri (Tree Borrows) は 1 回ごとに中の
        // UnsafeCell を全部たどる (1 回 0.3 秒、 添字で引けば 4〜20 ms)。 長さは raw の参照で見る。 release の機械語は同じ
        let k = (i >> CHUNK_BITS) as usize;
        if k >= (&raw const *self.chunks).len() {
            return None;
        }
        let c = self.chunks[k].get()?;
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
    /// 同じ球のもっと前の面 (飛び糸、 [`skip_gap`]。 `NONE` = 最初の面)。
    jump: AtomicU32,
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

    /// 最後の差分の時刻 (無ければ 0)。 書き手 (名簿の鍵の下) が次の時刻を決めるのに読む。
    #[inline]
    fn last_t(&self, guard: &Guard) -> u64 {
        self.read(guard, |t, _, _| t.last().copied().unwrap_or(0))
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

/// 大きい値の差分 1 つ (24 B)。 `into` = 入った (1) / 出た (0)。
#[derive(Clone, Copy)]
struct WideDiff {
    v: u64,
    t: u64,
    eid: u32,
    into: u32,
}

/// 大きい値の差分の末尾が何件で run にするか。 読みは末尾をなめるので小さく保つ。
const WIDE_TAIL: usize = 256;
/// 大きい値の差分の run の大きさの比 (`SparseRuns` と同じ)。
const WIDE_FANOUT: usize = 4;

/// 大きい値の差分の列 1 本 (値の下位 bit で選ぶ)。 値ごとの帳面の代わりに、 全部の値の差分を値の順の run に置く: 書き手は末尾
/// (`tail`) に足すだけで、 `WIDE_TAIL` 件溜まったら値で安定に並べて run にする。 run は大きい (古い) 順で、 足した run の
/// `WIDE_FANOUT` 倍より小さい末尾の run とは併合を繰り返す (同じ値は古い run を先に)。 同じ球の書き手は行の lock で並び、 列の
/// lock の下で足すので、 足した順は同じ球の中では時刻の順 = run の中の同じ値の並びも同じ球の中では時刻の順 (読みが要るのは
/// 球ごとの順だけ、 [`apply`])。 16 回目: 前は値 → 糸の頭の `BTreeMap` を引いて同じ値の糸でつないでいた。 値の種類の多い列は
/// 書き換えのたびに新しい鍵を足すので表が伸び続け、 書き換え 1 回に約 285 ns (値の種類とともに伸びる) かかっていた。
#[derive(Default)]
struct WideStripe {
    runs: Vec<WideRun>,
    tail: Vec<WideDiff>,
    /// `tail` の値だけの並び (読みは末尾を値で比べて回る。 1 件 24 B の並びを回ると 1 回 250 ns ほどかかった)。
    tail_vals: Vec<u64>,
}

/// 値で並べた run。 値 / 時刻 / 球 / 入った は別の並び (二分探索は値の並びだけを触る。 1 件 24 B の並びのままだと、 読みが
/// 前の表より 1 回 300 ns ほど遅かった)。
#[derive(Default)]
struct WideRun {
    vals: Vec<u64>,
    ts: Vec<u64>,
    eids: Vec<u32>,
    into: Vec<u8>,
}

impl WideRun {
    fn with_capacity(n: usize) -> Self {
        Self { vals: Vec::with_capacity(n), ts: Vec::with_capacity(n), eids: Vec::with_capacity(n), into: Vec::with_capacity(n) }
    }

    fn len(&self) -> usize {
        self.vals.len()
    }

    /// 末尾を値で安定に並べて run にする (同じ値は足した順のまま)。
    fn from_tail(mut tail: Vec<WideDiff>) -> Self {
        tail.sort_by_key(|d| d.v);
        let mut r = Self::with_capacity(tail.len());
        for d in tail {
            r.vals.push(d.v);
            r.ts.push(d.t);
            r.eids.push(d.eid);
            r.into.push(d.into as u8);
        }
        r
    }

    /// 2 本を併合する (同じ値は `older` を先に)。
    fn merge(older: &WideRun, newer: &WideRun) -> WideRun {
        let mut out = Self::with_capacity(older.len() + newer.len());
        let take = |out: &mut WideRun, r: &WideRun, k: usize| {
            out.vals.push(r.vals[k]);
            out.ts.push(r.ts[k]);
            out.eids.push(r.eids[k]);
            out.into.push(r.into[k]);
        };
        let (mut i, mut j) = (0, 0);
        while i < older.len() && j < newer.len() {
            if older.vals[i] <= newer.vals[j] {
                take(&mut out, older, i);
                i += 1;
            } else {
                take(&mut out, newer, j);
                j += 1;
            }
        }
        for (r, k) in [(older, i), (newer, j)] {
            out.vals.extend_from_slice(&r.vals[k..]);
            out.ts.extend_from_slice(&r.ts[k..]);
            out.eids.extend_from_slice(&r.eids[k..]);
            out.into.extend_from_slice(&r.into[k..]);
        }
        out
    }

    fn bytes(&self) -> usize {
        self.vals.capacity() * 8 + self.ts.capacity() * 8 + self.eids.capacity() * 4 + self.into.capacity()
    }
}

impl WideStripe {
    /// 末尾が一杯なら run にする。 列の lock を取った直後 (時刻の未定の差分が無い時) に呼ぶ。
    fn make_room(&mut self) {
        if self.tail.len() < WIDE_TAIL {
            return;
        }
        let mut run = WideRun::from_tail(std::mem::replace(&mut self.tail, Vec::with_capacity(WIDE_TAIL)));
        self.tail_vals.clear();
        while self.runs.last().is_some_and(|r| r.len() < run.len() * WIDE_FANOUT) {
            let older = self.runs.pop().unwrap();
            run = WideRun::merge(&older, &run);
        }
        self.runs.push(run);
    }

    /// 末尾に足す。 返り値は末尾の中の場所 (時刻を書く / 取り消す)。
    fn push(&mut self, d: WideDiff) -> usize {
        self.tail.push(d);
        self.tail_vals.push(d.v);
        self.tail.len() - 1
    }

    /// 最後に足した差分を取り消す。
    fn pop(&mut self) {
        self.tail.pop();
        self.tail_vals.pop();
    }

    /// 値 `v` の t より後の差分を、 球ごとに時刻の順に (古い run → 新しい run → 末尾)。
    fn after(&self, v: u64, t: u64) -> TailBuf {
        let mut out = Vec::new();
        for r in &self.runs {
            let a = r.vals.partition_point(|&x| x < v);
            let n = r.vals[a..].iter().take_while(|&&x| x == v).count();
            out.extend((a..a + n).filter(|&k| r.ts[k] > t).map(|k| (r.eids[k], r.into[k] == 1)));
        }
        for (k, _) in self.tail_vals.iter().enumerate().filter(|&(_, &x)| x == v) {
            let d = &self.tail[k];
            if d.t > t {
                out.push((d.eid, d.into == 1));
            }
        }
        TailBuf::new(out)
    }

    fn len(&self) -> usize {
        self.runs.iter().map(WideRun::len).sum::<usize>() + self.tail.len()
    }

    fn bytes(&self) -> usize {
        self.runs.iter().map(WideRun::bytes).sum::<usize>() + self.tail.capacity() * std::mem::size_of::<WideDiff>() + self.tail_vals.capacity() * 8
    }
}

/// [`Ball::reserve`] で取った場所 1 つ: (`held` の添字, 列の末尾の中の場所, 値)。
struct Slot {
    held: usize,
    at: usize,
    v: u64,
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
        self.held[s.held].as_mut().unwrap().1.tail[s.at].t = ts;
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
            debug_assert_eq!(g.tail.len(), s.at + 1);
            g.pop();
        }
    }
}

/// 紐 1 本の球の全部 (entity ごとの球 + 値ごとの差分の帳面)。
pub(crate) struct Ball {
    id: u64,
    faces: Chunked<Face>,
    /// 書き手に渡した面の場所の数 (書き手ごとの塊の、 まだ使っていない分を含む)。
    next: AtomicU64,
    /// entity → 今の面 + 1 (下位 32 bit、 0 = 球が無い) | 今の面の深さ (上位 32 bit、 最初の面 = 0。 飛び糸を張るのに使う)。
    heads: Chunked<AtomicU64>,
    /// 値 (`LOG_DENSE` 未満) → 帳面。
    logs: Chunked<OnceLock<Box<DiffLog>>>,
    /// `LOG_DENSE` 以上の値の差分。 値の下位 bit で `WIDE` 本に分ける (違う値の書き手は名簿の鍵では並ばないので lock で守る)。
    wide: Box<[Mutex<WideStripe>]>,
    /// 控えの間隔の下限 (差分の数)。
    snap_min: usize,
    /// 地平: これより前の時点は答えない (宣言の時刻、 組み直した球は土台の時点)。 宣言の前からある値の面は `from = 0` のまま。
    horizon: AtomicU64,
    /// 書き手が球に書く時刻の下限 (書き手を止めずに組んだ球: これより前の書き込みは組む側が当てた。 宣言した球は 0)。
    threshold: AtomicU64,
}

impl Ball {
    fn new(snap_min: usize) -> Self {
        Self {
            horizon: AtomicU64::new(0),
            threshold: AtomicU64::new(0),
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
        // 列の lock は番号の小さい方から (同じ列なら 1 つ)。 書き換えのたびに呼ぶので heap を使わない
        let (mut ks, mut nk) = ([0usize; 2], 0);
        for k in vs.iter().flatten().map(|&v| v as usize % WIDE) {
            if !ks[..nk].contains(&k) {
                ks[nk] = k;
                nk += 1;
            }
        }
        let ks = &mut ks[..nk];
        ks.sort_unstable();
        for (h, &k) in ks.iter().enumerate() {
            let mut g = self.wide[k].lock().unwrap_or_else(|e| e.into_inner());
            // 一杯なら先に run にする (この後に足す差分は時刻が未定で、 並べられない)
            g.make_room();
            r.held[h] = Some((k, g));
        }
        for (i, v) in vs.into_iter().enumerate() {
            let Some(v) = v else { continue };
            let h = ks.iter().position(|&k| k == v as usize % WIDE).unwrap();
            let at = r.held[h].as_mut().unwrap().1.push(WideDiff { v, t: PENDING, eid, into: i as u32 });
            r.slots[i] = Some(Slot { held: h, at, v });
        }
        r
    }

    /// 書き換え `old` → `new` を帳面に記す。 **名簿の鍵の中、 名簿を書き換える前に** 呼ぶ (`HimoStore::set_noting` の `note`):
    /// 時刻を取り (`hlc` = この書き込みの WAL の HLC を [`pack_hlc`] で詰めたもの、 0 = 無い)、 古い値の帳面に 「出た」、 新しい値の帳面に 「入った」 を足す (大きい値は `wide` に取った場所に時刻を書く)。
    /// 返り値は時刻 (同じ値なら何もせず None)。 鍵を離した後、 `wide` から控えを取り出して ([`Reserved::take_snaps`]) 落とし、
    /// 行の lock の下で [`Ball::turn`] にこの時刻を渡し、 控えを [`Ball::build_snaps`] で作ること。
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn note(&self, eid: u32, old: Option<u64>, new: Option<u64>, clock: &Clock, hlc: u64, guard: &Guard, wide: &mut Reserved<'_>) -> Option<u64> {
        if old == new {
            return None;
        }
        // 時刻: HLC (無ければ数え口)。 HLC は名簿の鍵より前に決まるので、 同じ値に書く 2 人の HLC の順と鍵の順はずれうる。
        // 古い値と新しい値の名簿の鍵を持っている今、 両方の帳面の最後とこの球の最後の面より後に上げる (帳面の並び = 時刻の順、
        // 球の面も時刻の順)。 上がるのは同時に書いた書き手の間だけで、 その順は鍵を取った順。 大きい値の差分は値の順の run に
        // 並べるので、 同じ球の中の順 (球の最後の面より後) だけでよい
        let (base, from_hlc) = clock.base(hlc);
        let mut floor = self.last_time(eid);
        for v in [old, new].into_iter().flatten().filter(|&v| v < LOG_DENSE) {
            floor = floor.max(self.logs.get(v as u32).and_then(|l| l.get()).map_or(0, |l| l.last_t(guard)));
        }
        let ts = base.max(floor + 1);
        if from_hlc || ts != base {
            clock.publish(ts);
        }
        if ts != base {
            RAISED.with(|c| c.set(ts));
        }
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
    fn push_face(&self, i: u32, prev: u32, jump: u32, v: u64, from: u64) {
        let f = self.faces.get_or_alloc(i);
        f.jump.store(jump, Ordering::Relaxed);
        f.prev.store(prev, Ordering::Relaxed);
        f.value.store(v, Ordering::Relaxed);
        f.from.store(from, Ordering::Relaxed);
        f.to.store(OPEN, Ordering::Relaxed);
    }

    /// 球を回す: `new` を上に向ける (None = 今の面を閉じるだけ)、 時刻 `ts` ([`Ball::note`] の返り値)。 行の lock の下で呼ぶ。
    pub(crate) fn turn(&self, eid: u32, new: Option<u64>, ts: u64) {
        let head = self.heads.get_or_alloc(eid);
        let h = head.load(Ordering::Relaxed);
        let cur = h as u32;
        if let Some(f) = (cur != 0).then(|| self.face(cur - 1)).filter(|f| f.to.load(Ordering::Relaxed) == OPEN) {
            f.to.store(ts, Ordering::Release);
        }
        if let Some(v) = new {
            let Some(i) = self.alloc() else { return };
            if cur == 0 {
                self.push_face(i, NONE, NONE, v, ts);
                head.store(i as u64 + 1, Ordering::Release);
                return;
            }
            let (p, d) = (cur - 1, (h >> 32) as u32 + 1);
            // 幅 1 なら親、 そうでなければ親の飛び先の飛び先 (深さ d − 幅)
            let jump = if skip_gap(d) == 1 { p } else { self.face(self.face(p).jump.load(Ordering::Relaxed)).jump.load(Ordering::Relaxed) };
            self.push_face(i, p, jump, v, ts);
            head.store((d as u64) << 32 | (i as u64 + 1), Ordering::Release);
        }
    }

    /// この球の最後の時刻 (今の面の上を向いた時刻、 閉じていれば外れた時刻、 球が無ければ 0)。 行の lock の下で。
    #[inline]
    fn last_time(&self, eid: u32) -> u64 {
        let Some(cur) = self.heads.get(eid).map(|h| h.load(Ordering::Relaxed) as u32).filter(|&c| c != 0) else { return 0 };
        let f = self.face(cur - 1);
        match f.to.load(Ordering::Relaxed) {
            OPEN => f.from.load(Ordering::Relaxed),
            to => to,
        }
    }

    /// 宣言の前からある値を 「前から」 (from = 0) の面にする (差分は書かない: t より後の差分の無い球は今と同じ)。
    fn seed(&self, eid: u32, v: u64) {
        let head = self.heads.get_or_alloc(eid);
        if head.load(Ordering::Relaxed) != 0 {
            return;
        }
        let Some(i) = self.alloc() else { return };
        self.push_face(i, NONE, NONE, v, 0);
        head.store(i as u64 + 1, Ordering::Release);
    }

    /// 地平 (これより前の時点の答えは無い)。
    pub fn horizon(&self) -> u64 {
        self.horizon.load(Ordering::Relaxed)
    }

    /// 時点 `t` に答えられるか (地平より前は [`TooOld`])。
    pub fn check(&self, t: u64) -> Result<(), TooOld> {
        let h = self.horizon();
        if t < h { Err(TooOld { horizon: h }) } else { Ok(()) }
    }

    /// 書き換え 1 つを当てる (組み直し: 書き手の居ない時に、 記録の順に)。 書き手の道 (`note` → `turn` → 控え) と同じ。
    /// 時刻は `hlc` ([`pack_hlc`]、 0 = 数え口)、 帳面と球の順を保つ分だけ上げる (上げたら [`take_raised`] が返す)。
    /// 呼び手の pin の下で (組む側が束ごとに 1 回 pin する)。
    pub(crate) fn replay_in(&self, guard: &Guard, clock: &Clock, eid: u32, old: Option<u64>, new: Option<u64>, hlc: u64) -> Option<u64> {
        let mut wide = self.reserve(eid, old, new);
        let ts = self.note(eid, old, new, clock, hlc, guard, &mut wide);
        let snaps = wide.take_snaps();
        drop(wide);
        if let Some(ts) = ts {
            self.turn(eid, new, ts);
        }
        self.build_snaps(snaps);
        ts
    }

    /// 時点 `t` に上を向いていた値。 `from <= t` の最初の面 ([`Ball::face_at`]) が `t` でまだ開いていれば値。
    pub fn value_at(&self, eid: u32, t: u64) -> Option<u64> {
        let f = self.face_at(eid, t).0?;
        (f.to.load(Ordering::Acquire) > t).then(|| f.value.load(Ordering::Relaxed))
    }

    /// 今の面から前へ、 `from <= t` の最初の面 (無ければ None)。 返り値の 2 つ目は通った面の数 (試験用)。 面の `from` は糸の向きに
    /// 減るので、 飛び先の `from` も t より後なら飛んでよい (間の面も全部 t より後)。 そうでなければ 1 つ前へ。
    #[inline]
    fn face_at(&self, eid: u32, t: u64) -> (Option<&Face>, u32) {
        let Some(mut i) = self.heads.get(eid).and_then(|h| (h.load(Ordering::Acquire) as u32).checked_sub(1)) else { return (None, 0) };
        let mut steps = 0;
        loop {
            let f = self.face(i);
            steps += 1;
            if f.from.load(Ordering::Relaxed) <= t {
                return (Some(f), steps);
            }
            let (p, j) = (f.prev.load(Ordering::Relaxed), f.jump.load(Ordering::Relaxed));
            i = if j != p && self.face(j).from.load(Ordering::Relaxed) > t { j } else { p };
            if i == NONE {
                return (None, steps);
            }
        }
    }

    /// 球の面を古い順に。
    pub fn faces_of(&self, eid: u32) -> Vec<FaceView> {
        let mut out = Vec::new();
        let Some(mut i) = self.heads.get(eid).and_then(|h| (h.load(Ordering::Acquire) as u32).checked_sub(1)) else {
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

    /// 帳面がどれも時刻の順か (値ごとの帳面も、 大きい値の糸も)。 試験用。
    #[cfg(test)]
    fn ledgers_in_time_order(&self) -> bool {
        let guard = epoch::pin();
        let mut ok = true;
        self.logs.for_each(|l| {
            if let Some(l) = l.get() {
                ok &= l.read(&guard, |ts, _, _| ts.windows(2).all(|w| w[0] < w[1]));
            }
        });
        // 大きい値の差分: run は値の順、 同じ (値, 球) は足した順 (古い run → 新しい run → 末尾) に時刻の順
        for w in self.wide.iter() {
            let w = w.lock().unwrap_or_else(|e| e.into_inner());
            let mut last: BTreeMap<(u64, u32), u64> = BTreeMap::new();
            for r in &w.runs {
                ok &= r.vals.windows(2).all(|p| p[0] <= p[1]);
            }
            // run の中は値の順に並べ替えてあるので、 (値, 球) ごとに見れば足した順
            let seq = w.runs.iter().flat_map(|r| (0..r.len()).map(move |k| (r.vals[k], r.eids[k], r.ts[k])));
            for (v, eid, t) in seq.chain(w.tail.iter().map(|d| (d.v, d.eid, d.t))) {
                let prev = last.insert((v, eid), t);
                ok &= prev.is_none_or(|p| p < t);
            }
        }
        ok
    }

    /// 大きい値の列ごとの (run の大きさ, 末尾の数)。 試験用。
    #[cfg(test)]
    fn wide_shape(&self) -> Vec<(Vec<usize>, usize)> {
        self.wide
            .iter()
            .map(|w| {
                let w = w.lock().unwrap_or_else(|e| e.into_inner());
                (w.runs.iter().map(WideRun::len).collect::<Vec<_>>(), w.tail.len())
            })
            .filter(|(r, t)| !r.is_empty() || *t > 0)
            .collect()
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
        n + self.wide.iter().map(|w| w.lock().unwrap_or_else(|e| e.into_inner()).len() as u64).sum::<u64>()
    }

    /// 取った大きさ (byte、 塊の表は除く)。
    pub fn bytes(&self) -> usize {
        let guard = epoch::pin();
        let mut logs = 0usize;
        self.logs.for_each(|l| logs += l.get().map_or(0, |l| l.bytes(&guard) + l.snap_bytes()));
        let wide: usize = self
            .wide
            .iter()
            .map(|w| w.lock().unwrap_or_else(|e| e.into_inner()).bytes())
            .sum();
        self.faces.allocated() * CHUNK * std::mem::size_of::<Face>()
            + self.heads.allocated() * CHUNK * 8
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

/// 紐ごとの球の状態 ([`Balls::for_write`])。
const PHASE_NONE: u8 = 0;
/// 土台があり、 まだ組んでいない (開き直した後、 最初に昔を読む時に組む)。
const PHASE_PENDING: u8 = 1;
/// 組む側が閾値を決めている最中 (書き手は組み終えるまで待つ)。
const PHASE_PUBLISHING: u8 = 2;
/// 組んだ (書き手は自分の時刻が閾値以上なら球に書く)。
const PHASE_READY: u8 = 3;

/// engine 1 つの球の全部。 球が 1 つも無ければ書き手は `any` の load 1 回で素通り。
///
/// ## 書き手を止めずに組む ([`Balls::begin_publish`] / [`Balls::publish`])
///
/// 書き手は WAL に積んだ後で [`Balls::for_write`] を呼び、 状態が組んでいない (前) なら球に書かない、 公開中なら組み終えるまで待つ、
/// 組んだなら自分の時刻が閾値 H 以上の時だけ球に書く。 組む側は公開中を置いてから WAL に Commit を積み、 その HLC を H にし、
/// H より前の書き込みを全部当ててから球を出す。 書き手が前の状態を読んだなら、 その書き込みは Commit より先に WAL に積んだ (H より前)
/// = 組む側が当てる。 公開中 / 組んだを読んだ書き手は H と比べる。 H より前は組む側、 H 以降は書き手 = 重ならず漏れない
/// (WAL の append の lock が、 書き手の 「積む → 読む」 と組む側の 「置く → 積む」 を並べる)。
pub(crate) struct Balls {
    any: AtomicBool,
    clock: Clock,
    /// これから宣言する球の控えの間隔の下限 (差分の数)。
    snap_min: AtomicUsize,
    /// 紐の番号 → 球。 最初の宣言で取る。
    slots: OnceLock<Box<[OnceLock<Box<Ball>>]>>,
    /// 紐の番号 → 状態 (`PHASE_NONE` / `PHASE_PENDING` / `PHASE_PUBLISHING` / `PHASE_READY`)。 最初に使う時に取る。
    phases: OnceLock<Box<[AtomicU8]>>,
    /// 数 (試験が場所を踏んだかを見る): 公開中で待った書き手 / 組んだ後に閾値より前で球に書かなかった書き手。
    waits: AtomicU64,
    covered: AtomicU64,
}

impl Balls {
    pub fn new() -> Self {
        Self {
            any: AtomicBool::new(false),
            clock: Clock::new(),
            snap_min: AtomicUsize::new(SNAP_MIN),
            slots: OnceLock::new(),
            phases: OnceLock::new(),
            waits: AtomicU64::new(0),
            covered: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn any(&self) -> bool {
        self.any.load(Ordering::SeqCst)
    }

    /// 今の時刻 (記した書き換えの時刻の最大)。 この後の書き換えは、 これより大きい時刻になる。
    pub fn now(&self) -> u64 {
        self.clock.now()
    }

    /// 球の時刻 ([`Ball::note`] に渡す)。
    #[inline]
    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    #[inline]
    pub fn get(&self, hid: u16) -> Option<&Ball> {
        self.slots.get()?[hid as usize].get().map(|b| &**b)
    }

    fn phases(&self) -> &[AtomicU8] {
        self.phases.get_or_init(|| (0..=u16::MAX as usize).map(|_| AtomicU8::new(PHASE_NONE)).collect())
    }

    #[inline]
    fn phase(&self, hid: u16) -> u8 {
        self.phases.get().map_or(PHASE_NONE, |p| p[hid as usize].load(Ordering::SeqCst))
    }

    /// 書き手 (WAL に積んだ後、 名簿の鍵の前): この書き込み (時刻 `t` = HLC を [`pack_hlc`] で詰めたもの、 WAL の無い書き込みは 0) を
    /// 球に書くか。 組んでいる最中 (公開中) は組み終えるまで待つ。
    #[inline]
    pub fn for_write(&self, hid: u16, t: u64) -> Option<&Ball> {
        let mut waited = false;
        loop {
            match self.phase(hid) {
                PHASE_READY => {
                    let b = self.get(hid)?;
                    if t >= b.threshold.load(Ordering::Relaxed) {
                        return Some(b);
                    }
                    self.covered.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                PHASE_PUBLISHING => {
                    if !waited {
                        waited = true;
                        self.waits.fetch_add(1, Ordering::Relaxed);
                    }
                    std::thread::yield_now()
                }
                _ => return None,
            }
        }
    }

    /// (公開中で待った書き手, 組んだ後に閾値より前で球に書かなかった書き手) の数 (試験用)。
    pub fn publish_counts(&self) -> (u64, u64) {
        (self.waits.load(Ordering::Relaxed), self.covered.load(Ordering::Relaxed))
    }

    /// 紐 `hid` は土台があって、 まだ組んでいない (開いた時に土台の番号から)。
    pub fn mark_pending(&self, hid: u16) {
        let _ = self.phases()[hid as usize].compare_exchange(PHASE_NONE, PHASE_PENDING, Ordering::SeqCst, Ordering::SeqCst);
    }

    /// 組んでいない球をやめる (土台が使えなかった)。
    pub fn unmark_pending(&self, hid: u16) {
        let _ = self.phases()[hid as usize].compare_exchange(PHASE_PENDING, PHASE_NONE, Ordering::SeqCst, Ordering::SeqCst);
    }

    /// まだ組んでいない球か。
    pub fn is_pending(&self, hid: u16) -> bool {
        self.phase(hid) == PHASE_PENDING
    }

    /// 組む側: 公開中を置く (この後で WAL に Commit を積み、 その HLC を閾値にする)。
    pub fn begin_publish(&self, hid: u16) {
        self.phases()[hid as usize].store(PHASE_PUBLISHING, Ordering::SeqCst);
        self.any.store(true, Ordering::SeqCst);
    }

    /// 組む側: やめて組んでいないに戻す (待っていた書き手は球に書かない = 次に組む時に当てる)。
    pub fn abort_publish(&self, hid: u16) {
        self.phases()[hid as usize].store(PHASE_PENDING, Ordering::SeqCst);
    }

    /// 組む側: 閾値 `threshold` (この時刻より前の書き込みは当て終えた) で球を出す。
    pub fn publish(&self, hid: u16, ball: Box<Ball>, threshold: u64) {
        ball.threshold.store(threshold, Ordering::Relaxed);
        let horizon = ball.horizon();
        let slots = self.slots.get_or_init(|| (0..=u16::MAX as usize).map(|_| OnceLock::new()).collect());
        let _ = slots[hid as usize].set(ball);
        if horizon != 0 {
            self.clock.publish(horizon);
        }
        self.phases()[hid as usize].store(PHASE_READY, Ordering::SeqCst);
        self.any.store(true, Ordering::SeqCst);
    }

    /// 組む側: 出す前の手元の球を作る (地平 `horizon` の時点の値 `seed` を種に。 この後で [`Ball::replay_in`] で当てる)。
    pub fn make(&self, horizon: u64, seed: impl FnOnce(&mut dyn FnMut(u32, u64))) -> Box<Ball> {
        let ball = Box::new(Ball::new(self.snap_min.load(Ordering::Relaxed)));
        ball.horizon.store(horizon, Ordering::Relaxed);
        let mut decks: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
        seed(&mut |eid, v| {
            ball.seed(eid, v);
            if v < LOG_DENSE {
                decks.entry(v as u32).or_default().push(eid);
            }
        });
        ball.seed_logs(decks);
        ball
    }

    /// これから宣言する球の控えの間隔の下限 (差分の数、 既定 `SNAP_MIN`)。 試験が小さくして控えの道を通す。
    pub fn set_snap_min(&self, n: usize) {
        self.snap_min.store(n.max(1), Ordering::Relaxed);
    }

    /// 紐 `hid` を球にする (書き手の居ない時に。 今からの書き込みは全部球に書く = 閾値 0)。 `seed` に地平 `horizon` の時点の値
    /// (eid, 値) を全部渡す。 宣言済み (組んだ / 組んでいる) なら false。 名簿 (円柱) は先に組んでおくこと。
    pub fn declare(&self, hid: u16, horizon: u64, seed: impl FnOnce(&mut dyn FnMut(u32, u64))) -> bool {
        if self.get(hid).is_some() || self.phase(hid) == PHASE_PUBLISHING {
            return false;
        }
        let ball = self.make(horizon, seed);
        self.publish(hid, ball, 0);
        true
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
            self.record_at(b, hid, eid, old, new, 0, None)
        }
        /// 時刻の元 `hlc` (0 = 数え口) を名簿の鍵の前に決めた書き込み (engine の WAL の HLC と同じ)。 上げたら `wal` (HLC の
        /// 数え口の代わり) をそこまで進める (engine の `observe_hlc` と同じ)。
        #[allow(clippy::too_many_arguments)]
        fn record_at(&self, b: &Balls, hid: u16, eid: u32, old: Option<u64>, new: Option<u64>, hlc: u64, wal: Option<&AtomicU64>) -> Option<u64> {
            let ball = b.get(hid)?;
            let mut wide = ball.reserve(eid, old, new);
            let mut ks: Vec<usize> = old.into_iter().chain(new).map(|v| v as usize % self.0.len()).collect();
            ks.sort_unstable();
            ks.dedup();
            let held: Vec<_> = ks.iter().map(|&k| self.0[k].lock()).collect();
            let ts = ball.note(eid, old, new, b.clock(), hlc, &epoch::pin(), &mut wide);
            let raised = take_raised();
            if let Some(w) = wal.filter(|_| raised != 0) {
                w.fetch_max(raised, Ordering::Relaxed);
            }
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
        /// HLC `hlc` の書き込み (`None` = 外す)。 付いた時刻を返す。
        fn put_at(&mut self, hid: u16, eid: u32, v: Option<u64>, hlc: u64) -> u64 {
            let old = match v {
                Some(v) => self.cur.insert((hid, eid), v),
                None => self.cur.remove(&(hid, eid)),
            };
            self.locks.record_at(&self.b, hid, eid, old, v, hlc, None).unwrap()
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
        assert!(x.b.declare(3, 0, |_| {}));
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
        x.b.declare(0, 0, |f| {
            f(1, 5);
            f(2, 5);
        });
        x.set(0, 1, 6);
        let t = x.b.now();
        assert_eq!(x.pull_at(0, 5, 0), vec![1, 2]);
        assert_eq!(x.pull_at(0, 5, t), vec![2]);
        assert_eq!(x.pull_at(0, 6, 0), Vec::<u32>::new());
        assert_eq!(x.b.get(0).unwrap().value_at(1, t - 1), Some(5));
        // 宣言の前からの値は時刻 0 (宣言の時) から上を向いている
        assert_eq!(x.b.get(0).unwrap().value_at(1, 0), Some(5));
        assert_eq!(x.b.get(0).unwrap().value_at(2, 0), Some(5));
    }

    #[test]
    fn wide_values_and_reused_eids() {
        let mut x = T::new();
        x.b.declare(1, 0, |_| {});
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

    /// 大きい値の差分は値ごとの帳面でなく列 1 本の末尾に足し、 溜まったら値の順の run に並べて併合する。 同じ列に落ちる値 (下位
    /// bit が同じ) が交互に書かれても、 末尾と run と併合した run にまたがっても、 値ごとの答えは混ざらない (16 回目に run の形に
    /// して、 回数を run にする所と併合を踏む数に増やした)。
    #[test]
    fn wide_values_sharing_a_stripe_keep_their_own_threads() {
        let mut x = T::new();
        x.b.declare(0, 0, |_| {});
        let vals: Vec<u64> = (0..4).map(|k| (1u64 << 30) + k * WIDE as u64).collect(); // 全部同じ列
        let mut log: Vec<(u64, BTreeMap<u32, u64>)> = vec![(0, BTreeMap::new())];
        let mut r = 0x2545_F491_4F6C_DD1Du64;
        for _ in 0..3000 {
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
        let shape = x.b.get(0).unwrap().wide_shape();
        assert!(shape.iter().any(|(runs, _)| runs.len() >= 2), "試験の前提: run が 2 本以上 {shape:?}");
        assert!(shape.iter().any(|(runs, _)| runs.iter().any(|&n| n > WIDE_TAIL)), "試験の前提: 併合した run がある {shape:?}");
        assert!(shape.iter().any(|&(_, tail)| tail > 0), "試験の前提: 末尾にも差分がある {shape:?}");
        assert!(x.b.get(0).unwrap().ledgers_in_time_order());
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
        x.b.declare(0, 0, |_| {});
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
        x.b.declare(0, 0, |_| {});
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

    /// 1 つの球に書き換え / 外すを 5000 回 (書き手の道で、 他の球の書き込みを挟んで面を散らす) 当て、 書いた時刻とその前後の全部の
    /// t で、 飛び糸で探した値 = 手元の (時刻, 値) の列、 かつ通った面の数 ≤ 3⌈log2(面の数 + 1)⌉ (飛び糸を張らない / 親だけでも答えは
    /// 合うので、 歩数を見る)。 宣言の前からの面 (時刻 0) から始め、 外した後に戻る面と大きい値も混ぜる。 Miri では 60 回 (外す 3 回、
    /// 大きい値 6 回、 飛び幅は 31 まで。 Miri の上では面を読むたびに遅くなり、 300 回でも答え合わせが 30 分で終わらない)。
    #[test]
    fn jumps_find_the_face_in_log_steps() {
        let writes = if cfg!(miri) { 60 } else { 5000 };
        let mut x = T::new();
        x.b.declare(0, 0, |s| s(3, 9));
        x.cur.insert((0, 3), 9);
        let mut log: Vec<(u64, Option<u64>)> = vec![(0, Some(9))];
        let mut r = 0x2545_F491_4F6C_DD1Du64;
        let mut next = || {
            r ^= r << 13;
            r ^= r >> 7;
            r ^= r << 17;
            r
        };
        for _ in 0..writes {
            let k = next();
            let last = log.last().unwrap().1;
            if k % 7 == 0 && last.is_some() {
                x.rm(0, 3);
                log.push((x.b.now(), None));
            } else {
                let mut v = if k % 11 == 0 { LOG_DENSE + (k >> 8) % 5 } else { (k >> 8) % 40 };
                if Some(v) == last {
                    v += 1;
                }
                x.set(0, 3, v);
                log.push((x.b.now(), Some(v)));
            }
            if k % 3 == 0 {
                x.set(0, 100 + (k >> 20) as u32 % 50, (k >> 30) % 5);
            }
        }
        let ball = x.b.get(0).unwrap();
        let n = ball.faces_of(3).len();
        let bound = 3 * (n + 1).next_power_of_two().trailing_zeros();
        let mut worst = 0;
        for &(tm, _) in &log {
            for t in [tm.saturating_sub(1), tm, tm + 1] {
                let want = log[log.partition_point(|&(ts, _)| ts <= t) - 1].1;
                assert_eq!(ball.value_at(3, t), want, "t={t}");
                let steps = ball.face_at(3, t).1;
                assert!(steps <= bound, "t={t}: 面 {n} 個に {steps} 歩 (上限 {bound})");
                worst = worst.max(steps);
            }
        }
        assert!(n > writes * 3 / 4, "面が少ない: {n}");
        eprintln!("面 {n} 個、 最大 {worst} 歩 (上限 {bound})");
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
        x.b.declare(0, 0, |f| seed.iter().for_each(|&(e, v)| f(e, v)));
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
        x.b.declare(0, 0, |_| {});
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
        x.b.declare(0, 0, |f| (0..30).for_each(|e| f(e, 1)));
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

    /// HLC を詰めた時刻は (wall, logical) の順を保ち、 元に戻せる。
    #[test]
    fn packed_hlc_keeps_the_order() {
        let hs = [(0u64, 0u32), (0, 1), (1, 0), (1_790_000_000_000, 0), (1_790_000_000_000, 7), (1_790_000_000_000, (1 << 23) - 1), (1_790_000_000_001, 0)];
        for w in hs.windows(2) {
            assert!(pack_hlc(w[0].0, w[0].1) < pack_hlc(w[1].0, w[1].1), "{w:?}");
        }
        for &(wall, logical) in &hs {
            assert_eq!(unpack_hlc(pack_hlc(wall, logical)), (wall, logical));
        }
    }

    /// 時刻を HLC (名簿の鍵の前に決まる) から取る時: HLC の小さい書き手が後から鍵を取ったら、 帳面の最後の後に上げる (帳面は時刻の
    /// 順)。 古い HLC で届いた外す / 書き換えは、 その球の前の面の後に上げる (球の面も時刻の順)。 上げない時は HLC のまま。 大きい値は
    /// 値の順の run に並べるので、 同じ値の他の球の差分より前の時刻でも上げない (16 回目まで、 糸の 1 つ前の差分の後に上げていた)。
    #[test]
    fn hlc_times_are_raised_to_keep_ledgers_and_balls_in_order() {
        let mut x = T::new();
        x.b.declare(0, 0, |_| {});
        let big = 1u64 << 30;
        // 値 1: e2 が HLC 200 で先に鍵を取り、 e1 (HLC 100) が後
        assert_eq!(x.put_at(0, 2, Some(1), 200), 200, "上げない時は HLC のまま");
        assert_eq!(x.put_at(0, 1, Some(1), 100), 201, "帳面の最後 (200) の後に上げる");
        // 球: e3 を HLC 300 で値 2 に、 その後に HLC 250 の外すが届く
        assert_eq!(x.put_at(0, 3, Some(2), 300), 300);
        assert_eq!(x.put_at(0, 3, None, 250), 301, "球の最後の面 (300) の後に上げる");
        // 大きい値: e4 が HLC 500、 e5 が HLC 400 で同じ値に
        assert_eq!(x.put_at(0, 4, Some(big), 500), 500);
        assert_eq!(x.put_at(0, 5, Some(big), 400), 400, "大きい値は他の球の差分の後に上げない");
        // 球だけが知っている時刻: e6 を値 4 に (600)、 外す (610)、 まだ誰も居ない値 5 に古い HLC 605 で。 値 5 の帳面は空なので、
        // 球の最後の面の外れた時刻 (610) の後に上げる
        assert_eq!(x.put_at(0, 6, Some(4), 600), 600);
        assert_eq!(x.put_at(0, 6, None, 610), 610);
        assert_eq!(x.put_at(0, 6, Some(5), 605), 611, "球の最後の面が外れた時刻 (610) の後に上げる");
        let ball = x.b.get(0).unwrap();
        assert!(ball.ledgers_in_time_order());
        assert_eq!(x.pull_at(0, 1, 200), vec![2]);
        assert_eq!(x.pull_at(0, 1, 201), vec![1, 2]);
        assert_eq!(ball.value_at(1, 200), None);
        assert_eq!(ball.value_at(1, 201), Some(1));
        assert_eq!(ball.faces_of(3), vec![FaceView { value: 2, from: 300, to: 301 }]);
        assert_eq!(x.pull_at(0, big, 399), Vec::<u32>::new());
        assert_eq!(x.pull_at(0, big, 400), vec![5]);
        assert_eq!(x.pull_at(0, big, 499), vec![5]);
        assert_eq!(x.pull_at(0, big, 500), vec![4, 5]);
        assert_eq!(
            ball.faces_of(6),
            vec![FaceView { value: 4, from: 600, to: 610 }, FaceView { value: 5, from: 611, to: OPEN }]
        );
        assert_eq!(x.b.now(), 611, "今の時刻は付けた時刻の最大");
    }

    /// 書き手 4 本が自分の entity 8 個に、 同じ値の組 (0..5) を書いたり外したりする。 回ごとに Barrier でそろえ、 そろった時刻を
    /// 外から控える (= その時刻の答えは手元の写しで決まる、 球の付けた時刻を使わない)。 最後に、 全部の回の時刻でデッキの答えと
    /// 球の答えを写しと比べる (今のデッキは最後の写しから渡す)。 回の途中の時刻は、 デッキの答えを球の面と突き合わせる。
    /// 名簿の鍵は試験が持つ (`Locks`、 値ごと)。
    #[test]
    fn concurrent_writers_agree_with_a_log() {
        concurrent_log(false);
    }

    /// 上と同じを、 時刻を名簿の鍵の前に決めて回す (engine の WAL の HLC と同じ: 共有の数え口で取ってから、 時々譲ってから鍵を取る =
    /// 時刻の順と鍵の順がずれる)。 帳面は時刻の順のまま、 答えも同じ。
    #[test]
    fn concurrent_writers_with_times_from_before_the_locks() {
        concurrent_log(true);
    }

    fn concurrent_log(hlc: bool) {
        const TH: u32 = 4;
        const PER: u32 = 8;
        const VALS: u64 = 5;
        let b = Balls::new();
        b.set_snap_min(16); // 控えを作る書き手と、 帳面に足す書き手 / 読み手が重なる
        b.declare(0, 0, |_| {});
        let locks = Locks::new(VALS as usize);
        let wal = AtomicU64::new(0);
        let rounds = if cfg!(miri) { 4 } else { 300 };
        let bar = std::sync::Barrier::new(TH as usize + 1);
        let log = std::sync::Mutex::new(vec![None::<u64>; (TH * PER) as usize]);
        let mut checks: Vec<(u64, Vec<Option<u64>>)> = Vec::new();
        std::thread::scope(|s| {
            for th in 0..TH {
                let (b, bar, log, locks, wal) = (&b, &bar, &log, &locks, &wal);
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
                            if hlc {
                                let h = wal.fetch_add(1, Ordering::Relaxed) + 1;
                                if r & 3 == 0 {
                                    std::thread::yield_now();
                                }
                                locks.record_at(b, 0, e, mine[k], new, h, Some(wal));
                            } else {
                                locks.record(b, 0, e, mine[k], new);
                            }
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
        assert!(ball.ledgers_in_time_order());
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
