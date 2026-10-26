//! 球 (試作、 branch `proto/ball-history`): 宣言した紐の値の履歴。
//!
//! 球は entity ごとに 1 つ。 書き換えると回って別の面が上を向く (球はどこにも動かない)。 デッキ = その値の面を上にしている
//! 球の集まり (見え方)。 円柱の bucket は、 その球を見つけるための名簿 (master のまま: 回って外れた行は印か Column で飛ばし、
//! 清書 = 詰め直しで捨てる)。 履歴は 2 か所に書く:
//!
//! - **球の面** (`Face`): (eid, 値, 上を向いた時刻 `from`, 外れた時刻 `to`)。 同じ球の 1 つ前の面へ糸 (`prev`) を張る。
//!   「時点 t の値」 ([`Ball::value_at`]) は今の面から糸をたどる
//! - **デッキの差分の帳面** (`DiffLog`): 値ごとに 「時刻 t に球 X が入った / 出た」 を書き足す。 時刻は帳面の鍵の中で取るので、
//!   帳面の並びはそのまま時刻の順。 「時点 t にこの値だった球」 ([`Ball::pull_at`]) は、 今のデッキから t より後の差分を逆に
//!   戻して作る (t より後に最初に 「入った」 球は t には居なかった、 最初に 「出た」 球は居た、 差分の無い球は今と同じ)。
//!   手間は 「t より後の差分の数」 に比例
//!
//! 同じ値の書き直しは球を回さない (面も差分も足さない)。 外す (untie / delete) は今の面を閉じ、 「出た」 を書く。
//!
//! ## 順序
//! 書き手は行の lock の下で、 円柱 (名簿) を書き換える**前に** [`Balls::record`] を呼ぶ: 古い値と新しい値の帳面の鍵を取り、
//! 時刻を取って両方の帳面に書き足し (len を Release)、 鍵を離してから球の面を回す。 読み手は今のデッキを先に読み、 その後で帳面を
//! 読む (Acquire)。 読み手が名簿の変化を見たなら、 その前に書いた差分も見える。 名簿の変化をまだ見ていない時に差分だけ見えても、
//! 入った / 出たを集合として当てるので答えは同じ。
//!
//! ## 試作の範囲
//! - メモリの上だけ (開き直すと消える)。 宣言も保存しない
//! - 時刻は engine の中の通し番号 ([`Balls::now`])。 HLC ではない。 宣言の前からある値の面は `from = 0` (「前から」、 差分は書かない)
//! - 宣言 ([`Balls::declare`]) は書き手の居ない時に
//! - 球は eid ごと: delete は今の面を閉じるだけで、 同じ eid を使い回した次の entity の面は前の entity の面の続きに付く
//! - 読みが時刻 t = 今 の時、 同時に書いている entity は前の向きに見えうる。 過去の t は、 その時刻までの書き込みが返った後なら揺れない
//! - 円柱を書き換えられなかった (ディスク満杯で `set` が false) 時も差分は残る

use crossbeam_epoch::{self as epoch, Atomic, Guard, Owned};
use std::cell::{Cell, UnsafeCell};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// 糸の終わり。
const NONE: u32 = u32::MAX;
/// まだ上を向いている面の `to`。
pub const OPEN: u64 = u64::MAX;

const CHUNK_BITS: u32 = 16;
const CHUNK: usize = 1 << CHUNK_BITS;
/// 帳面を配列で引く値の上限 (2^20、 円柱の dense と同じ)。 上は `WIDE` 本に分けた表。
const LOG_DENSE: u64 = 1 << 20;
const WIDE: usize = 64;
/// 帳面の鍵の本数 (値の下位 bit で選ぶ)。
const LOCKS: usize = 1024;
/// 書き手ごとに面の場所をまとめて取る数。
const BLOCK: u64 = 64;

/// 球に付ける通し番号 (書き手ごとの面の場所の塊がどの球の物かを見分ける。 番地は使い回されるので使わない)。
static BALL_IDS: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// この thread が取った面の場所の塊: (球の番号, 次, 終わり)。 球 8 つまで。 追い出した塊の残りは使わない場所になる。
    static BLOCKS: Cell<[(u64, u32, u32); 8]> = const { Cell::new([(0, 0, 0); 8]) };
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
#[derive(Clone, Copy, Default)]
struct Diff {
    t: u64,
    eid: u32,
    into: u32,
}

/// 帳面の backing。 `data[0..len]` が出した差分 (以後変わらない)。
struct LogBuf {
    data: Box<[UnsafeCell<Diff>]>,
    len: AtomicUsize,
}

// SAFETY: 出した範囲 [0..len] は書き換わらない。 書き手 (帳面の鍵の下で 1 本) は [len] だけに書いてから len を Release で進め、
// 読み手は len を Acquire で読んで [0..len] だけを読む。
unsafe impl Sync for LogBuf {}

/// 値 1 つのデッキの差分の帳面。 書き手 1 本 (帳面の鍵の下) / 読み手は lock を取らない (epoch)。 足すのは後ろだけ、 満杯なら
/// 倍の backing に写して差し替える (古い backing は読み手が抜けてから捨てる)。
struct DiffLog {
    buf: Atomic<LogBuf>,
}

impl DiffLog {
    fn new() -> Self {
        Self { buf: Atomic::new(LogBuf { data: Box::new([]), len: AtomicUsize::new(0) }) }
    }

    /// 後ろに足す。 帳面の鍵を持つ 1 thread だけ。
    fn push(&self, d: Diff, guard: &Guard) {
        let cur = self.buf.load(Ordering::Acquire, guard);
        // SAFETY: buf は常に非 null。
        let b = unsafe { cur.deref() };
        let l = b.len.load(Ordering::Relaxed);
        if l < b.data.len() {
            // SAFETY: [l] はまだ出していない (読み手は読まない)。 書き手は 1 本
            unsafe { *b.data[l].get() = d };
            b.len.store(l + 1, Ordering::Release);
            return;
        }
        let cap = (b.data.len() * 2).max(4);
        let data: Box<[UnsafeCell<Diff>]> = (0..cap).map(|_| UnsafeCell::new(Diff::default())).collect();
        for i in 0..l {
            // SAFETY: 古い backing の [0..l] は出した範囲で変わらない。 新しい backing はまだ誰も見ていない
            unsafe { *data[i].get() = *b.data[i].get() };
        }
        // SAFETY: 同上
        unsafe { *data[l].get() = d };
        self.buf.store(Owned::new(LogBuf { data, len: AtomicUsize::new(l + 1) }), Ordering::Release);
        // SAFETY: 古い backing は読み手が epoch を抜けてから捨てる。
        unsafe { guard.defer_destroy(cur) };
    }

    /// 出した差分を読む。
    fn read<R>(&self, guard: &Guard, f: impl FnOnce(&[Diff]) -> R) -> R {
        // SAFETY: buf は常に非 null、 guard が backing を生かす。
        let b = unsafe { self.buf.load(Ordering::Acquire, guard).deref() };
        let n = b.len.load(Ordering::Acquire);
        // SAFETY: [0..n] は出した範囲 (変わらない)。 UnsafeCell<Diff> は Diff と同じ表現
        let s = unsafe { std::slice::from_raw_parts(b.data.as_ptr() as *const Diff, n) };
        f(s)
    }

    fn len(&self, guard: &Guard) -> usize {
        self.read(guard, |s| s.len())
    }

    fn cap(&self, guard: &Guard) -> usize {
        // SAFETY: buf は常に非 null。
        unsafe { self.buf.load(Ordering::Acquire, guard).deref() }.data.len()
    }
}

impl Drop for DiffLog {
    fn drop(&mut self) {
        // SAFETY: drop は帳面を持つ最後の 1 つ (誰も読んでいない)。
        unsafe { drop(self.buf.load(Ordering::Relaxed, epoch::unprotected()).into_owned()) };
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
    /// `LOG_DENSE` 以上の値の帳面。 値の下位 bit で `WIDE` 本に分ける。 試作なので lock で引く。
    wide: Box<[Mutex<BTreeMap<u64, Arc<DiffLog>>>]>,
    /// 帳面の鍵 (値の下位 bit で選ぶ)。 帳面ごとの書き手 1 本と、 時刻の順 = 書き足した順を守る。
    locks: Box<[parking_lot::Mutex<()>]>,
}

impl Ball {
    fn new() -> Self {
        Self {
            id: BALL_IDS.fetch_add(1, Ordering::Relaxed),
            faces: Chunked::new(1 << 32),
            next: AtomicU64::new(0),
            heads: Chunked::new(1 << 32),
            logs: Chunked::new(LOG_DENSE),
            wide: (0..WIDE).map(|_| Mutex::new(BTreeMap::new())).collect(),
            locks: (0..LOCKS).map(|_| parking_lot::Mutex::new(())).collect(),
        }
    }

    #[inline]
    fn face(&self, i: u32) -> &Face {
        self.faces.get(i).expect("ball: 出した面の塊は取ってある")
    }

    /// 値 `v` の帳面に書き足す (無ければ作る)。 `v` の帳面の鍵の下で。
    fn log_push(&self, v: u64, d: Diff, guard: &Guard) {
        if v < LOG_DENSE {
            self.logs.get_or_alloc(v as u32).get_or_init(|| Box::new(DiffLog::new())).push(d, guard);
        } else {
            let log = self.wide[v as usize % WIDE].lock().unwrap_or_else(|e| e.into_inner()).entry(v).or_insert_with(|| Arc::new(DiffLog::new())).clone();
            log.push(d, guard);
        }
    }

    /// 書き換えを記す (行の lock の下、 円柱を書き換える前に): 古い値 `old` と新しい値 `new` の帳面の鍵を取り、 時刻を取って
    /// 両方の帳面に書き足し、 鍵を離してから球を回す。 返り値は時刻。 `old == new` で呼ばないこと。
    fn record(&self, eid: u32, old: Option<u64>, new: Option<u64>, clock: &AtomicU64) -> u64 {
        debug_assert!(old != new);
        let idx = |v: u64| v as usize % LOCKS;
        let mut ks: Vec<usize> = old.into_iter().chain(new).map(idx).collect();
        ks.sort_unstable();
        ks.dedup();
        let _held: Vec<_> = ks.iter().map(|&k| self.locks[k].lock()).collect();
        let ts = clock.fetch_add(1, Ordering::Relaxed) + 1;
        let guard = epoch::pin();
        if let Some(o) = old {
            self.log_push(o, Diff { t: ts, eid, into: 0 }, &guard);
        }
        if let Some(n) = new {
            self.log_push(n, Diff { t: ts, eid, into: 1 }, &guard);
        }
        drop(_held);
        self.turn(eid, new, ts);
        ts
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

    /// 球を回す: `new` を上に向ける (None = 今の面を閉じるだけ)、 時刻 `ts`。 行の lock の下で呼ぶ。
    fn turn(&self, eid: u32, new: Option<u64>, ts: u64) {
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

    /// 値 `v` の帳面 (無ければ None)。
    fn log_of(&self, v: u64) -> Option<LogRef<'_>> {
        if v < LOG_DENSE {
            self.logs.get(v as u32)?.get().map(|b| LogRef::Dense(b))
        } else {
            self.wide[v as usize % WIDE].lock().unwrap_or_else(|e| e.into_inner()).get(&v).cloned().map(LogRef::Wide)
        }
    }

    /// 時点 `t` に値 `v` の面を上にしていた球 (eid の昇順)。 `now` は今のデッキ (この呼び出しの**前に**読んだもの、 並びは問わない)。
    /// 今のデッキから、 帳面の t より後の差分を逆に戻す。
    pub fn pull_at(&self, v: u64, t: u64, now: Vec<u32>) -> Vec<u32> {
        let mut now = now;
        now.sort_unstable();
        let Some(log) = self.log_of(v) else { return now };
        let guard = epoch::pin();
        log.get().read(&guard, |d| {
            let after = &d[d.partition_point(|x| x.t <= t)..];
            if after.is_empty() {
                return now;
            }
            // 球ごとの t より後の最初の差分: 入った = t には居なかった、 出た = 居た
            let mut first: Vec<(u32, u32, bool)> = after.iter().enumerate().map(|(i, x)| (x.eid, i as u32, x.into == 1)).collect();
            first.sort_unstable_by_key(|&(e, i, _)| (e, i));
            first.dedup_by_key(|x| x.0);
            let mut out = Vec::with_capacity(now.len() + first.len());
            let mut j = 0;
            for &e in &now {
                while j < first.len() && first[j].0 < e {
                    if !first[j].2 {
                        out.push(first[j].0);
                    }
                    j += 1;
                }
                if j < first.len() && first[j].0 == e {
                    if !first[j].2 {
                        out.push(e);
                    }
                    j += 1;
                } else {
                    out.push(e);
                }
            }
            out.extend(first[j..].iter().filter(|x| !x.2).map(|x| x.0));
            out
        })
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
        for w in self.wide.iter() {
            n += w.lock().unwrap_or_else(|e| e.into_inner()).values().map(|l| l.len(&guard) as u64).sum::<u64>();
        }
        n
    }

    /// 取った大きさ (byte、 塊の表は除く)。
    pub fn bytes(&self) -> usize {
        let guard = epoch::pin();
        let mut logs = 0usize;
        self.logs.for_each(|l| logs += l.get().map_or(0, |l| l.cap(&guard)));
        for w in self.wide.iter() {
            logs += w.lock().unwrap_or_else(|e| e.into_inner()).values().map(|l| l.cap(&guard)).sum::<usize>();
        }
        self.faces.allocated() * CHUNK * std::mem::size_of::<Face>()
            + self.heads.allocated() * CHUNK * 4
            + self.logs.allocated() * CHUNK * std::mem::size_of::<OnceLock<Box<DiffLog>>>()
            + logs * std::mem::size_of::<Diff>()
    }
}

/// 帳面への参照 (配列の物は借用、 大きい値の物は `Arc`)。
enum LogRef<'a> {
    Dense(&'a DiffLog),
    Wide(Arc<DiffLog>),
}

impl LogRef<'_> {
    fn get(&self) -> &DiffLog {
        match self {
            LogRef::Dense(l) => l,
            LogRef::Wide(l) => l,
        }
    }
}

/// engine 1 つの球の全部。 宣言が 1 つも無ければ書き手は `any` の load 1 回で素通り。
pub(crate) struct Balls {
    any: AtomicBool,
    clock: AtomicU64,
    /// 紐の番号 → 球。 最初の宣言で取る。
    slots: OnceLock<Box<[OnceLock<Box<Ball>>]>>,
}

impl Balls {
    pub fn new() -> Self {
        Self { any: AtomicBool::new(false), clock: AtomicU64::new(0), slots: OnceLock::new() }
    }

    #[inline]
    pub fn any(&self) -> bool {
        self.any.load(Ordering::Acquire)
    }

    /// 今の時刻 (最後に記した書き換えの時刻)。 この後の書き換えは、 これより大きい時刻になる。
    pub fn now(&self) -> u64 {
        self.clock.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn get(&self, hid: u16) -> Option<&Ball> {
        self.slots.get()?[hid as usize].get().map(|b| &**b)
    }

    /// 紐 `hid` を球にする。 `seed` に今の値 (eid, 値) を全部渡す。 宣言済みなら false。
    pub fn declare(&self, hid: u16, seed: impl FnOnce(&mut dyn FnMut(u32, u64))) -> bool {
        let slots = self.slots.get_or_init(|| (0..=u16::MAX as usize).map(|_| OnceLock::new()).collect());
        let mut fresh = false;
        let ball = slots[hid as usize].get_or_init(|| {
            fresh = true;
            Box::new(Ball::new())
        });
        if fresh {
            seed(&mut |eid, v| ball.seed(eid, v));
            self.any.store(true, Ordering::Release);
        }
        fresh
    }

    /// 書き換え `old` → `new` を記す (行の lock の下、 円柱を書き換える前)。 球にしていない紐・同じ値は何もしない。 返り値は時刻。
    #[inline]
    pub fn record(&self, hid: u16, eid: u32, old: Option<u64>, new: Option<u64>) -> Option<u64> {
        if old == new {
            return None;
        }
        Some(self.get(hid)?.record(eid, old, new, &self.clock))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 試験の手元: 球と、 名簿の代わりの今の値の写し ((紐, eid) → 値)。
    struct T {
        b: Balls,
        cur: BTreeMap<(u16, u32), u64>,
    }

    impl T {
        fn new() -> Self {
            Self { b: Balls::new(), cur: BTreeMap::new() }
        }
        fn set(&mut self, hid: u16, eid: u32, v: u64) {
            let old = self.cur.insert((hid, eid), v);
            self.b.record(hid, eid, old, Some(v));
        }
        fn rm(&mut self, hid: u16, eid: u32) {
            let old = self.cur.remove(&(hid, eid));
            self.b.record(hid, eid, old, None);
        }
        /// 今のデッキ (名簿から読んだつもり)。
        fn now(&self, hid: u16, v: u64) -> Vec<u32> {
            self.cur.iter().filter(|&(&(h, _), &x)| h == hid && x == v).map(|(&(_, e), _)| e).collect()
        }
        fn pull_at(&self, hid: u16, v: u64, t: u64) -> Vec<u32> {
            self.b.get(hid).unwrap().pull_at(v, t, self.now(hid, v))
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

    /// 書き手 4 本が自分の entity 8 個に、 同じ値の組 (0..5) を書いたり外したりする。 回ごとに Barrier でそろえ、 そろった時刻を
    /// 外から控える (= その時刻の答えは手元の写しで決まる、 球の付けた時刻を使わない)。 最後に、 全部の回の時刻でデッキの答えと
    /// 球の答えを写しと比べる (今のデッキは最後の写しから渡す)。
    #[test]
    fn concurrent_writers_agree_with_a_log() {
        const TH: u32 = 4;
        const PER: u32 = 8;
        const VALS: u64 = 5;
        let b = Balls::new();
        b.declare(0, |_| {});
        let rounds = if cfg!(miri) { 4 } else { 300 };
        let bar = std::sync::Barrier::new(TH as usize + 1);
        let log = std::sync::Mutex::new(vec![None::<u64>; (TH * PER) as usize]);
        let mut checks: Vec<(u64, Vec<Option<u64>>)> = Vec::new();
        std::thread::scope(|s| {
            for th in 0..TH {
                let (b, bar, log) = (&b, &bar, &log);
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
                            b.record(0, e, mine[k], new);
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
                assert_eq!(ball.pull_at(v, *t, now), deck, "回 {r} t={t} v={v}");
            }
            for e in 0..TH * PER {
                assert_eq!(ball.value_at(e, *t), want[e as usize], "回 {r} t={t} e={e}");
            }
        }
    }
}
