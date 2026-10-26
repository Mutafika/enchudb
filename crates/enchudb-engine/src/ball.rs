//! 球 (試作、 branch `proto/ball-history`): 宣言した紐の値の履歴を、 entity ごとの面の並びとして持つ。
//!
//! カードは表 (紐) と裏 (値) の 2 面で、 書き換えると前の値は消える。 球は書き換えるたびに新しい面に書いて上に向け、
//! 前の面も残す。 面 = (entity, 値, 上を向いた時刻 `from`, 外れた時刻 `to`)。 時刻 t に上を向いていた面は
//! `from <= t < to` の面。 面は 2 本の糸でつなぐ:
//!
//! - 同じ球 (entity) の 1 つ前の面 (`prev`) — 「時点 t の値」 ([`Ball::value_at`]) は今の面から糸をたどる
//! - 同じデッキ (値) の 1 つ前の面 (`prev_v`) — 「時点 t にこの値だった entity」 ([`Ball::pull_at`]) は、 その値の
//!   最後の面から糸をたどる (その値に一度でも入った面を全部見る = デッキの履歴の長さに比例)
//!
//! 同じ値の書き直しは球を回さない (面を足さない)。 外す (untie / delete) は今の面を閉じるだけ (空の面は作らない)。
//!
//! ## 試作の範囲
//! - メモリの上だけ (開き直すと消える)。 宣言も保存しない
//! - 時刻は engine の中の通し番号 ([`Balls::now`]、 書き換え 1 回ごとに +1)。 HLC ではない。 宣言の前からある値の面は
//!   `from = 0` (「前から」)
//! - 宣言 ([`Balls::declare`]) は書き手の居ない時に (宣言と同時の書き込みは面を 2 つ作りうる)
//! - 球は eid ごと: delete は今の面を閉じるだけで、 同じ eid を使い回した次の entity の面は前の entity の面の続きに付く
//!   (「時点 t に eid X だったもの」 を答える。 デッキの糸も同じ eid で答えるので 2 つの読みは食い違わない)
//! - 読みが時刻 t = 今 の時、 同時に書いている entity は前の面が見えうる (面を閉じてから次の面を出すまでの間)。
//!   過去の t は、 その時刻までの書き込みが返った後なら揺れない
//!
//! ## 並行
//! 書き手は engine の行の lock の下 (`live_set` / `live_remove`) で呼ぶ = 同じ entity の書き手は 1 本。 面の場所は
//! `next` の fetch_add で取り、 中身を書いてから球の頭 (`heads`) を Release で出す。 デッキの頭は CAS で付け替える
//! (前の頭を `prev_v` に書いてから CAS、 読み手は頭を Acquire で読んで糸をたどる)。 読み手は lock を取らない。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

/// 糸の終わり。
const NONE: u32 = u32::MAX;
/// まだ上を向いている面の `to`。
pub const OPEN: u64 = u64::MAX;

const CHUNK_BITS: u32 = 16;
const CHUNK: usize = 1 << CHUNK_BITS;
/// u32 の添字を覆う塊の数。
const CHUNKS: usize = 1 << (32 - CHUNK_BITS);

/// u32 の添字で引く、 塊ごとに初めて触った時に取る配列。 読み手は取らない (無い塊は空として読む)。
struct Chunked<T> {
    chunks: Box<[OnceLock<Box<[T]>>]>,
}

impl<T: Default> Chunked<T> {
    fn new() -> Self {
        Self { chunks: (0..CHUNKS).map(|_| OnceLock::new()).collect() }
    }

    #[inline]
    fn get(&self, i: u32) -> Option<&T> {
        let c = self.chunks[(i >> CHUNK_BITS) as usize].get()?;
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
}

/// 球の面 1 つ。 中身は面を出す前に 1 回書き、 後から変わるのは `to` (閉じる時) だけ。
#[derive(Default)]
struct Face {
    eid: AtomicU32,
    /// 同じ球の 1 つ前の面 (`NONE` = 最初の面)。
    prev: AtomicU32,
    /// 同じデッキ (値) の 1 つ前の面。
    prev_v: AtomicU32,
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

/// 紐 1 本の球の全部 (entity ごとの球 + 値ごとのデッキの頭)。
pub(crate) struct Ball {
    faces: Chunked<Face>,
    /// 出した面の数 (= 次の面の場所)。
    next: AtomicU32,
    /// entity → 今の面 + 1 (0 = 球が無い)。
    heads: Chunked<AtomicU32>,
    /// 値 (u32 に入る) → その値の最後の面 + 1。
    decks: Chunked<AtomicU32>,
    /// u32 に入らない値のデッキの頭 (Number64 の列)。 試作なので lock で守る。
    wide: Mutex<BTreeMap<u64, u32>>,
}

impl Ball {
    fn new() -> Self {
        Self { faces: Chunked::new(), next: AtomicU32::new(0), heads: Chunked::new(), decks: Chunked::new(), wide: Mutex::new(BTreeMap::new()) }
    }

    #[inline]
    fn face(&self, i: u32) -> &Face {
        self.faces.get(i).expect("ball: 出した面の塊は取ってある")
    }

    /// 値 `v` の面を `i` として出す (中身を書いてからデッキの頭を付け替える)。
    fn push_face(&self, i: u32, eid: u32, prev: u32, v: u64, from: u64) {
        let f = self.faces.get_or_alloc(i);
        f.eid.store(eid, Ordering::Relaxed);
        f.prev.store(prev, Ordering::Relaxed);
        f.value.store(v, Ordering::Relaxed);
        f.from.store(from, Ordering::Relaxed);
        f.to.store(OPEN, Ordering::Relaxed);
        if let Ok(v32) = u32::try_from(v) {
            let head = self.decks.get_or_alloc(v32);
            let mut old = head.load(Ordering::Acquire);
            loop {
                f.prev_v.store(old.wrapping_sub(1), Ordering::Relaxed);
                match head.compare_exchange_weak(old, i + 1, Ordering::Release, Ordering::Acquire) {
                    Ok(_) => break,
                    Err(o) => old = o,
                }
            }
        } else {
            let mut w = self.wide.lock().unwrap_or_else(|e| e.into_inner());
            let old = w.insert(v, i + 1).unwrap_or(0);
            f.prev_v.store(old.wrapping_sub(1), Ordering::Release);
        }
    }

    fn alloc(&self) -> Option<u32> {
        let i = self.next.fetch_add(1, Ordering::Relaxed);
        // NONE は糸の終わりに使う。 使い切ったら記録をやめる (試作)
        (i < NONE - 1).then_some(i)
    }

    /// 球を回す: `new` を上に向ける (None = 今の面を閉じるだけ)。 行の lock の下で呼ぶ。
    fn turn(&self, eid: u32, new: Option<u64>, clock: &AtomicU64) {
        let head = self.heads.get_or_alloc(eid);
        let cur = head.load(Ordering::Relaxed);
        let open = (cur != 0).then(|| self.face(cur - 1)).filter(|f| f.to.load(Ordering::Relaxed) == OPEN);
        match (open, new) {
            (Some(f), Some(v)) if f.value.load(Ordering::Relaxed) == v => return,
            (None, None) => return,
            _ => {}
        }
        let ts = clock.fetch_add(1, Ordering::Relaxed) + 1;
        if let Some(f) = open {
            f.to.store(ts, Ordering::Release);
        }
        if let Some(v) = new {
            let Some(i) = self.alloc() else { return };
            self.push_face(i, eid, cur.wrapping_sub(1), v, ts);
            head.store(i + 1, Ordering::Release);
        }
    }

    /// 宣言の前からある値を 「前から」 (from = 0) の面にする。
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

    /// 時点 `t` に値 `v` が上を向いていた entity (eid の昇順)。 同じ球の面は時刻が重ならないので、 1 つの eid は 1 回だけ出る。
    pub fn pull_at(&self, v: u64, t: u64) -> Vec<u32> {
        let head = match u32::try_from(v) {
            Ok(v32) => self.decks.get(v32).map_or(0, |h| h.load(Ordering::Acquire)),
            Err(_) => self.wide.lock().unwrap_or_else(|e| e.into_inner()).get(&v).copied().unwrap_or(0),
        };
        let mut out = Vec::new();
        let Some(mut i) = head.checked_sub(1) else { return out };
        loop {
            let f = self.face(i);
            if f.from.load(Ordering::Relaxed) <= t && f.to.load(Ordering::Acquire) > t {
                out.push(f.eid.load(Ordering::Relaxed));
            }
            i = f.prev_v.load(Ordering::Acquire);
            if i == NONE {
                break;
            }
        }
        out.sort_unstable();
        out
    }

    /// 出した面の数。
    pub fn face_count(&self) -> u64 {
        self.next.load(Ordering::Relaxed).min(NONE - 1) as u64
    }

    /// 取った塊の大きさ (byte、 塊の表は除く)。
    pub fn bytes(&self) -> usize {
        self.faces.allocated() * CHUNK * std::mem::size_of::<Face>() + (self.heads.allocated() + self.decks.allocated()) * CHUNK * 4
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

    /// 今の時刻 (最後に回した球の時刻)。 この後の書き込みは、 これより大きい時刻になる。
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

    /// 値を書いた後 (行の lock の下)。
    #[inline]
    pub fn on_set(&self, hid: u16, eid: u32, v: u64) {
        if let Some(b) = self.get(hid) {
            b.turn(eid, Some(v), &self.clock);
        }
    }

    /// 値を外した後 (行の lock の下)。
    #[inline]
    pub fn on_remove(&self, hid: u16, eid: u32) {
        if let Some(b) = self.get(hid) {
            b.turn(eid, None, &self.clock);
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn faces_keep_the_old_values() {
        let b = Balls::new();
        assert!(b.declare(3, |_| {}));
        b.on_set(3, 7, 10);
        let t1 = b.now();
        b.on_set(3, 7, 10); // 同じ値は回さない
        assert_eq!(b.now(), t1);
        b.on_set(3, 7, 20);
        let t2 = b.now();
        b.on_remove(3, 7);
        let t3 = b.now();
        let ball = b.get(3).unwrap();
        assert_eq!(ball.value_at(7, 0), None, "書く前");
        assert_eq!(ball.value_at(7, t1), Some(10));
        assert_eq!(ball.value_at(7, t2), Some(20));
        assert_eq!(ball.value_at(7, t3), None, "外した後");
        assert_eq!(
            ball.faces_of(7),
            vec![FaceView { value: 10, from: t1, to: t2 }, FaceView { value: 20, from: t2, to: t3 }]
        );
        assert_eq!(ball.pull_at(10, t1), vec![7]);
        assert_eq!(ball.pull_at(10, t2), Vec::<u32>::new());
        assert_eq!(ball.pull_at(20, t2), vec![7]);
    }

    #[test]
    fn seeded_faces_are_from_the_start() {
        let b = Balls::new();
        b.declare(0, |f| {
            f(1, 5);
            f(2, 5);
        });
        b.on_set(0, 1, 6);
        let t = b.now();
        let ball = b.get(0).unwrap();
        assert_eq!(ball.pull_at(5, 0), vec![1, 2]);
        assert_eq!(ball.pull_at(5, t), vec![2]);
        assert_eq!(ball.value_at(1, t - 1), Some(5));
    }

    #[test]
    fn wide_values_and_reused_eids() {
        let b = Balls::new();
        b.declare(1, |_| {});
        let big = 1u64 << 40;
        b.on_set(1, 4, big);
        let t = b.now();
        b.on_remove(1, 4); // delete も外すだけ
        b.on_set(1, 4, 9); // 使い回した eid の次の entity
        let ball = b.get(1).unwrap();
        assert_eq!(ball.pull_at(big, t), vec![4]);
        assert_eq!(ball.value_at(4, t), Some(big), "球の糸もデッキの糸と同じく前の面を答える");
        assert_eq!(ball.value_at(4, t + 1), None);
        assert_eq!(
            ball.faces_of(4),
            vec![FaceView { value: big, from: t, to: t + 1 }, FaceView { value: 9, from: t + 2, to: OPEN }]
        );
    }

    /// 書き手 4 本が自分の entity 8 個に、 同じ値の組 (0..5) を書いたり外したりする。 回ごとに Barrier でそろえ、 そろった時刻を
    /// 外から控える (= その時刻の答えは手元の写しで決まる、 球の付けた時刻を使わない)。 最後に、 全部の回の時刻でデッキの答えと
    /// 球の答えを写しと比べる。
    #[test]
    fn concurrent_writers_agree_with_a_log() {
        const TH: u32 = 4;
        const PER: u32 = 8;
        const VALS: u64 = 5;
        let b = Balls::new();
        b.declare(0, |_| {});
        let rounds = if cfg!(miri) { 4 } else { 300 };
        let bar = std::sync::Barrier::new(TH as usize + 1);
        // 書き手が自分で持つ写し (書いた値 / 外した = None)
        let log = std::sync::Mutex::new(vec![None::<u64>; (TH * PER) as usize]);
        // checks[r] = (回 r の終わりの時刻, entity ごとの値)
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
                            let e = th * PER + (r % PER as u64) as u32;
                            match (r >> 8) % (VALS + 1) {
                                VALS => {
                                    b.on_remove(0, e);
                                    mine[(e - th * PER) as usize] = None;
                                }
                                v => {
                                    b.on_set(0, e, v);
                                    mine[(e - th * PER) as usize] = Some(v);
                                }
                            }
                        }
                        log.lock().unwrap()[(th * PER) as usize..((th + 1) * PER) as usize].copy_from_slice(&mine);
                        bar.wait();
                        bar.wait();
                    }
                });
            }
            for _ in 0..rounds {
                bar.wait();
                // 全員が書き終えて待っている間に、 時刻と写しを控える
                checks.push((b.now(), log.lock().unwrap().clone()));
                bar.wait();
            }
        });
        let ball = b.get(0).unwrap();
        for (r, (t, want)) in checks.iter().enumerate() {
            for v in 0..VALS {
                let deck: Vec<u32> = (0..TH * PER).filter(|&e| want[e as usize] == Some(v)).collect();
                assert_eq!(ball.pull_at(v, *t), deck, "回 {r} t={t} v={v}");
            }
            for e in 0..TH * PER {
                assert_eq!(ball.value_at(e, *t), want[e as usize], "回 {r} t={t} e={e}");
            }
        }
    }
}
