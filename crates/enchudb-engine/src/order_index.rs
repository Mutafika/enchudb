//! 並びの索引 ([`OrderIndex`]、 2026-10-10)。 ref 紐 `via` の逆引き (「会社 c を指している社員」) を、 別の紐 `key`
//! (年齢など) の値の帯ごとに分けて持つ。 会社単位の購読の 「会社 c の 30 歳以上は誰 / 何人」 を、 会社の全員をなめずに
//! 30 歳以上の帯だけ読む / 帯の件数を足すだけで答える。 宣言した via の紐の逆引き (`pull` など) も、 全部の帯を読んで
//! これが答える (その紐の円柱は作らない。 [`all_bands`](OrderIndex::all_bands))。
//!
//! ## 形
//!
//! - **帯**: 目盛り (昇順) の間。 内部では目盛りの先頭に 0 を足すので、 帯 0 = key に値が無い、 帯 1 = `[0, t1)`、
//!   帯 k = `[t_{k-1}, t_k)`、 最後の帯 = `[t_last, ∞)`
//! - **置き場**: (ref の先の値, 帯) ごとに 1 本の [`AppendBucket`] (足すだけ。 読みは lock-free、 一度 publish した
//!   要素は動かさない)。 置き場の番号 = `(値 − base) × 帯の数 + 帯` (`base` = ref の先の table の eid の始まり)。
//!   番号は 3 段の表 ([`Radix`]) で引き、 値 1024 個ぶん (× 帯の数) の葉を使う所だけ置く。 置いた葉は drop まで動かさない
//!   (伸ばす時の写しも epoch も要らない)。 中身の無い置き場は bucket を作らない ([`Slot`] の null)。 HashMap は使わない
//! - **置き場所の記録** (`placed`): entity ごとに今どの置き場に居るか (番号 + 1、 0 = どこにも居ない)。 同じく 3 段の表
//!
//! ## 書き込み
//!
//! via か key の列が書かれた後 ([`crate::engine::Engine`] の `live_set` / `live_remove`、 行の lock の下) に
//! [`place`](OrderIndex::place): 今の (via の値, key の帯) と記録が違えば、 記録の置き場の bucket を stale にし
//! (live −1、 removed flag)、 新しい置き場の bucket に足して記録を直す。 何帯動いても O(1)、 位置は持たない
//! (#95 の 「消さずに古い印」 と同じ)。
//!
//! 鍵: bucket は 1 本ごとに書き手 1 本の約束なので、 置き場を触る間は **値ごとの鍵** (値の下位 bit で選ぶ 64 本、
//! [`STRIPES`]) を取る (古い値と新しい値の 2 本、 番号の小さい方から)。 同じ entity の記録を書くのは、 その entity の
//! 書き手 = 行の lock を握っている thread だけ (engine の全部の書き込みの道が行の lock の下で `live_set` /
//! `live_remove` を呼ぶ)。 記録は置き場の鍵の下で書く (詰め直しが同じ鍵の下で他の entity の記録を読むので)。
//! 会社の紐の write_lock は作る前の確認にしか使わない — 年齢の書き手どうしは、 違う会社の社員なら待たない
//! (2026-10-10: 全部を via の write_lock で並べた版は、 年齢の書き手 4 本で 1 回 0.66 → 1.9 µs になった)。
//!
//! stale が bucket の半分を超えたら、 記録で残すものを決めて詰め直す (同じ置き場の鍵の下)。
//!
//! ## 読み
//!
//! [`arc_into`](OrderIndex::arc_into): 値 v の帯 `k_lo..=k_hi` の bucket を続けて読む。 stale のある bucket を読んだら、
//! 読んだ分を今の値で確かめて並べ直す (`HimoStore::pull` と同じ)。 [`arc_len`](OrderIndex::arc_len) = bucket の live の和
//! (正確)。 範囲 `lo..=hi` が帯の境目とちょうど合う時 (`exact`) は、 範囲の条件を entity ごとに確かめなくてよい。
//!
//! 帯を 2 本以上読む時は、 読み手は鍵を取らないので、 帯の間の行き来 (古い帯 −1 → 新しい帯 +1) の途中を読むと、 同じ
//! entity を 2 回数えたり (件数 2)、 詰め直しで消えた古い帯と足す前の新しい帯を読んで取りこぼしたり (件数 0) する
//! (2026-10-10 に実測: 30 万回の読みで件数違い 13 万回)。 そこで置き場の鍵ごとに版 (seqlock) を置き、 書き手は書き換えの
//! 前後で進め、 読み手は各帯の bucket を **控える** (publish 済みの slice と verify の要否、 数十 ns) 間だけ版を前後で
//! 比べる。 控えた slice は guard の間は不変なので、 中身を写すのは揃えた後でよい。 揃わなければ控え直し、 何度も揃わ
//! なければ None (呼び手は普通の逆引き)。 帯 1 本は bucket の 3 段の確かめ (slice → flag → backing) で足りる。
//!
//! ## 作る
//!
//! 遅延: 最初に読む時に、 via の write_lock の下で via の列をなめて全部 `place` する (円柱の遅延構築 #270 と同じく、
//! 作る前の書き込みは何もしない)。 書き手は 「作ったか」 を見て、 作っていなければ via の write_lock の下でもう一度見る
//! (作る側はその lock の下で列をなめるので、 見落としが無い。 `tests/loom_order_index_build.rs`)。 葉が置く entity の数に
//! 比べて多すぎる (値が散っている) か、 置き場の番号に入らない値 (`base` より前 = ref の先の表の外) が来たら索引をやめる
//! (`disabled`)。 やめた後の読みは None (呼び手は普通の逆引きを使う)。

use crate::append_bucket::AppendBucket;
use crate::lockfree_cylinder::Slot;
use crossbeam_epoch as epoch;
use parking_lot::Mutex;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicU64, AtomicUsize, Ordering};

/// 3 段の表の下 2 段の幅 (1 段 1024 = 10 bit)。 一番上は残りの 12 bit (4096) で、 u32 の全部を覆う。
const BITS: u32 = 10;
const FAN: usize = 1 << BITS;
const TOP: usize = 1 << (32 - 2 * BITS);
/// 置き場の鍵の本数 (値の下位 bit で選ぶ)。
pub(crate) const STRIPES: usize = 64;

/// 3 段の表: 番号 i (u32) の上位 22 bit → 葉 T (下位 10 bit は葉の中で引く)。 使う所だけ置き、 置いたら drop まで
/// 動かさない。 読みは lock なし、 置くのは CAS (同時に置いたら負けた方を捨てる)。
struct Radix<T> {
    top: Box<[AtomicPtr<Mid<T>>]>,
    /// 置いた葉の数 (伸ばしすぎの判定と診断)。
    leaves: AtomicUsize,
    mids: AtomicUsize,
    _own: PhantomData<Box<T>>,
}

struct Mid<T>(Box<[AtomicPtr<T>]>);

fn nulls<T>(n: usize) -> Box<[AtomicPtr<T>]> {
    (0..n).map(|_| AtomicPtr::new(std::ptr::null_mut())).collect()
}

impl<T> Radix<T> {
    fn new() -> Self {
        Radix { top: nulls(TOP), leaves: AtomicUsize::new(0), mids: AtomicUsize::new(0), _own: PhantomData }
    }

    /// 番号 `i` の葉 (無ければ None)。
    #[inline]
    fn get(&self, i: u32) -> Option<&T> {
        let m = self.top[(i >> (2 * BITS)) as usize].load(Ordering::Acquire);
        if m.is_null() {
            return None;
        }
        // SAFETY: 非 null なら `Box::into_raw` で置いた Mid で、 drop (`&mut self`) まで解放しない。
        let l = unsafe { &*m }.0[(i >> BITS) as usize & (FAN - 1)].load(Ordering::Acquire);
        // SAFETY: 同上 (葉)。
        (!l.is_null()).then(|| unsafe { &*l })
    }

    /// 番号 `i` の葉 (無ければ `make` で作って置く)。
    fn get_or_insert(&self, i: u32, make: impl FnOnce() -> T) -> &T {
        let top = &self.top[(i >> (2 * BITS)) as usize];
        let mut m = top.load(Ordering::Acquire);
        if m.is_null() {
            let fresh = Box::into_raw(Box::new(Mid(nulls::<T>(FAN))));
            match top.compare_exchange(std::ptr::null_mut(), fresh, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    self.mids.fetch_add(1, Ordering::Relaxed);
                    m = fresh;
                }
                Err(cur) => {
                    // SAFETY: 置けなかった fresh はまだ誰も見ていない。
                    drop(unsafe { Box::from_raw(fresh) });
                    m = cur;
                }
            }
        }
        // SAFETY: 非 null の Mid (`get` と同じ)。
        let slot = &unsafe { &*m }.0[(i >> BITS) as usize & (FAN - 1)];
        let mut l = slot.load(Ordering::Acquire);
        if l.is_null() {
            let fresh = Box::into_raw(Box::new(make()));
            match slot.compare_exchange(std::ptr::null_mut(), fresh, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    self.leaves.fetch_add(1, Ordering::Relaxed);
                    l = fresh;
                }
                Err(cur) => {
                    // SAFETY: 同上。
                    drop(unsafe { Box::from_raw(fresh) });
                    l = cur;
                }
            }
        }
        // SAFETY: 非 null の葉 (`get` と同じ)。
        unsafe { &*l }
    }

    /// 置いた葉を全部 (診断用)。
    fn for_each(&self, mut f: impl FnMut(&T)) {
        for t in self.top.iter() {
            let m = t.load(Ordering::Acquire);
            if m.is_null() {
                continue;
            }
            // SAFETY: `get` と同じ。
            for l in unsafe { &*m }.0.iter() {
                let p = l.load(Ordering::Acquire);
                if !p.is_null() {
                    // SAFETY: 同上。
                    f(unsafe { &*p });
                }
            }
        }
    }

    fn bytes(&self, leaf: usize) -> usize {
        TOP * 8 + self.mids.load(Ordering::Relaxed) * (FAN * 8 + 16) + self.leaves.load(Ordering::Relaxed) * leaf
    }
}

impl<T> Drop for Radix<T> {
    fn drop(&mut self) {
        for t in self.top.iter_mut() {
            let m = *t.get_mut();
            if m.is_null() {
                continue;
            }
            // SAFETY: drop は排他 (読み手も書き手も居ない)。 Mid と葉は `Box::into_raw` で置いたもの。
            let mid = unsafe { Box::from_raw(m) };
            for l in mid.0.iter() {
                let p = l.load(Ordering::Relaxed);
                if !p.is_null() {
                    // SAFETY: 同上。
                    drop(unsafe { Box::from_raw(p) });
                }
            }
        }
    }
}

/// 値 1024 個ぶんの置き場 (値ごとに帯の数だけ並ぶ)。
struct Bins(Box<[Slot]>);

/// 置き場の鍵 1 本 (隣の鍵と cache line を分ける。 Apple の M 系は 128 B)。
#[repr(align(128))]
struct Stripe {
    lock: Mutex<()>,
    /// 版 (奇数 = 書き換えの最中)。 鍵を持つ書き手が書き換えの前後で 1 ずつ進め、 帯を 2 本以上読む読み手が読む前と後で
    /// 比べる (seqlock)。 帯の間の行き来 (古い帯を −1 して新しい帯に +1) を、 読み手が片方だけ見ないように。
    seq: AtomicU64,
}

/// 帯を 2 本以上読む時、 控える間に書き換えと重なって控え直す回数の上限 (超えたら None = 呼び手は普通の逆引きを使う)。
const READ_TRIES: usize = 8;
/// 版が奇数 (書き換えの最中) の間に待つ回数の上限 (書き手が鍵を持ったまま止まった時に、 読み手が待ち続けない)。
const SPIN_LIMIT: usize = 1 << 12;

/// 範囲 `lo..=hi` が当たる帯と、 その帯がちょうど範囲と一致するか。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Bands {
    pub(crate) lo: usize,
    pub(crate) hi: usize,
    pub(crate) exact: bool,
}

pub(crate) struct OrderIndex {
    pub(crate) via: u16,
    pub(crate) key: u16,
    /// 内部の目盛り (先頭に 0、 狭義の昇順)。 帯の数 = `ticks.len() + 1`。
    ticks: Vec<u64>,
    /// 宣言された目盛り (宣言の照合用)。
    declared: Vec<u64>,
    /// key の列に入る最大の値 (`hi` がこれ以上の範囲は最後の帯とちょうど一致する)。
    key_max: u64,
    /// 置き場の番号の 0 が指す ref の値。
    base: u32,
    /// 置き場: 番号 (値 − base) → 帯の数ぶんの Slot。
    slots: Radix<Bins>,
    /// 置き場所の記録: entity → 置き場の番号 + 1 (0 = どこにも居ない)。
    placed: Radix<[AtomicU32; FAN]>,
    stripes: Box<[Stripe]>,
    /// 置き場に居る entity の数 (葉を置きすぎていないかの目安)。
    n_placed: AtomicUsize,
    /// 作る時に置く entity の数 (作っている間の目安。 置いた数だけで判定すると、 散った値を最初の方で置いた時にやめる)。
    planned: AtomicUsize,
    built: AtomicBool,
    disabled: AtomicBool,
    /// 計測: 読んだ回数と、 読んだ entity の数 (stale 込み)。
    hits: AtomicU64,
    read: AtomicU64,
}

impl OrderIndex {
    /// `ticks` は宣言された目盛り (狭義の昇順であること、 呼び手が確かめる)。
    pub(crate) fn new(via: u16, key: u16, ticks: &[u64], key_max: u64, base: u32) -> Self {
        let mut t = Vec::with_capacity(ticks.len() + 1);
        if ticks.first() != Some(&0) {
            t.push(0);
        }
        t.extend_from_slice(ticks);
        OrderIndex {
            via,
            key,
            ticks: t,
            declared: ticks.to_vec(),
            key_max,
            base,
            slots: Radix::new(),
            placed: Radix::new(),
            stripes: (0..STRIPES).map(|_| Stripe { lock: Mutex::new(()), seq: AtomicU64::new(0) }).collect(),
            n_placed: AtomicUsize::new(0),
            planned: AtomicUsize::new(0),
            built: AtomicBool::new(false),
            disabled: AtomicBool::new(false),
            hits: AtomicU64::new(0),
            read: AtomicU64::new(0),
        }
    }

    /// 宣言された目盛り。
    pub(crate) fn declared(&self) -> &[u64] {
        &self.declared
    }

    #[inline]
    fn n_bands(&self) -> usize {
        self.ticks.len() + 1
    }

    /// key の値の帯 (値が無い = 0)。
    #[inline]
    pub(crate) fn band(&self, v: Option<u64>) -> usize {
        v.map_or(0, |v| self.ticks.partition_point(|&t| t <= v))
    }

    /// 範囲 `lo..=hi` (lo <= hi) が当たる帯。
    pub(crate) fn bands_of(&self, lo: u64, hi: u64) -> Bands {
        let (k_lo, k_hi) = (self.band(Some(lo)), self.band(Some(hi)));
        let lo_ok = self.ticks[k_lo - 1] == lo;
        let hi_ok = match self.ticks.get(k_hi) {
            Some(&t) => hi.checked_add(1) == Some(t),
            None => hi >= self.key_max,
        };
        Bands { lo: k_lo, hi: k_hi, exact: lo_ok && hi_ok }
    }

    /// 全部の帯 (値が無い帯 0 から最後の帯まで) = via の値を指している entity の全員 (`pull` の代わりに読む時)。
    pub(crate) fn all_bands(&self) -> Bands {
        Bands { lo: 0, hi: self.n_bands() - 1, exact: true }
    }

    /// ref の値 `v` の帯 `k` の置き場の番号 (`v < base` か、 番号 + 1 が u32 に入らなければ None)。
    #[inline]
    fn slot_no(&self, v: u32, k: usize) -> Option<u32> {
        let d = u64::from(v.checked_sub(self.base)?);
        let s = d * self.n_bands() as u64 + k as u64;
        (s < u64::from(u32::MAX)).then_some(s as u32)
    }

    /// 番号 `s` の値の置き場 (帯の数ぶん)。 葉が無ければ None。
    #[inline]
    fn bins_at(&self, s: u32) -> Option<&[Slot]> {
        let nb = self.n_bands();
        let d = s as usize / nb;
        let leaf = self.slots.get(d as u32)?;
        let at = (d & (FAN - 1)) * nb;
        Some(&leaf.0[at..at + nb])
    }

    /// 番号 `s` の置き場 (葉が無ければ作る)。 **その値の置き場の鍵の下で**。
    fn slot_make(&self, s: u32) -> &Slot {
        let nb = self.n_bands();
        let d = s as usize / nb;
        let leaf = self.slots.get_or_insert(d as u32, || Bins((0..FAN * nb).map(|_| Slot::empty()).collect()));
        &leaf.0[(d & (FAN - 1)) * nb + s as usize % nb]
    }

    /// 番号 `s` の置き場の鍵の添字。
    #[inline]
    fn stripe_of(&self, s: u32) -> usize {
        (s as usize / self.n_bands()) & (STRIPES - 1)
    }

    #[inline]
    fn placed_get(&self, e: u32) -> u32 {
        self.placed.get(e).map_or(0, |p| p[e as usize & (FAN - 1)].load(Ordering::Relaxed))
    }

    pub(crate) fn is_built(&self) -> bool {
        self.built.load(Ordering::Acquire)
    }

    pub(crate) fn is_disabled(&self) -> bool {
        self.disabled.load(Ordering::Acquire)
    }

    /// 番号 `s` の置き場に葉が要る時、 置いてよいか。 葉が置く entity の数に比べて多すぎる (値が散っている) なら false。
    fn room_for(&self, s: u32) -> bool {
        if self.bins_at(s).is_some() {
            return true;
        }
        // 葉 1 枚 = 値 1024 個。 置く entity の 4 倍 + 余裕 の値の数まで (平らな配列で 「entity の 4 倍まで伸ばしてよい」
        // としていたのと同じ大きさ)
        let n = self.n_placed.load(Ordering::Relaxed).max(self.planned.load(Ordering::Relaxed));
        self.slots.leaves.load(Ordering::Relaxed) < (n + (1 << 16)) * 4 / FAN
    }

    /// entity `e` を今の (ref の値 `v`, key の値 `k`) の置き場に置き直す (記録と同じなら何もしない)。
    /// **e の書き手として** 呼ぶ (engine では行の lock の下、 作る時は via の write_lock の下で書き手が居ない間)。
    pub(crate) fn place(&self, e: u32, v: Option<u32>, k: Option<u64>) {
        if self.is_disabled() {
            return;
        }
        let want = match v {
            None => 0,
            Some(v) => match self.slot_no(v, self.band(k)) {
                Some(s) => s + 1,
                // ref の先が base より前 (表の外) か番号に入らない: 置けないので索引をやめる (読みは普通の逆引きに戻る)
                None => {
                    self.disabled.store(true, Ordering::Release);
                    return;
                }
            },
        };
        let cur = self.placed_get(e);
        if cur == want {
            return;
        }
        if want != 0 && !self.room_for(want - 1) {
            self.disabled.store(true, Ordering::Release);
            return;
        }
        // 古い置き場と新しい置き場の鍵 (添字の小さい方から。 同じなら 1 本)
        let (a, b) = match (cur, want) {
            (0, w) => (self.stripe_of(w - 1), usize::MAX),
            (c, 0) => (self.stripe_of(c - 1), usize::MAX),
            (c, w) => {
                let (x, y) = (self.stripe_of(c - 1), self.stripe_of(w - 1));
                if x == y { (x, usize::MAX) } else { (x.min(y), x.max(y)) }
            }
        };
        let _ga = self.stripes[a].lock.lock();
        let _gb = (b != usize::MAX).then(|| self.stripes[b].lock.lock());
        // 版を奇数に (読み手が 「書き換えの最中」 を見る)。 書き換えの後で偶数に戻す
        self.seq_begin(a);
        if b != usize::MAX {
            self.seq_begin(b);
        }
        let guard = epoch::pin();
        if cur != 0 {
            let nb = self.n_bands();
            if let Some(bk) = self.bins_at(cur - 1).and_then(|bins| bins[(cur - 1) as usize % nb].get()) {
                bk.note_stale();
                // stale が半分を超えたら、 記録でこの置き場に居るものだけ残して詰め直す (重複も落ちる)。 他の entity の
                // 記録は、 この置き場に出入りするならこの鍵を待つので、 読む間は動かない
                let len = bk.len();
                if len >= 64 && (len - bk.live() as usize) * 2 >= len {
                    bk.compact_in(&guard, |x| x != e && self.placed_get(x) == cur);
                }
            }
        }
        if want != 0 {
            let (bk, _) = self.slot_make(want - 1).get_or_create();
            bk.push_in(e, &guard);
            bk.live_inc();
        }
        match (cur, want) {
            (0, _) => {
                self.n_placed.fetch_add(1, Ordering::Relaxed);
            }
            (_, 0) => {
                self.n_placed.fetch_sub(1, Ordering::Relaxed);
            }
            _ => {}
        }
        // 記録は置き場の鍵の下で (詰め直しがこの鍵の下で読む)
        self.placed.get_or_insert(e, || std::array::from_fn(|_| AtomicU32::new(0)))[e as usize & (FAN - 1)]
            .store(want, Ordering::Relaxed);
        self.seq_end(a);
        if b != usize::MAX {
            self.seq_end(b);
        }
    }

    /// 書き換えの前: 版を奇数に。 **その鍵の下で**。 Release の fence で、 この後の書き換えを見た読み手は奇数か後の版を見る。
    #[inline]
    fn seq_begin(&self, i: usize) {
        let seq = &self.stripes[i].seq;
        seq.store(seq.load(Ordering::Relaxed) + 1, Ordering::Relaxed);
        std::sync::atomic::fence(Ordering::Release);
    }

    /// 書き換えの後: 版を偶数に (Release = 書き換えを見せてから)。 **その鍵の下で**。
    #[inline]
    fn seq_end(&self, i: usize) {
        let seq = &self.stripes[i].seq;
        seq.store(seq.load(Ordering::Relaxed) + 1, Ordering::Release);
    }

    /// 作る: `entities` (via の列に値のある entity) を全部、 `via_of` / `key_of` (今の via / key の値) で置く。
    /// **via の write_lock の下で** (書き手は作り終えるまで置かない)。
    pub(crate) fn build(&self, entities: &[u32], via_of: impl Fn(u32) -> Option<u32>, key_of: impl Fn(u32) -> Option<u64>) {
        if self.is_built() {
            return;
        }
        self.planned.store(entities.len(), Ordering::Relaxed);
        for &e in entities {
            if self.is_disabled() {
                break;
            }
            self.place(e, via_of(e), key_of(e));
        }
        self.built.store(true, Ordering::Release);
    }

    /// 値 `v` の帯 `bands.lo..=bands.hi` の entity を `out` の後ろに足す。 返り値 = 足した分が stale を含みうるか
    /// (呼び手が今の値で確かめる)。 作る前 / やめた後 / `v` が base より前は None。
    ///
    /// 帯を 2 本以上読む時は、 各帯の bucket を **控えて** (publish 済みの slice と verify が要るか。
    /// `AppendBucket::snapshot_in`) から中身を写す。 控える間だけ置き場の鍵の版を前後で比べ、 変わっていたら控え直す。
    /// 揃った控えは 1 つの瞬間の中身なので、 帯の間を行き来した entity を 2 回数えたり、 詰め直しの後の古い帯と足す前の
    /// 新しい帯を読んで取りこぼしたりしない。 控えた slice は guard の間は不変なので、 中身を写すのは揃えた後でよい
    /// (大きい会社を写す間に書き込みが来ても控え直さない)。 控え直しが [`READ_TRIES`] 回で揃わなければ None。
    /// 帯 1 本は bucket の 3 段の確かめで足りる。
    pub(crate) fn arc_into(&self, v: u32, bands: Bands, out: &mut Vec<u32>) -> Option<bool> {
        if !self.is_built() || self.is_disabled() {
            return None;
        }
        let s = self.slot_no(v, bands.lo)?;
        let guard = epoch::pin();
        let start = out.len();
        let mut verify = false;
        if let Some(bins) = self.bins_at(s) {
            let bins = &bins[bands.lo..=bands.hi];
            if let [one] = bins {
                if let Some(b) = one.get() {
                    verify = b.read_snapshot_verify_into(&guard, out);
                }
            } else {
                let q = &self.stripes[self.stripe_of(s)].seq;
                // 控える間に heap を確保しない (控える間を短く)
                let mut snaps = Vec::with_capacity(bins.len());
                self.consistent(q, || {
                    snaps.clear();
                    snaps.extend(bins.iter().filter_map(Slot::get).map(|b| b.snapshot_in(&guard)));
                })?;
                for (slice, v) in snaps {
                    out.extend_from_slice(slice);
                    verify |= v;
                }
            }
        }
        self.hits.fetch_add(1, Ordering::Relaxed);
        self.read.fetch_add((out.len() - start) as u64, Ordering::Relaxed);
        Some(verify)
    }

    /// 値 `v` の帯 `bands.lo..=bands.hi` の件数 (bucket の live の和、 正確)。 作る前 / やめた後 / `v` が base より前は
    /// None。 帯を 2 本以上読む時は [`arc_into`](Self::arc_into) と同じく版で揃える (揃わなければ None)。
    pub(crate) fn arc_len(&self, v: u32, bands: Bands) -> Option<usize> {
        if !self.is_built() || self.is_disabled() {
            return None;
        }
        let s = self.slot_no(v, bands.lo)?;
        let n = match self.bins_at(s) {
            None => 0,
            Some(bins) => {
                let bins = &bins[bands.lo..=bands.hi];
                let sum = || bins.iter().filter_map(Slot::get).map(|b| b.live() as usize).sum::<usize>();
                if bins.len() == 1 { sum() } else { self.consistent(&self.stripes[self.stripe_of(s)].seq, sum)? }
            }
        };
        self.hits.fetch_add(1, Ordering::Relaxed);
        Some(n)
    }

    /// 置き場の鍵の版 `q` が前後で同じ (偶数) 間に `f` を呼んだ結果 (seqlock の読み手)。 揃わなければ控え直し、
    /// [`READ_TRIES`] 回揃わないか、 版が奇数のまま [`SPIN_LIMIT`] 回待っても進まなければ None。
    fn consistent<R>(&self, q: &AtomicU64, mut f: impl FnMut() -> R) -> Option<R> {
        let (mut tries, mut spins) = (0, 0);
        loop {
            let s1 = q.load(Ordering::Acquire);
            if s1 & 1 == 1 {
                spins += 1;
                if spins > SPIN_LIMIT {
                    return None;
                }
                std::hint::spin_loop();
                continue;
            }
            let r = f();
            std::sync::atomic::fence(Ordering::Acquire);
            if q.load(Ordering::Relaxed) == s1 {
                return Some(r);
            }
            tries += 1;
            if tries >= READ_TRIES {
                return None;
            }
        }
    }

    /// 計測: (読んだ回数, 読んだ entity の数)。
    pub(crate) fn stats(&self) -> (u64, u64) {
        (self.hits.load(Ordering::Relaxed), self.read.load(Ordering::Relaxed))
    }

    /// 索引が使っている heap の大きさ (表、 葉、 bucket、 記録、 鍵)。 診断用。
    pub(crate) fn heap_bytes(&self) -> usize {
        let leaf = FAN * self.n_bands() * std::mem::size_of::<Slot>() + 16;
        let mut buckets = 0usize;
        self.slots.for_each(|bins| {
            buckets += bins.0.iter().filter_map(Slot::get).map(|b| std::mem::size_of::<AppendBucket>() + 24 + b.capacity() * 4).sum::<usize>();
        });
        self.slots.bytes(leaf) + buckets + self.placed.bytes(FAN * 4) + STRIPES * std::mem::size_of::<Stripe>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `rows` = (entity, via の値, key の値) で作る。
    fn build_from(o: &OrderIndex, rows: &[(u32, Option<u32>, Option<u64>)]) {
        let ids: Vec<u32> = rows.iter().map(|r| r.0).collect();
        let find = |e: u32| rows.iter().find(|r| r.0 == e).copied().unwrap();
        o.build(&ids, |e| find(e).1, |e| find(e).2);
    }

    #[test]
    fn bands_and_exact_ranges() {
        let o = OrderIndex::new(0, 1, &[30, 65], u32::MAX as u64 - 1, 0);
        assert_eq!(o.band(None), 0);
        assert_eq!(o.band(Some(0)), 1);
        assert_eq!(o.band(Some(29)), 1);
        assert_eq!(o.band(Some(30)), 2);
        assert_eq!(o.band(Some(64)), 2);
        assert_eq!(o.band(Some(65)), 3);
        assert_eq!(o.bands_of(30, 64), Bands { lo: 2, hi: 2, exact: true });
        assert_eq!(o.bands_of(30, u32::MAX as u64 - 1), Bands { lo: 2, hi: 3, exact: true });
        assert_eq!(o.bands_of(0, 29), Bands { lo: 1, hi: 1, exact: true });
        assert!(!o.bands_of(31, 64).exact, "下の端が目盛りでない");
        assert!(!o.bands_of(30, 1000).exact, "上の端が目盛りの手前でも列の最大でもない");
        assert_eq!(o.bands_of(31, 1000), Bands { lo: 2, hi: 3, exact: false });
        // 目盛りが 0 から始まる宣言は 0 を足さない
        let z = OrderIndex::new(0, 1, &[0, 10], 100, 0);
        assert_eq!(z.band(Some(0)), 1);
        assert_eq!(z.band(Some(10)), 2);
        assert_eq!(z.bands_of(10, 100), Bands { lo: 2, hi: 2, exact: true });
    }

    /// 3 段の表: 使う所だけ葉を置く、 番号の上の方 (u32 の端) も引ける、 置いた葉は同じものが返る。
    #[test]
    fn radix_places_leaves_only_where_used() {
        let r: Radix<[AtomicU32; FAN]> = Radix::new();
        assert!(r.get(5_000_000).is_none());
        let a = r.get_or_insert(5_000_000, || std::array::from_fn(|_| AtomicU32::new(0)));
        a[5_000_000 & (FAN - 1)].store(7, Ordering::Relaxed);
        assert_eq!(r.get(5_000_000).unwrap()[5_000_000 & (FAN - 1)].load(Ordering::Relaxed), 7);
        assert!(std::ptr::eq(r.get(5_000_001).unwrap(), a), "同じ 1024 個の中は同じ葉");
        assert!(r.get(5_000_000 + FAN as u32).is_none(), "隣の葉は置いていない");
        let top = r.get_or_insert(u32::MAX, || std::array::from_fn(|_| AtomicU32::new(0)));
        top[FAN - 1].store(9, Ordering::Relaxed);
        assert_eq!(r.get(u32::MAX).unwrap()[FAN - 1].load(Ordering::Relaxed), 9);
        assert_eq!((r.leaves.load(Ordering::Relaxed), r.mids.load(Ordering::Relaxed)), (2, 2));
        let mut n = 0;
        r.for_each(|_| n += 1);
        assert_eq!(n, 2);
    }

    /// 置き直し: 帯を何個またいでも 1 回、 live が件数と合う、 詰め直しても残すものは記録どおり。
    #[test]
    fn place_moves_between_bands_and_counts_stay_exact() {
        let o = OrderIndex::new(0, 1, &[30, 65], 1000, 10);
        build_from(&o, &[]);
        // 読んだ分と今の答えを比べる: stale の無い帯ならちょうど同じ、 stale がありうる (verify) なら今の答えを全部含み、
        // 余分は全部 「今はそこに居ない」 entity (呼び手が今の値で落とすもの)。 `truth(e)` = e の今の (値, key)
        let check = |v: u32, lo: u64, hi: u64, truth: &dyn Fn(u32) -> (u32, u64), label: &str| {
            let mut out = Vec::new();
            let verify = o.arc_into(v, o.bands_of(lo, hi), &mut out).unwrap();
            out.sort_unstable();
            let want: Vec<u32> = (0..200u32).filter(|&e| truth(e).0 == v && (lo..=hi).contains(&truth(e).1)).collect();
            if verify {
                out.dedup();
                let (live, extra): (Vec<u32>, Vec<u32>) =
                    out.iter().partition(|&&e| truth(e).0 == v && (lo..=hi).contains(&truth(e).1));
                assert_eq!(live, want, "{label}: v={v} {lo}..={hi} の今の答え");
                assert!(extra.iter().all(|&e| o.band(Some(truth(e).1)) < o.bands_of(lo, hi).lo
                    || o.band(Some(truth(e).1)) > o.bands_of(lo, hi).hi
                    || truth(e).0 != v), "{label}: 余分に今その帯に居る entity が混じった");
            } else {
                assert_eq!(out, want, "{label}: v={v} {lo}..={hi} (stale なし)");
            }
            assert_eq!(o.arc_len(v, o.bands_of(lo, hi)), Some(want.len()), "{label}: 件数 v={v} {lo}..={hi}");
        };
        // 会社 10 / 11、 社員 0..200
        for e in 0..200u32 {
            o.place(e, Some(10 + e % 2), Some((e % 90) as u64));
        }
        let first = |e: u32| (10 + e % 2, (e % 90) as u64);
        check(10, 30, 64, &first, "最初");
        let mut out = Vec::new();
        assert_eq!(o.arc_into(10, o.bands_of(30, 64), &mut out), Some(false), "書き換える前は stale が無い");
        // 何度も行き来させる (stale が溜まって詰め直しも走る)。 Miri では回数を減らす (それでも詰め直しは走る、 下で確かめる)
        let rounds: u64 = if cfg!(miri) { 10 } else { 50 };
        let mut pushes = 200usize;
        let mut at: Vec<(u32, usize)> = (0..200u32).map(|e| (first(e).0, o.band(Some(first(e).1)))).collect();
        for round in 0..rounds {
            for e in 0..200u32 {
                let (v, k) = (10 + (e + round as u32) % 2, (e as u64 + round * 7) % 90);
                o.place(e, Some(v), Some(k));
                if at[e as usize] != (v, o.band(Some(k))) {
                    at[e as usize] = (v, o.band(Some(k)));
                    pushes += 1;
                }
            }
        }
        // 詰め直しが走った: bucket に残っている数 (stale 込みで読んだ数) が、 足した数より少ない
        let mut raw = Vec::new();
        for v in [10u32, 11] {
            o.arc_into(v, o.bands_of(0, 1000), &mut raw).unwrap();
        }
        assert!(raw.len() < pushes, "詰め直しが走っていない (残り {} 件、 足した {pushes} 件)", raw.len());
        let last = rounds - 1;
        let now = |e: u32| (10 + (e + last as u32) % 2, (e as u64 + last * 7) % 90);
        for v in [10u32, 11] {
            for (lo, hi) in [(0u64, 29u64), (30, 64), (65, 1000), (0, 1000)] {
                check(v, lo, hi, &now, "行き来の後");
            }
        }
        // 外す (ref の値が無い) と、 どの帯にも居ない
        for e in 0..200u32 {
            o.place(e, None, None);
        }
        assert_eq!(o.arc_len(10, o.bands_of(0, 1000)), Some(0));
        check(10, 0, 1000, &|_| (0, 0), "外した後");
    }

    /// 並行の書き手 (entity ごとに 1 本 = 行の lock の代わりに entity を thread で分ける) が、 違う会社・同じ会社の間を
    /// 行き来しても、 止まった後の件数と中身が記録どおり (置き場の鍵が 2 本の時の順、 詰め直しと他の書き手の記録)。
    #[test]
    fn concurrent_writers_on_disjoint_entities_keep_counts_exact() {
        let o = std::sync::Arc::new(OrderIndex::new(0, 1, &[30, 65], 1000, 0));
        build_from(&o, &[]);
        let (threads, per, rounds) = if cfg!(miri) { (3u32, 20u32, 6u32) } else { (4, 300, 200) };
        // 会社 0..8 (鍵は値の下位 bit なので、 8 社は別々の鍵)。 社員 e は thread e % threads が書く
        let hs: Vec<_> = (0..threads)
            .map(|t| {
                let o = o.clone();
                std::thread::spawn(move || {
                    for r in 0..rounds {
                        for i in 0..per {
                            let e = i * threads + t;
                            o.place(e, Some((e + r) % 8), Some(u64::from((e * 7 + r * 13) % 90)));
                        }
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        let truth = |e: u32| ((e + rounds - 1) % 8, u64::from((e * 7 + (rounds - 1) * 13) % 90));
        let n = threads * per;
        for v in 0..8u32 {
            for (lo, hi) in [(0u64, 29u64), (30, 64), (65, 1000), (0, 1000)] {
                let want: Vec<u32> = (0..n).filter(|&e| truth(e).0 == v && (lo..=hi).contains(&truth(e).1)).collect();
                assert_eq!(o.arc_len(v, o.bands_of(lo, hi)), Some(want.len()), "件数 v={v} {lo}..={hi}");
                let mut out = Vec::new();
                o.arc_into(v, o.bands_of(lo, hi), &mut out).unwrap();
                out.sort_unstable();
                out.dedup();
                out.retain(|&e| truth(e).0 == v && (lo..=hi).contains(&truth(e).1));
                assert_eq!(out, want, "中身 v={v} {lo}..={hi}");
            }
        }
    }

    /// 読み手は帯を順に読む。 同じ値の帯の間を行き来する書き手と並行でも、 件数はいつも 1 (どちらかの帯に 1 人だけ居る)、
    /// 確かめなしで返した分 (返り値 false) はそのまま答え (重複も取りこぼしも無い)、 確かめる分 (true) にも必ず入っている。
    /// `gap` = 書き手が 1 回動かすごとに休む spin の数。 0 (休まず往復) では読み手はほとんど控え直しの上限で諦める
    /// (None = 呼び手は普通の逆引き。 正しさだけを見る)。 休みがあれば大半を索引から答える (対照)。
    fn band_moves_vs_readers(gap: usize, n: usize) -> (usize, usize, usize) {
        use std::sync::Arc;
        use std::sync::atomic::AtomicBool;
        let o = Arc::new(OrderIndex::new(0, 1, &[30], 1000, 0));
        build_from(&o, &[]);
        o.place(0, Some(5), Some(10));
        let stop = Arc::new(AtomicBool::new(false));
        let w = {
            let (o, stop) = (o.clone(), stop.clone());
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    for k in [40, 10] {
                        o.place(0, Some(5), Some(k));
                        for _ in 0..gap {
                            std::hint::spin_loop();
                        }
                    }
                }
            })
        };
        let (mut bad, mut got_len, mut got_read) = (0, 0, 0);
        let all = o.bands_of(0, 1000);
        for _ in 0..n {
            if let Some(c) = o.arc_len(5, all) {
                got_len += 1;
                bad += usize::from(c != 1);
            }
            let mut out = Vec::new();
            match o.arc_into(5, all, &mut out) {
                Some(false) => {
                    got_read += 1;
                    bad += usize::from(out != [0]);
                }
                Some(true) => {
                    got_read += 1;
                    bad += usize::from(!out.contains(&0));
                }
                None => {}
            }
        }
        stop.store(true, Ordering::Relaxed);
        w.join().unwrap();
        (bad, got_len, got_read)
    }

    #[test]
    fn readers_never_see_a_band_move_twice_or_not_at_all() {
        let n = if cfg!(miri) { 40 } else { 200_000 };
        let (bad, got_len, got_read) = band_moves_vs_readers(0, n);
        assert_eq!(bad, 0, "休まず往復: 違う答えを返した (件数 {got_len} 回 / 中身 {got_read} 回 答えた)");
        eprintln!("休まず往復: 件数 {got_len} / 中身 {got_read} / {n}");
        let (bad, got_len, got_read) = band_moves_vs_readers(if cfg!(miri) { 20 } else { 2000 }, n);
        assert_eq!(bad, 0, "休みあり: 違う答えを返した");
        eprintln!("休みあり: 件数 {got_len} / 中身 {got_read} / {n}");
        // 対照: 書き手に休みがあれば、 読み手は控え直しの上限で諦めてばかりではない
        if !cfg!(miri) {
            assert!(got_len > n / 2 && got_read > n / 2, "休みありでも索引から答えた回が少ない (件数 {got_len} / 中身 {got_read} / {n})");
        }
    }

    /// ref の先が base より前 (表の外) なら索引をやめる。 base より前の値の読みは None (普通の逆引きに戻る)。
    #[test]
    fn values_before_base_disable_the_index() {
        let o = OrderIndex::new(0, 1, &[30], 1000, 100);
        build_from(&o, &[(1, Some(100), Some(40)), (2, Some(101), Some(10))]);
        assert!(!o.is_disabled());
        assert_eq!(o.arc_len(100, o.bands_of(30, 1000)), Some(1));
        assert_eq!(o.arc_len(99, o.bands_of(30, 1000)), None, "base より前の値は読まない");
        let mut out = Vec::new();
        assert_eq!(o.arc_into(99, o.bands_of(30, 1000), &mut out), None);
        assert!(out.is_empty());
        o.place(3, Some(50), Some(40));
        assert!(o.is_disabled(), "base より前を指したらやめる");
        assert_eq!(o.arc_len(100, o.bands_of(30, 1000)), None);
        // 作る時に base より前の値があっても、 やめる
        let q = OrderIndex::new(0, 1, &[30], 1000, 100);
        build_from(&q, &[(1, Some(100), Some(40)), (2, Some(7), Some(40))]);
        assert!(q.is_disabled() && q.is_built());
    }

    /// 値が遠く (u32 の端) にあっても、 使う葉だけ置く (平らな配列のように手前を埋めない)。
    #[test]
    fn far_values_cost_only_their_leaves() {
        let o = OrderIndex::new(0, 1, &[30], 1000, 0);
        let rows: Vec<(u32, Option<u32>, Option<u64>)> = (0..100u32).map(|e| (e, Some(1_000_000_000 + e % 3), Some(u64::from(e)))).collect();
        build_from(&o, &rows);
        assert!(!o.is_disabled());
        assert_eq!(o.arc_len(1_000_000_000, o.bands_of(30, 1000)), Some((0..100u32).filter(|e| e % 3 == 0 && *e >= 30).count()));
        assert_eq!(o.slots.leaves.load(Ordering::Relaxed), 1, "値 3 つは同じ葉");
        assert!(o.heap_bytes() < 1 << 20, "heap {} B", o.heap_bytes());
    }

    /// 値が散っている (葉 1 枚に値が 1 つずつ) と、 置く entity の数に比べて葉が多すぎるところで索引をやめ、 読みは None。
    /// 対照: 同じ数の entity を 1 つの葉に集めるとやめない。
    #[test]
    fn scattered_values_disable_the_index() {
        // 余裕だけで葉 256 枚 ((0 + 2^16) × 4 / 1024)。 300 社を 1 社 1 葉に散らす
        let o = OrderIndex::new(0, 1, &[30], 1000, 0);
        build_from(&o, &[]);
        for e in 0..300u32 {
            o.place(e, Some(e * FAN as u32 * 7), Some(40));
        }
        assert!(o.is_disabled(), "葉 300 枚に 300 件はやめる");
        assert_eq!(o.arc_len(0, o.bands_of(30, 1000)), None, "やめた後の読みは None");
        let c = OrderIndex::new(0, 1, &[30], 1000, 0);
        build_from(&c, &[]);
        for e in 0..300u32 {
            c.place(e, Some(e), Some(40));
        }
        assert!(!c.is_disabled(), "対照: 1 つの葉に集まっていればやめない");
        assert_eq!(c.arc_len(7, c.bands_of(30, 1000)), Some(1));
    }
}
