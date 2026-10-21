//! HimoStore — 紐1本分のストレージ。
//!
//! Column（ソースオブトゥルース）+ LockFreeCylinder（検索キャッシュ、lock-free）。
//!
//! ぶら下げる → Column に書く + Cylinder に append
//! 引く       → Cylinder の raw を Column verify で filter（append-only なら skip）
//!
//! #95: 旧 `RwLock<BucketCylinder>` は read↔write が相互排他で、 長い read が write を
//! stall させた。 `LockFreeCylinder` に置換し read を完全 lock-free 化。 削除/更新は
//! Cylinder を触らず（append-only）、 stale は read 側の Column verify で落とす
//! （lazy / conditional verify）。

use crossbeam_epoch::{self as epoch, Guard};
use parking_lot::{Mutex, MutexGuard};
use std::ops::ControlFlow;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};

use crate::column::Column;
use crate::lockfree_cylinder::{LockFreeCylinder, Part};
use crate::region::Region;
use crate::value_locks::{HeldAll, ValueLocks};
use crate::write_gate::WriteGate;

/// column region の取得元 (request23 D2)。
///
/// v10 の DB では himo 1 本 = file 1 本なので、 open 時に全 himo の column を作ると
/// 「そのコマンドが触らない himo」 の分まで `open(2)` + `mmap` を払う。 コード索引の CLI (1 コマンド 1 process) の
/// 実測では 1 コマンドが触る himo は 48 本中 2〜13 本だった。 そこで column を
/// **最初に触ったときに** 組み立てる。
///
/// segment の存在と長さは `SegmentSet::open` が stat で確かめているので、 遅らせても
/// 「欠けた DB を黙って開く」 ことにはならない。
#[cfg(not(target_arch = "wasm32"))]
struct LazyCol {
    set: std::sync::Arc<crate::segments::SegmentSet>,
    kind: crate::segments::SegmentKind,
    /// header が届いていない列を組む時の形 (`Column::load_or_unwritten`)
    max_entities: u32,
    padded: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ValueType {
    /// 共有タグ — Vocabulary を引く (dedupe あり)。複数 entity が同じ tag を共有する hub。
    Tag = 0,
    /// タグなし — u32 をそのまま値として扱う。inline 数値・eid 等。
    Number = 1,
    /// 他 entity への参照 — u32 を eid として扱う。engine は素通しするだけ、FK 制約は schema 層。
    Ref = 2,
    /// 終端タグ — FreeStore を引く (dedupe なし)。1 entity しか繋がらない葉ノード。
    Leaf = 3,
    /// 64 bit の数値 — cell 8B、 値は u64 (`u64::MAX` は空の印で使えない)。 FILE_VERSION 11。
    Number64 = 4,
}

impl ValueType {
    pub fn from_byte(b: u8) -> Self {
        match b {
            0 => Self::Tag,
            2 => Self::Ref,
            3 => Self::Leaf,
            4 => Self::Number64,
            _ => Self::Number,
        }
    }

    /// cell の byte 数 (4 / 8)。
    pub fn width(self) -> u32 {
        match self {
            Self::Number64 => 8,
            _ => 4,
        }
    }

    /// 書ける値の上限 (含む)。 cell には値 + 1 を置く (0 = 未設定) ので幅の最大値は使えない。
    pub fn max_value(self) -> u64 {
        match self {
            Self::Number64 => u64::MAX - 1,
            _ => u32::MAX as u64 - 1,
        }
    }
}

/// tie 系の値の入口。 u8 / u16 / u32 / u64 / usize と、 負でない i32 / i64 / isize を受ける (負の数は
/// `None` = 書かない)。 i32 に実装してあるので数値リテラル (型推論で i32) もそのまま渡せる。 列の幅に
/// 入るか (u32 の列は `u32::MAX` 未満、 64 bit 列は `u64::MAX` 未満) は書く側 (engine) が確かめる。
pub trait CellValue: Copy {
    fn cell_value(self) -> Option<u64>;
}

macro_rules! cell_value_unsigned {
    ($($t:ty),*) => { $(impl CellValue for $t { #[inline] fn cell_value(self) -> Option<u64> { Some(self as u64) } })* };
}
macro_rules! cell_value_signed {
    ($($t:ty),*) => { $(impl CellValue for $t { #[inline] fn cell_value(self) -> Option<u64> { u64::try_from(self).ok() } })* };
}
cell_value_unsigned!(u8, u16, u32, u64, usize);
cell_value_signed!(i32, i64, isize);

// #95 並行性:
//   LockFreeCylinder 自体は「同時に 1 writer」を要求する。 write の呼び出し元は
//   1 本ではない — consumer thread (tie_async) に加えて、 **同期 tie API
//   (`tie_to_by_id` 系、 schema `RowBuilder::commit` の経路) は任意の user thread が
//   &self で直接呼ぶ**。 master ではこれを RwLock の write lock が直列化していた。
//   よって:
//   - set/remove/restore は per-himo `write_lock` で直列化（多 thread 呼び出し可、
//     master の write 直列度と同一。 himo が違えば並列）
//   - pull/slice_len は lock-free read（epoch）。 read は write を一切待たない
//     （write_lock は writer 同士のみ、 reader は一切触らない = #95 の目的は維持）
//   Column の write も write_lock 下に入る（get_value→set の read-modify-write が
//   atomic になる）。 pull の verify は Column を直読みするが、 これは #95 とは
//   別の既知の無同期読みで別 issue（#100 と同時に設計）。
//
//   2026-10-11: cylinder を組んでいない紐の set / remove は write_lock を取らず、 門
//   (`write_gate::WriteGate`) を通って Column だけを書く (同じ cell の書き手どうしは
//   engine の行の lock で並ぶ)。 write_lock を barrier に使う側 (lazy build、 並びの
//   索引の構築、 購読の登録) は lock を取った後で門を閉じ、 中の書き手が出るのを待つ。
//   組んだ後の set / remove は、 古い値と新しい値が配列の中なら値ごとの鍵 (`value_locks`、
//   bucket の書き手 1 本) だけを取る。 sparse に触る書き手と配列を伸ばす書き手は write_lock
//   も取る (伸ばすなら全部の鍵も)。 barrier は組んだ紐なら全部の鍵も取る。

/// test 用の割り込み点: 名前の一致した 1 回だけ、 登録した処理をこの thread で走らせる (`lockfree_cylinder` と同じ形)。
/// 門の中の書き手と、 門を閉じる側の窓を、 thread の運に頼らず踏むため。 test 以外では何もしない。
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

/// incremental compaction (request12 P2) の trigger: これ未満の bucket は
/// 組み直さない (小 bucket は verify の方が安い)。stale 率 (len-live)/len >= 1/2
/// と併用。閾値は churn_read ベンチで調整。
const COMPACT_MIN_LEN: usize = 64;

/// 読み手の詰め直し (`HimoStore::compact_if_reread`) を、 書き込みの無い照らしの 2 回目からする bucket の長さ (これより短い bucket
/// は 3 回目から)。 詰め直しには長さによらない手間 (新しい置き場の確保と、 古い置き場の epoch の後始末) があり、 長い bucket では照らす
/// 読み 1 回に比べて小さいので、 2 回目で詰め直してその後読まれなくても損は小さい。 短い bucket では照らす読みの 2 倍ほどかかる
/// ので、 読み直しが続くと分かるまで待つ (dbq の churn の後、 1 万件の bucket の範囲 10% の find は 2 回目からで static の速さ
/// (3 回目からだと 40 本のうち前半が照らしたまま)、 100 件の bucket の範囲 0.1% の find は 2 回目からだと 6〜13% 遅い)。
const REREAD_EARLY_LEN: usize = 1024;

pub struct HimoStore {
    /// `load_lazy` で作った store では最初の `col()` まで空 (request23 D2)。
    /// `init` / `load` は構築時に埋まる。
    col: OnceLock<Column>,
    #[cfg(not(target_arch = "wasm32"))]
    lazy: Option<LazyCol>,
    cyl: LockFreeCylinder,
    pub value_type: ValueType,
    /// 初期 bucket サイズのヒント。0 は「ヒントなし、必要時に拡張」。値の上限ではない。
    pub max_values: u32,
    /// cylinder が column から populate 済みか。lazy rebuild 用。
    /// `init`（新規 DB）も `load`（既存 DB）も **false で開始**し、 最初に引かれた時に
    /// `ensure_cylinder_built` が立てる。 #270 以降このフラグは「組み済みか」と
    /// **「以後この索引を維持するか」** を兼ねる (= writer の gate、 `cyl_live` 参照)。
    cyl_built: AtomicBool,
    /// cylinder を組む時の dense の窓の始まり ([`LockFreeCylinder::set_low_base`])。 ref の紐は指す先の表の eid の
    /// 始まり (engine が表の定義から置く、 [`set_dense_base_hint`](Self::set_dense_base_hint))。 組んだ後に変えても
    /// 効かない (組み直すまで今の窓のまま)。
    dense_base_hint: AtomicU64,
    /// writer 直列化 lock。 lazy build と restore と、 sparse に触る / 配列を伸ばす set / remove が取る。
    /// reader (pull 系) は一切取らない。 同期 tie / schema commit の多 thread
    /// 呼び出しを master (RwLock write) と同じ直列度で安全にする（#96 レビュー発覚分）。
    /// cylinder を組む前の set / remove は取らずに `gate` を通り、 組んだ後の配列の中の値の
    /// set / remove は取らずに値の鍵 (`vlocks`) を取る。
    write_lock: Mutex<()>,
    /// cylinder を組んでいない間の書き手の門 ([`WriteGate`])。 開いている = 組んでいない。 組む時に
    /// (write_lock の下で) 閉じて、 以後は開けない。 barrier ([`HimoStore::write_guard`]) は閉じて開け直す。
    gate: WriteGate,
    /// cylinder を組んだ後の書き手の値ごとの鍵 ([`ValueLocks`])。 組む時 (`cyl_built` を立てる前) に作る。
    vlocks: OnceLock<ValueLocks>,
    /// 値のある cell の数 ([`PresentCount`])。 `init` は 0 から、 `load` / `load_lazy` は最初に引かれた時に数える。
    present: PresentCount,
    /// 位置の印を持つと宣言されたか ([`declare_marks`](Self::declare_marks)、 engine の `MRK1`)。 円柱を組む時に見る。
    marks: AtomicBool,
}

/// 古い entry のある bucket (値 `value`) の eid の列 `eids` から、 今の値がちょうど `value` の entity を 1 回ずつ `out` に足す。
/// 頭の昇順 (広義) の長さは読む時に探す ([`sorted_prefix_len`](crate::append_bucket::sorted_prefix_len)、 昇順の塊は 256 個ごとに
/// 飛ぶので、 entry ごとの確かめの読みより軽い。 書き手は持たない、 #558)。 同じ entity が 2 つ以上あるのは往復した時だけで、
/// 頭の中では隣り合い (昇順)、 頭の後ろの分は頭か後ろのどこかにもう 1 つある。 頭は確かめて隣の重複を落とすだけ (並べない)。
/// 後ろは確かめた分を並べ、 隣の重複と、 頭に入れたものを落とす (頭は昇順なので、 並べた後ろの順に跳びながら探す)。 足した分の順:
/// 頭の分 (昇順) の後ろに後ろの分 (昇順)。
fn push_live_once(col: &Column, value: u64, eids: &[u32], out: &mut Vec<u32>) {
    let head = crate::append_bucket::sorted_prefix_len(eids);
    let want = value + 1;
    let start = out.len();
    let mut last = None;
    for &e in &eids[..head] {
        if last != Some(e) && stored_at(col, e) == want {
            out.push(e);
            last = Some(e);
        }
    }
    let mid = out.len();
    out.extend(eids[head..].iter().copied().filter(|&e| stored_at(col, e) == want));
    if out.len() == mid {
        return;
    }
    let (hd, tl) = out[start..].split_at_mut(mid - start);
    tl.sort_unstable();
    let (mut w, mut at, mut prev) = (0, 0, None);
    for r in 0..tl.len() {
        let e = tl[r];
        if prev == Some(e) {
            continue;
        }
        prev = Some(e);
        at = gallop_ge(hd, at, e);
        if at < hd.len() && hd[at] == e {
            continue;
        }
        tl[w] = e;
        w += 1;
    }
    out.truncate(mid + w);
}

/// eid の重複を落とす (残す順は決めない)。 件数が一番大きい eid の 1/256 以上なら eid の bit の印 (一番大きい eid / 8 byte を作り、
/// 1 件ごとに見て付ける。 並べない)、 少なければ並べて隣を落とす (印を作る手間の方が並べより重い)。
fn dedup_eids(v: &mut Vec<u32>) {
    let top = v.iter().copied().max().map_or(0, |m| m as usize + 1);
    if v.len() * 256 >= top {
        let mut seen = vec![0u64; top.div_ceil(64)];
        v.retain(|&e| {
            let (w, b) = (e as usize / 64, 1u64 << (e % 64));
            let fresh = seen[w] & b == 0;
            seen[w] |= b;
            fresh
        });
    } else {
        v.sort_unstable();
        v.dedup();
    }
}

/// 昇順の `s` の `from` 以降で、 `e` 以上の最初の位置 (無ければ `s.len()`)。 1, 2, 4, .. と跳んで越えたら、 その間を二分探索する
/// (近い所にある時は跳ぶ回数だけ)。
fn gallop_ge(s: &[u32], from: usize, e: u32) -> usize {
    let (mut lo, mut at, mut step) = (from, from, 1);
    while at < s.len() && s[at] < e {
        lo = at + 1;
        at += step;
        step <<= 1;
    }
    let hi = at.min(s.len());
    lo + s[lo..hi].partition_point(|&x| x < e)
}

/// `col.get(eid)` の cell を stored 形式 (0 = 未設定、 N = 値 N-1) で。 列の幅 (4 / 8 byte) はここと
/// `value_at` / `store` だけが見る。
///
/// `col()` が (遅延解決のため) atomic load になったので、 **要素ごとに `self.col()` を
/// 呼ぶとループ外に巻き上げられない**。 hot loop は `let col = self.col();` を 1 回だけ
/// 取って、 この free 関数に渡すこと (request23 D2 の計測で 負荷 harness の phase2_chaos が
/// 82.6s → 91.7s になった原因がこれだった)。
#[inline(always)]
fn stored_at(col: &Column, eid: u32) -> u64 {
    let b = col.get(eid);
    if col.value_size == 8 {
        u64::from_le_bytes(b.try_into().unwrap())
    } else {
        u32::from_le_bytes(b.try_into().unwrap()) as u64
    }
}

/// `stored_at` の atomic な読み (Acquire load)。 組んだ紐の書き手の照合 (詰め直しと配列を伸ばす時の `keep`) で使う:
/// 値の鍵で書く書き手が並んでいる間は、 他の cell を他の書き手が atomic に書いている。 `stored_at` は列全体の `&[u8]`
/// (`Column::get`) を作って読むので、 それと data race になる (Miri が見る。 読み手の `pull` などの同じ読みは前からの
/// 既知の別 issue #100 で、 値の鍵の前の形でも書き手と並ぶと同じ報告になる)。
#[inline(always)]
fn stored_sync(col: &Column, eid: u32) -> u64 {
    if col.value_size == 8 { col.load_u64_acquire(eid) } else { u64::from(col.load_u32_acquire(eid)) }
}

/// 値 `value` の bucket の詰め直しの照らし: `eid` の cell が今 `value` か。 その値の鍵の下で呼ぶ。 読みは Relaxed で足りる:
/// cell を `value` にする書き手も `value` から動かす書き手も同じ鍵を持つので、 その store は鍵の受け渡しで照らしより前に
/// 見えている。 鍵を持つ間に cell を動かす書き手は他の値どうしの間を動かすだけで、 どの値を読んでも答え (`value` でない) は
/// 同じ。 Acquire だと読みが 1 つずつ前の読みを待ち (後の読みを前に出せない)、 後ろに足した分 (eid がばらばら) の cell の
/// 読みが重ならなかった。
#[inline(always)]
fn holds_value(col: &Column, eid: u32, value: u64) -> bool {
    let stored = if col.value_size == 8 { col.load_u64_relaxed(eid) } else { u64::from(col.load_u32_relaxed(eid)) };
    stored == value + 1
}

/// `get_value` の col 受け取り版。
#[inline(always)]
fn value_at(col: &Column, eid: u32) -> Option<u64> {
    if eid >= col.count() {
        return None;
    }
    // #106: Acquire load。 writer の `store_*_release` と対。
    let stored = if col.value_size == 8 { col.load_u64_acquire(eid) } else { col.load_u32_acquire(eid) as u64 };
    if stored == 0 { None } else { Some(stored - 1) }
}

/// cell に stored 形式の値を Release で書く (幅に合わせて)。
#[inline(always)]
fn store_at(col: &Column, eid: u32, stored: u64) {
    if col.value_size == 8 {
        col.store_u64_release(eid, stored);
    } else {
        debug_assert!(stored <= u32::MAX as u64);
        col.store_u32_release(eid, stored as u32);
    }
}

fn ready(col: Column) -> OnceLock<Column> {
    let cell = OnceLock::new();
    let _ = cell.set(col);
    cell
}

/// [`PresentCount`] の塊の数。
const PRESENT_STRIPES: usize = 8;

/// 値のある cell の数 ([`HimoStore::present_count`])。 書き手は 「値無し → 有り」 で 1 足し、 「有り → 無し」 で 1 引く。
/// 足す所は thread ごとに決めた塊 (並んで書く書き手が 1 つの cache line を取り合わない)。 数 = `base` + 塊の和。
///
/// 開いた紐 (`load`) は開いた時の数が分からない (`known` が false)。 最初に引かれた時に書き手を止めて列を数え、
/// `base` = 数 − 塊の和 にする (それまでの書き込みも塊に足してあるので、 以後は足し引きだけで合う)。
struct PresentCount {
    known: AtomicBool,
    base: AtomicI64,
    stripes: [PresentStripe; PRESENT_STRIPES],
}

#[repr(align(128))]
struct PresentStripe(AtomicI64);

impl PresentCount {
    fn new(known: bool) -> Self {
        Self { known: AtomicBool::new(known), base: AtomicI64::new(0), stripes: std::array::from_fn(|_| PresentStripe(AtomicI64::new(0))) }
    }

    /// 書き手が呼ぶ (cell を書いた後、 門・鍵・write_lock の中で。 数える側は書き手を止めてから数える)。
    #[inline(always)]
    fn add(&self, d: i64) {
        self.stripes[present_stripe()].0.fetch_add(d, Ordering::Relaxed);
    }

    fn delta(&self) -> i64 {
        self.stripes.iter().map(|s| s.0.load(Ordering::Relaxed)).sum()
    }

    /// 並んだ書き手の途中で読むと、 先に引いた塊だけが見えて一時的に負になりうるので 0 で止める。
    fn get(&self) -> u64 {
        (self.base.load(Ordering::Relaxed) + self.delta()).max(0) as u64
    }
}

/// この thread が足す [`PresentCount`] の塊 (thread が初めて書いた順に配る)。
#[inline(always)]
fn present_stripe() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static MINE: std::cell::Cell<usize> = const { std::cell::Cell::new(usize::MAX) };
    }
    MINE.with(|m| {
        let s = m.get();
        if s != usize::MAX {
            return s;
        }
        let s = NEXT.fetch_add(1, Ordering::Relaxed) % PRESENT_STRIPES;
        m.set(s);
        s
    })
}

/// 1 つの値の bucket の中の entity を、 置いてある順・重複なしで、 要るなら Column で確かめながら 1 件ずつ出す
/// ([`HimoStore::walk_values`] が渡す)。
///
/// 並べ直さない: 同じ値の中の順は 「置いてある順」 (bucket に足した順。 sparse の値は run ごとの区間をつないだ順)。 eid は辞書の鍵で
/// 順に意味が無い (番号は使い直し・sync で付け替える) ので、 eid の順には並べない。 古い entry が無い bucket は写すだけ。 古い entry の
/// ある bucket は Column で確かめて飛ばし、 行って戻った entity の 2 つ目 (同じ eid が 2 回ある) を出した eid の集合で落とす。 手間は
/// 出した数 + 飛ばした古い entry の数 (古い entry が半分を超えると詰め直すので、 飛ばす数は出した数の高々同じくらい)。
pub(crate) struct BucketIter<'a> {
    raw: &'a [u32],
    at: usize,
    /// 古い印の列 (あれば印の立った entry を飛ばし、 Column で確かめない・集合も使わない)
    marks: Option<&'a [std::sync::atomic::AtomicU64]>,
    /// Column で確かめる時の (列, stored 形式の値 = 値 + 1)。 古い entry の無い bucket は None
    check: Option<(&'a Column, u64)>,
    /// 確かめのある bucket で出した eid (行って戻った entity の 2 つ目を落とす)。 古い entry の無い bucket には重複が無い
    seen: Seen,
}

/// 出した eid の集合。 16 個までは確保の要らない箱で探し、 それを超えたら hash にする (上位 k は k が小さい。 値の種類の多い列は
/// 値 1 つに 1〜2 行で、 値ごとにこの集合を作るので、 確保をしない)。
struct Seen {
    few: [u32; 16],
    n: usize,
    many: Option<std::collections::HashSet<u32>>,
}

impl Seen {
    fn new() -> Self {
        Seen { few: [0; 16], n: 0, many: None }
    }

    /// 初めてなら入れて true。
    #[inline]
    fn insert(&mut self, e: u32) -> bool {
        if let Some(h) = &mut self.many {
            return h.insert(e);
        }
        if self.few[..self.n].contains(&e) {
            return false;
        }
        if self.n < self.few.len() {
            self.few[self.n] = e;
            self.n += 1;
        } else {
            let mut h: std::collections::HashSet<u32> = self.few.iter().copied().collect();
            h.insert(e);
            self.many = Some(h);
        }
        true
    }
}

impl<'a> BucketIter<'a> {
    fn new(raw: &'a [u32], check: Option<(&'a Column, u64)>) -> Self {
        BucketIter { raw, at: 0, marks: None, check, seen: Seen::new() }
    }

    fn marked(raw: &'a [u32], marks: &'a [std::sync::atomic::AtomicU64]) -> Self {
        BucketIter { raw, at: 0, marks: Some(marks), check: None, seen: Seen::new() }
    }
}

impl Iterator for BucketIter<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<u32> {
        while let Some(&e) = self.raw.get(self.at) {
            let i = self.at;
            self.at += 1;
            if let Some(m) = self.marks {
                if (m[i / 64].load(Ordering::Acquire) >> (i % 64)) & 1 != 0 {
                    continue;
                }
                return Some(e);
            }
            if let Some((col, s)) = self.check
                && (stored_at(col, e) != s || !self.seen.insert(e))
            {
                continue;
            }
            return Some(e);
        }
        None
    }
}

// SAFETY: Cylinder の bucket の書き手は値ごとの鍵で 1 本 (組んだ後の書き手、 `value_locks`)、 sparse と配列の差し替えの
// 書き手は write_lock で 1 本 (配列を差し替える書き手は全部の鍵も持つ)、 reader は lock-free（Cylinder は epoch +
// Mutex(sparse)）。 組む前の門の中の書き手は Column の cell (atomic) と count (fetch_max) だけを書く。 Column の read は
// 無同期（既知、 別 issue）。
unsafe impl Sync for HimoStore {}
unsafe impl Send for HimoStore {}

/// 紐の書き手を止めている間持つ ([`HimoStore::write_guard`])。 drop で門を開け直してから、 鍵と write_lock を離す
/// (field の順)。
pub(crate) struct WriteGuard<'a> {
    store: &'a HimoStore,
    /// 組んだ紐なら全部の鍵 (鍵で書く書き手を止める)。
    _all: Option<HeldAll<'a>>,
    _lock: MutexGuard<'a, ()>,
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        // 持っている間に cylinder を組むことは無い (組む側も write_lock を取る) が、 組んだ後の紐の門は開けない
        if !self.store.cyl_built.load(Ordering::Acquire) {
            self.store.gate.open();
        }
    }
}

impl HimoStore {
    /// `padded` = 列の cell を header から離して置く (#400、 `Column::init`)。
    pub fn init(col_region: Region, ht: ValueType, max_values: u32, max_entities: u32, padded: bool) -> Self {
        let col = Column::init(col_region, ht.width(), max_entities, padded);
        Self {
            col: ready(col),
            #[cfg(not(target_arch = "wasm32"))]
            lazy: None,
            cyl: LockFreeCylinder::new(max_values),
            value_type: ht,
            max_values,
            // 新規 column は空なので「組み済み」で始めてよいが、 **それだと bulk load が
            // 誰も引かない index を育て続ける** (#270: 法令検索の消費側アプリのフルリビルドで 1.5GB /
            // 2,856 万確保)。 false で始めれば writer は cylinder を触らない (`cyl_live`)。
            cyl_built: AtomicBool::new(false),
            dense_base_hint: AtomicU64::new(0),
            write_lock: Mutex::new(()),
            gate: WriteGate::new(),
            vlocks: OnceLock::new(),
            present: PresentCount::new(true),
            marks: AtomicBool::new(false),
        }
    }

    /// open 時の load。 cylinder は空のまま返し、最初の cyl 触りで
    /// `ensure_cylinder_built` 経由で rebuild する（reopen latency を膨らませないため）。
    /// `max_entities` / `padded` は header が一度も届いていない列を組む時だけ使う (`Column::load_or_unwritten`)。
    /// 列の header が壊れていれば Err (`Column::load_or_unwritten`、 #526)。
    pub fn load(col_region: Region, ht: ValueType, max_values: u32, max_entities: u32, padded: bool) -> Result<Self, String> {
        let col = Column::load_or_unwritten(col_region, ht.width(), max_entities, padded)?;
        Ok(Self {
            col: ready(col),
            #[cfg(not(target_arch = "wasm32"))]
            lazy: None,
            cyl: LockFreeCylinder::new(max_values),
            value_type: ht,
            max_values,
            cyl_built: AtomicBool::new(false),
            dense_base_hint: AtomicU64::new(0),
            write_lock: Mutex::new(()),
            gate: WriteGate::new(),
            vlocks: OnceLock::new(),
            present: PresentCount::new(false),
            marks: AtomicBool::new(false),
        })
    }

    /// `load` の遅延版 (request23 D2) — column region の mmap を最初の read/write
    /// まで遅らせる。 触られなければ segment file は open すらされない。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load_lazy(
        set: std::sync::Arc<crate::segments::SegmentSet>,
        kind: crate::segments::SegmentKind,
        ht: ValueType,
        max_values: u32,
        max_entities: u32,
        padded: bool,
    ) -> Self {
        Self {
            col: OnceLock::new(),
            lazy: Some(LazyCol { set, kind, max_entities, padded }),
            cyl: LockFreeCylinder::new(max_values),
            value_type: ht,
            max_values,
            cyl_built: AtomicBool::new(false),
            dense_base_hint: AtomicU64::new(0),
            write_lock: Mutex::new(()),
            gate: WriteGate::new(),
            vlocks: OnceLock::new(),
            present: PresentCount::new(false),
            marks: AtomicBool::new(false),
        }
    }

    /// `eid` の cell を書けるところまで列を伸ばす。 伸ばせなければ (ディスクの空き不足) false (#316)。
    #[inline]
    pub(crate) fn ensure_room(&self, eid: u32) -> bool {
        let col = self.col();
        col.is_committed_for(eid) || col.ensure_committed_for(eid).is_ok()
    }

    /// column への参照。 遅延 store では初回だけ segment を mmap する。
    #[inline]
    fn col(&self) -> &Column {
        match self.col.get() {
            Some(c) => c,
            #[cfg(not(target_arch = "wasm32"))]
            None => self.col_slow(),
            #[cfg(target_arch = "wasm32")]
            None => unreachable!("HimoStore column is not initialized"),
        }
    }

    /// 列の segment を開いておく (#534)。 書き込みの道 (tie / untie / delete) は列を触る前にこれを呼び、 開けなければ
    /// 何も変えずに書き込みを断る。 [`col`](Self::col) は開けないと panic する (読みの API は `Result` を持たないので、
    /// 握ると 「値が無い」 という間違った答えになる)。 失敗した時は列を組まない (次に触った時にまた開いてみる)。
    pub(crate) fn ensure_loaded(&self) -> std::io::Result<()> {
        if self.col.get().is_some() {
            return Ok(());
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let lazy = self.lazy.as_ref().expect("HimoStore column is neither loaded nor lazy");
            let region = lazy.set.try_region(lazy.kind)?;
            let built = Column::load_or_unwritten(region, self.value_type.width(), lazy.max_entities, lazy.padded)
                .unwrap_or_else(|e| panic!("himo column segment {:?}: {e}", lazy.kind));
            // 競合して先に組まれていたら、 こちらは捨てる (header を読んだだけで副作用は無い)
            let _ = self.col.set(built);
        }
        Ok(())
    }

    /// 遅延 column の初回組み立て。 競合しても `OnceLock` が 1 本に絞る
    /// (負けた側の `Column::load` は header を読むだけで副作用が無い)。
    #[cold]
    #[cfg(not(target_arch = "wasm32"))]
    fn col_slow(&self) -> &Column {
        let lazy = self
            .lazy
            .as_ref()
            .expect("HimoStore column is neither loaded nor lazy");
        self.col.get_or_init(|| {
            // directory の DB は列を最初に触る時に読むので、 開く時に Err にできない (#526): 壊れた header は panic で止める
            // (そのまま組むと、 幅の合わない cell を読む所で意味の分からない panic になる)
            Column::load_or_unwritten(lazy.set.region(lazy.kind), self.value_type.width(), lazy.max_entities, lazy.padded)
                .unwrap_or_else(|e| panic!("himo column segment {:?}: {e}", lazy.kind))
        })
    }

    /// cylinder を組む時の dense の窓の始まりを置く (ref の紐 = 指す先の表の eid の始まり)。 組んだ後は効かない。
    pub fn set_dense_base_hint(&self, base: u64) {
        self.dense_base_hint.store(base, Ordering::Relaxed);
    }

    /// 組んだ cylinder の dense の窓の始まり (診断用。 組む前は 0)。
    pub fn cylinder_low_base(&self) -> u64 {
        self.cyl.low_base()
    }

    /// 組んだ cylinder で値を dense に置いているか (診断用。 組む前は false)。
    pub fn cylinder_is_dense(&self, value: u64) -> bool {
        self.cyl_built.load(Ordering::Acquire) && self.cyl.is_dense(value)
    }

    /// cylinder が未 build なら column から rebuild。lazy build の入口。
    /// fast path は AtomicBool::load で即 return。
    ///
    /// #95: build は多数 insert（単一 writer 必須）なので `write_lock` で排他する。
    /// build 中の他 thread は write_lock を待つ（one-time、load 直後の初回のみ）。
    #[inline]
    /// 位置の印 (書き手が置いた位置を覚え、 書き換え・消しの時に古い entry へ bit を立てる。 読みは印の立った entry を飛ばして写す) を
    /// 持つかを宣言する ([`Engine::declare_marks`](crate::Engine::declare_marks))。 円柱を組む前なら次に組む時から効く。 組んだ後に
    /// 付けても、 もう置いてある entry の位置が分からないので効かない (開き直して組み直す時から)。 外すのはいつでも効く (読みは
    /// Column で確かめる道に戻り、 書き手は印を付けない)。 返り値 = 今の円柱で効いているか。
    pub(crate) fn declare_marks(&self, on: bool) -> bool {
        self.marks.store(on, Ordering::Relaxed);
        if !on {
            self.cyl.disable_marks();
            return false;
        }
        self.cyl.marks_on()
    }

    /// 位置の印が宣言されているか (保存する宣言)。
    pub(crate) fn marks_declared(&self) -> bool {
        self.marks.load(Ordering::Relaxed)
    }

    /// 位置の印が今の円柱で効いているか (宣言して組んだ後)。
    #[cfg(test)]
    pub(crate) fn marks_live(&self) -> bool {
        self.cyl.marks_on()
    }

    fn ensure_cylinder_built(&self) {
        if self.cyl_built.load(Ordering::Acquire) {
            return;
        }
        let _g = self.write_lock.lock();
        // double-check（build 中に競合した別 thread が先に置いた可能性）
        if self.cyl_built.load(Ordering::Acquire) {
            return;
        }
        // 門の中で書いている書き手 (write_lock を取らない) が出るのを待ってから scan する。 以後は開けない
        // (組んだ cylinder は値ごとの鍵で手入れする、 `set_indexed`)
        self.gate.close();
        // 鍵は flag を立てる前に (flag を Acquire で見た書き手は鍵を見る)
        self.vlocks.get_or_init(ValueLocks::new);
        let col = self.col();
        let count = col.count();
        // dense の窓の始まりは組む前に (読み手はまだこの索引を読まない)
        self.cyl.set_low_base(self.dense_base_hint.load(Ordering::Relaxed));
        // 宣言された列だけ、 位置の表 (entity ごとの bucket の中の位置) を列の大きさで取る (匿名 mmap。 触った所だけが実際のメモリ)
        if self.marks.load(Ordering::Relaxed) {
            self.cyl.init_slots(col.max_entities);
        }
        // 全部を見てからまとめて組む (dense / sparse の振り分けを 1 度で決める、 #373)
        self.cyl.build(|| {
            (0..count).filter_map(|eid| {
                let stored = stored_at(col, eid);
                (stored != 0).then(|| (eid, stored - 1))
            })
        });
        self.cyl_built.store(true, Ordering::Release);
    }

    /// 維持対象の cylinder。 **未 build なら `None`** = index 操作は丸ごと no-op (#270)。
    ///
    /// `cyl_built` は「組み済みか」と「以後この索引を維持するか」を兼ねる。 新規 himo は
    /// `false` で始まる (`init`) ので、 bulk load (= `pull` を一度も引かない) は誰も使わない
    /// index を育てない。 読み手が最初に引いた時点で `ensure_cylinder_built` が Column から
    /// 組むので結果は同じ — むしろ stale ゼロで組み上がる。
    ///
    /// **判定は `write_lock` を取った後**でなければならない。 `ensure_cylinder_built` は
    /// 同じ lock の中で scan → flag を立てるので、 `Some` を見たなら build は完了済み
    /// (= writer が自分で index に入れる)、 `None` を見たなら build はまだ lock を取れて
    /// いない (= その後の scan が此の write を拾う)。 どちらかが必ず入れるので取りこぼさない。
    ///
    /// lock の **前** に読むと壊れる: writer が `None` を読む → reader が lock を取り
    /// **write 前の** Column を scan → flag を立てる → writer が lock を取り Column に
    /// 書くが `None` なので index に入れない → その eid は `pull` から永久に消える
    /// (silent lost row)。 `tests/loom_lazy_cylinder_build.rs` がその interleaving を
    /// 全探索で検出する。
    ///
    /// 門 (`gate`) を通る書き手は lock を取らないが、 組む側は lock の下で門を閉じて中の書き手が
    /// 出るのを待ってから scan するので、 門の中で書いた cell は scan に読まれる。 閉じた後に
    /// 来た書き手はこの lock の道に回る (同じ loom model が見る)。 lock を取った後に `Some` を見た
    /// 書き手は lock を離して値の鍵の道 (`set_indexed`) へ行く (組み終わった後の書き込みなので、
    /// 自分で index に入れる)。
    ///
    /// **guard を引数に取るのはこの順序を型で強制するため** — 返り値の lifetime を guard に
    /// 縛ってあるので、 lock の前に呼ぶことも、 guard を先に drop することもできない。
    /// 観測用途 (`cyl_backing_bytes`) は race して良いので flag を直に読む。
    #[inline]
    fn cyl_live<'a>(&'a self, _w: &'a MutexGuard<'_, ()>) -> Option<&'a LockFreeCylinder> {
        self.cyl_built.load(Ordering::Acquire).then_some(&self.cyl)
    }

    // ──── ぶら下げる / 外す ────

    /// cell に値を書く。 **書けなかったら `false`** (#167: growable backing で
    /// commit を伸ばせない = ディスク満杯。 未 commit page に書くと SIGBUS になるので
    /// 書かずに諦める)。 戻り値を無視しても従来どおり動く。
    pub fn set(&self, eid: u32, value: u64) -> bool {
        // 幅に入らない値は書かない (呼び側 = engine の tie 系が先に弾いて fault に積む)
        if value > self.value_type.max_value() {
            debug_assert!(false, "HimoStore::set: value {value} exceeds {:?}", self.value_type);
            return false;
        }
        // cylinder を組んだ紐は値ごとの鍵で書く (`set_indexed`)。 flag は組む側が鍵を作った後に Release で立てる
        if self.cyl_built.load(Ordering::Acquire) {
            return self.set_indexed(eid, value);
        }
        // cylinder を組んでいなければ、 write_lock を取らずに門の中で Column だけを書く (`WriteGate`)
        if let Some(_pass) = self.gate.try_enter() {
            hook::at("inside_gate");
            return self.store_unindexed(eid, value);
        }
        // 門が閉じている = 組んでいる最中か、 書き手を止めている最中。 lock を待ってから決める (#270、 順序契約は
        // `cyl_live` 参照。 組み終わっていれば鍵で書く)
        let w = self.write_lock.lock();
        if self.cyl_live(&w).is_some() {
            drop(w);
            return self.set_indexed(eid, value);
        }
        self.store_unindexed(eid, value)
    }

    /// cylinder を組んだ紐の書き込み ([`crate::value_locks`])。 古い値と新しい値がどちらも配列の中 (bucket で持つ値)
    /// なら、 その 2 つの値の鍵だけを取って書く (違う値の書き手どうしは並ばない)。 どちらかが sparse か、 配列を伸ばす値
    /// なら、 write_lock (sparse と配列の差し替えの書き手は 1 本) も取る。 伸ばすなら全部の鍵も取る。
    ///
    /// 同じ cell の書き手は engine の行の lock で並ぶので、 古い値はこの間動かない。
    fn set_indexed(&self, eid: u32, value: u64) -> bool {
        let col = self.col();
        // 位置の表の行を先に cache に載せる (Column の古い値の読みと並んで待つ。 値は鍵を取った後に読み直す)
        self.cyl.prefetch_slot(eid);
        if col.ensure_committed_for(eid).is_err() {
            return false;
        }
        col.ensure_count(eid);
        let old = value_at(col, eid);
        if old == Some(value) {
            return true; // 冗長な re-tie = no-op（bucket に dup を作らない）
        }
        let (cyl, locks) = (&self.cyl, self.vlocks());
        {
            let guard = epoch::pin();
            // 配列は伸びるだけ (伸ばす書き手は全部の鍵を持つ) なので、 鍵を取る前に中だった値は取った後も中
            if cyl.in_array(value, &guard) && old.is_none_or(|o| cyl.in_array(o, &guard)) {
                let _held = locks.lock_pair(old, Some(value));
                hook::at("inside_value_locks");
                // 値更新: 旧 value の bucket に stale が残る → その bucket の read は verify する。
                // Column を書き換える **前** に flag を立てる (request12、順序契約は note_stale 参照)
                let stale = old.and_then(|o| cyl.note_stale_of_in(o, eid, &guard).map(|s| (o, s)));
                // #106: Release store。 leaf offset を publish する前に書いた LeafStore slot
                // (payload/gen) を、 offset を Acquire で読む reader が必ず観測できるようにする。
                store_at(col, eid, value + 1);
                if old.is_none() {
                    self.present.add(1);
                }
                cyl.push_in_array(eid, value, &guard);
                // compaction は Column 更新の **後** (keep = value_eq が新状態を見るため)。 古い値の鍵の下で
                if let Some((o, (len, live))) = stale {
                    self.maybe_compact(o, len, live, &guard);
                }
                return true;
            }
        }
        // sparse に触るか、 配列を伸ばす。 write_lock の下で決め直す (write_lock を持つ間は配列が伸びない)
        let _w = self.write_lock.lock();
        let guard = epoch::pin();
        let old_in = old.filter(|&o| cyl.in_array(o, &guard));
        let new_in = cyl.in_array(value, &guard);
        let grow = !new_in && cyl.would_grow(value, &guard);
        // 伸ばすなら全部の鍵 (配列を写す間に他の書き手が bucket を置くと消える)、 伸ばさないなら配列の中の側の値の鍵
        let _all = grow.then(|| locks.lock_all());
        let _held = (!grow).then(|| locks.lock_pair(old_in, new_in.then_some(value)));
        let stale = old.and_then(|o| cyl.note_stale_of_in(o, eid, &guard).map(|s| (o, s)));
        store_at(col, eid, value + 1);
        if old.is_none() {
            self.present.add(1);
        }
        if new_in {
            cyl.push_in_array(eid, value, &guard);
        } else {
            cyl.insert_with(eid, value, grow, |v, e| stored_sync(col, e) == v + 1);
        }
        if let Some((o, (len, live))) = stale {
            self.maybe_compact(o, len, live, &guard);
        }
        self.maybe_compact_sparse(&self.cyl);
        true
    }

    /// 組んだ後の値ごとの鍵 (`cyl_built` を Acquire で見た後に呼ぶ。 組む側が flag の前に作る)。
    #[inline]
    fn vlocks(&self) -> &ValueLocks {
        self.vlocks.get().expect("HimoStore::vlocks: cylinder を組んだ後に呼ぶ")
    }

    /// cylinder の手入れの無い書き込み (門の中か、 組む前の write_lock の下)。 Column の cell (atomic) と count
    /// (fetch_max) だけを書くので、 同じ紐の書き手が並んで書いてよい (同じ cell の書き手は engine の行の lock で並ぶ)。
    ///
    /// `set` の中に展開する: 別の関数のままだと、 書き手 1 本の書き込みが門の無い形の 0.93〜0.96 倍だった (w1、 交互
    /// 20 回、 p = 0.01〜0.05)。 展開すると 0.98〜1.05 倍 (p = 0.26〜0.96)。
    #[inline(always)]
    fn store_unindexed(&self, eid: u32, value: u64) -> bool {
        let col = self.col();
        if col.ensure_committed_for(eid).is_err() {
            return false;
        }
        col.ensure_count(eid);
        // 冗長な re-tie は書かない (書き出しの dirty を増やさない)
        let old = value_at(col, eid);
        if old != Some(value) {
            // #106: Release store (上と同じ)
            store_at(col, eid, value + 1);
            if old.is_none() {
                self.present.add(1);
            }
        }
        true
    }

    /// sparse (大きな値の run) の古い entry が半分を超えたら Column 基準で組み直す。 write_lock 下・
    /// Column 更新後に。 償却 O(1) / write (組み直すたびに古い entry は 0 に戻る)。
    fn maybe_compact_sparse(&self, cyl: &LockFreeCylinder) {
        if cyl.sparse_needs_compact() {
            let col = self.col();
            // 値の鍵で書く書き手が並んでいるので atomic に読む (`stored_sync`)
            cyl.compact_sparse(|v, eid| stored_sync(col, eid) == v + 1);
        }
    }

    /// この紐の書き手を止める (並びの索引 `OrderIndex` を作る時。 via の紐の書き手と直列にする): write_lock を取り、
    /// 門を閉じて中の書き手が出るのを待つ。 cylinder を組んだ紐なら全部の鍵も取る (鍵で書く書き手が出るのを待つ)。
    /// 持っている間、 この紐の書き手は全部待つ。 drop で門を開け直す (cylinder を組んでいれば閉じたまま) — 開けた後に
    /// 入る書き手 (鍵を取った書き手) は、 持っている間に置いたもの (作った索引) を見る。
    pub(crate) fn write_guard(&self) -> WriteGuard<'_> {
        let lock = self.write_lock.lock();
        self.gate.close();
        // 組む側も write_lock を取るので、 持っている間に組んだかどうかは変わらない
        let all = self.cyl_live(&lock).map(|_| self.vlocks().lock_all());
        WriteGuard { store: self, _all: all, _lock: lock }
    }

    /// 書き手を 1 度止めて放すだけ ([`write_guard`](Self::write_guard) を取って離す)。 これより前に write_lock を離した
    /// 書き込みと、 門を出た書き込みは全て見え、 これより後の書き込みは、 これより前に置いたもの (購読の route) を見る
    /// (live query の登録 barrier、 `crate::live` module doc)。
    pub fn write_barrier(&self) {
        drop(self.write_guard());
    }

    /// 門が閉じているか (test 用)。
    #[cfg(test)]
    fn gate_is_closed(&self) -> bool {
        self.gate.is_closed()
    }

    pub fn remove(&self, eid: u32) {
        let col = self.col();
        if eid < col.count() {
            // cylinder を組んだ紐は値の鍵で消す (`set` と同じ)
            if self.cyl_built.load(Ordering::Acquire) {
                return self.remove_indexed(eid);
            }
            // cylinder を組んでいなければ門の中で消すだけ (`set` と同じ)
            if let Some(_pass) = self.gate.try_enter() {
                if value_at(col, eid).is_some() {
                    col.clear(eid);
                    self.present.add(-1);
                }
                return;
            }
            let w = self.write_lock.lock();
            if self.cyl_live(&w).is_some() {
                drop(w);
                return self.remove_indexed(eid);
            }
            if value_at(col, eid).is_some() {
                col.clear(eid);
                self.present.add(-1);
            }
        }
    }

    /// cylinder を組んだ紐の削除 (`set_indexed` と同じ鍵の取り方): 古い値が配列の中ならその値の鍵、 sparse なら
    /// write_lock。 旧 bucket に stale が残る（Cylinder は触らない、verify で落とす）。 flag → Column の順 (set と同じ)。
    fn remove_indexed(&self, eid: u32) {
        let col = self.col();
        let Some(o) = value_at(col, eid) else {
            return;
        };
        let cyl = &self.cyl;
        {
            let guard = epoch::pin();
            if cyl.in_array(o, &guard) {
                let _held = self.vlocks().lock_pair(Some(o), None);
                let stale = cyl.note_stale_of_in(o, eid, &guard);
                cyl.forget_slot(eid);
                col.clear(eid);
                self.present.add(-1);
                if let Some((len, live)) = stale {
                    self.maybe_compact(o, len, live, &guard);
                }
                return;
            }
        }
        // sparse の値。 write_lock の下で見直す (見た後で配列が伸びて中に入っていれば、 その値の鍵も取る。 write_lock を
        // 持つ間は配列は伸びない)
        let _w = self.write_lock.lock();
        let guard = epoch::pin();
        let _held = cyl.in_array(o, &guard).then(|| self.vlocks().lock_pair(Some(o), None));
        let stale = cyl.note_stale_of_in(o, eid, &guard);
        cyl.forget_slot(eid);
        col.clear(eid);
        self.present.add(-1);
        if let Some((len, live)) = stale {
            self.maybe_compact(o, len, live, &guard);
        }
        self.maybe_compact_sparse(cyl);
    }

    /// stale 率が閾値を超えた bucket を Column 基準で組み直す (request12 P2)。
    /// その値の鍵の下 (bucket の書き手 1 本)・Column 更新後に呼ぶこと。trigger は stale 率 50% なので
    /// amortized O(1)/write (Vec doubling と同じ理屈 — 組み直し後の stale は 0、
    /// 次の trigger までに live 相当数の churn が必要)。
    fn maybe_compact(&self, value: u64, len: usize, live: u32, guard: &Guard) {
        if len >= COMPACT_MIN_LEN && (len - live as usize) * 2 >= len {
            let col = self.col();
            hook::at("compacting");
            // 他の値の書き手が並んでいるので atomic に読む (`holds_value`)
            self.cyl.compact_bucket_in(value, guard, |eid| holds_value(col, eid, value));
        }
    }

    /// bucket 全体を読む時 (`pull` / `pull_into` / `pull_range_unordered`) に、 照らしが要ると分かったら呼ぶ (照らしの要らない読みは
    /// 呼ばない = 速い読みの手間は増やさない)。 今の (len, live) を控え、 書き手がその bucket に触らないまま照らすのが 2 回目
    /// (`REREAD_EARLY_LEN` 件より短い bucket は 3 回目) 以降なら (= 同じ照らしを繰り返している)、 値の鍵が空いていれば詰め直す。
    /// 詰め直したら true で、 呼び手は読み直す (古い entry が無いので照らさずに写すだけ)。 false なら呼び手はそのまま照らして読む。
    ///
    /// 書き手の詰め直しは古い entry が半分を超えた時だけなので、 1 割を書き換えた後の bucket は、 次に書かれるまで読みのたびに
    /// 全部を Column で照らし、 後ろに足した分を並べ直していた (dbq の churn の後: 範囲 10% の find が static の 19 倍)。 詰め直しの
    /// 手間は照らす読み 1〜2 回分で、 照らす前に詰め直すので、 詰め直した読みから静かな時と同じになる。 短い bucket を 2 回目で詰め
    /// 直さないのは、 1 回だけ読み直されてそれきりの bucket (値の種類が多い列をまばらに読む) で無駄にしないため
    /// (`REREAD_EARLY_LEN`)。 書き手と読み手が交互なら間に書き手が触るので詰め直さない。
    /// 鍵は待たない (`try_lock`): 書き手を待たせず、 鍵を持ったまま読む thread でも詰まらない。 詰め直しは書き手の詰め直しと同じ
    /// 約束 (その値の鍵の下、 Column が最新、 読み手は epoch で止まらない)
    fn compact_if_reread(&self, value: u64) -> bool {
        let Some((len, rereads)) = self.cyl.mark_verified(value) else { return false };
        if len < COMPACT_MIN_LEN || rereads < if len >= REREAD_EARLY_LEN { 1 } else { 2 } {
            return false;
        }
        let Some(_held) = self.vlocks().try_lock(value) else { return false };
        let col = self.col();
        hook::at("read_compacting");
        self.cyl.compact_bucket(value, |eid| holds_value(col, eid, value));
        true
    }

    /// 全 bucket を Column 基準で即時 compaction する明示 API (運用/テスト用)。
    /// reader は停止しない (bucket ごとの epoch swap)。
    pub fn compact_now(&self) {
        // #270: 未 build なら掃除する stale が無いので **組まずに返す**。 ここで
        // `ensure_cylinder_built` を呼ぶと、 掃除 API が index を丸ごと確保した上で
        // 「stale ゼロ」を確認するだけになる (遅延構築は定義上 stale-free)。
        let w = self.write_lock.lock();
        let Some(cyl) = self.cyl_live(&w) else {
            return;
        };
        // 鍵で書く書き手も止める (全部の bucket を組み直す)
        let _all = self.vlocks().lock_all();
        let col = self.col();
        if cyl.sparse_churned() {
            cyl.compact_sparse(|v, eid| stored_at(col, eid) == v + 1);
        }
        for v in cyl.unique_values().into_iter().filter(|&v| cyl.is_dense(v)) {
            // clean bucket (churn 痕なし) は組み直し不要 — 無条件 swap は巨大 himo で
            // write_lock の長期保持 + 旧 backing の epoch 滞留 (一時 ~2x RSS) を招く
            // (PR #103 レビュー)。write_lock と全部の鍵の下なので flag 判定は正確。
            if cyl.bucket_needs_verify(v) {
                cyl.compact_bucket(v, |eid| stored_at(col, eid) == v + 1);
            }
        }
    }

    /// 現在の unique 値数 (live 基準、churn があっても正確 — request12)。
    ///
    /// dense に置いている値の分は O(1)。 sparse (`SparseRuns`: 大きな値と、 dense の配列をそこまで伸ばしていない
    /// まばらな値、 #373) の分は値ごとの件数を持たないので、 Column と突き合わせて数える (O(sparse の entry 数)) —
    /// 値の種類が多い列 (時刻 / 64 bit ID / 一意な Tag) で書き込みのたびに値ごとの数を保つより、 呼ばれた時に数える方が安い。
    pub fn unique_count(&self) -> u32 {
        self.ensure_cylinder_built();
        let col = self.col();
        let mut sparse = self.cyl.sparse_range(0, u64::MAX);
        sparse.retain(|&(v, eid)| stored_at(col, eid) == v + 1);
        let mut vals: Vec<u64> = sparse.into_iter().map(|p| p.0).collect();
        vals.dedup(); // run ごとに値の順なので、 並べ直してから
        vals.sort_unstable();
        vals.dedup();
        self.cyl.unique_live() + vals.len() as u32
    }

    /// 値のある cell の数 (`entities_with_value().len()` と同じ)。 書き手が足し引きした数を読む (O(1)、 列をなめない)。
    /// 開いた紐は最初の 1 回だけ、 書き手を止めて (`write_guard`) 列を数える。 並んだ書き手の途中で読むと、 書きかけの
    /// 分がずれることがある (`count_range` などほかの数と同じ)。
    pub fn present_count(&self) -> u64 {
        if !self.present.known.load(Ordering::Acquire) {
            self.count_present_cells();
        }
        self.present.get()
    }

    /// 開いた紐の最初の [`present_count`](Self::present_count): 書き手を止めて列を数え、 それまでに書き手が塊に足した分を
    /// 引いて `base` にする。
    #[cold]
    fn count_present_cells(&self) {
        let _g = self.write_guard();
        if self.present.known.load(Ordering::Acquire) {
            return;
        }
        let n = self.scan_present_count();
        self.present.base.store(n as i64 - self.present.delta(), Ordering::Relaxed);
        self.present.known.store(true, Ordering::Release);
    }

    /// 列をなめて値のある cell を数える (O(列の長さ)、 足し引きした数を使わない)。
    pub fn scan_present_count(&self) -> u64 {
        let col = self.col();
        let n = if col.value_size == 4 {
            col.values_u32().iter().filter(|&&v| v != 0).count()
        } else {
            (0..col.count()).filter(|&e| stored_at(col, e) != 0).count()
        };
        n as u64
    }

    // ──── 読む（Column 直読み、Cylinder 非依存）────

    pub fn get_value(&self, eid: u32) -> Option<u64> {
        // #106: Acquire load。 writer の `store_u32_release` と対で、 leaf offset を
        // 掴んだら対応する LeafStore slot の payload/gen も必ず観測できるようにする。
        value_at(self.col(), eid)
    }

    /// u32 の列 (Tag / Leaf / Ref / Number) の値。 vid / leaf offset / local eid を読む engine 内部用。
    ///
    /// # Panics
    /// 64 bit 列 (u32 で読むと値が化ける)。
    #[inline]
    pub fn get_value32(&self, eid: u32) -> Option<u32> {
        let col = self.col();
        assert_eq!(col.value_size, 4, "get_value32 on a 64-bit column ({:?})", self.value_type);
        value_at(col, eid).map(|v| v as u32)
    }

    /// SIMD 集計向け raw stored values への view（stored 形式: 0 = 未設定、N = 値 N-1）。
    ///
    /// # Panics
    /// 64 bit 列 (u32 の集計には使えない)。
    #[inline]
    pub fn stored_slice(&self) -> &[u32] {
        self.col().values_u32()
    }

    /// bulk get-value（stored 形式、buffer reuse）。
    #[inline]
    pub fn get_stored_into(&self, eids: &[enchudb_oplog::EntityId], out: &mut Vec<u32>) {
        let col = self.col();
        assert_eq!(col.value_size, 4, "get_stored_into: 64 bit 列は u32 で読めない");
        let count = col.count();
        out.clear();
        out.reserve(eids.len());
        for &eid in eids {
            let lid = enchudb_oplog::eid_local(eid);
            if lid >= count {
                out.push(0);
                continue;
            }
            out.push(stored_at(col, lid) as u32);
        }
    }

    /// eid の現在値が value か（= lazy verify の primitive、Column 直読み）。
    #[inline(always)]
    pub fn value_eq(&self, eid: u32, value: impl CellValue) -> bool {
        let Some(value) = value.cell_value() else { return false };
        stored_at(self.col(), eid) == value + 1
    }

    /// cell の stored 形式の値 (0 = 未設定)。 `restore` と対。
    pub fn get_raw_stored(&self, eid: u32) -> u64 {
        let col = self.col();
        if eid >= col.count() {
            return 0;
        }
        stored_at(col, eid)
    }

    pub fn restore(&self, eid: u32, stored: u64) {
        let w = self.write_lock.lock();
        // #270: set / remove と同じ gate。 ここで `ensure_cylinder_built` を呼ぶと、
        // rollback 1 回で index が組まれて以後の bulk load 全体が維持モードに戻る。
        let cyl = self.cyl_live(&w);
        // 組んだ紐なら全部の鍵も取る (戻すのはまれなので、 どの値でも配列を伸ばしてよい形で書く)
        let _all = cyl.map(|_| self.vlocks().lock_all());
        // v10: `set` と同じく、 書く cell まで segment の commit を伸ばす (#167: 伸ばせなければ書かない)。
        let col = self.col();
        if col.ensure_committed_for(eid).is_err() {
            return;
        }
        col.ensure_count(eid);
        let old = value_at(col, eid);
        let new = if stored == 0 { None } else { Some(stored - 1) };
        if old == new {
            return; // 同値 restore = no-op (bytes も同一)
        }
        let guard = epoch::pin();
        let mut stale = None;
        if let (Some(cyl), Some(o)) = (cyl, old) {
            // flag → Column の順 (set と同じ、request12)。 覚えている位置の古い entry に印を立てる
            stale = cyl.note_stale_of_in(o, eid, &guard).map(|s| (o, s));
        }
        store_at(col, eid, stored);
        self.present.add(i64::from(new.is_some()) - i64::from(old.is_some()));
        if let (Some(cyl), Some(n)) = (cyl, new) {
            cyl.insert(eid, n, |v, e| stored_at(col, e) == v + 1);
        } else if let Some(cyl) = cyl {
            // 値が無くなった: bucket の中の位置は無い
            cyl.forget_slot(eid);
        }
        if let Some((o, (len, live))) = stale {
            self.maybe_compact(o, len, live, &guard);
        }
        if let Some(cyl) = cyl {
            self.maybe_compact_sparse(cyl);
        }
    }

    // ──── 引く ────

    /// 値に合致する entity。#95: Cylinder の raw を、削除/更新があった **bucket** でのみ
    /// Column verify + dedup で filter（churn していない bucket は raw 直返し =
    /// fast path。request12 で himo 単位 → bucket 単位に局所化）。
    pub fn pull(&self, value: impl CellValue) -> Vec<u32> {
        let Some(value) = value.cell_value() else { return Vec::new() };
        self.ensure_cylinder_built();
        let (mut raw, mut needs_verify, stale) = self.cyl.read_to_vec_marked(value);
        // 古い entry があり、 前に読んだ時から書き込みが無ければ、 詰め直して読み直す (照らさずに済む。 古い entry の無い読みは
        // 見ない。 古い印のそろった bucket も、 読み直しが続けば詰め直す = 印の立った entry を物理的に落とす)
        if stale && self.compact_if_reread(value) {
            (raw, needs_verify, _) = self.cyl.read_to_vec_marked(value);
        }
        if !needs_verify {
            // この bucket は append-only で全 live・dup なし (古い印のそろった bucket は印の立った entry を飛ばして写した)
            raw
        } else {
            // lazy verify: Column で現在値を確認、churn 由来の dup を除去
            let col = self.col();
            let mut out: Vec<u32> = raw
                .into_iter()
                .filter(|&eid| stored_at(col, eid) == value + 1)
                .collect();
            // bucket は 「組んだ時・組み直した時の昇順 + 後ろに足した順番外」 なので、 後ろだけ並べて混ぜる
            crate::append_bucket::sort_appended(&mut out);
            out.dedup();
            out
        }
    }

    /// [`pull`](Self::pull) の、 呼び手の buffer の後ろに足す版 (多くの値を続けて引く時に、 値ごとの Vec を
    /// 作らない)。 足した分の並びと verify は `pull` と同じ (verify した bucket だけ eid の昇順・重複なし)。
    pub fn pull_into(&self, value: impl CellValue, out: &mut Vec<u32>) {
        let Some(value) = value.cell_value() else { return };
        self.ensure_cylinder_built();
        let start = out.len();
        let (mut needs_verify, stale) = self.cyl.read_into_marked(value, out);
        // `pull` と同じ: 古い entry があり、 前に読んだ時から書き込みが無ければ、 詰め直して読み直す
        if stale && self.compact_if_reread(value) {
            out.truncate(start);
            (needs_verify, _) = self.cyl.read_into_marked(value, out);
        }
        if needs_verify {
            // lazy verify: Column で現在値を確認、churn 由来の dup を除去 (足した分だけ、 その場で)
            let col = self.col();
            let mut w = start;
            for r in start..out.len() {
                let eid = out[r];
                if stored_at(col, eid) == value + 1 {
                    out[w] = eid;
                    w += 1;
                }
            }
            out.truncate(w);
            crate::append_bucket::sort_appended(&mut out[start..]);
            let mut w = start;
            for r in start..out.len() {
                if w == start || out[r] != out[w - 1] {
                    out[w] = out[r];
                    w += 1;
                }
            }
            out.truncate(w);
        }
    }

    /// 値が `lo..=hi` の entity を値の順 (`desc` なら大きい順) に、 値ごとに `f(値, その値の entity を 1 件ずつ出す
    /// [`BucketIter`])` に渡す。 出すのは Column で確かめた live なものだけで、 置いてある順・重複なし。 `f` が `Break` を返したら
    /// 止める。
    ///
    /// 全部を引いて並べ直さない: 値ごとの bucket (dense) と値の順の run (sparse) を値の順にたどり、 bucket の中も要る分だけ
    /// 出すので、 上位 k・最小 / 最大は k 件で止まる。 dense の空の要素は飛ばす。
    pub(crate) fn walk_values(&self, lo: u64, hi: u64, desc: bool, mut f: impl FnMut(u64, &mut BucketIter<'_>) -> ControlFlow<()>) {
        self.ensure_cylinder_built();
        let col = self.col();
        self.cyl.walk(lo, hi, desc, |v, raw, verify, marks| {
            // 古い印がそろっていれば、 Column の確かめと出した eid の集合の代わりに印で飛ばす
            let mut iter = match (verify, marks) {
                (true, Some(m)) => BucketIter::marked(raw, m),
                _ => BucketIter::new(raw, verify.then_some((col, v + 1))),
            };
            f(v, &mut iter)
        });
    }

    /// 値が `lo..=hi` の live な entity の数 (`pull_range(lo, hi).len()` と同じ数)。 dense の値は bucket の live の数を
    /// 足すだけで entity を読まない (範囲の中の値の数に比例)。 sparse の値は run から数える (古い entry があれば
    /// Column で確かめる)。
    pub fn count_range(&self, lo: u64, hi: u64) -> usize {
        if lo > hi {
            return 0;
        }
        self.ensure_cylinder_built();
        let col = self.col();
        self.cyl.count_range(lo, hi, |v, e| stored_at(col, e) == v + 1)
    }

    /// 値が `lo..=hi` の entity (重複なし、 順は決めない)。 置き場の単位で写す (値の順にたどらない、 全体を並べない):
    /// - 古い entry の無い bucket と sparse の run の区間はそのまま写す (確かめも並べも無い)
    /// - 古い entry のある bucket は、 今の値がちょうどその値かを Column で確かめる。 同じ entity が 2 つあるのは書き換えで
    ///   戻ってきた時 (往復) だけで、 2 つ目は頭 (昇順の部分) より後ろにある。 頭は確かめて隣の重複を落とすだけ、 後ろは
    ///   確かめた分だけ並べて、 後ろの中の重複と頭にあるものを落とす (頭は跳びながら探す)
    /// - 古い entry のある sparse は、 今の値が sparse の区間の中かを Column で確かめ (値の列は読まない)、 通った eid の重複を最後に
    ///   落とす ([`dedup_eids`]: 往復した entity や、 区間の中で値が動いた entity の古い entry も通るので)。 区間どうしは重ならず
    ///   bucket の値とも重ならないので、 ほかの区間・bucket の分と重なることは無い
    ///
    /// ([`pull_range`](Self::pull_range) は全部を Column で確かめ、 全部を eid の順に並べる)。
    pub fn pull_range_unordered(&self, lo: u64, hi: u64) -> Vec<u32> {
        if lo > hi {
            return Vec::new();
        }
        self.ensure_cylinder_built();
        let col = self.col();
        let mut out = Vec::new();
        // 古い entry のある sparse で確かめた eid (最後に重複を落とす)
        let mut loose = Vec::new();
        self.cyl.for_each_part(lo, hi, |part| match part {
            Part::Bucket { eids, verify: false, .. } | Part::Run { eids, verify: false, .. } => out.extend_from_slice(eids),
            // 前に照らした時から書き込みの無い bucket は、 詰め直して詰め直した後の版を写す (渡された `eids` は詰め直す前の版。
            // 読み手は epoch で止まらないので、 この閉包の間は前の版も読める)
            Part::Bucket { value, .. } if self.compact_if_reread(value) => {
                let (mut eids, verify) = self.cyl.read_to_vec_verify(value);
                if verify {
                    // 詰め直した後に書き手が触った: `pull` と同じく照らして並べ、 往復の重複を落とす
                    eids.retain(|&e| stored_at(col, e) == value + 1);
                    crate::append_bucket::sort_appended(&mut eids);
                    eids.dedup();
                }
                out.extend_from_slice(&eids);
            }
            // 古い印がそろった bucket は印の立った entry を飛ばして写す (Column の確かめも重複落としも無い)
            Part::Bucket { eids, verify: true, marks: Some(m), .. } => crate::append_bucket::copy_unmarked(eids, m, &mut out),
            Part::Bucket { value, eids, verify: true, marks: None } => push_live_once(col, value, eids, &mut out),
            Part::Run { lo, hi, eids, verify: true } => loose.extend(eids.iter().copied().filter(|&e| {
                let s = stored_at(col, e);
                s != 0 && lo < s && s - 1 <= hi
            })),
        });
        if !loose.is_empty() {
            dedup_eids(&mut loose);
            out.extend_from_slice(&loose);
        }
        out
    }

    /// 値が `lo..=hi` の entity (eid の昇順・重複なし)。 索引の範囲 (dense は bucket を範囲の分、 大きな値は
    /// run の二分探索) を Column の現在値で確かめる。
    pub fn pull_range(&self, lo: u64, hi: u64) -> Vec<u32> {
        if lo > hi {
            return Vec::new();
        }
        self.ensure_cylinder_built();
        let col = self.col();
        let mut out = self.cyl.range_raw(lo, hi);
        out.retain(|&eid| {
            let s = stored_at(col, eid);
            // stored = 値 + 1 (0 = 未設定)
            s != 0 && lo < s && s - 1 <= hi
        });
        out.sort_unstable();
        out.dedup();
        out
    }

    /// eid `lo..hi` の値 (`None` = 未設定)。 64 bit 列でも読める集計用の走査 (u32 の SIMD 経路の代わり)。
    pub fn for_each_in(&self, lo: u32, hi: u32, mut f: impl FnMut(u32, u64)) {
        let col = self.col();
        let end = hi.min(col.count());
        for eid in lo..end {
            let s = stored_at(col, eid);
            if s != 0 {
                f(eid, s - 1);
            }
        }
    }

    /// 列の長さ (値を書いたことのある一番大きい eid + 1)。 [`entities_with_value`](Self::entities_with_value) がなめる数。
    pub fn column_len(&self) -> u32 {
        self.col().count()
    }

    /// 値が tie された全 entity（= column 非ゼロ走査）。O(next_eid) で重い。
    pub fn entities_with_value(&self) -> Vec<u32> {
        let col = self.col();
        let count = col.count();
        let mut result = Vec::new();
        for eid in 0..count {
            let stored = stored_at(col, eid);
            if stored != 0 {
                result.push(eid);
            }
        }
        result
    }

    /// value の live 件数 (= pull 結果の件数、正確)。planner の pivot 選択用。
    /// request12 で raw (stale 込み over-count) から live 基準に変更 — churn 後も
    /// 最小スライスを正しく選べる。
    pub fn slice_len(&self, value: impl CellValue) -> usize {
        let Some(value) = value.cell_value() else { return 0 };
        self.ensure_cylinder_built();
        match self.cyl.dense_live_len(value) {
            Some(n) => n,
            // sparse の値は件数を持たない: 引いて数える (sparse に置くのは、 値の種類が多い列か、 配列を伸ばすほど
            // 溜まっていない値なので、 1 値あたりの件数は小さい)
            None => self.pull(value).len(),
        }
    }

    /// 入っている値を列挙（順序保証なし、churn 時は stale 含む近似）。
    pub fn unique_values(&self) -> Vec<u64> {
        self.ensure_cylinder_built();
        self.cyl.unique_values()
    }

    /// #255: cylinder を**組まずに**、 column の設定済み cell の値を `f` に渡す (eid 順、
    /// 重複あり)。 `ensure_cylinder_built` と同じ走査を cylinder insert 抜きで行うので、
    /// 集めた集合は `unique_values()` と一致する。 writer open 時の LeafStore free-list
    /// 再構成用 — 以前は `unique_values()` 経由で leaf を持つ全 himo の index を eager に
    /// 組んでいた (117 himo / 9211 entity の DB で open +150 ms / drop +100 ms)。
    ///
    /// 走査は非 atomic な raw view (`values_u32`) なので、 **書き込みと並走しない場面**
    /// (open 直後) でだけ使うこと。
    pub fn for_each_set_value(&self, mut f: impl FnMut(u64)) {
        let col = self.col();
        if col.value_size == 8 {
            for eid in 0..col.count() {
                let stored = stored_at(col, eid);
                if stored != 0 {
                    f(stored - 1);
                }
            }
            return;
        }
        for &stored in col.values_u32() {
            if stored != 0 {
                f(stored as u64 - 1);
            }
        }
    }

    /// `for_each_set_value` の (eid, 値) 版。 同じく書き込みと並走しない場面 (open 直後) でだけ使うこと。
    pub fn for_each_set_cell(&self, mut f: impl FnMut(u32, u64)) {
        let col = self.col();
        for eid in 0..col.count() {
            let stored = stored_at(col, eid);
            if stored != 0 {
                f(eid, stored - 1);
            }
        }
    }

    /// cylinder (in-memory index) が組まれているか = **writer が以後それを維持するか**
    /// (#270)。 観測用 (#255 / #270 の gate)。
    pub fn cylinder_built(&self) -> bool {
        self.cyl_built.load(Ordering::Acquire)
    }

    /// 総件数 (live 基準、churn があっても正確 — request12)。
    pub fn total(&self) -> usize {
        self.ensure_cylinder_built();
        self.cyl.total_live()
    }

    /// Cylinder の eid backing 総 bytes（メモリ観測用、#95）。
    /// **未 build なら組まずに 0** (#270) — 観測 API が観測対象を確保しないため。
    pub fn cyl_backing_bytes(&self) -> usize {
        if !self.cyl_built.load(Ordering::Acquire) {
            return 0;
        }
        self.cyl.backing_bytes()
    }

    pub fn delta_eids(&self) -> &[u32] {
        &[]
    }
    pub fn delta_is_empty(&self) -> bool {
        true
    }
    pub fn delta_needs_rebuild(&self) -> bool {
        false
    }

    pub fn rebuild_cylinder(&self) {}

    pub fn scan(&self, value: impl CellValue) -> Vec<u32> {
        let Some(value) = value.cell_value() else { return Vec::new() };
        let col = self.col();
        let count = col.count();
        let target = value + 1;
        let mut result = Vec::new();
        for eid in 0..count {
            let stored = stored_at(col, eid);
            if stored == target {
                result.push(eid);
            }
        }
        result
    }

    pub fn sync(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    /// heap buffer 上の static backing (grower 無し = 全 commit 済み) の region。 8 byte にそろえる (`u8` の Vec は
    /// 1 byte そろえで、 列の cell と header の atomic の参照が Miri で 「そろっていない」 UB になる。 本物の列は mmap の上)。
    fn heap_region(bytes: usize) -> Region {
        let words = Box::leak(vec![0u64; bytes.div_ceil(8)].into_boxed_slice());
        unsafe { Region::new(words.as_mut_ptr().cast::<u8>(), bytes) }
    }

    /// heap buffer 上に static backing の store を作る。 leaf_store の `make_store` と同じ idiom。
    fn make_store(max_entities: u32) -> HimoStore {
        HimoStore::init(heap_region(64 * 1024), ValueType::Number, 0, max_entities, false)
    }

    /// 位置の印を宣言した列だけ、 書き換えの後の読みが Column の確かめを飛ばす (印の立った entry を飛ばして写す)。 答えは
    /// どちらも列をなめた答えと同じ (3 つの読みで)。 宣言した列も読み直しが続けば詰め直す (印の立った entry を物理的に落とす)。
    #[test]
    fn marks_declared_reads_skip_the_column_check() {
        const N: u32 = 12_000;
        for declared in [false, true] {
            let hs = make_store(N);
            if declared {
                assert!(!hs.declare_marks(true), "組む前は効いていない");
            }
            for e in 0..N {
                assert!(hs.set(e, (e % 20) as u64));
            }
            let _ = hs.pull(0u32); // 索引を組む
            assert_eq!(hs.marks_live(), declared, "宣言した列だけ組む時に印の表を取る");
            // 7 の倍数の行を書き換える (7 と 20 は互いに素なので、 どの値の bucket からも出ていく行がある)
            for e in (0..N).step_by(7) {
                assert!(hs.set(e, ((e / 7) % 20) as u64));
            }
            let value = |e: u32| if e.is_multiple_of(7) { (e / 7) % 20 } else { e % 20 };
            let want: Vec<u32> = (0..N).filter(|&e| value(e) == 3).collect();
            let (raw, needs, stale) = hs.cyl.read_to_vec_marked(3);
            assert!(stale, "[{declared}] 対照: 書き換えの後は古い entry がある");
            assert_eq!(needs, !declared, "[{declared}] 印を宣言した列だけ Column の確かめが要らない");
            if declared {
                let mut r = raw.clone();
                r.sort_unstable();
                assert_eq!(r, want, "印で飛ばした写しは live だけ");
            } else {
                assert!(raw.len() > want.len(), "対照: 宣言が無ければ古い entry 込みで写す");
            }
            for how in 0..4 {
                let mut got = match how {
                    0 | 3 => hs.pull(3u32),
                    1 => {
                        let mut out = Vec::new();
                        hs.pull_into(3u32, &mut out);
                        out
                    }
                    _ => hs.pull_range_unordered(3, 3),
                };
                got.sort_unstable();
                assert_eq!(got, want, "[{declared}] 読み {how}");
            }
            // 書き込みの無いまま読み直すと詰め直す (印のある列も)
            assert!(!hs.cyl.bucket_needs_verify(3), "[{declared}] 読み直しで詰め直す");
            let mut g = hs.pull(3u32);
            g.sort_unstable();
            assert_eq!(g, want, "[{declared}] 詰め直した後");
        }
    }

    /// `walk_values` の値ごとの [`BucketIter`] が、 bucket を写さずに置いてある順・重複なし・Column で確かめた live だけを
    /// 出す: 組んだ後に足した entry、 出ていった古い entry、 同じ値に戻った entity の重複 (往復) を混ぜて、 全部の cell を読んだ
    /// 答えと顔ぶれで比べる (同じ値の中の順は問わない)。 途中で止めた時 (先頭 k 件) も、 k 件ちょうど・重複なし・その値の live。
    #[test]
    fn walk_values_yields_sorted_live_eids_without_copying_the_bucket() {
        let n = if cfg!(miri) { 120u32 } else { 4_000 };
        let hs = make_store(n);
        let mut s = 0x9E37_79B9_7F4A_7C15u64;
        let mut rnd = move || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s
        };
        for e in 0..n {
            assert!(hs.set(e, rnd() % 8));
        }
        // 索引を組む (組んだ時の bucket は eid の昇順)
        assert_eq!(hs.count_range(0, 7), n as usize);
        // 書き換え (後ろに順番外で足す + 古い entry)、 往復 (同じ値の bucket に同じ eid が 2 つ)、 消し
        for _ in 0..n / 2 {
            let e = (rnd() % n as u64) as u32;
            assert!(hs.set(e, rnd() % 8));
        }
        for e in (0..n).step_by(37) {
            let v = hs.get_value(e).unwrap();
            assert!(hs.set(e, (v + 1) % 8));
            assert!(hs.set(e, v));
        }
        for e in (0..n).step_by(53) {
            hs.remove(e);
        }
        let want = |v: u64| -> Vec<u32> { (0..n).filter(|&e| hs.get_value(e) == Some(v)).collect() };
        for desc in [false, true] {
            let mut seen = Vec::new();
            hs.walk_values(0, u64::MAX, desc, |v, it| {
                let mut got: Vec<u32> = it.collect();
                got.sort_unstable();
                assert_eq!(got, want(v), "値 {v} (並べ直して比べる。 並べる前の重複は並べた後も隣り合って残る)");
                seen.push(v);
                ControlFlow::Continue(())
            });
            let mut vals: Vec<u64> = (0..8).filter(|&v| !want(v).is_empty()).collect();
            if desc {
                vals.reverse();
            }
            assert_eq!(seen, vals, "値の順 desc={desc}");
        }
        // 先頭 k 件で止める
        for k in [1usize, 5, 50] {
            let mut got = Vec::new();
            hs.walk_values(3, 3, false, |_, it| {
                got.extend(it.take(k));
                ControlFlow::Break(())
            });
            let all = want(3);
            assert_eq!(got.len(), k.min(all.len()), "k={k}: 数");
            let mut uniq = got.clone();
            uniq.sort_unstable();
            uniq.dedup();
            assert_eq!(uniq.len(), got.len(), "k={k}: 重複");
            assert!(got.iter().all(|e| all.contains(e)), "k={k}: 値 3 の live でない eid");
        }
    }

    /// #414: 列の header が一度もディスクに届いていない (作った直後に電源が落ちた = 全部ゼロ) region を開いても
    /// 落ちない。 旧 `Column::load` は value_size 0 の列を組み、 open の Leaf の free-list 再構成
    /// (`for_each_set_value` → `values_u32` の assert) で panic した。 書かれていない列として開き、 書ける。
    #[test]
    fn load_of_never_written_header_is_an_empty_column() {
        for ht in [ValueType::Leaf, ValueType::Number, ValueType::Number64] {
            let hs = HimoStore::load(heap_region(64 * 1024), ht, 0, 64, false).unwrap();
            let mut n = 0;
            hs.for_each_set_value(|_| n += 1);
            assert_eq!(n, 0, "{ht:?}");
            assert!(hs.set(3, 7), "{ht:?}");
            assert_eq!(hs.get_value(3), Some(7), "{ht:?}");
        }
    }

    /// 値のある cell の数 (`present_count`) が、 書き手 3 経路 (`set` / `remove` / `restore`) の組む前・組んだ後と、 開いた
    /// 紐 (`load`。 最初に引いた時に列を数える。 その前に書いた分も入る) で、 書くたびに列を数えた数と合う。 `restore` は
    /// workspace の中に呼び手が無いので、 ここでだけ見る。
    #[test]
    fn present_count_follows_every_writer() {
        let words = Box::leak(vec![0u64; 64 * 1024 / 8].into_boxed_slice());
        let ptr = words.as_mut_ptr().cast::<u8>();
        let region = || unsafe { Region::new(ptr, 64 * 1024) };
        let chk = |hs: &HimoStore, want: u64, at: &str| {
            assert_eq!(hs.entities_with_value().len() as u64, want, "{at}: 対照 (列を数えた数)");
            assert_eq!(hs.present_count(), want, "{at}");
        };
        let hs = HimoStore::init(region(), ValueType::Number, 0, 64, false);
        chk(&hs, 0, "空");
        // 組む前 (門の中の書き手)
        assert!(hs.set(0, 7));
        chk(&hs, 1, "set");
        assert!(hs.set(1, 7));
        chk(&hs, 2, "set 2 つ目");
        assert!(hs.set(1, 8));
        chk(&hs, 2, "書き換え");
        assert!(hs.set(1, 8));
        chk(&hs, 2, "同じ値");
        hs.remove(1);
        chk(&hs, 1, "remove");
        hs.remove(1);
        chk(&hs, 1, "無い値の remove");
        hs.restore(3, 8);
        chk(&hs, 2, "restore で置く");
        hs.restore(3, 9);
        chk(&hs, 2, "restore で書き換え");
        hs.restore(0, 0);
        chk(&hs, 1, "restore で外す");
        // 組んだ後 (配列の中の値と、 sparse の大きな値)
        assert_eq!(hs.pull(8), vec![3]);
        assert!(hs.cylinder_built());
        assert!(hs.set(4, 7));
        chk(&hs, 2, "組んだ後の set");
        assert!(hs.set(5, 1 << 20));
        chk(&hs, 3, "組んだ後の set (sparse)");
        assert!(hs.set(5, 1 << 21));
        chk(&hs, 3, "sparse の書き換え");
        assert!(hs.set(4, 1 << 22));
        chk(&hs, 3, "配列の値から sparse の値へ");
        hs.remove(4);
        chk(&hs, 2, "組んだ後の remove (sparse)");
        hs.remove(3);
        chk(&hs, 1, "組んだ後の remove (配列の中)");
        hs.restore(6, 3);
        chk(&hs, 2, "組んだ後の restore で置く");
        hs.restore(5, 0);
        chk(&hs, 1, "組んだ後の restore で外す");
        assert!(hs.set(7, 2));
        chk(&hs, 2, "組んだ後の set 2 つ目");
        drop(hs);
        // 開いた紐: 数を引く前に書いた分 (2 足して 1 引く) も入る
        let re = HimoStore::load(region(), ValueType::Number, 0, 64, false).unwrap();
        assert!(re.set(10, 1));
        assert!(re.set(11, 1));
        re.remove(6);
        chk(&re, 3, "開いて書いた後 (値は 7 と 10 と 11)");
        re.restore(12, 5);
        chk(&re, 4, "数えた後の restore");
        re.remove(7);
        chk(&re, 3, "数えた後の remove");

        // 門が閉じている間に来た書き手 (組む前の write_lock の道): 放した後に書く set と remove も数に入る
        let hs = make_store(64);
        assert!(hs.set(1, 5));
        let wait = std::time::Duration::from_millis(100);
        let g = hs.write_guard();
        assert!(!finishes_while(|| hs.remove(1), wait, || drop(g)), "write_guard を持つ間に remove が書いた");
        chk(&hs, 0, "write_lock の道の remove");
        let g = hs.write_guard();
        assert!(!finishes_while(|| assert!(hs.set(2, 6)), wait, || drop(g)), "write_guard を持つ間に set が書いた");
        assert!(!hs.cylinder_built());
        chk(&hs, 1, "write_lock の道の set");
    }

    /// #270: **writer 3 経路 (`set` / `remove` / `restore`) はどれも cylinder を組まない**。
    ///
    /// `restore` は workspace 内に caller が無い (oplog rollback 用の pub API) ので、
    /// gate 漏れを捕まえられるのはこの unit test だけ。 漏れると rollback 1 回で index が
    /// 組まれ、 以後の bulk load 全体が維持モードに戻る (= #270 の 1.2GB が復活する)。
    #[test]
    fn writers_never_build_the_cylinder() {
        let hs = make_store(64);
        assert!(!hs.cylinder_built(), "init は未 build で始まる");

        assert!(hs.set(0, 7));
        assert!(hs.set(1, 7));
        assert!(hs.set(2, 9));
        assert!(!hs.cylinder_built(), "set が組んでいる");

        hs.remove(2);
        assert!(!hs.cylinder_built(), "remove が組んでいる");

        // restore: eid 3 に「元は 7 だった」を書き戻す (stored = value + 1)。
        hs.restore(3, 8);
        assert!(!hs.cylinder_built(), "restore が組んでいる (#270 の gate 漏れ)");

        // 組む前に書いた 3 本 (0/1 は set、 3 は restore) を遅延構築が全部拾う。
        let mut got = hs.pull(7);
        got.sort_unstable();
        assert_eq!(got, vec![0, 1, 3], "遅延構築が write を取りこぼした");
        assert!(hs.cylinder_built(), "pull で初めて組む");
        assert!(hs.pull(9).is_empty(), "remove した値が残っている");

        // 組んだ後は writer が維持する (= 従来どおり)。
        assert!(hs.set(4, 7));
        let mut got = hs.pull(7);
        got.sort_unstable();
        assert_eq!(got, vec![0, 1, 3, 4], "build 後の write が index に入っていない");
    }

    /// `pull_into` は `pull` と同じ entity を (呼び手が先に積んだ分の後ろに) 足す。 書き換え・外しで古い entry の
    /// ある bucket (verify する経路)、 sparse の値 (2^20 以上)、 組む前 (遅延構築) を含む。
    #[test]
    fn pull_into_matches_pull_and_keeps_the_prefix() {
        let hs = make_store(4096);
        let big = 5_000_000u64; // DENSE_CAP (2^20) 以上 = sparse
        for e in 0..3000u32 {
            let e64 = e as u64;
            assert!(hs.set(e, if e % 5 == 0 { big + e64 % 2 } else { e64 % 7 }));
        }
        let check = |label: &str| {
            for v in (0..7u64).chain([big, big + 1, 99]) {
                let mut want = hs.pull(v);
                want.sort_unstable();
                let mut out = vec![u32::MAX, u32::MAX];
                hs.pull_into(v, &mut out);
                assert_eq!(&out[..2], &[u32::MAX, u32::MAX], "{label}: 先に積んだ分を変えた (値 {v})");
                let mut got = out[2..].to_vec();
                got.sort_unstable();
                assert_eq!(got, want, "{label}: 値 {v}");
            }
        };
        check("組む前");
        // 組んだ後の書き換え (古い entry が残る) と外し
        for e in (0..3000u32).step_by(3) {
            let e64 = e as u64;
            assert!(hs.set(e, if e % 2 == 0 { (e64 + 1) % 7 } else { big + (e64 + 1) % 2 }));
        }
        for e in (1..3000u32).step_by(11) {
            hs.remove(e);
        }
        // 行って戻った (A → B → A) entity: bucket に重複が残る
        for e in (2..3000u32).step_by(13) {
            let v = hs.get_value(e);
            assert!(hs.set(e, 6));
            if let Some(v) = v {
                assert!(hs.set(e, v));
            }
        }
        check("書き換えの後");
    }

    fn make_store64(max_entities: u32) -> HimoStore {
        let bytes = 16 + max_entities as usize * 8 + 64;
        HimoStore::init(heap_region(bytes), ValueType::Number64, 0, max_entities, false)
    }

    /// 大きな値 (sparse run) と小さな値 (dense bucket) を混ぜた書き込み / 書き換え / 削除の後でも、
    /// `pull` / `slice_len` / `unique_count` / `total` が Column の中身と一致する。 sparse の古い entry
    /// が半分を超えるたびに組み直される (件数が膨らまない) こと。
    /// verify の要る bucket の `pull` / `pull_into` は eid の昇順・重複なしで、 列をなめた答えと同じ。 並べるのは bucket の後ろだけ
    /// (`sort_appended`。 bucket は 「組んだ時・組み直した時の昇順 + 後ろに足した順番外」)。 書き換えを続け、 組み直しの前・後の両方で
    /// 見る (verify の要らない読みは追記順のままなので、 中身だけ比べる)。 `pull_into` は前に別の中身が入った buffer の後ろに足す。
    /// 負の対照: verify の要る読みのうち、 並べる前の写し (`read_to_vec_verify`) が昇順でないものがある (後ろだけ並べる道を通る)
    #[test]
    fn verified_pull_is_sorted_after_churn() {
        const N: u32 = 12_000; // make_store の region (64 KB) に入る数
        let hs = make_store(N);
        let mut cells = vec![0u32; N as usize];
        let mut x = 0x2545_f491_4f6c_dd1du64;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for e in 0..N {
            let v = (next() % 20) as u32;
            assert!(hs.set(e, v as u64));
            cells[e as usize] = v;
        }
        let _ = hs.pull(0u32); // 索引を組んでから (以後 writer が維持する)
        let (mut verified, mut unsorted_raw) = (0usize, 0usize);
        for round in 0..8 {
            // 1 round = 人数の半分の書き換え (2 round ほどで、 値ごとの bucket が組み直される)
            for _ in 0..N / 2 {
                let e = (next() % N as u64) as u32;
                let v = (next() % 20) as u32;
                assert!(hs.set(e, v as u64));
                cells[e as usize] = v;
            }
            for v in 0..20u32 {
                let want: Vec<u32> = (0..N).filter(|&i| cells[i as usize] == v).collect();
                let (raw, needs, stale) = hs.cyl.read_to_vec_marked(v as u64);
                let got = hs.pull(v);
                let mut into = vec![u32::MAX, 7];
                hs.pull_into(v, &mut into);
                assert_eq!(&into[..2], &[u32::MAX, 7], "pull_into が前の中身を変えた");
                if stale {
                    verified += 1;
                    unsorted_raw += usize::from(!raw.is_sorted());
                    if needs {
                        assert!(got.windows(2).all(|w| w[0] < w[1]), "round {round} 値 {v}: verify した pull が昇順・重複なしでない");
                        assert!(into[2..].windows(2).all(|w| w[0] < w[1]), "round {round} 値 {v}: verify した pull_into が昇順・重複なしでない");
                    } else {
                        // 古い印で飛ばした読み: 並べないが、 往復した entity の古い方は印で落ちるので重複は無い
                        let mut g = got.clone();
                        g.sort_unstable();
                        assert!(g.windows(2).all(|w| w[0] < w[1]), "round {round} 値 {v}: 印で飛ばした pull に重複");
                        let mut g = into[2..].to_vec();
                        g.sort_unstable();
                        assert!(g.windows(2).all(|w| w[0] < w[1]), "round {round} 値 {v}: 印で飛ばした pull_into に重複");
                    }
                }
                let mut g = got.clone();
                g.sort_unstable();
                assert_eq!(g, want, "round {round} 値 {v}: pull");
                let mut g = into[2..].to_vec();
                g.sort_unstable();
                assert_eq!(g, want, "round {round} 値 {v}: pull_into");
            }
        }
        assert!(verified >= 50 && unsorted_raw >= 10, "負の対照: 古い entry のある読み {verified}・並べる前が昇順でない読み {unsorted_raw} が少ない");
    }

    /// 読み手の詰め直し (`compact_if_reread`): 書き手の詰め直しの線 (古い entry が半分) に届かない churn の後、 書き込みの無いまま
    /// 同じ bucket を 3 回目に読む時に、 先に詰め直してから照らさずに読む。 1・2 回目の読み・間に書き込みのあった読み (入る / 出て
    /// いく)・値の鍵を誰かが持っている時の読みでは詰め直さない (鍵は待たない)。 詰め直すのは読んだ bucket だけ。 どの読みの答えも
    /// 列をなめた答えと同じ。 `pull` / `pull_into` / `pull_range_unordered` の 3 つの読みで。
    #[test]
    fn rereading_a_churned_bucket_compacts_it() {
        const N: u32 = 12_000; // make_store の region (64 KB) に入る数
        for how in ["pull", "pull_into", "pull_range_unordered"] {
            let hs = make_store(N);
            let mut cells = vec![0u32; N as usize];
            let mut x = 0x2545_f491_4f6c_dd1du64;
            let mut next = || {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x
            };
            for e in 0..N {
                cells[e as usize] = e % 20;
                assert!(hs.set(e, (e % 20) as u64));
            }
            let _ = hs.pull(0u32); // 索引を組んでから (以後 writer が維持する)
            // 1 割を書き換える: 値ごとの bucket の古い entry は 1 割ほど (書き手は詰め直さない)
            for _ in 0..N / 10 {
                let e = (next() % N as u64) as u32;
                let v = (next() % 20) as u32;
                assert!(hs.set(e, v as u64));
                cells[e as usize] = v;
            }
            let read = |v: u32| -> Vec<u32> {
                let mut got = match how {
                    "pull" => hs.pull(v),
                    "pull_into" => {
                        let mut out = vec![u32::MAX];
                        hs.pull_into(v, &mut out);
                        assert_eq!(out[0], u32::MAX, "pull_into が前の中身を変えた");
                        out.split_off(1)
                    }
                    _ => hs.pull_range_unordered(v as u64, v as u64),
                };
                got.sort_unstable();
                got
            };
            let want = |cells: &[u32], v: u32| -> Vec<u32> { (0..N).filter(|&i| cells[i as usize] == v).collect() };
            let v = 3u32;
            assert!(hs.cyl.bucket_needs_verify(v as u64), "[{how}] 対照: churn の後は照らしが要る");
            assert_eq!(read(v), want(&cells, v), "[{how}] 1 回目");
            assert!(hs.cyl.bucket_needs_verify(v as u64), "[{how}] 1 回目では詰め直さない");
            assert_eq!(read(v), want(&cells, v), "[{how}] 2 回目");
            assert!(hs.cyl.bucket_needs_verify(v as u64), "[{how}] 2 回目では詰め直さない");
            // 間に値 v の bucket から出ていく書き込み (live だけが動く)
            let e = (0..N).find(|&i| cells[i as usize] == v).unwrap();
            assert!(hs.set(e, 5));
            cells[e as usize] = 5;
            assert_eq!(read(v), want(&cells, v), "[{how}] 出ていく書き込みの後");
            assert_eq!(read(v), want(&cells, v), "[{how}] 出ていく書き込みの後の 2 回目");
            assert!(hs.cyl.bucket_needs_verify(v as u64), "[{how}] 間に出ていく書き込みがあれば数え直す");
            // 間に値 v の bucket に入る書き込み (len が動く)
            let e = (0..N).find(|&i| cells[i as usize] != v).unwrap();
            assert!(hs.set(e, v as u64));
            cells[e as usize] = v;
            assert_eq!(read(v), want(&cells, v), "[{how}] 入る書き込みの後");
            assert_eq!(read(v), want(&cells, v), "[{how}] 入る書き込みの後の 2 回目");
            assert!(hs.cyl.bucket_needs_verify(v as u64), "[{how}] 間に入る書き込みがあれば数え直す");
            {
                let _held = hs.vlocks().lock_pair(Some(v as u64), None);
                assert_eq!(read(v), want(&cells, v), "[{how}] 鍵を持たれている時の 3 回目");
                assert!(hs.cyl.bucket_needs_verify(v as u64), "[{how}] 鍵を持たれていれば詰め直さない");
            }
            assert_eq!(read(v), want(&cells, v), "[{how}] 書き込みの無い 4 回目");
            assert!(!hs.cyl.bucket_needs_verify(v as u64), "[{how}] 鍵が空いた後の照らしの前に詰め直す");
            assert_eq!(read(v), want(&cells, v), "[{how}] 詰め直した後");
            assert!(hs.cyl.bucket_needs_verify(4), "[{how}] 読んでいない bucket は詰め直さない");
            // 詰め直した後の書き込みは今まで通り (照らしが要る bucket に戻る)
            let e = (0..N).find(|&i| cells[i as usize] == v).unwrap();
            assert!(hs.set(e, 4));
            cells[e as usize] = 4;
            assert!(hs.cyl.bucket_needs_verify(v as u64), "[{how}] 詰め直した後の書き込み");
            assert_eq!(read(v), want(&cells, v), "[{how}] 詰め直した後の書き込みの後");
            assert_eq!(read(4), want(&cells, 4), "[{how}] 値 4");
        }
    }

    /// 長い bucket (`REREAD_EARLY_LEN` 件以上) は、 書き込みの無い照らしの 2 回目で詰め直す (短い bucket は 3 回目、
    /// `rereading_a_churned_bucket_compacts_it`)。 間に書き込みがあれば数え直す。 答えは列をなめた答えと同じ。
    #[test]
    fn long_bucket_compacts_on_the_second_read() {
        const N: u32 = 12_000; // make_store の region (64 KB) に入る数。 値 8 つで 1 値 1500 件
        let hs = make_store(N);
        let mut cells: Vec<u32> = (0..N).map(|e| e % 8).collect();
        for e in 0..N {
            assert!(hs.set(e, (e % 8) as u64));
        }
        let _ = hs.pull(0u32);
        for e in (0..N).step_by(10) {
            let v = (cells[e as usize] + 1) % 8;
            assert!(hs.set(e, v as u64));
            cells[e as usize] = v;
        }
        let want = |cells: &[u32], v: u32| -> Vec<u32> { (0..N).filter(|&i| cells[i as usize] == v).collect() };
        let read = |v: u32| {
            let mut got = hs.pull(v);
            got.sort_unstable();
            got
        };
        let v = 2u32;
        assert!(want(&cells, v).len() >= REREAD_EARLY_LEN, "前提: 長い bucket");
        assert!(hs.cyl.bucket_needs_verify(v as u64), "前提: 照らしが要る");
        assert_eq!(read(v), want(&cells, v), "1 回目");
        assert!(hs.cyl.bucket_needs_verify(v as u64), "1 回目では詰め直さない");
        let e = (0..N).find(|&i| cells[i as usize] == v).unwrap();
        assert!(hs.set(e, 3));
        cells[e as usize] = 3;
        assert_eq!(read(v), want(&cells, v), "書き込みの後");
        assert!(hs.cyl.bucket_needs_verify(v as u64), "間に書き込みがあれば数え直す");
        // 入りと出が 1 つずつ (live は差し引き同じ、 len だけ動く) でも数え直す
        let e_in = (0..N).find(|&i| cells[i as usize] != v).unwrap();
        assert!(hs.set(e_in, v as u64));
        cells[e_in as usize] = v;
        let e_out = (0..N).find(|&i| cells[i as usize] == v && i != e_in).unwrap();
        assert!(hs.set(e_out, 3));
        cells[e_out as usize] = 3;
        assert_eq!(read(v), want(&cells, v), "入りと出の後");
        assert!(hs.cyl.bucket_needs_verify(v as u64), "入りと出が 1 つずつでも数え直す");
        assert_eq!(read(v), want(&cells, v), "書き込みの無い 2 回目");
        assert!(!hs.cyl.bucket_needs_verify(v as u64), "長い bucket は 2 回目で詰め直す");
        assert_eq!(read(v), want(&cells, v), "詰め直した後");
        assert_eq!(read(3), want(&cells, 3), "値 3");
    }

    /// 読み手の詰め直しも書き手の詰め直しと同じく値の鍵の下: 詰め直しの途中 (割り込み点で止めた) に、 別の値の書き手は待たずに
    /// 書け、 同じ値の bucket に触る書き手は詰め直しが終わるまで待つ。 終わった後の cylinder は列と一致する。 CI の Miri の段でも
    /// 回す (詰め直しの照合の読みと、 別の値の書き手の store が並ぶ。 値の鍵だけを取り合い、 parking_lot の lock は取り合わない)。
    #[test]
    fn read_compaction_holds_the_value_lock() {
        // bucket は `COMPACT_MIN_LEN` (64) 件以上
        let n: u32 = if cfg!(miri) { 200 } else { 1_000 };
        let hs = make_store(n + 2);
        for e in 0..n {
            assert!(hs.set(e, 0));
        }
        assert!(hs.set(n, 2));
        assert!(hs.set(n + 1, 3));
        assert_eq!(hs.pull(0).len(), n as usize); // 索引を組む
        // 値 0 の bucket から 1 割を値 1 へ (書き手は詰め直さない)
        for e in 0..n / 10 {
            assert!(hs.set(e, 1));
        }
        assert!(hs.cyl.bucket_needs_verify(0), "前提: 照らしが要る");
        // 1・2 回目の照らした読み (3 回目の前に詰め直す)
        assert_eq!(hs.pull(0).len(), (n - n / 10) as usize);
        assert_eq!(hs.pull(0).len(), (n - n / 10) as usize);
        let go = std::sync::Arc::new(AtomicBool::new(false));
        let same_done = std::sync::Arc::new(AtomicBool::new(false));
        let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
        std::thread::scope(|s| {
            let (hs, go_r) = (&hs, go.clone());
            let reader = s.spawn(move || {
                hook::set("read_compacting", move || {
                    entered_tx.send(()).unwrap();
                    while !go_r.load(Ordering::Acquire) {
                        std::thread::yield_now();
                    }
                });
                // 書き込みの無い 3 回目 = 先に詰め直す。 詰め直した後に下の同じ値の書き手が入りうるので、 答えはどちらか
                let mut got = hs.pull(0);
                got.sort_unstable();
                let before: Vec<u32> = (n / 10..n).collect();
                let mut after = before.clone();
                after.insert(0, n / 10 - 1);
                assert!(got == before || got == after, "詰め直した読みの答え ({} 件)", got.len());
            });
            // 割り込み点に来なかったら (詰め直さなかったら)、 読み手の thread が終わって送り手が消え、 ここで落ちる
            entered_rx.recv().expect("前提: 読み手の詰め直しの割り込み点に来た");
            // 別の値の書き手 (鍵は値 2 と 3) は待たない
            assert!(hs.set(n, 3));
            // 同じ値の bucket に触る書き手 (値 1 → 0、 鍵は値 0 と 1) は詰め直しが終わるまで待つ
            let done = same_done.clone();
            let writer = s.spawn(move || {
                assert!(hs.set(n / 10 - 1, 0));
                done.store(true, Ordering::Release);
            });
            std::thread::sleep(std::time::Duration::from_millis(50));
            assert!(!same_done.load(Ordering::Acquire), "同じ値の書き手は詰め直しの間は待つ");
            go.store(true, Ordering::Release);
            reader.join().unwrap();
            writer.join().unwrap();
        });
        let mut got = hs.pull(0);
        got.sort_unstable();
        let mut want: Vec<u32> = (n / 10..n).collect();
        want.insert(0, n / 10 - 1);
        assert_eq!(got, want, "値 0");
        let mut got = hs.pull(1);
        got.sort_unstable();
        assert_eq!(got, (0..n / 10 - 1).collect::<Vec<_>>(), "値 1");
        assert!(hs.pull(2).is_empty(), "値 2");
        let mut got = hs.pull(3);
        got.sort_unstable();
        assert_eq!(got, vec![n, n + 1], "値 3");
    }

    #[test]
    fn sparse_runs_stay_exact_under_churn() {
        const N: u32 = 400;
        let hs = make_store64(N);
        let mut cells: Vec<Option<u64>> = vec![None; N as usize];
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let vals = [3u64, 9, crate::lockfree_cylinder::DENSE_CAP as u64 + 1, 1 << 33, 1 << 40, u64::MAX - 2];
        let _ = hs.pull(0u32); // 索引を組んでから (以後 writer が維持する)
        for step in 0..20_000 {
            let e = (next() % N as u64) as u32;
            match next() % 5 {
                0 => {
                    hs.remove(e);
                    cells[e as usize] = None;
                }
                1 => {
                    // restore: 別の値の stored 形式を書き戻す
                    let v = vals[(next() % vals.len() as u64) as usize];
                    hs.restore(e, v + 1);
                    cells[e as usize] = Some(v);
                }
                _ => {
                    let v = vals[(next() % vals.len() as u64) as usize];
                    assert!(hs.set(e, v));
                    cells[e as usize] = Some(v);
                }
            }
            if step % 997 == 0 || step == 19_999 {
                for &v in &vals {
                    let want: Vec<u32> = (0..N).filter(|&i| cells[i as usize] == Some(v)).collect();
                    // dense の bucket は verify が要らない時は追記順で返す (並びは約束しない)
                    let mut got = hs.pull(v);
                    got.sort_unstable();
                    assert_eq!(got, want, "step {step}: pull {v}");
                    assert_eq!(hs.slice_len(v), want.len(), "step {step}: slice_len {v}");
                }
                let distinct = vals.iter().filter(|&&v| cells.contains(&Some(v))).count();
                assert_eq!(hs.unique_count() as usize, distinct, "step {step}: unique_count");
                assert_eq!(hs.total(), cells.iter().filter(|c| c.is_some()).count(), "step {step}: total");
            }
        }
        // 古い entry が溜まり続けていない (組み直しが効いている)
        assert!(hs.cyl.sparse_range(0, u64::MAX).len() < 3 * N as usize, "sparse の entry が膨らんでいる");
        hs.compact_now();
        let live = cells.iter().filter(|c| c.is_some_and(|v| v >= crate::lockfree_cylinder::DENSE_CAP as u64)).count();
        assert_eq!(hs.cyl.sparse_range(0, u64::MAX).len(), live, "compact_now 後に古い entry が残る");
    }

    /// #373: dense の配列の窓の中でもまばらな値は sparse に置き、 行が溜まると配列を伸ばして dense へ移す。 その
    /// 移動を書き込み / 書き換え / 削除 / restore の途中で何度も踏んでも、 `pull` / `pull_range` / `slice_len` /
    /// `unique_count` / `total` が Column の中身と一致する (移す時に古い entry・重複を live に数えない)。
    #[test]
    fn values_moving_from_sparse_to_dense_stay_exact_under_churn() {
        const N: u32 = 6_000;
        let hs = make_store64(N);
        let mut cells: Vec<Option<u64>> = vec![None; N as usize];
        let mut x = 0x2545_f491_4f6c_dd1du64;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        // 3 / 9 は最初から dense、 2_000 / 2_003 / 5_000 / 70_000 は窓の中だが遠い (溜まるまで sparse)、 残りは窓の外
        let vals = [3u64, 9, 2_000, 2_003, 5_000, 70_000, crate::lockfree_cylinder::DENSE_CAP as u64 + 1, 1 << 33];
        let _ = hs.pull(0u32); // 索引を組んでから (以後 writer が維持する)
        let mut was_sparse = [false; 3];
        for step in 0..60_000 {
            let e = (next() % N as u64) as u32;
            // 遠い値に寄せる (溜まって配列が伸びるように)
            let pick = |r: u64| vals[if r % 4 == 0 { (r / 4) as usize % vals.len() } else { 2 + (r / 4) as usize % 3 }];
            match next() % 6 {
                0 => {
                    hs.remove(e);
                    cells[e as usize] = None;
                }
                1 => {
                    let v = pick(next());
                    hs.restore(e, v + 1);
                    cells[e as usize] = Some(v);
                }
                _ => {
                    let v = pick(next());
                    assert!(hs.set(e, v));
                    cells[e as usize] = Some(v);
                }
            }
            for (k, v) in [2_000u64, 2_003, 5_000].into_iter().enumerate() {
                was_sparse[k] |= !hs.cyl.is_dense(v);
            }
            if step % 1_499 == 0 || step == 59_999 {
                for &v in &vals {
                    let want: Vec<u32> = (0..N).filter(|&i| cells[i as usize] == Some(v)).collect();
                    let mut got = hs.pull(v);
                    got.sort_unstable();
                    assert_eq!(got, want, "step {step}: pull {v}");
                    assert_eq!(hs.slice_len(v), want.len(), "step {step}: slice_len {v}");
                }
                for (lo, hi) in [(0u64, 2_001), (1_000, 80_000), (2_003, 2_003), (4_000, u64::MAX - 1)] {
                    let want: Vec<u32> = (0..N).filter(|&i| cells[i as usize].is_some_and(|v| lo <= v && v <= hi)).collect();
                    assert_eq!(hs.pull_range(lo, hi), want, "step {step}: pull_range {lo}..={hi}");
                }
                let distinct = vals.iter().filter(|&&v| cells.contains(&Some(v))).count();
                assert_eq!(hs.unique_count() as usize, distinct, "step {step}: unique_count");
                assert_eq!(hs.total(), cells.iter().filter(|c| c.is_some()).count(), "step {step}: total");
            }
        }
        // 前提: 遠い値は最初 sparse に置かれ、 途中で dense に移った
        assert_eq!(was_sparse, [true; 3], "遠い値が最初から dense だった (移動を踏んでいない)");
        for v in [2_000u64, 2_003, 5_000] {
            assert!(hs.cyl.is_dense(v), "{v} が dense に移っていない");
        }
    }

    /// 観測 API が観測対象を確保しない (#270)。
    #[test]
    fn cyl_backing_bytes_does_not_build() {
        let hs = make_store(64);
        assert!(hs.set(0, 7));
        assert_eq!(hs.cyl_backing_bytes(), 0, "未 build なら 0");
        assert!(!hs.cylinder_built(), "観測が cylinder を組んでしまった");
        let _ = hs.pull(7);
        assert!(hs.cyl_backing_bytes() > 0, "build 後は実 backing を返す");
    }

    /// `f` を別 thread で走らせ、 `wait` の間に終わったか。 終わっていなければ `release` を呼んでから終わるのを待つ。
    fn finishes_while(f: impl FnOnce() + Send, wait: std::time::Duration, release: impl FnOnce()) -> bool {
        let done = std::sync::atomic::AtomicBool::new(false);
        std::thread::scope(|s| {
            let h = s.spawn(|| {
                f();
                done.store(true, Ordering::SeqCst);
            });
            let deadline = std::time::Instant::now() + wait;
            while !done.load(Ordering::SeqCst) && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            let finished = done.load(Ordering::SeqCst);
            release();
            h.join().unwrap();
            finished
        })
    }

    /// 2026-10-11: cylinder を組んでいない紐の書き手 (set / remove) は write_lock を取らずに門を通る。
    #[test]
    fn unindexed_writers_pass_the_gate_without_the_lock() {
        let hs = make_store(64);
        assert!(hs.set(1, 5));
        let held = hs.write_lock.lock();
        let ok = finishes_while(
            || {
                assert!(hs.set(2, 6));
                hs.remove(1);
            },
            std::time::Duration::from_secs(2),
            || drop(held),
        );
        assert!(ok, "組んでいない紐の書き手が write_lock を待った");
        assert_eq!((hs.get_value(1), hs.get_value(2)), (None, Some(6)));
        assert_eq!(hs.pull(6), vec![2]);
        assert!(hs.cylinder_built());
    }

    /// 2026-10-11 (`value_locks`): cylinder を組んだ紐の書き手は、 古い値と新しい値が配列の中なら値の鍵だけを取る
    /// (write_lock を持たれていても書ける)。 対照 3 つ: 同じ値の鍵を持たれていると待つ / 関係の無い値の鍵では待たない /
    /// sparse の値の書き手は write_lock を待つ。
    #[test]
    fn indexed_writers_take_value_locks_not_the_write_lock() {
        let hs = make_store(64);
        assert!(hs.set(1, 5));
        assert!(hs.set(2, 6));
        assert_eq!(hs.pull(6), vec![2]);
        assert!(hs.cylinder_built());
        let wait = std::time::Duration::from_millis(100);
        let held = hs.write_lock.lock();
        let ok = finishes_while(
            || {
                assert!(hs.set(3, 6));
                assert!(hs.set(1, 6));
                hs.remove(2);
            },
            std::time::Duration::from_secs(2),
            || drop(held),
        );
        assert!(ok, "組んだ紐の、 配列の中の値の書き手が write_lock を待った");
        let mut got = hs.pull(6);
        got.sort_unstable();
        assert_eq!(got, vec![1, 3]);
        assert!(hs.pull(5).is_empty());
        // 前提: 5 と 6 は配列の中 (下の 3 つは鍵だけで書く道を通る)。 鍵の番号は 5 / 6 / 9 で別
        {
            let guard = epoch::pin();
            assert!(hs.cyl.in_array(5, &guard) && hs.cyl.in_array(6, &guard));
        }
        // 対照: 新しい値の鍵を持たれていると待つ
        let held = hs.vlocks().lock_pair(Some(5), None);
        let ok = finishes_while(|| assert!(hs.set(4, 5)), wait, || drop(held));
        assert!(!ok, "対照: 新しい値の鍵を持たれていても書けた");
        // 対照: 古い値の鍵を持たれていると待つ (古い bucket に印を付けて詰め直すのは、 その値の書き手 1 本)
        let held = hs.vlocks().lock_pair(Some(6), None);
        let ok = finishes_while(|| assert!(hs.set(3, 5)), wait, || drop(held));
        assert!(!ok, "対照: 古い値の鍵を持たれていても書けた");
        // 関係の無い値の鍵では待たない
        let held = hs.vlocks().lock_pair(Some(9), None);
        let ok = finishes_while(|| assert!(hs.set(1, 5)), std::time::Duration::from_secs(2), || drop(held));
        assert!(ok, "関係の無い値の鍵で待った");
        // 対照: 配列を伸ばす書き手は全部の鍵を取る (関係の無い値の鍵でも待つ。 配列を写す間に他の書き手が bucket を
        // 置くと、 新しい配列から消える)
        let grow = {
            let guard = epoch::pin();
            (10..1024u64).find(|&v| !hs.cyl.in_array(v, &guard) && hs.cyl.would_grow(v, &guard)).expect("前提: 伸ばす値")
        };
        assert_ne!(grow as usize % crate::value_locks::STRIPES, 9);
        let held = hs.vlocks().lock_pair(Some(9), None);
        let ok = finishes_while(|| assert!(hs.set(6, grow)), wait, || drop(held));
        assert!(!ok, "対照: 配列を伸ばす書き手が全部の鍵を取らなかった");
        assert!(hs.cyl.is_dense(grow), "前提: 配列が伸びた");
        // 対照: sparse の値 (dense の窓 2^20 の外) の書き手は write_lock を待つ
        let far = 1u64 << 30;
        let held = hs.write_lock.lock();
        let ok = finishes_while(|| assert!(hs.set(7, far)), wait, || drop(held));
        assert!(!ok, "対照: sparse の値の書き手が write_lock を待たなかった");
        let mut got = hs.pull(5);
        got.sort_unstable();
        assert_eq!(got, vec![1, 3, 4]);
        assert!(hs.pull(6).is_empty());
        assert_eq!(hs.pull(grow), vec![6]);
        assert_eq!(hs.pull(far), vec![7]);
    }

    /// 組んだ紐の `write_guard` (並びの索引の構築) と `write_barrier` (購読の登録) は、 鍵で書く書き手も止める
    /// (全部の鍵を取る)。 値の鍵の中で書きかけの書き手が居る間は返らない (書き手を鍵の中で止めて窓を踏む)。
    #[test]
    fn barrier_waits_for_a_writer_inside_the_value_locks() {
        let hs = make_store(64);
        assert!(hs.set(1, 5));
        let _ = hs.pull(5);
        assert!(hs.cylinder_built());
        let g = hs.write_guard();
        let ok = finishes_while(|| assert!(hs.set(2, 5)), std::time::Duration::from_millis(100), || drop(g));
        assert!(!ok, "組んだ紐の write_guard を持つ間に、 鍵で書く書き手が書いた");
        let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        let passed = std::sync::atomic::AtomicBool::new(false);
        std::thread::scope(|s| {
            let w = s.spawn(|| {
                hook::set("inside_value_locks", move || {
                    entered_tx.send(()).unwrap();
                    go_rx.recv().unwrap();
                });
                assert!(hs.set(3, 5));
            });
            entered_rx.recv().unwrap();
            let b = s.spawn(|| {
                hs.write_barrier();
                passed.store(true, Ordering::SeqCst);
            });
            std::thread::sleep(std::time::Duration::from_millis(50));
            // 書き手を放してから確かめる (先に assert すると、 落ちた時に書き手が止まったままになる)
            let early = passed.load(Ordering::SeqCst);
            go_tx.send(()).unwrap();
            w.join().unwrap();
            b.join().unwrap();
            assert!(!early, "鍵の中の書き手が書き終える前に barrier が返った");
        });
        let mut got = hs.pull(5);
        got.sort_unstable();
        assert_eq!(got, vec![1, 2, 3]);
    }

    /// 組んだ紐の詰め直しは、 他の値の書き手が並んで列を書いている間に、 照合 (`keep`) で列を読む。 列全体の `&[u8]` を
    /// 作る読み (`stored_at`) だと、 並んだ書き手の atomic store と data race になるので、 照合は atomic に読む
    /// (`stored_sync`)。 **Miri の上でだけ意味がある test**: 詰め直しの直前で書き手を止め (割り込み点)、 その間に別の値の
    /// 書き手が書き、 同期を作らずに (Relaxed の印) 詰め直しを進める。 並べて書く test (下) では、 この重なりを Miri が
    /// 踏まなかった (照合を `stored_at` に戻しても通った、 2026-10-11)。
    #[test]
    fn compaction_reads_cells_atomically_while_other_writers_store() {
        const N: u32 = 2 * COMPACT_MIN_LEN as u32;
        let hs = make_store(256);
        for e in 0..N {
            assert!(hs.set(e, 0));
        }
        // 別の値の書き手が書く cell (値 2 → 3)。 3 も組む前に入れておく (配列の外の値だと、 書き手は配列を伸ばす道で
        // 全部の鍵を待ち、 詰め直しの途中で止めた書き手の鍵を待って進まない)
        assert!(hs.set(N, 2));
        assert!(hs.set(N + 1, 3));
        assert_eq!(hs.pull(0).len(), N as usize);
        assert!(hs.cylinder_built());
        {
            let guard = epoch::pin();
            assert!((0..4).all(|v| hs.cyl.in_array(v, &guard)), "前提: 値 0..4 は配列の中");
        }
        // 値 0 の bucket から半分 − 1 本を値 1 へ (古い entry が半分に届かないので、 まだ詰め直さない)
        for e in 0..N / 2 - 1 {
            assert!(hs.set(e, 1));
        }
        let go = std::sync::Arc::new(AtomicBool::new(false));
        let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
        std::thread::scope(|s| {
            let (hs, go_a) = (&hs, go.clone());
            let a = s.spawn(move || {
                hook::set("compacting", move || {
                    entered_tx.send(()).unwrap();
                    // 同期を作らずに待つ (Relaxed): 下の書き手の store と、 詰め直しの照合の読みが並ぶ
                    while !go_a.load(Ordering::Relaxed) {
                        std::thread::yield_now();
                    }
                });
                // 古い entry が半分に届く = 値 0 の bucket を詰め直す (鍵は値 0 と 1)
                assert!(hs.set(N / 2 - 1, 1));
            });
            // 割り込み点に来なかったら (詰め直さなかったら)、 書き手の thread が終わって送り手が消え、 ここで落ちる
            entered_rx.recv().expect("前提: 詰め直しの割り込み点に来た");
            // 別の値の書き手 (鍵は値 2 と 3、 詰め直す側の 0 / 1 と別なので待たない)
            assert!(hs.set(N, 3));
            go.store(true, Ordering::Relaxed);
            a.join().unwrap();
        });
        let mut got = hs.pull(0);
        got.sort_unstable();
        assert_eq!(got, (N / 2..N).collect::<Vec<_>>());
        let mut got = hs.pull(1);
        got.sort_unstable();
        assert_eq!(got, (0..N / 2).collect::<Vec<_>>());
        assert!(hs.pull(2).is_empty());
        let mut got = hs.pull(3);
        got.sort_unstable();
        assert_eq!(got, vec![N, N + 1]);
    }

    /// 組んだ紐の書き手が並んで書いても、 cylinder は列とちょうど一致する。 書き手 4 本が別々の eid を書き換え続ける
    /// (配列の中の値・sparse の値・配列を伸ばす値・消す)。 並んで barrier と全部の bucket の詰め直し (`compact_now`) と
    /// 読み手も回す。 区切りごとに全員を止めて、 cylinder (値ごとの `pull` と件数、 total、 unique_count) を列を
    /// `get_value` でなめる走査と照らす (詰め直しと古い印が重なってできる、 次の古い印で隠れる食い違いも捕まえるため)。
    /// 最後は書き手が最後に書いた値の写し (shadow) とも照らす。 前提 (対照): 途中で配列が伸びた (伸ばす値が最後は dense)。
    #[test]
    fn indexed_writers_racing_keep_the_cylinder_exact() {
        const THREADS: u32 = 4;
        const EIDS: u32 = 2_000;
        let far = 1u64 << 40;
        // 配列の中 (0..4、 少ない値に集めて同じ bucket の書き手を重ねる)、 sparse (窓の外 2 つ)、 窓の中で配列の外
        // (組んだ時の配列の先、 溜まると配列が伸びる)
        let values: Vec<u64> = (0..4).chain([far, far + 1]).chain((0..16).map(|i| 3_000 + i)).collect();
        let rounds = if cfg!(miri) { 1 } else { 10 };
        let phases = if cfg!(miri) { 2 } else { 20 };
        let ops = if cfg!(miri) { 100 } else { 1_000 };
        // 食い違いは panic せずに返す (区切りの途中で panic すると、 他の thread が区切りの barrier で待ち続ける)
        let check = |hs: &HimoStore, at: &str| -> Result<Vec<Vec<u32>>, String> {
            let mut by_cells = 0usize;
            let mut distinct = 0usize;
            let mut out = Vec::new();
            for &v in &values {
                let mut got = hs.pull(v);
                got.sort_unstable();
                let by_scan: Vec<u32> = (0..EIDS * THREADS).filter(|&e| hs.get_value(e) == Some(v)).collect();
                if got != by_scan {
                    return Err(format!("{at}: cylinder が列と違う (値 {v}: cylinder {} 件 / 列 {} 件)", got.len(), by_scan.len()));
                }
                if hs.slice_len(v) != by_scan.len() {
                    return Err(format!("{at}: 件数 (値 {v}: {} / 列 {})", hs.slice_len(v), by_scan.len()));
                }
                by_cells += by_scan.len();
                distinct += usize::from(!by_scan.is_empty());
                out.push(by_scan);
            }
            if hs.total() != by_cells {
                return Err(format!("{at}: total {} / 列 {by_cells}", hs.total()));
            }
            if hs.unique_count() as usize != distinct {
                return Err(format!("{at}: unique_count {} / 列 {distinct}", hs.unique_count()));
            }
            Ok(out)
        };
        for round in 0..rounds {
            let hs = HimoStore::init(heap_region(128 * 1024), ValueType::Number64, 0, EIDS * THREADS, false);
            assert!(hs.set(0, 1));
            let _ = hs.pull(1);
            assert!(hs.cylinder_built());
            let was_sparse = !hs.cyl.is_dense(3_000);
            // 区切りの始まりと終わり (書き手 + 横の thread + この thread)
            let (go, done) = (std::sync::Barrier::new(THREADS as usize + 2), std::sync::Barrier::new(THREADS as usize + 2));
            let writing = std::sync::atomic::AtomicU32::new(0);
            let mut failure: Option<String> = None;
            let shadow: Vec<(Vec<Option<u64>>, usize)> = std::thread::scope(|s| {
                let writers: Vec<_> = (0..THREADS)
                    .map(|t| {
                        let (hs, go, done, writing, values) = (&hs, &go, &done, &writing, &values);
                        s.spawn(move || {
                            let mut last = vec![None; EIDS as usize];
                            let mut bad = 0usize;
                            // eid 0 は組む前に書いた (thread 0 の写しにも入れる)
                            if t == 0 {
                                last[0] = Some(1);
                            }
                            let mut x = 0x9e37_79b9_7f4a_7c15u64 ^ (round << 8 | u64::from(t));
                            for _ in 0..phases {
                                go.wait();
                                for _ in 0..ops {
                                    x ^= x << 13;
                                    x ^= x >> 7;
                                    x ^= x << 17;
                                    let i = (x % u64::from(EIDS)) as usize;
                                    let eid = i as u32 * THREADS + t;
                                    if x >> 60 == 0 {
                                        hs.remove(eid);
                                        last[i] = None;
                                    } else {
                                        // 配列の中の値を多めに、 伸ばす値も十分に (溜まって配列が伸びるまで)
                                        let pick = (x >> 32) % 100;
                                        let v = if pick < 60 {
                                            values[(pick % 4) as usize]
                                        } else if pick < 64 {
                                            values[4 + (pick % 2) as usize]
                                        } else {
                                            values[6 + (pick % 16) as usize]
                                        };
                                        // 書けない (false) は数えて後で落とす (ここで panic すると区切りで止まる)
                                        bad += usize::from(!hs.set(eid, v));
                                        last[i] = Some(v);
                                    }
                                }
                                writing.fetch_sub(1, Ordering::SeqCst);
                                done.wait();
                            }
                            (last, bad)
                        })
                    })
                    .collect();
                let side = s.spawn(|| {
                    let mut n = 0u32;
                    for _ in 0..phases {
                        go.wait();
                        while writing.load(Ordering::SeqCst) > 0 {
                            hs.write_barrier();
                            if n.is_multiple_of(8) {
                                hs.compact_now();
                            }
                            // Miri では書き手と並ぶ読み手を回さない: 読み手の照合は列全体の `&[u8]` を作って読む (前からの
                            // 既知の別 issue #100。 値の鍵の前の形でも、 書き手と並ぶと Miri は同じ data race を報告する)
                            if !cfg!(miri) {
                                let _ = hs.pull(u64::from(n % 4));
                            }
                            n += 1;
                            std::thread::yield_now();
                        }
                        done.wait();
                    }
                });
                for phase in 0..phases {
                    writing.store(THREADS, Ordering::SeqCst);
                    go.wait();
                    done.wait();
                    if failure.is_none()
                        && let Err(e) = check(&hs, &format!("round {round} 区切り {phase}"))
                    {
                        failure = Some(e);
                    }
                }
                side.join().unwrap();
                writers.into_iter().map(|w| w.join().unwrap()).collect()
            });
            if let Some(e) = failure {
                panic!("{e}");
            }
            assert_eq!(shadow.iter().map(|(_, bad)| bad).sum::<usize>(), 0, "round {round}: 書けなかった set");
            let by_scan = check(&hs, &format!("round {round} 最後")).unwrap_or_else(|e| panic!("{e}"));
            for (v, scan) in values.iter().zip(&by_scan) {
                let mut by_shadow: Vec<u32> = (0..THREADS)
                    .flat_map(|t| {
                        let last = &shadow[t as usize].0;
                        (0..EIDS).filter(move |&i| last[i as usize] == Some(*v)).map(move |i| i * THREADS + t)
                    })
                    .collect();
                by_shadow.sort_unstable();
                assert_eq!(scan, &by_shadow, "round {round}: 列が書き手の写しと違う (値 {v})");
            }
            if !cfg!(miri) {
                assert!(was_sparse && hs.cyl.is_dense(3_000), "round {round}: 前提: 配列が伸びていない");
            }
        }
    }

    /// `write_guard` を持つ間は書き手が待ち (門が閉じて write_lock の道に回る)、 放すと門が開いて lock を取らない道に
    /// 戻る。 組んだ後の紐の門は放しても閉じたまま。
    #[test]
    fn write_guard_stops_writers_and_reopens_the_gate() {
        let hs = make_store(64);
        let g = hs.write_guard();
        assert!(hs.gate_is_closed());
        let ok = finishes_while(|| assert!(hs.set(1, 5)), std::time::Duration::from_millis(100), || drop(g));
        assert!(!ok, "write_guard を持つ間に書き手が書いた");
        assert_eq!(hs.get_value(1), Some(5));
        assert!(!hs.gate_is_closed(), "放した後に門が閉じたまま");
        hs.write_barrier();
        assert!(!hs.gate_is_closed(), "barrier の後に門が閉じたまま");
        let _ = hs.pull(5);
        assert!(hs.gate_is_closed(), "組んだのに門が開いている");
        hs.write_barrier();
        assert!(hs.gate_is_closed(), "組んだ紐の門を barrier が開けた");
    }

    /// 組む前の書き手と遅延構築が並んでも、 止まった後の cylinder は列とちょうど一致する (取りこぼし・重複なし)。 書き手
    /// 4 本が別々の eid を書き換え続け (値の書き換え・消す・書き戻す)、 途中で読み手が組む。 答えは列を `get_value` で
    /// なめる走査と、 書き手が最後に書いた値の写し (shadow) の 2 経路。
    #[test]
    fn writers_racing_the_lazy_build_all_reach_the_cylinder() {
        const THREADS: u32 = 4;
        const EIDS: u32 = 2_000;
        const VALUES: u64 = 7;
        for round in 0..30u64 {
            let hs = HimoStore::init(heap_region(64 * 1024), ValueType::Number, 0, EIDS * THREADS, false);
            let start = std::sync::Barrier::new(THREADS as usize + 1);
            let shadow: Vec<Vec<Option<u64>>> = std::thread::scope(|s| {
                let writers: Vec<_> = (0..THREADS)
                    .map(|t| {
                        let (hs, start) = (&hs, &start);
                        s.spawn(move || {
                            let mut last = vec![None; EIDS as usize];
                            let mut x = 0x9e37_79b9_7f4a_7c15u64 ^ (round << 8 | u64::from(t));
                            start.wait();
                            for _ in 0..20_000 {
                                x ^= x << 13;
                                x ^= x >> 7;
                                x ^= x << 17;
                                let i = (x % u64::from(EIDS)) as usize;
                                let eid = i as u32 * THREADS + t;
                                if x >> 60 == 0 {
                                    hs.remove(eid);
                                    last[i] = None;
                                } else {
                                    let v = (x >> 32) % VALUES;
                                    assert!(hs.set(eid, v));
                                    last[i] = Some(v);
                                }
                            }
                            last
                        })
                    })
                    .collect();
                start.wait();
                // 書き手が書いている途中で組む
                std::thread::sleep(std::time::Duration::from_micros(200 + 300 * (round % 5)));
                let _ = hs.pull(0);
                writers.into_iter().map(|w| w.join().unwrap()).collect()
            });
            assert!(hs.cylinder_built());
            for v in 0..VALUES {
                let mut got = hs.pull(v);
                got.sort_unstable();
                let mut by_scan: Vec<u32> = (0..EIDS * THREADS).filter(|&e| hs.get_value(e) == Some(v)).collect();
                by_scan.sort_unstable();
                let mut by_shadow: Vec<u32> = (0..THREADS)
                    .flat_map(|t| {
                        let last = &shadow[t as usize];
                        (0..EIDS).filter(move |&i| last[i as usize] == Some(v)).map(move |i| i * THREADS + t)
                    })
                    .collect();
                by_shadow.sort_unstable();
                assert_eq!(by_scan, by_shadow, "round {round}: 列が書き手の写しと違う (値 {v})");
                assert_eq!(got, by_scan, "round {round}: cylinder が列と違う (値 {v})");
            }
        }
    }

    /// 門の中で書きかけの書き手が居る間は、 遅延構築は scan しない (出るのを待つ)。 thread の運に頼らずに窓を踏む:
    /// 書き手を門に入った直後 (cell を書く前) で止め、 その間に読み手が組み始める。 組む側が待たないと、 scan は書く前の
    /// cell を読み、 書き手は門の道なので cylinder に入れず、 その eid は `pull` から消える (実測 2026-10-11: 門を閉じても
    /// 待たない形にすると、 この test が落ちる)。
    #[test]
    fn lazy_build_waits_for_a_writer_inside_the_gate() {
        let hs = make_store(64);
        let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        let built = std::sync::atomic::AtomicBool::new(false);
        std::thread::scope(|s| {
            let w = s.spawn(|| {
                hook::set("inside_gate", move || {
                    entered_tx.send(()).unwrap();
                    go_rx.recv().unwrap();
                });
                assert!(hs.set(3, 9));
            });
            entered_rx.recv().unwrap();
            let r = s.spawn(|| {
                let got = hs.pull(9);
                built.store(true, Ordering::SeqCst);
                got
            });
            std::thread::sleep(std::time::Duration::from_millis(50));
            // 書き手を放してから確かめる (先に assert すると、 落ちた時に書き手が止まったままになる)
            let early = built.load(Ordering::SeqCst);
            go_tx.send(()).unwrap();
            w.join().unwrap();
            let got = r.join().unwrap();
            assert!(!early, "門の中の書き手が書く前に組み終えた");
            assert_eq!(got, vec![3], "組む前に門の中で書いた cell を取りこぼした");
        });
        assert_eq!(hs.pull(9), vec![3]);
    }

    /// barrier (`write_barrier`、 購読の登録) も門の中の書き手が出るまで返らない。
    #[test]
    fn barrier_waits_for_a_writer_inside_the_gate() {
        let hs = make_store(64);
        let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
        let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
        let passed = std::sync::atomic::AtomicBool::new(false);
        std::thread::scope(|s| {
            let w = s.spawn(|| {
                hook::set("inside_gate", move || {
                    entered_tx.send(()).unwrap();
                    go_rx.recv().unwrap();
                });
                assert!(hs.set(5, 1));
            });
            entered_rx.recv().unwrap();
            let b = s.spawn(|| {
                hs.write_barrier();
                passed.store(true, Ordering::SeqCst);
                hs.get_value(5)
            });
            std::thread::sleep(std::time::Duration::from_millis(50));
            let early = passed.load(Ordering::SeqCst);
            go_tx.send(()).unwrap();
            w.join().unwrap();
            let got = b.join().unwrap();
            assert!(!early, "門の中の書き手が書く前に barrier を抜けた");
            assert_eq!(got, Some(1), "barrier の後に門の中で書いた cell が見えない");
        });
        assert!(!hs.gate_is_closed());
    }
}
