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

use parking_lot::{Mutex, MutexGuard};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::column::Column;
use crate::lockfree_cylinder::LockFreeCylinder;
use crate::region::Region;
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
//   組んだ後は門を閉じたまま = 上の直列化のまま。

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
    /// writer 直列化 lock。 lazy build と set/remove/restore が取る。
    /// reader (pull 系) は一切取らない。 同期 tie / schema commit の多 thread
    /// 呼び出しを master (RwLock write) と同じ直列度で安全にする（#96 レビュー発覚分）。
    /// cylinder を組む前の set / remove は取らずに `gate` を通る。
    write_lock: Mutex<()>,
    /// cylinder を組んでいない間の書き手の門 ([`WriteGate`])。 開いている = 組んでいない。 組む時に
    /// (write_lock の下で) 閉じて、 以後は開けない。 barrier ([`HimoStore::write_guard`]) は閉じて開け直す。
    gate: WriteGate,
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

// SAFETY: Cylinder の writer は write_lock で直列 (組んだ後は門が閉じていて、 全部の書き手が
// write_lock を取る)、 reader は lock-free（Cylinder は epoch + Mutex(sparse)）。 組む前の
// 門の中の書き手は Column の cell (atomic) と count (fetch_max) だけを書く。 Column の read は
// 無同期（既知、 別 issue）。
unsafe impl Sync for HimoStore {}
unsafe impl Send for HimoStore {}

/// 紐の書き手を止めている間持つ ([`HimoStore::write_guard`])。 drop で門を開け直してから write_lock を離す。
pub(crate) struct WriteGuard<'a> {
    store: &'a HimoStore,
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
        }
    }

    /// open 時の load。 cylinder は空のまま返し、最初の cyl 触りで
    /// `ensure_cylinder_built` 経由で rebuild する（reopen latency を膨らませないため）。
    /// `max_entities` / `padded` は header が一度も届いていない列を組む時だけ使う (`Column::load_or_unwritten`)。
    pub fn load(col_region: Region, ht: ValueType, max_values: u32, max_entities: u32, padded: bool) -> Self {
        let col = Column::load_or_unwritten(col_region, ht.width(), max_entities, padded);
        Self {
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
        }
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

    /// 遅延 column の初回組み立て。 競合しても `OnceLock` が 1 本に絞る
    /// (負けた側の `Column::load` は header を読むだけで副作用が無い)。
    #[cold]
    #[cfg(not(target_arch = "wasm32"))]
    fn col_slow(&self) -> &Column {
        let lazy = self
            .lazy
            .as_ref()
            .expect("HimoStore column is neither loaded nor lazy");
        self.col
            .get_or_init(|| {
                Column::load_or_unwritten(lazy.set.region(lazy.kind), self.value_type.width(), lazy.max_entities, lazy.padded)
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
        // (組んだ cylinder は write_lock の下の書き手 1 本で手入れする)
        self.gate.close();
        let col = self.col();
        let count = col.count();
        // dense の窓の始まりは組む前に (読み手はまだこの索引を読まない)
        self.cyl.set_low_base(self.dense_base_hint.load(Ordering::Relaxed));
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
    /// 来た書き手はこの lock の道に回る (同じ loom model が見る)。
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
        // cylinder を組んでいなければ、 write_lock を取らずに門の中で Column だけを書く (`WriteGate`)。 組んだ後は
        // 門が閉じたままなので、 先に flag を見て門の数に触らない (見誤っても門が閉じていれば lock の道に回る)
        if !self.cyl_built.load(Ordering::Relaxed)
            && let Some(_pass) = self.gate.try_enter()
        {
            hook::at("inside_gate");
            return self.store_unindexed(eid, value);
        }
        let w = self.write_lock.lock();
        // 未 build の cylinder は触らない (#270)。 順序契約は `cyl_live` 参照。
        let Some(cyl) = self.cyl_live(&w) else {
            return self.store_unindexed(eid, value);
        };
        let col = self.col();
        if col.ensure_committed_for(eid).is_err() {
            return false;
        }
        col.ensure_count(eid);
        let old = value_at(col, eid);
        if old == Some(value) {
            return true; // 冗長な re-tie = no-op（bucket に dup を作らない）
        }
        // 値更新: 旧 value の bucket に stale が残る → その bucket の read は verify する。
        // Column を書き換える **前** に flag を立てる (request12、順序契約は note_stale 参照)
        let stale = old.and_then(|o| cyl.note_stale(o).map(|s| (o, s)));
        // #106: Release store。 leaf offset を publish する前に書いた LeafStore slot
        // (payload/gen) を、 offset を Acquire で読む reader が必ず観測できるようにする。
        store_at(col, eid, value + 1);
        cyl.insert(eid, value, |v, e| stored_at(col, e) == v + 1);
        // compaction は Column 更新の **後** (keep = value_eq が新状態を見るため)
        if let Some((o, (len, live))) = stale {
            self.maybe_compact(o, len, live);
        }
        self.maybe_compact_sparse(cyl);
        true
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
        if value_at(col, eid) != Some(value) {
            // #106: Release store (上と同じ)
            store_at(col, eid, value + 1);
        }
        true
    }

    /// sparse (大きな値の run) の古い entry が半分を超えたら Column 基準で組み直す。 write_lock 下・
    /// Column 更新後に。 償却 O(1) / write (組み直すたびに古い entry は 0 に戻る)。
    fn maybe_compact_sparse(&self, cyl: &LockFreeCylinder) {
        if cyl.sparse_needs_compact() {
            let col = self.col();
            cyl.compact_sparse(|v, eid| stored_at(col, eid) == v + 1);
        }
    }

    /// この紐の書き手を止める (並びの索引 `OrderIndex` を作る時。 via の紐の書き手と直列にする): write_lock を取り、
    /// 門を閉じて中の書き手が出るのを待つ。 持っている間、 この紐の書き手は全部 write_lock を待つ。 drop で門を開け
    /// 直す (cylinder を組んでいれば閉じたまま) — 開けた後に入る書き手は、 持っている間に置いたもの (作った索引) を見る。
    pub(crate) fn write_guard(&self) -> WriteGuard<'_> {
        let lock = self.write_lock.lock();
        self.gate.close();
        WriteGuard { store: self, _lock: lock }
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
            // cylinder を組んでいなければ門の中で消すだけ (`set` と同じ)
            if !self.cyl_built.load(Ordering::Relaxed)
                && let Some(_pass) = self.gate.try_enter()
            {
                if value_at(col, eid).is_some() {
                    col.clear(eid);
                }
                return;
            }
            let w = self.write_lock.lock();
            let cyl = self.cyl_live(&w); // set と同じ契約
            if let Some(o) = value_at(col, eid) {
                // 削除: 旧 bucket に stale が残る（Cylinder は触らない、verify で落とす）。
                // flag → Column の順 (set と同じ)
                let stale = cyl.and_then(|c| c.note_stale(o));
                col.clear(eid);
                if let Some((len, live)) = stale {
                    self.maybe_compact(o, len, live);
                }
                if let Some(cyl) = cyl {
                    self.maybe_compact_sparse(cyl);
                }
            }
        }
    }

    /// stale 率が閾値を超えた bucket を Column 基準で組み直す (request12 P2)。
    /// write_lock 下・Column 更新後に呼ぶこと。trigger は stale 率 50% なので
    /// amortized O(1)/write (Vec doubling と同じ理屈 — 組み直し後の stale は 0、
    /// 次の trigger までに live 相当数の churn が必要)。
    fn maybe_compact(&self, value: u64, len: usize, live: u32) {
        if len >= COMPACT_MIN_LEN && (len - live as usize) * 2 >= len {
            let col = self.col();
            self.cyl.compact_bucket(value, |eid| stored_at(col, eid) == value + 1);
        }
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
        let col = self.col();
        if cyl.sparse_churned() {
            cyl.compact_sparse(|v, eid| stored_at(col, eid) == v + 1);
        }
        for v in cyl.unique_values().into_iter().filter(|&v| cyl.is_dense(v)) {
            // clean bucket (churn 痕なし) は組み直し不要 — 無条件 swap は巨大 himo で
            // write_lock の長期保持 + 旧 backing の epoch 滞留 (一時 ~2x RSS) を招く
            // (PR #103 レビュー)。write_lock 下なので flag 判定は正確。
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
        let mut stale = None;
        if let (Some(cyl), Some(o)) = (cyl, old) {
            // flag → Column の順 (set と同じ、request12)
            stale = cyl.note_stale(o).map(|s| (o, s));
        }
        store_at(col, eid, stored);
        if let (Some(cyl), Some(n)) = (cyl, new) {
            cyl.insert(eid, n, |v, e| stored_at(col, e) == v + 1);
        }
        if let Some((o, (len, live))) = stale {
            self.maybe_compact(o, len, live);
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
        let (raw, needs_verify) = self.cyl.read_to_vec_verify(value);
        if !needs_verify {
            // この bucket は append-only: 全 live、dup なし
            raw
        } else {
            // lazy verify: Column で現在値を確認、churn 由来の dup を除去
            let col = self.col();
            let mut out: Vec<u32> = raw
                .into_iter()
                .filter(|&eid| stored_at(col, eid) == value + 1)
                .collect();
            out.sort_unstable();
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
        if self.cyl.read_into_verify(value, out) {
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
            out[start..].sort_unstable();
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

    /// #414: 列の header が一度もディスクに届いていない (作った直後に電源が落ちた = 全部ゼロ) region を開いても
    /// 落ちない。 旧 `Column::load` は value_size 0 の列を組み、 open の Leaf の free-list 再構成
    /// (`for_each_set_value` → `values_u32` の assert) で panic した。 書かれていない列として開き、 書ける。
    #[test]
    fn load_of_never_written_header_is_an_empty_column() {
        for ht in [ValueType::Leaf, ValueType::Number, ValueType::Number64] {
            let hs = HimoStore::load(heap_region(64 * 1024), ht, 0, 64, false);
            let mut n = 0;
            hs.for_each_set_value(|_| n += 1);
            assert_eq!(n, 0, "{ht:?}");
            assert!(hs.set(3, 7), "{ht:?}");
            assert_eq!(hs.get_value(3), Some(7), "{ht:?}");
        }
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

    /// 2026-10-11: cylinder を組んでいない紐の書き手 (set / remove) は write_lock を取らずに門を通る。 組んだ後は門が
    /// 閉じたままで、 書き手は write_lock を待つ (対照: 同じ持ち方で、 組む前は待たず、 組んだ後は待つ)。
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
        let held = hs.write_lock.lock();
        let ok = finishes_while(|| assert!(hs.set(3, 6)), std::time::Duration::from_millis(100), || drop(held));
        assert!(!ok, "対照: 組んだ後の書き手が write_lock を待たない");
        let mut got = hs.pull(6);
        got.sort_unstable();
        assert_eq!(got, vec![2, 3]);
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
