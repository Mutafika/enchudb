//! HimoStore — 紐1本分のストレージ。
//!
//! Column（ソースオブトゥルース）+ Cylinder（検索キャッシュ）+ Delta（差分）。
//!
//! ぶら下げる → Column に書く + delta に eid 追記
//! 引く       → Cylinder 結果 + delta 分を Column 直読みで補正
//! rebuild    → delta を Cylinder にマージ（delta が大きくなった時だけ）

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::cell::UnsafeCell;

use crate::column::Column;
use crate::cylinder_v27::BucketCylinder;
use crate::region::Region;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum HimoType {
    /// 共有タグ — Vocabulary を引く (dedupe あり)。複数 entity が同じ tag を共有する hub。
    Tag = 0,
    /// タグなし — u32 をそのまま値として扱う。inline 数値・eid 等。
    Number = 1,
    /// 他 entity への参照 — u32 を eid として扱う。engine は素通しするだけ、FK 制約は schema 層。
    Ref = 2,
    /// 終端タグ — FreeStore を引く (dedupe なし)。1 entity しか繋がらない葉ノード。
    Leaf = 3,
}

impl HimoType {
    pub fn from_byte(b: u8) -> Self {
        match b {
            0 => Self::Tag,
            2 => Self::Ref,
            3 => Self::Leaf,
            _ => Self::Number,
        }
    }
}

// BucketCylinder を直接積む。
//
// 並行性:
//   BucketCylinder は RwLock で保護。
//   - set/remove/restore は write lock を取る
//   - pull/slice_one は read lock を取り、Vec<u32> を clone して返す(slice 返しは不可)
//   Engine を Arc<Engine> で複数 thread から共有可能(tie_async + 並行 reader)。
//   Column は UnsafeCell のまま(atomic-size = 4byte、Column::set/clear は単一 writer
//   を仮定しているが、write path が consumer thread 1 本 + 同期 tie が &mut self
//   なので競合しない)。

use std::sync::RwLock;

pub struct HimoStore {
    col: UnsafeCell<Column>,
    cyl: RwLock<BucketCylinder>,
    pub himo_type: HimoType,
    /// 初期 bucket サイズのヒント。0 は「ヒントなし、必要時に拡張」。
    /// 実際の値制限ではない。value > max_values でも tie 可能(BucketCylinder が動的拡張)。
    pub max_values: u32,
    /// 現在の unique 値数(非空バケット数)。`himo_cardinality` API が参照する。
    unique_count: AtomicU32,
    /// issue #3: cylinder が column から populate 済みか。 lazy rebuild 用。
    /// `init` (新規 DB) では即 true (column 空)、 `load` (既存 DB) では false で
    /// 開始し、 最初の cyl 読み書きで `ensure_cylinder_built` 経由で rebuild する。
    cyl_built: AtomicBool,
}

unsafe impl Sync for HimoStore {}
unsafe impl Send for HimoStore {}

impl HimoStore {
    pub fn init(col_region: Region, ht: HimoType, max_values: u32, max_entities: u32) -> Self {
        let col = Column::init(col_region, 4, max_entities);
        // max_values == 0 は「ヒントなし」。bucket を空で作って必要時に拡張。
        let cyl = BucketCylinder::new(max_values, max_entities);
        Self {
            col: UnsafeCell::new(col),
            cyl: RwLock::new(cyl),
            himo_type: ht,
            max_values,
            unique_count: AtomicU32::new(0),
            // 新規 column は空なので rebuild 不要、 即 built 状態。
            cyl_built: AtomicBool::new(true),
        }
    }

    /// open 時の load。 issue #3: 旧版は `cyl.rebuild_from_column` を eager に呼んで
    /// per-himo に column 全 eid を走査していた。 himo 数 × entity 数で reopen latency
    /// が膨らむのを避けるため、 cylinder は空のまま返し、 最初の cyl 触りで
    /// `ensure_cylinder_built` 経由で rebuild する。
    pub fn load(col_region: Region, ht: HimoType, max_values: u32) -> Self {
        let col = Column::load(col_region);
        let max_entities = col.max_entities;
        let cyl = BucketCylinder::new(max_values, max_entities);
        Self {
            col: UnsafeCell::new(col),
            cyl: RwLock::new(cyl),
            himo_type: ht,
            max_values,
            // unique_count は cyl built 後に正しい値が入る。 build 前に
            // `unique_count()` を呼ばれる経路があったら ensure_cylinder_built 経由で
            // build がトリガされ、 正しい値が同期される。
            unique_count: AtomicU32::new(0),
            cyl_built: AtomicBool::new(false),
        }
    }

    fn col(&self) -> &Column { unsafe { &*self.col.get() } }

    /// cylinder が未 build なら column から rebuild する。 lazy rebuild の入口。
    /// fast path は AtomicBool::load (cyl_built == true) で即 return、 cache 1 hit
    /// 程度のコスト。 build path は per-himo 1 回だけ走る。
    ///
    /// 並行性: write lock を取って rebuild 中に double-check する standard な
    /// double-checked locking。 RwLock の write lock は exclusive なので
    /// rebuild 中の他 reader は待たされる (= 旧 eager rebuild と同じ semantics)。
    #[inline]
    fn ensure_cylinder_built(&self) {
        if self.cyl_built.load(Ordering::Acquire) { return; }
        let mut cyl = self.cyl.write().unwrap();
        // double-check (build 中に競合した別 thread が先に置いた可能性)
        if self.cyl_built.load(Ordering::Acquire) { return; }
        cyl.rebuild_from_column(self.col());
        self.unique_count.store(cyl.unique_count(), Ordering::Relaxed);
        self.cyl_built.store(true, Ordering::Release);
    }

    // ──── ぶら下げる / 外す ────

    pub fn set(&self, eid: u32, value: u32) {
        self.ensure_cylinder_built();
        self.col().ensure_count(eid);
        self.col().set(eid, &(value + 1).to_le_bytes());
        let delta = self.cyl.write().unwrap().insert(eid, value);
        self.apply_unique_delta(delta);
    }

    pub fn remove(&self, eid: u32) {
        if eid < self.col().count() {
            self.ensure_cylinder_built();
            self.col().clear(eid);
            let delta = self.cyl.write().unwrap().remove(eid);
            self.apply_unique_delta(delta);
        }
    }

    #[inline]
    fn apply_unique_delta(&self, delta: crate::cylinder_v27::UniqueDelta) {
        if delta.added > 0 {
            self.unique_count.fetch_add(delta.added, Ordering::Relaxed);
        }
        if delta.removed > 0 {
            self.unique_count.fetch_sub(delta.removed, Ordering::Relaxed);
        }
    }

    /// 現在の unique 値数(非空バケット数)。O(1)。
    /// lazy rebuild 未完了なら build をトリガしてから返す。
    pub fn unique_count(&self) -> u32 {
        self.ensure_cylinder_built();
        self.unique_count.load(Ordering::Relaxed)
    }

    // ──── 読む ────

    pub fn get_value(&self, eid: u32) -> Option<u32> {
        if eid >= self.col().count() { return None; }
        let stored = u32::from_le_bytes(self.col().get(eid).try_into().unwrap());
        if stored == 0 { None } else { Some(stored - 1) }
    }

    /// bulk get-value (= stored 形式、 0 = missing、 >=1 のとき値は stored-1)。
    /// callsite が `out` を持つ buffer reuse 設計。 Option enum を avoid して、
    /// 同 himo の N entity を 1 関数呼び出しで column scan する。
    #[inline]
    pub fn get_stored_into(&self, eids: &[enchudb_oplog::EntityId], out: &mut Vec<u32>) {
        let col = self.col();
        let count = col.count();
        out.clear();
        out.reserve(eids.len());
        for &eid in eids {
            let lid = enchudb_oplog::eid_local(eid);
            if lid >= count { out.push(0); continue; }
            out.push(u32::from_le_bytes(col.get(lid).try_into().unwrap()));
        }
    }

    #[inline(always)]
    pub fn value_eq(&self, eid: u32, value: u32) -> bool {
        u32::from_le_bytes(self.col().get(eid).try_into().unwrap()) == value + 1
    }

    pub fn get_raw_bytes(&self, eid: u32) -> [u8; 4] {
        if eid >= self.col().count() { return [0u8; 4]; }
        self.col().get(eid).try_into().unwrap()
    }

    pub fn restore(&self, eid: u32, old_bytes: &[u8; 4]) {
        self.ensure_cylinder_built();
        self.col().ensure_count(eid);
        let stored = u32::from_le_bytes(*old_bytes);
        self.col().set(eid, old_bytes);
        let delta = {
            let mut cyl = self.cyl.write().unwrap();
            if stored == 0 {
                cyl.remove(eid)
            } else {
                cyl.insert(eid, stored - 1)
            }
        };
        self.apply_unique_delta(delta);
    }

    // ──── 引く ────

    /// 値に合致する entity を Vec<u32> で返す(read lock 下でクローン)。
    pub fn pull(&self, value: u32) -> Vec<u32> {
        self.ensure_cylinder_built();
        self.cyl.read().unwrap().slice_one(value).to_vec()
    }

    /// 値が tie された全 entity を Vec<u32> で返す (= column 非ゼロ走査)。
    /// O(next_eid) で重い。 schema の `all()` 経由など、 「table の任意 column を持つ
    /// 全 row」 を取りたい時に使う想定。
    pub fn entities_with_value(&self) -> Vec<u32> {
        let count = self.col().count();
        let mut result = Vec::new();
        for eid in 0..count {
            let stored = u32::from_le_bytes(self.col().get(eid).try_into().unwrap());
            if stored != 0 { result.push(eid); }
        }
        result
    }

    /// 同上だが長さだけ。プランナー用。
    pub fn slice_len(&self, value: u32) -> usize {
        self.ensure_cylinder_built();
        self.cyl.read().unwrap().slice_one(value).len()
    }

    /// 入っている値を列挙(順序は保証しない)。
    pub fn unique_values(&self) -> Vec<u32> {
        self.ensure_cylinder_built();
        self.cyl.read().unwrap().unique_values()
    }

    /// 総件数。
    pub fn total(&self) -> usize {
        self.ensure_cylinder_built();
        self.cyl.read().unwrap().total()
    }

    pub fn delta_eids(&self) -> &[u32] { &[] }
    pub fn delta_is_empty(&self) -> bool { true }
    pub fn delta_needs_rebuild(&self) -> bool { false }

    pub fn rebuild_cylinder(&self) {}

    pub fn scan(&self, value: u32) -> Vec<u32> {
        let count = self.col().count();
        let target = value + 1;
        let mut result = Vec::new();
        for eid in 0..count {
            let stored = u32::from_le_bytes(self.col().get(eid).try_into().unwrap());
            if stored == target {
                result.push(eid);
            }
        }
        result
    }

    pub fn sync(&self) {}
}
