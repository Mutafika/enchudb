//! BucketCylinder (v27→v32) — per-value bucket、ソート不要、rebuild 不要。
//!
//! # 設計
//!
//! - dense 領域: `buckets[value] = entity ids` (value < DENSE_CAP の時)
//! - sparse 領域: `sparse.get(&value)` (value >= DENSE_CAP の時)
//! - positions[eid] = (value, idx)。value = u32::MAX なら未 tie。
//!
//! # v32 変更点(sinfo 40GB alloc バグ対策)
//!
//! v27 までは `ensure_bucket(value)` が `buckets.resize(value+1)` していたため、
//! 巨大な値(例: epoch seconds 1.7e9)で 40GB 確保してハング。v32 では
//! value >= DENSE_CAP(=2^20 = 1M)を sparse HashMap に逃がす。
//!
//! # β-heavy phase 1: positions を BucketPositions enum で抽象化
//!
//! 旧 `Vec<(u32, u32)> + eid_offset` を `BucketPositions` enum に置換。
//! - `Heap`: 従来通り heap-backed Vec、 prepend / extend 自由。 default。
//! - `Region`: PositionsRegion (mmap-back 可)、 prepend 不可だが OOM 耐性大。
//!
//! BucketCylinder 自身は両 variant を見分けず get/set/update_idx/clear だけ
//! 呼ぶ。 HimoStore 側が backing 選択。 phase 1 step 2 では default は Heap
//! のまま (behavior 不変)。 step 3 で HimoStore が Region 側を使い始める。
//!
//! # 計算量
//!
//! - insert dense: O(1)
//! - insert sparse: O(1) amortized(HashMap)
//! - remove: O(1) swap_remove
//! - slice_one: O(1) スライス直返し
//! - rebuild_from_column: open 時の初期構築。O(n)。
//!
//! # max_values の扱い
//!
//! - 初期 bucket サイズのヒント。値の上限ではない。
//! - DENSE_CAP(1M)を超えるヒントは無視される(dense メモリ爆発防止)。
//! - 0 を渡すと「ヒントなし」。最初の tie で必要サイズまで伸びる。

use std::collections::HashMap;
use crate::column::Column;
use crate::positions_region::PositionsRegion;

/// dense バケット上限。これ以上の value は sparse HashMap に格納。
/// 2^20 = 1_048_576。dense 領域の最大メモリ: ~24MB (1M 個の空 Vec ヘッダ)。
pub const DENSE_CAP: u32 = 1 << 20;

/// 「未 tie」を示す value sentinel(tie は value < u32::MAX を assert するので衝突しない)。
const EMPTY_VALUE: u32 = u32::MAX;

/// BucketCylinder の positions 部 — eid → (value, idx_in_bucket) の dense map。
///
/// Heap variant は旧 `Vec<(u32, u32)> + eid_offset` を internalize。 Region
/// variant は PositionsRegion (mmap-back 可) を呼ぶだけ。
pub(crate) enum BucketPositions {
    Heap {
        entries: Vec<(u32, u32)>,
        /// `u32::MAX` の間は 「まだ何も tie されてない」。 最初の `set` で eid に確定。
        eid_offset: u32,
    },
    Region(PositionsRegion),
}

impl BucketPositions {
    fn heap() -> Self {
        Self::Heap { entries: Vec::new(), eid_offset: u32::MAX }
    }

    /// eid_offset を返す。 空 (まだ何も set されてない) なら `u32::MAX`。
    #[inline]
    fn eid_offset(&self) -> u32 {
        match self {
            Self::Heap { eid_offset, .. } => *eid_offset,
            Self::Region(r) => r.eid_offset(),
        }
    }

    /// eid の (value, idx) を取得。 範囲外 / sentinel なら None。
    #[inline]
    fn get(&self, eid: u32) -> Option<(u32, u32)> {
        match self {
            Self::Heap { entries, eid_offset } => {
                if *eid_offset == u32::MAX || eid < *eid_offset {
                    return None;
                }
                let idx = (eid - *eid_offset) as usize;
                if idx >= entries.len() {
                    return None;
                }
                let (v, i) = entries[idx];
                if v == EMPTY_VALUE { None } else { Some((v, i)) }
            }
            Self::Region(r) => r.get(eid),
        }
    }

    /// eid に (value, idx) を書き込む。 必要なら range を伸ばす。
    /// Region mode で eid < eid_offset の場合は panic (PositionsRegion 仕様)。
    #[inline]
    fn set(&mut self, eid: u32, value: u32, idx: u32) {
        match self {
            Self::Heap { entries, eid_offset } => {
                Self::heap_ensure(entries, eid_offset, eid);
                let pos = (eid - *eid_offset) as usize;
                entries[pos] = (value, idx);
            }
            Self::Region(r) => {
                r.set(eid, value, idx);
            }
        }
    }

    /// 既存 entry の idx だけ更新 (swap_remove で末尾と入れ替わった entity 用)。
    #[inline]
    fn update_idx(&mut self, eid: u32, new_idx: u32) {
        match self {
            Self::Heap { entries, eid_offset } => {
                if *eid_offset == u32::MAX || eid < *eid_offset {
                    return;
                }
                let pos = (eid - *eid_offset) as usize;
                if pos >= entries.len() {
                    return;
                }
                entries[pos].1 = new_idx;
            }
            Self::Region(r) => {
                r.update_idx(eid, new_idx);
            }
        }
    }

    /// eid の entry を空にする (sentinel に戻す)。
    #[inline]
    fn clear(&mut self, eid: u32) {
        match self {
            Self::Heap { entries, eid_offset } => {
                if *eid_offset == u32::MAX || eid < *eid_offset {
                    return;
                }
                let pos = (eid - *eid_offset) as usize;
                if pos < entries.len() {
                    entries[pos] = (EMPTY_VALUE, 0);
                }
            }
            Self::Region(r) => {
                r.clear(eid);
            }
        }
    }

    /// 全体をリセット (rebuild の最初に呼ぶ)。
    fn clear_all(&mut self) {
        match self {
            Self::Heap { entries, eid_offset } => {
                entries.clear();
                *eid_offset = u32::MAX;
            }
            Self::Region(r) => {
                r.clear_all();
            }
        }
    }

    /// heap mode: positions Vec を eid 範囲まで伸ばす (prepend / extend)。
    fn heap_ensure(entries: &mut Vec<(u32, u32)>, eid_offset: &mut u32, eid: u32) {
        if *eid_offset == u32::MAX {
            *eid_offset = eid;
            entries.push((EMPTY_VALUE, 0));
            return;
        }
        if eid < *eid_offset {
            // prepend: offset を eid まで下げる、 差分を空 entry で埋める
            let prepend_count = (*eid_offset - eid) as usize;
            let mut new_entries = Vec::with_capacity(prepend_count + entries.len());
            new_entries.resize(prepend_count, (EMPTY_VALUE, 0));
            new_entries.extend_from_slice(entries);
            *entries = new_entries;
            *eid_offset = eid;
        } else {
            let idx = (eid - *eid_offset) as usize;
            if idx >= entries.len() {
                entries.resize(idx + 1, (EMPTY_VALUE, 0));
            }
        }
    }

    /// rebuild_from_column の高速初期化用 — heap mode のみ。 eid 範囲を一括 resize する。
    /// Region mode では何もしない (Region は内部で incremental に伸びる)。
    fn rebuild_prepare(&mut self, min_eid: u32, max_eid: u32) {
        if let Self::Heap { entries, eid_offset } = self {
            *eid_offset = min_eid;
            entries.resize((max_eid - min_eid + 1) as usize, (EMPTY_VALUE, 0));
        }
    }
}

pub struct BucketCylinder {
    max_values: u32,
    buckets: Vec<Vec<u32>>,
    sparse: HashMap<u32, Vec<u32>>,
    positions: BucketPositions,
    total: u32,
    /// 現在の unique 値数。UniqueDelta で更新。
    unique_count_cache: u32,
}

/// insert / remove が unique 値カウントに与えた影響。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UniqueDelta {
    pub added: u32,
    pub removed: u32,
}

impl UniqueDelta {
    pub const NONE: Self = Self { added: 0, removed: 0 };
}

impl BucketCylinder {
    pub fn new(max_values: u32, _max_entities: u32) -> Self {
        // max_values は初期サイズのヒント。0 なら空で始めて必要時に伸ばす。
        // DENSE_CAP を超えるヒントは無視(メモリ爆発防止)。
        // positions は空 Heap で start し、`set` で on-demand 伸長する。
        // max_entities ヒントを尊重して 16M × 8 byte = 128 MB / himo を pre-alloc
        // すると、 数百 himo を抱える consumer (sinfohub-server 等) が schema
        // declare 段階で 20+ GB heap 消費して OOM kill される。
        Self::with_positions(max_values, BucketPositions::heap())
    }

    /// β-heavy phase 1: positions を外部 (sidecar mmap region) で受け取る ctor。
    /// engine が PositionsSidecar から払い出す。 既存テスト経路は `new` のまま
    /// Heap mode で動く。
    pub fn with_positions_region(max_values: u32, positions: PositionsRegion) -> Self {
        Self::with_positions(max_values, BucketPositions::Region(positions))
    }

    fn with_positions(max_values: u32, positions: BucketPositions) -> Self {
        let bucket_count = if max_values == 0 {
            0
        } else {
            (max_values as usize + 1).min(DENSE_CAP as usize)
        };
        Self {
            max_values,
            buckets: (0..bucket_count).map(|_| Vec::new()).collect(),
            sparse: HashMap::new(),
            positions,
            total: 0,
            unique_count_cache: 0,
        }
    }

    /// value を eid に紐づける。既存エントリがあれば置き換え。
    pub fn insert(&mut self, eid: u32, value: u32) -> UniqueDelta {
        debug_assert!(value != EMPTY_VALUE, "value == u32::MAX is sentinel");

        let mut delta = UniqueDelta::NONE;

        // 既存 entry を取り出す (positions には触らずに get だけ)。
        if let Some((old_value, old_idx)) = self.positions.get(eid) {
            let was_last = self.location_len(old_value) == 1;
            self.swap_remove_at(old_value, old_idx);
            if was_last {
                delta.removed += 1;
            }
        }

        self.ensure_bucket(value);
        let was_empty = self.location_len(value) == 0;
        let idx = self.push_to(value, eid);
        if was_empty {
            delta.added += 1;
        }

        // 新しい (value, idx) を書き込み (set が range 伸長を担う)
        let prev = self.positions.get(eid).is_some();
        self.positions.set(eid, value, idx);
        if !prev {
            self.total += 1;
        }

        self.unique_count_cache = self
            .unique_count_cache
            .saturating_add(delta.added)
            .saturating_sub(delta.removed);

        delta
    }

    /// eid の紐を外す。
    pub fn remove(&mut self, eid: u32) -> UniqueDelta {
        let (value, idx) = match self.positions.get(eid) {
            Some(p) => p,
            None => return UniqueDelta::NONE,
        };
        let was_last = self.location_len(value) == 1;
        self.swap_remove_at(value, idx);
        self.positions.clear(eid);
        self.total -= 1;
        let delta = UniqueDelta {
            added: 0,
            removed: if was_last { 1 } else { 0 },
        };
        self.unique_count_cache = self.unique_count_cache.saturating_sub(delta.removed);
        delta
    }

    /// value ちょうどの entity を返す(slice、ゼロコピー)。
    #[inline]
    pub fn slice_one(&self, value: u32) -> &[u32] {
        if value < DENSE_CAP {
            let bi = value as usize;
            if bi < self.buckets.len() {
                return &self.buckets[bi];
            }
            &[]
        } else {
            self.sparse.get(&value).map(|v| v.as_slice()).unwrap_or(&[])
        }
    }

    pub fn total(&self) -> usize {
        self.total as usize
    }

    pub fn max_values(&self) -> u32 {
        self.max_values
    }

    /// 現在の unique 値数(非空バケット + sparse 要素数)。
    pub fn unique_count(&self) -> u32 {
        self.unique_count_cache
    }

    /// 入っている値を列挙(順序は保証しない)。
    pub fn unique_values(&self) -> Vec<u32> {
        let mut out: Vec<u32> = self
            .buckets
            .iter()
            .enumerate()
            .filter_map(|(v, b)| if b.is_empty() { None } else { Some(v as u32) })
            .collect();
        out.extend(self.sparse.keys().copied());
        out
    }

    /// range は全幅探索。low..=high の各バケットを順に iter として返す。
    /// dense と sparse を両方走査する。
    pub fn range_iter(&self, low: u32, high: u32) -> Box<dyn Iterator<Item = &u32> + '_> {
        let dense_hi = (high as usize)
            .min(self.buckets.len().saturating_sub(1))
            .min(DENSE_CAP as usize - 1);
        let dense_lo = (low as usize).min(self.buckets.len());
        let dense_slice: &[Vec<u32>] = if dense_lo >= self.buckets.len() || dense_lo > dense_hi {
            &self.buckets[0..0]
        } else {
            &self.buckets[dense_lo..=dense_hi]
        };
        let dense_iter = dense_slice.iter().flatten();

        // sparse の range — HashMap は順序なしなので走査
        if high < DENSE_CAP {
            Box::new(dense_iter)
        } else {
            let sparse_lo = low.max(DENSE_CAP);
            let sparse_iter = self
                .sparse
                .iter()
                .filter(move |(k, _)| **k >= sparse_lo && **k <= high)
                .flat_map(|(_, v)| v.iter());
            Box::new(dense_iter.chain(sparse_iter))
        }
    }

    /// Column から全再構築。open 時に呼ぶ。
    pub fn rebuild_from_column(&mut self, column: &Column) {
        for b in &mut self.buckets {
            b.clear();
        }
        self.sparse.clear();
        self.positions.clear_all();
        self.total = 0;
        self.unique_count_cache = 0;

        let count = column.count();
        if count == 0 {
            return;
        }

        // pass 1: 実際に tie された min / max eid を特定
        let mut min_eid: u32 = u32::MAX;
        let mut max_eid: u32 = 0;
        for eid in 0..count {
            let bytes: [u8; 4] = column.get(eid).try_into().unwrap();
            if u32::from_le_bytes(bytes) != 0 {
                if min_eid == u32::MAX {
                    min_eid = eid;
                }
                max_eid = eid;
            }
        }
        if min_eid == u32::MAX {
            // 一度も tie されてない column → positions 空のまま
            return;
        }

        // pass 2: positions を tied range だけ確保して populate
        self.positions.rebuild_prepare(min_eid, max_eid);
        for eid in min_eid..=max_eid {
            let bytes: [u8; 4] = column.get(eid).try_into().unwrap();
            let stored = u32::from_le_bytes(bytes);
            if stored != 0 {
                let value = stored - 1;
                self.ensure_bucket(value);
                let was_empty = self.location_len(value) == 0;
                let idx = self.push_to(value, eid);
                self.positions.set(eid, value, idx);
                self.total += 1;
                if was_empty {
                    self.unique_count_cache += 1;
                }
            }
        }
    }

    // ═══════════════ internal helpers ═══════════════

    /// dense 領域 (value < DENSE_CAP の場合) を必要に応じて拡張。
    #[inline]
    fn ensure_bucket(&mut self, value: u32) {
        if value < DENSE_CAP && (value as usize) >= self.buckets.len() {
            self.buckets.resize(value as usize + 1, Vec::new());
        }
    }

    /// value の場所に eid を push し、idx を返す。
    fn push_to(&mut self, value: u32, eid: u32) -> u32 {
        if value < DENSE_CAP {
            let bi = value as usize;
            let idx = self.buckets[bi].len() as u32;
            self.buckets[bi].push(eid);
            idx
        } else {
            let v = self.sparse.entry(value).or_insert_with(Vec::new);
            let idx = v.len() as u32;
            v.push(eid);
            idx
        }
    }

    /// value の場所の長さ。存在しない場合 0。
    fn location_len(&self, value: u32) -> usize {
        if value < DENSE_CAP {
            self.buckets.get(value as usize).map(|b| b.len()).unwrap_or(0)
        } else {
            self.sparse.get(&value).map(|b| b.len()).unwrap_or(0)
        }
    }

    /// value の場所の idx 位置を swap_remove。末尾と入れ替えた entity の
    /// positions を更新。空になったら sparse からエントリ削除。
    fn swap_remove_at(&mut self, value: u32, idx: u32) {
        if value < DENSE_CAP {
            let bi = value as usize;
            let bucket = &mut self.buckets[bi];
            let last = bucket.len() - 1;
            if (idx as usize) != last {
                let moved_eid = bucket[last];
                bucket.swap_remove(idx as usize);
                self.positions.update_idx(moved_eid, idx);
            } else {
                bucket.pop();
            }
        } else {
            let remove_entry;
            let moved_eid_opt;
            let new_idx;
            {
                let bucket = self
                    .sparse
                    .get_mut(&value)
                    .expect("sparse bucket missing");
                let last = bucket.len() - 1;
                if (idx as usize) != last {
                    moved_eid_opt = Some(bucket[last]);
                    new_idx = idx;
                    bucket.swap_remove(idx as usize);
                } else {
                    moved_eid_opt = None;
                    new_idx = 0;
                    bucket.pop();
                }
                remove_entry = bucket.is_empty();
            }
            if let Some(moved_eid) = moved_eid_opt {
                self.positions.update_idx(moved_eid, new_idx);
            }
            if remove_entry {
                self.sparse.remove(&value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_slice_one() {
        let mut c = BucketCylinder::new(10, 100);
        c.insert(1, 3);
        c.insert(2, 3);
        c.insert(3, 7);
        assert_eq!(c.slice_one(3), &[1, 2]);
        assert_eq!(c.slice_one(7), &[3]);
        assert_eq!(c.slice_one(5), &[] as &[u32]);
        assert_eq!(c.total(), 3);
    }

    #[test]
    fn remove_swap() {
        let mut c = BucketCylinder::new(10, 100);
        c.insert(1, 5);
        c.insert(2, 5);
        c.insert(3, 5);
        c.remove(2);
        let slice = c.slice_one(5);
        assert_eq!(slice.len(), 2);
        assert!(slice.contains(&1));
        assert!(slice.contains(&3));
        assert_eq!(c.total(), 2);
    }

    #[test]
    fn insert_replaces_old_value() {
        let mut c = BucketCylinder::new(10, 100);
        c.insert(1, 3);
        c.insert(1, 7);
        assert_eq!(c.slice_one(3), &[] as &[u32]);
        assert_eq!(c.slice_one(7), &[1]);
        assert_eq!(c.total(), 1);
    }

    #[test]
    fn range_iter_all_buckets() {
        let mut c = BucketCylinder::new(20, 100);
        c.insert(1, 3);
        c.insert(2, 5);
        c.insert(3, 7);
        c.insert(4, 9);
        let got: Vec<u32> = c.range_iter(4, 8).copied().collect();
        let mut sorted = got.clone();
        sorted.sort();
        assert_eq!(sorted, vec![2, 3]);
    }

    #[test]
    fn dynamic_bucket_expansion() {
        let mut c = BucketCylinder::new(10, 100);
        c.insert(1, 1000);
        c.insert(2, 10);
        assert_eq!(c.slice_one(1000), &[1]);
        assert_eq!(c.slice_one(10), &[2]);
        assert_eq!(c.slice_one(500), &[] as &[u32]);
    }

    #[test]
    fn unique_values_enumerates_nonempty() {
        let mut c = BucketCylinder::new(20, 100);
        c.insert(1, 3);
        c.insert(2, 5);
        c.insert(3, 3);
        let mut u = c.unique_values();
        u.sort();
        assert_eq!(u, vec![3, 5]);
    }

    #[test]
    fn unique_delta_basic() {
        let mut c = BucketCylinder::new(10, 100);
        assert_eq!(c.insert(1, 3), UniqueDelta { added: 1, removed: 0 });
        assert_eq!(c.insert(2, 3), UniqueDelta::NONE);
        assert_eq!(c.insert(3, 5), UniqueDelta { added: 1, removed: 0 });
        assert_eq!(c.remove(1), UniqueDelta::NONE);
        assert_eq!(c.remove(2), UniqueDelta { added: 0, removed: 1 });
    }

    #[test]
    fn unique_delta_on_replace() {
        let mut c = BucketCylinder::new(10, 100);
        c.insert(1, 3);
        c.insert(2, 3);
        assert_eq!(c.insert(1, 7), UniqueDelta { added: 1, removed: 0 });
        assert_eq!(c.insert(2, 9), UniqueDelta { added: 1, removed: 1 });
    }

    #[test]
    fn zero_max_values_works() {
        let mut c = BucketCylinder::new(0, 100);
        c.insert(1, 42);
        assert_eq!(c.slice_one(42), &[1]);
        assert_eq!(c.slice_one(0), &[] as &[u32]);
    }

    // ═══ v32: sparse fallback ═══

    #[test]
    fn huge_value_uses_sparse() {
        // sinfo バグ: epoch seconds を tie → 以前は 40GB 確保してハング。
        // v32: sparse に逃がすので即座に insert 完了。
        let mut c = BucketCylinder::new(10, 100);
        let ts = 1_700_000_000u32; // 2023-11-15 頃
        c.insert(1, ts);
        assert_eq!(c.slice_one(ts), &[1]);
        assert_eq!(c.total(), 1);
        // dense 領域は膨張していない(max 11 = 10+1)
        assert!(c.buckets.len() <= 11);
    }

    #[test]
    fn sparse_insert_remove_roundtrip() {
        let mut c = BucketCylinder::new(0, 100);
        let v1 = 2_000_000_000u32;
        let v2 = 3_000_000_000u32;
        c.insert(1, v1);
        c.insert(2, v1);
        c.insert(3, v2);
        assert_eq!(c.slice_one(v1).len(), 2);
        assert_eq!(c.slice_one(v2).len(), 1);

        c.remove(2);
        assert_eq!(c.slice_one(v1), &[1]);
        c.remove(1);
        assert_eq!(c.slice_one(v1), &[] as &[u32]);
        c.remove(3);
        assert_eq!(c.slice_one(v2), &[] as &[u32]);
    }

    #[test]
    fn mixed_dense_and_sparse() {
        let mut c = BucketCylinder::new(100, 1000);
        c.insert(1, 50);
        c.insert(2, 50);
        c.insert(3, 5_000_000);   // sparse
        c.insert(4, 5_000_000);   // sparse
        c.insert(5, 42);           // dense

        assert_eq!(c.slice_one(50), &[1, 2]);
        assert_eq!(c.slice_one(5_000_000).len(), 2);
        assert_eq!(c.slice_one(42), &[5]);
        assert_eq!(c.total(), 5);
        assert_eq!(c.unique_count(), 3);
    }

    #[test]
    fn unique_count_tracks_sparse() {
        let mut c = BucketCylinder::new(0, 100);
        c.insert(1, 10_000_000);
        assert_eq!(c.unique_count(), 1);
        c.insert(2, 10_000_000);
        assert_eq!(c.unique_count(), 1);
        c.insert(3, 20_000_000);
        assert_eq!(c.unique_count(), 2);
        c.remove(1);
        assert_eq!(c.unique_count(), 2);
        c.remove(2);
        assert_eq!(c.unique_count(), 1);
    }

    #[test]
    fn dense_cap_hint_clamped() {
        // max_values に極端な値を指定しても dense 領域は DENSE_CAP で打ち切り。
        let c = BucketCylinder::new(u32::MAX - 2, 100);
        assert!(c.buckets.len() <= DENSE_CAP as usize);
    }

    // ═══ positions の lazy alloc ═══

    #[test]
    fn positions_start_empty_regardless_of_hint() {
        // max_entities ヒントを無視して positions を空で start する。
        // (sinfohub-server の OOM kill 修正の本筋。)
        let c = BucketCylinder::new(10, 16_000_000);
        if let BucketPositions::Heap { entries, .. } = &c.positions {
            assert_eq!(entries.len(), 0);
        } else {
            panic!("default ctor は Heap mode");
        }
    }

    #[test]
    fn remove_on_empty_positions_no_panic() {
        let mut c = BucketCylinder::new(10, 0);
        assert_eq!(c.remove(0), UniqueDelta::NONE);
        assert_eq!(c.remove(999_999), UniqueDelta::NONE);
        assert_eq!(c.total(), 0);
        assert_eq!(c.unique_count(), 0);
    }

    #[test]
    fn insert_at_large_eid_with_no_hint() {
        // max_entities = 0 ヒントでも、 大きい eid で insert すると
        // ensure_positions が伸ばして安全に書ける。
        let mut c = BucketCylinder::new(10, 0);
        c.insert(1_000_000, 3);
        assert_eq!(c.slice_one(3), &[1_000_000]);
        assert_eq!(c.total(), 1);
        // 中間 eid (insert されてない) を remove しても no panic。
        assert_eq!(c.remove(500_000), UniqueDelta::NONE);
    }

}
