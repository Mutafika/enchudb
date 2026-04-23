//! BucketCylinder (v27) — per-value bucket、ソート不要、rebuild 不要。
//!
//! 設計:
//!   buckets[value] = entity ids(追加順、未ソート)。slice 直返し可能。
//!   positions[eid] = (value+1, idx_in_bucket)。0 = 未 tie。
//!
//! 計算量:
//!   insert: O(1) push(初見バケットは resize 分だけ O(k))
//!   remove: O(1) swap_remove(末尾と入れ替えて positions 更新)
//!   slice_one: O(1) スライス直返し
//!   rebuild_from_column: open 時の初期構築。O(n)。
//!
//! max_values の扱い:
//!   max_values は「初期 bucket サイズのヒント」。値の上限ではない。
//!   tie に渡された value が現在の buckets.len() を超える場合は
//!   `ensure_bucket` で動的に resize する(silent clamp なし)。
//!   0 を渡した場合は「ヒントなし」。最初の tie で必要サイズまで伸びる。
//!
//! 並行性:
//!   段階 2 では &mut self 前提(単一スレッド)。HimoStore 段階で排他を被せる。

use super::column::Column;

pub struct BucketCylinder {
    max_values: u32,
    buckets: Vec<Vec<u32>>,
    /// eid → (value+1, idx_in_bucket)。value+1 == 0 なら未 tie。
    positions: Vec<(u32, u32)>,
    total: u32,
}

/// insert / remove が unique 値カウントに与えた影響。
/// HimoStore が AtomicU32 に反映する。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UniqueDelta {
    /// 新しい unique 値が増えたバケット数(0 or 1)。
    pub added: u32,
    /// 空になって消えた unique 値数(0, 1, or 2)。
    pub removed: u32,
}

impl UniqueDelta {
    pub const NONE: Self = Self { added: 0, removed: 0 };
}

impl BucketCylinder {
    pub fn new(max_values: u32, max_entities: u32) -> Self {
        // max_values は初期サイズのヒント。0 なら空で始めて必要時に伸ばす。
        let bucket_count = if max_values == 0 { 0 } else { max_values as usize + 1 };
        Self {
            max_values,
            buckets: (0..bucket_count).map(|_| Vec::new()).collect(),
            positions: vec![(0, 0); max_entities as usize],
            total: 0,
        }
    }

    /// value を eid に紐づける。既存エントリがあれば置き換え。
    /// unique 値数の変化を UniqueDelta で返す。
    pub fn insert(&mut self, eid: u32, value: u32) -> UniqueDelta {
        self.ensure_positions(eid);

        let mut delta = UniqueDelta::NONE;

        let (old_vp1, old_idx) = self.positions[eid as usize];
        if old_vp1 != 0 {
            let old_v = old_vp1 - 1;
            let was_last = self.buckets[old_v as usize].len() == 1;
            self.swap_remove_at(old_v, old_idx);
            if was_last {
                delta.removed += 1;
            }
        }

        self.ensure_bucket(value);
        let bi = value as usize;
        let was_empty = self.buckets[bi].is_empty();
        let idx = self.buckets[bi].len() as u32;
        self.buckets[bi].push(eid);
        if was_empty {
            delta.added += 1;
        }
        self.positions[eid as usize] = (value + 1, idx);
        if old_vp1 == 0 {
            self.total += 1;
        }

        delta
    }

    /// eid の紐を外す。unique 値数の変化を UniqueDelta で返す。
    pub fn remove(&mut self, eid: u32) -> UniqueDelta {
        if (eid as usize) >= self.positions.len() {
            return UniqueDelta::NONE;
        }
        let (vp1, idx) = self.positions[eid as usize];
        if vp1 == 0 {
            return UniqueDelta::NONE;
        }
        let v = vp1 - 1;
        let was_last = self.buckets[v as usize].len() == 1;
        self.swap_remove_at(v, idx);
        self.positions[eid as usize] = (0, 0);
        self.total -= 1;
        UniqueDelta {
            added: 0,
            removed: if was_last { 1 } else { 0 },
        }
    }

    /// value ちょうどの entity を返す(slice、ゼロコピー)。
    /// 範囲外(bucket が未作成)なら空 slice。
    #[inline]
    pub fn slice_one(&self, value: u32) -> &[u32] {
        match self.bucket_index(value) {
            Some(bi) => &self.buckets[bi],
            None => &[],
        }
    }

    pub fn total(&self) -> usize {
        self.total as usize
    }

    pub fn max_values(&self) -> u32 {
        self.max_values
    }

    /// 現在の unique 値数(非空バケット数)。
    /// 通常は HimoStore の AtomicU32 を使うが、デバッグ/検証用に直接数える。
    pub fn unique_count(&self) -> u32 {
        self.buckets.iter().filter(|b| !b.is_empty()).count() as u32
    }

    /// 入っている値を列挙(順序は保証しない)。
    pub fn unique_values(&self) -> Vec<u32> {
        self.buckets
            .iter()
            .enumerate()
            .filter_map(|(v, b)| if b.is_empty() { None } else { Some(v as u32) })
            .collect()
    }

    /// range は全幅探索。low..=high の各バケットを順に iter として返す。
    /// bucket が未作成の範囲は空としてスキップされる。
    pub fn range_iter(&self, low: u32, high: u32) -> impl Iterator<Item = &u32> {
        let lo = low as usize;
        let hi = (high as usize).min(self.buckets.len().saturating_sub(1));
        let buckets = if lo >= self.buckets.len() || lo > hi {
            &self.buckets[0..0]
        } else {
            &self.buckets[lo..=hi]
        };
        buckets.iter().flatten()
    }

    /// Column から全再構築。open 時に呼ぶ。
    pub fn rebuild_from_column(&mut self, column: &Column) {
        for b in &mut self.buckets {
            b.clear();
        }
        for p in &mut self.positions {
            *p = (0, 0);
        }
        self.total = 0;

        let count = column.count();
        self.ensure_positions(count.saturating_sub(1));

        for eid in 0..count {
            let bytes: [u8; 4] = column.get(eid).try_into().unwrap();
            let stored = u32::from_le_bytes(bytes);
            if stored != 0 {
                let value = stored - 1;
                self.ensure_bucket(value);
                let bi = value as usize;
                let idx = self.buckets[bi].len() as u32;
                self.buckets[bi].push(eid);
                self.positions[eid as usize] = (value + 1, idx);
                self.total += 1;
            }
        }
    }

    /// 指定 value のバケットが存在するよう buckets を resize。
    /// 既に存在するならなにもしない。silent clamp の代わり。
    #[inline]
    fn ensure_bucket(&mut self, value: u32) {
        if (value as usize) >= self.buckets.len() {
            self.buckets.resize(value as usize + 1, Vec::new());
        }
    }

    #[inline]
    fn bucket_index(&self, value: u32) -> Option<usize> {
        let bi = value as usize;
        if bi < self.buckets.len() { Some(bi) } else { None }
    }

    fn ensure_positions(&mut self, eid: u32) {
        if (eid as usize) >= self.positions.len() {
            self.positions.resize(eid as usize + 1, (0, 0));
        }
    }

    /// bucket[value] の idx 位置を swap_remove。末尾と入れ替えた entity の
    /// positions を更新。
    fn swap_remove_at(&mut self, value: u32, idx: u32) {
        let bi = value as usize;
        let bucket = &mut self.buckets[bi];
        let last = bucket.len() - 1;
        if (idx as usize) != last {
            let moved_eid = bucket[last];
            bucket.swap_remove(idx as usize);
            self.positions[moved_eid as usize].1 = idx;
        } else {
            bucket.pop();
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
        // max_values=10 で始めても、value=1000 を tie すれば裏で拡張される。
        let mut c = BucketCylinder::new(10, 100);
        c.insert(1, 1000);
        c.insert(2, 10);
        assert_eq!(c.slice_one(1000), &[1]);
        assert_eq!(c.slice_one(10), &[2]);
        // 未使用バケットは空
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
        // 初回 insert → added=1
        assert_eq!(c.insert(1, 3), UniqueDelta { added: 1, removed: 0 });
        // 同じ bucket → added=0
        assert_eq!(c.insert(2, 3), UniqueDelta::NONE);
        // 別 bucket → added=1
        assert_eq!(c.insert(3, 5), UniqueDelta { added: 1, removed: 0 });
        // remove → bucket はまだ空じゃない
        assert_eq!(c.remove(1), UniqueDelta::NONE);
        // 最後の eid を remove → bucket 空に
        assert_eq!(c.remove(2), UniqueDelta { added: 0, removed: 1 });
    }

    #[test]
    fn unique_delta_on_replace() {
        let mut c = BucketCylinder::new(10, 100);
        c.insert(1, 3);
        c.insert(2, 3);
        // 値を 3 → 7 に置換。bucket 3 はまだ残る(eid=2)が、bucket 7 は新規。
        assert_eq!(c.insert(1, 7), UniqueDelta { added: 1, removed: 0 });
        // 最後の bucket 3 も置換。bucket 3 が消えて、bucket 9 が生まれる。
        assert_eq!(c.insert(2, 9), UniqueDelta { added: 1, removed: 1 });
    }

    #[test]
    fn zero_max_values_works() {
        // 初期サイズ 0 でも tie で伸びる。
        let mut c = BucketCylinder::new(0, 100);
        c.insert(1, 42);
        assert_eq!(c.slice_one(42), &[1]);
        assert_eq!(c.slice_one(0), &[] as &[u32]);
    }
}
