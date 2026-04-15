//! BucketCylinder (v27) — per-value bucket、ソート不要、rebuild 不要。
//!
//! 設計:
//!   buckets[value] = entity ids(追加順、未ソート)。slice 直返し可能。
//!   positions[eid] = (value+1, idx_in_bucket)。0 = 未 tie。
//!
//! 計算量:
//!   insert: O(1) push
//!   remove: O(1) swap_remove(末尾と入れ替えて positions 更新)
//!   slice_one: O(1) スライス直返し
//!   rebuild_from_column: open 時の初期構築。O(n)。
//!
//! 並行性:
//!   段階 2 では &mut self 前提(単一スレッド)。HimoStore 段階で排他を被せる。

use crate::column::Column;

pub struct BucketCylinder {
    max_values: u32,
    buckets: Vec<Vec<u32>>,
    /// eid → (value+1, idx_in_bucket)。value+1 == 0 なら未 tie。
    positions: Vec<(u32, u32)>,
    total: u32,
}

impl BucketCylinder {
    pub fn new(max_values: u32, max_entities: u32) -> Self {
        let bucket_count = (max_values as usize).saturating_add(1);
        Self {
            max_values,
            buckets: (0..bucket_count).map(|_| Vec::new()).collect(),
            positions: vec![(0, 0); max_entities as usize],
            total: 0,
        }
    }

    /// value を eid に紐づける。既存エントリがあれば置き換え。
    pub fn insert(&mut self, eid: u32, value: u32) {
        self.ensure_positions(eid);

        let (old_vp1, old_idx) = self.positions[eid as usize];
        if old_vp1 != 0 {
            self.swap_remove_at(old_vp1 - 1, old_idx);
        }

        let bi = self.bucket_index(value);
        let idx = self.buckets[bi].len() as u32;
        self.buckets[bi].push(eid);
        self.positions[eid as usize] = (value + 1, idx);
        if old_vp1 == 0 {
            self.total += 1;
        }
    }

    /// eid の紐を外す。
    pub fn remove(&mut self, eid: u32) {
        if (eid as usize) >= self.positions.len() {
            return;
        }
        let (vp1, idx) = self.positions[eid as usize];
        if vp1 == 0 {
            return;
        }
        self.swap_remove_at(vp1 - 1, idx);
        self.positions[eid as usize] = (0, 0);
        self.total -= 1;
    }

    /// value ちょうどの entity を返す(slice、ゼロコピー)。
    #[inline]
    pub fn slice_one(&self, value: u32) -> &[u32] {
        let bi = self.bucket_index(value);
        &self.buckets[bi]
    }

    pub fn total(&self) -> usize {
        self.total as usize
    }

    pub fn max_values(&self) -> u32 {
        self.max_values
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
    pub fn range_iter(&self, low: u32, high: u32) -> impl Iterator<Item = &u32> {
        let lo = self.bucket_index(low);
        let hi = self.bucket_index(high);
        self.buckets[lo..=hi].iter().flatten()
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
                let bi = self.bucket_index(value);
                let idx = self.buckets[bi].len() as u32;
                self.buckets[bi].push(eid);
                self.positions[eid as usize] = (value + 1, idx);
                self.total += 1;
            }
        }
    }

    #[inline]
    fn bucket_index(&self, value: u32) -> usize {
        (value as usize).min(self.buckets.len() - 1)
    }

    fn ensure_positions(&mut self, eid: u32) {
        if (eid as usize) >= self.positions.len() {
            self.positions.resize(eid as usize + 1, (0, 0));
        }
    }

    /// bucket[value] の idx 位置を swap_remove。末尾と入れ替えた entity の
    /// positions を更新。
    fn swap_remove_at(&mut self, value: u32, idx: u32) {
        let bi = self.bucket_index(value);
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
    fn value_clamp_at_max() {
        let mut c = BucketCylinder::new(10, 100);
        c.insert(1, 100);
        c.insert(2, 10);
        assert!(c.slice_one(10).len() >= 1);
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
}
