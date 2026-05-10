/// bigram key → entity ID リストの逆引きインデックス。
/// BucketCylinder と同じ発想: key ごとに Vec<u64> を持つ。
///
/// key は u32（bigram の 2 文字を pack）、eid は u64（EnchuDB v32+ の entity ID）。
use std::collections::HashMap;

pub struct PostingList {
    /// bigram key → ソート済み entity ID リスト
    buckets: HashMap<u32, Vec<u64>>,
}

impl PostingList {
    pub fn new() -> Self {
        Self { buckets: HashMap::new() }
    }

    /// entity を追加。重複は許容（後で dedup）。
    pub fn add(&mut self, key: u32, eid: u64) {
        self.buckets.entry(key).or_default().push(eid);
    }

    /// entity を削除。
    pub fn remove(&mut self, key: u32, eid: u64) {
        if let Some(list) = self.buckets.get_mut(&key) {
            list.retain(|&e| e != eid);
            if list.is_empty() {
                self.buckets.remove(&key);
            }
        }
    }

    /// 指定 key の entity リスト。
    pub fn get(&self, key: u32) -> &[u64] {
        self.buckets.get(&key).map_or(&[], |v| v.as_slice())
    }

    /// 複数 key の AND（共通 entity）。最小リストから開始して絞り込む。
    pub fn intersect(&self, keys: &[u32]) -> Vec<u64> {
        if keys.is_empty() { return vec![]; }

        // 最小の posting list を起点にする
        let mut shortest_idx = 0;
        let mut shortest_len = usize::MAX;
        for (i, &key) in keys.iter().enumerate() {
            let len = self.get(key).len();
            if len == 0 { return vec![]; }
            if len < shortest_len {
                shortest_len = len;
                shortest_idx = i;
            }
        }

        let mut result: Vec<u64> = self.get(keys[shortest_idx]).to_vec();
        result.sort_unstable();
        result.dedup();

        for (i, &key) in keys.iter().enumerate() {
            if i == shortest_idx { continue; }
            let posting = self.get(key);
            let mut set: Vec<u64> = posting.to_vec();
            set.sort_unstable();
            set.dedup();
            result.retain(|eid| set.binary_search(eid).is_ok());
            if result.is_empty() { return vec![]; }
        }

        result
    }

    /// 全 posting を dedup + sort。bulk insert 後に呼ぶ。
    pub fn compact(&mut self) {
        for list in self.buckets.values_mut() {
            list.sort_unstable();
            list.dedup();
        }
    }

    /// 内部データへのアクセス（保存用）
    pub fn raw(&self) -> &HashMap<u32, Vec<u64>> { &self.buckets }

    /// 統計
    pub fn key_count(&self) -> usize { self.buckets.len() }
    pub fn total_entries(&self) -> usize { self.buckets.values().map(|v| v.len()).sum() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_get() {
        let mut pl = PostingList::new();
        pl.add(100, 1);
        pl.add(100, 2);
        pl.add(200, 2);
        pl.add(200, 3);
        pl.compact();

        assert_eq!(pl.get(100), &[1u64, 2]);
        assert_eq!(pl.get(200), &[2u64, 3]);
        assert_eq!(pl.get(999), &[] as &[u64]);
    }

    #[test]
    fn intersect_basic() {
        let mut pl = PostingList::new();
        pl.add(100, 1);
        pl.add(100, 2);
        pl.add(100, 3);
        pl.add(200, 2);
        pl.add(200, 3);
        pl.add(200, 4);
        pl.compact();

        let result = pl.intersect(&[100, 200]);
        assert_eq!(result, vec![2u64, 3]);
    }

    #[test]
    fn intersect_empty() {
        let mut pl = PostingList::new();
        pl.add(100, 1);
        pl.compact();

        assert_eq!(pl.intersect(&[100, 200]), Vec::<u64>::new());
        assert_eq!(pl.intersect(&[]), Vec::<u64>::new());
    }

    #[test]
    fn remove_entity() {
        let mut pl = PostingList::new();
        pl.add(100, 1);
        pl.add(100, 2);
        pl.compact();

        pl.remove(100, 1);
        assert_eq!(pl.get(100), &[2u64]);

        pl.remove(100, 2);
        assert_eq!(pl.get(100), &[] as &[u64]);
    }

    #[test]
    fn wide_eid() {
        // u32 を超える eid（v32 layout: [peer_id: 32][local_id: 32]）でも動く
        let mut pl = PostingList::new();
        let peer1_local0 = 1u64 << 32;
        let peer1_local1 = (1u64 << 32) | 1;
        let peer2_local0 = 2u64 << 32;
        pl.add(100, peer1_local0);
        pl.add(100, peer1_local1);
        pl.add(100, peer2_local0);
        pl.compact();
        assert_eq!(pl.get(100), &[peer1_local0, peer1_local1, peer2_local0]);
    }
}
