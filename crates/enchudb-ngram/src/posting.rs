/// n-gram key → entity ID リストの逆引きインデックス。
/// BucketCylinder と同じ発想: key ごとに Vec<u64> を持つ。
///
/// key は u64（gram の n 文字を 16bit ずつ pack、#121）、eid も u64（EnchuDB の
/// [peer|local] 合成 entity ID）。n = 2 の key は上位 32bit が 0 で、旧 u32 key と同じビット列。
///
/// list は**書く時に昇順（広義）に保つ**: 後ろに足す eid が最後以上ならそのまま（doc を eid の昇順に登録する普通の場合。
/// 1 つの doc に同じ gram が何度も出ると同じ eid が隣に並ぶ — 重複は `compact` と AND の種で落とす）。最後より小さい eid を
/// 足した list だけ `unsorted` に印を付け、`compact` で並べて印を消す（真ん中に差し込まない: 順の崩れた一括登録で list の
/// 長さ分ずつ動かさない）。AND（[`intersect`](Self::intersect)）は印の無い list を写さずに跳びながら探す。
/// 足す時に重複を落とす形（最後と同じなら足さない）は、doc の中の同じ gram で分かれ道の予測が外れて登録が 1.17 倍になった。
use std::collections::{HashMap, HashSet};

pub struct PostingList {
    /// gram key → entity ID リスト（`unsorted` に無い key は昇順 (広義)、`compact` の後は重複なし）
    buckets: HashMap<u64, Vec<u64>>,
    /// 最後より小さい eid を足して、昇順でなくなった list の key
    unsorted: HashSet<u64>,
}

impl PostingList {
    pub fn new() -> Self {
        Self { buckets: HashMap::new(), unsorted: HashSet::new() }
    }

    /// entity を追加。重複は許容（`compact` と AND の種で落とす）。最後より小さい eid を足した list は順が崩れた印を付ける
    /// （`compact` で並べる）。
    pub fn add(&mut self, key: u64, eid: u64) {
        let list = self.buckets.entry(key).or_default();
        if list.last().is_some_and(|&last| last > eid) {
            self.unsorted.insert(key);
        }
        list.push(eid);
    }

    /// entity を削除。
    pub fn remove(&mut self, key: u64, eid: u64) {
        if let Some(list) = self.buckets.get_mut(&key) {
            list.retain(|&e| e != eid);
            if list.is_empty() {
                self.buckets.remove(&key);
                self.unsorted.remove(&key);
            }
        }
    }

    /// 指定 key の entity リスト。
    pub fn get(&self, key: u64) -> &[u64] {
        self.buckets.get(&key).map_or(&[], |v| v.as_slice())
    }

    /// 複数 key の AND（共通 entity、昇順・重複なし）。一番短い list を写して種にし（隣の重複を落とす）、残りの list は種の eid の
    /// 順に跳びながら探す（昇順の list は写さない。順の崩れた印の付いた list だけ、写して並べてから探す）。
    pub fn intersect(&self, keys: &[u64]) -> Vec<u64> {
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

        let mut result: Vec<u64> = self.sorted(keys[shortest_idx]).into_owned();
        result.dedup();
        for (i, &key) in keys.iter().enumerate() {
            if i == shortest_idx { continue; }
            retain_in(&mut result, &self.sorted(key));
            if result.is_empty() { return vec![]; }
        }

        result
    }

    /// key の list を昇順で（印の無い list は写さない。印の付いた list は写して並べ、重複も落とす）。
    fn sorted(&self, key: u64) -> std::borrow::Cow<'_, [u64]> {
        let list = self.get(key);
        if self.unsorted.contains(&key) {
            let mut v = list.to_vec();
            v.sort_unstable();
            v.dedup();
            std::borrow::Cow::Owned(v)
        } else {
            std::borrow::Cow::Borrowed(list)
        }
    }

    /// 全 posting を dedup + sort（並んでいる list は並んでいるかを見るだけ）。bulk insert 後と書き出す前に呼ぶ。
    pub fn compact(&mut self) {
        for list in self.buckets.values_mut() {
            list.sort_unstable();
            list.dedup();
        }
        self.unsorted.clear();
    }

    /// 内部データへのアクセス（保存用）
    pub fn raw(&self) -> &HashMap<u64, Vec<u64>> { &self.buckets }

    /// 統計
    pub fn key_count(&self) -> usize { self.buckets.len() }
}

/// 昇順・重複なしの `result` を、昇順 (広義) の `list` にもある eid だけに絞る。`list` は `result` の順に跳びながら探す
/// （`list` の位置は持ち越すので、`result` が短ければ `list` の長さでなく `result` の長さ × log に比例）。
pub(crate) fn retain_in(result: &mut Vec<u64>, list: &[u64]) {
    let mut at = 0;
    result.retain(|&e| {
        at = gallop_ge(list, at, e);
        at < list.len() && list[at] == e
    });
}

/// 昇順の `s` の `from` 以降で、`e` 以上の最初の位置（無ければ `s.len()`）。1, 2, 4, .. と跳んで越えたら、その間を二分探索する。
pub(crate) fn gallop_ge(s: &[u64], from: usize, e: u64) -> usize {
    gallop_ge_by(s.len(), from, e, |i| s[i])
}

/// [`gallop_ge`] の、`i` 番目を `at(i)` で読む版（mmap の posting を 1 件ずつ読む時）。
pub(crate) fn gallop_ge_by(len: usize, from: usize, e: u64, at: impl Fn(usize) -> u64) -> usize {
    let (mut lo, mut i, mut step) = (from, from, 1);
    while i < len && at(i) < e {
        lo = i + 1;
        i += step;
        step <<= 1;
    }
    let (mut a, mut b) = (lo, i.min(len));
    while a < b {
        let mid = a + (b - a) / 2;
        if at(mid) < e { a = mid + 1; } else { b = mid; }
    }
    a
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
    fn wide_key() {
        // n >= 3 の gram key は 32bit を超える (16bit × 3 = 48bit)。#121
        let mut pl = PostingList::new();
        let k3 = crate::gram::extract_keys("国民は", 3)[0];
        let k4 = crate::gram::extract_keys("国民は法", 4)[0];
        assert!(k3 > u32::MAX as u64, "n=3 key が u32 に収まってしまっている");
        pl.add(k3, 7);
        pl.add(k4, 8);
        pl.compact();
        assert_eq!(pl.get(k3), &[7u64]);
        assert_eq!(pl.get(k4), &[8u64]);
        assert_eq!(pl.key_count(), 2, "u32 に潰れて衝突していない");
    }

    #[test]
    fn wide_eid() {
        // u32 を超える eid（[peer_id: 32][local_id: 32] layout）でも動く
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
