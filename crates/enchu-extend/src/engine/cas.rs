//! CASStore — コンテンツアドレッサブルストレージ
//!
//! 不変ブロック + ハッシュ参照。同じ内容は1箇所だけ保持。
//! 参照カウントで GC。commit/rollback の基盤。

use std::collections::HashMap;
use std::hash::{Hash, Hasher, DefaultHasher};

/// ブロックのハッシュ（64bit）
pub type BlockHash = u64;

/// 空ブロックのハッシュ（定数）
pub const EMPTY_HASH: BlockHash = 0;

/// ブロックの内容からハッシュを計算
#[inline]
pub fn hash_block(data: &[u32]) -> BlockHash {
    if data.is_empty() { return EMPTY_HASH; }
    let mut h = DefaultHasher::new();
    data.hash(&mut h);
    h.finish()
}

/// CAS エントリ: 参照カウント付きブロック
struct Entry {
    data: Vec<u32>,
    refs: u32,
}

/// コンテンツアドレッサブルストレージ
pub struct CASStore {
    blocks: HashMap<BlockHash, Entry>,
}

impl CASStore {
    pub fn new() -> Self {
        let mut store = CASStore { blocks: HashMap::new() };
        // 空ブロックを初期登録（参照カウント1で永続）
        store.blocks.insert(EMPTY_HASH, Entry { data: vec![], refs: 1 });
        store
    }

    /// ブロックを登録。既存なら参照カウント増加。ハッシュを返す。
    pub fn insert(&mut self, data: Vec<u32>) -> BlockHash {
        let h = hash_block(&data);
        if let Some(entry) = self.blocks.get_mut(&h) {
            entry.refs += 1;
            return h;
        }
        self.blocks.insert(h, Entry { data, refs: 1 });
        h
    }

    /// ハッシュからブロックを取得
    #[inline]
    pub fn get(&self, hash: BlockHash) -> &[u32] {
        match self.blocks.get(&hash) {
            Some(entry) => &entry.data,
            None => &[],
        }
    }

    /// 参照カウント増加
    pub fn inc_ref(&mut self, hash: BlockHash) {
        if let Some(entry) = self.blocks.get_mut(&hash) {
            entry.refs += 1;
        }
    }

    /// 参照カウント減少。0 になったら削除。
    /// 返り値: 削除されたか
    pub fn dec_ref(&mut self, hash: BlockHash) -> bool {
        if hash == EMPTY_HASH { return false; } // 空ブロックは消さない
        if let Some(entry) = self.blocks.get_mut(&hash) {
            entry.refs = entry.refs.saturating_sub(1);
            if entry.refs == 0 {
                self.blocks.remove(&hash);
                return true;
            }
        }
        false
    }

    /// 参照カウント取得
    pub fn ref_count(&self, hash: BlockHash) -> u32 {
        self.blocks.get(&hash).map(|e| e.refs).unwrap_or(0)
    }

    /// ブロック数（空ブロック含む）
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    /// 全ブロックの合計 entity 数
    pub fn total_entities(&self) -> usize {
        self.blocks.values().map(|e| e.data.len()).sum()
    }

    /// メモリ使用量（概算）
    pub fn memory_bytes(&self) -> usize {
        self.blocks.values().map(|e| e.data.len() * 4 + 32).sum::<usize>()
            + self.blocks.len() * 40 // HashMap overhead
    }

    /// ブロックの内容から entity を追加した新ブロックを作成して登録。
    /// 旧ブロックの参照カウントは変更しない（呼び出し側で管理）。
    pub fn insert_with_add(&mut self, base_hash: BlockHash, eid: u32) -> BlockHash {
        let mut data = self.get(base_hash).to_vec();
        match data.binary_search(&eid) {
            Ok(_) => return base_hash, // 既にある
            Err(pos) => data.insert(pos, eid),
        }
        self.insert(data)
    }

    /// ブロックの内容から entity を除去した新ブロックを作成して登録。
    /// 旧ブロックの参照カウントは変更しない（呼び出し側で管理）。
    pub fn insert_with_remove(&mut self, base_hash: BlockHash, eid: u32) -> BlockHash {
        let data: Vec<u32> = self.get(base_hash).iter()
            .copied()
            .filter(|&e| e != eid)
            .collect();
        self.insert(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_block() {
        let store = CASStore::new();
        assert_eq!(store.get(EMPTY_HASH), &[] as &[u32]);
        assert_eq!(store.block_count(), 1);
    }

    #[test]
    fn insert_and_get() {
        let mut store = CASStore::new();
        let h = store.insert(vec![1, 2, 3]);
        assert_eq!(store.get(h), &[1, 2, 3]);
        assert_eq!(store.ref_count(h), 1);
    }

    #[test]
    fn dedup() {
        let mut store = CASStore::new();
        let h1 = store.insert(vec![1, 2, 3]);
        let h2 = store.insert(vec![1, 2, 3]);
        assert_eq!(h1, h2);
        assert_eq!(store.ref_count(h1), 2);
        assert_eq!(store.block_count(), 2); // empty + [1,2,3]
    }

    #[test]
    fn ref_counting() {
        let mut store = CASStore::new();
        let h = store.insert(vec![10, 20]);
        assert_eq!(store.ref_count(h), 1);

        store.inc_ref(h);
        assert_eq!(store.ref_count(h), 2);

        store.dec_ref(h);
        assert_eq!(store.ref_count(h), 1);
        assert_eq!(store.block_count(), 2);

        store.dec_ref(h);
        assert_eq!(store.ref_count(h), 0);
        assert_eq!(store.block_count(), 1); // empty のみ
    }

    #[test]
    fn empty_never_deleted() {
        let mut store = CASStore::new();
        store.dec_ref(EMPTY_HASH);
        store.dec_ref(EMPTY_HASH);
        assert_eq!(store.get(EMPTY_HASH), &[] as &[u32]);
        assert_eq!(store.block_count(), 1);
    }

    #[test]
    fn insert_with_add_and_remove() {
        let mut store = CASStore::new();
        let h1 = store.insert(vec![1, 3, 5]);

        // add 4
        let h2 = store.insert_with_add(h1, 4);
        assert_eq!(store.get(h2), &[1, 3, 4, 5]);
        assert_ne!(h1, h2);

        // add existing
        let h3 = store.insert_with_add(h2, 3);
        assert_eq!(h3, h2); // 変わらない

        // remove 3
        let h4 = store.insert_with_remove(h2, 3);
        assert_eq!(store.get(h4), &[1, 4, 5]);

        // remove non-existent
        let h5 = store.insert_with_remove(h4, 99);
        assert_eq!(store.get(h5), &[1, 4, 5]);
    }

    #[test]
    fn dedup_after_add_remove() {
        let mut store = CASStore::new();
        let h1 = store.insert(vec![1, 2, 3]);
        let h2 = store.insert_with_remove(h1, 2);
        let h3 = store.insert(vec![1, 3]);
        // h2 と h3 は同じ内容だから同じハッシュ
        assert_eq!(h2, h3);
        assert_eq!(store.ref_count(h2), 2);
    }
}
