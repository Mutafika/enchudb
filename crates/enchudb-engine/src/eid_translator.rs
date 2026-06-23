//! EidTranslator — maps a foreign peer's EntityId to a local one (#9).
//!
//! # なぜ必要か
//!
//! EntityId は **peer ごとの空間**。 peer A の `eid=42` と peer B の `eid=42` は
//! 別 entity。 受信側 engine が foreign eid をそのまま local slot として apply
//! すると、 衝突した瞬間に自分の entity をサイレント上書きする (issue #9)。
//!
//! `EidTranslator` は `(author_peer, foreign_local) → local_eid` の写像を持ち、
//! 受信 op を apply する前に foreign eid を「自分の eid 空間の fresh な local」に
//! 翻訳する。 これで A の entity と B の entity が同じ local slot に落ちても
//! 衝突しない。
//!
//! - 初見の `(author_peer, foreign_local)` → 新しい local eid を払い出して登録
//! - 既知の組 → 既存 local eid に dispatch
//! - `author_peer == self.peer_id` なら identity (翻訳不要、 呼び出し側で判定)
//!
//! # 永続化
//!
//! commit 1 では in-memory のみ。 再起動で写像が消えると、 同じ foreign entity の
//! 再 sync が新しい local eid を払い出して **重複** するため、 後続 commit で
//! oplog (`Op::EidMap`) 経由の永続化 + recovery 復元を入れる。

use std::collections::HashMap;
use std::sync::RwLock;

use enchudb_oplog::PeerId;

/// `(author_peer, foreign_local)` を key にした foreign→local 写像。
type Key = (PeerId, u32);

/// foreign eid → local eid の翻訳テーブル。
pub struct EidTranslator {
    inner: RwLock<HashMap<Key, u32>>,
}

impl Default for EidTranslator {
    fn default() -> Self {
        Self::new()
    }
}

impl EidTranslator {
    pub fn new() -> Self {
        Self { inner: RwLock::new(HashMap::new()) }
    }

    /// 既存の写像を引く。 未登録なら None。
    pub fn get(&self, author_peer: PeerId, foreign_local: u32) -> Option<u32> {
        let guard = self.inner.read().unwrap();
        guard.get(&(author_peer, foreign_local)).copied()
    }

    /// 写像を登録 (上書き)。 recovery 復元時にも使う。
    pub fn insert(&self, author_peer: PeerId, foreign_local: u32, local: u32) {
        let mut guard = self.inner.write().unwrap();
        guard.insert((author_peer, foreign_local), local);
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_returns_none_for_unknown() {
        let t = EidTranslator::new();
        assert_eq!(t.get(1, 42), None);
    }

    #[test]
    fn insert_then_get_roundtrips() {
        let t = EidTranslator::new();
        t.insert(1, 42, 7);
        assert_eq!(t.get(1, 42), Some(7));
        // 別 peer の同じ foreign_local は別 entry
        assert_eq!(t.get(2, 42), None);
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn insert_overwrites_same_key() {
        let t = EidTranslator::new();
        t.insert(1, 42, 7);
        t.insert(1, 42, 9);
        assert_eq!(t.get(1, 42), Some(9));
        assert_eq!(t.len(), 1);
    }
}
