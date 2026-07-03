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
    /// #76 (0.9.0): 翻訳先として払い出された local eid の逆引き set。
    /// 「この local は foreign entity のレプリカか」を O(1) で判定するために持つ。
    /// レプリカへのローカル write-back は逆写像が無いため他 peer で別 entity に
    /// 断片化する — bridge 側 guard がこれを見て伝搬を止める。
    translated_locals: RwLock<std::collections::HashSet<u32>>,
}

impl Default for EidTranslator {
    fn default() -> Self {
        Self::new()
    }
}

impl EidTranslator {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            translated_locals: RwLock::new(std::collections::HashSet::new()),
        }
    }

    /// 既存の写像を引く。 未登録なら None。
    pub fn get(&self, author_peer: PeerId, foreign_local: u32) -> Option<u32> {
        let guard = self.inner.read().unwrap();
        guard.get(&(author_peer, foreign_local)).copied()
    }

    /// #76: local eid が foreign entity の翻訳先 (= レプリカ) かどうか。
    pub fn is_translated_local(&self, local: u32) -> bool {
        self.translated_locals.read().unwrap().contains(&local)
    }

    /// 写像を登録 (上書き)。 recovery 復元時にも使う。
    pub fn insert(&self, author_peer: PeerId, foreign_local: u32, local: u32) {
        let mut guard = self.inner.write().unwrap();
        guard.insert((author_peer, foreign_local), local);
        drop(guard);
        self.translated_locals.write().unwrap().insert(local);
    }

    /// 写像を **atomic** に get-or-insert する。 未登録なら `alloc` を呼んで local を
    /// 確保し、 write lock 下で再 check してから insert する。 これで並行 apply が同じ
    /// foreign entity を解決しても double-alloc / orphan が起きない (= 全 caller が同じ
    /// local を得る)。
    ///
    /// `alloc` が `None` を返したら (= 確保先 table を導けない table-less な op)、 insert
    /// せず `None` を返す → 呼び出し側はその op を skip する。
    ///
    /// 注意: `alloc` は **write lock 保持中** に呼ばれる。 `RwLock` は reentrant でない
    /// ので、 translator 自身を触る closure を渡してはならない (`alloc` = entity 確保は
    /// table lock しか取らないので安全)。
    pub fn get_or_insert_with(
        &self,
        author_peer: PeerId,
        foreign_local: u32,
        alloc: impl FnOnce() -> Option<u32>,
    ) -> Option<u32> {
        // fast path: 既に登録済みなら read lock だけで返す。
        {
            let guard = self.inner.read().unwrap();
            if let Some(&local) = guard.get(&(author_peer, foreign_local)) {
                return Some(local);
            }
        }
        // slow path: write lock 下で再 check (= double-checked) → 確保 → insert。
        let mut guard = self.inner.write().unwrap();
        if let Some(&local) = guard.get(&(author_peer, foreign_local)) {
            return Some(local); // 別 thread が先に確保した
        }
        let local = alloc()?;
        guard.insert((author_peer, foreign_local), local);
        drop(guard);
        self.translated_locals.write().unwrap().insert(local);
        Some(local)
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 全 entry の snapshot (sidecar 永続化用)。 各要素 = (author_peer, foreign_local, local)。
    pub fn snapshot(&self) -> Vec<(PeerId, u32, u32)> {
        let guard = self.inner.read().unwrap();
        guard
            .iter()
            .map(|(&(peer, foreign_local), &local)| (peer, foreign_local, local))
            .collect()
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
