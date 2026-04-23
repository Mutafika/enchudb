//! ACL — project 単位の書き込み権限を紐で表現する。
//!
//! # 設計
//!
//! Project 自体を 1 つの entity として扱い、`acl_writer` 紐で
//! 許可 peer を複数張る。
//!
//! ```
//! # use enchudb::{Engine, HimoType};
//! let path = format!("/tmp/enchudb-acl-doc-{}.db", std::process::id());
//! # let _ = std::fs::remove_file(&path);
//! let mut eng = Engine::create_standalone(&path).unwrap();
//! eng.define_himo("acl_writer", HimoType::Value, 4);
//! let project = eng.entity();
//! eng.tie(project, "acl_writer", 1);  // peer 1 に書き込み許可
//! eng.tie(project, "acl_writer", 2);  // peer 2 にも
//! # let _ = std::fs::remove_file(&path);
//! ```
//!
//! Syncer は受信 op の `author_peer` が project の `acl_writer` に入っているか
//! 確認する。入っていなければ reject。
//!
//! # Phase C の範囲
//!
//! - グローバル ACL(project = DB 全体)のみ。
//! - 細粒度(entity 単位、himo 単位)の ACL は Phase D 以降。
//! - ACL の変更自体も WAL に乗り分散される(ACL 自身が紐データ)。
//!
//! # 最初の空 DB
//!
//! ACL が空(0 件)なら「全員 OK」扱い。Phase D で「所有者が必須」に厳しくする。

use std::collections::HashSet;
use std::sync::RwLock;

use crate::PeerId;

/// 現在 apply 可能な writer peer の集合。
///
/// ACL 空 = 「全員許可」(bootstrap / single peer 時の挙動)。
pub struct Acl {
    /// 書き込み許可リスト。None の時は「全員許可」(ACL 未定義)。
    writers: RwLock<Option<HashSet<PeerId>>>,
}

impl Default for Acl {
    fn default() -> Self {
        Self::new()
    }
}

impl Acl {
    pub fn new() -> Self {
        Self { writers: RwLock::new(None) }
    }

    /// writer peer を 1 人追加。初回で None 状態が抜ける(ACL 有効化)。
    pub fn add_writer(&self, peer: PeerId) {
        let mut guard = self.writers.write().unwrap();
        let set = guard.get_or_insert_with(HashSet::new);
        set.insert(peer);
    }

    /// writer peer を除去。集合が空になっても「ACL 無効化」には戻さない
    /// (全員 reject になる)。
    pub fn remove_writer(&self, peer: PeerId) {
        if let Some(set) = self.writers.write().unwrap().as_mut() {
            set.remove(&peer);
        }
    }

    /// ACL が未設定(全員許可)に戻す。
    pub fn clear(&self) {
        *self.writers.write().unwrap() = None;
    }

    /// 指定 peer が書き込み許可されているか。
    pub fn is_writer(&self, peer: PeerId) -> bool {
        match &*self.writers.read().unwrap() {
            None => true, // ACL 未設定 → 全員 OK
            Some(set) => set.contains(&peer),
        }
    }

    /// ACL に登録されている writer の数。None 状態なら 0。
    pub fn writer_count(&self) -> usize {
        self.writers.read().unwrap().as_ref().map(|s| s.len()).unwrap_or(0)
    }

    /// 有効な ACL が設定されているか(None でなく Some)。
    pub fn is_enforced(&self) -> bool {
        self.writers.read().unwrap().is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_allows_all() {
        let acl = Acl::new();
        assert!(acl.is_writer(1));
        assert!(acl.is_writer(42));
        assert!(!acl.is_enforced());
    }

    #[test]
    fn add_writer_enforces() {
        let acl = Acl::new();
        acl.add_writer(1);
        assert!(acl.is_enforced());
        assert!(acl.is_writer(1));
        assert!(!acl.is_writer(2));
    }

    #[test]
    fn remove_writer_does_not_disable_enforcement() {
        let acl = Acl::new();
        acl.add_writer(1);
        acl.add_writer(2);
        acl.remove_writer(1);
        assert!(acl.is_enforced());
        assert!(!acl.is_writer(1));
        assert!(acl.is_writer(2));
        // 最後の 1 人を消しても enforced のまま(全員 reject)
        acl.remove_writer(2);
        assert!(acl.is_enforced());
        assert!(!acl.is_writer(2));
    }

    #[test]
    fn clear_disables_enforcement() {
        let acl = Acl::new();
        acl.add_writer(1);
        acl.clear();
        assert!(!acl.is_enforced());
        assert!(acl.is_writer(5));
    }
}
