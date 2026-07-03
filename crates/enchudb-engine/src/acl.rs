//! ACL — **インメモリ・プロセス内限定** の writer 許可リスト。
//!
//! # 現状の正直な仕様 (重要)
//!
//! この [`Acl`] は単なるプロセス内の allow-list で、以下の点に注意:
//!
//! - **永続化されない。** DB ファイルにも WAL にも書かれない。プロセスを
//!   落とせば消える。
//! - **どの himo にも紐付いていない。** 過去のドキュメントには `acl_writer`
//!   himo 経由で紐として表現する設計が書かれていたが、その経路は未実装
//!   (どこからも配線されていない)。
//! - **open するたびに allow-all で始まる。** デフォルト (writer 未登録) は
//!   「全 peer 許可」なので、ACL を効かせたい呼び出し側は **DB を開き直す
//!   たびに [`Acl::add_writer`] で再投入する必要がある。**
//! - 効果があるのは同一プロセス内で `is_writer()` を確認する経路
//!   (sync の op 受信チェック等) のみ。
//!
//! 永続化された ACL (紐データとして WAL に乗り、peer 間で分散されるもの) は
//! 将来の作業であり、現時点では存在しない。
//!
//! # 使い方
//!
//! ```
//! use enchudb_engine::acl::Acl;
//!
//! let acl = Acl::new();
//! // デフォルトは allow-all (ACL 未設定)
//! assert!(acl.is_writer(42));
//! assert!(!acl.is_enforced());
//!
//! // writer を登録した瞬間から allow-list として効く (このプロセス内のみ)
//! acl.add_writer(1);
//! assert!(acl.is_writer(1));
//! assert!(!acl.is_writer(2));
//!
//! // 永続化されないので、DB を開き直したら再登録が必要
//! ```
//!
//! # スコープ
//!
//! - グローバル (プロセス単位) の writer 集合のみ。
//! - 細粒度 (entity 単位、himo 単位) の ACL は未対応。
//! - ACL 変更の永続化 / 分散 (WAL 経由) は未対応 — 将来の作業。

use std::collections::HashSet;
use std::sync::RwLock;

use enchudb_oplog::PeerId;

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
