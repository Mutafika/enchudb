//! `SubscriptionFilter` — partial sync の policy hook (request4)。
//!
//! `enchudb-sync` は 3 つの cross-cutting policy を持つ:
//!
//! | policy | 経路 | trait |
//! |---|---|---|
//! | shard routing | write | `shard::ShardRouter` |
//! | ACL | write | `enchudb_engine::acl::Acl` |
//! | subscription | publish | `SubscriptionFilter` ← 本 module |
//!
//! SaaS (workspace 単位の full sync) は **default `AllRecords`** で OK
//! (= 全 peer に全 record を撒く、 旧来動作)。
//!
//! SNS (Twitter 系の partial sync) は **自前 struct で `impl SubscriptionFilter`**
//! して `Syncer::set_subscription_filter` に渡せば、 publish 経路の他の機構
//! (WAL iter / 署名 / transport) には触らずに「followee の post だけ送る」が
//! 実現できる。

use enchudb_engine::transport::WireRecord;
use enchudb_wal::PeerId;

/// 「どの peer にどの record を送るか」 の policy。
///
/// `Syncer::publish_since_for_peer` から呼ばれる。 `Syncer::set_subscription_filter`
/// で 1 度差し替える (起動時想定)。 caller が `Send + Sync` を満たせば、
/// 内部の subscription state (peer 別 follow set 等) は自由に持っていい。
pub trait SubscriptionFilter: Send + Sync {
    /// `target_peer` に `record` を送るべきか。 default 実装は **true**
    /// (= 全送り、 `AllRecords` 相当の挙動)。
    fn should_send(&self, target_peer: PeerId, record: &WireRecord) -> bool {
        let _ = (target_peer, record);
        true
    }
}

/// 既定 filter — 全 record を全 peer に送る (= `Syncer` の旧来動作 = SaaS 用)。
///
/// `Syncer::new` 直後はこれが装着されている。 `set_subscription_filter` で差し替える
/// まで、 `publish_since_for_peer` は全 record を素通しで送る。
pub struct AllRecords;

impl SubscriptionFilter for AllRecords {}

#[cfg(test)]
mod tests {
    use super::*;
    use enchudb_wal::{wal::DecodedOp, Hlc};

    fn rec(hlc_wall: u64, peer: PeerId) -> WireRecord {
        WireRecord {
            hlc: Hlc { wall: hlc_wall, logical: 0, peer },
            author_peer: peer,
            op: DecodedOp::Tie { eid: enchudb_wal::make_eid(peer, 1), himo_id: 0, value: 1 },
            signature: [0u8; 64],
            pubkey_fp: [0u8; 8],
            signed_bytes: Vec::new(),
        }
    }

    #[test]
    fn all_records_passes_everything() {
        let f = AllRecords;
        for target in 1..=3 {
            for wall in [10, 20, 30] {
                assert!(f.should_send(target, &rec(wall, 1)));
            }
        }
    }

    struct OnlyFromPeer1;
    impl SubscriptionFilter for OnlyFromPeer1 {
        fn should_send(&self, _target_peer: PeerId, record: &WireRecord) -> bool {
            record.author_peer == 1
        }
    }

    #[test]
    fn custom_filter_can_drop_by_author() {
        let f = OnlyFromPeer1;
        assert!(f.should_send(2, &rec(10, 1)));
        assert!(!f.should_send(2, &rec(10, 2)));
    }
}
