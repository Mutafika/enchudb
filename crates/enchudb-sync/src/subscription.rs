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
use enchudb_oplog::PeerId;

/// 「どの peer にどの record を送るか」 の policy。
///
/// `Syncer::publish_since_for_peer` から呼ばれる。 `Syncer::set_subscription_filter`
/// で 1 度差し替える (起動時想定)。 caller が `Send + Sync` を満たせば、
/// 内部の subscription state (peer 別 follow set 等) は自由に持っていい。
///
/// # 契約: filter は pull cursor を scope 依存にする (#219)
///
/// **publisher 側で落とした record は、 差分 pull では二度と届かない。**
///
/// puller の cursor は 「受け取った record の author 別 max HLC」 で前進する
/// (`Syncer::pull_once`)。 同一 author の record を filter が間引くと、 **cursor は
/// 落とされた分を飛び越える**。 cursor は author 粒度であって scope 粒度ではないので、
/// #216 の per-author 化でもこれは閉じない。
///
/// 帰結は 2 つ:
///
/// 1. 後から subscription を広げても、 **広げた scope の過去分は差分 pull では
///    永久に届かない**。 `history_truncated` も立たない (floor は 「reclaim で消えた」
///    分しか表さず、 「その peer には配らなかった」 分は表さない)
/// 2. cursor 述語 (#216/#217) では `hlc <= cursor[author]` が成立するので、
///    **author 側はその record を reclaim してよいと判断する** → 以降は bootstrap
///    以外に回復手段が無い
///
/// したがって:
///
/// > **subscription scope を広げた puller は、 広げた分について差分 pull の完全性を
/// > 仮定してはならない。** 過去分の回収は bootstrap (#140、
/// > `Syncer::bootstrap_pull_via`) で行う。
///
/// scope を広げる側の app は、 広げた author について明示的に bootstrap すること。
/// publisher 側で 「この target に何をどこまで配らなかったか」 を見るには
/// `Syncer::suppressed_since` / `Syncer::suppressed_records`。
///
/// **scope 変更を自動で truncation 扱いにする機構はまだ無い** — target 別の
/// scope 世代を transport に載せる必要があり、 実際の app の follow 変更フローを
/// 見てから形を決める (#219)。 それまでは上の契約が app 側の義務。
///
/// この hazard は `should_send` が false を返す filter にだけ存在する。 default の
/// [`AllRecords`] は何も落とさないので無関係。
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
    use enchudb_oplog::{oplog::DecodedOp, Hlc};

    fn rec(hlc_wall: u64, peer: PeerId) -> WireRecord {
        WireRecord {
            hlc: Hlc { wall: hlc_wall, logical: 0, peer },
            author_peer: peer,
            op: DecodedOp::Tie { eid: enchudb_oplog::make_eid(peer, 1), himo_id: 0, value: 1 },
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
