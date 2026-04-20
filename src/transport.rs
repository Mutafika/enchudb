//! Transport — peer 間で WAL レコードをやりとりする抽象。
//!
//! # 責務
//!
//! - リモート peer の WAL を「指定 LSN 以降」で引く(pull)
//! - 自 peer の WAL をリモートに送る(push)。push-based は Phase C 以降で。
//!
//! # 実装
//!
//! - `InMemoryTransport`: テスト用、2+ peer の WAL を Arc<Mutex<HashMap>> で共有
//! - `WebSocketTransport`: Phase C 以降
//! - `HttpTransport`: Phase C 以降
//!
//! # 同期プロトコル(Phase B 初期版)
//!
//! ```text
//! Peer A                            Peer B
//!   │                                 │
//!   │── pull(from_peer=B, since=L)──▶│
//!   │                                 │
//!   │◀── records[] ──────────────────│
//!   │                                 │
//!   │  (LWW で apply、HlcStore 更新)    │
//! ```
//!
//! records は commit 済みの WAL レコードのみ(uncommitted は送らない)。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::{Hlc, PeerId};
use crate::wal::{DecodedOp, RecoveredRecord};

/// peer 間で交換する 1 件の op。
/// Phase C: signature と pubkey_fp + 署名対象 bytes を同梱する。
#[derive(Debug, Clone)]
pub struct WireRecord {
    pub hlc: Hlc,
    pub author_peer: PeerId,
    pub op: DecodedOp,
    /// ed25519 署名(64B)。zeros なら未署名。
    pub signature: [u8; 64],
    /// 署名した公開鍵の先頭 8B(TOFU で識別に使う)。
    pub pubkey_fp: [u8; 8],
    /// 署名対象の生 bytes(WAL header 固定部 + payload)。
    /// peer 間通信では wire 上で再生成する設計でもいいが、簡便に同梱。
    pub signed_bytes: Vec<u8>,
}

impl From<RecoveredRecord> for WireRecord {
    fn from(r: RecoveredRecord) -> Self {
        Self {
            hlc: r.hlc,
            author_peer: r.author_peer,
            op: r.op,
            signature: r.signature,
            pubkey_fp: r.pubkey_fp,
            signed_bytes: r.signed_bytes,
        }
    }
}

impl WireRecord {
    /// テスト用: 未署名(署名 slot zero)で WireRecord を作る。
    /// 本番経路では Wal::iter_committed() 経由で signed な record が来るべき。
    pub fn unsigned(hlc: Hlc, author_peer: PeerId, op: DecodedOp) -> Self {
        Self {
            hlc, author_peer, op,
            signature: [0u8; 64],
            pubkey_fp: [0u8; 8],
            signed_bytes: Vec::new(),
        }
    }
}

/// 同期 transport。
///
/// ブロッキング API。将来的に async trait 化予定(Phase C)。
pub trait Transport: Send + Sync {
    /// `from` peer の、HLC が `since` より後のレコードを取得。
    /// 結果は HLC 昇順。
    fn pull(&self, from: PeerId, since: Hlc) -> Vec<WireRecord>;

    /// 自 peer の commit 済みレコードを broadcast(publish 相当)。
    /// Phase B InMemoryTransport では「共有 log に append する」だけ。
    fn publish(&self, peer: PeerId, records: Vec<WireRecord>);
}

/// テスト用: プロセス内で peer 間の WAL を共有する。
///
/// peer ごとに `(ordered log of WireRecord)` を持つ。HLC 昇順で入れる想定。
#[derive(Default, Clone)]
pub struct InMemoryTransport {
    inner: Arc<Mutex<HashMap<PeerId, Vec<WireRecord>>>>,
}

impl InMemoryTransport {
    pub fn new() -> Self {
        Self { inner: Arc::new(Mutex::new(HashMap::new())) }
    }

    /// 現在 `from` peer が持っている全レコード数(テスト用)。
    pub fn len_of(&self, from: PeerId) -> usize {
        self.inner.lock().unwrap().get(&from).map(|v| v.len()).unwrap_or(0)
    }
}

impl Transport for InMemoryTransport {
    fn pull(&self, from: PeerId, since: Hlc) -> Vec<WireRecord> {
        let guard = self.inner.lock().unwrap();
        let log = match guard.get(&from) {
            Some(l) => l,
            None => return Vec::new(),
        };
        log.iter()
            .filter(|r| r.hlc > since)
            .cloned()
            .collect()
    }

    fn publish(&self, peer: PeerId, mut records: Vec<WireRecord>) {
        if records.is_empty() { return; }
        records.sort_by_key(|r| r.hlc);
        let mut guard = self.inner.lock().unwrap();
        let log = guard.entry(peer).or_insert_with(Vec::new);
        log.extend(records);
        log.sort_by_key(|r| r.hlc);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::DecodedOp;

    fn rec(hlc_wall: u64, peer: PeerId, eid: u64, value: u32) -> WireRecord {
        WireRecord {
            hlc: Hlc { wall: hlc_wall, logical: 0, peer },
            author_peer: peer,
            op: DecodedOp::Tie { eid, himo_id: 0, value },
            signature: [0u8; 64],
            pubkey_fp: [0u8; 8],
            signed_bytes: Vec::new(),
        }
    }

    #[test]
    fn pull_returns_records_after_since() {
        let t = InMemoryTransport::new();
        t.publish(0, vec![
            rec(100, 0, 1, 10),
            rec(200, 0, 2, 20),
            rec(300, 0, 3, 30),
        ]);
        let since = Hlc { wall: 150, logical: 0, peer: 0 };
        let out = t.pull(0, since);
        assert_eq!(out.len(), 2);
        assert!(matches!(out[0].op, DecodedOp::Tie { eid: 2, .. }));
        assert!(matches!(out[1].op, DecodedOp::Tie { eid: 3, .. }));
    }

    #[test]
    fn pull_unknown_peer_empty() {
        let t = InMemoryTransport::new();
        let out = t.pull(42, Hlc::ZERO);
        assert!(out.is_empty());
    }

    #[test]
    fn publish_sorts_by_hlc() {
        let t = InMemoryTransport::new();
        t.publish(0, vec![
            rec(300, 0, 3, 30),
            rec(100, 0, 1, 10),
            rec(200, 0, 2, 20),
        ]);
        let out = t.pull(0, Hlc::ZERO);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].hlc.wall, 100);
        assert_eq!(out[1].hlc.wall, 200);
        assert_eq!(out[2].hlc.wall, 300);
    }

    #[test]
    fn pull_with_same_hlc_excluded() {
        let t = InMemoryTransport::new();
        let h1 = Hlc { wall: 100, logical: 0, peer: 0 };
        t.publish(0, vec![rec(100, 0, 1, 10)]);
        // since = 記録されている HLC と同じ → exclusive なので空
        let out = t.pull(0, h1);
        assert!(out.is_empty());
    }
}
