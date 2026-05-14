//! EnchuDB WAL crate — wire-level primitives shared across engine / sync / transport.
//!
//! このクレートは以下を提供する:
//! - **Hlc / PeerId / EntityId**: 分散 op に必要な全順序・peer 識別の primitive 型
//! - **Wal**: append-only operation log (v2 format)
//! - **Keypair / PubkeyStore**: WAL 署名検証用の ed25519 鍵管理
//!
//! `enchudb-engine` から切り出すことで「WAL は engine の内部詳細」ではなく
//! 「分散 DB の正準パケットフォーマット」として独立リリース可能にする。
//! 既存の `enchudb_engine::wal::*` / `enchudb_engine::keys::*` / `enchudb_engine::Hlc`
//! などのパスは engine 側で re-export して後方互換を保つ。

pub mod keys;
pub mod wal;

/// v32: Entity ID。u64 = `[peer_id: 32bit][local_id: 32bit]`。
/// 単独 peer 運用時は peer_id = 0、実質 local_id のみ使われる。
pub type EntityId = u64;

/// v32: Peer ID。各 peer が固有 u32 ID を持ち、上位 32bit を EntityId に埋め込む。
pub type PeerId = u32;

/// local_id と peer_id から EntityId を合成。
#[inline]
pub const fn make_eid(peer: PeerId, local: u32) -> EntityId {
    ((peer as u64) << 32) | (local as u64)
}

/// EntityId から peer 部分を取得。
#[inline]
pub const fn eid_peer(eid: EntityId) -> PeerId {
    (eid >> 32) as u32
}

/// EntityId から local 部分を取得。
#[inline]
pub const fn eid_local(eid: EntityId) -> u32 {
    eid as u32
}

/// v32: Hybrid Logical Clock。分散 peer 間で全順序を確立する。
/// wall: 物理時刻(ms since epoch)、logical: 同時刻内のカウンタ、peer: tiebreaker。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hlc {
    pub wall: u64,
    pub logical: u32,
    pub peer: PeerId,
}

impl Hlc {
    pub const ZERO: Hlc = Hlc { wall: 0, logical: 0, peer: 0 };

    /// 比較キー。`(wall, logical, peer)` 辞書順で全順序。
    #[inline]
    pub fn cmp_key(&self) -> (u64, u32, u32) {
        (self.wall, self.logical, self.peer)
    }

    /// 受信した HLC をもとに自 peer の次 HLC を計算する(HLC の merge ルール)。
    /// - wall = max(local_wall, received_wall, now_wall)
    /// - logical = 新 wall が全部一致なら max(local_logical, received_logical)+1、でなければ 0
    pub fn merge_recv(local: Hlc, recv: Hlc, now_wall: u64, local_peer: PeerId) -> Hlc {
        let new_wall = local.wall.max(recv.wall).max(now_wall);
        let new_logical = if new_wall == local.wall && new_wall == recv.wall {
            local.logical.max(recv.logical).wrapping_add(1)
        } else if new_wall == local.wall {
            local.logical.wrapping_add(1)
        } else if new_wall == recv.wall {
            recv.logical.wrapping_add(1)
        } else {
            0
        };
        Hlc { wall: new_wall, logical: new_logical, peer: local_peer }
    }
}

impl PartialOrd for Hlc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Hlc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.cmp_key().cmp(&other.cmp_key())
    }
}
