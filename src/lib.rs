//! EnchuDB — 紐ベース円柱エンジン。単一ファイル。
//!
//! ```ignore
//! let mut db = enchudb::Engine::create("/tmp/mydb.db").unwrap();
//! db.define_himo("age", enchudb::HimoType::Value, 100);
//! let e = db.entity();
//! db.tie(e, "age", 30);
//! db.tie_text(e, "city", "東京");
//! db.rebuild();
//! let result = db.pull_raw("age", 30); // O(1)
//! ```

pub(crate) mod region;
pub mod column;
pub mod vocabulary;
pub mod entity_set;
pub mod cylinder;
#[cfg(feature = "v27")]
pub mod cylinder_v27;
pub mod himo_store;
pub mod content_store;
pub mod undo;
pub mod engine;
pub mod query_lang;
pub mod ravn;
pub mod cas;
#[cfg(feature = "v27")]
pub mod write_queue;
#[cfg(feature = "v27")]
pub mod wal;
#[cfg(feature = "v32")]
pub mod hlc_store;
#[cfg(feature = "v32")]
pub mod transport;
#[cfg(feature = "v32")]
pub mod sync;
// Transport implementations moved to `enchu-transport` crate.
#[cfg(feature = "v32")]
pub mod keys;
#[cfg(feature = "v32")]
pub mod acl;
pub mod integrity;
pub mod blob_store;

pub use engine::{Engine, EntityValue, SnapshotFiles, AuditFilter};
#[cfg(feature = "v27")]
pub use engine::EngineStats;
pub use himo_store::HimoType;
pub use cas::{CASStore, BlockHash};
pub use ravn::{Ravn, RavnResult};

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
