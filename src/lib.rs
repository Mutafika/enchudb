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
pub mod integrity;

pub use engine::{Engine, EntityValue};
#[cfg(feature = "v27")]
pub use engine::EngineStats;
pub use himo_store::HimoType;
pub use cas::{CASStore, BlockHash};
pub use ravn::{Ravn, RavnResult};

/// v32: Entity ID 型エイリアス。
///
/// 現状 `u32`(v31 互換)。**次期 Phase で `u64` に拡張**し、
/// `[peer_id: 32bit][local_id: 32bit]` レイアウトで分散対応する。
/// 今のうちから API で u32 を直書きせず `EntityId` を使うよう誘導するための名前。
pub type EntityId = u32;

/// v32: Peer ID 型エイリアス。将来 distributed 時に u32 peer 識別子となる。
/// 現状単独 peer なので `peer_id = 0` 固定。
pub type PeerId = u32;

/// local_id と peer_id から EntityId を合成(将来の u64 化 preview)。
/// 現状 u32 なので peer 部分は保持されない(単独 peer 運用前提)。
#[inline]
pub const fn make_eid(_peer: PeerId, local: u32) -> EntityId {
    local
}

/// EntityId から peer 部分を取得(現状常に 0)。
#[inline]
pub const fn eid_peer(_eid: EntityId) -> PeerId {
    0
}

/// EntityId から local 部分を取得(現状 eid そのもの)。
#[inline]
pub const fn eid_local(eid: EntityId) -> u32 {
    eid
}
