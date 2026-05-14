//! EnchuDB — 紐ベース円柱エンジン。単一ファイル。
//!
//! ```
//! let path = format!("/tmp/enchudb-doc-{}.db", std::process::id());
//! let _ = std::fs::remove_file(&path);
//! let mut db = enchudb_engine::Engine::create_standalone(&path).unwrap();
//! db.define_himo("age", enchudb_engine::HimoType::Number, 100);
//! let e = db.entity();
//! db.tie(e, "age", 30);
//! db.tie_text(e, "city", "東京");
//! db.rebuild();
//! let result = db.pull_raw("age", 30); // O(1)
//! assert_eq!(result, vec![0]);
//! # let _ = std::fs::remove_file(&path);
//! ```

pub(crate) mod region;
#[cfg(not(target_arch = "wasm32"))]
pub mod growable_map;
pub mod column;
pub mod vocabulary;
pub mod entity_set;
pub mod cylinder;
pub mod cylinder_v27;
pub mod himo_store;
pub mod content_store;
pub mod undo;
pub mod engine;
pub mod query_lang;
pub mod ravn;
pub mod cas;
pub mod write_queue;
// `wal::*` / `keys::*` / Hlc / PeerId / EntityId / make_eid 等は
// `enchudb-wal` crate に分離済。 後方互換 re-export は提供しない。
// 移行ガイド: docs/migration-wal-crate.md
pub mod hlc_store;
pub mod transport;
// `sync::Syncer` は `enchudb-sync` crate に分離済。
// engine は single-peer でも動くので sync を直接持たない。
pub mod changefeed;
// Transport implementations moved to `enchu-transport` crate.
pub mod acl;
pub mod integrity;
pub mod blob_store;

pub use engine::{Engine, EntityValue, SnapshotFiles, AuditFilter};
pub use engine::EngineStats;
pub use himo_store::HimoType;
pub use cas::{CASStore, BlockHash};
pub use ravn::{Ravn, RavnResult};

