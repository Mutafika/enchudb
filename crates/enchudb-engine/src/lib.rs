//! EnchuDB — 紐ベース円柱エンジン。単一ファイル。
//!
//! ```
//! let path = format!("/tmp/enchudb-doc-{}.db", std::process::id());
//! let _ = std::fs::remove_file(&path);
//! let mut db = enchudb_engine::Engine::create_standalone(&path).unwrap();
//! db.define_himo("age", enchudb_engine::ValueType::Number, 100);
//! let e = db.entity();
//! db.tie(e, "age", 30);
//! db.tie_text(e, "city", "東京");
//! db.rebuild();
//! let result = db.pull_raw("age", 30); // O(1)
//! assert_eq!(result, vec![0]);
//! # let _ = std::fs::remove_file(&path);
//! ```

pub(crate) mod append_vec;
pub(crate) mod append_bucket;
pub(crate) mod lockfree_cylinder;
pub(crate) mod region;
// growable backing は「虚仮アドレス予約 (PROT_NONE) → MAP_FIXED で貼り直す」
// unix 固有の手を使う。 Windows の MapViewOfFileEx は空きアドレスにしかマップ
// できないので同じ実装は使えない。 ただし **growable が要るのは create 時だけ**
// (`Engine::open` は常に素の mmap backing で開き直す) なので、 Windows では
// 構築不能な stub を置き、 eager な `create_with_capacity` 系を使う。
#[cfg(all(not(target_arch = "wasm32"), unix))]
pub mod growable_map;
#[cfg(all(not(target_arch = "wasm32"), not(unix)))]
#[path = "growable_map_stub.rs"]
pub mod growable_map;
pub mod column;
pub mod vocabulary;
pub mod leaf_store;
pub mod entity_set;
pub mod cylinder;
pub mod cylinder_v27;
pub mod himo_store;
pub mod content_store;
pub mod engine;
pub mod query_lang;
pub mod ravn;
pub mod cas;
pub mod write_queue;
// `wal::*` / `keys::*` / Hlc / PeerId / EntityId / make_eid 等は
// `enchudb-wal` crate に分離済。 後方互換 re-export は提供しない。
// 移行ガイド: docs/migration-wal-crate.md
pub mod hlc_store;
pub mod eid_translator;
pub mod transport;
// `sync::Syncer` は `enchudb-sync` crate に分離済。
// engine は single-peer でも動くので sync を直接持たない。
pub mod changefeed;
// Transport implementations moved to `enchu-transport` crate.
pub mod acl;
pub mod integrity;
pub mod blob_store;

pub use engine::{Engine, EntityValue, SnapshotFiles, AuditFilter, MigrationStats, LeafScale, GrowableOptions};
pub use engine::EngineStats;
pub use himo_store::ValueType;
pub use cas::{CASStore, BlockHash};
pub use ravn::{Ravn, RavnResult};

