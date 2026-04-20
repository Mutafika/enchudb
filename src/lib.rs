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
