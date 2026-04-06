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
pub mod himo_store;
pub mod content_store;
pub mod undo;
pub mod engine;
pub mod query_lang;

pub use engine::Engine;
pub use himo_store::HimoType;
