//! EnchuDB — 紐ベース円柱エンジン。
//!
//! この `enchudb` crate は meta crate。
//! 単独 DB 本体は `enchudb-engine`、p2p sync 層は `enchudb-sync` に分離されている。
//! 互換性のために主要 API をここから再エクスポートする:
//!
//! ```
//! let path = format!("/tmp/enchudb-doc-{}.db", std::process::id());
//! let _ = std::fs::remove_file(&path);
//! let mut db = enchudb::Engine::create_standalone(&path).unwrap();
//! db.define_himo("age", enchudb::HimoType::Value, 100);
//! let e = db.entity();
//! db.tie(e, "age", 30);
//! db.tie_text(e, "city", "東京");
//! db.rebuild();
//! let result = db.pull_raw("age", 30);
//! assert_eq!(result, vec![0]);
//! # let _ = std::fs::remove_file(&path);
//! ```

pub use enchudb_engine::*;

/// v32: p2p sync 層(`enchudb-sync`)。`features = ["v32"]` で有効。
#[cfg(feature = "v32")]
pub mod sync {
    pub use enchudb_sync::*;
}
