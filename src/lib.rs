//! EnchuDB — 紐ベース円柱エンジン。
//!
//! この `enchudb` crate は `enchudb-engine` を便利に再エクスポートする meta crate。
//! 実装の本体は `enchudb-engine`、p2p sync 層は将来 `enchudb-sync` に切り出される。
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
