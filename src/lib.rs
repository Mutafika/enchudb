//! EnchuDB — 紐ベース円柱エンジン。
//!
//! この `enchudb` crate は meta crate。 主要 layer をここから再エクスポートする:
//!
//! - `enchudb::Engine` 等 — engine 層 (`enchudb-engine`)、 naked 紐 + 円柱
//! - `enchudb::schema::*` — **native API** (`enchudb-schema`)、 仮想 2D テーブル + 永続化
//! - `enchudb::sync::*` — p2p sync 層 (`enchudb-sync`)
//! - `enchudb-sql` crate — SQLite-compat parser (`features = ["sql"]`、 別 crate として直接 dep)
//!
//! ## native API (推奨)
//!
//! ```no_run
//! use enchudb::schema::{Database, ColumnType};
//!
//! let mut db = Database::create("/tmp/app.db")?;
//! let users = db.table("users")
//!     .number("id")
//!     .tag("name")
//!     .number("age")
//!     .primary_key("id")
//!     .build()?;
//!
//! let alice = users.insert().set("id", 1i64).set("name", "Alice").set("age", 30i64).commit()?;
//! let r = users.where_eq("age", 30i64).find()?;
//! # Ok::<(), enchudb::schema::SchemaError>(())
//! ```
//!
//! ## engine 層 (REPL / power user)
//!
//! ```
//! let path = format!("/tmp/enchudb-doc-{}.db", std::process::id());
//! let _ = std::fs::remove_file(&path);
//! let mut db = enchudb::Engine::create_standalone(&path).unwrap();
//! db.define_himo("age", enchudb::HimoType::Number, 100);
//! let e = db.entity();
//! db.tie(e, "age", 30);
//! db.rebuild();
//! assert_eq!(db.pull_raw("age", 30), vec![0]);
//! # let _ = std::fs::remove_file(&path);
//! ```

pub use enchudb_engine::*;

/// Native API (仮想 2D テーブル + 永続化)。 app 開発向けの primary path。
/// disk-backed なので wasm32 では提供されない (engine API を直接使うこと)。
#[cfg(not(target_arch = "wasm32"))]
pub mod schema {
    pub use enchudb_schema::*;
}

/// p2p sync 層 (`enchudb-sync`)。
pub mod sync {
    pub use enchudb_sync::*;
}
