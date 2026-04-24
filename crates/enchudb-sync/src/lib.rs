//! EnchuDB p2p sync — HLC-LWW Syncer 本体。
//!
//! `enchudb-engine` の `Engine` + `WAL` + `hlc_store` + `transport` の各部品の上に
//! 積む層。単独 peer 用途で embed する場合は引かなくていい。
//!
//! # 使い方
//!
//! ```
//! use std::sync::Arc;
//! use enchudb_engine::Engine;
//! use enchudb_engine::transport::{InMemoryTransport, Transport};
//! use enchudb_sync::Syncer;
//!
//! let path = format!("/tmp/enchudb-sync-doc-{}.db", std::process::id());
//! let _ = std::fs::remove_file(&path);
//! let _ = std::fs::remove_file(format!("{}.wal", path));
//! let eng_a = Engine::create_concurrent_with_wal(&path, 4 * 1024 * 1024).unwrap();
//!
//! let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
//! let syncer = Syncer::new(eng_a.clone(), transport);
//! let out = syncer.pull_once(2);
//! assert_eq!(out.received, 0);
//! # let _ = std::fs::remove_file(&path);
//! # let _ = std::fs::remove_file(format!("{}.wal", path));
//! ```

pub mod sync;

pub use sync::{Syncer, SyncOutcome};
