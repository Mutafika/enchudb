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
//! let path = format!("/tmp/enchudb-sync-lib-doc-{}.db", std::process::id());
//! let _ = std::fs::remove_file(&path);
//! let _ = std::fs::remove_file(format!("{}.oplog", path));
//! let _ = std::fs::remove_file(format!("{}.tables", path));
//! let _ = std::fs::remove_file(format!("{}.db.lock", path));
//! {
//!     let mut eng_init = Engine::create_standalone(&path).unwrap();
//!     eng_init.enable_sync_tables().unwrap();
//!     eng_init.flush().unwrap();
//! }
//! let eng_a = Engine::open_concurrent_with_oplog(&path, 4 * 1024 * 1024).unwrap();
//!
//! let transport: Arc<dyn Transport> = Arc::new(InMemoryTransport::new());
//! let syncer = Syncer::new(eng_a.clone(), transport);
//! let out = syncer.pull_once(2);
//! assert_eq!(out.received, 0);
//! # drop(eng_a);
//! # let _ = std::fs::remove_file(&path);
//! # let _ = std::fs::remove_file(format!("{}.oplog", path));
//! # let _ = std::fs::remove_file(format!("{}.tables", path));
//! # let _ = std::fs::remove_file(format!("{}.db.lock", path));
//! ```

pub mod sync;
pub mod shard;
pub mod subscription;

pub use sync::{Syncer, SyncOutcome};
pub use shard::{ShardRouter, ShardTransport, ShardQuery, HashRouter, ExplicitRouter, InMemoryShardTransport};
pub use subscription::{SubscriptionFilter, AllRecords};
