//! EnchuDB — 紐ベース円柱エンジン。単一ファイル。
//!
//! ```
//! let path = format!("/tmp/enchudb-doc-{}.db", std::process::id());
//! let _ = std::fs::remove_file(&path);
//! let mut db = enchudb_engine::Engine::create_standalone(&path).unwrap();
//! db.define_himo("age", enchudb_engine::ValueType::Number, 100);
//! let e = db.entity().unwrap();
//! db.tie(e, "age", 30);
//! db.tie_text(e, "city", "東京");
//! db.rebuild();
//! let result = db.pull_raw("age", 30); // O(1)
//! assert_eq!(result, vec![0]);
//! # let _ = std::fs::remove_file(&path);
//! ```
//!
//! # ディスク容量について (#167)
//!
//! 本体 DB は **sparse ファイル**。 apparent size は create 時に
//! `max_entities` x `max_himos` から決まる固定値で (既定 capacity で ~95 GB)、
//! 実際に消費するのは書いた分だけ (数百 KB)。 `df` は動かない。 これは設計どおり
//! だが、 3 つ注意が要る。
//!
//! **1. ディスクが埋まるとエラーではなく SIGBUS でプロセスごと落ちる。** 書き込みは
//! `mmap` 経由なので、 穴に block を割り当てられない (`ENOSPC`) 時に errno を返す先が
//! 無く signal になる。 `Result` で受けられないし、 通常のコードでは捕まえられない。
//! `create` は `set_len` するだけなので **作成時点では必ず成功し**、 落ちるのは後で
//! 書いた時。 **空きは apparent size ぶんを見込むこと** — 「`df` に空きがある」 は
//! 安全を意味しない。
//!
//! **2. DB を copy すると穴が実体化しうる。** `std::fs::copy` は macOS では穴を維持
//! する (APFS clonefile) が、 **Linux では 0 で埋めて書き出す**。 [`copy_sparse`] は
//! `SEEK_DATA` / `SEEK_HOLE` でデータ範囲だけを写すので、 こちらを使うこと
//! (`Engine::snapshot_export` は既に使っている)。
//!
//! **3. 外部のバックアップツールも同じ罠を踏む。** `--sparse` 無しの `rsync`、 素の
//! `cp`、 apparent size で数えるツール (Time Machine) はファイルを膨らませる。
//! apparent size 自体が問題になる用途では、 `max_entities` を小さくするか
//! `create_growable_*` を使う。

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
pub mod sparse_copy;

pub use engine::{Engine, EntityValue, SnapshotFiles, AuditFilter, MigrationStats, LeafScale, GrowableOptions, FaultKind, RemoteApply};
pub use sparse_copy::copy_sparse;
pub use engine::EngineStats;
pub use himo_store::ValueType;
pub use cas::{CASStore, BlockHash};
pub use ravn::{Ravn, RavnResult};

