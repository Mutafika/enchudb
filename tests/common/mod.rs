//! tests/ 間で共有する補助モジュール。
//!
//! - `crdt`: CRDT プリミティブ(旧 src/crdt.rs、feature = "crdt" 廃止に伴い移動)
//! - `chaos_sim`: 分散 simulator(旧 src/chaos_sim.rs、feature = "chaos" 廃止)
//! - `peer_sim`: 実 Engine + Syncer を transport にぶら下げた peer 試験環境
//!
//! いずれもランタイムに含めるものではなく、検証用の実験実装。
//! 各 `tests/*.rs` が `mod common;` で取り込む。

pub mod crdt;
pub mod chaos_sim;

/// `peer_sim`: 実 Engine + Syncer の peer 試験環境 (収束を assert する)
pub mod peer_sim;
