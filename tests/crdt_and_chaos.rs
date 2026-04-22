//! CRDT + chaos_sim の integration test entry。
//!
//! `mod common;` で tests/common/crdt.rs と tests/common/chaos_sim.rs を取り込み、
//! それぞれの `#[cfg(test)] mod tests {...}` を走らせる。
//!
//! v32 feature が有効時のみ意味がある(chaos_sim が PeerId 型を使う、CRDT は純 std)
//! が、両方とも外部依存ゼロなので feature gate しなくてもビルドは通る。

#![cfg(feature = "v32")]

mod common;
