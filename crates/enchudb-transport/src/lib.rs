//! enchu-transport — Transport trait の実装集。
//!
//! - [`http`]: `std::net` のみで実装した HTTP/1.1 relay (軽量、外部依存ゼロ)
//! - [`ws`]: `tungstenite` ベースの WebSocket push transport (feature `ws`、default on)
//!
//! Trait 本体と wire format は [`enchudb::transport`] に存在する。本 crate は enchudb に対する
//! transport 実装の分離先で、`Transport` trait を enchudb から借用している。

pub mod http;

#[cfg(feature = "ws")]
pub mod ws;

pub use http::{HttpRelay, HttpTransport};

#[cfg(feature = "ws")]
pub use ws::{WsPushClient, WsPushHub};
