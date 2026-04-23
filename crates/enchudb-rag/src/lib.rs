//! enchudb-rag — RAG ストア。
//!
//! # 設計
//!
//! - **メタデータ + テキスト**: enchudb（紐ベース、単一ファイル mmap）
//! - **ベクトル**: 別 mmap ファイル（`*.vec`）、固定次元 f32、オフセット = `eid * dim * 4`
//! - **検索**: Filter でメタを ns オーダー絞り込み → 候補だけ brute force cosine
//!
//! # なぜ ANN（HNSW 等）を使わないか
//!
//! メタで 1% に絞れれば、100 万 chunk → 1 万件の brute force cosine は数 ms で済む。
//! ANN の恩恵は「絞れない場合」に出るだけ。enchudb v27 のメタフィルタが強いので
//! brute force で十分勝てる。
//!
//! # 基本フロー
//!
//! ```no_run
//! use enchudb_rag::{RagStore, Chunk, Query, Meta, Filter};
//!
//! let mut store = RagStore::builder()
//!     .path("./rag")
//!     .dim(768)
//!     .meta_value("tenant", 1024)
//!     .meta_symbol("lang")
//!     .meta_value("date", 0)
//!     .build()
//!     .unwrap();
//!
//! let eid = store.insert(Chunk {
//!     text: "...".into(),
//!     vector: vec![0.0; 768],
//!     meta: Meta::new()
//!         .value("tenant", 3)
//!         .symbol("lang", "ja")
//!         .value("date", 9100),
//! }).unwrap();
//!
//! let hits = store.search(Query {
//!     vector: &vec![0.0; 768],
//!     filter: Filter::value("tenant", 3).and(Filter::symbol("lang", "ja")),
//!     top_k: 10,
//! }).unwrap();
//! ```

pub mod distance;
pub mod filter;
pub mod store;
pub mod chunk;
pub mod query;
pub mod embedder;
pub mod chunker;
pub mod bm25;
pub mod rerank;

pub use chunk::{Chunk, Meta, MetaValue};
pub use filter::Filter;
pub use query::{Query, Hit, HybridQuery};
pub use store::{RagStore, RagStoreBuilder, MetaSchema, MetaType};
pub use embedder::{Embedder, HashEmbedder};
#[cfg(feature = "fastembed")]
pub use embedder::FastEmbedder;
pub use chunker::{Chunker, RecursiveCharSplitter};
pub use rerank::Reranker;
pub use distance::{cosine, dot, l2};

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    DimMismatch { expected: usize, got: usize },
    UnknownMetaField(String),
    MissingVector,
    Engine(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io: {}", e),
            Error::DimMismatch { expected, got } => {
                write!(f, "dim mismatch: expected {}, got {}", expected, got)
            }
            Error::UnknownMetaField(s) => write!(f, "unknown meta field: {}", s),
            Error::MissingVector => write!(f, "vector not set for entity"),
            Error::Engine(s) => write!(f, "enchudb: {}", s),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self { Error::Io(e) }
}

pub type Result<T> = std::result::Result<T, Error>;
