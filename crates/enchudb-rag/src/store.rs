//! RagStore — ベクトル + メタ + テキストの統合ストア。
//!
//! # ファイル構成
//!
//! - `{path}.db` — enchudb 本体（メタ紐、テキスト content、entity 管理）
//! - `{path}.vec` — ベクトル mmap（16B ヘッダ + `eid * dim * 4` オフセット）
//!
//! `.vec` は sparse ファイル。仮想サイズは `max_entities * dim * 4` 分確保するが、
//! 実ディスクは書いた entity 分しか食わない。
//!
//! # ヘッダ（.vec 先頭 16 バイト）
//!
//! ```text
//! [0..4]   magic: b"ERV1"
//! [4..8]   dim: u32 LE
//! [8..12]  reserved (metric tag 用に予約)
//! [12..16] reserved
//! [16..]   vectors: eid 0, eid 1, ..., eid N-1 (各 dim * 4 バイト)
//! ```

use crate::{Chunk, Error, Hit, Meta, MetaValue, Query, HybridQuery, Result};
use crate::bm25::{Bm25Index, rrf_fuse};
use crate::distance::Metric;
use enchudb::{Engine, HimoType};
use enchudb_oplog::EntityId;
use memmap2::MmapMut;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

const VEC_MAGIC: &[u8; 4] = b"ERV1";
const VEC_HEADER_SIZE: usize = 16;
const TEXT_KEY: &str = "__text";

/// ユーザー定義メタフィールドの型。enchudb の HimoType に 1:1 マップ。
#[derive(Clone, Copy, Debug)]
pub enum MetaType {
    /// 整数値（例: tenant_id、date）。
    Value,
    /// 文字列（Vocabulary 経由、例: lang、category）。
    Symbol,
    /// entity 参照。
    Ref,
}

impl MetaType {
    fn to_himo(self) -> HimoType {
        match self {
            MetaType::Value => HimoType::Number,
            MetaType::Symbol => HimoType::Tag,
            MetaType::Ref => HimoType::Ref,
        }
    }
}

/// メタスキーマの 1 フィールド定義。
#[derive(Clone, Debug)]
pub struct MetaSchema {
    pub name: String,
    pub ty: MetaType,
    /// BucketCylinder の初期サイズヒント。0 で動的拡張。
    pub max_values: u32,
}

pub struct RagStoreBuilder {
    path: Option<PathBuf>,
    dim: Option<usize>,
    max_entities: u32,
    metric: Metric,
    schema: Vec<MetaSchema>,
}

impl RagStoreBuilder {
    pub fn path(mut self, p: impl Into<PathBuf>) -> Self {
        self.path = Some(p.into()); self
    }

    pub fn dim(mut self, d: usize) -> Self { self.dim = Some(d); self }

    pub fn max_entities(mut self, n: u32) -> Self { self.max_entities = n; self }

    pub fn metric(mut self, m: Metric) -> Self { self.metric = m; self }

    /// Value 型メタフィールド。
    pub fn meta_value(mut self, name: impl Into<String>, max_values: u32) -> Self {
        self.schema.push(MetaSchema { name: name.into(), ty: MetaType::Value, max_values });
        self
    }

    /// Symbol 型メタフィールド（文字列）。
    pub fn meta_symbol(mut self, name: impl Into<String>) -> Self {
        self.schema.push(MetaSchema { name: name.into(), ty: MetaType::Symbol, max_values: 0 });
        self
    }

    /// Ref 型メタフィールド（entity 参照）。
    pub fn meta_ref(mut self, name: impl Into<String>) -> Self {
        self.schema.push(MetaSchema { name: name.into(), ty: MetaType::Ref, max_values: 0 });
        self
    }

    pub fn build(self) -> Result<RagStore> {
        let path = self.path.ok_or_else(|| Error::Engine("path required".into()))?;
        let dim = self.dim.ok_or_else(|| Error::Engine("dim required".into()))?;
        let max_entities = self.max_entities;

        let db_path = with_ext(&path, "db");
        let vec_path = with_ext(&path, "vec");

        // enchudb を open or create
        let mut db = if db_path.exists() {
            Engine::open_standalone(db_path.to_str().unwrap()).map_err(io_err)?
        } else {
            Engine::create_with_capacity(db_path.to_str().unwrap(), max_entities).map_err(io_err)?
        };

        // スキーマ適用（idempotent、define_himo は重複定義 OK）
        for s in &self.schema {
            db.define_himo(&s.name, s.ty.to_himo(), s.max_values);
        }

        // ベクトル mmap を open or create
        let (vec_file, vec_mmap, actual_dim) = open_or_create_vec(&vec_path, dim, max_entities)?;
        if actual_dim != dim {
            return Err(Error::DimMismatch { expected: dim, got: actual_dim });
        }

        Ok(RagStore {
            db,
            schema: self.schema,
            dim,
            metric: self.metric,
            _vec_file: vec_file,
            vec_mmap,
            bm25: Bm25Index::new(),
        })
    }
}

pub struct RagStore {
    db: Engine,
    schema: Vec<MetaSchema>,
    dim: usize,
    metric: Metric,
    _vec_file: File,
    vec_mmap: MmapMut,
    bm25: Bm25Index,
}

impl RagStore {
    pub fn builder() -> RagStoreBuilder {
        RagStoreBuilder {
            path: None,
            dim: None,
            max_entities: 16_000_000,
            metric: Metric::Cosine,
            schema: Vec::new(),
        }
    }

    pub fn dim(&self) -> usize { self.dim }
    pub fn metric(&self) -> Metric { self.metric }
    pub fn len(&self) -> u32 { self.db.entity_count() }
    pub fn is_empty(&self) -> bool { self.len() == 0 }

    /// 下の Engine への参照（高度な操作が必要な場合用）。
    pub fn engine(&self) -> &Engine { &self.db }

    /// 下の Engine への可変参照。commit/flush/rebuild_pairs 等に必要。
    pub fn engine_mut(&mut self) -> &mut Engine { &mut self.db }

    /// Chunk を挿入。eid を返す。
    pub fn insert(&mut self, chunk: Chunk) -> Result<EntityId> {
        if chunk.vector.len() != self.dim {
            return Err(Error::DimMismatch { expected: self.dim, got: chunk.vector.len() });
        }
        let eid = self.db.entity();
        self.apply_meta(eid, &chunk.meta)?;
        if !chunk.text.is_empty() {
            self.db.content(eid, TEXT_KEY, chunk.text.as_bytes());
            self.bm25.add_document(eid, &chunk.text);
        }
        self.write_vector(eid, &chunk.vector);
        Ok(eid)
    }

    /// 複数 Chunk を一括挿入。
    pub fn insert_batch<I: IntoIterator<Item = Chunk>>(&mut self, chunks: I) -> Result<Vec<EntityId>> {
        let mut ids = Vec::new();
        for c in chunks { ids.push(self.insert(c)?); }
        Ok(ids)
    }

    /// 既存 entity を丸ごと書き換え（メタはクリア → 再設定）。
    pub fn upsert(&mut self, eid: EntityId, chunk: Chunk) -> Result<()> {
        if chunk.vector.len() != self.dim {
            return Err(Error::DimMismatch { expected: self.dim, got: chunk.vector.len() });
        }
        // 既存メタを untie（スキーマ上のフィールドのみ）
        for s in &self.schema {
            self.db.untie(eid, &s.name);
        }
        self.bm25.remove_document(eid);
        self.apply_meta(eid, &chunk.meta)?;
        if !chunk.text.is_empty() {
            self.db.content(eid, TEXT_KEY, chunk.text.as_bytes());
            self.bm25.add_document(eid, &chunk.text);
        }
        self.write_vector(eid, &chunk.vector);
        Ok(())
    }

    /// 削除。ベクトルスロットは 0 埋めするが、eid 自体は recycled されるので
    /// 次の insert で上書きされる。
    pub fn delete(&mut self, eid: EntityId) {
        self.db.delete(eid);
        self.bm25.remove_document(eid);
        self.zero_vector(eid);
    }

    /// テキストを取得。
    pub fn text(&self, eid: EntityId) -> Option<&str> {
        self.db.get_content(eid, TEXT_KEY)
            .and_then(|b| std::str::from_utf8(b).ok())
    }

    /// ベクトルを取得。
    pub fn vector(&self, eid: EntityId) -> &[f32] {
        self.vector_slice(eid)
    }

    /// メタ値を取得（Value/Ref は u32、Symbol も vocab_id）。
    pub fn meta_value(&self, eid: EntityId, field: &str) -> Option<u32> {
        self.db.get(eid, field)
    }

    /// Symbol の生文字列を取得。
    pub fn meta_symbol(&self, eid: EntityId, field: &str) -> Option<String> {
        self.db.get_text(eid, field)
            .and_then(|b| std::str::from_utf8(b).ok().map(|s| s.to_string()))
    }

    /// ベクトル検索。Filter でメタを絞り込んでから brute force cosine。
    pub fn search(&self, q: Query<'_>) -> Result<Vec<Hit>> {
        if q.vector.len() != self.dim {
            return Err(Error::DimMismatch { expected: self.dim, got: q.vector.len() });
        }

        // 候補集合
        let candidates = if q.filter.is_all() {
            None
        } else {
            let alive = self.alive_ids();
            Some(q.filter.evaluate(&self.db, &alive))
        };

        let mut top = TopK::new(q.top_k);

        match candidates {
            Some(ids) => {
                for eid in ids {
                    let score = self.metric.distance(q.vector, self.vector_slice(eid));
                    top.push(eid, score);
                }
            }
            None => {
                // 絞り込みなし = 全 alive を走査
                let alive = self.alive_ids();
                for eid in alive {
                    let score = self.metric.distance(q.vector, self.vector_slice(eid));
                    top.push(eid, score);
                }
            }
        }

        let mut hits = top.into_sorted();
        // テキストを詰める
        for h in &mut hits {
            h.text = self.text(h.eid).map(|s| s.to_string());
        }
        Ok(hits)
    }

    /// query が呼ばれた時点での alive entity id のソート済み Vec。
    /// enchudb の `Engine::entities()` を使って O(alive) で取得。
    fn alive_ids(&self) -> Vec<EntityId> {
        let mut v = self.db.entities();
        v.sort_unstable();
        v
    }

    fn apply_meta(&mut self, eid: EntityId, meta: &Meta) -> Result<()> {
        for (key, v) in meta.iter() {
            if !self.schema.iter().any(|s| s.name == *key) {
                return Err(Error::UnknownMetaField(key.clone()));
            }
            match v {
                MetaValue::Value(n) => self.db.tie(eid, key, *n),
                MetaValue::Symbol(s) => self.db.tie_text(eid, key, s),
                MetaValue::Ref(t) => self.db.tie_ref(eid, key, *t),
            }
        }
        Ok(())
    }

    /// eid の下位 32bit を vector スロット index として使う。
    /// 単一 peer 前提（RagStore は v32 分散を使わない）。
    fn vector_offset(&self, eid: EntityId) -> usize {
        let local = enchudb_oplog::eid_local(eid) as usize;
        VEC_HEADER_SIZE + local * self.dim * 4
    }

    fn vector_slice(&self, eid: EntityId) -> &[f32] {
        let off = self.vector_offset(eid);
        let bytes = &self.vec_mmap[off..off + self.dim * 4];
        // unsafe cast: mmap は常に 4 バイト境界からスタート（header 16B）
        unsafe {
            std::slice::from_raw_parts(bytes.as_ptr() as *const f32, self.dim)
        }
    }

    fn write_vector(&mut self, eid: EntityId, v: &[f32]) {
        let off = self.vector_offset(eid);
        let bytes = unsafe {
            std::slice::from_raw_parts(v.as_ptr() as *const u8, self.dim * 4)
        };
        self.vec_mmap[off..off + self.dim * 4].copy_from_slice(bytes);
    }

    fn zero_vector(&mut self, eid: EntityId) {
        let off = self.vector_offset(eid);
        for b in &mut self.vec_mmap[off..off + self.dim * 4] { *b = 0; }
    }

    /// トランザクション確定 (WAL 有効時は Commit marker append)。
    pub fn commit(&self) { self.db.commit(); }

    /// mmap 同期。
    pub fn flush(&mut self) -> Result<()> {
        self.vec_mmap.flush()?;
        self.db.flush()?;
        Ok(())
    }

    /// ハイブリッド検索（ベクトル類似 + BM25 を RRF で融合）。
    pub fn hybrid_search(&self, q: HybridQuery<'_>) -> Result<Vec<Hit>> {
        if q.vector.len() != self.dim {
            return Err(Error::DimMismatch { expected: self.dim, got: q.vector.len() });
        }

        // メタで候補を絞る
        let alive = self.alive_ids();
        let candidates: Vec<EntityId> = if q.filter.is_all() {
            alive
        } else {
            q.filter.evaluate(&self.db, &alive)
        };

        // ベクトル側のランキング（候補内で brute force）
        let mut vec_scores: Vec<(EntityId, f32)> = candidates.iter()
            .map(|&eid| (eid, self.metric.distance(q.vector, self.vector_slice(eid))))
            .collect();
        // 距離は小さいほど良い → 昇順
        vec_scores.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        // RRF は上位 K を使う（全件走査しても害は無いが計算量を抑える）
        let vec_top: Vec<(EntityId, f32)> = vec_scores.into_iter().take(q.top_k * 4).collect();

        // BM25 側のランキング（候補に限定）
        let bm25_top = self.bm25.search(q.text, Some(&candidates));
        let bm25_top: Vec<(EntityId, f32)> = bm25_top.into_iter().take(q.top_k * 4).collect();

        // RRF で融合
        let fused = rrf_fuse(&vec_top, &bm25_top, q.rrf_k);

        let mut hits: Vec<Hit> = fused.into_iter()
            .take(q.top_k)
            .map(|(eid, score)| Hit {
                eid,
                score, // RRF score (大きいほど良い)
                text: self.text(eid).map(|s| s.to_string()),
            })
            .collect();

        // text を詰めるのは既に済み。順序はそのまま（RRF 降順）。
        // 念のため score 降順に保つ
        hits.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        Ok(hits)
    }

    /// BM25 インデックスへの参照。
    pub fn bm25(&self) -> &Bm25Index { &self.bm25 }

    /// ペアテーブル再構築（v26 feature 有効時のみ意味あり）。
    /// v27 では no-op でも害はない。
    pub fn rebuild_pairs(&mut self) {
        // v26 feature の cross-crate 指定は現在のところ手軽に出来ないので
        // とりあえず何もしない。v26 併用したい人は `engine_mut().rebuild_pairs()` を直接呼ぶ。
    }
}

// ---- top-k ヒープ ----

/// 距離が「小さいほど良い」min-heap-of-max な構造。
/// 中では max-heap として保持し、top_k を超えたら根（最大）を捨てる。
struct TopK {
    k: usize,
    /// (score, eid) を score 降順で並べるため、BinaryHeap<(score, eid)> を使うと
    /// 最大が根に来る = これを pop/replace することで「悪いものを捨てる」になる。
    heap: std::collections::BinaryHeap<HeapEntry>,
}

#[derive(PartialEq)]
struct HeapEntry { score: f32, eid: EntityId }

impl Eq for HeapEntry {}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // NaN は最悪扱い。通常は partial_cmp で十分。
        self.score.partial_cmp(&other.score).unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl TopK {
    fn new(k: usize) -> Self {
        Self { k, heap: std::collections::BinaryHeap::with_capacity(k) }
    }

    fn push(&mut self, eid: EntityId, score: f32) {
        if self.heap.len() < self.k {
            self.heap.push(HeapEntry { score, eid });
        } else if let Some(top) = self.heap.peek() {
            if score < top.score {
                self.heap.pop();
                self.heap.push(HeapEntry { score, eid });
            }
        }
    }

    fn into_sorted(self) -> Vec<Hit> {
        let mut v: Vec<HeapEntry> = self.heap.into_vec();
        v.sort_by(|a, b| a.score.partial_cmp(&b.score).unwrap_or(std::cmp::Ordering::Equal));
        v.into_iter().map(|e| Hit { eid: e.eid, score: e.score, text: None }).collect()
    }
}

// ---- ファイル ops ----

fn with_ext(base: &Path, ext: &str) -> PathBuf {
    let mut p = base.to_path_buf();
    // base が "./rag" なら "rag.db" / "rag.vec" になる
    match p.extension() {
        Some(_) => { p.set_extension(ext); }
        None => { p.set_extension(ext); }
    }
    p
}

fn io_err(e: std::io::Error) -> Error { Error::Io(e) }

fn open_or_create_vec(path: &Path, dim: usize, max_entities: u32) -> Result<(File, MmapMut, usize)> {
    let total_size = VEC_HEADER_SIZE + (max_entities as usize) * dim * 4;
    let exists = path.exists();
    let file = OpenOptions::new().read(true).write(true).create(true).open(path)?;
    if !exists {
        file.set_len(total_size as u64)?;
    } else {
        let meta = file.metadata()?;
        if (meta.len() as usize) < total_size {
            file.set_len(total_size as u64)?;
        }
    }
    let mut mmap = unsafe { MmapMut::map_mut(&file)? };
    if !exists {
        // ヘッダ書き込み
        mmap[0..4].copy_from_slice(VEC_MAGIC);
        mmap[4..8].copy_from_slice(&(dim as u32).to_le_bytes());
        // 8..16 は reserved、0 のまま
        Ok((file, mmap, dim))
    } else {
        // ヘッダ検証
        let magic: [u8; 4] = mmap[0..4].try_into().unwrap();
        if &magic != VEC_MAGIC {
            return Err(Error::Engine("invalid vec file: bad magic".into()));
        }
        let stored_dim = u32::from_le_bytes(mmap[4..8].try_into().unwrap()) as usize;
        Ok((file, mmap, stored_dim))
    }
}
