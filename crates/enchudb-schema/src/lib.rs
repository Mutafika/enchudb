//! EnchuDB 仮想 2D テーブル層 — **native API**。
//!
//! SQL ではなく、 schema を declare すると himo_id が pre-resolve されて
//! query / insert は名前 lookup なしで engine に直 dispatch される。 SQL 層
//! (`enchudb-sql`) はこの上に薄く乗る parser。
//!
//! ## 使い方
//!
//! ```rust,no_run
//! use enchudb_schema::{Database, ColumnType};
//!
//! let mut db = Database::create("/tmp/app.db")?;
//!
//! let users = db.table("users")
//!     .number("id")
//!     .tag("name")
//!     .number("age")
//!     .tag("city")
//!     .primary_key("id")
//!     .build()?;
//!
//! // insert (row-shaped、 内部では N 本の tie)
//! let alice = users.insert()
//!     .set("id", 1)
//!     .set("name", "Alice")
//!     .set("age", 30)
//!     .set("city", "Tokyo")
//!     .commit()?;
//!
//! // query — col → himo_id は build 時に解決済み
//! let young = users.where_eq("age", 30).find()?;
//! let multi = users.where_eq("age", 30).where_eq("city", "Tokyo").find()?;
//!
//! // get / update / delete
//! let age = users.entity(alice).get("age");
//! users.entity(alice).set("age", 31).commit()?;
//! users.entity(alice).delete()?;
//! # Ok::<(), enchudb_schema::SchemaError>(())
//! ```
//!
//! ## relation (cross-table ref)
//!
//! ```rust,no_run
//! # use enchudb_schema::{Database, ColumnType};
//! # let mut db = Database::create("/tmp/x.db")?;
//! // 先に referenced 側 table を build
//! db.table("companies").number("id").tag("name").primary_key("id").build()?;
//!
//! let users = db.table("users")
//!     .number("id")
//!     .tag("name")
//!     .ref_to("company", "companies")  // users.company : Ref → companies.eid
//!     .primary_key("id")
//!     .build()?;
//! # Ok::<(), enchudb_schema::SchemaError>(())
//! ```
//!
//! ## 永続化
//!
//! schema は DB ファイル内の content blob として serialize される。 `Database::open`
//! 時に自動で復元、 himo_id も再 resolve される。 `Drop` で flush が呼ばれるので
//! 手動 flush は不要 (明示的に呼びたい場合は `db.engine_mut().flush()`)。

use enchudb_engine::{Engine, ValueType};
/// #118: growable layout の全 knob を露出する options 型を re-export
/// (`enchudb_schema::GrowableOptions` / `LeafScale` で使えるように)。
pub use enchudb_engine::engine::TableEidUsage;
pub use enchudb_engine::{GrowableOptions, LeafScale};
pub use enchudb_engine::LiveDelta;
use enchudb_oplog::EntityId;
use std::sync::Arc;

// 0.8.7: schema sidecar 拡張子。 `{db_path}.schema` で永続化。
// 0.6.x までの schema_meta_entity (= anonymous entity に blob を載せる方式) は
// `define_table` 後に anonymous が close されると panic する vestigial path
// だったため撤去、 `.schema` sidecar に置き換えた (issue note: schema_meta_entity
// は 0.7.0 の名残、 削除すべき)。 旧 DB 互換のため legacy blob 読み込み path も
// 残してあり、 初回 open で `.schema` sidecar に migrate されて以降は新 path のみ。
// legacy (= 0.6.x の blob entity) 互換読み込み path 用、 新規書き出しでは使わない。
const LEGACY_SCHEMA_META_HIMO: &str = "__enchu_schema_meta__";
const LEGACY_SCHEMA_MARKER: &str = "__enchu_schema_v1__";
const LEGACY_SCHEMA_BLOB_HIMO: &str = "__enchu_schema_blob";

/// 列の型。
///
/// - `Number` — inline 数値 (ValueType::Number)。 0 以上 `u32::MAX` 未満
/// - `BigInt` — 64 bit 整数 (ValueType::Number64)。 負の数、 ms / µs の時刻、 64 bit の ID。 値域は
///   [`BIGINT_MIN`]`..=`[`BIGINT_MAX`] (`i64::MAX` だけは空の印と重なるので使えない)。 engine には大小の順を
///   保つ符号化 (`v ^ 2^63`) の u64 で置くので、 範囲・並び・上位 k 件がそのまま効く
/// - `Tag` — 共有タグ、vocab 経由 (ValueType::Tag)。enum / カテゴリ / 名前など引かれる値向き
/// - `Leaf` — 終端タグ、FreeStore 経由 (ValueType::Leaf)。備考 / 本文など引かれない自由記述向き
/// - `Ref` — 他テーブル entity への参照 (ValueType::Ref)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Number,
    Tag,
    Leaf,
    Ref,
    BigInt,
}

/// `ColumnType::BigInt` の最小値。
pub const BIGINT_MIN: i64 = i64::MIN;
/// `ColumnType::BigInt` の最大値 (`i64::MAX` は符号化すると engine の空の印 `u64::MAX` になる)。
pub const BIGINT_MAX: i64 = i64::MAX - 1;

/// BigInt の値 → engine の u64 (大小の順を保つ: 符号 bit を反転)。 値域外 (`i64::MAX`) は None。
fn big_raw(v: i64) -> Option<u64> {
    (v <= BIGINT_MAX).then_some((v as u64) ^ (1 << 63))
}

/// engine の u64 → BigInt の値。
fn big_val(raw: u64) -> i64 {
    (raw ^ (1 << 63)) as i64
}

/// 範囲の条件の値 (整数だけ)。
fn num(v: Value) -> Option<i64> {
    match v {
        Value::Number(n) => Some(n),
        _ => None,
    }
}

/// 列 `cd` への等値の値 → engine の値。 列の値域に入らない / 型が合わなければ None (Tag は vocab を引く
/// 呼び側で)。
fn eq_raw(cd: &ColumnInner, v: &Value) -> Option<u64> {
    match (cd.ty, v) {
        (ColumnType::Number, Value::Number(n)) if *n >= 0 && (*n as u64) < u32::MAX as u64 => Some(*n as u64),
        (ColumnType::BigInt, Value::Number(n)) => big_raw(*n),
        (ColumnType::Ref, Value::Ref(eid)) => Some(enchudb_oplog::eid_local(*eid) as u64),
        _ => None,
    }
}

/// `where_in` / `where_not_in` の値 (u32) → engine の値 (昇順・重複なし)。
fn in_raw(cd: &ColumnInner, values: &[u32]) -> Vec<u64> {
    let mut raw: Vec<u64> = values
        .iter()
        .map(|&v| if cd.ty == ColumnType::BigInt { big_raw(v as i64).expect("u32 は値域内") } else { v as u64 })
        .collect();
    raw.sort_unstable();
    raw.dedup();
    raw
}

impl ColumnType {
    fn value_type(self) -> ValueType {
        match self {
            ColumnType::Number => ValueType::Number,
            ColumnType::BigInt => ValueType::Number64,
            ColumnType::Tag => ValueType::Tag,
            ColumnType::Leaf => ValueType::Leaf,
            ColumnType::Ref => ValueType::Ref,
        }
    }
    fn tag(self) -> &'static str {
        // 永続化 tag — 既存 DB 互換のため Number="I", Tag="T", Ref="R" は据え置き。
        // 新規 Leaf="L"。
        match self {
            ColumnType::Number => "I",
            ColumnType::Tag => "T",
            ColumnType::Leaf => "L",
            ColumnType::Ref => "R",
            ColumnType::BigInt => "B",
        }
    }
    fn from_tag(s: &str) -> Option<Self> {
        match s {
            "I" => Some(ColumnType::Number),
            "T" => Some(ColumnType::Tag),
            "L" => Some(ColumnType::Leaf),
            "R" => Some(ColumnType::Ref),
            "B" => Some(ColumnType::BigInt),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Number(i64),
    Text(String),
    Ref(EntityId),
}

impl From<i64> for Value { fn from(v: i64) -> Self { Value::Number(v) } }
impl From<i32> for Value { fn from(v: i32) -> Self { Value::Number(v as i64) } }
impl From<u32> for Value { fn from(v: u32) -> Self { Value::Number(v as i64) } }
impl From<&str> for Value { fn from(v: &str) -> Self { Value::Text(v.to_string()) } }
impl From<String> for Value { fn from(v: String) -> Self { Value::Text(v) } }
impl From<&String> for Value { fn from(v: &String) -> Self { Value::Text(v.clone()) } }

#[derive(Debug)]
pub enum SchemaError {
    Io(String),
    UnknownColumn(String),
    UnknownTable(String),
    TypeMismatch(String),
    BadValue(String),
    DuplicatePk,
    Parse(String),
    /// 0.9.0 (#73): 既存 table への再宣言が on-disk schema と矛盾する
    /// (on-disk 列の欠落 / 型不一致 / 並べ替え / PK 不一致)。 silent に進めると
    /// data loss になるため loud に落とす。
    SchemaConflict(String),
    /// 内部不整合 (himo_id 解決失敗など、 通常起こらない)
    Internal(String),
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SchemaError::Io(s) => write!(f, "io: {s}"),
            SchemaError::UnknownColumn(s) => write!(f, "unknown column: {s}"),
            SchemaError::UnknownTable(s) => write!(f, "unknown table: {s}"),
            SchemaError::TypeMismatch(s) => write!(f, "type mismatch: {s}"),
            SchemaError::BadValue(s) => write!(f, "bad value: {s}"),
            SchemaError::DuplicatePk => write!(f, "duplicate primary key"),
            SchemaError::Parse(s) => write!(f, "parse: {s}"),
            SchemaError::SchemaConflict(s) => write!(f, "schema conflict: {s}"),
            SchemaError::Internal(s) => write!(f, "internal: {s}"),
        }
    }
}

impl std::error::Error for SchemaError {}

#[derive(Debug, Clone)]
struct ColumnInner {
    name: String,
    ty: ColumnType,
    himo_name: String,
    himo_id: u16,
}

#[derive(Debug, Clone)]
struct RelationInner {
    from_col: String,
    to_table: String,
}

#[derive(Debug)]
struct TableInner {
    name: String,
    /// `__enchu_table` の Symbol vocab に登録した table 名の vocab_id。
    /// テーブル所属判定の超高速 path。
    table_vid: u32,
    cols: Vec<ColumnInner>,
    /// pk の `cols` index。 PK 未指定なら None。
    pk: Option<usize>,
    relations: Vec<RelationInner>,
    /// #60: PK upsert の lookup→allocate を直列化する per-table lock。
    /// 並行 mode で 2 thread が同一 PK を upsert したとき、 両方が「PK 不在」を
    /// 観測 → 両方 entity_in で別 eid を払い出す TOCTOU で重複行ができるのを防ぐ。
    /// per-table なので別 table の upsert は並行のまま。
    upsert_lock: std::sync::Mutex<()>,
}

impl TableInner {
    fn col(&self, name: &str) -> Option<&ColumnInner> {
        self.cols.iter().find(|c| c.name.eq_ignore_ascii_case(name))
    }
    fn col_or_err(&self, name: &str) -> Result<&ColumnInner, SchemaError> {
        self.col(name).ok_or_else(|| SchemaError::UnknownColumn(name.to_string()))
    }
}

/// #73: `TableInner` は `Arc` 共有の immutable snapshot なので、 schema 変更
/// (column 追加 / PK 補完) は copy-on-write で新 Arc に差し替える。 name /
/// table_vid は `src` から引き継ぎ、 upsert_lock は新規 (= lock 状態は引き継が
/// ない、 migration は書き込み静止時に行う前提)。
fn rebuild_table_inner(
    src: &TableInner,
    cols: Vec<ColumnInner>,
    pk: Option<usize>,
    relations: Vec<RelationInner>,
) -> Arc<TableInner> {
    Arc::new(TableInner {
        name: src.name.clone(),
        table_vid: src.table_vid,
        cols,
        pk,
        relations,
        upsert_lock: std::sync::Mutex::new(()),
    })
}

/// EnchuDB 上の virtual-table database。 schema 定義 + 永続化を担う。
///
/// 内部表現は `Arc<Engine>`。 build phase (Arc 共有前) は `Arc::get_mut` 経由で
/// `&mut Engine` を取り、 `define_himo` 等の schema 拡張ができる。 `finish_with_oplog`
/// 等で consumer thread を spawn して runtime phase に遷移すると、 以降は
/// `&Engine` 経由の API (tie_to / tie_text_to / query / oplog_sync) のみ。
pub struct Database {
    eng: Arc<Engine>,
    tables: Vec<Arc<TableInner>>,
    /// true なら consumer thread が走ってる (concurrent モード、 &mut 不可)。
    is_concurrent: bool,
}

impl Drop for Database {
    fn drop(&mut self) {
        // single-thread モード (Arc 単一所有 + consumer なし) のみ自前で flush。
        // concurrent モードは consumer thread の shutdown sync に委ねる。
        //
        // 0.8.2: schema sidecar の persist は build phase で coalesce してるので、
        // finish_* を呼ばずに drop された path でも schema が disk に残るよう、
        // ここで persist_schema を呼ぶ。 finish_* 経由は ManuallyDrop で Drop が
        // 走らないので二重 persist にはならない。
        // open_readonly Database は engine が read-only なので persist 不要 / 不可。
        // 0.8.7: tables 0 (= 空 Database が即 drop) の場合は sidecar 書き出しは
        // skip (= 不必要な I/O を回避、 引いては growable backing で msync が
        // SIGBUS する可能性も避ける)。
        // #117: builder の `self.tables` ではなく engine 実態の user table 有無で
        // 判定する。 raw `engine_mut().define_table()` で作った table は builder の
        // `self.tables` に載らないため、 旧条件だと「空 Database」と誤認して sidecar
        // 未永続 → reopen で table 消失 / next_local 巻き戻り → 生きた eid 再払出
        // (silent 破壊) を招いていた。 user table が実在する DB は region commit 済
        // なので、 skip が避けていた「空 growable の msync SIGBUS」経路には入らない。
        let has_user_tables =
            !self.tables.is_empty() || !self.eng.list_user_tables().is_empty();
        if !self.is_concurrent && !self.eng.is_readonly() && has_user_tables {
            self.eng.set_defer_tables_persist(false);
            let _ = self.persist_schema();
        }
    }
}

impl Database {
    pub fn create(path: &str) -> Result<Self, SchemaError> {
        let eng = Engine::create_standalone(path).map_err(|e| SchemaError::Io(e.to_string()))?;
        Self::wrap_new(eng)
    }
    pub fn create_compact(path: &str) -> Result<Self, SchemaError> {
        let eng = Engine::create_compact(path).map_err(|e| SchemaError::Io(e.to_string()))?;
        Self::wrap_new(eng)
    }
    /// eager (非 growable) backing の capacity 指定版。 `create_growable_with_capacity`
    /// と同じ layout を **最初に全部確保**して開く。 growable が使えない platform
    /// (非 unix) 向け。 sparse を自動でやらない FS (NTFS 既定) では見かけサイズが
    /// そのまま実消費になる点に注意。
    pub fn create_with_capacity(path: &str, max_entities: u32) -> Result<Self, SchemaError> {
        let eng = Engine::create_with_capacity(path, max_entities)
            .map_err(|e| SchemaError::Io(e.to_string()))?;
        Self::wrap_new(eng)
    }
    pub fn create_growable(path: &str) -> Result<Self, SchemaError> {
        let eng = Engine::create_growable(path).map_err(|e| SchemaError::Io(e.to_string()))?;
        Self::wrap_new(eng)
    }
    /// `create_growable` の max_entities を絞る版。 default の 16 M は
    /// layout.total_size ~25 GB の PROT_NONE 予約を発生させ、 process exit 時の
    /// munmap teardown を重くする (issue2)。 想定 row 数が判ってる app は
    /// ここで絞ると VSZ も apparent file size も大幅に縮む。
    ///
    /// 目安: max_entities=65_536 で layout ~1.3 GB、 max_entities=1_048_576 で
    /// layout ~2.6 GB。 default は 16_777_216 = 25 GB。
    pub fn create_growable_with_capacity(path: &str, max_entities: u32) -> Result<Self, SchemaError> {
        let eng = Engine::create_growable_with_capacity(path, max_entities)
            .map_err(|e| SchemaError::Io(e.to_string()))?;
        Self::wrap_new(eng)
    }
    pub fn create_growable_tiny(path: &str) -> Result<Self, SchemaError> {
        let eng = Engine::create_growable_tiny(path).map_err(|e| SchemaError::Io(e.to_string()))?;
        Self::wrap_new(eng)
    }

    /// growable backing で開く。`max_entities` と `vocab_data_size` を明示。
    ///
    /// 大規模 Leaf text を持つアプリで `create_growable*` 系の default 512 MiB
    /// vocab cap に当たる場合に使う。目安: 1 KB / row × 1 M rows ≒ 1 GiB
    /// (Leaf 列の値も vocab に積まれるため本文総量で見積もる)。
    pub fn create_growable_with_options(
        path: &str,
        max_entities: u32,
        vocab_data_size: usize,
    ) -> Result<Self, SchemaError> {
        let eng = Engine::create_growable_with_options(path, max_entities, vocab_data_size)
            .map_err(|e| SchemaError::Io(e.to_string()))?;
        Self::wrap_new(eng)
    }

    /// growable backing + **Leaf データ領域サイズ**を明示。 大量の Leaf text
    /// (chunk 本文 / tool 出力 / 長文備考など) を持つアプリで default 512 MiB の
    /// leaf 領域 cap に当たる場合に使う。 leaf 領域は create 時に固定確保 (auto-grow
    /// しない) なので、 本文総量から余裕を持って見積もる。 `leaf_scale` は既定
    /// `Gb16` (= leaf_data_size は 16 GiB まで指定可)。
    pub fn create_growable_with_leaf(
        path: &str,
        max_entities: u32,
        leaf_data_size: usize,
    ) -> Result<Self, SchemaError> {
        let eng = Engine::create_growable_with_leaf(
            path,
            max_entities,
            None,
            Some(leaf_data_size),
            enchudb_engine::LeafScale::Gb16,
        )
        .map_err(|e| SchemaError::Io(e.to_string()))?;
        Self::wrap_new(eng)
    }

    /// #118: 全 layout knob を `GrowableOptions` で指定して growable DB を作る一本化 API。
    /// `create_growable_with_capacity` / `_with_options` / `_with_leaf` が出し損ねていた
    /// `max_himos` / `content_data_size` / `cyl_max_values` もここから設定でき、 knob の
    /// 組合せも自由。 気にする knob だけ struct-update で:
    ///
    /// ```ignore
    /// let db = Database::create_growable_with(
    ///     path,
    ///     GrowableOptions { max_entities: 4_000_000, max_himos: 8192, ..Default::default() },
    /// )?;
    /// ```
    pub fn create_growable_with(path: &str, opts: GrowableOptions) -> Result<Self, SchemaError> {
        let eng = Engine::create_growable_opts(path, opts)
            .map_err(|e| SchemaError::Io(e.to_string()))?;
        Self::wrap_new(eng)
    }

    fn wrap_new(eng: Engine) -> Result<Self, SchemaError> {
        // 0.8.2: build phase の sidecar fsync を coalesce (= define_table /
        // define_himo_in が毎回呼ぶ try_persist_tables を no-op 化)。
        // finish_* / Drop で false に戻して 1 回 explicit fsync。
        eng.set_defer_tables_persist(true);
        Ok(Self {
            eng: Arc::new(eng),
            tables: Vec::new(),
            is_concurrent: false,
        })
    }

    /// 読み取り専用で開く。 writer lock を取らないので、 別 process が
    /// writer として開いていても並行 open 可能。 書き込み API は panic する。
    /// GUI の表示専用 process、 監視ツール等の用途。
    ///
    /// 0.8.7: schema sidecar (`{path}.schema`) があれば PK / column type 含む
    /// 完全な schema を復元。 sidecar が無い engine 直 DB (= mlbpulse のような
    /// `Engine::define_table` 構築) でも、 engine の `.tables` sidecar + value_types
    /// から fallback 復元 (= PK は不明扱い、 column type は value_type から推定)。
    pub fn open_readonly(path: &str) -> Result<Self, SchemaError> {
        let eng = Engine::open_readonly(path).map_err(|e| SchemaError::Io(e.to_string()))?;
        let mut db = Self {
            eng: Arc::new(eng),
            tables: Vec::new(),
            is_concurrent: false,
        };
        db.load_schema()?;
        Ok(db)
    }

    pub fn open(path: &str) -> Result<Self, SchemaError> {
        let eng = Engine::open_standalone(path).map_err(|e| SchemaError::Io(e.to_string()))?;
        let mut db = Self {
            eng: Arc::new(eng),
            tables: Vec::new(),
            is_concurrent: false,
        };
        db.load_schema()?;
        Ok(db)
    }

    /// 既存 DB を WAL 有効な concurrent モードで開く。 WAL があれば自動 recover。
    /// schema は blob から復元 + himo は engine 自体に保存済みなので追加 define 不要。
    /// 返り値は `Arc<Database>` — 全 thread / sub-store で clone 共有する用。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn open_with_oplog(path: &str, oplog_capacity: usize) -> Result<Arc<Self>, SchemaError> {
        let arc_eng = Engine::open_concurrent_with_oplog(path, oplog_capacity)
            .map_err(|e| SchemaError::Io(e.to_string()))?;
        Self::wrap_concurrent(arc_eng)
    }

    /// #116: `open_with_oplog` + write/oplog-record queue の capacity 指定。
    /// 多 DB を LRU pool で open/close する hosted 構成の open 側 knob (説明は
    /// `finish_with_oplog_with_queue` 参照)。
    pub fn open_with_oplog_with_queue(
        path: &str,
        oplog_capacity: usize,
        queue_capacity: usize,
    ) -> Result<Arc<Self>, SchemaError> {
        let arc_eng =
            Engine::open_concurrent_with_oplog_queue(path, oplog_capacity, queue_capacity)
                .map_err(|e| SchemaError::Io(e.to_string()))?;
        Self::wrap_concurrent(arc_eng)
    }

    fn wrap_concurrent(arc_eng: Arc<Engine>) -> Result<Arc<Self>, SchemaError> {
        // 0.8.7: schema sidecar / engine `.tables` から復元 (= marker himo は不要)。
        // mlbpulse のような engine 直構築 DB でも fallback 復元できる。
        let mut db = Self {
            eng: arc_eng,
            tables: Vec::new(),
            is_concurrent: true,
        };
        db.load_schema()?;
        Ok(Arc::new(db))
    }

    /// build phase 終了 + concurrent + WAL モードに遷移。 consumer thread を spawn し、
    /// `Arc<Database>` を返す。 sinfo のように複数の sub-store で `Arc<Database>` を
    /// clone 共有する用途向け。
    ///
    /// 失敗条件: `self` が既に `Arc<Database>` 経由で共有されている (= Arc count > 1)、
    /// もしくは WAL ファイル作成 / consumer 起動が失敗。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn finish_with_oplog(mut self, oplog_capacity: usize) -> Result<Arc<Self>, SchemaError> {
        // 0.8.2: schema sidecar を build phase で coalesce してるので、 ここで
        // 1 度だけ persist する (= N table 分の fsync を 1 回に圧縮)。
        // engine 側の sidecar fsync 抑止も解除して persist_schema -> eng.flush
        // 経由で try_persist_tables が走るようにする。
        self.eng.set_defer_tables_persist(false);
        self.persist_schema()?;
        let (eng, tables) = self.into_parts()?;
        let arc_eng = Engine::concurrentize_with_oplog(eng, oplog_capacity)
            .map_err(|e| SchemaError::Io(e.to_string()))?;
        Ok(Arc::new(Self {
            eng: arc_eng,
            tables,
            is_concurrent: true,
        }))
    }

    /// #116: `finish_with_oplog` + write/oplog-record queue の capacity 指定。
    ///
    /// queue は engine ごとに 2 本 eager 確保され、default 1M slot だと per-DB
    /// ~128MiB の固定 RSS になる (低 write / 多 DB 同居の host では 4096〜16384
    /// 程度で十分)。省略版 `finish_with_oplog` は `max_entities` 連動の scaled
    /// default (小 DB は自動で小さくなる)。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn finish_with_oplog_with_queue(
        mut self,
        oplog_capacity: usize,
        queue_capacity: usize,
    ) -> Result<Arc<Self>, SchemaError> {
        self.eng.set_defer_tables_persist(false);
        self.persist_schema()?;
        let (eng, tables) = self.into_parts()?;
        let arc_eng = Engine::concurrentize_with_oplog_queue(eng, oplog_capacity, queue_capacity)
            .map_err(|e| SchemaError::Io(e.to_string()))?;
        Ok(Arc::new(Self {
            eng: arc_eng,
            tables,
            is_concurrent: true,
        }))
    }

    /// build phase 終了 + consumer thread spawn (concurrent)、 WAL なし。
    /// crash consistency 不要 (cache / 揮発 store) なケース向け。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn finish_concurrent(mut self) -> Result<Arc<Self>, SchemaError> {
        // 0.8.2: build phase で coalesce した schema sidecar を 1 度 persist。
        self.eng.set_defer_tables_persist(false);
        self.persist_schema()?;
        let (eng, tables) = self.into_parts()?;
        let arc_eng = Engine::concurrentize(eng);
        Ok(Arc::new(Self {
            eng: arc_eng,
            tables,
            is_concurrent: true,
        }))
    }

    /// #116: `finish_concurrent` + queue capacity 指定 (説明は
    /// `finish_with_oplog_with_queue` 参照)。
    #[cfg(not(target_arch = "wasm32"))]
    pub fn finish_concurrent_with_queue(
        mut self,
        queue_capacity: usize,
    ) -> Result<Arc<Self>, SchemaError> {
        self.eng.set_defer_tables_persist(false);
        self.persist_schema()?;
        let (eng, tables) = self.into_parts()?;
        let arc_eng = Engine::concurrentize_queue(eng, queue_capacity);
        Ok(Arc::new(Self {
            eng: arc_eng,
            tables,
            is_concurrent: true,
        }))
    }

    /// `self.eng` から `Engine` を取り出す helper (Arc count = 1 が前提)。
    /// `ManuallyDrop` 経由でフィールドを steal、 Database の Drop は走らない。
    fn into_parts(self) -> Result<(Engine, Vec<Arc<TableInner>>), SchemaError> {
        use std::mem::ManuallyDrop;
        let mut me = ManuallyDrop::new(self);
        // 1 度 flush して mmap を sync (concurrent 化前の最終状態を確定)
        if let Some(eng_mut) = Arc::get_mut(&mut me.eng) {
            eng_mut.flush().map_err(|e| SchemaError::Io(e.to_string()))?;
        } else {
            return Err(SchemaError::Internal(
                "Database already shared via Arc — call finish_* before Arc-clone".into()
            ));
        }
        // SAFETY: ManuallyDrop 化したので元のフィールドは drop されない。
        // 各フィールドは ptr::read で moveout、 me 自体は ManuallyDrop なので
        // 後で drop されず leak しない (フィールドは個別に管理)。
        let eng_arc = unsafe { std::ptr::read(&me.eng) };
        let tables = unsafe { std::ptr::read(&me.tables) };
        let eng = Arc::try_unwrap(eng_arc).map_err(|_| {
            SchemaError::Internal("unexpected Arc strong_count > 1 after get_mut succeeded".into())
        })?;
        Ok((eng, tables))
    }

    pub fn engine(&self) -> &Engine { &self.eng }

    /// まだどの table にも割り当てていない eid 空間。 追加の table を
    /// `with_capacity` で切り出す前にここを見る。
    ///
    /// `max_entities` は create 時に header へ焼かれるので、 超える `with_capacity`
    /// は `build()` が `Err` になる (黙って縮めない — 頼んだ枠と違う table が
    /// できる方が事故になる)。
    pub fn remaining_eid_capacity(&self) -> u32 { self.eng.remaining_eid_capacity() }

    /// table の eid 枠の使用状況 (未定義 table は `None`)。 枠は create 時に固定で
    /// 後から伸ばせないので、 **満杯にする前に気付く**のがアプリ側の防御になる。
    pub fn table_eid_usage(&self, name: &str) -> Option<TableEidUsage> {
        self.eng.table_eid_usage(name)
    }

    /// `Arc<Engine>` を clone して返す。 engine 直接アクセス / 他 component との共有用。
    pub fn arc_engine(&self) -> Arc<Engine> { self.eng.clone() }

    /// 購読の束を作る。 束に [`LiveGroup::add`] した購読のうち、 前回から出入りのあったものの差分だけを
    /// [`LiveGroup::poll`] でまとめて受け取る (コストは購読の数でなく出入りの数に比例、 購読が数千本
    /// ある時向け)。 束に入れていない購読 (同じ `Database` を使う他の部品が持つもの) の差分は取り出さない。
    pub fn live_group(&self) -> LiveGroup {
        LiveGroup { inner: self.eng.live_group(), eng: self.eng.clone() }
    }

    /// build phase 用、 `Arc<Engine>` が他に共有されていない (count = 1) 時のみ
    /// `&mut Engine` を返す。 concurrent モード遷移後は常に None。
    pub fn engine_mut(&mut self) -> Option<&mut Engine> {
        if self.is_concurrent { return None; }
        Arc::get_mut(&mut self.eng)
    }

    /// 現在 concurrent モードか (consumer thread が走ってるか)。
    pub fn is_concurrent(&self) -> bool { self.is_concurrent }

    /// 0.7.0 (Phase 3): sync 用 reserved table (`_sync_ops` / `_sync_peers`) を
    /// engine に追加する。 build phase で呼ぶこと (= Arc 単一所有のうち)、
    /// `finish_with_oplog` 後は Arc 共有なので呼べない (= `SchemaError::Internal`)。
    ///
    /// idempotent: 既に有効化済みなら何もしない。 sync が要らない単独 DB は
    /// 呼ばなくて OK (= reserved table が物理的に存在しない、 eid 空間も浪費しない)。
    /// 一度有効化すると無効化は不可。
    pub fn enable_sync(&mut self) -> Result<(), SchemaError> {
        let eng_mut = Arc::get_mut(&mut self.eng).ok_or_else(|| {
            SchemaError::Internal(
                "Database already shared via Arc — call enable_sync before finish_*".into()
            )
        })?;
        eng_mut.enable_sync_tables()
            .map_err(|e| SchemaError::Internal(format!("enable_sync_tables: {e}")))
    }

    /// 0.7.0: sync table が有効化済みか (`enable_sync` 後 / open 時に既存)。
    pub fn sync_enabled(&self) -> bool {
        self.eng.sync_tables_enabled()
    }

    /// 新規 table 定義 builder。
    pub fn table<'a>(&'a mut self, name: &str) -> TableBuilder<'a> {
        TableBuilder {
            db: self,
            name: name.to_string(),
            cols: Vec::new(),
            pk: None,
            relations: Vec::new(),
            capacity: None,
            local_only: false,
        }
    }

    /// 既存 table の handle を取得。 未定義なら None。
    pub fn get_table<'a>(&'a self, name: &str) -> Option<Table<'a>> {
        self.tables.iter()
            .find(|t| t.name.eq_ignore_ascii_case(name))
            .map(|t| Table { db: self, inner: t.clone() })
    }

    /// 既存 table に column を追加。 同名 column が既にあれば idempotent に成功で
    /// 返る。 himo を新規 define して schema sidecar を書き戻すので、 以降の再 open
    /// で復元される。
    ///
    /// 0.9.0 (#73): himo 定義を `Engine::ensure_himo_dynamic_in` (`&self`,
    /// idempotent) に置き換えたので、 standalone (`Database::open`) だけでなく
    /// concurrent (`open_with_oplog` / `finish_*` 後) の Database でも呼べる。
    /// concurrent 側は `Arc<Database>` の単一所有時に `Arc::get_mut` 経由で
    /// `&mut Database` を取って呼ぶ (= open 直後の migration window)。
    pub fn add_column(
        &mut self,
        table_name: &str,
        col_name: &str,
        ty: ColumnType,
    ) -> Result<(), SchemaError> {
        self.add_column_to_table(table_name, col_name, ty, 0, None)?;
        self.persist_schema()?;
        Ok(())
    }

    /// #73: 既存 table への trailing column 追加の共通本体。 `add_column` と
    /// `TableBuilder::build` の auto-migrate の両方から呼ぶ。
    ///
    /// himo 定義は `Engine::ensure_himo_dynamic_in` (`&self`, `himo_def_lock` で
    /// 直列化 + idempotent) 経由なので standalone / concurrent 両モードで動く。
    /// Ref column (relation 付き) だけは `define_ref_in` が `&mut Engine` を
    /// 要求するため build phase (Arc 単一所有) 限定。
    ///
    /// schema sidecar の persist は **しない** — caller が末尾で 1 回
    /// `persist_schema` を呼ぶこと (N 列追加 = 1 fsync に coalesce)。
    fn add_column_to_table(
        &mut self,
        table_name: &str,
        col_name: &str,
        ty: ColumnType,
        cardinality: u32,
        ref_to: Option<&str>,
    ) -> Result<(), SchemaError> {
        if self.eng.is_readonly() {
            return Err(SchemaError::Internal(
                "cannot add column on read-only Database (open_readonly)".into()
            ));
        }
        let table_inner = self.find_table_inner(table_name)
            .ok_or_else(|| SchemaError::UnknownTable(table_name.to_string()))?;
        if table_inner.col(col_name).is_some() {
            return Ok(()); // idempotent: 同名 column 既存
        }
        validate_column_name(col_name)?;

        let hid: u16 = match ref_to {
            Some(to_table) => {
                // relation 先 table の存在チェック (self-ref は to_table == 自分で許可)
                if to_table != table_inner.name && self.find_table_inner(to_table).is_none() {
                    return Err(SchemaError::UnknownTable(to_table.to_string()));
                }
                let to_table = to_table.to_string();
                let table_name_owned = table_inner.name.clone();
                let eng_mut = Arc::get_mut(&mut self.eng).ok_or_else(|| {
                    SchemaError::Internal(
                        "Ref column addition requires exclusive Engine access — \
                         add ref columns on a standalone Database (build phase), \
                         not on a shared / concurrent one".into()
                    )
                })?;
                let hid = eng_mut.define_ref_in(&table_name_owned, col_name, &to_table)
                    .map_err(|e| SchemaError::Internal(format!(
                        "define_ref_in({table_name_owned}.{col_name} -> {to_table}) failed: {e}"
                    )))?;
                hid as u16
            }
            None => self.eng
                .ensure_himo_dynamic_in(&table_inner.name, col_name, ty.value_type(), cardinality)
                .map_err(|e| SchemaError::Internal(format!(
                    "ensure_himo_dynamic_in({}.{col_name}) failed: {e}", table_inner.name
                )))?,
        };

        let mut new_cols = table_inner.cols.clone();
        new_cols.push(ColumnInner {
            name: col_name.to_string(),
            ty,
            himo_name: format!("{}.{}", table_inner.name, col_name),
            himo_id: hid,
        });
        let mut new_relations = table_inner.relations.clone();
        if let Some(to_table) = ref_to {
            new_relations.push(RelationInner {
                from_col: col_name.to_string(),
                to_table: to_table.to_string(),
            });
        }
        let new_inner = rebuild_table_inner(&table_inner, new_cols, table_inner.pk, new_relations);
        let pos = self.tables.iter().position(|t| t.name.eq_ignore_ascii_case(table_name))
            .expect("table_inner found above");
        self.tables[pos] = new_inner;
        Ok(())
    }

    /// #73 (G1): 既存 table への再宣言の diff 検査 + trailing auto-migrate。
    ///
    /// - on-disk cols が declared の **prefix** (名前 + 型が順序込み一致) なら、
    ///   残りの trailing 新列を `add_column` 機構で自動追加する
    /// - 完全一致 (trailing なし) なら何もせず既存 handle を返す (従来の idempotent)
    /// - それ以外 (on-disk 列の欠落 / 型不一致 / 並べ替え / PK・relation 不一致)
    ///   は `SchemaError::SchemaConflict` で loud に落とす — 従来はここで宣言を
    ///   黙って捨てていて、 後続の `set("newcol", ...)` が UnknownColumn になるか
    ///   error swallow で silent data loss になっていた
    fn migrate_existing_table(
        &mut self,
        existing: Arc<TableInner>,
        col_specs: &[(String, ColumnType, u32)],
        pk: Option<&str>,
        relations: &[(String, String)],
    ) -> Result<Arc<TableInner>, SchemaError> {
        let tname = existing.name.clone();

        // 1. on-disk cols は declared の prefix でなければならない
        if col_specs.len() < existing.cols.len() {
            let declared: Vec<&str> = col_specs.iter().map(|(n, _, _)| n.as_str()).collect();
            let missing: Vec<&str> = existing.cols.iter()
                .filter(|ec| !declared.iter().any(|d| d.eq_ignore_ascii_case(&ec.name)))
                .map(|ec| ec.name.as_str())
                .collect();
            return Err(SchemaError::SchemaConflict(format!(
                "table {tname}: declaration is missing on-disk column(s) {missing:?} \
                 (declared: {declared:?}) — re-declare all existing columns in order, \
                 new columns must be trailing"
            )));
        }
        for (i, ec) in existing.cols.iter().enumerate() {
            let (dn, dt, _) = &col_specs[i];
            if !dn.eq_ignore_ascii_case(&ec.name) {
                let reordered = col_specs.iter().any(|(n, _, _)| n.eq_ignore_ascii_case(&ec.name));
                return Err(SchemaError::SchemaConflict(if reordered {
                    format!(
                        "table {tname}: declared columns are reordered — on-disk column #{i} \
                         is {:?} but declaration has {dn:?} there (existing columns must keep \
                         their on-disk order, new columns must be trailing)",
                        ec.name
                    )
                } else {
                    format!(
                        "table {tname}: declaration is missing on-disk column {:?} \
                         (declaration has {dn:?} at position {i}) — re-declare all existing \
                         columns in order",
                        ec.name
                    )
                }));
            }
            if *dt != ec.ty {
                return Err(SchemaError::SchemaConflict(format!(
                    "table {tname} column {dn:?}: declared as {dt:?} but on-disk type is {:?}",
                    ec.ty
                )));
            }
        }

        // 2. PK: 双方 Some で不一致は conflict。 declared None は既存維持。
        //    既存 None + declared Some は採用 (= synthesize 復元 DB の PK 補完)。
        let existing_pk = existing.pk.map(|i| existing.cols[i].name.clone());
        if let (Some(p), Some(ep)) = (pk, existing_pk.as_deref()) {
            if !p.eq_ignore_ascii_case(ep) {
                return Err(SchemaError::SchemaConflict(format!(
                    "table {tname}: declared primary key {p:?} but on-disk primary key is {ep:?}"
                )));
            }
        }

        // 3. 既存 (prefix) 列に対する relation 宣言は on-disk relation と一致必須
        for (from, to) in relations {
            if existing.col(from).is_none() { continue; } // trailing 新列は後段で追加
            match existing.relations.iter().find(|r| r.from_col.eq_ignore_ascii_case(from)) {
                Some(r) if r.to_table.eq_ignore_ascii_case(to) => {}
                Some(r) => return Err(SchemaError::SchemaConflict(format!(
                    "table {tname}: column {from:?} declared as ref to {to:?} but on-disk \
                     ref targets {:?}", r.to_table
                ))),
                None => return Err(SchemaError::SchemaConflict(format!(
                    "table {tname}: column {from:?} declared as ref to {to:?} but on-disk \
                     column has no relation"
                ))),
            }
        }

        // 4. trailing 新列を auto-migrate (#73 の本命)。 himo は
        //    ensure_himo_dynamic_in (&self) 経由なので concurrent でも通る。
        let mut changed = false;
        for (cn, ct, card) in &col_specs[existing.cols.len()..] {
            let ref_target = relations.iter()
                .find(|(c, _)| c.eq_ignore_ascii_case(cn))
                .map(|(_, t)| t.as_str());
            self.add_column_to_table(&tname, cn, *ct, *card, ref_target)?;
            changed = true;
        }

        // 5. PK 補完 (既存 None → declared Some)。 trailing 新列を PK にも出来る。
        if existing_pk.is_none() {
            if let Some(p) = pk {
                let cur = self.find_table_inner(&tname).expect("table present");
                let idx = cur.cols.iter().position(|c| c.name.eq_ignore_ascii_case(p))
                    .ok_or_else(|| SchemaError::UnknownColumn(p.to_string()))?;
                if cur.pk != Some(idx) {
                    let updated = rebuild_table_inner(
                        &cur, cur.cols.clone(), Some(idx), cur.relations.clone(),
                    );
                    let pos = self.tables.iter()
                        .position(|t| t.name.eq_ignore_ascii_case(&tname))
                        .expect("table present");
                    self.tables[pos] = updated;
                    changed = true;
                }
            }
        }

        if changed {
            self.persist_schema()?;
        }
        Ok(self.find_table_inner(&tname).expect("table present after migration"))
    }

    /// 全 table を列挙。
    pub fn list_tables(&self) -> Vec<TableInfo> {
        self.tables.iter().map(|t| TableInfo {
            name: t.name.clone(),
            columns: t.cols.iter().map(|c| ColumnInfo {
                name: c.name.clone(),
                ty: c.ty,
                is_pk: t.pk.map(|i| t.cols[i].name == c.name).unwrap_or(false),
                ref_to: t.relations.iter().find(|r| r.from_col == c.name).map(|r| r.to_table.clone()),
            }).collect(),
        }).collect()
    }

    // ───── Scope ─────
    //
    // table 名前空間の prefix レンズ。 physical layout (table cluster vs DB ファイル)
    // を隠して、 deployment が centralized (pattern A) でも distributed
    // (pattern B / C) でも同一 app code で書けるようにする抽象。 multi-tenant は
    // この機構のユースケースの 1 つ (詳細は issue #12、 rename 経緯は issue #24)。

    /// 名前付き scope の read レンズを取り出す。 prefix は `{name}.` (例: `alice`)。
    /// scope 内で `get_table("users")` は実 table `alice.users` に解決される。
    pub fn scope<'a>(&'a self, name: &str) -> Scope<'a> {
        Scope { db: self, prefix: Some(name.to_string()) }
    }

    /// 名前付き scope の build レンズを取り出す。 `table(...)` で建てる table は
    /// 自動で `{name}.` prefix が付与される。
    pub fn scope_mut<'a>(&'a mut self, name: &str) -> ScopeMut<'a> {
        ScopeMut { db: self, prefix: Some(name.to_string()) }
    }

    /// 絞りなしで全体を同じレンズ型に (= prefix 無し)。 pattern B の
    /// `Database::open` 直後に scope 型で扱いたい時に使う、 全 table が見える。
    pub fn as_scope<'a>(&'a self) -> Scope<'a> {
        Scope { db: self, prefix: None }
    }

    /// 絞りなしの build レンズ。 prefix 無しで table を建てる、 pattern B の builder。
    pub fn as_scope_mut<'a>(&'a mut self) -> ScopeMut<'a> {
        ScopeMut { db: self, prefix: None }
    }

    fn find_table_inner(&self, name: &str) -> Option<Arc<TableInner>> {
        self.tables.iter()
            .find(|t| t.name.eq_ignore_ascii_case(name))
            .cloned()
    }

    // ────── schema 永続化 (0.8.7: `.schema` sidecar file) ──────

    /// schema 情報 (= table 名 + column 型 + PK + relations) を `{path}.schema`
    /// sidecar に atomic write。 旧 blob entity 経路は撤去済 (= anonymous closed
    /// panic 問題の根治、 issue note "schema_meta_entity は 0.7.0 の名残")。
    fn persist_schema(&mut self) -> Result<(), SchemaError> {
        let path = self.eng.db_path().to_string();
        if !path.is_empty() {
            persist_schema_to_sidecar(&path, &self.tables)
                .map_err(|e| SchemaError::Io(e.to_string()))?;
        }
        // engine 本体 (= body + `.tables` sidecar) も flush。
        // flush は build phase (Arc 単一所有) でのみ可能。 concurrent 後は
        // consumer thread が背景 fsync するので skip。
        if let Some(eng_mut) = Arc::get_mut(&mut self.eng) {
            eng_mut.flush().map_err(|e| SchemaError::Io(e.to_string()))?;
        }
        Ok(())
    }

    fn load_schema(&mut self) -> Result<(), SchemaError> {
        let path = self.eng.db_path().to_string();
        // 0.8.15 (issue #52): open 前に `.schema.tmp` 残骸を掃除 (= self-heal)。
        if !path.is_empty() {
            cleanup_schema_tmp(&path);
        }
        // 1. `.schema` sidecar (= 0.8.7 以降の正規 path) を試す
        // 0.8.15 (issue #52): parse 失敗は **fail-readable** で扱う。 InvalidData
        // (= UTF-8 / format 破損) は warn + `.schema.corrupt-<ts>` rename で退避し、
        // 下流の legacy blob / engine からの synthesize fallback に流す。 disk full
        // → recovery 後に store 全体が unreadable になる失敗モードを避ける。
        let mut parsed_opt: Option<Vec<RawTableDef>> = if !path.is_empty() {
            match load_schema_from_sidecar(&path) {
                Ok(opt) => opt,
                Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                    rename_corrupt_schema_sidecar(&path, &e);
                    None
                }
                Err(e) => return Err(SchemaError::Io(e.to_string())),
            }
        } else {
            None
        };
        // 2. fallback: 旧 blob entity (= 0.6.x ~ 0.8.6 で書かれた DB)
        if parsed_opt.is_none() {
            parsed_opt = self.load_schema_from_legacy_blob()?;
        }
        // 3. fallback: engine `.tables` + value_types (= mlbpulse 等の engine 直 DB)
        if parsed_opt.is_none() {
            parsed_opt = self.synthesize_schema_from_engine()?;
        }
        let parsed = parsed_opt.unwrap_or_default();

        // 0.7.0: 各 table を engine table API で再 define。 既存 `.tables` sidecar
        // に存在すれば idempotent (= define_table が "already exists" を返す)、
        // legacy DB (0.5.0/0.6.0 で書いたやつ、 sidecar 空 / anonymous-only) では
        // 新規 define されて anonymous が close される。 既存 anonymous 配下の
        // legacy row は eid 不変で読める。
        // 0.7.0: readonly mode では engine 側の write API は panic するので、
        // load_schema は himo_id resolve だけして table register は skip。
        let is_readonly = self.eng.is_readonly();

        for raw in parsed {
            // table 自体を engine に再 register (idempotent、 readonly は skip)
            if !is_readonly {
                if let Some(eng_mut) = Arc::get_mut(&mut self.eng) {
                    let remaining = eng_mut.remaining_eid_space();
                    let size_hint = (remaining / 4).max(16).min(1_000_000);
                    // request19: `_` 始まり (= local-only / reserved) は
                    // `define_reserved_table` へ振る。 `define_table` は reserved
                    // namespace を弾くので、 ここを分けないと reopen 時に
                    // schema blob の再 register で失敗する。
                    let redefine = if enchudb_engine::engine::is_reserved_table_name(&raw.name) {
                        eng_mut.define_reserved_table(&raw.name, size_hint)
                    } else {
                        eng_mut.define_table(&raw.name, size_hint)
                    };
                    match redefine {
                        Ok(_) => {} // 新規 register (legacy DB か新規 DB の初回 load)
                        Err(e) if e.contains("already exists") => {} // 既に sidecar から復元済み
                        Err(e) => {
                            return Err(SchemaError::Internal(format!(
                                "define_table({}) failed during load_schema: {}", raw.name, e
                            )));
                        }
                    }
                }
            }

            let mut cols = Vec::with_capacity(raw.cols.len());
            for (col_name, ty) in &raw.cols {
                let himo_name = format!("{}.{}", raw.name, col_name);
                // himo を table-attached で再 define (idempotent、 readonly は skip)。
                // relation あり col は後段で fk_refs を再 register。
                if !is_readonly {
                    if let Some(eng_mut) = Arc::get_mut(&mut self.eng) {
                        let _ = eng_mut.define_himo_in(&raw.name, col_name, ty.value_type(), 0);
                    }
                }
                let hid = self.eng.himo_id(&himo_name)
                    .ok_or_else(|| SchemaError::Internal(format!("himo {himo_name} after define / open")))?;
                cols.push(ColumnInner {
                    name: col_name.clone(),
                    ty: *ty,
                    himo_name,
                    himo_id: hid as u16,
                });
            }

            // ref relation を engine 側に再 register (readonly は skip)
            if !is_readonly {
                for (from_col, to_table) in &raw.relations {
                    if let Some(eng_mut) = Arc::get_mut(&mut self.eng) {
                        // define_ref_in は idempotent
                        let _ = eng_mut.define_ref_in(&raw.name, from_col, to_table);
                    }
                }
            }

            let pk = raw.pk.and_then(|n| cols.iter().position(|c| c.name == n));
            let table_vid = self.intern_table_name(&raw.name);
            let relations = raw.relations.into_iter().map(|(from_col, to_table)| {
                RelationInner { from_col, to_table }
            }).collect();
            self.tables.push(Arc::new(TableInner {
                name: raw.name,
                table_vid,
                cols,
                pk,
                relations,
                upsert_lock: std::sync::Mutex::new(()),
            }));
        }
        Ok(())
    }

    /// 0.7.0: 旧実装は dummy entity 作成 → tie_text → delete の roundtrip で
    /// vocab に inject していたが、 `define_table` 後は anonymous closed で
    /// `entity()` が panic するので使えない。 engine の直接 vocab API に置換。
    fn intern_table_name(&self, name: &str) -> u32 {
        self.eng.vocab_intern_text(name)
    }

    /// 0.8.7: legacy blob entity 経路 (= 0.6.x ~ 0.8.6 で書かれた DB の互換読み)。
    /// 旧 marker himo (`__enchu_schema_meta__` / `__enchu_schema_v1__`) を engine から
    /// 探して blob を読む。 見つかれば parsed schema を返す、 marker / blob いずれか
    /// 欠落していれば None。 新規 DB の初回 open は marker himo 自体が無いので即 None。
    /// 0.8.7 以降の DB は `.schema` sidecar に移行済なので本 path は使わない。
    fn load_schema_from_legacy_blob(&self) -> Result<Option<Vec<RawTableDef>>, SchemaError> {
        // 旧 marker himo は engine の himo register に居ないと vocab_id も解決できない。
        if self.eng.himo_id(LEGACY_SCHEMA_META_HIMO).is_none() {
            return Ok(None);
        }
        let Some(vid) = self.eng.vocab_id(LEGACY_SCHEMA_MARKER) else { return Ok(None); };
        self.eng.rebuild();
        let eids = self.eng.pull_raw(LEGACY_SCHEMA_META_HIMO, vid);
        let Some(&eid) = eids.first() else { return Ok(None); };
        // #119: blob 読みも owned + verify 版へ (cross-process writer と併走する open がある)。
        let Some(blob) = self.eng.get_content_owned(eid, LEGACY_SCHEMA_BLOB_HIMO) else { return Ok(None); };
        let s = std::str::from_utf8(&blob)
            .map_err(|_| SchemaError::Parse("legacy schema blob not utf8".into()))?;
        let parsed = deserialize_schema(s)?;
        Ok(Some(parsed))
    }

    /// 0.8.7: engine `.tables` sidecar + value_types から synthetic な RawTableDef を
    /// 組み立てる fallback。 schema sidecar も legacy blob も無い engine 直構築 DB
    /// (= mlbpulse の 4.5M pitch DB 等) を query 可能にするための path。
    ///
    /// 復元できる情報: table 名、 column 名 (= himo full name の `.` 後)、 column type
    /// (= engine の value_type)、 relations (= engine の fk_refs)。
    /// 復元できない: PK (= sidecar に持たないので None 扱い)。 upsert したい場合は
    /// 0.9 で sidecar 拡張 or schema rebuild が要る。
    fn synthesize_schema_from_engine(&self) -> Result<Option<Vec<RawTableDef>>, SchemaError> {
        let tables_info = self.eng.list_user_tables();
        if tables_info.is_empty() {
            return Ok(None);
        }
        let himo_count = self.eng.himo_count();
        let mut out: Vec<RawTableDef> = Vec::with_capacity(tables_info.len());
        for (_tid, name, _lo, _hi) in tables_info {
            let prefix = format!("{}.", name);
            // engine 側の全 himo を walk して、 prefix が一致するものを column として拾う
            let mut cols: Vec<(String, ColumnType)> = Vec::new();
            for hid_idx in 0..himo_count {
                let Some(himo_name) = self.eng.himo_name_at(hid_idx) else { continue; };
                let Some(col_name) = himo_name.strip_prefix(&prefix) else { continue; };
                let Some(htype) = self.eng.value_type_at(hid_idx) else { continue; };
                let ty = match htype {
                    ValueType::Number => ColumnType::Number,
                    ValueType::Tag => ColumnType::Tag,
                    ValueType::Leaf => ColumnType::Leaf,
                    ValueType::Ref => ColumnType::Ref,
                    ValueType::Number64 => ColumnType::BigInt,
                };
                cols.push((col_name.to_string(), ty));
            }
            // relations は engine の fk_refs (= (child_himo_id, parent_table_id)) から復元
            let relations = self.eng.fk_refs_for_table_named(&name);
            out.push(RawTableDef {
                name,
                cols,
                pk: None,
                relations,
            });
        }
        Ok(Some(out))
    }
}

/// 0.8.7: schema sidecar の path を返す (v10: `{db}/schema`)。
#[cfg(not(target_arch = "wasm32"))]
fn schema_sidecar_path_for(db_path: &str) -> std::path::PathBuf {
    enchudb_engine::db_files::path_for(db_path, enchudb_engine::db_files::SCHEMA)
}

/// 0.8.7: tables の serialize_schema 出力を `.schema` sidecar に atomic write。
/// tmp file → fsync → rename で crash-safe。
///
/// #261: 実体は engine 側の [`enchudb_engine::db_files::write_atomic_if_changed`] に
/// 寄せた (同じ手順を 2 crate で書いていたのを 1 本に)。 内容が現行 `.schema` と同じ
/// なら書かないので、 schema をいじらずに開いて閉じただけの rw session は fsync を
/// 払わない。 戻り値は 「実際に書いたか」。
#[cfg(not(target_arch = "wasm32"))]
fn persist_schema_to_sidecar(
    db_path: &str,
    tables: &[Arc<TableInner>],
) -> std::io::Result<bool> {
    let sidecar = schema_sidecar_path_for(db_path);
    let bytes = serialize_schema(tables);
    enchudb_engine::db_files::write_atomic_if_changed(&sidecar, bytes.as_bytes())
}

/// 0.8.7: `.schema` sidecar を読む。 不在は Ok(None)、 parse 失敗は Err。
#[cfg(not(target_arch = "wasm32"))]
fn load_schema_from_sidecar(db_path: &str) -> std::io::Result<Option<Vec<RawTableDef>>> {
    let sidecar = schema_sidecar_path_for(db_path);
    match std::fs::read(&sidecar) {
        Ok(bytes) => {
            let s = std::str::from_utf8(&bytes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            let parsed = deserialize_schema(s).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("schema sidecar parse: {:?}", e),
                )
            })?;
            Ok(Some(parsed))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// 0.8.15 (issue #52): persist 失敗で残った `.schema.tmp` を open 時に明示削除。
#[cfg(not(target_arch = "wasm32"))]
fn cleanup_schema_tmp(db_path: &str) {
    let sidecar = schema_sidecar_path_for(db_path);
    // persist_schema_to_sidecar が使う tmp 名と合わせる (= `schema.tmp`)。
    let tmp = enchudb_engine::db_files::tmp_path_for(&sidecar);
    if tmp.exists() {
        if let Err(e) = std::fs::remove_file(&tmp) {
            eprintln!(
                "warning: failed to remove stale schema tmp {}: {}",
                tmp.display(),
                e
            );
        }
    }
}

/// 0.8.15 (issue #52): 破損した `.schema` sidecar を `.schema.corrupt-<unix_ts>` に
/// rename して退避し、 下流の synthesize fallback に流す。
#[cfg(not(target_arch = "wasm32"))]
fn rename_corrupt_schema_sidecar(db_path: &str, err: &std::io::Error) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let sidecar = schema_sidecar_path_for(db_path);
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let backup = enchudb_engine::db_files::corrupt_backup_path_for(&sidecar, ts);
    eprintln!(
        "warning: schema sidecar parse failed ({}): renaming to {} and falling back to engine synthesize",
        err,
        backup.display()
    );
    if let Err(e) = std::fs::rename(&sidecar, &backup) {
        eprintln!(
            "warning: failed to rename corrupt schema sidecar {} -> {}: {}",
            sidecar.display(),
            backup.display(),
            e
        );
    }
}

// ─────────────────────────── Scope / ScopeMut ───────────────────────────

/// 読み取り専用の scope (= table 名前空間の prefix レンズ)。
/// `Database::scope(name)` で取り出す、 あるいは pattern B (per-DB-file) 用に
/// `Database::as_scope()` で絞りなしのレンズを取り出す。 内部表現は薄い
/// ref + prefix のみ、 storage layout は変えない。 multi-tenant はこの機構の
/// ユースケースの 1 つ (詳細は issue #12、 rename 経緯は issue #24)。
pub struct Scope<'a> {
    db: &'a Database,
    prefix: Option<String>,
}

/// build phase 用 scope。 `table(...)` で建てる table 名に prefix が
/// 自動付与される。 絞りなしの build レンズは `Database::as_scope_mut()` から。
pub struct ScopeMut<'a> {
    db: &'a mut Database,
    prefix: Option<String>,
}

fn resolve_prefixed(prefix: Option<&str>, name: &str) -> String {
    match prefix {
        Some(p) => format!("{}.{}", p, name),
        None => name.to_string(),
    }
}

fn filter_tables_by_prefix(all: Vec<TableInfo>, prefix: Option<&str>) -> Vec<TableInfo> {
    match prefix {
        None => all,
        Some(p) => {
            let needle = format!("{}.", p);
            all.into_iter()
                .filter_map(|mut t| {
                    let new_name = t.name.strip_prefix(&needle).map(|s| s.to_string())?;
                    t.name = new_name;
                    Some(t)
                })
                .collect()
        }
    }
}

impl<'a> Scope<'a> {
    /// この scope の prefix。 絞りなし (`as_scope`) なら None。
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// 既存 table を引く。 prefix 自動付与。
    pub fn get_table(&self, name: &str) -> Option<Table<'a>> {
        self.db.get_table(&resolve_prefixed(self.prefix.as_deref(), name))
    }

    /// この scope から見える table の一覧。 名前付き scope なら `{prefix}.` で
    /// 始まる table のみ、 prefix は剥がして「scope 内の short name」 で返す。
    /// 絞りなし scope は全 table をそのまま返す。
    pub fn list_tables(&self) -> Vec<TableInfo> {
        filter_tables_by_prefix(self.db.list_tables(), self.prefix.as_deref())
    }
}

impl<'a> ScopeMut<'a> {
    /// この scope の prefix。
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// 新規 table を定義。 prefix が自動付与される (絞りなし scope なら付与なし)。
    pub fn table<'b>(&'b mut self, name: &str) -> TableBuilder<'b> {
        let full = resolve_prefixed(self.prefix.as_deref(), name);
        self.db.table(&full)
    }

    /// 既存 table を引く (read 系)。 prefix 自動付与。
    pub fn get_table<'b>(&'b self, name: &str) -> Option<Table<'b>> {
        self.db.get_table(&resolve_prefixed(self.prefix.as_deref(), name))
    }

    /// この scope の table 一覧。
    pub fn list_tables(&self) -> Vec<TableInfo> {
        filter_tables_by_prefix(self.db.list_tables(), self.prefix.as_deref())
    }
}

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub ty: ColumnType,
    pub is_pk: bool,
    /// `ColumnType::Ref` で `ref_to(col, table)` で declare された場合のみ Some。
    pub ref_to: Option<String>,
}

// ─────────────────────────── TableBuilder ───────────────────────────

pub struct TableBuilder<'a> {
    db: &'a mut Database,
    name: String,
    cols: Vec<(String, ColumnType, u32)>, // (name, type, cardinality hint; 0 = none)
    pk: Option<String>,
    relations: Vec<(String, String)>, // (from_col, to_table)
    capacity: Option<u32>,
    local_only: bool,
}

impl<'a> TableBuilder<'a> {
    pub fn column(mut self, name: impl Into<String>, ty: ColumnType) -> Self {
        self.cols.push((name.into(), ty, 0));
        self
    }
    /// inline 数値列 (ValueType::Number)。
    pub fn number(self, name: &str) -> Self { self.column(name, ColumnType::Number) }
    /// 共有タグ列 (ValueType::Tag、vocab 経由 / dedupe あり)。
    /// enum / カテゴリ / 名前など、引かれる値に向く。
    pub fn tag(self, name: &str) -> Self { self.column(name, ColumnType::Tag) }
    /// 終端タグ列 (ValueType::Leaf、FreeStore 経由 / dedupe なし)。
    /// 備考・メモ・本文など、引かれない自由記述に向く。
    pub fn leaf(self, name: &str) -> Self { self.column(name, ColumnType::Leaf) }
    /// 64 bit 整数列 ([`ColumnType::BigInt`])。 負の数、 ms / µs の時刻、 64 bit の ID 向き。
    pub fn bigint(self, name: &str) -> Self { self.column(name, ColumnType::BigInt) }

    /// 直前に宣言した列の cardinality hint (= distinct 値数の目安) を設定する。
    /// `BucketCylinder` の初期 size hint になると同時に、 **この列を group key に
    /// した集計 (`group_sum` / `group_min` / `group_max` / `histogram`) の
    /// dense + 並列 fast path を有効化**する (hint が `1..=65536` のとき)。
    ///
    /// 未指定 (= 0) だと engine の `group_dense_cap` が `None` を返し、 HashMap
    /// fallback + 並列無効に落ちる ([#46])。 `dept` / `status` / `category` など、
    /// group / filter される low-cardinality 列に付けると効く。 値の上限ではなく
    /// hint なので、 超過しても tie は可能 (`BucketCylinder` が動的拡張する)。
    ///
    /// 列宣言メソッド (`number` / `tag` / `leaf` / `column` / `ref_to`) の直後に
    /// chain する。 列が一つも宣言されていなければ no-op。
    ///
    /// [#46]: https://github.com/Mutafika/enchudb/issues/46
    pub fn cardinality(mut self, n: u32) -> Self {
        if let Some(last) = self.cols.last_mut() {
            last.2 = n;
        }
        self
    }

    /// Ref 型カラムを宣言。 値は他テーブルの EntityId を保持する。
    /// `Table::where_ref` で逆引きできる。 `to_table` 名は build 時に存在チェック。
    pub fn ref_to(mut self, col: &str, to_table: &str) -> Self {
        self.cols.push((col.to_string(), ColumnType::Ref, 0));
        self.relations.push((col.to_string(), to_table.to_string()));
        self
    }

    pub fn primary_key(mut self, col: &str) -> Self {
        self.pk = Some(col.to_string());
        self
    }

    /// 0.7.0: table の eid 空間を明示確保。 1 table に大量 (= 1M+) row を入れる
    /// workload で `entity_in() failed: eid range exhausted` を防ぐ用。
    /// 省略時は engine の remaining 空間を 4 等分した default (= 4 table 分の余地)。
    pub fn with_capacity(mut self, capacity: u32) -> Self {
        self.capacity = Some(capacity);
        self
    }

    /// request19: **local-only table** として作る — WAL / commit の耐久性は使うが、
    /// **peer には配らない**。
    ///
    /// 「この端末で観測した事実」 (例: 「この path を、 まさに disk と突き合わせた」)
    /// のように、 **他の端末に配ると嘘になる** state の置き場。 通常の table と同じく
    /// WAL に載り、 同じ commit group で durable になり、 crash 後は replay される。
    /// ただし `_sync_ops` へ bridge されないので peer には流れない。
    ///
    /// - **名前は `_` で始めること** (reserved namespace)。 そうでなければ build が失敗する
    /// - snapshot / bootstrap は body を丸ごと写すので**中身も乗る**。 受け取った側は
    ///   `Engine::clear_local_only_tables()` で空にしてから使うこと
    /// - `list_user_tables` などの user 向け列挙からは外れる (reserved 扱い)
    pub fn local_only(mut self) -> Self {
        self.local_only = true;
        self
    }

    pub fn build(self) -> Result<Table<'a>, SchemaError> {
        let TableBuilder { db, name, cols: col_specs, pk, relations, capacity: capacity_hint, local_only } = self;

        // 既存 table と同名の場合 (#73 G1):
        // - cols 未宣言 (= handle 取得 idiom) は従来通り existing を返す
        // - cols 宣言ありは on-disk schema との diff 検査へ。 prefix 一致 +
        //   trailing 新列なら auto-migrate、 矛盾は SchemaConflict で loud に落とす。
        //   従来はここで宣言を無条件に捨てていて、 新列への `set` が
        //   UnknownColumn になるか silent data loss になっていた。
        if let Some(existing) = db.find_table_inner(&name) {
            if col_specs.is_empty() {
                return Ok(Table { db, inner: existing });
            }
            let inner = db.migrate_existing_table(
                existing, &col_specs, pk.as_deref(), &relations,
            )?;
            return Ok(Table { db, inner });
        }

        // issue #61: 名前に sidecar 区切り文字が入ると round-trip で schema が壊れる。
        // 新規 table 定義の時点で table 名 + 全 column 名を弾く。
        // (column は #73 で `_c_` 予約 prefix 検証も追加)
        validate_schema_name("table", &name)?;
        for (col_name, _, _) in &col_specs {
            validate_column_name(col_name)?;
        }

        // PK 検証
        if let Some(pk_name) = &pk {
            if !col_specs.iter().any(|(n, _, _)| n == pk_name) {
                return Err(SchemaError::UnknownColumn(pk_name.clone()));
            }
        }

        // relation 先 table の存在チェック (self-ref は to_table == name で許可)
        for (_, to_table) in &relations {
            if to_table != &name && db.find_table_inner(to_table).is_none() {
                return Err(SchemaError::UnknownTable(to_table.clone()));
            }
        }

        // 0.8.7: schema_meta_entity の eager 予約は撤去 (= `.schema` sidecar に
        // 移行したので anonymous entity を確保する必要が無くなった)。
        // table_vid (= 名前を vocab に inject) は define_table 前に。 これは
        // entity 経路を一切触らないので順序自由だが、 ここで一括済ませる。
        let table_vid = db.intern_table_name(&name);

        // engine table を define + columns を define_himo_in (+ ref は define_ref_in) 経由
        // で attach。 build phase = Arc 単一所有の前提。
        let eng_mut = Arc::get_mut(&mut db.eng).ok_or_else(|| {
            SchemaError::Internal(
                "Database already shared via Arc — call db.table() before finish_*".into()
            )
        })?;
        // 0.7.0: TableBuilder::with_capacity が呼ばれていれば explicit な size_hint
        // を使う。 そうでなければ remaining 空間を 4 等分した default (= 4 table 分残す
        // 妥協)、 最低 16、 最大 1M に clamp。 大量 row を入れる use case (= 1 table に
        // 1M+) は明示的に `.with_capacity(n)` を呼ぶ。
        let size_hint = if let Some(cap) = capacity_hint {
            cap
        } else {
            let remaining = eng_mut.remaining_eid_space();
            (remaining / 4).max(16).min(1_000_000)
        };
        // 0.8.2-flush-patch: migration path (= 既存 DB に新 table を後付け) では
        // 前回 run の engine sidecar が table を持ってるが、 schema blob は未更新
        // (Drop が concurrent skip 仕様)。 ここで "already exists" は recoverable
        // として扱い、 db.tables だけ追加して engine 状態を流用する。
        // request19: local-only は engine の reserved table として作る (= bridge 除外)。
        let define = |eng_mut: &mut Engine, name: &str, size_hint: u32| {
            if local_only {
                eng_mut.define_reserved_table(name, size_hint)
            } else {
                eng_mut.define_table(name, size_hint)
            }
        };
        let already_in_engine = match define(eng_mut, &name, size_hint) {
            Ok(_) => false,
            Err(e) if e.contains("already exists") => true,
            Err(e) => return Err(SchemaError::Internal(format!("define_table({name}) failed: {e}"))),
        };

        let mut cols = Vec::with_capacity(col_specs.len());
        for (col_name, ty, card) in &col_specs {
            // ref column は define_ref_in、 それ以外は define_himo_in。
            let ref_target = relations.iter().find(|(c, _)| c == col_name).map(|(_, t)| t.clone());
            match ref_target {
                Some(to_table) => {
                    let r = eng_mut.define_ref_in(&name, col_name, &to_table);
                    match r {
                        Ok(_) => {}
                        Err(e) if already_in_engine && e.contains("already") => {}
                        Err(e) => return Err(SchemaError::Internal(format!(
                            "define_ref_in({name}.{col_name} -> {to_table}) failed: {e}"
                        ))),
                    }
                }
                None => {
                    let r = eng_mut.define_himo_in(&name, col_name, ty.value_type(), *card);
                    match r {
                        Ok(_) => {}
                        Err(e) if already_in_engine && e.contains("already") => {}
                        Err(e) => return Err(SchemaError::Internal(format!(
                            "define_himo_in({name}.{col_name}) failed: {e}"
                        ))),
                    }
                }
            }
            let himo_name = format!("{}.{}", name, col_name);
            let hid = eng_mut.himo_id(&himo_name)
                .ok_or_else(|| SchemaError::Internal(format!("himo {himo_name} after define_himo_in")))?;
            cols.push(ColumnInner {
                name: col_name.clone(),
                ty: *ty,
                himo_name,
                himo_id: hid as u16,
            });
        }
        let pk_idx = pk.as_ref().and_then(|n| cols.iter().position(|c| &c.name == n));

        // #141: PK himo を engine へ降ろす。 sync の apply 経路は schema 層を見られない
        // ので、 「同じ PK の既存 row に束ねる」判断を engine 側の情報だけでできるように
        // しておく必要がある。 これを怠ると cross-author apply が同一 PK の entity を
        // 二重に払い出す。
        if let Some(i) = pk_idx {
            let pk_hid = cols[i].himo_id;
            eng_mut
                .set_table_pk(&name, pk_hid)
                .map_err(SchemaError::Internal)?;
        }

        let inner = Arc::new(TableInner {
            name: name.clone(),
            table_vid,
            cols,
            pk: pk_idx,
            relations: relations.into_iter().map(|(from_col, to_table)| RelationInner { from_col, to_table }).collect(),
            upsert_lock: std::sync::Mutex::new(()),
        });
        db.tables.push(inner.clone());
        // 0.8.2: build phase 中の persist_schema は finish_* に coalesce
        // (= 1 build = 1 fsync ≒ 47ms の linear scaling を解消、 issue #19)。
        // build 中の schema blob は誰も読まないので中間 persist は無駄。
        // finish_with_oplog / finish_concurrent の冒頭で 1 度 persist する、
        // finish 経由しない drop path は Drop impl が safety net で persist。

        Ok(Table { db, inner })
    }
}

// ─────────────────────────── Table ───────────────────────────

/// Table への handle。 db を借りるので寿命は db に紐づく。
#[derive(Clone)]
pub struct Table<'a> {
    db: &'a Database,
    inner: Arc<TableInner>,
}

impl<'a> Table<'a> {
    pub fn name(&self) -> &str { &self.inner.name }

    pub fn columns(&self) -> Vec<ColumnInfo> {
        self.inner.cols.iter().map(|c| ColumnInfo {
            name: c.name.clone(),
            ty: c.ty,
            is_pk: self.inner.pk.map(|i| self.inner.cols[i].name == c.name).unwrap_or(false),
            ref_to: self.inner.relations.iter().find(|r| r.from_col == c.name).map(|r| r.to_table.clone()),
        }).collect()
    }

    /// 新規 row insert builder。
    pub fn insert(&self) -> RowBuilder<'a> {
        RowBuilder {
            db: self.db,
            table: self.inner.clone(),
            values: Vec::new(),
            replace_on_pk: false,
        }
    }

    /// `INSERT OR REPLACE` 相当: PK 一致 row があれば update、 無ければ insert。
    pub fn upsert(&self) -> RowBuilder<'a> {
        let mut rb = self.insert();
        rb.replace_on_pk = true;
        rb
    }

    /// `WHERE col = val` 単一条件 query 開始。
    pub fn where_eq<V: Into<Value>>(&self, col: &str, val: V) -> Query<'a> {
        Query::new(self.db, self.inner.clone()).where_eq(col, val)
    }

    /// `WHERE col >= lo AND col <= hi` 範囲 query 開始 (inclusive)。 `lo` / `hi` は整数 (u32 / i32 / i64)。
    pub fn where_range<V: Into<Value>>(&self, col: &str, lo: V, hi: V) -> Query<'a> {
        Query::new(self.db, self.inner.clone()).where_range(col, lo, hi)
    }

    /// `WHERE col = target_eid` ref-typed カラム経由の逆引き。
    pub fn where_ref(&self, col: &str, target: EntityId) -> Query<'a> {
        Query::new(self.db, self.inner.clone()).where_ref(col, target)
    }

    /// `WHERE col IN (v1, v2, ...)` set-membership。
    /// Integer / Ref 列向け。 Text 列は別途 helper が必要 (vocab_id 解決のため)。
    pub fn where_in(&self, col: &str, values: &[u32]) -> Query<'a> {
        Query::new(self.db, self.inner.clone()).where_in(col, values)
    }

    /// 全 row (WHERE 無し相当)。
    pub fn all(&self) -> Query<'a> {
        Query::new(self.db, self.inner.clone())
    }

    /// 既存 entity への accessor。 存在チェックはしない、 get で None が返れば未 tie。
    pub fn entity(&self, eid: EntityId) -> EntityRef<'a> {
        EntityRef { db: self.db, table: self.inner.clone(), eid }
    }

    // ──── bindings: 列名 / table 識別子から build 時 pre-resolve 済みの ID を引く ────
    //
    // hot path (高頻度 writer / reader) では、 起動時にここで u16 / u32 を抜き取って
    // 自前の struct に詰め、 runtime は `engine.tie_*_by_id` / `query_by_id` で直叩き
    // する。 schema layer の `commit` / `find` は declarative DSL であって、 per-row
    // で経由するのは想定外。 詳細は README "schema 層の位置付け" 節を参照。

    /// 列名から build 時に pre-resolve 済みの himo_id を取り出す。 大文字小文字無視。
    /// 未定義列は None。
    pub fn himo_id(&self, col: &str) -> Option<u16> {
        self.inner.col(col).map(|c| c.himo_id)
    }

    // ──── 集計 (= table-scoped、 column 直 scan で auto-vectorize) ────
    //
    // 0.8.6: schema 層の典型 use case (= テーブルが出てきて、 そこにある金額
    // みたいなのを sum) の 1 行 API。 内部で table の `[eid_range_lo, hi)` を
    // engine の `sum_range` / `count_range` / `group_sum_range` に bind。
    // eids 配列を経由しない、 stored_slice を sequential に舐めるだけの
    // branchless tight loop が LLVM で NEON SIMD reduce に auto-vectorize する。

    /// v10 Phase 3 (request20 案 B): table の eid extent。 auto-grow した table は複数本で、
    /// 間に他 table の eid が挟まるので集計は extent ごとに回して束ねる (1 本なら従来どおり)。
    fn eid_extents(&self) -> Vec<(u32, u32)> {
        self.db.eng.table_eid_extents(&self.inner.name).unwrap_or_default()
    }

    /// group 集計を extent 横断で束ねる。 1 本ならそのまま (順序も engine のまま)。
    fn merge_grouped<V: Copy>(
        &self,
        mut per_extent: impl FnMut(u32, u32) -> Vec<(u32, V)>,
        combine: impl Fn(V, V) -> V,
    ) -> Vec<(u32, V)> {
        let ext = self.eid_extents();
        if ext.len() <= 1 {
            return ext.first().map(|&(lo, hi)| per_extent(lo, hi)).unwrap_or_default();
        }
        let mut acc: std::collections::BTreeMap<u32, V> = std::collections::BTreeMap::new();
        for (lo, hi) in ext {
            for (g, v) in per_extent(lo, hi) {
                acc.entry(g).and_modify(|cur| *cur = combine(*cur, v)).or_insert(v);
            }
        }
        acc.into_iter().collect()
    }

    /// table 内の `col` の合計 (= SUM(col))。 1M rows / M2 Max で ~100µs
    /// (= DuckDB の `SELECT SUM(col) FROM table` の 5-6x 速い)。
    pub fn sum(&self, col: &str) -> u64 {
        let himo = format!("{}.{}", self.inner.name, col);
        self.eid_extents().into_iter().map(|(lo, hi)| self.db.eng.sum_range(&himo, lo, hi)).sum()
    }

    /// table 内の `col` に値が tie された row 数 (= COUNT(col))。
    pub fn count_col(&self, col: &str) -> u32 {
        let himo = format!("{}.{}", self.inner.name, col);
        self.eid_extents().into_iter().map(|(lo, hi)| self.db.eng.count_range(&himo, lo, hi)).sum()
    }

    /// `group` でグループ化した上での `sum` 合計 (= SUM(sum) GROUP BY group)。
    /// 戻り値: `Vec<(group_value, sum_total)>`、 順序は group_value 昇順 (dense cap 経路) or
    /// 任意 (HashMap 経路)。
    pub fn group_sum(&self, group: &str, sum: &str) -> Vec<(u32, u64)> {
        let group_himo = format!("{}.{}", self.inner.name, group);
        let sum_himo = format!("{}.{}", self.inner.name, sum);
        self.merge_grouped(|lo, hi| self.db.eng.group_sum_range(&group_himo, &sum_himo, lo, hi), |a, b| a + b)
    }

    // ──── 0.8.8 (#38): min / max / group_min / group_max / histogram ────
    //
    // `sum` と同じ pattern (= table の eid_range を auto-bind して engine の
    // `_range` primitive を呼ぶ)。 stored_slice 直 scan で auto-vectorize する。

    /// table 内の `col` の最小値 (= MIN(col))。 全 missing なら None。
    pub fn min(&self, col: &str) -> Option<u32> {
        let himo = format!("{}.{}", self.inner.name, col);
        self.eid_extents().into_iter().filter_map(|(lo, hi)| self.db.eng.min_range(&himo, lo, hi)).min()
    }

    /// table 内の `col` の最大値 (= MAX(col))。 全 missing なら None。
    pub fn max(&self, col: &str) -> Option<u32> {
        let himo = format!("{}.{}", self.inner.name, col);
        self.eid_extents().into_iter().filter_map(|(lo, hi)| self.db.eng.max_range(&himo, lo, hi)).max()
    }

    /// `group` でグループ化した上での `val` 最小値 (= MIN(val) GROUP BY group)。
    pub fn group_min(&self, group: &str, val: &str) -> Vec<(u32, u32)> {
        let group_himo = format!("{}.{}", self.inner.name, group);
        let val_himo = format!("{}.{}", self.inner.name, val);
        self.merge_grouped(|lo, hi| self.db.eng.group_min_range(&group_himo, &val_himo, lo, hi), |a, b| a.min(b))
    }

    /// `group` でグループ化した上での `val` 最大値 (= MAX(val) GROUP BY group)。
    pub fn group_max(&self, group: &str, val: &str) -> Vec<(u32, u32)> {
        let group_himo = format!("{}.{}", self.inner.name, group);
        let val_himo = format!("{}.{}", self.inner.name, val);
        self.merge_grouped(|lo, hi| self.db.eng.group_max_range(&group_himo, &val_himo, lo, hi), |a, b| a.max(b))
    }

    /// table 内の `col` の値域 `[vmin, vmax]` を `n_buckets` 等分した頻度
    /// ヒストグラム。 値域外の row はカウント外、 戻り値長は常に `n_buckets`。
    /// `n_buckets == 0` または `vmin > vmax` のときは空 Vec。
    pub fn histogram(&self, col: &str, vmin: u32, vmax: u32, n_buckets: u32) -> Vec<u32> {
        let himo = format!("{}.{}", self.inner.name, col);
        let ext = self.eid_extents();
        if ext.is_empty() {
            return if n_buckets == 0 || vmin > vmax { vec![] } else { vec![0; n_buckets as usize] };
        }
        let mut acc: Vec<u32> = Vec::new();
        for (lo, hi) in ext {
            let h = self.db.eng.histogram_range(&himo, lo, hi, vmin, vmax, n_buckets);
            if acc.is_empty() {
                acc = h;
            } else {
                for (a, b) in acc.iter_mut().zip(h) {
                    *a += b;
                }
            }
        }
        acc
    }
}

// ─────────────────────────── RowBuilder ───────────────────────────

pub struct RowBuilder<'a> {
    db: &'a Database,
    table: Arc<TableInner>,
    values: Vec<(String, Value)>,
    replace_on_pk: bool,
}

impl<'a> RowBuilder<'a> {
    pub fn set<V: Into<Value>>(mut self, col: &str, val: V) -> Self {
        self.values.push((col.to_string(), val.into()));
        self
    }

    pub fn commit(self) -> Result<EntityId, SchemaError> {
        let eng = self.db.engine();

        // 1. col 解決 (未知 col / 型不一致は早期エラー)
        let mut resolved: Vec<(&ColumnInner, &Value)> = Vec::with_capacity(self.values.len());
        for (col_name, v) in &self.values {
            let cd = self.table.col_or_err(col_name)?;
            resolved.push((cd, v));
        }

        // 2. upsert なら PK 一致 row を探す
        //
        // #60: lookup→allocate を per-table lock で直列化して TOCTOU を防ぐ。
        // PK upsert のときだけ取得 (= PK 無し / 非 upsert は lock 不要で並行のまま)。
        // poison しても () の中身は無いので into_inner で回復する。
        // guard は commit 末尾まで保持 = PK tie が次 upserter から見えてから解放。
        let _upsert_guard = if self.replace_on_pk && self.table.pk.is_some() {
            Some(self.table.upsert_lock.lock().unwrap_or_else(|e| e.into_inner()))
        } else {
            None
        };

        let mut target_eid: Option<EntityId> = None;
        if self.replace_on_pk {
            if let Some(pk_idx) = self.table.pk {
                let pk_col = &self.table.cols[pk_idx];
                let pk_value = resolved.iter()
                    .find(|(c, _)| c.name == pk_col.name)
                    .map(|(_, v)| *v);
                if let Some(pk_v) = pk_value {
                    let pk_raw = value_to_raw_for_query(eng, pk_col, pk_v)?;
                    if pk_raw != u64::MAX {
                        eng.rebuild();
                        // pk_col.himo_id は table 名 prefix 済み (`emp.pk_col`) なので
                        // 他テーブルと衝突しない → marker cond は不要。
                        let found = eng.query_by_id64(&[
                            (pk_col.himo_id, pk_raw),
                        ]);
                        target_eid = found.into_iter().next();
                    }
                }
            }
        }

        let eid = match target_eid {
            Some(e) => e,
            None => {
                // 0.7.0: 各 table 内の eid_range から払出。 entity_in は &self で
                // 動く (next_local は AtomicU32、 CAS で並行 safe)。
                eng.entity_in(&self.table.name)
                    .map_err(|e| SchemaError::Internal(format!(
                        "entity_in({}) failed: {e}", self.table.name
                    )))?
            }
        };
        // table 識別は column 名 prefix (`emp.foo` himo) で行うので、
        // 個別 row への marker tie は不要。 query 側も marker cond なしで
        // 当該 table の column を持つ entity のみ取れる。

        for (cd, v) in &resolved {
            tie_value(eng, eid, cd, v)?;
        }
        Ok(eid)
    }
}

// ─────────────────────────── LiveQuery ───────────────────────────

/// [`Query::subscribe`] の戻り値。 drop で購読解除。
///
/// engine を `Arc` で抱えているので `Database` を借用しない — struct に入れて持ち回れ、
/// 別 thread から poll してよい。
pub struct LiveQuery {
    inner: enchudb_engine::LiveQuery,
    eng: Arc<Engine>,
}

impl LiveQuery {
    /// 前回 poll からの差分。 初回は登録時点の全件が `added`。 removed → added の順に積めば
    /// 常にその時点の `find()` と一致する。 eid は `find()` と同じ形 (peer prefix 付き)。
    pub fn poll(&self) -> LiveDelta {
        self.inner.poll(&self.eng)
    }

    /// 今の件数 (`find()?.len()` と同じ)。
    pub fn count(&self) -> usize {
        self.inner.count(&self.eng)
    }

    /// `eid` が今の結果に含まれるか。
    pub fn contains(&self, eid: EntityId) -> bool {
        self.inner.contains(&self.eng, eid)
    }

    /// 今の結果全体 (eid 昇順)。 poll の状態は変えない。
    pub fn members(&self) -> Vec<EntityId> {
        self.inner.members(&self.eng)
    }

    /// `order_by(..).limit(k)` の購読: 今の先頭 k 件を並びの順に (先頭が 1 位)。 他は `members` と同じ。
    pub fn ranked(&self) -> Vec<EntityId> {
        self.inner.ranked(&self.eng)
    }

    /// 未 poll の変化がありうるか (false なら `poll` は空)。 評価しないので軽い。
    pub fn is_dirty(&self) -> bool {
        self.inner.is_dirty()
    }

    /// engine 内で一意な購読 id ([`LiveGroup::poll`] の差分の宛先)。
    pub fn id(&self) -> u64 {
        self.inner.id()
    }

    /// engine 層の購読 (ablation 用の hidden API などに降りる時)。
    pub fn engine_query(&self) -> &enchudb_engine::LiveQuery {
        &self.inner
    }
}

impl std::fmt::Debug for LiveQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

/// [`Query::subscribe_counts`] の戻り値。 group の値ごとの件数を購読する。 drop で購読解除、
/// `Database` を借用しない。
pub struct LiveCounts {
    inner: enchudb_engine::LiveCounts,
    eng: Arc<Engine>,
    ty: ColumnType,
    /// 合計の列が BigInt (engine の合計を元の値に戻す)。
    sum_big: bool,
    /// group ごとに最後に渡した (件数, 合計)。 engine は 「合計に足した件数」 (`Agg::summed`) が動いただけの
    /// group も報告する (値 0 ↔ 値なし) が、 ここから見える (件数, 合計) が同じなら渡さない。
    last: std::sync::Mutex<std::collections::BTreeMap<u64, (u64, i128)>>,
}

impl LiveCounts {
    // engine の group の値は u64。 schema の列 (Number / Ref / Tag / Leaf) の値は u32 に収まる
    fn value(&self, v: u64) -> Value {
        match self.ty {
            ColumnType::Number => Value::Number(v as i64),
            ColumnType::BigInt => Value::Number(big_val(v)),
            ColumnType::Ref => Value::Ref(enchudb_oplog::make_eid(self.eng.peer_id(), v as u32)),
            ColumnType::Tag | ColumnType::Leaf => Value::Text(String::from_utf8_lossy(self.eng.vocab_text(v as u32)).into_owned()),
        }
    }

    fn raw(&self, v: &Value) -> Option<u64> {
        match (self.ty, v) {
            (ColumnType::Number, Value::Number(n)) => u32::try_from(*n).ok().map(u64::from),
            (ColumnType::BigInt, Value::Number(n)) => big_raw(*n),
            (ColumnType::Ref, Value::Ref(e)) => Some(enchudb_oplog::eid_local(*e) as u64),
            (ColumnType::Tag, Value::Text(t)) => self.eng.vocab_id(t).map(u64::from),
            _ => None,
        }
    }

    /// engine の合計を元の値の合計に。 BigInt は 1 件ごとに `v + 2^63` で載っているので、 値を足した件数
    /// (`summed`) × 2^63 を引く。
    fn sum_of(&self, a: enchudb_engine::Agg) -> i128 {
        if self.sum_big {
            a.sum as i128 - ((a.summed as i128) << 63)
        } else {
            a.sum as i128
        }
    }

    /// 前回 poll から件数が変わった group と今の件数 (0 = その group の row が居なくなった)。
    /// 値で上書きして積めば常に今の件数。 初回は登録時点の全 group。 並びは値の内部表現の順。
    pub fn poll(&self) -> Vec<(Value, u64)> {
        self.poll_raw().into_iter().map(|(v, n, _)| (v, n)).collect()
    }

    /// engine の報告のうち、 (件数, 合計) が最後に渡したものと違う group だけ (件数 0 = 消えた)。
    fn poll_raw(&self) -> Vec<(Value, u64, i128)> {
        let mut last = self.last.lock().unwrap_or_else(|p| p.into_inner());
        let mut out = Vec::new();
        for (v, a) in self.inner.poll_sums(&self.eng) {
            let now = (a.count, self.sum_of(a));
            let changed = if a.count == 0 { last.remove(&v).is_some() } else { last.insert(v, now) != Some(now) };
            if changed {
                out.push((self.value(v), now.0, now.1));
            }
        }
        out
    }

    /// `poll` の、 件数と合計の両方を返す版 ([`Query::subscribe_sums`] の購読。 件数か合計が動いた
    /// group、 件数 0 = group が消えた)。 `poll` と報告状態を共有する。 合計しない購読では合計は 0。
    pub fn poll_sums(&self) -> Vec<(Value, u64, i128)> {
        self.poll_raw()
    }

    /// group `value` の今の合計 ([`Query::subscribe_sums`] の購読)。
    pub fn get_sum(&self, value: &Value) -> i128 {
        self.raw(value).map_or(0, |v| self.sum_of(self.inner.get_agg(&self.eng, v)))
    }

    /// 今の全 group と件数・合計。
    pub fn all_sums(&self) -> Vec<(Value, u64, i128)> {
        self.inner.all_sums(&self.eng).into_iter().map(|(v, a)| (self.value(v), a.count, self.sum_of(a))).collect()
    }

    /// group `value` の今の件数。
    pub fn get(&self, value: &Value) -> u64 {
        self.raw(value).map_or(0, |v| self.inner.get(&self.eng, v))
    }

    /// 今の全 group と件数。
    pub fn all(&self) -> Vec<(Value, u64)> {
        self.inner.all(&self.eng).into_iter().map(|(v, n)| (self.value(v), n)).collect()
    }

    /// 全 group の件数の和 (= 条件に当てはまり、 group の列に値のある row の数)。
    pub fn total(&self) -> usize {
        self.inner.total(&self.eng)
    }

    /// engine 内で一意な購読 id。
    pub fn id(&self) -> u64 {
        self.inner.id()
    }
}

impl std::fmt::Debug for LiveCounts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

/// [`LiveHaving::poll`] の戻り値。 件数が閾値をまたいだ group の値。
#[derive(Debug, Clone, PartialEq, Default)]
pub struct HavingDelta {
    /// 件数が閾値以上になった group。
    pub added: Vec<Value>,
    /// 件数が閾値未満になった group (0 件になったものも)。 適用順は removed → added。
    pub removed: Vec<Value>,
}

/// [`Query::subscribe_having`] の戻り値。 件数が閾値以上の group の集合を購読する (live の `GROUP BY col
/// HAVING COUNT(*) >= n`)。 drop で購読解除、 `Database` を借用しない。
pub struct LiveHaving {
    counts: LiveCounts,
    th: HavingTh,
    /// 閾値以上として報告済みの group (engine の値)。
    have: std::sync::Mutex<std::collections::BTreeSet<u64>>,
}

/// [`LiveHaving`] の閾値。
#[derive(Clone, Copy, Debug)]
enum HavingTh {
    Count(u64),
    /// 件数が 1 以上で和が閾値以上。
    Sum(i128),
}

impl LiveHaving {
    fn holds(&self, a: enchudb_engine::Agg) -> bool {
        match self.th {
            HavingTh::Count(min) => a.count >= min,
            HavingTh::Sum(min) => a.count > 0 && self.counts.sum_of(a) >= min,
        }
    }

    /// 前回 poll から件数が閾値をまたいだ group。 初回は登録時点で閾値以上の全 group が `added`。
    /// 積分した集合 = 今閾値以上の group。 書き込み 1 回あたりのコストは group の数によらない。
    pub fn poll(&self) -> HavingDelta {
        let mut have = self.have.lock().unwrap_or_else(|p| p.into_inner());
        let mut d = HavingDelta::default();
        for (g, a) in self.counts.inner.poll_sums(&self.counts.eng) {
            let now = self.holds(a);
            if now && have.insert(g) {
                d.added.push(self.counts.value(g));
            } else if !now && have.remove(&g) {
                d.removed.push(self.counts.value(g));
            }
        }
        d
    }

    /// 今件数が閾値以上の group。
    pub fn groups(&self) -> Vec<Value> {
        self.counts.inner.all_sums(&self.counts.eng).into_iter().filter(|&(_, a)| self.holds(a)).map(|(g, _)| self.counts.value(g)).collect()
    }

    /// group `value` の今の件数 (閾値未満でも)。
    pub fn count(&self, value: &Value) -> u64 {
        self.counts.get(value)
    }

    /// engine 内で一意な購読 id。
    pub fn id(&self) -> u64 {
        self.counts.id()
    }
}

impl std::fmt::Debug for LiveHaving {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveHaving").field("th", &self.th).field("counts", &self.counts).finish()
    }
}

/// 購読の束 ([`Database::live_group`])。 束を drop しても購読はそのまま (差分は各購読の `poll` で受け取れ、
/// 他の束に入れ直してもよい)。
pub struct LiveGroup {
    inner: enchudb_engine::LiveGroup,
    eng: Arc<Engine>,
}

impl LiveGroup {
    /// 購読をこの束に入れる (購読は 1 つの束にしか入らない、 後から入れた束に移る)。
    pub fn add(&self, q: &LiveQuery) {
        self.inner.add(&q.inner);
    }

    /// 会社単位の購読を入れる (差分は group の eid)。
    pub fn add_grouped(&self, q: &GroupedLiveQuery) {
        self.inner.add_grouped(&q.inner);
    }

    /// 束の購読のうち、 前回から出入りのあったものの差分 (`(LiveQuery::id, 差分)`、 id 昇順)。 各購読の
    /// `poll` と報告状態を共有するので、 同じ差分はどちらか一方にだけ届く。
    pub fn poll(&self) -> Vec<(u64, LiveDelta)> {
        self.inner.poll(&self.eng)
    }
}

impl std::fmt::Debug for LiveGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

/// [`Query::subscribe_grouped`] の戻り値。 結果を ref の先の row (group) 単位で持つ購読。
/// drop で購読解除、 `Database` を借用しない。
pub struct GroupedLiveQuery {
    inner: enchudb_engine::GroupedLiveQuery,
    eng: Arc<Engine>,
}

impl GroupedLiveQuery {
    /// 前回 poll からの group (ref の先の row) の差分。 初回は登録時点の全 group が `added`。
    pub fn poll(&self) -> LiveDelta {
        self.inner.poll(&self.eng)
    }

    /// 今条件を満たす group (eid 昇順)。
    pub fn groups(&self) -> Vec<EntityId> {
        self.inner.groups(&self.eng)
    }

    /// `group` を指していて、 根の table への条件も満たす row (eid 昇順)。 いつ引いても今の中身。
    pub fn members(&self, group: EntityId) -> Vec<EntityId> {
        self.inner.members(&self.eng, group)
    }

    /// 平らにした結果の件数 (= 同じ条件の `find()?.len()`)。
    pub fn count(&self) -> usize {
        self.inner.count(&self.eng)
    }

    /// 平らにした結果全体 (= 同じ条件の `find()`)。
    pub fn flatten(&self) -> Vec<EntityId> {
        self.inner.flatten(&self.eng)
    }

    /// 未 poll の group の変化がありうるか。
    pub fn is_dirty(&self) -> bool {
        self.inner.is_dirty()
    }

    /// engine 内で一意な購読 id ([`LiveGroup::poll`] の差分の宛先。 差分は group の eid)。
    pub fn id(&self) -> u64 {
        self.inner.id()
    }
}

impl std::fmt::Debug for GroupedLiveQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

// ─────────────────────────── JOIN (組) ───────────────────────────

enum JoinOn {
    /// 左の ref 列が右の row を指す。
    Ref(String),
    /// 左の列 (ref の先でもよい) と右の列の値が等しい。
    Eq(String, String),
    /// 左の列 (ref の先でもよい) の値が右の 2 つの列の値の間 (両端を含む)。
    Range(String, String, String),
}

/// [`Query::join_ref`] / [`Query::join_eq`] の戻り値。 2 つの table の row の組を引く / 購読する。
pub struct JoinQuery<'a> {
    left: Query<'a>,
    right: Query<'a>,
    on: JoinOn,
}

/// [`LiveJoin::poll`] の戻り値。 前回 poll からの組の差分 (**順不同**、 同じ組は各リストに高々 1 回)。 適用順は
/// removed → added。 同じ組が両方に居たら 「消えて、 別物として入り直した」 (削除された row の eid が使い回された)。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PairDelta {
    pub added: Vec<(EntityId, EntityId)>,
    pub removed: Vec<(EntityId, EntityId)>,
}

/// 組の計画: 左右の engine の条件と鍵。
enum JoinPlan {
    /// ref で結ぶ: 左の条件 + 右の条件を ref の先に置いたもの、 鍵 = ref 列。 `right` = 右の table の全 row (row の作り直しを
    /// 見るだけ。 右の条件で購読すると右の列の書き換えのたびに評価が走る)。
    Ref { preds: Vec<enchudb_engine::LivePred>, via: u16, right: Vec<enchudb_engine::LivePred> },
    /// 値で結ぶ: 左の条件と鍵 (ref の道 + 列)、 右の条件と鍵の列。
    Eq { left: Vec<enchudb_engine::LivePred>, path: Vec<u16>, left_key: u16, right: Vec<enchudb_engine::LivePred>, right_key: u16 },
    /// 範囲で結ぶ: 左の条件と値 (ref の道 + 列)、 右の条件と始点 / 終点の列。
    Range { left: Vec<enchudb_engine::LivePred>, path: Vec<u16>, left_key: u16, right: Vec<enchudb_engine::LivePred>, lo: u16, hi: u16 },
}

impl<'a> JoinQuery<'a> {
    /// 左右の条件を engine の条件に写す。 `None` = 常に 0 組 (未知の値の where_eq など)。
    fn plan(self) -> Result<Option<JoinPlan>, SchemaError> {
        let bad = |m: String| Err(SchemaError::BadValue(m));
        if self.left.limit.is_some() || self.right.limit.is_some() || self.left.order.is_some() || self.right.order.is_some() {
            return bad("join: limit / order_by are not supported".into());
        }
        match &self.on {
            JoinOn::Ref(col) => {
                let cd = self.left.table.col(col).cloned();
                let points = cd.as_ref().is_some_and(|c| c.ty == ColumnType::Ref)
                    && self.left.table.relations.iter().any(|r| {
                        r.from_col.eq_ignore_ascii_case(col) && r.to_table.eq_ignore_ascii_case(&self.right.table.name)
                    });
                let Some(cd) = cd.filter(|_| points) else {
                    return bad(format!("join_ref: {col} is not a ref column of {} pointing to {}", self.left.table.name, self.right.table.name));
                };
                let all = Query::new(self.right.db, self.right.table.clone());
                let (Some(mut l), Some(r), Some(rows)) = (self.left.live_preds()?, self.right.live_preds()?, all.live_preds()?) else { return Ok(None) };
                l.extend(r.into_iter().map(|p| enchudb_engine::LivePred::Via { path: vec![cd.himo_id], pred: Box::new(p) }));
                Ok(Some(JoinPlan::Ref { preds: l, via: cd.himo_id, right: rows }))
            }
            JoinOn::Eq(my, their) => {
                let Some((path, mine)) = self.left.resolve_col(my) else { return bad(format!("join_eq: unknown column {my}")) };
                let Some(theirs) = self.right.table.col(their).cloned() else { return bad(format!("join_eq: unknown column {their}")) };
                if mine.ty != theirs.ty || matches!(mine.ty, ColumnType::Leaf | ColumnType::Ref) {
                    return bad(format!("join_eq: {my} ({:?}) and {their} ({:?}) must have the same Tag / Number / BigInt type", mine.ty, theirs.ty));
                }
                let (Some(l), Some(r)) = (self.left.live_preds()?, self.right.live_preds()?) else { return Ok(None) };
                Ok(Some(JoinPlan::Eq { left: l, path, left_key: mine.himo_id, right: r, right_key: theirs.himo_id }))
            }
            JoinOn::Range(my, lo, hi) => {
                let Some((path, mine)) = self.left.resolve_col(my) else { return bad(format!("join_range: unknown column {my}")) };
                let (Some(lo), Some(hi)) = (self.right.table.col(lo).cloned(), self.right.table.col(hi).cloned()) else {
                    return bad(format!("join_range: unknown column {lo} / {hi} of {}", self.right.table.name));
                };
                if !matches!(mine.ty, ColumnType::Number | ColumnType::BigInt) || lo.ty != mine.ty || hi.ty != mine.ty {
                    return bad(format!("join_range: {my} and the range columns must all be Number or all be BigInt"));
                }
                let (Some(l), Some(r)) = (self.left.live_preds()?, self.right.live_preds()?) else { return Ok(None) };
                Ok(Some(JoinPlan::Range { left: l, path, left_key: mine.himo_id, right: r, lo: lo.himo_id, hi: hi.himo_id }))
            }
        }
    }

    /// この組の table `from` の row と、 その ref 列 `ref_col` が指す `other` の row をさらにつなぐ (3 つ以上の table の
    /// 組、 [`MultiJoin`])。 `from` は組に居る table の名前。
    ///
    /// ```ignore
    /// // (投稿, 作者, 作者の会社, 会社の街の開いた店)
    /// let q = posts.all()
    ///     .join_ref("author", users.all())
    ///     .then_ref("users", "company", companies.all())
    ///     .then_eq("companies", "city", shops.where_eq("open", 1i64), "city");
    /// ```
    pub fn then_ref(self, from: &str, ref_col: &str, other: Query<'a>) -> MultiJoin<'a> {
        MultiJoin::from_pair(self).then_ref(from, ref_col, other)
    }

    /// この組の table `from` の row の列 `my_col` (ref の先でもよい) と、 `other` の列 `their_col` の値が等しい row を
    /// さらにつなぐ ([`MultiJoin`])。
    pub fn then_eq(self, from: &str, my_col: &str, other: Query<'a>, their_col: &str) -> MultiJoin<'a> {
        MultiJoin::from_pair(self).then_eq(from, my_col, other, their_col)
    }

    /// 組を列 `col` の値ごとに数えた件数を購読する (live の `SELECT col, COUNT(*) FROM a JOIN b .. GROUP BY col`)。
    /// 戻り値の API は [`LiveCounts`] と同じ (`poll` / `get` / `all` …)。
    ///
    /// ```ignore
    /// // 公開済みの投稿の数を作者の街ごとに (ref の組は左の列で、 右の列も ref をたどって書ける)
    /// let by_city = posts.where_eq("published", 1i64).join_ref("author", users.all()).subscribe_counts("author.city")?;
    /// // 住人 × 開いた店の組の数を街ごとに (値の組は結ぶ列でだけ group にできる)
    /// let per_city = users.all().join_eq("city", shops.where_eq("open", 1i64), "city").subscribe_counts("city")?;
    /// ```
    ///
    /// - `join_ref` の組: `col` は左の table の列 (`"author.city"` のように ref をたどってもよい)。 組は左の row と
    ///   1 対 1 なので、 左の row を数える集計の購読と同じコスト
    /// - `join_eq` の組: `col` は結ぶ左の列 (`my_col`) だけ。 件数は鍵ごとの 「左の数 × 右の数」
    pub fn subscribe_counts(self, col: &str) -> Result<LiveJoinCounts, SchemaError> {
        self.subscribe_agg(col, None)
    }

    /// [`subscribe_counts`](Self::subscribe_counts) に加えて組ごとの列 `sum_col` の値の和も持つ (`SUM(sum_col)`)。
    ///
    /// - `join_ref`: `sum_col` は左の table の Number / BigInt 列
    /// - `join_eq`: `sum_col` は左の table の列、 右の列は `"{右の table}.{列}"` (`"shops.rev"`)。 和は鍵ごとに
    ///   「左の和 × 右の数」 / 「左の数 × 右の和」
    pub fn subscribe_sums(self, col: &str, sum_col: &str) -> Result<LiveJoinCounts, SchemaError> {
        self.subscribe_agg(col, Some(sum_col))
    }

    fn subscribe_agg(self, col: &str, sum_col: Option<&str>) -> Result<LiveJoinCounts, SchemaError> {
        let bad = |m: String| SchemaError::BadValue(m);
        let io = |e: std::io::Error| SchemaError::BadValue(e.to_string());
        let eng = self.left.db.arc_engine();
        let num = |t: &TableInner, c: &str| t.col(c).filter(|c| matches!(c.ty, ColumnType::Number | ColumnType::BigInt)).cloned();
        match &self.on {
            JoinOn::Ref(_) => {
                let (path, cd) = self.left.resolve_col(col).ok_or_else(|| bad(format!("join subscribe_counts: unknown column {col}")))?;
                if cd.ty == ColumnType::Leaf {
                    return Err(bad("join subscribe_counts: cannot group by a Leaf column".into()));
                }
                let sum = match sum_col {
                    Some(sc) => Some(num(&self.left.table, sc).ok_or_else(|| {
                        bad(format!("join subscribe_sums: {sc} is not a Number / BigInt column of {}", self.left.table.name))
                    })?),
                    None => None,
                };
                let Some(JoinPlan::Ref { preds, .. }) = self.plan()? else {
                    return Err(bad("join subscribe_counts: the join never matches (unknown value)".into()));
                };
                let inner = match &sum {
                    Some(s) => eng.subscribe_sums(preds, path, cd.himo_id, s.himo_id),
                    None => eng.subscribe_counts(preds, path, cd.himo_id),
                }
                .map_err(io)?;
                let sum_big = sum.is_some_and(|s| s.ty == ColumnType::BigInt);
                Ok(LiveJoinCounts(JoinCounts::Rows(LiveCounts { inner, eng, ty: cd.ty, sum_big, last: Default::default() })))
            }
            JoinOn::Range(..) => Err(bad("join subscribe_counts: not supported for join_range yet".into())),
            JoinOn::Eq(my, _) => {
                if !col.eq_ignore_ascii_case(my) {
                    return Err(bad(format!("join subscribe_counts: a join_eq groups only by its join column {my}")));
                }
                let right_name = self.right.table.name.clone();
                // 和の列: "{右の table}.{列}" は右、 他は左
                let side = match sum_col {
                    None => None,
                    Some(sc) => match sc.split_once('.').filter(|(t, _)| t.eq_ignore_ascii_case(&right_name)) {
                        Some((_, c)) => Some((true, num(&self.right.table, c).ok_or_else(|| bad(format!("join subscribe_sums: {sc} is not a Number / BigInt column")))?)),
                        None => Some((false, num(&self.left.table, sc).ok_or_else(|| bad(format!("join subscribe_sums: {sc} is not a Number / BigInt column")))?)),
                    },
                };
                let ty = self.left.resolve_col(my).map(|(_, c)| c.ty).ok_or_else(|| bad(format!("join_eq: unknown column {my}")))?;
                let Some(JoinPlan::Eq { left, path, left_key, right, right_key }) = self.plan()? else {
                    return Err(bad("join subscribe_counts: the join never matches (unknown value)".into()));
                };
                let counts = |preds, path, key, sum: Option<&ColumnInner>| -> Result<LiveCounts, SchemaError> {
                    let inner = match sum {
                        Some(s) => eng.subscribe_sums(preds, path, key, s.himo_id),
                        None => eng.subscribe_counts(preds, path, key),
                    }
                    .map_err(io)?;
                    let sum_big = sum.is_some_and(|s| s.ty == ColumnType::BigInt);
                    Ok(LiveCounts { inner, eng: eng.clone(), ty, sum_big, last: Default::default() })
                };
                let lsum = side.as_ref().filter(|s| !s.0).map(|s| &s.1);
                let rsum = side.as_ref().filter(|s| s.0).map(|s| &s.1);
                Ok(LiveJoinCounts(JoinCounts::Product {
                    left: counts(left, path, left_key, lsum)?,
                    right: counts(right, Vec::new(), right_key, rsum)?,
                    sum_right: side.as_ref().map(|s| s.0),
                    last: Default::default(),
                }))
            }
        }
    }

    /// 今の組 (昇順)。
    pub fn find(self) -> Result<Vec<(EntityId, EntityId)>, SchemaError> {
        let eng = self.left.db.arc_engine();
        let Some(plan) = self.plan()? else { return Ok(Vec::new()) };
        let peer = eng.peer_id();
        let io = |e: std::io::Error| SchemaError::BadValue(e.to_string());
        // entity の鍵 (ref の道をたどった先の列の値)
        let key = |e: EntityId, path: &[u16], h: u16| -> Option<u64> {
            let mut cur = e;
            for &p in path {
                cur = enchudb_oplog::make_eid(peer, eng.get_by_id(cur, p)? as u32);
            }
            eng.get_by_id(cur, h)
        };
        let mut out = Vec::new();
        match plan {
            JoinPlan::Ref { preds, via, .. } => {
                for a in eng.find_by(preds).map_err(io)? {
                    if let Some(k) = key(a, &[], via) {
                        out.push((a, enchudb_oplog::make_eid(peer, k as u32)));
                    }
                }
            }
            JoinPlan::Eq { left, path, left_key, right, right_key } => {
                let mut by_key: std::collections::BTreeMap<u64, Vec<EntityId>> = std::collections::BTreeMap::new();
                for b in eng.find_by(right).map_err(io)? {
                    if let Some(k) = key(b, &[], right_key) {
                        by_key.entry(k).or_default().push(b);
                    }
                }
                for a in eng.find_by(left).map_err(io)? {
                    if let Some(bs) = key(a, &path, left_key).and_then(|k| by_key.get(&k)) {
                        out.extend(bs.iter().map(|&b| (a, b)));
                    }
                }
            }
            JoinPlan::Range { left, path, left_key, right, lo, hi } => {
                let mut points: Vec<(u64, EntityId)> =
                    eng.find_by(left).map_err(io)?.into_iter().filter_map(|a| key(a, &path, left_key).map(|v| (v, a))).collect();
                points.sort_unstable();
                for b in eng.find_by(right).map_err(io)? {
                    if let (Some(l), Some(h)) = (key(b, &[], lo), key(b, &[], hi)) {
                        let from = points.partition_point(|p| p.0 < l);
                        out.extend(points[from..].iter().take_while(|p| p.0 <= h).map(|p| (p.1, b)));
                    }
                }
            }
        }
        out.sort_unstable();
        Ok(out)
    }

    /// 今の組の数。
    pub fn count(self) -> Result<usize, SchemaError> {
        Ok(self.find()?.len())
    }

    /// 組を購読する。 初回 poll は登録時点の全部の組が `added`。 どちらの table の row の出入り・結ぶ列の
    /// 書き換え (ref の付け替え、 ref の先の値の変化も) でも届く。 drop で購読解除、 `Database` を借用しない。
    pub fn subscribe(self) -> Result<LiveJoin, SchemaError> {
        let eng = self.left.db.arc_engine();
        let io = |e: std::io::Error| SchemaError::BadValue(e.to_string());
        let inner = match self.plan()? {
            None => JoinLive::Empty,
            Some(JoinPlan::Ref { preds, via, right }) => {
                JoinLive::Ref { left: eng.subscribe_keyed(preds, Vec::new(), via).map_err(io)?, right: eng.subscribe(right).map_err(io)?, via }
            }
            Some(JoinPlan::Eq { left, path, left_key, right, right_key }) => JoinLive::Eq {
                left: eng.subscribe_keyed(left, path, left_key).map_err(io)?,
                right: eng.subscribe_keyed(right, Vec::new(), right_key).map_err(io)?,
                state: Default::default(),
            },
            Some(JoinPlan::Range { left, path, left_key, right, lo, hi }) => JoinLive::Range {
                left: eng.subscribe_keyed(left, path, left_key).map_err(io)?,
                lo: eng.subscribe_keyed(right.clone(), Vec::new(), lo).map_err(io)?,
                hi: eng.subscribe_keyed(right, Vec::new(), hi).map_err(io)?,
                state: Default::default(),
            },
        };
        Ok(LiveJoin { inner, eng })
    }
}

/// 値で結ぶ組の、 鍵ごとの左右の row (最後に渡した組の元)。
#[derive(Default)]
struct Buckets {
    left: std::collections::BTreeMap<u64, std::collections::BTreeSet<EntityId>>,
    right: std::collections::BTreeMap<u64, std::collections::BTreeSet<EntityId>>,
}

enum JoinLive {
    Empty,
    /// 左の鍵付きの購読 (鍵 = ref 列)、 右の table の全 row の購読 (作り直しを見るだけ)、 ref 列。
    Ref { left: enchudb_engine::LiveKeyed, right: enchudb_engine::LiveQuery, via: u16 },
    Eq { left: enchudb_engine::LiveKeyed, right: enchudb_engine::LiveKeyed, state: std::sync::Mutex<Buckets> },
    /// LAG: group の鍵付きの購読 (None = group なし)、 並びの鍵付きの購読。
    Lag { part: Option<enchudb_engine::LiveKeyed>, order: enchudb_engine::LiveKeyed, state: std::sync::Mutex<LagState> },
    /// 左の値の鍵付きの購読、 右の始点 / 終点の鍵付きの購読。
    Range { left: enchudb_engine::LiveKeyed, lo: enchudb_engine::LiveKeyed, hi: enchudb_engine::LiveKeyed, state: std::sync::Mutex<RangeState> },
}

/// 範囲で結ぶ組の区間の索引。 区間 `[lo, hi]` を長さの桁 (`hi - lo` の 2 進の桁数) ごとに、 始点の順に持つ。 値 v を
/// 含む区間は、 桁 c の区間なら始点が `[v - (2^c - 1), v]` に居るので、 桁ごとに 1 回の範囲引きで見つかる
/// (範囲に居て v に届かない区間は、 その桁の中で始点が v の手前 2^(c-1) 以内の短いものだけ)。
#[derive(Default)]
struct Intervals {
    /// 桁ごとの (始点, row, 終点)
    by_len: Vec<std::collections::BTreeSet<(u64, EntityId, u64)>>,
}

impl Intervals {
    fn class(lo: u64, hi: u64) -> usize {
        (u64::BITS - (hi - lo).leading_zeros()) as usize
    }

    fn insert(&mut self, r: EntityId, lo: u64, hi: u64) {
        let c = Self::class(lo, hi);
        if self.by_len.len() <= c {
            self.by_len.resize_with(c + 1, Default::default);
        }
        self.by_len[c].insert((lo, r, hi));
    }

    fn remove(&mut self, r: EntityId, lo: u64, hi: u64) {
        if let Some(s) = self.by_len.get_mut(Self::class(lo, hi)) {
            s.remove(&(lo, r, hi));
        }
    }

    /// v を含む区間 (row, 始点, 終点)。
    fn stab(&self, v: u64, mut f: impl FnMut(EntityId, u64, u64)) {
        for (c, s) in self.by_len.iter().enumerate() {
            if s.is_empty() {
                continue;
            }
            let span = if c == 0 { 0 } else { (1u64 << (c - 1).min(63)).saturating_mul(2) - 1 };
            for &(lo, r, hi) in s.range((v.saturating_sub(span), 0, 0)..=(v, EntityId::MAX, u64::MAX)) {
                if hi >= v {
                    f(r, lo, hi);
                }
            }
        }
    }
}

/// 右の row の区間の変化: (旧区間, 新区間, 作り直したか)。 区間は (始点, 終点)、 None = 組にならない。
type IvChange = (Option<(u64, u64)>, Option<(u64, u64)>, bool);

/// 範囲で結ぶ組の状態 (最後に渡した組の元)。
#[derive(Default)]
struct RangeState {
    /// 左の (値, row)
    points: std::collections::BTreeSet<(u64, EntityId)>,
    /// 右の row の始点 / 終点 (値のある row)
    lo_of: std::collections::BTreeMap<EntityId, u64>,
    hi_of: std::collections::BTreeMap<EntityId, u64>,
    ivs: Intervals,
}

impl RangeState {
    fn iv(&self, r: EntityId) -> Option<(u64, u64)> {
        match (self.lo_of.get(&r), self.hi_of.get(&r)) {
            (Some(&lo), Some(&hi)) if lo <= hi => Some((lo, hi)),
            _ => None,
        }
    }

    /// 差分を当てて組の差分を返す。 抜く側は抜く前の相手と、 足す側は足した後の相手と組む (同じ組を 2 度数えない):
    /// 左の removed × 旧区間 → 旧区間 × (左 − 左の removed) → 左の added × (区間 − 旧区間) → 新区間 × 新しい左。
    /// 居続ける組 (どちらの row も作り直していなくて、 動く前も後も組になる) は、 それぞれの段で出さない
    /// (出してから打ち消すと、 まとめた poll で抜く組を全部集合に入れることになる)。
    fn apply(&mut self, dl: &enchudb_engine::KeyedDelta, dlo: &enchudb_engine::KeyedDelta, dhi: &enchudb_engine::KeyedDelta) -> PairDelta {
        use std::collections::{BTreeMap, BTreeSet};
        let within = |iv: Option<(u64, u64)>, v: u64| iv.is_some_and(|(lo, hi)| lo <= v && v <= hi);
        let mut d = PairDelta::default();
        // 左: 動く前 / 後の値 (作り直した row は別物 = 居続けない)。 鍵付きの購読の差分は eid の昇順なので二分探索で引く
        // (まとめた poll では点ごとに引くので、 木より速い)
        debug_assert!(dl.removed.is_sorted_by_key(|x| x.0) && dl.added.is_sorted_by_key(|x| x.0) && dl.reentered.is_sorted());
        let find = |xs: &[(EntityId, u64)], a: EntityId| xs.binary_search_by_key(&a, |x| x.0).ok().map(|i| xs[i].1);
        let reborn = |a: EntityId| dl.reentered.binary_search(&a).is_ok();
        let old_v = |a: EntityId| find(&dl.removed, a).filter(|_| !reborn(a));
        let new_v = |a: EntityId| find(&dl.added, a).filter(|_| !reborn(a));
        // 右: 区間の変わった (か作り直した) row の (旧区間, 新区間, 作り直したか)
        let rows: BTreeSet<EntityId> = [dlo, dhi].iter().flat_map(|k| k.removed.iter().chain(k.added.iter()).map(|x| x.0)).collect();
        let reborn_r: BTreeSet<EntityId> = dlo.reentered.iter().chain(dhi.reentered.iter()).copied().collect();
        let before: Vec<(EntityId, Option<(u64, u64)>)> = rows.iter().map(|&r| (r, self.iv(r))).collect();
        for (k, map) in [(dlo, &mut self.lo_of), (dhi, &mut self.hi_of)] {
            for (e, _) in &k.removed {
                map.remove(e);
            }
            for &(e, v) in &k.added {
                map.insert(e, v);
            }
        }
        let changed: BTreeMap<EntityId, IvChange> = before
            .into_iter()
            .map(|(r, old)| (r, (old, self.iv(r), reborn_r.contains(&r))))
            .filter(|(_, (old, new, re))| old != new || *re)
            .collect();
        // 1. 左の removed × 旧区間 (居続ける: 左が動いた先も、 右の今の区間に入る)
        for &(a, v) in &dl.removed {
            let nv = new_v(a);
            self.ivs.stab(v, |r, lo, hi| {
                let stays = nv.is_some_and(|nv| match changed.get(&r) {
                    None => lo <= nv && nv <= hi,
                    Some(&(_, new, re)) => !re && within(new, nv),
                });
                if !stays {
                    d.removed.push((a, r));
                }
            });
            self.points.remove(&(v, a));
        }
        // 2. 旧区間 × 残った左 (居続ける: 新区間にも入る)
        for (&r, &(old, new, re)) in &changed {
            if let Some((lo, hi)) = old {
                d.removed.extend(self.points.range((lo, 0)..=(hi, EntityId::MAX)).filter(|&&(v, _)| re || !within(new, v)).map(|&(_, a)| (a, r)));
                self.ivs.remove(r, lo, hi);
            }
        }
        // 3. 左の added × 変わらない区間 (居続ける: 左が動く前の値も入る)
        for &(a, v) in &dl.added {
            self.points.insert((v, a));
            let ov = old_v(a);
            self.ivs.stab(v, |r, lo, hi| {
                if !ov.is_some_and(|ov| lo <= ov && ov <= hi) {
                    d.added.push((a, r));
                }
            });
        }
        // 4. 新区間 × 新しい左 (居続ける: 動かなかった左は旧区間にも入る、 動いた左は動く前の値が旧区間に入る)
        for (&r, &(old, new, re)) in &changed {
            if let Some((lo, hi)) = new {
                d.added.extend(
                    self.points
                        .range((lo, 0)..=(hi, EntityId::MAX))
                        .filter(|&&(v, a)| {
                            re || match find(&dl.added, a) {
                                // 動かなかった左
                                None => !within(old, v),
                                // 作り直した左 / 入ってきた左 (動く前の値なし) / 動いた左
                                Some(_) => old_v(a).is_none_or(|ov| !within(old, ov)),
                            }
                        })
                        .map(|&(_, a)| (a, r)),
                );
                self.ivs.insert(r, lo, hi);
            }
        }
        d
    }
}

/// [`JoinQuery::subscribe`] の戻り値。 組の出入りを購読する。
///
/// - ref で結ぶ組は左の row 1 つにつき高々 1 つ: 書き込み 1 回のコストは普通の購読と同じ
/// - 値で結ぶ組は、 片側の row 1 つの出入り / 鍵の変化で、 同じ鍵のもう片側の row の数だけ組が動く。 左右の
///   row を鍵ごとに持つ (メモリは両側の結果の大きさに比例)
pub struct LiveJoin {
    inner: JoinLive,
    eng: Arc<Engine>,
}

impl LiveJoin {
    /// 前回 poll からの組の差分。
    pub fn poll(&self) -> PairDelta {
        let mut d = PairDelta::default();
        match &self.inner {
            JoinLive::Empty => {}
            JoinLive::Ref { left, right, via } => {
                let peer = self.eng.peer_id();
                let (kd, dr) = (left.poll(&self.eng), right.poll(&self.eng));
                // 作り直した右の row (消した row の eid が別の row になった): 左の row は同じ鍵で集合に居続けるので
                // 鍵付きの購読の差分に出ないが、 組の右は別物 → 居続けた組も出て入り直す
                let rs: std::collections::BTreeSet<EntityId> = dr.removed.iter().copied().collect();
                let reborn: Vec<EntityId> = dr.added.iter().copied().filter(|b| rs.contains(b)).collect();
                if !reborn.is_empty() {
                    let moved: std::collections::BTreeSet<EntityId> = kd.removed.iter().chain(kd.added.iter()).map(|x| x.0).collect();
                    for b in reborn {
                        let k = enchudb_oplog::eid_local(b);
                        for a in self.eng.query_by_id(&[(*via, k)]) {
                            if !moved.contains(&a) && left.reported_key(a) == Some(k as u64) {
                                d.removed.push((a, b));
                                d.added.push((a, b));
                            }
                        }
                    }
                }
                let pair = |(a, k): (EntityId, u64)| (a, enchudb_oplog::make_eid(peer, k as u32));
                d.removed.extend(kd.removed.into_iter().map(pair));
                d.added.extend(kd.added.into_iter().map(pair));
            }
            JoinLive::Eq { left, right, state } => {
                let (dl, dr) = (left.poll(&self.eng), right.poll(&self.eng));
                let mut st = state.lock().unwrap_or_else(|p| p.into_inner());
                let Buckets { left: lb, right: rb } = &mut *st;
                // 左右が同じ鍵から同じ鍵へ一緒に移った組 (居続けている = 出さない)
                let kept = kept_pairs(&dl, &dr);
                let keep = |a: EntityId, b: EntityId| !kept.is_empty() && kept.contains(&(a, b));
                // 抜く側は抜く前の相手と、 足す側は足した後の相手と組む (同じ組を 2 度数えない):
                // 左の removed × 旧右 → 右の removed × (左 − 左の removed) → 左の added × (右 − 右の removed) →
                // 右の added × 新左
                for &(a, k) in &dl.removed {
                    if let Some(bs) = rb.get(&k) {
                        d.removed.extend(bs.iter().filter(|&&b| !keep(a, b)).map(|&b| (a, b)));
                    }
                    take(lb, k, a);
                }
                for &(b, k) in &dr.removed {
                    if let Some(as_) = lb.get(&k) {
                        d.removed.extend(as_.iter().filter(|&&a| !keep(a, b)).map(|&a| (a, b)));
                    }
                    take(rb, k, b);
                }
                for &(a, k) in &dl.added {
                    lb.entry(k).or_default().insert(a);
                    if let Some(bs) = rb.get(&k) {
                        d.added.extend(bs.iter().filter(|&&b| !keep(a, b)).map(|&b| (a, b)));
                    }
                }
                for &(b, k) in &dr.added {
                    rb.entry(k).or_default().insert(b);
                    if let Some(as_) = lb.get(&k) {
                        d.added.extend(as_.iter().filter(|&&a| !keep(a, b)).map(|&a| (a, b)));
                    }
                }
            }
            JoinLive::Lag { part, order, state } => {
                let dp = part.as_ref().map(|q| q.poll(&self.eng));
                let dord = order.poll(&self.eng);
                d = state.lock().unwrap_or_else(|p| p.into_inner()).apply(dp.as_ref(), &dord);
            }
            JoinLive::Range { left, lo, hi, state } => {
                let (dl, dlo, dhi) = (left.poll(&self.eng), lo.poll(&self.eng), hi.poll(&self.eng));
                d = state.lock().unwrap_or_else(|p| p.into_inner()).apply(&dl, &dlo, &dhi);
            }
        }
        // 並べない (順不同): batch で書いてから poll すると組は右の row ごとの run が入り組み、 並べるだけで
        // poll の半分を使う。 積むのに順は要らない
        d
    }
}

/// 左右の row が同じ poll の中で同じ鍵から同じ鍵へ一緒に移った組 (旧鍵の組を抜いて新鍵の組を足す形になるが、
/// 組は居続けている)。 移った row = `removed` と `added` の両方に居て入り直していない row。 入り直した row
/// (eid の使い回し) の組は別物なので含めない。
fn kept_pairs(dl: &enchudb_engine::KeyedDelta, dr: &enchudb_engine::KeyedDelta) -> std::collections::BTreeSet<(EntityId, EntityId)> {
    use std::collections::{BTreeMap, BTreeSet};
    let mut out = BTreeSet::new();
    // row → (旧鍵, 新鍵)
    let moves = |k: &enchudb_engine::KeyedDelta| -> BTreeMap<EntityId, (u64, u64)> {
        if k.removed.is_empty() || k.added.is_empty() {
            return BTreeMap::new();
        }
        let re: BTreeSet<EntityId> = k.reentered.iter().copied().collect();
        let old: BTreeMap<EntityId, u64> = k.removed.iter().copied().collect();
        k.added.iter().filter(|(e, _)| !re.contains(e)).filter_map(|&(e, n)| old.get(&e).map(|&o| (e, (o, n)))).collect()
    };
    let ml = moves(dl);
    if ml.is_empty() {
        return out;
    }
    let mut by_move: BTreeMap<(u64, u64), Vec<EntityId>> = BTreeMap::new();
    for (b, m) in moves(dr) {
        by_move.entry(m).or_default().push(b);
    }
    for (a, m) in ml {
        if let Some(bs) = by_move.get(&m) {
            out.extend(bs.iter().map(|&b| (a, b)));
        }
    }
    out
}

/// 鍵 `k` の組から `e` を抜く (空になった鍵は消す)。
fn take(m: &mut std::collections::BTreeMap<u64, std::collections::BTreeSet<EntityId>>, k: u64, e: EntityId) {
    if let Some(s) = m.get_mut(&k) {
        s.remove(&e);
        if s.is_empty() {
            m.remove(&k);
        }
    }
}

/// [`JoinQuery::subscribe_counts`] / [`JoinQuery::subscribe_sums`] の戻り値。 組を group ごとに数えた件数 (と和)
/// を購読する。 API は [`LiveCounts`] と同じ。
pub struct LiveJoinCounts(JoinCounts);

enum JoinCounts {
    /// ref の組 = 左の row の集計。
    Rows(LiveCounts),
    /// 値の組 = 鍵ごとの左右の集計の積。
    Product {
        left: LiveCounts,
        right: LiveCounts,
        /// 和の列が右 (true) / 左 (false) / 和なし (None)。
        sum_right: Option<bool>,
        /// 鍵ごとに最後に渡した (件数, 和)。
        last: std::sync::Mutex<std::collections::BTreeMap<u64, (u64, i128)>>,
    },
}

impl LiveJoinCounts {
    /// 鍵 `k` の今の組の (件数, 和)。
    fn product(left: &LiveCounts, right: &LiveCounts, sum_right: Option<bool>, k: u64) -> (u64, i128) {
        let (l, r) = (left.inner.get_agg(&left.eng, k), right.inner.get_agg(&right.eng, k));
        let n = l.count * r.count;
        let sum = match sum_right {
            None => 0,
            Some(false) => left.sum_of(l) * r.count as i128,
            Some(true) => l.count as i128 * right.sum_of(r),
        };
        (n, sum)
    }

    /// 前回 poll から件数か和が変わった group と今の (件数, 和) (件数 0 = group が消えた)。 積分 = 値で上書き。
    pub fn poll_sums(&self) -> Vec<(Value, u64, i128)> {
        match &self.0 {
            JoinCounts::Rows(c) => c.poll_sums(),
            JoinCounts::Product { left, right, sum_right, last } => {
                let mut last = last.lock().unwrap_or_else(|p| p.into_inner());
                let mut keys: Vec<u64> = left.inner.poll_sums(&left.eng).into_iter().map(|x| x.0).collect();
                keys.extend(right.inner.poll_sums(&right.eng).into_iter().map(|x| x.0));
                keys.sort_unstable();
                keys.dedup();
                let mut out = Vec::new();
                for k in keys {
                    let now = Self::product(left, right, *sum_right, k);
                    let changed = if now.0 == 0 { last.remove(&k).is_some() } else { last.insert(k, now) != Some(now) };
                    if changed {
                        out.push((left.value(k), now.0, now.1));
                    }
                }
                out
            }
        }
    }

    /// `poll_sums` の件数だけの版 (報告状態を共有する)。
    pub fn poll(&self) -> Vec<(Value, u64)> {
        self.poll_sums().into_iter().map(|(v, n, _)| (v, n)).collect()
    }

    /// 今の全 group と (件数, 和)。
    pub fn all_sums(&self) -> Vec<(Value, u64, i128)> {
        match &self.0 {
            JoinCounts::Rows(c) => c.all_sums(),
            JoinCounts::Product { left, right, sum_right, .. } => left
                .inner
                .all_sums(&left.eng)
                .into_iter()
                .filter_map(|(k, _)| {
                    let (n, sum) = Self::product(left, right, *sum_right, k);
                    (n > 0).then(|| (left.value(k), n, sum))
                })
                .collect(),
        }
    }

    /// 今の全 group と件数。
    pub fn all(&self) -> Vec<(Value, u64)> {
        self.all_sums().into_iter().map(|(v, n, _)| (v, n)).collect()
    }

    /// group `value` の今の件数。
    pub fn get(&self, value: &Value) -> u64 {
        match &self.0 {
            JoinCounts::Rows(c) => c.get(value),
            JoinCounts::Product { left, right, sum_right, .. } => {
                left.raw(value).map_or(0, |k| Self::product(left, right, *sum_right, k).0)
            }
        }
    }

    /// group `value` の今の和。
    pub fn get_sum(&self, value: &Value) -> i128 {
        match &self.0 {
            JoinCounts::Rows(c) => c.get_sum(value),
            JoinCounts::Product { left, right, sum_right, .. } => {
                left.raw(value).map_or(0, |k| Self::product(left, right, *sum_right, k).1)
            }
        }
    }
}

impl std::fmt::Debug for LiveJoinCounts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveJoinCounts").finish_non_exhaustive()
    }
}

// ─────────────────────────── 3 つ以上の table の組 ───────────────────────────

/// 組の 1 つの段: 組に居る table (`parent` 番目) の row に、 新しい table の row をつなぐ。
struct Edge {
    /// つなぐ先の組の位置 (None = `from` の table が組に居ない)。
    parent: Option<usize>,
    on: JoinOn,
}

/// [`JoinQuery::then_ref`] / [`JoinQuery::then_eq`] の戻り値。 3 つ以上の table の row の組 (tuple) を引く / 購読する。
/// 組の並びは table をつないだ順 (最初の 2 つ、 then_* で足した順)。
pub struct MultiJoin<'a> {
    comps: Vec<Query<'a>>,
    edges: Vec<Edge>,
}

/// [`LiveMultiJoin::poll`] の戻り値。 前回 poll からの組の差分 (**順不同**)。 適用順は removed → added。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TupleDelta {
    pub added: Vec<Vec<EntityId>>,
    pub removed: Vec<Vec<EntityId>>,
}

/// 段の計画: 親の row の鍵 (ref の道, 列) と、 子の鍵 (None = 子の eid そのもの = ref で指される、 Some = 子の列)。
struct StepPlan {
    parent: usize,
    key_path: Vec<u16>,
    key_himo: u16,
    child_key: Option<u16>,
}

/// (各 table の条件, 段の計画)。
type MultiPlan = (Vec<Vec<enchudb_engine::LivePred>>, Vec<StepPlan>);

impl<'a> MultiJoin<'a> {
    fn from_pair(j: JoinQuery<'a>) -> Self {
        MultiJoin { comps: vec![j.left, j.right], edges: vec![Edge { parent: Some(0), on: j.on }] }
    }

    fn comp(&self, table: &str) -> Option<usize> {
        let mut hits = self.comps.iter().enumerate().filter(|(_, q)| q.table.name.eq_ignore_ascii_case(table)).map(|(i, _)| i);
        let first = hits.next();
        // 同じ table が 2 度居たら どちらか決められない
        if hits.next().is_some() { None } else { first }
    }

    /// 組の table `from` の row の ref 列 `ref_col` が指す `other` の row をつなぐ。
    pub fn then_ref(mut self, from: &str, ref_col: &str, other: Query<'a>) -> Self {
        let parent = self.comp(from);
        self.comps.push(other);
        self.edges.push(Edge { parent, on: JoinOn::Ref(ref_col.to_string()) });
        self
    }

    /// 組の table `from` の row の列 `my_col` と `other` の列 `their_col` の値が等しい row をつなぐ。
    pub fn then_eq(mut self, from: &str, my_col: &str, other: Query<'a>, their_col: &str) -> Self {
        let parent = self.comp(from);
        self.comps.push(other);
        self.edges.push(Edge { parent, on: JoinOn::Eq(my_col.to_string(), their_col.to_string()) });
        self
    }

    /// 各 table の条件と段の計画。 `None` = 常に 0 組。
    fn plan(self) -> Result<Option<MultiPlan>, SchemaError> {
        let bad = |m: String| Err(SchemaError::BadValue(m));
        if self.comps.iter().any(|q| q.limit.is_some() || q.order.is_some()) {
            return bad("join: limit / order_by are not supported".into());
        }
        let mut steps = Vec::new();
        for (k, e) in self.edges.iter().enumerate() {
            let child = &self.comps[k + 1];
            let Some(p) = e.parent.filter(|&p| p <= k) else {
                return bad(format!("join: the table to join from is not (uniquely) in the tuple (step {})", k + 1));
            };
            let parent = &self.comps[p];
            match &e.on {
                JoinOn::Ref(col) => {
                    let cd = parent.table.col(col).cloned();
                    let points = cd.as_ref().is_some_and(|c| c.ty == ColumnType::Ref)
                        && parent.table.relations.iter().any(|r| {
                            r.from_col.eq_ignore_ascii_case(col) && r.to_table.eq_ignore_ascii_case(&child.table.name)
                        });
                    let Some(cd) = cd.filter(|_| points) else {
                        return bad(format!("join: {col} is not a ref column of {} pointing to {}", parent.table.name, child.table.name));
                    };
                    steps.push(StepPlan { parent: p, key_path: Vec::new(), key_himo: cd.himo_id, child_key: None });
                }
                JoinOn::Range(..) => return bad("join: join_range is not supported in joins of 3 or more tables yet".into()),
                JoinOn::Eq(my, their) => {
                    let Some((path, mine)) = parent.resolve_col(my) else { return bad(format!("join: unknown column {my}")) };
                    let Some(theirs) = child.table.col(their).cloned() else { return bad(format!("join: unknown column {their}")) };
                    if mine.ty != theirs.ty || matches!(mine.ty, ColumnType::Leaf | ColumnType::Ref) {
                        return bad(format!("join: {my} ({:?}) and {their} ({:?}) must have the same Tag / Number / BigInt type", mine.ty, theirs.ty));
                    }
                    steps.push(StepPlan { parent: p, key_path: path, key_himo: mine.himo_id, child_key: Some(theirs.himo_id) });
                }
            }
        }
        let mut preds = Vec::with_capacity(self.comps.len());
        for q in self.comps {
            match q.live_preds()? {
                Some(p) => preds.push(p),
                None => return Ok(None),
            }
        }
        Ok(Some((preds, steps)))
    }

    /// 今の組 (昇順)。
    pub fn find(self) -> Result<Vec<Vec<EntityId>>, SchemaError> {
        let eng = self.comps[0].db.arc_engine();
        let Some((preds, steps)) = self.plan()? else { return Ok(Vec::new()) };
        let io = |e: std::io::Error| SchemaError::BadValue(e.to_string());
        let peer = eng.peer_id();
        let key = |e: EntityId, path: &[u16], h: u16| -> Option<u64> {
            let mut cur = e;
            for &p in path {
                cur = enchudb_oplog::make_eid(peer, eng.get_by_id(cur, p)? as u32);
            }
            eng.get_by_id(cur, h)
        };
        let mut rows = Vec::with_capacity(preds.len());
        for p in preds {
            rows.push(eng.find_by(p).map_err(io)?);
        }
        let mut tuples: Vec<Vec<EntityId>> = rows[0].iter().map(|&e| vec![e]).collect();
        for (k, st) in steps.iter().enumerate() {
            let mut by_key: std::collections::BTreeMap<u64, Vec<EntityId>> = std::collections::BTreeMap::new();
            for &c in &rows[k + 1] {
                let kk = match st.child_key {
                    None => Some(enchudb_oplog::eid_local(c) as u64),
                    Some(h) => key(c, &[], h),
                };
                if let Some(kk) = kk {
                    by_key.entry(kk).or_default().push(c);
                }
            }
            let mut next = Vec::new();
            for t in tuples {
                if let Some(cs) = key(t[st.parent], &st.key_path, st.key_himo).and_then(|kk| by_key.get(&kk)) {
                    for &c in cs {
                        let mut u = t.clone();
                        u.push(c);
                        next.push(u);
                    }
                }
            }
            tuples = next;
        }
        tuples.sort_unstable();
        Ok(tuples)
    }

    /// 今の組の数。
    pub fn count(self) -> Result<usize, SchemaError> {
        Ok(self.find()?.len())
    }

    /// 組を購読する。 初回 poll は登録時点の全部の組が `added`。 どの table の row の出入り・つなぐ列の書き換え
    /// (ref の付け替え、 ref の先の値の変化も) でも届く。 drop で購読解除、 `Database` を借用しない。
    ///
    /// 段ごとに、 親の row の鍵を鍵付きの購読で、 子の row を購読で追い、 前の段の組を親の row の今の鍵で束ねて子と
    /// 組む (差分の JOIN: 抜く側は抜く前の相手と、 足す側は足した後の相手と)。 メモリは各段の組の数に比例。
    pub fn subscribe(self) -> Result<LiveMultiJoin, SchemaError> {
        let eng = self.comps[0].db.arc_engine();
        let io = |e: std::io::Error| SchemaError::BadValue(e.to_string());
        let Some((preds, steps)) = self.plan()? else {
            return Ok(LiveMultiJoin { eng, root: None, steps: Vec::new(), state: Default::default() });
        };
        let root = eng.subscribe(preds[0].clone()).map_err(io)?;
        let mut live = Vec::with_capacity(steps.len());
        for (k, st) in steps.into_iter().enumerate() {
            let parent_key = eng.subscribe_keyed(preds[st.parent].clone(), st.key_path, st.key_himo).map_err(io)?;
            let child = match st.child_key {
                None => ChildStream::Rows(eng.subscribe(preds[k + 1].clone()).map_err(io)?),
                Some(h) => ChildStream::Keyed(eng.subscribe_keyed(preds[k + 1].clone(), Vec::new(), h).map_err(io)?),
            };
            live.push(LiveStep { parent: st.parent, parent_key, child });
        }
        let n = live.len();
        let state = ((0..=n).map(|_| Arena::default()).collect(), (0..n).map(|_| StepState::default()).collect());
        Ok(LiveMultiJoin { eng, root: Some(root), steps: live, state: std::sync::Mutex::new(state) })
    }
}

/// 段の子の row の流れ。
enum ChildStream {
    /// ref で指される子: 出入りだけ (鍵 = 子の local eid)。
    Rows(enchudb_engine::LiveQuery),
    /// 値でつなぐ子: 鍵付き。
    Keyed(enchudb_engine::LiveKeyed),
}

struct LiveStep {
    parent: usize,
    parent_key: enchudb_engine::LiveKeyed,
    child: ChildStream,
}

/// 組の段ごとの置き場。 段 k の組 = (段 k-1 の組の番号, 足した row) を 1 回だけ持ち、 番号で指す (組を複製しない)。
/// 消えた組の番号は poll が終わってから空ける (同じ poll の後の段がまだ組の中身を読む)。
#[derive(Default)]
struct Arena {
    prev: Vec<u32>,
    row: Vec<EntityId>,
    free: Vec<u32>,
    /// (段 k-1 の組の番号, row) を 1 つの u128 に詰めた鍵 → 番号 (比較 1 回で済む)。
    index: std::collections::BTreeMap<u128, u32>,
    /// この poll で消えた番号 (poll の終わりに `free` へ)。
    dead: Vec<u32>,
}

#[inline]
fn pack(prev: u32, row: EntityId) -> u128 {
    ((prev as u128) << 64) | row as u128
}

/// 段 0 の組の 「前の段の組」 の番号。
const ROOT: u32 = u32::MAX;

impl Arena {
    fn alloc(&mut self, prev: u32, row: EntityId) -> u32 {
        let id = match self.free.pop() {
            Some(i) => {
                self.prev[i as usize] = prev;
                self.row[i as usize] = row;
                i
            }
            None => {
                self.prev.push(prev);
                self.row.push(row);
                (self.prev.len() - 1) as u32
            }
        };
        self.index.insert(pack(prev, row), id);
        id
    }

    /// 組 (prev, row) を消す (番号は poll の終わりまで読める)。
    fn kill(&mut self, prev: u32, row: EntityId) -> Option<u32> {
        let id = self.index.remove(&pack(prev, row))?;
        self.dead.push(id);
        Some(id)
    }
}

/// 段 `level` の組 `id` の位置 `pos` の row。
fn row_at(arenas: &[Arena], mut level: usize, mut id: u32, pos: usize) -> EntityId {
    while level > pos {
        id = arenas[level].prev[id as usize];
        level -= 1;
    }
    arenas[level].row[id as usize]
}

/// 段 `level` の組 `id` の中身。
fn tuple_at(arenas: &[Arena], level: usize, id: u32) -> Vec<EntityId> {
    (0..=level).map(|pos| row_at(arenas, level, id, pos)).collect()
}

/// 段の状態 (最後に渡した組の元)。 集合は (鍵, 番号) の組を 1 本の木で持つ (鍵ごとの小さな木を作らない)。
#[derive(Default)]
struct StepState {
    /// (親の row, 前の段の組の番号)。
    by_row: std::collections::BTreeSet<(EntityId, u32)>,
    /// 親の row の鍵。
    pkey: std::collections::BTreeMap<EntityId, u64>,
    /// (鍵, 前の段の組の番号) (親の row に鍵のある組)。
    kl: std::collections::BTreeSet<(u64, u32)>,
    /// (鍵, 子の row)。
    right: std::collections::BTreeSet<(u64, EntityId)>,
}

/// 組の番号の差分。
#[derive(Default)]
struct IdDelta {
    removed: Vec<u32>,
    added: Vec<u32>,
}

/// 鍵付きの差分 (組の番号 / row → 鍵) と、 入り直したもの。
struct Keyed<T> {
    removed: Vec<(T, u64)>,
    added: Vec<(T, u64)>,
    reentered: std::collections::BTreeSet<T>,
}

/// [`MultiJoin::subscribe`] の戻り値。 3 つ以上の table の組の出入りを購読する。
pub struct LiveMultiJoin {
    eng: Arc<Engine>,
    root: Option<enchudb_engine::LiveQuery>,
    steps: Vec<LiveStep>,
    state: std::sync::Mutex<(Vec<Arena>, Vec<StepState>)>,
}

impl LiveMultiJoin {
    /// 前回 poll からの組の差分。
    pub fn poll(&self) -> TupleDelta {
        let Some(root) = &self.root else { return TupleDelta::default() };
        let mut guard = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let (arenas, states) = &mut *guard;
        let d0 = root.poll(&self.eng);
        let mut d = IdDelta::default();
        // 段 0: row そのもの (消えて入り直した row は別の番号になる)
        for e in d0.removed {
            d.removed.extend(arenas[0].kill(ROOT, e));
        }
        for e in d0.added {
            d.added.push(arenas[0].alloc(ROOT, e));
        }
        for (k, (step, st)) in self.steps.iter().zip(states.iter_mut()).enumerate() {
            let (done, rest) = arenas.split_at_mut(k + 1);
            d = step.step(&self.eng, st, done, &mut rest[0], d);
        }
        let last = arenas.len() - 1;
        let out = TupleDelta {
            removed: d.removed.iter().map(|&id| tuple_at(arenas, last, id)).collect(),
            added: d.added.iter().map(|&id| tuple_at(arenas, last, id)).collect(),
        };
        for a in arenas.iter_mut() {
            let dead = std::mem::take(&mut a.dead);
            a.free.extend(dead);
        }
        out
    }
}

impl LiveStep {
    /// 段 k (前の段 = `prev[k]`、 新しい段 = `next`) の差分。
    fn step(&self, eng: &Engine, st: &mut StepState, prev: &[Arena], next: &mut Arena, dleft: IdDelta) -> IdDelta {
        use std::collections::BTreeSet;
        let (p, lv) = (self.parent, prev.len() - 1);
        let row = |t: u32| row_at(prev, lv, t, p);
        let dp = self.parent_key.poll(eng);
        let dc: Keyed<EntityId> = match &self.child {
            ChildStream::Rows(q) => {
                let d = q.poll(eng);
                let key = |e: EntityId| enchudb_oplog::eid_local(e) as u64;
                let rs: BTreeSet<EntityId> = d.removed.iter().copied().collect();
                Keyed {
                    reentered: d.added.iter().copied().filter(|e| rs.contains(e)).collect(),
                    removed: d.removed.into_iter().map(|e| (e, key(e))).collect(),
                    added: d.added.into_iter().map(|e| (e, key(e))).collect(),
                }
            }
            ChildStream::Keyed(q) => {
                let d = q.poll(eng);
                Keyed { reentered: d.reentered.into_iter().collect(), removed: d.removed, added: d.added }
            }
        };
        // ── 前の段の組の鍵付きの差分 (組の出入り + 親の row の鍵の変化)。 番号は出入りのたびに新しいので、 同じ番号が
        // removed と added の両方に来ることは無い (入り直した組は別の番号)
        let gone: BTreeSet<u32> = dleft.removed.iter().copied().collect();
        let born: BTreeSet<u32> = dleft.added.iter().copied().collect();
        let mut affected: BTreeSet<u32> = gone.union(&born).copied().collect();
        for r in dp.removed.iter().chain(dp.added.iter()).map(|x| x.0) {
            affected.extend(st.by_row.range((r, 0)..=(r, u32::MAX)).map(|x| x.1));
        }
        // 親の row が入り直した (eid の使い回し) なら、 その row を含む組は別物
        let re_rows: BTreeSet<EntityId> = dp.reentered.iter().copied().collect();
        let old: Vec<(u32, EntityId, Option<u64>)> = affected
            .iter()
            .map(|&t| {
                let r = row(t);
                (t, r, if born.contains(&t) { None } else { st.pkey.get(&r).copied() })
            })
            .collect();
        for &t in &dleft.removed {
            st.by_row.remove(&(row(t), t));
        }
        for &t in &dleft.added {
            st.by_row.insert((row(t), t));
        }
        for (r, _) in &dp.removed {
            st.pkey.remove(r);
        }
        for &(r, k) in &dp.added {
            st.pkey.insert(r, k);
        }
        let mut dl: Keyed<u32> = Keyed { removed: Vec::new(), added: Vec::new(), reentered: BTreeSet::new() };
        for (t, r, was) in old {
            let now = if gone.contains(&t) { None } else { st.pkey.get(&r).copied() };
            let re = re_rows.contains(&r);
            if was == now && !re {
                continue;
            }
            if let Some(w) = was {
                dl.removed.push((t, w));
            }
            if let Some(n) = now {
                dl.added.push((t, n));
            }
            if re && was.is_some() && now.is_some() {
                dl.reentered.insert(t);
            }
        }
        // ── 前の段の組 × 子の row (値で結ぶ 2 つの table の組と同じ手順): 抜く側は抜く前の相手と、 足す側は足した後の相手と
        let kept = kept_ids(&dl, &dc);
        let keep = |t: u32, c: EntityId| !kept.is_empty() && kept.contains(&(t, c));
        let mut out = IdDelta::default();
        for &(t, k) in &dl.removed {
            for &(_, c) in st.right.range((k, 0)..=(k, u64::MAX)) {
                if !keep(t, c) {
                    out.removed.extend(next.kill(t, c));
                }
            }
            st.kl.remove(&(k, t));
        }
        for &(c, k) in &dc.removed {
            for &(_, t) in st.kl.range((k, 0)..=(k, u32::MAX)) {
                if !keep(t, c) {
                    out.removed.extend(next.kill(t, c));
                }
            }
            st.right.remove(&(k, c));
        }
        for &(t, k) in &dl.added {
            st.kl.insert((k, t));
            for &(_, c) in st.right.range((k, 0)..=(k, u64::MAX)) {
                if !keep(t, c) {
                    out.added.push(next.alloc(t, c));
                }
            }
        }
        for &(c, k) in &dc.added {
            st.right.insert((k, c));
            for &(_, t) in st.kl.range((k, 0)..=(k, u32::MAX)) {
                if !keep(t, c) {
                    out.added.push(next.alloc(t, c));
                }
            }
        }
        out
    }
}

/// 前の段の組と子の row が同じ poll の中で同じ鍵から同じ鍵へ一緒に移った組 (居続けている = 出さない)。
fn kept_ids(dl: &Keyed<u32>, dc: &Keyed<EntityId>) -> std::collections::BTreeSet<(u32, EntityId)> {
    use std::collections::{BTreeMap, BTreeSet};
    fn moves<T: Ord + Copy>(k: &Keyed<T>) -> BTreeMap<T, (u64, u64)> {
        if k.removed.is_empty() || k.added.is_empty() {
            return BTreeMap::new();
        }
        let old: BTreeMap<T, u64> = k.removed.iter().copied().collect();
        k.added.iter().filter(|(e, _)| !k.reentered.contains(e)).filter_map(|&(e, n)| old.get(&e).map(|&o| (e, (o, n)))).collect()
    }
    let mut out = BTreeSet::new();
    let ml = moves(dl);
    if ml.is_empty() {
        return out;
    }
    let mut by_move: BTreeMap<(u64, u64), Vec<EntityId>> = BTreeMap::new();
    for (c, m) in moves(dc) {
        by_move.entry(m).or_default().push(c);
    }
    for (t, m) in ml {
        if let Some(cs) = by_move.get(&m) {
            out.extend(cs.iter().map(|&c| (t, c)));
        }
    }
    out
}

// ─────────────────────────── window: 1 つ前の row (LAG) ───────────────────────────

/// [`Query::lag`] / [`Query::lag_all`] の戻り値。 同じ group の中で並びが 1 つ前の row との組を引く / 購読する。
pub struct LagQuery<'a> {
    rows: Query<'a>,
    /// group の列 (None = 結果全体で 1 つの group)
    part: Option<String>,
    order: String,
}

/// LAG の計画: 条件、 group の鍵 (ref の道 + 列)、 並びの鍵 (ref の道 + 列)。
type LagPlan = (Vec<enchudb_engine::LivePred>, Option<(Vec<u16>, u16)>, (Vec<u16>, u16));

impl<'a> LagQuery<'a> {
    fn plan(self) -> Result<Option<LagPlan>, SchemaError> {
        let bad = |m: String| Err(SchemaError::BadValue(m));
        if self.rows.limit.is_some() || self.rows.order.is_some() {
            return bad("lag: limit / order_by are not supported (the order is the lag's own)".into());
        }
        let part = match &self.part {
            None => None,
            Some(p) => match self.rows.resolve_col(p) {
                Some((path, c)) if c.ty != ColumnType::Leaf => Some((path, c.himo_id)),
                _ => return bad(format!("lag: {p} is not a column to group by (unknown or Leaf)")),
            },
        };
        let order = match self.rows.resolve_col(&self.order) {
            Some((path, c)) if matches!(c.ty, ColumnType::Number | ColumnType::BigInt) => (path, c.himo_id),
            _ => return bad(format!("lag: {} is not a Number / BigInt column to order by", self.order)),
        };
        let Some(preds) = self.rows.live_preds()? else { return Ok(None) };
        Ok(Some((preds, part, order)))
    }

    /// 今の組 `(row, 1 つ前の row)` (row の昇順)。 group の先頭の row は組にならない。
    pub fn find(self) -> Result<Vec<(EntityId, EntityId)>, SchemaError> {
        let eng = self.rows.db.arc_engine();
        let io = |e: std::io::Error| SchemaError::BadValue(e.to_string());
        let Some((preds, part, (opath, oh))) = self.plan()? else { return Ok(Vec::new()) };
        let peer = eng.peer_id();
        let key = |e: EntityId, path: &[u16], h: u16| -> Option<u64> {
            let mut cur = e;
            for &p in path {
                cur = enchudb_oplog::make_eid(peer, eng.get_by_id(cur, p)? as u32);
            }
            eng.get_by_id(cur, h)
        };
        let mut rows: Vec<(u64, u64, EntityId)> = Vec::new();
        for e in eng.find_by(preds).map_err(io)? {
            let p = match &part {
                None => Some(0),
                Some((path, h)) => key(e, path, *h),
            };
            if let (Some(p), Some(o)) = (p, key(e, &opath, oh)) {
                rows.push((p, o, e));
            }
        }
        rows.sort_unstable();
        let mut out: Vec<(EntityId, EntityId)> = rows.windows(2).filter(|w| w[0].0 == w[1].0).map(|w| (w[1].2, w[0].2)).collect();
        out.sort_unstable();
        Ok(out)
    }

    /// 今の組の数。
    pub fn count(self) -> Result<usize, SchemaError> {
        Ok(self.find()?.len())
    }

    /// 組 `(row, 1 つ前の row)` を購読する (`LiveJoin`、 poll は `PairDelta`)。 初回 poll は登録時点の全部の組が `added`。
    /// row の出入り・group / 並びの列の書き換え (ref の先の値も) で届く。 書き込み 1 回で動く組は高々 3 つずつ
    /// (抜けた所の前後がつながり、 入った所の前後が切れる)。
    pub fn subscribe(self) -> Result<LiveJoin, SchemaError> {
        let eng = self.rows.db.arc_engine();
        let io = |e: std::io::Error| SchemaError::BadValue(e.to_string());
        let inner = match self.plan()? {
            None => JoinLive::Empty,
            Some((preds, part, (opath, oh))) => JoinLive::Lag {
                part: match part {
                    Some((path, h)) => Some(eng.subscribe_keyed(preds.clone(), path, h).map_err(io)?),
                    None => None,
                },
                order: eng.subscribe_keyed(preds, opath, oh).map_err(io)?,
                state: Default::default(),
            },
        };
        Ok(LiveJoin { inner, eng })
    }
}

/// LAG の購読の状態。
#[derive(Default)]
struct LagState {
    /// row → (group, 並びの値)。 group の無い LAG は group を 0 で持つ
    vals: std::collections::BTreeMap<EntityId, (Option<u64>, Option<u64>)>,
    /// (group, 並びの値, row)
    seq: std::collections::BTreeSet<(u64, u64, EntityId)>,
    /// 最後に渡した 1 つ前の row
    prev_of: std::collections::BTreeMap<EntityId, EntityId>,
}

/// 並びの中の位置 (group, 並びの値, row)。
type LagPos = (u64, u64, EntityId);

impl LagState {
    fn next(&self, at: LagPos) -> Option<LagPos> {
        use std::ops::Bound::{Excluded, Unbounded};
        self.seq.range((Excluded(at), Unbounded)).next().filter(|n| n.0 == at.0).copied()
    }

    fn prev(&self, at: LagPos) -> Option<EntityId> {
        self.seq.range(..at).next_back().filter(|n| n.0 == at.0).map(|n| n.2)
    }

    /// 差分を当てて組の差分を返す。 1 つ前が変わりうる row = 動いた row と、 動く前 / 後の位置の直後の row。
    /// それぞれの今の 1 つ前を渡し済みのものと比べる (作り直した row が組のどちらかに居れば、 同じでも出て入り直す)。
    /// 動いた row の値は鍵付きの購読の差分 (eid の昇順) を二分探索で引き、 値の表は row ごとに 1 回だけ引く。
    fn apply(&mut self, dp: Option<&enchudb_engine::KeyedDelta>, dord: &enchudb_engine::KeyedDelta) -> PairDelta {
        use std::collections::btree_map::Entry;
        let deltas: Vec<&enchudb_engine::KeyedDelta> = dp.into_iter().chain(std::iter::once(dord)).collect();
        let mut moved: Vec<EntityId> = deltas.iter().flat_map(|k| k.removed.iter().chain(k.added.iter()).map(|x| x.0)).collect();
        moved.sort_unstable();
        moved.dedup();
        let mut reborn: Vec<EntityId> = deltas.iter().flat_map(|k| k.reentered.iter().copied()).collect();
        reborn.sort_unstable();
        let is_reborn = |e: EntityId| reborn.binary_search(&e).is_ok();
        // 列の新しい値: added に居ればその値、 removed だけに居れば無し、 どちらにも居なければ元のまま
        let step = |k: &enchudb_engine::KeyedDelta, e: EntityId, old: Option<u64>| -> Option<u64> {
            match k.added.binary_search_by_key(&e, |x| x.0) {
                Ok(i) => Some(k.added[i].1),
                Err(_) if k.removed.binary_search_by_key(&e, |x| x.0).is_ok() => None,
                Err(_) => old,
            }
        };
        let pos = |e: EntityId, v: (Option<u64>, Option<u64>)| Some((v.0?, v.1?, e));
        // (row, 動く前の位置, 動いた後の位置)
        let mut ups: Vec<(EntityId, Option<LagPos>, Option<LagPos>)> = Vec::with_capacity(moved.len());
        for &e in &moved {
            let next = |old: (Option<u64>, Option<u64>)| {
                let p = match dp {
                    Some(k) => step(k, e, old.0),
                    None => Some(0),
                };
                (p, step(dord, e, old.1))
            };
            let (old, new) = match self.vals.entry(e) {
                Entry::Occupied(mut o) => {
                    let old = *o.get();
                    let new = next(old);
                    if new == (None, None) {
                        o.remove();
                    } else {
                        *o.get_mut() = new;
                    }
                    (old, new)
                }
                Entry::Vacant(v) => {
                    let new = next((None, None));
                    if new != (None, None) {
                        v.insert(new);
                    }
                    ((None, None), new)
                }
            };
            ups.push((e, pos(e, old), pos(e, new)));
        }
        // 1 つ前が変わりうる row と、 その今の位置 (動いた row は動いた後の位置で上書きする)
        let mut touched: Vec<(EntityId, Option<LagPos>)> = Vec::new();
        for &(_, old, _) in &ups {
            if let Some(n) = old.and_then(|at| self.next(at)) {
                touched.push((n.2, Some(n)));
            }
        }
        for &(_, old, _) in &ups {
            if let Some(at) = old {
                self.seq.remove(&at);
            }
        }
        for &(_, _, new) in &ups {
            if let Some(at) = new {
                self.seq.insert(at);
            }
        }
        for &(_, _, new) in &ups {
            if let Some(n) = new.and_then(|at| self.next(at)) {
                touched.push((n.2, Some(n)));
            }
        }
        touched.retain(|t| moved.binary_search(&t.0).is_err());
        touched.extend(ups.iter().map(|&(e, _, new)| (e, new)));
        touched.sort_unstable_by_key(|t| t.0);
        touched.dedup_by_key(|t| t.0);
        let mut d = PairDelta::default();
        for (x, at) in touched {
            let now = at.and_then(|at| self.prev(at));
            let again = is_reborn(x) || now.is_some_and(is_reborn);
            match (self.prev_of.entry(x), now) {
                (Entry::Occupied(o), _) if Some(*o.get()) == now && !again => {}
                (Entry::Vacant(_), None) => {}
                (Entry::Occupied(mut o), Some(n)) => {
                    d.removed.push((x, *o.get()));
                    d.added.push((x, n));
                    o.insert(n);
                }
                (Entry::Occupied(o), None) => {
                    d.removed.push((x, *o.get()));
                    o.remove();
                }
                (Entry::Vacant(v), Some(n)) => {
                    d.added.push((x, n));
                    v.insert(n);
                }
            }
        }
        d
    }
}

// ─────────────────────────── 再帰 (階層の配下 / 上) ───────────────────────────

/// [`Query::under`] / [`Query::above`] の戻り値。 階層の配下 (または上) の row を引く / 購読する。
pub struct UnderQuery<'a> {
    rows: Query<'a>,
    seeds: Query<'a>,
    ref_col: String,
    /// 上向き (seed の上司をたどる、 [`Query::above`])
    up: bool,
}

/// 配下の計画: (結果を絞る条件, seed の条件, table の全 row の条件, ref 列)。
type UnderPlan = (Vec<enchudb_engine::LivePred>, Vec<enchudb_engine::LivePred>, Vec<enchudb_engine::LivePred>, u16);

impl<'a> UnderQuery<'a> {
    fn plan(self) -> Result<Option<UnderPlan>, SchemaError> {
        let bad = |m: String| Err(SchemaError::BadValue(m));
        let (rows, seeds) = (self.rows, self.seeds);
        let what = if self.up { "above" } else { "under" };
        if rows.limit.is_some() || rows.order.is_some() || seeds.limit.is_some() || seeds.order.is_some() {
            return bad(format!("{what}: limit / order_by are not supported"));
        }
        if !Arc::ptr_eq(&rows.table, &seeds.table) {
            return bad(format!("{what}: seeds must be a query on {}", rows.table.name));
        }
        let t = &rows.table;
        let cd = t.col(&self.ref_col).cloned();
        let points = cd.as_ref().is_some_and(|c| c.ty == ColumnType::Ref)
            && t.relations.iter().any(|r| r.from_col.eq_ignore_ascii_case(&self.ref_col) && r.to_table.eq_ignore_ascii_case(&t.name));
        let Some(cd) = cd.filter(|_| points) else {
            return bad(format!("{what}: {} is not a ref column of {} pointing to itself", self.ref_col, t.name));
        };
        let all = Query::new(rows.db, t.clone());
        let (Some(f), Some(sd), Some(a)) = (rows.live_preds()?, seeds.live_preds()?, all.live_preds()?) else { return Ok(None) };
        Ok(Some((f, sd, a, cd.himo_id)))
    }

    /// 今の配下 (または上) の row (昇順)。
    pub fn find(self) -> Result<Vec<EntityId>, SchemaError> {
        let eng = self.rows.db.arc_engine();
        let io = |e: std::io::Error| SchemaError::BadValue(e.to_string());
        let up = self.up;
        let Some((f, sd, a, via)) = self.plan()? else { return Ok(Vec::new()) };
        let peer = eng.peer_id();
        let local = |e: EntityId| enchudb_oplog::eid_local(e);
        let mut h = Hier::new(up);
        for e in eng.find_by(a).map_err(io)? {
            if let Some(p) = eng.get_by_id(e, via) {
                h.link(local(e), p as u32, &mut Default::default());
            }
        }
        let mut touched = Vec::new();
        for e in eng.find_by(sd).map_err(io)? {
            h.set_seed(local(e), true, &mut touched);
        }
        let mut out: Vec<EntityId> =
            eng.find_by(f).map_err(io)?.into_iter().filter(|&e| h.find_answer(local(e))).map(|e| enchudb_oplog::make_eid(peer, local(e))).collect();
        out.sort_unstable();
        Ok(out)
    }

    /// 今の配下 (または上) の row の数。
    pub fn count(self) -> Result<usize, SchemaError> {
        Ok(self.find()?.len())
    }

    /// 配下 (または上) の row の出入りを購読する。 初回 poll は登録時点の全部が `added`。 親の付け替え (異動・部署の移動)、
    /// seed の出入り、 この query の条件の変化で届く。 下向きは、 付け替えで配下の答えが変わらない row の下は見に行かない。
    /// 上向きは、 付け替え 1 回・seed の出入り 1 回が階層の深さに比例。
    pub fn subscribe(self) -> Result<LiveUnder, SchemaError> {
        let eng = self.rows.db.arc_engine();
        let io = |e: std::io::Error| SchemaError::BadValue(e.to_string());
        let up = self.up;
        let state = std::sync::Mutex::new(UnderState { hier: Hier::new(up), filt: Default::default(), reported: Default::default() });
        let Some((f, sd, a, via)) = self.plan()? else {
            return Ok(LiveUnder { eng, live: None, state });
        };
        let parents = eng.subscribe_keyed(a, Vec::new(), via).map_err(io)?;
        let seeds = eng.subscribe(sd).map_err(io)?;
        let filter = eng.subscribe(f).map_err(io)?;
        Ok(LiveUnder { eng, live: Some((parents, seeds, filter)), state })
    }
}

/// 階層 (子 → 親) と seed、 配下の答え。 row は local eid。
#[derive(Default)]
struct Tree {
    parent: std::collections::BTreeMap<u32, u32>,
    children: std::collections::BTreeSet<(u32, u32)>,
    seed: std::collections::BTreeSet<u32>,
    /// 配下である row (答えが真)。
    under: std::collections::BTreeSet<u32>,
}

impl Tree {
    fn set_parent(&mut self, c: u32, p: Option<u32>) {
        if let Some(old) = self.parent.remove(&c) {
            self.children.remove(&(old, c));
        }
        if let Some(p) = p {
            self.parent.insert(c, p);
            self.children.insert((p, c));
        }
    }

    /// 親をたどって seed に着くか (根 / 輪に着いたら偽)。 階層の深さに比例。
    fn walk(&self, r: u32) -> bool {
        let mut cur = r;
        let mut seen = std::collections::BTreeSet::new();
        while let Some(&p) = self.parent.get(&cur) {
            if self.seed.contains(&p) {
                return true;
            }
            if p == r || !seen.insert(p) {
                return false;
            }
            cur = p;
        }
        false
    }

    /// `roots` の答えを親をたどって決め直し、 答えが変わった row の子へ下向きに伝える (子の答え = 親が seed か配下か)。
    /// 答えが変わった row を返す。
    fn settle(&mut self, roots: impl IntoIterator<Item = u32>) -> Vec<u32> {
        let mut changed = Vec::new();
        let mut queue = std::collections::VecDeque::new();
        for r in roots {
            let now = self.walk(r);
            if now != self.under.contains(&r) {
                if now { self.under.insert(r) } else { self.under.remove(&r) };
                changed.push(r);
                queue.push_back(r);
            }
        }
        while let Some(x) = queue.pop_front() {
            let v = self.seed.contains(&x) || self.under.contains(&x);
            let kids: Vec<u32> = self.children.range((x, 0)..=(x, u32::MAX)).map(|k| k.1).collect();
            for c in kids {
                if v != self.under.contains(&c) {
                    if v { self.under.insert(c) } else { self.under.remove(&c) };
                    changed.push(c);
                    queue.push_back(c);
                }
            }
        }
        changed
    }
}

/// 上向き (seed の上司全員) の状態。 row x の `sub[x]` = x 自身か x の下 (何段下でも) に居る seed の数。 輪の上の row は、
/// 輪につながる全員 (輪と、 輪にぶら下がる木) の seed の数。 x が答え ⇔ `sub[x]` が x 自身の seed を除いて 1 以上。
///
/// 付け替えは 「切る」 と 「つなぐ」 に分ける。 切る時は旧い親から上へ `sub[c]` を引き、 つなぐ時は新しい親から上へ足す
/// (上へは輪を 1 周したら止まる)。 輪の上の row は 「自分と、 自分にぶら下がる木の seed の数」 (`own`) も持つ。 輪の上の
/// row を切ると輪が c を根とする鎖にほどけるので、 鎖の `sub` を `own` から数え直す。 つないで輪ができたら (新しい親が
/// c の下に居る)、 輪の row の `own` は道の隣どうしの `sub` の差、 `sub` は c の木の seed の数。 子の一覧は持たない。
#[derive(Default)]
struct Above {
    parent: std::collections::BTreeMap<u32, u32>,
    seed: std::collections::BTreeSet<u32>,
    sub: std::collections::BTreeMap<u32, u64>,
    /// 輪の上の row の own (0 は持たない)。
    own: std::collections::BTreeMap<u32, u64>,
}

impl Above {
    fn sub(&self, x: u32) -> u64 {
        self.sub.get(&x).copied().unwrap_or(0)
    }

    fn put(&mut self, x: u32, v: u64, touched: &mut Vec<u32>) {
        use std::collections::btree_map::Entry;
        match self.sub.entry(x) {
            Entry::Occupied(mut o) if *o.get() != v => {
                touched.push(x);
                if v == 0 {
                    o.remove();
                } else {
                    o.insert(v);
                }
            }
            Entry::Vacant(o) if v != 0 => {
                touched.push(x);
                o.insert(v);
            }
            _ => {}
        }
    }

    /// x と、 x から親をたどった row (輪を 1 周したら止まる)。 輪に着いたら、 輪に入った row も返す。
    fn up_from(&self, x: u32) -> (Vec<u32>, Option<u32>) {
        let mut out = vec![x];
        // 輪の検出: 浅いうちは道を線形に見る (階層は普通浅い)、 深くなったら集合に
        let mut seen: Option<std::collections::BTreeSet<u32>> = None;
        let mut cur = x;
        while let Some(&p) = self.parent.get(&cur) {
            let again = match seen.as_mut() {
                Some(set) => !set.insert(p),
                None if out.len() < 32 => out.contains(&p),
                None => {
                    let mut set: std::collections::BTreeSet<u32> = out.iter().copied().collect();
                    let again = !set.insert(p);
                    seen = Some(set);
                    again
                }
            };
            if again {
                return (out, Some(p));
            }
            out.push(p);
            cur = p;
        }
        (out, None)
    }

    /// x から上の全員に ±k。
    fn shift(&mut self, x: u32, k: u64, add: bool, touched: &mut Vec<u32>) {
        if k == 0 {
            return;
        }
        let (path, entry) = self.up_from(x);
        for y in path {
            let v = if add { self.sub(y) + k } else { self.sub(y) - k };
            self.put(y, v, touched);
        }
        if let Some(r) = entry {
            let o = self.own.get(&r).copied().unwrap_or(0);
            let o = if add { o + k } else { o - k };
            if o == 0 { self.own.remove(&r) } else { self.own.insert(r, o) };
        }
    }

    fn set_seed(&mut self, s: u32, on: bool, touched: &mut Vec<u32>) {
        if self.seed.contains(&s) == on {
            return;
        }
        if on { self.seed.insert(s) } else { self.seed.remove(&s) };
        touched.push(s);
        self.shift(s, 1, on, touched);
    }

    fn cut(&mut self, c: u32, touched: &mut Vec<u32>) {
        let Some(p) = self.parent.remove(&c) else { return };
        let k = self.sub(c);
        if k == 0 {
            // c の下にも (輪なら、 つながる全員にも) seed が居ない: 誰の数も変わらない
            return;
        }
        let (path, _) = self.up_from(p);
        if path.last() != Some(&c) {
            self.shift(p, k, false, touched);
            return;
        }
        // c は輪の上だった: 輪 (p → … → c) が c を根とする鎖にほどける。 p が一番下
        let mut acc = 0;
        for y in path {
            acc += self.own.remove(&y).unwrap_or(0);
            self.put(y, acc, touched);
        }
    }

    /// c の親を `to` にする。 下に seed の居ない row (ほとんど) は親の表を 1 回引くだけ。
    fn reparent(&mut self, c: u32, to: Option<u32>, touched: &mut Vec<u32>) {
        use std::collections::btree_map::Entry;
        if self.sub(c) == 0 {
            // c の下にも (輪なら、 つながる全員にも) seed が居ない: 誰の数も変わらない。 輪ができても c の木なので 0 のまま
            match (self.parent.entry(c), to) {
                (Entry::Occupied(mut o), Some(q)) => {
                    o.insert(q);
                }
                (Entry::Occupied(o), None) => {
                    o.remove();
                }
                (Entry::Vacant(v), Some(q)) => {
                    v.insert(q);
                }
                (Entry::Vacant(_), None) => {}
            }
            return;
        }
        if self.parent.get(&c).copied() == to {
            return;
        }
        self.cut(c, touched);
        if let Some(q) = to {
            self.link(c, q, touched);
        }
    }

    /// 根 c を q の子にする。
    fn link(&mut self, c: u32, q: u32, touched: &mut Vec<u32>) {
        let k = self.sub(c);
        let (path, _) = if k == 0 { (Vec::new(), None) } else { self.up_from(q) };
        self.parent.insert(c, q);
        if k == 0 {
            // 足す seed が無い (輪ができても、 つながる全員が c の木なので 0 のまま)
            return;
        }
        if path.last() != Some(&c) {
            self.shift(q, k, true, touched);
            return;
        }
        // 輪ができた (q → … → c → q): 輪の row の own は道の隣どうしの sub の差、 sub は c の木 (= つながる全員) の seed の数
        let mut below = 0;
        for &y in &path {
            let s = self.sub(y);
            if s > below {
                self.own.insert(y, s - below);
            }
            below = s;
        }
        for &y in &path {
            self.put(y, k, touched);
        }
    }
}

/// 階層の向きごとの状態。
enum Hier {
    Down(Tree),
    Up(Above),
}

impl Hier {
    fn new(up: bool) -> Hier {
        if up { Hier::Up(Above::default()) } else { Hier::Down(Tree::default()) }
    }

    /// 根 c の親を p にする (find の組み立て用)。
    fn link(&mut self, c: u32, p: u32, touched: &mut Vec<u32>) {
        match self {
            Hier::Down(t) => t.set_parent(c, Some(p)),
            Hier::Up(a) => a.link(c, p, touched),
        }
    }

    fn set_seed(&mut self, s: u32, on: bool, touched: &mut Vec<u32>) {
        match self {
            Hier::Down(t) => {
                if on { t.seed.insert(s) } else { t.seed.remove(&s) };
            }
            Hier::Up(a) => a.set_seed(s, on, touched),
        }
    }

    /// find 用の答え (下向きは親をたどる)。
    fn find_answer(&self, x: u32) -> bool {
        match self {
            Hier::Down(t) => t.walk(x),
            Hier::Up(a) => a.sub(x) > u64::from(a.seed.contains(&x)),
        }
    }

    /// 購読で持っている答え。
    fn answer(&self, x: u32) -> bool {
        match self {
            Hier::Down(t) => t.under.contains(&x),
            Hier::Up(a) => a.sub(x) > u64::from(a.seed.contains(&x)),
        }
    }

    /// 親の付け替えと seed の出入りを当てて、 答えが変わりうる row を返す。
    fn apply(&mut self, dp: &enchudb_engine::KeyedDelta, ds: &LiveDelta) -> Vec<u32> {
        use std::collections::BTreeSet;
        let local = |e: EntityId| enchudb_oplog::eid_local(e);
        // 外れた ref (付け替えは added で上書き)
        let mut readded: Vec<EntityId> = dp.added.iter().map(|x| x.0).collect();
        readded.sort_unstable();
        let cut: Vec<u32> = dp.removed.iter().filter(|(e, _)| readded.binary_search(e).is_err()).map(|(e, _)| local(*e)).collect();
        match self {
            Hier::Down(tree) => {
                // 答えを決め直す row: 親が変わった row と、 seed の出入りした row の子
                let mut roots: BTreeSet<u32> = dp.removed.iter().chain(dp.added.iter()).map(|(e, _)| local(*e)).collect();
                for &c in &cut {
                    tree.set_parent(c, None);
                }
                for &(e, p) in &dp.added {
                    tree.set_parent(local(e), Some(p as u32));
                }
                for &e in &ds.removed {
                    tree.seed.remove(&local(e));
                }
                for &e in &ds.added {
                    tree.seed.insert(local(e));
                }
                for &e in ds.removed.iter().chain(ds.added.iter()) {
                    let x = local(e);
                    roots.extend(tree.children.range((x, 0)..=(x, u32::MAX)).map(|k| k.1));
                }
                tree.settle(roots)
            }
            Hier::Up(a) => {
                let mut touched = Vec::new();
                for &c in &cut {
                    a.reparent(c, None, &mut touched);
                }
                for &(e, p) in &dp.added {
                    a.reparent(local(e), Some(p as u32), &mut touched);
                }
                for &e in &ds.removed {
                    a.set_seed(local(e), false, &mut touched);
                }
                for &e in &ds.added {
                    a.set_seed(local(e), true, &mut touched);
                }
                touched
            }
        }
    }
}

/// 配下の購読の状態。
struct UnderState {
    hier: Hier,
    /// この query の条件を満たす row。
    filt: std::collections::BTreeSet<u32>,
    /// 最後に渡した row。
    reported: std::collections::BTreeSet<u32>,
}

/// [`UnderQuery::subscribe`] の戻り値。 階層の配下 (または上) の row の出入りを購読する。
pub struct LiveUnder {
    eng: Arc<Engine>,
    /// (親の ref の鍵付きの購読, seed, 結果を絞る条件)
    live: Option<(enchudb_engine::LiveKeyed, enchudb_engine::LiveQuery, enchudb_engine::LiveQuery)>,
    state: std::sync::Mutex<UnderState>,
}

impl LiveUnder {
    /// 前回 poll からの差分 (昇順)。 同じ row が両方に居たら 「消えて、 別物として入り直した」。
    pub fn poll(&self) -> LiveDelta {
        use std::collections::BTreeSet;
        let Some((parents, seeds, filter)) = &self.live else { return LiveDelta::default() };
        let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let UnderState { hier, filt, reported } = &mut *st;
        let peer = self.eng.peer_id();
        let local = |e: EntityId| enchudb_oplog::eid_local(e);
        let (dp, ds, df) = (parents.poll(&self.eng), seeds.poll(&self.eng), filter.poll(&self.eng));
        // 入り直した row (eid の使い回し) は別物: 答えが同じでも出て入り直す
        let mut reborn: BTreeSet<u32> = dp.reentered.iter().map(|&e| local(e)).collect();
        let rs: BTreeSet<EntityId> = df.removed.iter().copied().collect();
        reborn.extend(df.added.iter().filter(|e| rs.contains(e)).map(|&e| local(e)));
        let mut touched = hier.apply(&dp, &ds);
        for &e in &df.removed {
            filt.remove(&local(e));
            touched.push(local(e));
        }
        for &e in &df.added {
            filt.insert(local(e));
            touched.push(local(e));
        }
        touched.extend(reborn.iter().copied());
        touched.sort_unstable();
        touched.dedup();
        let mut d = LiveDelta::default();
        for x in touched {
            let now = hier.answer(x) && filt.contains(&x);
            let was = reported.contains(&x);
            let e = enchudb_oplog::make_eid(peer, x);
            match (was, now) {
                (false, true) => {
                    reported.insert(x);
                    d.added.push(e);
                }
                (true, false) => {
                    reported.remove(&x);
                    d.removed.push(e);
                }
                (true, true) if reborn.contains(&x) => {
                    d.removed.push(e);
                    d.added.push(e);
                }
                _ => {}
            }
        }
        d
    }
}

// ─────────────────────────── 到達 (辺の table をたどる再帰) ───────────────────────────

/// [`Query::reachable`] の戻り値。 辺の table をたどって届く row を引く / 購読する。
pub struct ReachQuery<'a> {
    rows: Query<'a>,
    edges: Query<'a>,
    src_col: String,
    dst_col: String,
    seeds: Query<'a>,
}

/// 到達の計画。
struct ReachPlan {
    /// 結果を絞る条件
    filter: Vec<enchudb_engine::LivePred>,
    /// seed の条件
    seeds: Vec<enchudb_engine::LivePred>,
    /// 辺の row の条件
    edges: Vec<enchudb_engine::LivePred>,
    /// 辺の (始点, 終点) の ref 列
    src: u16,
    dst: u16,
}

impl<'a> ReachQuery<'a> {
    fn plan(self) -> Result<Option<ReachPlan>, SchemaError> {
        let bad = |m: String| Err(SchemaError::BadValue(m));
        let (rows, edges, seeds) = (self.rows, self.edges, self.seeds);
        for q in [&rows, &edges, &seeds] {
            if q.limit.is_some() || q.order.is_some() {
                return bad("reachable: limit / order_by are not supported".into());
            }
        }
        let t = &rows.table;
        if !Arc::ptr_eq(t, &seeds.table) {
            return bad(format!("reachable: seeds must be a query on {}", t.name));
        }
        let e = &edges.table;
        let col = |name: &str| -> Result<u16, SchemaError> {
            let cd = e.col(name).cloned();
            let points = cd.as_ref().is_some_and(|c| c.ty == ColumnType::Ref)
                && e.relations.iter().any(|r| r.from_col.eq_ignore_ascii_case(name) && r.to_table.eq_ignore_ascii_case(&t.name));
            match cd.filter(|_| points) {
                Some(c) => Ok(c.himo_id),
                None => Err(SchemaError::BadValue(format!("reachable: {name} is not a ref column of {} pointing to {}", e.name, t.name))),
            }
        };
        let (src, dst) = (col(&self.src_col)?, col(&self.dst_col)?);
        let (Some(filter), Some(sd), Some(ed)) = (rows.live_preds()?, seeds.live_preds()?, edges.live_preds()?) else { return Ok(None) };
        Ok(Some(ReachPlan { filter, seeds: sd, edges: ed, src, dst }))
    }

    /// 今届く row (昇順)。
    pub fn find(self) -> Result<Vec<EntityId>, SchemaError> {
        let eng = self.rows.db.arc_engine();
        let io = |e: std::io::Error| SchemaError::BadValue(e.to_string());
        let Some(p) = self.plan()? else { return Ok(Vec::new()) };
        let peer = eng.peer_id();
        let local = |e: EntityId| enchudb_oplog::eid_local(e);
        let mut add = Vec::new();
        for e in eng.find_by(p.edges).map_err(io)? {
            if let (Some(s), Some(d)) = (eng.get_by_id(e, p.src), eng.get_by_id(e, p.dst)) {
                add.push((s as u32, d as u32, local(e)));
            }
        }
        let seeds: Vec<u32> = eng.find_by(p.seeds).map_err(io)?.into_iter().map(local).collect();
        let mut g = Graph::default();
        g.update(&[], &add, &[], &seeds);
        let mut out: Vec<EntityId> =
            eng.find_by(p.filter).map_err(io)?.into_iter().filter(|&e| g.reached(local(e))).map(|e| enchudb_oplog::make_eid(peer, local(e))).collect();
        out.sort_unstable();
        Ok(out)
    }

    /// 今届く row の数。
    pub fn count(self) -> Result<usize, SchemaError> {
        Ok(self.find()?.len())
    }

    /// 届く row の出入りを購読する。 初回 poll は登録時点の全部が `added`。 辺の row の出入り・付け替え、 seed の出入り、
    /// この query の条件の変化で届く。
    pub fn subscribe(self) -> Result<LiveReach, SchemaError> {
        let eng = self.rows.db.arc_engine();
        let io = |e: std::io::Error| SchemaError::BadValue(e.to_string());
        let state = std::sync::Mutex::new(ReachState::default());
        let Some(p) = self.plan()? else { return Ok(LiveReach { eng, live: None, state }) };
        let src = eng.subscribe_keyed(p.edges.clone(), Vec::new(), p.src).map_err(io)?;
        let dst = eng.subscribe_keyed(p.edges, Vec::new(), p.dst).map_err(io)?;
        let seeds = eng.subscribe(p.seeds).map_err(io)?;
        let filter = eng.subscribe(p.filter).map_err(io)?;
        Ok(LiveReach { eng, live: Some(ReachLive { src, dst, seeds, filter }), state })
    }
}

/// 辺 (始点, 終点, 辺の row) と seed、 届く row と、 その row を届かせている辺の始点 (支え)。 row は local eid。
///
/// 支えは seed か届く row で、 支えをたどると必ず seed に着く (支えの森、 輪にならない)。 seed 自身も、 辺をたどって
/// 戻ってくれば届く。 支えを付け替える時は、 新しい支えから支えをたどって、 自分を通らずに seed に着くことを確かめる
/// (たどる長さは支えの森の深さ、 探している row は支えを外してあるので、 自分を通る鎖は seed に着かない)。 辺を足しても、 既に届く row の支えは変えない。
///
/// 1 回の poll で、 辺と seed を消して足し (置くだけ)、 支えの辺が消えた row と外れた seed が支えていた row の支えを
/// 外して探し直す: 元の支え → 入ってくる辺の始点の順に、 seed に着く支えを探す (付け替えた辺の新しい始点も候補、
/// 見つかればその先は見ない)。 見つからない row は外し、 その row が支えていた row も探し直す。 最後に、 外した
/// row のうち届く支えを持つもの・足した辺の先・入った seed の先から幅優先に広げる。
#[derive(Default)]
struct Graph {
    out: std::collections::BTreeSet<(u32, u32, u32)>,
    inn: std::collections::BTreeSet<(u32, u32, u32)>,
    seed: std::collections::BTreeSet<u32>,
    /// 届く row → 支え
    sup: std::collections::BTreeMap<u32, u32>,
    /// (支え, 支えられている row)
    kids: std::collections::BTreeSet<(u32, u32)>,
}

impl Graph {
    fn outs(&self, x: u32) -> impl Iterator<Item = u32> + '_ {
        self.out.range((x, 0, 0)..=(x, u32::MAX, u32::MAX)).map(|t| t.1)
    }

    fn ins(&self, x: u32) -> impl Iterator<Item = u32> + '_ {
        self.inn.range((x, 0, 0)..=(x, u32::MAX, u32::MAX)).map(|t| t.1)
    }

    fn reached(&self, x: u32) -> bool {
        self.sup.contains_key(&x)
    }

    /// w から辺を出して届かせられるか (seed か、 届く row)。
    fn source(&self, w: u32) -> bool {
        self.seed.contains(&w) || self.sup.contains_key(&w)
    }

    /// w が支えになれるか: w から支えをたどって seed に着く。 支えを探している row (支えを外してある) を通る鎖は着かない
    /// ので、 探している row 自身を通る鎖 (輪) も着かない。 鎖は輪にならないので必ず止まる。
    fn chain_ok(&self, mut w: u32) -> bool {
        loop {
            if self.seed.contains(&w) {
                return true;
            }
            match self.sup.get(&w) {
                Some(&p) => w = p,
                None => return false,
            }
        }
    }

    fn set_sup(&mut self, x: u32, w: u32) {
        self.sup.insert(x, w);
        self.kids.insert((w, x));
    }

    /// x の支えを外して `pending` に積む (支えていなければ何もしない)。
    fn unhook(&mut self, x: u32, pending: &mut Vec<(u32, u32)>) {
        if let Some(old) = self.sup.remove(&x) {
            self.kids.remove(&(old, x));
            pending.push((x, old));
        }
    }

    /// 辺を `del` だけ消して `add` だけ足し、 seed を `unseed` だけ外して `seed` だけ入れる。 届くかが変わりうる row を返す。
    fn update(&mut self, del: &[(u32, u32, u32)], add: &[(u32, u32, u32)], unseed: &[u32], seed: &[u32]) -> Vec<u32> {
        // 1. 辺と seed を消して足す (置くだけ)。 支えの辺が消えた row と、 外れた seed が支えていた row を覚える
        let mut roots: Vec<u32> = Vec::new();
        for &(s, d, r) in del {
            if self.out.remove(&(s, d, r)) {
                self.inn.remove(&(d, s, r));
                if self.sup.get(&d) == Some(&s) && self.out.range((s, d, 0)..=(s, d, u32::MAX)).next().is_none() {
                    roots.push(d);
                }
            }
        }
        for &s in unseed {
            if self.seed.remove(&s) {
                roots.extend(self.kids.range((s, 0)..=(s, u32::MAX)).map(|k| k.1));
            }
        }
        for &(s, d, r) in add {
            if self.out.insert((s, d, r)) {
                self.inn.insert((d, s, r));
            }
        }
        let fresh: Vec<u32> = seed.iter().copied().filter(|&s| self.seed.insert(s)).collect();
        // 2. 支えを探し直す (探している間は、 その row を通る鎖は seed に着かない)
        let mut pending: Vec<(u32, u32)> = Vec::new();
        for x in roots {
            self.unhook(x, &mut pending);
        }
        let mut gone: Vec<u32> = Vec::new();
        while let Some((x, old)) = pending.pop() {
            let has_old = self.out.range((old, x, 0)..=(old, x, u32::MAX)).next().is_some();
            let alt = if has_old && self.chain_ok(old) { Some(old) } else { self.ins(x).find(|&w| w != old && self.chain_ok(w)) };
            match alt {
                Some(w) => self.set_sup(x, w),
                None => {
                    gone.push(x);
                    let kids: Vec<u32> = self.kids.range((x, 0)..=(x, u32::MAX)).map(|k| k.1).collect();
                    for y in kids {
                        self.unhook(y, &mut pending);
                    }
                }
            }
        }
        // 3. 広げる: 外した row のうち届く支えを持つもの、 足した辺の先、 入った seed の先から (幅優先)
        let mut queue: std::collections::VecDeque<(u32, u32)> = std::collections::VecDeque::new();
        for &x in &gone {
            if let Some(w) = self.ins(x).find(|&w| self.source(w)) {
                queue.push_back((w, x));
            }
        }
        for &(s, d, _) in add {
            queue.push_back((s, d));
        }
        for &s in &fresh {
            queue.extend(self.outs(s).map(|y| (s, y)));
        }
        let mut touched = gone;
        while let Some((w, x)) = queue.pop_front() {
            if self.reached(x) || !self.source(w) {
                continue;
            }
            self.set_sup(x, w);
            touched.push(x);
            let next: Vec<u32> = self.outs(x).filter(|&y| !self.reached(y)).collect();
            queue.extend(next.into_iter().map(|y| (x, y)));
        }
        touched
    }
}

/// 到達の購読の元。
struct ReachLive {
    /// 辺の row の始点 / 終点の鍵付きの購読
    src: enchudb_engine::LiveKeyed,
    dst: enchudb_engine::LiveKeyed,
    seeds: enchudb_engine::LiveQuery,
    filter: enchudb_engine::LiveQuery,
}

/// 到達の購読の状態。
#[derive(Default)]
struct ReachState {
    graph: Graph,
    /// 辺の row → 始点 / 終点
    src_of: std::collections::BTreeMap<u32, u32>,
    dst_of: std::collections::BTreeMap<u32, u32>,
    /// この query の条件を満たす row。
    filt: std::collections::BTreeSet<u32>,
    /// 最後に渡した row。
    reported: std::collections::BTreeSet<u32>,
}

/// [`ReachQuery::subscribe`] の戻り値。 辺の table をたどって届く row の出入りを購読する。
pub struct LiveReach {
    eng: Arc<Engine>,
    live: Option<ReachLive>,
    state: std::sync::Mutex<ReachState>,
}

impl LiveReach {
    /// 前回 poll からの差分 (昇順)。 同じ row が両方に居たら 「消えて、 別物として入り直した」。
    pub fn poll(&self) -> LiveDelta {
        use std::collections::BTreeSet;
        let Some(lv) = &self.live else { return LiveDelta::default() };
        let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let ReachState { graph, src_of, dst_of, filt, reported } = &mut *st;
        let peer = self.eng.peer_id();
        let local = |e: EntityId| enchudb_oplog::eid_local(e);
        let (dsrc, ddst, ds, df) = (lv.src.poll(&self.eng), lv.dst.poll(&self.eng), lv.seeds.poll(&self.eng), lv.filter.poll(&self.eng));
        // 辺の row の (始点, 終点) の前後
        let rows: BTreeSet<u32> = [&dsrc, &ddst].iter().flat_map(|k| k.removed.iter().chain(k.added.iter()).map(|x| local(x.0))).collect();
        let before: Vec<(u32, Option<u32>, Option<u32>)> = rows.iter().map(|&r| (r, src_of.get(&r).copied(), dst_of.get(&r).copied())).collect();
        for (k, map) in [(&dsrc, &mut *src_of), (&ddst, &mut *dst_of)] {
            for (e, _) in &k.removed {
                map.remove(&local(*e));
            }
            for &(e, v) in &k.added {
                map.insert(local(e), v as u32);
            }
        }
        let (mut del, mut add) = (Vec::new(), Vec::new());
        for (r, s0, d0) in before {
            let (s1, d1) = (src_of.get(&r).copied(), dst_of.get(&r).copied());
            if (s0, d0) == (s1, d1) {
                continue;
            }
            if let (Some(s), Some(d)) = (s0, d0) {
                del.push((s, d, r));
            }
            if let (Some(s), Some(d)) = (s1, d1) {
                add.push((s, d, r));
            }
        }
        let unseed: Vec<u32> = ds.removed.iter().map(|&e| local(e)).collect();
        let seed: Vec<u32> = ds.added.iter().map(|&e| local(e)).collect();
        let mut touched = graph.update(&del, &add, &unseed, &seed);
        // 入り直した row (eid の使い回し) は別物: 答えが同じでも出て入り直す
        let rs: BTreeSet<EntityId> = df.removed.iter().copied().collect();
        let reborn: BTreeSet<u32> = df.added.iter().filter(|e| rs.contains(e)).map(|&e| local(e)).collect();
        for &e in &df.removed {
            filt.remove(&local(e));
            touched.push(local(e));
        }
        for &e in &df.added {
            filt.insert(local(e));
            touched.push(local(e));
        }
        touched.sort_unstable();
        touched.dedup();
        let mut d = LiveDelta::default();
        for x in touched {
            let now = graph.reached(x) && filt.contains(&x);
            let was = reported.contains(&x);
            let e = enchudb_oplog::make_eid(peer, x);
            match (was, now) {
                (false, true) => {
                    reported.insert(x);
                    d.added.push(e);
                }
                (true, false) => {
                    reported.remove(&x);
                    d.removed.push(e);
                }
                (true, true) if reborn.contains(&x) => {
                    d.removed.push(e);
                    d.added.push(e);
                }
                _ => {}
            }
        }
        d
    }
}

impl std::fmt::Debug for LiveReach {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveReach").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for LiveUnder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveUnder").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for LiveMultiJoin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveMultiJoin").field("steps", &self.steps.len()).finish_non_exhaustive()
    }
}

impl std::fmt::Debug for LiveJoin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveJoin").finish_non_exhaustive()
    }
}

// ─────────────────────────── Query ───────────────────────────

/// 条件。 値は engine の u64 (Number は値、 BigInt は符号化した値、 Ref は local eid、 Tag は vocab id)。
enum Predicate {
    /// himo_id == u16::MAX は「未知 col / 型不一致 → 結果 0 件」 sentinel。
    Eq(u16, u64),
    EqText(u16, String),
    Range { himo_name: String, lo: u64, hi: u64 },
    In(u16, Vec<u64>),
    /// ref 列を順にたどった先の table の列への条件 (`where_eq("company.city", ..)`)。
    Via(Vec<u16>, Box<Predicate>),
    /// 枝 (条件の AND) のどれか (`Query::or`)。
    Or(Vec<Vec<Predicate>>),
    /// 列に値がある。
    Present(u16),
    /// 単一列の条件 (Eq / EqText / In / Present) が偽 (値の無い row も真)。
    Not(Box<Predicate>),
    /// 別の table の row がこの row を ref 列 (himo `via`) で指していて、 条件 (engine の条件に写し済み、
    /// None = 常に 0 件) を満たすものが `min` 個以上ある (`where_exists` は 1)。
    Exists { via: u16, th: Th, preds: Option<Vec<enchudb_engine::LivePred>> },
    /// 別の table の row のうち、 列 (himo `theirs`) の値がこの row の列 (himo `mine`) の値と等しく、 条件を
    /// 満たすものが `min` 個以上ある (値で結ぶ準結合)。
    ExistsEq { mine: u16, theirs: u16, th: Th, preds: Option<Vec<enchudb_engine::LivePred>> },
}

/// 存在の条件の閾値 (engine の `CountAtLeast` / `SumAtLeast`)。
#[derive(Clone, Copy, Debug)]
enum Th {
    Count(u64),
    /// 和の列 (himo)、 閾値、 BigInt (値を 2^63 ずらして載せる列) か。
    Sum(u16, i128, bool),
}

/// `where_*` の閾値の指定 (和の列はまだ名前)。
enum ThSpec<'s> {
    Count(u64),
    Sum(&'s str, i128),
}

/// 閾値の指定を解決する。 和の列が `table` の Number / BigInt 列でなければ None。
fn th_of(table: &TableInner, spec: ThSpec) -> Option<Th> {
    match spec {
        ThSpec::Count(n) => Some(Th::Count(n)),
        ThSpec::Sum(col, n) => {
            let cd = table.col(col).filter(|c| matches!(c.ty, ColumnType::Number | ColumnType::BigInt))?;
            Some(Th::Sum(cd.himo_id, n, cd.ty == ColumnType::BigInt))
        }
    }
}

pub struct Query<'a> {
    db: &'a Database,
    table: Arc<TableInner>,
    preds: Vec<Predicate>,
    limit: Option<usize>,
    /// 並びの列と降順か (`order_by` / `order_by_desc`)。
    order: Option<(String, bool)>,
}

impl<'a> Query<'a> {
    fn new(db: &'a Database, table: Arc<TableInner>) -> Self {
        Self { db, table, preds: Vec::new(), limit: None, order: None }
    }

    /// 結果を列 `col` の値の昇順に並べる (同じ値は eid の昇順)。 `col` は ref 列をたどってもよい。
    /// **`col` に値の無い row は結果から外れる** (SQL の NULLS FIRST / LAST ではない)。
    /// `limit` と組むと `subscribe` が先頭 k 件の購読 (live の `ORDER BY .. LIMIT`) になる。
    pub fn order_by(mut self, col: &str) -> Self {
        self.order = Some((col.to_string(), false));
        self
    }

    /// `order_by` の降順。
    pub fn order_by_desc(mut self, col: &str) -> Self {
        self.order = Some((col.to_string(), true));
        self
    }

    /// 並びの列を (ref の道, himo_id) に。
    fn order_col(&self) -> Result<Option<(Vec<u16>, u16, bool)>, SchemaError> {
        let Some((col, desc)) = &self.order else { return Ok(None) };
        let (path, cd) = self
            .resolve_col(col)
            .ok_or_else(|| SchemaError::BadValue(format!("order_by: unknown column {col}")))?;
        if cd.ty == ColumnType::Leaf {
            return Err(SchemaError::BadValue("order_by: cannot order by a Leaf column".into()));
        }
        Ok(Some((path, cd.himo_id, *desc)))
    }

    /// 列名を解決する。 `"a.b.c"` は ref 列 `a`、 `b` を順にたどった先の table の列 `c`
    /// (`ref_to` で宣言した関係)。 返り値は (たどる ref 列の himo_id 列, 対象の列)。
    fn resolve_col(&self, col: &str) -> Option<(Vec<u16>, ColumnInner)> {
        let mut table = self.table.clone();
        let mut path = Vec::new();
        let mut segs = col.split('.').peekable();
        while let Some(seg) = segs.next() {
            let cd = table.col(seg)?.clone();
            if segs.peek().is_none() {
                return Some((path, cd));
            }
            if cd.ty != ColumnType::Ref {
                return None;
            }
            let to = &table.relations.iter().find(|r| r.from_col.eq_ignore_ascii_case(seg))?.to_table;
            let next = self.db.tables.iter().find(|t| t.name.eq_ignore_ascii_case(to))?.clone();
            path.push(cd.himo_id);
            table = next;
        }
        None
    }

    /// ref の道が空でなければ `Via` で包んで積む。
    fn push_at(&mut self, path: Vec<u16>, p: Predicate) {
        if path.is_empty() {
            self.preds.push(p);
        } else {
            self.preds.push(Predicate::Via(path, Box::new(p)));
        }
    }

    /// `col` の値が `val`。 `col` は `"company.city"` のように ref 列をたどってもよい
    /// (`ref_to` で宣言した関係。 ref の先の table の列への条件になる)。
    pub fn where_eq<V: Into<Value>>(mut self, col: &str, val: V) -> Self {
        let v = val.into();
        match self.resolve_col(col) {
            None => self.preds.push(Predicate::Eq(u16::MAX, u64::MAX)), // unknown col → empty
            Some((path, cd)) => {
                let p = match (cd.ty, v) {
                    (ColumnType::Tag, Value::Text(s)) => Some(Predicate::EqText(cd.himo_id, s)),
                    (_, v) => eq_raw(&cd, &v).map(|raw| Predicate::Eq(cd.himo_id, raw)),
                };
                match p {
                    Some(p) => self.push_at(path, p),
                    None => self.preds.push(Predicate::Eq(u16::MAX, u64::MAX)), // 値域外 / 型不一致 → empty
                }
            }
        }
        self
    }

    /// `col` の値が `lo..=hi` (両端を含む、 i64)。 列の値域で切る (Number は 0 以上 `u32::MAX` 未満、 BigInt は
    /// [`BIGINT_MIN`]`..=`[`BIGINT_MAX`])。 空の区間は常に偽の範囲 (購読でも書き間違い扱いにしない)。 未知の列は
    /// `unknown_empty` なら常に 0 件、 でなければ条件を足さない (`where_gt` 系の従来の挙動)。
    fn push_range(&mut self, col: &str, lo: Option<i64>, hi: Option<i64>, unknown_empty: bool) {
        let Some((path, cd)) = self.resolve_col(col) else {
            if unknown_empty {
                self.preds.push(Predicate::Eq(u16::MAX, u64::MAX));
            }
            return;
        };
        let (lo, hi) = match cd.ty {
            ColumnType::BigInt => {
                let (l, h) = (lo.unwrap_or(BIGINT_MIN), hi.unwrap_or(BIGINT_MAX).min(BIGINT_MAX));
                if l > h { (1, 0) } else { (big_raw(l).expect("値域内"), big_raw(h).expect("値域内")) }
            }
            _ => {
                let top = u32::MAX as i64 - 1;
                let (l, h) = (lo.unwrap_or(0).max(0), hi.unwrap_or(top).min(top));
                if l > h { (1, 0) } else { (l as u64, h as u64) }
            }
        };
        self.push_at(path, Predicate::Range { himo_name: cd.himo_name, lo, hi });
    }

    /// `col` の値が `lo` 以上 `hi` 以下。 `lo` / `hi` は整数 (u32 / i32 / i64)。
    pub fn where_range<V: Into<Value>>(mut self, col: &str, lo: V, hi: V) -> Self {
        match (num(lo.into()), num(hi.into())) {
            (Some(lo), Some(hi)) => self.push_range(col, Some(lo), Some(hi), true),
            _ => self.preds.push(Predicate::Eq(u16::MAX, u64::MAX)),
        }
        self
    }

    fn push_cmp(mut self, col: &str, against: Value, f: impl FnOnce(i64) -> (Option<i64>, Option<i64>)) -> Self {
        match num(against) {
            Some(a) => {
                let (lo, hi) = f(a);
                self.push_range(col, lo, hi, false);
            }
            None => self.preds.push(Predicate::Eq(u16::MAX, u64::MAX)),
        }
        self
    }

    /// `col` の値が `against` より大きい。 `against` は整数 (u32 / i32 / i64、 BigInt 列なら i64 の値域)。
    pub fn where_gt<V: Into<Value>>(self, col: &str, against: V) -> Self {
        self.push_cmp(col, against.into(), |a| match a.checked_add(1) {
            Some(lo) => (Some(lo), None),
            None => (Some(1), Some(0)),
        })
    }
    pub fn where_ge<V: Into<Value>>(self, col: &str, against: V) -> Self {
        self.push_cmp(col, against.into(), |a| (Some(a), None))
    }
    pub fn where_lt<V: Into<Value>>(self, col: &str, against: V) -> Self {
        self.push_cmp(col, against.into(), |a| match a.checked_sub(1) {
            Some(hi) => (None, Some(hi)),
            None => (Some(1), Some(0)),
        })
    }
    pub fn where_le<V: Into<Value>>(self, col: &str, against: V) -> Self {
        self.push_cmp(col, against.into(), |a| (None, Some(a)))
    }

    pub fn where_ref(mut self, col: &str, target: EntityId) -> Self {
        if let Some((path, cd)) = self.resolve_col(col) {
            self.push_at(path, Predicate::Eq(cd.himo_id, enchudb_oplog::eid_local(target) as u64));
        }
        self
    }

    /// `col` の値が `values` のどれか (Number / BigInt / Ref の列、 値は u32)。
    pub fn where_in(mut self, col: &str, values: &[u32]) -> Self {
        if let Some((path, cd)) = self.resolve_col(col) {
            let raw = in_raw(&cd, values);
            self.push_at(path, Predicate::In(cd.himo_id, raw));
        }
        self
    }

    /// `col` に値があり、 `val` と違う (SQL の `col <> val` — 値の無い row は入らない、 入れたい時は
    /// `.or(t.where_null(col))`)。 `col` は ref 列をたどってもよい (`"company.city"` = 会社があり、 その
    /// city に値があって `val` でない)。 型の合わない値は 「値があれば真」。
    pub fn where_ne<V: Into<Value>>(mut self, col: &str, val: V) -> Self {
        let v = val.into();
        let Some((path, cd)) = self.resolve_col(col) else {
            self.preds.push(Predicate::Eq(u16::MAX, u64::MAX)); // unknown col → empty
            return self;
        };
        let inner = match (cd.ty, v) {
            (ColumnType::Tag, Value::Text(s)) => Some(Predicate::EqText(cd.himo_id, s)),
            (_, v) => eq_raw(&cd, &v).map(|raw| Predicate::Eq(cd.himo_id, raw)),
        };
        self.push_at(path.clone(), Predicate::Present(cd.himo_id));
        if let Some(p) = inner {
            self.push_at(path, Predicate::Not(Box::new(p)));
        }
        self
    }

    /// `col` に値があり、 `values` のどれでもない (SQL の `col NOT IN (..)`、 値の無い row は入らない)。
    pub fn where_not_in(mut self, col: &str, values: &[u32]) -> Self {
        let Some((path, cd)) = self.resolve_col(col) else {
            self.preds.push(Predicate::Eq(u16::MAX, u64::MAX));
            return self;
        };
        self.push_at(path.clone(), Predicate::Present(cd.himo_id));
        let raw = in_raw(&cd, values);
        self.push_at(path, Predicate::Not(Box::new(Predicate::In(cd.himo_id, raw))));
        self
    }

    /// `col` に値が無い (SQL の `col IS NULL`)。 ref をたどる列 (`"company.city"`) は 「会社はあって、
    /// その city に値が無い」 (会社の無い row は入らない)。 否定だけでは候補を引けないので、 他に条件が
    /// 無ければ table の代表列を持つ全 row から絞る。
    pub fn where_null(mut self, col: &str) -> Self {
        let Some((path, cd)) = self.resolve_col(col) else {
            self.preds.push(Predicate::Eq(u16::MAX, u64::MAX));
            return self;
        };
        if !path.is_empty() {
            // 会社があること (1 段目の ref に値がある) を AND
            self.preds.push(Predicate::Present(path[0]));
        }
        self.push_at(path, Predicate::Not(Box::new(Predicate::Present(cd.himo_id))));
        self
    }

    /// `sub` (別の table への query) の row のうち、 ref 列 `via_col` でこの row を指しているものが 1 つ以上
    /// ある (SQL の `EXISTS (SELECT .. FROM sub WHERE sub.via_col = this.id AND ..)`)。
    ///
    /// ```ignore
    /// // 30 歳より上の社員が居る会社
    /// let q = companies.all().where_exists(users.all().where_gt("age", 30), "company");
    /// // いいねが 1 つも無い投稿
    /// let q = posts.all().where_not_exists(likes.all(), "target");
    /// ```
    ///
    /// `via_col` は `sub` の table の ref 列で、 この table を指すこと (違えば常に 0 件)。 find / count /
    /// subscribe のどれでも使える。 購読では、 指している row の出入り・中身の変化も届く。
    pub fn where_exists(mut self, sub: Query<'a>, via_col: &str) -> Self {
        let p = self.exists_pred(sub, via_col, ThSpec::Count(1));
        match p {
            Some(p) => self.preds.push(p),
            None => self.preds.push(Predicate::Eq(u16::MAX, u64::MAX)),
        }
        self
    }

    /// [`where_exists`](Self::where_exists) の否定: 指している row が 1 つも無い (SQL の `NOT EXISTS`)。
    pub fn where_not_exists(mut self, sub: Query<'a>, via_col: &str) -> Self {
        let p = self.exists_pred(sub, via_col, ThSpec::Count(1));
        match p {
            Some(p) => self.preds.push(Predicate::Not(Box::new(p))),
            None => self.preds.push(Predicate::Eq(u16::MAX, u64::MAX)),
        }
        self
    }

    /// `sub` (別の table への query) の row のうち、 列 `their_col` の値がこの row の列 `my_col` の値と等しい
    /// ものが 1 つ以上ある (値で結ぶ準結合、 SQL の `EXISTS (SELECT .. FROM sub WHERE sub.their_col =
    /// this.my_col AND ..)`)。
    ///
    /// ```ignore
    /// // 営業中の店がある街に住む user
    /// let q = users.all().where_exists_eq("city", shops.where_eq("open", 1i64), "city");
    /// // 会社の所在地に店が 1 軒も無い社員 (自分の列は ref の先でもよい)
    /// let q = users.all().where_not_exists_eq("company.city", shops.all(), "city");
    /// ```
    ///
    /// 2 つの列は同じ型 (Tag / Number / BigInt どうし) であること (違えば常に 0 件)。 ref でつなぐ時は
    /// [`where_exists`](Self::where_exists)。 find / count / subscribe のどれでも使える。 購読では、 この row の
    /// 列の書き換えも、 `sub` の row の出入り・中身の変化も届く (値の row が 0 件 ↔ 1 件以上をまたぐと、 その値を
    /// 持つ row がまとめて出入りする)。
    ///
    /// 1 つの値を持つ row が多い (1 街に 1 万人) なら、 row 単位の購読は値 1 つの出入りで 1 万件の差分になる。
    /// 値 (街) 単位の差分でよければ、 `sub` 側を値ごとに数える購読で足りる (書き込み 1 回 O(1)、 row は引く時に):
    ///
    /// ```ignore
    /// let open = shops.where_eq("open", 1i64).subscribe_counts("city")?;
    /// for (city, n) in open.poll() {
    ///     // n == 0: その街から開いた店が消えた。 前回まで 0 だった街の n >= 1: 開いた店ができた
    ///     let people = users.where_eq("city", city).find()?;   // 住人は引いた時点の中身
    /// }
    /// ```
    pub fn where_exists_eq(mut self, my_col: &str, sub: Query<'a>, their_col: &str) -> Self {
        match self.exists_eq_pred(my_col, sub, their_col, ThSpec::Count(1)) {
            Some((path, p)) => self.push_at(path, p),
            None => self.preds.push(Predicate::Eq(u16::MAX, u64::MAX)),
        }
        self
    }

    /// [`where_exists_eq`](Self::where_exists_eq) の否定: 値の等しい row が 1 つも無い (SQL の `NOT EXISTS`)。
    /// この row の列に値が無ければ真 (SQL と同じ)。
    pub fn where_not_exists_eq(mut self, my_col: &str, sub: Query<'a>, their_col: &str) -> Self {
        match self.exists_eq_pred(my_col, sub, their_col, ThSpec::Count(1)) {
            Some((path, p)) => {
                if !path.is_empty() {
                    // 1 段目の ref に値がある (where_ne と同じ)
                    self.preds.push(Predicate::Present(path[0]));
                }
                self.push_at(path, Predicate::Not(Box::new(p)))
            }
            None => self.preds.push(Predicate::Eq(u16::MAX, u64::MAX)),
        }
        self
    }

    /// この query の row と、 ref 列 `ref_col` で指している `other` (別の table への query) の row の **組** (JOIN)。
    /// 結果は `(この row, 指している row)`、 この row 1 つにつき組は高々 1 つ。
    ///
    /// ```ignore
    /// // 公開済みの投稿と、 その作者 (日本の user) の組
    /// let q = posts.where_eq("published", 1i64).join_ref("author", users.where_eq("country", "JP"));
    /// q.find()?;                     // Vec<(post, user)>
    /// let live = q.subscribe()?;     // 組の出入り (投稿の作者の付け替えも、 作者の条件の変化も届く)
    /// ```
    ///
    /// `ref_col` はこの table の ref 列で、 `other` の table を指すこと (違えば `find` / `subscribe` が `BadValue`)。
    pub fn join_ref(self, ref_col: &str, other: Query<'a>) -> JoinQuery<'a> {
        JoinQuery { left: self, right: other, on: JoinOn::Ref(ref_col.to_string()) }
    }

    /// 階層 (この table が自分を指す ref 列 `ref_col`、 上司 / 親部署 / 親カテゴリ …) で、 `seeds` (同じ table への
    /// query) の row の **配下** (何段下でも、 seed 自身は含まない) の row のうち、 この query の条件を満たすもの
    /// (SQL の再帰 CTE `WITH RECURSIVE`)。
    ///
    /// ```ignore
    /// // Alice の配下全員 (何段下でも)
    /// let q = employees.all().under("manager", employees.where_eq("name", "Alice"));
    /// q.find()?;
    /// let live = q.subscribe()?;   // 付け替え (異動・部署の移動) で配下が丸ごと出入りする
    /// ```
    ///
    /// - たどる階層は table の全 row (この query の条件は結果を絞るだけ)。 ref が輪になっていても、 seed に着かなければ配下でない
    /// - `ref_col` はこの table を指す ref 列、 `seeds` は同じ table への query (違えば `find` / `subscribe` が `BadValue`)
    pub fn under(self, ref_col: &str, seeds: Query<'a>) -> UnderQuery<'a> {
        UnderQuery { rows: self, seeds, ref_col: ref_col.to_string(), up: false }
    }

    /// [`Query::under`] の上向き: `seeds` の row の **上** (上司の上司 … 何段上でも、 seed 自身は含まない) の row のうち、
    /// この query の条件を満たすもの (SQL の再帰 CTE で祖先をたどる形)。
    ///
    /// ```ignore
    /// // Alice の上司全員 (何段上でも)
    /// employees.all().above("manager", employees.where_eq("name", "Alice")).subscribe()?;
    /// ```
    ///
    /// - ref が輪になっている時、 輪の row は 「輪とそこにぶら下がる row の seed (自分を除く)」 の上。 輪は 1 周で止まる
    /// - 付け替え 1 回・seed の出入り 1 回のコストは階層の深さに比例 (配下の数によらない)
    pub fn above(self, ref_col: &str, seeds: Query<'a>) -> UnderQuery<'a> {
        UnderQuery { rows: self, seeds, ref_col: ref_col.to_string(), up: true }
    }

    /// 辺の table (`edges`、 始点 `src_col` と終点 `dst_col` がどちらもこの table を指す ref 列) を何本たどっても `seeds` の row から
    /// 届く row のうち、 この query の条件を満たすもの (SQL の再帰 CTE でグラフをたどる形、 親が複数あってよい)。
    ///
    /// ```ignore
    /// // Alice から follow を何段たどっても届く人
    /// let q = users.all().reachable(follows.all(), "from", "to", users.where_eq("name", "Alice"));
    /// let live = q.subscribe()?;   // follow の増減・付け替えで届く人が出入りする
    /// ```
    ///
    /// - 辺は 1 本以上たどる (seed 自身は、 辺をたどって戻ってこなければ入らない)。 輪があってよい
    /// - 辺の query の条件 (`follows.where_eq("kind", "friend")` など) を満たす辺だけをたどる。 たどる途中の row は
    ///   この query の条件を問わない (条件は結果を絞るだけ)
    /// - 辺が消えた時は支えを失った row の段だけを決め直す。 橋になっていた辺を消すと、 その先の全部を決め直す
    pub fn reachable(self, edges: Query<'a>, src_col: &str, dst_col: &str, seeds: Query<'a>) -> ReachQuery<'a> {
        ReachQuery { rows: self, edges, src_col: src_col.to_string(), dst_col: dst_col.to_string(), seeds }
    }

    /// この query の row と `other` (別の table への query) の row のうち、 この row の列 `my_col` の値と `other` の
    /// 列 `their_col` の値が等しいものの **組** (値で結ぶ JOIN、 SQL の `JOIN .. ON a.my_col = b.their_col`)。
    ///
    /// ```ignore
    /// // 住人と、 住む街の開いた店の組
    /// let q = users.all().join_eq("city", shops.where_eq("open", 1i64), "city");
    /// ```
    ///
    /// `my_col` は ref の先でもよい (`"company.city"`)。 2 つの列は同じ型 (Tag / Number / BigInt、 違えば `BadValue`)。
    /// 1 つの値に両側が大勢いると組は掛け算で増える (街に住人 1 万 × 店 10 = 組 10 万、 店 1 軒の出入りで組 1 万)。
    pub fn join_eq(self, my_col: &str, other: Query<'a>, their_col: &str) -> JoinQuery<'a> {
        JoinQuery { left: self, right: other, on: JoinOn::Eq(my_col.to_string(), their_col.to_string()) }
    }

    /// この query の row と `other` (別の table への query) の row のうち、 この row の列 `my_col` の値が `other` の
    /// 列 `lo_col` と `hi_col` の値の間 (両端を含む) にあるものの **組** (範囲で結ぶ JOIN、 SQL の
    /// `JOIN .. ON a.my_col BETWEEN b.lo_col AND b.hi_col`)。
    ///
    /// ```ignore
    /// // イベントと、 その時刻を含むセッションの組
    /// let q = events.all().join_range("at", sessions.all(), "start", "end");
    /// q.find()?;                 // Vec<(イベント, セッション)>
    /// let live = q.subscribe()?; // イベントの時刻・セッションの始点 / 終点の書き換えで組が出入りする
    /// ```
    ///
    /// - 3 つの列は全部 Number か全部 BigInt (違えば `BadValue`)。 `my_col` は ref の先でもよい (`"company.founded"`)
    /// - 始点 > 終点の row・値の無い row は組にならない
    /// - 組の購読は左の値と右の区間を持つ (メモリは両側の結果に比例)。 区間の書き換え 1 回で、 旧区間と新区間に居る
    ///   左の row の数だけ組が動く (両方に居る row の組は居続ける)
    /// - `subscribe_counts` / 3 つ以上の table の組 (`then_*`) はまだ (`BadValue`)
    pub fn join_range(self, my_col: &str, other: Query<'a>, lo_col: &str, hi_col: &str) -> JoinQuery<'a> {
        JoinQuery { left: self, right: other, on: JoinOn::Range(my_col.to_string(), lo_col.to_string(), hi_col.to_string()) }
    }

    /// この query の row を列 `part_col` の値で group に分け、 group の中で列 `order_col` の順に並べた時の、 各 row と
    /// **1 つ前の row** の組 (window 関数 `LAG(..) OVER (PARTITION BY part_col ORDER BY order_col)`)。 同じ値は eid の昇順。
    ///
    /// ```ignore
    /// // user ごとに、 各イベントと 1 つ前のイベント (間隔・変化を見る)
    /// let live = events.all().lag("user", "at").subscribe()?;   // PairDelta: (イベント, 1 つ前のイベント)
    /// ```
    ///
    /// - group の先頭の row は組にならない。 1 つ後 (`LEAD`) は組を裏返す
    /// - `part_col` は Tag / Number / BigInt / Ref (ref の先でもよい)、 `order_col` は Number / BigInt (ref の先でもよい)。
    ///   値の無い row は並ばない
    /// - 書き込み 1 回で動く組は高々 3 つずつ (抜けた所の前後がつながり、 入った所の前後が切れる)
    pub fn lag(self, part_col: &str, order_col: &str) -> LagQuery<'a> {
        LagQuery { rows: self, part: Some(part_col.to_string()), order: order_col.to_string() }
    }

    /// [`Query::lag`] の group なし版 (結果全体を `order_col` の順に並べる)。
    pub fn lag_all(self, order_col: &str) -> LagQuery<'a> {
        LagQuery { rows: self, part: None, order: order_col.to_string() }
    }

    /// `sub` (別の table への query) の row のうち、 ref 列 `via_col` でこの row を指しているものが **`n` 個以上**
    /// ある (SQL の `HAVING COUNT(*) >= n`、 [`where_exists`](Self::where_exists) の件数版)。
    ///
    /// ```ignore
    /// // 30 歳より上の社員が 50 人以上いる会社
    /// let q = companies.all().where_count_ge(users.all().where_gt("age", 30i64), "company", 50);
    /// ```
    ///
    /// find / count / subscribe のどれでも使える。 購読では、 数えている row の出入り・中身の変化で件数が
    /// `n` をまたいだ row が出入りする (書き込み 1 回あたり、 またいだ group の数に比例)。 `n == 0` は常に真
    /// (条件を足さない)。 `via_col` の条件は [`where_exists`](Self::where_exists) と同じ。
    pub fn where_count_ge(mut self, sub: Query<'a>, via_col: &str, n: u64) -> Self {
        if n == 0 {
            return self;
        }
        match self.exists_pred(sub, via_col, ThSpec::Count(n)) {
            Some(p) => self.preds.push(p),
            None => self.preds.push(Predicate::Eq(u16::MAX, u64::MAX)),
        }
        self
    }

    /// [`where_count_ge`](Self::where_count_ge) の否定: 指している row が `n` 個未満 (0 個も含む、 SQL の
    /// `HAVING COUNT(*) < n` に 0 件の row を足したもの)。 `n == 0` は常に偽。
    pub fn where_count_lt(mut self, sub: Query<'a>, via_col: &str, n: u64) -> Self {
        match self.exists_pred(sub, via_col, ThSpec::Count(n)).filter(|_| n > 0) {
            Some(p) => self.preds.push(Predicate::Not(Box::new(p))),
            None => self.preds.push(Predicate::Eq(u16::MAX, u64::MAX)),
        }
        self
    }

    /// `sub` の row のうち、 列 `their_col` の値がこの row の列 `my_col` の値と等しいものが **`n` 個以上** ある
    /// ([`where_exists_eq`](Self::where_exists_eq) の件数版)。
    ///
    /// ```ignore
    /// // 開いた店が 3 軒以上ある街に住む user
    /// let q = users.all().where_value_count_ge("city", shops.where_eq("open", 1i64), "city", 3);
    /// ```
    ///
    /// 列の条件は [`where_exists_eq`](Self::where_exists_eq) と同じ。 `n == 0` は常に真 (条件を足さない)。
    pub fn where_value_count_ge(mut self, my_col: &str, sub: Query<'a>, their_col: &str, n: u64) -> Self {
        if n == 0 {
            return self;
        }
        match self.exists_eq_pred(my_col, sub, their_col, ThSpec::Count(n)) {
            Some((path, p)) => self.push_at(path, p),
            None => self.preds.push(Predicate::Eq(u16::MAX, u64::MAX)),
        }
        self
    }

    /// [`where_value_count_ge`](Self::where_value_count_ge) の否定: 値の等しい row が `n` 個未満 (0 個も含む)。
    /// この row の列に値が無ければ真 ([`where_not_exists_eq`](Self::where_not_exists_eq) と同じ)。 `n == 0` は常に偽。
    pub fn where_value_count_lt(mut self, my_col: &str, sub: Query<'a>, their_col: &str, n: u64) -> Self {
        match self.exists_eq_pred(my_col, sub, their_col, ThSpec::Count(n)).filter(|_| n > 0) {
            Some((path, p)) => {
                if !path.is_empty() {
                    self.preds.push(Predicate::Present(path[0]));
                }
                self.push_at(path, Predicate::Not(Box::new(p)))
            }
            None => self.preds.push(Predicate::Eq(u16::MAX, u64::MAX)),
        }
        self
    }

    /// `sub` (別の table への query) の row のうち、 ref 列 `via_col` でこの row を指しているものが 1 つ以上あり、
    /// その列 `sum_col` の値の和が **`n` 以上** (SQL の `HAVING SUM(sum_col) >= n`)。
    ///
    /// ```ignore
    /// // 注文の合計金額が 100 万以上の顧客
    /// let q = customers.all().where_sum_ge(orders.where_eq("status", "paid"), "customer", "amount", 1_000_000);
    /// ```
    ///
    /// - `sum_col` は `sub` の table の Number / BigInt 列 (違えば常に 0 件)。 値の無い row は 0 として足す
    /// - 指している row が 1 つも無い row は入らない (SQL と同じく、 行の無い group の和は NULL)
    /// - find / count / subscribe のどれでも。 購読では、 数えている row の出入り・和の列の書き換えで和が `n` を
    ///   またいだ row が出入りする
    pub fn where_sum_ge(mut self, sub: Query<'a>, via_col: &str, sum_col: &str, n: i64) -> Self {
        match self.exists_pred(sub, via_col, ThSpec::Sum(sum_col, n as i128)) {
            Some(p) => self.preds.push(p),
            None => self.preds.push(Predicate::Eq(u16::MAX, u64::MAX)),
        }
        self
    }

    /// [`where_sum_ge`](Self::where_sum_ge) の否定: 指している row が無いか、 和が `n` 未満。
    pub fn where_sum_lt(mut self, sub: Query<'a>, via_col: &str, sum_col: &str, n: i64) -> Self {
        match self.exists_pred(sub, via_col, ThSpec::Sum(sum_col, n as i128)) {
            Some(p) => self.preds.push(Predicate::Not(Box::new(p))),
            // 列が不正は常に 0 件 (他の where_* と同じ)
            None => self.preds.push(Predicate::Eq(u16::MAX, u64::MAX)),
        }
        self
    }

    /// `sub` の row のうち、 列 `their_col` の値がこの row の列 `my_col` の値と等しいものが 1 つ以上あり、 その列
    /// `sum_col` の和が **`n` 以上** ([`where_value_count_ge`](Self::where_value_count_ge) の和の版)。
    pub fn where_value_sum_ge(mut self, my_col: &str, sub: Query<'a>, their_col: &str, sum_col: &str, n: i64) -> Self {
        match self.exists_eq_pred(my_col, sub, their_col, ThSpec::Sum(sum_col, n as i128)) {
            Some((path, p)) => self.push_at(path, p),
            None => self.preds.push(Predicate::Eq(u16::MAX, u64::MAX)),
        }
        self
    }

    /// [`where_value_sum_ge`](Self::where_value_sum_ge) の否定: 値の等しい row が無いか、 和が `n` 未満。 この row の
    /// 列に値が無ければ真。
    pub fn where_value_sum_lt(mut self, my_col: &str, sub: Query<'a>, their_col: &str, sum_col: &str, n: i64) -> Self {
        match self.exists_eq_pred(my_col, sub, their_col, ThSpec::Sum(sum_col, n as i128)) {
            Some((path, p)) => {
                if !path.is_empty() {
                    self.preds.push(Predicate::Present(path[0]));
                }
                self.push_at(path, Predicate::Not(Box::new(p)))
            }
            None => self.preds.push(Predicate::Eq(u16::MAX, u64::MAX)),
        }
        self
    }

    /// `where_exists_eq` の条件と、 自分の列までの ref の道。 列が無い / 型が違う / Leaf / Ref なら None。
    fn exists_eq_pred(&self, my_col: &str, sub: Query<'a>, their_col: &str, th: ThSpec) -> Option<(Vec<u16>, Predicate)> {
        let (path, mine) = self.resolve_col(my_col)?;
        let (theirs_ty, theirs) = sub.table.col(their_col).map(|c| (c.ty, c.himo_id))?;
        if mine.ty != theirs_ty || matches!(mine.ty, ColumnType::Leaf | ColumnType::Ref) {
            return None;
        }
        let th = th_of(&sub.table, th)?;
        let preds = sub.live_preds().ok()?;
        Some((path, Predicate::ExistsEq { mine: mine.himo_id, theirs, th, preds }))
    }

    /// `where_exists` の条件。 `via_col` がこの table を指す ref 列でなければ None。
    fn exists_pred(&self, sub: Query<'a>, via_col: &str, th: ThSpec) -> Option<Predicate> {
        let cd = sub.table.col(via_col)?;
        let points_here = cd.ty == ColumnType::Ref
            && sub.table.relations.iter().any(|r| {
                r.from_col.eq_ignore_ascii_case(via_col) && r.to_table.eq_ignore_ascii_case(&self.table.name)
            });
        if !points_here {
            return None;
        }
        let via = cd.himo_id;
        let th = th_of(&sub.table, th)?;
        let preds = sub.live_preds().ok()?;
        Some(Predicate::Exists { via, th, preds })
    }

    /// `col` に値がある (SQL の `col IS NOT NULL`)。
    pub fn where_not_null(mut self, col: &str) -> Self {
        match self.resolve_col(col) {
            Some((path, cd)) => self.push_at(path, Predicate::Present(cd.himo_id)),
            None => self.preds.push(Predicate::Eq(u16::MAX, u64::MAX)),
        }
        self
    }

    /// この条件 **または** `other` の条件 (同じ table への query)。 `a.or(b).where_eq(..)` は
    /// `(a OR b) AND ..`。 find / count / subscribe のどれでも使える。 `limit` は `self` のものを使う。
    ///
    /// ```ignore
    /// let q = users.where_eq("city", "Tokyo").or(users.where_gt("age", 60));
    /// let live = q.subscribe()?;   // 東京在住 または 61 歳以上
    /// ```
    ///
    /// 別の table の query を渡すと `find` / `subscribe` が `BadValue`。
    pub fn or(mut self, other: Query<'a>) -> Self {
        if !Arc::ptr_eq(&self.table, &other.table) {
            self.preds = vec![Predicate::Or(Vec::new())];
            return self;
        }
        let mine = std::mem::take(&mut self.preds);
        self.preds.push(Predicate::Or(vec![mine, other.preds]));
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    pub fn count(self) -> Result<usize, SchemaError> {
        Ok(self.find()?.len())
    }

    pub fn find_one(mut self) -> Result<Option<EntityId>, SchemaError> {
        self.limit = Some(1);
        Ok(self.find()?.into_iter().next())
    }

    pub fn find(mut self) -> Result<Vec<EntityId>, SchemaError> {
        let Some((path, himo, desc)) = self.order_col()? else { return self.find_set() };
        // 並べてから切る: 集合は limit 無しで取り、 並びの値 (ref の道をたどった先) で並べる
        let limit = self.limit.take();
        self.order = None;
        let eng = self.db.engine();
        // 並びは engine の値 (BigInt の符号化は大小の順を保つ)
        let mut keyed: Vec<((u64, u32), EntityId)> = self
            .find_set()?
            .into_iter()
            .filter_map(|e| {
                let mut cur = e;
                for &h in &path {
                    cur = eng.get_by_id(cur, h)? as EntityId;
                }
                let v = eng.get_by_id(cur, himo)?;
                let v = if desc { u64::MAX - v } else { v };
                Some(((v, enchudb_oplog::eid_local(e)), e))
            })
            .collect();
        keyed.sort_unstable_by_key(|x| x.0);
        let mut out: Vec<EntityId> = keyed.into_iter().map(|x| x.1).collect();
        if let Some(n) = limit {
            out.truncate(n);
        }
        Ok(out)
    }

    fn find_set(self) -> Result<Vec<EntityId>, SchemaError> {
        let eng = self.db.engine();
        eng.rebuild();

        // ref をたどる条件 (`"company.city"`) を含むなら engine の live 条件評価に任せる
        // (候補を索引で引いて ref の逆引きで遡り、 全条件で評価)
        if self.preds.iter().any(|p| {
            matches!(p, Predicate::Via(..) | Predicate::Or(_) | Predicate::Not(_) | Predicate::Present(_) | Predicate::Exists { .. } | Predicate::ExistsEq { .. })
        }) {
            let limit = self.limit;
            let Some(preds) = self.live_preds()? else { return Ok(Vec::new()) };
            let mut out = eng.find_by(preds).map_err(|e| SchemaError::Io(e.to_string()))?;
            if let Some(n) = limit {
                out.truncate(n);
            }
            return Ok(out);
        }

        // 1. Eq / EqText / In を engine 側 query に折り込む。 Range は post-filter。
        // column 名は `{table}.{col}` で prefix されてて他テーブルと共有しない設計
        // (case 1: column 名空間分離)。 marker cond は不要。
        let mut eq_conds: Vec<(u16, u64)> = Vec::with_capacity(self.preds.len());

        let mut in_pred: Option<(u16, Vec<u64>)> = None;
        let mut range_preds: Vec<(String, u64, u64)> = Vec::new();
        let mut empty = false;

        for p in self.preds {
            match p {
                Predicate::Eq(h, _) if h == u16::MAX => { empty = true; }
                Predicate::Eq(h, v) => eq_conds.push((h, v)),
                Predicate::EqText(h, s) => match eng.vocab_id(&s) {
                    Some(vid) => eq_conds.push((h, vid as u64)),
                    None => empty = true,
                },
                Predicate::In(h, vs) => {
                    if in_pred.is_some() {
                        return Err(SchemaError::BadValue("multiple where_in not supported yet".into()));
                    }
                    in_pred = Some((h, vs));
                }
                Predicate::Range { himo_name, lo, hi } => range_preds.push((himo_name, lo, hi)),
                Predicate::Via(..) | Predicate::Or(_) | Predicate::Not(_) | Predicate::Present(_) | Predicate::Exists { .. } | Predicate::ExistsEq { .. } => {
                    unreachable!("Via / Or / Not / Present / Exists は find の先頭で find_by に回している")
                }
            }
        }
        if empty { return Ok(Vec::new()); }

        // 2. base candidates。
        //    - eq_conds あり → engine query が primary index
        //    - eq_conds 無し + IN あり → IN 集合自体を候補の seed にする (issue12:
        //      旧実装は IN を post-filter 専用にしていて、 単独 where_in が
        //      query_by_id(&[]) = 空集合の retain となり常に 0 件だった)
        //    - どちらも無し = `.all()` 系 → table 所属を表す代表 column (PK or
        //      first col) で「値が tie された全 entity」 を取る。
        //      case 1 設計では「table の row」 = 「table の column を 1 つ以上 tie してる entity」、
        //      厳密には全 column union だが、 代表 column slice で実用上十分。
        let mut candidates = if !eq_conds.is_empty() {
            let mut c = eng.query_by_id64(&eq_conds);
            // 3. IN は候補集合への set-membership filter として intersect
            //    (pull_in_by_id64 の結果は sort + dedup 済み)
            if let Some((h, vs)) = in_pred {
                let in_sorted = eng.pull_in_by_id64(h, &vs);
                c.retain(|e| in_sorted.binary_search(e).is_ok());
            }
            c
        } else if let Some((h, vs)) = in_pred {
            eng.pull_in_by_id64(h, &vs)
        } else {
            let representative_hid = self.table.pk
                .or_else(|| if self.table.cols.is_empty() { None } else { Some(0) })
                .map(|i| self.table.cols[i].himo_id);
            match representative_hid {
                Some(hid) => eng.entities_with_himo(hid),
                None => Vec::new(),
            }
        };

        // 4. post-filter range predicates (Column 直読み、 engine の値で比べる)
        if !range_preds.is_empty() {
            candidates.retain(|&eid| {
                range_preds.iter().all(|(h, lo, hi)| eng.get(eid, h).is_some_and(|v| *lo <= v && v <= *hi))
            });
        }

        if let Some(n) = self.limit { candidates.truncate(n); }
        Ok(candidates)
    }

    /// この条件の **結果集合を購読する** (live query)。 以降の書き込みで結果が変わった分
    /// だけを [`LiveQuery::poll`] が返す。 `find()` を毎回呼び直す代わりに使う。
    ///
    /// ```ignore
    /// let tokyo = users.where_eq("city", "Tokyo").subscribe()?;
    /// let first = tokyo.poll();   // 登録時点の全件が added
    /// users.insert().set("id", 9i64).set("city", "Tokyo").commit()?;
    /// let d = tokyo.poll();       // d.added == [新しい row]
    /// tokyo.count();              // 今の件数 (find().len() と同じ)
    /// ```
    ///
    /// - 結果集合は空から始まり、 `poll()` の差分 (removed → added の順) を積めば常に
    ///   その時点の `find()` と同じ集合になる。 初回 `poll()` は登録時点の全件を返す
    /// - local の書き込みも sync で届いた他 peer の書き込みも同じように届く
    /// - 追うのは **row の出入り**だけ。 結果に入ったままの row の中身の変化 (条件に無い
    ///   列の更新など) は届かないので、 中身は poll 後に読む
    /// - 返り値を drop すると購読解除。 `Database` を借用しないので struct に持てる
    /// - 条件は `find()` と同じ (`where_eq` / `where_ref` / `where_in` / `where_range` /
    ///   `where_gt` 系、 条件なし = table の全 row)。 まだ誰も書いていない文字列への
    ///   `where_eq` も、 後から書かれた時点で一致する
    ///
    /// `order_by` + `limit` は先頭 k 件の購読 (差分は先頭 k 件への出入り、 並びは `ranked`)。
    /// `where_in` / 枝が同じ形の `or` も使える (形の違う枝の `or` は `BadValue`)。
    ///
    /// `order_by` 無しの `limit`、 または未知の列 / 型の合わない値の `where_eq` は `BadValue`
    /// (`find()` なら常に 0 件になる条件 — 購読では書き間違いとして返す)。
    pub fn subscribe(self) -> Result<LiveQuery, SchemaError> {
        let order = self.order_col()?;
        let limit = self.limit;
        if limit.is_some() && order.is_none() {
            return Err(SchemaError::BadValue("subscribe: limit needs order_by".into()));
        }
        let eng = self.db.arc_engine();
        let preds = self.live_preds()?.ok_or_else(|| {
            SchemaError::BadValue(
                "subscribe: where_eq on an unknown column or with a mismatched value type".into(),
            )
        })?;
        let inner = match (order, limit) {
            (Some((path, himo, desc)), Some(k)) => {
                eng.subscribe_top(preds, path, himo, desc, k).map_err(|e| SchemaError::BadValue(e.to_string()))?
            }
            _ => eng.subscribe(preds).map_err(|e| SchemaError::Io(e.to_string()))?,
        };
        Ok(LiveQuery { inner, eng })
    }

    /// ref をたどる条件 (`where_eq("company.city", "Tokyo")`) を **ref の先の row (group) 単位**
    /// で購読する。 差分は 「条件を満たすようになった / 外れた会社」、 社員は
    /// [`GroupedLiveQuery::members`] で会社から逆引きする。 会社の所在地を 1 個書き換えた時の
    /// 差分は会社 1 件で、 配下の社員が何人でも O(1)。
    ///
    /// - dotted な条件は全部同じ ref 列から始まること (`company.city` と `company.region.name` は可、
    ///   `company.city` と `dept.name` は不可)。 dotted な条件が 1 本以上要る
    /// - dotted でない条件 (`where_gt("age", 30)` など) は `members` / `count` で絞る
    /// - 社員の異動や社員側の条件の変化は差分に出ない — `members` / `count` を引いた時点の中身が返る
    pub fn subscribe_grouped(self) -> Result<GroupedLiveQuery, SchemaError> {
        if self.limit.is_some() {
            return Err(SchemaError::BadValue("subscribe: limit is not supported".into()));
        }
        let eng = self.db.arc_engine();
        let preds = self.live_preds()?.ok_or_else(|| {
            SchemaError::BadValue(
                "subscribe: where_eq on an unknown column or with a mismatched value type".into(),
            )
        })?;
        let inner = eng.subscribe_grouped(preds).map_err(|e| SchemaError::BadValue(e.to_string()))?;
        Ok(GroupedLiveQuery { inner, eng })
    }

    /// この条件の結果を **列 `col` の値ごとに数えた件数** を購読する (live の `GROUP BY col` +
    /// `COUNT(*)`)。 `col` は `"company.city"` のように ref 列をたどってもよい。
    ///
    /// ```ignore
    /// let by_city = users.where_eq("status", "active").subscribe_counts("company.city")?;
    /// for (city, n) in by_city.poll() { /* 件数が変わった city と今の件数 (0 = 居なくなった) */ }
    /// by_city.get(&Value::Text("Tokyo".into()));
    /// ```
    ///
    /// - `col` に値の無い row は数えない (NULL の group は無い)
    /// - `col` が Leaf 列 (row ごとに固有の文字列)、 `limit` 付き、 形の違う枝の `or` は `BadValue`
    ///   (`where_in` や同じ形の `or` (`city = A OR city = B`) は可)
    pub fn subscribe_counts(self, col: &str) -> Result<LiveCounts, SchemaError> {
        self.subscribe_agg(col, None)
    }

    /// この条件の結果を列 `col` の値で group 分けし、 **件数が `n` 以上の group** を購読する (live の
    /// `GROUP BY col HAVING COUNT(*) >= n`)。 差分は閾値をまたいだ group の値。
    ///
    /// ```ignore
    /// // 開いた店が 3 軒以上ある街
    /// let busy = shops.where_eq("open", 1i64).subscribe_having("city", 3)?;
    /// for city in busy.poll().added { /* 3 軒以上になった街 */ }
    /// ```
    ///
    /// - group の row を引きたい時は [`where_count_ge`](Self::where_count_ge) (ref で指されている row) /
    ///   [`where_value_count_ge`](Self::where_value_count_ge) (値の等しい row) で条件にする
    /// - `col` の条件は [`subscribe_counts`](Self::subscribe_counts) と同じ。 `n == 0` は `BadValue`
    pub fn subscribe_having(self, col: &str, n: u64) -> Result<LiveHaving, SchemaError> {
        if n == 0 {
            return Err(SchemaError::BadValue("subscribe_having: n must be at least 1".into()));
        }
        let counts = self.subscribe_agg(col, None)?;
        Ok(LiveHaving { counts, th: HavingTh::Count(n), have: Default::default() })
    }

    /// この条件の結果を列 `col` の値で group 分けし、 **列 `sum_col` の和が `n` 以上の group** を購読する (live の
    /// `GROUP BY col HAVING SUM(sum_col) >= n`、 行の無い group は入らない)。 差分は閾値をまたいだ group の値。
    /// `sum_col` の条件は [`subscribe_sums`](Self::subscribe_sums) と同じ。
    ///
    /// ```ignore
    /// // 支払い済みの注文の合計が 100 万以上の地域
    /// let big = orders.where_eq("status", "paid").subscribe_having_sum("region", "amount", 1_000_000)?;
    /// ```
    pub fn subscribe_having_sum(self, col: &str, sum_col: &str, n: i64) -> Result<LiveHaving, SchemaError> {
        let counts = self.subscribe_sums(col, sum_col)?;
        Ok(LiveHaving { counts, th: HavingTh::Sum(n as i128), have: Default::default() })
    }

    /// [`subscribe_counts`](Self::subscribe_counts) に加えて、 group ごとに列 `sum_col` の値の和も持つ
    /// (live の `GROUP BY col` + `COUNT(*)` + `SUM(sum_col)`)。 平均は合計 / 件数。
    ///
    /// ```ignore
    /// let pay = users.where_eq("status", "active").subscribe_sums("company.city", "salary")?;
    /// for (city, n, total) in pay.poll_sums() { /* 件数か合計が変わった city */ }
    /// ```
    ///
    /// - `sum_col` はこの table の Number 列 (ref をたどる列は不可)。 値の無い row は件数に入り、
    ///   合計には 0 として足す (SQL の `SUM` と同じく NULL を無視)
    /// - 合計の列の書き換えも届く (row の出入りが無くても)
    pub fn subscribe_sums(self, col: &str, sum_col: &str) -> Result<LiveCounts, SchemaError> {
        let cd = self
            .table
            .col(sum_col)
            .filter(|c| matches!(c.ty, ColumnType::Number | ColumnType::BigInt))
            .ok_or_else(|| SchemaError::BadValue(format!("subscribe_sums: {sum_col} is not a Number / BigInt column of this table")))?;
        let big = cd.ty == ColumnType::BigInt;
        let h = cd.himo_id;
        self.subscribe_agg(col, Some((h, big)))
    }

    fn subscribe_agg(self, col: &str, sum: Option<(u16, bool)>) -> Result<LiveCounts, SchemaError> {
        if self.limit.is_some() {
            return Err(SchemaError::BadValue("subscribe_counts: limit is not supported".into()));
        }
        let (path, cd) = self
            .resolve_col(col)
            .ok_or_else(|| SchemaError::BadValue(format!("subscribe_counts: unknown column {col}")))?;
        if cd.ty == ColumnType::Leaf {
            return Err(SchemaError::BadValue("subscribe_counts: cannot group by a Leaf column".into()));
        }
        let eng = self.db.arc_engine();
        let preds = self.live_preds()?.ok_or_else(|| {
            SchemaError::BadValue(
                "subscribe: where_eq on an unknown column or with a mismatched value type".into(),
            )
        })?;
        let inner = match sum {
            Some((h, _)) => eng.subscribe_sums(preds, path, cd.himo_id, h),
            None => eng.subscribe_counts(preds, path, cd.himo_id),
        }
        .map_err(|e| SchemaError::BadValue(e.to_string()))?;
        Ok(LiveCounts { inner, eng, ty: cd.ty, sum_big: sum.is_some_and(|s| s.1), last: Default::default() })
    }

    /// ablation / 計測用: engine の `subscribe_expand_always` で購読する (結果は `subscribe`
    /// と同じ、 poll のコストだけが変わる)。
    #[doc(hidden)]
    pub fn subscribe_expand_always(self) -> Result<LiveQuery, SchemaError> {
        if self.limit.is_some() {
            return Err(SchemaError::BadValue("subscribe: limit is not supported".into()));
        }
        let eng = self.db.arc_engine();
        let preds = self.live_preds()?.ok_or_else(|| {
            SchemaError::BadValue(
                "subscribe: where_eq on an unknown column or with a mismatched value type".into(),
            )
        })?;
        let inner = eng.subscribe_expand_always(preds).map_err(|e| SchemaError::Io(e.to_string()))?;
        Ok(LiveQuery { inner, eng })
    }

    /// 条件を engine の `LivePred` に写す。 `None` = 常に 0 件になる条件 (未知の列 / 型不一致の
    /// `where_eq`)。 条件なし = table の全 row (`find()` と同じ代表列)。
    fn live_preds(self) -> Result<Option<Vec<enchudb_engine::LivePred>>, SchemaError> {
        use enchudb_engine::LivePred;
        /// 閾値を engine の条件に (件数 1 は `Exists` / `ExistsEq`)。
        fn th_pred(via: u16, mine: Option<u16>, th: Th, preds: Vec<LivePred>) -> LivePred {
            match (th, mine) {
                (Th::Count(1), None) => LivePred::Exists { via, preds },
                (Th::Count(1), Some(mine)) => LivePred::ExistsEq { mine, theirs: via, preds },
                (Th::Count(min), _) => LivePred::CountAtLeast { via, mine, min, preds },
                (Th::Sum(sum_himo, min, signed), _) => LivePred::SumAtLeast { via, mine, sum_himo, min, signed, preds },
            }
        }
        let eng = self.db.engine();
        fn conv(eng: &Engine, p: Predicate, rep: Option<u16>) -> Result<Option<LivePred>, SchemaError> {
            let hid_of = |name: &str| -> Result<u16, SchemaError> {
                eng.himo_id(name)
                    .map(|h| h as u16)
                    .ok_or_else(|| SchemaError::Internal(format!("himo not found: {name}")))
            };
            Ok(Some(match p {
                Predicate::Eq(h, _) if h == u16::MAX => return Ok(None),
                Predicate::Eq(h, v) => LivePred::Eq { himo_id: h, value: v },
                Predicate::EqText(h, text) => LivePred::EqText { himo_id: h, text },
                Predicate::In(h, values) => LivePred::In { himo_id: h, values },
                Predicate::Range { himo_name, lo, hi } => LivePred::Range { himo_id: hid_of(&himo_name)?, lo, hi },
                Predicate::Via(path, inner) => match conv(eng, *inner, rep)? {
                    Some(pred) => LivePred::Via { path, pred: Box::new(pred) },
                    None => return Ok(None),
                },
                Predicate::Present(h) => LivePred::Present { himo_id: h },
                Predicate::Exists { via, th, preds } => match preds {
                    Some(preds) => th_pred(via, None, th, preds),
                    None => return Ok(None),
                },
                Predicate::ExistsEq { mine, theirs, th, preds } => match preds {
                    Some(preds) => th_pred(theirs, Some(mine), th, preds),
                    None => return Ok(None),
                },
                Predicate::Not(inner) => match conv(eng, *inner, rep)? {
                    Some(pred) => LivePred::Not(Box::new(pred)),
                    // 中身が常に偽 (未知の値など) = 否定は常に真: 条件を足さない (代表列を持つ row)
                    None => {
                        let rep = rep.ok_or_else(|| SchemaError::BadValue("subscribe: table has no columns".into()))?;
                        LivePred::Present { himo_id: rep }
                    }
                },
                Predicate::Or(branches) => {
                    if branches.is_empty() {
                        return Err(SchemaError::BadValue("or: queries on different tables".into()));
                    }
                    // 常に 0 件になる枝 (未知の列など) は落とす。 全部落ちたら全体が 0 件
                    let mut out = Vec::new();
                    for b in branches {
                        if let Some(conj) = conv_all(eng, b, rep)? {
                            out.push(conj);
                        }
                    }
                    if out.is_empty() {
                        return Ok(None);
                    }
                    LivePred::Or(out)
                }
            }))
        }
        /// AND 1 つ分。 `None` = 常に 0 件。 条件なし (`.all()`) は table の全 row = 代表列を持つ row。
        fn conv_all(eng: &Engine, preds: Vec<Predicate>, rep: Option<u16>) -> Result<Option<Vec<LivePred>>, SchemaError> {
            let mut out = Vec::with_capacity(preds.len().max(1));
            for p in preds {
                match conv(eng, p, rep)? {
                    Some(lp) => out.push(lp),
                    None => return Ok(None),
                }
            }
            // この table の row であることを保証する条件 (自分の列に値がある / 自分の ref 列をたどる) が無ければ
            // (`.all()` / 否定だけ / `where_exists` だけ)、 代表列を持つ row = table の全 row から絞る。
            // `Exists` は 「指されている entity」 なので table の row とは限らない (削除済みの row を指したままの
            // ref もある)
            fn own(p: &LivePred) -> bool {
                match p {
                    LivePred::Not(_)
                    | LivePred::Exists { .. }
                    | LivePred::CountAtLeast { mine: None, .. }
                    | LivePred::SumAtLeast { mine: None, .. } => false,
                    // 自分の ref 列に値がある (中身が否定でも)
                    LivePred::Via { .. } => true,
                    LivePred::Or(bs) => bs.iter().all(|b| b.iter().any(own)),
                    _ => true,
                }
            }
            if !out.iter().any(own) {
                let rep = rep.ok_or_else(|| SchemaError::BadValue("subscribe: table has no columns".into()))?;
                out.push(LivePred::Present { himo_id: rep });
            }
            Ok(Some(out))
        }
        // `.all()` 系 — find() と同じ代表 column (PK or 先頭列) を持つ全 row
        let rep = self.table.pk
            .or_else(|| if self.table.cols.is_empty() { None } else { Some(0) })
            .map(|i| self.table.cols[i].himo_id);
        conv_all(eng, self.preds, rep)
    }

    // ──── 0.8.10 (#43): Query 終端の集計 chain API ────
    //
    // 既存 `find()` / `count()` の終端に加えて、 sub-set (= where_*) への
    // scalar / GROUP BY 集計を直接吐く method を追加。 callsite が engine
    // 直叩き (= cylinder + 手書き loop) に堕ちずに schema chain で完結できる。
    //
    // 実装は全て `self.find()` で eid 集合を取り、 engine の `_eids_par`
    // (= 64k 閾値で seq fallback) を呼ぶだけの薄い wrapper。 col 名は
    // `{table}.{col}` で prefix された himo 名に解決。

    /// u32 の集計 (`sum` / `min` / `max` / `group_*` / `histogram`) の列。 BigInt は `BadValue` — 64 bit の値は
    /// [`sum_i128`](Self::sum_i128) / [`min_i64`](Self::min_i64) / [`max_i64`](Self::max_i64) で。
    fn resolve_himo(&self, col: &str) -> Result<String, SchemaError> {
        let cd = self.table.col(col).ok_or_else(|| SchemaError::BadValue(format!("unknown col: {}", col)))?;
        if cd.ty == ColumnType::BigInt {
            return Err(SchemaError::BadValue(format!("{col} is a BigInt column (use sum_i128 / min_i64 / max_i64)")));
        }
        Ok(cd.himo_name.clone())
    }

    /// 64 bit の集計の列: (紐名, BigInt か)。 Number / BigInt の列だけ。
    fn resolve_num(&self, col: &str) -> Result<(String, bool), SchemaError> {
        match self.table.col(col) {
            Some(cd) if matches!(cd.ty, ColumnType::Number | ColumnType::BigInt) => {
                Ok((cd.himo_name.clone(), cd.ty == ColumnType::BigInt))
            }
            Some(_) => Err(SchemaError::BadValue(format!("{col} is not a Number / BigInt column"))),
            None => Err(SchemaError::BadValue(format!("unknown col: {}", col))),
        }
    }

    /// sub-set 内の `col` の合計 (Number / BigInt、 負の数も)。 値の無い row は足さない。
    pub fn sum_i128(self, col: &str) -> Result<i128, SchemaError> {
        let (himo, big) = self.resolve_num(col)?;
        let db = self.db;
        let eids = self.find()?;
        let raw = db.eng.sum64(&himo, &eids) as i128;
        if !big {
            return Ok(raw);
        }
        // BigInt は 1 件ごとに v + 2^63 で載っている
        let n = db.eng.count(&himo, &eids) as i128;
        Ok(raw - (n << 63))
    }

    /// sub-set 内の `col` の最小値 (Number / BigInt)。
    pub fn min_i64(self, col: &str) -> Result<Option<i64>, SchemaError> {
        let (himo, big) = self.resolve_num(col)?;
        let db = self.db;
        let eids = self.find()?;
        Ok(db.eng.min64(&himo, &eids).map(|v| if big { big_val(v) } else { v as i64 }))
    }

    /// sub-set 内の `col` の最大値 (Number / BigInt)。
    pub fn max_i64(self, col: &str) -> Result<Option<i64>, SchemaError> {
        let (himo, big) = self.resolve_num(col)?;
        let db = self.db;
        let eids = self.find()?;
        Ok(db.eng.max64(&himo, &eids).map(|v| if big { big_val(v) } else { v as i64 }))
    }

    /// sub-set 内で `col` が tie された entity 数 (= `SELECT COUNT(col) WHERE ...`)。
    /// 既存 `count()` は entity 数、 こちらは特定 column の non-missing 数。 どの型の列でも。
    pub fn count_col(self, col: &str) -> Result<u32, SchemaError> {
        let himo = self
            .table
            .col(col)
            .map(|cd| cd.himo_name.clone())
            .ok_or_else(|| SchemaError::BadValue(format!("unknown col: {}", col)))?;
        let db = self.db;
        let eids = self.find()?;
        Ok(db.eng.count_eids_par(&himo, &eids))
    }

    /// sub-set 内の `col` の合計 (= `SELECT SUM(col) WHERE ...`)。
    pub fn sum(self, col: &str) -> Result<u64, SchemaError> {
        let himo = self.resolve_himo(col)?;
        let db = self.db;
        let eids = self.find()?;
        Ok(db.eng.sum_eids_par(&himo, &eids))
    }

    /// sub-set 内の `col` の最小値 (= `SELECT MIN(col) WHERE ...`)。
    pub fn min(self, col: &str) -> Result<Option<u32>, SchemaError> {
        let himo = self.resolve_himo(col)?;
        let db = self.db;
        let eids = self.find()?;
        Ok(db.eng.min_eids_par(&himo, &eids))
    }

    /// sub-set 内の `col` の最大値 (= `SELECT MAX(col) WHERE ...`)。
    pub fn max(self, col: &str) -> Result<Option<u32>, SchemaError> {
        let himo = self.resolve_himo(col)?;
        let db = self.db;
        let eids = self.find()?;
        Ok(db.eng.max_eids_par(&himo, &eids))
    }

    /// sub-set 内で `group` × `sum` の GROUP BY 集計 (= `SUM(sum) GROUP BY group WHERE ...`)。
    pub fn group_sum(self, group: &str, sum: &str) -> Result<Vec<(u32, u64)>, SchemaError> {
        let group_himo = self.resolve_himo(group)?;
        let sum_himo = self.resolve_himo(sum)?;
        let db = self.db;
        let eids = self.find()?;
        Ok(db.eng.group_sum_eids_par(&group_himo, &sum_himo, &eids))
    }

    /// sub-set 内で `group` × `val` の MIN 集計 (= `MIN(val) GROUP BY group WHERE ...`)。
    pub fn group_min(self, group: &str, val: &str) -> Result<Vec<(u32, u32)>, SchemaError> {
        let group_himo = self.resolve_himo(group)?;
        let val_himo = self.resolve_himo(val)?;
        let db = self.db;
        let eids = self.find()?;
        Ok(db.eng.group_min_eids_par(&group_himo, &val_himo, &eids))
    }

    /// sub-set 内で `group` × `val` の MAX 集計 (= `MAX(val) GROUP BY group WHERE ...`)。
    pub fn group_max(self, group: &str, val: &str) -> Result<Vec<(u32, u32)>, SchemaError> {
        let group_himo = self.resolve_himo(group)?;
        let val_himo = self.resolve_himo(val)?;
        let db = self.db;
        let eids = self.find()?;
        Ok(db.eng.group_max_eids_par(&group_himo, &val_himo, &eids))
    }

    /// sub-set 内の `col` 値域 `[vmin, vmax]` を `n_buckets` 等分した頻度
    /// ヒストグラム。 値域外 drop、 戻り値長は常に `n_buckets`。
    pub fn histogram(self, col: &str, vmin: u32, vmax: u32, n_buckets: u32)
        -> Result<Vec<u32>, SchemaError>
    {
        let himo = self.resolve_himo(col)?;
        let db = self.db;
        let eids = self.find()?;
        Ok(db.eng.histogram_eids_par(&himo, &eids, vmin, vmax, n_buckets))
    }
}

// ─────────────────────────── EntityRef ───────────────────────────

pub struct EntityRef<'a> {
    db: &'a Database,
    table: Arc<TableInner>,
    eid: EntityId,
}

impl<'a> EntityRef<'a> {
    pub fn eid(&self) -> EntityId { self.eid }

    pub fn get(&self, col: &str) -> Option<Value> {
        let cd = self.table.col(col)?;
        let eng = self.db.engine();
        match cd.ty {
            ColumnType::Number => eng.get(self.eid, &cd.himo_name).map(|v| Value::Number(v as i64)),
            ColumnType::BigInt => eng.get_by_id(self.eid, cd.himo_id).map(|v| Value::Number(big_val(v))),
            // #184: storage の Ref 値は u32 (local 部) なので、素 cast すると find() /
            // commit() が返す full eid (peer prefix 付き) と食い違う。Ref は必ず自 DB 内
            // entity (翻訳済み foreign 含む = 自 prefix) を指すので自 peer_id で復元する。
            ColumnType::Ref => eng
                .get(self.eid, &cd.himo_name)
                .map(|v| Value::Ref(enchudb_oplog::make_eid(eng.peer_id(), v as u32))),
            // #119: 借用返しの `get_text` は writer 稼働中に seqlock verify を通らず torn
            // bytes を掴む (= from_utf8 が失敗して silent に None を返す)。 元々即コピーして
            // いるので、 verify 付きの owned 版に寄せてもコピー回数は変わらない。
            ColumnType::Tag | ColumnType::Leaf => eng.get_text_owned(self.eid, &cd.himo_name)
                .and_then(|b| String::from_utf8(b).ok().map(Value::Text)),
        }
    }

    /// 単発 set (1 column)。 chain したい場合は `update()` builder を使う。
    pub fn set<V: Into<Value>>(self, col: &str, val: V) -> EntityUpdate<'a> {
        EntityUpdate {
            db: self.db,
            table: self.table,
            eid: self.eid,
            values: vec![(col.to_string(), val.into())],
        }
    }

    pub fn update(self) -> EntityUpdate<'a> {
        EntityUpdate {
            db: self.db,
            table: self.table,
            eid: self.eid,
            values: Vec::new(),
        }
    }

    pub fn delete(self) -> Result<(), SchemaError> {
        self.db.engine().delete(self.eid);
        Ok(())
    }
}

pub struct EntityUpdate<'a> {
    db: &'a Database,
    table: Arc<TableInner>,
    eid: EntityId,
    values: Vec<(String, Value)>,
}

impl<'a> EntityUpdate<'a> {
    pub fn set<V: Into<Value>>(mut self, col: &str, val: V) -> Self {
        self.values.push((col.to_string(), val.into()));
        self
    }
    pub fn commit(self) -> Result<(), SchemaError> {
        let eng = self.db.engine();
        for (col, v) in &self.values {
            let cd = self.table.col_or_err(col)?;
            tie_value(eng, self.eid, cd, v)?;
        }
        Ok(())
    }
}

// ─────────────────────────── helpers ───────────────────────────

/// query 等値判定用の raw u32 化。 Text 列は vocab を引き、 未登録なら
/// `u32::MAX` を返す (caller 側で「結果 0 件」 として扱う前提)。
/// `u64::MAX` = 一致する row は無い (まだ vocab に無い文字列 / Leaf)。
fn value_to_raw_for_query(eng: &Engine, cd: &ColumnInner, v: &Value) -> Result<u64, SchemaError> {
    match (cd.ty, v) {
        (_, Value::Null) => Err(SchemaError::BadValue("Null is not a queryable value".into())),
        (ColumnType::Number, Value::Number(n)) => {
            if *n < 0 || (*n as u64) >= u32::MAX as u64 {
                return Err(SchemaError::BadValue(format!("integer out of u32 range: {n}")));
            }
            Ok(*n as u64)
        }
        (ColumnType::BigInt, Value::Number(n)) => {
            big_raw(*n).ok_or_else(|| SchemaError::BadValue(format!("integer out of BigInt range: {n}")))
        }
        (ColumnType::Tag, Value::Text(s)) => Ok(eng.vocab_id(s).map_or(u64::MAX, u64::from)),
        // Leaf は dedupe しないので等値クエリは原理的に成立しない (毎回別 vid)。
        // 念のため `u64::MAX` を返してクエリ結果 0 件にする。
        (ColumnType::Leaf, Value::Text(_)) => Ok(u64::MAX),
        (ColumnType::Ref, Value::Ref(eid)) => Ok(enchudb_oplog::eid_local(*eid) as u64),
        (t, v) => Err(SchemaError::TypeMismatch(format!("{t:?} vs {v:?}"))),
    }
}

/// 書き込み (tie) 用の型 dispatch。 Text は str をそのまま vocab に流す。
///
/// `cd.himo_id` は build 時に pre-resolve 済みなので、 hot path で `_by_id` 経路を
/// 使って string lookup を避ける。
fn tie_value(eng: &Engine, eid: EntityId, cd: &ColumnInner, v: &Value) -> Result<(), SchemaError> {
    match (cd.ty, v) {
        (_, Value::Null) => Err(SchemaError::BadValue("Null tie not supported (use entity.delete or untie)".into())),
        (ColumnType::Number, Value::Number(n)) => {
            if *n < 0 || (*n as u64) >= u32::MAX as u64 {
                return Err(SchemaError::BadValue(format!("integer out of u32 range: {n}")));
            }
            eng.tie_to_by_id(eid, cd.himo_id, *n as u32);
            Ok(())
        }
        (ColumnType::BigInt, Value::Number(n)) => {
            let raw = big_raw(*n).ok_or_else(|| SchemaError::BadValue(format!("integer out of BigInt range: {n}")))?;
            eng.tie_to_by_id(eid, cd.himo_id, raw);
            Ok(())
        }
        (ColumnType::Tag, Value::Text(s)) | (ColumnType::Leaf, Value::Text(s)) => {
            // engine の tie_text_to_by_id は himo の ValueType (Tag / Leaf) を見て
            // vocab.get_or_insert (dedupe) vs vocab.insert (新規 id) を dispatch する。
            eng.tie_text_to_by_id(eid, cd.himo_id, s);
            Ok(())
        }
        (ColumnType::Ref, Value::Ref(t)) => {
            eng.tie_ref_to_by_id(eid, cd.himo_id, *t);
            Ok(())
        }
        (t, v) => Err(SchemaError::TypeMismatch(format!("{t:?} vs {v:?}"))),
    }
}

// ─────────────────────────── schema (de)serialize ───────────────────────────

struct RawTableDef {
    name: String,
    cols: Vec<(String, ColumnType)>,
    pk: Option<String>,
    relations: Vec<(String, String)>,
}

/// `.schema` sidecar の serialize 形式は table / column / relation 名を
/// 区切り文字 `|` `;` `:` 改行、 relation は `->` で連結する (下記 `serialize_schema`
/// 参照)。 これらを名前に含むと round-trip で schema が壊れる (issue #61) ため、
/// table / column を define する build 時点で弾いて silent corruption を止める。
fn validate_schema_name(kind: &str, name: &str) -> Result<(), SchemaError> {
    const FORBIDDEN: &[char] = &['|', ';', ':', '\n', '\r'];
    if name.is_empty() {
        return Err(SchemaError::BadValue(format!("{kind} name must not be empty")));
    }
    if let Some(c) = name.chars().find(|c| FORBIDDEN.contains(c)) {
        return Err(SchemaError::BadValue(format!(
            "{kind} name {name:?} contains reserved character {c:?} \
             (| ; : newline and \"->\" are .schema sidecar delimiters)"
        )));
    }
    if name.contains("->") {
        return Err(SchemaError::BadValue(format!(
            "{kind} name {name:?} contains reserved sequence \"->\" \
             (.schema sidecar relation delimiter)"
        )));
    }
    Ok(())
}

/// column 名の追加検証。 `validate_schema_name` (sidecar 区切り文字) に加えて、
/// 0.9.0 で engine の content 互換 layer が `_c_{key}` himo 名を予約したため
/// (`Engine::content` 参照)、 `_c_` prefix の user column を schema 層で弾く。
fn validate_column_name(name: &str) -> Result<(), SchemaError> {
    validate_schema_name("column", name)?;
    if name.starts_with("_c_") {
        return Err(SchemaError::BadValue(format!(
            "column name {name:?} uses reserved prefix \"_c_\" \
             (engine content compat layer himo namespace, 0.9.0)"
        )));
    }
    Ok(())
}

fn serialize_schema(tables: &[Arc<TableInner>]) -> String {
    // format: "v1\n" + per-line "<table>|<col>:TAG[:pk];...|REL:<from>->\<to>;..."
    let mut s = String::from("v1\n");
    for t in tables {
        s.push_str(&t.name);
        s.push('|');
        for (i, c) in t.cols.iter().enumerate() {
            if i > 0 { s.push(';'); }
            s.push_str(&c.name);
            s.push(':');
            s.push_str(c.ty.tag());
            if Some(c.name.as_str()) == t.pk.map(|i| t.cols[i].name.as_str()) {
                s.push_str(":pk");
            }
        }
        if !t.relations.is_empty() {
            s.push('|');
            for (i, r) in t.relations.iter().enumerate() {
                if i > 0 { s.push(';'); }
                s.push_str(&r.from_col);
                s.push_str("->");
                s.push_str(&r.to_table);
            }
        }
        s.push('\n');
    }
    s
}

fn deserialize_schema(s: &str) -> Result<Vec<RawTableDef>, SchemaError> {
    let mut lines = s.lines();
    let header = lines.next().ok_or_else(|| SchemaError::Parse("empty schema".into()))?;
    if header != "v1" { return Err(SchemaError::Parse(format!("unknown schema version: {header}"))); }

    let mut out = Vec::new();
    for line in lines {
        if line.is_empty() { continue; }
        let mut parts = line.split('|');
        let name = parts.next().ok_or_else(|| SchemaError::Parse("missing table name".into()))?.to_string();
        let cols_part = parts.next().ok_or_else(|| SchemaError::Parse("missing cols".into()))?;
        let rel_part = parts.next();

        let mut cols = Vec::new();
        let mut pk = None;
        for spec in cols_part.split(';') {
            if spec.is_empty() { continue; }
            let mut sub = spec.split(':');
            let cname = sub.next().ok_or_else(|| SchemaError::Parse("col name".into()))?.to_string();
            let tag = sub.next().ok_or_else(|| SchemaError::Parse("col tag".into()))?;
            let ty = ColumnType::from_tag(tag)
                .ok_or_else(|| SchemaError::Parse(format!("unknown col tag: {tag}")))?;
            let mut is_pk = false;
            for opt in sub { if opt == "pk" { is_pk = true; } }
            if is_pk { pk = Some(cname.clone()); }
            cols.push((cname, ty));
        }

        let mut relations = Vec::new();
        if let Some(rp) = rel_part {
            for spec in rp.split(';') {
                if spec.is_empty() { continue; }
                let mut sides = spec.split("->");
                let from = sides.next().ok_or_else(|| SchemaError::Parse("rel from".into()))?.to_string();
                let to = sides.next().ok_or_else(|| SchemaError::Parse("rel to".into()))?.to_string();
                relations.push((from, to));
            }
        }
        out.push(RawTableDef { name, cols, pk, relations });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> String {
        format!("/tmp/enchudb-schema-test-{}-{}.db", name, std::process::id())
    }

    #[test]
    fn create_table_and_insert() {
        let path = tmp("create_insert");
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        {
            let mut db = Database::create(&path).unwrap();
            let users = db.table("users")
                .number("id")
                .tag("name")
                .number("age")
                .primary_key("id")
                .build()
                .unwrap();

            let alice = users.insert()
                .set("id", 1i64)
                .set("name", "Alice")
                .set("age", 30i64)
                .commit()
                .unwrap();

            assert_eq!(users.entity(alice).get("name"), Some(Value::Text("Alice".into())));
            assert_eq!(users.entity(alice).get("age"), Some(Value::Number(30)));
        }
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn where_eq_and_chain() {
        let path = tmp("where_eq");
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        {
            let mut db = Database::create(&path).unwrap();
            let users = db.table("users")
                .number("id")
                .tag("name")
                .number("age")
                .tag("city")
                .primary_key("id")
                .build()
                .unwrap();

            users.insert().set("id", 1i64).set("name", "Alice").set("age", 30i64).set("city", "Tokyo").commit().unwrap();
            users.insert().set("id", 2i64).set("name", "Bob").set("age", 30i64).set("city", "Osaka").commit().unwrap();
            users.insert().set("id", 3i64).set("name", "Carol").set("age", 25i64).set("city", "Tokyo").commit().unwrap();

            let age30 = users.where_eq("age", 30i64).find().unwrap();
            assert_eq!(age30.len(), 2);

            let tokyo30 = users.where_eq("age", 30i64).where_eq("city", "Tokyo").find().unwrap();
            assert_eq!(tokyo30.len(), 1);
        }
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn upsert_replaces_pk_row() {
        let path = tmp("upsert");
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        {
            let mut db = Database::create(&path).unwrap();
            let t = db.table("kv")
                .tag("key")
                .number("ts")
                .primary_key("key")
                .build()
                .unwrap();
            let _ = t.upsert().set("key", "k1").set("ts", 100i64).commit().unwrap();
            let _ = t.upsert().set("key", "k1").set("ts", 200i64).commit().unwrap();
            let rows = t.where_eq("key", "k1").find().unwrap();
            assert_eq!(rows.len(), 1);
            let ts = t.entity(rows[0]).get("ts");
            assert_eq!(ts, Some(Value::Number(200)));
        }
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
    }

    /// #60: 並行 mode で同一 PK を複数 thread が同時 upsert しても重複行が
    /// できないこと。 fix (per-table upsert_lock) 無効化時は TOCTOU で複数行が
    /// できて fail する。
    #[test]
    fn concurrent_upsert_same_pk_does_not_duplicate() {
        let path = tmp("concurrent_upsert_pk");
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        let db = {
            let mut db = Database::create(&path).unwrap();
            db.table("kv")
                .tag("key")
                .number("ts")
                .primary_key("key")
                .build()
                .unwrap();
            db.finish_concurrent().unwrap() // Arc<Database>
        };

        let n = 16usize;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(n));
        let mut handles = Vec::with_capacity(n);
        for i in 0..n {
            let db = db.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                // 全 thread を同時 release して contention を最大化
                barrier.wait();
                let t = db.get_table("kv").unwrap();
                t.upsert()
                    .set("key", "k1")
                    .set("ts", (i as i64) + 1)
                    .commit()
                    .unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let t = db.get_table("kv").unwrap();
        let rows = t.where_eq("key", "k1").find().unwrap();
        assert_eq!(
            rows.len(),
            1,
            "concurrent upsert of same PK must produce exactly 1 row, got {}",
            rows.len()
        );
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn schema_persists_across_reopen() {
        let path = tmp("persist");
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        {
            let mut db = Database::create(&path).unwrap();
            let users = db.table("users").number("id").tag("name").primary_key("id").build().unwrap();
            users.insert().set("id", 1i64).set("name", "Alice").commit().unwrap();
            users.insert().set("id", 2i64).set("name", "Bob").commit().unwrap();
        }
        // reopen — CREATE TABLE 再呼出なしで data 見える
        {
            let db = Database::open(&path).unwrap();
            let tables = db.list_tables();
            assert_eq!(tables.len(), 1);
            assert_eq!(tables[0].name, "users");
            assert_eq!(tables[0].columns.len(), 2);

            let users = db.get_table("users").unwrap();
            let alice = users.where_eq("name", "Alice").find_one().unwrap();
            assert!(alice.is_some());
            let name = users.entity(alice.unwrap()).get("name");
            assert_eq!(name, Some(Value::Text("Alice".into())));
        }
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn delete_and_update() {
        let path = tmp("del_upd");
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        {
            let mut db = Database::create(&path).unwrap();
            let t = db.table("notes")
                .number("id")
                .tag("body")
                .number("ts")
                .primary_key("id")
                .build().unwrap();
            let n1 = t.insert().set("id", 1i64).set("body", "hi").set("ts", 100i64).commit().unwrap();
            let _n2 = t.insert().set("id", 2i64).set("body", "yo").set("ts", 200i64).commit().unwrap();

            t.entity(n1).set("ts", 150i64).commit().unwrap();
            assert_eq!(t.entity(n1).get("ts"), Some(Value::Number(150)));

            t.entity(n1).delete().unwrap();
            // entity 削除後は all() からも消える。
            // (実体的には marker himo の tie も消える)
            let remaining = t.all().find().unwrap();
            assert_eq!(remaining.len(), 1);
        }
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn relation_ref_query() {
        let path = tmp("relation");
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        {
            let mut db = Database::create(&path).unwrap();

            // companies を build (TableBuilder は &mut db、 build 後 handle は &db 借用)。
            let (ant_id, goo_id) = {
                let companies = db.table("companies").number("id").tag("name").primary_key("id").build().unwrap();
                let a = companies.insert().set("id", 1i64).set("name", "Anthropic").commit().unwrap();
                let g = companies.insert().set("id", 2i64).set("name", "Google").commit().unwrap();
                (a, g)
            }; // companies の借用がここで終わる

            let users = db.table("users")
                .number("id")
                .tag("name")
                .ref_to("company", "companies")
                .primary_key("id")
                .build().unwrap();

            users.insert().set("id", 1i64).set("name", "Alice").set("company", Value::Ref(ant_id)).commit().unwrap();
            users.insert().set("id", 2i64).set("name", "Bob").set("company", Value::Ref(ant_id)).commit().unwrap();
            users.insert().set("id", 3i64).set("name", "Carol").set("company", Value::Ref(goo_id)).commit().unwrap();

            let staff = users.where_ref("company", ant_id).find().unwrap();
            assert_eq!(staff.len(), 2);
        }
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bindings_extract_himo_id_and_engine_direct_write() {
        // build 時に column 名 → himo_id が pre-resolve されてる。
        // table = 紐の束 declaration なので、 row 識別 marker は存在せず、
        // 抽出した himo_id だけで engine 直叩き write/read が schema find に揃う。
        let path = tmp("bindings");
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        let mut db = Database::create(&path).unwrap();
        let _ = db.table("posts")
            .tag("author")
            .number("year")
            .leaf("body")
            .primary_key("author")
            .build()
            .unwrap();

        // bindings 取り出し: column 名 → himo_id だけ
        let (author_hid, year_hid, body_hid) = {
            let posts = db.get_table("posts").unwrap();
            (
                posts.himo_id("author").expect("author hid"),
                posts.himo_id("year").expect("year hid"),
                posts.himo_id("body").expect("body hid"),
            )
        };
        assert_ne!(author_hid, year_hid);
        assert_ne!(author_hid, body_hid);
        // unknown col は None
        assert!(db.get_table("posts").unwrap().himo_id("nope").is_none());

        // engine 直叩き経路で 1 row 書く (marker tie は不要)。
        // 0.7.0: TableBuilder::build が define_table を呼ぶようになったので、
        // 既存 entity() ではなく entity_in("posts") で table 内 eid を払出。
        let e = {
            let eng = db.engine();
            let e = eng.entity_in("posts").expect("entity_in posts");
            eng.tie_text_to_by_id(e, author_hid, "alice");
            eng.tie_to_by_id(e, year_hid, 2026);
            eng.tie_text_to_by_id(e, body_hid, "hello");
            e
        };

        // schema find 経由で見える
        let posts = db.get_table("posts").unwrap();
        let rows = posts.where_eq("author", "alice").find().unwrap();
        assert_eq!(rows.len(), 1, "alice row should be visible via schema find");
        assert_eq!(rows[0], e);

        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_readonly_coexists_with_writer() {
        // writer (sf 風) と reader (Studio 風) が同 DB に並行 open できることを確認。
        // 既存 DB を 1 つ作って閉じ、 その後:
        //   - writer (open_with_oplog) で 1 つ open (writer lock 取る)
        //   - reader (open_readonly) を 3 つ並行 open (lock 取らない)
        // 全部 同じ schema が見えること。
        let path = tmp("readonly_coexist");
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}.oplog", path));
        let _ = std::fs::remove_file(format!("{}.lock", path));

        // create + 1 row insert + close
        {
            let mut db = Database::create(&path).unwrap();
            db.table("kv").tag("k").number("v").primary_key("k").build().unwrap();
            let kv = db.get_table("kv").unwrap();
            kv.insert().set("k", "x").set("v", 42i64).commit().unwrap();
        }

        // writer + 3 reader 同時 open
        let writer = Database::open(&path).unwrap();
        let r1 = Database::open_readonly(&path).unwrap();
        let r2 = Database::open_readonly(&path).unwrap();
        let r3 = Database::open_readonly(&path).unwrap();

        for db in [&writer, &r1, &r2, &r3] {
            let kv = db.get_table("kv").unwrap();
            let rows = kv.where_eq("k", "x").find().unwrap();
            assert_eq!(rows.len(), 1);
        }

        drop((writer, r1, r2, r3));
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}.oplog", path));
        let _ = std::fs::remove_file(format!("{}.lock", path));
    }

    #[test]
    fn create_growable_with_capacity_apparent_size_scales_down() {
        // issue2: default の 16 M max_entities では layout total ~25 GB で、
        // 空 DB の apparent size も 24 GB の sparse file になる。 cap を絞れば
        // apparent も layout も比例して縮むことを確認する。
        let path_default = tmp("growable_cap_default");
        let path_capped = tmp("growable_cap_65k");
        let _ = std::fs::remove_dir_all(&path_default); // v10: DB は directory
        let _ = std::fs::remove_file(&path_default);
        let _ = std::fs::remove_dir_all(&path_capped); // v10: DB は directory
        let _ = std::fs::remove_file(&path_capped);

        {
            let _db = Database::create_growable(&path_default).unwrap();
        }
        {
            let _db = Database::create_growable_with_capacity(&path_capped, 65_536).unwrap();
        }

        // v10 (request21): 本体は directory + segment で、 見かけ = 書いた分。 旧 test は
        // 「default は 20 GB 以上に見える」 を固定していたが、 それ自体が消えた。 どちらも
        // 数 MB 以下で、 capacity は見かけに効かないことを固定する。
        fn dir_bytes(p: &str) -> u64 {
            fn walk(p: &std::path::Path, acc: &mut u64) {
                for e in std::fs::read_dir(p).unwrap().flatten() {
                    if e.file_type().unwrap().is_dir() { walk(&e.path(), acc); } else { *acc += e.metadata().unwrap().len(); }
                }
            }
            let mut acc = 0; walk(std::path::Path::new(p), &mut acc); acc
        }
        let size_default = dir_bytes(&path_default);
        let size_capped = dir_bytes(&path_capped);
        assert!(size_default < 64 * 1024 * 1024, "default apparent = {}", size_default);
        assert!(size_capped < 64 * 1024 * 1024, "capped apparent = {}", size_capped);

        let _ = std::fs::remove_dir_all(&path_default); // v10: DB は directory
        let _ = std::fs::remove_file(&path_default);
        let _ = std::fs::remove_dir_all(&path_capped); // v10: DB は directory
        let _ = std::fs::remove_file(&path_capped);
    }

    #[test]
    fn finish_with_wal_transitions_to_concurrent() {
        let path = tmp("finish_wal");
        let oplog_path = format!("{}.oplog", path);
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&oplog_path); // v10: DB は directory
        let _ = std::fs::remove_file(&oplog_path);

        let db_arc: Arc<Database> = {
            let mut db = Database::create_growable_tiny(&path).unwrap();
            db.table("kv").tag("key").number("ts").primary_key("key").build().unwrap();
            assert!(!db.is_concurrent());
            db.finish_with_oplog(64 * 1024).unwrap()
        };
        assert!(db_arc.is_concurrent());
        assert_eq!(db_arc.list_tables().len(), 1);

        // concurrent モード下で insert + 確認
        let kv = db_arc.get_table("kv").unwrap();
        kv.insert().set("key", "k1").set("ts", 100i64).commit().unwrap();
        kv.insert().set("key", "k2").set("ts", 200i64).commit().unwrap();
        let rows = kv.where_eq("key", "k1").find().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(kv.entity(rows[0]).get("ts"), Some(Value::Number(100)));

        // Arc clone して別 thread からも見えるか
        let db_clone = db_arc.clone();
        let h = std::thread::spawn(move || {
            let kv = db_clone.get_table("kv").unwrap();
            kv.where_eq("key", "k2").find().unwrap().len()
        });
        assert_eq!(h.join().unwrap(), 1);

        drop(db_arc);
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&oplog_path); // v10: DB は directory
        let _ = std::fs::remove_file(&oplog_path);
    }

    #[test]
    fn open_with_wal_recovers_writes() {
        let path = tmp("open_wal");
        let oplog_path = format!("{}.oplog", path);
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&oplog_path); // v10: DB は directory
        let _ = std::fs::remove_file(&oplog_path);

        // 1: build → finish_with_oplog → 書き込み → oplog_sync で durable
        {
            let mut db = Database::create_growable_tiny(&path).unwrap();
            db.table("kv").tag("key").number("ts").primary_key("key").build().unwrap();
            let arc = db.finish_with_oplog(64 * 1024).unwrap();
            let kv = arc.get_table("kv").unwrap();
            kv.insert().set("key", "alpha").set("ts", 1000i64).commit().unwrap();
            kv.insert().set("key", "beta").set("ts", 2000i64).commit().unwrap();
            arc.engine().oplog_sync().unwrap();
            // Drop は最後の Arc が落ちる時 → consumer thread shutdown で sync
        }

        // 2: open_with_oplog で recover、 schema + data 両方見える
        {
            let arc = Database::open_with_oplog(&path, 64 * 1024).unwrap();
            assert_eq!(arc.list_tables().len(), 1);
            let kv = arc.get_table("kv").unwrap();
            let alpha = kv.where_eq("key", "alpha").find_one().unwrap();
            assert!(alpha.is_some());
            assert_eq!(kv.entity(alpha.unwrap()).get("ts"), Some(Value::Number(1000)));
        }

        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&oplog_path); // v10: DB は directory
        let _ = std::fs::remove_file(&oplog_path);
    }

    #[test]
    fn leaf_column_basic_write_read() {
        let path = tmp("leaf_basic");
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        {
            let mut db = Database::create(&path).unwrap();
            let posts = db.table("posts")
                .number("id")
                .tag("title")
                .leaf("body")
                .primary_key("id")
                .build()
                .unwrap();

            let e = posts.insert()
                .set("id", 1i64)
                .set("title", "hello")
                .set("body", "今日の天気は晴れ。良い一日だった。")
                .commit()
                .unwrap();

            // Leaf 列も get で読める (engine 内部では vocab 経由だが API は同じ)
            assert_eq!(
                posts.entity(e).get("body"),
                Some(Value::Text("今日の天気は晴れ。良い一日だった。".into()))
            );
        }
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn leaf_does_not_dedupe() {
        // 同じ文字列を複数 entity の Leaf 列に書いた時、vocab_id が別であることを確認。
        // Tag なら同じ vocab_id を共有するが、Leaf は dedupe しない。
        let path = tmp("leaf_no_dedupe");
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        {
            let mut db = Database::create(&path).unwrap();
            let notes = db.table("notes")
                .number("id")
                .leaf("memo")
                .primary_key("id")
                .build()
                .unwrap();

            let e1 = notes.insert().set("id", 1i64).set("memo", "same").commit().unwrap();
            let e2 = notes.insert().set("id", 2i64).set("memo", "same").commit().unwrap();

            // 両方読めて値は同じ
            assert_eq!(notes.entity(e1).get("memo"), Some(Value::Text("same".into())));
            assert_eq!(notes.entity(e2).get("memo"), Some(Value::Text("same".into())));

            // engine 直叩きで vocab_id が違うことを確認 (himo 名は "notes.memo")
            let eng = db.engine();
            let v1 = eng.get(e1, "notes.memo").unwrap();
            let v2 = eng.get(e2, "notes.memo").unwrap();
            assert_ne!(v1, v2, "Leaf は同じ文字列でも別 vocab_id を発行するべき");
        }
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn tag_dedupes_but_leaf_does_not_in_same_db() {
        // 同じ DB で Tag と Leaf を比較。Tag は dedupe、Leaf は dedupe しない。
        let path = tmp("tag_vs_leaf");
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        {
            let mut db = Database::create(&path).unwrap();
            let items = db.table("items")
                .number("id")
                .tag("category")
                .leaf("memo")
                .primary_key("id")
                .build()
                .unwrap();

            let e1 = items.insert().set("id", 1i64).set("category", "food").set("memo", "fresh").commit().unwrap();
            let e2 = items.insert().set("id", 2i64).set("category", "food").set("memo", "fresh").commit().unwrap();

            let eng = db.engine();
            // category (Tag) は dedupe → 同 vocab_id
            let cat1 = eng.get(e1, "items.category").unwrap();
            let cat2 = eng.get(e2, "items.category").unwrap();
            assert_eq!(cat1, cat2, "Tag は同じ文字列を dedupe するべき");

            // memo (Leaf) は dedupe しない → 別 vocab_id
            let m1 = eng.get(e1, "items.memo").unwrap();
            let m2 = eng.get(e2, "items.memo").unwrap();
            assert_ne!(m1, m2, "Leaf は dedupe しないべき");
        }
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn leaf_column_persists_across_reopen() {
        let path = tmp("leaf_persist");
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        let e = {
            let mut db = Database::create(&path).unwrap();
            let notes = db.table("notes")
                .number("id")
                .leaf("body")
                .primary_key("id")
                .build()
                .unwrap();
            notes.insert()
                .set("id", 1i64)
                .set("body", "to be persisted")
                .commit()
                .unwrap()
        };
        // reopen
        {
            let db = Database::open(&path).unwrap();
            let notes = db.get_table("notes").unwrap();
            assert_eq!(
                notes.entity(e).get("body"),
                Some(Value::Text("to be persisted".into()))
            );
        }
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn enable_sync_creates_reserved_tables() {
        // 0.7.0 Phase 3: enable_sync で _sync_ops / _sync_peers が engine に
        // 登録されること、 user-facing list_tables からは見えないことを確認。
        let path = tmp("sync_enable");
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}.tables", path));

        let mut db = Database::create(&path).unwrap();
        // build phase でいくつか user table を作る
        let _ = db.table("posts").number("id").tag("body").primary_key("id").build().unwrap();
        let _ = db.table("users").number("id").tag("name").primary_key("id").build().unwrap();

        // 有効化前は sync_enabled = false
        assert!(!db.sync_enabled());

        // sync を有効化
        db.enable_sync().unwrap();
        assert!(db.sync_enabled());

        // user-facing list_tables には _sync_ops / _sync_peers は出ない
        let info = db.list_tables();
        let user_tables: Vec<&str> = info.iter().map(|t| t.name.as_str()).collect();
        assert!(user_tables.contains(&"posts"));
        assert!(user_tables.contains(&"users"));
        assert!(!user_tables.iter().any(|n| n.starts_with('_')),
                "user list_tables should hide reserved tables, got: {user_tables:?}");

        // engine の list_user_tables (内部 raw) でも reserved は除外される
        let raw = db.engine().list_user_tables();
        assert!(!raw.iter().any(|(_, n, _, _)| n.starts_with('_')));

        // engine 内部 list_tables (= reserved 含む全件) には _sync_* が居る
        let raw_all = db.engine().list_tables();
        assert!(raw_all.iter().any(|(_, n, _, _)| n == "_sync_ops"));
        assert!(raw_all.iter().any(|(_, n, _, _)| n == "_sync_peers"));

        // 2 度目の enable_sync は idempotent
        db.enable_sync().unwrap();

        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn user_table_starting_with_underscore_rejected() {
        // 0.7.0 Phase 3: `_` 始まり名前は reserved 命名空間、 user 経路は弾く。
        let path = tmp("reserved_reject");
        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}.tables", path));

        let mut db = Database::create(&path).unwrap();
        let result = db.table("_my_table").number("id").build();
        let err = match result {
            Err(e) => format!("{:?}", e),
            Ok(_) => panic!("user table starting with '_' should be rejected"),
        };
        assert!(err.contains("reserved") || err.contains("_"),
                "error message should mention reserved namespace, got: {err}");

        let _ = std::fs::remove_dir_all(&path); // v10: DB は directory
        let _ = std::fs::remove_file(&path);
    }
}
