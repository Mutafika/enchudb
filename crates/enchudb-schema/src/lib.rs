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

use enchudb_engine::{Engine, HimoType};
use enchudb_oplog::EntityId;
use std::sync::Arc;

// 0.8.7: schema sidecar 拡張子。 `{db_path}.schema` で永続化。
// 0.6.x までの schema_meta_entity (= anonymous entity に blob を載せる方式) は
// `define_table` 後に anonymous が close されると panic する vestigial path
// だったため撤去、 `.schema` sidecar に置き換えた (issue note: schema_meta_entity
// は 0.7.0 の名残、 削除すべき)。 旧 DB 互換のため legacy blob 読み込み path も
// 残してあり、 初回 open で `.schema` sidecar に migrate されて以降は新 path のみ。
const SCHEMA_SIDECAR_EXT: &str = "schema";
// legacy (= 0.6.x の blob entity) 互換読み込み path 用、 新規書き出しでは使わない。
const LEGACY_SCHEMA_META_HIMO: &str = "__enchu_schema_meta__";
const LEGACY_SCHEMA_MARKER: &str = "__enchu_schema_v1__";
const LEGACY_SCHEMA_BLOB_HIMO: &str = "__enchu_schema_blob";

/// 列の型。
///
/// - `Number` — inline 数値 (HimoType::Number)
/// - `Tag` — 共有タグ、vocab 経由 (HimoType::Tag)。enum / カテゴリ / 名前など引かれる値向き
/// - `Leaf` — 終端タグ、FreeStore 経由 (HimoType::Leaf)。備考 / 本文など引かれない自由記述向き
/// - `Ref` — 他テーブル entity への参照 (HimoType::Ref)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Number,
    Tag,
    Leaf,
    Ref,
}

impl ColumnType {
    fn himo_type(self) -> HimoType {
        match self {
            ColumnType::Number => HimoType::Number,
            ColumnType::Tag => HimoType::Tag,
            ColumnType::Leaf => HimoType::Leaf,
            ColumnType::Ref => HimoType::Ref,
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
        }
    }
    fn from_tag(s: &str) -> Option<Self> {
        match s {
            "I" => Some(ColumnType::Number),
            "T" => Some(ColumnType::Tag),
            "L" => Some(ColumnType::Leaf),
            "R" => Some(ColumnType::Ref),
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

#[derive(Debug)]
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
}

impl TableInner {
    fn col(&self, name: &str) -> Option<&ColumnInner> {
        self.cols.iter().find(|c| c.name.eq_ignore_ascii_case(name))
    }
    fn col_or_err(&self, name: &str) -> Result<&ColumnInner, SchemaError> {
        self.col(name).ok_or_else(|| SchemaError::UnknownColumn(name.to_string()))
    }
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
        if !self.is_concurrent && !self.eng.is_readonly() && !self.tables.is_empty() {
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

    fn wrap_new(mut eng: Engine) -> Result<Self, SchemaError> {
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
    /// `Engine::define_table` 構築) でも、 engine の `.tables` sidecar + himo_types
    /// から fallback 復元 (= PK は不明扱い、 column type は himo_type から推定)。
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

    /// `Arc<Engine>` を clone して返す。 engine 直接アクセス / 他 component との共有用。
    pub fn arc_engine(&self) -> Arc<Engine> { self.eng.clone() }

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
        }
    }

    /// 既存 table の handle を取得。 未定義なら None。
    pub fn get_table<'a>(&'a self, name: &str) -> Option<Table<'a>> {
        self.tables.iter()
            .find(|t| t.name.eq_ignore_ascii_case(name))
            .map(|t| Table { db: self, inner: t.clone() })
    }

    /// 既存 table に column を追加。 standalone (non-concurrent) モードでのみ可能。
    /// 同名 column が既にあれば idempotent に成功で返る。 himo を新規 define して
    /// schema blob を書き戻すので、 以降の再 open で復元される。
    ///
    /// 典型用途: `Database::open` で開いた直後にスキーマ差分を当てて、
    /// `finish_with_oplog` で concurrent に flip するマイグレーション pattern。
    pub fn add_column(
        &mut self,
        table_name: &str,
        col_name: &str,
        ty: ColumnType,
    ) -> Result<(), SchemaError> {
        if self.is_concurrent {
            return Err(SchemaError::Internal(
                "add_column requires standalone Database — open via Database::open, not open_with_oplog".into()
            ));
        }
        let table_inner = self.find_table_inner(table_name)
            .ok_or_else(|| SchemaError::UnknownTable(table_name.to_string()))?;
        if table_inner.col(col_name).is_some() {
            return Ok(());
        }
        let himo_name = format!("{}.{}", table_inner.name, col_name);
        let eng_mut = Arc::get_mut(&mut self.eng).ok_or_else(|| {
            SchemaError::Internal("engine Arc shared — cannot mutate".into())
        })?;
        eng_mut.define_himo(&himo_name, ty.himo_type(), 0);
        let hid = eng_mut.himo_id(&himo_name)
            .ok_or_else(|| SchemaError::Internal(format!("himo {himo_name} after define")))?;

        let mut new_cols = table_inner.cols.clone();
        new_cols.push(ColumnInner {
            name: col_name.to_string(),
            ty,
            himo_name,
            himo_id: hid as u16,
        });
        let new_inner = Arc::new(TableInner {
            name: table_inner.name.clone(),
            table_vid: table_inner.table_vid,
            cols: new_cols,
            pk: table_inner.pk,
            relations: table_inner.relations.iter().map(|r| RelationInner {
                from_col: r.from_col.clone(),
                to_table: r.to_table.clone(),
            }).collect(),
        });
        let pos = self.tables.iter().position(|t| t.name.eq_ignore_ascii_case(table_name))
            .expect("table_inner found above");
        self.tables[pos] = new_inner;

        self.persist_schema()?;
        Ok(())
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

    // ───── TenantView ─────
    //
    // physical layout (table cluster vs DB ファイル) を隠して、 deployment が
    // centralized (pattern A) でも distributed (pattern B / C) でも同一 app code
    // で「ある tenant の view」 を扱えるようにする抽象。 詳細は issue #12。

    /// tenant scope の read view を取り出す。 prefix は `{name}.` (例: `alice`)。
    /// view 内で `get_table("users")` は実 table `alice.users` に解決される。
    pub fn tenant<'a>(&'a self, name: &str) -> TenantView<'a> {
        TenantView { db: self, prefix: Some(name.to_string()) }
    }

    /// tenant scope の build view を取り出す。 `table(...)` で建てる table は
    /// 自動で `{name}.` prefix が付与される。
    pub fn tenant_mut<'a>(&'a mut self, name: &str) -> TenantViewMut<'a> {
        TenantViewMut { db: self, prefix: Some(name.to_string()) }
    }

    /// root read view (= prefix 無し)。 pattern B の `Database::open` 直後に
    /// view 化したい時に使う、 全 table が見える。
    pub fn as_view<'a>(&'a self) -> TenantView<'a> {
        TenantView { db: self, prefix: None }
    }

    /// root build view。 prefix 無しで table を建てる、 pattern B の builder。
    pub fn as_view_mut<'a>(&'a mut self) -> TenantViewMut<'a> {
        TenantViewMut { db: self, prefix: None }
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
        // 1. `.schema` sidecar (= 0.8.7 以降の正規 path) を試す
        let mut parsed_opt: Option<Vec<RawTableDef>> = if !path.is_empty() {
            load_schema_from_sidecar(&path).map_err(|e| SchemaError::Io(e.to_string()))?
        } else {
            None
        };
        // 2. fallback: 旧 blob entity (= 0.6.x ~ 0.8.6 で書かれた DB)
        if parsed_opt.is_none() {
            parsed_opt = self.load_schema_from_legacy_blob()?;
        }
        // 3. fallback: engine `.tables` + himo_types (= mlbpulse 等の engine 直 DB)
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
                    match eng_mut.define_table(&raw.name, size_hint) {
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
                        let _ = eng_mut.define_himo_in(&raw.name, col_name, ty.himo_type(), 0);
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
        let Some(blob) = self.eng.get_content(eid, LEGACY_SCHEMA_BLOB_HIMO) else { return Ok(None); };
        let s = std::str::from_utf8(blob)
            .map_err(|_| SchemaError::Parse("legacy schema blob not utf8".into()))?;
        let parsed = deserialize_schema(s)?;
        Ok(Some(parsed))
    }

    /// 0.8.7: engine `.tables` sidecar + himo_types から synthetic な RawTableDef を
    /// 組み立てる fallback。 schema sidecar も legacy blob も無い engine 直構築 DB
    /// (= mlbpulse の 4.5M pitch DB 等) を query 可能にするための path。
    ///
    /// 復元できる情報: table 名、 column 名 (= himo full name の `.` 後)、 column type
    /// (= engine の himo_type)、 relations (= engine の fk_refs)。
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
                let Some(htype) = self.eng.himo_type_at(hid_idx) else { continue; };
                let ty = match htype {
                    HimoType::Number => ColumnType::Number,
                    HimoType::Tag => ColumnType::Tag,
                    HimoType::Leaf => ColumnType::Leaf,
                    HimoType::Ref => ColumnType::Ref,
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

/// 0.8.7: `.schema` sidecar の path を返す。
#[cfg(not(target_arch = "wasm32"))]
fn schema_sidecar_path_for(db_path: &str) -> std::path::PathBuf {
    let mut p = std::path::PathBuf::from(db_path);
    let ext = match p.extension() {
        Some(e) => format!("{}.{}", e.to_string_lossy(), SCHEMA_SIDECAR_EXT),
        None => SCHEMA_SIDECAR_EXT.to_string(),
    };
    p.set_extension(ext);
    p
}

/// 0.8.7: tables の serialize_schema 出力を `.schema` sidecar に atomic write。
/// tmp file → fsync → rename で crash-safe。
#[cfg(not(target_arch = "wasm32"))]
fn persist_schema_to_sidecar(
    db_path: &str,
    tables: &[Arc<TableInner>],
) -> std::io::Result<()> {
    use std::io::Write;
    let sidecar = schema_sidecar_path_for(db_path);
    let tmp = sidecar.with_extension(format!(
        "{}.tmp",
        sidecar
            .extension()
            .map(|e| e.to_string_lossy().into_owned())
            .unwrap_or_default()
    ));
    let bytes = serialize_schema(tables);
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(bytes.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &sidecar)?;
    Ok(())
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

// ─────────────────────────── TenantView / TenantViewMut ───────────────────────────

/// 読み取り専用の tenant view。 `Database::tenant(name)` で取り出す、
/// あるいは pattern B (per-DB-file 単一 tenant) 用に `Database::as_view()` で
/// prefix 無しの root view を取り出す。 内部表現は薄い ref + prefix のみ、
/// storage layout は変えない。 詳細は issue #12。
pub struct TenantView<'a> {
    db: &'a Database,
    prefix: Option<String>,
}

/// build phase 用 tenant view。 `table(...)` で建てる table 名に prefix が
/// 自動付与される。 root build view は `Database::as_view_mut()` から。
pub struct TenantViewMut<'a> {
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

impl<'a> TenantView<'a> {
    /// この view の prefix (= tenant 名)。 root view なら None。
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// 既存 table を引く。 prefix 自動付与。
    pub fn get_table(&self, name: &str) -> Option<Table<'a>> {
        self.db.get_table(&resolve_prefixed(self.prefix.as_deref(), name))
    }

    /// この view から見える table の一覧。 tenant scope なら `{prefix}.` で始まる
    /// table のみ、 prefix は剥がして「view 内の short name」 で返す。 root view は
    /// 全 table をそのまま返す。
    pub fn list_tables(&self) -> Vec<TableInfo> {
        filter_tables_by_prefix(self.db.list_tables(), self.prefix.as_deref())
    }
}

impl<'a> TenantViewMut<'a> {
    /// この view の prefix。
    pub fn prefix(&self) -> Option<&str> {
        self.prefix.as_deref()
    }

    /// 新規 table を定義。 prefix が自動付与される (root view なら付与なし)。
    pub fn table<'b>(&'b mut self, name: &str) -> TableBuilder<'b> {
        let full = resolve_prefixed(self.prefix.as_deref(), name);
        self.db.table(&full)
    }

    /// 既存 table を引く (read 系)。 prefix 自動付与。
    pub fn get_table<'b>(&'b self, name: &str) -> Option<Table<'b>> {
        self.db.get_table(&resolve_prefixed(self.prefix.as_deref(), name))
    }

    /// この view scope の table 一覧。
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
    cols: Vec<(String, ColumnType)>,
    pk: Option<String>,
    relations: Vec<(String, String)>, // (from_col, to_table)
    capacity: Option<u32>,
}

impl<'a> TableBuilder<'a> {
    pub fn column(mut self, name: impl Into<String>, ty: ColumnType) -> Self {
        self.cols.push((name.into(), ty));
        self
    }
    /// inline 数値列 (HimoType::Number)。
    pub fn number(self, name: &str) -> Self { self.column(name, ColumnType::Number) }
    /// 共有タグ列 (HimoType::Tag、vocab 経由 / dedupe あり)。
    /// enum / カテゴリ / 名前など、引かれる値に向く。
    pub fn tag(self, name: &str) -> Self { self.column(name, ColumnType::Tag) }
    /// 終端タグ列 (HimoType::Leaf、FreeStore 経由 / dedupe なし)。
    /// 備考・メモ・本文など、引かれない自由記述に向く。
    pub fn leaf(self, name: &str) -> Self { self.column(name, ColumnType::Leaf) }

    /// Ref 型カラムを宣言。 値は他テーブルの EntityId を保持する。
    /// `Table::where_ref` で逆引きできる。 `to_table` 名は build 時に存在チェック。
    pub fn ref_to(mut self, col: &str, to_table: &str) -> Self {
        self.cols.push((col.to_string(), ColumnType::Ref));
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

    pub fn build(self) -> Result<Table<'a>, SchemaError> {
        let TableBuilder { db, name, cols: col_specs, pk, relations, capacity: capacity_hint } = self;

        // 既存 table と同名なら handle を返す (idempotent)
        if let Some(existing) = db.find_table_inner(&name) {
            return Ok(Table { db, inner: existing });
        }

        // PK 検証
        if let Some(pk_name) = &pk {
            if !col_specs.iter().any(|(n, _)| n == pk_name) {
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
        eng_mut.define_table(&name, size_hint)
            .map_err(|e| SchemaError::Internal(format!("define_table({name}) failed: {e}")))?;

        let mut cols = Vec::with_capacity(col_specs.len());
        for (col_name, ty) in &col_specs {
            // ref column は define_ref_in、 それ以外は define_himo_in。
            let ref_target = relations.iter().find(|(c, _)| c == col_name).map(|(_, t)| t.clone());
            match ref_target {
                Some(to_table) => {
                    eng_mut.define_ref_in(&name, col_name, &to_table)
                        .map_err(|e| SchemaError::Internal(format!(
                            "define_ref_in({name}.{col_name} -> {to_table}) failed: {e}"
                        )))?;
                }
                None => {
                    eng_mut.define_himo_in(&name, col_name, ty.himo_type(), 0)
                        .map_err(|e| SchemaError::Internal(format!(
                            "define_himo_in({name}.{col_name}) failed: {e}"
                        )))?;
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

        let inner = Arc::new(TableInner {
            name: name.clone(),
            table_vid,
            cols,
            pk: pk_idx,
            relations: relations.into_iter().map(|(from_col, to_table)| RelationInner { from_col, to_table }).collect(),
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

    /// `WHERE col >= lo AND col <= hi` 範囲 query 開始 (inclusive)。
    pub fn where_range(&self, col: &str, lo: u32, hi: u32) -> Query<'a> {
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

    /// table 内の `col` の合計 (= SUM(col))。 1M rows / M2 Max で ~100µs
    /// (= DuckDB の `SELECT SUM(col) FROM table` の 5-6x 速い)。
    pub fn sum(&self, col: &str) -> u64 {
        let himo = format!("{}.{}", self.inner.name, col);
        let Some((lo, hi)) = self.db.eng.table_eid_range(&self.inner.name) else { return 0; };
        self.db.eng.sum_range(&himo, lo, hi)
    }

    /// table 内の `col` に値が tie された row 数 (= COUNT(col))。
    pub fn count_col(&self, col: &str) -> u32 {
        let himo = format!("{}.{}", self.inner.name, col);
        let Some((lo, hi)) = self.db.eng.table_eid_range(&self.inner.name) else { return 0; };
        self.db.eng.count_range(&himo, lo, hi)
    }

    /// `group` でグループ化した上での `sum` 合計 (= SUM(sum) GROUP BY group)。
    /// 戻り値: `Vec<(group_value, sum_total)>`、 順序は group_value 昇順 (dense cap 経路) or
    /// 任意 (HashMap 経路)。
    pub fn group_sum(&self, group: &str, sum: &str) -> Vec<(u32, u64)> {
        let group_himo = format!("{}.{}", self.inner.name, group);
        let sum_himo = format!("{}.{}", self.inner.name, sum);
        let Some((lo, hi)) = self.db.eng.table_eid_range(&self.inner.name) else { return vec![]; };
        self.db.eng.group_sum_range(&group_himo, &sum_himo, lo, hi)
    }

    // ──── 0.8.8 (#38): min / max / group_min / group_max / histogram ────
    //
    // `sum` と同じ pattern (= table の eid_range を auto-bind して engine の
    // `_range` primitive を呼ぶ)。 stored_slice 直 scan で auto-vectorize する。

    /// table 内の `col` の最小値 (= MIN(col))。 全 missing なら None。
    pub fn min(&self, col: &str) -> Option<u32> {
        let himo = format!("{}.{}", self.inner.name, col);
        let (lo, hi) = self.db.eng.table_eid_range(&self.inner.name)?;
        self.db.eng.min_range(&himo, lo, hi)
    }

    /// table 内の `col` の最大値 (= MAX(col))。 全 missing なら None。
    pub fn max(&self, col: &str) -> Option<u32> {
        let himo = format!("{}.{}", self.inner.name, col);
        let (lo, hi) = self.db.eng.table_eid_range(&self.inner.name)?;
        self.db.eng.max_range(&himo, lo, hi)
    }

    /// `group` でグループ化した上での `val` 最小値 (= MIN(val) GROUP BY group)。
    pub fn group_min(&self, group: &str, val: &str) -> Vec<(u32, u32)> {
        let group_himo = format!("{}.{}", self.inner.name, group);
        let val_himo = format!("{}.{}", self.inner.name, val);
        let Some((lo, hi)) = self.db.eng.table_eid_range(&self.inner.name) else { return vec![]; };
        self.db.eng.group_min_range(&group_himo, &val_himo, lo, hi)
    }

    /// `group` でグループ化した上での `val` 最大値 (= MAX(val) GROUP BY group)。
    pub fn group_max(&self, group: &str, val: &str) -> Vec<(u32, u32)> {
        let group_himo = format!("{}.{}", self.inner.name, group);
        let val_himo = format!("{}.{}", self.inner.name, val);
        let Some((lo, hi)) = self.db.eng.table_eid_range(&self.inner.name) else { return vec![]; };
        self.db.eng.group_max_range(&group_himo, &val_himo, lo, hi)
    }

    /// table 内の `col` の値域 `[vmin, vmax]` を `n_buckets` 等分した頻度
    /// ヒストグラム。 値域外の row はカウント外、 戻り値長は常に `n_buckets`。
    /// `n_buckets == 0` または `vmin > vmax` のときは空 Vec。
    pub fn histogram(&self, col: &str, vmin: u32, vmax: u32, n_buckets: u32) -> Vec<u32> {
        let himo = format!("{}.{}", self.inner.name, col);
        let Some((lo, hi)) = self.db.eng.table_eid_range(&self.inner.name) else {
            return if n_buckets == 0 || vmin > vmax { vec![] } else { vec![0; n_buckets as usize] };
        };
        self.db.eng.histogram_range(&himo, lo, hi, vmin, vmax, n_buckets)
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
        let mut target_eid: Option<EntityId> = None;
        if self.replace_on_pk {
            if let Some(pk_idx) = self.table.pk {
                let pk_col = &self.table.cols[pk_idx];
                let pk_value = resolved.iter()
                    .find(|(c, _)| c.name == pk_col.name)
                    .map(|(_, v)| *v);
                if let Some(pk_v) = pk_value {
                    let pk_raw = value_to_raw_for_query(eng, pk_col, pk_v)?;
                    if pk_raw != u32::MAX {
                        eng.rebuild();
                        // pk_col.himo_id は table 名 prefix 済み (`emp.pk_col`) なので
                        // 他テーブルと衝突しない → marker cond は不要。
                        let found = eng.query_by_id(&[
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

// ─────────────────────────── Query ───────────────────────────

#[derive(Clone, Copy, Debug)]
enum RangeOp { Gt, Ge, Lt, Le }

enum Predicate {
    /// himo_id == u16::MAX は「未知 col / 型不一致 → 結果 0 件」 sentinel。
    Eq(u16, u32),
    EqText(u16, String),
    Range { himo_name: String, lo: u32, hi: u32 },
    Cmp { himo_name: String, op: RangeOp, against: u32 },
    In(u16, Vec<u32>),
}

pub struct Query<'a> {
    db: &'a Database,
    table: Arc<TableInner>,
    preds: Vec<Predicate>,
    limit: Option<usize>,
}

impl<'a> Query<'a> {
    fn new(db: &'a Database, table: Arc<TableInner>) -> Self {
        Self { db, table, preds: Vec::new(), limit: None }
    }

    pub fn where_eq<V: Into<Value>>(mut self, col: &str, val: V) -> Self {
        let v = val.into();
        match self.table.col(col) {
            None => self.preds.push(Predicate::Eq(u16::MAX, u32::MAX)), // unknown col → empty
            Some(cd) => match (cd.ty, v) {
                (ColumnType::Tag, Value::Text(s)) => self.preds.push(Predicate::EqText(cd.himo_id, s)),
                (ColumnType::Number, Value::Number(n)) if n >= 0 && (n as u64) < u32::MAX as u64 => {
                    self.preds.push(Predicate::Eq(cd.himo_id, n as u32));
                }
                (ColumnType::Ref, Value::Ref(eid)) => {
                    self.preds.push(Predicate::Eq(cd.himo_id, eid as u32));
                }
                _ => self.preds.push(Predicate::Eq(u16::MAX, u32::MAX)), // type mismatch → empty
            },
        }
        self
    }

    pub fn where_range(mut self, col: &str, lo: u32, hi: u32) -> Self {
        if let Some(cd) = self.table.col(col) {
            self.preds.push(Predicate::Range {
                himo_name: cd.himo_name.clone(),
                lo, hi,
            });
        } else {
            self.preds.push(Predicate::Eq(u16::MAX, u32::MAX));
        }
        self
    }

    fn push_cmp(&mut self, col: &str, op: RangeOp, against: u32) {
        if let Some(cd) = self.table.col(col) {
            self.preds.push(Predicate::Cmp {
                himo_name: cd.himo_name.clone(),
                op, against,
            });
        }
    }

    pub fn where_gt(mut self, col: &str, against: u32) -> Self { self.push_cmp(col, RangeOp::Gt, against); self }
    pub fn where_ge(mut self, col: &str, against: u32) -> Self { self.push_cmp(col, RangeOp::Ge, against); self }
    pub fn where_lt(mut self, col: &str, against: u32) -> Self { self.push_cmp(col, RangeOp::Lt, against); self }
    pub fn where_le(mut self, col: &str, against: u32) -> Self { self.push_cmp(col, RangeOp::Le, against); self }

    pub fn where_ref(mut self, col: &str, target: EntityId) -> Self {
        if let Some(cd) = self.table.col(col) {
            self.preds.push(Predicate::Eq(cd.himo_id, target as u32));
        }
        self
    }

    pub fn where_in(mut self, col: &str, values: &[u32]) -> Self {
        if let Some(cd) = self.table.col(col) {
            self.preds.push(Predicate::In(cd.himo_id, values.to_vec()));
        }
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

    pub fn find(self) -> Result<Vec<EntityId>, SchemaError> {
        let eng = self.db.engine();
        eng.rebuild();

        // 1. Eq / EqText / In を engine 側 query に折り込む。 Range / Cmp は post-filter。
        // column 名は `{table}.{col}` で prefix されてて他テーブルと共有しない設計
        // (case 1: column 名空間分離)。 marker cond は不要。
        let mut eq_conds: Vec<(u16, u32)> = Vec::with_capacity(self.preds.len());

        let mut in_pred: Option<(u16, Vec<u32>)> = None;
        let mut range_preds: Vec<(String, u32, u32)> = Vec::new();
        let mut cmp_preds: Vec<(String, RangeOp, u32)> = Vec::new();
        let mut empty = false;

        for p in self.preds {
            match p {
                Predicate::Eq(h, _) if h == u16::MAX => { empty = true; }
                Predicate::Eq(h, v) => eq_conds.push((h, v)),
                Predicate::EqText(h, s) => match eng.vocab_id(&s) {
                    Some(vid) => eq_conds.push((h, vid)),
                    None => empty = true,
                },
                Predicate::In(h, vs) => {
                    if in_pred.is_some() {
                        return Err(SchemaError::BadValue("multiple where_in not supported yet".into()));
                    }
                    in_pred = Some((h, vs));
                }
                Predicate::Range { himo_name, lo, hi } => range_preds.push((himo_name, lo, hi)),
                Predicate::Cmp { himo_name, op, against } => cmp_preds.push((himo_name, op, against)),
            }
        }
        if empty { return Ok(Vec::new()); }

        // 2. base candidates。 eq_conds が空 = `.all()` 系 → table 所属を表す
        //    代表 column (PK or first col) で「値が tie された全 entity」 を取る。
        //    case 1 設計では「table の row」 = 「table の column を 1 つ以上 tie してる entity」、
        //    厳密には全 column union だが、 代表 column slice で実用上十分。
        let mut candidates = if eq_conds.is_empty() && in_pred.is_none() {
            let representative_hid = self.table.pk
                .or_else(|| if self.table.cols.is_empty() { None } else { Some(0) })
                .map(|i| self.table.cols[i].himo_id);
            match representative_hid {
                Some(hid) => eng.entities_with_himo(hid),
                None => Vec::new(),
            }
        } else {
            eng.query_by_id(&eq_conds)
        };

        // 3. apply IN as a set-membership filter
        if let Some((h, vs)) = in_pred {
            let in_set = eng.pull_in_by_id(h, &vs);
            // intersect with candidates
            let mut in_sorted = in_set;
            in_sorted.sort_unstable();
            candidates.retain(|e| in_sorted.binary_search(e).is_ok());
        }

        // 4. post-filter range / cmp predicates (Column 直読み)
        if !range_preds.is_empty() || !cmp_preds.is_empty() {
            candidates.retain(|&eid| {
                for (h, lo, hi) in &range_preds {
                    let v = match eng.get(eid, h) { Some(v) => v, None => return false };
                    if v < *lo || v > *hi { return false; }
                }
                for (h, op, against) in &cmp_preds {
                    let v = match eng.get(eid, h) { Some(v) => v, None => return false };
                    let ok = match op {
                        RangeOp::Gt => v > *against,
                        RangeOp::Ge => v >= *against,
                        RangeOp::Lt => v < *against,
                        RangeOp::Le => v <= *against,
                    };
                    if !ok { return false; }
                }
                true
            });
        }

        if let Some(n) = self.limit { candidates.truncate(n); }
        Ok(candidates)
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
            ColumnType::Ref => eng.get(self.eid, &cd.himo_name).map(|v| Value::Ref(v as EntityId)),
            ColumnType::Tag | ColumnType::Leaf => eng.get_text(self.eid, &cd.himo_name)
                .and_then(|b| std::str::from_utf8(b).ok().map(|s| Value::Text(s.to_string()))),
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
fn value_to_raw_for_query(eng: &Engine, cd: &ColumnInner, v: &Value) -> Result<u32, SchemaError> {
    match (cd.ty, v) {
        (_, Value::Null) => Err(SchemaError::BadValue("Null is not a queryable value".into())),
        (ColumnType::Number, Value::Number(n)) => {
            if *n < 0 || (*n as u64) >= u32::MAX as u64 {
                return Err(SchemaError::BadValue(format!("integer out of u32 range: {n}")));
            }
            Ok(*n as u32)
        }
        (ColumnType::Tag, Value::Text(s)) => Ok(eng.vocab_id(s).unwrap_or(u32::MAX)),
        // Leaf は dedupe しないので等値クエリは原理的に成立しない (毎回別 vid)。
        // 念のため `u32::MAX` を返してクエリ結果 0 件にする。
        (ColumnType::Leaf, Value::Text(_)) => Ok(u32::MAX),
        (ColumnType::Ref, Value::Ref(eid)) => Ok(*eid as u32),
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
        (ColumnType::Tag, Value::Text(s)) | (ColumnType::Leaf, Value::Text(s)) => {
            // engine の tie_text_to_by_id は himo の HimoType (Tag / Leaf) を見て
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
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn where_eq_and_chain() {
        let path = tmp("where_eq");
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
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn upsert_replaces_pk_row() {
        let path = tmp("upsert");
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
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn schema_persists_across_reopen() {
        let path = tmp("persist");
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
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn delete_and_update() {
        let path = tmp("del_upd");
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
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn relation_ref_query() {
        let path = tmp("relation");
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
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bindings_extract_himo_id_and_engine_direct_write() {
        // build 時に column 名 → himo_id が pre-resolve されてる。
        // table = 紐の束 declaration なので、 row 識別 marker は存在せず、
        // 抽出した himo_id だけで engine 直叩き write/read が schema find に揃う。
        let path = tmp("bindings");
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
        let _ = std::fs::remove_file(&path_default);
        let _ = std::fs::remove_file(&path_capped);

        {
            let _db = Database::create_growable(&path_default).unwrap();
        }
        {
            let _db = Database::create_growable_with_capacity(&path_capped, 65_536).unwrap();
        }

        let size_default = std::fs::metadata(&path_default).unwrap().len();
        let size_capped = std::fs::metadata(&path_capped).unwrap().len();

        // default は 20 GB 以上、 cap=65k は 2 GB 未満 (=10× 以上の差)
        assert!(size_default > 20 * 1024 * 1024 * 1024, "default apparent = {}", size_default);
        assert!(size_capped < 2 * 1024 * 1024 * 1024, "capped apparent = {}", size_capped);
        assert!(size_default / size_capped > 10);

        let _ = std::fs::remove_file(&path_default);
        let _ = std::fs::remove_file(&path_capped);
    }

    #[test]
    fn finish_with_wal_transitions_to_concurrent() {
        let path = tmp("finish_wal");
        let oplog_path = format!("{}.oplog", path);
        let _ = std::fs::remove_file(&path);
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
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&oplog_path);
    }

    #[test]
    fn open_with_wal_recovers_writes() {
        let path = tmp("open_wal");
        let oplog_path = format!("{}.oplog", path);
        let _ = std::fs::remove_file(&path);
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

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&oplog_path);
    }

    #[test]
    fn leaf_column_basic_write_read() {
        let path = tmp("leaf_basic");
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
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn leaf_does_not_dedupe() {
        // 同じ文字列を複数 entity の Leaf 列に書いた時、vocab_id が別であることを確認。
        // Tag なら同じ vocab_id を共有するが、Leaf は dedupe しない。
        let path = tmp("leaf_no_dedupe");
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
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn tag_dedupes_but_leaf_does_not_in_same_db() {
        // 同じ DB で Tag と Leaf を比較。Tag は dedupe、Leaf は dedupe しない。
        let path = tmp("tag_vs_leaf");
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
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn leaf_column_persists_across_reopen() {
        let path = tmp("leaf_persist");
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
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn enable_sync_creates_reserved_tables() {
        // 0.7.0 Phase 3: enable_sync で _sync_ops / _sync_peers が engine に
        // 登録されること、 user-facing list_tables からは見えないことを確認。
        let path = tmp("sync_enable");
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

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn user_table_starting_with_underscore_rejected() {
        // 0.7.0 Phase 3: `_` 始まり名前は reserved 命名空間、 user 経路は弾く。
        let path = tmp("reserved_reject");
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

        let _ = std::fs::remove_file(&path);
    }
}
