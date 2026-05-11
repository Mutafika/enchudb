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
//!     .integer("id")
//!     .text("name")
//!     .integer("age")
//!     .text("city")
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
//! db.table("companies").integer("id").text("name").primary_key("id").build()?;
//!
//! let users = db.table("users")
//!     .integer("id")
//!     .text("name")
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

use enchudb_engine::{Engine, EntityId, HimoType};
use std::sync::Arc;

const TABLE_MARKER_HIMO: &str = "__enchu_table";
const SCHEMA_MARKER: &str = "__enchu_schema_v1__";
const SCHEMA_BLOB_HIMO: &str = "__enchu_schema_blob";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnType {
    Integer,
    Text,
    Ref,
}

impl ColumnType {
    fn himo_type(self) -> HimoType {
        match self {
            ColumnType::Integer => HimoType::Value,
            ColumnType::Text => HimoType::Symbol,
            ColumnType::Ref => HimoType::Ref,
        }
    }
    fn tag(self) -> &'static str {
        match self {
            ColumnType::Integer => "I",
            ColumnType::Text => "T",
            ColumnType::Ref => "R",
        }
    }
    fn from_tag(s: &str) -> Option<Self> {
        match s {
            "I" => Some(ColumnType::Integer),
            "T" => Some(ColumnType::Text),
            "R" => Some(ColumnType::Ref),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Text(String),
    Ref(EntityId),
}

impl From<i64> for Value { fn from(v: i64) -> Self { Value::Integer(v) } }
impl From<i32> for Value { fn from(v: i32) -> Self { Value::Integer(v as i64) } }
impl From<u32> for Value { fn from(v: u32) -> Self { Value::Integer(v as i64) } }
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
pub struct Database {
    eng: Engine,
    tables: Vec<Arc<TableInner>>,
    marker_himo_id: u16,
}

impl Drop for Database {
    fn drop(&mut self) {
        // INSERT 系は flush しないので、 drop で 1 回 sync。
        // best effort: tmp DB の unlink 後などで失敗するのは想定内。
        let _ = self.eng.flush();
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
    pub fn create_growable_tiny(path: &str) -> Result<Self, SchemaError> {
        let eng = Engine::create_growable_tiny(path).map_err(|e| SchemaError::Io(e.to_string()))?;
        Self::wrap_new(eng)
    }

    fn wrap_new(mut eng: Engine) -> Result<Self, SchemaError> {
        eng.define_himo(TABLE_MARKER_HIMO, HimoType::Symbol, 0);
        eng.define_himo(SCHEMA_BLOB_HIMO, HimoType::Symbol, 0);
        let marker_himo_id = eng.himo_id(TABLE_MARKER_HIMO)
            .ok_or_else(|| SchemaError::Internal("marker himo id".into()))? as u16;
        Ok(Self { eng, tables: Vec::new(), marker_himo_id })
    }

    pub fn open(path: &str) -> Result<Self, SchemaError> {
        let mut eng = Engine::open_standalone(path).map_err(|e| SchemaError::Io(e.to_string()))?;
        // marker は idempotent 再定義 (新規 DB だった場合に備える)
        eng.define_himo(TABLE_MARKER_HIMO, HimoType::Symbol, 0);
        eng.define_himo(SCHEMA_BLOB_HIMO, HimoType::Symbol, 0);
        let marker_himo_id = eng.himo_id(TABLE_MARKER_HIMO)
            .ok_or_else(|| SchemaError::Internal("marker himo id".into()))? as u16;
        let mut db = Self { eng, tables: Vec::new(), marker_himo_id };
        db.load_schema()?;
        Ok(db)
    }

    pub fn engine(&self) -> &Engine { &self.eng }
    pub fn engine_mut(&mut self) -> &mut Engine { &mut self.eng }

    /// 新規 table 定義 builder。
    pub fn table<'a>(&'a mut self, name: &str) -> TableBuilder<'a> {
        TableBuilder {
            db: self,
            name: name.to_string(),
            cols: Vec::new(),
            pk: None,
            relations: Vec::new(),
        }
    }

    /// 既存 table の handle を取得。 未定義なら None。
    pub fn get_table<'a>(&'a self, name: &str) -> Option<Table<'a>> {
        self.tables.iter()
            .find(|t| t.name.eq_ignore_ascii_case(name))
            .map(|t| Table { db: self, inner: t.clone() })
    }

    /// 全 table を列挙。
    pub fn list_tables(&self) -> Vec<TableInfo> {
        self.tables.iter().map(|t| TableInfo {
            name: t.name.clone(),
            columns: t.cols.iter().map(|c| ColumnInfo {
                name: c.name.clone(),
                ty: c.ty,
                is_pk: t.pk.map(|i| t.cols[i].name == c.name).unwrap_or(false),
            }).collect(),
        }).collect()
    }

    fn find_table_inner(&self, name: &str) -> Option<Arc<TableInner>> {
        self.tables.iter()
            .find(|t| t.name.eq_ignore_ascii_case(name))
            .cloned()
    }

    // ────── schema 永続化 ──────

    fn persist_schema(&mut self) -> Result<(), SchemaError> {
        let eid = self.ensure_schema_entity();
        let blob = serialize_schema(&self.tables);
        self.eng.content(eid, SCHEMA_BLOB_HIMO, blob.as_bytes());
        self.eng.flush().map_err(|e| SchemaError::Io(e.to_string()))?;
        Ok(())
    }

    fn ensure_schema_entity(&mut self) -> EntityId {
        self.eng.rebuild();
        if let Some(vid) = self.eng.vocab_id(SCHEMA_MARKER) {
            let eids = self.eng.pull_raw(TABLE_MARKER_HIMO, vid);
            if let Some(&eid) = eids.first() { return eid; }
        }
        let eid = self.eng.entity();
        self.eng.tie_text(eid, TABLE_MARKER_HIMO, SCHEMA_MARKER);
        eid
    }

    fn load_schema(&mut self) -> Result<(), SchemaError> {
        let Some(vid) = self.eng.vocab_id(SCHEMA_MARKER) else { return Ok(()); };
        self.eng.rebuild();
        let eids = self.eng.pull_raw(TABLE_MARKER_HIMO, vid);
        let Some(&eid) = eids.first() else { return Ok(()); };
        let blob = match self.eng.get_content(eid, SCHEMA_BLOB_HIMO) {
            Some(b) => b.to_vec(),
            None => return Ok(()),
        };
        let s = std::str::from_utf8(&blob)
            .map_err(|_| SchemaError::Parse("schema blob not utf8".into()))?;
        let parsed = deserialize_schema(s)?;

        for raw in parsed {
            // himo を再 define + himo_id を解決
            let mut cols = Vec::with_capacity(raw.cols.len());
            for (name, ty) in raw.cols {
                let himo_name = format!("{}.{}", raw.name, name);
                self.eng.define_himo(&himo_name, ty.himo_type(), 0);
                let hid = self.eng.himo_id(&himo_name)
                    .ok_or_else(|| SchemaError::Internal(format!("himo {himo_name} after define")))?;
                cols.push(ColumnInner { name, ty, himo_name, himo_id: hid as u16 });
            }
            let pk = raw.pk.and_then(|n| cols.iter().position(|c| c.name == n));
            // table_vid: 名前を Vocabulary に注入する (新規 vocab insert)
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

    fn intern_table_name(&mut self, name: &str) -> u32 {
        // 既存ならそれを返す、 なければ marker himo 経由で vocab に入れる。
        if let Some(vid) = self.eng.vocab_id(name) { return vid; }
        // ダミー entity に tie して vocab を作る。 build の最後で本物の row を tie するので
        // この dummy は table の最初の row として永続。 ただし schema 永続化用の特別 row
        // (schema entity) とは別。 ここでは vocab を populate するためだけに登録した
        // 後で消す temp entity を使う。
        let tmp = self.eng.entity();
        self.eng.tie_text(tmp, TABLE_MARKER_HIMO, name);
        let vid = self.eng.vocab_id(name)
            .expect("vocab_id should exist after tie_text");
        self.eng.delete(tmp);
        vid
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
}

// ─────────────────────────── TableBuilder ───────────────────────────

pub struct TableBuilder<'a> {
    db: &'a mut Database,
    name: String,
    cols: Vec<(String, ColumnType)>,
    pk: Option<String>,
    relations: Vec<(String, String)>, // (from_col, to_table)
}

impl<'a> TableBuilder<'a> {
    pub fn column(mut self, name: impl Into<String>, ty: ColumnType) -> Self {
        self.cols.push((name.into(), ty));
        self
    }
    pub fn integer(self, name: &str) -> Self { self.column(name, ColumnType::Integer) }
    pub fn text(self, name: &str) -> Self { self.column(name, ColumnType::Text) }

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

    pub fn build(self) -> Result<Table<'a>, SchemaError> {
        let TableBuilder { db, name, cols: col_specs, pk, relations } = self;

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

        // relation 先 table の存在チェック
        for (_, to_table) in &relations {
            if db.find_table_inner(to_table).is_none() {
                return Err(SchemaError::UnknownTable(to_table.clone()));
            }
        }

        // himo を define
        let mut cols = Vec::with_capacity(col_specs.len());
        for (col_name, ty) in &col_specs {
            let himo_name = format!("{}.{}", name, col_name);
            db.eng.define_himo(&himo_name, ty.himo_type(), 0);
            let hid = db.eng.himo_id(&himo_name)
                .ok_or_else(|| SchemaError::Internal(format!("himo {himo_name} after define")))?;
            cols.push(ColumnInner {
                name: col_name.clone(),
                ty: *ty,
                himo_name,
                himo_id: hid as u16,
            });
        }
        let pk_idx = pk.as_ref().and_then(|n| cols.iter().position(|c| &c.name == n));

        // table_vid を intern
        let table_vid = db.intern_table_name(&name);

        let inner = Arc::new(TableInner {
            name: name.clone(),
            table_vid,
            cols,
            pk: pk_idx,
            relations: relations.into_iter().map(|(from_col, to_table)| RelationInner { from_col, to_table }).collect(),
        });
        db.tables.push(inner.clone());
        db.persist_schema()?;

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
        }).collect()
    }

    /// 新規 row insert builder。
    pub fn insert(&self) -> RowBuilder<'a> {
        RowBuilder {
            db: self.db,
            table: self.inner.clone(),
            marker_himo_id: self.db.marker_himo_id,
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
}

// ─────────────────────────── RowBuilder ───────────────────────────

pub struct RowBuilder<'a> {
    db: &'a Database,
    table: Arc<TableInner>,
    marker_himo_id: u16,
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
                        let found = eng.query_by_id(&[
                            (self.marker_himo_id, self.table.table_vid),
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
                let e = eng.entity();
                eng.tie_text_to(e, TABLE_MARKER_HIMO, &self.table.name);
                e
            }
        };

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
                (ColumnType::Text, Value::Text(s)) => self.preds.push(Predicate::EqText(cd.himo_id, s)),
                (ColumnType::Integer, Value::Integer(n)) if n >= 0 && (n as u64) < u32::MAX as u64 => {
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
        let mut eq_conds: Vec<(u16, u32)> = Vec::with_capacity(self.preds.len() + 1);
        eq_conds.push((self.db.marker_himo_id, self.table.table_vid));

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

        // 2. base candidates: AND of eq_conds (always includes the table marker)
        let mut candidates = eng.query_by_id(&eq_conds);

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
            ColumnType::Integer => eng.get(self.eid, &cd.himo_name).map(|v| Value::Integer(v as i64)),
            ColumnType::Ref => eng.get(self.eid, &cd.himo_name).map(|v| Value::Ref(v as EntityId)),
            ColumnType::Text => eng.get_text(self.eid, &cd.himo_name)
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
        (ColumnType::Integer, Value::Integer(n)) => {
            if *n < 0 || (*n as u64) >= u32::MAX as u64 {
                return Err(SchemaError::BadValue(format!("integer out of u32 range: {n}")));
            }
            Ok(*n as u32)
        }
        (ColumnType::Text, Value::Text(s)) => Ok(eng.vocab_id(s).unwrap_or(u32::MAX)),
        (ColumnType::Ref, Value::Ref(eid)) => Ok(*eid as u32),
        (t, v) => Err(SchemaError::TypeMismatch(format!("{t:?} vs {v:?}"))),
    }
}

/// 書き込み (tie) 用の型 dispatch。 Text は str をそのまま vocab に流す。
fn tie_value(eng: &Engine, eid: EntityId, cd: &ColumnInner, v: &Value) -> Result<(), SchemaError> {
    match (cd.ty, v) {
        (_, Value::Null) => Err(SchemaError::BadValue("Null tie not supported (use entity.delete or untie)".into())),
        (ColumnType::Integer, Value::Integer(n)) => {
            if *n < 0 || (*n as u64) >= u32::MAX as u64 {
                return Err(SchemaError::BadValue(format!("integer out of u32 range: {n}")));
            }
            eng.tie_to(eid, &cd.himo_name, *n as u32);
            Ok(())
        }
        (ColumnType::Text, Value::Text(s)) => {
            eng.tie_text_to(eid, &cd.himo_name, s);
            Ok(())
        }
        (ColumnType::Ref, Value::Ref(t)) => {
            eng.tie_ref_to(eid, &cd.himo_name, *t);
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
                .integer("id")
                .text("name")
                .integer("age")
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
            assert_eq!(users.entity(alice).get("age"), Some(Value::Integer(30)));
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
                .integer("id")
                .text("name")
                .integer("age")
                .text("city")
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
                .text("key")
                .integer("ts")
                .primary_key("key")
                .build()
                .unwrap();
            let _ = t.upsert().set("key", "k1").set("ts", 100i64).commit().unwrap();
            let _ = t.upsert().set("key", "k1").set("ts", 200i64).commit().unwrap();
            let rows = t.where_eq("key", "k1").find().unwrap();
            assert_eq!(rows.len(), 1);
            let ts = t.entity(rows[0]).get("ts");
            assert_eq!(ts, Some(Value::Integer(200)));
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn schema_persists_across_reopen() {
        let path = tmp("persist");
        let _ = std::fs::remove_file(&path);
        {
            let mut db = Database::create(&path).unwrap();
            let users = db.table("users").integer("id").text("name").primary_key("id").build().unwrap();
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
                .integer("id")
                .text("body")
                .integer("ts")
                .primary_key("id")
                .build().unwrap();
            let n1 = t.insert().set("id", 1i64).set("body", "hi").set("ts", 100i64).commit().unwrap();
            let _n2 = t.insert().set("id", 2i64).set("body", "yo").set("ts", 200i64).commit().unwrap();

            t.entity(n1).set("ts", 150i64).commit().unwrap();
            assert_eq!(t.entity(n1).get("ts"), Some(Value::Integer(150)));

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
                let companies = db.table("companies").integer("id").text("name").primary_key("id").build().unwrap();
                let a = companies.insert().set("id", 1i64).set("name", "Anthropic").commit().unwrap();
                let g = companies.insert().set("id", 2i64).set("name", "Google").commit().unwrap();
                (a, g)
            }; // companies の借用がここで終わる

            let users = db.table("users")
                .integer("id")
                .text("name")
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
}
