//! EnchuDB SQL frontend — SQLite の上位互換 (superset) を狙う。
//!
//! 設計方針:
//! - パーサ → engine メソッド直 dispatch。中間 AST 評価層は作らない (速度第一)。
//! - SQL は薄い syntactic sugar。`SELECT * WHERE pk=?` は `eng.pull_raw(...)` に 1:1 で訳す。
//! - SQLite のエッジケース挙動は完全模倣しない。意味的に同じ or より良い結果が返ればよい。
//! - DSL (`query_lang`) は残して "advanced query" として共存。集計は DSL を使う。
//!
//! ## v0 サポート
//!
//! ```sql
//! CREATE TABLE t (col TYPE [PRIMARY KEY], ...)
//! INSERT INTO t [(col, ...)] VALUES (...) [, (...)]
//! INSERT OR REPLACE INTO t [(col, ...)] VALUES (...)
//! SELECT [* | col, ...] FROM t [WHERE col = val [AND col = val]*]
//! UPDATE t SET col = val [, col = val]* WHERE col = val
//! DELETE FROM t WHERE col = val
//! ```
//!
//! TYPE は `INTEGER` / `TEXT` のみ。INTEGER は u32、TEXT は Symbol himo。
//!
//! ## 未対応 (今後)
//! - JOIN / subquery
//! - 集計 (`SUM`, `COUNT`, `GROUP BY`) — `query_lang` DSL を使う
//! - `ORDER BY` / `LIMIT`
//! - 範囲比較 (`>`, `<`, `BETWEEN`)
//! - 複数カラム PK
//! - `DEFAULT` / `NOT NULL` / `CHECK` 制約
//! - スキーマ永続化 (現状は in-memory、reopen 時に CREATE TABLE 再呼び出しが必要、`define_himo` は idempotent)
//!
//! ## 使用例
//!
//! ```rust,no_run
//! use enchudb_sql::{Database, Output, Value};
//! let mut db = Database::create("/tmp/notif.db").unwrap();
//! db.execute("CREATE TABLE notif (key TEXT PRIMARY KEY, dismissed_at INTEGER)").unwrap();
//! db.execute("INSERT INTO notif VALUES ('uuid-abc', 1715174400)").unwrap();
//! match db.execute("SELECT * FROM notif WHERE key = 'uuid-abc'").unwrap() {
//!     Output::Rows { rows, .. } => assert_eq!(rows.len(), 1),
//!     _ => panic!(),
//! }
//! ```

use enchudb_engine::{Engine, EntityId, HimoType};
use sqlparser::ast::{
    self, BinaryOperator, ColumnOption, DataType, Expr, ObjectName, Query, SelectItem, SetExpr,
    Statement, TableFactor, Value as SqlValue,
};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

const TABLE_MARKER_HIMO: &str = "__sql_table";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlType {
    Integer,
    Text,
}

#[derive(Debug, Clone)]
struct ColDef {
    name: String,
    ty: SqlType,
    himo: String,
}

#[derive(Debug, Clone)]
struct TableDef {
    name: String,
    cols: Vec<ColDef>,
    pk: Option<String>,
}

impl TableDef {
    fn col(&self, name: &str) -> Option<&ColDef> {
        self.cols.iter().find(|c| c.name.eq_ignore_ascii_case(name))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Text(String),
}

#[derive(Debug)]
pub enum Output {
    Created,
    Inserted(usize),
    Updated(usize),
    Deleted(usize),
    Rows { columns: Vec<String>, rows: Vec<Vec<Value>> },
}

#[derive(Debug)]
pub enum SqlError {
    Parse(String),
    Unsupported(String),
    UnknownTable(String),
    UnknownColumn(String),
    TypeMismatch(String),
    BadValue(String),
    DuplicatePk,
    Io(String),
}

impl std::fmt::Display for SqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SqlError::Parse(s) => write!(f, "parse: {s}"),
            SqlError::Unsupported(s) => write!(f, "unsupported: {s}"),
            SqlError::UnknownTable(s) => write!(f, "unknown table: {s}"),
            SqlError::UnknownColumn(s) => write!(f, "unknown column: {s}"),
            SqlError::TypeMismatch(s) => write!(f, "type mismatch: {s}"),
            SqlError::BadValue(s) => write!(f, "bad value: {s}"),
            SqlError::DuplicatePk => write!(f, "duplicate primary key"),
            SqlError::Io(s) => write!(f, "io: {s}"),
        }
    }
}

impl std::error::Error for SqlError {}

pub struct Database {
    eng: Engine,
    tables: Vec<TableDef>,
}

impl Database {
    pub fn create(path: &str) -> Result<Self, SqlError> {
        let eng = Engine::create_standalone(path).map_err(|e| SqlError::Io(e.to_string()))?;
        Ok(Self { eng, tables: Vec::new() })
    }

    /// Smaller mmap footprint variant for state-log style use cases.
    /// `Engine::create_compact` allocates ~32 MB of regions instead of
    /// the default GB-scale layout — large enough for tens of thousands
    /// of rows but doesn't make the file appear like a sparse 88 GB
    /// monster to backup tools (Time Machine, rsync without `--sparse`,
    /// etc.). Use this for app config / state DBs rather than data
    /// warehouses.
    pub fn create_compact(path: &str) -> Result<Self, SqlError> {
        let eng = Engine::create_compact(path).map_err(|e| SqlError::Io(e.to_string()))?;
        Ok(Self { eng, tables: Vec::new() })
    }

    /// Growable backing variant. Pre-commits the whole layout at create
    /// time today (file size matches `create_standalone`), but the
    /// underlying `Engine::create_growable` plumbs grow-on-write through
    /// Region — the next layout reorg can shrink the initial commit
    /// without breaking the API. Prefer this for app state DBs that
    /// will eventually want the lazy file-size behaviour.
    pub fn create_growable(path: &str) -> Result<Self, SqlError> {
        let eng = Engine::create_growable(path).map_err(|e| SqlError::Io(e.to_string()))?;
        Ok(Self { eng, tables: Vec::new() })
    }

    /// Tiny growable preset for app state-logs. Caps at 1024 rows /
    /// 16 himos / 64 KB per data section → layout total ≈ 250 KB.
    /// This is the right default for "a few hundred rows of
    /// dismissed-key / seen-at" stores (matcha's notif state, etc.).
    /// The file appears at ~250 KB on disk, not the 305 MB that the
    /// regular `create_compact` produces.
    pub fn create_growable_tiny(path: &str) -> Result<Self, SqlError> {
        let eng = Engine::create_growable_tiny(path).map_err(|e| SqlError::Io(e.to_string()))?;
        Ok(Self { eng, tables: Vec::new() })
    }

    pub fn open(path: &str) -> Result<Self, SqlError> {
        let eng = Engine::open_standalone(path).map_err(|e| SqlError::Io(e.to_string()))?;
        Ok(Self { eng, tables: Vec::new() })
    }

    pub fn engine(&self) -> &Engine { &self.eng }
    pub fn engine_mut(&mut self) -> &mut Engine { &mut self.eng }

    pub fn execute(&mut self, sql: &str) -> Result<Output, SqlError> {
        let dialect = SQLiteDialect {};
        let stmts = Parser::parse_sql(&dialect, sql).map_err(|e| SqlError::Parse(e.to_string()))?;
        if stmts.is_empty() { return Err(SqlError::Parse("empty".into())); }
        let mut last: Output = Output::Created;
        for stmt in stmts {
            last = self.exec_stmt(stmt)?;
        }
        Ok(last)
    }

    fn exec_stmt(&mut self, stmt: Statement) -> Result<Output, SqlError> {
        match stmt {
            Statement::CreateTable(ct) => self.exec_create(ct),
            Statement::Insert(ins) => self.exec_insert(ins),
            Statement::Query(q) => self.exec_select(*q),
            Statement::Update { table, assignments, selection, .. } => {
                self.exec_update(table, assignments, selection)
            }
            Statement::Delete(del) => self.exec_delete(del),
            other => Err(SqlError::Unsupported(format!("statement: {other}"))),
        }
    }

    // ────── CREATE TABLE ──────

    fn exec_create(&mut self, ct: ast::CreateTable) -> Result<Output, SqlError> {
        let name = obj_name(&ct.name)?;
        if self.find_table(&name).is_some() {
            // idempotent: 同名で同 schema なら no-op、違うなら error
            // v0 は単純に Created を返す (再 open 時の CREATE TABLE 用)
            return Ok(Output::Created);
        }

        let mut cols = Vec::new();
        let mut pk: Option<String> = None;
        for c in &ct.columns {
            let col_name = c.name.value.clone();
            let ty = match &c.data_type {
                DataType::Int(_) | DataType::Integer(_) | DataType::BigInt(_)
                | DataType::SmallInt(_) | DataType::TinyInt(_) | DataType::UnsignedInt(_)
                | DataType::UnsignedBigInt(_) | DataType::UnsignedInteger(_) => SqlType::Integer,
                DataType::Text | DataType::String(_) | DataType::Varchar(_)
                | DataType::Char(_) | DataType::CharacterVarying(_) => SqlType::Text,
                other => return Err(SqlError::Unsupported(format!("type: {other:?}"))),
            };

            for opt in &c.options {
                if let ColumnOption::Unique { is_primary: true, .. } = opt.option {
                    if pk.is_some() {
                        return Err(SqlError::Unsupported("multiple PK columns".into()));
                    }
                    pk = Some(col_name.clone());
                }
            }

            cols.push(ColDef {
                himo: format!("{name}.{col_name}"),
                name: col_name,
                ty,
            });
        }

        // table-level PRIMARY KEY (col) 構文も拾う
        for c in &ct.constraints {
            if let ast::TableConstraint::PrimaryKey { columns, .. } = c {
                if columns.len() != 1 {
                    return Err(SqlError::Unsupported("multi-column PK".into()));
                }
                if pk.is_some() {
                    return Err(SqlError::Unsupported("multiple PK columns".into()));
                }
                pk = Some(columns[0].value.clone());
            }
        }

        // himo を define
        self.eng.define_himo(TABLE_MARKER_HIMO, HimoType::Symbol, 0);
        for col in &cols {
            let ht = match col.ty {
                SqlType::Integer => HimoType::Value,
                SqlType::Text => HimoType::Symbol,
            };
            self.eng.define_himo(&col.himo, ht, 0);
        }

        self.tables.push(TableDef { name, cols, pk });
        Ok(Output::Created)
    }

    // ────── INSERT ──────

    fn exec_insert(&mut self, ins: ast::Insert) -> Result<Output, SqlError> {
        let table_name = obj_name(&ins.table_name)?;
        let table = self.find_table(&table_name).ok_or(SqlError::UnknownTable(table_name.clone()))?.clone();

        let or_replace = matches!(ins.or, Some(ast::SqliteOnConflict::Replace));

        // カラム順を決める
        let col_order: Vec<&ColDef> = if ins.columns.is_empty() {
            table.cols.iter().collect()
        } else {
            let mut out = Vec::with_capacity(ins.columns.len());
            for c in &ins.columns {
                let cd = table.col(&c.value).ok_or_else(|| SqlError::UnknownColumn(c.value.clone()))?;
                out.push(cd);
            }
            out
        };

        let source = ins.source.as_ref().ok_or_else(|| SqlError::Unsupported("INSERT without VALUES".into()))?;
        let rows = match &*source.body {
            SetExpr::Values(v) => &v.rows,
            other => return Err(SqlError::Unsupported(format!("INSERT source: {other:?}"))),
        };

        let mut count = 0usize;
        for row_exprs in rows {
            if row_exprs.len() != col_order.len() {
                return Err(SqlError::BadValue(format!("expected {} values, got {}", col_order.len(), row_exprs.len())));
            }
            let mut values: Vec<(&ColDef, Value)> = Vec::with_capacity(col_order.len());
            for (cd, e) in col_order.iter().zip(row_exprs.iter()) {
                values.push((cd, eval_literal(e)?));
            }

            // INSERT OR REPLACE: PK 一致行があれば置換
            let mut target_eid: Option<EntityId> = None;
            if or_replace {
                if let Some(pk_name) = &table.pk {
                    let pk_val = values.iter().find(|(c, _)| c.name == *pk_name).map(|(_, v)| v.clone());
                    if let Some(pkv) = pk_val {
                        target_eid = self.find_by_pk(&table, pk_name, &pkv)?;
                    }
                }
            }

            let eid = match target_eid {
                Some(e) => e,
                None => {
                    let e = self.eng.entity();
                    self.eng.tie_text(e, TABLE_MARKER_HIMO, &table.name);
                    e
                }
            };

            for (cd, v) in &values {
                tie_value(&mut self.eng, eid, cd, v)?;
            }
            count += 1;
        }
        Ok(Output::Inserted(count))
    }

    // ────── SELECT ──────

    fn exec_select(&mut self, q: Query) -> Result<Output, SqlError> {
        let select = match *q.body {
            SetExpr::Select(s) => s,
            other => return Err(SqlError::Unsupported(format!("SELECT body: {other:?}"))),
        };

        if select.from.len() != 1 {
            return Err(SqlError::Unsupported("FROM with 0 or multiple tables".into()));
        }
        let table_name = match &select.from[0].relation {
            TableFactor::Table { name, .. } => obj_name(name)?,
            other => return Err(SqlError::Unsupported(format!("FROM: {other:?}"))),
        };
        let table = self.find_table(&table_name).ok_or(SqlError::UnknownTable(table_name.clone()))?.clone();

        let projection = resolve_projection(&select.projection, &table)?;

        let eids = self.eval_where(&table, select.selection.as_ref())?;

        let mut rows = Vec::with_capacity(eids.len());
        for eid in eids {
            let mut row = Vec::with_capacity(projection.len());
            for cd in &projection {
                row.push(read_value(&self.eng, eid, cd));
            }
            rows.push(row);
        }
        let columns = projection.iter().map(|c| c.name.clone()).collect();
        Ok(Output::Rows { columns, rows })
    }

    // ────── UPDATE ──────

    fn exec_update(
        &mut self,
        table: ast::TableWithJoins,
        assignments: Vec<ast::Assignment>,
        selection: Option<Expr>,
    ) -> Result<Output, SqlError> {
        let table_name = match &table.relation {
            TableFactor::Table { name, .. } => obj_name(name)?,
            other => return Err(SqlError::Unsupported(format!("UPDATE FROM: {other:?}"))),
        };
        let tdef = self.find_table(&table_name).ok_or(SqlError::UnknownTable(table_name.clone()))?.clone();

        let mut sets: Vec<(&ColDef, Value)> = Vec::with_capacity(assignments.len());
        for a in &assignments {
            let col_name = match &a.target {
                ast::AssignmentTarget::ColumnName(n) => obj_name(n)?,
                other => return Err(SqlError::Unsupported(format!("UPDATE target: {other:?}"))),
            };
            let cd = tdef.col(&col_name).ok_or_else(|| SqlError::UnknownColumn(col_name.clone()))?;
            sets.push((cd, eval_literal(&a.value)?));
        }

        let eids = self.eval_where(&tdef, selection.as_ref())?;
        for eid in &eids {
            for (cd, v) in &sets {
                tie_value(&mut self.eng, *eid, cd, v)?;
            }
        }
        Ok(Output::Updated(eids.len()))
    }

    // ────── DELETE ──────

    fn exec_delete(&mut self, del: ast::Delete) -> Result<Output, SqlError> {
        let from: &Vec<ast::TableWithJoins> = match &del.from {
            ast::FromTable::WithFromKeyword(t) | ast::FromTable::WithoutKeyword(t) => t,
        };
        if from.len() != 1 {
            return Err(SqlError::Unsupported("DELETE FROM with 0 or multiple tables".into()));
        }
        let table_name = match &from[0].relation {
            TableFactor::Table { name, .. } => obj_name(name)?,
            other => return Err(SqlError::Unsupported(format!("DELETE FROM: {other:?}"))),
        };
        let tdef = self.find_table(&table_name).ok_or(SqlError::UnknownTable(table_name.clone()))?.clone();

        let eids = self.eval_where(&tdef, del.selection.as_ref())?;
        for eid in &eids { self.eng.delete(*eid); }
        Ok(Output::Deleted(eids.len()))
    }

    // ────── helpers ──────

    fn find_table(&self, name: &str) -> Option<&TableDef> {
        self.tables.iter().find(|t| t.name.eq_ignore_ascii_case(name))
    }

    fn eval_where(&self, table: &TableDef, sel: Option<&Expr>) -> Result<Vec<EntityId>, SqlError> {
        // WHERE 無し → テーブル内 全行
        let table_vid = match self.eng.vocab_id(&table.name) {
            Some(v) => v,
            None => return Ok(Vec::new()),
        };
        self.eng.rebuild();

        let preds = match sel {
            None => Vec::new(),
            Some(e) => collect_eq_preds(table, e)?,
        };

        // build query: __sql_table = <table_vid> AND <preds>
        let mut q: Vec<(String, u32)> = Vec::with_capacity(preds.len() + 1);
        q.push((TABLE_MARKER_HIMO.to_string(), table_vid));
        for (cd, v) in &preds {
            let raw = match (cd.ty, v) {
                (SqlType::Integer, Value::Integer(n)) => {
                    if *n < 0 || *n >= u32::MAX as i64 {
                        return Err(SqlError::BadValue(format!("integer out of u32 range: {n}")));
                    }
                    *n as u32
                }
                (SqlType::Text, Value::Text(s)) => match self.eng.vocab_id(s) {
                    Some(id) => id,
                    None => return Ok(Vec::new()), // 未知 vocab はマッチなし
                },
                (_, Value::Null) => return Ok(Vec::new()),
                (t, v) => return Err(SqlError::TypeMismatch(format!("{:?} vs {:?}", t, v))),
            };
            q.push((cd.himo.clone(), raw));
        }
        let refs: Vec<(&str, u32)> = q.iter().map(|(h, v)| (h.as_str(), *v)).collect();
        Ok(self.eng.query(&refs))
    }

    fn find_by_pk(&self, table: &TableDef, pk_name: &str, val: &Value) -> Result<Option<EntityId>, SqlError> {
        let cd = table.col(pk_name).ok_or_else(|| SqlError::UnknownColumn(pk_name.to_string()))?;
        let table_vid = match self.eng.vocab_id(&table.name) { Some(v) => v, None => return Ok(None) };
        self.eng.rebuild();
        let raw = match (cd.ty, val) {
            (SqlType::Integer, Value::Integer(n)) => {
                if *n < 0 || *n >= u32::MAX as i64 {
                    return Err(SqlError::BadValue(format!("integer out of u32 range: {n}")));
                }
                *n as u32
            }
            (SqlType::Text, Value::Text(s)) => match self.eng.vocab_id(s) {
                Some(id) => id,
                None => return Ok(None),
            },
            (_, Value::Null) => return Ok(None),
            (t, v) => return Err(SqlError::TypeMismatch(format!("{:?} vs {:?}", t, v))),
        };
        let result = self.eng.query(&[(TABLE_MARKER_HIMO, table_vid), (cd.himo.as_str(), raw)]);
        Ok(result.into_iter().next())
    }
}

// ────── free helpers ──────

fn obj_name(name: &ObjectName) -> Result<String, SqlError> {
    if name.0.len() != 1 {
        return Err(SqlError::Unsupported(format!("qualified name: {name}")));
    }
    Ok(name.0[0].value.clone())
}

fn resolve_projection(items: &[SelectItem], table: &TableDef) -> Result<Vec<ColDef>, SqlError> {
    let mut out = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard(_) => {
                for c in &table.cols { out.push(c.clone()); }
            }
            SelectItem::UnnamedExpr(Expr::Identifier(id)) => {
                let cd = table.col(&id.value).ok_or_else(|| SqlError::UnknownColumn(id.value.clone()))?;
                out.push(cd.clone());
            }
            other => return Err(SqlError::Unsupported(format!("projection: {other:?}"))),
        }
    }
    Ok(out)
}

fn collect_eq_preds<'a>(table: &'a TableDef, expr: &Expr) -> Result<Vec<(&'a ColDef, Value)>, SqlError> {
    let mut out = Vec::new();
    walk_and(table, expr, &mut out)?;
    Ok(out)
}

fn walk_and<'a>(table: &'a TableDef, expr: &Expr, out: &mut Vec<(&'a ColDef, Value)>) -> Result<(), SqlError> {
    match expr {
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            walk_and(table, left, out)?;
            walk_and(table, right, out)?;
            Ok(())
        }
        Expr::BinaryOp { left, op: BinaryOperator::Eq, right } => {
            let (col, val) = match (left.as_ref(), right.as_ref()) {
                (Expr::Identifier(id), v) => (&id.value, v),
                (v, Expr::Identifier(id)) => (&id.value, v),
                _ => return Err(SqlError::Unsupported(format!("WHERE: {expr}"))),
            };
            let cd = table.col(col).ok_or_else(|| SqlError::UnknownColumn(col.clone()))?;
            out.push((cd, eval_literal(val)?));
            Ok(())
        }
        Expr::Nested(inner) => walk_and(table, inner, out),
        other => Err(SqlError::Unsupported(format!("WHERE shape: {other}"))),
    }
}

fn eval_literal(e: &Expr) -> Result<Value, SqlError> {
    match e {
        Expr::Value(value) => match value {
            SqlValue::Number(n, _) => {
                let parsed = n.parse::<i64>().map_err(|_| SqlError::BadValue(format!("number: {n}")))?;
                Ok(Value::Integer(parsed))
            }
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => Ok(Value::Text(s.clone())),
            SqlValue::Null => Ok(Value::Null),
            SqlValue::Boolean(b) => Ok(Value::Integer(if *b { 1 } else { 0 })),
            other => Err(SqlError::Unsupported(format!("literal: {other:?}"))),
        },
        Expr::UnaryOp { op: ast::UnaryOperator::Minus, expr } => match eval_literal(expr)? {
            Value::Integer(n) => Ok(Value::Integer(-n)),
            other => Err(SqlError::TypeMismatch(format!("unary minus on {other:?}"))),
        },
        other => Err(SqlError::Unsupported(format!("expr: {other}"))),
    }
}

fn tie_value(eng: &mut Engine, eid: EntityId, cd: &ColDef, v: &Value) -> Result<(), SqlError> {
    match (cd.ty, v) {
        (SqlType::Integer, Value::Integer(n)) => {
            if *n < 0 || *n >= u32::MAX as i64 {
                return Err(SqlError::BadValue(format!("integer out of u32 range: {n}")));
            }
            eng.tie(eid, &cd.himo, *n as u32);
        }
        (SqlType::Text, Value::Text(s)) => eng.tie_text(eid, &cd.himo, s),
        (_, Value::Null) => { /* untie でも良いが v0 は no-op */ }
        (t, v) => return Err(SqlError::TypeMismatch(format!("col {} expects {:?}, got {:?}", cd.name, t, v))),
    }
    Ok(())
}

fn read_value(eng: &Engine, eid: EntityId, cd: &ColDef) -> Value {
    match cd.ty {
        SqlType::Integer => match eng.get(eid, &cd.himo) {
            Some(n) => Value::Integer(n as i64),
            None => Value::Null,
        },
        SqlType::Text => match eng.get_text(eid, &cd.himo) {
            Some(b) => match std::str::from_utf8(b) {
                Ok(s) => Value::Text(s.to_string()),
                Err(_) => Value::Null,
            },
            None => Value::Null,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh(name: &str) -> Database {
        let path = format!("/tmp/enchudb_sql_{name}.db");
        let _ = std::fs::remove_file(&path);
        Database::create(&path).unwrap()
    }

    #[test]
    fn create_and_insert_select() {
        let mut db = fresh("create_insert");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
        match db.execute("SELECT id, name FROM t WHERE id = 1").unwrap() {
            Output::Rows { rows, columns } => {
                assert_eq!(columns, vec!["id", "name"]);
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Integer(1));
                assert_eq!(rows[0][1], Value::Text("alice".into()));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn select_star() {
        let mut db = fresh("select_star");
        db.execute("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
        match db.execute("SELECT * FROM t").unwrap() {
            Output::Rows { rows, columns } => {
                assert_eq!(columns, vec!["id", "name"]);
                assert_eq!(rows.len(), 3);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn select_with_text_eq() {
        let mut db = fresh("select_text");
        db.execute("CREATE TABLE t (id INTEGER, name TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')").unwrap();
        match db.execute("SELECT id FROM t WHERE name = 'bob'").unwrap() {
            Output::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Integer(2));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn select_and_predicates() {
        let mut db = fresh("select_and");
        db.execute("CREATE TABLE t (id INTEGER, dept INTEGER, name TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 0, 'a'), (2, 1, 'b'), (3, 0, 'c')").unwrap();
        match db.execute("SELECT id FROM t WHERE dept = 0 AND name = 'a'").unwrap() {
            Output::Rows { rows, .. } => assert_eq!(rows.len(), 1),
            _ => panic!(),
        }
    }

    #[test]
    fn update_by_pk() {
        let mut db = fresh("update");
        db.execute("CREATE TABLE notif (key TEXT PRIMARY KEY, dismissed_at INTEGER)").unwrap();
        db.execute("INSERT INTO notif VALUES ('uuid-abc', 100)").unwrap();
        db.execute("UPDATE notif SET dismissed_at = 200 WHERE key = 'uuid-abc'").unwrap();
        match db.execute("SELECT dismissed_at FROM notif WHERE key = 'uuid-abc'").unwrap() {
            Output::Rows { rows, .. } => assert_eq!(rows[0][0], Value::Integer(200)),
            _ => panic!(),
        }
    }

    #[test]
    fn delete_by_pk() {
        let mut db = fresh("delete");
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')").unwrap();
        match db.execute("DELETE FROM t WHERE id = 1").unwrap() {
            Output::Deleted(1) => {}
            other => panic!("got {other:?}"),
        }
        match db.execute("SELECT id FROM t").unwrap() {
            Output::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Integer(2));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn insert_or_replace() {
        let mut db = fresh("ins_replace");
        db.execute("CREATE TABLE notif (key TEXT PRIMARY KEY, dismissed_at INTEGER)").unwrap();
        db.execute("INSERT INTO notif VALUES ('k', 100)").unwrap();
        db.execute("INSERT OR REPLACE INTO notif VALUES ('k', 200)").unwrap();
        // 1 行のみ、値は 200
        match db.execute("SELECT dismissed_at FROM notif").unwrap() {
            Output::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Integer(200));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn unknown_table() {
        let mut db = fresh("unknown");
        let err = db.execute("SELECT * FROM nope").unwrap_err();
        assert!(matches!(err, SqlError::UnknownTable(_)));
    }

    #[test]
    fn unknown_column() {
        let mut db = fresh("unknown_col");
        db.execute("CREATE TABLE t (id INTEGER)").unwrap();
        let err = db.execute("SELECT nope FROM t").unwrap_err();
        assert!(matches!(err, SqlError::UnknownColumn(_)));
    }

    #[test]
    fn matcha_use_case() {
        // matcha の通知 state 永続化シナリオ
        let mut db = fresh("matcha");
        db.execute("CREATE TABLE notif_state (key TEXT PRIMARY KEY, dismissed_at INTEGER)").unwrap();
        db.execute("INSERT INTO notif_state VALUES ('uuid-1', 1715174400)").unwrap();
        db.execute("INSERT INTO notif_state VALUES ('uuid-2', 1715174500)").unwrap();
        // dismissed を再上書き
        db.execute("INSERT OR REPLACE INTO notif_state VALUES ('uuid-1', 1715180000)").unwrap();
        match db.execute("SELECT key, dismissed_at FROM notif_state WHERE key = 'uuid-1'").unwrap() {
            Output::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], Value::Integer(1715180000));
            }
            _ => panic!(),
        }
        // un-dismiss → DELETE
        db.execute("DELETE FROM notif_state WHERE key = 'uuid-2'").unwrap();
        match db.execute("SELECT * FROM notif_state").unwrap() {
            Output::Rows { rows, .. } => assert_eq!(rows.len(), 1),
            _ => panic!(),
        }
    }
}
