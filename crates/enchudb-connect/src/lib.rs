//! enchudb の外との入口と出口。
//!
//! 外 (Kafka / Redpanda / Debezium の CDC / ファイル …) から流れてくるのは enchu の op ではなく
//! **table の行の変更** なので、 入口・出口とも schema 層の行単位で持つ。
//!
//! - 入口 ([`Ingest`]): [`Source`] から読んだメッセージを [`Decoder`] で行の変更 ([`Change`]) にし、
//!   主キーで upsert / delete する。 読んだ位置 (stream, partition, offset) は **同じ DB の中** に
//!   行と一緒に書き、 batch ごとに commit する。 再開時は [`Ingest::resume`] で source を最後に書いた
//!   位置の続きへ動かす — 落ちる直前の batch は読み直しになりうるが、 当てるのは主キーの upsert /
//!   delete なので何度当てても結果は同じ (取りこぼしは無い)。 当てた位置より前のメッセージが再送されたら
//!   位置で落とす
//! - 出口 ([`LiveExport`]): live 購読の差分 (row の出入り / group の件数と合計) を、 主キーと中身の
//!   JSON にして [`Sink`] へ流す。 外から見て意味のある識別子は eid でなく主キーなので、 出た row
//!   (削除済みで列が読めない) も主キーで届くように、 出力中の row の主キーを覚えておく
//!
//! ref 列は外の世界では 「参照先の table の主キー」 で来る / 出る (`orders.customer = 42`)。 参照先の
//! row がまだ無ければ主キーだけの row を作っておき、 後から本体が来たら埋まる。
//!
//! 接続先ごとのアダプタは [`Source`] / [`Sink`] を実装するだけ。 [`memory`] は同じプロセス内の
//! topic (テストと、 DB から DB へのつなぎ)。
//!
//! ```
//! use enchudb_connect::{memory::Topic, Ingest, JsonRows, LiveExport};
//! use enchudb_schema::Database;
//!
//! let path = format!("/tmp/enchu-connect-doc-{}.db", std::process::id());
//! # let _ = std::fs::remove_file(&path);
//! let mut db = Database::create_growable_tiny(&path).unwrap();
//! db.table("users").number("id").tag("city").primary_key("id").build().unwrap();
//! enchudb_connect::prepare(&mut db).unwrap();
//!
//! let (inbox, outbox) = (Topic::new("in"), Topic::new("out"));
//! let tokyo = db.get_table("users").unwrap().where_eq("city", "Tokyo").subscribe().unwrap();
//! let mut export = LiveExport::new(&db);
//! export.add("tokyo", "users", tokyo).unwrap();
//!
//! inbox.push(None, br#"{"table":"users","op":"upsert","row":{"id":1,"city":"Tokyo"}}"#);
//! let ingest = Ingest::new(&db, JsonRows).unwrap();
//! let mut src = inbox.source();
//! ingest.resume(&mut src).unwrap();
//! ingest.run_once(&mut src, 100).unwrap();
//! export.pump(&mut outbox.sink()).unwrap();
//! let out = outbox.messages();
//! assert_eq!(out.len(), 1);
//! assert!(String::from_utf8_lossy(&out[0].1).contains(r#""op":"add""#));
//! # drop(export); drop(db);
//! # let _ = std::fs::remove_file(&path);
//! ```

pub mod memory;

use enchudb_schema::{ColumnInfo, ColumnType, Database, LiveCounts, LiveQuery, Table, Value};
use serde_json::{Map, Value as Json};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;

// ─────────────────────────── 入口と出口の口 ───────────────────────────

/// source 上の位置。 `offset` は partition の中で単調増加 (Kafka の offset と同じ)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Position {
    pub stream: String,
    pub partition: u32,
    pub offset: u64,
}

/// source から読んだ 1 件。 `payload` が None は削除の印 (Kafka の tombstone)。
#[derive(Debug, Clone)]
pub struct Message {
    pub position: Position,
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
}

/// 外から読む口。 partition ごとに offset の昇順で返すこと。
pub trait Source {
    /// 次の最大 `max` 件 (無ければ空)。
    fn fetch(&mut self, max: usize) -> io::Result<Vec<Message>>;
    /// (`stream`, `partition`) の次に読む offset を `offset` にする ([`Ingest::resume`] が呼ぶ)。
    fn seek(&mut self, stream: &str, partition: u32, offset: u64) -> io::Result<()>;
}

/// 外へ送る 1 件。 `key` は Kafka なら partition の振り分けに使われる (同じ key は同じ順序で届く)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutMessage {
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
}

/// 外へ送る口。 `send` が Ok を返したら送れたものとする。
pub trait Sink {
    fn send(&mut self, msgs: &[OutMessage]) -> io::Result<()>;
}

// ─────────────────────────── 行の変更と decoder ───────────────────────────

/// 行の変更 1 件。 値は JSON のまま (列の型に合わせた変換は [`Ingest`] が table を見てやる)。
#[derive(Debug, Clone, PartialEq)]
pub enum Change {
    /// 主キーで upsert。 `row` に無い列は触らない、 null の列は値を外す。
    Upsert { table: String, row: Map<String, Json> },
    /// 主キーで削除 (無ければ何もしない)。
    Delete { table: String, key: Json },
}

/// メッセージ → 行の変更。 1 件から 0 件以上 (tombstone や関係ない event は 0 件)。
pub trait Decoder {
    fn decode(&self, db: &Database, msg: &Message) -> Result<Vec<Change>, String>;
}

/// 素朴な JSON 行: `{"table": "users", "op": "upsert", "row": {...}}` /
/// `{"table": "users", "op": "delete", "key": 42}`。 op を省くと upsert。
pub struct JsonRows;

impl Decoder for JsonRows {
    fn decode(&self, _db: &Database, msg: &Message) -> Result<Vec<Change>, String> {
        let Some(p) = &msg.payload else { return Ok(Vec::new()) };
        let j: Json = serde_json::from_slice(p).map_err(|e| format!("JSON: {e}"))?;
        let table = j.get("table").and_then(Json::as_str).ok_or("no \"table\"")?.to_string();
        match j.get("op").and_then(Json::as_str).unwrap_or("upsert") {
            "upsert" => {
                let row = j.get("row").and_then(Json::as_object).ok_or("no \"row\" object")?.clone();
                Ok(vec![Change::Upsert { table, row }])
            }
            "delete" => Ok(vec![Change::Delete { table, key: j.get("key").cloned().ok_or("no \"key\"")? }]),
            op => Err(format!("unknown op {op:?}")),
        }
    }
}

/// Debezium の変更 event (Postgres / MySQL などの CDC)。 `{"schema":..,"payload":{..}}` で包まれていても
/// 包まれていなくてもよい。 `op` が c / r / u は `after` の upsert、 d は `before` の主キーで削除。
/// table 名は `source.table` (`table_map` で enchu の table 名に写せる)。
pub struct Debezium {
    table_map: Option<TableMap>,
}

/// 元の table 名 → enchu の table 名。
type TableMap = Box<dyn Fn(&str) -> String + Send + Sync>;

impl Debezium {
    pub fn new() -> Self {
        Debezium { table_map: None }
    }

    /// 元の table 名 → enchu の table 名。
    pub fn with_table_map(f: impl Fn(&str) -> String + Send + Sync + 'static) -> Self {
        Debezium { table_map: Some(Box::new(f)) }
    }
}

impl Default for Debezium {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for Debezium {
    fn decode(&self, db: &Database, msg: &Message) -> Result<Vec<Change>, String> {
        let Some(p) = &msg.payload else { return Ok(Vec::new()) };
        let j: Json = serde_json::from_slice(p).map_err(|e| format!("JSON: {e}"))?;
        let ev = j.get("payload").filter(|x| x.is_object()).unwrap_or(&j);
        let src = ev.get("source").and_then(|s| s.get("table")).and_then(Json::as_str).ok_or("no source.table")?;
        let table = self.table_map.as_ref().map_or_else(|| src.to_string(), |f| f(src));
        let row = |k: &str| ev.get(k).and_then(Json::as_object).cloned().ok_or(format!("no \"{k}\" object"));
        match ev.get("op").and_then(Json::as_str).ok_or("no op")? {
            "c" | "r" | "u" => Ok(vec![Change::Upsert { table, row: row("after")? }]),
            "d" => {
                let before = row("before")?;
                let t = db.get_table(&table).ok_or(format!("unknown table {table}"))?;
                let pk = pk_col(&t).ok_or(format!("table {table} has no primary key"))?;
                let key = before.get(&pk.name).cloned().ok_or(format!("before has no {}", pk.name))?;
                Ok(vec![Change::Delete { table, key }])
            }
            // truncate / message など
            _ => Ok(Vec::new()),
        }
    }
}

// ─────────────────────────── 入口 ───────────────────────────

/// 読んだ位置を置く table。 [`prepare`] が作る。
pub const OFFSETS_TABLE: &str = "enchu_connect_offsets";

/// [`Ingest`] が読んだ位置を置く table を定義する (build phase で 1 回。 何度呼んでもよい)。
pub fn prepare(db: &mut Database) -> Result<(), String> {
    db.table(OFFSETS_TABLE)
        .tag("slot")
        .leaf("next")
        .primary_key("slot")
        .build()
        .map(|_| ())
        .map_err(|e| format!("{e:?}"))
}

/// [`Ingest::apply`] の結果。
#[derive(Debug, Default)]
pub struct IngestReport {
    /// 適用した行の変更の数。
    pub applied: usize,
    /// 既に適用済みの位置だったので飛ばしたメッセージの数 (再送)。
    pub replayed: usize,
    /// 読めなかった / 当てられなかったメッセージと理由 (流れは止めない)。
    pub rejected: Vec<(Position, String)>,
}

/// 外から来た行の変更を DB に当てる。
pub struct Ingest<'a, D: Decoder> {
    db: &'a Database,
    decoder: D,
    /// table 名 → 列と主キー (行ごとに列の一覧を作り直さない)。
    meta: RefCell<BTreeMap<String, Arc<Meta>>>,
}

/// table の列と主キー。
struct Meta {
    cols: Vec<ColumnInfo>,
    pk: ColumnInfo,
    pk_himo: u16,
}

impl<'a, D: Decoder> Ingest<'a, D> {
    /// [`prepare`] 済みの DB に対して作る。
    pub fn new(db: &'a Database, decoder: D) -> Result<Self, String> {
        if db.get_table(OFFSETS_TABLE).is_none() {
            return Err(format!("{OFFSETS_TABLE} is missing — call enchudb_connect::prepare in the build phase"));
        }
        Ok(Ingest { db, decoder, meta: RefCell::default() })
    }

    fn meta(&self, table: &str) -> Result<Arc<Meta>, String> {
        if let Some(m) = self.meta.borrow().get(table) {
            return Ok(m.clone());
        }
        let t = self.db.get_table(table).ok_or(format!("unknown table {table}"))?;
        let pk = pk_col(&t).ok_or(format!("table {table} has no primary key"))?;
        let pk_himo = t.himo_id(&pk.name).ok_or(format!("table {table}: no himo for {}", pk.name))?;
        let m = Arc::new(Meta { cols: t.columns(), pk, pk_himo });
        self.meta.borrow_mut().insert(table.to_string(), m.clone());
        Ok(m)
    }

    /// 主キー `k` の row (無ければ None)。 query を組まずに engine の索引を直接引く。
    fn lookup(&self, table: &str, m: &Meta, k: &Value) -> Result<Option<u64>, String> {
        let eng = self.db.engine();
        let raw = match (m.pk.ty, k) {
            (ColumnType::Number, Value::Number(n)) => *n as u32,
            // vocab に無い text = その主キーの row は無い
            (ColumnType::Tag, Value::Text(s)) => match eng.vocab_id(s) {
                Some(v) => v,
                None => return Ok(None),
            },
            (ColumnType::Ref, Value::Ref(e)) => enchudb_oplog::eid_local(*e),
            _ => {
                let t = self.db.get_table(table).ok_or(format!("unknown table {table}"))?;
                return t.where_eq(&m.pk.name, k.clone()).find_one().map_err(|e| format!("{e:?}"));
            }
        };
        Ok(eng.query_by_id(&[(m.pk_himo, raw)]).into_iter().next())
    }

    fn offsets(&self) -> Table<'a> {
        self.db.get_table(OFFSETS_TABLE).expect("checked in new")
    }

    fn slot(stream: &str, partition: u32) -> String {
        format!("{stream}#{partition}")
    }

    /// (`stream`, `partition`) の次に読む offset (まだ読んでいなければ 0)。
    pub fn next_offset(&self, stream: &str, partition: u32) -> u64 {
        let t = self.offsets();
        let Ok(Some(e)) = t.where_eq("slot", Self::slot(stream, partition)).find_one() else { return 0 };
        match t.entity(e).get("next") {
            Some(Value::Text(s)) => s.parse().unwrap_or(0),
            _ => 0,
        }
    }

    /// 覚えている全 partition について、 source を続きの位置へ動かす (起動時に 1 回)。
    pub fn resume(&self, source: &mut dyn Source) -> io::Result<()> {
        let t = self.offsets();
        for e in t.all().find().unwrap_or_default() {
            let (Some(Value::Text(slot)), Some(Value::Text(next))) = (t.entity(e).get("slot"), t.entity(e).get("next")) else {
                continue;
            };
            let Some((stream, part)) = slot.rsplit_once('#') else { continue };
            let (Ok(part), Ok(next)) = (part.parse(), next.parse()) else { continue };
            source.seek(stream, part, next)?;
        }
        Ok(())
    }

    /// `source` から最大 `max` 件読んで当てる。
    pub fn run_once(&self, source: &mut dyn Source, max: usize) -> io::Result<IngestReport> {
        let msgs = source.fetch(max)?;
        Ok(self.apply(&msgs))
    }

    /// メッセージを当て、 読んだ位置を書いて commit する。 既に当てた位置 (次に読む offset より前) の
    /// メッセージは飛ばす。
    pub fn apply(&self, msgs: &[Message]) -> IngestReport {
        let mut report = IngestReport::default();
        // (slot) → 次に読む offset。 同じ batch の中の位置は覚えておき、 最後にまとめて書く
        let mut next: BTreeMap<(String, u32), u64> = BTreeMap::new();
        for m in msgs {
            let k = (m.position.stream.clone(), m.position.partition);
            let n = *next.entry(k.clone()).or_insert_with(|| self.next_offset(&k.0, k.1));
            if m.position.offset < n {
                report.replayed += 1;
                continue;
            }
            match self.decoder.decode(self.db, m).and_then(|cs| {
                cs.iter().try_for_each(|c| self.apply_change(c))?;
                Ok(cs.len())
            }) {
                Ok(n) => report.applied += n,
                Err(e) => report.rejected.push((m.position.clone(), e)),
            }
            next.insert(k, m.position.offset + 1);
        }
        let t = self.offsets();
        for ((stream, part), n) in next {
            let _ = t.upsert().set("slot", Self::slot(&stream, part)).set("next", n.to_string()).commit();
        }
        // 行と位置を commit (oplog が無い DB では何もしない)。 落ちたらこの batch は読み直し = 当て直し
        self.db.engine().commit();
        report
    }

    /// 行の変更 1 件を当てる。
    pub fn apply_change(&self, c: &Change) -> Result<(), String> {
        match c {
            Change::Upsert { table, row } => {
                let m = self.meta(table)?;
                let t = self.db.get_table(table).ok_or(format!("unknown table {table}"))?;
                let (pk, cols) = (&m.pk, &m.cols);
                if row.get(&pk.name).is_none_or(Json::is_null) {
                    return Err(format!("row has no primary key {}", pk.name));
                }
                let mut b = t.upsert();
                let mut nulls = Vec::new();
                for (name, v) in row {
                    let Some(cd) = cols.iter().find(|c| c.name.eq_ignore_ascii_case(name)) else {
                        continue; // enchu 側に無い列は読み捨てる (元の table の列が多いのは普通)
                    };
                    if v.is_null() {
                        nulls.push(cd.name.clone());
                        continue;
                    }
                    b = b.set(&cd.name, self.to_value(cd, v)?);
                }
                let eid = b.commit().map_err(|e| format!("{e:?}"))?;
                for col in nulls {
                    if let Some(h) = t.himo_id(&col) {
                        self.db.engine().untie_by_id(eid, h);
                    }
                }
                Ok(())
            }
            Change::Delete { table, key } => {
                let m = self.meta(table)?;
                let t = self.db.get_table(table).ok_or(format!("unknown table {table}"))?;
                let k = self.to_value(&m.pk, key)?;
                if let Some(e) = self.lookup(table, &m, &k)? {
                    t.entity(e).delete().map_err(|e| format!("{e:?}"))?;
                }
                Ok(())
            }
        }
    }

    /// JSON の値を列の型の値に。 ref 列は参照先の主キー (無ければ主キーだけの row を作る)。
    fn to_value(&self, cd: &ColumnInfo, v: &Json) -> Result<Value, String> {
        let bad = || format!("column {}: {v} does not fit {:?}", cd.name, cd.ty);
        Ok(match cd.ty {
            ColumnType::Number => {
                let n = match v {
                    Json::Number(n) => n.as_u64().ok_or_else(bad)?,
                    Json::Bool(b) => *b as u64,
                    Json::String(s) => s.trim().parse::<u64>().map_err(|_| bad())?,
                    _ => return Err(bad()),
                };
                // engine の値は u32 (u32::MAX は予約)
                if n >= u32::MAX as u64 {
                    return Err(format!("column {}: {n} is out of range (0..{})", cd.name, u32::MAX));
                }
                Value::Number(n as i64)
            }
            ColumnType::Tag | ColumnType::Leaf => match v {
                Json::String(s) => Value::Text(s.clone()),
                Json::Number(_) | Json::Bool(_) => Value::Text(v.to_string()),
                _ => return Err(bad()),
            },
            ColumnType::Ref => {
                let to = cd.ref_to.as_deref().ok_or(format!("column {} has no ref target", cd.name))?;
                let m = self.meta(to)?;
                let k = self.to_value(&m.pk, v)?;
                let e = match self.lookup(to, &m, &k)? {
                    Some(e) => e,
                    // 参照先がまだ来ていない: 主キーだけの row を作っておく (本体が来たら upsert で埋まる)
                    None => {
                        let t = self.db.get_table(to).ok_or(format!("unknown table {to}"))?;
                        t.upsert().set(&m.pk.name, k).commit().map_err(|e| format!("{e:?}"))?
                    }
                };
                Value::Ref(e)
            }
        })
    }
}

fn pk_col(t: &Table) -> Option<ColumnInfo> {
    t.columns().into_iter().find(|c| c.is_pk)
}

// ─────────────────────────── 出口 ───────────────────────────

enum Sub {
    Rows {
        name: String,
        table: String,
        q: LiveQuery,
        /// 出力中の row (eid → 主キー)。 出た row は列が読めないことがある (削除) ので覚えておく。
        keys: BTreeMap<u64, Json>,
    },
    Counts {
        name: String,
        q: LiveCounts,
    },
}

/// live 購読の差分を外へ流す。
///
/// row の購読 ([`add`](Self::add)) は 1 row = 1 メッセージ:
/// `{"sub": name, "op": "add", "key": 主キー, "row": {列: 値}}` / `{"sub": name, "op": "remove", "key": 主キー}`。
/// 集計の購読 ([`add_counts`](Self::add_counts)) は動いた group ごとに
/// `{"sub": name, "group": 値, "count": 件数, "sum": 合計}` (件数 0 = group が消えた)。 メッセージの key は
/// `name` と主キー / group (同じ row の出入りは同じ key = Kafka では同じ partition で順に届く)。
pub struct LiveExport<'a> {
    db: &'a Database,
    subs: Vec<Sub>,
}

impl<'a> LiveExport<'a> {
    pub fn new(db: &'a Database) -> Self {
        LiveExport { db, subs: Vec::new() }
    }

    /// table `table` への row の購読 `q` を `name` で流す。 `table` には主キーが要る。
    pub fn add(&mut self, name: &str, table: &str, q: LiveQuery) -> Result<(), String> {
        let t = self.db.get_table(table).ok_or(format!("unknown table {table}"))?;
        pk_col(&t).ok_or(format!("table {table} has no primary key"))?;
        self.subs.push(Sub::Rows { name: name.into(), table: table.into(), q, keys: BTreeMap::new() });
        Ok(())
    }

    /// 集計の購読 `q` を `name` で流す。
    pub fn add_counts(&mut self, name: &str, q: LiveCounts) {
        self.subs.push(Sub::Counts { name: name.into(), q });
    }

    /// 全購読の差分を取り、 メッセージにして `sink` へ送る。 送った数を返す。
    ///
    /// 送るのに失敗したら Err (その回の差分は購読側では受け取り済み — 取りこぼしたくない時は
    /// sink 側で再送すること)。
    pub fn pump(&mut self, sink: &mut dyn Sink) -> io::Result<usize> {
        let mut out = Vec::new();
        for sub in &mut self.subs {
            match sub {
                Sub::Rows { name, table, q, keys } => {
                    let t = self.db.get_table(table).expect("checked in add");
                    let d = q.poll();
                    for e in d.removed {
                        let key = keys.remove(&e).unwrap_or(Json::Null);
                        out.push(msg(name, &key, serde_json::json!({ "sub": name, "op": "remove", "key": key })));
                    }
                    for e in d.added {
                        let (key, row) = row_json(self.db, &t, e);
                        keys.insert(e, key.clone());
                        out.push(msg(name, &key, serde_json::json!({ "sub": name, "op": "add", "key": key, "row": row })));
                    }
                }
                Sub::Counts { name, q } => {
                    for (v, n, s) in q.poll_sums() {
                        let g = value_json(self.db, None, &v);
                        out.push(msg(name, &g, serde_json::json!({ "sub": name, "group": g, "count": n, "sum": s })));
                    }
                }
            }
        }
        if !out.is_empty() {
            sink.send(&out)?;
        }
        Ok(out.len())
    }
}

fn msg(name: &str, key: &Json, payload: Json) -> OutMessage {
    OutMessage { key: format!("{name}/{key}").into_bytes(), payload: payload.to_string().into_bytes() }
}

/// row の (主キー, 全列) を JSON に。 ref 列は参照先の主キー。
fn row_json(db: &Database, t: &Table, e: u64) -> (Json, Json) {
    let mut row = Map::new();
    let mut key = Json::Null;
    for c in t.columns() {
        let Some(v) = t.entity(e).get(&c.name) else { continue };
        let j = value_json(db, c.ref_to.as_deref(), &v);
        if c.is_pk {
            key = j.clone();
        }
        row.insert(c.name, j);
    }
    (key, Json::Object(row))
}

/// 値を JSON に。 `ref_to` があれば ref は参照先の主キー (無ければ eid の数)。
fn value_json(db: &Database, ref_to: Option<&str>, v: &Value) -> Json {
    match v {
        Value::Null => Json::Null,
        Value::Number(n) => Json::from(*n),
        Value::Text(s) => Json::from(s.as_str()),
        Value::Ref(e) => {
            let target = ref_to.and_then(|to| db.get_table(to));
            match target.as_ref().and_then(|t| Some((t, pk_col(t)?))) {
                Some((t, pk)) => t.entity(*e).get(&pk.name).map_or(Json::Null, |k| value_json(db, None, &k)),
                None => Json::from(*e),
            }
        }
    }
}
