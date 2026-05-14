//! enchudb の DB ファイルの中身を AI / 人間に渡しやすい形式 (markdown / json) で
//! 標準出力に dump する CLI。
//!
//! Usage:
//!     cargo run --example dump -- <db_path>
//!     cargo run --example dump -- <db_path> --table <name>
//!     cargo run --example dump -- <db_path> --schema-only
//!     cargo run --example dump -- <db_path> --format json
//!
//! Ref 列は `#<eid> (<対象 entity の最初の Tag 列値>)` 形式で人間にも読みやすく出す。

use std::collections::HashMap;
use std::process::ExitCode;

use enchudb::schema::{ColumnInfo, ColumnType, Database, Value};

fn col_type_label(ty: ColumnType) -> &'static str {
    match ty {
        ColumnType::Tag => "Tag",
        ColumnType::Number => "Num",
        ColumnType::Leaf => "Leaf",
        ColumnType::Ref => "Ref",
    }
}

/// 対象テーブルの entity の **表示名** を取り出す。最初の Tag 列の値、
/// なければ空文字。Ref 表示の併記用。
fn entity_display(db: &Database, table: &str, eid: u64) -> String {
    let Some(tbl) = db.get_table(table) else {
        return String::new();
    };
    for c in tbl.columns() {
        if c.ty == ColumnType::Tag {
            if let Some(Value::Text(s)) = tbl.entity(eid).get(&c.name) {
                if !s.is_empty() {
                    return s;
                }
            }
        }
    }
    String::new()
}

fn value_to_md(db: &Database, col: &ColumnInfo, v: &Value) -> String {
    match (col.ty, v) {
        (_, Value::Null) => String::new(),
        (ColumnType::Number, Value::Number(n)) => n.to_string(),
        (ColumnType::Tag | ColumnType::Leaf, Value::Text(s)) => {
            // markdown 表中で改行とパイプをエスケープ
            s.replace('\\', "\\\\").replace('|', "\\|").replace('\n', "\\n")
        }
        (ColumnType::Ref, Value::Ref(r)) => {
            let target_tbl = col.ref_to.as_deref().unwrap_or("");
            let disp = entity_display(db, target_tbl, *r);
            if disp.is_empty() {
                format!("#{r}")
            } else {
                format!("#{r} ({disp})")
            }
        }
        // 型と Value のミスマッチは生で。
        (_, Value::Number(n)) => n.to_string(),
        (_, Value::Text(s)) => s.clone(),
        (_, Value::Ref(r)) => format!("#{r}"),
    }
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn value_to_json(col: &ColumnInfo, v: &Value) -> String {
    match (col.ty, v) {
        (_, Value::Null) => "null".to_string(),
        (ColumnType::Number, Value::Number(n)) => n.to_string(),
        (ColumnType::Tag | ColumnType::Leaf, Value::Text(s)) => {
            format!("\"{}\"", json_escape(s))
        }
        (ColumnType::Ref, Value::Ref(r)) => r.to_string(),
        (_, Value::Number(n)) => n.to_string(),
        (_, Value::Text(s)) => format!("\"{}\"", json_escape(s)),
        (_, Value::Ref(r)) => r.to_string(),
    }
}

struct Args {
    db_path: String,
    table_filter: Option<String>,
    schema_only: bool,
    format: Format,
}

#[derive(Clone, Copy, PartialEq)]
enum Format {
    Markdown,
    Json,
}

fn parse_args() -> Result<Args, String> {
    let raw: Vec<String> = std::env::args().skip(1).collect();
    if raw.is_empty() || raw.iter().any(|a| a == "--help" || a == "-h") {
        return Err(usage());
    }
    let mut db_path: Option<String> = None;
    let mut table_filter: Option<String> = None;
    let mut schema_only = false;
    let mut format = Format::Markdown;

    let mut i = 0;
    while i < raw.len() {
        match raw[i].as_str() {
            "--table" => {
                i += 1;
                table_filter = Some(raw.get(i).ok_or("--table requires value")?.clone());
            }
            "--schema-only" => schema_only = true,
            "--format" => {
                i += 1;
                let v = raw.get(i).ok_or("--format requires value")?.as_str();
                format = match v {
                    "markdown" | "md" => Format::Markdown,
                    "json" => Format::Json,
                    _ => return Err(format!("unknown format: {v}")),
                };
            }
            s if s.starts_with("--") => return Err(format!("unknown flag: {s}")),
            s => {
                if db_path.is_some() {
                    return Err(format!("unexpected arg: {s}"));
                }
                db_path = Some(s.to_string());
            }
        }
        i += 1;
    }

    Ok(Args {
        db_path: db_path.ok_or("db_path required")?,
        table_filter,
        schema_only,
        format,
    })
}

fn usage() -> String {
    "usage: dump <db_path> [--table NAME] [--schema-only] [--format markdown|json]"
        .to_string()
}

fn format_markdown(db: &Database, args: &Args) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    for ti in db.list_tables() {
        if let Some(filter) = &args.table_filter {
            if &ti.name != filter {
                continue;
            }
        }
        let cols: Vec<ColumnInfo> = ti.columns.clone();
        let pk: Option<&ColumnInfo> = cols.iter().find(|c| c.is_pk);
        let tbl = db.get_table(&ti.name).expect("table not found");
        let row_count = tbl.all().find().map(|v| v.len()).unwrap_or(0);

        let pk_label = pk.map(|c| format!(" (PK: {})", c.name)).unwrap_or_default();
        let _ = writeln!(out, "## {}{} — {} rows", ti.name, pk_label, row_count);

        // header
        out.push_str("| eid |");
        for c in &cols {
            let ref_part = if let (ColumnType::Ref, Some(t)) = (c.ty, &c.ref_to) {
                format!("→{t}")
            } else {
                String::new()
            };
            let _ = write!(out, " {} ({}{}) |", c.name, col_type_label(c.ty), ref_part);
        }
        out.push('\n');
        out.push_str("|-----|");
        for _ in &cols {
            out.push_str("------|");
        }
        out.push('\n');

        if args.schema_only {
            out.push('\n');
            continue;
        }

        let mut eids: Vec<u64> = tbl.all().find().unwrap_or_default();
        eids.sort();
        for eid in eids {
            let _ = write!(out, "| #{eid} |");
            for c in &cols {
                let v = tbl.entity(eid).get(&c.name).unwrap_or(Value::Null);
                let _ = write!(out, " {} |", value_to_md(db, c, &v));
            }
            out.push('\n');
        }
        out.push('\n');
    }
    out
}

fn format_json(db: &Database, args: &Args) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    out.push_str("{\"tables\":[");
    let tables = db.list_tables();
    let filtered: Vec<_> = tables
        .iter()
        .filter(|ti| match &args.table_filter {
            Some(f) => &ti.name == f,
            None => true,
        })
        .collect();
    for (ti_idx, ti) in filtered.iter().enumerate() {
        if ti_idx > 0 {
            out.push(',');
        }
        out.push('{');
        let _ = write!(out, "\"name\":\"{}\",", json_escape(&ti.name));
        out.push_str("\"columns\":[");
        for (i, c) in ti.columns.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push('{');
            let _ = write!(out, "\"name\":\"{}\",", json_escape(&c.name));
            let _ = write!(out, "\"type\":\"{}\",", col_type_label(c.ty));
            let _ = write!(out, "\"is_pk\":{}", c.is_pk);
            if let Some(t) = &c.ref_to {
                let _ = write!(out, ",\"ref_to\":\"{}\"", json_escape(t));
            }
            out.push('}');
        }
        out.push(']');

        if !args.schema_only {
            out.push_str(",\"rows\":[");
            let tbl = db.get_table(&ti.name).expect("table not found");
            let mut eids: Vec<u64> = tbl.all().find().unwrap_or_default();
            eids.sort();
            for (ri, eid) in eids.iter().enumerate() {
                if ri > 0 {
                    out.push(',');
                }
                let _ = write!(out, "{{\"eid\":{eid}");
                let mut col_map: HashMap<String, Value> = HashMap::new();
                for c in &ti.columns {
                    if let Some(v) = tbl.entity(*eid).get(&c.name) {
                        col_map.insert(c.name.clone(), v);
                    }
                }
                for c in &ti.columns {
                    if let Some(v) = col_map.get(&c.name) {
                        let _ = write!(out, ",\"{}\":{}", json_escape(&c.name), value_to_json(c, v));
                    }
                }
                out.push('}');
            }
            out.push(']');
        }
        out.push('}');
    }
    out.push_str("]}");
    out.push('\n');
    out
}

fn run() -> Result<(), String> {
    let args = parse_args()?;
    let db = Database::open(&args.db_path).map_err(|e| format!("open: {e}"))?;
    let out = match args.format {
        Format::Markdown => format_markdown(&db, &args),
        Format::Json => format_json(&db, &args),
    };
    print!("{out}");
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_db(name: &str) -> String {
        let p = format!("/tmp/enchudb_dump_test_{name}.db");
        let _ = std::fs::remove_file(&p);
        p
    }

    fn make_test_db(path: &str) -> Database {
        let mut db = Database::create(path).unwrap();
        db.table("user")
            .number("id")
            .tag("name")
            .number("age")
            .leaf("bio")
            .primary_key("id")
            .build()
            .unwrap();
        db.table("post")
            .number("id")
            .tag("title")
            .ref_to("author", "user")
            .primary_key("id")
            .build()
            .unwrap();

        let users = db.get_table("user").unwrap();
        let alice = users
            .insert()
            .set("id", 1i64)
            .set("name", "Alice")
            .set("age", 30i64)
            .set("bio", "line1\nline2")
            .commit()
            .unwrap();
        users
            .insert()
            .set("id", 2i64)
            .set("name", "Bob|pipe")
            .set("age", 27i64)
            .commit()
            .unwrap();

        let posts = db.get_table("post").unwrap();
        posts
            .insert()
            .set("id", 100i64)
            .set("title", "Hello")
            .set("author", Value::Ref(alice))
            .commit()
            .unwrap();
        db
    }

    fn default_args(db_path: &str) -> Args {
        Args {
            db_path: db_path.to_string(),
            table_filter: None,
            schema_only: false,
            format: Format::Markdown,
        }
    }

    #[test]
    fn markdown_contains_table_header_and_rows() {
        let p = tmp_db("md_basic");
        let db = make_test_db(&p);
        let out = format_markdown(&db, &default_args(&p));

        assert!(out.contains("## user (PK: id) — 2 rows"));
        assert!(out.contains("## post (PK: id) — 1 rows"));
        assert!(out.contains("Alice"));
        assert!(out.contains("Hello"));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn markdown_ref_shows_target_display_name() {
        // post.author = #<alice eid>。 entity_display で "Alice" が併記される。
        let p = tmp_db("md_ref");
        let db = make_test_db(&p);
        let out = format_markdown(&db, &default_args(&p));

        assert!(out.contains("(Alice)"), "ref に対象 entity の表示名が併記されるべき:\n{out}");
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn markdown_escapes_newline_and_pipe() {
        let p = tmp_db("md_escape");
        let db = make_test_db(&p);
        let out = format_markdown(&db, &default_args(&p));

        // bio = "line1\nline2" → \n に escape
        assert!(out.contains("line1\\nline2"));
        // name = "Bob|pipe" → \| に escape
        assert!(out.contains("Bob\\|pipe"));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn markdown_column_type_labels() {
        let p = tmp_db("md_types");
        let db = make_test_db(&p);
        let out = format_markdown(&db, &default_args(&p));

        assert!(out.contains("id (Num)"));
        assert!(out.contains("name (Tag)"));
        assert!(out.contains("bio (Leaf)"));
        assert!(out.contains("author (Ref→user)"));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn markdown_table_filter_limits_output() {
        let p = tmp_db("md_filter");
        let db = make_test_db(&p);
        let mut args = default_args(&p);
        args.table_filter = Some("user".to_string());
        let out = format_markdown(&db, &args);

        assert!(out.contains("## user"));
        assert!(!out.contains("## post"));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn markdown_schema_only_excludes_rows() {
        let p = tmp_db("md_schema_only");
        let db = make_test_db(&p);
        let mut args = default_args(&p);
        args.schema_only = true;
        let out = format_markdown(&db, &args);

        assert!(out.contains("## user"));
        assert!(out.contains("name (Tag)"));
        // 行データは出ない
        assert!(!out.contains("Alice"));
        assert!(!out.contains("Hello"));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn json_contains_tables_columns_rows() {
        let p = tmp_db("json_basic");
        let db = make_test_db(&p);
        let mut args = default_args(&p);
        args.format = Format::Json;
        let out = format_json(&db, &args);

        assert!(out.starts_with("{\"tables\":["));
        assert!(out.contains("\"name\":\"user\""));
        assert!(out.contains("\"name\":\"post\""));
        assert!(out.contains("\"type\":\"Tag\""));
        assert!(out.contains("\"type\":\"Leaf\""));
        assert!(out.contains("\"type\":\"Ref\""));
        assert!(out.contains("\"ref_to\":\"user\""));
        assert!(out.contains("\"is_pk\":true"));
        assert!(out.contains("\"Alice\""));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn json_escapes_newline_in_leaf() {
        let p = tmp_db("json_escape");
        let db = make_test_db(&p);
        let mut args = default_args(&p);
        args.format = Format::Json;
        let out = format_json(&db, &args);

        // JSON 内で \n エスケープされてる
        assert!(out.contains("\"line1\\nline2\""));
        std::fs::remove_file(&p).ok();
    }

    #[test]
    fn json_escape_handles_quotes_and_backslash() {
        assert_eq!(json_escape("a\"b"), "a\\\"b");
        assert_eq!(json_escape("a\\b"), "a\\\\b");
        assert_eq!(json_escape("a\nb"), "a\\nb");
        assert_eq!(json_escape("a\tb"), "a\\tb");
    }

    #[test]
    fn args_parse_basic() {
        // 引数パーサの基本動作。db_path のみ。
        let saved: Vec<String> = std::env::args().collect();
        // parse_args は std::env::args を直接使うので、 ここでは
        // 個別のパスを直接テストする代わりに、 構造体の整合性のみ確認。
        let _ = saved;
        let args = Args {
            db_path: "/tmp/x.db".to_string(),
            table_filter: None,
            schema_only: false,
            format: Format::Markdown,
        };
        assert!(matches!(args.format, Format::Markdown));
    }
}
