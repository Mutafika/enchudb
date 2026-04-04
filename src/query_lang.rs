//! 円柱クエリ言語 — 紐を並べるだけ。
//!
//!   age:30 city:東京          → 重なった entity 一覧
//!   age:30 city:東京 | count  → 件数
//!   + age:30 city:東京 name:"田中"  → 追加
//!   - 42                      → 削除

use crate::engine::Engine;

#[derive(Debug)]
pub enum QueryResult {
    Entities(Vec<u32>),
    Count(usize),
    Inserted(u32),
    Deleted,
    Error(String),
}

impl std::fmt::Display for QueryResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            QueryResult::Entities(v) if v.is_empty() => write!(f, "(empty)"),
            QueryResult::Entities(v) => {
                for (i, eid) in v.iter().enumerate() {
                    if i > 0 { write!(f, " ")?; }
                    write!(f, "{eid}")?;
                }
                Ok(())
            }
            QueryResult::Count(n) => write!(f, "{n}"),
            QueryResult::Inserted(id) => write!(f, "+{id}"),
            QueryResult::Deleted => write!(f, "ok"),
            QueryResult::Error(e) => write!(f, "! {e}"),
        }
    }
}

pub fn execute(eng: &mut Engine, input: &str) -> QueryResult {
    let input = input.trim();
    if input.is_empty() { return QueryResult::Error("empty".into()); }

    if input.starts_with('+') {
        return exec_insert(eng, &input[1..].trim());
    }
    if input.starts_with('-') {
        return exec_delete(eng, &input[1..].trim());
    }

    // パイプでモード分離: "age:30 | count"
    let (cond_str, mode) = if let Some(pos) = input.find('|') {
        (&input[..pos], input[pos + 1..].trim())
    } else {
        (input, "")
    };

    let pairs = parse_pairs(eng, cond_str.trim());
    if let Err(e) = pairs { return QueryResult::Error(e); }
    let pairs = pairs.unwrap();

    if pairs.is_empty() { return QueryResult::Error("no conditions".into()); }

    eng.rebuild();
    let refs: Vec<(&str, u32)> = pairs.iter().map(|(d, v)| (d.as_str(), *v)).collect();
    let result = eng.query(&refs);

    match mode {
        "count" => QueryResult::Count(result.len()),
        _ => QueryResult::Entities(result),
    }
}

fn exec_insert(eng: &mut Engine, input: &str) -> QueryResult {
    let tokens = parse_kv_tokens(input);
    if tokens.is_empty() { return QueryResult::Error("nothing to insert".into()); }

    let eid = eng.entity();
    for (himo, val) in &tokens {
        if val.starts_with('"') && val.ends_with('"') {
            let text = &val[1..val.len() - 1];
            eng.tie_text(eid, himo, text);
        } else {
            match val.parse::<u32>() {
                Ok(v) => eng.tie(eid, himo, v),
                Err(_) => eng.tie_text(eid, himo, val),
            }
        }
    }
    QueryResult::Inserted(eid)
}

fn exec_delete(eng: &mut Engine, input: &str) -> QueryResult {
    match input.parse::<u32>() {
        Ok(eid) => { eng.delete(eid); QueryResult::Deleted }
        Err(_) => QueryResult::Error(format!("invalid id: {input}")),
    }
}

/// "age:30 city:東京" → Vec<(himo, u32_value)>
fn parse_pairs(eng: &Engine, input: &str) -> Result<Vec<(String, u32)>, String> {
    let tokens = parse_kv_tokens(input);
    let mut result = Vec::new();
    for (himo, val) in &tokens {
        let v = resolve_value(eng, val)?;
        result.push((himo.clone(), v));
    }
    Ok(result)
}

/// "himo:val himo:val" → Vec<(himo, val_str)>
fn parse_kv_tokens(input: &str) -> Vec<(String, String)> {
    let mut result = Vec::new();
    let mut chars = input.chars().peekable();

    while chars.peek().is_some() {
        while chars.peek().map_or(false, |c| c.is_whitespace()) { chars.next(); }
        if chars.peek().is_none() { break; }

        let mut key = String::new();
        while let Some(&c) = chars.peek() {
            if c == ':' { break; }
            if c.is_whitespace() { break; }
            key.push(c);
            chars.next();
        }

        if chars.peek() != Some(&':') { continue; }
        chars.next(); // skip ':'

        let mut val = String::new();
        if chars.peek() == Some(&'"') {
            val.push(chars.next().unwrap());
            while let Some(c) = chars.next() {
                val.push(c);
                if c == '"' { break; }
            }
        } else {
            while let Some(&c) = chars.peek() {
                if c.is_whitespace() || c == '|' { break; }
                val.push(c);
                chars.next();
            }
        }

        if !key.is_empty() && !val.is_empty() {
            result.push((key, val));
        }
    }
    result
}

fn resolve_value(eng: &Engine, val: &str) -> Result<u32, String> {
    if val.starts_with('"') && val.ends_with('"') {
        let text = &val[1..val.len() - 1];
        eng.vocab_id(text).ok_or(format!("unknown: {text}"))
    } else {
        val.parse::<u32>().map_err(|_| format!("invalid: {val}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup(name: &str) -> Engine {
        let dir = format!("/tmp/enchu_v23_ql_{name}");
        let _ = std::fs::remove_dir_all(&dir);
        let mut eng = Engine::create(&dir).unwrap();

        for i in 0..10u32 {
            let e = eng.entity();
            eng.tie(e, "age", 20 + i);
            eng.tie(e, "dept", i % 3);
            eng.tie_text(e, "city", if i < 5 { "東京" } else { "大阪" });
        }
        eng
    }

    #[test]
    fn single_pull() {
        let mut eng = setup("single");
        match execute(&mut eng, "age:25") {
            QueryResult::Entities(v) => assert_eq!(v.len(), 1),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn count() {
        let mut eng = setup("count");
        match execute(&mut eng, "age:25 | count") {
            QueryResult::Count(1) => {}
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn multi_condition() {
        let mut eng = setup("multi");
        match execute(&mut eng, r#"city:"東京" dept:0 | count"#) {
            QueryResult::Count(2) => {}
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn insert() {
        let mut eng = setup("insert");
        let r = execute(&mut eng, r#"+ age:99 city:"福岡""#);
        assert!(matches!(r, QueryResult::Inserted(_)));
        match execute(&mut eng, "age:99 | count") {
            QueryResult::Count(1) => {}
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn delete() {
        let mut eng = setup("delete");
        let eid = match execute(&mut eng, "age:20") {
            QueryResult::Entities(v) => v[0],
            _ => panic!(),
        };
        execute(&mut eng, &format!("- {eid}"));
        match execute(&mut eng, "age:20 | count") {
            QueryResult::Count(0) => {}
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn empty_result() {
        let mut eng = setup("empty");
        match execute(&mut eng, "age:99 | count") {
            QueryResult::Count(0) => {}
            other => panic!("got {other:?}"),
        }
    }
}
