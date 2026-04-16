use std::sync::Arc;
use crate::engine::Engine;

pub struct Ravn {
    engine: Arc<Engine>,
}

pub enum RavnResult {
    Entities(Vec<u32>),
    Count(usize),
    Values(Vec<(u32, Vec<Option<u32>>)>),
    Error(String),
}

impl std::fmt::Debug for RavnResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            RavnResult::Entities(v) => write!(f, "Entities({v:?})"),
            RavnResult::Count(n) => write!(f, "Count({n})"),
            RavnResult::Values(v) => write!(f, "Values({v:?})"),
            RavnResult::Error(e) => write!(f, "Error({e:?})"),
        }
    }
}

impl Ravn {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    pub fn path(&self, eid: u32, steps: &[&str]) -> Option<u32> {
        if steps.is_empty() { return None; }
        let mut current = eid;
        for (i, step) in steps.iter().enumerate() {
            let val = self.engine.get(current, step)?;
            if i < steps.len() - 1 {
                current = val;
            } else {
                return Some(val);
            }
        }
        None
    }

    pub fn path_text(&self, eid: u32, steps: &[&str]) -> Option<Vec<u8>> {
        if steps.is_empty() { return None; }
        let mut current = eid;
        for (i, step) in steps.iter().enumerate() {
            if i < steps.len() - 1 {
                current = self.engine.get(current, step)?;
            } else {
                return self.engine.get_text(current, step).map(|b| b.to_vec());
            }
        }
        None
    }

    pub fn follow(&self, start: &[u32], steps: &[&str]) -> Vec<u32> {
        let mut current: Vec<u32> = start.to_vec();
        for step in steps {
            let mut next = Vec::new();
            for &eid in &current {
                if let Some(val) = self.engine.get(eid, step) {
                    next.push(val);
                }
            }
            current = next;
        }
        current
    }

    pub fn select(&self, conds: &[(&str, u32)], fields: &[&str]) -> Vec<(u32, Vec<Option<u32>>)> {
        let eids = self.engine.query(conds);
        eids.iter().map(|&eid| {
            let values: Vec<Option<u32>> = fields.iter()
                .map(|f| self.engine.get(eid, f))
                .collect();
            (eid, values)
        }).collect()
    }

    pub fn select_text(&self, conds: &[(&str, u32)], fields: &[&str]) -> Vec<(u32, Vec<Option<Vec<u8>>>)> {
        let eids = self.engine.query(conds);
        eids.iter().map(|&eid| {
            let values: Vec<Option<Vec<u8>>> = fields.iter()
                .map(|f| self.engine.get_text(eid, f).map(|b| b.to_vec()))
                .collect();
            (eid, values)
        }).collect()
    }

    pub fn exec(&self, input: &str) -> RavnResult {
        let input = input.trim();
        if input.is_empty() { return RavnResult::Error("empty".into()); }

        let segments = split_pipes(input);
        if segments.is_empty() { return RavnResult::Error("empty".into()); }

        // First segment: conditions
        let cond_str = segments[0].trim();
        let pairs = match parse_pairs(&self.engine, cond_str) {
            Ok(p) => p,
            Err(e) => return RavnResult::Error(e),
        };
        if pairs.is_empty() { return RavnResult::Error("no conditions".into()); }

        self.engine.rebuild();
        let refs: Vec<(&str, u32)> = pairs.iter().map(|(h, v)| (h.as_str(), *v)).collect();
        let mut eids = self.engine.query(&refs);

        // Process pipe stages
        for seg in &segments[1..] {
            let seg = seg.trim();
            let (cmd, args) = split_cmd(seg);
            match cmd {
                "count" => return RavnResult::Count(eids.len()),
                "follow" => {
                    if args.is_empty() {
                        return RavnResult::Error("follow: no himo specified".into());
                    }
                    let steps: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
                    eids = self.follow(&eids, &steps);
                }
                "select" => {
                    if args.is_empty() {
                        return RavnResult::Error("select: no fields specified".into());
                    }
                    let fields: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
                    let rows: Vec<(u32, Vec<Option<u32>>)> = eids.iter().map(|&eid| {
                        let vals: Vec<Option<u32>> = fields.iter()
                            .map(|f| self.engine.get(eid, f))
                            .collect();
                        (eid, vals)
                    }).collect();
                    return RavnResult::Values(rows);
                }
                "get" => {
                    if args.is_empty() {
                        return RavnResult::Error("get: no himo specified".into());
                    }
                    let himo = args[0].as_str();
                    let rows: Vec<(u32, Vec<Option<u32>>)> = eids.iter().map(|&eid| {
                        (eid, vec![self.engine.get(eid, himo)])
                    }).collect();
                    return RavnResult::Values(rows);
                }
                _ => return RavnResult::Error(format!("unknown command: {cmd}")),
            }
        }

        RavnResult::Entities(eids)
    }
}

fn split_pipes(input: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    for c in input.chars() {
        if c == '"' { in_quote = !in_quote; }
        if c == '|' && !in_quote {
            result.push(current.clone());
            current.clear();
        } else {
            current.push(c);
        }
    }
    if !current.is_empty() {
        result.push(current);
    }
    result
}

fn split_cmd(seg: &str) -> (&str, Vec<String>) {
    let parts: Vec<&str> = seg.split_whitespace().collect();
    if parts.is_empty() { return ("", vec![]); }
    let cmd = parts[0];
    let args: Vec<String> = parts[1..].iter().map(|s| s.to_string()).collect();
    (cmd, args)
}

fn parse_pairs(eng: &Engine, input: &str) -> Result<Vec<(String, u32)>, String> {
    let tokens = parse_kv_tokens(input);
    let mut result = Vec::new();
    for (himo, val) in &tokens {
        let v = resolve_value(eng, val)?;
        result.push((himo.clone(), v));
    }
    Ok(result)
}

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
        chars.next();

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
    use crate::{Engine, HimoType};

    fn setup(name: &str) -> (Arc<Engine>, Ravn) {
        let path = format!("/tmp/enchu_ravn_{name}.db");
        let _ = std::fs::remove_file(&path);
        let mut eng = Engine::create(&path).unwrap();

        eng.define_himo("type", HimoType::Value, 10);
        eng.define_himo("region", HimoType::Value, 10);
        eng.define_himo("country", HimoType::Value, 10);
        eng.define_himo("name", HimoType::Symbol, 0);
        eng.define_himo("parent", HimoType::Ref, 0);
        eng.define_himo("manager", HimoType::Ref, 0);

        // country: Japan(eid=0)
        let japan = eng.entity();
        eng.tie(japan, "type", 1);
        eng.tie_text(japan, "name", "Japan");

        // region: Kanto(eid=1), parent → Japan
        let kanto = eng.entity();
        eng.tie(kanto, "type", 2);
        eng.tie_text(kanto, "name", "Kanto");
        eng.tie(kanto, "parent", japan);

        // region: Kansai(eid=2), parent → Japan
        let kansai = eng.entity();
        eng.tie(kansai, "type", 2);
        eng.tie_text(kansai, "name", "Kansai");
        eng.tie(kansai, "parent", japan);

        // manager: Tanaka(eid=3)
        let tanaka = eng.entity();
        eng.tie(tanaka, "type", 3);
        eng.tie_text(tanaka, "name", "Tanaka");
        eng.tie(tanaka, "region", 1); // store kanto's eid as region value? No. Use parent ref.

        // dept(eid=4), manager → Tanaka, parent → Kanto
        let dept = eng.entity();
        eng.tie(dept, "type", 4);
        eng.tie(dept, "manager", tanaka);
        eng.tie(dept, "parent", kanto);

        eng.rebuild();
        let eng = Arc::new(eng);
        let ravn = Ravn::new(Arc::clone(&eng));
        (eng, ravn)
    }

    #[test]
    fn path_single_step() {
        let (eng, ravn) = setup("path1");
        // dept(4) → manager → Tanaka(3)
        assert_eq!(ravn.path(4, &["manager"]), Some(3));
        // get Tanaka's type
        assert_eq!(ravn.path(3, &["type"]), Some(3));
    }

    #[test]
    fn path_multi_step() {
        let (_eng, ravn) = setup("path2");
        // dept(4) → parent(=kanto=1) → parent(=japan=0) → type(=1)
        assert_eq!(ravn.path(4, &["parent", "parent", "type"]), Some(1));
    }

    #[test]
    fn path_text_last_step() {
        let (_eng, ravn) = setup("path_text");
        // dept(4) → manager(=tanaka=3) → name → "Tanaka"
        let text = ravn.path_text(4, &["manager", "name"]);
        assert_eq!(text, Some(b"Tanaka".to_vec()));
    }

    #[test]
    fn path_missing() {
        let (_eng, ravn) = setup("path_miss");
        assert_eq!(ravn.path(4, &["nonexistent"]), None);
        assert_eq!(ravn.path(4, &["parent", "nonexistent"]), None);
    }

    #[test]
    fn follow_basic() {
        let (_eng, ravn) = setup("follow");
        // kanto(1), kansai(2) → parent → japan(0)
        let result = ravn.follow(&[1, 2], &["parent"]);
        assert_eq!(result, vec![0, 0]);
    }

    #[test]
    fn follow_multi_step() {
        let (_eng, ravn) = setup("follow_multi");
        // dept(4) → parent → kanto(1) → parent → japan(0)
        let result = ravn.follow(&[4], &["parent", "parent"]);
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn select_basic() {
        let (_eng, ravn) = setup("select");
        // type=2 → kanto(1), kansai(2)
        let rows = ravn.select(&[("type", 2)], &["parent"]);
        assert_eq!(rows.len(), 2);
        for (_, vals) in &rows {
            assert_eq!(vals[0], Some(0)); // parent = japan
        }
    }

    #[test]
    fn exec_count() {
        let (_eng, ravn) = setup("exec_count");
        match ravn.exec("type:2 | count") {
            RavnResult::Count(2) => {}
            other => panic!("expected Count(2), got {other:?}"),
        }
    }
}
