use std::sync::Arc;
use crate::engine::Engine;

pub struct Ravn {
    engine: Arc<Engine>,
}

pub enum RavnResult {
    Entities(Vec<enchudb_oplog::EntityId>),
    Count(usize),
    Values(Vec<(enchudb_oplog::EntityId, Vec<Option<u32>>)>),
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

    pub fn path(&self, eid: enchudb_oplog::EntityId, steps: &[&str]) -> Option<u32> {
        if steps.is_empty() { return None; }
        let mut current: enchudb_oplog::EntityId = eid;
        for (i, step) in steps.iter().enumerate() {
            let val = self.engine.get(current, step)?;
            if i < steps.len() - 1 {
                current = val as enchudb_oplog::EntityId;
            } else {
                return Some(val);
            }
        }
        None
    }

    pub fn path_text(&self, eid: enchudb_oplog::EntityId, steps: &[&str]) -> Option<Vec<u8>> {
        if steps.is_empty() { return None; }
        let mut current: enchudb_oplog::EntityId = eid;
        for (i, step) in steps.iter().enumerate() {
            if i < steps.len() - 1 {
                current = self.engine.get(current, step)? as enchudb_oplog::EntityId;
            } else {
                return self.engine.get_text(current, step).map(|b| b.to_vec());
            }
        }
        None
    }

    pub fn follow(&self, start: &[enchudb_oplog::EntityId], steps: &[&str]) -> Vec<enchudb_oplog::EntityId> {
        let mut current: Vec<enchudb_oplog::EntityId> = start.to_vec();
        for step in steps {
            let mut next = Vec::new();
            for &eid in &current {
                if let Some(val) = self.engine.get(eid, step) {
                    next.push(val as enchudb_oplog::EntityId);
                }
            }
            current = next;
        }
        current
    }

    /// v31: 逆方向 tie 辿り。
    pub fn reverse_follow(&self, start: &[enchudb_oplog::EntityId], himo: &str) -> Vec<enchudb_oplog::EntityId> {
        let mut out = Vec::new();
        for &e in start {
            // pull_raw は "himo に値 e を持つ全 entity" を返す(EntityId)。
            // tie_ref は target_eid を u32 値として格納するので、u64 eid の local 部分で引く。
            let pulled = self.engine.pull_raw(himo, enchudb_oplog::eid_local(e));
            out.extend(pulled);
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    /// v31: 深さ制限付き BFS。`himo` を繰り返し forward で辿る。
    pub fn bfs(&self, start: &[enchudb_oplog::EntityId], himo: &str, max_depth: u32) -> Vec<(u32, Vec<enchudb_oplog::EntityId>)> {
        use std::collections::HashSet;
        let mut visited: HashSet<enchudb_oplog::EntityId> = start.iter().copied().collect();
        let mut result = vec![(0u32, start.to_vec())];
        let mut frontier: Vec<enchudb_oplog::EntityId> = start.to_vec();
        for d in 1..=max_depth {
            let next = self.follow(&frontier, &[himo]);
            let fresh: Vec<enchudb_oplog::EntityId> = next.into_iter().filter(|e| visited.insert(*e)).collect();
            if fresh.is_empty() { break; }
            result.push((d, fresh.clone()));
            frontier = fresh;
        }
        result
    }

    /// v31: entity 集合を「`himo == value` を満たすもの」だけに絞る。
    pub fn filter_by(&self, eids: &[enchudb_oplog::EntityId], himo: &str, value: u32) -> Vec<enchudb_oplog::EntityId> {
        eids.iter()
            .filter(|&&e| self.engine.get(e, himo) == Some(value))
            .copied()
            .collect()
    }

    /// v31: テキスト値でフィルタ(Symbol 型 himo 用)。
    pub fn filter_by_text(&self, eids: &[enchudb_oplog::EntityId], himo: &str, text: &str) -> Vec<enchudb_oplog::EntityId> {
        let Some(vid) = self.engine.vocab_id(text) else { return Vec::new(); };
        self.filter_by(eids, himo, vid)
    }

    /// v31: entity 集合から himo の値(u32)を抽出。
    /// None は除外。
    pub fn extract(&self, eids: &[enchudb_oplog::EntityId], himo: &str) -> Vec<u32> {
        eids.iter()
            .filter_map(|&e| self.engine.get(e, himo))
            .collect()
    }

    /// v31: テキスト抽出。
    pub fn extract_text(&self, eids: &[enchudb_oplog::EntityId], himo: &str) -> Vec<Vec<u8>> {
        eids.iter()
            .filter_map(|&e| self.engine.get_text(e, himo).map(|b| b.to_vec()))
            .collect()
    }

    /// v31: content 抽出。
    pub fn extract_content(&self, eids: &[enchudb_oplog::EntityId], key: &str) -> Vec<Vec<u8>> {
        eids.iter()
            .filter_map(|&e| self.engine.get_content(e, key).map(|b| b.to_vec()))
            .collect()
    }

    pub fn select(&self, conds: &[(&str, u32)], fields: &[&str]) -> Vec<(enchudb_oplog::EntityId, Vec<Option<u32>>)> {
        let eids = self.engine.query(conds);
        eids.iter().map(|&eid| {
            let values: Vec<Option<u32>> = fields.iter()
                .map(|f| self.engine.get(eid, f))
                .collect();
            (eid, values)
        }).collect()
    }

    pub fn select_text(&self, conds: &[(&str, u32)], fields: &[&str]) -> Vec<(enchudb_oplog::EntityId, Vec<Option<Vec<u8>>>)> {
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
                // v31: 逆方向 tie 辿り。`reverse parent` で parent 指している entity 集合。
                "reverse" | "rev" => {
                    if args.is_empty() {
                        return RavnResult::Error("reverse: no himo specified".into());
                    }
                    eids = self.reverse_follow(&eids, &args[0]);
                }
                // v31: フィルタ。`where himo:value` で絞り込み。
                "where" => {
                    // args 形式: "himo:value" or "himo:\"text\""
                    let joined = args.join(" ");
                    let pairs = match parse_pairs(&self.engine, &joined) {
                        Ok(p) => p,
                        Err(e) => return RavnResult::Error(format!("where: {}", e)),
                    };
                    for (himo, value) in pairs {
                        eids = self.filter_by(&eids, &himo, value);
                    }
                }
                // v31: 深さ制限 BFS。`bfs <himo> <depth>` で全レベル展開(flat)。
                "bfs" => {
                    if args.len() < 2 {
                        return RavnResult::Error("bfs: usage `bfs <himo> <depth>`".into());
                    }
                    let depth: u32 = match args[1].parse() {
                        Ok(d) => d,
                        Err(_) => return RavnResult::Error("bfs: invalid depth".into()),
                    };
                    let levels = self.bfs(&eids, &args[0], depth);
                    eids = levels.into_iter().flat_map(|(_, v)| v).collect();
                }
                "select" => {
                    if args.is_empty() {
                        return RavnResult::Error("select: no fields specified".into());
                    }
                    let fields: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
                    let rows: Vec<(enchudb_oplog::EntityId, Vec<Option<u32>>)> = eids.iter().map(|&eid| {
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
                    let rows: Vec<(enchudb_oplog::EntityId, Vec<Option<u32>>)> = eids.iter().map(|&eid| {
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
        let mut eng = Engine::create_standalone(&path).unwrap();

        eng.define_himo("type", HimoType::Number, 10);
        eng.define_himo("region", HimoType::Number, 10);
        eng.define_himo("country", HimoType::Number, 10);
        eng.define_himo("name", HimoType::Tag, 0);
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
        eng.tie_ref(kanto, "parent", japan);

        // region: Kansai(eid=2), parent → Japan
        let kansai = eng.entity();
        eng.tie(kansai, "type", 2);
        eng.tie_text(kansai, "name", "Kansai");
        eng.tie_ref(kansai, "parent", japan);

        // manager: Tanaka(eid=3)
        let tanaka = eng.entity();
        eng.tie(tanaka, "type", 3);
        eng.tie_text(tanaka, "name", "Tanaka");
        eng.tie(tanaka, "region", 1); // store kanto's eid as region value? No. Use parent ref.

        // dept(eid=4), manager → Tanaka, parent → Kanto
        let dept = eng.entity();
        eng.tie(dept, "type", 4);
        eng.tie_ref(dept, "manager", tanaka);
        eng.tie_ref(dept, "parent", kanto);

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

    // ──── v31 additions ────

    #[test]
    fn reverse_follow_finds_children() {
        let (_eng, ravn) = setup("reverse");
        // parent に japan(0) を持つ entity を逆引き → kanto(1), kansai(2)
        let children = ravn.reverse_follow(&[0], "parent");
        assert_eq!(children, vec![1, 2]);
    }

    #[test]
    fn bfs_explores_by_depth() {
        let (_eng, ravn) = setup("bfs");
        // dept(4) -> parent -> kanto(1) -> parent -> japan(0)
        let levels = ravn.bfs(&[4], "parent", 3);
        assert_eq!(levels[0].0, 0);
        assert_eq!(levels[0].1, vec![4]);
        assert_eq!(levels[1].0, 1);
        assert_eq!(levels[1].1, vec![1]); // kanto
        assert_eq!(levels[2].0, 2);
        assert_eq!(levels[2].1, vec![0]); // japan
    }

    #[test]
    fn filter_by_value() {
        let (_eng, ravn) = setup("filter");
        // entity 0..=4 のうち type=2 のもの
        let all = vec![0u64, 1, 2, 3, 4];
        let regions = ravn.filter_by(&all, "type", 2);
        assert_eq!(regions, vec![1, 2]);
    }

    #[test]
    fn filter_by_text_finds_named() {
        let (_eng, ravn) = setup("filter_text");
        let all = vec![0u64, 1, 2, 3];
        let tanaka = ravn.filter_by_text(&all, "name", "Tanaka");
        assert_eq!(tanaka, vec![3]);
    }

    #[test]
    fn extract_values() {
        let (_eng, ravn) = setup("extract");
        // kanto(1), kansai(2) の parent を抽出 → [japan, japan]
        let parents = ravn.extract(&[1, 2], "parent");
        assert_eq!(parents, vec![0, 0]);
    }

    #[test]
    fn exec_reverse_pipe() {
        let (_eng, ravn) = setup("exec_rev");
        // type=1 (japan) に parent 指している entity → kanto, kansai
        match ravn.exec("type:1 | reverse parent | count") {
            RavnResult::Count(2) => {}
            other => panic!("expected Count(2), got {other:?}"),
        }
    }

    #[test]
    fn exec_where_pipe() {
        let (_eng, ravn) = setup("exec_where");
        // type=2 (regions) かつ name=Kanto → kanto のみ
        match ravn.exec(r#"type:2 | where name:"Kanto" | count"#) {
            RavnResult::Count(1) => {}
            other => panic!("expected Count(1), got {other:?}"),
        }
    }

    #[test]
    fn exec_bfs_pipe() {
        let (_eng, ravn) = setup("exec_bfs");
        // dept(type=4) から parent を 2 段辿り → kanto, japan
        match ravn.exec("type:4 | bfs parent 2 | count") {
            RavnResult::Count(n) => { assert!(n >= 2, "expected >= 2 in bfs, got {}", n); }
            other => panic!("expected Count, got {other:?}"),
        }
    }

    #[test]
    fn extract_text_names() {
        let (_eng, ravn) = setup("extract_text");
        let names = ravn.extract_text(&[0, 1, 2, 3], "name");
        let names_s: Vec<String> = names.iter()
            .map(|b| String::from_utf8_lossy(b).to_string())
            .collect();
        assert_eq!(names_s, vec!["Japan", "Kanto", "Kansai", "Tanaka"]);
    }
}
