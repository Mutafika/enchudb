//! 円柱クエリ言語 — 紐を並べる、集計は pipe で。
//!
//! ## 検索
//!   age:30 city:"東京"                → 重なった entity 一覧（AND）
//!   age:20..30                         → 範囲（inclusive）
//!   age:20..30 dept:0                  → 範囲 + 等値の AND
//!
//! ## 集計（pipe で繋ぐ）
//!   age:30 | count                     → 件数
//!   age:30 | sum salary                → 合計
//!   age:30 | avg salary                → 平均（整数除算）
//!   age:30 | min age                   → 最小
//!   age:30 | max age                   → 最大
//!   age:30 | distinct dept             → ユニーク値
//!
//! ## グループ化（| group の後に集計を繋ぐ）
//!   age:30 | group dept | count        → 部署ごとの件数
//!   age:30 | group dept | sum salary   → 部署ごとの給与合計
//!   age:30 | group dept | avg salary   → 部署ごとの平均給与
//!   age:30 | group dept | min age      → 部署ごとの最年少
//!   age:30 | group dept | max age      → 部署ごとの最年長
//!
//! ## 編集
//!   + age:30 city:"田中"               → 新規 entity に紐をぶら下げる
//!   ~ 42 age:31 city:"福岡"            → 既存 entity の紐を置換 (in-place mutate)
//!   - 42                               → entity 削除
//!
//! Symbol 紐(tie_text)で group / distinct すると、結果の key は自動で text 表示される。

use crate::engine::Engine;
use crate::HimoType;

#[derive(Debug, Clone, PartialEq)]
pub enum GroupKey {
    Num(u32),
    Text(String),
}

impl std::fmt::Display for GroupKey {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            GroupKey::Num(n) => write!(f, "{n}"),
            GroupKey::Text(s) => write!(f, "{s}"),
        }
    }
}

#[derive(Debug)]
pub enum QueryResult {
    Entities(Vec<enchudb_oplog::EntityId>),
    Count(usize),
    Sum(u64),
    Avg(Option<u64>),
    Min(Option<u32>),
    Max(Option<u32>),
    Distinct(Vec<GroupKey>),
    GroupCount(Vec<(GroupKey, u32)>),
    GroupSum(Vec<(GroupKey, u64)>),
    GroupAvg(Vec<(GroupKey, u64)>),
    GroupMin(Vec<(GroupKey, u32)>),
    GroupMax(Vec<(GroupKey, u32)>),
    Inserted(enchudb_oplog::EntityId),
    Updated(enchudb_oplog::EntityId),
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
            QueryResult::Sum(n) => write!(f, "{n}"),
            QueryResult::Avg(None) | QueryResult::Min(None) | QueryResult::Max(None) => write!(f, "(no values)"),
            QueryResult::Avg(Some(n)) => write!(f, "{n}"),
            QueryResult::Min(Some(n)) | QueryResult::Max(Some(n)) => write!(f, "{n}"),
            QueryResult::Distinct(v) if v.is_empty() => write!(f, "(empty)"),
            QueryResult::Distinct(v) => {
                for (i, k) in v.iter().enumerate() {
                    if i > 0 { write!(f, " ")?; }
                    write!(f, "{k}")?;
                }
                Ok(())
            }
            QueryResult::GroupCount(v) => write_groups(f, v),
            QueryResult::GroupSum(v) => write_groups(f, v),
            QueryResult::GroupAvg(v) => write_groups(f, v),
            QueryResult::GroupMin(v) => write_groups(f, v),
            QueryResult::GroupMax(v) => write_groups(f, v),
            QueryResult::Inserted(id) => write!(f, "+{id}"),
            QueryResult::Updated(id) => write!(f, "~{id}"),
            QueryResult::Deleted => write!(f, "ok"),
            QueryResult::Error(e) => write!(f, "! {e}"),
        }
    }
}

fn write_groups<V: std::fmt::Display>(
    f: &mut std::fmt::Formatter,
    v: &[(GroupKey, V)],
) -> std::fmt::Result {
    if v.is_empty() { return write!(f, "(empty)"); }
    for (i, (k, val)) in v.iter().enumerate() {
        if i > 0 { write!(f, " ")?; }
        write!(f, "{k}={val}")?;
    }
    Ok(())
}

pub fn execute(eng: &mut Engine, input: &str) -> QueryResult {
    let input = input.trim();
    if input.is_empty() { return QueryResult::Error("empty".into()); }

    if let Some(rest) = input.strip_prefix('+') {
        return exec_insert(eng, rest.trim());
    }
    if let Some(rest) = input.strip_prefix('~') {
        return exec_update(eng, rest.trim());
    }
    if let Some(rest) = input.strip_prefix('-') {
        return exec_delete(eng, rest.trim());
    }

    // パイプ分割: "cond | stage1 | stage2 ..."
    let mut parts = input.split('|').map(str::trim);
    let cond_str = parts.next().unwrap_or("");
    let stages: Vec<&str> = parts.collect();

    let conds = match parse_conds(eng, cond_str) {
        Ok(c) => c,
        Err(e) => return QueryResult::Error(e),
    };
    if conds.is_empty() { return QueryResult::Error("no conditions".into()); }

    eng.rebuild();
    let eids = eval_conds(eng, &conds);

    apply_stages(eng, eids, &stages)
}

// ────────────── 条件 ──────────────

#[derive(Debug)]
enum Cond {
    Eq(String, u32),
    Range(String, u32, u32),
}

fn parse_conds(eng: &Engine, input: &str) -> Result<Vec<Cond>, String> {
    let tokens = parse_kv_tokens(input);
    let mut result = Vec::new();
    for (himo, val) in tokens {
        result.push(parse_cond(eng, &himo, &val)?);
    }
    Ok(result)
}

fn parse_cond(eng: &Engine, himo: &str, val: &str) -> Result<Cond, String> {
    if val.starts_with('"') && val.ends_with('"') && val.len() >= 2 {
        let text = &val[1..val.len() - 1];
        let id = eng.vocab_id(text).ok_or_else(|| format!("unknown text: {text}"))?;
        return Ok(Cond::Eq(himo.to_string(), id));
    }
    if let Some(idx) = val.find("..") {
        let lo_str = &val[..idx];
        let hi_str = &val[idx + 2..];
        let lo = lo_str.parse::<u32>().map_err(|_| format!("invalid range lo: {lo_str}"))?;
        let hi = hi_str.parse::<u32>().map_err(|_| format!("invalid range hi: {hi_str}"))?;
        if lo > hi { return Err(format!("range lo > hi: {lo}..{hi}")); }
        return Ok(Cond::Range(himo.to_string(), lo, hi));
    }
    let v = val.parse::<u32>().map_err(|_| format!("invalid: {val}"))?;
    Ok(Cond::Eq(himo.to_string(), v))
}

fn eval_conds(eng: &Engine, conds: &[Cond]) -> Vec<enchudb_oplog::EntityId> {
    // Eq だけ集めて query() に流す。Range は post-filter。
    let eq_pairs: Vec<(&str, u32)> = conds.iter().filter_map(|c| match c {
        Cond::Eq(h, v) => Some((h.as_str(), *v)),
        _ => None,
    }).collect();

    let mut eids: Vec<enchudb_oplog::EntityId> = if !eq_pairs.is_empty() {
        eng.query(&eq_pairs)
    } else {
        // 全部 Range のとき、最初の Range で起点
        let first = conds.iter().find_map(|c| match c {
            Cond::Range(h, lo, hi) => Some((h, lo, hi)),
            _ => None,
        });
        match first {
            Some((h, lo, hi)) => eng.pull_range(h, *lo, *hi),
            None => return vec![],
        }
    };

    // Range 条件で絞る。eq_pairs が空なら最初の Range は起点として消費済み。
    let skip_first_range = eq_pairs.is_empty();
    let mut skipped = false;
    for cond in conds {
        if let Cond::Range(h, lo, hi) = cond {
            if skip_first_range && !skipped { skipped = true; continue; }
            let h = h.clone();
            let lo = *lo; let hi = *hi;
            eids.retain(|&eid| eng.get(eid, &h).map_or(false, |v| v >= lo && v <= hi));
        }
    }
    eids
}

// ────────────── ステージ適用 ──────────────

fn apply_stages(eng: &Engine, eids: Vec<enchudb_oplog::EntityId>, stages: &[&str]) -> QueryResult {
    if stages.is_empty() {
        return QueryResult::Entities(eids);
    }

    // group は intermediate state を作る。group の直後の stage は per-group aggregation。
    let mut idx = 0;
    while idx < stages.len() {
        let stage = stages[idx];
        let (op, arg) = split_stage(stage);
        match op {
            "group" => {
                let group_himo = arg.ok_or("group: needs himo");
                let group_himo = match group_himo {
                    Ok(h) => h,
                    Err(e) => return QueryResult::Error(e.into()),
                };
                // 次のステージで集計を選ぶ
                let next = stages.get(idx + 1).copied().unwrap_or("count");
                let (next_op, next_arg) = split_stage(next);
                let result = apply_group(eng, &eids, group_himo, next_op, next_arg);
                // group は 2 stage 消費（自分 + 直後）。直後がなければ count 扱い。
                if stages.get(idx + 1).is_some() { idx += 2; } else { idx += 1; }
                if idx < stages.len() {
                    return QueryResult::Error(format!("unexpected stage after group: {}", stages[idx]));
                }
                return result;
            }
            _ => {
                if idx != stages.len() - 1 {
                    return QueryResult::Error(format!("stage `{op}` must be last (only `group` chains)"));
                }
                return apply_simple(eng, &eids, op, arg);
            }
        }
    }
    QueryResult::Entities(eids)
}

fn split_stage(stage: &str) -> (&str, Option<&str>) {
    let mut it = stage.split_whitespace();
    let op = it.next().unwrap_or("");
    let arg = it.next();
    (op, arg)
}

fn apply_simple(eng: &Engine, eids: &[enchudb_oplog::EntityId], op: &str, arg: Option<&str>) -> QueryResult {
    match op {
        "count" => QueryResult::Count(eids.len()),
        "sum" => match arg {
            Some(h) => QueryResult::Sum(eng.sum(h, eids)),
            None => QueryResult::Error("sum: needs himo".into()),
        },
        "avg" => match arg {
            Some(h) => QueryResult::Avg(eng.avg(h, eids)),
            None => QueryResult::Error("avg: needs himo".into()),
        },
        "min" => match arg {
            Some(h) => QueryResult::Min(eng.min(h, eids)),
            None => QueryResult::Error("min: needs himo".into()),
        },
        "max" => match arg {
            Some(h) => QueryResult::Max(eng.max(h, eids)),
            None => QueryResult::Error("max: needs himo".into()),
        },
        "distinct" => match arg {
            Some(h) => {
                let vals = eng.distinct(h, eids);
                let ht = eng.himo_type(h);
                let is_text = ht == Some(HimoType::Tag) || ht == Some(HimoType::Leaf);
                let keys = vals.into_iter().map(|v| make_key(eng, v, is_text)).collect();
                QueryResult::Distinct(keys)
            }
            None => QueryResult::Error("distinct: needs himo".into()),
        },
        "" => QueryResult::Entities(eids.to_vec()),
        other => QueryResult::Error(format!("unknown stage: {other}")),
    }
}

fn apply_group(
    eng: &Engine,
    eids: &[enchudb_oplog::EntityId],
    group_himo: &str,
    agg_op: &str,
    agg_arg: Option<&str>,
) -> QueryResult {
    let ht = eng.himo_type(group_himo);
    let is_text = ht == Some(HimoType::Tag) || ht == Some(HimoType::Leaf);
    let to_key = |v: u32| make_key(eng, v, is_text);

    match agg_op {
        "count" | "" => {
            let v = eng.group_count(group_himo, eids);
            QueryResult::GroupCount(v.into_iter().map(|(k, n)| (to_key(k), n)).collect())
        }
        "sum" => match agg_arg {
            Some(h) => {
                let v = eng.group_sum(group_himo, h, eids);
                QueryResult::GroupSum(v.into_iter().map(|(k, n)| (to_key(k), n)).collect())
            }
            None => QueryResult::Error("group ... | sum: needs himo".into()),
        },
        "avg" => match agg_arg {
            Some(h) => {
                let v = eng.group_avg(group_himo, h, eids);
                QueryResult::GroupAvg(v.into_iter().map(|(k, n)| (to_key(k), n)).collect())
            }
            None => QueryResult::Error("group ... | avg: needs himo".into()),
        },
        "min" => match agg_arg {
            Some(h) => {
                let v = eng.group_min(group_himo, h, eids);
                QueryResult::GroupMin(v.into_iter().map(|(k, n)| (to_key(k), n)).collect())
            }
            None => QueryResult::Error("group ... | min: needs himo".into()),
        },
        "max" => match agg_arg {
            Some(h) => {
                let v = eng.group_max(group_himo, h, eids);
                QueryResult::GroupMax(v.into_iter().map(|(k, n)| (to_key(k), n)).collect())
            }
            None => QueryResult::Error("group ... | max: needs himo".into()),
        },
        other => QueryResult::Error(format!("unknown group aggregation: {other}")),
    }
}

fn make_key(eng: &Engine, v: u32, is_text: bool) -> GroupKey {
    if is_text {
        let bytes = eng.vocab_text(v);
        match std::str::from_utf8(bytes) {
            Ok(s) => GroupKey::Text(s.to_string()),
            Err(_) => GroupKey::Num(v),
        }
    } else {
        GroupKey::Num(v)
    }
}

// ────────────── insert / delete ──────────────

fn exec_insert(eng: &mut Engine, input: &str) -> QueryResult {
    let tokens = parse_kv_tokens(input);
    if tokens.is_empty() { return QueryResult::Error("nothing to insert".into()); }

    let eid = eng.entity();
    for (himo, val) in &tokens {
        if val.starts_with('"') && val.ends_with('"') && val.len() >= 2 {
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

fn exec_update(eng: &mut Engine, input: &str) -> QueryResult {
    // "<eid> himo:val [himo:val ...]"
    let mut it = input.splitn(2, char::is_whitespace);
    let eid_str = match it.next() { Some(s) if !s.is_empty() => s, _ => return QueryResult::Error("update: missing eid".into()) };
    let rest = it.next().unwrap_or("").trim();
    let eid: enchudb_oplog::EntityId = match eid_str.parse() {
        Ok(e) => e,
        Err(_) => return QueryResult::Error(format!("update: invalid eid: {eid_str}")),
    };
    let tokens = parse_kv_tokens(rest);
    if tokens.is_empty() { return QueryResult::Error("update: nothing to set".into()); }

    for (himo, val) in &tokens {
        if val.starts_with('"') && val.ends_with('"') && val.len() >= 2 {
            let text = &val[1..val.len() - 1];
            eng.tie_text(eid, himo, text);
        } else {
            match val.parse::<u32>() {
                Ok(v) => eng.tie(eid, himo, v),
                Err(_) => eng.tie_text(eid, himo, val),
            }
        }
    }
    QueryResult::Updated(eid)
}

fn exec_delete(eng: &mut Engine, input: &str) -> QueryResult {
    match input.parse::<enchudb_oplog::EntityId>() {
        Ok(eid) => { eng.delete(eid); QueryResult::Deleted }
        Err(_) => QueryResult::Error(format!("invalid id: {input}")),
    }
}

// ────────────── tokenizer ──────────────

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

#[cfg(test)]
mod tests {
    use super::*;

    fn setup(name: &str) -> Engine {
        let dir = format!("/tmp/enchu_v24_ql_{name}.db");
        let _ = std::fs::remove_file(&dir);
        let mut eng = Engine::create_standalone(&dir).unwrap();

        for i in 0..10u32 {
            let e = eng.entity();
            eng.tie(e, "age", 20 + i);
            eng.tie(e, "dept", i % 3);
            eng.tie(e, "salary", 100 + i * 10);
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
    fn update_replaces_value() {
        let mut eng = setup("update");
        let eid = match execute(&mut eng, "age:20") {
            QueryResult::Entities(v) => v[0],
            _ => panic!(),
        };
        // age:20 → age:99 に置換、city も追加
        let r = execute(&mut eng, &format!(r#"~ {eid} age:99 city:"福岡""#));
        assert!(matches!(r, QueryResult::Updated(_)));
        // 元の age:20 は消えてる
        match execute(&mut eng, "age:20 | count") {
            QueryResult::Count(0) => {}
            other => panic!("expected 0, got {other:?}"),
        }
        // age:99 でヒット
        match execute(&mut eng, "age:99 | count") {
            QueryResult::Count(1) => {}
            other => panic!("got {other:?}"),
        }
        // city:"福岡" でも引ける
        match execute(&mut eng, r#"city:"福岡" | count"#) {
            QueryResult::Count(1) => {}
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn update_invalid_eid() {
        let mut eng = setup("update_invalid");
        let r = execute(&mut eng, "~ notanumber age:99");
        assert!(matches!(r, QueryResult::Error(_)));
    }

    #[test]
    fn update_no_kvs() {
        let mut eng = setup("update_empty");
        let r = execute(&mut eng, "~ 0");
        assert!(matches!(r, QueryResult::Error(_)));
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

    // ── 集計 ──

    #[test]
    fn sum_simple() {
        let mut eng = setup("sum_simple");
        match execute(&mut eng, "dept:0 | sum salary") {
            // dept=0: i=0,3,6,9 → salary=100,130,160,190 = 580
            QueryResult::Sum(580) => {}
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn avg_simple() {
        let mut eng = setup("avg_simple");
        match execute(&mut eng, "dept:0 | avg salary") {
            QueryResult::Avg(Some(145)) => {} // 580/4 = 145
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn min_max() {
        let mut eng = setup("min_max");
        assert!(matches!(execute(&mut eng, "dept:0 | min age"), QueryResult::Min(Some(20))));
        assert!(matches!(execute(&mut eng, "dept:0 | max age"), QueryResult::Max(Some(29))));
    }

    #[test]
    fn distinct_numeric() {
        let mut eng = setup("distinct_num");
        match execute(&mut eng, "age:20..29 | distinct dept") {
            QueryResult::Distinct(v) => {
                assert_eq!(v.len(), 3);
                let nums: Vec<u32> = v.iter().filter_map(|k| match k {
                    GroupKey::Num(n) => Some(*n), _ => None,
                }).collect();
                assert!(nums.contains(&0));
                assert!(nums.contains(&1));
                assert!(nums.contains(&2));
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn distinct_text() {
        let mut eng = setup("distinct_text");
        match execute(&mut eng, "age:20..29 | distinct city") {
            QueryResult::Distinct(v) => {
                let texts: Vec<String> = v.iter().filter_map(|k| match k {
                    GroupKey::Text(s) => Some(s.clone()), _ => None,
                }).collect();
                assert_eq!(texts.len(), 2);
                assert!(texts.contains(&"東京".to_string()));
                assert!(texts.contains(&"大阪".to_string()));
            }
            other => panic!("got {other:?}"),
        }
    }

    // ── group ──

    #[test]
    fn group_count() {
        let mut eng = setup("group_count");
        match execute(&mut eng, "age:20..29 | group dept | count") {
            QueryResult::GroupCount(v) => {
                assert_eq!(v.len(), 3);
                // dept=0,1,2 で i=0..9 を 3 で割った余り → 各 4,3,3
                let total: u32 = v.iter().map(|(_, n)| n).sum();
                assert_eq!(total, 10);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn group_count_default() {
        // | group X だけで count 扱いになるか
        let mut eng = setup("group_default");
        match execute(&mut eng, "age:20..29 | group dept") {
            QueryResult::GroupCount(v) => assert_eq!(v.len(), 3),
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn group_sum() {
        let mut eng = setup("group_sum");
        match execute(&mut eng, "age:20..29 | group dept | sum salary") {
            QueryResult::GroupSum(v) => {
                let total: u64 = v.iter().map(|(_, n)| n).sum();
                // salary = 100,110,120,...,190 → 1450
                assert_eq!(total, 1450);
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn group_by_text_himo() {
        // Symbol 紐 (city) で group して、key が text 化されるか
        let mut eng = setup("group_text");
        match execute(&mut eng, "age:20..29 | group city | count") {
            QueryResult::GroupCount(v) => {
                let texts: Vec<String> = v.iter().map(|(k, _)| format!("{k}")).collect();
                assert!(texts.contains(&"東京".to_string()));
                assert!(texts.contains(&"大阪".to_string()));
            }
            other => panic!("got {other:?}"),
        }
    }

    // ── range ──

    #[test]
    fn range_only() {
        let mut eng = setup("range_only");
        match execute(&mut eng, "age:22..25 | count") {
            QueryResult::Count(4) => {}
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn range_plus_eq() {
        let mut eng = setup("range_eq");
        // age:22..28 (7 件) で dept:0 → i=3,6 のみ
        match execute(&mut eng, "age:22..28 dept:0 | count") {
            QueryResult::Count(2) => {}
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn range_invalid() {
        let mut eng = setup("range_invalid");
        match execute(&mut eng, "age:30..20") {
            QueryResult::Error(_) => {}
            other => panic!("expected error, got {other:?}"),
        }
    }

    // ── 表示 ──

    #[test]
    fn display_group_count() {
        let mut eng = setup("display_group");
        let r = execute(&mut eng, "age:20..29 | group dept | count");
        let s = format!("{r}");
        // "0=4 1=3 2=3" 風の出力（順序は処理順）
        assert!(s.contains('='));
        assert_eq!(s.matches('=').count(), 3);
    }
}
