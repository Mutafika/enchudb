//! フィルタ DSL。
//!
//! # 設計
//!
//! - `Filter::value("field", v)` — 値一致（`pull_raw` 直引き、ns〜）
//! - `Filter::symbol("field", "s")` — 文字列一致（Vocabulary で ID 化 → `pull_raw`）
//! - `Filter::range("field", min..=max)` — 範囲（`pull_range`、連続バケット直引き）
//! - `Filter::any(vec![...])` — 値の OR（複数バケット concat）
//! - `a.and(b)` — AND（両方を候補集合の積）
//! - `a.or(b)` — OR（候補集合の和）
//! - `Filter::all()` — 全 entity（絞り込みなし）
//!
//! # 評価
//!
//! AND: 子の件数を索引の数で見積もり（値は bucket の数で O(1)、範囲は範囲の中の値の数に比例）、一番少ない子（Not / All
//! 以外）だけを引いて、残りの子は候補ごとに列の値を読んで確かめる（他の子の集合は作らない。Not の子も確かめるだけなので、
//! 全体の補集合を作らない）。
//! OR: 和集合。NOT（AND の外）: 生きている entity 全体との差。
//! 生きている entity の全部（All / Not の基底）は、要る時だけ 1 回作る（条件が値・範囲と、それを AND / OR したものだけなら作らない）。
//! 評価結果は**ソート済み EntityId (u64) の Vec**として返す（後段の集合演算と、BM25 の候補の二分探索のため）。

use enchudb::{Engine};
use enchudb_oplog::EntityId;
use std::cell::OnceCell;
use std::ops::RangeInclusive;

/// 評価の間の、生きている entity の全部（ソート済み）。最初に要った時に 1 回だけ作る。
pub(crate) struct Universe<'a> {
    db: &'a Engine,
    alive: OnceCell<Vec<EntityId>>,
}

impl<'a> Universe<'a> {
    pub(crate) fn new(db: &'a Engine) -> Self {
        Self { db, alive: OnceCell::new() }
    }

    fn alive(&self) -> &[EntityId] {
        self.alive.get_or_init(|| {
            let mut v = self.db.entities();
            v.sort_unstable();
            v
        })
    }
}

#[derive(Clone, Debug)]
pub enum Filter {
    /// 全 entity 許可（絞り込みなし）。
    All,
    /// 値一致。
    Value { field: String, value: u32 },
    /// 文字列一致（Vocabulary 経由）。
    Symbol { field: String, value: String },
    /// 範囲（inclusive）。
    Range { field: String, min: u32, max: u32 },
    /// 値の OR。
    AnyValue { field: String, values: Vec<u32> },
    /// 文字列の OR。
    AnySymbol { field: String, values: Vec<String> },
    /// AND（すべて満たす）。
    And(Vec<Filter>),
    /// OR（いずれか満たす）。
    Or(Vec<Filter>),
    /// NOT（補集合。alive な entity 全体との差）。
    Not(Box<Filter>),
}

impl Filter {
    pub fn all() -> Self { Filter::All }

    pub fn value(field: impl Into<String>, v: u32) -> Self {
        Filter::Value { field: field.into(), value: v }
    }

    pub fn symbol(field: impl Into<String>, v: impl Into<String>) -> Self {
        Filter::Symbol { field: field.into(), value: v.into() }
    }

    pub fn range(field: impl Into<String>, r: RangeInclusive<u32>) -> Self {
        Filter::Range { field: field.into(), min: *r.start(), max: *r.end() }
    }

    pub fn any_value(field: impl Into<String>, values: Vec<u32>) -> Self {
        Filter::AnyValue { field: field.into(), values }
    }

    pub fn any_symbol(field: impl Into<String>, values: Vec<String>) -> Self {
        Filter::AnySymbol { field: field.into(), values }
    }

    pub fn and(self, other: Filter) -> Self {
        match self {
            Filter::And(mut v) => { v.push(other); Filter::And(v) }
            _ => Filter::And(vec![self, other]),
        }
    }

    pub fn or(self, other: Filter) -> Self {
        match self {
            Filter::Or(mut v) => { v.push(other); Filter::Or(v) }
            _ => Filter::Or(vec![self, other]),
        }
    }

    pub fn not(self) -> Self { Filter::Not(Box::new(self)) }

    /// 絞り込みなしかどうか（search の fast path 判定用）。
    pub fn is_all(&self) -> bool { matches!(self, Filter::All) }

    /// フィルタを評価して、マッチする entity id のソート済み Vec を返す。
    /// 生きている entity の全部（Filter::All / Filter::Not の基底）は `u` が要る時だけ作る。
    pub(crate) fn evaluate(&self, u: &Universe<'_>) -> Vec<EntityId> {
        let db = u.db;
        match self {
            Filter::All => u.alive().to_vec(),

            Filter::Value { field, value } => pull_sorted(db, field, *value),

            Filter::Symbol { field, value } => {
                let Some(vid) = db.vocab_id(value) else { return Vec::new(); };
                pull_sorted(db, field, vid)
            }

            Filter::Range { field, min, max } => {
                let mut r = db.pull_range(field, *min, *max);
                r.sort_unstable();
                r.dedup();
                r
            }

            Filter::AnyValue { field, values } => {
                let mut out = Vec::new();
                for v in values {
                    out.extend(pull_sorted(db, field, *v));
                }
                out.sort_unstable();
                out.dedup();
                out
            }

            Filter::AnySymbol { field, values } => {
                let mut out = Vec::new();
                for s in values {
                    if let Some(vid) = db.vocab_id(s) {
                        out.extend(pull_sorted(db, field, vid));
                    }
                }
                out.sort_unstable();
                out.dedup();
                out
            }

            Filter::And(filters) => {
                // 種 = 見積もりが一番少ない子（Not / All は全体から引くので種にしない）。残りの子は候補ごとに列で確かめる
                let seed = filters.iter().enumerate()
                    .filter(|(_, f)| !matches!(f, Filter::Not(_) | Filter::All))
                    .min_by_key(|(_, f)| f.estimate(db))
                    .map(|(i, _)| i);
                let mut cur = match seed {
                    Some(i) => filters[i].evaluate(u),
                    None => u.alive().to_vec(),
                };
                let rest: Vec<Probe> = filters.iter().enumerate()
                    .filter(|&(i, _)| Some(i) != seed)
                    .map(|(_, f)| f.probe(db))
                    .collect();
                if !rest.is_empty() {
                    cur.retain(|&e| rest.iter().all(|p| p.holds(db, e)));
                }
                cur
            }

            Filter::Or(filters) => {
                let mut out = Vec::new();
                for f in filters {
                    out.extend(f.evaluate(u));
                }
                out.sort_unstable();
                out.dedup();
                out
            }

            Filter::Not(f) => {
                let exclude = f.evaluate(u);
                diff_sorted(u.alive(), &exclude)
            }
        }
    }

    /// 当たる entity の数の上からの見積もり（索引の数だけで、entity は読まない。AND の種を選ぶ用）。
    fn estimate(&self, db: &Engine) -> usize {
        let all = db.entity_count() as usize;
        let count = |field: &str, v: u64| himo(db, field).map_or(0, |h| db.count_value_by_id(h, v));
        match self {
            Filter::All | Filter::Not(_) => all,
            Filter::Value { field, value } => count(field, *value as u64),
            Filter::Symbol { field, value } => db.vocab_id(value).map_or(0, |vid| count(field, vid as u64)),
            Filter::Range { field, min, max } => {
                if min > max { 0 } else { himo(db, field).map_or(0, |h| db.count_range_by_id(h, *min as u64, *max as u64)) }
            }
            Filter::AnyValue { field, values } => values.iter().map(|&v| count(field, v as u64)).sum(),
            Filter::AnySymbol { field, values } => {
                values.iter().filter_map(|s| db.vocab_id(s)).map(|vid| count(field, vid as u64)).sum()
            }
            Filter::And(filters) => filters.iter().map(|f| f.estimate(db)).min().unwrap_or(all),
            Filter::Or(filters) => filters.iter().map(|f| f.estimate(db)).fold(0usize, |a, b| a.saturating_add(b)).min(all),
        }
    }

    /// 1 つの entity が当たるかを列の値で確かめる形に（紐の名前と語は先に引いておく）。
    fn probe(&self, db: &Engine) -> Probe {
        let eq = |field: &str, v: u64| himo(db, field).map_or(Probe::False, |h| Probe::Eq(h, v));
        let any = |field: &str, vs: Vec<u64>| match himo(db, field) {
            Some(h) if !vs.is_empty() => {
                let mut vs = vs;
                vs.sort_unstable();
                vs.dedup();
                Probe::In(h, vs)
            }
            _ => Probe::False,
        };
        match self {
            Filter::All => Probe::True,
            Filter::Value { field, value } => eq(field, *value as u64),
            Filter::Symbol { field, value } => db.vocab_id(value).map_or(Probe::False, |vid| eq(field, vid as u64)),
            Filter::Range { field, min, max } => {
                himo(db, field).map_or(Probe::False, |h| Probe::Range(h, *min as u64, *max as u64))
            }
            Filter::AnyValue { field, values } => any(field, values.iter().map(|&v| v as u64).collect()),
            Filter::AnySymbol { field, values } => {
                any(field, values.iter().filter_map(|s| db.vocab_id(s)).map(|vid| vid as u64).collect())
            }
            Filter::And(filters) => Probe::And(filters.iter().map(|f| f.probe(db)).collect()),
            Filter::Or(filters) => Probe::Or(filters.iter().map(|f| f.probe(db)).collect()),
            Filter::Not(f) => Probe::Not(Box::new(f.probe(db))),
        }
    }
}

/// [`Filter`] を 1 つの entity について確かめる形（紐は himo_id、文字は語の番号に引いたもの）。
enum Probe {
    True,
    /// 紐が無い・語が無いなど、どの entity も当たらない。
    False,
    Eq(u16, u64),
    /// 両端を含む。
    Range(u16, u64, u64),
    /// 値の昇順・重複なし。
    In(u16, Vec<u64>),
    And(Vec<Probe>),
    Or(Vec<Probe>),
    Not(Box<Probe>),
}

impl Probe {
    fn holds(&self, db: &Engine, e: EntityId) -> bool {
        match self {
            Probe::True => true,
            Probe::False => false,
            Probe::Eq(h, v) => db.get_by_id(e, *h) == Some(*v),
            Probe::Range(h, lo, hi) => db.get_by_id(e, *h).is_some_and(|x| *lo <= x && x <= *hi),
            Probe::In(h, vs) => db.get_by_id(e, *h).is_some_and(|x| vs.binary_search(&x).is_ok()),
            Probe::And(ps) => ps.iter().all(|p| p.holds(db, e)),
            Probe::Or(ps) => ps.iter().any(|p| p.holds(db, e)),
            Probe::Not(p) => !p.holds(db, e),
        }
    }
}

fn himo(db: &Engine, field: &str) -> Option<u16> {
    db.himo_id(field).map(|h| h as u16)
}

fn pull_sorted(db: &Engine, field: &str, value: u32) -> Vec<EntityId> {
    // enchudb の pull_raw は &[EntityId] を返す。
    let mut v = db.pull_raw(field, value).to_vec();
    v.sort_unstable();
    v
}

/// 昇順ソート済み配列の差集合 a \ b。
fn diff_sorted(a: &[EntityId], b: &[EntityId]) -> Vec<EntityId> {
    let mut out = Vec::with_capacity(a.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() {
        if j >= b.len() || a[i] < b[j] { out.push(a[i]); i += 1; }
        else if a[i] > b[j] { j += 1; }
        else { i += 1; j += 1; }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_basic() {
        assert_eq!(diff_sorted(&[1, 2, 3, 4], &[2, 4]), vec![1u64, 3]);
        assert_eq!(diff_sorted(&[1, 2], &[]), vec![1u64, 2]);
    }
}
