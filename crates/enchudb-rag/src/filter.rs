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
//! AND: 最も小さい候補集合を取って、他の条件で filter する（v24 の非 bitmap 戦略に近い）。
//! OR: 和集合。
//! 評価結果は**ソート済み EntityId (u64) の Vec**として返す（後段の集合演算をしやすくするため）。

use enchudb::{Engine};
use enchudb_oplog::EntityId;
use std::ops::RangeInclusive;

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
    /// `all_alive` は Filter::All / Filter::Not の基底集合として使う。
    pub(crate) fn evaluate(&self, db: &Engine, all_alive: &[EntityId]) -> Vec<EntityId> {
        match self {
            Filter::All => all_alive.to_vec(),

            Filter::Value { field, value } => {
                let s = pull_sorted(db, field, *value);
                s
            }

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
                if filters.is_empty() { return all_alive.to_vec(); }
                // 最小集合を起点に、残りで filter（v24 的戦略）
                let mut sets: Vec<Vec<EntityId>> = filters.iter()
                    .map(|f| f.evaluate(db, all_alive))
                    .collect();
                sets.sort_by_key(|s| s.len());
                let mut cur = sets.remove(0);
                for s in sets {
                    cur = intersect_sorted(&cur, &s);
                    if cur.is_empty() { break; }
                }
                cur
            }

            Filter::Or(filters) => {
                let mut out = Vec::new();
                for f in filters {
                    out.extend(f.evaluate(db, all_alive));
                }
                out.sort_unstable();
                out.dedup();
                out
            }

            Filter::Not(f) => {
                let exclude = f.evaluate(db, all_alive);
                diff_sorted(all_alive, &exclude)
            }
        }
    }
}

fn pull_sorted(db: &Engine, field: &str, value: u32) -> Vec<EntityId> {
    // enchudb は v27 feature 前提で依存している。pull_raw は &[EntityId] を返す。
    let mut v = db.pull_raw(field, value).to_vec();
    v.sort_unstable();
    v
}

/// 昇順ソート済み配列の積集合。
fn intersect_sorted(a: &[EntityId], b: &[EntityId]) -> Vec<EntityId> {
    let mut out = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => { out.push(a[i]); i += 1; j += 1; }
        }
    }
    out
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
    fn intersect_basic() {
        assert_eq!(intersect_sorted(&[1, 2, 3], &[2, 3, 4]), vec![2u64, 3]);
        assert_eq!(intersect_sorted(&[1, 2], &[3, 4]), Vec::<EntityId>::new());
        assert_eq!(intersect_sorted(&[], &[1]), Vec::<EntityId>::new());
    }

    #[test]
    fn diff_basic() {
        assert_eq!(diff_sorted(&[1, 2, 3, 4], &[2, 4]), vec![1u64, 3]);
        assert_eq!(diff_sorted(&[1, 2], &[]), vec![1u64, 2]);
    }
}
