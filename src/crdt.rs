//! CRDT (Conflict-free Replicated Data Type) 実験。
//!
//! enchudb の sync 層は現在 LWW (Last-Write-Wins) で衝突解決する。これは
//! **属性の上書き** (例: `age = 30 → 31 → 32`) には適切だが、**リスト追加 /
//! イベントログ / チャット** では並行 write の一方がロスする。
//!
//! このモジュールは CRDT のアルゴリズムを純粋 in-memory で実装し、
//! LWW との差分 (特にデータロスの有無) を tests で可視化する。
//!
//! Engine には統合しない。将来 [`crate::hlc_store::HlcStore`] を CRDT 化する
//! 実験をするときの土台。
//!
//! # 実装済み型
//!
//! - [`GCounter`] — grow-only counter (増加のみ)
//! - [`PnCounter`] — positive-negative counter (増減可)
//! - [`GSet<T>`] — grow-only set (追加のみ、削除無し)
//! - [`OrSet<T>`] — observed-remove set (追加/削除、並行時 add-wins)
//! - [`LwwRegister<T>`] — last-write-wins register (比較用、現 enchudb と同じ)
//!
//! # 参考
//!
//! Shapiro et al. "A comprehensive study of Convergent and Commutative Replicated Data Types" (2011).

#![cfg(feature = "crdt")]

use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use crate::PeerId;

// ─────────────────────────────────────────────────────────────
// G-Counter (Grow-only Counter)
// ─────────────────────────────────────────────────────────────

/// 各 peer が自分の increment 数を保持、value = 全 peer の合計。
/// merge は peer ごとに max を取るだけ (monotonic)。
#[derive(Clone, Default, Debug)]
pub struct GCounter {
    counts: HashMap<PeerId, u64>,
}

impl GCounter {
    pub fn new() -> Self { Self::default() }

    /// 自 peer で 1 増やす。
    pub fn inc(&mut self, peer: PeerId) {
        *self.counts.entry(peer).or_insert(0) += 1;
    }

    pub fn inc_by(&mut self, peer: PeerId, n: u64) {
        *self.counts.entry(peer).or_insert(0) += n;
    }

    /// 現在の合計値。
    pub fn value(&self) -> u64 {
        self.counts.values().sum()
    }

    /// 他 replica と merge。peer ごとに max を取る (monotonic)。
    pub fn merge(&mut self, other: &GCounter) {
        for (peer, v) in &other.counts {
            let entry = self.counts.entry(*peer).or_insert(0);
            *entry = (*entry).max(*v);
        }
    }
}

// ─────────────────────────────────────────────────────────────
// PN-Counter (Positive-Negative Counter)
// ─────────────────────────────────────────────────────────────

/// 2 つの G-Counter (増分と減分) の組合せ。value = P.sum() - N.sum()。
#[derive(Clone, Default, Debug)]
pub struct PnCounter {
    p: GCounter,
    n: GCounter,
}

impl PnCounter {
    pub fn new() -> Self { Self::default() }

    pub fn inc(&mut self, peer: PeerId) { self.p.inc(peer); }
    pub fn dec(&mut self, peer: PeerId) { self.n.inc(peer); }
    pub fn inc_by(&mut self, peer: PeerId, n: u64) { self.p.inc_by(peer, n); }
    pub fn dec_by(&mut self, peer: PeerId, n: u64) { self.n.inc_by(peer, n); }

    pub fn value(&self) -> i64 {
        self.p.value() as i64 - self.n.value() as i64
    }

    pub fn merge(&mut self, other: &PnCounter) {
        self.p.merge(&other.p);
        self.n.merge(&other.n);
    }
}

// ─────────────────────────────────────────────────────────────
// G-Set (Grow-only Set)
// ─────────────────────────────────────────────────────────────

/// 追加のみ可能な Set。削除無し。merge = union。絶対にデータロスしない。
#[derive(Clone, Default, Debug)]
pub struct GSet<T: Eq + Hash + Clone> {
    items: HashSet<T>,
}

impl<T: Eq + Hash + Clone> GSet<T> {
    pub fn new() -> Self { Self { items: HashSet::new() } }

    pub fn add(&mut self, item: T) { self.items.insert(item); }
    pub fn contains(&self, item: &T) -> bool { self.items.contains(item) }
    pub fn len(&self) -> usize { self.items.len() }
    pub fn iter(&self) -> impl Iterator<Item = &T> { self.items.iter() }

    pub fn merge(&mut self, other: &GSet<T>) {
        for item in &other.items {
            self.items.insert(item.clone());
        }
    }
}

// ─────────────────────────────────────────────────────────────
// OR-Set (Observed-Remove Set)
// ─────────────────────────────────────────────────────────────

/// 追加/削除両方可能、並行時 add-wins (add と remove が同時なら add を残す)。
/// 各 add は unique tag (peer_id, local_counter) を持ち、remove はその tag を消す。
///
/// 同じ element を「消して、また足す」が可能 (re-add する際は新しい tag が付く)。
#[derive(Clone, Debug)]
pub struct OrSet<T: Eq + Hash + Clone> {
    /// element → live な tag の集合
    adds: HashMap<T, HashSet<Tag>>,
    /// 既に削除された tag の集合
    tombstones: HashSet<Tag>,
    /// 自 peer の local counter
    next_tag: u64,
    /// 自 peer の id
    self_peer: PeerId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Tag {
    peer: PeerId,
    counter: u64,
}

impl<T: Eq + Hash + Clone> OrSet<T> {
    pub fn new(self_peer: PeerId) -> Self {
        Self {
            adds: HashMap::new(),
            tombstones: HashSet::new(),
            next_tag: 0,
            self_peer,
        }
    }

    pub fn add(&mut self, item: T) -> Tag {
        let tag = Tag { peer: self.self_peer, counter: self.next_tag };
        self.next_tag += 1;
        self.adds.entry(item).or_insert_with(HashSet::new).insert(tag);
        tag
    }

    /// 現在 live な tags 全部 tombstone 化。並行で他 peer が新しい tag で add したら
    /// その add は残る (add-wins)。
    pub fn remove(&mut self, item: &T) {
        if let Some(tags) = self.adds.get(item) {
            for t in tags.iter() { self.tombstones.insert(*t); }
            self.adds.remove(item);
        }
    }

    pub fn contains(&self, item: &T) -> bool {
        self.adds.get(item)
            .map(|tags| tags.iter().any(|t| !self.tombstones.contains(t)))
            .unwrap_or(false)
    }

    pub fn len(&self) -> usize {
        self.adds.iter()
            .filter(|(_, tags)| tags.iter().any(|t| !self.tombstones.contains(t)))
            .count()
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> + '_ {
        self.adds.iter()
            .filter(|(_, tags)| tags.iter().any(|t| !self.tombstones.contains(t)))
            .map(|(item, _)| item)
    }

    pub fn merge(&mut self, other: &OrSet<T>) {
        // tombstones: まず union (後の add フィルタでも使う)
        for t in &other.tombstones { self.tombstones.insert(*t); }
        // adds: tag 単位で union、tombstoned なものは除外
        for (item, other_tags) in &other.adds {
            let entry = self.adds.entry(item.clone()).or_insert_with(HashSet::new);
            for t in other_tags {
                if !self.tombstones.contains(t) {
                    entry.insert(*t);
                }
            }
            if entry.is_empty() { self.adds.remove(item); }
        }
        // 自分側の既存 adds からも tombstoned な tag を除く (other.tombstones が新たに入ったため)
        self.adds.retain(|_, tags| {
            tags.retain(|t| !self.tombstones.contains(t));
            !tags.is_empty()
        });
    }
}

// ─────────────────────────────────────────────────────────────
// LWW-Register (Last-Write-Wins)  — 比較用、現 enchudb と同じ挙動
// ─────────────────────────────────────────────────────────────

/// Timestamp 付き value、merge で timestamp が大きい方が勝つ。
/// これが **データロスを起こす** 典型例。
#[derive(Clone, Debug)]
pub struct LwwRegister<T: Clone> {
    value: Option<T>,
    timestamp: u64,
}

impl<T: Clone> Default for LwwRegister<T> {
    fn default() -> Self { Self { value: None, timestamp: 0 } }
}

impl<T: Clone> LwwRegister<T> {
    pub fn new() -> Self { Self::default() }

    pub fn set(&mut self, value: T, timestamp: u64) {
        if timestamp > self.timestamp {
            self.value = Some(value);
            self.timestamp = timestamp;
        }
    }

    pub fn get(&self) -> Option<&T> { self.value.as_ref() }

    pub fn merge(&mut self, other: &LwwRegister<T>) {
        if other.timestamp > self.timestamp {
            self.value = other.value.clone();
            self.timestamp = other.timestamp;
        }
    }
}

// ─────────────────────────────────────────────────────────────
// Tests — LWW は失う、CRDT は失わない
// ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── G-Counter ──
    #[test]
    fn gcounter_concurrent_inc_no_loss() {
        let mut a = GCounter::new();
        let mut b = GCounter::new();
        for _ in 0..10 { a.inc(1); }
        for _ in 0..20 { b.inc(2); }
        a.merge(&b);
        b.merge(&a);
        assert_eq!(a.value(), 30);
        assert_eq!(b.value(), 30);
        // 両 replica で同じ値 (convergent)
    }

    #[test]
    fn gcounter_duplicate_merge_idempotent() {
        let mut a = GCounter::new();
        let mut b = GCounter::new();
        a.inc_by(1, 5);
        b.inc_by(2, 3);
        a.merge(&b);
        a.merge(&b); // 同じ状態を 2 回 merge
        assert_eq!(a.value(), 8);
    }

    // ── PN-Counter ──
    #[test]
    fn pncounter_concurrent_inc_dec() {
        let mut a = PnCounter::new();
        let mut b = PnCounter::new();
        for _ in 0..10 { a.inc(1); }
        for _ in 0..4 { b.dec(2); }
        a.merge(&b);
        assert_eq!(a.value(), 6);
    }

    // ── G-Set ──
    #[test]
    fn gset_concurrent_add_no_loss() {
        let mut a: GSet<i32> = GSet::new();
        let mut b: GSet<i32> = GSet::new();
        a.add(1); a.add(2); a.add(3);
        b.add(3); b.add(4); b.add(5); // 3 が重複、両 peer で add
        a.merge(&b);
        assert_eq!(a.len(), 5); // 1,2,3,4,5 全部残る
        for i in 1..=5 { assert!(a.contains(&i)); }
    }

    // ── OR-Set ──
    #[test]
    fn orset_add_remove_locally() {
        let mut s: OrSet<&str> = OrSet::new(1);
        s.add("cat");
        s.add("dog");
        assert!(s.contains(&"cat"));
        s.remove(&"cat");
        assert!(!s.contains(&"cat"));
        assert!(s.contains(&"dog"));
    }

    #[test]
    fn orset_concurrent_add_survives() {
        // peer A add x、peer B 何もしない、merge → x 残る
        let mut a: OrSet<&str> = OrSet::new(1);
        let b: OrSet<&str> = OrSet::new(2);
        a.add("hello");
        a.merge(&b);
        assert!(a.contains(&"hello"));
    }

    #[test]
    fn orset_add_wins_over_concurrent_remove() {
        // peer A が "x" を add/remove する間、peer B が独立で "x" を add
        // merge すると B の add は残る (add-wins)
        let mut a: OrSet<&str> = OrSet::new(1);
        let mut b: OrSet<&str> = OrSet::new(2);

        // A の履歴: add "x" → remove "x"
        a.add("x");
        a.remove(&"x");
        assert!(!a.contains(&"x"));

        // B は独立で add "x"
        b.add("x");
        assert!(b.contains(&"x"));

        // merge
        a.merge(&b);
        assert!(a.contains(&"x"), "B の並行 add は残る (add-wins)");
    }

    #[test]
    fn orset_convergent_both_directions() {
        let mut a: OrSet<i32> = OrSet::new(1);
        let mut b: OrSet<i32> = OrSet::new(2);
        a.add(1); a.add(2);
        b.add(2); b.add(3);
        a.remove(&1);
        b.remove(&3);

        let a_before = a.clone();
        let b_before = b.clone();
        a.merge(&b_before);
        b.merge(&a_before);
        // 両方向 merge で同じ結果 (commutative)
        let mut a_items: Vec<i32> = a.iter().cloned().collect();
        let mut b_items: Vec<i32> = b.iter().cloned().collect();
        a_items.sort(); b_items.sort();
        assert_eq!(a_items, b_items);
        // 1 と 3 は消えて、2 は両 peer で add されてるので残る
        assert_eq!(a_items, vec![2]);
    }

    // ── LWW vs CRDT 比較 ──
    #[test]
    fn lww_concurrent_write_loses_data() {
        // Alice と Bob が同じ register に独立に書く。片方は消える。
        let mut alice: LwwRegister<&str> = LwwRegister::new();
        let mut bob: LwwRegister<&str> = LwwRegister::new();
        alice.set("alice_msg", 100);
        bob.set("bob_msg", 95); // timestamp 小 → 消える
        alice.merge(&bob);
        assert_eq!(alice.get(), Some(&"alice_msg"));
        assert_ne!(alice.get(), Some(&"bob_msg"), "Bob のデータは消えた (LWW の典型的損失)");
    }

    #[test]
    fn orset_same_scenario_preserves_both() {
        // 上と同じシナリオを OR-Set で。両方のメッセージが残る。
        let mut alice: OrSet<&str> = OrSet::new(1);
        let mut bob: OrSet<&str> = OrSet::new(2);
        alice.add("alice_msg");
        bob.add("bob_msg");
        alice.merge(&bob);
        assert!(alice.contains(&"alice_msg"));
        assert!(alice.contains(&"bob_msg"), "OR-Set では両方残る (データロス無し)");
        assert_eq!(alice.len(), 2);
    }

    // ── CAP 系: network partition シミュレーション ──
    #[test]
    fn orset_partition_heal_with_concurrent_ops() {
        // 3 peer が並行に操作、partition 中は merge せず、後で全員 merge
        let mut a: OrSet<i32> = OrSet::new(1);
        let mut b: OrSet<i32> = OrSet::new(2);
        let mut c: OrSet<i32> = OrSet::new(3);

        // partition 中
        a.add(1); a.add(2);
        b.add(3); b.remove(&3); // B が local で 3 を消す
        b.add(3); // また add
        c.add(1); c.add(4); // C が 1 を独立に add (別 tag)

        // heal: 全員が互いに merge
        let a0 = a.clone(); let b0 = b.clone(); let c0 = c.clone();
        a.merge(&b0); a.merge(&c0);
        b.merge(&a0); b.merge(&c0);
        c.merge(&a0); c.merge(&b0);

        // 3 peer で同じ state (convergent)
        let mut av: Vec<i32> = a.iter().cloned().collect();
        let mut bv: Vec<i32> = b.iter().cloned().collect();
        let mut cv: Vec<i32> = c.iter().cloned().collect();
        av.sort(); bv.sort(); cv.sort();
        assert_eq!(av, bv);
        assert_eq!(bv, cv);
        // 期待: {1, 2, 3, 4} (全 add が残る)
        assert_eq!(av, vec![1, 2, 3, 4]);
    }
}
