//! EidTranslator — maps a foreign peer's EntityId to a local one (#9).
//!
//! # なぜ必要か
//!
//! EntityId は **peer ごとの空間**。 peer A の `eid=42` と peer B の `eid=42` は
//! 別 entity。 受信側 engine が foreign eid をそのまま local slot として apply
//! すると、 衝突した瞬間に自分の entity をサイレント上書きする (issue #9)。
//!
//! `EidTranslator` は `(author_peer, foreign_local) → local_eid` の写像を持ち、
//! 受信 op を apply する前に foreign eid を「自分の eid 空間の fresh な local」に
//! 翻訳する。 これで A の entity と B の entity が同じ local slot に落ちても
//! 衝突しない。
//!
//! - 初見の `(author_peer, foreign_local)` → 新しい local eid を払い出して登録
//! - 既知の組 → 既存 local eid に dispatch
//! - eid の産みの親 (`eid_peer`) が self なら identity (翻訳不要、 呼び出し側で判定)
//!
//! # 逆写像 (0.11、 #76 / request10)
//!
//! `translated_locals` は逆方向 `local → (author_peer, foreign_local)` の写像。
//! write-back (= レプリカへの self-authored write) を bridge が元 entity の
//! 世界番号に宛名を書き戻して発送するために使う。 forward と同じ insert 地点で
//! 両方向を維持するので、 `.eidmap` sidecar の format は不変のまま再構築できる。
//!
//! # 永続化
//!
//! `.eidmap` sidecar (engine 側) に forward entry を永続化し、 open 時に
//! `insert` 経由で両方向を復元する。

use std::collections::HashMap;
use std::sync::RwLock;

use enchudb_oplog::PeerId;

/// `(author_peer, foreign_local)` を key にした foreign→local 写像。
type Key = (PeerId, u32);

/// foreign eid → local eid の翻訳テーブル。
pub struct EidTranslator {
    inner: RwLock<HashMap<Key, u32>>,
    /// 0.11 (#76 逆写像): 翻訳先 local eid → 元 entity の世界番号成分
    /// `(author_peer, foreign_local)` の逆写像。 用途は 2 つ:
    /// 1. 「この local は foreign entity のレプリカか」の O(1) 判定 (旧 HashSet 相当)
    /// 2. write-back の宛名解決 — bridge が self-authored write を元 entity の
    ///    世界番号に書き戻して発送する (`reverse`)
    translated_locals: RwLock<HashMap<u32, Key>>,
    /// #166: `get_or_insert_with` の 「引く → 確保 → 登録」 を直列化する専用 lock。
    ///
    /// 以前は `inner` の write lock を保持したまま `alloc` を呼んでいたが、
    /// `alloc` (= `entity_in`) は free list から slot を再利用する際に
    /// `remove_local` を呼ぶようになった (#166) ため、 `inner` の再入で
    /// **self-deadlock** する。 `RwLock` は reentrant ではない。
    ///
    /// 確保の atomicity は写像の lock ではなくこの lock が担保する。 保持順は必ず
    /// `alloc_lock` → (`inner` | `translated_locals`) で、 逆向きに取る経路は無い。
    alloc_lock: std::sync::Mutex<()>,
}

impl Default for EidTranslator {
    fn default() -> Self {
        Self::new()
    }
}

impl EidTranslator {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            translated_locals: RwLock::new(HashMap::new()),
            alloc_lock: std::sync::Mutex::new(()),
        }
    }

    /// 既存の写像を引く。 未登録なら None。
    pub fn get(&self, author_peer: PeerId, foreign_local: u32) -> Option<u32> {
        let guard = self.inner.read().unwrap();
        guard.get(&(author_peer, foreign_local)).copied()
    }

    /// #76: local eid が foreign entity の翻訳先 (= レプリカ) かどうか。
    pub fn is_translated_local(&self, local: u32) -> bool {
        self.translated_locals.read().unwrap().contains_key(&local)
    }

    /// 0.11 (#76 逆写像): 翻訳先 local から元 entity の世界番号成分
    /// `(author_peer, foreign_local)` を引く。 レプリカでなければ None。
    pub fn reverse(&self, local: u32) -> Option<(PeerId, u32)> {
        self.translated_locals.read().unwrap().get(&local).copied()
    }

    /// 写像を登録 (上書き)。 recovery 復元時にも使う。 同 key の上書きで local が
    /// 変わる場合は stale な逆写像 entry を掃除する。
    pub fn insert(&self, author_peer: PeerId, foreign_local: u32, local: u32) {
        let mut guard = self.inner.write().unwrap();
        let old = guard.insert((author_peer, foreign_local), local);
        drop(guard);
        let mut rev = self.translated_locals.write().unwrap();
        if let Some(old_local) = old {
            if old_local != local {
                rev.remove(&old_local);
            }
        }
        rev.insert(local, (author_peer, foreign_local));
    }

    /// 写像を **atomic** に get-or-insert する。 未登録なら `alloc` を呼んで local を
    /// 確保し、 write lock 下で再 check してから insert する。 これで並行 apply が同じ
    /// foreign entity を解決しても double-alloc / orphan が起きない (= 全 caller が同じ
    /// local を得る)。
    ///
    /// `alloc` が `None` を返したら (= 確保先 table を導けない table-less な op)、 insert
    /// せず `None` を返す → 呼び出し側はその op を skip する。
    ///
    /// #166: `alloc` は **写像の lock を一切保持しない状態**で呼ばれる。 直列化は
    /// 専用の `alloc_lock` が行うので、 `alloc` の中から translator を触ってよい
    /// (`entity_in` は slot 再利用時に `remove_local` を呼ぶ)。 以前は `inner` の
    /// write lock 保持中に呼んでいて、 その経路が self-deadlock していた。
    pub fn get_or_insert_with(
        &self,
        author_peer: PeerId,
        foreign_local: u32,
        alloc: impl FnOnce() -> Option<u32>,
    ) -> Option<u32> {
        // fast path: 既に登録済みなら read lock だけで返す。
        if let Some(local) = self.get(author_peer, foreign_local) {
            return Some(local);
        }
        // slow path: alloc_lock で直列化 → 再 check (= double-checked) → 確保 → 登録。
        let _serialize = self.alloc_lock.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(local) = self.get(author_peer, foreign_local) {
            return Some(local); // 別 thread が先に確保した
        }
        let local = alloc()?;
        self.insert(author_peer, foreign_local, local);
        Some(local)
    }

    /// #166: **local slot が再利用されるとき**に写像を外す。 外した
    /// `(author_peer, foreign_local)` を返す (レプリカでなければ `None`)。
    ///
    /// 写像は local slot を指すが、 slot は削除後に free list へ戻って **別の
    /// entity に払い出される**。 写像を残したままだと、 その foreign entity 宛の
    /// record が新しい住人へ書き込まれる (= 無関係な行の silent 破壊)。
    ///
    /// 呼ぶのは **slot を手放す時ではなく、 別の住人に渡す時**。 手放しただけの
    /// 段階では写像も slot 上の tombstone も有効で、 削除済み entity 宛の古い
    /// record はその tombstone で正しく弾ける。
    ///
    /// **戻り値を捨ててはいけない**: 呼び元は外した identity 宛の tombstone を
    /// slot から退避する責務がある (`Engine::evict_translation_for_reuse`)。
    /// これを怠ると 「破壊」 が 「削除済み entity の復活」 に化けるだけになる。
    #[must_use = "外した identity の tombstone を退避しないと削除済み entity が復活する (#166)"]
    pub fn remove_local(&self, local: u32) -> Option<Key> {
        // lock 順序は `inner` → `translated_locals` に固定する (`insert` は両方を
        // 同時には保持しないので、 この順序だけ守れば cycle は生じない)。
        let mut fwd = self.inner.write().unwrap();
        let key = self.translated_locals.write().unwrap().remove(&local)?;
        // 同 key が別 local に張り替わっている可能性があるので、 一致する時だけ消す。
        if fwd.get(&key) == Some(&local) {
            fwd.remove(&key);
        }
        Some(key)
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 全 entry の snapshot (sidecar 永続化用)。 各要素 = (author_peer, foreign_local, local)。
    pub fn snapshot(&self) -> Vec<(PeerId, u32, u32)> {
        let guard = self.inner.read().unwrap();
        guard
            .iter()
            .map(|(&(peer, foreign_local), &local)| (peer, foreign_local, local))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_returns_none_for_unknown() {
        let t = EidTranslator::new();
        assert_eq!(t.get(1, 42), None);
    }

    #[test]
    fn insert_then_get_roundtrips() {
        let t = EidTranslator::new();
        t.insert(1, 42, 7);
        assert_eq!(t.get(1, 42), Some(7));
        // 別 peer の同じ foreign_local は別 entry
        assert_eq!(t.get(2, 42), None);
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn insert_overwrites_same_key() {
        let t = EidTranslator::new();
        t.insert(1, 42, 7);
        t.insert(1, 42, 9);
        assert_eq!(t.get(1, 42), Some(9));
        assert_eq!(t.len(), 1);
        // 0.11: 上書きで stale になった旧 local の逆写像は掃除される
        assert_eq!(t.reverse(7), None);
        assert!(!t.is_translated_local(7));
        assert_eq!(t.reverse(9), Some((1, 42)));
    }

    /// #166: slot 再利用時に写像を外せること。 forward / reverse の両方が消える。
    #[test]
    fn remove_local_drops_both_directions() {
        let t = EidTranslator::new();
        t.insert(1, 42, 7);
        assert_eq!(t.remove_local(7), Some((1, 42)));
        assert_eq!(t.get(1, 42), None, "forward が残っている");
        assert_eq!(t.reverse(7), None, "reverse が残っている");
        assert!(!t.is_translated_local(7));
        assert_eq!(t.len(), 0);
        // 2 度目は None (= 既に外れている)
        assert_eq!(t.remove_local(7), None);
    }

    /// レプリカでない local を渡しても何も壊さない。
    #[test]
    fn remove_local_is_noop_for_unmapped() {
        let t = EidTranslator::new();
        t.insert(1, 42, 7);
        assert_eq!(t.remove_local(99), None);
        assert_eq!(t.get(1, 42), Some(7), "無関係な写像を巻き込んだ");
    }

    /// 同じ key が別 local へ張り替わった後に、 **古い local** で remove しても
    /// 新しい forward 写像を巻き込まない (`insert` が reverse を掃除するので
    /// 通常は起きないが、 forward を消す条件が緩いと事故る)。
    #[test]
    fn remove_local_does_not_kill_a_rebound_mapping() {
        let t = EidTranslator::new();
        t.insert(1, 42, 7);
        t.insert(1, 42, 9); // 張り替え。 reverse(7) はここで消える
        assert_eq!(t.remove_local(7), None, "既に張り替わっているので何も外さない");
        assert_eq!(t.get(1, 42), Some(9), "張り替え後の写像を巻き込んだ");
        assert_eq!(t.reverse(9), Some((1, 42)));
    }

    #[test]
    fn reverse_roundtrips() {
        let t = EidTranslator::new();
        t.insert(3, 100, 55);
        assert_eq!(t.reverse(55), Some((3, 100)));
        assert!(t.is_translated_local(55));
        // 未登録 local は None
        assert_eq!(t.reverse(56), None);
        // get_or_insert_with 経由でも逆写像が載る
        let l = t.get_or_insert_with(4, 200, || Some(77)).unwrap();
        assert_eq!(l, 77);
        assert_eq!(t.reverse(77), Some((4, 200)));
    }
}
