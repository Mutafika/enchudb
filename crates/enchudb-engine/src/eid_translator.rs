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
//!
//! # 別名 (#178)
//!
//! 同じ entity が 2 つの世界番号で知られることがある (2 台が同じ自然キーの行を別々に作ってから束ねた)。 2 つ目の世界番号は
//! **別名** (`alias`) として forward だけを張る: 別名宛ての record も同じ local に当たるが、 その local に書いた record の宛名
//! (逆写像) は変えない。 別名は `.eidmap` に入れない (`snapshot` は別名を除く。 入れると、 開き直した時の `insert` が逆写像を
//! 別名で上書きしうる)。 engine は別の sidecar (`eidalias`) に置く (`alias_snapshot`)。 slot を手放す時 (`remove_local`) は
//! 別名も一緒に外す (残すと、 別名宛ての record が使い回された slot の別の行に入る)。

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
    /// #178: local → その local を指す別名の key (forward だけの写像、 module doc の 「別名」)。 lock の順は
    /// `inner` → `translated_locals` → `aliases`。
    aliases: RwLock<HashMap<u32, Vec<Key>>>,
    /// #226: replica を保持している author の一覧。 relay が `serve_state` で
    /// 「どの author の state を配れるか」を pull ごとに引くので、 写像全体の
    /// 走査 (O(entry 数)) を避けるための索引。
    ///
    /// **grow-only** — `remove_local` で最後の entry が消えても author は残る。
    /// 残っても `state_records_for` が空を返し provider が名乗りを取り下げるだけ
    /// なので害が無く、 その代わり remove 経路に走査を持ち込まずに済む。
    authors: RwLock<std::collections::BTreeSet<PeerId>>,
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
    /// #560: `.eidmap` の中身 (逆写像の組) が変わった key。 engine が書き出しの時に取り出し、 その key の今の中身だけを
    /// `eidmap.log` に追記する。 変えた後に積む (先に積むと、 取り出した書き出しが古い中身を書き、 後の変更に印が無い)
    dirty: std::sync::Mutex<Vec<Key>>,
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
            aliases: RwLock::new(HashMap::new()),
            authors: RwLock::new(std::collections::BTreeSet::new()),
            alloc_lock: std::sync::Mutex::new(()),
            dirty: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// #560: key の `.eidmap` の中身が変わった印 (engine の削除の版数の変化もここに積む)。
    pub fn mark_dirty(&self, key: Key) {
        self.dirty.lock().unwrap_or_else(|p| p.into_inner()).push(key);
    }

    /// #560: 印の付いた key を取り出す (重ねて積まれた key は 1 つに)。
    pub fn take_dirty(&self) -> Vec<Key> {
        let mut v = std::mem::take(&mut *self.dirty.lock().unwrap_or_else(|p| p.into_inner()));
        v.sort_unstable();
        v.dedup();
        v
    }

    /// #560: 書き出しに失敗した key を印に戻す。
    pub fn restore_dirty(&self, keys: Vec<Key>) {
        self.dirty.lock().unwrap_or_else(|p| p.into_inner()).extend(keys);
    }

    /// #560: 印を全部捨てる (開く時、 `.eidmap` から読んだ中身は disk と同じ)。
    pub fn clear_dirty(&self) {
        self.dirty.lock().unwrap_or_else(|p| p.into_inner()).clear();
    }

    /// #560: key の `.eidmap` の写像の面 (`snapshot` に入る = 前向きの写像があり、 その local の別名でない) の local。 無ければ None。
    pub fn mapped_local(&self, author_peer: PeerId, foreign_local: u32) -> Option<u32> {
        let key = (author_peer, foreign_local);
        let fwd = self.inner.read().unwrap();
        let local = *fwd.get(&key)?;
        let al = self.aliases.read().unwrap();
        al.get(&local).is_none_or(|keys| !keys.contains(&key)).then_some(local)
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
        let key = (author_peer, foreign_local);
        // lock の順は `inner` → `translated_locals` → `aliases` (`alias` と同じ)
        let mut fwd = self.inner.write().unwrap();
        let old = fwd.insert(key, local);
        let mut rev = self.translated_locals.write().unwrap();
        if let Some(old_local) = old {
            // #178: 前の local の逆写像がこの key の時だけ外す (key がその local の別名だった時、 その local の逆写像は別の key)
            if old_local != local && rev.get(&old_local) == Some(&key) {
                rev.remove(&old_local);
            }
        }
        let displaced = rev.insert(local, key).filter(|&k| k != key);
        let mut al = self.aliases.write().unwrap();
        // #178: 別名だった key なら別名から外す (この local の逆写像になった / 他の local の別名だった)
        if let Some(old_local) = old {
            drop_alias(&mut al, old_local, key);
            drop_alias(&mut al, local, key);
        }
        // #560: この local の前の逆写像の key が、 まだこの local を指していれば別名にする (宛名ではないが、 その世界番号宛ての
        // record はこの local に当たる)。 前は前向きの写像だけが残り、 別名にも載らなかったので、 slot を使い回す時に外れず
        // (`remove_local`)、 `.eidmap` では逆写像と同じ扱いの 2 つ目の key になっていた
        if let Some(prev) = displaced
            && fwd.get(&prev) == Some(&local)
        {
            let v = al.entry(local).or_default();
            if !v.contains(&prev) {
                v.push(prev);
            }
        }
        drop((fwd, rev, al));
        // #560: この key と、 この local の前の逆写像の key の中身が変わった
        self.mark_dirty(key);
        if let Some(prev) = displaced {
            self.mark_dirty(prev);
        }
        // #226: author 索引。 lock は単独で取る (`inner` / `translated_locals` と
        // 同時には保持しないので、 既存の順序制約に影響しない)。
        if !self.authors.read().unwrap().contains(&author_peer) {
            self.authors.write().unwrap().insert(author_peer);
        }
    }

    /// #178: `(author_peer, foreign_local)` を `local` の別名にする (同じ entity の 2 つ目の世界番号、 module doc の 「別名」)。
    /// forward だけを張り、 `local` の逆写像 (書き戻しの宛名) は変えない。 key が他の local を指していたら外す: その local の
    /// 逆写像だったなら、 その local はもう翻訳先ではない (畳まれた側)。 key が `local` の逆写像そのものなら何もしない。
    pub fn alias(&self, author_peer: PeerId, foreign_local: u32, local: u32) {
        let key = (author_peer, foreign_local);
        let mut fwd = self.inner.write().unwrap();
        let mut rev = self.translated_locals.write().unwrap();
        let mut al = self.aliases.write().unwrap();
        if let Some(old) = fwd.insert(key, local)
            && old != local
        {
            if rev.get(&old) == Some(&key) {
                rev.remove(&old);
            }
            drop_alias(&mut al, old, key);
        }
        if rev.get(&local) != Some(&key) {
            let v = al.entry(local).or_default();
            if !v.contains(&key) {
                v.push(key);
            }
        }
        drop((fwd, rev, al));
        // #560: 逆写像だった key が別名になったかもしれない
        self.mark_dirty(key);
        if !self.authors.read().unwrap().contains(&author_peer) {
            self.authors.write().unwrap().insert(author_peer);
        }
    }

    /// #612: 別名 `(author_peer, foreign_local)` を外す (畳んだ自分の行を元の slot に戻す時)。 逆写像の key は外さない。 外したら true。
    /// 印 (#560) は付けない: 別名は `.eidmap` に入らないので、 外しても `.eidmap` の中身は変わらない。
    pub fn unalias(&self, author_peer: PeerId, foreign_local: u32) -> bool {
        let key = (author_peer, foreign_local);
        let mut fwd = self.inner.write().unwrap();
        let rev = self.translated_locals.read().unwrap();
        let mut al = self.aliases.write().unwrap();
        let Some(&old) = fwd.get(&key) else { return false };
        if rev.get(&old) == Some(&key) {
            return false;
        }
        fwd.remove(&key);
        drop_alias(&mut al, old, key);
        drop((fwd, rev, al));
        true
    }

    /// #178: `local` を指す別名 (別名でなく逆写像の key は含まない)。
    pub fn aliases_of(&self, local: u32) -> Vec<Key> {
        self.aliases.read().unwrap().get(&local).cloned().unwrap_or_default()
    }

    /// #178: 別名の snapshot (sidecar `eidalias` の永続化用)。 各要素 = (author_peer, foreign_local, local)。
    pub fn alias_snapshot(&self) -> Vec<(PeerId, u32, u32)> {
        let fwd = self.inner.read().unwrap();
        let al = self.aliases.read().unwrap();
        let mut out: Vec<(PeerId, u32, u32)> = al
            .iter()
            .flat_map(|(&local, keys)| keys.iter().map(move |&k| (k, local)))
            .filter(|(k, local)| fwd.get(k) == Some(local))
            .map(|((peer, foreign_local), local)| (peer, foreign_local, local))
            .collect();
        out.sort_unstable();
        out
    }

    /// #226: replica を保持している author の一覧 (grow-only、 `authors` field 参照)。
    pub fn authors(&self) -> Vec<PeerId> {
        self.authors.read().unwrap().iter().copied().collect()
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

    /// #490: 写像が無ければ `f` を呼ぶ。 写像があれば `f` を呼ばずに `Err(local)`。
    ///
    /// 確かめと `f` は、 `get_or_insert_with` の払い出しと同じ `alloc_lock` の下で行う。 `f` の間に同じ key の写像が
    /// 払い出されることはなく、 `f` の後で払い出す側は `f` が書いたものを見る。 写像の無い entity 宛ての Delete を
    /// 退避表に載せるのに使う (間に払い出しが入ると、 払い出した側は退避表をまだ見ず、 載せる側は写像を見ずに載せて、
    /// 削除が新しい slot に載らない)。
    pub fn if_unmapped<R>(
        &self,
        author_peer: PeerId,
        foreign_local: u32,
        f: impl FnOnce() -> R,
    ) -> Result<R, u32> {
        let _serialize = self.alloc_lock.lock().unwrap_or_else(|p| p.into_inner());
        match self.get(author_peer, foreign_local) {
            Some(local) => Err(local),
            None => Ok(f()),
        }
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
    ///
    /// #178: その local を指す別名も外して返す (逆写像の key が先頭。 別名を残すと、 別名宛ての record が新しい住人に入る)。
    #[must_use = "外した identity の tombstone を退避しないと削除済み entity が復活する (#166)"]
    pub fn remove_local(&self, local: u32) -> Vec<Key> {
        // lock 順序は `inner` → `translated_locals` → `aliases` に固定する (`insert` は同時には保持しないので、 この順序
        // だけ守れば cycle は生じない)。
        let mut fwd = self.inner.write().unwrap();
        let mut out = Vec::new();
        if let Some(key) = self.translated_locals.write().unwrap().remove(&local) {
            // 同 key が別 local に張り替わっている可能性があるので、 一致する時だけ消す。
            if fwd.get(&key) == Some(&local) {
                fwd.remove(&key);
            }
            out.push(key);
        }
        if let Some(keys) = self.aliases.write().unwrap().remove(&local) {
            for key in keys {
                if fwd.get(&key) == Some(&local) {
                    fwd.remove(&key);
                    out.push(key);
                }
            }
        }
        drop(fwd);
        // #560: 外した key の中身が変わった
        for &key in &out {
            self.mark_dirty(key);
        }
        out
    }

    pub fn len(&self) -> usize {
        self.inner.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 全 entry の snapshot (sidecar 永続化用)。 各要素 = (author_peer, foreign_local, local)。 #178: 別名は除く
    /// ([`alias_snapshot`](Self::alias_snapshot) が持つ)。
    pub fn snapshot(&self) -> Vec<(PeerId, u32, u32)> {
        let guard = self.inner.read().unwrap();
        let al = self.aliases.read().unwrap();
        guard
            .iter()
            .filter(|&(k, local)| al.get(local).is_none_or(|keys| !keys.contains(k)))
            .map(|(&(peer, foreign_local), &local)| (peer, foreign_local, local))
            .collect()
    }
}

/// `local` の別名の並びから `key` を外す (空になったら並びごと)。
fn drop_alias(al: &mut HashMap<u32, Vec<Key>>, local: u32, key: Key) {
    if let Some(v) = al.get_mut(&local) {
        v.retain(|&k| k != key);
        if v.is_empty() {
            al.remove(&local);
        }
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

    /// #178: 別名は forward だけを張る (逆写像は変えない)。 他の local の逆写像だった key を別名にすると、 その local は
    /// 翻訳先でなくなる。 `snapshot` は別名を除き、 `alias_snapshot` が持つ。 `remove_local` は別名も外して返す。 逆写像として
    /// `insert` し直すと別名から外れる。
    #[test]
    fn alias_maps_forward_only() {
        let t = EidTranslator::new();
        t.insert(1, 42, 7); // 7 は (1, 42) の写し
        t.insert(2, 50, 8); // 8 は (2, 50) の写し
        t.alias(1, 42, 8); // (1, 42) を 8 の別名に (7 を 8 に畳んだ)
        assert_eq!(t.get(1, 42), Some(8));
        assert_eq!(t.reverse(8), Some((2, 50)), "別名が逆写像を変えた");
        assert_eq!(t.reverse(7), None, "畳まれた側の逆写像が残った");
        assert_eq!(t.aliases_of(8), vec![(1, 42)]);
        assert_eq!(t.snapshot(), vec![(2, 50, 8)], "snapshot に別名が入った");
        assert_eq!(t.alias_snapshot(), vec![(1, 42, 8)]);
        t.alias(2, 50, 8); // 逆写像そのものを別名にしても何もしない
        assert_eq!(t.aliases_of(8), vec![(1, 42)]);
        // 逆写像として張り直すと別名から外れる
        t.insert(1, 42, 9);
        assert!(t.aliases_of(8).is_empty(), "張り直した key が別名に残った");
        assert_eq!(t.alias_snapshot(), vec![]);
        t.alias(1, 42, 8);
        assert_eq!(t.reverse(9), None);
        // slot を手放すと別名も外れる (逆写像が先頭)
        let mut gone = t.remove_local(8);
        gone.sort();
        assert_eq!(gone, vec![(1, 42), (2, 50)]);
        assert_eq!((t.get(1, 42), t.get(2, 50)), (None, None));
        assert!(t.aliases_of(8).is_empty() && t.alias_snapshot().is_empty());
    }

    /// #166: slot 再利用時に写像を外せること。 forward / reverse の両方が消える。
    #[test]
    fn remove_local_drops_both_directions() {
        let t = EidTranslator::new();
        t.insert(1, 42, 7);
        assert_eq!(t.remove_local(7), vec![(1, 42)]);
        assert_eq!(t.get(1, 42), None, "forward が残っている");
        assert_eq!(t.reverse(7), None, "reverse が残っている");
        assert!(!t.is_translated_local(7));
        assert_eq!(t.len(), 0);
        // 2 度目は空 (= 既に外れている)
        assert_eq!(t.remove_local(7), vec![]);
    }

    /// レプリカでない local を渡しても何も壊さない。
    #[test]
    fn remove_local_is_noop_for_unmapped() {
        let t = EidTranslator::new();
        t.insert(1, 42, 7);
        assert_eq!(t.remove_local(99), vec![]);
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
        assert_eq!(t.remove_local(7), vec![], "既に張り替わっているので何も外さない");
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

    /// #490: 写像が無い時だけ `f` を呼び、 あればその local を返す (写像は作らない)。
    #[test]
    fn if_unmapped_calls_only_without_a_mapping() {
        let t = EidTranslator::new();
        assert_eq!(t.if_unmapped(1, 42, || "called"), Ok("called"));
        assert_eq!(t.get(1, 42), None, "写像を作った");
        t.insert(1, 42, 7);
        let mut called = false;
        assert_eq!(t.if_unmapped(1, 42, || called = true), Err(7));
        assert!(!called, "写像があるのに呼んだ");
    }

    /// #560: 逆写像を別の key に張り替えた (`insert`) 時、 前の key がまだその local を指していれば別名になり、 slot を使い回す時
    /// (`remove_local`) に一緒に外れる。 前は前向きの写像だけが残り、 使い回した slot の新しい住人にその世界番号宛ての record が
    /// 入った (`.eidmap` の書き出しを disk と突き合わせて見つけた)。
    #[test]
    fn a_displaced_reverse_key_goes_with_the_slot() {
        let t = EidTranslator::new();
        t.insert(1, 10, 5);
        t.insert(2, 20, 5);
        assert_eq!(t.reverse(5), Some((2, 20)));
        assert_eq!(t.get(1, 10), Some(5), "前提: 前の key もまだ同じ行に当たる");
        let mut gone = t.remove_local(5);
        gone.sort_unstable();
        assert_eq!(gone, vec![(1, 10), (2, 20)], "slot を手放す時に前の key を外さない");
        assert_eq!(t.get(1, 10), None, "前の key が使い回す slot を指したまま");
    }

    /// #560: `.eidmap` の写像の面 (`snapshot`) が変わった key には、 変えた操作の後に必ず印がある (印の付け忘れは、 落ちた後に
    /// 開くとその写像を失う)。 4 つの書き換え (`insert` / `alias` / `unalias` / `remove_local`) を小さな key と slot の空間で
    /// 乱数で並べ、 操作ごとに見る。
    #[test]
    fn every_change_to_the_eidmap_face_is_marked() {
        let face = |t: &EidTranslator| -> HashMap<Key, u32> { t.snapshot().into_iter().map(|(p, f, l)| ((p, f), l)).collect() };
        let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut next = |n: u64| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x % n) as u32
        };
        for _ in 0..200 {
            let t = EidTranslator::new();
            for _ in 0..60 {
                let before = face(&t);
                let (peer, fl, local) = (1 + next(2), next(5), next(6));
                let op = next(4);
                match op {
                    0 => t.insert(peer, fl, local),
                    1 => t.alias(peer, fl, local),
                    2 => {
                        t.unalias(peer, fl);
                    }
                    _ => {
                        let _ = t.remove_local(local);
                    }
                }
                let after = face(&t);
                let marked = t.take_dirty();
                let mut changed: Vec<Key> = before.keys().chain(after.keys()).copied().filter(|k| before.get(k) != after.get(k)).collect();
                changed.sort_unstable();
                changed.dedup();
                for k in changed {
                    assert!(marked.contains(&k), "op {op} ({peer}, {fl}, {local}) で {k:?} の写像の面が {:?} → {:?} に変わったのに印が無い", before.get(&k), after.get(&k));
                }
            }
        }
    }
}
