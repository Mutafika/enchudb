//! 主キーの前の名前 (改名の前の値)。
//!
//! 主キーが同じなら同じ行 (#141 / #513)。 受け手は行を受け取った時に、 主キーで同じ行を引いて束ねる。 ところが主キーの値は
//! 書き換えられる (改名、 東京 → 新東京)。 今の名前だけで引くと、 改名を知らない peer が古い名前で書いた行が、 改名を先に
//! 受けた受け手では別の行、 後に受けた受け手では同じ行になり、 届く順で答えが割れた。 前の名前を覚えておき、 今その名前の
//! 行が無ければ前にその名前だった行に束ねる (前の名前 = 同じ物)。
//!
//! - (主キーの himo, 前の値の鍵) → 行の slot。 値の鍵は `pk_tombs.rs` と同じ (Tag は文字列の bytes、 それ以外は値の 8 byte)
//! - 行が消えても残す (改名して消した行に、 改名を知らずに後から古い名前で作った行も同じ行にする)。 slot を別の行に
//!   使い回す時に外す
//! - 消えた行への改名 (書き込みは捨てる) の名前も、 その行の前の名前に足す (#551。 後から別の author のより新しい削除が
//!   届いた時に、 その名前の削除の時刻も上げる)
//! - 行を消す時、 前の名前も消した名前として覚える (`pk_tombs.rs`、 #551)
//!
//! - slot を使い回す時 (`clear_cell_versions`)、 消えた行の名前 (消えた時の名前と前の名前) を、 その slot に写っていた
//!   identity (作った peer, その peer での番号) に移す (#545)。 写像の外れた identity 宛ての削除が後から届いた時に、 その名前の
//!   削除の時刻を上げるため (slot の記録は使い回しで外れる)
//!
//! - 名前で束ねた slot は、 束ねた identity (作った peer, その peer での番号。 自分の行は (自分の peer, slot)) とその作成の
//!   HLC を持つ (#581)。 束ねた後に削除が届いた時、 削除より後に作られた identity を切り離す (削除を先に受けた peer では別の
//!   デッキになっている)。 束ねない行は何も持たない
//!
//! - 束ねた slot は、 束ねた identity ごとのカード (列ごとの値と版数) も持つ。 束ねた行の cell は LWW で 1 枚にまとまるので、
//!   改名の時刻で identity を切り離す時 (改名の後に前の名前で作られた identity が、 改名より先に今の名前で束ねられていた)、
//!   それぞれの identity の値をここから戻す。 最初に束ねた時に元の identity の cell を写し、 以後はその slot への書き込みを宛名の
//!   identity のカードに積む (identity と列ごとに版数の新しい方)
//!
//! - 改名した行は名前の年表 (名前のカードへの書き込みの宛名の identity・値・版数。 LWW で負けた書き込みも) を持つ。 前の名前で
//!   束ねるのは、 年表の上で作成の時刻にその名前だった行だけ (改名より前に作った行は同じデッキ、 後に作った行は新しいデッキ。
//!   削除優先と同じく時刻で決める)。 identity ごとに最初の書き込みが作成、 値の変わる書き込みが改名で、 改名は行の全部の identity に
//!   当たる (同じデッキ)。 identity の時刻 t の名前は、 作成の名前と、 作成より後で t 以下の改名の最後 (t が作成より前なら作成の
//!   名前)。 行がその名前だった = どれかの identity がその名前だった (束ねる前の別々の行は、 それぞれの名前を持つ)。 時刻 t に
//!   その名前で作られた行は、 t 以後にその名前だったことのある行と同じデッキ (同じ時刻に同じ名前の行は同じデッキ。 作った行は
//!   改名するまでその名前のまま)。 年表は届く
//!   順によらない (2 人が別々の名前に改名しても、 版数の順で決まる)。 最初の改名で、 束ねた identity のカード (無ければ行の元の
//!   identity の、 改名の前の名前と版数) を載せ、 以後はその行への名前の書き込みを全部載せる
//!
//! 永続化は DB directory の `pkformer` に、 変わった時にまるごと書く (改名の数だけなので小さい)。 無ければ空。 読まない旧
//! binary は前の名前を知らないだけ (前と同じ、 今の名前でだけ束ねる)。
//!
//! file: magic "PKFN"(4) + version u32 + count u32、 続いて (himo u16, slot u32, 鍵の長さ u32, 鍵) × count。 version 2 は続けて
//! identity の名前: count u32、 (peer u32, その peer での番号 u32, himo u16, 鍵の長さ u32, 鍵) × count。 version 3 は続けて
//! 束ねた identity: count u32、 (slot u32, peer u32, その peer での番号 u32, wall u64, logical u32, HLC の peer u32) × count。
//! version 4 は続けて束ねた identity のカード: count u32、 (slot u32, peer u32, その peer での番号 u32, himo u16, wall u64,
//! logical u32, HLC の peer u32, 種類 u8 (0 外した / 1 値 / 2 中身), 値 u64 か 中身の長さ u32 + 中身) × count、 名前の年表:
//! count u32、 (slot u32, peer u32, その peer での番号 u32, himo u16, wall u64, logical u32, HLC の peer u32, 鍵の長さ u32, 鍵)
//! × count (version 1〜3 も読む。
//! 年表の無い行の前の名前は、 前と同じく作成の時刻によらず束ねる)

use std::collections::HashMap;

/// (主キーの himo, 値の鍵)
type Name = (u16, Box<[u8]>);

const MAGIC: &[u8; 4] = b"PKFN";
const VERSION: u32 = 4;

/// #581: 束ねた identity (作った peer, その peer での番号) と作成の HLC。
pub(crate) type Bound = ((u32, u32), enchudb_oplog::Hlc);

/// 束ねた identity のカード 1 枚の中身。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Card {
    /// 外した (値が無く版数だけ)
    Gone,
    /// 値 (Number / Tag の語の番号 / Ref の slot)
    Value(u64),
    /// Leaf の中身
    Bytes(Box<[u8]>),
}

/// 束ねた slot の identity ごとのカード: (identity, himo, 中身, 版数)。
pub(crate) type BoundCard = ((u32, u32), u16, Card, enchudb_oplog::Hlc);

/// 名前の年表の 1 行: (宛名の identity, 主キーの himo, 値の鍵, 版数)。
pub(crate) type NameWrite = ((u32, u32), u16, Box<[u8]>, enchudb_oplog::Hlc);

/// カードの束から、 列ごとに版数の最も新しいカード (identity の集まりの今の値)。
pub(crate) fn newest_per_himo(cards: &[BoundCard]) -> Vec<(u16, Card, enchudb_oplog::Hlc)> {
    let mut out: Vec<(u16, Card, enchudb_oplog::Hlc)> = Vec::new();
    for (_, himo, card, hlc) in cards {
        match out.iter_mut().find(|(h, _, _)| h == himo) {
            Some(e) if e.2 < *hlc => *e = (*himo, card.clone(), *hlc),
            Some(_) => {}
            None => out.push((*himo, card.clone(), *hlc)),
        }
    }
    out.sort_by_key(|e| e.0);
    out
}

/// 主キーの前の名前。 engine が `RwLock` で包んで持つ。
#[derive(Default, Debug)]
pub(crate) struct PkFormer {
    /// (主キーの himo, 前の値の鍵) → 行の slot
    by_key: HashMap<Name, u32>,
    /// slot → その行の前の名前 (slot を使い回す時に外す)
    by_slot: HashMap<u32, Vec<Name>>,
    /// (作った peer, その peer での番号) → slot を使い回した時に移した、 消えた行の名前 (#545)
    by_identity: HashMap<(u32, u32), Vec<Name>>,
    /// #581: slot → 名前で束ねた identity とその作成の HLC (束ねた slot だけ)
    bound: HashMap<u32, Vec<Bound>>,
    /// slot → 束ねた identity ごとのカード (束ねた slot だけ)
    cards: HashMap<u32, Vec<BoundCard>>,
    /// slot → 名前の年表。 改名した行だけ
    timeline: HashMap<u32, Vec<NameWrite>>,
}

impl PkFormer {
    pub(crate) fn is_empty(&self) -> bool {
        self.by_key.is_empty()
            && self.by_identity.is_empty()
            && self.bound.is_empty()
            && self.cards.is_empty()
            && self.timeline.is_empty()
    }

    /// 行 `slot` は名前の年表を持つか (改名した行)。
    pub(crate) fn has_timeline(&self, slot: u32) -> bool {
        self.timeline.contains_key(&slot)
    }

    /// 行 `slot` の名前の年表に、 identity `id` 宛ての名前 `(himo, key)` への版数 `hlc` の書き込みを載せる (LWW で負けた書き込みも)。
    /// 変えたら true。
    pub(crate) fn note_name(&mut self, slot: u32, id: (u32, u32), himo: u16, key: &[u8], hlc: enchudb_oplog::Hlc) -> bool {
        let v = self.timeline.entry(slot).or_default();
        if v.iter().any(|(i, h, k, t)| *i == id && *h == himo && *t == hlc && &k[..] == key) {
            return false;
        }
        v.push((id, himo, key.into(), hlc));
        true
    }

    /// 年表の上で、 行 `slot` は時刻 `t` に名前 `(himo, key)` だったか (module doc の 「名前の年表」)。 年表の無い行は None
    /// (いつの名前かは分からない)。
    pub(crate) fn held_at(&self, slot: u32, himo: u16, key: &[u8], t: enchudb_oplog::Hlc) -> Option<bool> {
        let v = self.timeline.get(&slot)?;
        let mut writes: Vec<&NameWrite> = v.iter().filter(|w| w.1 == himo).collect();
        if writes.is_empty() {
            return None;
        }
        writes.sort_by_key(|w| (w.0, w.3));
        // identity ごとの作成 (最初の書き込み) と、 値の変わる書き込み (改名、 行の全部の identity に当たる)
        let mut created: Vec<&NameWrite> = Vec::new();
        let mut renames: Vec<&NameWrite> = Vec::new();
        for (i, w) in writes.iter().enumerate() {
            match i.checked_sub(1).map(|j| writes[j]).filter(|prev| prev.0 == w.0) {
                None => created.push(w),
                Some(prev) if prev.2 != w.2 => renames.push(w),
                Some(_) => {}
            }
        }
        Some(created.iter().any(|c| {
            let at = renames.iter().filter(|r| r.3 > c.3 && r.3 <= t).max_by_key(|r| r.3);
            match at {
                Some(r) => &r.2[..] == key,
                None => &c.2[..] == key,
            }
        }))
    }

    /// #614: 行 `slot` の名前の年表で、 名前 `(himo, key)` への最初の書き込みの版数 (その名前で作られた / その名前に改名した時刻。
    /// LWW で負けた書き込みも)。 年表の無い行と、 年表にその名前の無い行は None。
    pub(crate) fn first_held(&self, slot: u32, himo: u16, key: &[u8]) -> Option<enchudb_oplog::Hlc> {
        self.timeline.get(&slot)?.iter().filter(|w| w.1 == himo && &w.2[..] == key).map(|w| w.3).min()
    }

    /// 行 `slot` の名前の年表で、 identity `id` 宛ての最初の書き込みの版数 (その identity の作成)。
    pub(crate) fn first_name_write(&self, slot: u32, id: (u32, u32)) -> Option<enchudb_oplog::Hlc> {
        self.timeline.get(&slot)?.iter().filter(|w| w.0 == id).map(|w| w.3).min()
    }

    /// 年表の上で、 行 `slot` は時刻 `t` 以後に名前 `(himo, key)` だったことがあるか (`t` にその名前だったか、 `t` 以後にその名前へ
    /// 改名した)。 時刻 `t` にその名前で作られた行と同じ時刻に同じ名前を持つ = 同じデッキ。 年表の無い行は None。
    pub(crate) fn held_since(&self, slot: u32, himo: u16, key: &[u8], t: enchudb_oplog::Hlc) -> Option<bool> {
        let held = self.held_at(slot, himo, key, t)?;
        let v = self.timeline.get(&slot)?;
        // 改名 (identity ごとに値の変わる書き込み) のうち t 以後にその名前にしたもの
        let renamed_to = v.iter().any(|w| {
            w.1 == himo
                && &w.2[..] == key
                && w.3 >= t
                && v.iter().any(|e| e.0 == w.0 && e.1 == himo && e.3 < w.3)
                && self.name_before(slot, w).is_some_and(|prev| prev != key)
        });
        Some(held || renamed_to)
    }

    /// 年表で、 書き込み `w` の直前の同じ identity の名前。
    fn name_before(&self, slot: u32, w: &NameWrite) -> Option<&[u8]> {
        self.timeline.get(&slot)?.iter().filter(|e| e.0 == w.0 && e.1 == w.1 && e.3 < w.3).max_by_key(|e| e.3).map(|e| &e.2[..])
    }

    /// 行 `slot` の名前の年表のうち、 identity `ids` 宛ての書き込みを取り出す (改名の時刻で切り離す時、 移す側の分)。
    pub(crate) fn take_names(&mut self, slot: u32, ids: &[(u32, u32)]) -> Vec<NameWrite> {
        let Some(v) = self.timeline.get_mut(&slot) else { return Vec::new() };
        let (taken, kept): (Vec<_>, Vec<_>) = std::mem::take(v).into_iter().partition(|w| ids.contains(&w.0));
        if kept.is_empty() {
            self.timeline.remove(&slot);
        } else {
            *self.timeline.get_mut(&slot).unwrap() = kept;
        }
        taken
    }

    /// 改名の時刻で切り離した後: 行 `from` の前の名前のうち、 年表で `from` に残った identity が一度も名乗らず、 `to` に移った
    /// identity が名乗った名前を `to` の前の名前にする。 変えたら true。
    pub(crate) fn move_names_by_timeline(&mut self, from: u32, to: u32) -> bool {
        let named = |slot: u32, n: &Name| self.timeline.get(&slot).is_some_and(|v| v.iter().any(|w| w.1 == n.0 && w.2 == n.1));
        let moving: Vec<Name> = self.by_slot.get(&from).into_iter().flatten().filter(|n| !named(from, n) && named(to, n)).cloned().collect();
        for k in &moving {
            self.insert(k.0, &k.1, to);
        }
        !moving.is_empty()
    }

    /// 行 `slot` の名前の年表に書き込みを載せる。
    pub(crate) fn put_names(&mut self, slot: u32, names: Vec<NameWrite>) {
        for (id, himo, key, hlc) in names {
            self.note_name(slot, id, himo, &key, hlc);
        }
    }

    /// 束ねた slot (束ねた identity のある slot) の一覧。
    pub(crate) fn bound_slots(&self) -> impl Iterator<Item = u32> + '_ {
        self.bound.keys().copied()
    }

    /// slot `slot` の identity `id` の列 `himo` のカードを、 版数 `hlc` の `card` にする (持っているカードの方が新しければ何も
    /// しない。 版数の無い書き込みはいつも置き換える)。 変えたら true。
    pub(crate) fn note_card(&mut self, slot: u32, id: (u32, u32), himo: u16, card: Card, hlc: enchudb_oplog::Hlc) -> bool {
        let v = self.cards.entry(slot).or_default();
        match v.iter_mut().find(|(i, h, _, _)| *i == id && *h == himo) {
            Some(e) if hlc != enchudb_oplog::Hlc::ZERO && e.3 >= hlc => false,
            Some(e) => {
                e.2 = card;
                e.3 = hlc;
                true
            }
            None => {
                v.push((id, himo, card, hlc));
                true
            }
        }
    }

    /// slot `slot` の identity `id` の列 `himo` のカード。
    pub(crate) fn card_of(&self, slot: u32, id: (u32, u32), himo: u16) -> Option<(&Card, enchudb_oplog::Hlc)> {
        self.cards.get(&slot)?.iter().find(|(i, h, _, _)| *i == id && *h == himo).map(|(_, _, c, t)| (c, *t))
    }

    /// slot `slot` から identity `ids` のカードを取り出す。
    pub(crate) fn take_cards(&mut self, slot: u32, ids: &[(u32, u32)]) -> Vec<BoundCard> {
        let Some(v) = self.cards.get_mut(&slot) else { return Vec::new() };
        let (taken, kept): (Vec<_>, Vec<_>) = std::mem::take(v).into_iter().partition(|c| ids.contains(&c.0));
        if kept.is_empty() {
            self.cards.remove(&slot);
        } else {
            *self.cards.get_mut(&slot).unwrap() = kept;
        }
        taken
    }

    /// slot `slot` のカードを全部取り出す。
    pub(crate) fn drop_cards(&mut self, slot: u32) -> Vec<BoundCard> {
        self.cards.remove(&slot).unwrap_or_default()
    }

    /// slot `slot` にカードを積む (`note_card` と同じく identity と列ごとに新しい方)。
    pub(crate) fn put_cards(&mut self, slot: u32, cards: Vec<BoundCard>) {
        for (id, himo, card, hlc) in cards {
            self.note_card(slot, id, himo, card, hlc);
        }
    }

    /// #581: slot `slot` に束ねた identity `id` (作成 `created`) を足す。 既にあれば作成の早い方。 変えたら true。
    pub(crate) fn add_bound(&mut self, slot: u32, id: (u32, u32), created: enchudb_oplog::Hlc) -> bool {
        let v = self.bound.entry(slot).or_default();
        match v.iter_mut().find(|(i, _)| *i == id) {
            Some((_, c)) if *c <= created => false,
            Some((_, c)) => {
                *c = created;
                true
            }
            None => {
                v.push((id, created));
                true
            }
        }
    }

    /// #581: slot `slot` に束ねた identity。
    pub(crate) fn bound_of(&self, slot: u32) -> &[Bound] {
        self.bound.get(&slot).map_or(&[], |v| &v[..])
    }

    /// #581: slot `slot` に束ねた identity を `v` に置き換える (空なら外す)。
    pub(crate) fn set_bound(&mut self, slot: u32, v: Vec<Bound>) {
        if v.is_empty() {
            self.bound.remove(&slot);
        } else {
            self.bound.insert(slot, v);
        }
    }

    /// 前に `(himo, key)` だった行の slot。
    pub(crate) fn slot_of(&self, himo: u16, key: &[u8]) -> Option<u32> {
        self.by_key.get(&(himo, Box::<[u8]>::from(key))).copied()
    }

    /// 行 `slot` の前の名前 (主キーの himo, 値の鍵)。
    pub(crate) fn names_of(&self, slot: u32) -> impl Iterator<Item = (u16, &[u8])> {
        self.by_slot.get(&slot).into_iter().flatten().map(|(h, k)| (*h, &k[..]))
    }

    /// 行 `slot` の前の名前に `(himo, key)` を足す。 別の行の前の名前だったなら、 後から改名した方 (この行) にする。
    /// 変えたら true。
    pub(crate) fn insert(&mut self, himo: u16, key: &[u8], slot: u32) -> bool {
        let k = (himo, Box::<[u8]>::from(key));
        match self.by_key.insert(k.clone(), slot) {
            Some(old) if old == slot => return false,
            Some(old) => {
                if let Some(v) = self.by_slot.get_mut(&old) {
                    v.retain(|x| *x != k);
                }
            }
            None => {}
        }
        self.by_slot.entry(slot).or_default().push(k);
        true
    }

    /// identity `(peer, foreign_local)` に名前 `(himo, key)` を足す (消えた行の slot を使い回す時、 #545)。 変えたら true。
    pub(crate) fn insert_identity(&mut self, peer: u32, foreign_local: u32, himo: u16, key: &[u8]) -> bool {
        let names = self.by_identity.entry((peer, foreign_local)).or_default();
        if names.iter().any(|(h, k)| *h == himo && &k[..] == key) {
            return false;
        }
        names.push((himo, key.into()));
        true
    }

    /// identity `(peer, foreign_local)` に移した名前。
    pub(crate) fn names_of_identity(&self, peer: u32, foreign_local: u32) -> impl Iterator<Item = (u16, &[u8])> {
        self.by_identity.get(&(peer, foreign_local)).into_iter().flatten().map(|(h, k)| (*h, &k[..]))
    }

    /// slot `from` の前の名前と名前の年表を slot `to` に移す (行を畳む時)。 変えたら true。
    pub(crate) fn move_names(&mut self, from: u32, to: u32) -> bool {
        let moved_timeline = match self.timeline.remove(&from) {
            Some(v) => {
                self.put_names(to, v);
                true
            }
            None => false,
        };
        let Some(keys) = self.by_slot.remove(&from) else { return moved_timeline };
        for k in keys {
            if let Some(e) = self.by_key.get_mut(&k)
                && *e == from
            {
                *e = to;
                let v = self.by_slot.entry(to).or_default();
                if !v.contains(&k) {
                    v.push(k);
                }
            }
        }
        true
    }

    /// slot を別の行に使い回す前に、 その行の前の名前を外す。 変えたら true。
    pub(crate) fn forget_slot(&mut self, slot: u32) -> bool {
        let unbound =
            self.bound.remove(&slot).is_some() | self.cards.remove(&slot).is_some() | self.timeline.remove(&slot).is_some();
        let Some(keys) = self.by_slot.remove(&slot) else { return unbound };
        for k in keys {
            if self.by_key.get(&k) == Some(&slot) {
                self.by_key.remove(&k);
            }
        }
        true
    }

    pub(crate) fn serialize(&self) -> Vec<u8> {
        let mut entries: Vec<_> = self.by_key.iter().collect();
        entries.sort();
        let mut out = Vec::with_capacity(12 + entries.len() * 16);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for ((himo, key), slot) in entries {
            out.extend_from_slice(&himo.to_le_bytes());
            out.extend_from_slice(&slot.to_le_bytes());
            out.extend_from_slice(&(key.len() as u32).to_le_bytes());
            out.extend_from_slice(key);
        }
        let mut idents: Vec<(&(u32, u32), &(u16, Box<[u8]>))> =
            self.by_identity.iter().flat_map(|(id, names)| names.iter().map(move |n| (id, n))).collect();
        idents.sort();
        out.extend_from_slice(&(idents.len() as u32).to_le_bytes());
        for ((peer, fl), (himo, key)) in idents {
            out.extend_from_slice(&peer.to_le_bytes());
            out.extend_from_slice(&fl.to_le_bytes());
            out.extend_from_slice(&himo.to_le_bytes());
            out.extend_from_slice(&(key.len() as u32).to_le_bytes());
            out.extend_from_slice(key);
        }
        let mut bound: Vec<(u32, Bound)> =
            self.bound.iter().flat_map(|(&slot, v)| v.iter().map(move |b| (slot, *b))).collect();
        bound.sort();
        out.extend_from_slice(&(bound.len() as u32).to_le_bytes());
        for (slot, ((peer, fl), h)) in bound {
            out.extend_from_slice(&slot.to_le_bytes());
            out.extend_from_slice(&peer.to_le_bytes());
            out.extend_from_slice(&fl.to_le_bytes());
            out.extend_from_slice(&h.wall.to_le_bytes());
            out.extend_from_slice(&h.logical.to_le_bytes());
            out.extend_from_slice(&h.peer.to_le_bytes());
        }
        let mut cards: Vec<(u32, &BoundCard)> =
            self.cards.iter().flat_map(|(&slot, v)| v.iter().map(move |c| (slot, c))).collect();
        cards.sort_by_key(|(slot, (id, himo, _, _))| (*slot, *id, *himo));
        out.extend_from_slice(&(cards.len() as u32).to_le_bytes());
        for (slot, ((peer, fl), himo, card, h)) in cards {
            out.extend_from_slice(&slot.to_le_bytes());
            out.extend_from_slice(&peer.to_le_bytes());
            out.extend_from_slice(&fl.to_le_bytes());
            out.extend_from_slice(&himo.to_le_bytes());
            out.extend_from_slice(&h.wall.to_le_bytes());
            out.extend_from_slice(&h.logical.to_le_bytes());
            out.extend_from_slice(&h.peer.to_le_bytes());
            match card {
                Card::Gone => out.push(0),
                Card::Value(v) => {
                    out.push(1);
                    out.extend_from_slice(&v.to_le_bytes());
                }
                Card::Bytes(b) => {
                    out.push(2);
                    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                    out.extend_from_slice(b);
                }
            }
        }
        let mut names: Vec<(u32, &NameWrite)> =
            self.timeline.iter().flat_map(|(&slot, v)| v.iter().map(move |n| (slot, n))).collect();
        names.sort();
        out.extend_from_slice(&(names.len() as u32).to_le_bytes());
        for (slot, ((peer, fl), himo, key, h)) in names {
            out.extend_from_slice(&slot.to_le_bytes());
            out.extend_from_slice(&peer.to_le_bytes());
            out.extend_from_slice(&fl.to_le_bytes());
            out.extend_from_slice(&himo.to_le_bytes());
            out.extend_from_slice(&h.wall.to_le_bytes());
            out.extend_from_slice(&h.logical.to_le_bytes());
            out.extend_from_slice(&h.peer.to_le_bytes());
            out.extend_from_slice(&(key.len() as u32).to_le_bytes());
            out.extend_from_slice(key);
        }
        out
    }

    /// `serialize` の逆。 magic / 版の不一致・切れた中身は Err。
    pub(crate) fn deserialize(buf: &[u8]) -> Result<Self, String> {
        if buf.len() < 12 || &buf[0..4] != MAGIC {
            return Err("pkformer sidecar: bad magic".into());
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if !(1..=VERSION).contains(&version) {
            return Err(format!("pkformer sidecar: unsupported version {version}"));
        }
        let count = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let mut out = PkFormer::default();
        let mut p = 12;
        for _ in 0..count {
            let head = buf.get(p..p + 10).ok_or("pkformer sidecar: torn entry")?;
            let himo = u16::from_le_bytes(head[0..2].try_into().unwrap());
            let slot = u32::from_le_bytes(head[2..6].try_into().unwrap());
            let len = u32::from_le_bytes(head[6..10].try_into().unwrap()) as usize;
            p += 10;
            let key = buf.get(p..p + len).ok_or("pkformer sidecar: torn key")?;
            p += len;
            out.insert(himo, key, slot);
        }
        if version >= 2 {
            let count = buf.get(p..p + 4).ok_or("pkformer sidecar: torn identity count")?;
            let count = u32::from_le_bytes(count.try_into().unwrap());
            p += 4;
            for _ in 0..count {
                let head = buf.get(p..p + 14).ok_or("pkformer sidecar: torn identity entry")?;
                let peer = u32::from_le_bytes(head[0..4].try_into().unwrap());
                let fl = u32::from_le_bytes(head[4..8].try_into().unwrap());
                let himo = u16::from_le_bytes(head[8..10].try_into().unwrap());
                let len = u32::from_le_bytes(head[10..14].try_into().unwrap()) as usize;
                p += 14;
                let key = buf.get(p..p + len).ok_or("pkformer sidecar: torn identity key")?;
                p += len;
                out.insert_identity(peer, fl, himo, key);
            }
        }
        if version >= 3 {
            let count = buf.get(p..p + 4).ok_or("pkformer sidecar: torn bound count")?;
            let count = u32::from_le_bytes(count.try_into().unwrap());
            p += 4;
            for _ in 0..count {
                let e = buf.get(p..p + 28).ok_or("pkformer sidecar: torn bound entry")?;
                let u = |i: usize| u32::from_le_bytes(e[i..i + 4].try_into().unwrap());
                let created = enchudb_oplog::Hlc { wall: u64::from_le_bytes(e[12..20].try_into().unwrap()), logical: u(20), peer: u(24) };
                out.add_bound(u(0), (u(4), u(8)), created);
                p += 28;
            }
        }
        if version >= 4 {
            let count = buf.get(p..p + 4).ok_or("pkformer sidecar: torn card count")?;
            let count = u32::from_le_bytes(count.try_into().unwrap());
            p += 4;
            for _ in 0..count {
                let e = buf.get(p..p + 31).ok_or("pkformer sidecar: torn card")?;
                let u = |i: usize| u32::from_le_bytes(e[i..i + 4].try_into().unwrap());
                let himo = u16::from_le_bytes(e[12..14].try_into().unwrap());
                let hlc = enchudb_oplog::Hlc { wall: u64::from_le_bytes(e[14..22].try_into().unwrap()), logical: u(22), peer: u(26) };
                let (slot, id, kind) = (u(0), (u(4), u(8)), e[30]);
                p += 31;
                let card = match kind {
                    0 => Card::Gone,
                    1 => {
                        let v = buf.get(p..p + 8).ok_or("pkformer sidecar: torn card value")?;
                        p += 8;
                        Card::Value(u64::from_le_bytes(v.try_into().unwrap()))
                    }
                    2 => {
                        let n = buf.get(p..p + 4).ok_or("pkformer sidecar: torn card length")?;
                        let n = u32::from_le_bytes(n.try_into().unwrap()) as usize;
                        p += 4;
                        let b = buf.get(p..p + n).ok_or("pkformer sidecar: torn card bytes")?;
                        p += n;
                        Card::Bytes(b.into())
                    }
                    k => return Err(format!("pkformer sidecar: unknown card kind {k}")),
                };
                out.note_card(slot, id, himo, card, hlc);
            }
            let count = buf.get(p..p + 4).ok_or("pkformer sidecar: torn timeline count")?;
            let count = u32::from_le_bytes(count.try_into().unwrap());
            p += 4;
            for _ in 0..count {
                let e = buf.get(p..p + 34).ok_or("pkformer sidecar: torn timeline entry")?;
                let u = |i: usize| u32::from_le_bytes(e[i..i + 4].try_into().unwrap());
                let himo = u16::from_le_bytes(e[12..14].try_into().unwrap());
                let hlc = enchudb_oplog::Hlc { wall: u64::from_le_bytes(e[14..22].try_into().unwrap()), logical: u(22), peer: u(26) };
                let (slot, id, len) = (u(0), (u(4), u(8)), u(30) as usize);
                p += 34;
                let key = buf.get(p..p + len).ok_or("pkformer sidecar: torn timeline key")?;
                p += len;
                out.note_name(slot, id, himo, key, hlc);
            }
        }
        if p != buf.len() {
            return Err(format!("pkformer sidecar: {} trailing bytes (corrupt)", buf.len() - p));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(wall: u64) -> enchudb_oplog::Hlc {
        enchudb_oplog::Hlc { wall, logical: 0, peer: 1 }
    }

    #[test]
    fn a_name_moves_to_the_row_that_took_it_last_and_leaves_with_the_slot() {
        let mut f = PkFormer::default();
        assert!(f.insert(3, b"tokyo", 10));
        assert!(!f.insert(3, b"tokyo", 10), "同じ記録を変えたと言う");
        assert!(f.insert(3, b"osaka", 10));
        assert!(f.insert(3, b"tokyo", 11), "別の行が後から同じ前の名前を持った");
        assert_eq!(f.slot_of(3, b"tokyo"), Some(11));
        assert_eq!(f.slot_of(4, b"tokyo"), None, "himo を見ていない");
        assert!(f.move_names(11, 12), "畳んだ行の前の名前を移さない");
        assert_eq!(f.slot_of(3, b"tokyo"), Some(12));
        assert_eq!(f.names_of(11).count(), 0);
        assert!(f.move_names(12, 11));
        assert!(f.forget_slot(10));
        assert_eq!(f.slot_of(3, b"osaka"), None);
        assert_eq!(f.slot_of(3, b"tokyo"), Some(11), "使い回した slot の記録が他の行の記録を消した");
        assert!(!f.forget_slot(10));
    }

    #[test]
    fn round_trips_and_rejects_torn_files() {
        let mut f = PkFormer::default();
        f.insert(3, b"tokyo", 10);
        f.insert(5, &7u64.to_le_bytes(), 12);
        let bytes = f.serialize();
        let g = PkFormer::deserialize(&bytes).unwrap();
        assert_eq!(g.slot_of(3, b"tokyo"), Some(10));
        assert_eq!(g.slot_of(5, &7u64.to_le_bytes()), Some(12));
        for cut in [0, 4, 11, bytes.len() - 1] {
            assert!(PkFormer::deserialize(&bytes[..cut]).is_err(), "{cut} byte で切れた file を読んだ");
        }
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(PkFormer::deserialize(&extra).is_err());
    }

    #[test]
    fn identity_names_round_trip_and_version_1_still_reads() {
        let mut f = PkFormer::default();
        f.insert(3, b"tokyo", 10);
        assert!(f.insert_identity(7, 1, 3, b"osaka"));
        assert!(!f.insert_identity(7, 1, 3, b"osaka"), "同じ名前を変えたと言う");
        assert!(f.insert_identity(7, 1, 3, b"kyoto"));
        assert!(!f.is_empty());
        let bytes = f.serialize();
        let g = PkFormer::deserialize(&bytes).unwrap();
        let mut names: Vec<_> = g.names_of_identity(7, 1).map(|(h, k)| (h, k.to_vec())).collect();
        names.sort();
        assert_eq!(names, vec![(3, b"kyoto".to_vec()), (3, b"osaka".to_vec())]);
        assert_eq!(g.names_of_identity(7, 2).count(), 0);
        assert_eq!(g.slot_of(3, b"tokyo"), Some(10));
        for cut in [bytes.len() - 1, bytes.len() - 6] {
            assert!(PkFormer::deserialize(&bytes[..cut]).is_err(), "{cut} byte で切れた file を読んだ");
        }
        // #581: 束ねた identity も往復する
        let mut b = PkFormer::default();
        let h = |w: u64| enchudb_oplog::Hlc { wall: w, logical: 1, peer: 2 };
        assert!(b.add_bound(5, (7, 1), h(30)));
        assert!(b.add_bound(5, (0, 5), h(10)));
        assert!(!b.add_bound(5, (7, 1), h(40)), "遅い作成で置き換えた");
        assert!(b.add_bound(5, (7, 1), h(20)), "早い作成に置き換えない");
        let back = PkFormer::deserialize(&b.serialize()).unwrap();
        let mut got = back.bound_of(5).to_vec();
        got.sort();
        assert_eq!(got, vec![((0, 5), h(10)), ((7, 1), h(20))]);
        assert!(!back.is_empty());
        let mut c = PkFormer::deserialize(&b.serialize()).unwrap();
        assert!(c.forget_slot(5), "slot を使い回す時に束ねた identity を外さない");
        assert!(c.bound_of(5).is_empty());
        // version 1 (identity の節も束ねた identity の節も無い) / version 2 (束ねた identity の節が無い) / version 3 (カードと
        // 名前の年表の節が無い) も読む
        let mut v1 = PkFormer::default();
        v1.insert(3, b"tokyo", 10);
        for (version, cut) in [(1u32, 16), (2, 12), (3, 8)] {
            let mut old = v1.serialize();
            old[4..8].copy_from_slice(&version.to_le_bytes());
            old.truncate(old.len() - cut); // 後ろの節の数 (0) を落とす
            let g = PkFormer::deserialize(&old).unwrap();
            assert_eq!(g.slot_of(3, b"tokyo"), Some(10), "version {version}");
            assert_eq!(g.held_at(10, 3, b"tokyo", h(1)), None, "version {version}: 年表の無い行に年表がある");
        }
    }

    #[test]
    fn cards_keep_the_newest_per_identity_and_column_and_round_trip() {
        let mut f = PkFormer::default();
        let (a, b) = ((1, 7), (2, 9));
        assert!(f.note_card(5, a, 0, Card::Value(10), h(3)));
        assert!(!f.note_card(5, a, 0, Card::Value(11), h(2)), "古い書き込みでカードを変えた");
        assert!(f.note_card(5, a, 0, Card::Value(12), h(4)));
        assert!(f.note_card(5, a, 1, Card::Bytes(b"memo".as_slice().into()), h(4)));
        assert!(f.note_card(5, b, 0, Card::Value(20), h(6)));
        assert!(f.note_card(5, b, 1, Card::Gone, h(1)));
        assert_eq!(f.card_of(5, a, 0), Some((&Card::Value(12), h(4))));
        assert_eq!(f.card_of(5, a, 2), None);
        assert_eq!(f.card_of(6, a, 0), None, "slot を見ていない");
        // 列ごとに版数の新しいカード (identity の集まりの今の値)
        let all: Vec<_> = [a, b].iter().flat_map(|&id| [0u16, 1].map(|hm| (id, hm))).filter_map(|(id, hm)| {
            f.card_of(5, id, hm).map(|(c, t)| (id, hm, c.clone(), t))
        }).collect();
        assert_eq!(
            newest_per_himo(&all),
            vec![(0, Card::Value(20), h(6)), (1, Card::Bytes(b"memo".as_slice().into()), h(4))]
        );
        let back = PkFormer::deserialize(&f.serialize()).unwrap();
        assert_eq!(back.card_of(5, a, 1), Some((&Card::Bytes(b"memo".as_slice().into()), h(4))));
        assert_eq!(back.card_of(5, b, 1), Some((&Card::Gone, h(1))));
        assert_eq!(back.card_of(5, b, 0), Some((&Card::Value(20), h(6))));
        let bytes = f.serialize();
        for cut in [bytes.len() - 1, bytes.len() - 5, bytes.len() - 20] {
            assert!(PkFormer::deserialize(&bytes[..cut]).is_err(), "{cut} byte で切れた file を読んだ");
        }
        // identity ごとに取り出す / slot を使い回す時に外す
        let taken = f.take_cards(5, &[b]);
        assert_eq!(taken.len(), 2);
        assert_eq!(f.card_of(5, b, 0), None);
        assert_eq!(f.card_of(5, a, 0), Some((&Card::Value(12), h(4))));
        f.put_cards(8, taken);
        assert_eq!(f.card_of(8, b, 0), Some((&Card::Value(20), h(6))));
        assert!(f.forget_slot(5));
        assert_eq!(f.card_of(5, a, 0), None);
        assert_eq!(f.drop_cards(8).len(), 2);
        assert!(f.is_empty());
    }

    /// 名前の年表: その時刻の名前は版数の順で決まり、 載せた順によらない。 identity ごとに最初の書き込みが作成で、 作成より前は
    /// その名前。 改名は行の全部の identity に当たる。
    #[test]
    fn the_name_timeline_answers_by_version_not_by_arrival() {
        // (identity, 名前, 版数): F (1, 1) は 10 に k1 で作り 30 に k2 へ、 E (2, 1) は 20 に k1 で作り 40 に k0 へ
        let writes = [((1, 1), b"k1".as_slice(), 10), ((2, 1), b"k1", 20), ((1, 1), b"k2", 30), ((2, 1), b"k0", 40)];
        let mut orders = vec![[0usize, 1, 2, 3], [3, 2, 1, 0], [3, 0, 2, 1]];
        for order in orders.drain(..) {
            let mut f = PkFormer::default();
            for i in order {
                f.note_name(7, writes[i].0, 3, writes[i].1, h(writes[i].2));
            }
            assert!(!f.note_name(7, (1, 1), 3, b"k1", h(10)), "同じ書き込みを 2 度載せた");
            let at = |key: &[u8], t| f.held_at(7, 3, key, h(t));
            assert_eq!(at(b"k1", 5), Some(true), "最初の書き込みより前は最初の名前");
            assert_eq!(at(b"k1", 25), Some(true));
            assert_eq!(at(b"k1", 35), Some(false), "F の改名が E に当たらない ({order:?})");
            assert_eq!(at(b"k2", 35), Some(true));
            assert_eq!(at(b"k0", 40), Some(true), "同じ時刻の書き込みを含めない");
            assert_eq!(f.held_at(7, 4, b"k1", h(25)), None, "himo を見ていない");
            assert_eq!(f.held_at(8, 3, b"k1", h(25)), None, "年表の無い行に年表がある");
            // 往復・畳む時に移す・使い回す時に外す
            let mut g = PkFormer::deserialize(&f.serialize()).unwrap();
            assert_eq!(g.held_at(7, 3, b"k2", h(35)), Some(true));
            assert!(g.move_names(7, 9));
            assert_eq!(g.held_at(9, 3, b"k1", h(25)), Some(true));
            assert!(!g.has_timeline(7));
            assert!(g.forget_slot(9));
            assert!(g.is_empty());
        }
        // 束ねる前の別々の行はそれぞれの名前を持つ: G (3, 1) は 30 に k2 で作り 40 に k0 へ、 E (4, 1) は 10 に k0 で作った。
        // 20 に作られた k2 の行は G と同じデッキ (G の作成より前は G の作成の名前)
        let mut g = PkFormer::default();
        for (id, key, t) in [((3, 1), b"k2".as_slice(), 30), ((3, 1), b"k0", 40), ((4, 1), b"k0", 10)] {
            g.note_name(5, id, 3, key, h(t));
        }
        assert_eq!(g.held_at(5, 3, b"k2", h(20)), Some(true), "E の作成の名前で G の前の名前を隠した");
        assert_eq!(g.held_at(5, 3, b"k0", h(20)), Some(true));
        assert_eq!(g.held_at(5, 3, b"k2", h(45)), Some(false));
        // その時刻以後にその名前だったことがあるか: E (4, 1) は 10 に k5 で作り 50 に k0 へ改名、 60 に k1 へ改名。 20 に k0 で作られた
        // 行は、 50〜60 に E と同じ k0 を持つ (同じデッキ)。 E が改名で名乗っていない k2 は持たない。 70 に作られた k0 の行は持たない
        let mut e = PkFormer::default();
        for (id, key, t) in [((4, 1), b"k5".as_slice(), 10), ((4, 1), b"k0", 50), ((4, 1), b"k1", 60)] {
            e.note_name(5, id, 3, key, h(t));
        }
        assert_eq!(e.held_at(5, 3, b"k0", h(20)), Some(false));
        assert_eq!(e.held_since(5, 3, b"k0", h(20)), Some(true), "後で同じ名前を持ったのに束ねない");
        assert_eq!(e.held_since(5, 3, b"k0", h(70)), Some(false), "名前を離れた後に作られた行を束ねる");
        assert_eq!(e.held_since(5, 3, b"k5", h(20)), Some(true), "作成の名前を作成より後の時刻で持たない");
        assert_eq!(e.held_since(5, 3, b"k2", h(20)), Some(false));
        assert_eq!(e.held_since(9, 3, b"k0", h(20)), None, "年表の無い行に年表がある");
        let taken = g.take_names(5, &[(4, 1)]);
        assert_eq!(taken.len(), 1);
        assert_eq!(g.held_at(5, 3, b"k0", h(20)), Some(false), "取り出した identity の名前が残った");
        g.put_names(6, taken);
        assert_eq!(g.held_at(6, 3, b"k0", h(20)), Some(true));
        let bytes = {
            let mut f = PkFormer::default();
            f.note_name(7, (1, 1), 3, b"k1", h(10));
            f.serialize()
        };
        for cut in [bytes.len() - 1, bytes.len() - 3, bytes.len() - 20] {
            assert!(PkFormer::deserialize(&bytes[..cut]).is_err(), "{cut} byte で切れた file を読んだ");
        }
    }
}
