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
//! 永続化は DB directory の `pkformer` と、 その後に変わった分の追記 `pkformer.log` (#720)。 無ければ空。 読まない旧
//! binary は前の名前を知らないだけ (前と同じ、 今の名前でだけ束ねる)。
//!
//! 前は変わるたびに `pkformer` をまるごと書いていた。 束ねた行は identity ごとのカードを Leaf の中身ごと持つので、 同じ名前の
//! 行を 2 台が 2,000 個ずつ作る表で 1.36 MB になり、 1 行の書き換えのたびに全部を書き直した。 今は `eidmap.log` (#560) と同じ形:
//!
//! - 変える method は、 変えた slot / identity に印を付ける (`LogTrack`)。 書き出しは、 印の付いた slot / identity の**今の中身**
//!   (slot なら前の名前・束ねた identity・カード・名前の年表の全部) を 1 record にして log に足す。 読む時は record の順に、
//!   その slot / identity の中身を置き換える
//! - `pkformer` の形は変えない (版 4)。 log を読まない 0.37.4 以前は `pkformer` だけを読む。 きれいに閉じる時にまとめ直す
//!   (`pkformer` を書き直して log を header だけにする) ので、 きれいに閉じた後は全部ある。 落ちた後 (log に record が残って
//!   いる) に 0.37.4 以前で開くと、 log の分 (最後にまとめ直した後のカード・年表・前の名前の変化) を読まない
//! - log の header は、 続く `pkformer` の長さと FNV-1a 64。 合わない log は読まない (まとめ直しの途中で落ちた跡か、 log を
//!   知らない binary が `pkformer` を書き直した跡)。 まとめ直しは `pkformer` → 空の log の順に置き換える
//! - 切れた / 壊れた / 知らない種類の record から後は捨て、 次の書き出しでまとめ直す (切れた跡の後ろに追記すると、 次に読む
//!   時にそこで止まって失う)
//! - まとめ直すのは他に、 disk (`pkformer` + log) が今の中身の 2 倍 + 64 KiB を越えた時 (書き換えが溜まった / 消えた行の
//!   カードを落として中身が減った)。 今の中身の大きさは slot / identity ごとの record の長さの和で持つ
//! - 消えた行のカードは落とす (engine の `drop_cards_of_dead_slot`。 カードは生きた行を切り離す時にだけ使う)
//!
//! log の file: magic "PKFL"(4) + version u32 + 続く `pkformer` の長さ u64 + その FNV-1a 64、 続いて record: 種類 u8 (1 = slot /
//! 2 = identity) + 中身の長さ u32 + 中身 + そこまでの FNV-1a 32。 slot の中身は slot u32、 前の名前 (数 u32、 (himo u16, 鍵の長さ
//! u32, 鍵))、 束ねた identity (数、 (peer, 番号, HLC 16 B))、 カード (数、 (peer, 番号, himo, HLC, 種類 u8, 値 u64 か 長さ u32 +
//! 中身))、 名前の年表 (数、 (peer, 番号, HLC, himo, 鍵の長さ, 鍵))。 identity の中身は peer u32、 番号 u32、 名前 (数、 (himo,
//! 鍵の長さ, 鍵))。
//!
//! file: magic "PKFN"(4) + version u32 + count u32、 続いて (himo u16, slot u32, 鍵の長さ u32, 鍵) × count。 version 2 は続けて
//! identity の名前: count u32、 (peer u32, その peer での番号 u32, himo u16, 鍵の長さ u32, 鍵) × count。 version 3 は続けて
//! 束ねた identity: count u32、 (slot u32, peer u32, その peer での番号 u32, wall u64, logical u32, HLC の peer u32) × count。
//! version 4 は続けて束ねた identity のカード: count u32、 (slot u32, peer u32, その peer での番号 u32, himo u16, wall u64,
//! logical u32, HLC の peer u32, 種類 u8 (0 外した / 1 値 / 2 中身), 値 u64 か 中身の長さ u32 + 中身) × count、 名前の年表:
//! count u32、 (slot u32, peer u32, その peer での番号 u32, himo u16, wall u64, logical u32, HLC の peer u32, 鍵の長さ u32, 鍵)
//! × count (version 1〜3 も読む。
//! 年表の無い行の前の名前は、 前と同じく作成の時刻によらず束ねる)

use std::collections::{HashMap, HashSet};

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
    /// 追記の log (`pkformer.log`) の様子と、 前の書き出しから変わった slot / identity (#720)
    log: LogTrack,
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
        self.touch(slot);
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

    /// #620: 行 `slot` の identity `id` (作成 `created`) が名前 `(himo, key)` を持ち始めた時刻。 その identity 宛てにその名前への書き込み
    /// があれば最初のもの (その名前で作られた / その名前に改名した)。 無ければ、 作成より後に行の別の identity がその名前に改名した
    /// 最初の時刻 (改名は同じデッキの全部の identity に当たる)。 どちらも無ければ作成。
    pub(crate) fn entered(
        &self,
        slot: u32,
        id: (u32, u32),
        created: enchudb_oplog::Hlc,
        himo: u16,
        key: &[u8],
    ) -> enchudb_oplog::Hlc {
        let Some(v) = self.timeline.get(&slot) else { return created };
        let named = |w: &&NameWrite| w.1 == himo && &w.2[..] == key;
        if let Some(t) = v.iter().filter(named).filter(|w| w.0 == id).map(|w| w.3).min() {
            return t;
        }
        // 別の名前からの改名 (その identity の前の書き込みが別の名前)
        let renamed_into = |w: &&NameWrite| v.iter().any(|e| e.0 == w.0 && e.1 == himo && e.3 < w.3 && &e.2[..] != key);
        v.iter().filter(named).filter(|w| w.3 >= created).filter(renamed_into).map(|w| w.3).min().unwrap_or(created)
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
        if !taken.is_empty() {
            self.touch(slot);
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
            Some(e) if hlc != enchudb_oplog::Hlc::ZERO && e.3 >= hlc => return false,
            Some(e) => {
                e.2 = card;
                e.3 = hlc;
            }
            None => v.push((id, himo, card, hlc)),
        }
        self.touch(slot);
        true
    }

    /// slot `slot` の identity `id` の列 `himo` のカード。
    pub(crate) fn card_of(&self, slot: u32, id: (u32, u32), himo: u16) -> Option<(&Card, enchudb_oplog::Hlc)> {
        self.cards.get(&slot)?.iter().find(|(i, h, _, _)| *i == id && *h == himo).map(|(_, _, c, t)| (c, *t))
    }

    /// slot `slot` のカード全部 (束ねた identity ごと・列ごと)。
    pub(crate) fn cards_of(&self, slot: u32) -> &[BoundCard] {
        self.cards.get(&slot).map_or(&[], |v| &v[..])
    }

    /// 行 `slot` の名前の年表で、 identity `ids` 宛ての書き込みの名前 (主キーの himo, 値の鍵)。 LWW で負けた書き込みも。
    pub(crate) fn names_written_to(&self, slot: u32, ids: &[(u32, u32)]) -> Vec<(u16, Box<[u8]>)> {
        let mut out: Vec<(u16, Box<[u8]>)> = Vec::new();
        for (id, himo, key, _) in self.timeline.get(&slot).into_iter().flatten() {
            if ids.contains(id) && !out.iter().any(|(h, k)| h == himo && k == key) {
                out.push((*himo, key.clone()));
            }
        }
        out
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
        if !taken.is_empty() {
            self.touch(slot);
        }
        taken
    }

    /// slot `slot` のカードを全部取り出す。
    pub(crate) fn drop_cards(&mut self, slot: u32) -> Vec<BoundCard> {
        let out = self.cards.remove(&slot).unwrap_or_default();
        if !out.is_empty() {
            self.touch(slot);
        }
        out
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
            Some((_, c)) if *c <= created => return false,
            Some((_, c)) => *c = created,
            None => v.push((id, created)),
        }
        self.touch(slot);
        true
    }

    /// #581: slot `slot` に束ねた identity。
    pub(crate) fn bound_of(&self, slot: u32) -> &[Bound] {
        self.bound.get(&slot).map_or(&[], |v| &v[..])
    }

    /// #581: slot `slot` に束ねた identity を `v` に置き換える (空なら外す)。
    pub(crate) fn set_bound(&mut self, slot: u32, v: Vec<Bound>) {
        if self.bound_of(slot) != &v[..] {
            self.touch(slot);
        }
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
                self.touch(old);
            }
            None => {}
        }
        self.by_slot.entry(slot).or_default().push(k);
        self.touch(slot);
        true
    }

    /// identity `(peer, foreign_local)` に名前 `(himo, key)` を足す (消えた行の slot を使い回す時、 #545)。 変えたら true。
    pub(crate) fn insert_identity(&mut self, peer: u32, foreign_local: u32, himo: u16, key: &[u8]) -> bool {
        let names = self.by_identity.entry((peer, foreign_local)).or_default();
        if names.iter().any(|(h, k)| *h == himo && &k[..] == key) {
            return false;
        }
        names.push((himo, key.into()));
        self.log.ids.insert((peer, foreign_local));
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
        if moved_timeline {
            self.touch(from);
        }
        let Some(keys) = self.by_slot.remove(&from) else { return moved_timeline };
        self.touch(from);
        self.touch(to);
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
        if unbound {
            self.touch(slot);
        }
        let Some(keys) = self.by_slot.remove(&slot) else { return unbound };
        self.touch(slot);
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
        // 読み込みで付いた印は捨てる (読んだ中身は disk と同じ)
        out.log = LogTrack::default();
        Ok(out)
    }
}

// ---- 追記の log (`pkformer.log`、 #720) ----

const LOG_MAGIC: &[u8; 4] = b"PKFL";
const LOG_VERSION: u32 = 1;
/// log の header の長さ: magic(4) + version u32 + 続く `pkformer` の長さ u64 + その FNV-1a 64。
pub(crate) const LOG_HEADER_LEN: usize = 24;
/// record の種類: slot の今の中身 (前の名前・束ねた identity・カード・名前の年表) / identity に移した名前。
const REC_SLOT: u8 = 1;
const REC_IDENTITY: u8 = 2;
/// まとめ直しの余裕: disk (`pkformer` + log) が今の中身の 2 倍とこの和を越えたらまとめ直す。
const LOG_SLACK: u64 = 64 * 1024;

/// 追記の log の様子と、 前の書き出しから変わった所。
#[derive(Default, Debug)]
struct LogTrack {
    /// 前の書き出しの後に変わった slot
    slots: HashSet<u32>,
    /// 前の書き出しの後に変わった identity (移した名前)
    ids: HashSet<(u32, u32)>,
    /// log が今の `pkformer` の続きとして信用できるか。 false なら次の書き出しでまとめ直す (log が無い / header が合わない /
    /// 切れた record がある / 前の書き出しが失敗した)
    ok: bool,
    /// disk の `pkformer` の長さ
    snapshot_bytes: u64,
    /// disk の log の長さ (header 込み)
    log_bytes: u64,
    /// 今の中身の大きさ (中身のある slot / identity の record の長さの和。 まとめ直した `pkformer` の長さの目安)
    live_bytes: u64,
    /// slot → その record の長さ (中身のある slot だけ。 `live_bytes` を変わった分だけ直すため)
    slot_bytes: HashMap<u32, u32>,
    /// identity → その record の長さ
    id_bytes: HashMap<(u32, u32), u32>,
}

/// 書き出す仕事 (`PkFormer::take_for_persist`)。
#[derive(Debug)]
pub(crate) enum Persist {
    /// `pkformer` を `snapshot` に書き直し (空なら file を消す)、 log を `log` (header だけ) に置き換える
    Rewrite { snapshot: Vec<u8>, log: Vec<u8> },
    /// log に `bytes` を追記する
    Append { bytes: Vec<u8> },
}

/// `pkformer` の bytes `snapshot` (無ければ空) の続きの、 record の無い log。
pub(crate) fn log_header(snapshot: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(LOG_HEADER_LEN);
    out.extend_from_slice(LOG_MAGIC);
    out.extend_from_slice(&LOG_VERSION.to_le_bytes());
    out.extend_from_slice(&(snapshot.len() as u64).to_le_bytes());
    out.extend_from_slice(&crate::change_log::fnv1a64(snapshot).to_le_bytes());
    out
}

fn put_hlc(out: &mut Vec<u8>, h: enchudb_oplog::Hlc) {
    out.extend_from_slice(&h.wall.to_le_bytes());
    out.extend_from_slice(&h.logical.to_le_bytes());
    out.extend_from_slice(&h.peer.to_le_bytes());
}

fn put_key(out: &mut Vec<u8>, himo: u16, key: &[u8]) {
    out.extend_from_slice(&himo.to_le_bytes());
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(key);
}

/// record 1 つ: 種類 u8 + 中身の長さ u32 + 中身 + そこまでの FNV-1a 32。
fn push_record(out: &mut Vec<u8>, kind: u8, payload: &[u8]) {
    let start = out.len();
    out.push(kind);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
    let sum = crate::integrity::fnv1a_region(&out[start..]);
    out.extend_from_slice(&sum.to_le_bytes());
}

/// 読む側の位置付きの切り出し (足りなければ None)。
struct Cur<'a>(&'a [u8]);
impl<'a> Cur<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let (a, b) = self.0.split_at_checked(n)?;
        self.0 = b;
        Some(a)
    }
    fn u8(&mut self) -> Option<u8> {
        self.take(1).map(|b| b[0])
    }
    fn u16(&mut self) -> Option<u16> {
        self.take(2).map(|b| u16::from_le_bytes(b.try_into().unwrap()))
    }
    fn u32(&mut self) -> Option<u32> {
        self.take(4).map(|b| u32::from_le_bytes(b.try_into().unwrap()))
    }
    fn u64(&mut self) -> Option<u64> {
        self.take(8).map(|b| u64::from_le_bytes(b.try_into().unwrap()))
    }
    fn hlc(&mut self) -> Option<enchudb_oplog::Hlc> {
        Some(enchudb_oplog::Hlc { wall: self.u64()?, logical: self.u32()?, peer: self.u32()? })
    }
    fn id(&mut self) -> Option<(u32, u32)> {
        Some((self.u32()?, self.u32()?))
    }
    fn key(&mut self) -> Option<Name> {
        let himo = self.u16()?;
        let n = self.u32()? as usize;
        Some((himo, self.take(n)?.into()))
    }
}

impl PkFormer {
    fn touch(&mut self, slot: u32) {
        self.log.slots.insert(slot);
    }

    /// slot `slot` の今の中身の record (何も無い slot は中身の空の record = 外す)。
    fn encode_slot(&self, slot: u32, out: &mut Vec<u8>) {
        let mut p = Vec::new();
        p.extend_from_slice(&slot.to_le_bytes());
        let names = self.by_slot.get(&slot).map_or(&[][..], |v| &v[..]);
        p.extend_from_slice(&(names.len() as u32).to_le_bytes());
        for (himo, key) in names {
            put_key(&mut p, *himo, key);
        }
        let bound = self.bound_of(slot);
        p.extend_from_slice(&(bound.len() as u32).to_le_bytes());
        for ((peer, fl), h) in bound {
            p.extend_from_slice(&peer.to_le_bytes());
            p.extend_from_slice(&fl.to_le_bytes());
            put_hlc(&mut p, *h);
        }
        let cards = self.cards_of(slot);
        p.extend_from_slice(&(cards.len() as u32).to_le_bytes());
        for ((peer, fl), himo, card, h) in cards {
            p.extend_from_slice(&peer.to_le_bytes());
            p.extend_from_slice(&fl.to_le_bytes());
            p.extend_from_slice(&himo.to_le_bytes());
            put_hlc(&mut p, *h);
            match card {
                Card::Gone => p.push(0),
                Card::Value(v) => {
                    p.push(1);
                    p.extend_from_slice(&v.to_le_bytes());
                }
                Card::Bytes(b) => {
                    p.push(2);
                    p.extend_from_slice(&(b.len() as u32).to_le_bytes());
                    p.extend_from_slice(b);
                }
            }
        }
        let names = self.timeline.get(&slot).map_or(&[][..], |v| &v[..]);
        p.extend_from_slice(&(names.len() as u32).to_le_bytes());
        for ((peer, fl), himo, key, h) in names {
            p.extend_from_slice(&peer.to_le_bytes());
            p.extend_from_slice(&fl.to_le_bytes());
            put_hlc(&mut p, *h);
            put_key(&mut p, *himo, key);
        }
        push_record(out, REC_SLOT, &p);
    }

    /// identity `id` に移した名前の record。
    fn encode_identity(&self, id: (u32, u32), out: &mut Vec<u8>) {
        let mut p = Vec::new();
        p.extend_from_slice(&id.0.to_le_bytes());
        p.extend_from_slice(&id.1.to_le_bytes());
        let names = self.by_identity.get(&id).map_or(&[][..], |v| &v[..]);
        p.extend_from_slice(&(names.len() as u32).to_le_bytes());
        for (himo, key) in names {
            put_key(&mut p, *himo, key);
        }
        push_record(out, REC_IDENTITY, &p);
    }

    /// record の中身を当てる (slot / identity の中身をその record の物に置き換える)。 読めなければ None (何も変えない)。
    fn apply_record(&mut self, kind: u8, payload: &[u8]) -> Option<()> {
        let mut c = Cur(payload);
        match kind {
            REC_SLOT => {
                let slot = c.u32()?;
                let names: Vec<Name> = (0..c.u32()?).map(|_| c.key()).collect::<Option<_>>()?;
                let bound: Vec<Bound> = (0..c.u32()?).map(|_| Some((c.id()?, c.hlc()?))).collect::<Option<_>>()?;
                let cards: Vec<BoundCard> = (0..c.u32()?)
                    .map(|_| {
                        let id = c.id()?;
                        let himo = c.u16()?;
                        let h = c.hlc()?;
                        let card = match c.u8()? {
                            0 => Card::Gone,
                            1 => Card::Value(c.u64()?),
                            2 => {
                                let n = c.u32()? as usize;
                                Card::Bytes(c.take(n)?.into())
                            }
                            _ => return None,
                        };
                        Some((id, himo, card, h))
                    })
                    .collect::<Option<_>>()?;
                let timeline: Vec<NameWrite> = (0..c.u32()?)
                    .map(|_| {
                        let id = c.id()?;
                        let h = c.hlc()?;
                        let (himo, key) = c.key()?;
                        Some((id, himo, key, h))
                    })
                    .collect::<Option<_>>()?;
                if !c.0.is_empty() {
                    return None;
                }
                // 前の名前: この slot の分を外してから置く (他の slot が持っていた名前は、 後から持った方 = この record の slot)
                for k in self.by_slot.remove(&slot).unwrap_or_default() {
                    if self.by_key.get(&k) == Some(&slot) {
                        self.by_key.remove(&k);
                    }
                }
                for (himo, key) in names {
                    self.insert(himo, &key, slot);
                }
                self.set_bound(slot, bound);
                self.cards.remove(&slot);
                if !cards.is_empty() {
                    self.cards.insert(slot, cards);
                }
                self.timeline.remove(&slot);
                if !timeline.is_empty() {
                    self.timeline.insert(slot, timeline);
                }
            }
            REC_IDENTITY => {
                let id = c.id()?;
                let names: Vec<Name> = (0..c.u32()?).map(|_| c.key()).collect::<Option<_>>()?;
                if !c.0.is_empty() {
                    return None;
                }
                self.by_identity.remove(&id);
                if !names.is_empty() {
                    self.by_identity.insert(id, names);
                }
            }
            _ => return None,
        }
        Some(())
    }

    /// `pkformer` の bytes `snapshot` と、 その続きの log `log` (どちらも無ければ None) を読む。 log は header が `snapshot` と合う
    /// 時だけ当てる (合わない log は、 まとめ直しの途中で落ちた跡か、 log を知らない binary が `pkformer` を書き直した跡)。 切れた /
    /// 壊れた / 知らない種類の record から後は捨て、 次の書き出しでまとめ直す (切れた跡の後ろに追記すると、 次に読む時にそこで
    /// 止まって失う)。 `snapshot` が読めなければ Err。
    pub(crate) fn load(snapshot: Option<&[u8]>, log: Option<&[u8]>) -> Result<Self, String> {
        let mut out = match snapshot {
            Some(buf) => Self::deserialize(buf)?,
            None => Self::default(),
        };
        let snap = snapshot.unwrap_or(&[]);
        let mut ok = false;
        let mut log_bytes = 0u64;
        if let Some(log) = log
            && log.len() >= LOG_HEADER_LEN
            && log[..LOG_HEADER_LEN] == log_header(snap)[..]
        {
            let mut off = LOG_HEADER_LEN;
            ok = true;
            while off < log.len() {
                let rec = (|| {
                    let head = log.get(off..off + 5)?;
                    let len = u32::from_le_bytes(head[1..5].try_into().unwrap()) as usize;
                    let end = off.checked_add(5)?.checked_add(len)?;
                    let sum = log.get(end..end.checked_add(4)?)?;
                    if crate::integrity::fnv1a_region(&log[off..end]) != u32::from_le_bytes(sum.try_into().unwrap()) {
                        return None;
                    }
                    Some((head[0], &log[off + 5..end], end + 4))
                })();
                match rec.and_then(|(kind, payload, next)| out.apply_record(kind, payload).map(|()| next)) {
                    Some(next) => off = next,
                    None => {
                        ok = false;
                        break;
                    }
                }
            }
            log_bytes = off as u64;
        }
        out.log = LogTrack { ok, snapshot_bytes: snap.len() as u64, log_bytes, ..LogTrack::default() };
        // 今の中身の大きさを数える (slot / identity ごとの record の長さ)
        let mut slots: HashSet<u32> = out.by_slot.keys().copied().collect();
        slots.extend(out.bound.keys().chain(out.cards.keys()).chain(out.timeline.keys()));
        let ids: Vec<(u32, u32)> = out.by_identity.keys().copied().collect();
        let mut scratch = Vec::new();
        for slot in slots {
            scratch.clear();
            out.account_slot(slot, &mut scratch);
        }
        for id in ids {
            scratch.clear();
            out.account_identity(id, &mut scratch);
        }
        Ok(out)
    }

    /// log に record があるか (きれいに閉じる時にまとめ直すかの判定)。
    pub(crate) fn log_has_records(&self) -> bool {
        self.log.log_bytes > LOG_HEADER_LEN as u64
    }

    /// slot `slot` に中身があるか。
    fn slot_has_state(&self, slot: u32) -> bool {
        self.by_slot.get(&slot).is_some_and(|v| !v.is_empty())
            || !self.bound_of(slot).is_empty()
            || !self.cards_of(slot).is_empty()
            || self.timeline.contains_key(&slot)
    }

    /// slot `slot` の今の record を `out` に足し、 今の中身の大きさ (`live_bytes`) を直す。
    fn account_slot(&mut self, slot: u32, out: &mut Vec<u8>) {
        let start = out.len();
        self.encode_slot(slot, out);
        let now = if self.slot_has_state(slot) { (out.len() - start) as u32 } else { 0 };
        let before = match now {
            0 => self.log.slot_bytes.remove(&slot),
            n => self.log.slot_bytes.insert(slot, n),
        };
        self.log.live_bytes = self.log.live_bytes + now as u64 - before.unwrap_or(0) as u64;
    }

    /// [`Self::account_slot`] の identity 版。
    fn account_identity(&mut self, id: (u32, u32), out: &mut Vec<u8>) {
        let start = out.len();
        self.encode_identity(id, out);
        let now = if self.by_identity.contains_key(&id) { (out.len() - start) as u32 } else { 0 };
        let before = match now {
            0 => self.log.id_bytes.remove(&id),
            n => self.log.id_bytes.insert(id, n),
        };
        self.log.live_bytes = self.log.live_bytes + now as u64 - before.unwrap_or(0) as u64;
    }

    /// 書き出す仕事を取り出す (変わった所の印を下ろす)。 何も変わっていなければ None。
    ///
    /// - log が信用できない / `compact` (きれいに閉じる時、 log に record があれば) / 足した後の disk の大きさ (`pkformer` +
    ///   log) が今の中身の 2 倍 + 64 KiB を越える (書き換えが溜まった / 消えた行のカードを落として中身が減った) → まとめ直す
    /// - それ以外 → 変わった slot / identity の今の中身を log に足す
    ///
    /// 書けたら [`Self::persisted`]、 書けなかったら [`Self::persist_failed`] を呼ぶ。
    pub(crate) fn take_for_persist(&mut self, compact: bool) -> Option<Persist> {
        let mut slots: Vec<u32> = std::mem::take(&mut self.log.slots).into_iter().collect();
        let mut ids: Vec<(u32, u32)> = std::mem::take(&mut self.log.ids).into_iter().collect();
        slots.sort_unstable();
        ids.sort_unstable();
        let mut bytes = Vec::new();
        for slot in slots {
            self.account_slot(slot, &mut bytes);
        }
        for id in ids {
            self.account_identity(id, &mut bytes);
        }
        let after = self.log.snapshot_bytes + self.log.log_bytes + bytes.len() as u64;
        if !self.log.ok || (compact && self.log_has_records()) || after > 2 * self.log.live_bytes + LOG_SLACK {
            let snapshot = if self.is_empty() { Vec::new() } else { self.serialize() };
            let log = log_header(&snapshot);
            return Some(Persist::Rewrite { snapshot, log });
        }
        (!bytes.is_empty()).then_some(Persist::Append { bytes })
    }

    /// `job` を書けた。
    pub(crate) fn persisted(&mut self, job: &Persist) {
        match job {
            Persist::Rewrite { snapshot, log } => {
                self.log.ok = true;
                self.log.snapshot_bytes = snapshot.len() as u64;
                self.log.log_bytes = log.len() as u64;
            }
            Persist::Append { bytes } => self.log.log_bytes += bytes.len() as u64,
        }
    }

    /// 書けなかった: 次の書き出しでまとめ直す (途中まで書けた record の後ろに足さない)。
    pub(crate) fn persist_failed(&mut self) {
        self.log.ok = false;
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
    /// #720 の試験の disk: `pkformer` (無ければ None) と `pkformer.log`。
    #[derive(Default)]
    struct Disk {
        snapshot: Option<Vec<u8>>,
        log: Option<Vec<u8>>,
    }

    impl Disk {
        /// 書き出しを 1 回 (engine の `persist_pk_former` と同じ当て方)。 何を書いたか。
        fn persist(&mut self, f: &mut PkFormer, compact: bool) -> Option<&'static str> {
            let job = f.take_for_persist(compact)?;
            let what = match &job {
                Persist::Rewrite { snapshot, log } => {
                    self.snapshot = (!snapshot.is_empty()).then(|| snapshot.clone());
                    self.log = Some(log.clone());
                    "rewrite"
                }
                Persist::Append { bytes } => {
                    self.log.as_mut().expect("log の無い所に追記した").extend_from_slice(bytes);
                    "append"
                }
            };
            f.persisted(&job);
            Some(what)
        }

        fn load(&self) -> PkFormer {
            PkFormer::load(self.snapshot.as_deref(), self.log.as_deref()).unwrap()
        }
    }

    struct Rng(u64);
    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            (z ^ (z >> 31)) % n
        }
    }

    /// 変える method を乱数で 1 つ呼ぶ (engine が呼ぶ物を全部)。
    fn mutate(f: &mut PkFormer, r: &mut Rng) {
        let slot = r.below(6) as u32;
        let other = r.below(6) as u32;
        let id = (1 + r.below(3) as u32, r.below(3) as u32);
        let himo = r.below(2) as u16;
        let key = [b"k0".as_slice(), b"k1", b"k2", b"k3"][r.below(4) as usize];
        let t = h(1 + r.below(50));
        match r.below(15) {
            0 => drop(f.insert(himo, key, slot)),
            1 => drop(f.insert_identity(id.0, id.1, himo, key)),
            2 => drop(f.move_names(slot, other)),
            3 => drop(f.forget_slot(slot)),
            4 => drop(f.add_bound(slot, id, t)),
            5 => {
                let mut v = f.bound_of(slot).to_vec();
                v.pop();
                f.set_bound(slot, v);
            }
            6 => drop(f.note_card(slot, id, himo, Card::Value(r.below(9)), t)),
            7 => drop(f.note_card(slot, id, himo, Card::Bytes(vec![7u8; r.below(40) as usize].into()), t)),
            8 => drop(f.note_card(slot, id, himo, Card::Gone, t)),
            9 => {
                let taken = f.take_cards(slot, &[id]);
                f.put_cards(other, taken);
            }
            10 => drop(f.drop_cards(slot)),
            11 => drop(f.note_name(slot, id, himo, key, t)),
            12 => {
                let taken = f.take_names(slot, &[id]);
                f.put_names(other, taken);
            }
            13 => drop(f.move_names_by_timeline(slot, other)),
            _ => drop(f.note_name(slot, id, himo, key, t)),
        }
    }

    /// #720: どの method で変えても、 変わった所には印が付く = 追記した log を当てた disk の中身が memory と同じ。 まとめ直しと
    /// 追記の両方を踏む (前提)。 印を 1 つ外すと落ちる (変異で確かめた)。
    #[test]
    fn the_log_plus_the_snapshot_always_reads_back_what_is_in_memory() {
        let (mut appends, mut rewrites) = (0, 0);
        for seed in 0..200u64 {
            let mut r = Rng(seed);
            let mut f = PkFormer::default();
            let mut disk = Disk::default();
            for round in 0..40 {
                for _ in 0..1 + r.below(4) {
                    mutate(&mut f, &mut r);
                }
                match disk.persist(&mut f, false) {
                    Some("append") => appends += 1,
                    Some(_) => rewrites += 1,
                    None => {}
                }
                assert_eq!(disk.load().serialize(), f.serialize(), "seed {seed} round {round}: disk と memory が違う");
                // 開き直した物に続けて足しても同じ (log の様子が戻る)
                if round % 9 == 8 {
                    f = disk.load();
                }
            }
            // きれいに閉じる: log は header だけ、 `pkformer` だけで全部ある (log を読まない旧 binary の読み方 = deserialize)
            disk.persist(&mut f, true);
            assert_eq!(disk.log.as_ref().unwrap().len(), LOG_HEADER_LEN, "seed {seed}: 閉じた後の log に record が残った");
            let old_reader = disk.snapshot.as_deref().map(|b| PkFormer::deserialize(b).unwrap()).unwrap_or_default();
            assert_eq!(old_reader.serialize(), f.serialize(), "seed {seed}: 閉じた後の pkformer だけでは足りない");
        }
        assert!(appends > 1000 && rewrites >= 200, "前提: 追記 {appends} 回 / まとめ直し {rewrites} 回");
    }

    /// #720: 追記の途中で切れた / 壊れた record から後は捨て、 次の書き出しでまとめ直す。 header が今の `pkformer` と合わない log
    /// (まとめ直しの途中で落ちた / log を知らない binary が `pkformer` を書き直した) は読まない。 log の無い `pkformer` (0.37.4 が
    /// 書いた物) はそのまま読め、 最初の書き出しでまとめ直して log を作る。
    #[test]
    fn a_torn_or_foreign_log_is_not_trusted() {
        let mut f = PkFormer::default();
        let mut disk = Disk::default();
        f.insert(3, b"tokyo", 10);
        assert_eq!(disk.persist(&mut f, false), Some("rewrite"), "log の無い所はまとめて書く");
        f.note_card(10, (1, 1), 0, Card::Bytes(b"memo".as_slice().into()), h(3));
        f.add_bound(10, (1, 1), h(2));
        assert_eq!(disk.persist(&mut f, false), Some("append"));
        let after_first = f.serialize();
        f.insert(3, b"osaka", 11);
        assert_eq!(disk.persist(&mut f, false), Some("append"));
        assert_eq!(disk.persist(&mut f, false), None, "変わっていないのに書いた");
        let full = disk.log.clone().unwrap();

        // 最後の record の途中で切れた / 1 byte 化けた
        for damage in ["cut", "flip"] {
            let mut log = full.clone();
            match damage {
                "cut" => log.truncate(full.len() - 3),
                _ => *log.last_mut().unwrap() ^= 1,
            }
            let torn = Disk { snapshot: disk.snapshot.clone(), log: Some(log) };
            let mut back = torn.load();
            assert_eq!(back.serialize(), after_first, "{damage}: 切れていない record まで読む");
            assert!(!back.log.ok, "{damage}: 切れた log を信用した");
            back.insert(3, b"kyoto", 12);
            let mut torn = torn;
            assert_eq!(torn.persist(&mut back, false), Some("rewrite"), "{damage}: 切れた跡の後ろに追記した");
            assert_eq!(torn.load().serialize(), back.serialize());
        }
        // header が合わない log (別の `pkformer` の続き)
        let mut other = PkFormer::default();
        other.insert(3, b"nagoya", 20);
        let foreign = Disk { snapshot: Some(other.serialize()), log: Some(full.clone()) };
        let back = foreign.load();
        assert_eq!(back.serialize(), other.serialize(), "別の pkformer の続きの log を当てた");
        assert!(!back.log.ok);
        // log が無い (0.37.4 が書いた `pkformer`)
        let old = Disk { snapshot: Some(f.serialize()), log: None };
        let mut back = old.load();
        assert_eq!(back.serialize(), f.serialize());
        back.insert(3, b"kyoto", 12);
        let mut old = old;
        assert_eq!(old.persist(&mut back, false), Some("rewrite"));
        assert_eq!(old.log.as_ref().unwrap().len(), LOG_HEADER_LEN);
        // どちらも無い
        assert!(Disk::default().load().is_empty());
        // 書けなかった後はまとめ直す
        let mut g = disk.load();
        g.insert(3, b"kobe", 13);
        let _lost = g.take_for_persist(false).unwrap();
        g.persist_failed();
        assert_eq!(disk.persist(&mut g, false), Some("rewrite"), "失敗した追記の後ろに足した");
        assert_eq!(disk.load().serialize(), g.serialize());
    }

    /// #720: 1 つの slot の書き換えは、 その slot の record だけを足す (全体の大きさによらない)。 中身が減って disk が今の中身の
    /// 2 倍 + 64 KiB を越えたら、 まとめ直して縮む。
    #[test]
    fn an_append_is_one_slot_and_a_shrunk_store_is_compacted() {
        let mut f = PkFormer::default();
        let mut disk = Disk::default();
        let body = vec![b'x'; 200];
        for slot in 0..2000u32 {
            for id in [(1, slot), (2, slot)] {
                f.add_bound(slot, id, h(1));
                f.note_card(slot, id, 0, Card::Value(slot as u64), h(1));
                f.note_card(slot, id, 2, Card::Bytes(body.clone().into()), h(1));
            }
        }
        assert_eq!(disk.persist(&mut f, false), Some("rewrite"));
        let snapshot = disk.snapshot.as_ref().unwrap().len();
        assert!(snapshot > 1_000_000, "前提: pkformer は 1 MB を越える ({snapshot} B)");
        for round in 0..5u64 {
            f.note_card(7, (1, 7), 1, Card::Value(round), h(10 + round));
            let before = disk.log.as_ref().unwrap().len();
            assert_eq!(disk.persist(&mut f, false), Some("append"));
            let grew = disk.log.as_ref().unwrap().len() - before;
            assert!(grew < 1024, "1 つの slot の書き換えで {grew} B 足した");
            assert_eq!(disk.snapshot.as_ref().unwrap().len(), snapshot, "pkformer を書き直した");
        }
        // 全部の行が消えた (カードを落とす)
        for slot in 0..2000u32 {
            f.drop_cards(slot);
        }
        assert_eq!(disk.persist(&mut f, false), Some("rewrite"), "中身が減ったのにまとめ直さない");
        let after = disk.snapshot.as_ref().map_or(0, |s| s.len()) + disk.log.as_ref().unwrap().len();
        assert!(after < snapshot / 4, "消した後も {after} B (前は {snapshot} B)");
        assert_eq!(disk.load().serialize(), f.serialize());
    }
}
