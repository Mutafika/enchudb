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
//!   削除の時刻を上げるため (slot の記録は使い回しで外れる)。 自分の消えた行は identity (自分の peer, 世代 | 場所) に移す (#914 /
//!   #915、 engine の `keep_gone_own_delete`: 使い回した場所の新しい住人とは世代の違う番号)
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
//! - 変える method は、 変えた slot に印を付ける (`LogTrack`)。 書き出しは、 印の付いた slot の**今の中身** (前の名前・束ねた
//!   identity・カード・名前の年表の全部) を 1 record にして log に足す。 identity (移した名前) は、 変える method がその場で今の
//!   中身の record を作り、 書き出しはそれを取り出すだけ (#959: identity は場所を使い回すたびに 1 つ増え、 書き出しが印の全部の
//!   中身を `pk_former` の書き込みの lock の下で作ると、 その間 主キーのある表の書き手が止まった)。 読む時は record の順に、
//!   その slot / identity の中身を置き換える
//! - `pkformer` の形は変えない (版 4)。 log を読まない 0.37.4 以前は `pkformer` だけを読む。 きれいに閉じる時にまとめ直す
//!   (`pkformer` を書き直して log を header だけにする) ので、 きれいに閉じた後は全部ある。 落ちた後 (log に record が残って
//!   いる) に 0.37.4 以前で開くと、 log の分 (最後にまとめ直した後のカード・年表・前の名前の変化) を読まない
//! - log の header は、 続く `pkformer` の長さと FNV-1a 64。 合わない log は読まない (まとめ直しの途中で落ちた跡か、 log を
//!   知らない binary が `pkformer` を書き直した跡)。 まとめ直しは `pkformer` → 空の log の順に置き換える
//! - 切れた / 壊れた / 知らない種類の record から後は捨て、 次の書き出しでまとめ直す (切れた跡の後ろに追記すると、 次に読む
//!   時にそこで止まって失う)
//! - まとめ直すのは他に、 disk (`pkformer` + log) が今の中身の 2 倍 + 64 KiB を越えた時 (書き換えが溜まった / 消えた行の
//!   カードを落として中身が減った)。 今の中身の大きさは slot / identity ごとの record の長さの和で持つ。 #959: この時は追記した
//!   後に、 書き手の lock の外で、 追記の済んだ disk の 2 つから作る (disk の中身 = 追記の仕事を取り出した時の memory の中身。
//!   前は lock の下で memory の全部を並べ直した)。 review987 [1]: 書き出しの lock (engine の `sidecar_persist_lock`) の外でもある
//!   ([`PkFormer::begin_compact`])。 作っている間に log が伸びたら、 伸びた分を作った中身に当てて作り直す (`pkformer` を置き換えた後・
//!   log を置き換える前の電源断の像では古い log を読まないので、 伸びた分を新しい log に写すのでは失う)。 2 回伸びたら、 最後は
//!   書き出しの lock の下で当てて並べる (前の 「lock の下で memory から並べる」 と同じ大きさの仕事)
//! - disk に何も無い (file の無い DB を開いた / 作った、 または `pkformer` が読めずに空で始めた) 間の最初の書き出しは、 log の
//!   header + 開いてからの record だけ (#959、 memory の全部を並べない)。 書けたら、 書き出しの lock の外で `pkformer` に
//!   まとめ直す (review987 [3]: log を読まない 0.37.4 以前で開いても `pkformer` に前の名前がある)
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

    /// 名前 `(himo, key)` を前の名前として持つ行から外す (#921 の読み直し [5]: 場所を使い回した自分の消えた行への改名で、 その名前が
    /// 消えた行の名前になった時。 場所を使い回していない peer は消えた行の slot に付け替える = [`Self::insert`] の 「後から改名した方に
    /// する」 で外れる。 消えた行は slot を持たないので外すだけ)。 変えたら true。
    pub(crate) fn release_key(&mut self, himo: u16, key: &[u8]) -> bool {
        let k = (himo, Box::<[u8]>::from(key));
        let Some(old) = self.by_key.remove(&k) else { return false };
        if let Some(v) = self.by_slot.get_mut(&old) {
            v.retain(|x| *x != k);
        }
        self.touch(old);
        true
    }

    /// identity `(peer, foreign_local)` に名前 `(himo, key)` を足す (消えた行の slot を使い回す時、 #545)。 変えたら true。
    pub(crate) fn insert_identity(&mut self, peer: u32, foreign_local: u32, himo: u16, key: &[u8]) -> bool {
        self.insert_identity_names(peer, foreign_local, std::iter::once((himo, key)))
    }

    /// [`Self::insert_identity`] の、 名前をまとめて足す形 (record は 1 つ。 名前ごとに足すと、 その都度 identity の今の中身の
    /// record を作るので、 名前 k 個で k² に比例した、 review987 [5])。 変えたら true。
    pub(crate) fn insert_identity_names<'a>(
        &mut self,
        peer: u32,
        foreign_local: u32,
        add: impl IntoIterator<Item = (u16, &'a [u8])>,
    ) -> bool {
        let names = self.by_identity.entry((peer, foreign_local)).or_default();
        let before = identity_record_len(names);
        let len = names.len();
        for (himo, key) in add {
            if !names.iter().any(|(h, k)| *h == himo && &k[..] == key) {
                names.push((himo, key.into()));
            }
        }
        if names.len() == len {
            if names.is_empty() {
                self.by_identity.remove(&(peer, foreign_local));
            }
            return false;
        }
        // 大きさは目安 (まとめ直す時を決めるだけ)。 数えていない物 (`deserialize` だけで作った) を変えても下に抜けない
        self.log.live_bytes = (self.log.live_bytes + identity_record_len(names)).saturating_sub(before);
        if !self.log.off {
            encode_identity_record((peer, foreign_local), names, &mut self.log.id_records);
        }
        true
    }

    /// identity `(peer, foreign_local)` に移した名前を外す (その identity の削除の版数を忘れた時、 engine の
    /// `forget_deletes_before`)。 変えたら true。
    pub(crate) fn forget_identity(&mut self, peer: u32, foreign_local: u32) -> bool {
        let Some(names) = self.by_identity.remove(&(peer, foreign_local)) else { return false };
        self.log.live_bytes = self.log.live_bytes.saturating_sub(identity_record_len(&names));
        if !self.log.off {
            encode_identity_record((peer, foreign_local), &[], &mut self.log.id_records);
        }
        true
    }

    /// 書き出さない engine (path の無い / wasm) の記録: identity の record を作らない (誰も取り出さずに溜まる、 review987 [5])。
    pub(crate) fn without_log() -> Self {
        let mut f = Self::default();
        f.log.off = true;
        f
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
                // `insert_identity` と同じ (読み込みでは record を作らない)
                let names = out.by_identity.entry((peer, fl)).or_default();
                if !names.iter().any(|(h, k)| *h == himo && &k[..] == key) {
                    names.push((himo, key.into()));
                }
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
        out.log = LogTrack { fresh: false, ..LogTrack::default() };
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
#[derive(Debug)]
struct LogTrack {
    /// 前の書き出しの後に変わった slot
    slots: HashSet<u32>,
    /// 前の書き出しの後に変わった identity (移した名前) の record。 #959: identity は変える method がその場で record を作る
    /// (前は印を付けて、 書き出しが `pk_former` の書き込みの lock の下で全部の印の今の中身を作っていた。 場所を使い回すたびに
    /// identity が 1 つ増えるので、 使い回しの多い DB では 1 回の書き出しが数万個を作り、 その間 書き手が止まった)。 同じ identity を
    /// 2 回変えれば record も 2 つ (読む時は後の物が勝つ)
    id_records: Vec<u8>,
    /// log が今の `pkformer` の続きとして信用できるか。 false なら次の書き出しでまとめ直す (log が無い / header が合わない /
    /// 切れた record がある / 前の書き出しが失敗した)
    ok: bool,
    /// disk に何も無い (`pkformer` も log も無い DB を開いた、 または作った。 `pkformer` が読めずに空で始めた時も) まま、 まだ
    /// 書き出していない。 この間の変化は全部、 印と `id_records` にある = 最初の書き出しは log の header + それ (memory の全部を
    /// 並べない、 #959)
    fresh: bool,
    /// identity の record を作らない (書き出さない engine = path の無い / wasm。 作ると誰も取り出さずに溜まる、 review987 [5])
    off: bool,
    /// 読んだ時に log を 1 つも当てなかった (log が無い / header が合わない = 0.37.4 以前が書いた `pkformer`、 bootstrap で `pkformer`
    /// だけを受けた、 log を読まない binary が `pkformer` を書き直した) = memory は disk の `pkformer` + 印。 その `pkformer` の続きの
    /// log の header。 最初の書き出しは、 この header + 印の record で log を作る (`pkformer` を並べ直さない、 review987 [4])
    resume: Option<Vec<u8>>,
    /// 書き出しの lock の外のまとめ直し ([`PkFormer::begin_compact`]、 review987 [1])
    compact: CompactState,
    /// disk の `pkformer` の長さ
    snapshot_bytes: u64,
    /// disk の log の長さ (header 込み)
    log_bytes: u64,
    /// 今の中身の大きさ (中身のある slot / identity の record の長さの和。 まとめ直した `pkformer` の長さの目安)
    live_bytes: u64,
    /// slot → その record の長さ (中身のある slot だけ。 `live_bytes` を変わった分だけ直すため)
    slot_bytes: HashMap<u32, u32>,
}

impl Default for LogTrack {
    fn default() -> Self {
        Self {
            slots: HashSet::new(),
            id_records: Vec::new(),
            ok: false,
            fresh: true,
            off: false,
            resume: None,
            compact: CompactState::default(),
            snapshot_bytes: 0,
            log_bytes: 0,
            live_bytes: 0,
            slot_bytes: HashMap::new(),
        }
    }
}

/// 書き出しの lock の外のまとめ直しの様子 (`pk_tombs.rs` の同じ名前の物と同じ)。
#[derive(Default, Debug, Clone, Copy)]
struct CompactState {
    /// まとめ直しが要る (disk が今の中身の 2 倍 + 64 KiB を越えた追記の後 / disk に何も無かった DB の最初の書き出しの後)
    wanted: bool,
    /// まとめ直しの最中 (`begin_compact` から `end_compact` まで。 2 本目は始めない)
    running: bool,
    /// disk を置き換えた / 信用しなくなった回数。 まとめ直しは、 始めた時と置く時でこれが同じ時だけ置く
    epoch: u64,
}

/// まとめ直しを始めた時の disk ([`PkFormer::begin_compact`])。
#[derive(Debug, Clone, Copy)]
pub(crate) struct CompactMark {
    /// disk の `pkformer` の長さ (0 = 無い)
    pub(crate) snapshot_bytes: u64,
    /// disk の log の長さ (header 込み)
    pub(crate) log_bytes: u64,
    epoch: u64,
}

/// identity `id` に移した名前 `names` の record の長さ (名前が無ければ 0 = 中身なし)。 [`encode_identity_record`] の長さと同じ。
fn identity_record_len(names: &[Name]) -> u64 {
    if names.is_empty() {
        return 0;
    }
    // 種類 1 + 中身の長さ 4 + (peer 4 + 番号 4 + 数 4 + 名前ごとに (himo 2 + 鍵の長さ 4 + 鍵)) + 検査値 4
    21 + names.iter().map(|(_, k)| 6 + k.len() as u64).sum::<u64>()
}

/// identity `id` に移した名前 `names` の record を `out` に足す (名前が無ければ 「外す」 record)。
fn encode_identity_record(id: (u32, u32), names: &[Name], out: &mut Vec<u8>) {
    let start = out.len();
    out.push(REC_IDENTITY);
    out.extend_from_slice(&[0; 4]);
    out.extend_from_slice(&id.0.to_le_bytes());
    out.extend_from_slice(&id.1.to_le_bytes());
    out.extend_from_slice(&(names.len() as u32).to_le_bytes());
    for (himo, key) in names {
        put_key(out, *himo, key);
    }
    let len = (out.len() - start - 5) as u32;
    out[start + 1..start + 5].copy_from_slice(&len.to_le_bytes());
    let sum = crate::integrity::fnv1a_region(&out[start..]);
    out.extend_from_slice(&sum.to_le_bytes());
}

/// 書き出す仕事 (`PkFormer::take_for_persist`)。
#[derive(Debug)]
pub(crate) enum Persist {
    /// `pkformer` を `snapshot` に書き直し (空なら file を消す)、 log を `log` (header だけ) に置き換える
    Rewrite { snapshot: Vec<u8>, log: Vec<u8> },
    /// log に `bytes` を追記する。 `compact` = disk が今の中身の 2 倍 + 64 KiB を越えた (書けたら、 書き手の lock と書き出しの lock の
    /// 外でまとめ直す = [`PkFormer::begin_compact`]、 #959 / review987 [1])
    Append { bytes: Vec<u8>, compact: bool },
    /// disk に何も無い DB の最初の書き出し: `pkformer` を消し (あれば)、 log を `log_header(&[])` + `records` にする (#959。 header を
    /// 別に書く = lock の下で写さない、 review987 [6])
    Fresh { records: Vec<u8> },
    /// log を 1 つも当てずに読んだ DB の最初の書き出し: `pkformer` はそのまま、 log を `header` (読んだ `pkformer` の続き) + `records`
    /// に置き換える (`pkformer` を並べ直さない、 review987 [4])
    Resume { header: Vec<u8>, records: Vec<u8> },
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
        let mut out = Self::load_records(snapshot, log)?;
        // 今の中身の大きさを数える (slot / identity ごとの record の長さ)
        let mut slots: HashSet<u32> = out.by_slot.keys().copied().collect();
        slots.extend(out.bound.keys().chain(out.cards.keys()).chain(out.timeline.keys()));
        let mut scratch = Vec::new();
        for slot in slots {
            scratch.clear();
            out.account_slot(slot, &mut scratch);
        }
        out.log.live_bytes += out.by_identity.values().map(|v| identity_record_len(v)).sum::<u64>();
        Ok(out)
    }

    /// [`Self::load`] の、 今の中身の大きさを数えない形 (読んだ中身を並べ直すだけの [`Self::compact_files`] 用)。
    fn load_records(snapshot: Option<&[u8]>, log: Option<&[u8]>) -> Result<Self, String> {
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
            let (end, all) = out.apply_records(log, LOG_HEADER_LEN);
            ok = all;
            log_bytes = end as u64;
        }
        // log を 1 つも当てなかった (無い / header が合わない) なら、 読んだ `pkformer` の続きの log を新しく始められる
        let resume = (!ok && log_bytes == 0).then(|| log_header(snap));
        out.log = LogTrack { ok, fresh: false, resume, snapshot_bytes: snap.len() as u64, log_bytes, ..LogTrack::default() };
        Ok(out)
    }

    /// log の `from` から後の record を順に当てる。 (読めた所の終わり, 終わりまで全部読めたか)。 切れた / 壊れた / 知らない種類の
    /// record の手前で止まる。
    fn apply_records(&mut self, log: &[u8], from: usize) -> (usize, bool) {
        let mut off = from;
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
            match rec.and_then(|(kind, payload, next)| self.apply_record(kind, payload).map(|()| next)) {
                Some(next) => off = next,
                None => return (off, false),
            }
        }
        (off, true)
    }

    /// review987 [1]: まとめ直す disk (`pkformer` の bytes `snapshot` と、 その続きの log `log` の全部) を読む (書き手の lock と書き出しの
    /// lock の外)。 log が `snapshot` の続きとして終わりまで読めなければ Err (memory から書き直す)。
    pub(crate) fn compact_read(snapshot: Option<&[u8]>, log: &[u8]) -> Result<Self, String> {
        let f = Self::load_records(snapshot, Some(log))?;
        if !f.log.ok || f.log.log_bytes != log.len() as u64 {
            return Err("pkformer.log is not readable as the continuation of pkformer".into());
        }
        Ok(f)
    }

    /// review987 [1]: まとめ直しを始めた後に log に足された record `tail` を当てる (全部読めなければ Err)。
    pub(crate) fn apply_log_tail(&mut self, tail: &[u8]) -> Result<(), String> {
        match self.apply_records(tail, 0) {
            (_, true) => Ok(()),
            (at, false) => Err(format!("pkformer.log: unreadable record at +{at} after the compaction started")),
        }
    }

    /// まとめ直した disk の 2 つ: `pkformer` (中身が無ければ空 = file を消す) と、 その続きの record の無い log。
    pub(crate) fn compacted_files(&self) -> (Vec<u8>, Vec<u8>) {
        let snapshot = if self.is_empty() { Vec::new() } else { self.serialize() };
        let log = log_header(&snapshot);
        (snapshot, log)
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

    /// 書き出す仕事を取り出す (変わった所の印を下ろす)。 何も変わっていなければ None。
    ///
    /// - log が信用できない / `compact` (きれいに閉じる時、 log に record があれば) → memory の中身からまとめ直す
    /// - それ以外 → 変わった slot / identity の今の中身を log に足す。 足した後の disk の大きさ (`pkformer` + log) が今の中身の
    ///   2 倍 + 64 KiB を越える (書き換えが溜まった / 消えた行のカードを落として中身が減った) 時は、 足した後にまとめ直す
    ///   (`Append { compact: true }`)。 #959: 呼び手は書き込みの lock を握っている。 前はここで memory の全部を並べ直していて
    ///   (`serialize`)、 その間、 主キーのある表の行を消す / 場所を使い回す書き手が止まった (使い回した identity 10 万で 16〜28 ms)。
    ///   まとめ直しは書き手の lock と書き出しの lock の外で、 追記の済んだ disk から作る ([`Self::begin_compact`])
    ///
    /// 書けたら [`Self::persisted`]、 書けなかったら [`Self::persist_failed`] を呼ぶ。
    pub(crate) fn take_for_persist(&mut self, compact: bool) -> Option<Persist> {
        let mut slots: Vec<u32> = std::mem::take(&mut self.log.slots).into_iter().collect();
        slots.sort_unstable();
        // review987 [5]: 次の周期の分は前の周期の大きさで確保しておく (0 から伸ばすと、 書き手が lock の下で倍々に写し直す)
        let cap = self.log.id_records.len();
        let mut bytes = std::mem::replace(&mut self.log.id_records, Vec::with_capacity(cap));
        for slot in slots {
            self.account_slot(slot, &mut bytes);
        }
        let after = self.log.snapshot_bytes + self.log.log_bytes + bytes.len() as u64;
        if self.log.fresh && !compact {
            // disk に何も無い DB の最初の書き出し: 開いてからの変化は全部 `bytes` にある (並べ直さない、 #959)。 きれいに閉じる時
            // (`compact`) は下で `pkformer` に並べる (log を読まない旧 binary も読める形)
            if bytes.is_empty() {
                return None;
            }
            return Some(Persist::Fresh { records: bytes });
        }
        if !self.log.ok
            && !compact
            && let Some(header) = self.log.resume.take()
        {
            // log を 1 つも当てずに読んだ DB の最初の書き出し: memory は disk の `pkformer` + 印 = その続きの log を作る
            if bytes.is_empty() {
                self.log.resume = Some(header);
                return None;
            }
            return Some(Persist::Resume { header, records: bytes });
        }
        if !self.log.ok || (compact && self.log_has_records()) {
            let snapshot = if self.is_empty() { Vec::new() } else { self.serialize() };
            let log = log_header(&snapshot);
            return Some(Persist::Rewrite { snapshot, log });
        }
        let compact = after > 2 * self.log.live_bytes + LOG_SLACK;
        (!bytes.is_empty() || compact).then_some(Persist::Append { bytes, compact })
    }

    /// `job` を書けた。
    pub(crate) fn persisted(&mut self, job: &Persist) {
        match job {
            Persist::Rewrite { snapshot, log } => {
                self.log.ok = true;
                self.log.fresh = false;
                self.log.snapshot_bytes = snapshot.len() as u64;
                self.log.log_bytes = log.len() as u64;
                self.log.compact.epoch += 1;
                self.log.compact.wanted = false;
            }
            Persist::Fresh { records } => {
                self.log.ok = true;
                self.log.fresh = false;
                self.log.snapshot_bytes = 0;
                self.log.log_bytes = (LOG_HEADER_LEN + records.len()) as u64;
                self.log.compact.epoch += 1;
                // review987 [3]: 書き出しの lock の外で `pkformer` にまとめ直す (log を読まない 0.37.4 以前で開いても前の名前がある。
                // log だけのまま落ちて 0.37.4 で開き、 その session が場所を使い回すと、 0.38 で開き直した時に log の前の名前が今の住人に
                // 当たった)
                self.log.compact.wanted = true;
            }
            Persist::Resume { header, records } => {
                self.log.ok = true;
                self.log.log_bytes = (header.len() + records.len()) as u64;
                self.log.compact.epoch += 1;
            }
            Persist::Append { bytes, compact } => {
                self.log.log_bytes += bytes.len() as u64;
                if *compact {
                    self.log.compact.wanted = true;
                }
            }
        }
    }

    /// 書けなかった: 次の書き出しでまとめ直す (途中まで書けた record の後ろに足さない)。
    pub(crate) fn persist_failed(&mut self) {
        self.log.ok = false;
        // 取り出した印 / record は書けたか分からないので、 次は memory の全部から
        self.log.fresh = false;
        self.log.resume = None;
        self.log.compact.epoch += 1;
        self.log.compact.wanted = false;
    }

    /// review987 [1]: 書き出しの lock の外のまとめ直しを始める。 呼び手は書き出しの lock (`sidecar_persist_lock`) を握り、 同じ lock の
    /// 下で disk の 2 つを開く (長さは [`CompactMark`] の物)。 頼まれていない / 走っている / log を信用していない時は None。 始めたら、
    /// 終わりに必ず [`Self::end_compact`] を呼ぶ。
    pub(crate) fn begin_compact(&mut self) -> Option<CompactMark> {
        let c = &mut self.log.compact;
        if !c.wanted || c.running || !self.log.ok || self.log.fresh {
            return None;
        }
        c.running = true;
        Some(CompactMark { snapshot_bytes: self.log.snapshot_bytes, log_bytes: self.log.log_bytes, epoch: c.epoch })
    }

    /// まとめ直しを始めた時の disk が、 今もそのまま (log への追記だけ) か。 そうなら今の log の長さ。 呼び手は書き出しの lock を
    /// 握っている。
    pub(crate) fn compact_check(&self, m: &CompactMark) -> Option<u64> {
        (self.log.compact.epoch == m.epoch && self.log.ok && self.log.snapshot_bytes == m.snapshot_bytes)
            .then_some(self.log.log_bytes)
    }

    /// まとめ直した disk (`pkformer` の長さ `snapshot`、 log の長さ `log`) を置いた。 呼び手は書き出しの lock を握り、 置く直前に
    /// [`Self::compact_check`] が通った。
    pub(crate) fn compact_installed(&mut self, m: &CompactMark, snapshot: u64, log: u64) {
        // 見直しの後に disk を置き換えるのは書き出しの lock の下だけ (呼び手が握っている) = ここでも同じ
        debug_assert_eq!(self.log.compact.epoch, m.epoch);
        self.log.snapshot_bytes = snapshot;
        self.log.log_bytes = log;
        self.log.compact.epoch += 1;
        self.log.compact.wanted = false;
    }

    /// まとめ直す disk が読めなかった: 次の書き出しで memory から書き直す。 呼び手は書き出しの lock を握り、 [`Self::compact_check`]
    /// が通った。
    pub(crate) fn compact_unreadable(&mut self) {
        self.persist_failed();
    }

    /// まとめ直しを終えた (置いた / やめた)。
    pub(crate) fn end_compact(&mut self) {
        self.log.compact.running = false;
    }

    /// まとめ直しが走っているか (試験の待ち合わせ用)。
    pub(crate) fn compact_running(&self) -> bool {
        self.log.compact.running
    }

    /// まとめ直しを頼まれているか。
    pub(crate) fn compact_wanted(&self) -> bool {
        let c = &self.log.compact;
        c.wanted && !c.running && self.log.ok && !self.log.fresh
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
        /// 書き出しを 1 回 (engine の `persist_pk_former`)。 何を書いたか (まとめ直しは別 = [`Disk::compact`])。
        fn persist(&mut self, f: &mut PkFormer, compact: bool) -> Option<&'static str> {
            let job = f.take_for_persist(compact)?;
            let what = match &job {
                Persist::Rewrite { .. } | Persist::Fresh { .. } => "rewrite",
                Persist::Resume { .. } => "resume",
                Persist::Append { compact: true, .. } => "compact",
                Persist::Append { .. } => "append",
            };
            self.write(&job);
            f.persisted(&job);
            Some(what)
        }

        fn write(&mut self, job: &Persist) {
            match job {
                Persist::Rewrite { snapshot, log } => {
                    self.snapshot = (!snapshot.is_empty()).then(|| snapshot.clone());
                    self.log = Some(log.clone());
                }
                Persist::Fresh { records } => {
                    self.snapshot = None;
                    self.log = Some([&log_header(&[])[..], records].concat());
                }
                Persist::Resume { header, records } => {
                    self.log = Some([&header[..], records].concat());
                }
                Persist::Append { bytes, .. } => {
                    self.log.as_mut().expect("log の無い所に追記した").extend_from_slice(bytes);
                }
            }
        }

        /// 頼まれていれば、 書き出しの lock の外で disk からまとめ直して置く (engine の `compact_pk_former_files`)。 disk を読んだ後・
        /// 置く前に `meanwhile` を呼ぶ (書き手が進み、 別の書き出しが log に追記する。 置く時に伸びた分を当てて作り直す)。 置いたか。
        fn compact(&mut self, f: &mut PkFormer, meanwhile: impl FnOnce(&mut PkFormer, &mut Disk)) -> bool {
            let Some(mark) = f.begin_compact() else { return false };
            assert!(f.begin_compact().is_none(), "まとめ直しを 2 本始めた");
            assert_eq!(self.snapshot.as_ref().map_or(0, |s| s.len()) as u64, mark.snapshot_bytes);
            let log = self.log.clone().unwrap();
            assert_eq!(log.len() as u64, mark.log_bytes);
            let mut c = PkFormer::compact_read(self.snapshot.as_deref(), &log).unwrap();
            meanwhile(f, self);
            let now = f.compact_check(&mark).expect("log への追記だけなのに置けない");
            let cur = self.log.as_ref().unwrap();
            assert_eq!(cur.len() as u64, now);
            c.apply_log_tail(&cur[mark.log_bytes as usize..]).unwrap();
            let (snapshot, log) = c.compacted_files();
            f.compact_installed(&mark, snapshot.len() as u64, log.len() as u64);
            f.end_compact();
            self.snapshot = (!snapshot.is_empty()).then_some(snapshot);
            self.log = Some(log);
            true
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
        match r.below(16) {
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
            14 => drop(f.forget_identity(id.0, id.1)),
            _ => drop(f.note_name(slot, id, himo, key, t)),
        }
    }

    /// #720: どの method で変えても、 変わった所には印が付く = 追記した log を当てた disk の中身が memory と同じ。 まとめ直しと
    /// 追記の両方を踏む (前提)。 印を 1 つ外すと落ちる (変異で確かめた)。
    #[test]
    fn the_log_plus_the_snapshot_always_reads_back_what_is_in_memory() {
        let (mut appends, mut rewrites, mut compacts, mut tails) = (0, 0, 0, 0);
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
                    Some("compact") => compacts += 1,
                    Some(_) => rewrites += 1,
                    None => {}
                }
                assert_eq!(disk.load().serialize(), f.serialize(), "seed {seed} round {round}: disk と memory が違う");
                // #959 / review987 [1]: 書き出しの lock の外で disk からまとめ直す (大きくなった時 / 最初の書き出しの後の道。 ここでは
                // 大きさによらず踏む)。 まとめ直す間に書き手が進み、 別の書き出しが log に追記する (置く時に当てて作り直す)。 置かずに
                // 残った印は次の書き出しで新しい log に足される
                if r.below(4) == 0 {
                    f.log.compact.wanted = true;
                }
                let racing = r.below(3);
                let mut appended = false;
                let mut r2 = Rng(r.below(u64::MAX));
                if disk.compact(&mut f, |f, d| {
                    for _ in 0..racing {
                        mutate(f, &mut r2);
                    }
                    appended = d.persist(f, false).is_some();
                }) {
                    compacts += 1;
                    if appended {
                        tails += 1;
                    }
                    assert_eq!(disk.load().serialize(), f.serialize(), "seed {seed} round {round}: まとめ直した後に disk と memory が違う");
                    // `pkformer` を置いた後・log を置く前の電源断の像 (古い log は header が合わず読まない) でも同じ
                    let placed = Disk { snapshot: disk.snapshot.clone(), log: None };
                    assert_eq!(placed.load().serialize(), f.serialize(), "seed {seed} round {round}: 置いた pkformer だけでは足りない");
                }
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
        assert!(
            appends > 1000 && rewrites >= 200 && compacts > 1000 && tails > 300,
            "前提: 追記 {appends} 回 / まとめ直し {rewrites} 回 / disk から {compacts} 回 (間に追記 {tails} 回)"
        );
    }

    /// #720: 追記の途中で切れた / 壊れた record から後は捨て、 次の書き出しでまとめ直す。 header が今の `pkformer` と合わない log
    /// (まとめ直しの途中で落ちた / log を知らない binary が `pkformer` を書き直した) は読まない。 log の無い `pkformer` (0.37.4 が
    /// 書いた物) はそのまま読め、 最初の書き出しで、 その `pkformer` の続きの log を作る (review987 [4]: `pkformer` を並べ直さない。
    /// 前は書き込みの lock の下で memory の全部を並べ直した)。
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
        let mut back = foreign.load();
        assert_eq!(back.serialize(), other.serialize(), "別の pkformer の続きの log を当てた");
        assert!(!back.log.ok);
        // 最初の書き出しは、 読んだ `pkformer` の続きの log を新しく作る (`pkformer` はそのまま)
        let mut foreign = foreign;
        assert_eq!(foreign.persist(&mut back, false), None, "何も変えていないのに書いた");
        back.insert(3, b"sendai", 21);
        assert_eq!(foreign.persist(&mut back, false), Some("resume"));
        assert_eq!(foreign.snapshot.as_deref(), Some(&other.serialize()[..]), "pkformer を書き直した");
        assert_eq!(foreign.load().serialize(), back.serialize());
        assert_eq!(foreign.persist(&mut back, false), None);
        back.insert(3, b"sapporo", 22);
        assert_eq!(foreign.persist(&mut back, false), Some("append"), "続きを追記しない");
        assert_eq!(foreign.load().serialize(), back.serialize());
        // log が無い (0.37.4 が書いた `pkformer`)
        let old = Disk { snapshot: Some(f.serialize()), log: None };
        let mut back = old.load();
        assert_eq!(back.serialize(), f.serialize());
        back.insert(3, b"kyoto", 12);
        let mut old = old;
        assert_eq!(old.persist(&mut back, false), Some("resume"), "log の無い pkformer の最初の書き出しで並べ直した");
        assert_eq!(old.snapshot.as_deref(), Some(&f.serialize()[..]), "pkformer を書き直した");
        assert!(old.log.as_ref().unwrap().len() > LOG_HEADER_LEN);
        assert_eq!(old.load().serialize(), back.serialize());
        // きれいに閉じると `pkformer` に並べる (log を読まない旧 binary の読み方 = deserialize)
        assert_eq!(old.persist(&mut back, true), Some("rewrite"));
        assert_eq!(old.log.as_ref().unwrap().len(), LOG_HEADER_LEN);
        assert_eq!(PkFormer::deserialize(old.snapshot.as_ref().unwrap()).unwrap().serialize(), back.serialize());
        // 書けなかった後は memory から (log を新しく作る印も捨てる)。 続きの log を作る前に、 きれいに閉じる時の書き直しが書けなかった時も
        for close_first in [false, true] {
            let mut back = Disk { snapshot: Some(f.serialize()), log: None }.load();
            back.insert(3, b"nara", 14);
            let lost = back.take_for_persist(close_first).unwrap();
            assert_eq!(matches!(lost, Persist::Rewrite { .. }), close_first);
            back.persist_failed();
            back.insert(3, b"kobe", 15);
            assert!(
                matches!(back.take_for_persist(false), Some(Persist::Rewrite { .. })),
                "{close_first}: 書けなかった後に後の印だけの log を作った"
            );
        }
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
        // きれいに閉じる時と同じ書き出しで `pkformer` に並べる (disk の無い DB の最初の書き出しは log だけ、 #959)
        assert_eq!(disk.persist(&mut f, true), Some("rewrite"));
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
        assert_eq!(disk.persist(&mut f, false), Some("compact"), "中身が減ったのにまとめ直さない");
        assert!(disk.compact(&mut f, |_, _| {}), "まとめ直しを頼んでいない");
        let after = disk.snapshot.as_ref().map_or(0, |s| s.len()) + disk.log.as_ref().unwrap().len();
        assert!(after < snapshot / 4, "消した後も {after} B (前は {snapshot} B)");
        assert_eq!(disk.load().serialize(), f.serialize());
    }

    /// #959: identity は変える method がその場で record を作る (書き出しは取り出すだけ)。 disk の無い DB の最初の書き出しは log の
    /// header + 開いてからの record (memory の全部を並べない)。 書けなかった後と、 disk を読んだ物は memory から並べる。
    #[test]
    fn identities_are_logged_as_they_change_and_a_fresh_store_writes_only_the_log() {
        let mut f = PkFormer::default();
        let mut disk = Disk::default();
        for n in 0..1000u32 {
            f.insert_identity(1, n, 3, &n.to_le_bytes());
            f.insert_identity(1, n, 4, b"former");
        }
        for n in 0..10u32 {
            f.forget_identity(1, n);
        }
        f.insert(3, b"tokyo", 7);
        let records = f.log.id_records.len();
        assert!(records > 0 && f.log.slots.len() == 1, "前提: identity は record、 slot は印");
        assert_eq!(disk.persist(&mut f, false), Some("rewrite"));
        assert!(disk.snapshot.is_none(), "disk の無い DB の最初の書き出しで pkformer を並べた");
        assert!(disk.log.as_ref().unwrap().len() > LOG_HEADER_LEN + records, "identity の record を書いていない");
        let back = disk.load();
        assert_eq!(back.serialize(), f.serialize());
        assert_eq!(back.log.live_bytes, f.log.live_bytes, "変える時に数えた大きさが読み直した大きさと違う");
        // review987 [3]: 書けたら、 書き出しの lock の外で `pkformer` にまとめ直す (log を読まない 0.37.4 以前も前の名前を読める)
        assert!(disk.compact(&mut f, |_, _| {}), "最初の書き出しの後に pkformer にまとめ直さない");
        let old_reader = PkFormer::deserialize(disk.snapshot.as_ref().expect("pkformer を置いていない")).unwrap();
        assert_eq!(old_reader.serialize(), f.serialize());
        assert_eq!(disk.log.as_ref().unwrap().len(), LOG_HEADER_LEN);
        // 続きは追記
        f.insert_identity(2, 1, 3, b"k");
        assert_eq!(disk.persist(&mut f, false), Some("append"));
        assert_eq!(disk.load().serialize(), f.serialize());
        // 書けなかった後は memory から並べる (取り出した record を落とさない)
        f.insert_identity(2, 2, 3, b"lost");
        let _lost = f.take_for_persist(false).unwrap();
        f.persist_failed();
        f.insert_identity(2, 3, 3, b"k");
        let Some(Persist::Rewrite { snapshot, .. }) = f.take_for_persist(false) else { panic!("書けなかった後に追記した") };
        assert_eq!(snapshot, f.serialize());
        // disk の無い DB でも、 書けなかった後は memory から
        let mut g = PkFormer::default();
        g.insert_identity(1, 1, 3, b"a");
        let _lost = g.take_for_persist(false).unwrap();
        g.persist_failed();
        g.insert_identity(1, 2, 3, b"b");
        let Some(Persist::Rewrite { snapshot, .. }) = g.take_for_persist(false) else { panic!() };
        assert_eq!(snapshot, g.serialize(), "最初の書き出しが失敗した後に、 後の record だけを書いた");
        // 読んだ物は disk の無い DB ではない
        assert!(!PkFormer::deserialize(&f.serialize()).unwrap().log.fresh);
        assert!(!disk.load().log.fresh);
    }

    /// #959 / review987 [1]: disk が大きくなった時、 書き込みの lock の下では印の付いた所を取り出すだけ (`Append { compact: true }`、
    /// memory の全部を並べない)。 まとめ直しは書き出しの lock の外で、 追記の済んだ disk から作る。 作っている間に log が伸びたら、
    /// 伸びた分を作った中身に当てて作り直す: 置いた `pkformer` だけで全部ある (`pkformer` を置いた後・log を置く前の電源断の像では
    /// 古い log を読まないので、 伸びた分を新しい log に写すだけでは失う)。 log が `pkformer` の続きとして読めなければ作らない。 間に
    /// disk が置き換わったら置かない。
    #[test]
    fn a_grown_store_is_compacted_outside_the_lock_and_the_appends_made_meanwhile_go_into_pkformer() {
        let mut f = PkFormer::default();
        let mut disk = Disk::default();
        f.insert_identity(1, 0, 3, b"k");
        assert_eq!(disk.persist(&mut f, false), Some("rewrite"));
        assert!(disk.compact(&mut f, |_, _| {}));
        let mut n = 0u32;
        loop {
            n += 1;
            // 場所を使い回すたびに、 前の住人の名前を identity に移して slot を外す (engine の `move_names_and_forget_slot`)
            f.insert(3, &n.to_le_bytes(), n % 7);
            f.insert_identity(1, n, 3, &n.to_le_bytes());
            f.forget_identity(1, n.saturating_sub(3));
            match disk.persist(&mut f, false) {
                Some("compact") => break,
                Some("append") => assert!(f.begin_compact().is_none(), "頼んでいないのにまとめ直した ({n})"),
                other => panic!("log を信用できるのに {other:?} ({n})"),
            }
        }
        assert!(n > 100, "前提: 追記を重ねてからまとめ直す ({n})");
        let before = disk.snapshot.as_ref().map_or(0, |s| s.len()) + disk.log.as_ref().unwrap().len();
        assert!(disk.compact(&mut f, |f, d| {
            // まとめ直しの間に書き手が進み、 別の書き出しが追記する
            f.insert_identity(2, 9, 3, b"later");
            f.forget_slot(1);
            f.insert(3, b"renamed", 5);
            // まだ置いていないので、 足した後も大きい (まとめ直しを頼み直すだけ)
            assert!(matches!(d.persist(f, false), Some("append" | "compact")));
        }));
        assert_eq!(disk.log.as_ref().unwrap().len(), LOG_HEADER_LEN, "置いた log に record が残った");
        assert_eq!(disk.load().serialize(), f.serialize(), "まとめ直しの間の追記を落とした");
        let only = PkFormer::deserialize(disk.snapshot.as_ref().unwrap()).unwrap();
        assert_eq!(only.serialize(), f.serialize(), "まとめ直しの間の追記が pkformer に無い (pkformer を置いた後の電源断で失う)");
        assert!(disk.snapshot.as_ref().unwrap().len() + LOG_HEADER_LEN < before, "まとめ直して縮んでいない");
        assert!(f.begin_compact().is_none(), "置いた後も頼んだまま");
        // 切れた log / 続きでない log からはまとめ直さない
        f.insert_identity(2, 10, 3, b"x");
        disk.persist(&mut f, false);
        let log = disk.log.clone().unwrap();
        assert!(PkFormer::compact_read(disk.snapshot.as_deref(), &log[..log.len() - 2]).is_err());
        assert!(PkFormer::compact_read(None, &log).is_err());
        assert!(PkFormer::default().apply_log_tail(&log[LOG_HEADER_LEN..log.len() - 2]).is_err());
        // 間に disk が置き換わった (書けなかった / memory から書き直した) 時は置かない
        for interrupt in 0..2 {
            f.log.compact.wanted = true;
            let mark = f.begin_compact().unwrap();
            match interrupt {
                0 => f.persist_failed(),
                _ => {
                    // きれいに閉じる時の書き直し (memory から)
                    f.insert(3, b"y", 6);
                    assert_eq!(disk.persist(&mut f, false), Some("append"));
                    assert_eq!(disk.persist(&mut f, true), Some("rewrite"));
                }
            }
            assert_eq!(f.compact_check(&mark), None, "{interrupt}: 置き換わった disk にまとめ直しを置く");
            f.end_compact();
            disk.persist(&mut f, false);
            assert_eq!(disk.load().serialize(), f.serialize());
        }
    }
}
