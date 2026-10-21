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
//! 永続化は DB directory の `pkformer` に、 変わった時にまるごと書く (改名の数だけなので小さい)。 無ければ空。 読まない旧
//! binary は前の名前を知らないだけ (前と同じ、 今の名前でだけ束ねる)。
//!
//! file: magic "PKFN"(4) + version u32 + count u32、 続いて (himo u16, slot u32, 鍵の長さ u32, 鍵) × count。 version 2 は続けて
//! identity の名前: count u32、 (peer u32, その peer での番号 u32, himo u16, 鍵の長さ u32, 鍵) × count。 version 3 は続けて
//! 束ねた identity: count u32、 (slot u32, peer u32, その peer での番号 u32, wall u64, logical u32, HLC の peer u32) × count
//! (version 1 / 2 も読む)

use std::collections::HashMap;

/// (主キーの himo, 値の鍵)
type Name = (u16, Box<[u8]>);

const MAGIC: &[u8; 4] = b"PKFN";
const VERSION: u32 = 3;

/// #581: 束ねた identity (作った peer, その peer での番号) と作成の HLC。
pub(crate) type Bound = ((u32, u32), enchudb_oplog::Hlc);

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
}

impl PkFormer {
    pub(crate) fn is_empty(&self) -> bool {
        self.by_key.is_empty() && self.by_identity.is_empty() && self.bound.is_empty()
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

    /// slot を別の行に使い回す前に、 その行の前の名前を外す。 変えたら true。
    pub(crate) fn forget_slot(&mut self, slot: u32) -> bool {
        let unbound = self.bound.remove(&slot).is_some();
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
        if p != buf.len() {
            return Err(format!("pkformer sidecar: {} trailing bytes (corrupt)", buf.len() - p));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_moves_to_the_row_that_took_it_last_and_leaves_with_the_slot() {
        let mut f = PkFormer::default();
        assert!(f.insert(3, b"tokyo", 10));
        assert!(!f.insert(3, b"tokyo", 10), "同じ記録を変えたと言う");
        assert!(f.insert(3, b"osaka", 10));
        assert!(f.insert(3, b"tokyo", 11), "別の行が後から同じ前の名前を持った");
        assert_eq!(f.slot_of(3, b"tokyo"), Some(11));
        assert_eq!(f.slot_of(4, b"tokyo"), None, "himo を見ていない");
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
        // version 1 (identity の節も束ねた identity の節も無い) / version 2 (束ねた identity の節が無い) も読む
        let mut v1 = PkFormer::default();
        v1.insert(3, b"tokyo", 10);
        for (version, cut) in [(1u32, 8), (2, 4)] {
            let mut old = v1.serialize();
            old[4..8].copy_from_slice(&version.to_le_bytes());
            old.truncate(old.len() - cut); // 後ろの節の数 (0) を落とす
            assert_eq!(PkFormer::deserialize(&old).unwrap().slot_of(3, b"tokyo"), Some(10), "version {version}");
        }
    }
}
