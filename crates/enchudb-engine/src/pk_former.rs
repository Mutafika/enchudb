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
//!
//! 永続化は DB directory の `pkformer` に、 変わった時にまるごと書く (改名の数だけなので小さい)。 無ければ空。 読まない旧
//! binary は前の名前を知らないだけ (前と同じ、 今の名前でだけ束ねる)。
//!
//! file: magic "PKFN"(4) + version u32=1 + count u32、 続いて (himo u16, slot u32, 鍵の長さ u32, 鍵) × count

use std::collections::HashMap;

/// (主キーの himo, 値の鍵)
type Name = (u16, Box<[u8]>);

const MAGIC: &[u8; 4] = b"PKFN";
const VERSION: u32 = 1;

/// 主キーの前の名前。 engine が `RwLock` で包んで持つ。
#[derive(Default, Debug)]
pub(crate) struct PkFormer {
    /// (主キーの himo, 前の値の鍵) → 行の slot
    by_key: HashMap<Name, u32>,
    /// slot → その行の前の名前 (slot を使い回す時に外す)
    by_slot: HashMap<u32, Vec<Name>>,
}

impl PkFormer {
    pub(crate) fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    /// 前に `(himo, key)` だった行の slot。
    pub(crate) fn slot_of(&self, himo: u16, key: &[u8]) -> Option<u32> {
        self.by_key.get(&(himo, Box::<[u8]>::from(key))).copied()
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

    /// slot を別の行に使い回す前に、 その行の前の名前を外す。 変えたら true。
    pub(crate) fn forget_slot(&mut self, slot: u32) -> bool {
        let Some(keys) = self.by_slot.remove(&slot) else { return false };
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
        out
    }

    /// `serialize` の逆。 magic / 版の不一致・切れた中身は Err。
    pub(crate) fn deserialize(buf: &[u8]) -> Result<Self, String> {
        if buf.len() < 12 || &buf[0..4] != MAGIC {
            return Err("pkformer sidecar: bad magic".into());
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version != VERSION {
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
}
