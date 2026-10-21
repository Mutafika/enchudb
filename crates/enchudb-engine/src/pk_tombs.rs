//! 消した主キーの記録 (#513)。
//!
//! 主キーのある表では、 違う author が同じ主キーの行を別々に作っても、 受け手は主キーで 1 つの行に束ねる (#141)。 ところが
//! 行を消した後に、 別の author の同じ主キーの行 (削除より古い) が届くと、 束ねる先の生きた行が無いので新しい行として
//! 生きる。 その author の行を先に受けて束ねていた peer では、 同じ削除で一緒に消えているので、 peer どうしが恒久に食い違った。
//!
//! 答えの形 (主キーが同じなら同じ行): 削除はその主キーの行の、 削除より古い書き込みを、 どの author のものでも消す
//! (削除より新しい書き込みは残る = 主キーごとの LWW)。 受け手は主キーのある行を消した時に 「主キー → 削除の版数」 を覚え、
//! 後から同じ主キーの行が届いたら、 束ねた行 / 払い出す slot にその削除を当てる。
//!
//! - 主キーごと: (主キーの himo, 値の鍵) → その主キーの行を消した削除の版数の最大
//! - slot ごと: 主キーの行だった slot → その主キー。 消えた slot に後から別の author の (より新しい) 削除が届いた時、
//!   主キーの cell はもう無いので、 ここから主キーを引いて版数を上げる (届く順で答えが変わらないように)
//!
//! 値の鍵は、 辞書の値 (Tag) は文字列の bytes (辞書の番号は回収で別の値に使い回されうる、 #381)、 それ以外は値の 8 byte。
//!
//! 消した主キーの数だけ増える (減らさない)。 永続化は DB directory の `pktombs` に **追記** する (変わった分だけ。 他の
//! sidecar のように毎回まるごと書くと、 消すほど書き出しが重くなる)。 record が生きている記録の 2 倍を越えたら、 書き出しの
//! 時にまとめ直す (一時 file に書いて rename)。
//!
//! file: magic "PKTL"(4) + version u32、 続いて record の列 (どれも末尾に、 record の先頭からの FNV-1a 32 の検査値 u32):
//!
//! - 記録: tag 1, himo u16, slot u32 (`NO_SLOT` = slot 無し), 鍵の長さ u32, 鍵, 版数 wall u64, logical u32, peer u32
//! - slot を外す: tag 2, slot u32
//!
//! 読む時は、 切れた / 壊れた record から後を捨てる (追記の途中で落ちた跡)。 その時は次の書き出しでまとめ直す (切れた跡の
//! 後ろに追記すると、 次に読む時にそこで止まって失う)。

use std::collections::HashMap;

use enchudb_oplog::Hlc;

const MAGIC: &[u8; 4] = b"PKTL";
const VERSION: u32 = 1;
const HEADER_LEN: usize = 8;
const TAG_NOTE: u8 = 1;
const TAG_FORGET: u8 = 2;
/// 記録の record で 「slot 無し」 (まとめ直した file の、 slot の記録の無い主キー)。 slot は `max_entities` 未満。
pub(crate) const NO_SLOT: u32 = u32::MAX;
/// まとめ直す目安: file の record の数が、 生きている記録 (主キー + slot) の 2 倍とこの数の和を越えたら。
const REWRITE_SLACK: u64 = 1024;

/// 消した主キーの記録。 engine が `RwLock` で包んで持つ。
#[derive(Default, Debug)]
pub(crate) struct PkTombs {
    /// 主キーの himo → 値の鍵 → 主キーの番号 (`keys` の何番目か)。
    ids: HashMap<u16, HashMap<Box<[u8]>, u32>>,
    /// 主キーの番号 → (himo, 値の鍵, 削除の版数の最大)。
    keys: Vec<(u16, Box<[u8]>, Hlc)>,
    /// 主キーの行だった slot → 主キーの番号。
    by_slot: HashMap<u32, u32>,
    /// まだ file に書いていない record。
    pending: Vec<u8>,
    pending_records: u64,
    /// file にある record の数。 `None` = file が無い / 信用できない (次の書き出しでまとめ直す)。
    in_file: Option<u64>,
}

/// 書き出しの仕事 ([`PkTombs::take_for_persist`])。
#[derive(Debug)]
pub(crate) enum Persist {
    /// file に追記する record。
    Append { bytes: Vec<u8>, records: u64 },
    /// file 全体 (header から) を書き直す。
    Rewrite { bytes: Vec<u8>, records: u64 },
}

impl PkTombs {
    pub(crate) fn is_empty(&self) -> bool {
        self.keys.is_empty() && self.by_slot.is_empty()
    }

    fn id_of(&self, himo: u16, key: &[u8]) -> Option<u32> {
        self.ids.get(&himo)?.get(key).copied()
    }

    /// 主キー `(himo, key)` の行を消した削除の版数。
    pub(crate) fn tomb_of(&self, himo: u16, key: &[u8]) -> Option<Hlc> {
        self.id_of(himo, key).map(|id| self.keys[id as usize].2)
    }

    /// slot が最後に主キーの行だった時の主キー。
    pub(crate) fn key_of_slot(&self, slot: u32) -> Option<(u16, &[u8])> {
        let &id = self.by_slot.get(&slot)?;
        let (h, k, _) = &self.keys[id as usize];
        Some((*h, &k[..]))
    }

    /// 覚えている主キーの数 (観測・test 用)。
    pub(crate) fn key_count(&self) -> usize {
        self.keys.len()
    }

    /// 覚えている主キー (himo, 値の鍵)。
    pub(crate) fn keys(&self) -> impl Iterator<Item = (u16, &[u8])> {
        self.keys.iter().map(|(h, k, _)| (*h, &k[..]))
    }

    /// slot `slot` (主キー `(himo, key)` の行) を版数 `hlc` で消した、 と覚える (`slot` が `NO_SLOT` なら主キーだけ)。 主キーの
    /// 版数は大きい方を残す。 何か変わったら `true` (書き出しが要る)。
    pub(crate) fn note(&mut self, himo: u16, key: &[u8], slot: u32, hlc: Hlc) -> bool {
        if !self.apply_note(himo, key, slot, hlc) {
            return false;
        }
        encode_note(&mut self.pending, himo, key, slot, hlc);
        self.pending_records += 1;
        true
    }

    fn apply_note(&mut self, himo: u16, key: &[u8], slot: u32, hlc: Hlc) -> bool {
        let mut changed = false;
        let id = match self.id_of(himo, key) {
            Some(id) => {
                let t = &mut self.keys[id as usize].2;
                if *t < hlc {
                    *t = hlc;
                    changed = true;
                }
                id
            }
            None => {
                let id = self.keys.len() as u32;
                self.keys.push((himo, key.into(), hlc));
                self.ids.entry(himo).or_default().insert(key.into(), id);
                changed = true;
                id
            }
        };
        if slot != NO_SLOT && self.by_slot.insert(slot, id) != Some(id) {
            changed = true;
        }
        changed
    }

    /// slot を別の住人に渡す前に、 slot ごとの記録を外す (主キーごとの版数は残す)。 外したら `true`。
    pub(crate) fn forget_slot(&mut self, slot: u32) -> bool {
        if self.by_slot.remove(&slot).is_none() {
            return false;
        }
        encode_forget(&mut self.pending, slot);
        self.pending_records += 1;
        true
    }

    /// 書き出す分を取り出す (呼び手は書いた結果を [`PkTombs::persisted`] / [`PkTombs::persist_failed`] で返す)。 file を
    /// 信用できない時と、 record が増えすぎた時はまとめ直す。 書くものが無ければ `None`。
    pub(crate) fn take_for_persist(&mut self) -> Option<Persist> {
        let live = (self.keys.len() + self.by_slot.len()) as u64;
        let rewrite = match self.in_file {
            None => true,
            Some(n) => n + self.pending_records > 2 * live + REWRITE_SLACK,
        };
        if rewrite {
            self.pending.clear();
            self.pending_records = 0;
            let (bytes, records) = self.snapshot();
            return Some(Persist::Rewrite { bytes, records });
        }
        if self.pending_records == 0 {
            return None;
        }
        let bytes = std::mem::take(&mut self.pending);
        let records = std::mem::replace(&mut self.pending_records, 0);
        Some(Persist::Append { bytes, records })
    }

    /// 書き出しが済んだ。
    pub(crate) fn persisted(&mut self, job: &Persist) {
        self.in_file = match (job, self.in_file) {
            (Persist::Rewrite { records, .. }, _) => Some(*records),
            (Persist::Append { records, .. }, Some(n)) => Some(n + records),
            // 書いている間に別の書き出しが失敗した (今は書き出しを 1 本に並べているので起きない)。 まとめ直しに倒す
            (Persist::Append { .. }, None) => None,
        };
    }

    /// 書き出しに失敗した。 追記の途中で切れた record が file に残りうるので、 次はまとめ直す (取り出した record は
    /// memory の記録に入っているので、 まとめ直しが書く)。
    pub(crate) fn persist_failed(&mut self) {
        self.in_file = None;
    }

    /// file 全体 (header + 生きている記録)。 並びは決める (同じ中身なら同じ bytes)。 戻り値の 2 つ目は record の数。
    pub(crate) fn snapshot(&self) -> (Vec<u8>, u64) {
        let mut order: Vec<u32> = (0..self.keys.len() as u32).collect();
        order.sort_unstable_by(|&a, &b| {
            let (ha, ka, _) = &self.keys[a as usize];
            let (hb, kb, _) = &self.keys[b as usize];
            (ha, ka).cmp(&(hb, kb))
        });
        let mut slots_of: HashMap<u32, Vec<u32>> = HashMap::new();
        for (&slot, &id) in &self.by_slot {
            slots_of.entry(id).or_default().push(slot);
        }
        let mut out = Vec::with_capacity(HEADER_LEN + self.keys.len() * 40 + self.by_slot.len() * 40);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        let mut records = 0u64;
        for id in order {
            let (h, k, t) = &self.keys[id as usize];
            match slots_of.get_mut(&id) {
                Some(slots) => {
                    slots.sort_unstable();
                    for &slot in slots.iter() {
                        encode_note(&mut out, *h, k, slot, *t);
                        records += 1;
                    }
                }
                None => {
                    encode_note(&mut out, *h, k, NO_SLOT, *t);
                    records += 1;
                }
            }
        }
        (out, records)
    }

    /// file を読む。 magic・版が違えば Err。 切れた / 壊れた record から後は捨て、 その時は次の書き出しでまとめ直す。
    /// 戻り値の 2 つ目は捨てた跡があったか。
    pub(crate) fn load(buf: &[u8]) -> Result<(Self, bool), String> {
        if buf.len() < HEADER_LEN || &buf[..4] != MAGIC {
            return Err("pktombs sidecar: bad magic".into());
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version != VERSION {
            return Err(format!("pktombs sidecar: unsupported version {version}"));
        }
        let mut out = PkTombs::default();
        let mut off = HEADER_LEN;
        let mut records = 0u64;
        let mut torn = false;
        while off < buf.len() {
            match decode(&buf[off..]) {
                Some((Record::Note { himo, key, slot, hlc }, len)) => {
                    out.apply_note(himo, key, slot, hlc);
                    off += len;
                }
                Some((Record::Forget { slot }, len)) => {
                    out.by_slot.remove(&slot);
                    off += len;
                }
                None => {
                    torn = true;
                    break;
                }
            }
            records += 1;
        }
        out.in_file = if torn { None } else { Some(records) };
        Ok((out, torn))
    }
}

enum Record<'a> {
    Note { himo: u16, key: &'a [u8], slot: u32, hlc: Hlc },
    Forget { slot: u32 },
}

fn encode_note(out: &mut Vec<u8>, himo: u16, key: &[u8], slot: u32, hlc: Hlc) {
    let start = out.len();
    out.push(TAG_NOTE);
    out.extend_from_slice(&himo.to_le_bytes());
    out.extend_from_slice(&slot.to_le_bytes());
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&hlc.wall.to_le_bytes());
    out.extend_from_slice(&hlc.logical.to_le_bytes());
    out.extend_from_slice(&hlc.peer.to_le_bytes());
    let check = fnv1a(&out[start..]);
    out.extend_from_slice(&check.to_le_bytes());
}

fn encode_forget(out: &mut Vec<u8>, slot: u32) {
    let start = out.len();
    out.push(TAG_FORGET);
    out.extend_from_slice(&slot.to_le_bytes());
    let check = fnv1a(&out[start..]);
    out.extend_from_slice(&check.to_le_bytes());
}

/// 先頭の record を読む (record と長さ)。 切れている / 検査値が合わない / tag が違うなら `None`。
fn decode(buf: &[u8]) -> Option<(Record<'_>, usize)> {
    let u32_at = |off: usize| buf.get(off..off + 4).map(|b| u32::from_le_bytes(b.try_into().unwrap()));
    let (record, body) = match *buf.first()? {
        TAG_NOTE => {
            let himo = u16::from_le_bytes(buf.get(1..3)?.try_into().unwrap());
            let slot = u32_at(3)?;
            let len = u32_at(7)? as usize;
            let key = buf.get(11..11usize.checked_add(len)?)?;
            let at = 11 + len;
            let wall = u64::from_le_bytes(buf.get(at..at + 8)?.try_into().unwrap());
            let logical = u32_at(at + 8)?;
            let peer = u32_at(at + 12)?;
            (Record::Note { himo, key, slot, hlc: Hlc { wall, logical, peer } }, at + 16)
        }
        TAG_FORGET => (Record::Forget { slot: u32_at(1)? }, 5),
        _ => return None,
    };
    (u32_at(body)? == fnv1a(&buf[..body])).then_some((record, body + 4))
}

fn fnv1a(bytes: &[u8]) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &b in bytes {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hlc(wall: u64) -> Hlc {
        Hlc { wall, logical: 0, peer: 1 }
    }

    /// 書き出しの仕事を file (Vec) に当てる。
    fn write(file: &mut Vec<u8>, p: &mut PkTombs) -> Option<&'static str> {
        let job = p.take_for_persist()?;
        let kind = match &job {
            Persist::Rewrite { bytes, .. } => {
                *file = bytes.clone();
                "rewrite"
            }
            Persist::Append { bytes, .. } => {
                file.extend_from_slice(bytes);
                "append"
            }
        };
        p.persisted(&job);
        Some(kind)
    }

    fn same(a: &PkTombs, b: &PkTombs) -> bool {
        a.snapshot().0 == b.snapshot().0
    }

    #[test]
    fn note_keeps_the_newest_delete_per_key() {
        let mut p = PkTombs::default();
        assert!(p.note(3, b"z", 10, hlc(5)));
        assert!(!p.note(3, b"z", 10, hlc(4)), "古い削除で下げない");
        assert_eq!(p.tomb_of(3, b"z"), Some(hlc(5)));
        assert!(p.note(3, b"z", 11, hlc(7)), "別の slot の新しい削除で上げる");
        assert_eq!(p.tomb_of(3, b"z"), Some(hlc(7)));
        assert_eq!(p.key_of_slot(10), Some((3, &b"z"[..])));
        assert_eq!(p.key_of_slot(11), Some((3, &b"z"[..])));
        assert_eq!(p.tomb_of(4, b"z"), None, "himo が違えば別の主キー");
        assert_eq!(p.tomb_of(3, b"y"), None);
        assert!(p.forget_slot(10));
        assert!(!p.forget_slot(10));
        assert_eq!(p.key_of_slot(10), None);
        assert_eq!(p.tomb_of(3, b"z"), Some(hlc(7)), "slot を外しても主キーの版数は残す");
        assert!(p.note(3, b"y", NO_SLOT, hlc(2)), "slot 無しの記録");
        assert_eq!(p.key_count(), 2);
    }

    #[test]
    fn appends_what_changed_and_reads_back_the_same() {
        let mut p = PkTombs::default();
        let mut file = Vec::new();
        assert_eq!(write(&mut file, &mut p), Some("rewrite"), "file が無ければまとめて書く");
        assert_eq!(write(&mut file, &mut p), None, "変わっていなければ書かない");
        p.note(3, b"z", 10, hlc(5));
        p.note(3, b"", 12, hlc(6));
        p.note(9, &42u64.to_le_bytes(), 13, Hlc { wall: 8, logical: 2, peer: 7 });
        let before = file.len();
        assert_eq!(write(&mut file, &mut p), Some("append"));
        assert!(file.len() > before);
        p.note(3, b"z", 14, hlc(9));
        p.forget_slot(12);
        assert_eq!(write(&mut file, &mut p), Some("append"));
        let (back, torn) = PkTombs::load(&file).unwrap();
        assert!(!torn);
        assert!(same(&back, &p));
        assert_eq!(back.tomb_of(3, b"z"), Some(hlc(9)));
        assert_eq!(back.key_of_slot(12), None);
        assert_eq!(back.tomb_of(3, b""), Some(hlc(6)), "slot を外しても主キーは残る");
        // 読んだ物に続けて追記できる
        let mut back = back;
        back.note(3, b"w", 15, hlc(10));
        assert_eq!(write(&mut file, &mut back), Some("append"));
        let (again, _) = PkTombs::load(&file).unwrap();
        assert!(same(&again, &back));
    }

    #[test]
    fn a_torn_tail_is_dropped_and_rewritten_next_time() {
        let mut p = PkTombs::default();
        let mut file = Vec::new();
        p.note(3, b"z", 10, hlc(5));
        write(&mut file, &mut p);
        let whole = file.len();
        p.note(3, b"y", 11, hlc(6));
        write(&mut file, &mut p);
        // 最後の record の途中で切れた / 末尾が 0 で埋まった / 1 byte 化けた
        for broken in [
            file[..file.len() - 3].to_vec(),
            [&file[..whole], &[0u8; 9][..]].concat(),
            {
                let mut b = file.clone();
                let i = b.len() - 6;
                b[i] ^= 1;
                b
            },
        ] {
            let (mut back, torn) = PkTombs::load(&broken).unwrap();
            assert!(torn);
            assert_eq!(back.tomb_of(3, b"z"), Some(hlc(5)), "切れた所より前は残る");
            assert_eq!(back.tomb_of(3, b"y"), None);
            let mut f = broken.clone();
            back.note(3, b"x", 12, hlc(7));
            assert_eq!(write(&mut f, &mut back), Some("rewrite"), "切れた跡の後ろに追記しない");
            let (again, torn) = PkTombs::load(&f).unwrap();
            assert!(!torn);
            assert!(same(&again, &back));
        }
        assert!(PkTombs::load(b"PKTX\x01\0\0\0").is_err(), "magic");
        assert!(PkTombs::load(b"PKTL\x02\0\0\0").is_err(), "版");
        assert!(PkTombs::load(b"PKT").is_err());
        // 鍵の長さが壊れて巨大 (確保せずに切れた扱い)
        let mut huge = Vec::new();
        huge.extend_from_slice(MAGIC);
        huge.extend_from_slice(&VERSION.to_le_bytes());
        huge.extend_from_slice(&[TAG_NOTE, 3, 0, 1, 0, 0, 0]);
        huge.extend_from_slice(&u32::MAX.to_le_bytes());
        let (back, torn) = PkTombs::load(&huge).unwrap();
        assert!(torn && back.is_empty());
    }

    #[test]
    fn a_failed_write_is_rewritten_and_many_records_are_compacted() {
        let mut p = PkTombs::default();
        let mut file = Vec::new();
        p.note(3, b"z", 10, hlc(5));
        write(&mut file, &mut p);
        p.note(3, b"y", 11, hlc(6));
        let job = p.take_for_persist().unwrap();
        assert!(matches!(job, Persist::Append { .. }));
        p.persist_failed();
        assert_eq!(write(&mut file, &mut p), Some("rewrite"), "失敗の後はまとめ直す (取り出した record も入る)");
        assert_eq!(PkTombs::load(&file).unwrap().0.tomb_of(3, b"y"), Some(hlc(6)));
        // 同じ主キーの削除を何度も上げると record は増えるが、 生きている記録は 1 つ
        let mut appended = 0;
        for i in 0..3000u64 {
            p.note(3, b"z", 10, hlc(10 + i));
            if write(&mut file, &mut p) == Some("append") {
                appended += 1;
            }
        }
        assert!(appended > 0);
        let (back, _) = PkTombs::load(&file).unwrap();
        assert!(same(&back, &p));
        // 生きている記録は 主キー 2 (z, y) + slot 2 (10, 11)
        assert!(back.in_file.unwrap() <= 2 * 4 + REWRITE_SLACK, "まとめ直して record が減った");
    }
}
