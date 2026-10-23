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
//! memory (#560): 主キー 1 つに記録 32 B (8 byte 以下の鍵はその中に置き、 長い鍵だけ `arena` に 1 回置く) + 番号の表 4 B ×
//! 2〜4 + slot の記録。 前は鍵を 2 回 heap に写し、 主キーの himo ごとの `HashMap` で引いていた (数の鍵で 1 鍵 147 B、 100 万鍵)。
//! 受け手が lock を取らずに弾く bit の表 ([`PkBloom`]) も、 主キーの数に合わせて作り直す (前は 64 KiB 固定で、 100 万鍵で
//! 85 % が素通りした)。
//!
//! file: magic "PKTL"(4) + version u32、 続いて record の列 (どれも末尾に、 record の先頭からの FNV-1a 32 の検査値 u32):
//!
//! - 記録: tag 1, himo u16, slot u32 (`NO_SLOT` = slot 無し), 鍵の長さ u32, 鍵, 版数 wall u64, logical u32, peer u32
//! - slot を外す: tag 2, slot u32
//!
//! 読む時は、 切れた / 壊れた record から後を捨てる (追記の途中で落ちた跡)。 その時は次の書き出しでまとめ直す (切れた跡の
//! 後ろに追記すると、 次に読む時にそこで止まって失う)。

use std::collections::HashMap;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

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
/// 番号の表の空き。
const EMPTY: u32 = u32::MAX;
/// 記録の中に置く鍵の長さの上限 (これより長い鍵は `arena`)。
const INLINE: usize = 8;
/// slot の記録の表を縮める目安 (確保した数がこれを越え、 中身が 1/4 未満になったら)。
const SLOT_SHRINK_MIN: usize = 4096;

/// 主キーの記録 1 つ (32 B)。 鍵が [`INLINE`] byte 以下なら `data` に置き、 長ければ `data` は `arena` の位置 (u64 LE)。
#[derive(Debug, Clone, Copy)]
struct Key {
    wall: u64,
    data: [u8; INLINE],
    logical: u32,
    peer: u32,
    len: u32,
    himo: u16,
}

impl Key {
    fn hlc(&self) -> Hlc {
        Hlc { wall: self.wall, logical: self.logical, peer: self.peer }
    }
}

/// 主キー `(himo, key)` の hash (番号の表と [`PkBloom`] が使う)。 FNV-1a 64 の後に混ぜる (表は下の bit、 bit の表は上の bit を使う)。
fn key_hash(himo: u16, key: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in himo.to_le_bytes().iter().chain(key) {
        h ^= b as u64;
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

/// 消した主キーの記録。 engine が `RwLock` で包んで持つ。
#[derive(Default, Debug)]
pub(crate) struct PkTombs {
    /// 主キーの番号 → 記録。
    keys: Vec<Key>,
    /// [`INLINE`] byte より長い鍵の bytes (主キーごとに 1 回)。
    arena: Vec<u8>,
    /// (himo, 鍵) → 主キーの番号 の開番地の表 (線形に探す、 `EMPTY` = 空き)。 大きさは 2 の冪で主キーの数の 2 倍以上 (空きが
    /// 必ずあるので探しは止まる)。
    table: Vec<u32>,
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

    fn bytes<'a>(&'a self, k: &'a Key) -> &'a [u8] {
        let len = k.len as usize;
        if len <= INLINE {
            &k.data[..len]
        } else {
            let at = u64::from_le_bytes(k.data) as usize;
            &self.arena[at..at + len]
        }
    }

    fn id_of(&self, himo: u16, key: &[u8]) -> Option<u32> {
        if self.table.is_empty() {
            return None;
        }
        let mask = self.table.len() - 1;
        let mut i = key_hash(himo, key) as usize & mask;
        loop {
            let id = self.table[i];
            if id == EMPTY {
                return None;
            }
            let k = &self.keys[id as usize];
            if k.himo == himo && self.bytes(k) == key {
                return Some(id);
            }
            i = (i + 1) & mask;
        }
    }

    /// 番号 `id` を表の空きに置く。
    fn place(&mut self, id: u32, hash: u64) {
        let mask = self.table.len() - 1;
        let mut i = hash as usize & mask;
        while self.table[i] != EMPTY {
            i = (i + 1) & mask;
        }
        self.table[i] = id;
    }

    /// 新しい主キーを足す (呼び手は無いことを確かめた)。
    fn push_key(&mut self, himo: u16, key: &[u8], hlc: Hlc) -> u32 {
        if (self.keys.len() + 1) * 2 > self.table.len() {
            self.table = vec![EMPTY; (self.table.len() * 2).max(16)];
            for id in 0..self.keys.len() {
                let k = self.keys[id];
                let hash = key_hash(k.himo, self.bytes(&k));
                self.place(id as u32, hash);
            }
        }
        let id = self.keys.len() as u32;
        let mut data = [0u8; INLINE];
        if key.len() <= INLINE {
            data[..key.len()].copy_from_slice(key);
        } else {
            data = (self.arena.len() as u64).to_le_bytes();
            self.arena.extend_from_slice(key);
        }
        self.keys.push(Key { wall: hlc.wall, data, logical: hlc.logical, peer: hlc.peer, len: key.len() as u32, himo });
        self.place(id, key_hash(himo, key));
        id
    }

    /// 主キー `(himo, key)` の行を消した削除の版数。
    pub(crate) fn tomb_of(&self, himo: u16, key: &[u8]) -> Option<Hlc> {
        self.id_of(himo, key).map(|id| self.keys[id as usize].hlc())
    }

    /// slot が最後に主キーの行だった時の主キー。
    pub(crate) fn key_of_slot(&self, slot: u32) -> Option<(u16, &[u8])> {
        let &id = self.by_slot.get(&slot)?;
        let k = &self.keys[id as usize];
        Some((k.himo, self.bytes(k)))
    }

    /// 覚えている主キーの数 (観測・test 用)。
    pub(crate) fn key_count(&self) -> usize {
        self.keys.len()
    }

    /// 覚えている主キー (himo, 値の鍵)。
    pub(crate) fn keys(&self) -> impl Iterator<Item = (u16, &[u8])> {
        self.keys.iter().map(|k| (k.himo, self.bytes(k)))
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
                let k = &mut self.keys[id as usize];
                if k.hlc() < hlc {
                    (k.wall, k.logical, k.peer) = (hlc.wall, hlc.logical, hlc.peer);
                    changed = true;
                }
                id
            }
            None => {
                changed = true;
                self.push_key(himo, key, hlc)
            }
        };
        if slot != NO_SLOT && self.by_slot.insert(slot, id) != Some(id) {
            changed = true;
        }
        changed
    }

    /// slot の記録を外す。 表の中身が確保の 1/4 を切ったら縮める (slot は使い回されると外れるので、 消した後に使い回す DB で
    /// 確保だけが山のまま残らないように)。
    fn remove_slot(&mut self, slot: u32) -> bool {
        if self.by_slot.remove(&slot).is_none() {
            return false;
        }
        let cap = self.by_slot.capacity();
        if cap > SLOT_SHRINK_MIN && self.by_slot.len() * 4 < cap {
            self.by_slot.shrink_to(self.by_slot.len() * 2);
        }
        true
    }

    /// slot を別の住人に渡す前に、 slot ごとの記録を外す (主キーごとの版数は残す)。 外したら `true`。
    pub(crate) fn forget_slot(&mut self, slot: u32) -> bool {
        if !self.remove_slot(slot) {
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
            // 確保も手放す (clear だと溜めた山の大きさのまま残る)
            self.pending = Vec::new();
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
            let (ka, kb) = (&self.keys[a as usize], &self.keys[b as usize]);
            (ka.himo, self.bytes(ka)).cmp(&(kb.himo, self.bytes(kb)))
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
            let k = &self.keys[id as usize];
            let (h, key, t) = (k.himo, self.bytes(k), k.hlc());
            match slots_of.get_mut(&id) {
                Some(slots) => {
                    slots.sort_unstable();
                    for &slot in slots.iter() {
                        encode_note(&mut out, h, key, slot, t);
                        records += 1;
                    }
                }
                None => {
                    encode_note(&mut out, h, key, NO_SLOT, t);
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
                    out.remove_slot(slot);
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

/// 覚えた主キーの bit の表。 受け手が新しい行ごとに引く時に、 記録の lock を取らずに 「覚えていない」 を答える (立っていない
/// 主キーは覚えていない。 立っていても覚えていないことはある)。
///
/// 主キー 1 つに 2 bit を 64 bit の 1 語の中に立てる (読むのは 1 語)。 主キー 1 つあたり 16 bit 以上を保つ: 主キーが増えたら
/// 2 倍以上の表を記録の全部の主キーから作って替える (素通りは 2 % 前後)。 前の表は lock を取らない読み手がまだ見ているかも
/// しれないので、 開き直すまで残す (合わせて今の表の 2 倍まで)。 替えた後に立てた主キーを前の表で読み損ねるのは、 立てる前に
/// 読んだのと同じ。
#[derive(Debug)]
pub(crate) struct PkBloom {
    /// 今の表 (`tables` の何番目 + 1、 0 = まだ無い)。
    level: AtomicUsize,
    /// 番目 i の表は 2^(BLOOM_MIN_LOG2 + i) bit。
    tables: [OnceLock<Box<[AtomicU64]>>; BLOOM_LEVELS],
}

/// 最初の表の大きさ (bit の 2 の冪)。 64 KiB。
const BLOOM_MIN_LOG2: u32 = 19;
/// 主キー 1 つあたりの bit の下限。
const BLOOM_BITS_PER_KEY: usize = 16;
/// 表の段の数 (2^19 .. 2^36 bit = 主キー 2^32 個まで)。
const BLOOM_LEVELS: usize = 18;

impl Default for PkBloom {
    fn default() -> Self {
        Self { level: AtomicUsize::new(0), tables: std::array::from_fn(|_| OnceLock::new()) }
    }
}

impl PkBloom {
    /// 語の番目と立てる bit。
    fn spot(words: usize, hash: u64) -> (usize, u64) {
        ((hash >> 32) as usize & (words - 1), 1u64 << (hash & 63) | 1u64 << ((hash >> 6) & 63))
    }

    /// `keys` 個の主キーに要る段 (1 から)。
    fn level_for(keys: usize) -> usize {
        let bits = keys.saturating_mul(BLOOM_BITS_PER_KEY).max(1).next_power_of_two();
        ((bits.trailing_zeros().max(BLOOM_MIN_LOG2) - BLOOM_MIN_LOG2) as usize + 1).min(BLOOM_LEVELS)
    }

    /// 主キー `(himo, key)` を覚えているかもしれない (false なら覚えていない)。
    pub(crate) fn maybe(&self, himo: u16, key: &[u8]) -> bool {
        let level = self.level.load(Ordering::Acquire);
        if level == 0 {
            return false;
        }
        let Some(words) = self.tables[level - 1].get() else { return true };
        let (w, m) = Self::spot(words.len(), key_hash(himo, key));
        words[w].load(Ordering::Acquire) & m == m
    }

    /// 表が `tombs` の主キーの数に対して小さければ、 大きい表を `tombs` の全部の主キーから作って替える。 替えたら `true`。
    /// 呼び手は記録の書き込みの lock を握っている (作り直しは 1 本ずつ)。
    pub(crate) fn fit(&self, tombs: &PkTombs) -> bool {
        let level = self.level.load(Ordering::Relaxed);
        let want = Self::level_for(tombs.key_count());
        if tombs.key_count() == 0 || want <= level {
            return false;
        }
        let words: Box<[AtomicU64]> =
            (0..(1usize << (BLOOM_MIN_LOG2 as usize + want - 1)) / 64).map(|_| AtomicU64::new(0)).collect();
        for (himo, key) in tombs.keys() {
            let (w, m) = Self::spot(words.len(), key_hash(himo, key));
            words[w].fetch_or(m, Ordering::Relaxed);
        }
        // 段は上がるだけで、 1 つの段は 1 度しか作らない
        let _ = self.tables[want - 1].set(words);
        self.level.store(want, Ordering::Release);
        true
    }

    /// 主キー `(himo, key)` を立てる。 `tombs` はこの主キーを載せた後の記録で、 呼び手はその書き込みの lock を握っている。
    pub(crate) fn add(&self, tombs: &PkTombs, himo: u16, key: &[u8]) {
        if self.fit(tombs) {
            return; // 作り直した表に入っている
        }
        let level = self.level.load(Ordering::Relaxed);
        let Some(words) = level.checked_sub(1).and_then(|l| self.tables[l].get()) else { return };
        let (w, m) = Self::spot(words.len(), key_hash(himo, key));
        words[w].fetch_or(m, Ordering::Release);
    }

    /// 今の表の bit の数 (観測・test 用)。
    #[cfg(test)]
    fn bits(&self) -> usize {
        let level = self.level.load(Ordering::Acquire);
        level.checked_sub(1).and_then(|l| self.tables[l].get()).map_or(0, |w| w.len() * 64)
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

    /// 乱数 (xorshift)。
    fn rng(state: &mut u64) -> u64 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    }

    #[test]
    fn keys_of_every_length_are_found_and_read_back() {
        // 長さ 0..=20 (記録の中に置く 8 byte 以下と arena の境をまたぐ)、 himo 4 本、 同じ主キーを版数を上げ下げして何度も
        let mut p = PkTombs::default();
        let mut want: HashMap<(u16, Vec<u8>), Hlc> = HashMap::new();
        let mut slot_of: HashMap<u32, (u16, Vec<u8>)> = HashMap::new();
        let mut st = 0x9e37_79b9_7f4a_7c15u64;
        for i in 0..20_000u32 {
            let r = rng(&mut st);
            let himo = (r % 4) as u16;
            let len = ((r >> 8) % 21) as usize;
            // 鍵の種類を絞って同じ主キーを何度も当てる
            let seed = (r >> 16) % 6000;
            let key: Vec<u8> = (0..len).map(|j| (seed >> (j % 8 * 8)) as u8 ^ j as u8).collect();
            let t = Hlc { wall: (r >> 40) % 1000, logical: (r >> 20) as u32 % 3, peer: 1 };
            let slot = if r.is_multiple_of(5) { NO_SLOT } else { i };
            p.note(himo, &key, slot, t);
            let e = want.entry((himo, key.clone())).or_insert(t);
            if *e < t {
                *e = t;
            }
            if slot != NO_SLOT {
                slot_of.insert(slot, (himo, key));
            }
            if r.is_multiple_of(7) {
                let gone = (rng(&mut st) % (i as u64 + 1)) as u32;
                assert_eq!(p.forget_slot(gone), slot_of.remove(&gone).is_some());
            }
        }
        let check = |p: &PkTombs| {
            assert_eq!(p.key_count(), want.len());
            for ((h, k), t) in &want {
                assert_eq!(p.tomb_of(*h, k), Some(*t), "himo {h} 鍵 {k:?}");
            }
            let mut seen: Vec<(u16, Vec<u8>)> = p.keys().map(|(h, k)| (h, k.to_vec())).collect();
            seen.sort();
            let mut all: Vec<(u16, Vec<u8>)> = want.keys().cloned().collect();
            all.sort();
            assert_eq!(seen, all);
            for slot in 0..20_000u32 {
                assert_eq!(p.key_of_slot(slot), slot_of.get(&slot).map(|(h, k)| (*h, &k[..])), "slot {slot}");
            }
            // 無い主キー (himo が違う / 長さが違う / 中身が違う)
            assert_eq!(p.tomb_of(9, b""), None);
            assert_eq!(p.tomb_of(0, &[0xEE; 21]), None);
            assert_eq!(p.tomb_of(1, &[0xEE; 8]), None);
        };
        check(&p);
        let mut file = Vec::new();
        write(&mut file, &mut p);
        let (back, torn) = PkTombs::load(&file).unwrap();
        assert!(!torn);
        check(&back);
        assert!(same(&back, &p));
    }

    #[test]
    fn the_same_bytes_under_another_himo_are_another_key() {
        // 同じ bytes の主キーを himo 2000 本に: 表の探しは別の himo の同じ bytes の番号を何度も通る
        let mut p = PkTombs::default();
        for himo in 0..2000u16 {
            assert!(p.note(himo, b"same", himo as u32, hlc(himo as u64)));
            assert!(p.note(himo, &[7u8; 12], NO_SLOT, hlc(10_000 + himo as u64)));
        }
        assert_eq!(p.key_count(), 4000);
        for himo in 0..2000u16 {
            assert_eq!(p.tomb_of(himo, b"same"), Some(hlc(himo as u64)), "himo {himo}");
            assert_eq!(p.tomb_of(himo, &[7u8; 12]), Some(hlc(10_000 + himo as u64)), "himo {himo}");
            assert_eq!(p.key_of_slot(himo as u32), Some((himo, &b"same"[..])));
        }
        assert_eq!(p.tomb_of(2000, b"same"), None);
    }

    #[test]
    fn the_bloom_grows_with_the_keys_and_misses_none() {
        let n = 300_000u64;
        let mut p = PkTombs::default();
        let bloom = PkBloom::default();
        assert!(!bloom.maybe(3, &0u64.to_le_bytes()), "何も覚えていなければ表も無い");
        for i in 0..n {
            let key = (i * 2).to_le_bytes();
            p.note(3, &key, NO_SLOT, hlc(i));
            bloom.add(&p, 3, &key);
        }
        assert!(bloom.bits() >= n as usize * BLOOM_BITS_PER_KEY, "主キー 1 つに 16 bit 以上: {}", bloom.bits());
        for i in 0..n {
            assert!(bloom.maybe(3, &(i * 2).to_le_bytes()), "覚えた主キーを落とさない: {i}");
        }
        // 覚えていない主キーの素通り (64 KiB 固定の表だと 1 − e^(−30 万 / 2^19) ≈ 44 %)
        let passed = (0..n).filter(|i| bloom.maybe(3, &(i * 2 + 1).to_le_bytes())).count();
        let rate = passed as f64 / n as f64;
        assert!(rate < 0.04, "素通り {:.2} %", rate * 100.0);
        // 開き直し: 読んだ記録から一度に作る
        let mut file = Vec::new();
        write(&mut file, &mut p);
        let (back, _) = PkTombs::load(&file).unwrap();
        let fresh = PkBloom::default();
        assert!(fresh.fit(&back));
        assert!(!fresh.fit(&back), "足りていれば作り直さない");
        assert_eq!(fresh.bits(), bloom.bits());
        assert!((0..n).all(|i| fresh.maybe(3, &(i * 2).to_le_bytes())));
    }

    #[test]
    fn memory_no_longer_needed_is_released() {
        let mut p = PkTombs::default();
        let mut file = Vec::new();
        for i in 0..50_000u32 {
            p.note(3, &i.to_le_bytes(), i, hlc(i as u64));
        }
        // file が無いのでまとめ直し: 溜めの確保も手放す
        assert_eq!(write(&mut file, &mut p), Some("rewrite"));
        assert_eq!(p.pending.capacity(), 0, "まとめ直しの後に溜めの山が残らない");
        let peak = p.by_slot.capacity();
        for i in 0..49_000u32 {
            p.forget_slot(i);
        }
        assert!(p.by_slot.capacity() * 8 < peak, "slot の記録を外したら縮む: {} → {}", peak, p.by_slot.capacity());
        for i in 49_000..50_000u32 {
            assert_eq!(p.key_of_slot(i), Some((3, &i.to_le_bytes()[..])));
        }
        assert_eq!(p.key_count(), 50_000, "主キーの版数は残る");
    }
}
