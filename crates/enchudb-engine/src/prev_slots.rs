//! 場所を使い回した時の前の住人の記録 (`prev.seg`、 file version 15 の DB だけ。 計画は repo 直下の `EID_REUSE_PLAN.md` の
//! 「決まった事 (2026-11-15 夜)」 = 案 C)。
//!
//! 使い回す時 (`Engine::prepare_reuse`、 場所を生かす前) は、 前の住人 (消えた行) の削除の版数と名前を identity に移す (自分の行は
//! #914 / #915 の (自分の peer, 世代 | 場所)、 翻訳した行は #166 の写像を外した (相手の peer, 相手の番号))。 移した先 (`foreign_tombs`
//! と名前の記録) は consumer の次の周期 (`.eidmap` / `pktombs` / `pkformer` の書き出し) まで memory にだけあり、 場所の削除の版数
//! (tomb.seg) は使い回しがその場で 0 にする。 その間に落ちると前の住人の削除がどこにも残らない (kill = page cache は残るので、
//! 本体には 0 にした tomb.seg が届く。 review921 [4] / review930 [Q4])。
//!
//! そこで使い回す時に、 tomb.seg を 0 にする前に、 場所ごとの 32 B に前の住人を書く (mmap に書くだけ、 system call なし):
//!
//! ```text
//! 0..4    前の住人の行の番号 (この DB の 世代 | 場所)
//! 4..8    identity の peer (自分の行 = 使い回した時の自分の peer、 翻訳した行 = 相手の peer)
//! 8..12   identity の番号 (自分の行 = 0..4 と同じ、 翻訳した行 = 相手の番号)
//! 12..28  削除の版数 (HLC: wall u64 / logical u32 / peer u32)
//! 28..32  下の 8 bit = 印 (1 = 自分の行、 2 = 翻訳した行)、 上の 24 bit = 0..28 と印の FNV-1a (0 にしない。 書きかけ /
//!         書き出しの途中の page を記録と読まない)
//! ```
//!
//! - きれいに閉じなかった後に開く時:
//!   - `.eidmap` の写像の面が、 記録の identity (翻訳した行) のまま場所を指していて、 使い回しが本体に届いている (場所の最後の行が
//!     記録の行でない) なら、 写像を戻さず削除の版数を identity に退避する (`Engine::prev_evicted`、 #925 の推測より先に決める)
//!   - 記録の identity の削除が退避表に無ければ戻す (`Engine::restore_prev_residents`。 名前は今まで通り sidecar から、
//!     `move_stale_slot_names` と同じ決め方)
//!   - WAL の再生は、 記録の行 (使い回した前の住人) 宛ての record を当てない (`Engine::replay_slot`)
//! - bridge は、 覚え (`FormerAddresses`) の無い前の住人の record の宛名を記録から引く (`Engine::bridge_row_address`)
//! - 書き出しの順: 本体の書き出し (`write_out_body`) は `prev.seg` を生きている印 (entities / gen.seg) と tomb.seg より先に書き出す。
//!   使い回しは記録を書いてから門 ([`PrevSlots::enter`]) の中で tomb.seg を 0 にし、 書き出しは門を閉じて (門を通った書き手が
//!   居れば) `prev.seg` をもう一度書き出してから tomb.seg を書き出す (#656 の 「中身 → 列」 と同じ並べ方)。 0 にした tomb.seg が
//!   届いた像には記録も届いている
//! - commit は書いた場所まで歩幅で伸ばす (file は疎、 gen.seg と同じ)
//! - 場所を次に使い回したら上書きする (書き出し済みの退避と重なっても残してよい: 開く時に退避表と比べて、 載っていれば何もしない)
//! - 記録は sync する DB (`sync_tables_on`) の使い回しで、 移した削除がある時だけ書く。 世代を持たない DB (v14 以下 / packed /
//!   wasm / 予約の表) は作らない・書かない

use crate::region::Region;
use enchudb_oplog::{Hlc, PeerId};
use std::sync::OnceLock;

/// `prev.seg` の先頭の header の長さ。 [`MAGIC`] だけ (残りは空き)。
pub(crate) const PREV_HEADER: usize = 4096;
const MAGIC: [u8; 4] = *b"PRV1";
/// 場所ごとの記録の長さ。
pub(crate) const RECORD: usize = 32;

const KIND_OWN: u32 = 1;
const KIND_TRANSLATED: u32 = 2;

/// `prev.seg` の大きさ (header + 32 B / 場所)。
pub(crate) fn segment_len(entities: u32) -> usize {
    PREV_HEADER + RECORD * entities as usize
}

#[inline]
fn record_off(slot: u32) -> usize {
    PREV_HEADER + RECORD * slot as usize
}

/// 前の住人が自分の行か、 翻訳した行 (受けた他の peer の行) か。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PrevKind {
    Own,
    Translated,
}

/// 場所の前の住人の記録。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PrevRecord {
    pub(crate) kind: PrevKind,
    /// 前の住人の行の番号 (この DB の 世代 | 場所)
    pub(crate) gone: u32,
    /// identity の peer
    pub(crate) peer: PeerId,
    /// identity の番号 (自分の行 = `gone`、 翻訳した行 = 相手の番号)
    pub(crate) number: u32,
    /// 削除の版数
    pub(crate) tomb: Hlc,
}

/// 0..28 と印の FNV-1a の上の 24 bit (0 にしない)。
fn check(b: &[u8], kind: u8) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &x in b.iter().chain(std::iter::once(&kind)) {
        h ^= u32::from(x);
        h = h.wrapping_mul(0x0100_0193);
    }
    (h >> 8).max(1)
}

impl PrevRecord {
    fn encode(&self) -> [u8; RECORD] {
        let mut b = [0u8; RECORD];
        b[0..4].copy_from_slice(&self.gone.to_le_bytes());
        b[4..8].copy_from_slice(&self.peer.to_le_bytes());
        b[8..12].copy_from_slice(&self.number.to_le_bytes());
        b[12..20].copy_from_slice(&self.tomb.wall.to_le_bytes());
        b[20..24].copy_from_slice(&self.tomb.logical.to_le_bytes());
        b[24..28].copy_from_slice(&self.tomb.peer.to_le_bytes());
        let kind = match self.kind {
            PrevKind::Own => KIND_OWN,
            PrevKind::Translated => KIND_TRANSLATED,
        } as u8;
        let word = (check(&b[..28], kind) << 8) | u32::from(kind);
        b[28..32].copy_from_slice(&word.to_le_bytes());
        b
    }

    fn decode(b: &[u8]) -> Option<Self> {
        let u32_at = |i: usize| u32::from_le_bytes(b[i..i + 4].try_into().unwrap());
        let word = u32_at(28);
        let kind = (word & 0xFF) as u8;
        if word >> 8 != check(&b[..28], kind) {
            return None;
        }
        let kind = match u32::from(kind) {
            KIND_OWN => PrevKind::Own,
            KIND_TRANSLATED => PrevKind::Translated,
            _ => return None,
        };
        let tomb = Hlc { wall: u64::from_le_bytes(b[12..20].try_into().unwrap()), logical: u32_at(20), peer: u32_at(24) };
        if tomb == Hlc::ZERO {
            return None;
        }
        Some(Self { kind, gone: u32_at(0), peer: u32_at(4), number: u32_at(8), tomb })
    }
}

/// `prev.seg` (世代を持たない DB / 開けなかった DB は off = 書かない・読むと None)。 [`crate::generation::Generations`] が持つ
/// (Engine に欄を足すと書き手の欄と同じ cache line に並びうる、 generation.rs の `Engine::generations` の doc)。
pub(crate) struct PrevSlots {
    region: OnceLock<Region>,
    /// 使い回しの tomb.seg を 0 にする書き込みと本体の書き出しの順 (列は 0 の 1 本だけ使う)
    gate: crate::publish_gate::PublishGate,
    /// 開く時に記録から退避を戻した場所の数 (観測・試験用)
    restored: std::sync::atomic::AtomicU32,
}

impl PrevSlots {
    pub(crate) fn off() -> Self {
        Self {
            region: OnceLock::new(),
            gate: crate::publish_gate::PublishGate::new(),
            restored: std::sync::atomic::AtomicU32::new(0),
        }
    }

    /// 開く時に記録から退避を戻した場所の数を足す / 読む (観測・試験用)。
    pub(crate) fn note_restored(&self, n: u32) {
        self.restored.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    }

    pub(crate) fn restored(&self) -> u32 {
        self.restored.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// 開いた v15 の DB の `prev.seg`。 header が合わない (作った直後に落ちた / 前の binary の残り) 時は、 `writable` なら中身を 0 に
    /// 戻して使う (記録なし)、 readonly なら使わない。 使えるか。
    pub(crate) fn attach(&self, region: Region, writable: bool) -> bool {
        let ok = region.committed_len() >= PREV_HEADER && region.read_atomic(0, 4) == MAGIC;
        if ok {
            let _ = self.region.set(region);
            return true;
        }
        writable && self.prepare(region).is_ok()
    }

    /// 0 から始める (前の残りは消す): v15 に移る時 / 開いた `prev.seg` の header が合わない時。
    pub(crate) fn prepare(&self, region: Region) -> std::io::Result<()> {
        region.ensure_committed(PREV_HEADER)?;
        let used = region.committed_len();
        if used > 0 {
            region.fill_at(0, used, 0);
            region.mark_dirty(0, used);
        }
        region.write_atomic(0, &MAGIC);
        region.mark_dirty(0, PREV_HEADER);
        let _ = self.region.set(region);
        Ok(())
    }

    /// `prev.seg` を持つか。
    #[inline]
    pub(crate) fn active(&self) -> bool {
        self.region.get().is_some()
    }

    /// 記録のありうる場所の上限 (commit 済みの範囲。 開く時のなめの終わり)。
    pub(crate) fn slots_end(&self) -> u32 {
        let Some(r) = self.region.get() else { return 0 };
        let len = r.committed_len().min(r.len());
        (len.saturating_sub(PREV_HEADER) / RECORD).min(u32::MAX as usize) as u32
    }

    /// 場所 `slot` に前の住人を書く (mmap に書くだけ。 commit を伸ばす時だけ file を伸ばす、 歩幅は `grow_amortized`)。 書けたか (書けない = `prev.seg` が無い /
    /// 範囲の外 / 空き不足)。 呼び手は場所を握っている (行の lock と空きの一覧から取り出した場所 = 同じ場所の書き手は 1 本)。
    pub(crate) fn write(&self, slot: u32, rec: &PrevRecord) -> bool {
        let Some(r) = self.region.get() else { return false };
        let off = record_off(slot);
        // commit は gen.seg と同じく歩幅で伸ばす (`ensure_committed`)。 page ごとに伸ばす `ensure_committed_sparse` は、 使い回す
        // 場所が上へ進む間 page ごとに file を伸ばして空きを見に行き、 名前のある表の 「消して足す」 が 1 回 +0.5 µs だった
        if off + RECORD > r.len() || r.ensure_committed(off + RECORD).is_err() {
            return false;
        }
        r.write_atomic(off, &rec.encode());
        r.mark_dirty(off, RECORD);
        true
    }

    /// 場所 `slot` の記録 (無い / 書きかけ / 範囲の外は None)。
    pub(crate) fn read(&self, slot: u32) -> Option<PrevRecord> {
        let r = self.region.get()?;
        let off = record_off(slot);
        if off + RECORD > r.len() || !r.is_committed(off + RECORD) {
            return None;
        }
        // 書いていない場所 (印と check の欄が 0) は残りを読まない (開く時のなめ)
        let last = r.as_atomic_u32(off + RECORD - 4).load(std::sync::atomic::Ordering::Relaxed);
        if last == 0 {
            return None;
        }
        let mut b = [0u8; RECORD];
        for i in (0..RECORD).step_by(4) {
            b[i..i + 4].copy_from_slice(&r.as_atomic_u32(off + i).load(std::sync::atomic::Ordering::Relaxed).to_le_bytes());
        }
        PrevRecord::decode(&b)
    }

    /// 使い回しが tomb.seg を 0 にする間持つ (記録を書いた後)。 書き出しが門を閉じていれば開くまで待つ。
    #[inline]
    pub(crate) fn enter(&self, slot: u32) -> crate::publish_gate::Pass<'_> {
        self.gate.enter(0, slot)
    }

    /// 書き出しが tomb.seg を書き出す間閉じる。 返った後は、 門の中で 0 にした tomb.seg の前に書いた記録が全部見える。 印
    /// ([`Self::take_written`]) が立っていれば `prev.seg` をもう一度書き出してから tomb.seg を書き出す。
    pub(crate) fn close(&self) -> crate::publish_gate::Closed<'_> {
        self.gate.close([0])
    }

    /// 前に閉じてから門を通った使い回しが居たか (印を下ろす。 閉じている間に呼ぶ)。
    pub(crate) fn take_written(&self) -> bool {
        self.gate.take_written(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn region(v: &mut [u64]) -> Region {
        unsafe { Region::new(v.as_mut_ptr() as *mut u8, v.len() * 8) }
    }

    #[test]
    fn a_record_reads_back_and_a_torn_one_does_not() {
        let mut buf = vec![0u64; segment_len(64).div_ceil(8)];
        let p = PrevSlots::off();
        p.prepare(region(&mut buf)).unwrap();
        assert_eq!(p.read(3), None, "書いていない場所は記録なし");
        let rec = PrevRecord {
            kind: PrevKind::Own,
            gone: (2 << 24) | 3,
            peer: 7,
            number: (2 << 24) | 3,
            tomb: Hlc { wall: 1_234, logical: 5, peer: 7 },
        };
        assert!(p.write(3, &rec));
        assert_eq!(p.read(3), Some(rec));
        let tr = PrevRecord {
            kind: PrevKind::Translated,
            gone: (1 << 24) | 3,
            peer: 9,
            number: 11,
            tomb: Hlc { wall: 99, logical: 0, peer: 9 },
        };
        assert!(p.write(3, &tr), "上書き");
        assert_eq!(p.read(3), Some(tr));
        // 書きかけ (版数の 1 byte だけ違う) は記録と読まない
        let r = p.region.get().unwrap();
        r.write_at(record_off(3) + 13, &[0xAA]);
        assert_eq!(p.read(3), None, "check の合わない記録を読んだ");
        assert!(!p.write(64, &tr), "範囲の外に書いた");
    }

    #[test]
    fn attach_starts_over_a_segment_without_the_header_only_when_writable() {
        let mut buf = vec![0u64; segment_len(8).div_ceil(8)];
        let p = PrevSlots::off();
        assert!(!p.attach(region(&mut buf), false), "readonly で header の無い segment を使った");
        assert!(!p.active());
        let q = PrevSlots::off();
        assert!(q.attach(region(&mut buf), true));
        let rec = PrevRecord { kind: PrevKind::Translated, gone: 1, peer: 2, number: 5, tomb: Hlc { wall: 1, logical: 0, peer: 2 } };
        assert!(q.write(1, &rec));
        // もう一度開く (header がある) と記録が読める
        let again = PrevSlots::off();
        assert!(again.attach(region(&mut buf), false));
        assert_eq!(again.read(1), Some(rec));
    }
}
